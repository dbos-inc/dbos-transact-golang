package dbos

import (
	"context"
	"encoding/gob"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
)

/*******************************/
/******* WORKFLOW STATUS *******/
/*******************************/

type WorkflowStatusType string

const (
	WorkflowStatusPending         WorkflowStatusType = "PENDING"
	WorkflowStatusEnqueued        WorkflowStatusType = "ENQUEUED"
	WorkflowStatusSuccess         WorkflowStatusType = "SUCCESS"
	WorkflowStatusError           WorkflowStatusType = "ERROR"
	WorkflowStatusCancelled       WorkflowStatusType = "CANCELLED"
	WorkflowStatusRetriesExceeded WorkflowStatusType = "RETRIES_EXCEEDED"
)

type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`
	Status             WorkflowStatusType `json:"status"`
	Name               string             `json:"name"`
	AuthenticatedUser  *string            `json:"authenticated_user"`
	AssumedRole        *string            `json:"assumed_role"`
	AuthenticatedRoles *string            `json:"authenticated_roles"`
	Output             any                `json:"output"`
	Error              error              `json:"error"`
	ExecutorID         string             `json:"executor_id"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	ApplicationVersion string             `json:"application_version"`
	ApplicationID      *string            `json:"application_id"`
	Attempts           int                `json:"attempts"`
	QueueName          string             `json:"queue_name"`
	Timeout            time.Duration      `json:"timeout"`
	Deadline           time.Time          `json:"deadline"`
	StartedAt          time.Time          `json:"started_at"`
	DeduplicationID    *string            `json:"deduplication_id"`
	Input              any                `json:"input"`
	Priority           int                `json:"priority"`
}

// WorkflowState holds the runtime state for a workflow execution
type WorkflowState struct {
	WorkflowID  string
	stepCounter int
}

// NextStepID returns the next step ID and increments the counter
func (ws *WorkflowState) NextStepID() int {
	ws.stepCounter++
	return ws.stepCounter
}

/********************************/
/******* WORKFLOW HANDLES ********/
/********************************/
type WorkflowHandle[R any] interface {
	GetResult() (R, error)
	GetWorkflowID() string // XXX we could have a base struct with GetWorkflowID and then embed it in the implementations
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	workflowID string
	resultChan chan R
	errorChan  chan error
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult() (R, error) {
	// TODO check on channels to see if they are closed
	select {
	case result := <-h.resultChan:
		return result, nil
	case err := <-h.errorChan:
		var zero R
		return zero, err
	}
}

func (h *workflowHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

type workflowPollingHandle[R any] struct {
	workflowID string
}

func (h *workflowPollingHandle[R]) GetResult() (R, error) {
	ctx := context.Background()
	result, err := getExecutor().systemDB.AwaitWorkflowResult(ctx, h.workflowID)
	if result != nil {
		typedResult, ok := result.(R)
		if !ok {
			return *new(R), fmt.Errorf("failed to cast workflow result to expected type %T", typedResult)
		}
		return typedResult, err
	}
	return *new(R), err
}

func (h *workflowPollingHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type TypedErasedWorkflowWrapperFunc func(ctx context.Context, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)

var registry = make(map[string]TypedErasedWorkflowWrapperFunc)
var regMutex sync.RWMutex

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(fqn string, fn TypedErasedWorkflowWrapperFunc) {
	regMutex.Lock()
	defer regMutex.Unlock()

	if _, exists := registry[fqn]; exists {
		// TODO: consider printing a warning instead of panicking
		panic(fmt.Sprintf("workflow function '%s' is already registered", fqn))
	}

	registry[fqn] = fn
}

func WithWorkflow[P any, R any](fn WorkflowFunc[P, R]) WorkflowWrapperFunc[P, R] {
	if fn == nil {
		panic("workflow function cannot be nil")
	}
	// Registry the input/output types for gob encoding
	var p P
	var r R
	gob.Register(p)
	gob.Register(r)

	// Wrap the function in a durable workflow
	wrappedFunction := WorkflowWrapperFunc[P, R](func(ctx context.Context, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
		return runAsWorkflow(ctx, fn, input, opts...)
	})

	// Register a type-erased version of the durable workflow for recovery
	fqn := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	typeErasedWrapper := func(ctx context.Context, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, fmt.Errorf("invalid input type for workflow %s", fqn)
		}

		handle, err := wrappedFunction(ctx, typedInput, opts...)
		if err != nil {
			return nil, err
		}
		return &workflowPollingHandle[any]{workflowID: handle.GetWorkflowID()}, nil
	}
	registerWorkflow(fqn, typeErasedWrapper)

	return wrappedFunction
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/
type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)
type WorkflowWrapperFunc[P any, R any] func(ctx context.Context, input P, opts ...WorkflowOption) (WorkflowHandle[R], error)

type WorkflowParams struct {
	WorkflowID string
	Timeout    time.Duration
	Deadline   time.Time
	QueueName  string
}

// WorkflowOption is a functional option for configuring workflow parameters
type WorkflowOption func(*WorkflowParams)

// WithWorkflowID sets the workflow ID
func WithWorkflowID(id string) WorkflowOption {
	return func(p *WorkflowParams) {
		p.WorkflowID = id
	}
}

// WithTimeout sets the workflow timeout
func WithTimeout(timeout time.Duration) WorkflowOption {
	return func(p *WorkflowParams) {
		p.Timeout = timeout
	}
}

// WithDeadline sets the workflow deadline
func WithDeadline(deadline time.Time) WorkflowOption {
	return func(p *WorkflowParams) {
		p.Deadline = deadline
	}
}

// WithQueue sets the queue name for the workflow
func WithQueue(queueName string) WorkflowOption {
	return func(p *WorkflowParams) {
		p.QueueName = queueName
	}
}

func runAsWorkflow[P any, R any](ctx context.Context, fn WorkflowFunc[P, R], input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	// Apply options to build params
	params := WorkflowParams{}
	for _, opt := range opts {
		opt(&params)
	}

	// First, create a context for the workflow
	dbosWorkflowContext := context.Background()

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := ctx.Value("workflowState").(*WorkflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

	// TODO Check if cancelled

	// Generate an ID for the workflow if not provided
	var workflowID string
	if params.WorkflowID == "" {
		if isChildWorkflow {
			stepID := parentWorkflowState.NextStepID()
			workflowID = fmt.Sprintf("%s-%d", parentWorkflowState.WorkflowID, stepID)
		} else {
			workflowID = uuid.New().String()
		}
	} else {
		workflowID = params.WorkflowID
	}

	// If this is a child workflow that has already been recorded in operations_output, return directly a polling handle
	if isChildWorkflow {
		childWorkflowID, err := getExecutor().systemDB.CheckChildWorkflow(dbosWorkflowContext, parentWorkflowState.WorkflowID, parentWorkflowState.stepCounter)
		if err != nil {
			return nil, fmt.Errorf("checking child workflow: %w", err)
		}
		if childWorkflowID != nil {
			return &workflowPollingHandle[R]{workflowID: *childWorkflowID}, nil
		}
	}

	var status WorkflowStatusType
	if params.QueueName != "" {
		status = WorkflowStatusEnqueued
	} else {
		status = WorkflowStatusPending
	}

	workflowStatus := WorkflowStatus{
		Name:               runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // XXX the interface approach would encapsulate this
		ApplicationVersion: APP_VERSION,
		ExecutorID:         EXECUTOR_ID,
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           params.Deadline,
		Timeout:            params.Timeout,
		Input:              input,
		ApplicationID:      nil, // TODO: set application ID if available
		QueueName:          params.QueueName,
	}
	fmt.Println("Workflow status:", workflowStatus)

	// Init status and record child workflow relationship in a single transaction
	tx, err := getExecutor().systemDB.(*systemDatabase).pool.Begin(dbosWorkflowContext)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(dbosWorkflowContext) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := InsertWorkflowStatusDBInput{
		Status: workflowStatus,
		Tx:     tx,
	}
	insertStatusResult, err := getExecutor().systemDB.InsertWorkflowStatus(dbosWorkflowContext, insertInput)
	if err != nil {
		return nil, fmt.Errorf("inserting workflow status: %w", err)
	}

	// Return a polling handle if: we are enqueueing, the workflow is already in a terminal state (success or error),
	if len(params.QueueName) > 0 || insertStatusResult.Status == WorkflowStatusSuccess || insertStatusResult.Status == WorkflowStatusError {
		// Commit the transaction to update the number of attempts and/or enact the enqueue
		if err := tx.Commit(dbosWorkflowContext); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
		return &workflowPollingHandle[R]{workflowID: workflowStatus.ID}, nil
	}

	// Record child workflow relationship if this is a child workflow
	if isChildWorkflow {
		// Get the step ID that was used for generating the child workflow ID
		stepID := parentWorkflowState.stepCounter
		childInput := RecordChildWorkflowDBInput{
			ParentWorkflowID: parentWorkflowState.WorkflowID,
			ChildWorkflowID:  workflowStatus.ID,
			FunctionName:     runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // Will need to test this
			FunctionID:       stepID,
			Tx:               tx,
		}
		err = getExecutor().systemDB.RecordChildWorkflow(dbosWorkflowContext, childInput)
		if err != nil {
			return nil, fmt.Errorf("recording child workflow: %w", err)
		}
	}

	// Commit the transaction
	if err := tx.Commit(dbosWorkflowContext); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Channel to receive the result from the goroutine
	// The buffer size of 1 allows the goroutine to send the result without blocking
	// In addition it allows the channel to be garbage collected
	resultChan := make(chan R, 1)
	errorChan := make(chan error, 1)

	// Create the handle
	handle := &workflowHandle[R]{
		workflowID: workflowStatus.ID,
		resultChan: resultChan,
		errorChan:  errorChan,
	}

	// Create workflow state to track step execution
	workflowState := &WorkflowState{
		WorkflowID:  workflowStatus.ID,
		stepCounter: -1,
	}

	// Run the function in a goroutine
	augmentUserContext := context.WithValue(ctx, "workflowState", workflowState)
	go func() {
		result, err := fn(augmentUserContext, input)
		if err != nil {
			fmt.Println("workflow function returned an error:", err)
			recordErr := getExecutor().systemDB.UpdateWorkflowOutcome(dbosWorkflowContext, UpdateWorkflowOutcomeDBInput{workflowID: workflowStatus.ID, status: WorkflowStatusError, err: err})
			if recordErr != nil {
				// TODO: make sure to return both errors
				errorChan <- recordErr
				return
			}
			errorChan <- err
		} else {
			fmt.Println("workflow function completed successfully, output:", result)
			recordErr := getExecutor().systemDB.UpdateWorkflowOutcome(dbosWorkflowContext, UpdateWorkflowOutcomeDBInput{workflowID: workflowStatus.ID, status: WorkflowStatusSuccess, output: result})
			if recordErr != nil {
				// We cannot return the user code result because we failed to record the output
				errorChan <- recordErr
				return
			}
			resultChan <- result
		}
	}()

	// Run the peer goroutine to handle cancellation and timeout
	/*
		if dbosWorkflowContext.Done() != nil {
			fmt.Println("starting goroutine to handle workflow context cancellation or timeout")
			go func() {
				select {
				case <-dbosWorkflowContext.Done():
					// The context was cancelled or timed out: record timeout or cancellation
					// CANCEL WORKFLOW HERE
					return
				}
			}()
		}
	*/

	return handle, nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

type StepFunc[P any, R any] func(ctx context.Context, input P) (R, error)

type StepParams struct {
	MaxAttempts int
	BackoffRate int
}

// StepOption is a functional option for configuring step parameters
type StepOption func(*StepParams)

// WithMaxAttempts sets the maximum number of retry attempts for a step
func WithMaxAttempts(maxAttempts int) StepOption {
	return func(p *StepParams) {
		p.MaxAttempts = maxAttempts
	}
}

// WithBackoffRate sets the backoff rate for retries
func WithBackoffRate(backoffRate int) StepOption {
	return func(p *StepParams) {
		p.BackoffRate = backoffRate
	}
}

func RunAsStep[P any, R any](ctx context.Context, fn StepFunc[P, R], input P, opts ...StepOption) (R, error) {
	// Apply options to build params
	params := StepParams{}
	for _, opt := range opts {
		opt(&params)
	}

	// Get workflow state from context
	workflowState, ok := ctx.Value("workflowState").(*WorkflowState)
	if !ok || workflowState == nil {
		return *new(R), fmt.Errorf("context does not contain valid workflow state, cannot run step")
	}

	// Get next step ID
	operationID := workflowState.NextStepID()

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := getExecutor().systemDB.CheckOperationExecution(ctx, CheckOperationExecutionDBInput{
		workflowID:   workflowState.WorkflowID,
		operationID:  operationID,
		functionName: runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(),
	})
	if err != nil {
		return *new(R), fmt.Errorf("failed to check operation execution: %w", err)
	}
	if recordedOutput != nil {
		return recordedOutput.output.(R), recordedOutput.err
	}

	dbInput := recordOperationResultDBInput{
		workflowID:    workflowState.WorkflowID,
		operationName: runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(),
		operationID:   operationID,
	}
	stepOutput, stepError := fn(ctx, input)
	if stepError != nil {
		fmt.Println("step function returned an error:", stepError)
		dbInput.err = stepError
		err := getExecutor().systemDB.RecordOperationResult(ctx, dbInput)
		if err != nil {
			fmt.Println("failed to record step error:", err)
			return *new(R), fmt.Errorf("failed to record step error: %w", err)
		}
	} else {
		fmt.Println("step function completed successfully, output:", stepOutput)
		dbInput.output = stepOutput
		err := getExecutor().systemDB.RecordOperationResult(ctx, dbInput)
		if err != nil {
			fmt.Println("failed to record step output:", err)
			return *new(R), fmt.Errorf("failed to record step output: %w", err)
		}
	}
	return stepOutput, stepError
}

/***********************************/
/******* WORKFLOW MANAGEMENT *******/
/***********************************/

func RetrieveWorkflow[R any](workflowID string) (workflowPollingHandle[R], error) {
	ctx := context.Background()
	workflowStatus, err := getExecutor().systemDB.ListWorkflows(ctx, ListWorkflowsDBInput{
		WorkflowIDs: []string{workflowID},
	})
	if err != nil {
		return workflowPollingHandle[R]{}, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return workflowPollingHandle[R]{}, fmt.Errorf("workflow with ID '%s' not found", workflowID)
	}
	return workflowPollingHandle[R]{workflowID: workflowID}, nil
}

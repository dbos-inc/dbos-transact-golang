package dbos

import (
	"context"
	"encoding/gob"
	"errors"
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
	ApplicationID      string             `json:"application_id"`
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

// workflowOutcome holds the result and error from workflow execution
type workflowOutcome[R any] struct {
	result R
	err    error
}

type WorkflowHandle[R any] interface {
	GetResult(ctx context.Context) (R, error)
	GetStatus() (WorkflowStatus, error)
	GetWorkflowID() string // XXX we could have a base struct with GetWorkflowID and then embed it in the implementations
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	workflowID  string
	outcomeChan chan workflowOutcome[R]
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult(ctx context.Context) (R, error) {
	outcome, ok := <-h.outcomeChan // Blocking read
	if !ok {
		// Return an error if the channel was closed. In normal operations this would happen if GetResul() is called twice on a handler. The first call should get the buffered result, the second call find zero values (channel is empty and closed).
		return *new(R), errors.New("workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?")
	}
	// If we are calling GetResult inside a workflow, record the result as a step result
	parentWorkflowState, ok := ctx.Value(workflowStateKey).(*WorkflowState)
	isChildWorkflow := ok && parentWorkflowState != nil
	if isChildWorkflow {
		encodedOutput, encErr := serialize(outcome.result)
		if encErr != nil {
			return *new(R), NewWorkflowExecutionError(parentWorkflowState.WorkflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
		}
		recordGetResultInput := recordChildGetResultDBInput{
			parentWorkflowID: parentWorkflowState.WorkflowID,
			childWorkflowID:  h.workflowID,
			operationID:      parentWorkflowState.NextStepID(),
			output:           encodedOutput,
			err:              outcome.err,
		}
		recordResultErr := getExecutor().systemDB.RecordChildGetResult(ctx, recordGetResultInput)
		if recordResultErr != nil {
			// XXX do we want to fail this?
			fmt.Println("failed to record get result:", recordResultErr)
		}
	}
	return outcome.result, outcome.err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowHandle[R]) GetStatus() (WorkflowStatus, error) {
	ctx := context.Background()
	workflowStatuses, err := getExecutor().systemDB.ListWorkflows(ctx, ListWorkflowsDBInput{
		WorkflowIDs: []string{h.workflowID},
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, NewNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

type workflowPollingHandle[R any] struct {
	workflowID string
}

func (h *workflowPollingHandle[R]) GetResult(ctx context.Context) (R, error) {
	result, err := getExecutor().systemDB.AwaitWorkflowResult(ctx, h.workflowID)
	if result != nil {
		typedResult, ok := result.(R)
		if !ok {
			// TODO check what this looks like in practice
			return *new(R), NewWorkflowUnexpectedResultType(h.workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", result))
		}
		// If we are calling GetResult inside a workflow, record the result as a step result
		parentWorkflowState, ok := ctx.Value(workflowStateKey).(*WorkflowState)
		isChildWorkflow := ok && parentWorkflowState != nil
		if isChildWorkflow {
			encodedOutput, encErr := serialize(typedResult)
			if encErr != nil {
				return *new(R), NewWorkflowExecutionError(parentWorkflowState.WorkflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
			}
			recordGetResultInput := recordChildGetResultDBInput{
				parentWorkflowID: parentWorkflowState.WorkflowID,
				childWorkflowID:  h.workflowID,
				operationID:      parentWorkflowState.NextStepID(),
				output:           encodedOutput,
				err:              err,
			}
			recordResultErr := getExecutor().systemDB.RecordChildGetResult(ctx, recordGetResultInput)
			if recordResultErr != nil {
				// XXX do we want to fail this?
				fmt.Println("failed to record get result:", recordResultErr)
			}
		}
		return typedResult, err
	}
	return *new(R), err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowPollingHandle[R]) GetStatus() (WorkflowStatus, error) {
	ctx := context.Background()
	workflowStatuses, err := getExecutor().systemDB.ListWorkflows(ctx, ListWorkflowsDBInput{
		WorkflowIDs: []string{h.workflowID},
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, NewNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowPollingHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type TypedErasedWorkflowWrapperFunc func(ctx context.Context, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)

type workflowRegistryEntry struct {
	wrappedFunction TypedErasedWorkflowWrapperFunc
	maxRetries      int
}

var registry = make(map[string]workflowRegistryEntry)
var regMutex sync.RWMutex

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(fqn string, fn TypedErasedWorkflowWrapperFunc, maxRetries int) {
	regMutex.Lock()
	defer regMutex.Unlock()

	if _, exists := registry[fqn]; exists {
		fmt.Println("Error: workflow function already registered:", fqn)
		panic(NewConflictingRegistrationError(fqn))
	}

	registry[fqn] = workflowRegistryEntry{
		wrappedFunction: fn,
		maxRetries:      maxRetries,
	}
}

type WorkflowRegistrationParams struct {
	MaxRetries int
	// Likely we will allow a name here
}

type WorkflowRegistrationOption func(*WorkflowRegistrationParams)

const (
	DEFAULT_MAX_RECOVERY_ATTEMPTS = 100
)

func WithMaxRetries(maxRetries int) WorkflowRegistrationOption {
	return func(p *WorkflowRegistrationParams) {
		p.MaxRetries = maxRetries
	}
}

func WithWorkflow[P any, R any](fn WorkflowFunc[P, R], opts ...WorkflowRegistrationOption) WorkflowWrapperFunc[P, R] {
	if getExecutor() != nil {
		fmt.Println("warning: WithWorkflow called after DBOS initialization, dynamic registration is not supported")
		return nil
	}

	registrationParams := WorkflowRegistrationParams{
		MaxRetries: DEFAULT_MAX_RECOVERY_ATTEMPTS, // TODO implement "infinite" retries if wanted
	}
	for _, opt := range opts {
		opt(&registrationParams)
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}
	// Registry the input/output types for gob encoding
	var p P
	var r R
	gob.Register(p)
	gob.Register(r)

	// Wrap the function in a durable workflow
	wrappedFunction := WorkflowWrapperFunc[P, R](func(ctx context.Context, workflowInput P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
		opts = append(opts, WithWorkflowMaxRetries(registrationParams.MaxRetries))
		return runAsWorkflow(ctx, fn, workflowInput, opts...)
	})

	// Register a type-erased version of the durable workflow for recovery
	fqn := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	typeErasedWrapper := func(ctx context.Context, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, NewWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input))
		}

		handle, err := wrappedFunction(ctx, typedInput, opts...)
		if err != nil {
			return nil, err
		}
		return &workflowPollingHandle[any]{workflowID: handle.GetWorkflowID()}, nil
	}
	registerWorkflow(fqn, typeErasedWrapper, registrationParams.MaxRetries)

	return wrappedFunction
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type contextKey string

const workflowStateKey contextKey = "workflowState"

type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)
type WorkflowWrapperFunc[P any, R any] func(ctx context.Context, input P, opts ...WorkflowOption) (WorkflowHandle[R], error)

type WorkflowParams struct {
	WorkflowID         string
	Timeout            time.Duration
	Deadline           time.Time
	QueueName          string
	ApplicationVersion string
	MaxRetries         int
}

type WorkflowOption func(*WorkflowParams)

func WithWorkflowID(id string) WorkflowOption {
	return func(p *WorkflowParams) {
		p.WorkflowID = id
	}
}

func WithTimeout(timeout time.Duration) WorkflowOption {
	return func(p *WorkflowParams) {
		p.Timeout = timeout
	}
}

func WithDeadline(deadline time.Time) WorkflowOption {
	return func(p *WorkflowParams) {
		p.Deadline = deadline
	}
}

func WithQueue(queueName string) WorkflowOption {
	return func(p *WorkflowParams) {
		p.QueueName = queueName
	}
}

func WithApplicationVersion(version string) WorkflowOption {
	return func(p *WorkflowParams) {
		p.ApplicationVersion = version
	}
}

func WithWorkflowMaxRetries(maxRetries int) WorkflowOption {
	return func(p *WorkflowParams) {
		p.MaxRetries = maxRetries
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
	parentWorkflowState, ok := ctx.Value(workflowStateKey).(*WorkflowState)
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
			return nil, NewWorkflowExecutionError(parentWorkflowState.WorkflowID, fmt.Sprintf("checking child workflow: %v", err))
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

	var appVersion string
	if len(params.ApplicationVersion) > 0 {
		appVersion = params.ApplicationVersion
	} else {
		appVersion = APP_VERSION
	}

	workflowStatus := WorkflowStatus{
		Name:               runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // XXX the interface approach would encapsulate this
		ApplicationVersion: appVersion,
		ExecutorID:         EXECUTOR_ID,
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           params.Deadline, // TODO compute the deadline based on the timeout
		Timeout:            params.Timeout,
		Input:              input,
		ApplicationID:      APP_ID,
		QueueName:          params.QueueName,
	}

	// Init status and record child workflow relationship in a single transaction
	tx, err := getExecutor().systemDB.(*systemDatabase).pool.Begin(dbosWorkflowContext)
	if err != nil {
		return nil, NewWorkflowExecutionError(workflowID, fmt.Sprintf("failed to begin transaction: %v", err))
	}
	defer tx.Rollback(dbosWorkflowContext) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := InsertWorkflowStatusDBInput{
		status:     workflowStatus,
		maxRetries: params.MaxRetries,
		tx:         tx,
	}
	insertStatusResult, err := getExecutor().systemDB.InsertWorkflowStatus(dbosWorkflowContext, insertInput)
	if err != nil {
		return nil, err
	}

	// Return a polling handle if: we are enqueueing, the workflow is already in a terminal state (success or error),
	if len(params.QueueName) > 0 || insertStatusResult.Status == WorkflowStatusSuccess || insertStatusResult.Status == WorkflowStatusError {
		// Commit the transaction to update the number of attempts and/or enact the enqueue
		if err := tx.Commit(dbosWorkflowContext); err != nil {
			return nil, NewWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
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
			return nil, NewWorkflowExecutionError(parentWorkflowState.WorkflowID, fmt.Sprintf("recording child workflow: %v", err))
		}
	}

	// Commit the transaction
	if err := tx.Commit(dbosWorkflowContext); err != nil {
		return nil, NewWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
	}

	// Channel to receive the outcome from the goroutine
	// The buffer size of 1 allows the goroutine to send the outcome without blocking
	// In addition it allows the channel to be garbage collected
	outcomeChan := make(chan workflowOutcome[R], 1)

	// Create the handle
	handle := &workflowHandle[R]{
		workflowID:  workflowStatus.ID,
		outcomeChan: outcomeChan,
	}

	// Create workflow state to track step execution
	workflowState := &WorkflowState{
		WorkflowID:  workflowStatus.ID,
		stepCounter: -1,
	}

	// Run the function in a goroutine
	augmentUserContext := context.WithValue(ctx, workflowStateKey, workflowState)
	go func() {
		result, err := fn(augmentUserContext, input)
		status := WorkflowStatusSuccess
		if err != nil {
			status = WorkflowStatusError
		}
		recordErr := getExecutor().systemDB.UpdateWorkflowOutcome(dbosWorkflowContext, UpdateWorkflowOutcomeDBInput{workflowID: workflowStatus.ID, status: status, err: err, output: result})
		if recordErr != nil {
			fmt.Println("Failed to record workflow outcome:", recordErr)
			// TODO: return both the recording error and the function error
			outcomeChan <- workflowOutcome[R]{result: *new(R), err: recordErr}
			close(outcomeChan) // Close the channel to signal completion
			return
		}
		outcomeChan <- workflowOutcome[R]{result: result, err: err}
		close(outcomeChan) // Close the channel to signal completion
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
	if fn == nil {
		return *new(R), NewStepExecutionError("", "", "step function cannot be nil")
	}

	operationName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	// Apply options to build params
	params := StepParams{}
	for _, opt := range opts {
		opt(&params)
	}

	// Get workflow state from context
	workflowState, ok := ctx.Value(workflowStateKey).(*WorkflowState)
	if !ok || workflowState == nil {
		return *new(R), NewStepExecutionError("", operationName, "workflow state not found in context")
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
		return *new(R), NewStepExecutionError(workflowState.WorkflowID, operationName, fmt.Sprintf("checking operation execution: %v", err))
	}
	if recordedOutput != nil {
		return recordedOutput.output.(R), recordedOutput.err
	}

	dbInput := recordOperationResultDBInput{
		workflowID:    workflowState.WorkflowID,
		operationName: operationName,
		operationID:   operationID,
	}
	stepOutput, stepError := fn(ctx, input)
	if stepError != nil {
		fmt.Println("step function returned an error:", stepError)
		dbInput.err = stepError
		err := getExecutor().systemDB.RecordOperationResult(ctx, dbInput)
		if err != nil {
			fmt.Println("failed to record step error:", err)
			return *new(R), NewStepExecutionError(workflowState.WorkflowID, operationName, fmt.Sprintf("recording step error: %v", err))
		}
	} else {
		fmt.Println("step function completed successfully, output:", stepOutput)
		dbInput.output = stepOutput
		err := getExecutor().systemDB.RecordOperationResult(ctx, dbInput)
		if err != nil {
			fmt.Println("failed to record step output:", err)
			return *new(R), NewStepExecutionError(workflowState.WorkflowID, operationName, fmt.Sprintf("recording step output: %v", err))
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
		return workflowPollingHandle[R]{}, NewNonExistentWorkflowError(workflowID)
	}
	return workflowPollingHandle[R]{workflowID: workflowID}, nil
}

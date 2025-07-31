package dbos

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
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

// workflowState holds the runtime state for a workflow execution
type workflowState struct {
	workflowID   string
	stepCounter  int
	isWithinStep bool
}

// NextStepID returns the next step ID and increments the counter
func (ws *workflowState) NextStepID() int {
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
	GetResult() (R, error)
	GetStatus() (WorkflowStatus, error)
	GetWorkflowID() string
}

type workflowHandleInternal struct {
	workflowID     string
	workflowStatus WorkflowStatus
}

// unimplemented
func (h *workflowHandleInternal) GetResult() (any, error) {
	return nil, nil
}

func (h *workflowHandleInternal) GetStatus() (WorkflowStatus, error) {
	return h.workflowStatus, nil
}

func (h *workflowHandleInternal) GetWorkflowID() string {
	return h.workflowID
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	workflowID  string
	outcomeChan chan workflowOutcome[R]
	dbosContext DBOSContext
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult() (R, error) {
	outcome, ok := <-h.outcomeChan // Blocking read
	if !ok {
		// Return an error if the channel was closed. In normal operations this would happen if GetResul() is called twice on a handler. The first call should get the buffered result, the second call find zero values (channel is empty and closed).
		return *new(R), errors.New("workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?")
	}
	// If we are calling GetResult inside a workflow, record the result as a step result
	parentWorkflowState, ok := h.dbosContext.(*dbosContext).ctx.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil
	if isChildWorkflow {
		encodedOutput, encErr := serialize(outcome.result)
		if encErr != nil {
			return *new(R), newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
		}
		recordGetResultInput := recordChildGetResultDBInput{
			parentWorkflowID: parentWorkflowState.workflowID,
			childWorkflowID:  h.workflowID,
			stepID:           parentWorkflowState.NextStepID(),
			output:           encodedOutput,
			err:              outcome.err,
		}
		recordResultErr := h.dbosContext.(*dbosContext).systemDB.RecordChildGetResult(h.dbosContext.(*dbosContext).ctx, recordGetResultInput)
		if recordResultErr != nil {
			getLogger().Error("failed to record get result", "error", recordResultErr)
			return *new(R), newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("recording child workflow result: %v", recordResultErr))
		}
	}
	return outcome.result, outcome.err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowHandle[R]) GetStatus() (WorkflowStatus, error) {
	workflowStatuses, err := h.dbosContext.(*dbosContext).systemDB.ListWorkflows(h.dbosContext.(*dbosContext).ctx, listWorkflowsDBInput{
		workflowIDs: []string{h.workflowID},
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

type workflowPollingHandle[R any] struct {
	workflowID  string
	dbosContext DBOSContext
}

func (h *workflowPollingHandle[R]) GetResult() (R, error) {
	ctx := context.Background()
	result, err := h.dbosContext.(*dbosContext).systemDB.AwaitWorkflowResult(h.dbosContext.(*dbosContext).ctx, h.workflowID)
	if result != nil {
		typedResult, ok := result.(R)
		if !ok {
			// TODO check what this looks like in practice
			return *new(R), newWorkflowUnexpectedResultType(h.workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", result))
		}
		// If we are calling GetResult inside a workflow, record the result as a step result
		parentWorkflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
		isChildWorkflow := ok && parentWorkflowState != nil
		if isChildWorkflow {
			encodedOutput, encErr := serialize(typedResult)
			if encErr != nil {
				return *new(R), newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("serializing child workflow result: %v", encErr))
			}
			recordGetResultInput := recordChildGetResultDBInput{
				parentWorkflowID: parentWorkflowState.workflowID,
				childWorkflowID:  h.workflowID,
				stepID:           parentWorkflowState.NextStepID(),
				output:           encodedOutput,
				err:              err,
			}
			recordResultErr := h.dbosContext.(*dbosContext).systemDB.RecordChildGetResult(h.dbosContext.(*dbosContext).ctx, recordGetResultInput)
			if recordResultErr != nil {
				// XXX do we want to fail this?
				getLogger().Error("failed to record get result", "error", recordResultErr)
			}
		}
		return typedResult, err
	}
	return *new(R), err
}

// GetStatus returns the current status of the workflow from the database
func (h *workflowPollingHandle[R]) GetStatus() (WorkflowStatus, error) {
	workflowStatuses, err := h.dbosContext.(*dbosContext).systemDB.ListWorkflows(h.dbosContext.(*dbosContext).ctx, listWorkflowsDBInput{
		workflowIDs: []string{h.workflowID},
	})
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowPollingHandle[R]) GetWorkflowID() string {
	return h.workflowID
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type GenericWrappedWorkflowFunc[P any, R any] func(dbosCtx DBOSContext, input P, opts ...WorkflowOption) (WorkflowHandle[R], error)
type WrappedWorkflowFunc func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)

type workflowRegistryEntry struct {
	wrappedFunction WrappedWorkflowFunc
	maxRetries      int
}

// Register adds a workflow function to the registry (thread-safe, only once per name)
func (c *dbosContext) RegisterWorkflow(fqn string, fn WrappedWorkflowFunc, maxRetries int) {
	c.workflowRegMutex.Lock()
	defer c.workflowRegMutex.Unlock()

	if _, exists := c.workflowRegistry[fqn]; exists {
		getLogger().Error("workflow function already registered", "fqn", fqn)
		panic(newConflictingRegistrationError(fqn))
	}

	c.workflowRegistry[fqn] = workflowRegistryEntry{
		wrappedFunction: fn,
		maxRetries:      maxRetries,
	}
}

func (c *dbosContext) RegisterScheduledWorkflow(fqn string, fn WrappedWorkflowFunc, cronSchedule string, maxRetries int) {
	c.getWorkflowScheduler().Start()
	var entryID cron.EntryID
	entryID, err := c.getWorkflowScheduler().AddFunc(cronSchedule, func() {
		// Execute the workflow on the cron schedule once DBOS is launched
		if c == nil {
			return
		}
		// Get the scheduled time from the cron entry
		entry := c.getWorkflowScheduler().Entry(entryID)
		scheduledTime := entry.Prev
		if scheduledTime.IsZero() {
			// Use Next if Prev is not set, which will only happen for the first run
			scheduledTime = entry.Next
		}
		wfID := fmt.Sprintf("sched-%s-%s", fqn, scheduledTime) // XXX we can rethink the format
		fn(c, scheduledTime, WithWorkflowID(wfID), WithQueue(_DBOS_INTERNAL_QUEUE_NAME))
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register scheduled workflow: %v", err))
	}
	getLogger().Info("Registered scheduled workflow", "fqn", fqn, "cron_schedule", cronSchedule)
}

type workflowRegistrationParams struct {
	cronSchedule string
	maxRetries   int
	// Likely we will allow a name here
}

type workflowRegistrationOption func(*workflowRegistrationParams)

const (
	_DEFAULT_MAX_RECOVERY_ATTEMPTS = 100
)

func WithMaxRetries(maxRetries int) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.maxRetries = maxRetries
	}
}

func WithSchedule(schedule string) workflowRegistrationOption {
	return func(p *workflowRegistrationParams) {
		p.cronSchedule = schedule
	}
}

// RegisterWorkflow registers the provided function as a durable workflow with the provided DBOSContext workflow registry
// If the workflow is a scheduled workflow (determined by the presence of a cron schedule), it will also register a cron job to execute it
// RegisterWorkflow is generically typed, allowing us to register the workflow input and output types for gob encoding
// The registered workflow is wrapped in a typed-erased wrapper which performs runtime type checks and conversions
// To execute the workflow, use DBOSContext.RunAsWorkflow
func RegisterWorkflow[P any, R any](dbosCtx DBOSContext, fn GenericWorkflowFunc[P, R], opts ...workflowRegistrationOption) {
	if dbosCtx == nil {
		panic("dbosCtx cannot be nil")
	}

	registrationParams := workflowRegistrationParams{
		maxRetries: _DEFAULT_MAX_RECOVERY_ATTEMPTS,
	}
	for _, opt := range opts {
		opt(&registrationParams)
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}
	fqn := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	// Registry the input/output types for gob encoding
	var p P
	var r R
	gob.Register(p)
	gob.Register(r)

	// Register a type-erased version of the durable workflow for recovery
	typeErasedWrapper := func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, newWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", typedInput), fmt.Sprintf("%T", input))
		}

		opts = append(opts, WithWorkflowMaxRetries(registrationParams.maxRetries))
		handle, err := RunAsWorkflow(ctx, fn, typedInput, opts...)
		if err != nil {
			return nil, err
		}
		return &workflowPollingHandle[any]{workflowID: handle.GetWorkflowID(), dbosContext: ctx}, nil
	}
	dbosCtx.RegisterWorkflow(fqn, typeErasedWrapper, registrationParams.maxRetries)

	// If this is a scheduled workflow, register a cron job
	if registrationParams.cronSchedule != "" {
		if reflect.TypeOf(p) != reflect.TypeOf(time.Time{}) {
			panic(fmt.Sprintf("scheduled workflow function must accept a time.Time as input, got %T", p))
		}
		dbosCtx.RegisterScheduledWorkflow(fqn, typeErasedWrapper, registrationParams.cronSchedule, registrationParams.maxRetries)
	}
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type contextKey string

const workflowStateKey contextKey = "workflowState"

type GenericWorkflowFunc[P any, R any] func(dbosCtx DBOSContext, input P) (R, error)
type WorkflowFunc func(ctx DBOSContext, input any) (WorkflowHandle[any], error)

type workflowParams struct {
	workflowID         string
	timeout            time.Duration
	deadline           time.Time
	queueName          string
	applicationVersion string
	maxRetries         int
}

type WorkflowOption func(*workflowParams)

func WithWorkflowID(id string) WorkflowOption {
	return func(p *workflowParams) {
		p.workflowID = id
	}
}

func WithTimeout(timeout time.Duration) WorkflowOption {
	return func(p *workflowParams) {
		p.timeout = timeout
	}
}

func WithDeadline(deadline time.Time) WorkflowOption {
	return func(p *workflowParams) {
		p.deadline = deadline
	}
}

func WithQueue(queueName string) WorkflowOption {
	return func(p *workflowParams) {
		p.queueName = queueName
	}
}

func WithApplicationVersion(version string) WorkflowOption {
	return func(p *workflowParams) {
		p.applicationVersion = version
	}
}

func WithWorkflowMaxRetries(maxRetries int) WorkflowOption {
	return func(p *workflowParams) {
		p.maxRetries = maxRetries
	}
}

func RunAsWorkflow[P any, R any](dbosCtx DBOSContext, fn GenericWorkflowFunc[P, R], input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	// Do the durability things
	typedErasedWorkflow := WorkflowFunc(func(ctx DBOSContext, input any) (WorkflowHandle[any], error) {
		// Dummy typed erased workflow -- we just need the name inside dbosCtx.RunAsWorkflow but want a matching signature
		return nil, nil
	})
	// Print fn name
	fmt.Println("Running workflow function:", runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name())
	// Print t wrapped function name
	fmt.Println("Running typed-erased workflow function:", runtime.FuncForPC(reflect.ValueOf(typedErasedWorkflow).Pointer()).Name())
	internalHandle, err := dbosCtx.(*dbosContext).RunAsWorkflow(dbosCtx, typedErasedWorkflow, input, opts...)
	if err != nil {
		return nil, err
	}

	// If we got a polling handle, return it directly
	if pollingHandle, ok := internalHandle.(*workflowPollingHandle[any]); ok {
		// We need to convert the polling handle to a typed handle
		typedPollingHandle := &workflowPollingHandle[R]{
			workflowID:  pollingHandle.workflowID,
			dbosContext: dbosCtx,
		}
		return typedPollingHandle, nil
	}

	// Channel to receive the outcome from the goroutine
	// The buffer size of 1 allows the goroutine to send the outcome without blocking
	// In addition it allows the channel to be garbage collected
	outcomeChan := make(chan workflowOutcome[R], 1)

	// Create the handle
	handle := &workflowHandle[R]{
		workflowID:  internalHandle.GetWorkflowID(),
		outcomeChan: outcomeChan,
		dbosContext: dbosCtx,
	}

	// Create workflow state to track step execution
	wfState := &workflowState{
		workflowID:  internalHandle.GetWorkflowID(),
		stepCounter: -1,
	}

	// Run the function in a goroutine
	augmentUserContext := dbosCtx.(*dbosContext).withValue(workflowStateKey, wfState)
	dbosCtx.(*dbosContext).workflowsWg.Add(1)
	go func() {
		defer dbosCtx.(*dbosContext).workflowsWg.Done()
		result, err := fn(augmentUserContext, input)
		status := WorkflowStatusSuccess
		if err != nil {
			status = WorkflowStatusError
		}
		recordErr := dbosCtx.(*dbosContext).systemDB.UpdateWorkflowOutcome(dbosCtx.(*dbosContext).ctx, updateWorkflowOutcomeDBInput{
			workflowID: internalHandle.GetWorkflowID(),
			status:     status,
			err:        err,
			output:     result,
		})
		if recordErr != nil {
			outcomeChan <- workflowOutcome[R]{result: *new(R), err: recordErr}
			close(outcomeChan) // Close the channel to signal completion
			return
		}
		outcomeChan <- workflowOutcome[R]{result: result, err: err}
		close(outcomeChan) // Close the channel to signal completion
	}()

	return handle, nil
}

func (c *dbosContext) RunAsWorkflow(dbosCtx DBOSContext, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
	// Apply options to build params
	params := workflowParams{
		applicationVersion: dbosCtx.GetApplicationVersion(),
	}
	for _, opt := range opts {
		opt(&params)
	}

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := dbosCtx.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

	// TODO Check if cancelled

	// Generate an ID for the workflow if not provided
	var workflowID string
	if params.workflowID == "" {
		if isChildWorkflow {
			stepID := parentWorkflowState.NextStepID()
			workflowID = fmt.Sprintf("%s-%d", parentWorkflowState.workflowID, stepID)
		} else {
			workflowID = uuid.New().String()
		}
	} else {
		workflowID = params.workflowID
	}

	// If this is a child workflow that has already been recorded in operations_output, return directly a polling handle
	if isChildWorkflow {
		childWorkflowID, err := dbosCtx.(*dbosContext).systemDB.CheckChildWorkflow(dbosCtx.(*dbosContext).ctx, parentWorkflowState.workflowID, parentWorkflowState.stepCounter)
		if err != nil {
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("checking child workflow: %v", err))
		}
		if childWorkflowID != nil {
			return &workflowPollingHandle[any]{workflowID: *childWorkflowID, dbosContext: dbosCtx}, nil
		}
	}

	var status WorkflowStatusType
	if params.queueName != "" {
		status = WorkflowStatusEnqueued
	} else {
		status = WorkflowStatusPending
	}

	workflowStatus := WorkflowStatus{
		Name:               runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // TODO factor out somewhere else so we dont' have to reflect here
		ApplicationVersion: params.applicationVersion,
		ExecutorID:         dbosCtx.GetExecutorID(),
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           params.deadline, // TODO compute the deadline based on the timeout
		Timeout:            params.timeout,
		Input:              input,
		ApplicationID:      dbosCtx.GetApplicationID(),
		QueueName:          params.queueName,
	}

	// Init status and record child workflow relationship in a single transaction
	tx, err := dbosCtx.(*dbosContext).systemDB.(*systemDatabase).pool.Begin(dbosCtx.(*dbosContext).ctx)
	if err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to begin transaction: %v", err))
	}
	defer tx.Rollback(dbosCtx.(*dbosContext).ctx) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := insertWorkflowStatusDBInput{
		status:     workflowStatus,
		maxRetries: params.maxRetries,
		tx:         tx,
	}
	insertStatusResult, err := dbosCtx.(*dbosContext).systemDB.InsertWorkflowStatus(dbosCtx.(*dbosContext).ctx, insertInput)
	if err != nil {
		return nil, err
	}

	// Return a polling handle if: we are enqueueing, the workflow is already in a terminal state (success or error),
	if len(params.queueName) > 0 || insertStatusResult.status == WorkflowStatusSuccess || insertStatusResult.status == WorkflowStatusError {
		// Commit the transaction to update the number of attempts and/or enact the enqueue
		if err := tx.Commit(dbosCtx.(*dbosContext).ctx); err != nil {
			return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
		}
		return &workflowPollingHandle[any]{workflowID: workflowStatus.ID, dbosContext: dbosCtx}, nil
	}

	// Record child workflow relationship if this is a child workflow
	if isChildWorkflow {
		// Get the step ID that was used for generating the child workflow ID
		stepID := parentWorkflowState.stepCounter
		childInput := recordChildWorkflowDBInput{
			parentWorkflowID: parentWorkflowState.workflowID,
			childWorkflowID:  workflowStatus.ID,
			stepName:         runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name(), // Will need to test this
			stepID:           stepID,
			tx:               tx,
		}
		err = dbosCtx.(*dbosContext).systemDB.RecordChildWorkflow(dbosCtx.(*dbosContext).ctx, childInput)
		if err != nil {
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Sprintf("recording child workflow: %v", err))
		}
	}

	// Commit the transaction
	if err := tx.Commit(dbosCtx.(*dbosContext).ctx); err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Sprintf("failed to commit transaction: %v", err))
	}

	return &workflowHandleInternal{workflowID: workflowID}, nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

type GenericStepFunc[P any, R any] func(ctx context.Context, input P) (R, error)
type StepFunc func(ctx context.Context, input any) (any, error)

type StepParams struct {
	MaxRetries    int
	BackoffFactor float64
	BaseInterval  time.Duration
	MaxInterval   time.Duration
}

// StepOption is a functional option for configuring step parameters
type StepOption func(*StepParams)

// WithStepMaxRetries sets the maximum number of retries for a step
func WithStepMaxRetries(maxRetries int) StepOption {
	return func(p *StepParams) {
		p.MaxRetries = maxRetries
	}
}

// WithBackoffFactor sets the backoff factor for retries (multiplier for exponential backoff)
func WithBackoffFactor(backoffFactor float64) StepOption {
	return func(p *StepParams) {
		p.BackoffFactor = backoffFactor
	}
}

// WithBaseInterval sets the base delay for the first retry
func WithBaseInterval(baseInterval time.Duration) StepOption {
	return func(p *StepParams) {
		p.BaseInterval = baseInterval
	}
}

// WithMaxInterval sets the maximum delay for retries
func WithMaxInterval(maxInterval time.Duration) StepOption {
	return func(p *StepParams) {
		p.MaxInterval = maxInterval
	}
}

func (c *dbosContext) RunAsStep(_ DBOSContext, fn StepFunc, input any, stepName string, opts ...StepOption) (any, error) {
	if fn == nil {
		return nil, newStepExecutionError("", "", "step function cannot be nil")
	}

	// Apply options to build params with defaults
	params := StepParams{
		MaxRetries:    0,
		BackoffFactor: 2.0,
		BaseInterval:  500 * time.Millisecond,
		MaxInterval:   1 * time.Hour,
	}
	for _, opt := range opts {
		opt(&params)
	}

	// Get workflow state from context
	wfState, ok := c.ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", stepName, "workflow state not found in context: are you running this step within a workflow?")
	}

	// If within a step, just run the function directly
	if wfState.isWithinStep {
		return fn(c.ctx, input)
	}

	// Get next step ID
	stepID := wfState.NextStepID()

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := c.systemDB.CheckOperationExecution(c.ctx, checkOperationExecutionDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   stepName,
	})
	if err != nil {
		return nil, newStepExecutionError(wfState.workflowID, stepName, fmt.Sprintf("checking operation execution: %v", err))
	}
	if recordedOutput != nil {
		return recordedOutput.output, recordedOutput.err
	}

	// Execute step with retry logic if MaxRetries > 0
	stepState := workflowState{
		workflowID:   wfState.workflowID,
		stepCounter:  wfState.stepCounter,
		isWithinStep: true,
	}

	// Spawn a child DBOSContext with the step state
	stepCtx := c.withValue(workflowStateKey, &stepState)

	stepOutput, stepError := fn(stepCtx, input)

	// Retry if MaxRetries > 0 and the first execution failed
	var joinedErrors error
	if stepError != nil && params.MaxRetries > 0 {
		joinedErrors = errors.Join(joinedErrors, stepError)

		for retry := 1; retry <= params.MaxRetries; retry++ {
			// Calculate delay for exponential backoff
			delay := params.BaseInterval
			if retry > 1 {
				exponentialDelay := float64(params.BaseInterval) * math.Pow(params.BackoffFactor, float64(retry-1))
				delay = time.Duration(math.Min(exponentialDelay, float64(params.MaxInterval)))
			}

			getLogger().Error("step failed, retrying", "step_name", stepName, "retry", retry, "max_retries", params.MaxRetries, "delay", delay, "error", stepError)

			// Wait before retry
			select {
			case <-c.ctx.Done():
				return nil, newStepExecutionError(wfState.workflowID, stepName, fmt.Sprintf("context cancelled during retry: %v", c.ctx.Err()))
			case <-time.After(delay):
				// Continue to retry
			}

			// Execute the retry
			stepOutput, stepError = fn(stepCtx, input)

			// If successful, break
			if stepError == nil {
				break
			}

			// Join the error with existing errors
			joinedErrors = errors.Join(joinedErrors, stepError)

			// If max retries reached, create MaxStepRetriesExceeded error
			if retry == params.MaxRetries {
				stepError = newMaxStepRetriesExceededError(wfState.workflowID, stepName, params.MaxRetries, joinedErrors)
				break
			}
		}
	}

	// Record the final result
	dbInput := recordOperationResultDBInput{
		workflowID: wfState.workflowID,
		stepName:   stepName,
		stepID:     stepID,
		err:        stepError,
		output:     stepOutput,
	}
	recErr := c.systemDB.RecordOperationResult(c.ctx, dbInput)
	if recErr != nil {
		return nil, newStepExecutionError(wfState.workflowID, stepName, fmt.Sprintf("recording step outcome: %v", recErr))
	}

	return stepOutput, stepError
}

func RunAsStep[P any, R any](dbosCtx DBOSContext, fn GenericStepFunc[P, R], input P, opts ...StepOption) (R, error) {
	if dbosCtx == nil {
		return *new(R), errors.New("dbosCtx cannot be nil")
	}
	if fn == nil {
		return *new(R), newStepExecutionError("", "", "step function cannot be nil")
	}

	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()

	// Type-erase the function
	typeErasedFn := func(ctx context.Context, input any) (any, error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, fmt.Errorf("unexpected input type: expected %T, got %T", *new(P), input)
		}
		return fn(ctx, typedInput)
	}

	// Call the executor method
	result, err := dbosCtx.RunAsStep(dbosCtx, typeErasedFn, input, stepName, opts...)
	if err != nil {
		return *new(R), err
	}

	// Type-check and cast the result
	typedResult, ok := result.(R)
	if !ok {
		return *new(R), fmt.Errorf("unexpected result type: expected %T, got %T", *new(R), result)
	}

	return typedResult, nil
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

type WorkflowSendInput[R any] struct {
	DestinationID string
	Message       R
	Topic         string
}

func (c *dbosContext) Send(_ DBOSContext, input WorkflowSendInputInternal) error {
	return c.systemDB.Send(c.ctx, input)
}

// Send sends a message to another workflow.
// Send automatically registers the type of R for gob encoding
func Send[R any](dbosCtx DBOSContext, input WorkflowSendInput[R]) error {
	if dbosCtx == nil {
		return errors.New("dbosCtx cannot be nil")
	}
	var typedMessage R
	gob.Register(typedMessage)
	return dbosCtx.Send(dbosCtx, WorkflowSendInputInternal{
		DestinationID: input.DestinationID,
		Message:       input.Message,
		Topic:         input.Topic,
	})
}

type WorkflowRecvInput struct {
	Topic   string
	Timeout time.Duration
}

func (c *dbosContext) Recv(_ DBOSContext, input WorkflowRecvInput) (any, error) {
	return c.systemDB.Recv(c.ctx, input)
}

func Recv[R any](dbosCtx DBOSContext, input WorkflowRecvInput) (R, error) {
	if dbosCtx == nil {
		return *new(R), errors.New("dbosCtx cannot be nil")
	}
	msg, err := dbosCtx.Recv(dbosCtx, input)
	if err != nil {
		return *new(R), err
	}
	// Type check
	var typedMessage R
	if msg != nil {
		var ok bool
		typedMessage, ok = msg.(R)
		if !ok {
			return *new(R), newWorkflowUnexpectedResultType("", fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", msg))
		}
	}
	return typedMessage, nil
}

type WorkflowSetEventInputGeneric[R any] struct {
	Key     string
	Message R
}

func (c *dbosContext) SetEvent(_ DBOSContext, input WorkflowSetEventInput) error {
	return c.systemDB.SetEvent(c.ctx, input)
}

// Sets an event from a workflow.
// The event is a key value pair
// SetEvent automatically registers the type of R for gob encoding
func SetEvent[R any](dbosCtx DBOSContext, input WorkflowSetEventInputGeneric[R]) error {
	if dbosCtx == nil {
		return errors.New("dbosCtx cannot be nil")
	}
	var typedMessage R
	gob.Register(typedMessage)
	return dbosCtx.SetEvent(dbosCtx, WorkflowSetEventInput{
		Key:     input.Key,
		Message: input.Message,
	})
}

type WorkflowGetEventInput struct {
	TargetWorkflowID string
	Key              string
	Timeout          time.Duration
}

func (c *dbosContext) GetEvent(_ DBOSContext, input WorkflowGetEventInput) (any, error) {
	return c.systemDB.GetEvent(c.ctx, input)
}

func GetEvent[R any](dbosCtx DBOSContext, input WorkflowGetEventInput) (R, error) {
	if dbosCtx == nil {
		return *new(R), errors.New("dbosCtx cannot be nil")
	}
	value, err := dbosCtx.GetEvent(dbosCtx, input)
	if err != nil {
		return *new(R), err
	}
	if value == nil {
		return *new(R), nil
	}
	// Type check
	typedValue, ok := value.(R)
	if !ok {
		return *new(R), newWorkflowUnexpectedResultType("", fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", value))
	}
	return typedValue, nil
}

func (c *dbosContext) Sleep(duration time.Duration) (time.Duration, error) {
	return c.systemDB.Sleep(c.ctx, duration)
}

/***********************************/
/******* WORKFLOW MANAGEMENT *******/
/***********************************/

// GetWorkflowID retrieves the workflow ID from the context if called within a DBOS workflow
func (c *dbosContext) GetWorkflowID() (string, error) {
	wfState, ok := c.ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.workflowID, nil
}

func (c *dbosContext) RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error) {
	workflowStatus, err := c.systemDB.ListWorkflows(c.ctx, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return &workflowPollingHandle[any]{workflowID: workflowID, dbosContext: c}, nil
}

func RetrieveWorkflow[R any](dbosCtx DBOSContext, workflowID string) (workflowPollingHandle[R], error) {
	if dbosCtx == nil {
		return workflowPollingHandle[R]{}, errors.New("dbosCtx cannot be nil")
	}
	workflowStatus, err := dbosCtx.(*dbosContext).systemDB.ListWorkflows(dbosCtx.(*dbosContext).ctx, listWorkflowsDBInput{
		workflowIDs: []string{workflowID},
	})
	if err != nil {
		return workflowPollingHandle[R]{}, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return workflowPollingHandle[R]{}, newNonExistentWorkflowError(workflowID)
	}
	return workflowPollingHandle[R]{workflowID: workflowID, dbosContext: dbosCtx}, nil
}

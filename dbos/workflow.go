package dbos

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/robfig/cron/v3"
)

/*******************************/
/******* WORKFLOW STATUS *******/
/*******************************/

// WorkflowStatusType represents the current execution state of a workflow.
type WorkflowStatusType string

const (
	WorkflowStatusPending                     WorkflowStatusType = "PENDING"                        // Workflow is running or ready to run
	WorkflowStatusEnqueued                    WorkflowStatusType = "ENQUEUED"                       // Workflow is queued and waiting for execution
	WorkflowStatusSuccess                     WorkflowStatusType = "SUCCESS"                        // Workflow completed successfully
	WorkflowStatusError                       WorkflowStatusType = "ERROR"                          // Workflow completed with an error
	WorkflowStatusCancelled                   WorkflowStatusType = "CANCELLED"                      // Workflow was cancelled (manually or due to timeout)
	WorkflowStatusMaxRecoveryAttemptsExceeded WorkflowStatusType = "MAX_RECOVERY_ATTEMPTS_EXCEEDED" // Workflow exceeded maximum retry attempts
)

// WorkflowStatus contains comprehensive information about a workflow's current state and execution history.
type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`                 // Unique identifier for the workflow
	Status             WorkflowStatusType `json:"status"`                        // Current execution status
	Name               string             `json:"name"`                          // Function name of the workflow
	AuthenticatedUser  string             `json:"authenticated_user,omitempty"`  // User who initiated the workflow (if applicable)
	AssumedRole        string             `json:"assumed_role,omitempty"`        // Role assumed during execution (if applicable)
	AuthenticatedRoles []string           `json:"authenticated_roles,omitempty"` // Roles available to the user (if applicable)
	Output             any                `json:"output,omitempty"`              // Workflow output (available after completion)
	Error              error              `json:"error,omitempty"`               // Error information (if status is ERROR)
	ExecutorID         string             `json:"executor_id"`                   // ID of the executor running this workflow
	CreatedAt          time.Time          `json:"created_at"`                    // When the workflow was created
	UpdatedAt          time.Time          `json:"updated_at"`                    // When the workflow status was last updated
	ApplicationVersion string             `json:"application_version"`           // Version of the application that created this workflow
	ApplicationID      string             `json:"application_id,omitempty"`      // Application identifier
	Attempts           int                `json:"attempts"`                      // Number of execution attempts
	QueueName          string             `json:"queue_name,omitempty"`          // Queue name (if workflow was enqueued)
	Timeout            time.Duration      `json:"timeout,omitempty"`             // Workflow timeout duration
	Deadline           time.Time          `json:"deadline"`                      // Absolute deadline for workflow completion
	StartedAt          time.Time          `json:"started_at"`                    // When the workflow execution actually started
	DeduplicationID    string             `json:"deduplication_id,omitempty"`    // Queue deduplication identifier
	Input              any                `json:"input,omitempty"`               // Input parameters passed to the workflow
	Priority           int                `json:"priority,omitempty"`            // Queue execution priority (lower numbers have higher priority)
	QueuePartitionKey  string             `json:"queue_partition_key,omitempty"` // Queue partition key for partitioned queues
	ForkedFrom         string             `json:"forked_from,omitempty"`         // ID of the original workflow if this is a fork
	ParentWorkflowID   string             `json:"parent_workflow_id,omitempty"`  // ID of the parent workflow if this is a child
}

// workflowState holds the runtime state for a workflow execution
type workflowState struct {
	workflowID   string
	stepID       int
	isWithinStep bool
}

// nextStepID returns the next step ID and increments the counter
func (ws *workflowState) nextStepID() int {
	ws.stepID++
	return ws.stepID
}

/********************************/
/******* WORKFLOW HANDLES ********/
/********************************/

// workflowOutcome holds the result and error from workflow execution
type workflowOutcome[R any] struct {
	result        R
	err           error
	needsDecoding bool // true if result came from awaitWorkflowResult (ID conflict path) and needs decoding
}

type stepCheckpointedOutcome struct {
	value any // The encoded value (should be a *string)
}

// WorkflowHandle provides methods to interact with a running or completed workflow.
// The type parameter R represents the expected return type of the workflow.
// Handles can be used to wait for workflow completion, check status, and retrieve results.
type WorkflowHandle[R any] interface {
	GetResult(opts ...GetResultOption) (R, error) // Wait for workflow completion and return the result
	GetStatus() (WorkflowStatus, error)           // Get current workflow status without waiting
	GetWorkflowID() string                        // Get the unique workflow identifier
}

type baseWorkflowHandle struct {
	workflowID  string
	dbosContext DBOSContext
}

// GetResultOption is a functional option for configuring GetResult behavior.
type GetResultOption func(*getResultOptions)

// getResultOptions holds the configuration for GetResult execution.
type getResultOptions struct {
	timeout      time.Duration
	pollInterval time.Duration
}

func defaultGetResultOptions() *getResultOptions {
	return &getResultOptions{pollInterval: _DB_RETRY_INTERVAL}
}

// WithHandleTimeout sets a timeout for the GetResult operation.
// If the timeout is reached before the workflow completes, GetResult will return a timeout error.
func WithHandleTimeout(timeout time.Duration) GetResultOption {
	return func(opts *getResultOptions) {
		opts.timeout = timeout
	}
}

// WithHandlePollingInterval sets the polling interval for awaiting workflow completion in GetResult.
// If a non-positive interval is provided, the default interval is used.
func WithHandlePollingInterval(interval time.Duration) GetResultOption {
	return func(opts *getResultOptions) {
		if interval > 0 {
			opts.pollInterval = interval
		}
	}
}

// GetStatus returns the current status of the workflow from the database
// If the DBOSContext is running in client mode, do not load input and outputs
func (h *baseWorkflowHandle) GetStatus() (WorkflowStatus, error) {
	loadInput := false
	loadOutput := false
	if h.dbosContext.(*dbosContext).launched.Load() {
		loadInput = false
		loadOutput = false
	}
	c := h.dbosContext.(*dbosContext)
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var workflowStatuses []WorkflowStatus
	var err error
	if isWithinWorkflow {
		workflowStatuses, err = RunAsStep(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return retryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.systemDB.listWorkflows(ctx, listWorkflowsDBInput{
					workflowIDs: []string{h.workflowID},
					loadInput:   loadInput,
					loadOutput:  loadOutput,
				})
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.getStatus"))
	} else {
		workflowStatuses, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.systemDB.listWorkflows(c, listWorkflowsDBInput{
				workflowIDs: []string{h.workflowID},
				loadInput:   loadInput,
				loadOutput:  loadOutput,
			})
		})
	}
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *baseWorkflowHandle) GetWorkflowID() string {
	return h.workflowID
}

func newWorkflowHandle[R any](ctx DBOSContext, workflowID string, outcomeChan chan workflowOutcome[R]) *workflowHandle[R] {
	return &workflowHandle[R]{
		baseWorkflowHandle: baseWorkflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
		outcomeChan: outcomeChan,
	}
}

func newWorkflowPollingHandle[R any](ctx DBOSContext, workflowID string) *workflowPollingHandle[R] {
	return &workflowPollingHandle[R]{
		baseWorkflowHandle: baseWorkflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
	}
}

type workflowHandle[R any] struct {
	baseWorkflowHandle
	outcomeChan chan workflowOutcome[R]
}

func (h *workflowHandle[R]) GetResult(opts ...GetResultOption) (R, error) {
	options := defaultGetResultOptions()
	for _, opt := range opts {
		opt(options)
	}

	startTime := time.Now()

	var timeoutChan <-chan time.Time
	if options.timeout > 0 {
		timeoutChan = time.After(options.timeout)
	}

	select {
	case outcome, ok := <-h.outcomeChan:
		if !ok {
			// Return error if channel closed (happens when GetResult() called twice)
			return *new(R), errors.New("workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?")
		}
		completedTime := time.Now()
		return h.processOutcome(outcome, startTime, completedTime)
	case <-h.dbosContext.Done():
		return *new(R), context.Cause(h.dbosContext)
	case <-timeoutChan:
		return *new(R), fmt.Errorf("workflow result timeout after %v: %w", options.timeout, context.DeadlineExceeded)
	}
}

// processOutcome handles the common logic for processing workflow outcomes
func (h *workflowHandle[R]) processOutcome(outcome workflowOutcome[R], startTime, completedTime time.Time) (R, error) {
	decodedResult := outcome.result
	// If we are calling GetResult inside a workflow, record the result as a step result
	workflowState, ok := h.dbosContext.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		if _, ok := h.dbosContext.(*dbosContext); !ok {
			return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("invalid DBOSContext: expected *dbosContext"))
		}
		serializer := newJSONSerializer[R]()
		encodedOutput, encErr := serializer.Encode(decodedResult)
		if encErr != nil {
			return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("serializing child workflow result: %w", encErr))
		}
		recordGetResultInput := recordChildGetResultDBInput{
			parentWorkflowID: workflowState.workflowID,
			childWorkflowID:  h.workflowID,
			stepID:           workflowState.nextStepID(),
			output:           encodedOutput,
			err:              outcome.err,
			startedAt:        startTime,
			completedAt:      completedTime,
		}
		recordResultErr := retry(h.dbosContext, func() error {
			return h.dbosContext.(*dbosContext).systemDB.recordChildGetResult(h.dbosContext, recordGetResultInput)
		}, withRetrierLogger(h.dbosContext.(*dbosContext).logger))
		if recordResultErr != nil {
			h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
			return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("recording child workflow result: %w", recordResultErr))
		}
	}
	return decodedResult, outcome.err
}

type workflowPollingHandle[R any] struct {
	baseWorkflowHandle
}

func (h *workflowPollingHandle[R]) GetResult(opts ...GetResultOption) (R, error) {
	options := defaultGetResultOptions()
	for _, opt := range opts {
		opt(options)
	}

	startTime := time.Now()

	// Use timeout if specified, otherwise use DBOS context directly
	ctx := h.dbosContext
	var cancel context.CancelFunc
	if options.timeout > 0 {
		ctx, cancel = WithTimeout(h.dbosContext, options.timeout)
		defer cancel()
	}

	encodedResult, err := retryWithResult(ctx, func() (any, error) {
		return h.dbosContext.(*dbosContext).systemDB.awaitWorkflowResult(ctx, h.workflowID, options.pollInterval)
	}, withRetrierLogger(h.dbosContext.(*dbosContext).logger))

	completedTime := time.Now()

	// Deserialize the result directly into the target type
	var typedResult R
	if encodedResult != nil {
		encodedStr, ok := encodedResult.(*string)
		if !ok { // Should never happen
			return *new(R), newWorkflowUnexpectedResultType(h.workflowID, "string (encoded)", fmt.Sprintf("%T", encodedResult))
		}
		serializer := newJSONSerializer[R]()
		var deserErr error
		typedResult, deserErr = serializer.Decode(encodedStr)
		if deserErr != nil {
			return *new(R), fmt.Errorf("failed to deserialize workflow result: %w", deserErr)
		}

		// If we are calling GetResult inside a workflow, record the result as a step result
		workflowState, ok := h.dbosContext.Value(workflowStateKey).(*workflowState)
		isWithinWorkflow := ok && workflowState != nil
		if isWithinWorkflow {
			recordGetResultInput := recordChildGetResultDBInput{
				parentWorkflowID: workflowState.workflowID,
				childWorkflowID:  h.workflowID,
				stepID:           workflowState.nextStepID(),
				output:           encodedStr,
				err:              err,
				startedAt:        startTime,
				completedAt:      completedTime,
			}
			recordResultErr := retry(h.dbosContext, func() error {
				return h.dbosContext.(*dbosContext).systemDB.recordChildGetResult(h.dbosContext, recordGetResultInput)
			}, withRetrierLogger(h.dbosContext.(*dbosContext).logger))
			if recordResultErr != nil {
				h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
				return *new(R), newWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("recording child workflow result: %w", recordResultErr))
			}
		}
		return typedResult, err
	}
	return *new(R), err
}

// Wrapper handle -- useful for handling mocks in RunWorkflow
type workflowHandleProxy[R any] struct {
	wrappedHandle WorkflowHandle[any]
}

func (h *workflowHandleProxy[R]) GetResult(opts ...GetResultOption) (R, error) {
	result, err := h.wrappedHandle.GetResult(opts...)
	if err != nil {
		var zero R
		return zero, err
	}

	// Convert from any to R
	if typed, ok := result.(R); ok {
		return typed, nil
	}

	var zero R
	return zero, fmt.Errorf("cannot convert result of type %T to %T", result, zero)
}

func (h *workflowHandleProxy[R]) GetStatus() (WorkflowStatus, error) {
	return h.wrappedHandle.GetStatus()
}

func (h *workflowHandleProxy[R]) GetWorkflowID() string {
	return h.wrappedHandle.GetWorkflowID()
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type wrappedWorkflowFunc func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error)

type WorkflowRegistryEntry struct {
	wrappedFunction wrappedWorkflowFunc
	MaxRetries      int
	Name            string
	FQN             string // Fully qualified name of the workflow function
	CronSchedule    string // Empty string for non-scheduled workflows
}

func registerWorkflow(ctx DBOSContext, workflowFQN string, fn wrappedWorkflowFunc, maxRetries int, customName string) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register workflow after DBOS has launched")
	}

	// Check if workflow already exists and store atomically using LoadOrStore
	entry := WorkflowRegistryEntry{
		wrappedFunction: fn,
		FQN:             workflowFQN,
		MaxRetries:      maxRetries,
		Name:            customName,
		CronSchedule:    "",
	}

	if _, exists := c.workflowRegistry.LoadOrStore(workflowFQN, entry); exists {
		c.logger.Error("workflow function already registered", "fqn", workflowFQN)
		panic(newConflictingRegistrationError(workflowFQN))
	}

	// We need to get a mapping from custom name to FQN for registry lookups that might not know the FQN (queue, recovery)
	// We also panic if we found the name was already registered (this could happen if registering two different workflows under the same custom name)
	if len(customName) > 0 {
		if _, exists := c.workflowCustomNametoFQN.LoadOrStore(customName, workflowFQN); exists {
			c.logger.Error("workflow function already registered", "custom_name", customName)
			panic(newConflictingRegistrationError(customName))
		}
	} else {
		c.workflowCustomNametoFQN.Store(workflowFQN, workflowFQN) // Store the FQN as the custom name if none was provided
	}
}

func registerScheduledWorkflow(ctx DBOSContext, workflowName string, fn WorkflowFunc, cronSchedule string) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register scheduled workflow after DBOS has launched")
	}

	// Update the existing workflow entry with the cron schedule
	registryEntryAny, exists := c.workflowRegistry.Load(workflowName)
	if !exists {
		panic(fmt.Sprintf("workflow %s must be registered before scheduling", workflowName))
	}
	registryEntry := registryEntryAny.(WorkflowRegistryEntry)
	registryEntry.CronSchedule = cronSchedule
	c.workflowRegistry.Store(workflowName, registryEntry)

	var entryID cron.EntryID
	entryID, err := c.getWorkflowScheduler().AddFunc(cronSchedule, func() {
		// Execute the workflow on the cron schedule once DBOS is launched
		if !c.launched.Load() {
			return
		}
		// Get the scheduled time from the cron entry
		entry := c.getWorkflowScheduler().Entry(entryID)
		scheduledTime := entry.Prev
		if scheduledTime.IsZero() {
			// Use Next if Prev is not set, which will only happen for the first run
			scheduledTime = entry.Next
		}
		wfID := fmt.Sprintf("sched-%s-%s", workflowName, scheduledTime)
		opts := []WorkflowOption{
			WithWorkflowID(wfID),
			WithQueue(_DBOS_INTERNAL_QUEUE_NAME),
			withWorkflowName(workflowName),
		}
		_, err := ctx.RunWorkflow(ctx, fn, scheduledTime, opts...)
		if err != nil {
			c.logger.Error("failed to run scheduled workflow", "fqn", workflowName, "error", err)
		}
	})
	if err != nil {
		panic(fmt.Sprintf("failed to register scheduled workflow: %v", err))
	}
	c.logger.Info("Registered scheduled workflow", "fqn", workflowName, "cron_schedule", cronSchedule)
}

type workflowRegistrationOptions struct {
	cronSchedule string
	maxRetries   int
	name         string
}

type WorkflowRegistrationOption func(*workflowRegistrationOptions)

const (
	_DEFAULT_MAX_RECOVERY_ATTEMPTS = 100

	// Step retry defaults
	_DEFAULT_STEP_BASE_INTERVAL  = 100 * time.Millisecond
	_DEFAULT_STEP_MAX_INTERVAL   = 5 * time.Second
	_DEFAULT_STEP_BACKOFF_FACTOR = 2.0
)

// WithMaxRetries sets the maximum number of retry attempts for workflow recovery.
// If a workflow fails or is interrupted, it will be retried up to this many times.
// After exceeding max retries, the workflow status becomes MAX_RECOVERY_ATTEMPTS_EXCEEDED.
func WithMaxRetries(maxRetries int) WorkflowRegistrationOption {
	return func(p *workflowRegistrationOptions) {
		p.maxRetries = maxRetries
	}
}

// WithSchedule registers the workflow as a scheduled workflow using cron syntax.
// The schedule string follows standard cron format with second precision.
// Scheduled workflows automatically receive a time.Time input parameter.
func WithSchedule(schedule string) WorkflowRegistrationOption {
	return func(p *workflowRegistrationOptions) {
		p.cronSchedule = schedule
	}
}

func WithWorkflowName(name string) WorkflowRegistrationOption {
	return func(p *workflowRegistrationOptions) {
		p.name = name
	}
}

// resolveWorkflowFunctionName resolves the function name for a workflow function,
// handling generic workflows by appending the actual type parameters.
func resolveWorkflowFunctionName[P any, R any](fn Workflow[P, R]) string {
	ptr := reflect.ValueOf(fn).Pointer()
	fqn := runtime.FuncForPC(ptr).Name()

	// If this is a generic workflow, append the actual types to the FQN
	if strings.Contains(fqn, "[") {
		fqn = strings.Split(fqn, "[")[0]
		fqn = fmt.Sprintf("%s[%s,%s]",
			fqn,
			reflect.TypeFor[P]().String(),
			reflect.TypeFor[R]().String(),
		)
	}

	return fqn
}

// RegisterWorkflow registers a function as a durable workflow that can be executed and recovered.
// The function is registered with type safety - P represents the input type and R the return type.
//
// Registration options include:
//   - WithMaxRetries: Set maximum retry attempts for workflow recovery
//   - WithSchedule: Register as a scheduled workflow with cron syntax
//   - WithWorkflowName:: Set a custom name for the workflow
//
// Scheduled workflows receive a time.Time as input representing the scheduled execution time.
//
// Example:
//
//	func MyWorkflow(ctx dbos.DBOSContext, input string) (int, error) {
//	    // workflow implementation
//	    return len(input), nil
//	}
//
//	dbos.RegisterWorkflow(ctx, MyWorkflow)
//
//	// With options:
//	dbos.RegisterWorkflow(ctx, MyWorkflow,
//	    dbos.WithMaxRetries(5),
//	    dbos.WithSchedule("0 0 * * * *")) // daily at midnight
//		dbos.WithWorkflowName("MyCustomWorkflowName") // Custom name for the workflow
func RegisterWorkflow[P any, R any](ctx DBOSContext, fn Workflow[P, R], opts ...WorkflowRegistrationOption) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}

	var p P

	registrationParams := workflowRegistrationOptions{
		maxRetries: _DEFAULT_MAX_RECOVERY_ATTEMPTS,
	}

	for _, opt := range opts {
		opt(&registrationParams)
	}

	fqn := resolveWorkflowFunctionName(fn)

	// Register a type-erased version of the durable workflow for recovery and queue runner
	// Input will always come from the database and encoded as *string, so we decode it into the target type (captured by this wrapped closure)
	typedErasedWorkflow := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return *new(R), newWorkflowExecutionError("", fmt.Errorf("getting workflow ID: %w", err))
		}
		encodedInput, ok := input.(*string)
		if !ok {
			return *new(R), newWorkflowUnexpectedInputType(fqn, "*string (encoded)", fmt.Sprintf("%T", input))
		}
		// Decode directly into the target type
		serializer := newJSONSerializer[P]()
		typedInput, err := serializer.Decode(encodedInput)
		if err != nil {
			return *new(R), newWorkflowExecutionError(workflowID, err)
		}
		return fn(ctx, typedInput)
	})

	typeErasedWrapper := wrappedWorkflowFunc(func(ctx DBOSContext, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		opts = append(opts, withWorkflowName(fqn)) // Append the name so ctx.RunWorkflow can look it up from the registry to apply registration-time options
		handle, err := ctx.RunWorkflow(ctx, typedErasedWorkflow, input, opts...)
		if err != nil {
			return nil, err
		}
		return newWorkflowPollingHandle[any](ctx, handle.GetWorkflowID()), nil // this is only used by recovery -- the queue runner dismisses it
	})
	registerWorkflow(ctx, fqn, typeErasedWrapper, registrationParams.maxRetries, registrationParams.name)

	// If this is a scheduled workflow, register a cron job
	if registrationParams.cronSchedule != "" {
		if reflect.TypeOf(p) != reflect.TypeFor[time.Time]() {
			panic(fmt.Sprintf("scheduled workflow function must accept a time.Time as input, got %T", p))
		}
		registerScheduledWorkflow(ctx, fqn, typedErasedWorkflow, registrationParams.cronSchedule)
	}
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type dbosContextKey string

const workflowStateKey dbosContextKey = "workflowState"

// Workflow represents a type-safe workflow function with specific input and output types.
// P is the input parameter type and R is the return type.
// All workflow functions must accept a DBOSContext as their first parameter.
type Workflow[P any, R any] func(ctx DBOSContext, input P) (R, error)

// WorkflowFunc represents a type-erased workflow function used internally.
type WorkflowFunc func(ctx DBOSContext, input any) (any, error)

type workflowOptions struct {
	WorkflowName        string
	WorkflowID          string
	QueueName           string
	ApplicationVersion  string
	MaxRetries          int
	DeduplicationID     string
	Priority            uint
	AuthenticatedUser   string
	AssumedRole         string
	AuthenticatedRoles  []string
	QueuePartitionKey   string
	alreadyEncodedInput bool
	isDequeue           bool
	isRecovery          bool
}

// WorkflowOption is a functional option for configuring workflow execution parameters.
type WorkflowOption func(*workflowOptions)

// WithWorkflowID sets a custom workflow ID instead of generating one automatically.
func WithWorkflowID(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowID = id
	}
}

// WithQueue enqueues the workflow to the specified queue instead of executing immediately.
// Queued workflows will be processed by the queue runner according to the queue's configuration.
func WithQueue(queueName string) WorkflowOption {
	return func(p *workflowOptions) {
		p.QueueName = queueName
	}
}

// WithApplicationVersion overrides the DBOS Context application version for this workflow.
// This affects workflow recovery.
func WithApplicationVersion(version string) WorkflowOption {
	return func(p *workflowOptions) {
		p.ApplicationVersion = version
	}
}

// WithDeduplicationID sets a deduplication ID for a queue workflow.
func WithDeduplicationID(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.DeduplicationID = id
	}
}

// WithPriority sets the execution priority for a queue workflow.
func WithPriority(priority uint) WorkflowOption {
	return func(p *workflowOptions) {
		p.Priority = priority
	}
}

// WithQueuePartitionKey sets the queue partition key for partitioned queues.
// When a queue is partitioned, workflows with the same partition key are processed
// with separate concurrency limits per partition.
func WithQueuePartitionKey(partitionKey string) WorkflowOption {
	return func(p *workflowOptions) {
		p.QueuePartitionKey = partitionKey
	}
}

// An internal option we use to map the reflection function name to the registration options.
func withWorkflowName(name string) WorkflowOption {
	return func(p *workflowOptions) {
		if p.WorkflowName == "" {
			p.WorkflowName = name
		}
	}
}

// An internal option we use to indicate that the input is already encoded, so we don't need to encode it again
func withAlreadyEncodedInput() WorkflowOption {
	return func(p *workflowOptions) {
		p.alreadyEncodedInput = true
	}
}

// Private option set when RunWorkflow is invoked from the queue runner (dbos/queue.go).
func withIsDequeue() WorkflowOption {
	return func(p *workflowOptions) {
		p.isDequeue = true
	}
}

// Private option set when RunWorkflow is invoked from the recovery path (dbos/recovery.go).
func withIsRecovery() WorkflowOption {
	return func(p *workflowOptions) {
		p.isRecovery = true
	}
}

// Sets the authenticated user for the workflow
func WithAuthenticatedUser(user string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedUser = user
	}
}

// Sets the assumed role for the workflow
func WithAssumedRole(role string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AssumedRole = role
	}
}

// Sets the authenticated role for the workflow
func WithAuthenticatedRoles(roles []string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedRoles = roles
	}
}

// RunWorkflow executes a workflow function with type safety and durability guarantees.
// The workflow can be executed immediately or enqueued for later execution based on options.
// Returns a typed handle that can be used to wait for completion and retrieve results.
//
// The workflow will be automatically recovered if the process crashes or is interrupted.
// All workflow state is persisted to ensure exactly-once execution semantics.
//
// Example:
//
//	handle, err := dbos.RunWorkflow(ctx, MyWorkflow, "input string", dbos.WithWorkflowID("my-custom-id"))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := handle.GetResult()
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %v", result)
//	}
func RunWorkflow[P any, R any](ctx DBOSContext, fn Workflow[P, R], input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx cannot be nil")
	}

	// Add the fn name to the options so we can communicate it with DBOSContext.RunWorkflow
	opts = append(opts, withWorkflowName(resolveWorkflowFunctionName(fn)))

	typedErasedWorkflow := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
		return fn(ctx, input.(P))
	})

	handle, err := ctx.RunWorkflow(ctx, typedErasedWorkflow, input, opts...)
	if err != nil {
		return nil, err
	}

	// If we got a polling handle, return its typed version
	if pollingHandle, ok := handle.(*workflowPollingHandle[any]); ok {
		// We need to convert the polling handle to a typed handle
		typedPollingHandle := newWorkflowPollingHandle[R](pollingHandle.dbosContext, pollingHandle.workflowID)
		return typedPollingHandle, nil
	}

	// Create a typed channel for the user to get a typed handle
	if handle, ok := handle.(*workflowHandle[any]); ok {
		typedOutcomeChan := make(chan workflowOutcome[R], 1)

		go func() {
			defer close(typedOutcomeChan)
			outcome := <-handle.outcomeChan

			resultErr := outcome.err
			var typedResult R

			// Handle nil results - nil cannot be type-asserted to any interface
			if outcome.result == nil {
				typedOutcomeChan <- workflowOutcome[R]{
					result: typedResult,
					err:    resultErr,
				}
				return
			}

			// Check if this is a mocked path
			if _, ok := handle.dbosContext.(*dbosContext); !ok {
				typedOutcomeChan <- workflowOutcome[R]{
					result: outcome.result.(R),
					err:    resultErr,
				}
				return
			}

			// Convert result to expected type R
			// Result can be either an encoded *string (from ID conflict path) or already decoded
			if outcome.needsDecoding {
				encodedResult, ok := outcome.result.(*string)
				if !ok { // Should never happen
					resultErr = errors.Join(resultErr, newWorkflowUnexpectedResultType(handle.workflowID, "string (encoded)", fmt.Sprintf("%T", outcome.result)))
				} else {
					// Result is encoded, decode directly into target type
					serializer := newJSONSerializer[R]()
					var decodeErr error
					typedResult, decodeErr = serializer.Decode(encodedResult)
					if decodeErr != nil {
						resultErr = errors.Join(resultErr, newWorkflowExecutionError(handle.workflowID, fmt.Errorf("decoding workflow result to type %T: %w", *new(R), decodeErr)))
					}
				}
			} else if typedRes, ok := outcome.result.(R); ok {
				// Normal path - result already has the correct type
				typedResult = typedRes
			} else {
				// Type assertion failed
				typeErr := newWorkflowUnexpectedResultType(handle.workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", outcome.result))
				resultErr = errors.Join(resultErr, typeErr)
			}

			typedOutcomeChan <- workflowOutcome[R]{
				result: typedResult,
				err:    resultErr,
			}
		}()

		typedHandle := newWorkflowHandle(handle.dbosContext, handle.workflowID, typedOutcomeChan)

		return typedHandle, nil
	}

	// Usually on a mocked path
	return &workflowHandleProxy[R]{wrappedHandle: handle}, nil
}

func (c *dbosContext) RunWorkflow(_ DBOSContext, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
	// Apply options to build params
	params := workflowOptions{
		ApplicationVersion: c.GetApplicationVersion(),
	}
	for _, opt := range opts {
		opt(&params)
	}

	// Lookup the registry for registration-time options
	registeredWorkflowAny, exists := c.workflowRegistry.Load(params.WorkflowName)
	if !exists {
		c.logger.Error("workflow not found in registry", "workflow_name", params.WorkflowName)
		return nil, newNonExistentWorkflowError(params.WorkflowName)
	}
	registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
	if !ok {
		c.logger.Error("invalid workflow registry entry type for workflow", "workflow_name", params.WorkflowName)
		return nil, fmt.Errorf("invalid workflow registry entry type for workflow %s", params.WorkflowName)
	}
	if registeredWorkflow.MaxRetries > 0 {
		params.MaxRetries = registeredWorkflow.MaxRetries
	}
	if len(registeredWorkflow.Name) > 0 {
		params.WorkflowName = registeredWorkflow.Name
	}

	// Validate partition key is not provided without queue name
	if len(params.QueuePartitionKey) > 0 && len(params.QueueName) == 0 {
		c.logger.Error("partition key provided but queue name is missing", "workflow_name", params.WorkflowName)
		return nil, newWorkflowExecutionError("", fmt.Errorf("partition key provided but queue name is missing"))
	}

	// Validate partition key and deduplication ID are not both provided (they are incompatible)
	if len(params.QueuePartitionKey) > 0 && len(params.DeduplicationID) > 0 {
		c.logger.Error("partition key and deduplication ID cannot be used together", "workflow_name", params.WorkflowName)
		return nil, newWorkflowExecutionError("", fmt.Errorf("partition key and deduplication ID cannot be used together"))
	}

	// Validate queue exists if provided
	if len(params.QueueName) > 0 {
		queue := c.queueRunner.getQueue(params.QueueName)
		if queue == nil {
			c.logger.Error("queue does not exist", "workflow_name", params.WorkflowName, "queue_name", params.QueueName)
			return nil, newWorkflowExecutionError("", fmt.Errorf("queue %s does not exist", params.QueueName))
		}
		// If queue has partitions enabled, partition key must be provided
		if queue.PartitionQueue && len(params.QueuePartitionKey) == 0 {
			c.logger.Error("queue has partitions enabled but no partition key was provided", "workflow_name", params.WorkflowName, "queue_name", params.QueueName)
			return nil, newWorkflowExecutionError("", fmt.Errorf("queue %s has partitions enabled, but no partition key was provided", params.QueueName))
		}
		// If partition key is provided, queue must have partitions enabled
		if len(params.QueuePartitionKey) > 0 && !queue.PartitionQueue {
			c.logger.Error("queue is not a partitioned queue but a partition key was provided", "workflow_name", params.WorkflowName, "queue_name", params.QueueName)
			return nil, newWorkflowExecutionError("", fmt.Errorf("queue %s is not a partitioned queue, but a partition key was provided", params.QueueName))
		}
	}

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

	// Prevent spawning child workflows from within a step
	if isChildWorkflow && parentWorkflowState.isWithinStep {
		c.logger.Error("cannot spawn child workflow from within a step", "workflow_name", params.WorkflowName, "parent_workflow_id", parentWorkflowState.workflowID)
		return nil, newStepExecutionError(parentWorkflowState.workflowID, params.WorkflowName, fmt.Errorf("cannot spawn child workflow from within a step"))
	}

	if isChildWorkflow {
		// Advance step ID if we are a child workflow
		parentWorkflowState.nextStepID()
	}

	// Generate an ID for the workflow if not provided
	var workflowID string
	if params.WorkflowID == "" {
		if isChildWorkflow {
			stepID := parentWorkflowState.stepID
			workflowID = fmt.Sprintf("%s-%d", parentWorkflowState.workflowID, stepID)
		} else {
			workflowID = uuid.New().String()
		}
	} else {
		workflowID = params.WorkflowID
	}

	// Create an uncancellable context for the DBOS operations
	// This detaches it from any deadline or cancellation signal set by the user
	uncancellableCtx := WithoutCancel(c)

	// If this is a child workflow that has already been recorded in operations_output, return directly a polling handle
	if isChildWorkflow {
		childWorkflowID, err := retryWithResult(uncancellableCtx, func() (*string, error) {
			return c.systemDB.checkChildWorkflow(uncancellableCtx, parentWorkflowState.workflowID, parentWorkflowState.stepID)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("failed to check child workflow", "error", err, "parent_workflow_id", parentWorkflowState.workflowID, "step_id", parentWorkflowState.stepID)
			return nil, newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Errorf("checking child workflow: %w", err))
		}
		if childWorkflowID != nil {
			c.logger.Info("child workflow already recorded", "workflow_name", params.WorkflowName, "parent_workflow_id", parentWorkflowState.workflowID, "step_id", parentWorkflowState.stepID, "child_workflow_id", *childWorkflowID)
			return newWorkflowPollingHandle[any](uncancellableCtx, *childWorkflowID), nil
		}
	}

	var status WorkflowStatusType
	if params.QueueName != "" {
		status = WorkflowStatusEnqueued
	} else {
		status = WorkflowStatusPending
	}

	// Compute the timeout based on the context deadline, if any
	deadline, ok := c.Deadline()
	if !ok {
		deadline = time.Time{} // No deadline set
	}
	var timeout time.Duration
	if !deadline.IsZero() {
		timeout = time.Until(deadline)
		// The timeout could be in the past, for small deadlines, to propagation delays. If so set it to a minimal value
		if timeout < 0 {
			timeout = 1 * time.Millisecond
		}
	}
	// When enqueuing, we do not set a deadline. It'll be computed with the timeout during dequeue.
	if status == WorkflowStatusEnqueued {
		deadline = time.Time{}
	}

	if params.Priority > uint(math.MaxInt) {
		c.logger.Error("priority exceeds maximum allowed value", "workflow_name", params.WorkflowName, "priority", params.Priority, "max_allowed_value", math.MaxInt)
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.Priority, math.MaxInt)
	}

	// Serialize input before storing in workflow status
	var encodedInput any
	if params.alreadyEncodedInput {
		encodedInput = input
	} else {
		serializer := newJSONSerializer[any]()
		var serErr error
		encodedInput, serErr = serializer.Encode(input)
		if serErr != nil {
			c.logger.Error("failed to serialize workflow input", "error", serErr, "workflow_id", workflowID)
			return nil, newWorkflowExecutionError(workflowID, fmt.Errorf("failed to serialize workflow input: %w", serErr))
		}
	}

	workflowStatus := WorkflowStatus{
		Name:               params.WorkflowName,
		ApplicationVersion: params.ApplicationVersion,
		ExecutorID:         c.GetExecutorID(),
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            timeout,
		Input:              encodedInput,
		ApplicationID:      c.GetApplicationID(),
		QueueName:          params.QueueName,
		DeduplicationID:    params.DeduplicationID,
		Priority:           int(params.Priority),
		AuthenticatedUser:  params.AuthenticatedUser,
		AssumedRole:        params.AssumedRole,
		AuthenticatedRoles: params.AuthenticatedRoles,
		QueuePartitionKey:  params.QueuePartitionKey,
	}
	if isChildWorkflow {
		workflowStatus.ParentWorkflowID = parentWorkflowState.workflowID
	}

	var earlyReturnPollingHandle *workflowPollingHandle[any]
	var insertStatusResult *insertWorkflowResult

	// Init status and record child workflow relationship in a single transaction
	err := retry(c, func() error {
		tx, err := c.systemDB.(*sysDB).pool.Begin(uncancellableCtx)
		if err != nil {
			return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx) // Rollback if not committed

		// Insert workflow status with transaction
		ownerXID := uuid.New().String()
		insertInput := insertWorkflowStatusDBInput{
			status:            workflowStatus,
			maxRetries:        params.MaxRetries,
			tx:                tx,
			ownerXID:          &ownerXID,
			incrementAttempts: params.isDequeue || params.isRecovery,
		}
		insertStatusResult, err = c.systemDB.insertWorkflowStatus(uncancellableCtx, insertInput)
		if err != nil {
			c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
			return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to insert workflow status: %w", err))
		}

		// Record child workflow relationship if this is a child workflow
		// We already have checked this earlier so this path should only be taken if the child is executing the first time
		if isChildWorkflow {
			// Get the step ID that was used for generating the child workflow ID
			childInput := recordChildWorkflowDBInput{
				parentWorkflowID: parentWorkflowState.workflowID,
				childWorkflowID:  workflowID,
				stepName:         params.WorkflowName,
				stepID:           parentWorkflowState.stepID,
				tx:               tx,
			}
			err = retry(uncancellableCtx, func() error {
				return c.systemDB.recordChildWorkflow(uncancellableCtx, childInput)
			}, withRetrierLogger(c.logger))
			if err != nil {
				c.logger.Error("failed to record child workflow", "error", err, "parent_workflow_id", parentWorkflowState.workflowID, "child_workflow_id", workflowID)
				return newWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Errorf("recording child workflow: %w", err))
			}
		}

		var loaded bool
		if c.activeWorkflowIDs != nil {
			_, loaded = c.activeWorkflowIDs.Load(workflowID)
		}

		shouldSkip :=
			len(params.QueueName) > 0 || // We are enqueueing OR
				insertStatusResult.status == WorkflowStatusSuccess || // workflow is in a terminal state (success) OR
				insertStatusResult.status == WorkflowStatusError || // workflow is in a terminal state (error) OR
				(!params.isDequeue && !params.isRecovery && insertStatusResult.ownerXID != ownerXID) || // another executor, not us dequeueing or being instructed to recover, is already owning the workflow OR
				loaded // this executor is already running the workflow

		if shouldSkip {
			// Commit the transaction to update the number of attempts and/or enact the enqueue
			if err := tx.Commit(uncancellableCtx); err != nil {
				return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to commit transaction: %w", err))
			}
			earlyReturnPollingHandle = newWorkflowPollingHandle[any](uncancellableCtx, workflowStatus.ID)
			return nil
		}

		// Commit the transaction. This must happen before we start the goroutine to ensure the workflow is found by steps in the database
		if err := tx.Commit(uncancellableCtx); err != nil {
			return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to commit transaction: %w", err))
		}

		return nil
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}
	if earlyReturnPollingHandle != nil {
		return earlyReturnPollingHandle, nil
	}

	// Create workflow state to track step execution
	wfState := &workflowState{
		workflowID: workflowID,
		stepID:     -1, // Steps are O-indexed
	}
	workflowCtx := WithValue(c, workflowStateKey, wfState)

	// If the workflow has a timeout but no deadline, compute the deadline from the timeout.
	// Else use the durable deadline.
	durableDeadline := time.Time{}
	if insertStatusResult.timeout > 0 && insertStatusResult.workflowDeadline.IsZero() {
		durableDeadline = time.Now().Add(insertStatusResult.timeout)
	} else if !insertStatusResult.workflowDeadline.IsZero() {
		durableDeadline = insertStatusResult.workflowDeadline
	}

	var stopFunc func() bool
	cancelFuncCompleted := make(chan struct{})
	if !durableDeadline.IsZero() {
		workflowCtx, _ = WithTimeout(workflowCtx, time.Until(durableDeadline))
		// Register a cancel function that cancels the workflow in the DB as soon as the context is cancelled
		workflowCancelFunction := func() {
			c.logger.Info("Cancelling workflow", "workflow_id", workflowID)
			err = retry(c, func() error {
				return c.systemDB.cancelWorkflow(uncancellableCtx, cancelWorkflowDBInput{workflowID: workflowID})
			}, withRetrierLogger(c.logger))
			if err != nil {
				c.logger.Error("Failed to cancel workflow", "error", err)
			}
			close(cancelFuncCompleted)
		}
		stopFunc = context.AfterFunc(workflowCtx, workflowCancelFunction)
	}

	// Run the function in a goroutine
	outcomeChan := make(chan workflowOutcome[any], 1)
	c.workflowsWg.Add(1)
	go func() {
		defer c.workflowsWg.Done()

		if c.activeWorkflowIDs != nil {
			_, loaded := c.activeWorkflowIDs.LoadOrStore(workflowID, struct{}{})
			if loaded { // This should never happen, but if it does, we need to log it
				c.logger.Error("UNREACHABLE: workflow already running on this context", "workflow_id", workflowID)
			}
			defer c.activeWorkflowIDs.Delete(workflowID)
		}

		var result any
		var err error

		result, err = fn(workflowCtx, input)

		// Handle DBOS ID conflict errors by waiting workflow result
		if errors.Is(err, &DBOSError{Code: ConflictingIDError}) {
			c.logger.Warn("Workflow ID conflict detected. Waiting for existing workflow to complete", "workflow_id", workflowID)
			var encodedResult any
			encodedResult, err = retryWithResult(c, func() (any, error) {
				return c.systemDB.awaitWorkflowResult(uncancellableCtx, workflowID, _DB_RETRY_INTERVAL)
			}, withRetrierLogger(c.logger))
			// Keep the encoded result - decoding will happen in RunWorkflow[P,R] when we know the target type
			outcomeChan <- workflowOutcome[any]{result: encodedResult, err: err, needsDecoding: true}
			close(outcomeChan)
			return
		} else {
			status := WorkflowStatusSuccess

			// If an error occurred, set the status to error
			if err != nil {
				status = WorkflowStatusError
			}

			// If the afterFunc has started, the workflow was cancelled and the status should be set to cancelled
			if stopFunc != nil && !stopFunc() {
				c.logger.Info("Workflow was cancelled. Waiting for cancel function to complete", "workflow_id", workflowID)
				<-cancelFuncCompleted // Wait for the cancel function to complete
				status = WorkflowStatusCancelled
			}

			// Serialize the output before recording
			serializer := newJSONSerializer[any]()
			encodedOutput, serErr := serializer.Encode(result)
			if serErr != nil {
				c.logger.Error("Failed to serialize workflow output", "workflow_id", workflowID, "error", serErr)
				outcomeChan <- workflowOutcome[any]{result: nil, err: fmt.Errorf("failed to serialize output: %w", serErr)}
				close(outcomeChan)
				return
			}

			recordErr := retry(c, func() error {
				return c.systemDB.updateWorkflowOutcome(uncancellableCtx, updateWorkflowOutcomeDBInput{
					workflowID: workflowID,
					status:     status,
					err:        err,
					output:     encodedOutput,
				})
			}, withRetrierLogger(c.logger))
			if recordErr != nil {
				c.logger.Error("Error recording workflow outcome", "workflow_id", workflowID, "error", recordErr)
				outcomeChan <- workflowOutcome[any]{result: nil, err: recordErr}
				close(outcomeChan)
				return
			}
		}
		outcomeChan <- workflowOutcome[any]{result: result, err: err}
		close(outcomeChan)
	}()

	return newWorkflowHandle(uncancellableCtx, workflowID, outcomeChan), nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

// StepFunc represents a type-erased step function used internally.
type StepFunc func(ctx context.Context) (any, error)

// Step represents a type-safe step function with a specific output type R.
type Step[R any] func(ctx context.Context) (R, error)

// txnFunc represents a type-erased step function that receives a transaction.
// Used internally by runAsTxn when the step body and checkpoint share one transaction.
type txnFunc func(ctx context.Context, tx pgx.Tx) (any, error)

// txn represents a type-safe step function with output type R that receives a transaction.
type txn[R any] func(ctx context.Context, tx pgx.Tx) (R, error)

// stepOptions holds the configuration for step execution using functional options pattern.
type stepOptions struct {
	maxRetries         int             // Maximum number of retry attempts (0 = no retries)
	backoffFactor      float64         // Exponential backoff multiplier between retries (default: 2.0)
	baseInterval       time.Duration   // Initial delay between retries (default: 100ms)
	maxInterval        time.Duration   // Maximum delay between retries (default: 5s)
	stepName           string          // Custom name for the step (defaults to function name)
	preGeneratedStepID *int            // Pre generated stepID
	txIsoLevel         *pgx.TxIsoLevel // Transaction isolation level for runAsTxn (nil = ReadCommitted)
}

// setDefaults applies default values to stepOptions
func (opts *stepOptions) setDefaults() {
	if opts.backoffFactor == 0 {
		opts.backoffFactor = _DEFAULT_STEP_BACKOFF_FACTOR
	}
	if opts.baseInterval == 0 {
		opts.baseInterval = _DEFAULT_STEP_BASE_INTERVAL
	}
	if opts.maxInterval == 0 {
		opts.maxInterval = _DEFAULT_STEP_MAX_INTERVAL
	}
}

// StepOption is a functional option for configuring step execution parameters.
type StepOption func(*stepOptions)

// WithStepName sets a custom name for the step. If the step name has already been set
// by a previous call to WithStepName, this option will be ignored
func WithStepName(name string) StepOption {
	return func(opts *stepOptions) {
		if opts.stepName == "" {
			opts.stepName = name
		}
	}
}

// WithStepMaxRetries sets the maximum number of retry attempts for the step.
// A value of 0 means no retries (default behavior).
func WithStepMaxRetries(maxRetries int) StepOption {
	return func(opts *stepOptions) {
		opts.maxRetries = maxRetries
	}
}

// WithBackoffFactor sets the exponential backoff multiplier between retries.
// The delay between retries is calculated as: BaseInterval * (BackoffFactor^(retry-1))
// Default value is 2.0.
func WithBackoffFactor(factor float64) StepOption {
	return func(opts *stepOptions) {
		opts.backoffFactor = factor
	}
}

// WithBaseInterval sets the initial delay between retries.
// Default value is 100ms.
func WithBaseInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.baseInterval = interval
	}
}

// WithMaxInterval sets the maximum delay between retries.
// Default value is 5s.
func WithMaxInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.maxInterval = interval
	}
}

func WithNextStepID(stepID int) StepOption {
	return func(opts *stepOptions) {
		opts.preGeneratedStepID = &stepID
	}
}

// withTxIsolationLevel sets the transaction isolation level for runAsTxn (package-private).
func withTxIsolationLevel(level pgx.TxIsoLevel) StepOption {
	return func(opts *stepOptions) {
		opts.txIsoLevel = &level
	}
}

// StepOutcome holds the result and error from a step execution
// This struct is returned as part of a channel from the Go function when running the step inside a Go routine
type StepOutcome[R any] struct {
	Result R     `json:"result"`
	Err    error `json:"err"`
}

// StreamValue holds a value, error, and closed status from a stream read operation
// This struct is returned as part of a channel from ReadStreamAsync
type StreamValue[R any] struct {
	Value  R     // The stream value (zero value if error/closed)
	Err    error // Error if one occurred (nil otherwise)
	Closed bool  // Whether the stream is closed
}

// convertStepResult converts a generic step result to a typed result R.
// It handles both checkpointed outcomes (encoded values from database) and direct type conversions.
// Supports both real DBOS contexts and testing/mocking scenarios.
func convertStepResult[R any](ctx DBOSContext, result any) (R, error) {
	var typedResult R
	// Check if we're in a real DBOS context (not a mock)
	if _, ok := ctx.(*dbosContext); ok {
		// First check if this is a checkpointed outcome (encoded value from database)
		if checkpointed, ok := result.(stepCheckpointedOutcome); ok {
			// This came from the database and needs decoding
			encodedOutput, ok := checkpointed.value.(*string)
			if !ok {
				workflowID, _ := GetWorkflowID(ctx)
				return *new(R), newWorkflowExecutionError(workflowID, fmt.Errorf("checkpointed outcome value is not *string, got %T", checkpointed.value))
			}
			serializer := newJSONSerializer[R]()
			var decodeErr error
			typedResult, decodeErr = serializer.Decode(encodedOutput)
			if decodeErr != nil {
				workflowID, _ := GetWorkflowID(ctx)
				return *new(R), newWorkflowExecutionError(workflowID, fmt.Errorf("decoding step result to expected type %T: %w", *new(R), decodeErr))
			}
		} else if typedRes, ok := result.(R); ok {
			// When the step is executed, the result is already decoded and should be directly convertible
			typedResult = typedRes
		} else {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	} else {
		// Fallback for testing/mocking scenarios
		if typedRes, ok := result.(R); ok {
			typedResult = typedRes
		} else {
			workflowID, _ := GetWorkflowID(ctx)
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	}
	return typedResult, nil
}

type preparedStep struct {
	WorkflowID   string         // for error messages when StepState is nil
	StepOpts     *stepOptions   // always set
	StepState    *workflowState // nil when IsWithinStep
	IsWithinStep bool
}

// prepareStepExecution parses opts, loads workflow state, and optionally computes stepState.
// When wfState.isWithinStep, returns IsWithinStep=true and StepState=nil; caller should return fn(c) or fn(c,nil) and not continue.
func prepareStepExecution(c *dbosContext, opts []StepOption) (*preparedStep, error) {
	stepOpts := &stepOptions{}
	for _, opt := range opts {
		opt(stepOpts)
	}
	stepOpts.setDefaults()

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", stepOpts.stepName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	if wfState.isWithinStep {
		return &preparedStep{WorkflowID: wfState.workflowID, StepOpts: stepOpts, StepState: nil, IsWithinStep: true}, nil
	}

	var stepID int
	if stepOpts.preGeneratedStepID != nil {
		stepID = *stepOpts.preGeneratedStepID
	} else {
		stepID = wfState.nextStepID()
	}
	stepState := workflowState{
		workflowID:   wfState.workflowID,
		stepID:       stepID,
		isWithinStep: true,
	}
	return &preparedStep{WorkflowID: wfState.workflowID, StepOpts: stepOpts, StepState: &stepState, IsWithinStep: false}, nil
}

// executeStepWithRetry runs runOnce (the step body) and retries with backoff on error when maxRetries > 0.
func executeStepWithRetry(c *dbosContext, workflowID string, stepOpts *stepOptions, runOnce func() (any, error)) (stepOutput any, stepError error) {
	stepOutput, stepError = runOnce()
	if stepError == nil || stepOpts.maxRetries <= 0 {
		return stepOutput, stepError
	}
	var joinedErrors error
	joinedErrors = errors.Join(joinedErrors, stepError)
	for retry := 1; retry <= stepOpts.maxRetries; retry++ {
		delay := stepOpts.baseInterval
		if retry > 1 {
			exponentialDelay := float64(stepOpts.baseInterval) * math.Pow(stepOpts.backoffFactor, float64(retry-1))
			delay = time.Duration(math.Min(exponentialDelay, float64(stepOpts.maxInterval)))
		}
		c.logger.Error("step failed, retrying", "step_name", stepOpts.stepName, "retry", retry, "max_retries", stepOpts.maxRetries, "delay", delay, "error", stepError)
		select {
		case <-c.Done():
			return nil, newStepExecutionError(workflowID, stepOpts.stepName, fmt.Errorf("context cancelled during retry: %w", c.Err()))
		case <-time.After(delay):
		}
		stepOutput, stepError = runOnce()
		if stepError == nil {
			return stepOutput, stepError
		}
		joinedErrors = errors.Join(joinedErrors, stepError)
		if retry == stepOpts.maxRetries {
			stepError = newMaxStepRetriesExceededError(workflowID, stepOpts.stepName, stepOpts.maxRetries, joinedErrors)
			break
		}
	}
	return stepOutput, stepError
}

// RunAsStep executes a function as a durable step within a workflow.
// Steps provide at-least-once execution guarantees and automatic retry capabilities.
// If a step has already been executed (e.g., during workflow recovery), its recorded
// result is returned instead of re-executing the function.
//
// Steps can be configured with functional options:
//
//	data, err := dbos.RunAsStep(ctx, func(ctx context.Context) ([]byte, error) {
//	    return MyStep(ctx, "https://api.example.com/data")
//	}, dbos.WithStepMaxRetries(3), dbos.WithBaseInterval(500*time.Millisecond))
//
// Available options:
//   - WithStepName: Custom name for the step (only sets if not already set)
//   - WithStepMaxRetries: Maximum retry attempts (default: 0)
//   - WithBackoffFactor: Exponential backoff multiplier (default: 2.0)
//   - WithBaseInterval: Initial delay between retries (default: 100ms)
//   - WithMaxInterval: Maximum delay between retries (default: 5s)
//
// Example:
//
//	func MyStep(ctx context.Context, url string) ([]byte, error) {
//	    resp, err := http.Get(url)
//	    if err != nil {
//	        return nil, err
//	    }
//	    defer resp.Body.Close()
//	    return io.ReadAll(resp.Body)
//	}
//
//	// Within a workflow:
//	data, err := dbos.RunAsStep(ctx, func(ctx context.Context) ([]byte, error) {
//	    return MyStep(ctx, "https://api.example.com/data")
//	}, dbos.WithStepName("FetchData"), dbos.WithStepMaxRetries(3))
//	if err != nil {
//	    return nil, err
//	}
//
// Note that the function passed to RunAsStep must accept a context.Context as its first parameter
// and this context *must* be the one specified in the function's signature (not the context passed to RunAsStep).
// Under the hood, DBOS uses the provided context to manage durable execution.
func RunAsStep[R any](ctx DBOSContext, fn Step[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	// Type-erase the function
	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.RunAsStep(ctx, typeErasedFn, opts...)
	// Step function could return a nil result
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

func (c *dbosContext) RunAsStep(_ DBOSContext, fn StepFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		return fn(c)
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepOpts := prep.StepOpts

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := retryWithResult(c, func() (*recordedResult, error) {
		return c.systemDB.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowID: stepState.workflowID,
			stepID:     stepState.stepID,
			stepName:   stepOpts.stepName,
		})
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
	}
	if recordedOutput != nil {
		// Return the encoded output wrapped in stepCheckpointedOutcome
		// This allows RunAsStep[R] to distinguish encoded values from direct values
		return stepCheckpointedOutcome{value: recordedOutput.output}, recordedOutput.err
	}

	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()
	stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, func() (any, error) { return fn(stepCtx) })

	// Serialize step output before recording
	serializer := newJSONSerializer[any]()
	encodedStepOutput, serErr := serializer.Encode(stepOutput)
	if serErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
	}

	// Record the final result
	stepCompletedTime := time.Now()
	dbInput := recordOperationResultDBInput{
		workflowID:  stepState.workflowID,
		stepName:    stepOpts.stepName,
		stepID:      stepState.stepID,
		err:         stepError,
		startedAt:   stepStartTime,
		completedAt: stepCompletedTime,
		output:      encodedStepOutput,
	}
	recErr := retry(c, func() error {
		return c.systemDB.recordOperationResult(uncancellableCtx, dbInput)
	}, withRetrierLogger(c.logger))
	if recErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, recErr)
	}

	return stepOutput, stepError
}

// runAsTxn executes a step function that receives a transaction when run on its own.
// The step body and checkpoint share one transaction, so system DB writes and recordOperationResult commit together.
// Like RunAsStep but uses txn[R] / txnFunc; transaction is begun and committed inside this function.
func runAsTxn[R any](ctx DBOSContext, fn txn[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
	}

	c, ok := ctx.(*dbosContext)
	if !ok {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("runAsTxn requires *dbosContext. Mock the caller of this function if you are testing."))
	}

	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	typeErasedFn := txnFunc(func(ctx context.Context, tx pgx.Tx) (any, error) { return fn(ctx, tx) })

	result, err := c.runAsTxn(ctx, typeErasedFn, opts...)
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

func (c *dbosContext) runAsTxn(_ DBOSContext, fn txnFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		return fn(c, nil)
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepOpts := prep.StepOpts
	pool := c.systemDB.(*sysDB).pool
	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()
	serializer := newJSONSerializer[any]()

	txOpts := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	if stepOpts.txIsoLevel != nil {
		txOpts.IsoLevel = *stepOpts.txIsoLevel
	}
	return retryWithResult(c, func() (any, error) {
		tx, err := pool.BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)

		recordedOutput, err := c.systemDB.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowID: stepState.workflowID,
			stepID:     stepState.stepID,
			stepName:   stepOpts.stepName,
			tx:         tx,
		})
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
		}
		if recordedOutput != nil {
			return stepCheckpointedOutcome{value: recordedOutput.output}, recordedOutput.err
		}

		stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, func() (any, error) { return fn(stepCtx, tx) })

		encodedStepOutput, serErr := serializer.Encode(stepOutput)
		if serErr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
		}

		dbInput := recordOperationResultDBInput{
			workflowID:  stepState.workflowID,
			stepName:    stepOpts.stepName,
			stepID:      stepState.stepID,
			err:         stepError,
			startedAt:   stepStartTime,
			completedAt: time.Now(),
			output:      encodedStepOutput,
			tx:          tx,
		}
		recErr := c.systemDB.recordOperationResult(uncancellableCtx, dbInput)
		if recErr != nil {
			if stepError != nil {
				recErr = errors.Join(recErr, stepError)
			}
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, recErr)
		}
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", err))
		}
		return stepOutput, stepError
	}, withRetrierLogger(c.logger))
}

// Go runs a step inside a Go routine and returns a channel to receive the result.
// Go generates a deterministic step ID for the step before running the step in a routine, since goroutines are not deterministic.
// Example:
//
// resultChan, err := dbos.Go(ctx, func(ctx context.Context) (string, error) {
//   return "Hello, World!", nil
// })
//
// resultChan := <-resultChan // wait for the channel to receive
// if resultChan.err != nil {
//   // Handle error
// }

func Go[R any](ctx DBOSContext, fn Step[R], opts ...StepOption) (chan StepOutcome[R], error) {
	if ctx == nil {
		return nil, newStepExecutionError("", "", errors.New("ctx cannot be nil"))
	}

	if fn == nil {
		return nil, newStepExecutionError("", "", errors.New("step function cannot be nil"))
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	// Type-erase the function
	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.Go(ctx, typeErasedFn, opts...)
	if err != nil {
		return nil, err
	}

	// Create the typed channel to return immediately (non-blocking)
	outcomeChan := make(chan StepOutcome[R], 1)

	// Start a goroutine to handle decoding and type conversion asynchronously
	go func() {
		defer close(outcomeChan)

		outcome := <-result // Block here waiting for the step to complete

		// If the step function returns a nil result, send the error through the channel
		if outcome.Result == nil {
			outcomeChan <- StepOutcome[R]{
				Result: *new(R),
				Err:    outcome.Err,
			}
			return
		}

		typedResult, convertErr := convertStepResult[R](ctx, outcome.Result)
		if convertErr != nil {
			outcomeChan <- StepOutcome[R]{
				Result: *new(R),
				Err:    convertErr,
			}
			return
		}

		outcomeChan <- StepOutcome[R]{
			Result: typedResult,
			Err:    outcome.Err,
		}
	}()

	return outcomeChan, nil
}

func (c *dbosContext) Go(ctx DBOSContext, fn StepFunc, opts ...StepOption) (chan StepOutcome[any], error) {
	// Create a deterministic step ID
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", "", errors.New("workflow state not found in context: are you running this step within a workflow?"))
	}
	opts = append(opts, WithNextStepID(wfState.nextStepID()))

	// Run step inside a Go routine
	result := make(chan StepOutcome[any], 1)
	go func() {
		defer close(result)
		res, err := ctx.RunAsStep(ctx, fn, opts...)
		result <- StepOutcome[any]{
			Result: res,
			Err:    err,
		}
	}()

	return result, nil
}

// Select performs a durable select operation over a slice of channels obtained from Go.
// It checkpoints the selected channel index and value so that workflow replay produces deterministic results.
// Select can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	ch1, _ := dbos.Go(ctx, func(ctx context.Context) (string, error) { return "result1", nil })
//	ch2, _ := dbos.Go(ctx, func(ctx context.Context) (string, error) { return "result2", nil })
//	outcome, err := dbos.Select(ctx, []<-chan dbos.StepOutcome[string]{ch1, ch2})
//	if err != nil {
//	    // Handle error
//	    return err
//	}
//	log.Printf("Selected result: %v, error: %v", outcome.result, outcome.err)
func Select[R any](ctx DBOSContext, channels []<-chan StepOutcome[R]) (R, error) {
	if ctx == nil {
		var zero R
		return zero, errors.New("ctx cannot be nil")
	}

	// If channels slice is empty, log warning and return zero value
	if len(channels) == 0 {
		if c, ok := ctx.(*dbosContext); ok {
			c.logger.Warn("Select called with empty channels slice, returning zero value")
		}
		var zero R
		return zero, nil
	}

	// Convert typed channels to any channels for internal processing
	// Create a context that will be cancelled when Select completes to prevent goroutine leaks
	selectCtx, cancelSelect := context.WithCancel(ctx)
	defer cancelSelect()

	anyChannels := make([]<-chan StepOutcome[any], len(channels))
	for i := range channels {
		anyCh := make(chan StepOutcome[any], cap(channels[i]))
		srcCh := channels[i]
		go func() {
			defer close(anyCh)
			for {
				select {
				case <-selectCtx.Done():
					return
				case outcome, ok := <-srcCh:
					if !ok {
						// Source channel closed
						return
					}
					select {
					case anyCh <- StepOutcome[any]{
						Result: outcome.Result,
						Err:    outcome.Err,
					}:
					case <-selectCtx.Done():
						// Select completed while trying to send, discard value
						return
					}
				}
			}
		}()
		anyChannels[i] = anyCh
	}

	result, err := ctx.Select(ctx, anyChannels)
	// Step function could return a nil result
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

func (c *dbosContext) Select(_ DBOSContext, channels []<-chan StepOutcome[any]) (any, error) {
	// If channels slice is empty, log warning and return zero value
	if len(channels) == 0 {
		c.logger.Warn("Select called with empty channels slice, returning zero value")
		return nil, nil
	}

	// Use RunAsStep to wrap the select operation
	result, err := c.RunAsStep(c, func(ctx context.Context) (any, error) {
		// Build select cases using reflect.Select
		cases := make([]reflect.SelectCase, 0, len(channels)+1)

		// Add context cancellation case first (highest priority)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})

		// Add all channel cases
		for _, ch := range channels {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		// Perform the select
		chosen, value, ok := reflect.Select(cases)

		// Handle context cancellation (chosen == 0 means context.Done() was selected)
		if chosen == 0 {
			return nil, ctx.Err()
		}

		// Check if channel was closed
		if !ok {
			// Adjust index since context case is at index 0
			selectedIndex := chosen - 1
			// If context was cancelled, return cancellation error instead of channel closed error
			// This handles the race condition after a closed channel (due to cancellation) is selected
			// instead of context.Done() (both are eligible to be selected).
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("channel at index %d was closed", selectedIndex)
		}

		// Extract the StepOutcome[any] from the reflect.Value
		outcomeValue := value.Interface()
		outcome, ok := outcomeValue.(StepOutcome[any])
		if !ok {
			// Adjust index since context case is at index 0
			selectedIndex := chosen - 1
			return nil, fmt.Errorf("unexpected value type from channel at index %d: expected StepOutcome[any], got %T", selectedIndex, outcomeValue)
		}

		return outcome.Result, outcome.Err
	}, WithStepName("DBOS.select"))

	// Return both result and error, similar to RunAsStep
	// The step function can return both a result and an error
	return result, err
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

func (c *dbosContext) Send(_ DBOSContext, destinationID string, message any, topic string) error {
	// Send cannot be sent from within a step if used within a workflow
	isWithinWorkflow := false
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if ok && wfState != nil {
		isWithinWorkflow = true
		if wfState.isWithinStep {
			return newStepExecutionError(wfState.workflowID, "DBOS.send", fmt.Errorf("cannot call Send within a step"))
		}
	}

	// Serialize the message before sending
	serializer := newJSONSerializer[any]()
	encodedMessage, err := serializer.Encode(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	input := WorkflowSendInput{
		DestinationID: destinationID,
		Message:       encodedMessage,
		Topic:         topic,
	}

	if isWithinWorkflow {
		_, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
			input.tx = tx
			return nil, ctx.(*dbosContext).systemDB.send(ctx, input)
		}, WithStepName("DBOS.send"))
	} else {
		err = retry(c, func() error {
			return c.systemDB.send(c, input)
		}, withRetrierLogger(c.logger))
	}
	return err
}

// Send sends a message to another workflow with type safety.
//
// Send can be called from within a workflow (as a durable step) or from outside workflows.
// When called within a workflow, the send operation becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.Send(ctx, "target-workflow-id", "Hello from sender", "notifications")
func Send[P any](ctx DBOSContext, destinationID string, message P, topic string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.Send(ctx, destinationID, message, topic)
}

type recvInput struct {
	Topic   string        // Topic to listen for (empty string receives from default topic)
	Timeout time.Duration // Maximum time to wait for a message
}

func (c *dbosContext) Recv(_ DBOSContext, topic string, timeout time.Duration) (any, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", "DBOS.recv", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return nil, newStepExecutionError(wfState.workflowID, "DBOS.recv", fmt.Errorf("cannot call Recv within a step"))
	}
	input := recvInput{
		Topic:   topic,
		Timeout: timeout,
	}
	recvRetryOpts := []retryOption{withRetrierLogger(c.logger)}
	if sysDB, ok := c.systemDB.(*sysDB); ok && sysDB.isCockroachDB {
		recvRetryOpts = append(recvRetryOpts, withRetryCondition(isRetryableTransaction))
	}
	return retryWithResult(c, func() (*string, error) {
		return c.systemDB.recv(c, input)
	}, recvRetryOpts...)
}

// Recv receives a message sent to this workflow with type safety.
// This function blocks until a message is received or the timeout is reached.
// Messages are consumed in FIFO order and each message is delivered exactly once.
//
// Recv can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	message, err := dbos.Recv[string](ctx, "notifications", 30 * time.Second)
//	if err != nil {
//	    // Handle timeout or error
//	    return err
//	}
//	log.Printf("Received: %s", message)
func Recv[R any](ctx DBOSContext, topic string, timeout time.Duration) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	msg, err := ctx.Recv(ctx, topic, timeout)
	if err != nil {
		return *new(R), err
	}

	// Handle nil message
	if msg == nil {
		return *new(R), nil
	}

	var typedMessage R
	// Check if we're in a real DBOS context (not a mock)
	if _, ok := ctx.(*dbosContext); ok {
		encodedMsg, ok := msg.(*string)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, "string (encoded)", fmt.Sprintf("%T", msg))
		}
		serializer := newJSONSerializer[R]()
		var decodeErr error
		typedMessage, decodeErr = serializer.Decode(encodedMsg)
		if decodeErr != nil {
			return *new(R), fmt.Errorf("decoding received message to type %T: %w", *new(R), decodeErr)
		}
		return typedMessage, nil
	} else {
		// Fallback for testing/mocking scenarios where serializer is nil
		var ok bool
		typedMessage, ok = msg.(R)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", msg))
		}
	}
	return typedMessage, nil
}

func (c *dbosContext) SetEvent(_ DBOSContext, key string, message any) error {
	// Serialize the event value before storing
	serializer := newJSONSerializer[any]()
	encodedMessage, err := serializer.Encode(message)
	if err != nil {
		return fmt.Errorf("failed to serialize event value: %w", err)
	}

	_, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
		return nil, c.systemDB.setEvent(ctx, WorkflowSetEventInput{
			Key:     key,
			Message: encodedMessage,
			tx:      tx,
		})
	}, WithStepName("DBOS.setEvent"))
	return err
}

// SetEvent sets a key-value event for the current workflow with type safety.
// Events are persistent and can be retrieved by other workflows using GetEvent.
//
// SetEvent can only be called from within a workflow and becomes part of the workflow's durable state.
// Setting an event with the same key will overwrite the previous value.
//
// Example:
//
//	err := dbos.SetEvent(ctx, "status", "processing-complete")
func SetEvent[P any](ctx DBOSContext, key string, message P) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetEvent(ctx, key, message)
}

type getEventInput struct {
	TargetWorkflowID string        // Workflow ID to get the event from
	Key              string        // Event key to retrieve
	Timeout          time.Duration // Maximum time to wait for the event to be set
}

func (c *dbosContext) GetEvent(_ DBOSContext, targetWorkflowID, key string, timeout time.Duration) (any, error) {
	input := getEventInput{
		TargetWorkflowID: targetWorkflowID,
		Key:              key,
		Timeout:          timeout,
	}
	return retryWithResult(c, func() (any, error) {
		return c.systemDB.getEvent(c, input)
	}, withRetrierLogger(c.logger))
}

// GetEvent retrieves a key-value event from a target workflow with type safety.
// This function blocks until the event is set or the timeout is reached.
//
// When called within a workflow, the get operation becomes part of the workflow's durable state.
// The returned value is of type R and will be type-checked at runtime.
//
// Example:
//
//	status, err := dbos.GetEvent[string](ctx, "target-workflow-id", "status", 30 * time.Second)
//	if err != nil {
//	    // Handle timeout or error
//	    return err
//	}
//	log.Printf("Status: %s", status)
func GetEvent[R any](ctx DBOSContext, targetWorkflowID, key string, timeout time.Duration) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	value, err := ctx.GetEvent(ctx, targetWorkflowID, key, timeout)
	if err != nil {
		return *new(R), err
	}
	if value == nil {
		return *new(R), nil
	}

	var typedValue R
	// Check if we're in a real DBOS context (not a mock)
	if _, ok := ctx.(*dbosContext); ok {
		encodedValue, ok := value.(*string)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, "string (encoded)", fmt.Sprintf("%T", value))
		}

		serializer := newJSONSerializer[R]()
		var decodeErr error
		typedValue, decodeErr = serializer.Decode(encodedValue)
		if decodeErr != nil {
			return *new(R), fmt.Errorf("decoding event value to type %T: %w", *new(R), decodeErr)
		}
		return typedValue, nil
	} else {
		var ok bool
		typedValue, ok = value.(R)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", value))
		}
	}
	return typedValue, nil
}

func (c *dbosContext) WriteStream(_ DBOSContext, key string, value any) error {
	// value is already encoded as *string at the package level
	encodedValue, ok := value.(*string)
	if !ok {
		return fmt.Errorf("value must be *string (already encoded), got %T", value)
	}
	_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
		return "", c.systemDB.writeStream(ctx, writeStreamDBInput{
			Key:   key,
			Value: encodedValue,
			tx:    tx,
		})
	}, WithStepName("DBOS.writeStream"))
	return err
}

// WriteStream writes a value to a durable stream with type safety.
// Streams are append-only and ordered by offset.
//
// WriteStream can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.WriteStream(ctx, "my-stream", "stream-value")
func WriteStream[P any](ctx DBOSContext, key string, value P) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	// Serialize the stream value before storing (using typed serializer)
	serializer := newJSONSerializer[P]()
	encodedValue, err := serializer.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to serialize stream value: %w", err)
	}
	return ctx.WriteStream(ctx, key, encodedValue)
}

// readStream runs the read stream polling logic in a goroutine
// and sends values through a channel as they're read
func (c *dbosContext) readStream(workflowID string, key string) <-chan StreamValue[any] {
	ch := make(chan StreamValue[any], 1) // Buffered to allow non-blocking sends

	go func() {
		defer close(ch)

		var currentOffset int
		closed := false

		// Continue reading until workflow is inactive or stream is closed
		for {
			// Read stream entries from current offset
			input := readStreamDBInput{
				WorkflowID: workflowID,
				Key:        key,
				FromOffset: currentOffset,
			}

			var entries []streamEntry
			err := retry(c, func() error {
				var retryErr error
				entries, closed, retryErr = c.systemDB.readStream(c, input)
				return retryErr
			}, withRetrierLogger(c.logger))

			if err != nil {
				ch <- StreamValue[any]{Err: err}
				return
			}

			// Send each entry value to the channel
			for _, entry := range entries {
				ch <- StreamValue[any]{Value: entry.Value}
				currentOffset = entry.Offset + 1 // Next offset to read from
			}

			// If stream is closed (sentinel found), send final message and stop
			if closed {
				ch <- StreamValue[any]{Closed: true}
				return
			}

			// Check if workflow is still active (PENDING or ENQUEUED)
			status, err := retryWithResult(c, func() (WorkflowStatusType, error) {
				workflows, err := c.systemDB.listWorkflows(c, listWorkflowsDBInput{
					workflowIDs: []string{workflowID},
					loadInput:   false,
					loadOutput:  false,
				})
				if err != nil {
					return "", err
				}
				if len(workflows) == 0 {
					return "", newNonExistentWorkflowError(workflowID)
				}
				return workflows[0].Status, nil
			}, withRetrierLogger(c.logger))

			if err != nil {
				ch <- StreamValue[any]{Err: err}
				return
			}

			// If workflow is inactive, send final message with Closed: true (BUG FIX)
			if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
				ch <- StreamValue[any]{Closed: true}
				return
			}

			// If no new entries, wait a bit before polling again
			if len(entries) == 0 {
				select {
				case <-c.Done():
					ch <- StreamValue[any]{Err: c.Err()}
					return
				case <-time.After(_DB_RETRY_INTERVAL):
					// Continue loop to read again
				}
			}
		}
	}()

	return ch
}

func (c *dbosContext) ReadStream(_ DBOSContext, workflowID string, key string) ([]any, bool, error) {
	var allValues []any
	closed := false

	ch := c.readStream(workflowID, key)

	for streamValue := range ch {
		if streamValue.Err != nil {
			return nil, false, streamValue.Err
		}

		if streamValue.Closed {
			closed = true
			break
		}

		// Collect the value
		allValues = append(allValues, streamValue.Value)
	}

	return allValues, closed, nil
}

// ReadStream reads values from a durable stream.
// This method blocks until the stream is closed or an error occurs.
// The stream is considered close when the sentinel value is found or the workflow becomes inactive (status is not PENDING or ENQUEUED)
//
// Returns the values, whether the stream is closed, and any error.
//
// Example:
//
//	values, closed, err := dbos.ReadStream[string](ctx, "workflow-id", "my-stream")
//	if err != nil {
//	    return err
//	}
//	for _, value := range values {
//	    log.Printf("Stream value: %s", value)
//	}
func ReadStream[R any](ctx DBOSContext, workflowID string, key string) ([]R, bool, error) {
	if ctx == nil {
		return nil, false, errors.New("ctx cannot be nil")
	}
	values, closed, err := ctx.ReadStream(ctx, workflowID, key)
	if err != nil {
		return nil, false, err
	}

	// Decode each value to type R
	serializer := newJSONSerializer[R]()
	typedValues := make([]R, len(values))
	for i, val := range values {
		encodedStr, ok := val.(string)
		if !ok {
			return nil, false, fmt.Errorf("stream value is not a string, got %T", val)
		}
		decodedValue, decodeErr := serializer.Decode(&encodedStr)
		if decodeErr != nil {
			return nil, false, fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)
		}
		typedValues[i] = decodedValue
	}

	return typedValues, closed, nil
}

// ReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
func (c *dbosContext) ReadStreamAsync(_ DBOSContext, workflowID string, key string) (<-chan StreamValue[any], error) {
	return c.readStream(workflowID, key), nil
}

// ReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
//
// This method returns immediately with a channel. Values will be sent to the channel
// as they're read from the stream. The channel will be closed when the stream is closed or an error occurs.
// The stream is considered close when the sentinel value is found or the workflow becomes inactive (status is not PENDING or ENQUEUED)
//
// Example:
//
//	ch, err := dbos.ReadStreamAsync[string](ctx, "workflow-id", "my-stream")
//	if err != nil {
//	    return err
//	}
//	for streamValue := range ch {
//	    if streamValue.Err != nil {
//	        log.Printf("Error: %v", streamValue.Err)
//	        break
//	    }
//	    if streamValue.Closed {
//	        log.Println("Stream closed")
//	        break
//	    }
//	    log.Printf("Received value: %s", streamValue.Value)
//	}
func ReadStreamAsync[R any](ctx DBOSContext, workflowID string, key string) (<-chan StreamValue[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyCh, err := ctx.ReadStreamAsync(ctx, workflowID, key)
	if err != nil {
		return nil, err
	}

	typedCh := make(chan StreamValue[R], 1)

	go func() {
		defer close(typedCh)

		serializer := newJSONSerializer[R]()

		for streamValue := range anyCh {
			if streamValue.Err != nil {
				typedCh <- StreamValue[R]{Err: streamValue.Err}
				return
			}

			if streamValue.Closed {
				typedCh <- StreamValue[R]{Closed: true}
				return
			}

			encodedStr, ok := streamValue.Value.(string)
			if !ok {
				typedCh <- StreamValue[R]{Err: fmt.Errorf("stream value is not a string, got %T", streamValue.Value)}
				return
			}

			decodedValue, decodeErr := serializer.Decode(&encodedStr)
			if decodeErr != nil {
				typedCh <- StreamValue[R]{Err: fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)}
				return
			}

			typedCh <- StreamValue[R]{Value: decodedValue}
		}
	}()

	return typedCh, nil
}

func (c *dbosContext) CloseStream(_ DBOSContext, key string) error {
	_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
		sentinel := _DBOS_STREAM_CLOSED_SENTINEL
		return "", c.systemDB.writeStream(ctx, writeStreamDBInput{
			Key:   key,
			Value: &sentinel,
			tx:    tx,
		})
	}, WithStepName("DBOS.closeStream"))
	return err
}

// CloseStream closes a durable stream by writing the sentinel value.
//
// CloseStream can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.CloseStream(ctx, "my-stream")
//	if err != nil {
//	    return err
//	}
func CloseStream(ctx DBOSContext, key string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CloseStream(ctx, key)
}

func (c *dbosContext) Sleep(_ DBOSContext, duration time.Duration) (time.Duration, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, newStepExecutionError("", "DBOS.sleep", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return 0, newStepExecutionError(wfState.workflowID, "DBOS.sleep", fmt.Errorf("cannot call Sleep within a step"))
	}
	return retryWithResult(c, func() (time.Duration, error) {
		return c.systemDB.sleep(c, sleepInput{duration: duration, skipSleep: false})
	}, withRetrierLogger(c.logger))
}

// Sleep pauses workflow execution for the specified duration.
// This is a durable sleep - if the workflow is recovered during the sleep period,
// it will continue sleeping for the remaining time.
// Returns the actual duration slept.
//
// Example:
//
//	actualDuration, err := dbos.Sleep(ctx, 5*time.Second)
//	if err != nil {
//	    return err
//	}
func Sleep(ctx DBOSContext, duration time.Duration) (time.Duration, error) {
	if ctx == nil {
		return 0, errors.New("ctx cannot be nil")
	}
	return ctx.Sleep(ctx, duration)
}

const _DBOS_PATCH_PREFIX = "DBOS.patch-"

func (c *dbosContext) Patch(_ DBOSContext, patchName string) (bool, error) {
	if !c.config.EnablePatching {
		return false, newPatchingNotEnabledError()
	}

	if patchName == "" {
		return false, errors.New("patch name cannot be empty")
	}

	// Get workflow state to determine current step ID
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return false, errors.New("patch can only be called within a workflow")
	}

	if wfState.isWithinStep {
		return false, newStepExecutionError(wfState.workflowID, patchName, fmt.Errorf("cannot call Patch within a step"))
	}

	// Automatically prefix the patch name with _DBOS_PATCH_PREFIX
	prefixedPatchName := _DBOS_PATCH_PREFIX + patchName

	patched, err := retryWithResult(c, func() (bool, error) {
		return c.systemDB.patch(c, patchDBInput{
			workflowID: wfState.workflowID,
			stepID:     wfState.stepID + 1, // We are checking if the upcoming step should use the patched code
			patchName:  prefixedPatchName,
		})
	}, withRetrierLogger(c.logger))

	if patched && err == nil {
		// The patch take its own step ID
		wfState.nextStepID()
	}

	return patched, err
}

// Patch checks if the current workflow should use patched code.
// Returns true if the workflow should use new code, false if it should use old code.
//
// The patch system allows modifying code while long-lived workflows are running:
// - Existing workflows that already passed this patch point continue with old code
// - New workflows use new code
// - Workflows that started but haven't reached this point yet use new code
//
// Example:
//
//	if dbos.Patch(ctx, "my-patch") {
//	    // New code path
//	} else {
//	    // Old code path
//	}
func Patch(ctx DBOSContext, patchName string) (bool, error) {
	if ctx == nil {
		return false, errors.New("ctx cannot be nil")
	}
	return ctx.Patch(ctx, patchName)
}

func (c *dbosContext) DeprecatePatch(_ DBOSContext, patchName string) error {
	if !c.config.EnablePatching {
		return newPatchingNotEnabledError()
	}

	if patchName == "" {
		return errors.New("patch name cannot be empty")
	}

	// Get workflow state to determine current step ID
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return errors.New("deprecate patch can only be called within a workflow")
	}

	if wfState.isWithinStep {
		return newStepExecutionError(wfState.workflowID, patchName, fmt.Errorf("cannot call DeprecatePatch within a step"))
	}

	// Automatically prefix the patch name with _DBOS_PATCH_PREFIX
	prefixedPatchName := _DBOS_PATCH_PREFIX + patchName

	patchNameFromDB, err := retryWithResult(c, func() (string, error) {
		return c.systemDB.doesPatchExists(c, patchDBInput{
			workflowID: wfState.workflowID,
			stepID:     wfState.stepID + 1,
			patchName:  prefixedPatchName,
		})
	}, withRetrierLogger(c.logger))

	// If patch doesn't exist, it's already deprecated (or never existed)
	if patchNameFromDB != prefixedPatchName || err == pgx.ErrNoRows {
		return nil
	}

	// If there was an error checking, return it
	if err != nil {
		return err
	}

	// Patch exists, deprecate it by incrementing step ID
	wfState.nextStepID()
	return nil
}

// DeprecatePatch allows removing patches from code while ensuring the correct history
// of workflows that were executing before the patch was deprecated.
//
// Example:
//
// err := dbos.DeprecatePatch(ctx, "my-patch")
//
//	if err != nil {
//	    return err
//	}
//
// // New code path
func DeprecatePatch(ctx DBOSContext, patchName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.DeprecatePatch(ctx, patchName)
}

/***********************************/
/******* WORKFLOW MANAGEMENT *******/
/***********************************/

func (c *dbosContext) GetWorkflowID() (string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.workflowID, nil
}

func (c *dbosContext) GetStepID() (int, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return -1, errors.New("not within a DBOS workflow context")
	}
	return wfState.stepID, nil
}

// GetWorkflowID retrieves the workflow ID from the context if called within a DBOS workflow.
// Returns an error if not called from within a workflow context.
//
// Example:
//
//	workflowID, err := dbos.GetWorkflowID(ctx)
//	if err != nil {
//	    log.Printf("Not within a workflow context")
//	} else {
//	    log.Printf("Current workflow ID: %s", workflowID)
//	}
func GetWorkflowID(ctx DBOSContext) (string, error) {
	if ctx == nil {
		return "", errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowID()
}

// GetStepID retrieves the current step ID from the context if called within a DBOS workflow.
// Returns -1 and an error if not called from within a workflow context.
//
// Example:
//
//	stepID, err := dbos.GetStepID(ctx)
//	if err != nil {
//	    log.Printf("Not within a workflow context")
//	} else {
//	    log.Printf("Current step ID: %d", stepID)
//	}
func GetStepID(ctx DBOSContext) (int, error) {
	if ctx == nil {
		return -1, errors.New("ctx cannot be nil")
	}
	return ctx.GetStepID()
}

func (c *dbosContext) RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error) {
	loadInput := false
	loadOutput := false
	if c.launched.Load() {
		loadInput = false
		loadOutput = false
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var workflowStatus []WorkflowStatus
	var err error
	if isWithinWorkflow {
		workflowStatus, err = RunAsStep(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return retryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.systemDB.listWorkflows(ctx, listWorkflowsDBInput{
					workflowIDs: []string{workflowID},
					loadInput:   loadInput,
					loadOutput:  loadOutput,
				})
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.retrieveWorkflow"))
	} else {
		workflowStatus, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.systemDB.listWorkflows(c, listWorkflowsDBInput{
				workflowIDs: []string{workflowID},
				loadInput:   loadInput,
				loadOutput:  loadOutput,
			})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return newWorkflowPollingHandle[any](c, workflowID), nil
}

// RetrieveWorkflow returns a typed handle to an existing workflow.
// The handle can be used to check status and wait for results.
// The type parameter R must match the workflow's actual return type.
//
// Example:
//
//	handle, err := dbos.RetrieveWorkflow[int](ctx, "workflow-id")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := handle.GetResult()
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %d", result)
//	}
func RetrieveWorkflow[R any](ctx DBOSContext, workflowID string) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("dbosCtx cannot be nil")
	}

	// Call the interface method
	handle, err := ctx.RetrieveWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	// Convert to typed polling handle
	return newWorkflowPollingHandle[R](ctx, handle.GetWorkflowID()), nil
}

func (c *dbosContext) CancelWorkflow(_ DBOSContext, workflowID string) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
			err := c.systemDB.cancelWorkflow(ctx, cancelWorkflowDBInput{workflowID: workflowID, tx: tx})
			return "", err
		}, WithStepName("DBOS.cancelWorkflow"))
		return err
	} else {
		return retry(c, func() error {
			return c.systemDB.cancelWorkflow(c, cancelWorkflowDBInput{workflowID: workflowID})
		}, withRetrierLogger(c.logger))
	}
}

// CancelWorkflow cancels a running or enqueued workflow by setting its status to CANCELLED and removing it from the queue.
// Once cancelled, the workflow will stop executing at the start of the next step. Executing steps will not be interrupted.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowID: The unique identifier of the workflow to cancel
//
// Returns an error if the workflow does not exist or if the cancellation operation fails.
//
// Example:
//
//	err := dbos.CancelWorkflow(ctx, "workflow-to-cancel")
//	if err != nil {
//	    log.Printf("Failed to cancel workflow: %v", err)
//	}
func CancelWorkflow(ctx DBOSContext, workflowID string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CancelWorkflow(ctx, workflowID)
}

func (c *dbosContext) ResumeWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error) {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var err error
	if isWithinWorkflow {
		_, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
			return nil, c.systemDB.resumeWorkflow(ctx, resumeWorkflowDBInput{workflowID: workflowID, tx: tx})
		}, withTxIsolationLevel(pgx.RepeatableRead), WithStepName("DBOS.resumeWorkflow"))
	} else {
		err = retry(c, func() error {
			return c.systemDB.resumeWorkflow(c, resumeWorkflowDBInput{workflowID: workflowID})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[any](c, workflowID), nil
}

// ResumeWorkflow resumes a workflow by starting it from its last completed step.
// You can use this to resume workflows that are cancelled or have exceeded their maximum
// recovery attempts. You can also use this to start an enqueued workflow immediately,
// bypassing its queue.
// If the workflow is already completed, this is a no-op.
// Returns a handle that can be used to wait for completion and retrieve results.
// Returns an error if the workflow does not exist or if the operation fails.
//
// Example:
//
//	handle, err := dbos.ResumeWorkflow[int](ctx, "workflow-id")
//	if err != nil {
//	    log.Printf("Failed to resume workflow: %v", err)
//	} else {
//	    result, err := handle.GetResult()
//	    if err != nil {
//	        log.Printf("Workflow failed: %v", err)
//	    } else {
//	        log.Printf("Result: %d", result)
//	    }
//	}
func ResumeWorkflow[R any](ctx DBOSContext, workflowID string) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	_, err := ctx.ResumeWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[R](ctx, workflowID), nil
}

// ForkWorkflowInput holds configuration parameters for forking workflows.
// OriginalWorkflowID is required. Other fields are optional.
type ForkWorkflowInput struct {
	OriginalWorkflowID string // Required: The UUID of the original workflow to fork from
	ForkedWorkflowID   string // Optional: Custom workflow ID for the forked workflow (auto-generated if empty)
	StartStep          uint   // Optional: Step to start the forked workflow from (default: 0)
	ApplicationVersion string // Optional: Application version for the forked workflow (inherits from original if empty)
}

func (c *dbosContext) ForkWorkflow(_ DBOSContext, input ForkWorkflowInput) (WorkflowHandle[any], error) {
	if input.OriginalWorkflowID == "" {
		return nil, errors.New("original workflow ID cannot be empty")
	}

	// Create input for system database
	if input.StartStep > uint(math.MaxInt) {
		return nil, fmt.Errorf("start step too large: %d", input.StartStep)
	}
	dbInput := forkWorkflowDBInput{
		originalWorkflowID: input.OriginalWorkflowID,
		forkedWorkflowID:   input.ForkedWorkflowID,
		startStep:          int(input.StartStep),
		applicationVersion: input.ApplicationVersion,
	}

	// Call system database method
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var forkedWorkflowID string
	var err error
	if isWithinWorkflow {
		forkedWorkflowID, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (string, error) {
			dbInput.tx = tx
			return c.systemDB.forkWorkflow(ctx, dbInput)
		}, WithStepName("DBOS.forkWorkflow"))
	} else {
		forkedWorkflowID, err = retryWithResult(c, func() (string, error) {
			return c.systemDB.forkWorkflow(c, dbInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	return newWorkflowPollingHandle[any](c, forkedWorkflowID), nil
}

// ForkWorkflow creates a new workflow instance by copying an existing workflow from a specific step.
// The forked workflow will have a new UUID and will execute from the specified StartStep.
// If StartStep > 0, the forked workflow will reuse the operation outputs from steps 0 to StartStep-1
// copied from the original workflow.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - input: Configuration parameters for the forked workflow
//
// Returns a typed workflow handle for the newly created forked workflow.
//
// Example usage:
//
//	// Basic fork from step 5
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    StartStep:          5,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Fork with custom workflow ID and application version
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    ForkedWorkflowID:   "my-custom-fork-id",
//	    StartStep:          3,
//	    ApplicationVersion: "v2.0.0",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func ForkWorkflow[R any](ctx DBOSContext, input ForkWorkflowInput) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	handle, err := ctx.ForkWorkflow(ctx, input)
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[R](ctx, handle.GetWorkflowID()), nil
}

// listWorkflowsOptions holds configuration parameters for listing workflows
type listWorkflowsOptions struct {
	workflowIDs      []string
	status           []WorkflowStatusType
	startTime        time.Time
	endTime          time.Time
	name             []string
	appVersion       []string
	user             []string
	limit            *int
	offset           *int
	sortDesc         bool
	workflowIDPrefix []string
	loadInput        bool
	loadOutput       bool
	queueName        []string
	queuesOnly       bool
	executorIDs      []string
	forkedFrom       []string
	parentWorkflowID []string
	deduplicationID  []string
}

// ListWorkflowsOption is a functional option for configuring workflow listing parameters.
type ListWorkflowsOption func(*listWorkflowsOptions)

// WithWorkflowIDs filters workflows by the specified workflow IDs.
func WithWorkflowIDs(workflowIDs []string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.workflowIDs = workflowIDs
	}
}

// WithStatus filters workflows by the specified list of statuses.
func WithStatus(status []WorkflowStatusType) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.status = status
	}
}

// WithStartTime filters workflows created after the specified time.
func WithStartTime(startTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.startTime = startTime
	}
}

// WithEndTime filters workflows created before the specified time.
func WithEndTime(endTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.endTime = endTime
	}
}

// WithName filters workflows by the specified workflow function name(s).
func WithName(name ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.name = name
	}
}

// WithAppVersion filters workflows by the specified application version(s).
func WithAppVersion(appVersion ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.appVersion = appVersion
	}
}

// WithUser filters workflows by the specified authenticated user(s).
func WithUser(user ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.user = user
	}
}

// WithLimit limits the number of workflows returned.
func WithLimit(limit int) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.limit = &limit
	}
}

// WithOffset sets the offset for pagination.
func WithOffset(offset int) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.offset = &offset
	}
}

// WithSortDesc enables descending sort by creation time (default is ascending).
func WithSortDesc() ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.sortDesc = true
	}
}

// WithWorkflowIDPrefix filters workflows by workflow ID prefix(es).
func WithWorkflowIDPrefix(prefix ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.workflowIDPrefix = prefix
	}
}

// WithLoadInput controls whether to load workflow input data (default: true).
func WithLoadInput(loadInput bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.loadInput = loadInput
	}
}

// WithLoadOutput controls whether to load workflow output data (default: true).
func WithLoadOutput(loadOutput bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.loadOutput = loadOutput
	}
}

// WithQueueName filters workflows by the specified queue name(s).
// This is typically used when listing queued workflows.
func WithQueueName(queueName ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.queueName = queueName
	}
}

// WithQueuesOnly filters to only return workflows that are in a queue.
func WithQueuesOnly() ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.queuesOnly = true
	}
}

// WithExecutorIDs filters workflows by the specified executor IDs.
func WithExecutorIDs(executorIDs []string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.executorIDs = executorIDs
	}
}

// WithForkedFrom filters workflows by the specified forked_from workflow ID(s).
func WithForkedFrom(forkedFrom ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.forkedFrom = forkedFrom
	}
}

// WithParentWorkflowID filters workflows by the specified parent workflow ID(s).
func WithParentWorkflowID(parentWorkflowID ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.parentWorkflowID = parentWorkflowID
	}
}

// WithFilterDeduplicationID filters workflows by the specified deduplication ID(s).
func WithFilterDeduplicationID(deduplicationID ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.deduplicationID = deduplicationID
	}
}

func (c *dbosContext) ListWorkflows(_ DBOSContext, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	// Initialize parameters with defaults
	loadInput := true
	loadOutput := true
	if !c.launched.Load() {
		loadInput = false
		loadOutput = false
	}
	params := &listWorkflowsOptions{
		loadInput:  loadInput,
		loadOutput: loadOutput,
	}

	// Apply all provided options
	for _, opt := range opts {
		opt(params)
	}

	// If we are asked to retrieve only queue workflows with no status, only fetch ENQUEUED and PENDING tasks
	if params.queuesOnly && len(params.status) == 0 {
		params.status = []WorkflowStatusType{WorkflowStatusEnqueued, WorkflowStatusPending}
	}

	// Convert to system database input structure
	dbInput := listWorkflowsDBInput{
		workflowIDs:        params.workflowIDs,
		status:             params.status,
		startTime:          params.startTime,
		endTime:            params.endTime,
		workflowName:       params.name,
		applicationVersion: params.appVersion,
		authenticatedUser:  params.user,
		limit:              params.limit,
		offset:             params.offset,
		sortDesc:           params.sortDesc,
		workflowIDPrefix:   params.workflowIDPrefix,
		loadInput:          params.loadInput,
		loadOutput:         params.loadOutput,
		queueName:          params.queueName,
		queuesOnly:         params.queuesOnly,
		executorIDs:        params.executorIDs,
		forkedFrom:         params.forkedFrom,
		parentWorkflowID:   params.parentWorkflowID,
		deduplicationID:    params.deduplicationID,
	}

	// Call the context method to list workflows
	var workflows []WorkflowStatus
	var err error
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		workflows, err = RunAsStep(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return retryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.systemDB.listWorkflows(ctx, dbInput)
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.listWorkflows"))
	} else {
		workflows, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.systemDB.listWorkflows(c, dbInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	// Deserialize Input and Output fields if they were loaded
	if params.loadInput || params.loadOutput {
		for i := range workflows {
			if params.loadInput && workflows[i].Input != nil {
				encodedInput, ok := workflows[i].Input.(*string)
				if !ok {
					return nil, fmt.Errorf("workflow input must be encoded string, got %T", workflows[i].Input)
				}
				if encodedInput == nil || *encodedInput == nilMarker {
					workflows[i].Input = nil
				} else {
					decodedBytes, err := base64.StdEncoding.DecodeString(*encodedInput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode base64 workflow input for %s: %w", workflows[i].ID, err)
					}
					workflows[i].Input = string(decodedBytes)
				}
			}
			if params.loadOutput && workflows[i].Output != nil {
				encodedOutput, ok := workflows[i].Output.(*string)
				if !ok {
					return nil, fmt.Errorf("workflow output must be encoded *string, got %T", workflows[i].Output)
				}
				if encodedOutput == nil || *encodedOutput == nilMarker {
					workflows[i].Output = nil
				} else {
					decodedBytes, err := base64.StdEncoding.DecodeString(*encodedOutput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode base64 workflow output for %s: %w", workflows[i].ID, err)
					}
					workflows[i].Output = string(decodedBytes)
				}
			}
		}
	}

	return workflows, nil
}

// ListWorkflows retrieves a list of workflows based on the provided filters.
//
// The function supports filtering by workflow IDs, status, time ranges, names, application versions,
// workflow ID prefixes, and more. It also supports pagination through
// limit/offset parameters and sorting control (ascending by default, or descending with WithSortDesc).
//
// By default, both input and output data are loaded for each workflow. This can be controlled
// using WithLoadInput(false) and WithLoadOutput(false) options for better performance when
// the data is not needed.
//
// Parameters:
//   - opts: Functional options to configure the query filters and parameters
//
// Returns a slice of WorkflowStatus structs containing the workflow information.
//
// Example usage:
//
//	// List all successful workflows from the last 24 hours
//	workflows, err := dbos.ListWorkflows(
//	    dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess}),
//	    dbos.WithStartTime(time.Now().Add(-24*time.Hour)),
//	    dbos.WithLimit(100))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows by specific IDs without loading input/output data
//	workflows, err := dbos.ListWorkflows(
//	    dbos.WithWorkflowIDs([]string{"workflow1", "workflow2"}),
//	    dbos.WithLoadInput(false),
//	    dbos.WithLoadOutput(false))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows with pagination
//	workflows, err := dbos.ListWorkflows(
//	    dbos.WithUser("john.doe"),
//	    dbos.WithOffset(50),
//	    dbos.WithLimit(25),
//	    dbos.WithSortDesc()
//	if err != nil {
//	    log.Fatal(err)
//	}
func ListWorkflows(ctx DBOSContext, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListWorkflows(ctx, opts...)
}

type StepInfo struct {
	StepID          int       // The sequential ID of the step within the workflow
	StepName        string    // The name of the step function
	Output          any       // The output returned by the step (if any)
	Error           error     // The error returned by the step (if any)
	ChildWorkflowID string    // The ID of a child workflow spawned by this step (if applicable)
	StartedAt       time.Time // When the step execution started
	CompletedAt     time.Time // When the step execution completed
}

func (c *dbosContext) GetWorkflowSteps(_ DBOSContext, workflowID string) ([]StepInfo, error) {
	var loadOutput bool
	if c.launched.Load() {
		loadOutput = true
	} else {
		loadOutput = false
	}
	getWorkflowStepsInput := getWorkflowStepsInput{
		workflowID: workflowID,
		loadOutput: loadOutput,
	}

	var steps []stepInfo
	var err error
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		steps, err = RunAsStep(c, func(ctx context.Context) ([]stepInfo, error) {
			return retryWithResult(ctx, func() ([]stepInfo, error) {
				return c.systemDB.getWorkflowSteps(ctx, getWorkflowStepsInput)
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.getWorkflowSteps"))
	} else {
		steps, err = retryWithResult(c, func() ([]stepInfo, error) {
			return c.systemDB.getWorkflowSteps(c, getWorkflowStepsInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}
	stepInfos := make([]StepInfo, len(steps))
	for i, step := range steps {
		stepInfos[i] = StepInfo{
			StepID:          step.StepID,
			StepName:        step.StepName,
			Error:           step.Error,
			ChildWorkflowID: step.ChildWorkflowID,
			StartedAt:       step.StartedAt,
			CompletedAt:     step.CompletedAt,
		}
	}

	// Deserialize outputs if asked to
	if loadOutput {
		for i := range steps {
			encodedOutput := steps[i].Output
			if encodedOutput == nil || *encodedOutput == nilMarker {
				stepInfos[i].Output = nil
				continue
			}
			decodedBytes, err := base64.StdEncoding.DecodeString(*encodedOutput)
			if err != nil {
				return nil, fmt.Errorf("failed to decode base64 step output for step %d: %w", steps[i].StepID, err)
			}
			stepInfos[i].Output = string(decodedBytes)
		}
	}

	return stepInfos, nil
}

// GetWorkflowSteps retrieves the execution steps of a workflow.
// Returns a list of step information including step IDs, names, outputs, errors, and child workflow IDs.
// The list is sorted by step ID in ascending order.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowID: The unique identifier of the workflow
//
// Returns a slice of StepInfo structs containing information about each executed step.
//
// Example:
//
//	steps, err := dbos.GetWorkflowSteps(ctx, "workflow-id")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, step := range steps {
//	    log.Printf("Step %d: %s", step.StepID, step.StepName)
//	}
func GetWorkflowSteps(ctx DBOSContext, workflowID string) ([]StepInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowSteps(ctx, workflowID)
}

// listRegisteredWorkflowsOptions holds configuration parameters for listing registered workflows
type listRegisteredWorkflowsOptions struct {
	scheduledOnly bool
}

// ListRegisteredWorkflowsOption is a functional option for configuring registered workflow listing parameters.
type ListRegisteredWorkflowsOption func(*listRegisteredWorkflowsOptions)

// WithScheduledOnly filters to only return scheduled workflows (those with a cron schedule).
func WithScheduledOnly() ListRegisteredWorkflowsOption {
	return func(p *listRegisteredWorkflowsOptions) {
		p.scheduledOnly = true
	}
}

// ListRegisteredWorkflows returns information about workflows registered with DBOS.
// Each WorkflowRegistryEntry contains:
// - MaxRetries: Maximum number of retry attempts for workflow recovery
// - Name: Custom name if provided during registration, otherwise empty
// - FQN: Fully qualified name of the workflow function (always present)
// - CronSchedule: Empty string for non-scheduled workflows
//
// The function supports filtering using functional options:
// - WithScheduledOnly(): Return only scheduled workflows
//
// Example:
//
//	// List all registered workflows
//	workflows, err := dbos.ListRegisteredWorkflows(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List only scheduled workflows
//	scheduled, err := dbos.ListRegisteredWorkflows(ctx, dbos.WithScheduledOnly())
//	if err != nil {
//	    log.Fatal(err)
//	}
func ListRegisteredWorkflows(ctx DBOSContext, opts ...ListRegisteredWorkflowsOption) ([]WorkflowRegistryEntry, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListRegisteredWorkflows(ctx, opts...)
}

// ListRegisteredQueues returns all registered workflow queues.
//
// Example:
//
//	queues := dbos.ListRegisteredQueues(ctx)
//	for _, queue := range queues {
//	    log.Printf("Queue: %s", queue.Name)
//	}
func ListRegisteredQueues(ctx DBOSContext) ([]WorkflowQueue, error) {
	if ctx == nil {
		return []WorkflowQueue{}, errors.New("ctx cannot be nil")
	}
	return ctx.ListRegisteredQueues(ctx)
}

func (c *dbosContext) ListenQueues(_ DBOSContext, queues ...WorkflowQueue) {
	if c.launched.Load() {
		panic("Cannot call ListenQueues after DBOS has launched")
	}

	// Set listen to true for each provided queue
	for _, queue := range queues {
		if registeredQueue, exists := c.queueRunner.workflowQueueRegistry[queue.Name]; exists {
			registeredQueue.listen = true
			c.queueRunner.workflowQueueRegistry[queue.Name] = registeredQueue
		} else {
			c.logger.Warn("Queue not found in registry when calling ListenQueues. Did you create it with NewWorkflowQueue?", "queue_name", queue.Name)
		}
	}
}

// ListenQueues configures which queues the current DBOS process should listen to.
// By default, all registered queues are listened to. When ListenQueues is called,
// only the specified queues (and the internal DBOS queue) will be listened to.
// This allows multiple DBOS processes to share the same queue registry but listen
// to different subsets of queues.
//
// ListenQueues can only be called before DBOS has been launched. Calling it after
// Launch will result in a panic.
//
// Example:
//
//	queue1 := dbos.NewWorkflowQueue(ctx, "queue-1")
//	queue2 := dbos.NewWorkflowQueue(ctx, "queue-2")
//	queue3 := dbos.NewWorkflowQueue(ctx, "queue-3")
//
//	// Only listen to queue1 and queue2
//	dbos.ListenQueues(ctx, queue1, queue2)
//
//	dbos.Launch(ctx)
func ListenQueues(ctx DBOSContext, queues ...WorkflowQueue) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	ctx.ListenQueues(ctx, queues...)
}

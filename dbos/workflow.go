package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/google/uuid"
)

/*******************************/
/******* WORKFLOW STATUS *******/
/*******************************/

// workflowState holds the runtime state for a workflow execution
type workflowState struct {
	workflowID          string
	stepID              int
	isWithinStep        bool
	isWithinTransaction bool
	isPortableWorkflow  bool
	authenticatedUser   string
	assumedRole         string
	authenticatedRoles  []string
	workflowCtx         context.Context
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
	needsDecoding bool   // true if result came from awaitWorkflowResult (ID conflict path) and needs decoding
	serialization string // serialization format of the encoded result (only used when needsDecoding is true)
	// cancelled reports that the workflow settled in CANCELLED. An awaiting parent
	// records it as an ErrorCodeAwaitedWorkflowCancelled outcome for its getResult step.
	cancelled bool
}

type stepCheckpointedOutcome struct {
	value         any    // The encoded value (should be a *string)
	serialization string // DB-stored serialization format
}

// rawStepOutput is returned by internal special steps (e.g. recv) whose output is
// already serialized. runAsTxn records value as-is under the given serialization
// name instead of re-encoding it with the workflow serializer.
type rawStepOutput struct {
	value         *string
	serialization string
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
	dbosContext Context
}

// GetResultOption is a functional option for configuring GetResult behavior.
type GetResultOption func(*getResultOptions)

// getResultOptions holds the configuration for GetResult execution.
type getResultOptions struct {
	timeout      time.Duration
	pollInterval time.Duration
}

func defaultGetResultOptions() *getResultOptions {
	return &getResultOptions{pollInterval: sysdb.DBRetryInterval}
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
// If the Context is running in client mode, do not load input and outputs
func (h *baseWorkflowHandle) GetStatus() (WorkflowStatus, error) {
	loadInput := false
	loadOutput := false
	if h.dbosContext.(*dbosContext).launched.Load() {
		loadInput = true
		loadOutput = true
	}
	c := h.dbosContext.(*dbosContext)
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var workflowStatuses []WorkflowStatus
	var err error
	if isWithinWorkflow {
		// Decode inside the step so the checkpoint records decoded values: a
		// recovery replay returns the recorded output as-is, and the raw
		// encoded *string columns do not survive checkpoint serialization.
		workflowStatuses, err = RunAsStep(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			statuses, err := sysdb.RetryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
					WorkflowIDs: []string{h.workflowID},
					LoadInput:   loadInput,
					LoadOutput:  loadOutput,
				})
			}, sysdb.WithRetrierLogger(c.logger))
			if err != nil {
				return nil, err
			}
			if err := c.decodeWorkflowsInputOutput(statuses, loadInput, loadOutput); err != nil {
				return nil, err
			}
			return statuses, nil
		}, WithStepName("DBOS.getStatus"))
	} else {
		workflowStatuses, err = sysdb.RetryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.systemDB.ListWorkflows(c, sysdb.ListWorkflowsDBInput{
				WorkflowIDs: []string{h.workflowID},
				LoadInput:   loadInput,
				LoadOutput:  loadOutput,
			})
		})
		if err == nil {
			err = c.decodeWorkflowsInputOutput(workflowStatuses, loadInput, loadOutput)
		}
	}
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, models.NewNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *baseWorkflowHandle) GetWorkflowID() string {
	return h.workflowID
}

func newWorkflowHandle[R any](ctx Context, workflowID string, outcomeChan chan workflowOutcome[R]) *workflowHandle[R] {
	return &workflowHandle[R]{
		baseWorkflowHandle: baseWorkflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
		outcomeChan: outcomeChan,
	}
}

func newWorkflowPollingHandle[R any](ctx Context, workflowID string) *workflowPollingHandle[R] {
	return &workflowPollingHandle[R]{
		baseWorkflowHandle: baseWorkflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
	}
}

// checkGetResultExecution checks if GetResult was already executed as a step within a workflow.
// Returns (result, found, err). Callers that need workflowState should retrieve it separately.
func checkGetResultExecution[R any](dbosCtx context.Context) (R, bool, error) {
	workflowState, ok := dbosCtx.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if !isWithinWorkflow {
		return *new(R), false, nil
	}
	recordedOutputs, err := sysdb.RetryWithResult(dbosCtx, func() (*sysdb.RecordedResult, error) {
		uncancellableCtx := context.WithoutCancel(dbosCtx)
		return dbosCtx.(*dbosContext).systemDB.CheckOperationExecution(uncancellableCtx, sysdb.CheckOperationExecutionDBInput{
			WorkflowID: workflowState.workflowID,
			StepID:     workflowState.stepID + 1,
			StepName:   "DBOS.getResult",
		})
	}, sysdb.WithRetrierLogger(dbosCtx.(*dbosContext).logger))
	if err != nil {
		return *new(R), false, models.NewStepExecutionError(workflowState.workflowID, "DBOS.getResult", fmt.Errorf("checking operation execution: %w", err))
	}
	if recordedOutputs != nil {
		workflowState.nextStepID()
		var decodedOutput R
		if recordedOutputs.Output != nil {
			decoder, err := resolveDecoder[R](recordedOutputs.Serialization, dbosCtx.(*dbosContext).serializer)
			if err != nil {
				return *new(R), false, fmt.Errorf("failed to resolve decoder: %w", err)
			}
			decodedOutput, err = decoder.Decode(recordedOutputs.Output)
			if err != nil {
				return *new(R), false, fmt.Errorf("failed to decode operation result: %w", err)
			}
		}
		return decodedOutput, true, deserializeWorkflowError(recordedOutputs.ErrStr)
	}
	return *new(R), false, nil
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

	// If within a workflow, check if we already ran that step
	result, found, err := checkGetResultExecution[R](h.dbosContext)
	if found {
		return result, err
	}
	if err != nil { // not found and err means err is an infrastructure error
		return *new(R), err
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
		return *new(R), models.NewTimeoutError(h.workflowID, "", fmt.Sprintf("workflow result timeout after %v", options.timeout), context.DeadlineExceeded)
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
			return *new(R), models.NewWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("invalid Context: expected *dbosContext"))
		}
		// A cancellation outcome delivered while the awaiting workflow is itself
		// cancelled interrupts the getResult step: don't checkpoint it, so resume
		// re-executes the await.
		if stepInterruptedByCancellation(workflowState, outcome.err) {
			return *new(R), models.NewWorkflowCancelledError(workflowState.workflowID, outcome.err)
		}
		// A cancelled child is a terminal outcome for the awaiting parent: checkpoint
		// it like any other child error so replay is deterministic.
		// Resuming the child later does not change what the parent saw.
		if outcome.cancelled {
			decodedResult = *new(R)
			outcome.err = models.NewAwaitedWorkflowCancelledError(h.workflowID)
		}
		ser := resolveEncoder(h.dbosContext)
		encodedOutput, encErr := ser.Encode(decodedResult)
		if encErr != nil {
			return *new(R), models.NewWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("serializing child workflow result: %w", encErr))
		}
		var serializedOutcomeErr *string
		if outcome.err != nil {
			s := serializeWorkflowError(h.dbosContext.(*dbosContext).logger, outcome.err, ser.Name())
			serializedOutcomeErr = &s
		}
		recordGetResultInput := sysdb.RecordOperationResultDBInput{
			WorkflowID:      workflowState.workflowID,
			ChildWorkflowID: h.workflowID,
			StepID:          workflowState.nextStepID(),
			Output:          encodedOutput,
			ErrStr:          serializedOutcomeErr,
			StartedAt:       startTime,
			CompletedAt:     completedTime,
			StepName:        "DBOS.getResult",
			Serialization:   ser.Name(),
		}
		uncancellableCtx := context.WithoutCancel(h.dbosContext)
		recordResultErr := sysdb.Retry(h.dbosContext, func() error {
			return h.dbosContext.(*dbosContext).systemDB.RecordOperationResult(uncancellableCtx, recordGetResultInput)
		}, sysdb.WithRetrierLogger(h.dbosContext.(*dbosContext).logger))
		if recordResultErr != nil {
			h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
			return *new(R), models.NewWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("recording child workflow result: %w", recordResultErr))
		}
	} else if outcome.cancelled && !isCancellationError(outcome.err) {
		// The workflow swallowed its cancellation (returned normally or with an
		// unrelated error) but we triggered durable cancel and no output was
		// recorded: report cancellation, not success.
		return *new(R), models.NewWorkflowCancelledError(h.workflowID, outcome.err)
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

	// If within a workflow, check if we already ran that step
	result, found, err := checkGetResultExecution[R](h.dbosContext)
	if found {
		return result, err
	}
	if err != nil {
		return *new(R), err
	}
	startTime := time.Now()

	// Use timeout if specified, otherwise use DBOS context directly
	ctx := h.dbosContext
	var cancel context.CancelFunc
	if options.timeout > 0 {
		ctx, cancel = WithTimeout(h.dbosContext, options.timeout)
		defer cancel()
	}

	awaitResult, awaitErr := sysdb.RetryWithResult(ctx, func() (*sysdb.AwaitWorkflowResultOutput, error) {
		return h.dbosContext.(*dbosContext).systemDB.AwaitWorkflowResult(ctx, h.workflowID, options.pollInterval)
	}, sysdb.WithRetrierLogger(h.dbosContext.(*dbosContext).logger))

	completedTime := time.Now()

	// awaitErr is a real DB/network/cancellation error; the workflow's recorded error is in awaitResult.errStr
	err = awaitErr
	if awaitErr == nil && awaitResult.ErrStr != nil {
		err = deserializeWorkflowError(awaitResult.ErrStr)
	}

	workflowState, ok := h.dbosContext.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	// A cancellation outcome delivered while the awaiting workflow is itself
	// cancelled interrupts the getResult step: don't checkpoint it, so resume
	// re-executes the await.
	if isWithinWorkflow && stepInterruptedByCancellation(workflowState, err) {
		return *new(R), models.NewWorkflowCancelledError(workflowState.workflowID, err)
	}

	// A cancelled child is a terminal outcome for the awaiting parent: checkpoint
	// it like any other child error so replay is deterministic.
	// Resuming the child later does not change what the parent saw.
	childCancelled := errors.Is(awaitErr, ErrAwaitedWorkflowCancelled)

	// Deserialize the result directly into the target type
	var typedResult R
	var encodedStr *string
	var storedSerialization string
	if awaitResult != nil {
		encodedStr = awaitResult.Output
		storedSerialization = awaitResult.Serialization
	}
	if encodedStr != nil {
		var deserErr error
		decoder, deserErr := resolveDecoder[R](storedSerialization, h.dbosContext.(*dbosContext).serializer)
		if deserErr != nil {
			return *new(R), fmt.Errorf("failed to resolve decoder: %w", deserErr)
		}
		typedResult, deserErr = decoder.Decode(encodedStr)
		if deserErr != nil {
			return *new(R), fmt.Errorf("failed to deserialize workflow result: %w", deserErr)
		}
	}

	// If we are calling GetResult inside a workflow, record the outcome as a step
	// result: either the workflow result proper (no dlq, no raw awaitWorkflowResult
	// error) or the child's cancellation.
	if isWithinWorkflow && (childCancelled || (awaitErr == nil && encodedStr != nil)) {
		errStr := awaitResult.ErrStr
		serialization := storedSerialization
		if childCancelled {
			serialization = resolveEncoder(h.dbosContext).Name()
			serializedErr := serializeWorkflowError(h.dbosContext.(*dbosContext).logger, awaitErr, serialization)
			errStr = &serializedErr
		}
		recordGetResultInput := sysdb.RecordOperationResultDBInput{
			WorkflowID:      workflowState.workflowID,
			ChildWorkflowID: h.workflowID,
			StepID:          workflowState.nextStepID(),
			Output:          encodedStr,
			ErrStr:          errStr,
			StartedAt:       startTime,
			CompletedAt:     completedTime,
			StepName:        "DBOS.getResult",
			Serialization:   serialization,
		}
		uncancellableCtx := context.WithoutCancel(h.dbosContext)
		recordResultErr := sysdb.Retry(h.dbosContext, func() error {
			return h.dbosContext.(*dbosContext).systemDB.RecordOperationResult(uncancellableCtx, recordGetResultInput)
		}, sysdb.WithRetrierLogger(h.dbosContext.(*dbosContext).logger))
		if recordResultErr != nil {
			h.dbosContext.(*dbosContext).logger.Error("failed to record get result", "error", recordResultErr)
			return *new(R), models.NewWorkflowExecutionError(workflowState.workflowID, fmt.Errorf("recording child workflow result: %w", recordResultErr))
		}
	}
	return typedResult, err
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

// typedHandle adapts an untyped handle returned by a Client interface method
// into a WorkflowHandle[R]. Real DBOS contexts get a typed polling handle that
// decodes results from the database; mocked Clients get a proxy that
// type-asserts the wrapped handle's results.
func typedHandle[R any](c Client, handle WorkflowHandle[any]) WorkflowHandle[R] {
	if ctx, ok := c.(*dbosContext); ok {
		return newWorkflowPollingHandle[R](ctx, handle.GetWorkflowID())
	}
	return &workflowHandleProxy[R]{wrappedHandle: handle}
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
type wrappedWorkflowFunc func(ctx Context, input any, inputSerialization string, opts ...WorkflowOption) (WorkflowHandle[any], error)

type WorkflowRegistryEntry struct {
	wrappedFunction wrappedWorkflowFunc
	workflowFn      WorkflowFunc // Type-erased registered function taking a raw (non-encoded) input. Used by RunWorkflow for direct execution.
	MaxRetries      int          // Maximum recovery attempts before dead-lettering (set via WithMaxRecoveryAttempts); not step retries
	Name            string
	FQN             string // Fully qualified name of the workflow function. For configured instances, qualified with the config name.
	ClassName       string // Receiver type name for configured instance workflows
	ConfigName      string // Config name for configured instance workflows
}

func registerWorkflow(ctx Context, entry WorkflowRegistryEntry) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register workflow after DBOS has launched")
	}

	// Check if workflow already exists and store atomically using LoadOrStore
	if _, exists := c.workflowRegistry.LoadOrStore(entry.FQN, entry); exists {
		c.logger.Error("workflow function already registered", "fqn", entry.FQN)
		panic(models.NewConflictingRegistrationError(entry.FQN))
	}

	// We need to get a mapping from custom name to FQN for registry lookups that might not know the FQN (queue, recovery)
	// We also panic if we found the name was already registered (this could happen if registering two different workflows under the same custom name)
	// Configured instance workflows are keyed by name + "/" + config name, matching the lookup key
	// queue and recovery rebuild from the recorded (name, config_name) pair. The same workflow
	// name can thus be shared by many instances, like in the other Transact SDKs.
	if len(entry.Name) > 0 {
		lookupName := entry.Name
		if len(entry.ConfigName) > 0 {
			lookupName = instanceQualifiedName(entry.Name, entry.ConfigName)
		}
		if _, exists := c.workflowCustomNametoFQN.LoadOrStore(lookupName, entry.FQN); exists {
			c.logger.Error("workflow function already registered", "custom_name", lookupName)
			panic(models.NewConflictingRegistrationError(lookupName))
		}
	} else {
		c.workflowCustomNametoFQN.Store(entry.FQN, entry.FQN) // Store the FQN as the custom name if none was provided
	}
}

// ConfiguredInstance is implemented by objects whose methods are registered as workflows.
// ConfigName must return a stable, unique name for the instance: it disambiguates method
// values bound to different receivers (which share a function name) and is durably recorded
// so recovery runs the workflow on the correct instance. Instances must be registered with
// the same config name on every process start, before Launch.
type ConfiguredInstance interface {
	ConfigName() string
}

// instanceQualifiedName returns the per-instance registry key for a workflow method.
func instanceQualifiedName(name, configName string) string {
	return name + "/" + configName
}

type workflowRegistrationOptions struct {
	maxRetries int
	name       string
	instance   ConfiguredInstance
}

type WorkflowRegistrationOption func(*workflowRegistrationOptions)

const (
	_DEFAULT_MAX_RECOVERY_ATTEMPTS = 100

	// Step retry defaults
	_DEFAULT_STEP_BASE_INTERVAL  = 100 * time.Millisecond
	_DEFAULT_STEP_MAX_INTERVAL   = 5 * time.Second
	_DEFAULT_STEP_BACKOFF_FACTOR = 2.0
)

// WithMaxRecoveryAttempts sets the maximum number of times an interrupted workflow
// is recovered (re-executed after a crash or restart). After exceeding this limit,
// the workflow status becomes MAX_RECOVERY_ATTEMPTS_EXCEEDED. This is unrelated to
// step retries; see WithStepMaxRetries for those.
func WithMaxRecoveryAttempts(maxRecoveryAttempts int) WorkflowRegistrationOption {
	return func(p *workflowRegistrationOptions) {
		p.maxRetries = maxRecoveryAttempts
	}
}

// WithWorkflowName registers the workflow under a custom name instead of its
// fully qualified function name. The custom name is what workflow status
// records show and what by-name dispatch (e.g. Client.Enqueue) resolves.
func WithWorkflowName(name string) WorkflowRegistrationOption {
	return func(p *workflowRegistrationOptions) {
		p.name = name
	}
}

// WithInstance registers a workflow method bound to a specific configured instance.
// Method values bound to different receivers (e.g. a.Run and b.Run) share a function
// name, so each instance's method must be registered under a per-instance key:
//
//	dbos.RegisterWorkflow(ctx, slack.Send, dbos.WithInstance(slack))
//	dbos.RegisterWorkflow(ctx, email.Send, dbos.WithInstance(email))
//
// Run the workflow with the matching dbos.WithRunInstance option.
func WithInstance(instance ConfiguredInstance) WorkflowRegistrationOption {
	return func(p *workflowRegistrationOptions) {
		p.instance = instance
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
// Workflows are identified by a name derived from the function's code pointer, so each
// registered function value must have a unique name. Registrable:
//   - Top-level named functions: the recommended form. Each has a unique name.
//   - Generic function instantiations: type parameters are automatically appended to the name,
//     so distinct instantiations are distinct workflows.
//   - Method values bound to a configured instance (e.g. inst.Run), registered with
//     WithInstance: the instance's config name qualifies the workflow name, so each
//     instance registers its own workflow. Run these with WithRunInstance.
//   - A closure or method value, at most ONE per source expression: all values built
//     from the same func literal or method (e.g. a.Run and b.Run, or closures from one
//     factory) share a name. Registering a second one panics with
//     ErrorCodeConflictingRegistration; use WithInstance (methods) or distinct top-level
//     functions (closures) instead.
//
// Registration options include:
//   - WithMaxRecoveryAttempts: Set maximum recovery attempts for the workflow
//   - WithWorkflowName: Set a custom name for the workflow
//   - WithInstance: Register a method bound to a named instance
//
// Example:
//
//	func MyWorkflow(ctx dbos.Context, input string) (int, error) {
//	    // workflow implementation
//	    return len(input), nil
//	}
//
//	dbos.RegisterWorkflow(ctx, MyWorkflow)
//
//	// With options:
//	dbos.RegisterWorkflow(ctx, MyWorkflow,
//	    dbos.WithMaxRecoveryAttempts(5),
//	    dbos.WithWorkflowName("MyCustomWorkflowName"))
func RegisterWorkflow[P any, R any](ctx Context, fn Workflow[P, R], opts ...WorkflowRegistrationOption) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}

	registrationParams := workflowRegistrationOptions{
		maxRetries: _DEFAULT_MAX_RECOVERY_ATTEMPTS,
	}

	for _, opt := range opts {
		opt(&registrationParams)
	}

	fqn := resolveWorkflowFunctionName(fn)

	// Method values bound to different receivers share an FQN: qualify the registry key
	// with the instance config name so each instance registers its own entry. The recorded
	// workflow name stays unqualified; the config name is durably recorded alongside it.
	var className, configName string
	if registrationParams.instance != nil {
		configName = registrationParams.instance.ConfigName()
		if configName == "" {
			panic(fmt.Sprintf("configured instance for workflow %s must have a non-empty config name", fqn))
		}
		className = reflect.Indirect(reflect.ValueOf(registrationParams.instance)).Type().Name()
		if registrationParams.name == "" {
			registrationParams.name = fqn
		}
		fqn = instanceQualifiedName(fqn, configName)
	}

	// Register a type-erased version of the durable workflow for recovery and queue runner
	// Input will always come, encoded, from the database, so we decode it into the target type (captured by this wrapped closure)
	// inputSerialization is the DB-stored serialization format for the encoded input.
	typedErasedWorkflow := func(ctx Context, input any, inputSerialization string) (any, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return *new(R), models.NewWorkflowExecutionError("", fmt.Errorf("getting workflow ID: %w", err))
		}
		encodedInput, ok := input.(*string)
		if !ok {
			return *new(R), models.NewWorkflowUnexpectedInputType(fqn, "*string (encoded)", fmt.Sprintf("%T", input))
		}
		var typedInput P
		if inputSerialization == PortableSerializerName {
			typedInput, err = decodePortableArgs[P](encodedInput)
		} else {
			inputDecoder, resolveErr := resolveDecoder[P](inputSerialization, getCustomSerializerFromCtx(ctx))
			if resolveErr != nil {
				return *new(R), models.NewWorkflowExecutionError(workflowID, resolveErr)
			}
			typedInput, err = inputDecoder.Decode(encodedInput)
		}
		if err != nil {
			return *new(R), models.NewWorkflowExecutionError(workflowID, err)
		}
		return fn(ctx, typedInput)
	}

	typeErasedWrapper := wrappedWorkflowFunc(func(ctx Context, input any, inputSerialization string, opts ...WorkflowOption) (WorkflowHandle[any], error) {
		wfFunc := WorkflowFunc(func(ctx Context, input any) (any, error) {
			return typedErasedWorkflow(ctx, input, inputSerialization)
		})
		opts = append(opts, withWorkflowName(fqn), withAlreadyEncodedInput()) // Append the name so ctx.RunWorkflow can look it up from the registry to apply registration-time options
		if inputSerialization == PortableSerializerName {
			opts = append(opts, WithPortableWorkflow())
		}
		handle, err := ctx.RunWorkflow(ctx, wfFunc, input, opts...)
		if err != nil {
			return nil, err
		}
		return newWorkflowPollingHandle[any](ctx, handle.GetWorkflowID()), nil // this is only used by recovery -- the queue runner dismisses it
	})

	// Wrapper for direct calls in RunWorkflow
	registeredWorkflow := WorkflowFunc(func(ctx Context, input any) (any, error) {
		typedInput, ok := input.(P)
		if !ok {
			return nil, models.NewWorkflowUnexpectedInputType(fqn, fmt.Sprintf("%T", *new(P)), fmt.Sprintf("%T", input))
		}
		return fn(ctx, typedInput)
	})

	registerWorkflow(ctx, WorkflowRegistryEntry{
		wrappedFunction: typeErasedWrapper,
		workflowFn:      registeredWorkflow,
		FQN:             fqn,
		MaxRetries:      registrationParams.maxRetries,
		Name:            registrationParams.name,
		ClassName:       className,
		ConfigName:      configName,
	})

}

// resolveWorkflowName returns either the FQN or the custom name of a function, if present in the workflow registry
func (c *dbosContext) resolveWorkflowName(workflowFn any) (string, error) {
	if workflowFn == nil {
		return "", errors.New("workflow function is required")
	}
	fqn := runtime.FuncForPC(reflect.ValueOf(workflowFn).Pointer()).Name()
	value, ok := c.workflowRegistry.Load(fqn)
	if !ok {
		return "", fmt.Errorf("workflow function not registered: %s (note: configured instances are not supported with scheduled workflows)", fqn)
	}
	entry := value.(WorkflowRegistryEntry)
	if entry.Name != "" {
		return entry.Name, nil
	}
	return entry.FQN, nil
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type dbosContextKey string

const workflowStateKey dbosContextKey = "workflowState"

// Workflow represents a type-safe workflow function with specific input and output types.
// P is the input parameter type and R is the return type.
// All workflow functions must accept a Context as their first parameter.
type Workflow[P any, R any] func(ctx Context, input P) (R, error)

// WorkflowFunc represents a type-erased workflow function used internally.
type WorkflowFunc func(ctx Context, input any) (any, error)

type activeWorkflowEntry struct {
	queueName         string
	queuePartitionKey string
}

func (c *dbosContext) countActiveWorkflowsForQueue(queueName, queuePartitionKey string) int {
	if c.activeWorkflowIDs == nil {
		return 0
	}
	count := 0
	c.activeWorkflowIDs.Range(func(_, value any) bool {
		if entry, ok := value.(activeWorkflowEntry); ok {
			if entry.queueName == queueName && entry.queuePartitionKey == queuePartitionKey {
				count++
			}
		}
		return true
	})
	return count
}

// DeduplicationPolicy controls how a colliding deduplication ID on the same queue is handled.
type DeduplicationPolicy int

const (
	// DeduplicationPolicyReject (default) rejects the enqueue with an error matching
	// ErrQueueDeduplicated (via errors.Is) if another workflow already holds the
	// deduplication ID on the queue.
	DeduplicationPolicyReject DeduplicationPolicy = iota
	// DeduplicationPolicyReturnExisting returns a handle to the existing workflow instead of an
	// error.
	DeduplicationPolicyReturnExisting
)

type workflowOptions struct {
	WorkflowName        string
	WorkflowID          string
	QueueName           string
	queue               Queue
	ApplicationVersion  string
	MaxRetries          int
	DeduplicationID     string
	DeduplicationPolicy DeduplicationPolicy
	Priority            uint
	AuthenticatedUser   string
	AssumedRole         string
	AuthenticatedRoles  []string
	QueuePartitionKey   string
	DelayDuration       time.Duration
	WorkflowAttributes  map[string]any
	alreadyEncodedInput bool
	isDequeue           bool
	isRecovery          bool
	isPortableWorkflow  bool
	runInstance         ConfiguredInstance
	err                 error // invalid option usage, surfaced when options are parsed
}

// WorkflowOption is a functional option for configuring workflow execution parameters.
type WorkflowOption func(*workflowOptions)

// WithWorkflowID sets a custom workflow ID instead of generating one automatically.
func WithWorkflowID(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowID = id
	}
}

// WithRunInstance runs a workflow method registered with dbos.WithInstance. The instance's
// config name selects the per-instance registration, so the workflow executes on (and
// recovers to) the correct instance:
//
//	handle, err := dbos.RunWorkflow(ctx, slack.Send, input, dbos.WithRunInstance(slack))
func WithRunInstance(instance ConfiguredInstance) WorkflowOption {
	return func(p *workflowOptions) {
		p.runInstance = instance
	}
}

// WithQueue enqueues the workflow to the given queue instead of executing immediately.
// Queued workflows will be processed by the queue runner according to the queue's configuration.
// The queue must be a non-nil handle from [RegisterQueue], [RetrieveQueue], or [ListQueues];
// passing nil makes the enclosing call return an error.
// To enqueue by name, use [Enqueue].
func WithQueue(queue Queue) WorkflowOption {
	return func(p *workflowOptions) {
		if queue == nil {
			p.err = errors.Join(p.err, models.NewInvalidOptionError("WithQueue: queue cannot be nil"))
			return
		}
		p.queue = queue
		p.QueueName = queue.GetName()
	}
}

// withQueueName enqueues the workflow to a queue identified by name only.
// It is a no-op if a queue handle was already set with WithQueue: an explicit
// user choice always wins over an internally-supplied name.
// XXX: this will be removed when we update the debouncer implementation.
func withQueueName(queueName string) WorkflowOption {
	return func(p *workflowOptions) {
		if p.queue != nil {
			return
		}
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

// WithDeduplicationPolicy sets how a colliding deduplication ID is handled for a queue workflow.
// DeduplicationPolicyReturnExisting requires both a queue (WithQueue) and a deduplication ID
// (WithDeduplicationID).
func WithDeduplicationPolicy(policy DeduplicationPolicy) WorkflowOption {
	return func(p *workflowOptions) {
		p.DeduplicationPolicy = policy
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

// WithWorkflowAttributes attaches custom key-value attributes to the workflow.
// Attributes are recorded in the workflow status at creation, must be
// JSON-serializable, and are not inherited by child workflows. On Postgres they
// are stored as GIN-indexed JSONB and can be searched with WithFilterAttributes.
func WithWorkflowAttributes(attributes map[string]any) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowAttributes = attributes
	}
}

// WithDelay delays execution of a queued workflow by the specified duration.
// The workflow starts in the DELAYED status and transitions to ENQUEUED after the delay expires.
// Must be used together with WithQueue.
func WithDelay(delay time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.DelayDuration = delay
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

// WithPortableWorkflow marks the workflow to use the cross-language portable JSON format
// for all serialized data (inputs, step outputs, events, messages, streams).
// This is set automatically during dequeue/recovery for workflows stored with portable serialization.
func WithPortableWorkflow() WorkflowOption {
	return func(p *workflowOptions) {
		p.isPortableWorkflow = true
	}
}

// WithAuthenticatedUser sets the authenticated user recorded on the workflow.
func WithAuthenticatedUser(user string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedUser = user
	}
}

// WithAssumedRole sets the assumed role recorded on the workflow.
func WithAssumedRole(role string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AssumedRole = role
	}
}

// WithAuthenticatedRoles sets the authenticated roles for the workflow.
func WithAuthenticatedRoles(roles ...string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedRoles = roles
	}
}

// RunWorkflow executes a workflow function with type safety and durability guarantees.
// The workflow can be executed immediately or enqueued for later execution based on options.
// Returns a typed handle that can be used to wait for completion and retrieve results.
//
// The context must have been launched with Launch; calling RunWorkflow before
// Launch returns an initialization error.
//
// The workflow will be automatically recovered if the process crashes or is interrupted.
// All workflow state is persisted to ensure exactly-once execution semantics.
//
// Workflow IDs are idempotency keys. If WithWorkflowID supplies the ID of a
// workflow that already completed, RunWorkflow does not re-execute it: it
// returns a handle to the recorded execution and the recorded result, and the
// new input is ignored. To re-execute with the same ID, use ForkWorkflow.
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
func RunWorkflow[P any, R any](ctx Context, fn Workflow[P, R], input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, fmt.Errorf("ctx cannot be nil")
	}

	fqn := resolveWorkflowFunctionName(fn)

	// If a configured instance was provided, qualify the name with its config name to
	// select the per-instance registration (see WithInstance).
	var providedOpts workflowOptions
	for _, opt := range opts {
		opt(&providedOpts)
	}
	if providedOpts.runInstance != nil {
		fqn = instanceQualifiedName(fqn, providedOpts.runInstance.ConfigName())
	}

	// Add the fn name to the options so we can communicate it with Context.RunWorkflow
	opts = append(opts, withWorkflowName(fqn))

	// Execute the registered function (fallback on provided function for mocked contexts)
	typedErasedWorkflow := WorkflowFunc(func(ctx Context, input any) (any, error) {
		return fn(ctx, input.(P))
	})
	if c, ok := ctx.(*dbosContext); ok {
		if entryAny, exists := c.workflowRegistry.Load(fqn); exists {
			if entry, ok := entryAny.(WorkflowRegistryEntry); ok && entry.workflowFn != nil {
				typedErasedWorkflow = entry.workflowFn
			}
		}
	}

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
					result:    typedResult,
					err:       resultErr,
					cancelled: outcome.cancelled,
				}
				return
			}

			// Check if this is a mocked path
			if _, ok := handle.dbosContext.(*dbosContext); !ok {
				typedOutcomeChan <- workflowOutcome[R]{
					result:    outcome.result.(R),
					err:       resultErr,
					cancelled: outcome.cancelled,
				}
				return
			}

			// Convert result to expected type R
			// Result can be either an encoded *string (from ID conflict path) or already decoded
			if outcome.needsDecoding {
				encodedResult, ok := outcome.result.(*string)
				if !ok { // Should never happen
					resultErr = errors.Join(resultErr, models.NewWorkflowUnexpectedResultType(handle.workflowID, "string (encoded)", fmt.Sprintf("%T", outcome.result)))
				} else {
					// Result is encoded, decode directly into target type
					resultDecoder, resolveErr := resolveDecoder[R](outcome.serialization, getCustomSerializerFromCtx(ctx))
					if resolveErr != nil {
						resultErr = errors.Join(resultErr, models.NewWorkflowExecutionError(handle.workflowID, resolveErr))
					} else if decoded, decodeErr := resultDecoder.Decode(encodedResult); decodeErr != nil {
						resultErr = errors.Join(resultErr, models.NewWorkflowExecutionError(handle.workflowID, fmt.Errorf("decoding workflow result to type %T: %w", *new(R), decodeErr)))
					} else {
						typedResult = decoded
					}
				}
			} else if typedRes, ok := outcome.result.(R); ok {
				// Normal path - result already has the correct type
				typedResult = typedRes
			} else {
				// Type assertion failed
				typeErr := models.NewWorkflowUnexpectedResultType(handle.workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", outcome.result))
				resultErr = errors.Join(resultErr, typeErr)
			}

			typedOutcomeChan <- workflowOutcome[R]{
				result:    typedResult,
				err:       resultErr,
				cancelled: outcome.cancelled,
			}
		}()

		typedHandle := newWorkflowHandle(handle.dbosContext, handle.workflowID, typedOutcomeChan)

		return typedHandle, nil
	}

	// Usually on a mocked path
	return &workflowHandleProxy[R]{wrappedHandle: handle}, nil
}

func (c *dbosContext) RunWorkflow(_ Context, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) {
	// Apply options to build params
	params := workflowOptions{
		ApplicationVersion: c.GetApplicationVersion(),
	}
	for _, opt := range opts {
		opt(&params)
	}
	if params.err != nil {
		c.logger.Error("invalid workflow options", "workflow_name", params.WorkflowName, "error", params.err)
		return nil, params.err
	}

	// Lookup the registry for registration-time options
	registeredWorkflowAny, exists := c.workflowRegistry.Load(params.WorkflowName)
	if !exists {
		c.logger.Error("workflow not found in registry", "workflow_name", params.WorkflowName)
		return nil, models.NewNonExistentWorkflowError(params.WorkflowName)
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

	// Validate delay is not provided without queue name
	if params.DelayDuration > 0 && len(params.QueueName) == 0 {
		c.logger.Error("delay provided but queue name is missing", "workflow_name", params.WorkflowName)
		return nil, models.NewInvalidOptionError("delay provided but queue name is missing")
	}

	// Validate partition key is not provided without queue name
	if len(params.QueuePartitionKey) > 0 && len(params.QueueName) == 0 {
		c.logger.Error("partition key provided but queue name is missing", "workflow_name", params.WorkflowName)
		return nil, models.NewInvalidOptionError("partition key provided but queue name is missing")
	}

	// Validate partition key and deduplication ID are not both provided (they are incompatible)
	if len(params.QueuePartitionKey) > 0 && len(params.DeduplicationID) > 0 {
		c.logger.Error("partition key and deduplication ID cannot be used together", "workflow_name", params.WorkflowName)
		return nil, models.NewInvalidOptionError("partition key and deduplication ID cannot be used together")
	}

	// A non-default deduplication policy only applies to a queued workflow with a deduplication ID
	if params.DeduplicationPolicy != DeduplicationPolicyReject {
		if len(params.DeduplicationID) == 0 {
			return nil, models.NewInvalidOptionError("a deduplication policy requires a deduplication ID")
		}
		if len(params.QueueName) == 0 {
			return nil, models.NewInvalidOptionError("a deduplication policy requires a queue name")
		}
	}

	// Validate partitioned-queue usage. Prefer the handle provided via WithQueue
	// (configuration as of registration/retrieval time); for name-only enqueues,
	// fall back to the queue runner's latest reconciled snapshot. If neither knows
	// the queue (e.g. before launch, or a queue this process does not listen to),
	// skip validation and let the queue runner resolve the name.
	var partitionQueue, queueKnown bool
	if params.queue != nil {
		partitionQueue, queueKnown = params.queue.GetPartitionQueue(), true
	} else if len(params.QueueName) > 0 {
		if q, ok := c.queueRunner.currentQueueConfig(params.QueueName); ok {
			partitionQueue, queueKnown = q.PartitionQueue, true
		}
	}
	if queueKnown {
		// If queue has partitions enabled, partition key must be provided
		if partitionQueue && len(params.QueuePartitionKey) == 0 {
			c.logger.Error("queue has partitions enabled but no partition key was provided", "workflow_name", params.WorkflowName, "queue_name", params.QueueName)
			return nil, models.NewInvalidOptionError(fmt.Sprintf("queue %s has partitions enabled, but no partition key was provided", params.QueueName))
		}
		// If partition key is provided, queue must have partitions enabled
		if len(params.QueuePartitionKey) > 0 && !partitionQueue {
			c.logger.Error("queue is not a partitioned queue but a partition key was provided", "workflow_name", params.WorkflowName, "queue_name", params.QueueName)
			return nil, models.NewInvalidOptionError(fmt.Sprintf("queue %s is not a partitioned queue, but a partition key was provided", params.QueueName))
		}
	}

	// Check if we are within a workflow (and thus a child workflow)
	parentWorkflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isChildWorkflow := ok && parentWorkflowState != nil

	// Direct invocations require a launched runtime. Recovery, dequeue, and
	// child workflow calls are internal paths that may run before Launch completes.
	if !c.launched.Load() && !params.isRecovery && !params.isDequeue && !isChildWorkflow {
		c.logger.Error("RunWorkflow called before Launch", "workflow_name", params.WorkflowName)
		return nil, models.NewInitializationError("DBOS must be launched before running workflows; call Launch first")
	}

	// Prevent spawning child workflows from within a step
	if isChildWorkflow && parentWorkflowState.isWithinStep {
		c.logger.Error("cannot spawn child workflow from within a step", "workflow_name", params.WorkflowName, "parent_workflow_id", parentWorkflowState.workflowID)
		return nil, models.NewStepExecutionError(parentWorkflowState.workflowID, params.WorkflowName, fmt.Errorf("cannot spawn child workflow from within a step"))
	}

	if isChildWorkflow {
		// Advance step ID if we are a child workflow
		parentWorkflowState.nextStepID()

		// Propagate parent auth identity to child unless caller explicitlyoverrode  it
		if params.AuthenticatedUser == "" {
			params.AuthenticatedUser = parentWorkflowState.authenticatedUser
		}
		if params.AssumedRole == "" {
			params.AssumedRole = parentWorkflowState.assumedRole
		}
		if len(params.AuthenticatedRoles) == 0 {
			params.AuthenticatedRoles = parentWorkflowState.authenticatedRoles
		}
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
		childWorkflowID, err := sysdb.RetryWithResult(c, func() (*string, error) {
			return c.systemDB.CheckChildWorkflow(uncancellableCtx, parentWorkflowState.workflowID, parentWorkflowState.stepID, params.WorkflowName)
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			// A non-determinism error (a different child workflow recorded at this
			// step ID) is deterministic: surface it directly instead of masking it
			// as a generic execution error.
			if dbosErr := (*Error)(nil); errors.As(err, &dbosErr) && dbosErr.Code == ErrorCodeUnexpectedStep {
				c.logger.Error("non-deterministic child workflow invocation", "error", err, "parent_workflow_id", parentWorkflowState.workflowID, "step_id", parentWorkflowState.stepID)
				return nil, err
			}
			c.logger.Error("failed to check child workflow", "error", err, "parent_workflow_id", parentWorkflowState.workflowID, "step_id", parentWorkflowState.stepID)
			return nil, models.NewWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Errorf("checking child workflow: %w", err))
		}
		if childWorkflowID != nil {
			c.logger.Info("child workflow already recorded", "workflow_name", params.WorkflowName, "parent_workflow_id", parentWorkflowState.workflowID, "step_id", parentWorkflowState.stepID, "child_workflow_id", *childWorkflowID)
			return newWorkflowPollingHandle[any](uncancellableCtx, *childWorkflowID), nil
		}
	}

	var status WorkflowStatusType
	if params.QueueName != "" {
		if params.DelayDuration > 0 {
			status = WorkflowStatusDelayed
		} else {
			status = WorkflowStatusEnqueued
		}
	} else {
		status = WorkflowStatusPending
	}

	var delayUntil time.Time
	if params.DelayDuration > 0 {
		delayUntil = time.Now().Add(params.DelayDuration)
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
	// When enqueuing or delaying, we do not set a deadline. It'll be computed with the timeout during dequeue.
	if status == WorkflowStatusEnqueued || status == WorkflowStatusDelayed {
		deadline = time.Time{}
	}

	if params.Priority > uint(math.MaxInt) {
		c.logger.Error("priority exceeds maximum allowed value", "workflow_name", params.WorkflowName, "priority", params.Priority, "max_allowed_value", math.MaxInt)
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.Priority, math.MaxInt)
	}

	// Serialize input before storing in workflow status
	var encodedInput any
	if params.alreadyEncodedInput { // Comes from the queue runner, or the recovery path
		encodedInput = input
	} else if params.isPortableWorkflow { // Direct call to a portable workflow
		var serErr error
		encodedInput, serErr = encodePortableArgs(input)
		if serErr != nil {
			c.logger.Error("failed to serialize portable workflow input", "error", serErr, "workflow_id", workflowID)
			return nil, models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to serialize portable workflow input: %w", serErr))
		}
	} else {
		var serErr error
		encodedInput, serErr = resolveEncoder(c).Encode(input)
		if serErr != nil {
			c.logger.Error("failed to serialize workflow input", "error", serErr, "workflow_id", workflowID)
			return nil, models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to serialize workflow input: %w", serErr))
		}
	}

	var configName *string
	if registeredWorkflow.ConfigName != "" {
		configName = &registeredWorkflow.ConfigName
	}

	workflowStatus := WorkflowStatus{
		Name:               params.WorkflowName,
		ClassName:          registeredWorkflow.ClassName,
		ConfigName:         configName,
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
		DelayUntil:         delayUntil,
		Attributes:         params.WorkflowAttributes,
		Serialization: func() string {
			if params.isPortableWorkflow {
				return PortableSerializerName
			}
			return resolveEncoder(c).Name()
		}(),
	}
	if isChildWorkflow {
		workflowStatus.ParentWorkflowID = parentWorkflowState.workflowID
	}

	var earlyReturnPollingHandle *workflowPollingHandle[any]
	var insertStatusResult *sysdb.InsertWorkflowResult
	returnExisting := params.DeduplicationPolicy == DeduplicationPolicyReturnExisting
	ownerXID := uuid.New().String()

	// Init status and record child workflow relationship in a single transaction
	insertWorkflowStatusTx := func() error {
		tx, err := c.systemDB.Pool().BeginTx(uncancellableCtx, TxOptions{})
		if err != nil {
			return models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx) // Rollback if not committed

		// Insert workflow status with transaction
		insertInput := sysdb.InsertWorkflowStatusDBInput{
			Status:            workflowStatus,
			MaxRetries:        params.MaxRetries,
			Tx:                tx,
			OwnerXID:          &ownerXID,
			IncrementAttempts: params.isDequeue || params.isRecovery,
		}
		insertStatusResult, err = c.systemDB.InsertWorkflowStatus(uncancellableCtx, insertInput)
		if err != nil {
			// Silence dedup error under return-existing policy.
			if !(returnExisting && errors.Is(err, ErrQueueDeduplicated)) {
				c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
			}
			return models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to insert workflow status: %w", err))
		}

		// Record child workflow relationship if this is a child workflow
		// We already have checked this earlier so this path should only be taken if the child is executing the first time
		if isChildWorkflow {
			// Get the step ID that was used for generating the child workflow ID
			childInput := sysdb.RecordChildWorkflowDBInput{
				ParentWorkflowID: parentWorkflowState.workflowID,
				ChildWorkflowID:  workflowID,
				StepName:         params.WorkflowName,
				StepID:           parentWorkflowState.stepID,
				Tx:               tx,
			}
			err = c.systemDB.RecordChildWorkflow(uncancellableCtx, childInput)
			if err != nil {
				c.logger.Error("failed to record child workflow", "error", err, "parent_workflow_id", parentWorkflowState.workflowID, "child_workflow_id", workflowID)
				return models.NewWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Errorf("recording child workflow: %w", err))
			}
		}

		var loaded bool
		if c.activeWorkflowIDs != nil {
			_, loaded = c.activeWorkflowIDs.Load(workflowID)
		}

		shouldSkip :=
			len(params.QueueName) > 0 || // We are enqueueing OR
				insertStatusResult.Status == WorkflowStatusSuccess || // workflow is in a terminal state (success) OR
				insertStatusResult.Status == WorkflowStatusError || // workflow is in a terminal state (error) OR
				(!params.isDequeue && !params.isRecovery && insertStatusResult.OwnerXID != ownerXID) || // another executor, not us dequeueing or being instructed to recover, is already owning the workflow OR
				loaded // this executor is already running the workflow

		if shouldSkip {
			// Commit the transaction to update the number of attempts and/or enact the enqueue
			if err := tx.Commit(uncancellableCtx); err != nil {
				return models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to commit transaction: %w", err))
			}
			earlyReturnPollingHandle = newWorkflowPollingHandle[any](uncancellableCtx, workflowStatus.ID)
			return nil
		}

		// Commit the transaction. This must happen before we start the goroutine to ensure the workflow is found by steps in the database
		if err := tx.Commit(uncancellableCtx); err != nil {
			return models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to commit transaction: %w", err))
		}

		return nil
	}

	for {
		err := sysdb.Retry(c, insertWorkflowStatusTx, sysdb.WithRetrierLogger(c.logger))
		if err == nil {
			// Common path
			break
		}
		// Now handle the case where the insert failed because the deduplication ID is already held by another workflow.
		// We must also handle the case were a parent workflow spawned a return-existing child, and record their parent-child relationship.
		if !returnExisting || !errors.Is(err, ErrQueueDeduplicated) {
			return nil, err
		}
		existingID, lookupErr := sysdb.RetryWithResult(c, func() (*string, error) {
			return c.systemDB.GetDeduplicatedWorkflow(uncancellableCtx, params.QueueName, params.DeduplicationID)
		}, sysdb.WithRetrierLogger(c.logger))
		if lookupErr != nil {
			return nil, models.NewWorkflowExecutionError(workflowID, fmt.Errorf("looking up deduplicated workflow: %w", lookupErr))
		}
		if existingID == nil {
			continue // the slot was cleared between our insert and the lookup; try to claim it
		}
		// Attach to the existing workflow holding the deduplication slot. For a child workflow, record
		// the parent->child mapping at the reserved step ID so replay resolves to the same workflow.
		if isChildWorkflow {
			childInput := sysdb.RecordChildWorkflowDBInput{
				ParentWorkflowID: parentWorkflowState.workflowID,
				ChildWorkflowID:  *existingID,
				StepName:         params.WorkflowName,
				StepID:           parentWorkflowState.stepID,
			}
			if err := c.systemDB.RecordChildWorkflow(uncancellableCtx, childInput); err != nil {
				return nil, models.NewWorkflowExecutionError(parentWorkflowState.workflowID, fmt.Errorf("recording child workflow: %w", err))
			}
		}
		c.logger.Info("returning handle to existing deduplicated workflow", "workflow_name", params.WorkflowName, "queue_name", params.QueueName, "deduplication_id", params.DeduplicationID, "existing_workflow_id", *existingID)
		return newWorkflowPollingHandle[any](uncancellableCtx, *existingID), nil
	}
	if earlyReturnPollingHandle != nil {
		return earlyReturnPollingHandle, nil
	}

	// Create workflow state to track step execution
	wfState := &workflowState{
		workflowID:         workflowID,
		stepID:             -1, // Steps are O-indexed
		isPortableWorkflow: params.isPortableWorkflow,
		authenticatedUser:  params.AuthenticatedUser,
		assumedRole:        params.AssumedRole,
		authenticatedRoles: params.AuthenticatedRoles,
	}
	workflowCtx := WithValue(c, workflowStateKey, wfState)

	// If the workflow has a timeout but no deadline, compute the deadline from the timeout.
	// Else use the durable deadline.
	durableDeadline := time.Time{}
	if insertStatusResult.Timeout > 0 && insertStatusResult.WorkflowDeadline.IsZero() {
		durableDeadline = time.Now().Add(insertStatusResult.Timeout)
	} else if !insertStatusResult.WorkflowDeadline.IsZero() {
		durableDeadline = insertStatusResult.WorkflowDeadline
	}

	if !durableDeadline.IsZero() {
		workflowCtx, _ = WithTimeout(workflowCtx, time.Until(durableDeadline))
	}
	// Register a cancel function that durably cancels the workflow in the DB as soon as
	// the context is cancelled (durable deadline, user cancel, or parent cancellation).
	cancelFuncCompleted := make(chan struct{})
	workflowCancelFunction := func() {
		c.logger.Info("Cancelling workflow", "workflow_id", workflowID)
		err := sysdb.Retry(c, func() error {
			_, err := c.systemDB.CancelWorkflows(uncancellableCtx, sysdb.CancelWorkflowsDBInput{WorkflowIDs: []string{workflowID}})
			return err
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to cancel workflow", "error", err)
		}
		close(cancelFuncCompleted)
	}
	stopFunc := context.AfterFunc(workflowCtx, workflowCancelFunction)
	wfState.workflowCtx = workflowCtx

	// Run the function in a goroutine
	outcomeChan := make(chan workflowOutcome[any], 1)

	// awaitExistingOutcome delivers the result of another execution of this workflow
	// (one this run does not own) to the outcome channel.
	awaitExistingOutcome := func() {
		awaitOut, awaitErr := sysdb.RetryWithResult(c, func() (*sysdb.AwaitWorkflowResultOutput, error) {
			return c.systemDB.AwaitWorkflowResult(uncancellableCtx, workflowID, sysdb.DBRetryInterval)
		}, sysdb.WithRetrierLogger(c.logger))
		err := awaitErr
		if awaitErr == nil && awaitOut != nil && awaitOut.ErrStr != nil {
			err = deserializeWorkflowError(awaitOut.ErrStr)
		}
		var encodedResult any
		var ser string
		if awaitOut != nil {
			encodedResult = awaitOut.Output
			ser = awaitOut.Serialization
		}
		// Keep the encoded result - decoding will happen in RunWorkflow[P,R] when we know the target type
		outcomeChan <- workflowOutcome[any]{result: encodedResult, err: err, needsDecoding: true, serialization: ser,
			cancelled: errors.Is(err, ErrAwaitedWorkflowCancelled)}
		close(outcomeChan)
	}

	c.workflowsWg.Add(1)
	go func() {
		defer c.workflowsWg.Done()

		removeActive := func() {}
		if c.activeWorkflowIDs != nil {
			entry := activeWorkflowEntry{}
			if insertStatusResult.QueueName != nil {
				entry.queueName = *insertStatusResult.QueueName
			}
			if insertStatusResult.QueuePartitionKey != nil {
				entry.queuePartitionKey = *insertStatusResult.QueuePartitionKey
			}
			_, loaded := c.activeWorkflowIDs.LoadOrStore(workflowID, entry)
			if loaded {
				// Lost a start race: a concurrent start of this workflow (recovery and
				// dequeue bypass the ownerXID guard) activated itself between this run's
				// active-ID check and here. The winner owns the active entry, so leave it
				// alone, disarm the durable cancel, and await the winner's result.
				stopFunc()
				c.logger.Warn("Workflow is already executing on this executor. Waiting for the existing execution to complete", "workflow_id", workflowID)
				awaitExistingOutcome()
				return
			}
			var removeOnce sync.Once
			removeActive = func() { removeOnce.Do(func() { c.activeWorkflowIDs.Delete(workflowID) }) }
		}
		defer removeActive()

		var result any
		var err error

		result, err = fn(workflowCtx, input)

		// Handle DBOS ID conflict errors by waiting workflow result
		if errors.Is(err, ErrConflictingWorkflowID) {
			// This run lost the ID conflict: it does not own the workflow, so its
			// context must no longer durably cancel it. Disarm the cancel function.
			stopFunc()
			c.logger.Warn("Workflow ID conflict detected. Waiting for existing workflow to complete", "workflow_id", workflowID)
			awaitExistingOutcome()
			return
		} else {
			// A cancelled run skips updateWorkflowOutcome entirely so it can never
			// clobber the row (e.g., ENQUEUED written by a concurrent resume).
			if !stopFunc() {
				// AfterFunc fired => context is cancelled. Wait for the DB cancel to finish.
				c.logger.Info("Workflow was cancelled. Waiting for cancel function to complete", "workflow_id", workflowID)
				<-cancelFuncCompleted
				removeActive()
				outcomeChan <- workflowOutcome[any]{result: result, err: err, cancelled: true}
				close(outcomeChan)
				return
			}
			if workflowCtx.Err() != nil && isCancellationError(err) {
				// We stopped the AfterFunc but the context was already cancelled
				// so we need to run the durable cancel ourselves.
				workflowCancelFunction()
				removeActive()
				outcomeChan <- workflowOutcome[any]{result: result, err: err, cancelled: true}
				close(outcomeChan)
				return
			}
			if errors.Is(err, ErrWorkflowCancelled) {
				// The workflow observed its own cancellation in the DB (external
				// cancel): the row is already CANCELLED. Skip the outcome write.
				removeActive()
				outcomeChan <- workflowOutcome[any]{result: result, err: err, cancelled: true}
				close(outcomeChan)
				return
			}

			status := WorkflowStatusSuccess
			if err != nil {
				status = WorkflowStatusError
			}

			// Serialize the output before recording
			encodedOutput, serErr := resolveEncoder(workflowCtx).Encode(result)
			if serErr != nil {
				c.logger.Error("Failed to serialize workflow output", "workflow_id", workflowID, "error", serErr)
				outcomeChan <- workflowOutcome[any]{result: nil, err: fmt.Errorf("failed to serialize output: %w", serErr)}
				close(outcomeChan)
				return
			}

			var serializedErr string
			if err != nil {
				serializedErr = serializeWorkflowError(c.logger, err, resolveEncoder(workflowCtx).Name())
			}
			// Remove from the active set before the outcome becomes durable: once it is
			// visible, a resume→dequeue can re-dispatch this workflow to this executor,
			// marking it PENDING. But a stale activeID entry would prevent the workflow from running.
			removeActive()
			recordErr := sysdb.Retry(c, func() error {
				return c.systemDB.UpdateWorkflowOutcome(uncancellableCtx, sysdb.UpdateWorkflowOutcomeDBInput{
					WorkflowID: workflowID,
					Status:     status,
					ErrStr:     serializedErr,
					Output:     encodedOutput,
				})
			}, sysdb.WithRetrierLogger(c.logger))
			if recordErr != nil {
				// A cancellation error means the write was refused because the workflow
				// was cancelled (e.g. during the final step): end as cancelled, not
				// complete. Deliver a cancellation outcome wrapping the workflow's own
				// error so context.Canceled/DeadlineExceeded still match via errors.Is.
				// Any other error is a genuine DB failure and is delivered as-is below.
				if errors.Is(recordErr, ErrWorkflowCancelled) {
					outcomeChan <- workflowOutcome[any]{result: result, err: models.NewWorkflowCancelledError(workflowID, err), cancelled: true}
					close(outcomeChan)
					return
				}
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
/*********** ENQUEUE **********/
/******************************/

// EnqueueOption is a functional option for configuring workflow enqueue parameters.
type EnqueueOption func(*enqueueOptions)

// WithEnqueueWorkflowID sets a custom workflow ID instead of generating one automatically.
func WithEnqueueWorkflowID(id string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.workflowID = id
	}
}

// WithEnqueueApplicationVersion overrides the application version for the enqueued workflow.
func WithEnqueueApplicationVersion(version string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.applicationVersion = version
	}
}

// WithEnqueueDeduplicationID sets a deduplication ID for the enqueued workflow.
func WithEnqueueDeduplicationID(id string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.deduplicationID = id
	}
}

// WithEnqueueDeduplicationPolicy sets how a colliding deduplication ID is handled.
// DeduplicationPolicyReturnExisting requires a deduplication ID (WithEnqueueDeduplicationID).
func WithEnqueueDeduplicationPolicy(policy DeduplicationPolicy) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.deduplicationPolicy = policy
	}
}

// WithEnqueuePriority sets the execution priority for the enqueued workflow.
func WithEnqueuePriority(priority uint) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.priority = priority
	}
}

// WithEnqueueTimeout sets the maximum execution time for the enqueued workflow.
func WithEnqueueTimeout(timeout time.Duration) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.workflowTimeout = timeout
	}
}

// WithEnqueueQueuePartitionKey sets the queue partition key for partitioned queues.
// When a queue is partitioned, workflows with the same partition key are processed
// with separate concurrency limits per partition.
func WithEnqueueQueuePartitionKey(partitionKey string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.queuePartitionKey = partitionKey
	}
}

// WithEnqueueClassName sets the class/namespace name for the enqueued workflow.
// This is required when enqueueing to Python, TypeScript, or Java targets, which
// dispatch workflows by (class_name, workflow_name) pair.
func WithEnqueueClassName(className string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.className = className
	}
}

// WithEnqueueConfigName sets the config/instance name for the enqueued workflow.
// This is required when enqueueing to a workflow registered on a configured instance:
// a Go workflow registered with WithInstance, or a Python/TypeScript/Java class
// instance workflow (e.g. DBOSConfiguredInstance / ConfiguredInstance).
// Pass an empty string ("") to target the default (unnamed) instance.
func WithEnqueueConfigName(configName string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.configName = &configName
	}
}

// WithEnqueueDelay delays execution of the enqueued workflow by the specified duration.
// The workflow starts in the DELAYED status and transitions to ENQUEUED after the delay expires.
func WithEnqueueDelay(delay time.Duration) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.delayDuration = delay
	}
}

// WithEnqueueAuthenticatedUser sets the authenticated user for the enqueued workflow.
func WithEnqueueAuthenticatedUser(user string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.authenticatedUser = user
	}
}

// WithEnqueueAssumedRole sets the assumed role for the enqueued workflow.
func WithEnqueueAssumedRole(role string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.assumedRole = role
	}
}

// WithEnqueueAuthenticatedRoles sets the authenticated roles for the enqueued workflow.
func WithEnqueueAuthenticatedRoles(roles ...string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.authenticatedRoles = roles
	}
}

// WithEnqueueAttributes attaches custom key-value attributes to the enqueued workflow.
// Attributes are recorded in the workflow status at creation, must be
// JSON-serializable, and can be searched with WithFilterAttributes on Postgres.
func WithEnqueueAttributes(attributes map[string]any) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.attributes = attributes
	}
}

type enqueueOptions struct {
	workflowName        string
	workflowID          string
	applicationVersion  string
	deduplicationID     string
	deduplicationPolicy DeduplicationPolicy
	priority            uint
	workflowTimeout     time.Duration
	workflowInput       any
	queuePartitionKey   string
	className           string
	configName          *string
	delayDuration       time.Duration
	authenticatedUser   string
	assumedRole         string
	authenticatedRoles  []string
	attributes          map[string]any
}

// Enqueue enqueues a workflow by name to a named queue for deferred execution.
func (c *dbosContext) Enqueue(_ Client, queueName, workflowName string, input any, opts ...EnqueueOption) (WorkflowHandle[any], error) {
	// Process options
	params := &enqueueOptions{
		workflowName:       workflowName,
		applicationVersion: c.GetApplicationVersion(),
		workflowInput:      input,
	}
	for _, opt := range opts {
		opt(params)
	}

	if len(queueName) == 0 {
		return nil, models.NewInvalidOptionError("queue name is required")
	}

	if len(workflowName) == 0 {
		return nil, models.NewInvalidOptionError("workflow name is required")
	}

	// Validate partition key and deduplication ID are not both provided (they are incompatible)
	if len(params.queuePartitionKey) > 0 && len(params.deduplicationID) > 0 {
		return nil, models.NewInvalidOptionError("partition key and deduplication ID cannot be used together")
	}

	// A non-default deduplication policy only applies with a deduplication ID
	if params.deduplicationPolicy != DeduplicationPolicyReject && len(params.deduplicationID) == 0 {
		return nil, models.NewInvalidOptionError("a deduplication policy requires a deduplication ID")
	}

	workflowID := params.workflowID
	if workflowID == "" {
		workflowID = uuid.New().String()
	}

	if params.priority > uint(math.MaxInt) {
		return nil, models.NewInvalidOptionError(fmt.Sprintf("priority %d exceeds maximum allowed value %d", params.priority, math.MaxInt))
	}

	if params.workflowTimeout > 0 {
		c.logger.Warn("enqueue timeout does not set a deadline: the timeout clock starts when the workflow is dequeued", "workflow_id", workflowID, "timeout", params.workflowTimeout)
	}

	// Encode input and determine serialization format
	var encodedInput *string
	var serialization string
	if _, ok := input.(PortableWorkflowArgs); ok {
		ser := newPortableSerializer[any]()
		var err error
		encodedInput, err = ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize portable workflow input: %w", err)
		}
		serialization = PortableSerializerName
	} else {
		ser := resolveEncoder(c)
		var err error
		encodedInput, err = ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize workflow input: %w", err)
		}
		serialization = ser.Name()
	}

	var wfStatus WorkflowStatusType
	var delayUntil time.Time
	if params.delayDuration > 0 {
		wfStatus = WorkflowStatusDelayed
		delayUntil = time.Now().Add(params.delayDuration)
	} else {
		wfStatus = WorkflowStatusEnqueued
	}

	status := WorkflowStatus{
		Name:               params.workflowName,
		ApplicationVersion: params.applicationVersion,
		Status:             wfStatus,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Timeout:            params.workflowTimeout,
		Input:              encodedInput,
		QueueName:          queueName,
		DeduplicationID:    params.deduplicationID,
		Priority:           int(params.priority),
		QueuePartitionKey:  params.queuePartitionKey,
		ClassName:          params.className,
		ConfigName:         params.configName,
		Serialization:      serialization,
		DelayUntil:         delayUntil,
		AuthenticatedUser:  params.authenticatedUser,
		AssumedRole:        params.assumedRole,
		AuthenticatedRoles: params.authenticatedRoles,
		Attributes:         params.attributes,
	}

	uncancellableCtx := WithoutCancel(c)
	returnExisting := params.deduplicationPolicy == DeduplicationPolicyReturnExisting

	for {
		err := func() error {
			tx, err := c.systemDB.Pool().BeginTx(uncancellableCtx, TxOptions{})
			if err != nil {
				return models.NewWorkflowExecutionError(workflowID, fmt.Errorf("failed to begin transaction: %w", err))
			}
			defer tx.Rollback(uncancellableCtx)

			insertInput := sysdb.InsertWorkflowStatusDBInput{
				Status: status,
				Tx:     tx,
			}
			if _, err := c.systemDB.InsertWorkflowStatus(uncancellableCtx, insertInput); err != nil {
				return err
			}
			if err := tx.Commit(uncancellableCtx); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}
			return nil
		}()
		if err != nil {
			if returnExisting && errors.Is(err, ErrQueueDeduplicated) {
				existingID, lookupErr := c.systemDB.GetDeduplicatedWorkflow(uncancellableCtx, queueName, params.deduplicationID)
				if lookupErr != nil {
					return nil, models.NewWorkflowExecutionError(workflowID, fmt.Errorf("looking up deduplicated workflow: %w", lookupErr))
				}
				if existingID != nil {
					return newWorkflowPollingHandle[any](uncancellableCtx, *existingID), nil
				}
				// Try again if the deduplication record was not found. Means that the dedup slot was freed.
				continue
			}
			c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
			return nil, err
		}

		return newWorkflowPollingHandle[any](uncancellableCtx, workflowID), nil
	}
}

// Enqueue adds a workflow to a named queue for later execution with type safety.
// The workflow will be persisted with ENQUEUED status until picked up by a DBOS process.
// This provides asynchronous workflow execution with durability guarantees.
//
// Parameters:
//   - ctx: Client or Context instance for the operation
//   - queueName: Name of the queue to enqueue the workflow to
//   - workflowName: Name of the registered workflow function to execute
//   - input: Input parameters to pass to the workflow (type P, inferred; only the result type R needs to be specified)
//   - opts: Optional configuration options
//
// Available options:
//   - WithEnqueueWorkflowID: Custom workflow ID (auto-generated if not provided)
//   - WithEnqueueApplicationVersion: Application version override
//   - WithEnqueueDeduplicationID: Deduplication identifier for idempotent enqueuing
//   - WithEnqueueDeduplicationPolicy: How a colliding deduplication ID is handled
//   - WithEnqueuePriority: Execution priority
//   - WithEnqueueTimeout: Maximum execution time for the workflow
//   - WithEnqueueDelay: Delay before the workflow becomes eligible for dequeue
//   - WithEnqueueQueuePartitionKey: Queue partition key for partitioned queues
//   - WithEnqueueClassName: Class/namespace name for cross-language dispatch
//   - WithEnqueueConfigName: Config/instance name for configured-instance workflows
//   - WithEnqueueAuthenticatedUser, WithEnqueueAssumedRole, WithEnqueueAuthenticatedRoles: Auth metadata recorded on the workflow
//   - WithEnqueueAttributes: Custom key-value attributes recorded on the workflow
//
// Returns a typed workflow handle that can be used to check status and retrieve results.
// The handle uses polling to check workflow completion since the execution is asynchronous.
//
// Example usage:
//
//	// Enqueue a workflow with string input and int output
//	handle, err := dbos.Enqueue[int](client, "data-processing", "ProcessDataWorkflow", "input data",
//	    dbos.WithEnqueueTimeout(30 * time.Minute))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Check status
//	status, err := handle.GetStatus()
//	if err != nil {
//	    log.Printf("Failed to get status: %v", err)
//	}
//
//	// Wait for completion and get result
//	result, err := handle.GetResult()
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %d", result)
//	}
//
//	// Enqueue with deduplication and custom workflow ID
//	handle, err := dbos.Enqueue[MyOutputType](client, "my-queue", "MyWorkflow", MyInputType{Field: "value"},
//	    dbos.WithEnqueueWorkflowID("custom-workflow-id"),
//	    dbos.WithEnqueueDeduplicationID("unique-operation-id"))
//
// To enqueue a workflow for a DBOS application in another language (e.g., Python),
// pass a [PortableWorkflowArgs] as the input. This automatically uses portable JSON
// serialization, encoding the envelope with positional and named arguments:
//
//	args := dbos.PortableWorkflowArgs{
//	    PositionalArgs: []any{"hello", 42},
//	    NamedArgs:      map[string]any{"key": "value"},
//	}
//	handle, err := dbos.Enqueue[any](client, "queue", "py_workflow", args)
func Enqueue[R any, P any](ctx Client, queueName, workflowName string, input P, opts ...EnqueueOption) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("client cannot be nil")
	}

	// Call the interface method — encoding happens there
	handle, err := ctx.Enqueue(ctx, queueName, workflowName, input, opts...)
	if err != nil {
		return nil, err
	}

	return typedHandle[R](ctx, handle), nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

// StepFunc represents a type-erased step function used internally.
type StepFunc func(ctx context.Context) (any, error)

// Step represents a type-safe step function with a specific output type R.
type Step[R any] func(ctx context.Context) (R, error)

// TxnFunc is a type-erased transaction function that receives a portable Tx.
type TxnFunc func(ctx context.Context, tx Tx) (any, error)

// Txn represents a type-safe step function with output type R that receives a transaction.
type Txn[R any] func(ctx context.Context, tx Tx) (R, error)

// stepOptions holds the configuration for step execution using functional options pattern.
type stepOptions struct {
	maxRetries         int              // Maximum number of retry attempts (0 = no retries)
	backoffFactor      float64          // Exponential backoff multiplier between retries (default: 2.0)
	baseInterval       time.Duration    // Initial delay between retries (default: 100ms)
	maxInterval        time.Duration    // Maximum delay between retries (default: 5s)
	stepName           string           // Custom name for the step (defaults to function name)
	preGeneratedStepID *int             // Pre generated stepID
	txIsoLevel         *IsoLevel        // Transaction isolation level for runAsTxn (nil = ReadCommitted)
	retryPredicate     func(error) bool // Optional predicate: nil = retry all errors up to maxRetries
}

// setDefaults applies default values to stepOptions
func (opts *stepOptions) setDefaults(logger *slog.Logger) {
	usesDefaultMaxInterval := opts.maxInterval == 0
	if opts.backoffFactor == 0 {
		opts.backoffFactor = _DEFAULT_STEP_BACKOFF_FACTOR
	}
	if opts.baseInterval == 0 {
		opts.baseInterval = _DEFAULT_STEP_BASE_INTERVAL
	}
	if opts.maxInterval == 0 {
		opts.maxInterval = _DEFAULT_STEP_MAX_INTERVAL
	}
	if usesDefaultMaxInterval && opts.baseInterval > opts.maxInterval {
		if logger != nil {
			logger.Warn("Step base interval exceeds the default max interval; increasing max interval to match base interval",
				"base_interval", opts.baseInterval,
				"default_max_interval", opts.maxInterval,
			)
		}
		opts.maxInterval = opts.baseInterval
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

// WithStepBackoffFactor sets the exponential backoff multiplier between retries.
// The delay between retries is calculated as: BaseInterval * (BackoffFactor^(retry-1))
// Default value is 2.0.
func WithStepBackoffFactor(factor float64) StepOption {
	return func(opts *stepOptions) {
		opts.backoffFactor = factor
	}
}

// WithStepBaseInterval sets the initial delay between retries.
// Default value is 100ms.
func WithStepBaseInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.baseInterval = interval
	}
}

// WithStepMaxInterval sets the maximum delay between retries.
// Default value is 5s.
func WithStepMaxInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.maxInterval = interval
	}
}

// WithStepRetryPredicate sets a function to decide whether a step error is retryable.
// If the predicate returns false for an error, the step stops retrying immediately
// and returns that error even if maxRetries has not been reached.
// If not set (nil), all errors are retried up to maxRetries (default behaviour).
//
// The predicate is evaluated before the backoff delay, so a non-retryable error
// exits immediately without waiting.
//
// Example : only retry HTTP 5xx errors, not 4xx client errors:
//
//	dbos.RunAsStep(ctx, callPaymentAPI,
//	    dbos.WithStepMaxRetries(3),
//	    dbos.WithStepRetryPredicate(func(err error) bool {
//	        var apiErr *APIError
//	        if errors.As(err, &apiErr) {
//	            return apiErr.StatusCode >= 500
//	        }
//	        return true
//	    }),
//	)
func WithStepRetryPredicate(fn func(error) bool) StepOption {
	return func(opts *stepOptions) {
		opts.retryPredicate = fn
	}
}

// WithTxIsolation sets the transaction isolation level used by [RunAsTransaction].
// It has no effect outside a transaction: RunAsStep and Go ignore it.
// If not set, the data source's default isolation level is used.
func WithTxIsolation(level IsoLevel) StepOption {
	return func(opts *stepOptions) {
		opts.txIsoLevel = &level
	}
}

func withNextStepID(stepID int) StepOption {
	return func(opts *stepOptions) {
		opts.preGeneratedStepID = &stepID
	}
}

// StepOutcome holds the result and error from a step execution.
// This struct is delivered on the channel returned by Go when running the step inside a goroutine.
type StepOutcome[R any] struct {
	Result R
	Err    error
}

// StreamValue holds a value, error, and closed status from a stream read operation.
// This struct is delivered on the channel returned by ReadStreamAsync.
type StreamValue[R any] struct {
	Value  R     // The stream value (zero value if error/closed)
	Err    error // Error if one occurred (nil otherwise)
	Closed bool  // Whether the stream is closed
}

// convertStepResult converts a generic step result to a typed result R.
// It handles both checkpointed outcomes (encoded values from database) and direct type conversions.
// Supports both real DBOS contexts and testing/mocking scenarios.
func convertStepResult[R any](ctx Context, result any) (R, error) {
	var typedResult R
	// Check if we're in a real DBOS context (not a mock)
	if _, ok := ctx.(*dbosContext); ok {
		// First check if this is a checkpointed outcome (encoded value from database)
		if checkpointed, ok := result.(stepCheckpointedOutcome); ok {
			// This came from the database and needs decoding
			encodedOutput, ok := checkpointed.value.(*string)
			if !ok {
				workflowID, _ := GetWorkflowID(ctx)
				return *new(R), models.NewWorkflowExecutionError(workflowID, fmt.Errorf("checkpointed outcome value is not *string, got %T", checkpointed.value))
			}
			var decodeErr error
			stepDecoder, resolveErr := resolveDecoder[R](checkpointed.serialization, getCustomSerializerFromCtx(ctx))
			if resolveErr != nil {
				workflowID, err := GetWorkflowID(ctx)
				if err != nil {
					return *new(R), fmt.Errorf("getting workflow ID from context: %w; original error: %v", err, resolveErr)
				}
				return *new(R), models.NewWorkflowExecutionError(workflowID, resolveErr)
			}
			typedResult, decodeErr = stepDecoder.Decode(encodedOutput)
			if decodeErr != nil {
				workflowID, _ := GetWorkflowID(ctx)
				return *new(R), models.NewWorkflowExecutionError(workflowID, fmt.Errorf("decoding step result to expected type %T: %w", *new(R), decodeErr))
			}
		} else if typedRes, ok := result.(R); ok {
			// When the step is executed, the result is already decoded and should be directly convertible
			typedResult = typedRes
		} else {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), models.NewWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	} else {
		// Fallback for testing/mocking scenarios
		if typedRes, ok := result.(R); ok {
			typedResult = typedRes
		} else {
			workflowID, _ := GetWorkflowID(ctx)
			return *new(R), models.NewWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
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
	stepOpts.setDefaults(c.logger)

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, models.NewStepExecutionError("", stepOpts.stepName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
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
		workflowCtx:  wfState.workflowCtx,
	}
	return &preparedStep{WorkflowID: wfState.workflowID, StepOpts: stepOpts, StepState: &stepState, IsWithinStep: false}, nil
}

// checkStepContext verifies that ctx carries workflow state marked as within a step.
// DBOS invokes step bodies with a dedicated step context (isWithinStep == true); if that
// invariant is broken (e.g. the raw workflow context is passed instead of the step context),
// return a clear ErrorCodeStepExecution rather than running the step body with a mis-wired context.
func checkStepContext(ctx Context, workflowID, stepName string) error {
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil || !wfState.isWithinStep {
		return models.NewStepExecutionError(workflowID, stepName, fmt.Errorf("step must use the context.Context received from its dbos.Func closure."))
	}
	return nil
}

// executeStepWithRetry runs runOnce (the step body) and retries with backoff on error when maxRetries > 0.
func executeStepWithRetry(c *dbosContext, workflowID string, stepOpts *stepOptions, runOnce func() (any, error)) (stepOutput any, stepError error) {
	work := func() error {
		stepOutput, stepError = runOnce()
		return stepError
	}
	sched := sysdb.BackoffSchedule{
		Base:      stepOpts.baseInterval,
		Max:       stepOpts.maxInterval,
		Factor:    stepOpts.backoffFactor,
		JitterMin: 0.95,
		JitterMax: 1.05,
	}
	var joinedErrors error
	// decide: runs is the number of completed runs (>=1). runs > maxRetries means
	// the last allowed run just failed. With maxRetries <= 0 the very first run is
	// terminal, returning the raw error (no wrapping). The predicate gates the
	// NEXT retry and is not consulted once the budget is exhausted.
	decide := func(err error, runs int) (bool, error) {
		joinedErrors = errors.Join(joinedErrors, err)
		if runs > stepOpts.maxRetries {
			if stepOpts.maxRetries <= 0 {
				return false, err
			}
			return false, models.NewMaxStepRetriesExceededError(workflowID, stepOpts.stepName, stepOpts.maxRetries, joinedErrors)
		}
		if stepOpts.retryPredicate != nil && !stepOpts.retryPredicate(err) {
			return false, err
		}
		return true, nil
	}
	onRetry := func(err error, runs int, delay time.Duration) {
		c.logger.Error("step failed, retrying", "step_name", stepOpts.stepName, "retry", runs, "max_retries", stepOpts.maxRetries, "delay", delay, "error", err)
	}
	onCancel := func() error {
		return models.NewStepExecutionError(workflowID, stepOpts.stepName, fmt.Errorf("context cancelled during retry: %w", c.Err()))
	}
	if err := sysdb.RetryLoop(c, sched, work, decide, onRetry, onCancel); err != nil {
		stepError = err
	}
	return stepOutput, stepError
}

func isCancellationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrWorkflowCancelled) || errors.Is(err, ErrAwaitedWorkflowCancelled)
}

func stepInterruptedByCancellation(stepState *workflowState, stepError error) bool {
	if stepState.workflowCtx == nil || stepState.workflowCtx.Err() == nil {
		return false
	}
	return isCancellationError(stepError)
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
//	}, dbos.WithStepMaxRetries(3), dbos.WithStepBaseInterval(500*time.Millisecond))
//
// Available options:
//   - WithStepName: Custom name for the step (only sets if not already set)
//   - WithStepMaxRetries: Maximum retry attempts (default: 0)
//   - WithStepBackoffFactor: Exponential backoff multiplier (default: 2.0)
//   - WithStepBaseInterval: Initial delay between retries (default: 100ms)
//   - WithStepMaxInterval: Maximum delay between retries (default: 5s)
//   - WithStepRetryPredicate: Function called before each retry to decide whether the error is retryable.
//     If it returns false the step stops retrying immediately, even if maxRetries has not been reached.
//     If not set, all errors are retried up to maxRetries (default behaviour).
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
//
// Context cancellation: if the workflow's context is cancelled while the step is running,
// the step's outcome is not checkpointed and RunAsStep returns a *Error with code
// ErrorCodeWorkflowCancelled, wrapping the underlying context error. The workflow should return
// promptly: it is marked CANCELLED and, when resumed, re-executes the interrupted step.
// Do not swallow this error to run further durable work — replay after resume would diverge.
// By contrast, cancelling a context that wraps only the step (e.g. a per-step timeout)
// records the step's error as its durable outcome and the workflow continues normally.
func RunAsStep[R any](ctx Context, fn Step[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), models.NewStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), models.NewStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
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

func (c *dbosContext) RunAsStep(_ Context, fn StepFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, models.NewStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		return fn(c)
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepOpts := prep.StepOpts

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := sysdb.RetryWithResult(c, func() (*sysdb.RecordedResult, error) {
		return c.systemDB.CheckOperationExecution(uncancellableCtx, sysdb.CheckOperationExecutionDBInput{
			WorkflowID: stepState.workflowID,
			StepID:     stepState.stepID,
			StepName:   stepOpts.stepName,
		})
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
	}
	if recordedOutput != nil {
		// Return the encoded output wrapped in stepCheckpointedOutcome
		// This allows RunAsStep[R] to distinguish encoded values from direct values
		return stepCheckpointedOutcome{value: recordedOutput.Output, serialization: recordedOutput.Serialization}, deserializeWorkflowError(recordedOutput.ErrStr)
	}

	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()
	stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, func() (any, error) {
		if err := checkStepContext(stepCtx, stepState.workflowID, stepOpts.stepName); err != nil {
			return nil, err
		}
		return fn(stepCtx)
	})

	if stepInterruptedByCancellation(stepState, stepError) {
		return stepOutput, models.NewWorkflowCancelledError(stepState.workflowID, stepError)
	}

	// Serialize step output before recording
	ser := resolveEncoder(c)
	encodedStepOutput, serErr := ser.Encode(stepOutput)
	if serErr != nil {
		return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
	}

	// Record the final result
	stepCompletedTime := time.Now()
	var serializedStepErr *string
	if stepError != nil {
		s := serializeWorkflowError(c.logger, stepError, ser.Name())
		serializedStepErr = &s
	}
	dbInput := sysdb.RecordOperationResultDBInput{
		WorkflowID:    stepState.workflowID,
		StepName:      stepOpts.stepName,
		StepID:        stepState.stepID,
		ErrStr:        serializedStepErr,
		StartedAt:     stepStartTime,
		CompletedAt:   stepCompletedTime,
		Output:        encodedStepOutput,
		Serialization: ser.Name(),
	}
	recErr := sysdb.Retry(c, func() error {
		return c.systemDB.RecordOperationResult(uncancellableCtx, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
	if recErr != nil {
		return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, recErr)
	}

	return stepOutput, stepError
}

// runAsTxn executes a step function that receives a transaction when run on its own.
// The step body and checkpoint share one transaction, so system DB writes and recordOperationResult commit together.
// Like RunAsStep but uses txn[R] / TxnFunc; transaction is begun and committed inside this function.
func runAsTxn[R any](ctx Context, fn Txn[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), models.NewStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), models.NewStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
	}

	c, ok := ctx.(*dbosContext)
	if !ok {
		return *new(R), models.NewStepExecutionError("", "", fmt.Errorf("runAsTxn requires *dbosContext. Mock the caller of this function if you are testing."))
	}

	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	typeErasedFn := TxnFunc(func(ctx context.Context, tx Tx) (any, error) { return fn(ctx, tx) })

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

// withinTransactionContext returns a child context whose workflow state is
// flagged as executing inside a data source transaction.
func withinTransactionContext(c *dbosContext) Context {
	var state workflowState
	if existing, ok := c.Value(workflowStateKey).(*workflowState); ok && existing != nil {
		state = *existing
	}
	state.isWithinStep = true
	state.isWithinTransaction = true
	return WithValue(c, workflowStateKey, &state)
}

func (c *dbosContext) runAsTxn(_ Context, fn TxnFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, models.NewStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		// Invoked inside an enclosing step: manage the transaction but record no durability
		txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}
		if prep.StepOpts.txIsoLevel != nil {
			txOpts.IsoLevel = *prep.StepOpts.txIsoLevel
		}
		uncancellableCtx := WithoutCancel(c)
		tx, err := c.systemDB.Pool().BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, models.NewStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)
		output, err := fn(withinTransactionContext(c), tx)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, models.NewStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", err))
		}
		return output, nil
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepState.isWithinTransaction = true
	stepOpts := prep.StepOpts
	pool := c.systemDB.Pool()
	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()

	txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}
	if stepOpts.txIsoLevel != nil {
		txOpts.IsoLevel = *stepOpts.txIsoLevel
	}
	txnRetryOpts := []sysdb.RetryOption{
		sysdb.WithRetrierLogger(c.logger),
		sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction),
	}
	return sysdb.RetryWithResult(c, func() (any, error) {
		tx, err := pool.BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)

		recordedOutput, err := c.systemDB.CheckOperationExecution(uncancellableCtx, sysdb.CheckOperationExecutionDBInput{
			WorkflowID: stepState.workflowID,
			StepID:     stepState.stepID,
			StepName:   stepOpts.stepName,
			Tx:         tx,
		})
		if err != nil {
			return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
		}
		if recordedOutput != nil {
			return stepCheckpointedOutcome{value: recordedOutput.Output, serialization: recordedOutput.Serialization}, deserializeWorkflowError(recordedOutput.ErrStr)
		}

		stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, func() (any, error) {
			// Without a savepoint fn's writes could not be discarded on error, so
			// don't run fn at all.
			if _, spErr := tx.Exec(uncancellableCtx, "SAVEPOINT dbos_step"); spErr != nil {
				return nil, fmt.Errorf("failed to create savepoint: %w", spErr)
			}
			output, err := fn(stepCtx, tx)
			if err != nil {
				if _, rbErr := tx.Exec(uncancellableCtx, "ROLLBACK TO SAVEPOINT dbos_step"); rbErr != nil {
					return nil, errors.Join(err, fmt.Errorf("failed to roll back to savepoint: %w", rbErr))
				}
				return output, err
			}
			if _, relErr := tx.Exec(uncancellableCtx, "RELEASE SAVEPOINT dbos_step"); relErr != nil {
				return nil, fmt.Errorf("failed to release savepoint: %w", relErr)
			}
			return output, nil
		})

		if stepInterruptedByCancellation(stepState, stepError) {
			return stepOutput, models.NewWorkflowCancelledError(stepState.workflowID, stepError)
		}

		txnSer := resolveEncoder(c)
		serialization := txnSer.Name()
		var encodedStepOutput *string
		if raw, ok := stepOutput.(rawStepOutput); ok {
			// Pre-serialized payload: record as-is under its own serialization name
			encodedStepOutput = raw.value
			serialization = raw.serialization
		} else {
			var serErr error
			encodedStepOutput, serErr = txnSer.Encode(stepOutput)
			if serErr != nil {
				return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
			}
		}

		var serializedTxnErr *string
		if stepError != nil {
			s := serializeWorkflowError(c.logger, stepError, txnSer.Name())
			serializedTxnErr = &s
		}
		dbInput := sysdb.RecordOperationResultDBInput{
			WorkflowID:    stepState.workflowID,
			StepName:      stepOpts.stepName,
			StepID:        stepState.stepID,
			ErrStr:        serializedTxnErr,
			StartedAt:     stepStartTime,
			CompletedAt:   time.Now(),
			Output:        encodedStepOutput,
			Tx:            tx,
			Serialization: serialization,
		}
		recErr := c.systemDB.RecordOperationResult(uncancellableCtx, dbInput)
		if recErr != nil {
			if stepError != nil {
				recErr = errors.Join(recErr, stepError)
			}
			return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, recErr)
		}
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, models.NewStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", err))
		}
		return stepOutput, stepError
	}, txnRetryOpts...)
}

// Go runs a step inside a Go routine and returns a channel to receive the result.
// Go generates a deterministic step ID for the step before running the step in a routine, since goroutines are not deterministic.
// Example:
//
//	resultChan, err := dbos.Go(ctx, func(ctx context.Context) (string, error) {
//	    return "Hello, World!", nil
//	})
//
//	outcome := <-resultChan // wait for the channel to receive
//	if outcome.Err != nil {
//	    // Handle error
//	}
func Go[R any](ctx Context, fn Step[R], opts ...StepOption) (<-chan StepOutcome[R], error) {
	if ctx == nil {
		return nil, models.NewStepExecutionError("", "", errors.New("ctx cannot be nil"))
	}

	if fn == nil {
		return nil, models.NewStepExecutionError("", "", errors.New("step function cannot be nil"))
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

func (c *dbosContext) Go(ctx Context, fn StepFunc, opts ...StepOption) (<-chan StepOutcome[any], error) {
	// Create a deterministic step ID
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, models.NewStepExecutionError("", "", errors.New("workflow state not found in context: are you running this step within a workflow?"))
	}
	opts = append(opts, withNextStepID(wfState.nextStepID()))

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
//	result, err := dbos.Select(ctx, []<-chan dbos.StepOutcome[string]{ch1, ch2})
//	if err != nil {
//	    // Handle error
//	    return err
//	}
//	log.Printf("Selected result: %v", result)
func Select[R any](ctx Context, channels []<-chan StepOutcome[R]) (R, error) {
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

func (c *dbosContext) Select(_ Context, channels []<-chan StepOutcome[any]) (any, error) {
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

// sendOptions holds configuration for a Send call.
type sendOptions struct {
	usePortableSerializer bool
	idempotencyKey        string
}

// SendOption is a functional option for configuring a Send call.
type SendOption func(*sendOptions)

// WithPortableSend configures Send to use the portable JSON serializer,
// enabling cross-language interoperability regardless of the workflow's serializer.
func WithPortableSend() SendOption {
	return func(opts *sendOptions) {
		opts.usePortableSerializer = true
	}
}

// WithIdempotencyKey makes a Send deliver at most once. The key is combined with
// the destination workflow ID to form the message's primary key, so retrying a
// Send with the same key (after a crash, timeout, or network failure) inserts the
// message only once. Keys are scoped per destination. Without a key, every Send
// delivers a new message.
func WithIdempotencyKey(key string) SendOption {
	return func(opts *sendOptions) {
		opts.idempotencyKey = key
	}
}

func (c *dbosContext) Send(_ Client, destinationID string, message any, topic string, opts ...SendOption) error {
	// Send cannot be sent from within a step if used within a workflow
	isWithinWorkflow := false
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if ok && wfState != nil {
		isWithinWorkflow = true
		if wfState.isWithinStep {
			return models.NewStepExecutionError(wfState.workflowID, "DBOS.send", fmt.Errorf("cannot call Send within a step"))
		}
	}

	options := &sendOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var sendSer Serializer[any]
	if options.usePortableSerializer {
		sendSer = newPortableSerializer[any]()
	} else {
		sendSer = resolveEncoder(c)
	}

	encodedMessage, err := sendSer.Encode(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	input := sysdb.WorkflowSendInput{
		DestinationID:  destinationID,
		Message:        encodedMessage,
		Topic:          topic,
		Serialization:  sendSer.Name(),
		IdempotencyKey: options.idempotencyKey,
	}

	if isWithinWorkflow {
		_, err = runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			input.Tx = tx
			return nil, ctx.(*dbosContext).systemDB.Send(ctx, input)
		}, WithStepName("DBOS.send"))
	} else {
		uncancellableCtx := WithoutCancel(c)
		err = sysdb.Retry(c, func() error {
			return c.systemDB.Send(uncancellableCtx, input)
		}, sysdb.WithRetrierLogger(c.logger))
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
func Send[P any](ctx Client, destinationID string, message P, topic string, opts ...SendOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.Send(ctx, destinationID, message, topic, opts...)
}

// recvResult carries the received message along with its serialization format from the notifications table.
type recvResult struct {
	message       *string
	serialization string
}

func (c *dbosContext) Recv(_ Context, topic string, timeout time.Duration) (any, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, models.NewStepExecutionError("", "DBOS.recv", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return nil, models.NewStepExecutionError(wfState.workflowID, "DBOS.recv", fmt.Errorf("cannot call Recv within a step"))
	}
	workflowID := wfState.workflowID
	// The recv step ID precedes its internal timeout sleep's; both are allocated
	// up front so the recorded layout matches even when the sleep is skipped.
	stepID := wfState.nextStepID()
	sleepStepID := wfState.nextStepID()
	if len(topic) == 0 {
		topic = sysdb.NullTopic
	}

	// Early exit when this recv already has a checkpoint (recovery, fork),
	// so replay neither waits nor records a spurious sleep step.
	recorded, err := sysdb.RetryWithResult(c, func() (*sysdb.RecordedResult, error) {
		return c.systemDB.CheckOperationExecution(WithoutCancel(c), sysdb.CheckOperationExecutionDBInput{
			WorkflowID: workflowID,
			StepID:     stepID,
			StepName:   "DBOS.recv",
		})
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}
	if recorded != nil {
		return &recvResult{message: recorded.Output, serialization: recorded.Serialization}, deserializeWorkflowError(recorded.ErrStr)
	}

	// Register as the receiver for this workflow/topic.
	waiter, err := c.systemDB.StartRecvListener(c, workflowID, topic)
	if err != nil {
		return nil, err
	}
	defer waiter.Release()

	var timeoutOccurred bool
	if !waiter.Pending {
		// Checkpoint the timeout deadline as a "DBOS.sleep" step before waiting. On
		// re-execution the recorded deadline is returned, so only the remaining time is waited.
		deadlineMs, err := runAsTxn(c, func(ctx context.Context, tx Tx) (int64, error) {
			return time.Now().Add(timeout).UnixMilli(), nil
		}, WithStepName("DBOS.sleep"), withNextStepID(sleepStepID))
		if err != nil {
			return nil, err
		}
		// Wait for a pending message with no transaction open.
		timeoutOccurred, err = waiter.Wait(time.UnixMilli(deadlineMs))
		if err != nil {
			return nil, err
		}
	}

	// Consume the message and checkpoint the recv result in a single transaction.
	// If another executor already checkpointed this step, runAsTxn returns the recorded result.
	out, err := c.runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		message, msgSerialization, err := c.systemDB.ConsumeMessage(ctx, tx, workflowID, topic)
		if err != nil {
			return nil, err
		}
		// Use the sender's serialization; fall back to the receiver's format for the timeout/no-message case
		serialization := resolveEncoder(c).Name()
		if msgSerialization != nil && len(*msgSerialization) > 0 {
			serialization = *msgSerialization
		}
		output := rawStepOutput{value: message, serialization: serialization}
		if message == nil && timeoutOccurred {
			return output, models.NewTimeoutError(workflowID, "DBOS.recv", fmt.Sprintf("no message received within %v", timeout), nil)
		}
		return output, nil
	}, WithStepName("DBOS.recv"), withNextStepID(stepID))

	switch v := out.(type) {
	case rawStepOutput: // executed now
		return &recvResult{message: v.value, serialization: v.serialization}, err
	case stepCheckpointedOutcome: // replayed from a recorded checkpoint
		message, ok := v.value.(*string)
		if !ok {
			return nil, models.NewWorkflowExecutionError(workflowID, fmt.Errorf("recv checkpoint value is not *string, got %T", v.value))
		}
		return &recvResult{message: message, serialization: v.serialization}, err
	case nil:
		return nil, err
	default:
		return nil, models.NewWorkflowUnexpectedResultType(workflowID, "rawStepOutput", fmt.Sprintf("%T", out))
	}
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
func Recv[R any](ctx Context, topic string, timeout time.Duration) (R, error) {
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
		result, ok := msg.(*recvResult)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), models.NewWorkflowUnexpectedResultType(workflowID, "*recvResult", fmt.Sprintf("%T", msg))
		}
		if result.message == nil {
			return *new(R), nil
		}
		msgDecoder, resolveErr := resolveDecoder[R](result.serialization, getCustomSerializerFromCtx(ctx))
		if resolveErr != nil {
			return *new(R), resolveErr
		}
		var decodeErr error
		typedMessage, decodeErr = msgDecoder.Decode(result.message)
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
			return *new(R), models.NewWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", msg))
		}
	}
	return typedMessage, nil
}

// setEventOptions holds configuration for a SetEvent call.
type setEventOptions struct {
	usePortableSerializer bool
}

// SetEventOption is a functional option for configuring a SetEvent call.
type SetEventOption func(*setEventOptions)

// WithPortableSetEvent configures SetEvent to use the portable JSON serializer,
// enabling cross-language interoperability regardless of the workflow's serializer.
func WithPortableSetEvent() SetEventOption {
	return func(opts *setEventOptions) {
		opts.usePortableSerializer = true
	}
}

func (c *dbosContext) SetEvent(_ Context, key string, message any, opts ...SetEventOption) error {
	options := &setEventOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var evtSer Serializer[any]
	if options.usePortableSerializer {
		evtSer = newPortableSerializer[any]()
	} else {
		evtSer = resolveEncoder(c)
	}

	encodedMessage, err := evtSer.Encode(message)
	if err != nil {
		return fmt.Errorf("failed to serialize event value: %w", err)
	}

	_, err = runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
		if !ok || wfState == nil {
			return nil, models.NewStepExecutionError("", "DBOS.setEvent", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
		}
		return nil, c.systemDB.SetEvent(ctx, sysdb.WorkflowSetEventInput{
			Key:           key,
			Message:       encodedMessage,
			Tx:            tx,
			Serialization: evtSer.Name(),
			WorkflowID:    wfState.workflowID,
			StepID:        wfState.stepID,
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
func SetEvent[P any](ctx Context, key string, message P, opts ...SetEventOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetEvent(ctx, key, message, opts...)
}

// getEventResult carries the event value along with its serialization format from the workflow_events table.
type getEventResult struct {
	value         *string
	serialization string
}

func (c *dbosContext) GetEvent(_ Client, targetWorkflowID, key string, timeout time.Duration) (any, error) {
	// GetEvent may run inside or outside a workflow. When inside, it is checkpointed.
	var wfState *workflowState
	if v := c.Value(workflowStateKey); v != nil {
		var ok bool
		if wfState, ok = v.(*workflowState); !ok {
			return nil, models.NewStepExecutionError("", "DBOS.getEvent", fmt.Errorf("workflow state in context has unexpected type %T", v))
		}
	}
	isInWorkflow := wfState != nil
	var workflowID string
	var stepID, sleepStepID int
	if isInWorkflow {
		if wfState.isWithinStep {
			return nil, models.NewStepExecutionError(wfState.workflowID, "DBOS.getEvent", fmt.Errorf("cannot call GetEvent within a step"))
		}
		workflowID = wfState.workflowID
		stepID = wfState.nextStepID()
		sleepStepID = wfState.nextStepID()

		// Early exit when this getEvent already has a checkpoint (recovery, fork),
		// so replay neither waits nor records a spurious sleep step.
		recorded, err := sysdb.RetryWithResult(c, func() (*sysdb.RecordedResult, error) {
			return c.systemDB.CheckOperationExecution(WithoutCancel(c), sysdb.CheckOperationExecutionDBInput{
				WorkflowID: workflowID,
				StepID:     stepID,
				StepName:   "DBOS.getEvent",
			})
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			return nil, err
		}
		if recorded != nil {
			return &getEventResult{value: recorded.Output, serialization: recorded.Serialization}, deserializeWorkflowError(recorded.ErrStr)
		}
	}

	// Register as a waiter for this event.
	waiter, err := c.systemDB.StartEventListener(c, targetWorkflowID, key)
	if err != nil {
		return nil, err
	}
	defer waiter.Release()

	var timeoutOccurred bool
	if !waiter.Pending {
		deadline := time.Now().Add(timeout)
		if isInWorkflow {
			// Checkpoint the timeout deadline as a "DBOS.sleep" step before waiting. On
			// re-execution the recorded deadline is returned, so only the remaining time is waited.
			deadlineMs, txErr := runAsTxn(c, func(ctx context.Context, tx Tx) (int64, error) {
				return time.Now().Add(timeout).UnixMilli(), nil
			}, WithStepName("DBOS.sleep"), withNextStepID(sleepStepID))
			if txErr != nil {
				return nil, txErr
			}
			deadline = time.UnixMilli(deadlineMs)
		}
		// Wait for the event with no transaction open.
		timeoutOccurred, err = waiter.Wait(deadline)
		if err != nil {
			return nil, err
		}
	}

	// Use the event's serialization from the DB; fall back to the caller's format for the timeout/no-event case
	fallbackSerialization := resolveEncoder(c).Name()

	// If we aren't in a workflow, (attempt to) read and return the event
	if !isInWorkflow {
		var value, evtSerialization *string
		err := sysdb.Retry(c, func() error {
			var qErr error
			value, evtSerialization, qErr = c.systemDB.GetEventValue(c, nil, targetWorkflowID, key)
			return qErr
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			return nil, err
		}
		serialization := fallbackSerialization
		if evtSerialization != nil && len(*evtSerialization) > 0 {
			serialization = *evtSerialization
		}
		if value == nil && timeoutOccurred {
			return nil, models.NewTimeoutError("", "DBOS.getEvent", fmt.Sprintf("no event found for key '%s' within %v", key, timeout), nil)
		}
		return &getEventResult{value: value, serialization: serialization}, nil
	}

	// Read the event value and checkpoint the getEvent result in a single transaction.
	// If another executor already checkpointed this step, runAsTxn returns the recorded result.
	out, err := c.runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		value, evtSerialization, err := c.systemDB.GetEventValue(ctx, tx, targetWorkflowID, key)
		if err != nil {
			return nil, err
		}
		serialization := fallbackSerialization
		if evtSerialization != nil && len(*evtSerialization) > 0 {
			serialization = *evtSerialization
		}
		output := rawStepOutput{value: value, serialization: serialization}
		if value == nil && timeoutOccurred {
			return output, models.NewTimeoutError(workflowID, "DBOS.getEvent", fmt.Sprintf("no event found for key '%s' within %v", key, timeout), nil)
		}
		return output, nil
	}, WithStepName("DBOS.getEvent"), withNextStepID(stepID))

	switch v := out.(type) {
	case rawStepOutput: // executed now
		return &getEventResult{value: v.value, serialization: v.serialization}, err
	case stepCheckpointedOutcome: // replayed from a recorded checkpoint
		value, ok := v.value.(*string)
		if !ok {
			return nil, models.NewWorkflowExecutionError(workflowID, fmt.Errorf("getEvent checkpoint value is not *string, got %T", v.value))
		}
		return &getEventResult{value: value, serialization: v.serialization}, err
	case nil:
		return nil, err
	default:
		return nil, models.NewWorkflowUnexpectedResultType(workflowID, "rawStepOutput", fmt.Sprintf("%T", out))
	}
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
func GetEvent[R any](ctx Client, targetWorkflowID, key string, timeout time.Duration) (R, error) {
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
	if dc, ok := ctx.(*dbosContext); ok {
		result, ok := value.(*getEventResult)
		if !ok {
			workflowID, wfIDErr := dc.GetWorkflowID() // Must be within a workflow
			if wfIDErr != nil {
				dc.logger.Error("UNREACHABLE: failed to get workflow ID", "error", wfIDErr)
			}
			return *new(R), models.NewWorkflowUnexpectedResultType(workflowID, "*getEventResult", fmt.Sprintf("%T", value))
		}
		if result.value == nil {
			return *new(R), nil
		}
		evtDecoder, resolveErr := resolveDecoder[R](result.serialization, getCustomSerializerFromCtx(ctx))
		if resolveErr != nil {
			return *new(R), resolveErr
		}
		var decodeErr error
		typedValue, decodeErr = evtDecoder.Decode(result.value)
		if decodeErr != nil {
			return *new(R), fmt.Errorf("decoding event value to type %T: %w", *new(R), decodeErr)
		}
		return typedValue, nil
	} else {
		var ok bool
		typedValue, ok = value.(R)
		if !ok {
			var workflowID string
			if dc, isDBOSCtx := ctx.(Context); isDBOSCtx {
				var wfIDErr error
				workflowID, wfIDErr = dc.GetWorkflowID()
				if wfIDErr != nil {
					slog.Error("UNREACHABLE: failed to get workflow ID", "error", wfIDErr)
				}
			}
			return *new(R), models.NewWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", value))
		}
	}
	return typedValue, nil
}

// writeStreamOptions holds configuration for a WriteStream call.
type writeStreamOptions struct {
	usePortableSerializer bool
}

// WriteStreamOption is a functional option for configuring a WriteStream call.
type WriteStreamOption func(*writeStreamOptions)

// WithPortableWriteStream configures WriteStream to use the portable JSON serializer,
// enabling cross-language interoperability regardless of the workflow's serializer.
func WithPortableWriteStream() WriteStreamOption {
	return func(opts *writeStreamOptions) {
		opts.usePortableSerializer = true
	}
}

func (c *dbosContext) WriteStream(_ Context, key string, value any, opts ...WriteStreamOption) error {
	options := &writeStreamOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var ser Serializer[any]
	if options.usePortableSerializer {
		ser = newPortableSerializer[any]()
	} else {
		ser = resolveEncoder(c)
	}

	encodedValue, err := ser.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to serialize stream value: %w", err)
	}

	_, err = runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
		if !ok || wfState == nil {
			return "", fmt.Errorf("workflow state not found in context: are you running this within a workflow?")
		}
		return "", c.systemDB.WriteStream(ctx, sysdb.WriteStreamDBInput{
			Key:           key,
			Value:         encodedValue,
			Tx:            tx,
			Serialization: ser.Name(),
			WorkflowID:    wfState.workflowID,
			StepID:        wfState.stepID,
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
func WriteStream[P any](ctx Context, key string, value P, opts ...WriteStreamOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.WriteStream(ctx, key, value, opts...)
}

// ReadStreamOption is a functional option for ReadStream.
type ReadStreamOption func(*readStreamOptions)

type readStreamOptions struct {
	snapshot   bool
	fromOffset int
}

// WithReadStreamSnapshot makes a stream read return as soon as all currently-available
// values have been drained, instead of blocking until the stream is closed or
// the workflow becomes inactive.
func WithReadStreamSnapshot() ReadStreamOption {
	return func(o *readStreamOptions) {
		o.snapshot = true
	}
}

// WithReadStreamFromOffset sets the offset (0-based index) at which the read
// starts; values before it are skipped. Defaults to 0 (the start of the stream).
func WithReadStreamFromOffset(offset int) ReadStreamOption {
	return func(o *readStreamOptions) {
		o.fromOffset = offset
	}
}

// readStream runs the read stream polling logic in a goroutine
// and sends values through a channel as they're read
func (c *dbosContext) readStream(workflowID string, key string, snapshot bool, fromOffset int) <-chan StreamValue[any] {
	ch := make(chan StreamValue[any], 1) // Buffered to allow non-blocking sends

	go func() {
		defer close(ch)

		// send delivers v to ch, returning false if the context is cancelled first.
		// This prevents the goroutine from leaking when the consumer stops reading.
		send := func(v StreamValue[any]) bool {
			select {
			case ch <- v:
				return true
			case <-c.Done():
				return false
			}
		}

		currentOffset := fromOffset
		closed := false
		// finalRead is set once the producer is observed inactive; the loop then
		// makes one more read pass to drain any values it committed just before
		// terminating, then closes the stream.
		finalRead := false

		// Wake-up hint fired by the streams LISTEN/NOTIFY trigger when a value
		// is written. Readers of the same (workflowID, key) share one channel, so
		// a signal may be consumed by another reader and the first reader to
		// finish drops the registration for all of them. The bounded wait below
		// remains the fallback: workflow completion fires no stream notification,
		// and polling backends never signal (wakeCh stays nil there).
		wakeCh, cleanupWake := c.systemDB.StreamWakeChannel(workflowID, key)
		defer cleanupWake()

		// Continue reading until workflow is inactive or stream is closed
		for {
			// Clear any stale hint: the read below will pick up the rows it
			// signals. This clear prevents a spurious wake-up if we get race to this
			// point with the notification.
			select {
			case <-wakeCh:
			default:
			}

			// Read stream entries from current offset
			input := sysdb.ReadStreamDBInput{
				WorkflowID: workflowID,
				Key:        key,
				FromOffset: currentOffset,
			}

			var entries []sysdb.StreamEntry
			err := sysdb.Retry(c, func() error {
				var retryErr error
				entries, closed, retryErr = c.systemDB.ReadStream(c, input)
				return retryErr
			}, sysdb.WithRetrierLogger(c.logger))

			if err != nil {
				send(StreamValue[any]{Err: err})
				return
			}

			// Send each entry value to the channel
			for _, entry := range entries {
				if !send(StreamValue[any]{Value: streamEntryWithSerialization{value: entry.Value, serialization: entry.Serialization}}) {
					return
				}
				currentOffset = entry.Offset + 1 // Next offset to read from
			}

			// If stream is closed (sentinel found), send final message and stop
			if closed {
				send(StreamValue[any]{Closed: true})
				return
			}

			// Snapshot mode: all currently-available values have been drained,
			// so stop here instead of polling for more.
			if snapshot {
				return
			}

			// A previous iteration observed the workflow was inactive; this pass
			// has now drained anything it committed in the meantime, so close.
			if finalRead {
				send(StreamValue[any]{Closed: true})
				return
			}

			// Check if workflow is still active (PENDING or ENQUEUED)
			status, err := sysdb.RetryWithResult(c, func() (WorkflowStatusType, error) {
				workflows, err := c.systemDB.ListWorkflows(c, sysdb.ListWorkflowsDBInput{
					WorkflowIDs: []string{workflowID},
					LoadInput:   false,
					LoadOutput:  false,
				})
				if err != nil {
					return "", err
				}
				if len(workflows) == 0 {
					return "", models.NewNonExistentWorkflowError(workflowID)
				}
				return workflows[0].Status, nil
			}, sysdb.WithRetrierLogger(c.logger))

			if err != nil {
				send(StreamValue[any]{Err: err})
				return
			}

			// If the workflow is inactive it may still have committed values
			// between the read above and this status check. Once it is terminal
			// all of its writes are committed, so make one more read pass to drain
			// to the end of the stream before closing, rather than returning here
			// and dropping a value written just before completion.
			if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
				finalRead = true
				continue
			}

			// If no new entries, wait for a write notification, with a bounded
			// fallback to poll for workflow termination and missed notifications
			if len(entries) == 0 {
				select {
				case <-c.Done():
					send(StreamValue[any]{Err: c.Err()})
					return
				case <-wakeCh:
					// A value was written; read again immediately
				case <-time.After(sysdb.DBRetryInterval):
					// Continue loop to read again
				}
			}
		}
	}()

	return ch
}

// streamEntryWithSerialization wraps a stream value with its stored serialization format.
type streamEntryWithSerialization struct {
	value         string
	serialization string
}

func (c *dbosContext) ReadStream(_ Client, workflowID string, key string, opts ...ReadStreamOption) ([]any, bool, error) {
	var o readStreamOptions
	for _, opt := range opts {
		opt(&o)
	}

	var allValues []any
	closed := false

	ch := c.readStream(workflowID, key, o.snapshot, o.fromOffset)

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
// The stream is considered closed when the sentinel value is found or the workflow becomes inactive (status is not PENDING or ENQUEUED).
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
func ReadStream[R any](ctx Client, workflowID string, key string, opts ...ReadStreamOption) ([]R, bool, error) {
	if ctx == nil {
		return nil, false, errors.New("ctx cannot be nil")
	}
	values, closed, err := ctx.ReadStream(ctx, workflowID, key, opts...)
	if err != nil {
		return nil, false, err
	}

	// Decode each value using the serialization stored with that stream entry.
	typedValues := make([]R, len(values))
	if _, ok := ctx.(*dbosContext); ok {
		customSer := getCustomSerializerFromCtx(ctx)
		for i, val := range values {
			entry, ok := val.(streamEntryWithSerialization)
			if !ok {
				return nil, false, fmt.Errorf("stream value is not streamEntryWithSerialization, got %T", val)
			}
			decoder, resolveErr := resolveDecoder[R](entry.serialization, customSer)
			if resolveErr != nil {
				return nil, false, resolveErr
			}
			decodedValue, decodeErr := decoder.Decode(&entry.value)
			if decodeErr != nil {
				return nil, false, fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)
			}
			typedValues[i] = decodedValue
		}
	} else {
		// Fallback for testing/mocking scenarios
		for i, val := range values {
			typedVal, ok := val.(R)
			if !ok {
				return nil, false, fmt.Errorf("stream value is not %T, got %T", *new(R), val)
			}
			typedValues[i] = typedVal
		}
	}

	return typedValues, closed, nil
}

// ReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
func (c *dbosContext) ReadStreamAsync(_ Client, workflowID string, key string) (<-chan StreamValue[any], error) {
	return c.readStream(workflowID, key, false, 0), nil
}

// ReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
//
// This method returns immediately with a channel. Values will be sent to the channel
// as they're read from the stream. The channel will be closed when the stream is closed or an error occurs.
// The stream is considered closed when the sentinel value is found or the workflow becomes inactive (status is not PENDING or ENQUEUED).
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
func ReadStreamAsync[R any](ctx Client, workflowID string, key string) (<-chan StreamValue[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyCh, err := ctx.ReadStreamAsync(ctx, workflowID, key)
	if err != nil {
		return nil, err
	}

	typedCh := make(chan StreamValue[R], 1)

	_, isReal := ctx.(*dbosContext)

	go func() {
		defer close(typedCh)

		send := func(v StreamValue[R]) bool {
			select {
			case typedCh <- v:
				return true
			case <-ctx.Done():
				return false
			}
		}

		customSer := getCustomSerializerFromCtx(ctx)

		for streamValue := range anyCh {
			if streamValue.Err != nil {
				send(StreamValue[R]{Err: streamValue.Err})
				return
			}

			if streamValue.Closed {
				send(StreamValue[R]{Closed: true})
				return
			}

			if isReal {
				entry, ok := streamValue.Value.(streamEntryWithSerialization)
				if !ok {
					send(StreamValue[R]{Err: fmt.Errorf("stream value is not streamEntryWithSerialization, got %T", streamValue.Value)})
					return
				}

				asyncDecoder, resolveErr := resolveDecoder[R](entry.serialization, customSer)
				if resolveErr != nil {
					send(StreamValue[R]{Err: resolveErr})
					return
				}

				decodedValue, decodeErr := asyncDecoder.Decode(&entry.value)
				if decodeErr != nil {
					send(StreamValue[R]{Err: fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)})
					return
				}

				if !send(StreamValue[R]{Value: decodedValue}) {
					return
				}
			} else {
				// Fallback for testing/mocking scenarios
				typedVal, ok := streamValue.Value.(R)
				if !ok {
					send(StreamValue[R]{Err: fmt.Errorf("stream value is not %T, got %T", *new(R), streamValue.Value)})
					return
				}
				if !send(StreamValue[R]{Value: typedVal}) {
					return
				}
			}
		}
	}()

	return typedCh, nil
}

func (c *dbosContext) CloseStream(_ Context, key string) error {
	_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		sentinel := sysdb.StreamClosedSentinel
		wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
		if !ok || wfState == nil {
			return "", fmt.Errorf("workflow state not found in context: are you running this within a workflow?")
		}
		return "", c.systemDB.WriteStream(ctx, sysdb.WriteStreamDBInput{
			Key:        key,
			Value:      &sentinel,
			Tx:         tx,
			WorkflowID: wfState.workflowID,
			StepID:     wfState.stepID,
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
func CloseStream(ctx Context, key string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CloseStream(ctx, key)
}

func (c *dbosContext) Sleep(_ Context, duration time.Duration) (time.Duration, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, models.NewStepExecutionError("", "DBOS.sleep", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return 0, models.NewStepExecutionError(wfState.workflowID, "DBOS.sleep", fmt.Errorf("cannot call Sleep within a step"))
	}
	// Checkpoint the wakeup time as a "DBOS.sleep" step; on re-execution the
	// recorded deadline is returned, so only the remaining duration is slept.
	deadlineMs, err := runAsTxn(c, func(ctx context.Context, tx Tx) (int64, error) {
		return time.Now().Add(duration).UnixMilli(), nil
	}, WithStepName("DBOS.sleep"))
	if err != nil {
		return 0, err
	}
	remainingDuration := max(0, time.Until(time.UnixMilli(deadlineMs)))

	// Sleep for the remaining duration, but wake early if the context is cancelled.
	// If interrupted, return the duration actually slept.
	sleepStart := time.Now()
	timer := time.NewTimer(remainingDuration)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-c.Done():
		return time.Since(sleepStart), c.Err()
	}
	return remainingDuration, nil
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
func Sleep(ctx Context, duration time.Duration) (time.Duration, error) {
	if ctx == nil {
		return 0, errors.New("ctx cannot be nil")
	}
	return ctx.Sleep(ctx, duration)
}

const _DBOS_PATCH_PREFIX = "DBOS.patch-"

func (c *dbosContext) Patch(_ Context, patchName string) (bool, error) {
	if !c.config.EnablePatching {
		return false, models.NewPatchingNotEnabledError()
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
		return false, models.NewStepExecutionError(wfState.workflowID, patchName, fmt.Errorf("cannot call Patch within a step"))
	}

	// Automatically prefix the patch name with _DBOS_PATCH_PREFIX
	prefixedPatchName := _DBOS_PATCH_PREFIX + patchName

	patched, err := sysdb.RetryWithResult(c, func() (bool, error) {
		return c.systemDB.Patch(c, sysdb.PatchDBInput{
			WorkflowID: wfState.workflowID,
			StepID:     wfState.stepID + 1, // We are checking if the upcoming step should use the patched code
			PatchName:  prefixedPatchName,
		})
	}, sysdb.WithRetrierLogger(c.logger))

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
//	patched, err := dbos.Patch(ctx, "my-patch")
//	if err != nil {
//	    return err
//	}
//	if patched {
//	    // New code path
//	} else {
//	    // Old code path
//	}
func Patch(ctx Context, patchName string) (bool, error) {
	if ctx == nil {
		return false, errors.New("ctx cannot be nil")
	}
	return ctx.Patch(ctx, patchName)
}

func (c *dbosContext) DeprecatePatch(_ Context, patchName string) error {
	if !c.config.EnablePatching {
		return models.NewPatchingNotEnabledError()
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
		return models.NewStepExecutionError(wfState.workflowID, patchName, fmt.Errorf("cannot call DeprecatePatch within a step"))
	}

	// Automatically prefix the patch name with _DBOS_PATCH_PREFIX
	prefixedPatchName := _DBOS_PATCH_PREFIX + patchName

	patchNameFromDB, err := sysdb.RetryWithResult(c, func() (string, error) {
		return c.systemDB.DoesPatchExists(c, sysdb.PatchDBInput{
			WorkflowID: wfState.workflowID,
			StepID:     wfState.stepID + 1,
			PatchName:  prefixedPatchName,
		})
	}, sysdb.WithRetrierLogger(c.logger))

	if err != nil {
		// If patch doesn't exist, it's already deprecated (or never existed)
		if errors.Is(err, sysdb.ErrNoRows) {
			return nil
		}
		return err
	}

	// Patch exists, deprecate it by incrementing step ID
	if patchNameFromDB == prefixedPatchName {
		wfState.nextStepID()
	}
	return nil
}

// DeprecatePatch allows removing patches from code while ensuring the correct history
// of workflows that were executing before the patch was deprecated.
//
// Example:
//
//	err := dbos.DeprecatePatch(ctx, "my-patch")
//	if err != nil {
//	    return err
//	}
//	// New code path
func DeprecatePatch(ctx Context, patchName string) error {
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
func GetWorkflowID(ctx Context) (string, error) {
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
func GetStepID(ctx Context) (int, error) {
	if ctx == nil {
		return -1, errors.New("ctx cannot be nil")
	}
	return ctx.GetStepID()
}

func (c *dbosContext) RetrieveWorkflow(_ Client, workflowID string) (WorkflowHandle[any], error) {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var workflowStatus []WorkflowStatus
	var err error
	if isWithinWorkflow {
		workflowStatus, err = RunAsStep(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return sysdb.RetryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
					WorkflowIDs: []string{workflowID},
					LoadInput:   false,
					LoadOutput:  false,
				})
			}, sysdb.WithRetrierLogger(c.logger))
		}, WithStepName("DBOS.retrieveWorkflow"))
	} else {
		workflowStatus, err = sysdb.RetryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.systemDB.ListWorkflows(c, sysdb.ListWorkflowsDBInput{
				WorkflowIDs: []string{workflowID},
				LoadInput:   false,
				LoadOutput:  false,
			})
		}, sysdb.WithRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, models.NewNonExistentWorkflowError(workflowID)
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
func RetrieveWorkflow[R any](ctx Client, workflowID string) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("dbosCtx cannot be nil")
	}

	// Call the interface method
	handle, err := ctx.RetrieveWorkflow(ctx, workflowID)
	if err != nil {
		return nil, err
	}

	// Convert to typed polling handle
	return typedHandle[R](ctx, handle), nil
}

// WithCancelChildren enables cancellation for children workflows
func WithCancelChildren() CancelWorkflowOption {
	return func(cwo *models.CancelWorkflowInput) {
		cwo.CancelChildren = true
	}
}

func (c *dbosContext) CancelWorkflow(_ Client, workflowID string, opts ...CancelWorkflowOption) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var found []string
	var err error
	cwo := models.CancelWorkflowInput{}
	for _, opt := range opts {
		opt(&cwo)
	}

	if isWithinWorkflow {
		found, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			return c.systemDB.CancelWorkflows(ctx, sysdb.CancelWorkflowsDBInput{
				WorkflowIDs:    []string{workflowID},
				CancelChildren: cwo.CancelChildren,
				Tx:             tx,
			})
		}, WithStepName("DBOS.cancelWorkflow"))
	} else {
		found, err = sysdb.RetryWithResult(c, func() ([]string, error) {
			return c.systemDB.CancelWorkflows(c, sysdb.CancelWorkflowsDBInput{
				WorkflowIDs:    []string{workflowID},
				CancelChildren: cwo.CancelChildren,
			})
		}, sysdb.WithRetrierLogger(c.logger))
	}
	if err != nil {
		return err
	}
	if len(found) == 0 {
		return models.NewNonExistentWorkflowError(workflowID)
	}
	return nil
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
func CancelWorkflow(ctx Client, workflowID string, opts ...CancelWorkflowOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}

	return ctx.CancelWorkflow(ctx, workflowID, opts...)
}

func (c *dbosContext) SetWorkflowAttributes(_ Client, workflowID string, attributes map[string]any) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil

	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (struct{}, error) {
			return struct{}{}, c.systemDB.SetWorkflowAttributes(ctx, sysdb.SetWorkflowAttributesDBInput{
				WorkflowID: workflowID,
				Attributes: attributes,
				Tx:         tx,
			})
		}, WithStepName("DBOS.updateWorkflowAttributes"))
		return err
	}
	return sysdb.Retry(c, func() error {
		return c.systemDB.SetWorkflowAttributes(c, sysdb.SetWorkflowAttributesDBInput{
			WorkflowID: workflowID,
			Attributes: attributes,
		})
	}, sysdb.WithRetrierLogger(c.logger))
}

// SetWorkflowAttributes replaces the custom attributes attached to an existing
// workflow, identified by workflowID. Pass a nil attributes map to clear all
// attributes. Attributes must be JSON-serializable.
//
// Returns an error if the workflow does not exist or the update fails.
//
// Example:
//
//	err := dbos.SetWorkflowAttributes(ctx, "my-workflow-id", map[string]any{"customer": "acme"})
func SetWorkflowAttributes(ctx Client, workflowID string, attributes map[string]any) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetWorkflowAttributes(ctx, workflowID, attributes)
}

func (c *dbosContext) CancelWorkflows(_ Client, workflowIDs []string, opts ...CancelWorkflowOption) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	cwo := models.CancelWorkflowInput{}
	for _, opt := range opts {
		opt(&cwo)
	}

	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			return c.systemDB.CancelWorkflows(ctx, sysdb.CancelWorkflowsDBInput{CancelChildren: cwo.CancelChildren, WorkflowIDs: workflowIDs, Tx: tx})
		}, WithStepName("DBOS.cancelWorkflows"))
		return err
	}
	_, err := sysdb.RetryWithResult(c, func() ([]string, error) {
		return c.systemDB.CancelWorkflows(c, sysdb.CancelWorkflowsDBInput{CancelChildren: cwo.CancelChildren, WorkflowIDs: workflowIDs})
	}, sysdb.WithRetrierLogger(c.logger))
	return err
}

// CancelWorkflows cancels multiple workflows in a single database round-trip.
// Each workflow that exists and is not already in a terminal state (SUCCESS, ERROR, CANCELLED)
// is moved to CANCELLED and removed from its queue. Missing or already-terminal IDs are silently
// skipped. Unlike the singular CancelWorkflow, this function does not return
// ErrorCodeNonExistentWorkflow when some IDs are missing.
//
// Example:
//
//	err := dbos.CancelWorkflows(ctx, []string{"wf-1", "wf-2"})
//	if err != nil {
//	    log.Fatal(err)
//	}
func CancelWorkflows(ctx Client, workflowIDs []string, opts ...CancelWorkflowOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CancelWorkflows(ctx, workflowIDs, opts...)
}

// SetWorkflowDelayOption configures how the delay is set on a workflow.
type SetWorkflowDelayOption func(*setWorkflowDelayOptions)

type setWorkflowDelayOptions struct {
	delay      time.Duration
	delayUntil time.Time
}

// WithDelayDuration sets a relative delay from now.
func WithDelayDuration(d time.Duration) SetWorkflowDelayOption {
	return func(o *setWorkflowDelayOptions) {
		o.delay = d
	}
}

// WithDelayUntil sets an absolute time until which the workflow should remain delayed.
func WithDelayUntil(t time.Time) SetWorkflowDelayOption {
	return func(o *setWorkflowDelayOptions) {
		o.delayUntil = t
	}
}

func resolveDelayUntil(opts []SetWorkflowDelayOption) (time.Time, error) {
	params := &setWorkflowDelayOptions{}
	for _, opt := range opts {
		opt(params)
	}
	hasDelay := params.delay > 0
	hasUntil := !params.delayUntil.IsZero()
	if hasDelay && hasUntil {
		return time.Time{}, models.NewInvalidOptionError("specify either WithDelayDuration or WithDelayUntil, not both")
	}
	if !hasDelay && !hasUntil {
		return time.Time{}, models.NewInvalidOptionError("must specify either WithDelayDuration or WithDelayUntil")
	}
	if hasDelay {
		return time.Now().Add(params.delay), nil
	}
	return params.delayUntil, nil
}

func (c *dbosContext) SetWorkflowDelay(_ Client, workflowID string, opts ...SetWorkflowDelayOption) error {
	delayUntil, err := resolveDelayUntil(opts)
	if err != nil {
		return err
	}
	input := sysdb.SetWorkflowDelayDBInput{WorkflowID: workflowID, DelayUntil: delayUntil}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			input.Tx = tx
			return nil, c.systemDB.SetWorkflowDelay(ctx, input)
		}, WithStepName("DBOS.setWorkflowDelay"))
		return err
	}
	return sysdb.Retry(c, func() error {
		return c.systemDB.SetWorkflowDelay(c, input)
	}, sysdb.WithRetrierLogger(c.logger))
}

// SetWorkflowDelay sets or updates the delay on a DELAYED workflow.
// Provide exactly one of WithDelayDuration (relative) or WithDelayUntil (absolute).
// Only affects workflows in the DELAYED status.
//
// Example:
//
//	err := dbos.SetWorkflowDelay(ctx, workflowID, dbos.WithDelayDuration(5*time.Second))
//	err := dbos.SetWorkflowDelay(ctx, workflowID, dbos.WithDelayUntil(time.Now().Add(10*time.Minute)))
func SetWorkflowDelay(ctx Client, workflowID string, opts ...SetWorkflowDelayOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetWorkflowDelay(ctx, workflowID, opts...)
}

func (c *dbosContext) DeleteWorkflows(_ Client, workflowIDs []string, opts ...DeleteWorkflowOption) error {
	// Process options
	params := &deleteWorkflowOptions{}
	for _, opt := range opts {
		opt(params)
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			err := c.systemDB.DeleteWorkflows(ctx, sysdb.DeleteWorkflowsDBInput{
				WorkflowIDs:    workflowIDs,
				DeleteChildren: params.deleteChildren,
				Tx:             tx,
			})
			return "", err
		}, WithStepName("DBOS.deleteWorkflows"))
		return err
	} else {
		return sysdb.Retry(c, func() error {
			return c.systemDB.DeleteWorkflows(c, sysdb.DeleteWorkflowsDBInput{
				WorkflowIDs:    workflowIDs,
				DeleteChildren: params.deleteChildren,
			})
		}, sysdb.WithRetrierLogger(c.logger))
	}
}

// deleteWorkflowOptions holds configuration parameters for deleting workflows.
type deleteWorkflowOptions struct {
	deleteChildren bool
}

// DeleteWorkflowOption is a functional option for configuring workflow deletion.
type DeleteWorkflowOption func(*deleteWorkflowOptions)

// WithDeleteChildren enables recursive deletion of child workflows.
// When set, all child workflows (and their children, recursively) will be deleted
// along with the parent workflow.
func WithDeleteChildren() DeleteWorkflowOption {
	return func(o *deleteWorkflowOptions) {
		o.deleteChildren = true
	}
}

// DeleteWorkflows permanently deletes one or more workflows and all their associated data
// from the database, regardless of their current status. This includes active (PENDING, ENQUEUED) workflows.
//
// This operation is irreversible and removes the workflow status, operation outputs,
// events, event history, and streams associated with each workflow.
//
// Options:
//   - WithDeleteChildren: Also delete all child workflows recursively
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowIDs: The unique identifiers of the workflows to delete
//
// Example:
//
//	// Delete a single workflow
//	err := dbos.DeleteWorkflows(ctx, []string{"workflow-to-delete"})
//
//	// Delete workflows and all their children
//	err := dbos.DeleteWorkflows(ctx, []string{"wf1", "wf2"}, dbos.WithDeleteChildren())
func DeleteWorkflows(ctx Client, workflowIDs []string, opts ...DeleteWorkflowOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.DeleteWorkflows(ctx, workflowIDs, opts...)
}

// WithResumeQueue re-enqueues the resumed workflow(s) on the specified queue instead of the internal queue.
func WithResumeQueue(queueName string) ResumeWorkflowOption {
	return func(o *models.ResumeWorkflowInput) {
		o.QueueName = queueName
	}
}

func (c *dbosContext) ResumeWorkflow(_ Client, workflowID string, opts ...ResumeWorkflowOption) (WorkflowHandle[any], error) {
	handles, err := c.ResumeWorkflows(c, []string{workflowID}, opts...)
	if err != nil {
		return nil, err
	}
	if len(handles) == 0 {
		return nil, models.NewNonExistentWorkflowError(workflowID)
	}
	return handles[0], nil
}

func (c *dbosContext) ResumeWorkflows(_ Client, workflowIDs []string, opts ...ResumeWorkflowOption) ([]WorkflowHandle[any], error) {
	params := &models.ResumeWorkflowInput{}
	for _, opt := range opts {
		opt(params)
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var foundIDs []string
	var err error
	if isWithinWorkflow {
		foundIDs, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			return c.systemDB.ResumeWorkflows(ctx, sysdb.ResumeWorkflowsDBInput{
				WorkflowIDs: workflowIDs,
				QueueName:   params.QueueName,
				Tx:          tx,
			})
		}, WithStepName("DBOS.resumeWorkflow"))
	} else {
		foundIDs, err = sysdb.RetryWithResult(c, func() ([]string, error) {
			return c.systemDB.ResumeWorkflows(c, sysdb.ResumeWorkflowsDBInput{
				WorkflowIDs: workflowIDs,
				QueueName:   params.QueueName,
			})
		}, sysdb.WithRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	handles := make([]WorkflowHandle[any], 0, len(foundIDs))
	for _, id := range foundIDs {
		handles = append(handles, newWorkflowPollingHandle[any](c, id))
	}
	return handles, nil
}

// ResumeWorkflow resumes a workflow by starting it from its last completed step.
// You can use this to resume workflows that are cancelled or have exceeded their maximum
// recovery attempts. You can also use this to start an enqueued workflow immediately,
// bypassing its queue.
// If the workflow is already completed, this is a no-op.
// Returns a handle that can be used to wait for completion and retrieve results.
// Returns an error if the workflow does not exist or if the operation fails.
//
// Options:
//   - WithResumeQueue: re-enqueue the workflow on a named queue instead of the internal queue.
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
func ResumeWorkflow[R any](ctx Client, workflowID string, opts ...ResumeWorkflowOption) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	handle, err := ctx.ResumeWorkflow(ctx, workflowID, opts...)
	if err != nil {
		return nil, err
	}
	return typedHandle[R](ctx, handle), nil
}

// ResumeWorkflows resumes multiple workflows in a single database round-trip. Each workflow
// that exists and is not in a terminal state is re-enqueued; completed or missing workflows
// are skipped.
//
// Unlike the singular ResumeWorkflow, this function does not return ErrorCodeNonExistentWorkflow
// when some IDs are missing.
//
// Options:
//   - WithResumeQueue: re-enqueue the workflows on a named queue instead of the internal queue.
//
// Example:
//
//	handles, err := dbos.ResumeWorkflows[int](ctx, []string{"wf-1", "wf-2"}, dbos.WithResumeQueue("priority"))
//	if err != nil {
//	    log.Fatal(err)
//	}
func ResumeWorkflows[R any](ctx Client, workflowIDs []string, opts ...ResumeWorkflowOption) ([]WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyHandles, err := ctx.ResumeWorkflows(ctx, workflowIDs, opts...)
	if err != nil {
		return nil, err
	}
	handles := make([]WorkflowHandle[R], 0, len(anyHandles))
	for _, h := range anyHandles {
		handles = append(handles, typedHandle[R](ctx, h))
	}
	return handles, nil
}

// ForkWorkflowSpec describes a single workflow to fork within a batch.
// OriginalWorkflowID is required. Other fields are optional.
type ForkWorkflowSpec struct {
	OriginalWorkflowID string // Required: The UUID of the original workflow to fork from
	ForkedWorkflowID   string // Optional: Custom workflow ID for the forked workflow (auto-generated if empty)
	StartStep          uint   // Optional: Step to start the forked workflow from (default: 0)
}

// ForkWorkflowsInput holds configuration parameters for forking a batch of
// workflows in a single database round-trip. Workflows is required. The
// ApplicationVersion, QueueName, and QueuePartitionKey fields apply to every
// forked workflow in the batch.
type ForkWorkflowsInput struct {
	Workflows          []ForkWorkflowSpec // Required: The workflows to fork
	ApplicationVersion string             // Optional: Application version for the forked workflows (inherits from originals if empty)
	QueueName          string             // Optional: Queue to enqueue the forked workflows on (defaults to the internal queue)
	QueuePartitionKey  string             // Optional: Partition key when enqueueing the forked workflows onto a partitioned queue
}

func (c *dbosContext) ForkWorkflow(_ Client, input ForkWorkflowInput) (WorkflowHandle[any], error) {
	handles, err := c.ForkWorkflows(c, ForkWorkflowsInput{
		Workflows: []ForkWorkflowSpec{{
			OriginalWorkflowID: input.OriginalWorkflowID,
			ForkedWorkflowID:   input.ForkedWorkflowID,
			StartStep:          input.StartStep,
		}},
		ApplicationVersion: input.ApplicationVersion,
		QueueName:          input.QueueName,
		QueuePartitionKey:  input.QueuePartitionKey,
	})
	if err != nil {
		return nil, err
	}
	return handles[0], nil
}

func (c *dbosContext) ForkWorkflows(_ Client, input ForkWorkflowsInput) ([]WorkflowHandle[any], error) {
	if len(input.Workflows) == 0 {
		return nil, models.NewInvalidOptionError("at least one workflow to fork is required")
	}
	if input.QueuePartitionKey != "" && input.QueueName == "" {
		return nil, models.NewInvalidOptionError("queue partition key requires a queue name")
	}

	// Build the system database input, validating each workflow spec.
	originalWorkflowIDs := make([]string, len(input.Workflows))
	forkedWorkflowIDs := make([]string, len(input.Workflows))
	startSteps := make([]int, len(input.Workflows))
	for i, wf := range input.Workflows {
		if wf.OriginalWorkflowID == "" {
			return nil, models.NewInvalidOptionError("original workflow ID cannot be empty")
		}
		if wf.StartStep > uint(math.MaxInt) {
			return nil, models.NewInvalidOptionError(fmt.Sprintf("start step too large: %d", wf.StartStep))
		}
		originalWorkflowIDs[i] = wf.OriginalWorkflowID
		forkedWorkflowIDs[i] = wf.ForkedWorkflowID
		startSteps[i] = int(wf.StartStep)
	}
	dbInput := sysdb.ForkWorkflowsDBInput{
		OriginalWorkflowIDs: originalWorkflowIDs,
		ForkedWorkflowIDs:   forkedWorkflowIDs,
		StartSteps:          startSteps,
		ApplicationVersion:  input.ApplicationVersion,
		QueueName:           input.QueueName,
		QueuePartitionKey:   input.QueuePartitionKey,
	}

	// Call system database method
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	forkBatch := func(ctx context.Context) ([]string, error) {
		return c.systemDB.ForkWorkflows(ctx, dbInput)
	}
	var forkedIDs []string
	var err error
	if isWithinWorkflow {
		forkedIDs, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			dbInput.Tx = tx
			return forkBatch(ctx)
		}, WithStepName("DBOS.forkWorkflow"))
	} else {
		uncancellableCtx := WithoutCancel(c)
		forkedIDs, err = sysdb.RetryWithResult(c, func() ([]string, error) {
			return forkBatch(uncancellableCtx)
		}, sysdb.WithRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	handles := make([]WorkflowHandle[any], len(forkedIDs))
	for i, id := range forkedIDs {
		handles[i] = newWorkflowPollingHandle[any](c, id)
	}
	return handles, nil
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
//
//	// Fork onto a named queue instead of the internal queue.
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    QueueName:          "priority",
//	})
func ForkWorkflow[R any](ctx Client, input ForkWorkflowInput) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	handle, err := ctx.ForkWorkflow(ctx, input)
	if err != nil {
		return nil, err
	}
	return typedHandle[R](ctx, handle), nil
}

// ForkWorkflows forks a batch of workflows in a single database round-trip.
// Each forked workflow gets a new UUID (unless a custom ForkedWorkflowID is
// provided) and executes from its specified StartStep, reusing the operation
// outputs of steps 0 to StartStep-1 copied from the original workflow.
//
// The returned handles are in the same order as input.Workflows.
//
// Example usage:
//
//	handles, err := dbos.ForkWorkflows[MyResultType](ctx, dbos.ForkWorkflowsInput{
//	    Workflows: []dbos.ForkWorkflowSpec{
//	        {OriginalWorkflowID: "wf-1", StartStep: 5},
//	        {OriginalWorkflowID: "wf-2"},
//	    },
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
func ForkWorkflows[R any](ctx Client, input ForkWorkflowsInput) ([]WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	handles, err := ctx.ForkWorkflows(ctx, input)
	if err != nil {
		return nil, err
	}
	typedHandles := make([]WorkflowHandle[R], len(handles))
	for i, handle := range handles {
		typedHandles[i] = typedHandle[R](ctx, handle)
	}
	return typedHandles, nil
}

// WithFilterWorkflowIDs filters workflows by the specified workflow IDs.
func WithFilterWorkflowIDs(workflowIDs ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.WorkflowIDs = workflowIDs
	}
}

// WithFilterStatus filters workflows by the specified list of statuses.
func WithFilterStatus(status ...WorkflowStatusType) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.Status = status
	}
}

// WithFilterCreatedAfter filters workflows created after the specified time.
func WithFilterCreatedAfter(createdAfter time.Time) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.StartTime = createdAfter
	}
}

// WithFilterCreatedBefore filters workflows created before the specified time.
func WithFilterCreatedBefore(createdBefore time.Time) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.EndTime = createdBefore
	}
}

// WithFilterName filters workflows by the specified workflow function name(s).
func WithFilterName(name ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.Name = name
	}
}

// WithFilterAppVersion filters workflows by the specified application version(s).
func WithFilterAppVersion(appVersion ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.AppVersion = appVersion
	}
}

// WithFilterUser filters workflows by the specified authenticated user(s).
func WithFilterUser(user ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.User = user
	}
}

// WithFilterLimit limits the number of workflows returned.
func WithFilterLimit(limit int) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.Limit = &limit
	}
}

// WithFilterOffset sets the offset for pagination.
func WithFilterOffset(offset int) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.Offset = &offset
	}
}

// WithFilterSortDesc enables descending sort by creation time (default is ascending).
func WithFilterSortDesc() ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.SortDesc = true
	}
}

// WithFilterWorkflowIDPrefix filters workflows by workflow ID prefix(es).
func WithFilterWorkflowIDPrefix(prefix ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.WorkflowIDPrefix = prefix
	}
}

// WithFilterLoadInput controls whether to load workflow input data (default: true).
func WithFilterLoadInput(loadInput bool) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.LoadInput = loadInput
	}
}

// WithFilterLoadOutput controls whether to load workflow output data (default: true).
func WithFilterLoadOutput(loadOutput bool) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.LoadOutput = loadOutput
	}
}

// WithFilterQueueName filters workflows by the specified queue name(s).
// This is typically used when listing queued workflows.
func WithFilterQueueName(queueName ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.QueueName = queueName
	}
}

// WithFilterQueuesOnly filters to only return workflows that are in a queue.
func WithFilterQueuesOnly() ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.QueuesOnly = true
	}
}

// WithFilterExecutorIDs filters workflows by the specified executor IDs.
func WithFilterExecutorIDs(executorIDs ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.ExecutorIDs = executorIDs
	}
}

// WithFilterForkedFrom filters workflows by the specified forked_from workflow ID(s).
func WithFilterForkedFrom(forkedFrom ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.ForkedFrom = forkedFrom
	}
}

// WithFilterParentWorkflowID filters workflows by the specified parent workflow ID(s).
func WithFilterParentWorkflowID(parentWorkflowID ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.ParentWorkflowID = parentWorkflowID
	}
}

// WithFilterDeduplicationID filters workflows by the specified deduplication ID(s).
func WithFilterDeduplicationID(deduplicationID ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.DeduplicationID = deduplicationID
	}
}

// WithFilterCompletedAfter filters workflows that reached a terminal state at or after the specified time.
func WithFilterCompletedAfter(completedAfter time.Time) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.CompletedAfter = completedAfter
	}
}

// WithFilterCompletedBefore filters workflows that reached a terminal state at or before the specified time.
func WithFilterCompletedBefore(completedBefore time.Time) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.CompletedBefore = completedBefore
	}
}

// WithFilterDequeuedAfter filters workflows that started executing at or after the specified time.
func WithFilterDequeuedAfter(dequeuedAfter time.Time) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.DequeuedAfter = dequeuedAfter
	}
}

// WithFilterDequeuedBefore filters workflows that started executing at or before the specified time.
func WithFilterDequeuedBefore(dequeuedBefore time.Time) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.DequeuedBefore = dequeuedBefore
	}
}

// WithFilterWasForkedFrom filters workflows by whether they have been forked from (true) or not (false).
func WithFilterWasForkedFrom(wasForkedFrom bool) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.WasForkedFrom = &wasForkedFrom
	}
}

// WithFilterHasParent filters workflows by whether they have a parent workflow (true) or not (false).
func WithFilterHasParent(hasParent bool) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.HasParent = &hasParent
	}
}

// WithFilterAttributes filters workflows whose attributes contain all the given
// key-value pairs (JSONB containment). Requires a Postgres system database;
// listing fails with an error on SQLite.
func WithFilterAttributes(attributes map[string]any) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.Attributes = attributes
	}
}

// WithFilterScheduleName filters workflows by the name(s) of the schedule that
// enqueued them. Only workflows enqueued by a named schedule match.
func WithFilterScheduleName(scheduleName ...string) ListWorkflowsOption {
	return func(p *models.ListWorkflowsInput) {
		p.ScheduleName = scheduleName
	}
}

func (c *dbosContext) ListWorkflows(_ Client, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	// Initialize parameters with defaults
	loadInput := true
	loadOutput := true
	if !c.launched.Load() {
		loadInput = false
		loadOutput = false
	}
	params := &models.ListWorkflowsInput{
		LoadInput:  loadInput,
		LoadOutput: loadOutput,
	}

	// Apply all provided options
	for _, opt := range opts {
		opt(params)
	}

	// If we are asked to retrieve only queue workflows with no status, only fetch ENQUEUED, PENDING, and DELAYED tasks
	if params.QueuesOnly && len(params.Status) == 0 {
		params.Status = []WorkflowStatusType{WorkflowStatusEnqueued, WorkflowStatusPending, WorkflowStatusDelayed}
	}

	// Convert to system database input structure
	dbInput := sysdb.ListWorkflowsDBInput{
		WorkflowIDs:        params.WorkflowIDs,
		Status:             params.Status,
		StartTime:          params.StartTime,
		EndTime:            params.EndTime,
		WorkflowName:       params.Name,
		ApplicationVersion: params.AppVersion,
		AuthenticatedUser:  params.User,
		Limit:              params.Limit,
		Offset:             params.Offset,
		SortDesc:           params.SortDesc,
		WorkflowIDPrefix:   params.WorkflowIDPrefix,
		LoadInput:          params.LoadInput,
		LoadOutput:         params.LoadOutput,
		QueueName:          params.QueueName,
		QueuesOnly:         params.QueuesOnly,
		ExecutorIDs:        params.ExecutorIDs,
		ForkedFrom:         params.ForkedFrom,
		ParentWorkflowID:   params.ParentWorkflowID,
		DeduplicationID:    params.DeduplicationID,
		CompletedAfter:     params.CompletedAfter,
		CompletedBefore:    params.CompletedBefore,
		DequeuedAfter:      params.DequeuedAfter,
		DequeuedBefore:     params.DequeuedBefore,
		WasForkedFrom:      params.WasForkedFrom,
		HasParent:          params.HasParent,
		Attributes:         params.Attributes,
		ScheduleName:       params.ScheduleName,
	}

	// Call the context method to list workflows
	var workflows []WorkflowStatus
	var err error
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		// Decode inside the step so the checkpoint records decoded values: a
		// recovery replay returns the recorded output as-is, and the raw
		// encoded *string columns do not survive checkpoint serialization.
		workflows, err = RunAsStep(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			listed, err := sysdb.RetryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.systemDB.ListWorkflows(ctx, dbInput)
			}, sysdb.WithRetrierLogger(c.logger))
			if err != nil {
				return nil, err
			}
			if err := c.decodeWorkflowsInputOutput(listed, params.LoadInput, params.LoadOutput); err != nil {
				return nil, err
			}
			return listed, nil
		}, WithStepName("DBOS.listWorkflows"))
		if err != nil {
			return nil, err
		}
		return workflows, nil
	}

	workflows, err = sysdb.RetryWithResult(c, func() ([]WorkflowStatus, error) {
		return c.systemDB.ListWorkflows(c, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}

	// Deserialize Input and Output fields if they were loaded
	if err := c.decodeWorkflowsInputOutput(workflows, params.LoadInput, params.LoadOutput); err != nil {
		return nil, err
	}

	return workflows, nil
}

// decodeWorkflowsInputOutput decodes the raw encoded input/output columns loaded from the system database.
func (c *dbosContext) decodeWorkflowsInputOutput(workflows []WorkflowStatus, loadInput, loadOutput bool) error {
	if loadInput || loadOutput {
		for i := range workflows {
			if loadInput && workflows[i].Input != nil {
				encodedInput, ok := workflows[i].Input.(*string)
				if !ok {
					return fmt.Errorf("workflow input must be encoded string, got %T", workflows[i].Input)
				}
				if encodedInput == nil || *encodedInput == nilMarker {
					workflows[i].Input = nil
				} else {
					decoded, err := decodeListingValue(encodedInput, workflows[i].Serialization, c.serializer)
					if err != nil {
						c.logger.Warn("failed to decode workflow input, storing raw value", "workflow_id", workflows[i].ID, "error", err)
					}
					workflows[i].Input = decoded
				}
			}
			if loadOutput && workflows[i].Output != nil {
				encodedOutput, ok := workflows[i].Output.(*string)
				if !ok {
					return fmt.Errorf("workflow output must be encoded *string, got %T", workflows[i].Output)
				}
				if encodedOutput == nil || *encodedOutput == nilMarker {
					workflows[i].Output = nil
				} else {
					decoded, err := decodeListingValue(encodedOutput, workflows[i].Serialization, c.serializer)
					if err != nil {
						c.logger.Warn("failed to decode workflow output, storing raw value", "workflow_id", workflows[i].ID, "error", err)
					}
					workflows[i].Output = decoded
				}
			}
			if loadOutput && workflows[i].Error != nil {
				s := workflows[i].Error.Error()
				workflows[i].Error = deserializeWorkflowError(&s)
			}
		}
	}

	return nil
}

// ListWorkflows retrieves a list of workflows based on the provided filters.
//
// The function supports filtering by workflow IDs, status, time ranges, names, application versions,
// workflow ID prefixes, and more. It also supports pagination through
// limit/offset parameters and sorting control (ascending by default, or descending with WithFilterSortDesc).
//
// By default, both input and output data are loaded for each workflow. This can be controlled
// using WithFilterLoadInput(false) and WithFilterLoadOutput(false) options for better performance when
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
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithFilterStatus(dbos.WorkflowStatusSuccess),
//	    dbos.WithFilterCreatedAfter(time.Now().Add(-24*time.Hour)),
//	    dbos.WithFilterLimit(100))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows by specific IDs without loading input/output data
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithFilterWorkflowIDs("workflow1", "workflow2"),
//	    dbos.WithFilterLoadInput(false),
//	    dbos.WithFilterLoadOutput(false))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows with pagination
//	workflows, err := dbos.ListWorkflows(ctx,
//	    dbos.WithFilterUser("john.doe"),
//	    dbos.WithFilterOffset(50),
//	    dbos.WithFilterLimit(25),
//	    dbos.WithFilterSortDesc())
//	if err != nil {
//	    log.Fatal(err)
//	}
func ListWorkflows(ctx Client, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListWorkflows(ctx, opts...)
}

// GetWorkflowStepsInput holds optional parameters for GetWorkflowSteps.

// WithStepsLoadOutput controls whether to load step output data.
// When unset, output is loaded only if the DBOS context has been launched.
func WithStepsLoadOutput(loadOutput bool) GetWorkflowStepsOption {
	return func(o *models.GetWorkflowStepsInput) {
		o.LoadOutput = &loadOutput
	}
}

// WithStepsLimit limits the number of steps returned, ordered by function ID ascending.
func WithStepsLimit(limit int) GetWorkflowStepsOption {
	return func(o *models.GetWorkflowStepsInput) {
		o.Limit = &limit
	}
}

// WithStepsOffset skips the given number of steps before returning results.
func WithStepsOffset(offset int) GetWorkflowStepsOption {
	return func(o *models.GetWorkflowStepsInput) {
		o.Offset = &offset
	}
}

func (c *dbosContext) GetWorkflowSteps(_ Client, workflowID string, opts ...GetWorkflowStepsOption) ([]StepInfo, error) {
	options := models.GetWorkflowStepsInput{}
	for _, opt := range opts {
		opt(&options)
	}
	loadOutput := c.launched.Load()
	if options.LoadOutput != nil {
		loadOutput = *options.LoadOutput
	}
	getWorkflowStepsInput := sysdb.GetWorkflowStepsInput{
		WorkflowID: workflowID,
		LoadOutput: loadOutput,
		Limit:      options.Limit,
		Offset:     options.Offset,
	}

	var steps []sysdb.StepRow
	var err error
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		steps, err = RunAsStep(c, func(ctx context.Context) ([]sysdb.StepRow, error) {
			return sysdb.RetryWithResult(ctx, func() ([]sysdb.StepRow, error) {
				return c.systemDB.GetWorkflowSteps(ctx, getWorkflowStepsInput)
			}, sysdb.WithRetrierLogger(c.logger))
		}, WithStepName("DBOS.getWorkflowSteps"))
	} else {
		steps, err = sysdb.RetryWithResult(c, func() ([]sysdb.StepRow, error) {
			return c.systemDB.GetWorkflowSteps(c, getWorkflowStepsInput)
		}, sysdb.WithRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}
	stepInfos := make([]StepInfo, len(steps))
	for i, step := range steps {
		var stepErr error
		if step.Error != nil {
			s := step.Error.Error()
			stepErr = deserializeWorkflowError(&s)
		}
		stepInfos[i] = StepInfo{
			StepID:          step.StepID,
			StepName:        step.StepName,
			Error:           stepErr,
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
			decoded, err := decodeListingValue(encodedOutput, steps[i].Serialization, c.serializer)
			if err != nil {
				c.logger.Warn("failed to decode step output, storing raw value", "workflow_id", workflowID, "step_id", steps[i].StepID, "error", err)
			}
			stepInfos[i].Output = decoded
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
func GetWorkflowSteps(ctx Client, workflowID string, opts ...GetWorkflowStepsOption) ([]StepInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowSteps(ctx, workflowID, opts...)
}

func (c *dbosContext) GetWorkflowAggregates(_ Client, input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error) {
	if input.TimeBucketSize < 0 {
		return nil, errors.New("TimeBucketSize must be >= 0")
	}
	dbInput := sysdb.GetWorkflowAggregatesDBInput{
		GroupByStatus:             input.GroupByStatus,
		GroupByName:               input.GroupByName,
		GroupByQueueName:          input.GroupByQueueName,
		GroupByExecutorID:         input.GroupByExecutorID,
		GroupByApplicationVersion: input.GroupByApplicationVersion,
		SelectCount:               input.SelectCount,
		SelectMinCreatedAt:        input.SelectMinCreatedAt,
		SelectMaxQueueWaitMs:      input.SelectMaxQueueWaitMs,
		SelectMaxTotalLatencyMs:   input.SelectMaxTotalLatencyMs,
		TimeBucketSizeMs:          input.TimeBucketSize.Milliseconds(),
		Status:                    input.Status,
		StartTime:                 input.StartTime,
		EndTime:                   input.EndTime,
		CompletedAfter:            input.CompletedAfter,
		CompletedBefore:           input.CompletedBefore,
		DequeuedAfter:             input.DequeuedAfter,
		DequeuedBefore:            input.DequeuedBefore,
		WorkflowName:              input.Name,
		ApplicationVersion:        input.ApplicationVersion,
		ExecutorID:                input.ExecutorID,
		QueueName:                 input.QueueName,
		WorkflowIDPrefix:          input.WorkflowIDPrefix,
		WorkflowIDs:               input.WorkflowIDs,
		AuthenticatedUser:         input.AuthenticatedUser,
		ForkedFrom:                input.ForkedFrom,
		ParentWorkflowID:          input.ParentWorkflowID,
		WasForkedFrom:             input.WasForkedFrom,
		HasParent:                 input.HasParent,
		Attributes:                input.Attributes,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		return runAsTxn(c, func(ctx context.Context, tx Tx) ([]WorkflowAggregateRow, error) {
			in := dbInput
			in.Tx = tx
			return c.systemDB.GetWorkflowAggregates(ctx, in)
		}, WithStepName("DBOS.getWorkflowAggregates"))
	}
	return sysdb.RetryWithResult(c, func() ([]WorkflowAggregateRow, error) {
		return c.systemDB.GetWorkflowAggregates(c, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
}

// GetWorkflowAggregates returns aggregate counts of workflows grouped by one or more
// columns and/or by created_at time bucket.
//
// At least one GroupBy* flag in the input must be true, or TimeBucketSize must be > 0.
// Filter fields (Status, StartTime, EndTime, Name, ApplicationVersion, ExecutorID,
// QueueName, WorkflowIDPrefix, WorkflowIDs, AuthenticatedUser, ForkedFrom,
// ParentWorkflowID, WasForkedFrom, HasParent, Attributes) narrow which workflows are
// counted before grouping. Attributes filtering requires a Postgres-compatible system database.
//
// At least one Select* flag must be true. Returns one WorkflowAggregateRow per non-empty
// group. Each row's Group map contains an entry per enabled grouping column ("status",
// "name", "queue_name", "executor_id", "application_version", "time_bucket"). Map values are
// pointers to allow representing NULL grouping values (e.g. workflows without a queue_name).
// Count, MinCreatedAt, MaxQueueWaitMs and MaxTotalLatencyMs are populated only for the
// corresponding enabled Select* flag; the rest are nil.
//
// Example:
//
//	rows, err := dbos.GetWorkflowAggregates(ctx, dbos.GetWorkflowAggregatesInput{
//	    GroupByStatus: true,
//	    SelectCount:   true,
//	    StartTime:     time.Now().Add(-24 * time.Hour),
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, r := range rows {
//	    log.Printf("status=%s count=%d", *r.Group["status"], *r.Count)
//	}
func GetWorkflowAggregates(ctx Client, input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowAggregates(ctx, input)
}

func (c *dbosContext) GetStepAggregates(_ Client, input GetStepAggregatesInput) ([]StepAggregateRow, error) {
	if input.TimeBucketSize < 0 {
		return nil, errors.New("TimeBucketSize must be >= 0")
	}
	dbInput := sysdb.GetStepAggregatesDBInput{
		GroupByFunctionName: input.GroupByFunctionName,
		GroupByStatus:       input.GroupByStatus,
		SelectCount:         input.SelectCount,
		SelectMaxDurationMs: input.SelectMaxDurationMs,
		TimeBucketSizeMs:    input.TimeBucketSize.Milliseconds(),
		Status:              input.Status,
		FunctionName:        input.FunctionName,
		WorkflowIDPrefix:    input.WorkflowIDPrefix,
		CompletedAfter:      input.CompletedAfter,
		CompletedBefore:     input.CompletedBefore,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		return runAsTxn(c, func(ctx context.Context, tx Tx) ([]StepAggregateRow, error) {
			in := dbInput
			in.Tx = tx
			return c.systemDB.GetStepAggregates(ctx, in)
		}, WithStepName("DBOS.getStepAggregates"))
	}
	return sysdb.RetryWithResult(c, func() ([]StepAggregateRow, error) {
		return c.systemDB.GetStepAggregates(c, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
}

// GetStepAggregates returns aggregate counts and/or max durations of steps grouped by
// function name and/or derived status, optionally bucketed by completed_at time.
//
// At least one GroupBy* flag must be true, or TimeBucketSize must be > 0. At least one
// Select* flag must be true. Step status is derived from operation_outputs: steps with no
// recorded error are "SUCCESS", otherwise "ERROR".
//
// Returns one StepAggregateRow per non-empty group. Each row's Group map contains an entry
// per enabled grouping column ("function_name", "status", "time_bucket"). Count and
// MaxDurationMs are populated only for the corresponding enabled Select* flag.
func GetStepAggregates(ctx Client, input GetStepAggregatesInput) ([]StepAggregateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.GetStepAggregates(ctx, input)
}

// ListRegisteredWorkflows returns information about workflows registered with DBOS.
// Each WorkflowRegistryEntry contains:
// - MaxRetries: Maximum recovery attempts before dead-lettering (WithMaxRecoveryAttempts)
// - Name: Custom name if provided during registration, otherwise empty
// - FQN: Fully qualified name of the workflow function (always present)
//
// Example:
//
//	workflows := dbos.ListRegisteredWorkflows(ctx)
func ListRegisteredWorkflows(ctx Context) []WorkflowRegistryEntry {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	return ctx.ListRegisteredWorkflows(ctx)
}

func (c *dbosContext) ListenQueues(_ Context, names ...string) {
	newSet := make(map[string]bool, len(names))
	for _, name := range names {
		newSet[name] = true
	}
	c.queueRunner.listenMu.Lock()
	c.queueRunner.listenedQueues = newSet
	c.queueRunner.listenMu.Unlock()
}

func (c *dbosContext) ListenedQueues(_ Context) []string {
	c.queueRunner.listenMu.Lock()
	names := make([]string, 0, len(c.queueRunner.listenedQueues))
	for name := range c.queueRunner.listenedQueues {
		names = append(names, name)
	}
	c.queueRunner.listenMu.Unlock()
	slices.Sort(names)
	return names
}

// ListenQueues configures which queues the current DBOS process should listen to.
// By default, all queues are listened to. Once ListenQueues has been called, only
// the named queues (and the internal DBOS queue) are listened to. This lets
// multiple DBOS processes share the same queues but listen to different subsets.
//
// Each call REPLACES the entire listen set. Calling with a
// reduced set unlistens the removed queues; calling with no names clears the
// filter, restoring the default of listening to every queue.
// To add or remove a queue incrementally, read the current set with
// ListenedQueues and pass the modified set back.
//
// Names are resolved against the queues table on each reconcile tick, so a queue
// can be listened to before it exists in the database. The set may be replaced
// at any time, including after Launch, allowing it to change dynamically.
//
// Example:
//
//	dbos.RegisterQueue(ctx, "queue-1")
//	dbos.RegisterQueue(ctx, "queue-2")
//
//	// Only listen to queue-1 and queue-2.
//	dbos.ListenQueues(ctx, "queue-1", "queue-2")
func ListenQueues(ctx Context, names ...string) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	ctx.ListenQueues(ctx, names...)
}

// ListenedQueues returns the current listen set as a sorted slice. An empty
// slice means the process listens to every queue. Use it to modify the set
// incrementally:
//
//	queues := dbos.ListenedQueues(ctx)
//	dbos.ListenQueues(ctx, append(queues, "new-queue")...)
func ListenedQueues(ctx Context) []string {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	return ctx.ListenedQueues(ctx)
}

/*******************************/
/******* SCHEDULE MANAGEMENT ********/
/*******************************/

// validateScheduledWorkflowFn ensures fn has signature
// func(Context, ScheduledWorkflowInput) (any, error). Used by
// ApplySchedules where each entry's WorkflowFn is type-erased.
func validateScheduledWorkflowFn(fn any) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return errors.New("workflow function must be a function")
	}
	if t.NumIn() < 2 {
		return errors.New("workflow function must accept (Context, ScheduledWorkflowInput)")
	}
	if t.In(1) != reflect.TypeFor[ScheduledWorkflowInput]() {
		return fmt.Errorf("scheduled workflow function must accept a ScheduledWorkflowInput as input, got %v", t.In(1))
	}
	return nil
}

// resolveScheduleWorkflowName returns the workflow name targeted by a
// ScheduleSpec. When spec.Workflow is set it takes precedence: the function
// must be registered on this context and its registered name is returned.
func (c *dbosContext) resolveScheduleWorkflowName(spec ScheduleSpec) (string, error) {
	if spec.Workflow != nil {
		if err := validateScheduledWorkflowFn(spec.Workflow); err != nil {
			return "", err
		}
		return c.resolveWorkflowName(spec.Workflow)
	}
	if spec.WorkflowName == "" {
		return "", models.NewInvalidOptionError("one of workflow_name or workflow is required")
	}
	return spec.WorkflowName, nil
}

func (c *dbosContext) CreateSchedule(_ Client, spec ScheduleSpec) error {
	if spec.ScheduleName == "" {
		return models.NewInvalidOptionError("schedule_name is required")
	}

	workflowName, err := c.resolveScheduleWorkflowName(spec)
	if err != nil {
		return err
	}

	if err := validateCronSchedule(spec.Schedule, spec.CronTimezone); err != nil {
		return err
	}

	contextJSON, err := json.Marshal(spec.Context)
	if err != nil {
		return fmt.Errorf("failed to serialize context: %w", err)
	}

	scheduleID := uuid.New().String()
	dbInput := sysdb.CreateScheduleDBInput{
		ScheduleID:        scheduleID,
		ScheduleName:      spec.ScheduleName,
		WorkflowName:      workflowName,
		WorkflowClassName: spec.WorkflowClassName,
		Schedule:          spec.Schedule,
		Context:           string(contextJSON),
		Status:            ScheduleStatusActive,
		AutomaticBackfill: spec.AutomaticBackfill,
		CronTimezone:      spec.CronTimezone,
		QueueName:         spec.QueueName,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			input := dbInput
			input.Tx = tx
			return nil, c.systemDB.CreateSchedule(ctx, input)
		}, WithStepName("DBOS.createSchedule"))
		return err
	}

	uncancellableCtx := WithoutCancel(c)
	return sysdb.Retry(c, func() error {
		return c.systemDB.CreateSchedule(uncancellableCtx, dbInput)
	}, sysdb.WithRetrierLogger(c.logger), sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction))
}

// CreateSchedule creates a new schedule for a workflow. The reconciler loop
// picks the new schedule up on its next tick and installs it in the cron
// scheduler.
//
// The target workflow is identified by spec.WorkflowName, so schedules can be
// created from a Client for workflows owned by any process or language. From a
// Context, spec.Workflow can reference a registered Go workflow function
// directly instead.
//
// Example:
//
//	err := dbos.CreateSchedule(ctx, dbos.ScheduleSpec{
//	    ScheduleName: "my-schedule",
//	    Workflow:     myWorkflow,
//	    Schedule:     "*/5 * * * *",
//	    Context:      "my context",
//	})
func CreateSchedule(ctx Client, spec ScheduleSpec) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CreateSchedule(ctx, spec)
}

func (c *dbosContext) ApplySchedules(_ Client, schedules []ScheduleSpec) error {
	if state, ok := c.Value(workflowStateKey).(*workflowState); ok && state != nil {
		return errors.New("DBOS.ApplySchedules cannot be called from within a workflow")
	}

	if len(schedules) == 0 {
		return nil
	}

	for i, spec := range schedules {
		if spec.ScheduleName == "" {
			return models.NewInvalidOptionError(fmt.Sprintf("schedule entry %d is missing required field 'schedule_name'", i))
		}
		if err := validateCronSchedule(spec.Schedule, spec.CronTimezone); err != nil {
			return fmt.Errorf("schedule entry %d: %w", i, err)
		}
		if _, err := c.resolveScheduleWorkflowName(spec); err != nil {
			return fmt.Errorf("schedule entry %d: %w", i, err)
		}
	}

	return sysdb.Retry(c, func() error {
		tx, err := c.systemDB.Pool().BeginTx(c, TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(c)

		for _, spec := range schedules {
			workflowName, err := c.resolveScheduleWorkflowName(spec)
			if err != nil {
				return err
			}

			contextJSON, err := json.Marshal(spec.Context)
			if err != nil {
				return fmt.Errorf("failed to serialize context: %w", err)
			}

			queueName := spec.QueueName
			if queueName == "" {
				queueName = models.InternalQueueName
			}

			scheduleID := uuid.New().String()
			if err := c.systemDB.UpsertSchedule(c, sysdb.UpsertScheduleDBInput{
				ScheduleID:        scheduleID,
				ScheduleName:      spec.ScheduleName,
				WorkflowName:      workflowName,
				WorkflowClassName: spec.WorkflowClassName,
				Schedule:          spec.Schedule,
				Context:           string(contextJSON),
				Status:            ScheduleStatusActive,
				AutomaticBackfill: spec.AutomaticBackfill,
				CronTimezone:      spec.CronTimezone,
				QueueName:         queueName,
				Tx:                tx,
			}); err != nil {
				return fmt.Errorf("failed to upsert schedule: %w", err)
			}
		}

		if err := tx.Commit(c); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}, sysdb.WithRetrierLogger(c.logger), sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction))
}

// ApplySchedules applies a list of schedules, creating new ones or updating existing ones.
// Existing rows are upserted by schedule_name: definition fields are replaced while
// schedule_id, status, and last_fired_at are preserved. Useful for defining a set of
// static schedules to be created on program start.
//
// Example:
//
//	err := dbos.ApplySchedules(ctx, []dbos.ScheduleSpec{
//	    {ScheduleName: "schedule-a", Workflow: workflowA, Schedule: "*/10 * * * *"},
//	    {ScheduleName: "schedule-b", Workflow: workflowB, Schedule: "0 0 * * *"},
//	})
func ApplySchedules(ctx Client, schedules []ScheduleSpec) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.ApplySchedules(ctx, schedules)
}

func (c *dbosContext) PauseSchedule(_ Client, scheduleName string) error {
	if scheduleName == "" {
		return errors.New("schedule_name is required")
	}

	if _, err := c.GetSchedule(c, scheduleName); err != nil {
		return fmt.Errorf("failed to get schedule: %w", err)
	}

	dbInput := sysdb.UpdateScheduleDBInput{
		ScheduleName: scheduleName,
		Status:       ScheduleStatusPaused,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			in := dbInput
			in.Tx = tx
			return nil, c.systemDB.UpdateSchedule(ctx, in)
		}, WithStepName("DBOS.pauseSchedule"))
		return err
	}

	return sysdb.Retry(c, func() error {
		return c.systemDB.UpdateSchedule(c, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
}

// PauseSchedule pauses a schedule so it stops firing.
//
// Example:
//
//	err := dbos.PauseSchedule(ctx, "my-schedule")
func PauseSchedule(ctx Client, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.PauseSchedule(ctx, scheduleName)
}

func (c *dbosContext) ResumeSchedule(_ Client, scheduleName string) error {
	if scheduleName == "" {
		return errors.New("schedule_name is required")
	}

	if _, err := c.GetSchedule(c, scheduleName); err != nil {
		return fmt.Errorf("failed to get schedule: %w", err)
	}

	dbInput := sysdb.UpdateScheduleDBInput{
		ScheduleName: scheduleName,
		Status:       ScheduleStatusActive,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			in := dbInput
			in.Tx = tx
			return nil, c.systemDB.UpdateSchedule(ctx, in)
		}, WithStepName("DBOS.resumeSchedule"))
		return err
	}

	return sysdb.Retry(c, func() error {
		return c.systemDB.UpdateSchedule(c, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
}

// ResumeSchedule resumes a paused schedule.
//
// Example:
//
//	err := dbos.ResumeSchedule(ctx, "my-schedule")
func ResumeSchedule(ctx Client, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.ResumeSchedule(ctx, scheduleName)
}

func (c *dbosContext) DeleteSchedule(_ Client, scheduleName string) error {
	if scheduleName == "" {
		return errors.New("schedule_name is required")
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			return nil, c.systemDB.DeleteSchedule(ctx, sysdb.DeleteScheduleDBInput{ScheduleName: scheduleName, Tx: tx})
		}, WithStepName("DBOS.deleteSchedule"))
		return err
	}

	uncancellableCtx := WithoutCancel(c)
	return sysdb.Retry(c, func() error {
		return c.systemDB.DeleteSchedule(uncancellableCtx, sysdb.DeleteScheduleDBInput{ScheduleName: scheduleName})
	}, sysdb.WithRetrierLogger(c.logger))
}

// DeleteSchedule deletes a schedule.
//
// Example:
//
//	err := dbos.DeleteSchedule(ctx, "my-schedule")
func DeleteSchedule(ctx Client, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.DeleteSchedule(ctx, scheduleName)
}

func (c *dbosContext) GetSchedule(_ Client, scheduleName string) (WorkflowSchedule, error) {
	if scheduleName == "" {
		return WorkflowSchedule{}, errors.New("schedule_name is required")
	}

	dbInput := sysdb.ListSchedulesDBInput{ScheduleNamePrefixes: []string{scheduleName}}

	var schedules []WorkflowSchedule
	var err error
	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		schedules, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]WorkflowSchedule, error) {
			in := dbInput
			in.Tx = tx
			return c.systemDB.ListSchedules(ctx, in)
		}, WithStepName("DBOS.getSchedule"))
	} else {
		schedules, err = sysdb.RetryWithResult(c, func() ([]WorkflowSchedule, error) {
			return c.systemDB.ListSchedules(c, dbInput)
		}, sysdb.WithRetrierLogger(c.logger))
	}
	if err != nil {
		return WorkflowSchedule{}, err
	}
	for i := range schedules {
		if schedules[i].ScheduleName == scheduleName {
			return schedules[i], nil
		}
	}
	return WorkflowSchedule{}, models.NewScheduleNotFoundError(scheduleName)
}

// GetSchedule gets a schedule by name. If no schedule with the given name
// exists, it returns an error matching ErrScheduleNotFound.
//
// Example:
//
//	schedule, err := dbos.GetSchedule(ctx, "my-schedule")
func GetSchedule(ctx Client, scheduleName string) (WorkflowSchedule, error) {
	if ctx == nil {
		return WorkflowSchedule{}, errors.New("ctx cannot be nil")
	}
	return ctx.GetSchedule(ctx, scheduleName)
}

func (c *dbosContext) ListSchedules(_ Client, opts ...ListSchedulesOption) ([]WorkflowSchedule, error) {
	var o models.ListSchedulesInput
	for _, opt := range opts {
		opt(&o)
	}
	dbInput := sysdb.ListSchedulesDBInput{
		Statuses:             o.Statuses,
		WorkflowNames:        o.WorkflowNames,
		ScheduleNamePrefixes: o.ScheduleNamePrefixes,
	}
	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		return runAsTxn(c, func(ctx context.Context, tx Tx) ([]WorkflowSchedule, error) {
			in := dbInput
			in.Tx = tx
			return c.systemDB.ListSchedules(ctx, in)
		}, WithStepName("DBOS.listSchedules"))
	}
	return sysdb.RetryWithResult(c, func() ([]WorkflowSchedule, error) {
		return c.systemDB.ListSchedules(c, dbInput)
	}, sysdb.WithRetrierLogger(c.logger))
}

// WithScheduleStatuses filters schedules by the specified status(es).
func WithScheduleStatuses(statuses ...ScheduleStatus) ListSchedulesOption {
	return func(o *models.ListSchedulesInput) {
		o.Statuses = statuses
	}
}

// WithScheduleWorkflowNames filters schedules by the specified workflow name(s).
func WithScheduleWorkflowNames(names ...string) ListSchedulesOption {
	return func(o *models.ListSchedulesInput) {
		o.WorkflowNames = names
	}
}

// WithScheduleNamePrefixes filters schedules by schedule name prefix(es).
func WithScheduleNamePrefixes(prefixes ...string) ListSchedulesOption {
	return func(o *models.ListSchedulesInput) {
		o.ScheduleNamePrefixes = prefixes
	}
}

// ListSchedules lists schedules, optionally filtered by the supplied options.
// Pass no options to return all schedules.
//
// Example:
//
//	schedules, err := dbos.ListSchedules(ctx, dbos.WithScheduleStatuses(dbos.ScheduleStatusActive))
func ListSchedules(ctx Client, opts ...ListSchedulesOption) ([]WorkflowSchedule, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListSchedules(ctx, opts...)
}

func (c *dbosContext) BackfillSchedule(_ Client, scheduleName string, start time.Time, end time.Time) ([]string, error) {
	if state, ok := c.Value(workflowStateKey).(*workflowState); ok && state != nil {
		return nil, errors.New("DBOS.BackfillSchedule cannot be called from within a workflow")
	}
	if scheduleName == "" {
		return nil, errors.New("schedule_name is required")
	}

	existing, err := c.GetSchedule(c, scheduleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}

	var ids []string
	err = sysdb.Retry(c, func() error {
		var bfErr error
		ids, bfErr = c.systemDB.BackfillSchedule(c, sysdb.BackfillScheduleDBInput{
			ScheduleName: scheduleName,
			Schedule:     existing.Schedule,
			StartTime:    start,
			EndTime:      end,
		})
		return bfErr
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// BackfillSchedule backfills a schedule, executing it for each time slot in the range.
// Already-executed times are automatically skipped. Returns the IDs of the
// workflows enqueued for the backfilled time slots.
//
// Example:
//
//	ids, err := dbos.BackfillSchedule(ctx, "my-schedule", startTime, endTime)
func BackfillSchedule(ctx Client, scheduleName string, start, end time.Time) ([]string, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.BackfillSchedule(ctx, scheduleName, start, end)
}

func (c *dbosContext) TriggerSchedule(_ Client, scheduleName string) (WorkflowHandle[any], error) {
	if scheduleName == "" {
		return nil, errors.New("schedule_name is required")
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	if ok && workflowState != nil {
		return nil, errors.New("DBOS.TriggerSchedule cannot be called from within a workflow")
	}

	workflowID, err := c.systemDB.TriggerSchedule(c, scheduleName)
	if err != nil {
		return nil, err
	}
	return newWorkflowPollingHandle[any](c, workflowID), nil
}

// TriggerSchedule triggers a schedule immediately, returning a typed handle to
// the enqueued workflow whose GetResult decodes the workflow output into type R.
//
// Example:
//
//	handle, err := dbos.TriggerSchedule[string](ctx, "my-schedule")
func TriggerSchedule[R any](ctx Client, scheduleName string) (WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	handle, err := ctx.TriggerSchedule(ctx, scheduleName)
	if err != nil {
		return nil, err
	}
	return typedHandle[R](ctx, handle), nil
}

// ListApplicationVersions returns every registered application version ordered
// by timestamp (newest first).
func (c *dbosContext) ListApplicationVersions(_ Client) ([]VersionInfo, error) {
	return sysdb.RetryWithResult(c, func() ([]VersionInfo, error) {
		return c.systemDB.ListApplicationVersions(c)
	}, sysdb.WithRetrierLogger(c.logger))
}

// ListApplicationVersions is the package-level wrapper for Context.ListApplicationVersions.
func ListApplicationVersions(ctx Client) ([]VersionInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListApplicationVersions(ctx)
}

// GetLatestApplicationVersion returns the application version with the most
// recent timestamp.
func (c *dbosContext) GetLatestApplicationVersion(_ Client) (VersionInfo, error) {
	latest, err := sysdb.RetryWithResult(c, func() (*VersionInfo, error) {
		return c.systemDB.GetLatestApplicationVersion(c, nil)
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return VersionInfo{}, err
	}
	return *latest, nil
}

// GetLatestApplicationVersion is the package-level wrapper for Context.GetLatestApplicationVersion.
func GetLatestApplicationVersion(ctx Client) (VersionInfo, error) {
	if ctx == nil {
		return VersionInfo{}, errors.New("ctx cannot be nil")
	}
	return ctx.GetLatestApplicationVersion(ctx)
}

// SetLatestApplicationVersion marks the named application version as latest by
// updating its timestamp to the current time.
func (c *dbosContext) SetLatestApplicationVersion(_ Client, versionName string) error {
	if versionName == "" {
		return errors.New("version_name is required")
	}
	return sysdb.Retry(c, func() error {
		return c.systemDB.UpdateApplicationVersionTimestamp(c, versionName, time.Now().UnixMilli())
	}, sysdb.WithRetrierLogger(c.logger))
}

// SetLatestApplicationVersion is the package-level wrapper for Context.SetLatestApplicationVersion.
func SetLatestApplicationVersion(ctx Client, versionName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetLatestApplicationVersion(ctx, versionName)
}

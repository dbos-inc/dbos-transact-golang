package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const _DEBOUNCER_TOPIC = "_dbos_debouncer_topic"

// debouncerInput is the input to the internal debouncer workflow
type debouncerInput[P any] struct {
	InitialInput                  P
	TargetWorkflowFQNOrCustomName string
	TargetWorkflowID              string
	Delay                         time.Duration   // Time by which to delay workflow execution
	Timeout                       time.Duration   // Maximum time before starting the workflow
	WorkflowOptions               workflowOptions // Options to pass to target workflow (serializable)
}

// DebounceMessage is sent to the debouncer workflow to update inputs
type DebounceMessage[P any] struct {
	Input P
	Delay time.Duration
	ID    string // Used for ACK protocol
}

// Debouncer provides workflow debouncing functionality.
// It delays workflow execution by a configurable delay amount, with each
// subsequent call pushing back the start time by the delay (up to an optional maximum timeout).
//
// The debouncer uses an internal workflow that collects inputs and delays
// execution. Each call to Debounce pushes back the start time by the delay
// amount. If a timeout is configured, the start time cannot exceed the timeout
// from the first invocation. If timeout is zero, there is no maximum time limit.
//
// The same debounce can be used with different keys to debounce multiple independent groups of workflow invocations.
type Debouncer[P any, R any] struct {
	WorkflowFQN          string        // Fully qualified name of the target workflow
	Timeout              time.Duration // Maximum time before starting the workflow (0 = no timeout)
	internalDebouncerFQN string        // Fully qualified name of the internal debouncer workflow
}

// DebouncerOption is a functional option for configuring debouncer creation parameters.
type DebouncerOption func(*time.Duration)

// WithDebouncerTimeout sets the maximum time before starting the workflow.
// If timeout is zero (the default), there is no maximum time limit.
func WithDebouncerTimeout(timeout time.Duration) DebouncerOption {
	return func(t *time.Duration) {
		*t = timeout
	}
}

// NewDebouncer creates a new debouncer for the specified workflow.
//
// Parameters:
//   - ctx: DBOS context for the debouncer
//   - workflow: The workflow function to debounce (must be registered)
//   - opts: Optional functional options for configuring the debouncer:
//   - WithDebouncerTimeout: Maximum time before starting the workflow (0 = no timeout) [optional]
//
// Returns a pointer to a Debouncer instance that can be used to call Debounce.
//
// Example:
//
//	// Create a debouncer with maximum timeout of 10 seconds
//	debouncer := dbos.NewDebouncer(ctx, MyWorkflowFunction, WithDebouncerTimeout(10*time.Second))
//
//	// Create a debouncer with no timeout
//	debouncerNoTimeout := dbos.NewDebouncer(ctx, MyWorkflowFunction)
//
//	// Later, use the debouncer with different keys and delays
//	handle1, err := debouncer.Debounce(ctx, "user-123", 2*time.Second, inputData1)
//	handle2, err := debouncer.Debounce(ctx, "user-456", 3*time.Second, inputData2)
func NewDebouncer[P any, R any](
	ctx DBOSContext,
	workflow Workflow[P, R],
	opts ...DebouncerOption,
) *Debouncer[P, R] {
	timeout := time.Duration(0) // Default: no timeout
	for _, opt := range opts {
		opt(&timeout)
	}

	dbosCtx, ok := ctx.(*dbosContext)
	if !ok {
		return &Debouncer[P, R]{} // Do nothing if the concrete type is not dbosContext
	}

	// Enforce that debouncers can only be created before DBOS has launched
	// because they need to register the internal debouncer workflow
	if dbosCtx.launched.Load() {
		panic(newInitializationError("cannot create debouncer after DBOS has launched"))
	}

	// Get the fully qualified name of the workflow function using reflection
	fqn := resolveWorkflowFunctionName(workflow)

	dbosCtx.logger.Debug("Creating new debouncer", "workflow_fqn", fqn)

	// Validate that the workflow is registered in the registry
	// Assertively panic if the workflow is not registered, as a sign of highly unexpected behavior
	if _, exists := dbosCtx.workflowRegistry.Load(fqn); !exists {
		panic(newNonExistentWorkflowError(fqn))
	}

	// Register the internal debouncer workflow for this debouncer if it has not been registered yet (first debouncer for this workflow)
	internalDebouncerFQN := resolveWorkflowFunctionName(internalDebouncerWF[P, R])
	if _, exists := dbosCtx.workflowCustomNametoFQN.Load(internalDebouncerFQN); !exists {
		RegisterWorkflow(ctx, internalDebouncerWF[P, R])
	}

	return &Debouncer[P, R]{
		WorkflowFQN:          fqn,
		Timeout:              timeout,
		internalDebouncerFQN: internalDebouncerFQN,
	}
}

func (d *Debouncer[P, R]) Debounce(ctx DBOSContext, key string, delay time.Duration, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	workflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil

	// Resolve workflow ID.
	options := workflowOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.WorkflowID == "" {
		if isWithinWorkflow {
			workflowID, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
				return uuid.New().String(), nil
			}, WithStepName("DBOS.debounce.assignWorkflowID"))
			if err != nil {
				return nil, err
			}
			options.WorkflowID = workflowID
		} else {
			options.WorkflowID = uuid.New().String()
		}
		opts = append(opts, WithWorkflowID(options.WorkflowID))
	}

	// Generate a message ID if communicating with an existing internal debouncing workflow.
	var messageID string
	if isWithinWorkflow {
		msgID, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
			return uuid.New().String(), nil
		}, WithStepName("DBOS.debounce.assignMessageID"))
		if err != nil {
			return nil, err
		}
		messageID = msgID
	} else {
		messageID = uuid.New().String()
	}

	dInput := debouncerInput[P]{
		InitialInput:                  input,
		TargetWorkflowFQNOrCustomName: d.WorkflowFQN,
		TargetWorkflowID:              options.WorkflowID,
		Delay:                         delay,
		Timeout:                       d.Timeout,
		WorkflowOptions:               options,
	}

	for {
		// internalDebouncerWF[P, R] is a generic workflow, so its dynamic name resolution will yield a different name than its registration name
		// This is because the function passed through as an argument can have a different reflection name
		_, err := RunWorkflow(ctx, internalDebouncerWF[P, R], dInput, WithQueue(_DBOS_INTERNAL_QUEUE_NAME), WithDeduplicationID(key), withWorkflowName(d.internalDebouncerFQN))
		if err == nil {
			return newWorkflowPollingHandle[R](ctx, dInput.TargetWorkflowID), nil
		}
		// A dedup error means the internal debouncer workflow was already started, in which case we should send it the new input
		var dbosErr *DBOSError
		if errors.As(err, &dbosErr) && dbosErr.Code == QueueDeduplicated {
			// Identify the ID of the internal debouncer workflow from the dedup error
			debouncerWorkflowStatus, err := ListWorkflows(ctx, WithFilterDeduplicationID(key))
			if err != nil {
				return nil, err
			}
			if len(debouncerWorkflowStatus) == 0 {
				continue // The debouncer workflow might have started the user workflow and exited already, in which case we should try again to create a new internal debouncer workflow
			}
			debouncerWorkflowID := debouncerWorkflowStatus[0].ID

			// Send the new input to the internal debouncer workflow
			err = Send(ctx, debouncerWorkflowID, DebounceMessage[P]{
				Input: input,
				Delay: delay,
				ID:    messageID,
			}, _DEBOUNCER_TOPIC)
			if err != nil {
				return nil, err
			}

			// Acknowledge the send by getting an event with the message ID
			_, err = GetEvent[bool](ctx, debouncerWorkflowID, messageID, 2*time.Second) // XXX unclear what's a good timeout here.
			if errors.Is(err, &DBOSError{Code: TimeoutError}) {
				continue // The debouncer workflow might have started the user workflow and exited already, in which case we should try again to create a new internal debouncer workflow
			} else if err != nil {
				return nil, err
			}

			// Retrieve the user workflow ID from the input of the internal debouncer workflow
			// The input comes from the DB and was decoded as a typeless JSON string
			encodedInput, ok := debouncerWorkflowStatus[0].Input.(string)
			if !ok {
				return nil, fmt.Errorf("internal debouncer workflow input is not encoded")
			}
			var decodedInput debouncerInput[P]
			if err := json.Unmarshal([]byte(encodedInput), &decodedInput); err != nil {
				return nil, fmt.Errorf("failed to unmarshal debouncer workflow input: %w", err)
			}
			return newWorkflowPollingHandle[R](ctx, decodedInput.TargetWorkflowID), nil
		}
		return nil, err
	}
}

// DebouncerClient provides workflow debouncing functionality using a Client.
// It is similar to Debouncer but uses a Client interface instead of a DBOSContext
// and takes a workflow name string instead of a workflow function.
type DebouncerClient[P any, R any] struct {
	WorkflowName         string        // Name of the target workflow
	Client               Client        // DBOS client for operations
	Timeout              time.Duration // Maximum time before starting the workflow (0 = no timeout)
	internalDebouncerFQN string        // Fully qualified name of the internal debouncer workflow
}

// NewDebouncerClient creates a new debouncer client for the specified workflow.
//
// Parameters:
//   - workflowName: The name of the workflow to debounce
//   - client: The DBOS client to use for operations
//   - opts: Optional functional options for configuring the debouncer:
//   - WithDebouncerTimeout: Maximum time before starting the workflow (0 = no timeout) [optional]
//
// Returns a pointer to a DebouncerClient instance that can be used to call Debounce.
func NewDebouncerClient[P any, R any](
	workflowName string,
	client Client,
	opts ...DebouncerOption,
) *DebouncerClient[P, R] {
	timeout := time.Duration(0) // Default: no timeout
	for _, opt := range opts {
		opt(&timeout)
	}

	return &DebouncerClient[P, R]{
		WorkflowName: workflowName,
		Client:       client,
		Timeout:      timeout,
		// Use the any,any internal debouncer workflow FQN because that's all the server knows
		internalDebouncerFQN: resolveWorkflowFunctionName(internalDebouncerWF[any, any]),
	}
}

// Debounce delays workflow execution by a configurable delay amount, with each
// subsequent call pushing back the start time by the delay (up to an optional maximum timeout).
//
// Unlike Debouncer.Debounce, this method never checks if we're within a workflow
// and never attempts to run operations as steps. It uses the Client's Enqueue,
// Send, ListWorkflows, and GetEvent methods.
//
// Parameters:
//   - key: A unique key to group debounce calls (calls with the same key are debounced together)
//   - delay: Time by which to delay workflow execution
//   - input: Input parameters to pass to the workflow
//   - opts: Optional workflow options (e.g., WithWorkflowID, WithQueue, etc.)
//
// Returns a WorkflowHandle that can be used to check status and retrieve results.
func (dc *DebouncerClient[P, R]) Debounce(key string, delay time.Duration, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	// Resolve workflow options
	options := workflowOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	// Generate workflow ID if not provided
	if options.WorkflowID == "" {
		options.WorkflowID = uuid.New().String()
	}

	// Generate message ID for ACK protocol
	messageID := uuid.New().String()

	// Create debouncer input
	dInput := debouncerInput[P]{
		InitialInput:                  input,
		TargetWorkflowFQNOrCustomName: dc.WorkflowName,
		TargetWorkflowID:              options.WorkflowID,
		Delay:                         delay,
		Timeout:                       dc.Timeout,
		WorkflowOptions:               options,
	}

	for {
		// Try to enqueue the internal debouncer workflow
		// Use the package-level Enqueue function which handles encoding automatically
		_, err := Enqueue[debouncerInput[P], R](dc.Client, _DBOS_INTERNAL_QUEUE_NAME, dc.internalDebouncerFQN, dInput, WithEnqueueDeduplicationID(key))
		if err == nil {
			return newWorkflowPollingHandle[R](dc.Client.(*client).dbosCtx, dInput.TargetWorkflowID), nil
		}

		// Check if error is due to deduplication (workflow already exists)
		var dbosErr *DBOSError
		if errors.As(err, &dbosErr) && dbosErr.Code == QueueDeduplicated {
			// The internal debouncer workflow already exists, send it the new input
			// List workflows with the deduplication ID to find the existing debouncer workflow
			debouncerWorkflowStatus, err := dc.Client.ListWorkflows(WithFilterDeduplicationID(key), WithLoadInput(true))
			if err != nil {
				return nil, err
			}
			if len(debouncerWorkflowStatus) == 0 {
				// The debouncer workflow might have started the user workflow and exited already, try again
				continue
			}
			debouncerWorkflowID := debouncerWorkflowStatus[0].ID

			// Send the new input to the internal debouncer workflow
			err = dc.Client.Send(debouncerWorkflowID, DebounceMessage[P]{
				Input: input,
				Delay: delay,
				ID:    messageID,
			}, _DEBOUNCER_TOPIC)
			if err != nil {
				return nil, err
			}

			// Acknowledge the send by getting an event with the message ID
			_, err = dc.Client.GetEvent(debouncerWorkflowID, messageID, 2*time.Second)
			if errors.Is(err, &DBOSError{Code: TimeoutError}) {
				// The debouncer workflow might have started the user workflow and exited already, try again
				continue
			} else if err != nil {
				return nil, err
			}

			// Retrieve the user workflow ID from the input of the internal debouncer workflow
			// The input comes from the DB and was decoded as a typeless JSON string
			encodedInputStr, ok := debouncerWorkflowStatus[0].Input.(string)
			if !ok {
				return nil, fmt.Errorf("internal debouncer workflow input is not encoded")
			}
			var decodedInput debouncerInput[P]
			if err := json.Unmarshal([]byte(encodedInputStr), &decodedInput); err != nil {
				return nil, fmt.Errorf("failed to unmarshal debouncer workflow input: %w", err)
			}
			return newWorkflowPollingHandle[R](dc.Client.(*client).dbosCtx, decodedInput.TargetWorkflowID), nil
		}
		return nil, err
	}
}

// internalDebouncerWF is the internal workflow that implements debouncing logic.
// It collects inputs, delays execution, and runs the target workflow with the latest input.
func internalDebouncerWF[P any, R any](ctx DBOSContext, input debouncerInput[P]) (R, error) {
	var zero R

	dbosCtx, ok := ctx.(*dbosContext)
	if !ok { // do nothing if the context is not a dbosContext
		return zero, nil
	}

	// Track the first creation time and current input
	startTime, err := RunAsStep(ctx, func(ctx context.Context) (time.Time, error) {
		return time.Now(), nil
	}, WithStepName("DBOS.debounce.startTime"))
	if err != nil {
		return zero, err
	}
	currentInput := input.InitialInput
	delay := input.Delay
	timeout := input.Timeout
	maxStartTime := startTime.Add(timeout)

	// Calculate initial target start time: startTime + delay
	targetStartTime := startTime.Add(delay)

	// If timeout is set, ensure target start time doesn't exceed startTime + timeout
	if timeout > 0 {
		if targetStartTime.After(maxStartTime) {
			targetStartTime = maxStartTime
		}
	}

	// Loop until we reach the target start time
	for {
		var now time.Time
		now, err = RunAsStep(ctx, func(ctx context.Context) (time.Time, error) {
			return time.Now(), nil
		}, WithStepName("DBOS.debounce.loopTime"))
		if err != nil {
			return zero, err
		}
		remainingTime := targetStartTime.Sub(now)
		// If we've reached or passed the target start time, break and execute
		if remainingTime <= 0 {
			break
		}

		// Try to receive a new input message with the remaining time as timeout
		msg, err := Recv[DebounceMessage[P]](ctx, _DEBOUNCER_TOPIC, remainingTime)
		if err != nil {
			// Timeout or error - break and execute with current input
			break
		}

		// Update the current input with the new message
		currentInput = msg.Input

		// Calculate new target start time: now + delay
		newTargetStartTime := now.Add(msg.Delay)

		// If timeout is set, cap the new target start time
		if timeout > 0 {
			if newTargetStartTime.After(maxStartTime) {
				newTargetStartTime = maxStartTime
			}
		}

		targetStartTime = newTargetStartTime

		// ACK the message by setting an event with the message ID
		if msg.ID != "" {
			err = SetEvent(ctx, msg.ID, true)
			if err != nil {
				ctx.(*dbosContext).logger.Error("failed to ACK debounce message", "error", err)
			}
		}
	}

	// Now execute the target workflow with the latest input
	// Look up the workflow from the registry
	// First resolve the FQN
	// workflowCustomNametoFQN stores all types of name to FQN: custom name -> FQN if the workflow was registered with a custom name, otherwise FQN->FQN
	targetWorkflowFQN := input.TargetWorkflowFQNOrCustomName
	if fqn, ok := dbosCtx.workflowCustomNametoFQN.Load(input.TargetWorkflowFQNOrCustomName); ok { // ok should always be true
		targetWorkflowFQN = fqn.(string)
	}

	registeredWorkflowAny, exists := dbosCtx.workflowRegistry.Load(targetWorkflowFQN)
	if !exists {
		return zero, fmt.Errorf("target workflow %s not found in registry", input.TargetWorkflowFQNOrCustomName)
	}

	registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
	if !ok {
		return zero, fmt.Errorf("invalid workflow registry entry type for workflow %s", input.TargetWorkflowFQNOrCustomName)
	}

	// Reconstruct WorkflowOptions from serializable format
	workflowOpts := []WorkflowOption{}
	if input.WorkflowOptions.WorkflowID != "" {
		workflowOpts = append(workflowOpts, WithWorkflowID(input.WorkflowOptions.WorkflowID))
	}
	if input.WorkflowOptions.QueueName != "" {
		workflowOpts = append(workflowOpts, WithQueue(input.WorkflowOptions.QueueName))
	}
	if input.WorkflowOptions.ApplicationVersion != "" {
		workflowOpts = append(workflowOpts, WithApplicationVersion(input.WorkflowOptions.ApplicationVersion))
	}
	if input.WorkflowOptions.DeduplicationID != "" {
		workflowOpts = append(workflowOpts, WithDeduplicationID(input.WorkflowOptions.DeduplicationID))
	}
	if input.WorkflowOptions.Priority > 0 {
		workflowOpts = append(workflowOpts, WithPriority(input.WorkflowOptions.Priority))
	}
	if input.WorkflowOptions.AuthenticatedUser != "" {
		workflowOpts = append(workflowOpts, WithAuthenticatedUser(input.WorkflowOptions.AuthenticatedUser))
	}
	if input.WorkflowOptions.AssumedRole != "" {
		workflowOpts = append(workflowOpts, WithAssumedRole(input.WorkflowOptions.AssumedRole))
	}
	if len(input.WorkflowOptions.AuthenticatedRoles) > 0 {
		workflowOpts = append(workflowOpts, WithAuthenticatedRoles(input.WorkflowOptions.AuthenticatedRoles))
	}
	if input.WorkflowOptions.QueuePartitionKey != "" {
		workflowOpts = append(workflowOpts, WithQueuePartitionKey(input.WorkflowOptions.QueuePartitionKey))
	}

	// We use the wrapped, type-erased workflow wrapper from the workflow registry that calls ctx.RunWorkflow
	// Which doesn't do any pre-encoding of the input, and calls a type-erased function that expects an encoded input
	// So we need to serialize the input here
	workflowOpts = append(workflowOpts, withAlreadyEncodedInput())
	serializer := newJSONSerializer[P]()
	encodedInput, err := serializer.Encode(currentInput)
	if err != nil {
		return zero, fmt.Errorf("failed to serialize input: %w", err)
	}

	// Call the target workflow using its wrapped function
	_, err = registeredWorkflow.wrappedFunction(ctx, encodedInput, workflowOpts...)
	if err != nil {
		return zero, fmt.Errorf("failed to run target workflow: %w", err)
	}

	return zero, nil
}

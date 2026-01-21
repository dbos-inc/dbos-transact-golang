package dbos

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/google/uuid"
)

const _DEBOUNCER_TOPIC = "_dbos_debouncer_topic"

// DebouncerInput is the input to the internal debouncer workflow
type DebouncerInput[P any] struct {
	InitialInput      P
	TargetWorkflowFQN string
	TargetWorkflowID  string
	Delay             time.Duration    // Time by which to delay workflow execution
	Timeout           time.Duration    // Maximum time before starting the workflow
	WorkflowOptions   []WorkflowOption // Options to pass to target workflow
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
// Debouncer instances are registered with DBOSContext, indexed by workflow FQN.
// Each workflow can have at most one debouncer configuration. The same debouncer
// can be used with different keys to debounce multiple independent groups of workflow invocations.
type Debouncer[P any, R any] struct {
	WorkflowFQN string        // Fully qualified name of the target workflow
	Timeout     time.Duration // Maximum time before starting the workflow (0 = no timeout)
}

// NewDebouncer creates and registers a new debouncer for the specified workflow.
//
// The debouncer delays workflow execution. Each call to Debounce pushes back
// the start time by a specified delay. If a timeout is
// specified, the start time cannot exceed the timeout from the first invocation.
// If timeout is zero, there is no maximum time limit.
//
// Each workflow can have at most one debouncer configuration, indexed by the
// workflow's fully qualified name (FQN). But multiple invocations of the same workflow function
// can be grouped for debouncing by using debouncing keys.
//
// Parameters:
//   - ctx: DBOS context for the debouncer
//   - workflow: The workflow function to debounce (must be registered)
//   - timeout: Maximum time before starting the workflow (0 = no timeout)
//
// Returns a Debouncer instance that can be used to call Debounce.
//
// Example:
//
//	// Register a debouncer with maximum timeout of 10 seconds
//	debouncer := dbos.NewDebouncer(ctx, MyWorkflowFunction, 10*time.Second)
//
//	// Register a debouncer with no timeout
//	debouncerNoTimeout := dbos.NewDebouncer(ctx, MyWorkflowFunction, 0)
//
//	// Later, use the debouncer with different keys and delays
//	handle1, err := debouncer.Debounce(ctx, "user-123", 2*time.Second, inputData1)
//	handle2, err := debouncer.Debounce(ctx, "user-456", 3*time.Second, inputData2)
func NewDebouncer[P any, R any](
	ctx DBOSContext,
	workflow Workflow[P, R],
	timeout time.Duration,
) Debouncer[P, R] {
	dbosCtx, ok := ctx.(*dbosContext)
	if !ok {
		return Debouncer[P, R]{} // Do nothing if the concrete type is not dbosContext
	}

	// Get the fully qualified name of the workflow function using reflection
	fqn := runtime.FuncForPC(reflect.ValueOf(workflow).Pointer()).Name()

	dbosCtx.logger.Debug("Creating new debouncer", "workflow_fqn", fqn)

	// Check if debouncer already exists for this workflow
	// Assertively panic if the debouncer is already registered for this workflow, as a sign of highly unexpected behavior
	if _, exists := dbosCtx.debouncerRegistry.Load(fqn); exists {
		panic(newConflictingRegistrationError(fqn))
	}

	// Validate that the workflow is registered in the registry
	// Assertively panic if the workflow is not registered, as a sign of highly unexpected behavior
	if _, exists := dbosCtx.workflowRegistry.Load(fqn); !exists {
		panic(newNonExistentWorkflowError(fqn))
	}

	// Create and register the debouncer in the global registry, indexed by FQN
	d := Debouncer[P, R]{
		WorkflowFQN: fqn,
		Timeout:     timeout,
	}
	dbosCtx.debouncerRegistry.Store(fqn, d)

	return d
}

func (d *Debouncer[P, R]) Debounce(ctx DBOSContext, key string, delay time.Duration, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	workflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil

	// Resolve workflow ID.
	options := workflowOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.workflowID == "" {
		if isWithinWorkflow {
			workflowID, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
				return uuid.New().String(), nil
			}, WithStepName("Debounce.assignWorkflowID"))
			if err != nil {
				return nil, err
			}
			options.workflowID = workflowID
		} else {
			options.workflowID = uuid.New().String()
		}
		opts = append(opts, WithWorkflowID(options.workflowID))
	}

	// Generate a message ID if communicating with an existing internal debouncing workflow.
	var messageID string
	if isWithinWorkflow {
		_, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
			messageID = uuid.New().String()
			return messageID, nil
		}, WithStepName("Debounce.assignMessageID"))
		if err != nil {
			return nil, err
		}
	} else {
		messageID = uuid.New().String()
	}

	debouncerInput := DebouncerInput[P]{
		InitialInput:      input,
		TargetWorkflowFQN: d.WorkflowFQN,
		TargetWorkflowID:  options.workflowID,
		Delay:             delay,
		Timeout:           d.Timeout,
		WorkflowOptions:   opts,
	}

	for {
		_, err := RunWorkflow(ctx, internalDebouncerWF[P, R], debouncerInput, WithQueue(_DBOS_INTERNAL_QUEUE_NAME), WithDeduplicationID(key))
		if err == nil {
			return newWorkflowPollingHandle[R](ctx, debouncerInput.TargetWorkflowID), nil
		} else {
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
				_, err = GetEvent[bool](ctx, debouncerWorkflowID, key, 1*time.Second) // XXX unclear what's a good timeout here.
				if errors.Is(err, &DBOSError{Code: TimeoutError}) {
					continue // The debouncer workflow might have started the user workflow and exited already, in which case we should try again to create a new internal debouncer workflow
				} else if err != nil {
					return nil, err
				}

				// Retrieve the user workflow ID from the input of the internal debouncer workflow
				// The input comes from the DB and is encoded
				serializer := newJSONSerializer[DebouncerInput[P]]()
				encodedInput, ok := debouncerWorkflowStatus[0].Input.(*string)
				if !ok {
					return nil, fmt.Errorf("internal debouncer workflow input is not encoded")
				}
				decodedInput, err := serializer.Decode(encodedInput)
				if err != nil {
					return nil, fmt.Errorf("failed to decode internal debouncer workflow input: %w", err)
				}
				return newWorkflowPollingHandle[R](ctx, decodedInput.TargetWorkflowID), nil
			} else {
				return nil, err
			}
		}
	}
}

// internalDebouncerWF is the internal workflow that implements debouncing logic.
// It collects inputs, delays execution, and runs the target workflow with the latest input.
func internalDebouncerWF[P any, R any](ctx DBOSContext, input DebouncerInput[P]) (R, error) {
	var zero R

	dbosCtx, ok := ctx.(*dbosContext)
	if !ok { // do nothing if the context is not a dbosContext
		return zero, nil
	}

	// Track the first creation time and current input
	startTime := time.Now()
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
		now := time.Now()
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
	registeredWorkflowAny, exists := dbosCtx.workflowRegistry.Load(input.TargetWorkflowFQN)
	if !exists {
		return zero, fmt.Errorf("target workflow %s not found in registry", input.TargetWorkflowFQN)
	}

	registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
	if !ok {
		return zero, fmt.Errorf("invalid workflow registry entry type for workflow %s", input.TargetWorkflowFQN)
	}

	// Because we reuse the workflow registry, where the wrapped function is expecting an encoded input, we need to serialize it
	// In the future we could optimize this with a dedicated registry for debouncers, where the wrapped function is expecting a non-encoded, typed input
	serializer := newJSONSerializer[P]()
	encodedInput, err := serializer.Encode(currentInput)
	if err != nil {
		return zero, fmt.Errorf("failed to serialize input: %w", err)
	}

	// Call the target workflow using its wrapped function
	_, err = registeredWorkflow.wrappedFunction(ctx, encodedInput, input.WorkflowOptions...)
	if err != nil {
		return zero, fmt.Errorf("failed to run target workflow: %w", err)
	}

	return zero, nil
}

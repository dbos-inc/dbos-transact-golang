package dbos

/**
Test workflow and steps features
[x] Wrapping various golang methods in DBOS workflows
[x] workflow idempotency
[x] workflow DLQ
[] workflow conflicting name
[] workflow timeout
[] workflow deadlines
*/

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Global counter for idempotency testing
var idempotencyCounter int64

var (
	simpleWf              = WithWorkflow(simpleWorkflow)
	simpleWfError         = WithWorkflow(simpleWorkflowError)
	simpleWfWithStep      = WithWorkflow(simpleWorkflowWithStep)
	simpleWfWithStepError = WithWorkflow(simpleWorkflowWithStepError)
	// struct methods
	s              = workflowStruct{}
	simpleWfStruct = WithWorkflow(s.simpleWorkflow)
	simpleWfValue  = WithWorkflow(s.simpleWorkflowValue)
	// interface method workflow
	workflowIface TestWorkflowInterface = &workflowImplementation{
		field: "example",
	}
	simpleWfIface = WithWorkflow(workflowIface.Execute)
	// Generic workflow
	wfInt = WithWorkflow(Identity[string]) // FIXME make this an int eventually
	// Closure with captured state
	prefix  = "hello-"
	wfClose = WithWorkflow(func(ctx context.Context, in string) (string, error) {
		return prefix + in, nil
	})
	// Workflow for idempotency testing
	idempotencyWf         = WithWorkflow(idempotencyWorkflow)
	idempotencyWfWithStep = WithWorkflow(idempotencyWorkflowWithStep)
)

func simpleWorkflow(ctxt context.Context, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(ctx context.Context, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(ctx context.Context, input string) (string, error) {
	return RunAsStep(ctx, simpleStep, input)
}

func simpleStep(ctx context.Context, input string) (string, error) {
	return "from step", nil
}

func simpleStepError(ctx context.Context, input string) (string, error) {
	return "", fmt.Errorf("step failure")
}

func simpleWorkflowWithStepError(ctx context.Context, input string) (string, error) {
	return RunAsStep(ctx, simpleStepError, input)
}

// idempotencyWorkflow increments a global counter and returns the input
func incrementCounter(_ context.Context, value int64) (int64, error) {
	idempotencyCounter += value
	return idempotencyCounter, nil
}

func idempotencyWorkflow(ctx context.Context, input string) (string, error) {
	incrementCounter(ctx, 1)
	return input, nil
}

var blockingStepStopEvent *Event

func blockingStep(ctx context.Context, input string) (string, error) {
	blockingStepStopEvent.Wait()
	return "", nil
}

var idempotencyWorkflowWithStepEvent *Event

func idempotencyWorkflowWithStep(ctx context.Context, input string) (int64, error) {
	RunAsStep(ctx, incrementCounter, 1)
	idempotencyWorkflowWithStepEvent.Set()
	RunAsStep(ctx, blockingStep, input)
	return idempotencyCounter, nil
}

// Unified struct that demonstrates both pointer and value receiver methods
type workflowStruct struct{}

// Pointer receiver method
func (w *workflowStruct) simpleWorkflow(ctx context.Context, input string) (string, error) {
	return simpleWorkflow(ctx, input)
}

// Value receiver method on the same struct
func (w workflowStruct) simpleWorkflowValue(ctx context.Context, input string) (string, error) {
	return input + "-value", nil
}

// interface for workflow methods
type TestWorkflowInterface interface {
	Execute(ctx context.Context, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(ctx context.Context, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

// Generic workflow function
func Identity[T any](ctx context.Context, in T) (T, error) {
	return in, nil
}

var (
	anonymousWf = WithWorkflow(func(ctx context.Context, in string) (string, error) {
		return "anonymous-" + in, nil
	})
)

// TODO: spin into dbos_test.go
func TestAppVersion(t *testing.T) {
	if _, err := hex.DecodeString(APP_VERSION); err != nil {
		t.Fatalf("APP_VERSION is not a valid hex string: %v", err)
	}

	// Save the original registry content
	originalRegistry := make(map[string]workflowRegistryEntry)
	maps.Copy(originalRegistry, registry)

	// Restore the registry after the test
	defer func() {
		registry = originalRegistry
	}()

	// Replace the registry and verify the hash is different
	registry = make(map[string]workflowRegistryEntry)

	WithWorkflow(func(ctx context.Context, input string) (string, error) {
		return "new-registry-workflow-" + input, nil
	})
	hash2 := computeApplicationVersion()
	if APP_VERSION == hash2 {
		t.Fatalf("APP_VERSION hash did not change after replacing registry")
	}
}

func TestWorkflowsWrapping(t *testing.T) {
	setupDBOS(t)

	type testCase struct {
		name           string
		workflowFunc   func(context.Context, string, ...WorkflowOption) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWf(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				result, err := handle.GetResult(ctx)
				_, err2 := handle.GetResult(ctx)
				if err2 == nil {
					t.Fatal("Second call to GetResult should return an error")
				}
				expectedErrorMsg := "workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?"
				if err2.Error() != expectedErrorMsg {
					t.Fatal("Unexpected error message:", err2, "expected:", expectedErrorMsg)
				}
				return result, err
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWfError(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWfWithStep(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWfStruct(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWfValue(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWfIface(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				// For generic workflow, we need to convert string to int for testing
				handle, err := wfInt(ctx, "42", opts...) // FIXME for now this returns a string because sys db accepts this
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "42", // input not used in this case
			expectedResult: "42", // FIXME make this an int eventually
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := wfClose(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := anonymousWf(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(ctx context.Context, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWfWithStepError(ctx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult(ctx)
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(context.Background(), tc.input, WithWorkflowID(uuid.NewString()))

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				if tc.expectedError != "" && err.Error() != tc.expectedError {
					t.Fatalf("expected error %q but got %q", tc.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error but got: %v", err)
				}
				if result != tc.expectedResult {
					t.Fatalf("expected result %v but got %v", tc.expectedResult, result)
				}
			}
		})
	}
}

var (
	childWf = WithWorkflow(func(ctx context.Context, i int) (string, error) {
		workflowState, ok := ctx.Value(WorkflowStateKey).(*WorkflowState)
		if !ok {
			return "", fmt.Errorf("workflow state not found in context")
		}
		fmt.Println("childWf workflow state:", workflowState)
		expectedCurrentID := fmt.Sprintf("%s-%d", workflowState.WorkflowID, i)
		if workflowState.WorkflowID != expectedCurrentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedCurrentID, workflowState.WorkflowID)
		}
		// XXX right now the steps of a child workflow start with an incremented step ID, because the first step ID is allocated to the child workflow
		return RunAsStep(ctx, simpleStep, "")
	})
	parentWf = WithWorkflow(func(ctx context.Context, i int) (string, error) {
		workflowState, ok := ctx.Value(WorkflowStateKey).(*WorkflowState)
		if !ok {
			return "", fmt.Errorf("workflow state not found in context")
		}
		fmt.Println("parentWf workflow state:", workflowState)

		childHandle, err := childWf(ctx, i)
		if err != nil {
			return "", err
		}

		// Check this wf ID is built correctly
		expectedParentID := fmt.Sprintf("%s-%d", workflowState.WorkflowID, i)
		if workflowState.WorkflowID != expectedParentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedParentID, workflowState.WorkflowID)
		}

		// Verify child workflow ID follows the pattern: parentID-functionID
		childWorkflowID := childHandle.GetWorkflowID()
		expectedChildID := fmt.Sprintf("%s-%d", workflowState.WorkflowID, i)
		if childWorkflowID != expectedChildID {
			return "", fmt.Errorf("expected childWf ID to be %s, got %s", expectedChildID, childWorkflowID)
		}
		return childHandle.GetResult(ctx)
	})
	grandParentWf = WithWorkflow(func(ctx context.Context, _ string) (string, error) {
		for i := range 3 {
			workflowState, ok := ctx.Value(WorkflowStateKey).(*WorkflowState)
			if !ok {
				return "", fmt.Errorf("workflow state not found in context")
			}
			fmt.Println("grandParentWf workflow state:", workflowState)

			childHandle, err := parentWf(ctx, i)
			if err != nil {
				return "", err
			}

			// The handle should a direct handle
			_, ok = childHandle.(*workflowHandle[string])
			if !ok {
				return "", fmt.Errorf("expected childHandle to be of type *workflowHandle[string], got %T", childHandle)
			}

			// Verify child workflow ID follows the pattern: parentID-functionID
			childWorkflowID := childHandle.GetWorkflowID()
			expectedPrefix := fmt.Sprintf("%s-%d", workflowState.WorkflowID, i)
			if childWorkflowID != expectedPrefix {
				return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedPrefix, childWorkflowID)
			}

			// Calling the child a second time should return a polling handle
			childHandle, err = parentWf(ctx, i, WithWorkflowID(childHandle.GetWorkflowID()))
			if err != nil {
				return "", err
			}
			_, ok = childHandle.(*workflowPollingHandle[string])
			if !ok {
				return "", fmt.Errorf("expected childHandle to be of type *workflowPollingHandle[string], got %T", childHandle)
			}

		}

		return "", nil
	})
)

// TODO Check timeouts behaviors for parents and children (e.g. awaited cancelled, etc)
func TestChildWorkflow(t *testing.T) {
	setupDBOS(t)

	t.Run("ChildWorkflowIDPattern", func(t *testing.T) {
		h, err := grandParentWf(context.Background(), "")
		if err != nil {
			t.Fatalf("failed to execute grand parent workflow: %v", err)
		}
		_, err = h.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from grand parent workflow: %v", err)
		}
	})
}

func TestWorkflowIdempotency(t *testing.T) {
	setupDBOS(t)

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "idempotency-test"

		// Execute the same workflow twice with the same ID
		// First execution
		handle1, err := idempotencyWf(context.Background(), input, WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}
		result1, err := handle1.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from first execution: %v", err)
		}

		// Second execution with the same workflow ID
		handle2, err := idempotencyWf(context.Background(), input, WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to execute workflow second time: %v", err)
		}
		result2, err := handle2.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from second execution: %v", err)
		}

		// Verify the second handle is a polling handle
		_, ok := handle2.(*workflowPollingHandle[string])
		if !ok {
			t.Fatalf("expected handle2 to be of type workflowPollingHandle, got %T", handle2)
		}

		// Verify both executions return the same result
		if result1 != result2 {
			t.Fatalf("expected same result from both executions, got %v and %v", result1, result2)
		}

		// Verify the counter was only incremented once (idempotency)
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to be 1 (workflow executed only once), but got %d", idempotencyCounter)
		}
	})
}

func TestWorkflowRecovery(t *testing.T) {
	setupDBOS(t)

	t.Run("RecoveryResumeWhereItLeftOff", func(t *testing.T) {

		// Reset the global counter
		idempotencyCounter = 0

		// First execution - run the workflow once
		input := "recovery-test"
		idempotencyWorkflowWithStepEvent = NewEvent()
		blockingStepStopEvent = NewEvent()
		handle1, err := idempotencyWfWithStep(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}

		idempotencyWorkflowWithStepEvent.Wait() // Wait for the first step to complete. The second spins forever.

		// Run recovery for pending workflows with "local" executor
		recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}

		// Check that we have a single handle in the return list
		if len(recoveredHandles) != 1 {
			t.Fatalf("expected 1 recovered handle, got %d", len(recoveredHandles))
		}

		// Check that the workflow ID from the handle is the same as the first handle
		recoveredHandle := recoveredHandles[0]
		_, ok := recoveredHandle.(*workflowPollingHandle[any])
		if !ok {
			t.Fatalf("expected handle to be of type workflowPollingHandle, got %T", recoveredHandle)
		}
		if recoveredHandle.GetWorkflowID() != handle1.GetWorkflowID() {
			t.Fatalf("expected recovered workflow ID %s, got %s", handle1.GetWorkflowID(), recoveredHandle.GetWorkflowID())
		}

		idempotencyWorkflowWithStepEvent.Clear()
		idempotencyWorkflowWithStepEvent.Wait()

		// Check that the first step was *not* re-executed (idempotency counter is still 1)
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to remain 1 after recovery (idempotent), but got %d", idempotencyCounter)
		}

		// Using ListWorkflows, retrieve the status of the workflow
		workflows, err := getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{handle1.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}

		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow, got %d", len(workflows))
		}

		workflow := workflows[0]

		// Ensure its number of attempts is 2
		if workflow.Attempts != 2 {
			t.Fatalf("expected workflow attempts to be 2, got %d", workflow.Attempts)
		}

		// unlock the workflow & wait for result
		blockingStepStopEvent.Set() // This will allow the blocking step to complete
		result, err := recoveredHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from recovered handle: %v", err)
		}
		if result != idempotencyCounter {
			t.Fatalf("expected result to be %s, got %s", input, result)
		}

	})
}

var (
	maxRecoveryAttempts       = 20
	deadLetterQueueWf         = WithWorkflow(deadLetterQueueWorkflow, WithMaxRetries(maxRecoveryAttempts))
	infiniteDeadLetterQueueWf = WithWorkflow(infiniteDeadLetterQueueWorkflow, WithMaxRetries(-1)) // A negative value means infinite retries
	deadLetterQueueStartEvent *Event
	deadLetterQueueEvent      *Event
	recoveryCount             int64
)

func deadLetterQueueWorkflow(ctx context.Context, input string) (int, error) {
	recoveryCount++
	fmt.Printf("Dead letter queue workflow started, recovery count: %d\n", recoveryCount)
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}

func infiniteDeadLetterQueueWorkflow(ctx context.Context, input string) (int, error) {
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}
func TestWorkflowDeadLetterQueue(t *testing.T) {
	setupDBOS(t)

	t.Run("DeadLetterQueueBehavior", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()
		recoveryCount = 0

		// Start a workflow that blocks forever
		wfID := uuid.NewString()
		handle, err := deadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
		if err != nil {
			t.Fatalf("failed to start dead letter queue workflow: %v", err)
		}
		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()

		// Attempt to recover the blocked workflow the maximum number of times
		for i := range maxRecoveryAttempts {
			_, err := recoverPendingWorkflows(context.Background(), []string{"local"})
			if err != nil {
				t.Fatalf("failed to recover pending workflows on attempt %d: %v", i+1, err)
			}
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
			expectedCount := int64(i + 2) // +1 for initial execution, +1 for each recovery
			if recoveryCount != expectedCount {
				t.Fatalf("expected recovery count to be %d, got %d", expectedCount, recoveryCount)
			}
		}

		// Verify an additional attempt throws a DLQ error and puts the workflow in the DLQ status
		_, err = recoverPendingWorkflows(context.Background(), []string{"local"})
		if err == nil {
			t.Fatal("expected dead letter queue error but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}
		if dbosErr.Code != DeadLetterQueueError {
			t.Fatalf("expected DeadLetterQueueError, got %v", dbosErr.Code)
		}

		// Verify workflow status is RETRIES_EXCEEDED
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}
		if status.Status != WorkflowStatusRetriesExceeded {
			t.Fatalf("expected workflow status to be RETRIES_EXCEEDED, got %v", status.Status)
		}

		// Verify that attempting to start a workflow with the same ID throws a DLQ error
		_, err = deadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
		if err == nil {
			t.Fatal("expected dead letter queue error when restarting workflow with same ID but got none")
		}

		dbosErr, ok = err.(*DBOSError)
		if !ok {
			t.Fatalf("expected DBOSError, got %T", err)
		}
		if dbosErr.Code != DeadLetterQueueError {
			t.Fatalf("expected DeadLetterQueueError, got %v", dbosErr.Code)
		}

		/*
				// TODO: test resume when implemented
				resumedHandle, err := ...

				// Recover pending workflows again - should work without error
				_, err = recoverPendingWorkflows(context.Background(), []string{"local"})
				if err != nil {
					t.Fatalf("failed to recover pending workflows after resume: %v", err)
				}

				// Complete the blocked workflow
				deadLetterQueueEvent.Set()

				// Wait for both handles to complete
				result1, err = handle.GetResult(context.Background())
				if err != nil {
					t.Fatalf("failed to get result from original handle: %v", err)
				}

				result2, err := resumedHandle.GetResult(context.Background())
				if err != nil {
					t.Fatalf("failed to get result from resumed handle: %v", err)
				}

				if result1 != result2 {
					t.Fatalf("expected both handles to return same result, got %v and %v", result1, result2)
				}

				// Verify workflow status is SUCCESS
				status, err = handle.GetStatus()
				if err != nil {
					t.Fatalf("failed to get final workflow status: %v", err)
				}
				if status.Status != WorkflowStatusSuccess {
					t.Fatalf("expected workflow status to be SUCCESS, got %v", status.Status)
				}

			// Verify that retries of a completed workflow do not raise the DLQ exception
			for i := 0; i < maxRecoveryAttempts*2; i++ {
				_, err = deadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
				if err != nil {
					t.Fatalf("unexpected error when retrying completed workflow: %v", err)
				}
			}
		*/
	})

	t.Run("InfiniteRetriesWorkflow", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()

		// Verify that a workflow with MaxRetries=0 (infinite retries) is retried infinitely
		wfID := uuid.NewString()

		handle, err := infiniteDeadLetterQueueWf(context.Background(), "test", WithWorkflowID(wfID))
		if err != nil {
			t.Fatalf("failed to start infinite dead letter queue workflow: %v", err)
		}

		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()
		// Attempt to recover the blocked workflow many times (should never fail)
		handles := []WorkflowHandle[any]{}
		for i := range DEFAULT_MAX_RECOVERY_ATTEMPTS * 2 {
			recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
			if err != nil {
				t.Fatalf("failed to recover pending workflows on attempt %d: %v", i+1, err)
			}
			handles = append(handles, recoveredHandles...)
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
		}

		// Complete the workflow
		deadLetterQueueEvent.Set()

		result, err := handle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from infinite dead letter queue workflow: %v", err)
		}
		if result != 0 {
			t.Fatalf("expected result to be 0, got %v", result)
		}

		// Wait for all handles to complete
		for i, h := range handles {
			result, err := h.GetResult(context.Background())
			if err != nil {
				t.Fatalf("failed to get result from handle %d: %v", i, err)
			}
			if result != 0 {
				t.Fatalf("expected 0 result, got %v", result)
			}
		}
	})
}

var (
	counter    = 0
	counter1Ch = make(chan time.Time, 100)
	_          = WithWorkflow(func(ctx context.Context, scheduledTime time.Time) (string, error) {
		startTime := time.Now()
		// fmt.Println("scheduled time:", scheduledTime, "current time:", startTime)
		counter++
		if counter == 10 {
			return "", fmt.Errorf("counter reached 100, stopping workflow")
		}
		select {
		case counter1Ch <- startTime:
		default:
		}
		return fmt.Sprintf("Scheduled workflow scheduled at time %v and executed at time %v", scheduledTime, startTime), nil
	}, WithSchedule("* * * * * *")) // Every second
)

func TestScheduledWorkflows(t *testing.T) {
	setupDBOS(t)

	// Helper function to collect execution times
	collectExecutionTimes := func(ch chan time.Time, target int, timeout time.Duration) ([]time.Time, error) {
		var executionTimes []time.Time
		for len(executionTimes) < target {
			select {
			case execTime := <-ch:
				executionTimes = append(executionTimes, execTime)
			case <-time.After(timeout):
				return nil, fmt.Errorf("timeout waiting for %d executions, got %d", target, len(executionTimes))
			}
		}
		return executionTimes, nil
	}

	t.Run("ScheduledWorkflowExecution", func(t *testing.T) {
		// Wait for workflow to execute at least 10 times (should take ~9-10 seconds)
		executionTimes, err := collectExecutionTimes(counter1Ch, 10, 10*time.Second)
		if err != nil {
			t.Fatalf("Failed to collect scheduled workflow execution times: %v", err)
		}
		if len(executionTimes) < 10 {
			t.Fatalf("Expected at least 10 executions, got %d", len(executionTimes))
		}

		// Verify timing - each execution should be approximately 1 second apart
		scheduleInterval := 1 * time.Second
		allowedSlack := 1 * time.Second // Allow 500ms slack

		for i, execTime := range executionTimes {
			// Calculate expected execution time based on schedule interval
			expectedTime := executionTimes[0].Add(time.Duration(i+1) * scheduleInterval)

			// Calculate the delta between actual and expected execution time
			delta := execTime.Sub(expectedTime)
			if delta < 0 {
				delta = -delta // Get absolute value
			}

			// Check if delta is within acceptable slack
			if delta > allowedSlack {
				t.Fatalf("Execution %d timing deviation too large: expected around %v, got %v (delta: %v, allowed slack: %v)",
					i+1, expectedTime, execTime, delta, allowedSlack)
			}

			t.Logf("Execution %d: expected %v, actual %v, delta %v", i+1, expectedTime, execTime, delta)
		}

		// Stop the workflowScheduler and check if it stops executing
		currentCounter := counter
		workflowScheduler.Stop()
		time.Sleep(3 * time.Second) // Wait a bit to ensure no more executions
		if counter >= currentCounter+1 {
			t.Fatalf("Scheduled workflow continued executing after stopping scheduler: %d (expected < %d)", counter, currentCounter+1)
		}
	})
}

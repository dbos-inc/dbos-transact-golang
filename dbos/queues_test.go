package dbos

import (
	"context"
	"fmt"
	"testing"
)

/**
This suite tests
[x] Normal wf with a step
[x] enqueued workflow starts a child workflow
[x] workflow enqueues another workflow
[x] recover queued workflow
[] queued workflow times out
[] scheduled workflow enqueues another workflow
*/

var (
	queue                      = NewWorkflowQueue("test-queue")
	queueWf                    = WithWorkflow(queueWorkflow)
	queueIdempotencyWfWithStep = WithWorkflow(queueIdempotencyWorkflowWithStep)
)

// Events for queue recovery testing
var queueBlockingStepStopEvent *Event
var queueIdempotencyWorkflowWithStepEvent *Event

func queueWorkflow(ctx context.Context, input string) (string, error) {
	step1, err := RunAsStep(ctx, queueStep, input)
	if err != nil {
		return "", fmt.Errorf("failed to run step: %v", err)
	}
	return step1, nil
}

func queueStep(ctx context.Context, input string) (string, error) {
	return input, nil
}

func queueWorkflowWithChild(ctx context.Context, input string) (string, error) {
	// Start a child workflow
	childHandle, err := queueWf(ctx, input+"-child")
	if err != nil {
		return "", fmt.Errorf("failed to start child workflow: %v", err)
	}

	// Get result from child workflow
	childResult, err := childHandle.GetResult()
	if err != nil {
		return "", fmt.Errorf("failed to get child result: %v", err)
	}

	return childResult, nil
}

func queueWorkflowThatEnqueues(ctx context.Context, input string) (string, error) {
	// Enqueue another workflow to the same queue
	enqueuedHandle, err := queueWf(ctx, input+"-enqueued", WithQueue(queue.name))
	if err != nil {
		return "", fmt.Errorf("failed to enqueue workflow: %v", err)
	}

	// Get result from the enqueued workflow
	enqueuedResult, err := enqueuedHandle.GetResult()
	if err != nil {
		return "", fmt.Errorf("failed to get enqueued workflow result: %v", err)
	}

	return enqueuedResult, nil
}

func queueBlockingStep(ctx context.Context, input string) (string, error) {
	queueBlockingStepStopEvent.Wait()
	return "", nil
}

// idempotencyWorkflow increments a global counter and returns the input
func queueIdempotencyWorkflowWithStep(ctx context.Context, input string) (int64, error) {
	RunAsStep(ctx, incrementCounter, 1)
	queueIdempotencyWorkflowWithStepEvent.Set()
	RunAsStep(ctx, queueBlockingStep, input)
	return idempotencyCounter, nil
}

func TestWorkflowQueues(t *testing.T) {
	setupDBOS(t)

	t.Run("EnqueueWorkflow", func(t *testing.T) {
		handle, err := queueWf(context.Background(), "test-input", WithQueue(queue.name))
		if err != nil {
			t.Fatalf("failed to enqueue workflow: %v", err)
		}

		_, ok := handle.(*workflowPollingHandle[string])
		if !ok {
			t.Fatalf("expected handle to be of type workflowPollingHandle, got %T", handle)
		}

		res, err := handle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if res != "test-input" {
			t.Fatalf("expected workflow result to be 'test-input', got %v", res)
		}
	})

	t.Run("EnqueuedWorkflowStartsChildWorkflow", func(t *testing.T) {
		queueWfWithChild := WithWorkflow(queueWorkflowWithChild)

		handle, err := queueWfWithChild(context.Background(), "test-input", WithQueue(queue.name))
		if err != nil {
			t.Fatalf("failed to enqueue workflow with child: %v", err)
		}

		res, err := handle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}

		// Expected result: child workflow returns "test-input-child"
		expectedResult := "test-input-child"
		if res != expectedResult {
			t.Fatalf("expected workflow result to be '%s', got %v", expectedResult, res)
		}
	})

	t.Run("WorkflowEnqueuesAnotherWorkflow", func(t *testing.T) {
		queueWfThatEnqueues := WithWorkflow(queueWorkflowThatEnqueues)

		handle, err := queueWfThatEnqueues(context.Background(), "test-input", WithQueue(queue.name))
		if err != nil {
			t.Fatalf("failed to enqueue workflow that enqueues another workflow: %v", err)
		}

		res, err := handle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}

		// Expected result: enqueued workflow returns "test-input-enqueued"
		expectedResult := "test-input-enqueued"
		if res != expectedResult {
			t.Fatalf("expected workflow result to be '%s', got %v", expectedResult, res)
		}
	})

	t.Run("RecoverQueuedWorkflow", func(t *testing.T) {
		// Reset the global counter
		idempotencyCounter = 0

		// First execution - run the queued workflow once
		input := "queue-recovery-test"
		queueIdempotencyWorkflowWithStepEvent = NewEvent()
		queueBlockingStepStopEvent = NewEvent()
		handle1, err := queueIdempotencyWfWithStep(context.Background(), input, WithQueue(queue.name))
		if err != nil {
			t.Fatalf("failed to execute queued workflow first time: %v", err)
		}

		queueIdempotencyWorkflowWithStepEvent.Wait() // Wait for the first step to complete. The second spins forever.

		// Verify the workflow is in pending state before recovery
		workflows, err := getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{handle1.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows before recovery: %v", err)
		}
		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow before recovery, got %d", len(workflows))
		}
		workflowBeforeRecovery := workflows[0]
		if workflowBeforeRecovery.Status != WorkflowStatusPending {
			t.Fatalf("expected workflow status to be PENDING before recovery, got %s", workflowBeforeRecovery.Status)
		}

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

		queueIdempotencyWorkflowWithStepEvent.Clear()
		queueIdempotencyWorkflowWithStepEvent.Wait()

		// Check that the first step was *not* re-executed (idempotency counter is still 1)
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to remain 1 after recovery (idempotent), but got %d", idempotencyCounter)
		}

		// Using ListWorkflows, retrieve the status of the workflow after recovery
		workflows, err = getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{handle1.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows after recovery: %v", err)
		}

		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow, got %d", len(workflows))
		}

		workflow := workflows[0]

		// Ensure its number of attempts is 2
		if workflow.Attempts != 2 {
			t.Fatalf("expected workflow attempts to be 2, got %d. Status: %s", workflow.Attempts, workflow.Status)
		}

		// unlock the workflow & wait for result
		queueBlockingStepStopEvent.Set() // This will allow the blocking step to complete
		result, err := recoveredHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from recovered handle: %v", err)
		}
		if result != idempotencyCounter {
			t.Fatalf("expected result to be %d, got %v", idempotencyCounter, result)
		}
	})
}

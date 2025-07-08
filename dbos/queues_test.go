package dbos

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

/**
This suite tests
[x] Normal wf with a step
[x] enqueued workflow starts a child workflow
[x] workflow enqueues another workflow
[x] recover queued workflow
[x] global concurrency (one at a time with a single queue and a single worker)
[] worker concurrency (2 at a time across two "workers")
[] worker concurrency X recovery
[] rate limiter
[] queued workflow times out
[] scheduled workflow enqueues another workflow
*/

var (
	queue               = NewWorkflowQueue("test-queue")
	queueWf             = WithWorkflow(queueWorkflow)
	queueWfWithChild    = WithWorkflow(queueWorkflowWithChild)
	queueWfThatEnqueues = WithWorkflow(queueWorkflowThatEnqueues)
)

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

		if !queueEntriesAreCleanedUp() {
			t.Fatal("expected queue entries to be cleaned up after global concurrency test")
		}
	})

	t.Run("EnqueuedWorkflowStartsChildWorkflow", func(t *testing.T) {
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

		if !queueEntriesAreCleanedUp() {
			t.Fatal("expected queue entries to be cleaned up after global concurrency test")
		}
	})

	t.Run("WorkflowEnqueuesAnotherWorkflow", func(t *testing.T) {
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

		if !queueEntriesAreCleanedUp() {
			t.Fatal("expected queue entries to be cleaned up after global concurrency test")
		}
	})
}

var (
	testQueue = NewWorkflowQueue("test_queue")

	recoveryStepCounter = 0
	recoveryStepEvents  = make([]*Event, 5) // 5 queued steps
	recoveryEvent       = NewEvent()

	recoveryStepWorkflow = WithWorkflow(func(ctx context.Context, i int) (int, error) {
		recoveryStepCounter++
		recoveryStepEvents[i].Set()
		recoveryEvent.Wait()
		return i, nil
	})

	recoveryWorkflow = WithWorkflow(func(ctx context.Context, input string) ([]int, error) {
		handles := make([]WorkflowHandle[int], 0, 5) // 5 queued steps
		for i := range 5 {
			handle, err := recoveryStepWorkflow(ctx, i, WithQueue(testQueue.name))
			if err != nil {
				return nil, fmt.Errorf("failed to enqueue step %d: %v", i, err)
			}
			handles = append(handles, handle)
		}

		results := make([]int, 0, 5)
		for _, handle := range handles {
			result, err := handle.GetResult()
			if err != nil {
				return nil, fmt.Errorf("failed to get result for handle: %v", err)
			}
			results = append(results, result)
		}
		return results, nil
	})
)

func TestQueueRecovery(t *testing.T) {
	setupDBOS(t)

	queuedSteps := 5

	for i := range recoveryStepEvents {
		recoveryStepEvents[i] = NewEvent()
	}

	wfid := uuid.NewString()

	// Start the workflow. Wait for all steps to start. Verify that they started.
	handle, err := recoveryWorkflow(context.Background(), "", WithWorkflowID(wfid))
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}

	for _, e := range recoveryStepEvents {
		e.Wait()
		e.Clear()
	}

	if recoveryStepCounter != queuedSteps {
		t.Fatalf("expected recoveryStepCounter to be %d, got %d", queuedSteps, recoveryStepCounter)
	}

	// Recover the workflow, then resume it.
	recoveryHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
	if err != nil {
		t.Fatalf("failed to recover pending workflows: %v", err)
	}

	for _, e := range recoveryStepEvents {
		e.Wait()
	}
	recoveryEvent.Set()

	if len(recoveryHandles) != queuedSteps+1 {
		t.Fatalf("expected %d recovery handles, got %d", queuedSteps+1, len(recoveryHandles))
	}

	for _, h := range recoveryHandles {
		if h.GetWorkflowID() == wfid {
			// Root workflow case
			result, err := h.GetResult()
			if err != nil {
				t.Fatalf("failed to get result from recovered root workflow handle: %v", err)
			}
			castedResult, ok := result.([]int)
			if !ok {
				t.Fatalf("expected result to be of type []int for root workflow, got %T", result)
			}
			expectedResult := []int{0, 1, 2, 3, 4}
			if !equal(castedResult, expectedResult) {
				t.Fatalf("expected result %v, got %v", expectedResult, castedResult)
			}
		}
	}

	result, err := handle.GetResult()
	if err != nil {
		t.Fatalf("failed to get result from original handle: %v", err)
	}
	expectedResult := []int{0, 1, 2, 3, 4}
	if !equal(result, expectedResult) {
		t.Fatalf("expected result %v, got %v", expectedResult, result)
	}

	if recoveryStepCounter != queuedSteps*2 {
		t.Fatalf("expected recoveryStepCounter to be %d, got %d", queuedSteps*2, recoveryStepCounter)
	}

	// Rerun the workflow. Because each step is complete, none should start again.
	rerunHandle, err := recoveryWorkflow(context.Background(), "test-input", WithWorkflowID(wfid))
	if err != nil {
		t.Fatalf("failed to rerun workflow: %v", err)
	}
	rerunResult, err := rerunHandle.GetResult()
	if err != nil {
		t.Fatalf("failed to get result from rerun handle: %v", err)
	}
	if !equal(rerunResult, expectedResult) {
		t.Fatalf("expected result %v, got %v", expectedResult, rerunResult)
	}

	if recoveryStepCounter != queuedSteps*2 {
		t.Fatalf("expected recoveryStepCounter to remain %d, got %d", queuedSteps*2, recoveryStepCounter)
	}

	if !queueEntriesAreCleanedUp() {
		t.Fatal("expected queue entries to be cleaned up after global concurrency test")
	}
}

var (
	globalConcurrencyQueue    = NewWorkflowQueue("test-worker-concurrency-queue", WithGlobalConcurrency(1))
	workflowEvent1            = NewEvent()
	workflowEvent2            = NewEvent()
	workflowDoneEvent         = NewEvent()
	globalConcurrencyWorkflow = WithWorkflow(func(ctx context.Context, input string) (string, error) {
		switch input {
		case "workflow1":
			workflowEvent1.Set()
			workflowDoneEvent.Wait()
		case "workflow2":
			workflowEvent2.Set()
		}
		return input, nil
	})
)

func TestGlobalConcurrency(t *testing.T) {
	setupDBOS(t)

	// Enqueue two workflows
	handle1, err := globalConcurrencyWorkflow(context.Background(), "workflow1", WithQueue(globalConcurrencyQueue.name))
	if err != nil {
		t.Fatalf("failed to enqueue workflow1: %v", err)
	}

	handle2, err := globalConcurrencyWorkflow(context.Background(), "workflow2", WithQueue(globalConcurrencyQueue.name))
	if err != nil {
		t.Fatalf("failed to enqueue workflow2: %v", err)
	}

	// Wait for the first workflow to start
	workflowEvent1.Wait()

	// Ensure the second workflow has not started yet
	if workflowEvent2.IsSet {
		t.Fatalf("expected workflow2 to not start while workflow1 is running")
	}
	status, err := handle2.GetStatus()
	if err != nil {
		t.Fatalf("failed to get status of workflow2: %v", err)
	}
	if status != WorkflowStatusEnqueued {
		t.Fatalf("expected workflow2 to be in ENQUEUED status, got %v", status)
	}

	// Allow the first workflow to complete
	workflowDoneEvent.Set()

	result1, err := handle1.GetResult()
	if err != nil {
		t.Fatalf("failed to get result from workflow1: %v", err)
	}
	if result1 != "workflow1" {
		t.Fatalf("expected result from workflow1 to be 'workflow1', got %v", result1)
	}

	// Wait for the second workflow to start
	workflowEvent2.Wait()

	result2, err := handle2.GetResult()
	if err != nil {
		t.Fatalf("failed to get result from workflow2: %v", err)
	}
	if result2 != "workflow2" {
		t.Fatalf("expected result from workflow2 to be 'workflow2', got %v", result2)
	}
	if !queueEntriesAreCleanedUp() {
		t.Fatal("expected queue entries to be cleaned up after global concurrency test")
	}
}

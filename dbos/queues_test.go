package dbos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

/**
This suite tests
[x] Normal wf with a step
[x] enqueued workflow starts a child workflow
[x] workflow enqueues another workflow
[x] recover queued workflow
[x] queued workflow DLQ
[x] global concurrency (one at a time with a single queue and a single worker)
[x] worker concurrency (2 at a time across two "workers")
[x] worker concurrency X recovery
[x] rate limiter
[] queue deduplication
[] queue priority
[] queued workflow times out
[] scheduled workflow enqueues another workflow
*/

var (
	queue               = NewWorkflowQueue("test-queue")
	queueWf             = WithWorkflow(queueWorkflow)
	queueWfWithChild    = WithWorkflow(queueWorkflowWithChild)
	queueWfThatEnqueues = WithWorkflow(queueWorkflowThatEnqueues)

	// Variables for successive enqueue test
	dlqEnqueueQueue    = NewWorkflowQueue("test-successive-enqueue-queue")
	dlqStartEvent      = NewEvent()
	dlqCompleteEvent   = NewEvent()
	dlqMaxRetries      = 10
	enqueueWorkflowDLQ = WithWorkflow(func(ctx context.Context, input string) (string, error) {
		dlqStartEvent.Set()
		dlqCompleteEvent.Wait()
		return input, nil
	}, WithMaxRetries(dlqMaxRetries))
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
	childResult, err := childHandle.GetResult(ctx)
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
	enqueuedResult, err := enqueuedHandle.GetResult(ctx)
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

		res, err := handle.GetResult(context.Background())
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

		res, err := handle.GetResult(context.Background())
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

		res, err := handle.GetResult(context.Background())
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

	t.Run("DynamicRegistration", func(t *testing.T) {
		q := NewWorkflowQueue("dynamic-queue")
		if len(q.name) > 0 {
			t.Fatalf("expected nil queue for dynamic registration after DBOS initialization, got %v", q)
		}
	})

	t.Run("QueueWorkflowDLQ", func(t *testing.T) {
		workflowID := "blocking-workflow-test"

		// Enqueue the workflow for the first time
		originalHandle, err := enqueueWorkflowDLQ(context.Background(), "test-input", WithQueue(dlqEnqueueQueue.name), WithWorkflowID(workflowID))
		if err != nil {
			t.Fatalf("failed to enqueue blocking workflow: %v", err)
		}

		// Wait for the workflow to start
		dlqStartEvent.Wait()
		dlqStartEvent.Clear()

		// Try to enqueue the same workflow more times
		for i := range dlqMaxRetries * 2 {
			_, err := enqueueWorkflowDLQ(context.Background(), "test-input", WithQueue(dlqEnqueueQueue.name), WithWorkflowID(workflowID))
			if err != nil {
				t.Fatalf("failed to enqueue workflow attempt %d: %v", i+1, err)
			}
		}

		// Get the status from the original handle and check the attempts counter
		status, err := originalHandle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get status of original workflow handle: %v", err)
		}

		// The attempts counter should still be 1 (the original enqueue)
		if status.Attempts != 1 {
			t.Fatalf("expected attempts to be 1, got %d", status.Attempts)
		}

		// Check that the workflow hits DLQ after re-running max retries
		handles := make([]WorkflowHandle[any], 0, dlqMaxRetries+1)
		for i := range dlqMaxRetries {
			recoveryHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
			if err != nil {
				t.Fatalf("failed to recover pending workflows: %v", err)
			}
			if len(recoveryHandles) != 1 {
				t.Fatalf("expected 1 handle, got %d", len(recoveryHandles))
			}
			dlqStartEvent.Wait()
			dlqStartEvent.Clear()
			handle := recoveryHandles[0]
			handles = append(handles, handle)
			status, err := handle.GetStatus()
			if err != nil {
				t.Fatalf("failed to get status of recovered workflow handle: %v", err)
			}
			if i == dlqMaxRetries {
				// On the last retry, the workflow should be in DLQ
				if status.Status != WorkflowStatusRetriesExceeded {
					t.Fatalf("expected workflow status to be %s, got %v", WorkflowStatusRetriesExceeded, status.Status)
				}
			}
		}

		// Check the workflow completes
		dlqCompleteEvent.Set()
		for _, handle := range handles {
			result, err := handle.GetResult(context.Background())
			if err != nil {
				t.Fatalf("failed to get result from recovered workflow handle: %v", err)
			}
			if result != "test-input" {
				t.Fatalf("expected result to be 'test-input', got %v", result)
			}
		}

		if !queueEntriesAreCleanedUp() {
			t.Fatal("expected queue entries to be cleaned up after successive enqueues test")
		}
	})
}

var (
	recoveryQueue = NewWorkflowQueue("recovery-queue")

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
			handle, err := recoveryStepWorkflow(ctx, i, WithQueue(recoveryQueue.name))
			if err != nil {
				return nil, fmt.Errorf("failed to enqueue step %d: %v", i, err)
			}
			handles = append(handles, handle)
		}

		results := make([]int, 0, 5)
		for _, handle := range handles {
			result, err := handle.GetResult(ctx)
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
			result, err := h.GetResult(context.Background())
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

	result, err := handle.GetResult(context.Background())
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
	rerunResult, err := rerunHandle.GetResult(context.Background())
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
	globalConcurrencyQueue    = NewWorkflowQueue("test-global-concurrency-queue", WithGlobalConcurrency(1))
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
	time.Sleep(2 * time.Second) // Wait for a few seconds to let the queue runner loop

	// Ensure the second workflow has not started yet
	if workflowEvent2.IsSet {
		t.Fatalf("expected workflow2 to not start while workflow1 is running")
	}
	status, err := handle2.GetStatus()
	if err != nil {
		t.Fatalf("failed to get status of workflow2: %v", err)
	}
	if status.Status != WorkflowStatusEnqueued {
		t.Fatalf("expected workflow2 to be in ENQUEUED status, got %v", status.Status)
	}

	// Allow the first workflow to complete
	workflowDoneEvent.Set()

	result1, err := handle1.GetResult(context.Background())
	if err != nil {
		t.Fatalf("failed to get result from workflow1: %v", err)
	}
	if result1 != "workflow1" {
		t.Fatalf("expected result from workflow1 to be 'workflow1', got %v", result1)
	}

	// Wait for the second workflow to start
	workflowEvent2.Wait()

	result2, err := handle2.GetResult(context.Background())
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

var (
	workerConcurrencyQueue = NewWorkflowQueue("test-worker-concurrency-queue", WithWorkerConcurrency(1))
	startEvents            = []*Event{
		NewEvent(),
		NewEvent(),
		NewEvent(),
		NewEvent(),
	}
	completeEvents = []*Event{
		NewEvent(),
		NewEvent(),
		NewEvent(),
		NewEvent(),
	}
	blockingWf = WithWorkflow(func(ctx context.Context, i int) (int, error) {
		// Simulate a blocking operation
		startEvents[i].Set()
		completeEvents[i].Wait()
		return i, nil
	})
)

func TestWorkerConcurrency(t *testing.T) {
	setupDBOS(t)

	// First enqueue four blocking workflows
	handle1, err := blockingWf(context.Background(), 0, WithQueue(workerConcurrencyQueue.name), WithWorkflowID("worker-cc-wf-1"))
	if err != nil {
		t.Fatalf("failed to enqueue blocking workflow 1: %v", err)
	}
	handle2, err := blockingWf(context.Background(), 1, WithQueue(workerConcurrencyQueue.name), WithWorkflowID("worker-cc-wf-2"))
	if err != nil {
		t.Fatalf("failed to enqueue blocking workflow 2: %v", err)
	}
	_, err = blockingWf(context.Background(), 2, WithQueue(workerConcurrencyQueue.name), WithWorkflowID("worker-cc-wf-3"))
	if err != nil {
		t.Fatalf("failed to enqueue blocking workflow 3: %v", err)
	}
	_, err = blockingWf(context.Background(), 3, WithQueue(workerConcurrencyQueue.name), WithWorkflowID("worker-cc-wf-4"))
	if err != nil {
		t.Fatalf("failed to enqueue blocking workflow 4: %v", err)
	}

	// wait for the blocking workflow to start
	startEvents[0].Wait()
	// Ensure the three other workflows are not started yet
	if startEvents[1].IsSet || startEvents[2].IsSet || startEvents[3].IsSet {
		t.Fatal("expected only blocking workflow 1 to start, but others have started")
	}
	workflows, err := dbos.systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
		status:    []WorkflowStatusType{WorkflowStatusEnqueued},
		queueName: workerConcurrencyQueue.name,
	})
	if err != nil {
		t.Fatalf("failed to list workflows: %v", err)
	}
	if len(workflows) != 3 {
		t.Fatalf("expected 3 workflows to be enqueued, got %d", len(workflows))
	}

	// Stop the queue runner before changing executor ID to avoid race conditions
	stopQueueRunner()
	// Change the EXECUTOR_ID global variable to a different value
	_EXECUTOR_ID = "worker-2"
	// Restart the queue runner
	restartQueueRunner()

	// Wait for the second workflow to start on the second worker
	startEvents[1].Wait()
	// Ensure the two other workflows are not started yet
	if startEvents[2].IsSet || startEvents[3].IsSet {
		t.Fatal("expected only blocking workflow 2 to start, but others have started")
	}
	workflows, err = dbos.systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
		status:    []WorkflowStatusType{WorkflowStatusEnqueued},
		queueName: workerConcurrencyQueue.name,
	})
	if err != nil {
		t.Fatalf("failed to list workflows: %v", err)
	}
	if len(workflows) != 2 {
		t.Fatalf("expected 2 workflows to be enqueued, got %d", len(workflows))
	}

	// Unlock workflow 1, check wf 3 starts, check 4 stays blocked
	completeEvents[0].Set()
	result1, err := handle1.GetResult(context.Background())
	if err != nil {
		t.Fatalf("failed to get result from blocking workflow 1: %v", err)
	}
	if result1 != 0 {
		t.Fatalf("expected result from blocking workflow 1 to be 0, got %v", result1)
	}
	// Stop the queue runner before changing executor ID to avoid race conditions
	stopQueueRunner()
	// Change the executor again and wait for the third workflow to start
	_EXECUTOR_ID = "local"
	// Restart the queue runner
	restartQueueRunner()
	startEvents[2].Wait()
	// Ensure the fourth workflow is not started yet
	if startEvents[3].IsSet {
		t.Fatal("expected only blocking workflow 3 to start, but workflow 4 has started")
	}
	// Check that only one workflow is pending
	workflows, err = dbos.systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
		status:    []WorkflowStatusType{WorkflowStatusEnqueued},
		queueName: workerConcurrencyQueue.name,
	})
	if err != nil {
		t.Fatalf("failed to list workflows: %v", err)
	}
	if len(workflows) != 1 {
		t.Fatalf("expected 1 workflow to be enqueued, got %d", len(workflows))
	}

	// Unlock workflow 2 and check wf 4 starts
	completeEvents[1].Set()
	result2, err := handle2.GetResult(context.Background())
	if err != nil {
		t.Fatalf("failed to get result from blocking workflow 2: %v", err)
	}
	if result2 != 1 {
		t.Fatalf("expected result from blocking workflow 2 to be 1, got %v", result2)
	}
	// Stop the queue runner before changing executor ID to avoid race conditions
	stopQueueRunner()
	// change executor again and wait for the fourth workflow to start
	_EXECUTOR_ID = "worker-2"
	// Restart the queue runner
	restartQueueRunner()
	startEvents[3].Wait()
	// Check no workflow is enqueued
	workflows, err = dbos.systemDB.ListWorkflows(context.Background(), listWorkflowsDBInput{
		status:    []WorkflowStatusType{WorkflowStatusEnqueued},
		queueName: workerConcurrencyQueue.name,
	})
	if err != nil {
		t.Fatalf("failed to list workflows: %v", err)
	}
	if len(workflows) != 0 {
		t.Fatalf("expected 0 workflows to be enqueued, got %d", len(workflows))
	}

	// Unblock both workflows 3 and 4
	completeEvents[2].Set()
	completeEvents[3].Set()

	if !queueEntriesAreCleanedUp() {
		t.Fatal("expected queue entries to be cleaned up after global concurrency test")
	}

	_EXECUTOR_ID = "local" // Reset executor ID for future tests
}

var (
	workerConcurrencyRecoveryQueue          = NewWorkflowQueue("test-worker-concurrency-recovery-queue", WithWorkerConcurrency(1))
	workerConcurrencyRecoveryStartEvent1    = NewEvent()
	workerConcurrencyRecoveryStartEvent2    = NewEvent()
	workerConcurrencyRecoveryCompleteEvent1 = NewEvent()
	workerConcurrencyRecoveryCompleteEvent2 = NewEvent()
	workerConcurrencyRecoveryBlockingWf1    = WithWorkflow(func(ctx context.Context, input string) (string, error) {
		workerConcurrencyRecoveryStartEvent1.Set()
		workerConcurrencyRecoveryCompleteEvent1.Wait()
		return input, nil
	})
	workerConcurrencyRecoveryBlockingWf2 = WithWorkflow(func(ctx context.Context, input string) (string, error) {
		workerConcurrencyRecoveryStartEvent2.Set()
		workerConcurrencyRecoveryCompleteEvent2.Wait()
		return input, nil
	})
)

func TestWorkerConcurrencyXRecovery(t *testing.T) {
	setupDBOS(t)

	// Enqueue two workflows on a queue with worker concurrency = 1
	handle1, err := workerConcurrencyRecoveryBlockingWf1(context.Background(), "workflow1", WithQueue(workerConcurrencyRecoveryQueue.name), WithWorkflowID("worker-cc-x-recovery-wf-1"))
	if err != nil {
		t.Fatalf("failed to enqueue blocking workflow 1: %v", err)
	}
	handle2, err := workerConcurrencyRecoveryBlockingWf2(context.Background(), "workflow2", WithQueue(workerConcurrencyRecoveryQueue.name), WithWorkflowID("worker-cc-x-recovery-wf-2"))
	if err != nil {
		t.Fatalf("failed to enqueue blocking workflow 2: %v", err)
	}

	// Start the first workflow and wait for it to start
	workerConcurrencyRecoveryStartEvent1.Wait()
	workerConcurrencyRecoveryStartEvent1.Clear()

	// Ensure the 2nd workflow is still ENQUEUED
	status2, err := handle2.GetStatus()
	if err != nil {
		t.Fatalf("failed to get status of workflow2: %v", err)
	}
	if status2.Status != WorkflowStatusEnqueued {
		t.Fatalf("expected workflow2 to be in ENQUEUED status, got %v", status2.Status)
	}

	// Verify workflow2 hasn't started yet
	if workerConcurrencyRecoveryStartEvent2.IsSet {
		t.Fatal("expected workflow2 to not start while workflow1 is running")
	}

	// Now, manually call the recoverPendingWorkflows method
	recoveryHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
	if err != nil {
		t.Fatalf("failed to recover pending workflows: %v", err)
	}

	// You should get 1 handle associated with the first workflow
	if len(recoveryHandles) != 1 {
		t.Fatalf("expected 1 recovery handle, got %d", len(recoveryHandles))
	}

	// The handle status should tell you the workflow is ENQUEUED
	recoveredHandle := recoveryHandles[0]
	if recoveredHandle.GetWorkflowID() != "worker-cc-x-recovery-wf-1" {
		t.Fatalf("expected recovered handle to be for workflow1, got %s", recoveredHandle.GetWorkflowID())
	}
	wf1Status, err := recoveredHandle.GetStatus()
	if err != nil {
		t.Fatalf("failed to get status of recovered workflow1: %v", err)
	}
	if wf1Status.Status != WorkflowStatusEnqueued {
		t.Fatalf("expected recovered handle to be in ENQUEUED status, got %v", wf1Status.Status)
	}

	// The 1 first workflow should have been dequeued again (FIFO ordering) and the 2nd workflow should still be enqueued
	workerConcurrencyRecoveryStartEvent1.Wait()
	status2, err = handle2.GetStatus()
	if err != nil {
		t.Fatalf("failed to get status of workflow2: %v", err)
	}
	if status2.Status != WorkflowStatusEnqueued {
		t.Fatalf("expected workflow2 to still be in ENQUEUED status, got %v", status2.Status)
	}

	// Let the 1st workflow complete and let the 2nd workflow start and complete
	workerConcurrencyRecoveryCompleteEvent1.Set()
	workerConcurrencyRecoveryStartEvent2.Wait()
	workerConcurrencyRecoveryCompleteEvent2.Set()

	// Get result from first workflow
	result1, err := handle1.GetResult(context.Background())
	if err != nil {
		t.Fatalf("failed to get result from workflow1: %v", err)
	}
	if result1 != "workflow1" {
		t.Fatalf("expected result from workflow1 to be 'workflow1', got %v", result1)
	}

	// Get result from second workflow
	result2, err := handle2.GetResult(context.Background())
	if err != nil {
		t.Fatalf("failed to get result from workflow2: %v", err)
	}
	if result2 != "workflow2" {
		t.Fatalf("expected result from workflow2 to be 'workflow2', got %v", result2)
	}

	// Ensure queueEntriesAreCleanedUp is set to true
	if !queueEntriesAreCleanedUp() {
		t.Fatal("expected queue entries to be cleaned up after worker concurrency recovery test")
	}
}

var (
	rateLimiterQueue    = NewWorkflowQueue("test-rate-limiter-queue", WithRateLimiter(&RateLimiter{Limit: 5, Period: 1.8}))
	rateLimiterWorkflow = WithWorkflow(rateLimiterTestWorkflow)
)

func rateLimiterTestWorkflow(ctx context.Context, _ string) (time.Time, error) {
	return time.Now(), nil // Return current time
}

func TestQueueRateLimiter(t *testing.T) {
	setupDBOS(t)

	limit := 5
	period := 1.8
	numWaves := 3

	var handles []WorkflowHandle[time.Time]
	var times []time.Time

	// Launch a number of tasks equal to three times the limit.
	// This should lead to three "waves" of the limit tasks being
	// executed simultaneously, followed by a wait of the period,
	// followed by the next wave.
	for i := 0; i < limit*numWaves; i++ {
		handle, err := rateLimiterWorkflow(context.Background(), "", WithQueue(rateLimiterQueue.name))
		if err != nil {
			t.Fatalf("failed to enqueue workflow %d: %v", i, err)
		}
		handles = append(handles, handle)
	}

	// Get results from all workflows
	for _, handle := range handles {
		result, err := handle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("failed to get result from workflow: %v", err)
		}
		// XXX in reality this should use the actual start time -- not the completion time.
		times = append(times, result)
	}

	// We'll now group the workflows into "waves" based on their start times, and verify that each wave has fewer than the limit of workflows.

	// Sort times to ensure we process them in chronological order
	sortedTimes := make([]time.Time, len(times))
	copy(sortedTimes, times)
	// Simple sort implementation for time.Time slice
	for i := range sortedTimes {
		for j := i + 1; j < len(sortedTimes); j++ {
			if sortedTimes[j].Before(sortedTimes[i]) {
				sortedTimes[i], sortedTimes[j] = sortedTimes[j], sortedTimes[i]
			}
		}
	}

	// Dynamically compute waves based on start times
	if len(sortedTimes) == 0 {
		t.Fatal("no workflow times recorded")
	}

	baseTime := sortedTimes[0]
	waveMap := make(map[int][]time.Time)

	// Group workflows into waves based on their start time
	for _, workflowTime := range sortedTimes {
		timeSinceBase := workflowTime.Sub(baseTime).Seconds()
		waveIndex := int(timeSinceBase / period)
		waveMap[waveIndex] = append(waveMap[waveIndex], workflowTime)
	}
	// Verify each wave has fewer than the limit
	for waveIndex, wave := range waveMap {
		if len(wave) > limit {
			t.Fatalf("wave %d has %d workflows, which exceeds the limit of %d", waveIndex, len(wave), limit)
		}
		if len(wave) == 0 {
			t.Fatalf("wave %d is empty, which shouldn't happen", waveIndex)
		}
	}
	// Verify we have the expected number of waves (allowing some tolerance)
	expectedWaves := numWaves
	if len(waveMap) < expectedWaves-1 || len(waveMap) > expectedWaves+1 {
		t.Fatalf("expected approximately %d waves, got %d", expectedWaves, len(waveMap))
	}

	// Verify all workflows get the SUCCESS status eventually
	for i, handle := range handles {
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get status for workflow %d: %v", i, err)
		}
		if status.Status != WorkflowStatusSuccess {
			t.Fatalf("expected workflow %d to have SUCCESS status, got %v", i, status.Status)
		}
	}

	// Verify all queue entries eventually get cleaned up.
	if !queueEntriesAreCleanedUp() {
		t.Fatal("expected queue entries to be cleaned up after rate limiter test")
	}
}

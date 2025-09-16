package dbos

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnqueue(t *testing.T) {
	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, true, true)

	// Create queue for communication between client and server
	queue := NewWorkflowQueue(serverCtx, "client-enqueue-queue")

	// Create a priority-enabled queue with max concurrency of 1 to ensure ordering
	// Must be created before Launch()
	priorityQueue := NewWorkflowQueue(serverCtx, "priority-test-queue", WithGlobalConcurrency(1), WithPriorityEnabled())

	// Track execution order for priority test
	var executionOrder []string
	var mu sync.Mutex

	// Register workflows with custom names so client can reference them
	type wfInput struct {
		Input string
	}
	serverWorkflow := func(ctx DBOSContext, input wfInput) (string, error) {
		if input.Input != "test-input" {
			return "", fmt.Errorf("unexpected input: %s", input.Input)
		}
		return "processed: " + input.Input, nil
	}
	RegisterWorkflow(serverCtx, serverWorkflow, WithWorkflowName("ServerWorkflow"))

	// Workflow that blocks until cancelled (for timeout test)
	blockingWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(10 * time.Second):
			return "should-never-complete", nil
		}
	}
	RegisterWorkflow(serverCtx, blockingWorkflow, WithWorkflowName("BlockingWorkflow"))

	// Register a workflow that records its execution order (for priority test)
	priorityWorkflow := func(ctx DBOSContext, input string) (string, error) {
		mu.Lock()
		executionOrder = append(executionOrder, input)
		mu.Unlock()
		return input, nil
	}
	RegisterWorkflow(serverCtx, priorityWorkflow, WithWorkflowName("PriorityWorkflow"))

	// Launch the server context to start processing tasks
	err := serverCtx.Launch()
	require.NoError(t, err)

	// Setup client - this will enqueue tasks
	databaseURL := getDatabaseURL()
	config := Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	t.Run("EnqueueAndGetResult", func(t *testing.T) {
		// Client enqueues a task using the new Enqueue method
		handle, err := Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err)

		// Verify we got a polling handle
		_, ok := handle.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle to be of type workflowPollingHandle, got %T", handle)

		// Client retrieves the result
		result, err := handle.GetResult()
		require.NoError(t, err)

		expectedResult := "processed: test-input"
		assert.Equal(t, expectedResult, result)

		// Verify the workflow status
		status, err := handle.GetStatus()
		require.NoError(t, err)

		assert.Equal(t, WorkflowStatusSuccess, status.Status)
		assert.Equal(t, "ServerWorkflow", status.Name)
		assert.Equal(t, queue.Name, status.QueueName)

		assert.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueueWithCustomWorkflowID", func(t *testing.T) {
		customWorkflowID := "custom-client-workflow-id"

		// Client enqueues a task with a custom workflow ID
		_, err := Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueWorkflowID(customWorkflowID))
		require.NoError(t, err)

		// Verify the workflow ID is what we set
		retrieveHandle, err := client.RetrieveWorkflow(customWorkflowID)
		require.NoError(t, err)

		result, err := retrieveHandle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, "processed: test-input", result)

		assert.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueueWithTimeout", func(t *testing.T) {
		handle, err := Enqueue[string, string](client, queue.Name, "BlockingWorkflow", "blocking-input",
			WithEnqueueTimeout(500*time.Millisecond))
		require.NoError(t, err)

		// Should timeout when trying to get result
		_, err = handle.GetResult()
		require.Error(t, err, "expected timeout error, but got none")

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T (%v)", err, err)

		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code)

		// Verify workflow is cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err)

		assert.Equal(t, WorkflowStatusCancelled, status.Status)
	})

	t.Run("EnqueueWithPriority", func(t *testing.T) {
		// Reset execution order for this test
		mu.Lock()
		executionOrder = []string{}
		mu.Unlock()

		// Enqueue workflow without priority (will use default priority of 0)
		handle1, err := Enqueue[string, string](client, priorityQueue.Name, "PriorityWorkflow", "abc",
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow without priority")

		// Enqueue with a lower priority (higher number = lower priority)
		handle2, err := Enqueue[string, string](client, priorityQueue.Name, "PriorityWorkflow", "def",
			WithEnqueuePriority(5),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow with priority 5")

		// Enqueue with a higher priority (lower number = higher priority)
		handle3, err := Enqueue[string, string](client, priorityQueue.Name, "PriorityWorkflow", "ghi",
			WithEnqueuePriority(1),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow with priority 1")

		// Get results
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		assert.Equal(t, "abc", result1)

		result3, err := handle3.GetResult()
		require.NoError(t, err, "failed to get result from third workflow")
		assert.Equal(t, "ghi", result3)

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second workflow")
		assert.Equal(t, "def", result2)

		// Verify execution order: workflows should execute in priority order
		// Priority 0 (abc) executes first (already running when others are enqueued)
		// Priority 1 (ghi) executes second (higher priority than def)
		// Priority 5 (def) executes last (lowest priority)
		expectedOrder := []string{"abc", "ghi", "def"}
		assert.Equal(t, expectedOrder, executionOrder, "workflows should execute in priority order")

		// Verify queue entries are cleaned up
		assert.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after priority test")
	})

	t.Run("EnqueueWithDedupID", func(t *testing.T) {
		dedupID := "my-client-dedup-id"
		wfid1 := "client-dedup-wf1"
		wfid2 := "client-dedup-wf2"

		// First workflow with deduplication ID - should succeed
		handle1, err := Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueWorkflowID(wfid1),
			WithEnqueueDeduplicationID(dedupID),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue first workflow with deduplication ID")

		// Second workflow with same deduplication ID but different workflow ID - should fail
		_, err = Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueWorkflowID(wfid2),
			WithEnqueueDeduplicationID(dedupID),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.Error(t, err, "expected error when enqueueing workflow with same deduplication ID")

		// Check that it's the correct error type and message
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		assert.Equal(t, QueueDeduplicated, dbosErr.Code, "expected error code to be QueueDeduplicated")

		expectedMsgPart := fmt.Sprintf("Workflow %s was deduplicated due to an existing workflow in queue %s with deduplication ID %s", wfid2, queue.Name, dedupID)
		assert.Contains(t, err.Error(), expectedMsgPart, "expected error message to contain deduplication information")

		// Third workflow with different deduplication ID - should succeed
		handle3, err := Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueDeduplicationID("different-dedup-id"),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow with different deduplication ID")

		// Fourth workflow without deduplication ID - should succeed
		handle4, err := Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow without deduplication ID")

		// Wait for all successful workflows to complete
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		assert.Equal(t, "processed: test-input", result1)

		result3, err := handle3.GetResult()
		require.NoError(t, err, "failed to get result from third workflow")
		assert.Equal(t, "processed: test-input", result3)

		result4, err := handle4.GetResult()
		require.NoError(t, err, "failed to get result from fourth workflow")
		assert.Equal(t, "processed: test-input", result4)

		// After first workflow completes, we should be able to enqueue with same deduplication ID
		handle5, err := Enqueue[wfInput, string](client, queue.Name, "ServerWorkflow", wfInput{Input: "test-input"},
			WithEnqueueWorkflowID(wfid2),        // Reuse the workflow ID that failed before
			WithEnqueueDeduplicationID(dedupID), // Same deduplication ID as first workflow
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow with same dedup ID after completion")

		result5, err := handle5.GetResult()
		require.NoError(t, err, "failed to get result from fifth workflow")
		assert.Equal(t, "processed: test-input", result5)

		assert.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after deduplication test")
	})

	// Verify all queue entries are cleaned up
	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after client tests")
}

func TestCancelResume(t *testing.T) {
	var stepsCompleted int

	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, true, true)

	// Create queue for communication between client and server
	queue := NewWorkflowQueue(serverCtx, "cancel-resume-queue")

	// Step functions
	step := func(ctx context.Context) (string, error) {
		stepsCompleted++
		return "step-complete", nil
	}

	// Events for synchronization
	workflowStarted := NewEvent()
	proceedSignal := NewEvent()

	// Workflow that executes steps with blocking behavior
	cancelResumeWorkflow := func(ctx DBOSContext, input int) (int, error) {
		// Execute step one
		_, err := RunAsStep(ctx, step)
		if err != nil {
			return 0, err
		}

		// Signal that workflow has started and step one completed
		workflowStarted.Set()

		// Wait for signal from main test to proceed
		proceedSignal.Wait()

		// Execute step two (will only happen if not cancelled)
		_, err = RunAsStep(ctx, step)
		if err != nil {
			return 0, err
		}

		return input, nil
	}
	RegisterWorkflow(serverCtx, cancelResumeWorkflow, WithWorkflowName("CancelResumeWorkflow"))

	// Timeout blocking workflow that spins until context is done
	timeoutBlockingWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		for {
			select {
			case <-ctx.Done():
				return "cancelled", ctx.Err()
			default:
				// Small sleep to avoid tight loop
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	RegisterWorkflow(serverCtx, timeoutBlockingWorkflow, WithWorkflowName("TimeoutBlockingWorkflow"))

	// Launch the server context to start processing tasks
	err := serverCtx.Launch()
	require.NoError(t, err)

	// Setup client - this will enqueue tasks
	databaseURL := getDatabaseURL()
	config := Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	t.Run("CancelAndResume", func(t *testing.T) {
		// Reset the global counter
		stepsCompleted = 0
		input := 5
		workflowID := "test-cancel-resume-workflow"

		// Start the workflow - it will execute step one and then wait
		handle, err := Enqueue[int, int](client, queue.Name, "CancelResumeWorkflow", input,
			WithEnqueueWorkflowID(workflowID),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow from client")

		// Wait for workflow to signal it has started and step one completed
		workflowStarted.Wait()

		// Verify step one completed but step two hasn't
		assert.Equal(t, 1, stepsCompleted, "expected steps completed to be 1")

		// Cancel the workflow
		err = client.CancelWorkflow(workflowID)
		require.NoError(t, err, "failed to cancel workflow")

		// Verify workflow is cancelled
		cancelStatus, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")

		assert.Equal(t, WorkflowStatusCancelled, cancelStatus.Status, "expected workflow status to be CANCELLED")

		// Resume the workflow
		resumeHandle, err := client.ResumeWorkflow(workflowID)
		require.NoError(t, err, "failed to resume workflow")

		// Wait for workflow completion
		proceedSignal.Set() // Allow the workflow to proceed to step two
		result, err := resumeHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed workflow")

		// Verify the result
		assert.Equal(t, input, result, "expected result to match input")

		// Verify both steps completed
		assert.Equal(t, 2, stepsCompleted, "expected steps completed to be 2")

		// Check final status
		finalStatus, err := resumeHandle.GetStatus()
		require.NoError(t, err, "failed to get final workflow status")

		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status, "expected final workflow status to be SUCCESS")

		// After resume, the queue name should change to the internal queue name
		assert.Equal(t, _DBOS_INTERNAL_QUEUE_NAME, finalStatus.QueueName, "expected queue name to be %s", _DBOS_INTERNAL_QUEUE_NAME)

		// Resume the workflow again - should not run again
		resumeAgainHandle, err := client.ResumeWorkflow(workflowID)
		require.NoError(t, err, "failed to resume workflow again")

		resultAgain, err := resumeAgainHandle.GetResult()
		require.NoError(t, err, "failed to get result from second resume")

		assert.Equal(t, input, resultAgain, "expected second resume result to match input")

		// Verify steps didn't run again
		assert.Equal(t, 2, stepsCompleted, "expected steps completed to remain 2 after second resume")

		require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after cancel/resume test")
	})

	t.Run("CancelAndResumeTimeout", func(t *testing.T) {
		workflowID := "test-cancel-resume-timeout-workflow"
		workflowTimeout := 2 * time.Second

		// Start the workflow with a 2-second timeout
		handle, err := Enqueue[string, string](client, queue.Name, "TimeoutBlockingWorkflow", "timeout-test",
			WithEnqueueWorkflowID(workflowID),
			WithEnqueueTimeout(workflowTimeout),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue timeout blocking workflow")

		// Wait 500ms (well before the timeout expires)
		time.Sleep(500 * time.Millisecond)

		// Cancel the workflow before timeout expires
		err = client.CancelWorkflow(workflowID)
		require.NoError(t, err, "failed to cancel workflow")

		// Verify workflow is cancelled
		cancelStatus, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status after cancel")

		assert.Equal(t, WorkflowStatusCancelled, cancelStatus.Status, "expected workflow status to be CANCELLED")

		// Record the original deadline before resume
		originalDeadline := cancelStatus.Deadline

		// Resume the workflow
		resumeHandle, err := client.ResumeWorkflow(workflowID)
		require.NoError(t, err, "failed to resume workflow")
		resumeStart := time.Now()

		// Get status after resume to check the deadline
		resumeStatus, err := resumeHandle.GetStatus()
		require.NoError(t, err, "failed to get workflow status after resume")

		// Verify the deadline was reset (should be different from original)
		assert.False(t, resumeStatus.Deadline.Equal(originalDeadline), "expected deadline to be reset after resume, but it remained the same: %v", originalDeadline)

		// Wait for the workflow to complete
		_, err = resumeHandle.GetResult()
		require.Error(t, err, "expected timeout error, but got none")

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be AwaitedWorkflowCancelled")

		assert.Contains(t, dbosErr.Error(), "test-cancel-resume-timeout-workflow was cancelled", "expected error message to contain cancellation text")

		finalStatus, err := resumeHandle.GetStatus()
		require.NoError(t, err, "failed to get final workflow status")

		// The new deadline should have been set after resumeStart + workflowTimeout
		expectedDeadline := resumeStart.Add(workflowTimeout - 100*time.Millisecond) // Allow some leeway for processing time
		assert.True(t, finalStatus.Deadline.After(expectedDeadline), "deadline %v is too early (expected around %v)", resumeStatus.Deadline, expectedDeadline)

		assert.Equal(t, WorkflowStatusCancelled, finalStatus.Status, "expected final workflow status to be CANCELLED")

		require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after cancel/resume timeout test")
	})

	t.Run("CancelNonExistentWorkflow", func(t *testing.T) {
		nonExistentWorkflowID := "non-existent-workflow-id"

		// Try to cancel a non-existent workflow
		err := client.CancelWorkflow(nonExistentWorkflowID)
		require.Error(t, err, "expected error when canceling non-existent workflow, but got none")

		// Verify error type and code
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, NonExistentWorkflowError, dbosErr.Code, "expected error code to be NonExistentWorkflowError")

		assert.Equal(t, nonExistentWorkflowID, dbosErr.DestinationID, "expected DestinationID to match")
	})

	t.Run("ResumeNonExistentWorkflow", func(t *testing.T) {
		nonExistentWorkflowID := "non-existent-resume-workflow-id"

		// Try to resume a non-existent workflow
		_, err := client.ResumeWorkflow(nonExistentWorkflowID)
		require.Error(t, err, "expected error when resuming non-existent workflow, but got none")

		// Verify error type and code
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, NonExistentWorkflowError, dbosErr.Code, "expected error code to be NonExistentWorkflowError")

		assert.Equal(t, nonExistentWorkflowID, dbosErr.DestinationID, "expected DestinationID to match")
	})
}

func TestForkWorkflow(t *testing.T) {
	// Global counters for tracking execution (no mutex needed since workflows run solo)
	var (
		stepCount1  int
		stepCount2  int
		child1Count int
		child2Count int
	)

	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, true, true)

	// Create queue for communication between client and server
	queue := NewWorkflowQueue(serverCtx, "fork-workflow-queue")

	// Simple child workflows (no steps, just increment counters)
	childWorkflow1 := func(ctx DBOSContext, input string) (string, error) {
		child1Count++
		return "child1-" + input, nil
	}
	RegisterWorkflow(serverCtx, childWorkflow1, WithWorkflowName("ChildWorkflow1"))

	childWorkflow2 := func(ctx DBOSContext, input string) (string, error) {
		child2Count++
		return "child2-" + input, nil
	}
	RegisterWorkflow(serverCtx, childWorkflow2, WithWorkflowName("ChildWorkflow2"))

	// Parent workflow with 2 steps and 2 child workflows
	parentWorkflow := func(ctx DBOSContext, input string) (string, error) {
		// Step 1
		step1Result, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
			stepCount1++
			return "step1-" + input, nil
		})
		if err != nil {
			return "", err
		}

		// Child workflow 1
		child1Handle, err := RunWorkflow(ctx, childWorkflow1, input)
		if err != nil {
			return "", err
		}
		child1Result, err := child1Handle.GetResult()
		if err != nil {
			return "", err
		}

		// Step 2
		step2Result, err := RunAsStep(ctx, func(ctx context.Context) (string, error) {
			stepCount2++
			return "step2-" + input, nil
		})
		if err != nil {
			return "", err
		}

		// Child workflow 2
		child2Handle, err := RunWorkflow(ctx, childWorkflow2, input)
		if err != nil {
			return "", err
		}
		child2Result, err := child2Handle.GetResult()
		if err != nil {
			return "", err
		}

		return step1Result + "+" + step2Result + "+" + child1Result + "+" + child2Result, nil
	}
	RegisterWorkflow(serverCtx, parentWorkflow, WithWorkflowName("ParentWorkflow"))

	// Launch the server context to start processing tasks
	err := serverCtx.Launch()
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	t.Run("ForkAtAllSteps", func(t *testing.T) {
		// Reset counters
		stepCount1, stepCount2, child1Count, child2Count = 0, 0, 0, 0

		originalWorkflowID := "original-workflow-fork-test"

		// 1. Run the entire workflow first and check counters are 1
		handle, err := Enqueue[string, string](client, queue.Name, "ParentWorkflow", "test",
			WithEnqueueWorkflowID(originalWorkflowID),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue original workflow")

		// Wait for the original workflow to complete
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from original workflow")

		expectedResult := "step1-test+step2-test+child1-test+child2-test"
		assert.Equal(t, expectedResult, result, "expected result to match")

		// Verify all counters are 1 after original workflow
		assert.Equal(t, 1, stepCount1, "step1 counter should be 1")
		assert.Equal(t, 1, stepCount2, "step2 counter should be 1")
		assert.Equal(t, 1, child1Count, "child1 counter should be 1")
		assert.Equal(t, 1, child2Count, "child2 counter should be 1")

		// 2. Fork from each step 1 to 6 and verify results
		// Note: there's 6 steps: 2 steps 2 children and 2 GetResults
		for step := 1; step <= 6; step++ {
			t.Logf("Forking at step %d", step)

			customForkedWorkflowID := fmt.Sprintf("forked-workflow-step-%d", step)
			forkedHandle, err := client.ForkWorkflow(ForkWorkflowInput{
				OriginalWorkflowID: originalWorkflowID,
				ForkedWorkflowID:   customForkedWorkflowID,
				StartStep:          uint(step - 1),
			})
			require.NoError(t, err, "failed to fork workflow at step %d", step)

			forkedWorkflowID := forkedHandle.GetWorkflowID()
			assert.Equal(t, customForkedWorkflowID, forkedWorkflowID, "expected forked workflow ID to match")

			forkedResult, err := forkedHandle.GetResult()
			require.NoError(t, err, "failed to get result from forked workflow at step %d", step)

			// 1) Verify workflow result is correct
			assert.Equal(t, expectedResult, forkedResult, "forked workflow at step %d: expected result to match", step)

			// 2) Verify counters are at expected totals based on the step where we're forking
			t.Logf("Step %d: actual counters - step1:%d, step2:%d, child1:%d, child2:%d", step, stepCount1, stepCount2, child1Count, child2Count)

			// First step is executed only once
			assert.Equal(t, 2, stepCount1, "forked workflow at step %d: step1 counter should be 2", step)

			// First child will be executed twice
			if step < 3 {
				assert.Equal(t, 1+step, child1Count, "forked workflow at step %d: child1 counter should be %d", step, 1+step)
			} else {
				assert.Equal(t, 3, child1Count, "forked workflow at step %d: child1 counter should be 3", step)
			}

			// Second step (in reality step 4) will be executed 4 times
			if step < 5 {
				assert.Equal(t, 1+step, stepCount2, "forked workflow at step %d: step2 counter should be %d", step, 1+step)
			} else {
				assert.Equal(t, 5, stepCount2, "forked workflow at step %d: step2 counter should be 5", step)
			}

			// Second child will be executed 5 times
			if step < 6 {
				assert.Equal(t, 1+step, child2Count, "forked workflow at step %d: child2 counter should be %d", step, 1+step)
			} else {
				assert.Equal(t, 6, child2Count, "forked workflow at step %d: child2 counter should be 6", step)
			}

			t.Logf("Step %d: all counter totals verified correctly", step)
		}

		t.Logf("Final counters after all forks - steps:%d, child1:%d, child2:%d", stepCount1, child1Count, child2Count)
	})

	t.Run("ForkNonExistentWorkflow", func(t *testing.T) {
		nonExistentWorkflowID := "non-existent-workflow-for-fork"

		// Try to fork a non-existent workflow
		_, err := client.ForkWorkflow(ForkWorkflowInput{
			OriginalWorkflowID: nonExistentWorkflowID,
			StartStep:          1,
		})
		require.Error(t, err, "expected error when forking non-existent workflow, but got none")

		// Verify error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, NonExistentWorkflowError, dbosErr.Code, "expected error code to be NonExistentWorkflowError")

		assert.Equal(t, nonExistentWorkflowID, dbosErr.DestinationID, "expected DestinationID to match")
	})

	// Verify all queue entries are cleaned up
	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after fork workflow tests")
}

func TestListWorkflows(t *testing.T) {
	// Setup server context
	serverCtx := setupDBOS(t, true, true)

	// Create queue for communication
	queue := NewWorkflowQueue(serverCtx, "list-workflows-queue")

	// Simple test workflow
	type testInput struct {
		Value int
		ID    string
	}

	simpleWorkflow := func(ctx DBOSContext, input testInput) (string, error) {
		if input.Value < 0 {
			return "", fmt.Errorf("negative value: %d", input.Value)
		}
		return fmt.Sprintf("result-%d-%s", input.Value, input.ID), nil
	}
	RegisterWorkflow(serverCtx, simpleWorkflow, WithWorkflowName("SimpleWorkflow"))

	// Launch server
	err := serverCtx.Launch()
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	t.Run("ListWorkflowsFiltering", func(t *testing.T) {
		var workflowIDs []string
		var handles []WorkflowHandle[string]

		// Record start time for filtering tests
		testStartTime := time.Now()

		// Start 10 workflows at 100ms intervals with different patterns
		for i := range 10 {
			var workflowID string
			var handle WorkflowHandle[string]

			if i < 5 {
				// First 5 workflows: use prefix "test-batch-" and succeed
				workflowID = fmt.Sprintf("test-batch-%d", i)
				handle, err = Enqueue[testInput, string](client, queue.Name, "SimpleWorkflow", testInput{Value: i, ID: fmt.Sprintf("success-%d", i)},
					WithEnqueueWorkflowID(workflowID),
					WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
			} else {
				// Last 5 workflows: use prefix "test-other-" and some will fail
				workflowID = fmt.Sprintf("test-other-%d", i)
				value := i
				if i >= 8 {
					value = -i // These will fail
				}
				handle, err = Enqueue[testInput, string](client, queue.Name, "SimpleWorkflow", testInput{Value: value, ID: fmt.Sprintf("test-%d", i)},
					WithEnqueueWorkflowID(workflowID),
					WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
			}

			require.NoError(t, err, "failed to enqueue workflow %d", i)

			workflowIDs = append(workflowIDs, workflowID)
			handles = append(handles, handle)

			// Wait 100ms between workflow starts
			time.Sleep(100 * time.Millisecond)
		}

		// Wait for all workflows to complete
		for i, handle := range handles {
			_, err := handle.GetResult()
			if i < 8 {
				// First 8 should succeed
				require.NoError(t, err, "workflow %d should have succeeded", i)
			} else {
				// Last 2 should fail
				require.Error(t, err, "workflow %d should have failed", i)
			}
		}

		// Test 1: List all workflows (no filters)
		allWorkflows, err := client.ListWorkflows()
		require.NoError(t, err, "failed to list all workflows")
		assert.GreaterOrEqual(t, len(allWorkflows), 10, "expected at least 10 workflows")

		// Test 2: Filter by workflow IDs
		expectedIDs := workflowIDs[:3]
		specificWorkflows, err := client.ListWorkflows(WithWorkflowIDs(expectedIDs))
		require.NoError(t, err, "failed to list workflows by IDs")
		assert.Len(t, specificWorkflows, 3, "expected 3 workflows")
		// Verify returned workflow IDs match expected
		returnedIDs := make(map[string]bool)
		for _, wf := range specificWorkflows {
			returnedIDs[wf.ID] = true
		}
		for _, expectedID := range expectedIDs {
			assert.True(t, returnedIDs[expectedID], "expected workflow ID %s not found in results", expectedID)
		}

		// Test 3: Filter by workflow ID prefix
		batchWorkflows, err := client.ListWorkflows(WithWorkflowIDPrefix("test-batch-"))
		require.NoError(t, err, "failed to list workflows by prefix")
		assert.Len(t, batchWorkflows, 5, "expected 5 batch workflows")
		// Verify all returned workflow IDs have the correct prefix
		for _, wf := range batchWorkflows {
			assert.True(t, strings.HasPrefix(wf.ID, "test-batch-"), "workflow ID %s does not have expected prefix 'test-batch-'", wf.ID)
		}

		// Test 4: Filter by status - SUCCESS
		successWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"), // Only our test workflows
			WithStatus([]WorkflowStatusType{WorkflowStatusSuccess}))
		require.NoError(t, err, "failed to list successful workflows")
		assert.Len(t, successWorkflows, 8, "expected 8 successful workflows")
		// Verify all returned workflows have SUCCESS status
		for _, wf := range successWorkflows {
			assert.Equal(t, WorkflowStatusSuccess, wf.Status, "workflow %s has unexpected status", wf.ID)
		}

		// Test 5: Filter by status - ERROR
		errorWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithStatus([]WorkflowStatusType{WorkflowStatusError}))
		require.NoError(t, err, "failed to list error workflows")
		assert.Len(t, errorWorkflows, 2, "expected 2 error workflows")
		// Verify all returned workflows have ERROR status
		for _, wf := range errorWorkflows {
			assert.Equal(t, WorkflowStatusError, wf.Status, "workflow %s has unexpected status", wf.ID)
		}

		// Test 6: Filter by time range - first 5 workflows (start to start+500ms)
		firstHalfTime := testStartTime.Add(500 * time.Millisecond)
		firstHalfWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithEndTime(firstHalfTime))
		require.NoError(t, err, "failed to list first half workflows by time range")
		assert.Len(t, firstHalfWorkflows, 5, "expected 5 workflows in first half time range")

		// Test 6b: Filter by time range - last 5 workflows (start+500ms to end)
		secondHalfWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithStartTime(firstHalfTime))
		require.NoError(t, err, "failed to list second half workflows by time range")
		assert.Len(t, secondHalfWorkflows, 5, "expected 5 workflows in second half time range")

		// Test 7: Test sorting order (ascending - default)
		ascWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"))
		require.NoError(t, err, "failed to list workflows ascending")

		// Test 8: Test sorting order (descending)
		descWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithSortDesc())
		require.NoError(t, err, "failed to list workflows descending")

		// Verify sorting - workflows should be ordered by creation time
		// First workflow in desc should be last in asc (latest created)
		assert.Equal(t, ascWorkflows[len(ascWorkflows)-1].ID, descWorkflows[0].ID, "sorting verification failed: asc last != desc first")
		// Last workflow in desc should be first in asc (earliest created)
		assert.Equal(t, ascWorkflows[0].ID, descWorkflows[len(descWorkflows)-1].ID, "sorting verification failed: asc first != desc last")

		// Verify ascending order: each workflow should be created at or after the previous
		for i := 1; i < len(ascWorkflows); i++ {
			assert.False(t, ascWorkflows[i].CreatedAt.Before(ascWorkflows[i-1].CreatedAt), "ascending order violation: workflow at index %d created before previous", i)
		}

		// Verify descending order: each workflow should be created at or before the previous
		for i := 1; i < len(descWorkflows); i++ {
			assert.False(t, descWorkflows[i].CreatedAt.After(descWorkflows[i-1].CreatedAt), "descending order violation: workflow at index %d created after previous", i)
		}

		// Test 9: Test limit and offset
		limitedWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithLimit(5))
		require.NoError(t, err, "failed to list workflows with limit")
		assert.Len(t, limitedWorkflows, 5, "expected 5 workflows with limit")
		// Verify we got the first 5 workflows (earliest created)
		expectedFirstFive := ascWorkflows[:5]
		for i, wf := range limitedWorkflows {
			assert.Equal(t, expectedFirstFive[i].ID, wf.ID, "limited workflow at index %d: unexpected ID", i)
		}

		offsetWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithOffset(5),
			WithLimit(3))
		require.NoError(t, err, "failed to list workflows with offset")
		assert.Len(t, offsetWorkflows, 3, "expected 3 workflows with offset")
		// Verify we got workflows 5, 6, 7 from the ascending list
		expectedOffsetThree := ascWorkflows[5:8]
		for i, wf := range offsetWorkflows {
			assert.Equal(t, expectedOffsetThree[i].ID, wf.ID, "offset workflow at index %d: unexpected ID", i)
		}

		// Test 10: Test input/output loading
		noDataWorkflows, err := client.ListWorkflows(
			WithWorkflowIDs(workflowIDs[:2]),
			WithLoadInput(false),
			WithLoadOutput(false))
		require.NoError(t, err, "failed to list workflows without data")
		assert.Len(t, noDataWorkflows, 2, "expected 2 workflows without data")

		// Verify input/output are not loaded
		for _, wf := range noDataWorkflows {
			assert.Nil(t, wf.Input, "expected input to be nil when LoadInput=false")
			assert.Nil(t, wf.Output, "expected output to be nil when LoadOutput=false")
		}
	})

	// Verify all queue entries are cleaned up
	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after list workflows tests")
}

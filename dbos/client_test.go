package dbos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientEnqueue(t *testing.T) {
	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create queue for communication between client and server
	queue := NewWorkflowQueue(serverCtx, "client-enqueue-queue")

	// Create a priority-enabled queue with max concurrency of 1 to ensure ordering
	// Must be created before Launch()
	priorityQueue := NewWorkflowQueue(serverCtx, "priority-test-queue", WithGlobalConcurrency(1), WithPriorityEnabled())

	// Create a partitioned queue for partition key test
	// Must be created before Launch()
	partitionedQueue := NewWorkflowQueue(serverCtx, "client-partitioned-queue", WithPartitionQueue())

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

	// Simple workflow for partitioned queue test
	partitionedWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return "partitioned: " + input, nil
	}
	RegisterWorkflow(serverCtx, partitionedWorkflow, WithWorkflowName("PartitionedWorkflow"))

	// Launch the server context to start processing tasks
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client - this will enqueue tasks
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
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

	t.Run("EnqueueToPartitionedQueue", func(t *testing.T) {
		// Enqueue a workflow to a partitioned queue with a partition key
		handle, err := Enqueue[string, string](client, partitionedQueue.Name, "PartitionedWorkflow", "test-input",
			WithEnqueueQueuePartitionKey("partition-1"),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue workflow to partitioned queue")

		// Verify we got a polling handle
		_, ok := handle.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle to be of type workflowPollingHandle, got %T", handle)

		// Get the result
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from partitioned queue workflow")

		expectedResult := "partitioned: test-input"
		assert.Equal(t, expectedResult, result, "expected result to match")

		// Verify the workflow status
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")

		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected workflow status to be SUCCESS")
		assert.Equal(t, "PartitionedWorkflow", status.Name, "expected workflow name to match")
		assert.Equal(t, partitionedQueue.Name, status.QueueName, "expected queue name to match")

		assert.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after partitioned queue test")
	})

	t.Run("EnqueueWithPartitionKeyWithoutQueue", func(t *testing.T) {
		// Attempt to enqueue with a partition key but no queue name
		_, err := Enqueue[string, string](client, "", "PartitionedWorkflow", "test-input",
			WithEnqueueQueuePartitionKey("partition-1"))
		require.Error(t, err, "expected error when enqueueing with partition key but no queue name")

		// Verify the error message contains the expected text
		assert.Contains(t, err.Error(), "queue name is required", "expected error message to contain 'queue name is required'")
	})

	t.Run("EnqueueWithPartitionKeyAndDeduplicationID", func(t *testing.T) {
		// Attempt to enqueue with both partition key and deduplication ID
		// This should return an error
		_, err := Enqueue[string, string](client, partitionedQueue.Name, "PartitionedWorkflow", "test-input",
			WithEnqueueQueuePartitionKey("partition-1"),
			WithEnqueueDeduplicationID("dedup-id"))
		require.Error(t, err, "expected error when enqueueing with both partition key and deduplication ID")

		// Verify the error message contains the expected text
		assert.Contains(t, err.Error(), "partition key and deduplication ID cannot be used together", "expected error message to contain validation message")
	})

	t.Run("EnqueueWithEmptyQueueName", func(t *testing.T) {
		// Attempt to enqueue with empty queue name
		// This should return an error
		_, err := Enqueue[wfInput, string](client, "", "ServerWorkflow", wfInput{Input: "test-input"})
		require.Error(t, err, "expected error when enqueueing with empty queue name")

		// Verify the error message contains the expected text
		assert.Contains(t, err.Error(), "queue name is required", "expected error message to contain 'queue name is required'")
	})

	t.Run("EnqueueWithEmptyWorkflowName", func(t *testing.T) {
		// Attempt to enqueue with empty workflow name
		// This should return an error
		_, err := Enqueue[wfInput, string](client, queue.Name, "", wfInput{Input: "test-input"})
		require.Error(t, err, "expected error when enqueueing with empty workflow name")

		// Verify the error message contains the expected text
		assert.Contains(t, err.Error(), "workflow name is required", "expected error message to contain 'workflow name is required'")
	})

	// Verify all queue entries are cleaned up
	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after client tests")
}

func TestCancelResume(t *testing.T) {
	var stepsCompleted int

	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

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
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client - this will enqueue tasks
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
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
		resultAny, err := resumeHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed workflow")

		// Will be a float64 from json decode
		require.Equal(t, input, int(resultAny.(float64)), "expected result to match input")

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

		resultAgainAny, err := resumeAgainHandle.GetResult()
		require.NoError(t, err, "failed to get result from second resume")

		// Will be a float64 from json decode
		require.Equal(t, input, int(resultAgainAny.(float64)), "expected result to match input")

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
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

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
		// Set events: A=1, B=1, A=2, B=2
		err := SetEvent(ctx, "A", "1")
		if err != nil {
			return "", err
		}

		err = SetEvent(ctx, "B", "1")
		if err != nil {
			return "", err
		}

		err = SetEvent(ctx, "A", "2")
		if err != nil {
			return "", err
		}

		err = SetEvent(ctx, "B", "2")
		if err != nil {
			return "", err
		}

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
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
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

		// 2. Fork from each startStep 1 to 10 and verify results
		// Step mapping: 0=SetEvent A=1, 1=SetEvent B=1, 2=SetEvent A=2, 3=SetEvent B=2,
		//               4=RunAsStep(step1), 5=RunWorkflow(child1), 6=GetResult(child1),
		//               7=RunAsStep(step2), 8=RunWorkflow(child2), 9=GetResult(child2)
		// Expected events history: function_id 0: A=1, function_id 1: B=1, function_id 2: A=2, function_id 3: B=2
		type eventTuple struct {
			functionID int
			key        string
			value      string
		}
		expectedEventTuples := []eventTuple{
			{0, "A", "1"},
			{1, "B", "1"},
			{2, "A", "2"},
			{3, "B", "2"},
		}

		for startStep := 0; startStep <= 9; startStep++ {
			t.Logf("Forking at step %d", startStep)

			customForkedWorkflowID := fmt.Sprintf("forked-workflow-step-%d", startStep)
			forkedHandle, err := client.ForkWorkflow(ForkWorkflowInput{
				OriginalWorkflowID: originalWorkflowID,
				ForkedWorkflowID:   customForkedWorkflowID,
				StartStep:          uint(startStep),
			})
			require.NoError(t, err, "failed to fork workflow at step %d", startStep)

			forkedWorkflowID := forkedHandle.GetWorkflowID()
			assert.Equal(t, customForkedWorkflowID, forkedWorkflowID, "expected forked workflow ID to match")

			// Verify forked_from is set
			forkedStatus, err := forkedHandle.GetStatus()
			require.NoError(t, err, "failed to get forked workflow status")
			assert.Equal(t, originalWorkflowID, forkedStatus.ForkedFrom, "expected forked_from to be set to original workflow ID")

			forkedResult, err := forkedHandle.GetResult()
			require.NoError(t, err, "failed to get result from forked workflow at step %d", startStep)

			// 1) Verify workflow result is correct
			assert.Equal(t, expectedResult, forkedResult, "forked workflow at step %d: expected result to match", startStep)

			// 2) Verify events in workflow_events_history table
			// The forked workflow will always execute all 4 SetEvent calls, so we should always have all 4 entries
			// Get database pool from serverCtx to query workflow_events_history
			dbosCtx, ok := serverCtx.(*dbosContext)
			require.True(t, ok, "expected dbosContext")
			sysDB, ok := dbosCtx.systemDB.(*sysDB)
			require.True(t, ok, "expected sysDB")

			// Query all events from workflow_events_history
			query := fmt.Sprintf(`SELECT function_id, key, value FROM %s.workflow_events_history WHERE workflow_uuid = $1 ORDER BY function_id, key`, pgx.Identifier{sysDB.schema}.Sanitize())
			rows, err := sysDB.pool.Query(context.Background(), query, forkedWorkflowID)
			require.NoError(t, err, "failed to query workflow_events_history for forked workflow at step %d", startStep)
			defer rows.Close()

			// Collect all events as (function_id, key, value) tuples

			var actualEventTuples []eventTuple
			for rows.Next() {
				var functionID int
				var key, jsonb64Value string
				err := rows.Scan(&functionID, &key, &jsonb64Value)
				require.NoError(t, err, "failed to scan workflow_events_history row")
				jsonValue, err := base64.StdEncoding.DecodeString(jsonb64Value)
				require.NoError(t, err, "failed to decode base64 value")
				var value string
				err = json.Unmarshal(jsonValue, &value)
				require.NoError(t, err, "failed to unmarshal value")
				actualEventTuples = append(actualEventTuples, eventTuple{functionID, key, value})
			}
			require.NoError(t, rows.Err(), "error iterating workflow_events_history rows")

			// Verify all 4 events are present and match
			assert.Equal(t, expectedEventTuples, actualEventTuples, "forked workflow at step %d: events history mismatch", startStep)

			// 3) Verify counters are at expected totals based on the step where we're forking
			t.Logf("Step %d: actual counters - step1:%d, step2:%d, child1:%d, child2:%d", startStep, stepCount1, stepCount2, child1Count, child2Count)

			expectedStep1Count := 1 + min(startStep+1, 5)
			assert.Equal(t, expectedStep1Count, stepCount1, "forked workflow at step %d: step1 counter should be %d", startStep, expectedStep1Count)

			expectedChild1Count := 1 + min(startStep+1, 6)
			assert.Equal(t, expectedChild1Count, child1Count, "forked workflow at step %d: child1 counter should be %d", startStep, expectedChild1Count)

			expectedStep2Count := 1 + min(startStep+1, 8)
			assert.Equal(t, expectedStep2Count, stepCount2, "forked workflow at step %d: step2 counter should be %d", startStep, expectedStep2Count)

			expectedChild2Count := 1 + min(startStep+1, 9)
			assert.Equal(t, expectedChild2Count, child2Count, "forked workflow at step %d: child2 counter should be %d", startStep, expectedChild2Count)
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
	// Setup server context with custom schema
	databaseURL := getDatabaseURL()
	resetTestDatabase(t, databaseURL)

	customSchema := "dbos_list_test"
	serverCtx, err := NewDBOSContext(context.Background(), Config{
		DatabaseURL:    databaseURL,
		AppName:        "test-list-workflows",
		DatabaseSchema: customSchema,
	})
	require.NoError(t, err)
	require.NotNil(t, serverCtx)

	// Register cleanup for server context
	t.Cleanup(func() {
		if serverCtx != nil {
			Shutdown(serverCtx, 30*time.Second)
		}
	})

	// Create queues for communication (second queue used for multi-value filter tests)
	queue := NewWorkflowQueue(serverCtx, "list-workflows-queue")
	queue2 := NewWorkflowQueue(serverCtx, "list-workflows-queue-2")

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
	otherWorkflow := func(ctx DBOSContext, input testInput) (string, error) {
		if input.Value < 0 {
			return "", fmt.Errorf("negative value: %d", input.Value)
		}
		return fmt.Sprintf("result-%d-%s", input.Value, input.ID), nil
	}
	RegisterWorkflow(serverCtx, simpleWorkflow, WithWorkflowName("SimpleWorkflow"))
	RegisterWorkflow(serverCtx, otherWorkflow, WithWorkflowName("OtherWorkflow"))

	// Parent/child workflows for WithParentWorkflowID filter test
	childWfForListTest := func(ctx DBOSContext, input string) (string, error) { return input, nil }
	parentWfForListTest := func(ctx DBOSContext, _ string) (string, error) {
		h, err := RunWorkflow(ctx, childWfForListTest, "child-input")
		if err != nil {
			return "", err
		}
		return h.GetResult()
	}
	RegisterWorkflow(serverCtx, childWfForListTest, WithWorkflowName("ChildForListTest"))
	RegisterWorkflow(serverCtx, parentWfForListTest, WithWorkflowName("ParentForListTest"))

	// Launch server
	err = Launch(serverCtx)
	require.NoError(t, err)

	// Setup client with same custom schema
	config := ClientConfig{
		DatabaseURL:    databaseURL,
		DatabaseSchema: customSchema,
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

		// Run 2 workflows with different name (OtherWorkflow) for multi-name filter test
		for i := range 2 {
			h, err := Enqueue[testInput, string](client, queue.Name, "OtherWorkflow", testInput{Value: i, ID: fmt.Sprintf("other-%d", i)},
				WithEnqueueWorkflowID(fmt.Sprintf("test-other-name-%d", i)),
				WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
			require.NoError(t, err, "failed to enqueue OtherWorkflow %d", i)
			_, err = h.GetResult()
			require.NoError(t, err, "OtherWorkflow %d should succeed", i)
		}

		// Run 2 workflows on second queue for multi-queue filter test
		for i := range 2 {
			h, err := Enqueue[testInput, string](client, queue2.Name, "SimpleWorkflow", testInput{Value: 100 + i, ID: fmt.Sprintf("q2-%d", i)},
				WithEnqueueWorkflowID(fmt.Sprintf("test-queue2-%d", i)),
				WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
			require.NoError(t, err, "failed to enqueue to queue2 %d", i)
			_, err = h.GetResult()
			require.NoError(t, err, "queue2 workflow %d should succeed", i)
		}

		// Test 1: List all workflows (no filters)
		allWorkflows, err := client.ListWorkflows()
		require.NoError(t, err, "failed to list all workflows")
		assert.GreaterOrEqual(t, len(allWorkflows), 14, "expected at least 14 workflows (10 initial + 2 OtherWorkflow + 2 on queue2)")

		for _, wf := range allWorkflows {
			// These fields should exist (may be zero/empty for some workflows)
			// Timeout and Deadline are time.Duration and time.Time, so they're always present
			_ = wf.Timeout
			_ = wf.Deadline
			_ = wf.DeduplicationID
			_ = wf.Priority
			_ = wf.QueuePartitionKey
			_ = wf.ForkedFrom
		}

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
		assert.Len(t, successWorkflows, 12, "expected 12 successful workflows (8 initial + 2 OtherWorkflow + 2 queue2)")
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

		// Test 6b: Filter by time range - workflows started at or after firstHalfTime
		secondHalfWorkflows, err := client.ListWorkflows(
			WithWorkflowIDPrefix("test-"),
			WithStartTime(firstHalfTime))
		require.NoError(t, err, "failed to list second half workflows by time range")
		assert.Len(t, secondHalfWorkflows, 9, "expected 9 workflows in second half (5 test-other-5..9 + 2 test-other-name + 2 test-queue2)")

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

		// Test 11: Filter by multiple workflow ID prefixes (slice option)
		multiPrefixWorkflows, err := client.ListWorkflows(WithWorkflowIDPrefix("test-batch-", "test-other-"))
		require.NoError(t, err, "failed to list workflows by multiple prefixes")
		// Matches test-batch-0..4 (5) + test-other-5..9 (5) + test-other-name-0,1 (2) = 12
		assert.Len(t, multiPrefixWorkflows, 12, "expected 12 workflows matching either prefix")
		for _, wf := range multiPrefixWorkflows {
			assert.True(t, strings.HasPrefix(wf.ID, "test-batch-") || strings.HasPrefix(wf.ID, "test-other-"),
				"workflow ID %s should have one of the prefixes", wf.ID)
		}

		// Test 12: Filter by multiple workflow names (slice option)
		multiNameWorkflows, err := client.ListWorkflows(WithName("SimpleWorkflow", "OtherWorkflow"))
		require.NoError(t, err, "failed to list workflows by multiple names")
		assert.Len(t, multiNameWorkflows, 14, "expected 14 workflows (10 SimpleWorkflow + 2 OtherWorkflow + 2 SimpleWorkflow on queue2)")
		namesSeen := make(map[string]int)
		for _, wf := range multiNameWorkflows {
			if wf.Name != "" {
				namesSeen[wf.Name]++
			}
		}
		assert.GreaterOrEqual(t, namesSeen["SimpleWorkflow"], 12, "expected at least 12 SimpleWorkflow")
		assert.GreaterOrEqual(t, namesSeen["OtherWorkflow"], 2, "expected at least 2 OtherWorkflow")

		// Test 13: Filter by multiple queue names (slice option)
		multiQueueWorkflows, err := client.ListWorkflows(WithQueueName(queue.Name, queue2.Name))
		require.NoError(t, err, "failed to list workflows by multiple queues")
		assert.Len(t, multiQueueWorkflows, 14, "expected 14 workflows (12 on queue + 2 on queue2)")
		queuesSeen := make(map[string]int)
		for _, wf := range multiQueueWorkflows {
			if wf.QueueName != "" {
				queuesSeen[wf.QueueName]++
			}
		}
		assert.GreaterOrEqual(t, queuesSeen[queue.Name], 12, "expected at least 12 workflows on first queue")
		assert.GreaterOrEqual(t, queuesSeen[queue2.Name], 2, "expected at least 2 workflows on second queue")

		// Test 14: Filter by parent workflow ID (child ID is parentID-0 for first step)
		parentID := "list-test-parent-id"
		parentHandle, err := Enqueue[string, string](client, queue.Name, "ParentForListTest", "ignored",
			WithEnqueueWorkflowID(parentID),
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err, "failed to enqueue parent workflow")
		_, err = parentHandle.GetResult()
		require.NoError(t, err, "parent workflow should succeed")
		assert.Equal(t, parentID, parentHandle.GetWorkflowID(), "parent should have requested workflow ID")
		expectedChildID := parentID + "-0"
		childWorkflows, err := client.ListWorkflows(WithParentWorkflowID(parentID))
		require.NoError(t, err, "failed to list workflows by parent ID")
		assert.Len(t, childWorkflows, 1, "expected one child workflow")
		assert.Equal(t, parentID, childWorkflows[0].ParentWorkflowID, "child should have ParentWorkflowID set")
		assert.Equal(t, expectedChildID, childWorkflows[0].ID, "child workflow ID should be parentID-0")
		// Filter with nonexistent parent returns empty
		nonexistent, err := client.ListWorkflows(WithParentWorkflowID("nonexistent-parent-id"))
		require.NoError(t, err)
		assert.Len(t, nonexistent, 0)
	})
	// Verify all queue entries are cleaned up
	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after list workflows tests")
}

func TestGetWorkflowSteps(t *testing.T) {
	// Setup server context
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create queue for communication
	queue := NewWorkflowQueue(serverCtx, "get-workflow-steps-queue")

	// Workflow with one step
	stepFunction := func(ctx context.Context) (string, error) {
		return "abc", nil
	}

	testWorkflow := func(ctx DBOSContext, input string) (string, error) {
		result, err := RunAsStep(ctx, stepFunction, WithStepName("TestStep"))
		if err != nil {
			return "", err
		}
		return result, nil
	}
	RegisterWorkflow(serverCtx, testWorkflow, WithWorkflowName("TestWorkflow"))

	// Launch server
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	// Enqueue and run the workflow
	workflowID := "test-get-workflow-steps"
	handle, err := Enqueue[string, string](client, queue.Name, "TestWorkflow", "test-input", WithEnqueueWorkflowID(workflowID))
	require.NoError(t, err)

	// Wait for workflow to complete
	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "abc", result)

	// Test GetWorkflowSteps with loadOutput = true
	stepsWithOutput, err := client.GetWorkflowSteps(workflowID)
	require.NoError(t, err)
	require.Len(t, stepsWithOutput, 1, "expected exactly 1 step")

	step := stepsWithOutput[0]
	assert.Equal(t, 0, step.StepID, "expected step ID to be 0")
	assert.Equal(t, "TestStep", step.StepName, "expected step name to be set")
	assert.Nil(t, step.Error, "expected no error in step")
	assert.Equal(t, "", step.ChildWorkflowID, "expected no child workflow ID")

	// Verify timestamps are present
	assert.False(t, step.StartedAt.IsZero(), "expected step to have StartedAt timestamp")
	assert.False(t, step.CompletedAt.IsZero(), "expected step to have CompletedAt timestamp")
	assert.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt), "expected CompletedAt to be after or equal to StartedAt")

	// Verify the output wasn't loaded
	require.Nil(t, step.Output, "expected output not to be loaded")

	// Verify all queue entries are cleaned up
	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after get workflow steps test")
}

// clientReadStreamFunc is a function type that reads from a stream using a client and returns values, closed status, and error
type clientReadStreamFunc func(c Client, workflowID string, key string) ([]string, bool, error)

// syncClientReadStream wraps ClientReadStream for use in test table
func syncClientReadStream(c Client, workflowID string, key string) ([]string, bool, error) {
	return ClientReadStream[string](c, workflowID, key)
}

// asyncClientReadStream wraps ClientReadStreamAsync and collects values for use in test table
func asyncClientReadStream(c Client, workflowID string, key string) ([]string, bool, error) {
	ch, err := ClientReadStreamAsync[string](c, workflowID, key)
	if err != nil {
		return nil, false, err
	}
	return collectStreamValues(ch)
}

func TestClientReadStream(t *testing.T) {
	// Setup server context
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create queue for communication
	queue := NewWorkflowQueue(serverCtx, "read-stream-queue")

	// Workflow that writes to a stream
	streamWriterWorkflow := func(ctx DBOSContext, input struct {
		StreamKey string
		Values    []string
	}) (string, error) {
		// Write values to stream
		for _, value := range input.Values {
			if err := WriteStream(ctx, input.StreamKey, value); err != nil {
				return "", err
			}
		}
		return "done", nil
	}
	RegisterWorkflow(serverCtx, streamWriterWorkflow, WithWorkflowName("StreamWriterWorkflow"))

	// Launch server
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	// Test table for sync and async versions
	readFuncs := map[string]clientReadStreamFunc{
		"Sync":  syncClientReadStream,
		"Async": asyncClientReadStream,
	}

	for name, readFunc := range readFuncs {
		t.Run(name, func(t *testing.T) {
			streamKey := "test-client-stream"
			workflowID := "test-read-stream-workflow-" + name
			testValues := []string{"value1", "value2", "value3"}

			// Enqueue and run the writer workflow
			handle, err := Enqueue[struct {
				StreamKey string
				Values    []string
			}, string](client, queue.Name, "StreamWriterWorkflow", struct {
				StreamKey string
				Values    []string
			}{
				StreamKey: streamKey,
				Values:    testValues,
			}, WithEnqueueWorkflowID(workflowID))
			require.NoError(t, err, "failed to enqueue stream writer workflow")

			// Wait for workflow to complete
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from writer workflow")
			assert.Equal(t, "done", result)

			// Read from the stream using client
			values, closed, err := readFunc(client, workflowID, streamKey)
			require.NoError(t, err, "failed to read stream from client")
			assert.Equal(t, testValues, values, "expected stream values to match")
			assert.True(t, closed, "expected stream to be closed when workflow terminates")

			// Verify all queue entries are cleaned up
			require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after read stream test")
		})
	}
}

// TestDebouncerClient tests the DebouncerClient functionality using a Client interface
func TestDebouncerClient(t *testing.T) {
	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Set internal queue polling interval to 10ms for faster tests
	internalQueue := serverCtx.(*dbosContext).queueRunner.workflowQueueRegistry[_DBOS_INTERNAL_QUEUE_NAME]
	internalQueue.basePollingInterval = 10 * time.Millisecond
	serverCtx.(*dbosContext).queueRunner.workflowQueueRegistry[_DBOS_INTERNAL_QUEUE_NAME] = internalQueue

	// Register test workflow with a custom name
	debounceTestWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(serverCtx, debounceTestWorkflow, WithWorkflowName("DebounceTestWorkflow"))

	// Launch the server context to start processing tasks
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	// Create debouncer clients
	debouncer10sTimeout := NewDebouncerClient[string, string]("DebounceTestWorkflow", client, WithDebouncerTimeout(10*time.Second))
	debouncer200msTimeout := NewDebouncerClient[string, string]("DebounceTestWorkflow", client, WithDebouncerTimeout(200*time.Millisecond))

	t.Run("TestSingleDebounceCall", func(t *testing.T) {
		startTime := time.Now()
		handle, err := debouncer10sTimeout.Debounce("test-key-1", 500*time.Millisecond, "test-input-1")
		require.NoError(t, err, "failed to call Debounce")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "test-input-1", result, "result should match input")

		// Verify execution happened approximately 500ms after first call
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "execution should take at least 450ms")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")
	})

	t.Run("TestMultipleCallsPushBackAndLatestInput", func(t *testing.T) {

		pool := serverCtx.(*dbosContext).systemDB.(*sysDB).pool
		conn, err := pool.Acquire(serverCtx)
		require.NoError(t, err)
		defer conn.Release()
		isCockroachDB := isCockroachDB(context.Background(), conn.Conn())

		// CockroachDB has longer notification latency due to polling
		var delay time.Duration
		if isCockroachDB {
			delay = 2000 * time.Millisecond
		} else {
			delay = 200 * time.Millisecond
		}

		// Call Debounce 5 times
		key := "test-key-2"
		startTime := time.Now()

		// First call
		handle1, err := debouncer10sTimeout.Debounce(key, delay, "input-1")
		require.NoError(t, err, "failed to call Debounce (first call)")

		handle2, err := debouncer10sTimeout.Debounce(key, delay, "input-2")
		require.NoError(t, err, "failed to call Debounce (second call)")

		handle3, err := debouncer10sTimeout.Debounce(key, delay, "input-3")
		require.NoError(t, err, "failed to call Debounce (third call)")

		handle4, err := debouncer10sTimeout.Debounce(key, delay, "input-4")
		require.NoError(t, err, "failed to call Debounce (fourth call)")

		handle5, err := debouncer10sTimeout.Debounce(key, delay, "input-5")
		require.NoError(t, err, "failed to call Debounce (fifth call)")

		// All handles should refer to the same workflow ID
		assert.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID(), "all handles should refer to the same workflow ID")
		assert.Equal(t, handle1.GetWorkflowID(), handle3.GetWorkflowID(), "all handles should refer to the same workflow ID")
		assert.Equal(t, handle1.GetWorkflowID(), handle4.GetWorkflowID(), "all handles should refer to the same workflow ID")
		assert.Equal(t, handle1.GetWorkflowID(), handle5.GetWorkflowID(), "all handles should refer to the same workflow ID")

		result, err := handle5.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "input-5", result, "result should match latest input")

		// Verify execution happened at least delay after first call
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, delay, "execution should take at least delay")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")
	})

	t.Run("TestDelayGreaterThanTimeout", func(t *testing.T) {
		// Call Debounce with delay=2s (greater than timeout of 200ms)
		startTime := time.Now()
		handle, err := debouncer200msTimeout.Debounce("test-key-4", 2*time.Second, "timeout-input")
		require.NoError(t, err, "failed to call Debounce with delay > timeout")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "timeout-input", result, "result should match input")

		// Verify execution happened at timeout (200ms), not delay (2s)
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "execution should take at least 200ms")
		assert.LessOrEqual(t, elapsed, 2*time.Second, "execution should take less than 2s")
	})

	t.Run("TestDelayOverride", func(t *testing.T) {
		// First call: Debounce with a very long delay (creates debouncer workflow)
		key := "test-key-5"
		handle1, err := debouncer10sTimeout.Debounce(key, 10*time.Second, "first-input")
		require.NoError(t, err, "failed to call Debounce (first call)")

		// Second call: Debounce with delay=0 (should trigger immediate execution)
		startTime := time.Now()
		handle2, err := debouncer10sTimeout.Debounce(key, 0, "second-input")
		require.NoError(t, err, "failed to call Debounce (second call)")

		// Verify both handles refer to the same workflow ID
		assert.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID(), "both handles should refer to the same workflow ID")

		// Verify the second call completes immediately
		result, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "second-input", result, "result should match latest input")

		elapsed := time.Since(startTime)
		assert.LessOrEqual(t, elapsed, 2*time.Second, "execution should happen immediately with delay=0")
	})

	t.Run("TestDifferentKeys", func(t *testing.T) {
		// Call Debounce with different keys - each should create a separate group
		handle1, err := debouncer10sTimeout.Debounce("different-key-1", 200*time.Millisecond, "input-key-1")
		require.NoError(t, err, "failed to call Debounce with first key")

		handle2, err := debouncer10sTimeout.Debounce("different-key-2", 200*time.Millisecond, "input-key-2")
		require.NoError(t, err, "failed to call Debounce with second key")

		handle3, err := debouncer10sTimeout.Debounce("different-key-3", 200*time.Millisecond, "input-key-3")
		require.NoError(t, err, "failed to call Debounce with third key")

		// All handles should have different workflow IDs
		assert.NotEqual(t, handle1.GetWorkflowID(), handle2.GetWorkflowID(), "different keys should create different workflow IDs")
		assert.NotEqual(t, handle2.GetWorkflowID(), handle3.GetWorkflowID(), "different keys should create different workflow IDs")
		assert.NotEqual(t, handle1.GetWorkflowID(), handle3.GetWorkflowID(), "different keys should create different workflow IDs")

		// Each handle should get its own input
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first handle")
		assert.Equal(t, "input-key-1", result1, "first handle should get its own input")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second handle")
		assert.Equal(t, "input-key-2", result2, "second handle should get its own input")

		result3, err := handle3.GetResult()
		require.NoError(t, err, "failed to get result from third handle")
		assert.Equal(t, "input-key-3", result3, "third handle should get its own input")
	})

	t.Run("TestDifferentKeysExecuteIndependently", func(t *testing.T) {
		// Call Debounce with different keys and verify they execute independently
		handle1, err := debouncer10sTimeout.Debounce("independent-key-1", 5*time.Second, "independent-1")
		require.NoError(t, err, "failed to call Debounce with first key")

		startTime2 := time.Now()
		handle2, err := debouncer10sTimeout.Debounce("independent-key-2", 200*time.Millisecond, "independent-2")
		require.NoError(t, err, "failed to call Debounce with second key")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second handle")
		assert.Equal(t, "independent-2", result2, "second handle should get its own input")

		// Verify key-2 executed independently (should complete before the 5s delay of key-1)
		elapsed2 := time.Since(startTime2)
		assert.GreaterOrEqual(t, elapsed2, 200*time.Millisecond, "key-2 should execute after its delay")
		assert.Less(t, elapsed2, 5*time.Second, "key-2 should not be affected by key-1's delay")

		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first handle")
		assert.Equal(t, "independent-1", result1, "first handle should get its own input")
	})
}

func TestDebouncerClientWorkflowOptions(t *testing.T) {
	// Setup server context
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create test queue
	testQueue := NewWorkflowQueue(serverCtx, "debouncer-client-options-test-queue", WithPriorityEnabled(), WithPartitionQueue())

	// Register test workflow with a custom name
	debounceTestWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(serverCtx, debounceTestWorkflow, WithWorkflowName("DebounceTestWorkflow"))

	// Launch the server context
	err := Launch(serverCtx)
	require.NoError(t, err)

	// Setup client
	databaseURL := getDatabaseURL()
	config := ClientConfig{
		DatabaseURL: databaseURL,
	}
	client, err := NewClient(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(30 * time.Second)
		}
	})

	// Create debouncer client
	debouncer := NewDebouncerClient[string, string]("DebounceTestWorkflow", client, WithDebouncerTimeout(10*time.Second))

	// Test workflow options
	expectedWorkflowID := "test-workflow-id-12345"
	expectedPriority := uint(5)
	expectedPartitionKey := "partition-key-123"
	expectedAssumedRole := "test-assumed-role"
	expectedAuthenticatedUser := "test-user"
	expectedAuthenticatedRoles := []string{"role1", "role2", "role3"}
	testInput := "test-input-with-options"

	// Call Debounce with all workflow options
	handle, err := debouncer.Debounce(
		"workflow-options-key",
		200*time.Millisecond,
		testInput,
		WithWorkflowID(expectedWorkflowID),
		WithQueue(testQueue.Name),
		WithPriority(expectedPriority),
		WithQueuePartitionKey(expectedPartitionKey),
		WithAssumedRole(expectedAssumedRole),
		WithAuthenticatedUser(expectedAuthenticatedUser),
		WithAuthenticatedRoles(expectedAuthenticatedRoles),
	)
	require.NoError(t, err, "failed to call Debounce with workflow options")

	// Verify the handle returns the expected workflow ID
	workflowID := handle.GetWorkflowID()
	assert.Equal(t, expectedWorkflowID, workflowID, "handle should return the expected workflow ID")

	// Wait for the workflow to execute
	result, err := handle.GetResult()
	require.NoError(t, err, "failed to get result")
	assert.Equal(t, testInput, result, "result should match input")

	// List the workflow to verify all options are set correctly
	workflows, err := client.ListWorkflows(WithWorkflowIDs([]string{workflowID}))
	require.NoError(t, err, "failed to list workflows")
	require.Len(t, workflows, 1, "should find exactly one workflow")

	workflow := workflows[0]

	// Verify all workflow options are set correctly
	assert.Equal(t, expectedWorkflowID, workflow.ID, "workflow ID should match")
	assert.Equal(t, testQueue.Name, workflow.QueueName, "queue name should match")
	assert.Equal(t, int(expectedPriority), workflow.Priority, "priority should match")
	assert.Equal(t, expectedPartitionKey, workflow.QueuePartitionKey, "queue partition key should match")
	assert.Equal(t, expectedAssumedRole, workflow.AssumedRole, "assumed role should match")
	assert.Equal(t, expectedAuthenticatedUser, workflow.AuthenticatedUser, "authenticated user should match")
	assert.Equal(t, expectedAuthenticatedRoles, workflow.AuthenticatedRoles, "authenticated roles should match")
	assert.Equal(t, WorkflowStatusSuccess, workflow.Status, "workflow should have succeeded")
}

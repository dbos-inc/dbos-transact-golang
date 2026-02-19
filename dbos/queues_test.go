package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queueWorkflow(ctx DBOSContext, input string) (string, error) {
	step1, err := RunAsStep(ctx, func(context context.Context) (string, error) {
		return queueStep(context, input)
	})
	if err != nil {
		return "", fmt.Errorf("failed to run step: %v", err)
	}
	return step1, nil
}

func queueStep(_ context.Context, input string) (string, error) {
	return input, nil
}

func TestWorkflowQueues(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	queue := NewWorkflowQueue(dbosCtx, "test-queue",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))
	dlqEnqueueQueue := NewWorkflowQueue(dbosCtx, "test-successive-enqueue-queue",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))
	conflictQueue1 := NewWorkflowQueue(dbosCtx, "conflict-queue-1",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))
	conflictQueue2 := NewWorkflowQueue(dbosCtx, "conflict-queue-2",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))
	dedupQueue := NewWorkflowQueue(dbosCtx, "test-dedup-queue",
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueMaxPollingInterval(500*time.Millisecond))

	dlqStartEvent := NewEvent()
	dlqCompleteEvent := NewEvent()
	dlqMaxRetries := 10

	// Register workflows with dbosContext
	RegisterWorkflow(dbosCtx, queueWorkflow)

	// Custom name workflows
	queueWorkflowCustomName := func(ctx DBOSContext, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(dbosCtx, queueWorkflowCustomName, WithWorkflowName("custom-name"))

	queueWorkflowCustomNameEnqueingAnotherCustomNameWorkflow := func(ctx DBOSContext, input string) (string, error) {
		// Start a child workflow
		childHandle, err := RunWorkflow(ctx, queueWorkflowCustomName, input+"-enqueued", WithQueue(queue.Name))
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
	RegisterWorkflow(dbosCtx, queueWorkflowCustomNameEnqueingAnotherCustomNameWorkflow, WithWorkflowName("custom-name-enqueuing"))

	// Queue deduplication test workflows
	var dedupWorkflowEvent *Event
	childWorkflow := func(ctx DBOSContext, var1 string) (string, error) {
		if dedupWorkflowEvent != nil {
			dedupWorkflowEvent.Wait()
		}
		return var1 + "-c", nil
	}
	RegisterWorkflow(dbosCtx, childWorkflow)

	testWorkflow := func(ctx DBOSContext, var1 string) (string, error) {
		// Make sure the child workflow is not blocked by the same deduplication ID
		childHandle, err := RunWorkflow(ctx, childWorkflow, var1, WithQueue(dedupQueue.Name))
		if err != nil {
			return "", fmt.Errorf("failed to enqueue child workflow: %v", err)
		}
		if dedupWorkflowEvent != nil {
			dedupWorkflowEvent.Wait()
		}
		result, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get child result: %v", err)
		}
		return result + "-p", nil
	}
	RegisterWorkflow(dbosCtx, testWorkflow)

	// Create workflow with child that can call the main workflow
	queueWorkflowWithChild := func(ctx DBOSContext, input string) (string, error) {
		// Start a child workflow
		childHandle, err := RunWorkflow(ctx, queueWorkflow, input+"-child")
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
	RegisterWorkflow(dbosCtx, queueWorkflowWithChild)

	// Create workflow that enqueues another workflow
	queueWorkflowThatEnqueues := func(ctx DBOSContext, input string) (string, error) {
		// Enqueue another workflow to the same queue
		enqueuedHandle, err := RunWorkflow(ctx, queueWorkflow, input+"-enqueued", WithQueue(queue.Name))
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
	RegisterWorkflow(dbosCtx, queueWorkflowThatEnqueues)

	enqueueWorkflowDLQ := func(ctx DBOSContext, input string) (string, error) {
		dlqStartEvent.Set()
		dlqCompleteEvent.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, enqueueWorkflowDLQ, WithMaxRetries(dlqMaxRetries))

	// Create a workflow that enqueues another workflow to test step tracking
	workflowEnqueuesAnother := func(ctx DBOSContext, input string) (string, error) {
		// Enqueue a child workflow
		childHandle, err := RunWorkflow(ctx, queueWorkflow, input+"-child", WithQueue(queue.Name))
		if err != nil {
			return "", fmt.Errorf("failed to enqueue child workflow: %v", err)
		}

		// Get result from the child workflow
		childResult, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get child result: %v", err)
		}

		return childResult, nil
	}
	RegisterWorkflow(dbosCtx, workflowEnqueuesAnother)

	// Simple workflow for NonExistingQueue test
	simpleWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(dbosCtx, simpleWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err)

	t.Run("EnqueueWorkflow", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflow, "test-input", WithQueue(queue.Name))
		require.NoError(t, err)

		_, ok := handle.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle to be of type workflowPollingHandle, got %T", handle)

		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "test-input", res)

		// List steps: the workflow should have 1 step
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
		assert.Len(t, steps, 1)
		assert.Equal(t, 0, steps[0].StepID)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueueWorkflowCustomName", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflowCustomName, "test-input", WithQueue(queue.Name))
		require.NoError(t, err)

		_, ok := handle.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle to be of type workflowPollingHandle, got %T", handle)

		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "test-input", res)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueuedWorkflowStartsChildWorkflow", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflowWithChild, "test-input", WithQueue(queue.Name))
		require.NoError(t, err)

		res, err := handle.GetResult()
		require.NoError(t, err)

		// Expected result: child workflow returns "test-input-child"
		expectedResult := "test-input-child"
		assert.Equal(t, expectedResult, res)

		// List steps: the workflow should have 2 steps (Start the child and GetResult)
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
		assert.Len(t, steps, 2)
		assert.Equal(t, runtime.FuncForPC(reflect.ValueOf(queueWorkflow).Pointer()).Name(), steps[0].StepName)
		assert.Equal(t, 0, steps[0].StepID)
		assert.Equal(t, "DBOS.getResult", steps[1].StepName)
		assert.Equal(t, 1, steps[1].StepID)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("WorkflowEnqueuesAnother", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflowThatEnqueues, "test-input", WithQueue(queue.Name))
		require.NoError(t, err)

		res, err := handle.GetResult()
		require.NoError(t, err)

		// Expected result: enqueued workflow returns "test-input-enqueued"
		expectedResult := "test-input-enqueued"
		assert.Equal(t, expectedResult, res)

		// List steps: the workflow should have 2 steps (Start the child and GetResult)
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
		assert.Len(t, steps, 2)
		assert.Equal(t, runtime.FuncForPC(reflect.ValueOf(queueWorkflow).Pointer()).Name(), steps[0].StepName)
		assert.Equal(t, 0, steps[0].StepID)
		assert.Equal(t, "DBOS.getResult", steps[1].StepName)
		assert.Equal(t, 1, steps[1].StepID)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("CustomNameWorkflowEnqueuesAnotherCustomNameWorkflow", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflowCustomNameEnqueingAnotherCustomNameWorkflow, "test-input", WithQueue(queue.Name))
		require.NoError(t, err)

		res, err := handle.GetResult()
		require.NoError(t, err)

		// Expected result: enqueued workflow returns "test-input-enqueued"
		expectedResult := "test-input-enqueued"
		assert.Equal(t, expectedResult, res)

		// List steps: the workflow should have 2 steps (Start the child and GetResult)
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
		assert.Len(t, steps, 2)
		assert.Equal(t, "custom-name", steps[0].StepName)
		assert.Equal(t, 0, steps[0].StepID)
		assert.Equal(t, "DBOS.getResult", steps[1].StepName)
		assert.Equal(t, 1, steps[1].StepID)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueuedWorkflowEnqueuesAnother", func(t *testing.T) {
		// Run the pre-registered workflow that enqueues another workflow
		// Enqueue the parent workflow to a queue
		handle, err := RunWorkflow(dbosCtx, workflowEnqueuesAnother, "test-input", WithQueue(queue.Name))
		require.NoError(t, err)

		res, err := handle.GetResult()
		require.NoError(t, err)

		// Expected result: child workflow returns "test-input-child"
		expectedResult := "test-input-child"
		assert.Equal(t, expectedResult, res)

		// Check that the parent workflow (the one we ran directly) has 2 steps:
		// one for enqueueing the child and one for calling GetResult
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
		assert.Len(t, steps, 2)
		assert.Equal(t, runtime.FuncForPC(reflect.ValueOf(queueWorkflow).Pointer()).Name(), steps[0].StepName)
		assert.Equal(t, 0, steps[0].StepID)
		assert.Equal(t, "DBOS.getResult", steps[1].StepName)
		assert.Equal(t, 1, steps[1].StepID)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after workflow enqueues another workflow test")
	})

	t.Run("DynamicRegistration", func(t *testing.T) {
		// Attempting to register a queue after launch should panic
		defer func() {
			r := recover()
			assert.NotNil(t, r, "expected panic from queue registration after launch but got none")
		}()
		NewWorkflowQueue(dbosCtx, "dynamic-queue")
	})

	t.Run("QueueWorkflowDLQ", func(t *testing.T) {
		workflowID := "queue-dlq-workflow-test"
		dlqCompleteEvent.Clear()

		// Enqueue once; workflow will run and block on dlqCompleteEvent
		originalHandle, err := RunWorkflow(dbosCtx, enqueueWorkflowDLQ, "test-input", WithQueue(dlqEnqueueQueue.Name), WithWorkflowID(workflowID))
		require.NoError(t, err)

		// Wait for the workflow to start (blocked on dlqCompleteEvent)
		dlqStartEvent.Wait()
		dlqStartEvent.Clear()

		// Re-enqueue the same workflow ID many times; should not trigger DLQ (attempts stay 1)
		for i := range dlqMaxRetries * 2 {
			_, err := RunWorkflow(dbosCtx, enqueueWorkflowDLQ, "test-input", WithQueue(dlqEnqueueQueue.Name), WithWorkflowID(workflowID))
			require.NoError(t, err, "failed to enqueue workflow attempt %d", i+1)
		}

		// ListWorkflows for this queue should show a single pending workflow
		workflows, err := ListWorkflows(dbosCtx, WithQueueName(dlqEnqueueQueue.Name))
		require.NoError(t, err, "failed to list workflows for queue")
		require.Len(t, workflows, 1, "expected single workflow on queue, got %d", len(workflows))
		assert.Equal(t, WorkflowStatusPending, workflows[0].Status, "expected workflow to be PENDING")

		// Attempts counter should still be 1 (re-enqueues do not increment it)
		status, err := originalHandle.GetStatus()
		require.NoError(t, err, "failed to get status of original workflow handle")
		assert.Equal(t, 1, status.Attempts, "expected attempts to be 1")

		// Deblock so the workflow can complete
		dlqCompleteEvent.Set()
		result, err := originalHandle.GetResult()
		require.NoError(t, err, "failed to get result from initial run")
		assert.Equal(t, "test-input", result)

		// Flip to PENDING and loop: recover, GetResult, flip (same pattern as TestWorkflowDeadLetterQueue)
		setWorkflowStatusPending(t, dbosCtx, workflowID)
		for i := range dlqMaxRetries {
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			require.Len(t, recoveredHandles, 1, "expected 1 handle on attempt %d", i+1)
			_, err = recoveredHandles[0].GetResult()
			require.NoError(t, err, "failed to get result from recovered handle on attempt %d", i+1)
			// check number of attempts is correctly increment
			status, err := recoveredHandles[0].GetStatus()
			require.NoError(t, err, "failed to get status from recovered handle")
			assert.Equal(t, i+2, status.Attempts, "expected number of attempts to be %d, got %d", i+2, status.Attempts)
			setWorkflowStatusPending(t, dbosCtx, workflowID)
		}

		// Next recover should clear the queue assignment, no error should be returned
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "expected no error when recovering pending workflows")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle")
		require.Equal(t, workflowID, recoveredHandles[0].GetWorkflowID(), "expected recovered handle to have the same ID as the original workflow")

		// The workflow will be eventually dequeued but hit a DLQ error
		require.Eventually(t, func() bool {
			status, err := recoveredHandles[0].GetStatus()
			return err == nil && status.Status == WorkflowStatusMaxRecoveryAttemptsExceeded
		}, 10*time.Second, 100*time.Millisecond, "expected workflow status to become MAX_RECOVERY_ATTEMPTS_EXCEEDED")

		// Resume the workflow (clears DLQ status), wait for result, then verify it completes with SUCCESS
		resumedHandle, err := ResumeWorkflow[string](dbosCtx, workflowID)
		require.NoError(t, err, "failed to resume workflow")
		resumedResult, err := resumedHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed handle")
		assert.Equal(t, "test-input", resumedResult)

		require.Eventually(t, func() bool {
			status, err := originalHandle.GetStatus()
			return err == nil && status.Status == WorkflowStatusSuccess
		}, 10*time.Second, 100*time.Millisecond, "expected workflow status to become SUCCESS after resume")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after successive enqueues test")
	})

	t.Run("ConflictingWorkflowOnDifferentQueues", func(t *testing.T) {
		workflowID := "conflicting-workflow-id"

		// Enqueue the same workflow ID on the first queue
		handle, err := RunWorkflow(dbosCtx, queueWorkflow, "test-input-1", WithQueue(conflictQueue1.Name), WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to enqueue workflow on first queue")

		// Get the result from the first workflow to ensure it completes
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		assert.Equal(t, "test-input-1", result, "expected 'test-input-1'")

		// Now try to enqueue the same workflow ID on a different queue
		// This should trigger a ConflictingWorkflowError
		_, err = RunWorkflow(dbosCtx, queueWorkflow, "test-input-2", WithQueue(conflictQueue2.Name), WithWorkflowID(workflowID))
		require.Error(t, err, "expected ConflictingWorkflowError when enqueueing same workflow ID on different queue, but got none")

		// Check that it's the correct error type
		require.True(t, errors.Is(err, &DBOSError{Code: ConflictingWorkflowError}), "expected error to be ConflictingWorkflowError, got %T", err)

		// Check that the error message contains queue information
		expectedMsgPart := "Workflow already exists in a different queue"
		assert.Contains(t, err.Error(), expectedMsgPart, "expected error message to contain expected part")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after conflicting workflow test")
	})

	t.Run("QueueDeduplication", func(t *testing.T) {
		workflowEvent := NewEvent()
		dedupWorkflowEvent = workflowEvent
		defer func() {
			dedupWorkflowEvent = nil
		}()

		// Make sure only one workflow is running at a time
		wfid := uuid.NewString()
		dedupID := "my_dedup_id"
		handle1, err := RunWorkflow(dbosCtx, testWorkflow, "abc", WithQueue(dedupQueue.Name), WithWorkflowID(wfid), WithDeduplicationID(dedupID))
		require.NoError(t, err, "failed to enqueue first workflow with deduplication ID")

		// Enqueue the same workflow with a different deduplication ID should be fine
		anotherHandle, err := RunWorkflow(dbosCtx, testWorkflow, "ghi", WithQueue(dedupQueue.Name), WithDeduplicationID("my_other_dedup_id"))
		require.NoError(t, err, "failed to enqueue workflow with different deduplication ID")

		// Enqueue a workflow without deduplication ID should be fine
		nodedupHandle1, err := RunWorkflow(dbosCtx, testWorkflow, "jkl", WithQueue(dedupQueue.Name))
		require.NoError(t, err, "failed to enqueue workflow without deduplication ID")

		// Enqueued multiple times without deduplication ID but with different inputs should be fine, but get the result of the first one
		nodedupHandle2, err := RunWorkflow(dbosCtx, testWorkflow, "mno", WithQueue(dedupQueue.Name), WithWorkflowID(wfid))
		require.NoError(t, err, "failed to enqueue workflow with same workflow ID")

		// Enqueue the same workflow with the same deduplication ID should raise an exception
		wfid2 := uuid.NewString()
		_, err = RunWorkflow(dbosCtx, testWorkflow, "def", WithQueue(dedupQueue.Name), WithWorkflowID(wfid2), WithDeduplicationID(dedupID))
		require.Error(t, err, "expected error when enqueueing workflow with same deduplication ID")

		// Check that it's the correct error type and message
		require.True(t, errors.Is(err, &DBOSError{Code: QueueDeduplicated}), "expected error to be QueueDeduplicated, got %T", err)

		expectedMsgPart := fmt.Sprintf("Workflow %s was deduplicated due to an existing workflow in queue %s with deduplication ID %s", wfid2, dedupQueue.Name, dedupID)
		assert.Contains(t, err.Error(), expectedMsgPart, "expected error message to contain deduplication information")

		// Now unblock the workflows and wait for them to finish
		workflowEvent.Set()
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		assert.Equal(t, "abc-c-p", result1, "expected first workflow result to be 'abc-c-p'")

		resultAnother, err := anotherHandle.GetResult()
		require.NoError(t, err, "failed to get result from workflow with different dedup ID")
		assert.Equal(t, "ghi-c-p", resultAnother, "expected another workflow result to be 'ghi-c-p'")

		resultNodedup1, err := nodedupHandle1.GetResult()
		require.NoError(t, err, "failed to get result from workflow without dedup ID")
		assert.Equal(t, "jkl-c-p", resultNodedup1, "expected nodedup1 workflow result to be 'jkl-c-p'")

		resultNodedup2, err := nodedupHandle2.GetResult()
		require.NoError(t, err, "failed to get result from reused workflow ID")
		assert.Equal(t, "abc-c-p", resultNodedup2, "expected nodedup2 workflow result to be 'abc-c-p'")

		// Invoke the workflow again with the same deduplication ID now should be fine because it's no longer in the queue
		handle2, err := RunWorkflow(dbosCtx, testWorkflow, "def", WithQueue(dedupQueue.Name), WithWorkflowID(wfid2), WithDeduplicationID(dedupID))
		require.NoError(t, err, "failed to enqueue workflow with same dedup ID after completion")
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second workflow with same dedup ID")
		assert.Equal(t, "def-c-p", result2, "expected second workflow result to be 'def-c-p'")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after deduplication test")
	})

	t.Run("NonExistingQueue", func(t *testing.T) {
		// Attempt to enqueue to a non-existing queue
		// This should return an error
		_, err := RunWorkflow(dbosCtx, simpleWorkflow, "test-input", WithQueue("non-existing-queue"))
		require.Error(t, err, "expected error when enqueueing to non-existing queue")

		// Check that it's the correct error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		// Verify the error is wrapped by newWorkflowExecutionError with WorkflowExecutionError code
		assert.True(t, errors.Is(err, &DBOSError{Code: WorkflowExecutionError}), "expected error to be WorkflowExecutionError")

		// Verify the unwrapped error contains the validation message
		unwrappedErr := errors.Unwrap(dbosErr)
		require.NotNil(t, unwrappedErr, "expected error to have an unwrapped error")
		expectedMsgPart := "does not exist"
		assert.Contains(t, unwrappedErr.Error(), expectedMsgPart, "expected unwrapped error message to contain expected part")
	})

	t.Run("ListRegisteredQueues", func(t *testing.T) {
		// Get all registered queues
		registeredQueues, err := ListRegisteredQueues(dbosCtx)
		require.NoError(t, err, "failed to list registered queues")

		// Create a map of expected queue names for easy lookup
		expectedQueueNames := map[string]bool{
			queue.Name:                true,
			dlqEnqueueQueue.Name:      true,
			conflictQueue1.Name:       true,
			conflictQueue2.Name:       true,
			dedupQueue.Name:           true,
			_DBOS_INTERNAL_QUEUE_NAME: true, // Internal queue is always registered
		}

		// Verify we got the expected number of queues
		assert.Equal(t, len(expectedQueueNames), len(registeredQueues), "expected %d registered queues, got %d", len(expectedQueueNames), len(registeredQueues))

		// Verify all expected queues are present
		actualQueueNames := make(map[string]bool)
		for _, q := range registeredQueues {
			actualQueueNames[q.Name] = true
			// Verify the queue exists in our expected list
			assert.True(t, expectedQueueNames[q.Name], "unexpected queue found: %s", q.Name)
		}

		// Verify all expected queues are in the result
		for queueName := range expectedQueueNames {
			assert.True(t, actualQueueNames[queueName], "expected queue %s not found in registered queues", queueName)
		}

		// Verify specific queue properties for known queues
		for _, q := range registeredQueues {
			switch q.Name {
			case queue.Name:
				// Verify default queue properties
				assert.Nil(t, q.WorkerConcurrency, "expected queue to have nil WorkerConcurrency")
				assert.Nil(t, q.GlobalConcurrency, "expected queue to have nil GlobalConcurrency")
				assert.False(t, q.PriorityEnabled, "expected queue to have PriorityEnabled=false")
			case dedupQueue.Name:
				// Verify dedup queue properties
				assert.Nil(t, q.WorkerConcurrency, "expected dedup queue to have nil WorkerConcurrency")
			}
		}
	})
}

func TestQueueRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	recoveryQueue := NewWorkflowQueue(dbosCtx, "recovery-queue")
	var recoveryStepCounter int64

	recoveryStepWorkflowFunc := func(ctx DBOSContext, i int) (int, error) {
		atomic.AddInt64(&recoveryStepCounter, 1)
		return i, nil
	}
	RegisterWorkflow(dbosCtx, recoveryStepWorkflowFunc)

	recoveryWorkflowFunc := func(ctx DBOSContext, input string) ([]int, error) {
		handles := make([]WorkflowHandle[int], 0, 5)
		for i := range 5 {
			handle, err := RunWorkflow(ctx, recoveryStepWorkflowFunc, i, WithQueue(recoveryQueue.Name))
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
	}
	RegisterWorkflow(dbosCtx, recoveryWorkflowFunc)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	queuedSteps := 5
	wfid := uuid.NewString()

	// Run parent workflow to completion
	handle, err := RunWorkflow(dbosCtx, recoveryWorkflowFunc, "", WithWorkflowID(wfid))
	require.NoError(t, err, "failed to start workflow")

	result, err := handle.GetResult()
	require.NoError(t, err, "failed to get result from parent workflow")
	expectedResult := []int{0, 1, 2, 3, 4}
	assert.Equal(t, expectedResult, result, "expected result %v, got %v", expectedResult, result)

	// Parent: 5 RunWorkflow (enqueue children) then 5 GetResult — steps 0..4 enqueue, 5..9 getResult
	steps, err := GetWorkflowSteps(dbosCtx, wfid)
	require.NoError(t, err, "failed to get parent workflow steps")
	require.Len(t, steps, 10, "expected 10 steps (5 enqueued child + 5 getResult)")

	recoveryStepStepName := runtime.FuncForPC(reflect.ValueOf(recoveryStepWorkflowFunc).Pointer()).Name()
	for i := range queuedSteps {
		// RunWorkflow steps (enqueue children) — steps 0..4
		require.Equal(t, i, steps[i].StepID, "step %d StepID", i)
		require.Equal(t, recoveryStepStepName, steps[i].StepName, "step %d (enqueue) StepName", i)
	}
	for i := range queuedSteps {
		// GetResult steps — steps 5..9
		idx := queuedSteps + i
		require.Equal(t, idx, steps[idx].StepID, "step %d StepID", idx)
		require.Equal(t, "DBOS.getResult", steps[idx].StepName, "step %d (getResult) StepName", idx)
	}

	assert.Equal(t, int64(queuedSteps), atomic.LoadInt64(&recoveryStepCounter), "expected recoveryStepCounter to match queuedSteps")

	// Get child workflow IDs (they were enqueued on recoveryQueue)
	workflowsOnQueue, err := ListWorkflows(dbosCtx, WithQueueName(recoveryQueue.Name))
	require.NoError(t, err, "failed to list workflows on recovery queue")
	require.Len(t, workflowsOnQueue, queuedSteps, "expected %d child workflows on queue", queuedSteps)
	childIDs := make([]string, 0, queuedSteps)
	for _, wf := range workflowsOnQueue {
		childIDs = append(childIDs, wf.ID)
	}

	// Flip state of parent and all children to PENDING
	setWorkflowStatusPending(t, dbosCtx, wfid)
	for _, childID := range childIDs {
		setWorkflowStatusPending(t, dbosCtx, childID)
	}

	// Recover and wait for each workflow to finish
	recoveryHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")
	require.Len(t, recoveryHandles, queuedSteps+1, "expected parent + %d children", queuedSteps)

	for _, h := range recoveryHandles {
		resultAny, err := h.GetResult()
		require.NoError(t, err, "failed to get result from recovered handle %s", h.GetWorkflowID())
		if h.GetWorkflowID() == wfid {
			encodedResult, ok := resultAny.([]any)
			require.True(t, ok, "expected parent result to be []any")
			jsonBytes, err := json.Marshal(encodedResult)
			require.NoError(t, err, "failed to marshal result to JSON")
			var castedResult []int
			err = json.Unmarshal(jsonBytes, &castedResult)
			require.NoError(t, err, "failed to decode result to []int")
			assert.Equal(t, expectedResult, castedResult, "expected recovered parent result %v, got %v", expectedResult, castedResult)
		} else {
			// Child result (float64 from JSON or int)
			var val int
			switch v := resultAny.(type) {
			case float64:
				val = int(v)
			case int:
				val = v
			default:
				t.Fatalf("unexpected child result type %T", resultAny)
			}
			assert.Contains(t, expectedResult, val, "child result %d not in expected set", val)
		}
	}

	assert.Equal(t, int64(queuedSteps*2), atomic.LoadInt64(&recoveryStepCounter), "expected recoveryStepCounter to be %d after recovery", queuedSteps*2)

	// Rerun the workflow; steps should not re-execute (idempotent)
	rerunHandle, err := RunWorkflow(dbosCtx, recoveryWorkflowFunc, "test-input", WithWorkflowID(wfid))
	require.NoError(t, err, "failed to rerun workflow")
	rerunResult, err := rerunHandle.GetResult()
	require.NoError(t, err, "failed to get result from rerun handle")
	assert.Equal(t, expectedResult, rerunResult, "expected result %v, got %v", expectedResult, rerunResult)

	assert.Equal(t, int64(queuedSteps*2), atomic.LoadInt64(&recoveryStepCounter), "expected recoveryStepCounter to remain %d after rerun", queuedSteps*2)

	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after recovery test")
}

// Note: we could update this test to have the same logic than TestWorkerConcurrency
func TestGlobalConcurrency(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	globalConcurrencyQueue := NewWorkflowQueue(dbosCtx, "test-global-concurrency-queue", WithGlobalConcurrency(1))
	workflowEvent1 := NewEvent()
	workflowEvent2 := NewEvent()
	workflowDoneEvent := NewEvent()

	// Create workflow with dbosContext
	globalConcurrencyWorkflowFunc := func(ctx DBOSContext, input string) (string, error) {
		switch input {
		case "workflow1":
			workflowEvent1.Set()
			workflowDoneEvent.Wait()
		case "workflow2":
			workflowEvent2.Set()
		}
		return input, nil
	}
	RegisterWorkflow(dbosCtx, globalConcurrencyWorkflowFunc)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	// Enqueue two workflows
	handle1, err := RunWorkflow(dbosCtx, globalConcurrencyWorkflowFunc, "workflow1", WithQueue(globalConcurrencyQueue.Name))
	require.NoError(t, err, "failed to enqueue workflow1")

	handle2, err := RunWorkflow(dbosCtx, globalConcurrencyWorkflowFunc, "workflow2", WithQueue(globalConcurrencyQueue.Name))
	require.NoError(t, err, "failed to enqueue workflow2")

	// Wait for the first workflow to start
	workflowEvent1.Wait()
	time.Sleep(2 * time.Second) // Wait for a few seconds to let the queue runner loop

	// Ensure the second workflow has not started yet
	assert.False(t, workflowEvent2.IsSet, "expected workflow2 to not start while workflow1 is running")
	status, err := handle2.GetStatus()
	require.NoError(t, err, "failed to get status of workflow2")
	assert.Equal(t, WorkflowStatusEnqueued, status.Status, "expected workflow2 to be in ENQUEUED status")

	// Allow the first workflow to complete
	workflowDoneEvent.Set()

	result1, err := handle1.GetResult()
	require.NoError(t, err, "failed to get result from workflow1")
	assert.Equal(t, "workflow1", result1, "expected result from workflow1 to be 'workflow1'")

	// Wait for the second workflow to start
	workflowEvent2.Wait()

	result2, err := handle2.GetResult()
	require.NoError(t, err, "failed to get result from workflow2")
	assert.Equal(t, "workflow2", result2, "expected result from workflow2 to be 'workflow2'")
	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
}

func TestWorkerConcurrency(t *testing.T) {
	// Create two contexts that will represent 2 DBOS executors
	os.Setenv("DBOS__VMID", "worker1")
	dbosCtx1 := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	os.Setenv("DBOS__VMID", "worker2")
	dbosCtx2 := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false}) // Don't check for leaks because t.Cancel is called in LIFO order. Also don't reset the DB here.
	os.Unsetenv("DBOS__VMID")

	assert.Equal(t, "worker1", dbosCtx1.GetExecutorID(), "expected first executor ID to be 'worker1'")
	assert.Equal(t, "worker2", dbosCtx2.GetExecutorID(), "expected second executor ID to be 'worker2'")

	workerConcurrencyQueue := NewWorkflowQueue(dbosCtx1, "test-worker-concurrency-queue", WithWorkerConcurrency(1))
	NewWorkflowQueue(dbosCtx2, "test-worker-concurrency-queue", WithWorkerConcurrency(1))
	startEvents := []*Event{
		NewEvent(),
		NewEvent(),
		NewEvent(),
		NewEvent(),
	}
	completeEvents := []*Event{
		NewEvent(),
		NewEvent(),
		NewEvent(),
		NewEvent(),
	}

	// Helper function to check the status of workflows in the queue
	checkWorkflowStatus := func(t *testing.T, expectedPendingPerExecutor, expectedEnqueued int) {
		workflows, err := dbosCtx1.(*dbosContext).systemDB.listWorkflows(context.Background(), listWorkflowsDBInput{
			queueName: []string{workerConcurrencyQueue.Name},
		})
		require.NoError(t, err, "failed to list workflows")

		pendings := make(map[string]int)
		enqueuedCount := 0

		for _, wf := range workflows {
			switch wf.Status {
			case WorkflowStatusPending:
				pendings[wf.ExecutorID]++
			case WorkflowStatusEnqueued:
				enqueuedCount++
			}
		}

		for executorID, count := range pendings {
			assert.Equal(t, expectedPendingPerExecutor, count, "expected %d pending workflow on executor %s", expectedPendingPerExecutor, executorID)
		}

		assert.Equal(t, expectedEnqueued, enqueuedCount, "expected %d workflows to be enqueued", expectedEnqueued)
	}

	// Create workflow with dbosContext
	blockingWfFunc := func(ctx DBOSContext, i int) (int, error) {
		// Simulate a blocking operation
		startEvents[i].Set()
		completeEvents[i].Wait()
		return i, nil
	}
	RegisterWorkflow(dbosCtx1, blockingWfFunc)
	RegisterWorkflow(dbosCtx2, blockingWfFunc)

	err := Launch(dbosCtx1)
	require.NoError(t, err, "failed to launch DBOS instance")

	err = Launch(dbosCtx2)
	require.NoError(t, err, "failed to launch DBOS instance")

	// First enqueue four blocking workflows
	handle1, err := RunWorkflow(dbosCtx1, blockingWfFunc, 0, WithQueue(workerConcurrencyQueue.Name), WithWorkflowID("worker-cc-wf-1"))
	require.NoError(t, err)
	handle2, err := RunWorkflow(dbosCtx1, blockingWfFunc, 1, WithQueue(workerConcurrencyQueue.Name), WithWorkflowID("worker-cc-wf-2"))
	require.NoError(t, err)
	_, err = RunWorkflow(dbosCtx1, blockingWfFunc, 2, WithQueue(workerConcurrencyQueue.Name), WithWorkflowID("worker-cc-wf-3"))
	require.NoError(t, err)
	_, err = RunWorkflow(dbosCtx1, blockingWfFunc, 3, WithQueue(workerConcurrencyQueue.Name), WithWorkflowID("worker-cc-wf-4"))
	require.NoError(t, err)

	// The two first workflows should dequeue on both workers
	startEvents[0].Wait()
	startEvents[1].Wait()
	// Ensure the two other workflows are not started yet
	assert.False(t, startEvents[2].IsSet || startEvents[3].IsSet, "expected only blocking workflow 1 and 2 to start, but others have started")

	// Expect 1 workflow pending on each executor and 2 workflows enqueued
	checkWorkflowStatus(t, 1, 2)

	// Unlock workflow 1, check wf 3 starts, check 4 stays blocked
	completeEvents[0].Set()
	result1, err := handle1.GetResult()
	require.NoError(t, err, "failed to get result from blocking workflow 1")
	assert.Equal(t, 0, result1, "expected result from blocking workflow 1 to be 0")
	// 3rd workflow should start
	startEvents[2].Wait()
	// Ensure the fourth workflow is not started yet
	assert.False(t, startEvents[3].IsSet, "expected only blocking workflow 3 to start, but workflow 4 has started")

	// Check that 1 workflow is pending on each executor and 1 workflow is enqueued
	checkWorkflowStatus(t, 1, 1)

	// Unlock workflow 2 and check wf 4 starts
	completeEvents[1].Set()
	result2, err := handle2.GetResult()
	require.NoError(t, err, "failed to get result from blocking workflow 2")
	assert.Equal(t, 1, result2, "expected result from blocking workflow 2 to be 1")
	// 4th workflow should start now
	startEvents[3].Wait()
	// workflow 3 and 4 should be pending, one per executor, and no workflows enqueued
	checkWorkflowStatus(t, 1, 0)

	// Unblock both workflows 3 and 4
	completeEvents[2].Set()
	completeEvents[3].Set()

	require.True(t, queueEntriesAreCleanedUp(dbosCtx1), "expected queue entries to be cleaned up after global concurrency test")
}

func rateLimiterTestWorkflow(ctx DBOSContext, _ string) (time.Time, error) {
	return time.Now(), nil // Return current time
}

func TestQueueRateLimiter(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	rateLimiterQueue := NewWorkflowQueue(dbosCtx, "test-rate-limiter-queue", WithRateLimiter(&RateLimiter{Limit: 5, Period: time.Duration(1800 * time.Millisecond)}))

	// Create workflow with dbosContext
	RegisterWorkflow(dbosCtx, rateLimiterTestWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	limit := 5
	periodSeconds := 1.8
	numWaves := 3

	var handles []WorkflowHandle[time.Time]
	var times []time.Time

	// Launch a number of tasks equal to three times the limit.
	// This should lead to three "waves" of the limit tasks being
	// executed simultaneously, followed by a wait of the period,
	// followed by the next wave.
	for i := 0; i < limit*numWaves; i++ {
		handle, err := RunWorkflow(dbosCtx, rateLimiterTestWorkflow, "", WithQueue(rateLimiterQueue.Name))
		require.NoError(t, err, "failed to enqueue workflow %d", i)
		handles = append(handles, handle)
	}

	// Get results from all workflows
	for _, handle := range handles {
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")
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
	require.Greater(t, len(sortedTimes), 0, "no workflow times recorded")

	baseTime := sortedTimes[0]
	waveMap := make(map[int][]time.Time)

	// Group workflows into waves based on their start time
	for _, workflowTime := range sortedTimes {
		timeSinceBase := workflowTime.Sub(baseTime).Seconds()
		waveIndex := int(timeSinceBase / periodSeconds)
		waveMap[waveIndex] = append(waveMap[waveIndex], workflowTime)
	}
	// Verify each wave has fewer than the limit
	for waveIndex, wave := range waveMap {
		assert.LessOrEqual(t, len(wave), limit, "wave %d has %d workflows, which exceeds the limit of %d", waveIndex, len(wave), limit)
		assert.Greater(t, len(wave), 0, "wave %d is empty, which shouldn't happen", waveIndex)
	}
	// Verify we have the expected number of waves (allowing some tolerance)
	expectedWaves := numWaves
	assert.GreaterOrEqual(t, len(waveMap), expectedWaves-1, "expected approximately %d waves, got %d", expectedWaves, len(waveMap))
	assert.LessOrEqual(t, len(waveMap), expectedWaves+1, "expected approximately %d waves, got %d", expectedWaves, len(waveMap))

	// Verify all workflows get the SUCCESS status eventually
	for i, handle := range handles {
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get status for workflow %d", i)
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected workflow %d to have SUCCESS status", i)
	}

	// Verify all queue entries eventually get cleaned up.
	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after rate limiter test")
}

func TestQueueTimeouts(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	timeoutQueue := NewWorkflowQueue(dbosCtx, "timeout-queue")

	queuedWaitForCancelWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded), "workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, queuedWaitForCancelWorkflow)

	enqueuedWorkflowEnqueuesATimeoutWorkflow := func(ctx DBOSContext, childWorkflowID string) (string, error) {
		// This workflow will enqueue a workflow that waits indefinitely until it is cancelled
		handle, err := RunWorkflow(ctx, queuedWaitForCancelWorkflow, "enqueued-wait-for-cancel", WithQueue(timeoutQueue.Name), WithWorkflowID(childWorkflowID))
		require.NoError(t, err, "failed to start enqueued wait for cancel workflow")
		// Workflow should get AwaitedWorkflowCancelled DBOSError
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when waiting for enqueued workflow to complete, but got none")
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)
		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be AwaitedWorkflowCancelled")
		return "", nil
	}
	RegisterWorkflow(dbosCtx, enqueuedWorkflowEnqueuesATimeoutWorkflow)

	detachedWorkflow := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
			return "detached-workflow-completed", nil
		}
	}

	enqueuedWorkflowEnqueuesADetachedWorkflow := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		myId, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %v", err)
		}
		childID := fmt.Sprintf("%s-child", myId)
		// This workflow will enqueue a workflow that is not cancelable
		childCtx := WithoutCancel(ctx)
		handle, err := RunWorkflow(childCtx, detachedWorkflow, timeout*2, WithQueue(timeoutQueue.Name), WithWorkflowID(childID))
		require.NoError(t, err, "failed to start enqueued detached workflow")

		// Wait for the enqueued workflow to complete
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from enqueued detached workflow")
		assert.Equal(t, "detached-workflow-completed", result, "expected result to be 'detached-workflow-completed'")
		return result, nil
	}

	RegisterWorkflow(dbosCtx, detachedWorkflow)
	RegisterWorkflow(dbosCtx, enqueuedWorkflowEnqueuesADetachedWorkflow)

	timeoutOnDequeueQueue := NewWorkflowQueue(dbosCtx, "timeout-on-dequeue-queue", WithGlobalConcurrency(1))
	blockingEvent := NewEvent()
	blockingWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		blockingEvent.Wait()
		return "blocking-done", nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)
	fastWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, fastWorkflow)

	Launch(dbosCtx)

	t.Run("EnqueueWorkflowTimeout", func(t *testing.T) {
		// Start a workflow that will wait indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunWorkflow(cancelCtx, queuedWaitForCancelWorkflow, "enqueue-wait-for-cancel", WithQueue(timeoutQueue.Name))
		require.NoError(t, err, "failed to enqueue wait for cancel workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.Error(t, err, "expected error but got none")

		// Check the error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be AwaitedWorkflowCancelled")

		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after workflow cancellation, but they are not")
	})

	t.Run("EnqueueWorkflowThatEnqueuesATimeoutWorkflow", func(t *testing.T) {
		// Start a workflow that enqueues another workflow that waits indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		childWorkflowID := uuid.NewString()
		handle, err := RunWorkflow(cancelCtx, enqueuedWorkflowEnqueuesATimeoutWorkflow, childWorkflowID, WithQueue(timeoutQueue.Name))
		require.NoError(t, err, "failed to start enqueued workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.Error(t, err, "expected error but got none")

		// Check the error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be AwaitedWorkflowCancelled")

		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Wait for the child workflow status to become cancelled
		require.Eventually(t, func() bool {
			childHandle, err := RetrieveWorkflow[string](dbosCtx, childWorkflowID)
			require.NoError(t, err, "failed to retrieve child workflow")

			status, err := childHandle.GetStatus()
			if err != nil {
				return false
			}
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "expected enqueued workflow status to be WorkflowStatusCancelled")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after workflow cancellation, but they are not")
	})

	t.Run("EnqueueWorkflowThatEnqueuesADetachedWorkflow", func(t *testing.T) {
		// Start a workflow that enqueues another workflow that is not cancelable
		timeout := 100 * time.Millisecond
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunWorkflow(cancelCtx, enqueuedWorkflowEnqueuesADetachedWorkflow, timeout, WithQueue(timeoutQueue.Name))
		require.NoError(t, err, "failed to start enqueued detached workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.Error(t, err, "expected error but got none")

		// Check the error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be AwaitedWorkflowCancelled")

		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get enqueued detached workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected enqueued detached workflow status to be WorkflowStatusCancelled")

		// Check the child's status: should be success because it is detached
		require.Eventually(t, func() bool {
			childID := fmt.Sprintf("%s-child", handle.GetWorkflowID())
			childHandle, err := RetrieveWorkflow[string](dbosCtx, childID)
			require.NoError(t, err, "failed to retrieve detached workflow")

			status, err := childHandle.GetStatus()
			if err != nil {
				return false
			}
			return status.Status == WorkflowStatusSuccess
		}, 5*time.Second, 100*time.Millisecond, "expected detached workflow status to be WorkflowStatusSuccess")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after workflow cancellation, but they are not")
	})

	t.Run("TimeoutOnlySetOnDequeue", func(t *testing.T) {
		// Test that deadline is only set when workflow is dequeued, not when enqueued

		// Enqueue blocking workflow first
		blockingHandle, err := RunWorkflow(dbosCtx, blockingWorkflow, "blocking", WithQueue(timeoutOnDequeueQueue.Name))
		require.NoError(t, err, "failed to enqueue blocking workflow")

		// Set a timeout that would expire if set on enqueue
		timeout := 2 * time.Second
		timeoutCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()

		// Enqueue second workflow with timeout
		handle, err := RunWorkflow(timeoutCtx, fastWorkflow, "timeout-test", WithQueue(timeoutOnDequeueQueue.Name))
		require.NoError(t, err, "failed to enqueue timeout workflow")

		// Sleep for duration exceeding the timeout
		time.Sleep(timeout * 2)

		// Signal the blocking workflow to complete
		blockingEvent.Set()

		// Wait for blocking workflow to complete
		blockingResult, err := blockingHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocking workflow")
		assert.Equal(t, "blocking-done", blockingResult, "expected blocking workflow result")

		// Now the second workflow should dequeue and complete successfully (timeout should be much longer than execution time)
		// Note: this might be flaky if we the dequeue is delayed too long
		_, err = handle.GetResult()
		require.NoError(t, err, "unexpected error from workflow")

		// Check the workflow status: should be success
		finalStatus, err := handle.GetStatus()
		require.NoError(t, err, "failed to get final status of timeout workflow")
		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status, "expected timeout workflow status to be WorkflowStatusSuccess")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after test")
	})
}

func TestPriorityQueue(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create priority-enabled queue with max concurrency of 1
	priorityQueue := NewWorkflowQueue(dbosCtx, "test_queue_priority", WithGlobalConcurrency(1), WithPriorityEnabled())
	childQueue := NewWorkflowQueue(dbosCtx, "test_queue_child")

	workflowEvent := NewEvent()
	var wfPriorityList []int
	var mu sync.Mutex

	childWorkflow := func(ctx DBOSContext, p int) (int, error) {
		workflowEvent.Wait()
		return p, nil
	}
	RegisterWorkflow(dbosCtx, childWorkflow)

	testWorkflow := func(ctx DBOSContext, priority int) (int, error) {
		mu.Lock()
		wfPriorityList = append(wfPriorityList, priority)
		mu.Unlock()

		childHandle, err := RunWorkflow(ctx, childWorkflow, priority, WithQueue(childQueue.Name))
		if err != nil {
			return 0, fmt.Errorf("failed to enqueue child workflow: %v", err)
		}
		workflowEvent.Wait()
		result, err := childHandle.GetResult()
		if err != nil {
			return 0, fmt.Errorf("failed to get child result: %v", err)
		}
		return result + priority, nil
	}
	RegisterWorkflow(dbosCtx, testWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err)

	var wfHandles []WorkflowHandle[int]

	// First, enqueue a workflow without priority (default to priority 0)
	handle, err := RunWorkflow(dbosCtx, testWorkflow, 0, WithQueue(priorityQueue.Name))
	require.NoError(t, err)
	wfHandles = append(wfHandles, handle)

	// Then, enqueue workflows with priority 5 to 1
	reversedPriorityHandles := make([]WorkflowHandle[int], 0, 5)
	for i := 5; i > 0; i-- {
		handle, err := RunWorkflow(dbosCtx, testWorkflow, i, WithQueue(priorityQueue.Name), WithPriority(uint(i)))
		require.NoError(t, err)
		reversedPriorityHandles = append(reversedPriorityHandles, handle)
	}
	for i := 0; i < len(reversedPriorityHandles); i++ {
		wfHandles = append(wfHandles, reversedPriorityHandles[len(reversedPriorityHandles)-i-1])
	}

	// Finally, enqueue two workflows without priority again (default priority 0)
	handle6, err := RunWorkflow(dbosCtx, testWorkflow, 6, WithQueue(priorityQueue.Name))
	require.NoError(t, err)
	wfHandles = append(wfHandles, handle6)

	time.Sleep(10 * time.Millisecond) // Avoid collisions in created_at...
	handle7, err := RunWorkflow(dbosCtx, testWorkflow, 7, WithQueue(priorityQueue.Name))
	require.NoError(t, err)
	wfHandles = append(wfHandles, handle7)

	// The finish sequence should be 0, 6, 7, 1, 2, 3, 4, 5
	// (lower priority numbers execute first, same priority follows FIFO)
	workflowEvent.Set()

	for i, handle := range wfHandles {
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow %d", i)
		assert.Equal(t, i*2, result, "expected result %d for workflow %d", i*2, i)
	}

	mu.Lock()
	expectedOrder := []int{0, 6, 7, 1, 2, 3, 4, 5}
	assert.Equal(t, expectedOrder, wfPriorityList, "expected workflow execution order %v, got %v", expectedOrder, wfPriorityList)
	mu.Unlock()

	// Verify that handle6 and handle7 workflows were dequeued in FIFO order
	// by checking that their StartedAt time is in the correct order (6 is before 7)
	status6, err := handle6.GetStatus()
	require.NoError(t, err, "failed to get status for workflow 6")
	status7, err := handle7.GetStatus()
	require.NoError(t, err, "failed to get status for workflow 7")

	assert.True(t, status6.StartedAt.Before(status7.StartedAt),
		"expected workflow 6 to be dequeued before workflow 7, but got 6 started at %v (created at %v) and 7 started at %v (created at %v)",
		status6.StartedAt, status6.CreatedAt, status7.StartedAt, status7.CreatedAt)

	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after priority queue test")
}

func TestListQueuedWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Simple test workflow that completes immediately
	testWorkflow := func(ctx DBOSContext, input string) (string, error) {
		return "completed-" + input, nil
	}

	// Blocking workflow for testing pending/enqueued workflows
	startEvent := NewEvent()
	blockEvent := NewEvent()
	blockingWorkflow := func(ctx DBOSContext, input string) (string, error) {
		startEvent.Set()
		blockEvent.Wait()
		return "blocked-" + input, nil
	}

	RegisterWorkflow(dbosCtx, testWorkflow)
	RegisterWorkflow(dbosCtx, blockingWorkflow)

	// Create queue for testing
	testQueue1 := NewWorkflowQueue(dbosCtx, "list-test-queue", WithGlobalConcurrency(1))
	testQueue2 := NewWorkflowQueue(dbosCtx, "list-test-queue2", WithGlobalConcurrency(1))

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("WithQueuesOnly", func(t *testing.T) {
		blockEvent.Clear()
		startEvent.Clear()
		// Create a non-queued workflow (completed) - this should NOT appear in WithQueuesOnly results
		nonQueuedHandle, err := RunWorkflow(dbosCtx, testWorkflow, "non-queued-test1")
		require.NoError(t, err, "failed to start non-queued workflow")
		_, err = nonQueuedHandle.GetResult()
		require.NoError(t, err, "failed to complete non-queued workflow")

		// Create queued workflows that will be pending/enqueued
		queuedHandle1, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-1-test1", WithQueue(testQueue1.Name))
		require.NoError(t, err, "failed to start queued workflow 1")

		queuedHandle2, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-2-test1", WithQueue(testQueue1.Name))
		require.NoError(t, err, "failed to start queued workflow 2")

		startEvent.Wait()

		// List workflows with WithQueuesOnly - should only return queued workflows
		queuedWorkflows, err := ListWorkflows(dbosCtx, WithQueuesOnly())
		require.NoError(t, err, "failed to list queued workflows")

		// Verify all returned workflows are in a queue and have pending/enqueued status
		require.Equal(t, 2, len(queuedWorkflows), "expected 2 queued workflows to be returned")
		for _, wf := range queuedWorkflows {
			require.NotEmpty(t, wf.QueueName, "workflow %s should have a queue name", wf.ID)
			require.True(t, wf.Status == WorkflowStatusPending || wf.Status == WorkflowStatusEnqueued,
				"workflow %s status should be PENDING or ENQUEUED, got %s", wf.ID, wf.Status)
			require.True(t, wf.ID == queuedHandle1.GetWorkflowID() || wf.ID == queuedHandle2.GetWorkflowID())
		}

		// Unblock the workflows for cleanup
		blockEvent.Set()
		_, err = queuedHandle1.GetResult()
		require.NoError(t, err, "failed to complete queued workflow 1")
		_, err = queuedHandle2.GetResult()
		require.NoError(t, err, "failed to complete queued workflow 2")
		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "queue entries should be cleaned up")
	})

	t.Run("WithQueuesOnlyAndStatusFilter", func(t *testing.T) {
		blockEvent.Clear()
		startEvent.Clear()
		// Create queued workflow that will complete with SUCCESS status
		completedQueuedHandle, err := RunWorkflow(dbosCtx, testWorkflow, "queued-completed", WithQueue(testQueue2.Name))
		require.NoError(t, err, "failed to start queued workflow for completion")

		// Wait for it to complete
		_, err = completedQueuedHandle.GetResult()
		require.NoError(t, err, "failed to complete queued workflow")

		// Create pending queued workflows that will NOT have SUCCESS status
		pendingHandle1, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-pending-1", WithQueue(testQueue2.Name))
		require.NoError(t, err, "failed to start pending queued workflow 1")

		pendingHandle2, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-pending-2", WithQueue(testQueue2.Name))
		require.NoError(t, err, "failed to start pending queued workflow 2")

		startEvent.Wait()

		// List queued workflows with SUCCESS status filter
		successWorkflows, err := ListWorkflows(dbosCtx, WithQueuesOnly(), WithStatus([]WorkflowStatusType{WorkflowStatusSuccess}), WithQueueName(testQueue2.Name))
		require.NoError(t, err, "failed to list queued workflows with SUCCESS status")

		require.Equal(t, 1, len(successWorkflows), "expected 1 queued workflow with SUCCESS status")
		require.True(t, successWorkflows[0].ID == completedQueuedHandle.GetWorkflowID(), "our queued workflow should be found in the results")

		// Unblock the pending workflows for cleanup
		blockEvent.Set()
		_, err = pendingHandle1.GetResult()
		require.NoError(t, err, "failed to complete pending workflow 1")
		_, err = pendingHandle2.GetResult()
		require.NoError(t, err, "failed to complete pending workflow 2")
		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "queue entries should be cleaned up")
	})
}

func TestPartitionedQueues(t *testing.T) {
	t.Run("PartitionKeyWithoutQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Register a simple workflow
		simpleWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, simpleWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Attempt to enqueue with a partition key but no queue name
		// This should return an error
		_, err = RunWorkflow(dbosCtx, simpleWorkflow, "test-input", WithQueuePartitionKey("partition-1"))
		require.Error(t, err, "expected error when enqueueing with partition key but no queue name")

		// Check that it's the correct error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		// Verify the error is wrapped by newWorkflowExecutionError with WorkflowExecutionError code
		assert.True(t, errors.Is(err, &DBOSError{Code: WorkflowExecutionError}), "expected error to be WorkflowExecutionError")

		// Verify the unwrapped error contains the validation message
		unwrappedErr := errors.Unwrap(dbosErr)
		require.NotNil(t, unwrappedErr, "expected error to have an unwrapped error")
		expectedMsgPart := "partition key provided but queue name is missing"
		assert.Contains(t, unwrappedErr.Error(), expectedMsgPart, "expected unwrapped error message to contain expected part")
	})

	t.Run("PartitionKeyOnNonPartitionedQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Create a non-partitioned queue
		nonPartitionedQueue := NewWorkflowQueue(dbosCtx, "non-partitioned-queue")

		// Register a simple workflow
		simpleWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, simpleWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Attempt to enqueue with a partition key on a non-partitioned queue
		// This should return an error
		_, err = RunWorkflow(dbosCtx, simpleWorkflow, "test-input", WithQueue(nonPartitionedQueue.Name), WithQueuePartitionKey("partition-1"))
		require.Error(t, err, "expected error when enqueueing with partition key on non-partitioned queue")

		// Check that it's the correct error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		// Verify the error is wrapped by newWorkflowExecutionError with WorkflowExecutionError code
		assert.True(t, errors.Is(err, &DBOSError{Code: WorkflowExecutionError}), "expected error to be WorkflowExecutionError")

		// Verify the unwrapped error contains the validation message
		unwrappedErr := errors.Unwrap(dbosErr)
		require.NotNil(t, unwrappedErr, "expected error to have an unwrapped error")
		expectedMsgPart := "is not a partitioned queue, but a partition key was provided"
		assert.Contains(t, unwrappedErr.Error(), expectedMsgPart, "expected unwrapped error message to contain expected part")
	})

	t.Run("PartitionedQueueWithoutPartitionKey", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Create a partitioned queue
		partitionedQueue := NewWorkflowQueue(dbosCtx, "partitioned-queue-required", WithPartitionQueue())

		// Register a simple workflow
		simpleWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, simpleWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Attempt to enqueue to a partitioned queue without a partition key
		// This should return an error
		_, err = RunWorkflow(dbosCtx, simpleWorkflow, "test-input", WithQueue(partitionedQueue.Name))
		require.Error(t, err, "expected error when enqueueing to partitioned queue without partition key")

		// Check that it's the correct error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		// Verify the error is wrapped by newWorkflowExecutionError with WorkflowExecutionError code
		assert.True(t, errors.Is(err, &DBOSError{Code: WorkflowExecutionError}), "expected error to be WorkflowExecutionError")

		// Verify the unwrapped error contains the validation message
		unwrappedErr := errors.Unwrap(dbosErr)
		require.NotNil(t, unwrappedErr, "expected error to have an unwrapped error")
		expectedMsgPart := "has partitions enabled, but no partition key was provided"
		assert.Contains(t, unwrappedErr.Error(), expectedMsgPart, "expected unwrapped error message to contain expected part")
	})

	t.Run("PartitionKeyWithDeduplicationID", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Create a partitioned queue
		partitionedQueue := NewWorkflowQueue(dbosCtx, "partitioned-queue-test", WithPartitionQueue())

		// Register a simple workflow
		simpleWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, simpleWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Attempt to enqueue with both partition key and deduplication ID
		// This should return an error
		_, err = RunWorkflow(dbosCtx, simpleWorkflow, "test-input", WithQueue(partitionedQueue.Name), WithQueuePartitionKey("partition-1"), WithDeduplicationID("dedup-id"))
		require.Error(t, err, "expected error when enqueueing with both partition key and deduplication ID")

		// Check that it's the correct error type
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		// Verify the error is wrapped by newWorkflowExecutionError with WorkflowExecutionError code
		assert.True(t, errors.Is(err, &DBOSError{Code: WorkflowExecutionError}), "expected error to be WorkflowExecutionError")

		// Verify the unwrapped error contains the validation message
		unwrappedErr := errors.Unwrap(dbosErr)
		require.NotNil(t, unwrappedErr, "expected error to have an unwrapped error")
		expectedMsgPart := "partition key and deduplication ID cannot be used together"
		assert.Contains(t, unwrappedErr.Error(), expectedMsgPart, "expected unwrapped error message to contain expected part")
	})

	t.Run("Dequeue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Create a partitioned queue with concurrency limit of 1 per partition
		partitionedQueue := NewWorkflowQueue(dbosCtx, "partitioned-queue", WithPartitionQueue(), WithGlobalConcurrency(1))

		// Create events for blocking workflow on partition 1
		partition1StartEvent := NewEvent()
		partition1BlockEvent := NewEvent()

		// Create blocking workflow for partition 1
		blockingWorkflowP1 := func(ctx DBOSContext, input string) (string, error) {
			partition1StartEvent.Set()
			partition1BlockEvent.Wait()
			return "p1-" + input, nil
		}

		// Create non-blocking workflow (used for both partitions)
		nonBlockingWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}

		RegisterWorkflow(dbosCtx, blockingWorkflowP1)
		RegisterWorkflow(dbosCtx, nonBlockingWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Enqueue a blocking workflow on partition 1
		handleP1Blocked, err := RunWorkflow(dbosCtx, blockingWorkflowP1, "blocked", WithQueue(partitionedQueue.Name), WithQueuePartitionKey("partition-1"))
		require.NoError(t, err, "failed to enqueue blocking workflow on partition 1")

		// Wait for the blocking workflow on partition 1 to start
		partition1StartEvent.Wait()

		// Enqueue a non-blocking workflow on partition 1 - this should be blocked behind the blocking one
		handleP1Normal, err := RunWorkflow(dbosCtx, nonBlockingWorkflow, "p1-normal", WithQueue(partitionedQueue.Name), WithQueuePartitionKey("partition-1"))
		require.NoError(t, err, "failed to enqueue normal workflow on partition 1")

		// Verify the normal workflow is blocked (ENQUEUED status) behind the blocking one
		statusP1Normal, err := handleP1Normal.GetStatus()
		require.NoError(t, err, "failed to get status of normal workflow on partition 1")
		assert.Equal(t, WorkflowStatusEnqueued, statusP1Normal.Status, "expected normal workflow on partition 1 to be ENQUEUED behind the blocking one")

		// Enqueue multiple non-blocking workflows on partition 2 - these should all complete
		// even though partition 1 is blocked, demonstrating partition independence
		numP2Workflows := 3
		handlesP2 := make([]WorkflowHandle[string], numP2Workflows)
		for i := range numP2Workflows {
			handle, err := RunWorkflow(dbosCtx, nonBlockingWorkflow, fmt.Sprintf("p2-workflow-%d", i), WithQueue(partitionedQueue.Name), WithQueuePartitionKey("partition-2"))
			require.NoError(t, err, "failed to enqueue workflow %d on partition 2", i)
			handlesP2[i] = handle
		}

		// Wait for all partition 2 workflows to complete
		for i, handle := range handlesP2 {
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from partition 2 workflow %d", i)
			expectedResult := fmt.Sprintf("p2-workflow-%d", i)
			assert.Equal(t, expectedResult, result, "expected result from partition 2 workflow %d", i)
		}

		// Verify partition 1 blocking workflow is still pending
		statusP1Blocked, err := handleP1Blocked.GetStatus()
		require.NoError(t, err, "failed to get status of blocking workflow on partition 1")
		assert.Equal(t, WorkflowStatusPending, statusP1Blocked.Status, "expected blocking workflow on partition 1 to still be pending")

		// Verify the normal workflow on partition 1 is still enqueued
		statusP1Normal, err = handleP1Normal.GetStatus()
		require.NoError(t, err, "failed to get status of normal workflow on partition 1")
		assert.Equal(t, WorkflowStatusEnqueued, statusP1Normal.Status, "expected normal workflow on partition 1 to still be ENQUEUED")

		// Now unblock partition 1 blocking workflow
		partition1BlockEvent.Set()
		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after partitioned queue test")
	})
}

func TestNewQueueRunner(t *testing.T) {
	t.Run("init queue runner", func(t *testing.T) {
		runner := newQueueRunner(slog.New(slog.NewTextHandler(os.Stdout, nil)))
		require.NotNil(t, runner)
		require.NotNil(t, runner.workflowQueueRegistry)
	})
}

func TestQueuePollingIntervals(t *testing.T) {
	t.Run("queue uses default intervals when not specified", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		queue := NewWorkflowQueue(ctx, "test-queue")
		// Intervals are resolved during creation, so defaults should be applied
		require.Equal(t, _DEFAULT_BASE_POLLING_INTERVAL, queue.basePollingInterval)
		require.Equal(t, _DEFAULT_MAX_POLLING_INTERVAL, queue.maxPollingInterval)
	})

	t.Run("queue uses custom intervals when specified", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: false})

		basePollingInterval := 2 * time.Second
		maxPollingInterval := 60 * time.Second

		queue := NewWorkflowQueue(ctx, "test-queue",
			WithQueueBasePollingInterval(basePollingInterval),
			WithQueueMaxPollingInterval(maxPollingInterval),
		)

		require.Equal(t, basePollingInterval, queue.basePollingInterval)
		require.Equal(t, maxPollingInterval, queue.maxPollingInterval)
	})
}

func TestListenQueues(t *testing.T) {
	t.Run("ListenToSubsetOfQueues", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Register 3 queues
		queue1 := NewWorkflowQueue(dbosCtx, "listen-test-queue-1")
		queue2 := NewWorkflowQueue(dbosCtx, "listen-test-queue-2")
		queue3 := NewWorkflowQueue(dbosCtx, "listen-test-queue-3")

		// Register a simple workflow
		testWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, testWorkflow)

		// Call ListenQueues twice, each time with a list of one queue (so we want to listen to only 2 out of 3 queues)
		ListenQueues(dbosCtx, queue1)
		ListenQueues(dbosCtx, queue2)

		// Launch DBOS
		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Enqueue workflows in all 3 queues
		handle1, err := RunWorkflow(dbosCtx, testWorkflow, "queue1-input", WithQueue(queue1.Name))
		require.NoError(t, err, "failed to enqueue workflow to queue1")

		handle2, err := RunWorkflow(dbosCtx, testWorkflow, "queue2-input", WithQueue(queue2.Name))
		require.NoError(t, err, "failed to enqueue workflow to queue2")

		handle3, err := RunWorkflow(dbosCtx, testWorkflow, "queue3-input", WithQueue(queue3.Name))
		require.NoError(t, err, "failed to enqueue workflow to queue3")

		// Verify that workflows are dequeued and complete in the 2 queues we are actively listening from
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from queue1 workflow")
		assert.Equal(t, "queue1-input", result1, "expected queue1 workflow to complete")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from queue2 workflow")
		assert.Equal(t, "queue2-input", result2, "expected queue2 workflow to complete")

		// Verify that workflow stays in ENQUEUED state for the queue that's not listened from
		// Wait a bit to ensure the queue runner has had time to process
		time.Sleep(2 * time.Second)

		status3, err := handle3.GetStatus()
		require.NoError(t, err, "failed to get status of queue3 workflow")
		assert.Equal(t, WorkflowStatusEnqueued, status3.Status, "expected queue3 workflow to remain ENQUEUED")
	})

	t.Run("InternalQueueIsAlwaysListenedTo", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Register a queue
		queue1 := NewWorkflowQueue(dbosCtx, "listen-internal-test-queue-1")

		// Register a simple workflow
		testWorkflow := func(ctx DBOSContext, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, testWorkflow)

		// Call ListenQueues with only queue1 (internal queue should still be listened to)
		ListenQueues(dbosCtx, queue1)

		// Launch DBOS
		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Run a workflow that completes successfully
		originalHandle, err := RunWorkflow(dbosCtx, testWorkflow, "original-input")
		require.NoError(t, err, "failed to run original workflow")
		originalResult, err := originalHandle.GetResult()
		require.NoError(t, err, "failed to get result from original workflow")
		assert.Equal(t, "original-input", originalResult, "expected original workflow to complete")

		// Fork the workflow - this will enqueue it to the internal queue
		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowID: originalHandle.GetWorkflowID(),
			StartStep:          0,
		})
		require.NoError(t, err, "failed to fork workflow")

		// Verify the forked workflow completes (proving the internal queue is being listened to)
		forkResult, err := forkHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked workflow")
		assert.Equal(t, "original-input", forkResult, "expected forked workflow to complete")

		// Verify the forked workflow was on the internal queue
		forkStatus, err := forkHandle.GetStatus()
		require.NoError(t, err, "failed to get status of forked workflow")
		assert.Equal(t, _DBOS_INTERNAL_QUEUE_NAME, forkStatus.QueueName, "expected forked workflow to be on internal queue")

	})

	t.Run("ListenQueuesAfterLaunchPanics", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		queue1 := NewWorkflowQueue(dbosCtx, "listen-panic-test-queue-1")
		queue2 := NewWorkflowQueue(dbosCtx, "listen-panic-test-queue-2")

		// Launch DBOS first
		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Attempting to call ListenQueues after Launch should panic
		defer func() {
			r := recover()
			assert.NotNil(t, r, "expected panic from ListenQueues after launch but got none")
			assert.Contains(t, fmt.Sprintf("%v", r), "Cannot call ListenQueues after DBOS has launched", "expected panic message to contain specific text")
		}()

		ListenQueues(dbosCtx, queue1, queue2)
		t.Error("expected panic from ListenQueues after launch, but none occurred")
	})
}

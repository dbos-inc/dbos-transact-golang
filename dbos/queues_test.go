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
	dbosCtx := setupDBOS(t, true, true)

	queue := NewWorkflowQueue(dbosCtx, "test-queue")
	dlqEnqueueQueue := NewWorkflowQueue(dbosCtx, "test-successive-enqueue-queue")
	conflictQueue1 := NewWorkflowQueue(dbosCtx, "conflict-queue-1")
	conflictQueue2 := NewWorkflowQueue(dbosCtx, "conflict-queue-2")
	dedupQueue := NewWorkflowQueue(dbosCtx, "test-dedup-queue")

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
		workflowID := "blocking-workflow-test"

		// Enqueue the workflow for the first time
		originalHandle, err := RunWorkflow(dbosCtx, enqueueWorkflowDLQ, "test-input", WithQueue(dlqEnqueueQueue.Name), WithWorkflowID(workflowID))
		require.NoError(t, err)

		// Wait for the workflow to start
		dlqStartEvent.Wait()
		dlqStartEvent.Clear()

		// Try to enqueue the same workflow more times
		for i := range dlqMaxRetries * 2 {
			_, err := RunWorkflow(dbosCtx, enqueueWorkflowDLQ, "test-input", WithQueue(dlqEnqueueQueue.Name), WithWorkflowID(workflowID))
			require.NoError(t, err, "failed to enqueue workflow attempt %d", i+1)
		}

		// Get the status from the original handle and check the attempts counter
		status, err := originalHandle.GetStatus()
		require.NoError(t, err, "failed to get status of original workflow handle")

		// The attempts counter should still be 1 (the original enqueue)
		assert.Equal(t, 1, status.Attempts, "expected attempts to be 1")

		// Check that the workflow hits DLQ after re-running max retries
		handles := make([]WorkflowHandle[any], 0, dlqMaxRetries+1)
		for range dlqMaxRetries {
			recoveryHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows")
			assert.Len(t, recoveryHandles, 1, "expected 1 handle")
			dlqStartEvent.Wait()
			dlqStartEvent.Clear()
			handle := recoveryHandles[0]
			handles = append(handles, handle)
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status of recovered workflow handle")
			assert.Equal(t, WorkflowStatusPending, status.Status, "expected workflow to be in PENDING status after recovery")
		}

		dlqHandle, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		assert.Len(t, dlqHandle, 1, "expected 1 handle in DLQ")
		retries := 0
		for {
			dlqStatus, err := dlqHandle[0].GetStatus()
			require.NoError(t, err, "failed to get status of DLQ workflow handle")
			if dlqStatus.Status != WorkflowStatusMaxRecoveryAttemptsExceeded && retries < 10 {
				time.Sleep(1 * time.Second) // Wait a bit before checking again
				retries++
				continue
			}
			require.NoError(t, err, "failed to get status of DLQ workflow handle")
			assert.Equal(t, WorkflowStatusMaxRecoveryAttemptsExceeded, dlqStatus.Status, "expected workflow to be in DLQ after max retries exceeded")
			handles = append(handles, dlqHandle[0])
			break
		}

		// Check the workflow completes
		dlqCompleteEvent.Set()
		for _, handle := range handles {
			result, err := handle.GetResult()
			fmt.Print(result)
			fmt.Print(err)
			// assert.Equal(t, WorkflowStatusMaxRecoveryAttemptsExceeded, err.Status, "expected workflow to be in DLQ after max retries exceeded")
		}

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
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, ConflictingWorkflowError, dbosErr.Code, "expected error code to be ConflictingWorkflowError")

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
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)
		assert.Equal(t, QueueDeduplicated, dbosErr.Code, "expected error code to be QueueDeduplicated")

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
		registeredQueues := dbosCtx.ListRegisteredQueues()

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
	dbosCtx := setupDBOS(t, true, true)

	recoveryQueue := NewWorkflowQueue(dbosCtx, "recovery-queue")
	var recoveryStepCounter int64
	recoveryStepEvents := make([]*Event, 5) // 5 queued steps
	recoveryEvent := NewEvent()

	recoveryStepWorkflowFunc := func(ctx DBOSContext, i int) (int, error) {
		atomic.AddInt64(&recoveryStepCounter, 1)
		recoveryStepEvents[i].Set()
		recoveryEvent.Wait()
		return i, nil
	}
	RegisterWorkflow(dbosCtx, recoveryStepWorkflowFunc)

	recoveryWorkflowFunc := func(ctx DBOSContext, input string) ([]int, error) {
		handles := make([]WorkflowHandle[int], 0, 5) // 5 queued steps
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

	for i := range recoveryStepEvents {
		recoveryStepEvents[i] = NewEvent()
	}

	wfid := uuid.NewString()

	// Start the workflow. Wait for all steps to start. Verify that they started.
	handle, err := RunWorkflow(dbosCtx, recoveryWorkflowFunc, "", WithWorkflowID(wfid))
	require.NoError(t, err, "failed to start workflow")

	for _, e := range recoveryStepEvents {
		e.Wait()
		e.Clear()
	}

	assert.Equal(t, int64(queuedSteps), atomic.LoadInt64(&recoveryStepCounter), "expected recoveryStepCounter to match queuedSteps")

	// Recover the workflow, then resume it.
	recoveryHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")

	for _, e := range recoveryStepEvents {
		e.Wait()
	}
	recoveryEvent.Set()

	assert.Len(t, recoveryHandles, queuedSteps+1, "expected specific number of recovery handles")

	for _, h := range recoveryHandles {
		if h.GetWorkflowID() == wfid {
			// Root workflow case
			resultAny, err := h.GetResult()
			require.NoError(t, err, "failed to get result from recovered root workflow handle")
			// re-encode and decode the result from []interface{} to []int
			encodedResult, ok := resultAny.([]any)
			require.True(t, ok, "expected result to be a []any")
			jsonBytes, err := json.Marshal(encodedResult)
			require.NoError(t, err, "failed to marshal result to JSON")
			var castedResult []int
			err = json.Unmarshal(jsonBytes, &castedResult)
			require.NoError(t, err, "failed to decode result to []int")
			expectedResult := []int{0, 1, 2, 3, 4}
			assert.Equal(t, expectedResult, castedResult, "expected result %v, got %v", expectedResult, castedResult)
		}
	}

	result, err := handle.GetResult()
	require.NoError(t, err, "failed to get result from original handle")
	expectedResult := []int{0, 1, 2, 3, 4}
	assert.Equal(t, expectedResult, result, "expected result %v, got %v", expectedResult, result)

	assert.Equal(t, int64(queuedSteps*2), atomic.LoadInt64(&recoveryStepCounter), "expected recoveryStepCounter to be %d", queuedSteps*2)

	// Rerun the workflow. Because each step is complete, none should start again.
	rerunHandle, err := RunWorkflow(dbosCtx, recoveryWorkflowFunc, "test-input", WithWorkflowID(wfid))
	require.NoError(t, err, "failed to rerun workflow")
	rerunResult, err := rerunHandle.GetResult()
	require.NoError(t, err, "failed to get result from rerun handle")
	assert.Equal(t, expectedResult, rerunResult, "expected result %v, got %v", expectedResult, rerunResult)

	assert.Equal(t, int64(queuedSteps*2), atomic.LoadInt64(&recoveryStepCounter), "expected recoveryStepCounter to remain %d", queuedSteps*2)

	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
}

// Note: we could update this test to have the same logic than TestWorkerConcurrency
func TestGlobalConcurrency(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

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
	dbosCtx1 := setupDBOS(t, true, true)
	os.Setenv("DBOS__VMID", "worker2")
	dbosCtx2 := setupDBOS(t, false, false) // Don't check for leaks because t.Cancel is called in LIFO order. Also don't reset the DB here.
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
			queueName: workerConcurrencyQueue.Name,
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

func TestWorkerConcurrencyXRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	workerConcurrencyRecoveryQueue := NewWorkflowQueue(dbosCtx, "test-worker-concurrency-recovery-queue", WithWorkerConcurrency(1))
	workerConcurrencyRecoveryStartEvent1 := NewEvent()
	workerConcurrencyRecoveryStartEvent2 := NewEvent()
	workerConcurrencyRecoveryCompleteEvent1 := NewEvent()
	workerConcurrencyRecoveryCompleteEvent2 := NewEvent()

	// Create workflows with dbosContext
	workerConcurrencyRecoveryBlockingWf1 := func(ctx DBOSContext, input string) (string, error) {
		workerConcurrencyRecoveryStartEvent1.Set()
		workerConcurrencyRecoveryCompleteEvent1.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, workerConcurrencyRecoveryBlockingWf1)
	workerConcurrencyRecoveryBlockingWf2 := func(ctx DBOSContext, input string) (string, error) {
		workerConcurrencyRecoveryStartEvent2.Set()
		workerConcurrencyRecoveryCompleteEvent2.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, workerConcurrencyRecoveryBlockingWf2)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	// Enqueue two workflows on a queue with worker concurrency = 1
	handle1, err := RunWorkflow(dbosCtx, workerConcurrencyRecoveryBlockingWf1, "workflow1", WithQueue(workerConcurrencyRecoveryQueue.Name), WithWorkflowID("worker-cc-x-recovery-wf-1"))
	require.NoError(t, err)
	handle2, err := RunWorkflow(dbosCtx, workerConcurrencyRecoveryBlockingWf2, "workflow2", WithQueue(workerConcurrencyRecoveryQueue.Name), WithWorkflowID("worker-cc-x-recovery-wf-2"))
	require.NoError(t, err)

	// Start the first workflow and wait for it to start
	workerConcurrencyRecoveryStartEvent1.Wait()
	workerConcurrencyRecoveryStartEvent1.Clear()
	// Wait for a few seconds to let the queue runner loop
	time.Sleep(2 * time.Second)

	// Ensure the 2nd workflow is still ENQUEUED
	status2, err := handle2.GetStatus()
	require.NoError(t, err, "failed to get status of workflow2")
	assert.Equal(t, WorkflowStatusEnqueued, status2.Status, "expected workflow2 to be in ENQUEUED status")

	// Verify workflow2 hasn't started yet
	assert.False(t, workerConcurrencyRecoveryStartEvent2.IsSet, "expected workflow2 to not start while workflow1 is running")

	// Now, manually call the recoverPendingWorkflows method
	recoveryHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")

	// You should get 1 handle associated with the first workflow
	assert.Len(t, recoveryHandles, 1, "expected 1 recovery handle")

	// The handle status should tell you the workflow is ENQUEUED
	recoveredHandle := recoveryHandles[0]
	assert.Equal(t, "worker-cc-x-recovery-wf-1", recoveredHandle.GetWorkflowID(), "expected recovered handle to be for workflow1")
	wf1Status, err := recoveredHandle.GetStatus()
	require.NoError(t, err, "failed to get status of recovered workflow1")
	assert.Equal(t, WorkflowStatusEnqueued, wf1Status.Status, "expected recovered handle to be in ENQUEUED status")

	// The 1 first workflow should have been dequeued again (FIFO ordering) and the 2nd workflow should still be enqueued
	workerConcurrencyRecoveryStartEvent1.Wait()
	status2, err = handle2.GetStatus()
	require.NoError(t, err, "failed to get status of workflow2")
	assert.Equal(t, WorkflowStatusEnqueued, status2.Status, "expected workflow2 to still be in ENQUEUED status")

	// Let the 1st workflow complete and let the 2nd workflow start and complete
	workerConcurrencyRecoveryCompleteEvent1.Set()
	workerConcurrencyRecoveryStartEvent2.Wait()
	workerConcurrencyRecoveryCompleteEvent2.Set()

	// Get result from first workflow
	result1, err := handle1.GetResult()
	require.NoError(t, err, "failed to get result from workflow1")
	assert.Equal(t, "workflow1", result1, "expected result from workflow1 to be 'workflow1'")

	// Get result from second workflow
	result2, err := handle2.GetResult()
	require.NoError(t, err, "failed to get result from workflow2")
	assert.Equal(t, "workflow2", result2, "expected result from workflow2 to be 'workflow2'")

	// Ensure queueEntriesAreCleanedUp is set to true
	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after worker concurrency recovery test")
}

func rateLimiterTestWorkflow(ctx DBOSContext, _ string) (time.Time, error) {
	return time.Now(), nil // Return current time
}

func TestQueueRateLimiter(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

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
	dbosCtx := setupDBOS(t, true, true)

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
	dbosCtx := setupDBOS(t, true, true)

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
	dbosCtx := setupDBOS(t, true, true)

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
		dbosCtx := setupDBOS(t, true, true)

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
		dbosCtx := setupDBOS(t, true, true)

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
		dbosCtx := setupDBOS(t, true, true)

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
		dbosCtx := setupDBOS(t, true, true)

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
		dbosCtx := setupDBOS(t, true, true)

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
	t.Run("init queue with default interval", func(t *testing.T) {
		runner := newQueueRunner(slog.New(slog.NewTextHandler(os.Stdout, nil)), QueueRunnerConfig{})
		require.Equal(t, _DEFAULT_BASE_INTERVAL, runner.baseInterval)
		require.Equal(t, _DEFAULT_MAX_INTERVAL, runner.maxInterval)
		require.Equal(t, _DEFAULT_MIN_INTERVAL, runner.minInterval)
	})

	t.Run("init queue with custom interval", func(t *testing.T) {
		runner := newQueueRunner(slog.New(slog.NewTextHandler(os.Stdout, nil)), QueueRunnerConfig{
			BaseInterval: 1,
			MinInterval:  2,
			MaxInterval:  3,
		})
		require.Equal(t, float64(1), runner.baseInterval)
		require.Equal(t, float64(2), runner.minInterval)
		require.Equal(t, float64(3), runner.maxInterval)

	})
}

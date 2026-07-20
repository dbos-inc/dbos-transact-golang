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

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// registerWFQ / retrieveWFQ / listWFQ are test helpers that call the public
// queue API (which returns the Queue interface) and unwrap the concrete
// *workflowQueue, so existing tests can keep reading struct fields directly.
func registerWFQ(ctx Context, name string, options ...QueueOption) (*workflowQueue, error) {
	q, err := RegisterQueue(ctx, name, options...)
	if err != nil {
		return nil, err
	}
	return q.(*workflowQueue), nil
}

func intPtr(i int) *int { return &i }

func retrieveWFQ(ctx Context, name string) (*workflowQueue, error) {
	q, err := RetrieveQueue(ctx, name)
	if err != nil {
		return nil, err
	}
	return q.(*workflowQueue), nil
}

func listWFQ(ctx Context) ([]workflowQueue, error) {
	qs, err := ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]workflowQueue, len(qs))
	for i, q := range qs {
		out[i] = *q.(*workflowQueue)
	}
	return out, nil
}

func queueWorkflow(ctx Context, input string) (string, error) {
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

	// Database-backed queues are registered after Launch (below). Declare the
	// handles up front so the workflow closures registered before launch can
	// reference their names; they are assigned before any closure runs.
	var (
		queue           *workflowQueue
		dlqEnqueueQueue *workflowQueue
		conflictQueue1  *workflowQueue
		conflictQueue2  *workflowQueue
		dedupQueue      *workflowQueue
	)

	dlqStartEvent := NewEvent()
	dlqCompleteEvent := NewEvent()
	dlqMaxRetries := 10

	// Register workflows with dbosContext
	RegisterWorkflow(dbosCtx, queueWorkflow)

	// Custom name workflows
	queueWorkflowCustomName := func(ctx Context, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(dbosCtx, queueWorkflowCustomName, WithWorkflowName("custom-name"))

	queueWorkflowCustomNameEnqueingAnotherCustomNameWorkflow := func(ctx Context, input string) (string, error) {
		// Start a child workflow
		childHandle, err := RunWorkflow(ctx, queueWorkflowCustomName, input+"-enqueued", WithQueue(queue))
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
	childWorkflow := func(ctx Context, var1 string) (string, error) {
		if dedupWorkflowEvent != nil {
			dedupWorkflowEvent.Wait()
		}
		return var1 + "-c", nil
	}
	RegisterWorkflow(dbosCtx, childWorkflow)

	testWorkflow := func(ctx Context, var1 string) (string, error) {
		// Make sure the child workflow is not blocked by the same deduplication ID
		childHandle, err := RunWorkflow(ctx, childWorkflow, var1, WithQueue(dedupQueue))
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

	// Parent workflow that spawns a singleton child via the return-existing policy.
	// Multiple parents using the same dedup ID must all attach to the same child.
	const parentSingletonDedupID = "parent_singleton_child_dedup"
	parentSpawnsSingletonChild := func(ctx Context, childInput string) (string, error) {
		h, err := RunWorkflow(ctx, childWorkflow, childInput,
			WithQueue(dedupQueue),
			WithDeduplicationID(parentSingletonDedupID),
			WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		if err != nil {
			return "", fmt.Errorf("failed to spawn singleton child: %v", err)
		}
		return h.GetResult()
	}
	RegisterWorkflow(dbosCtx, parentSpawnsSingletonChild)

	// Create workflow with child that can call the main workflow
	queueWorkflowWithChild := func(ctx Context, input string) (string, error) {
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
	queueWorkflowThatEnqueues := func(ctx Context, input string) (string, error) {
		// Enqueue another workflow to the same queue
		enqueuedHandle, err := RunWorkflow(ctx, queueWorkflow, input+"-enqueued", WithQueue(queue))
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

	enqueueWorkflowDLQ := func(ctx Context, input string) (string, error) {
		dlqStartEvent.Set()
		dlqCompleteEvent.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, enqueueWorkflowDLQ, WithMaxRecoveryAttempts(dlqMaxRetries))

	// Create a workflow that enqueues another workflow to test step tracking
	workflowEnqueuesAnother := func(ctx Context, input string) (string, error) {
		// Enqueue a child workflow
		childHandle, err := RunWorkflow(ctx, queueWorkflow, input+"-child", WithQueue(queue))
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
	simpleWorkflow := func(ctx Context, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(dbosCtx, simpleWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err)

	// Register the database-backed queues now that DBOS has launched.
	queue, err = registerWFQ(dbosCtx, "test-queue", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)
	dlqEnqueueQueue, err = registerWFQ(dbosCtx, "test-successive-enqueue-queue", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)
	conflictQueue1, err = registerWFQ(dbosCtx, "conflict-queue-1", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)
	conflictQueue2, err = registerWFQ(dbosCtx, "conflict-queue-2", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)
	dedupQueue, err = registerWFQ(dbosCtx, "test-dedup-queue", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)

	t.Run("EnqueueWorkflow", func(t *testing.T) {
		before := time.Now()
		handle, err := RunWorkflow(dbosCtx, queueWorkflow, "test-input", WithQueue(queue))
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

		// Dequeue time filters: an enqueued workflow gets started_at set when it
		// is dequeued, so WithFilterDequeuedAfter/Before should bracket it.
		wfID := handle.GetWorkflowID()
		after := time.Now()
		listed, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(wfID))
		require.NoError(t, err)
		require.Len(t, listed, 1)
		require.False(t, listed[0].StartedAt.IsZero(), "dequeued workflow should have StartedAt set")

		inRange, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(wfID), WithFilterDequeuedAfter(before.Add(-time.Second)))
		require.NoError(t, err)
		assert.Len(t, inRange, 1, "WithFilterDequeuedAfter before enqueue should include the workflow")
		afterEmpty, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(wfID), WithFilterDequeuedAfter(after.Add(time.Hour)))
		require.NoError(t, err)
		assert.Len(t, afterEmpty, 0, "WithFilterDequeuedAfter in the future should exclude the workflow")
		beforeRange, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(wfID), WithFilterDequeuedBefore(after.Add(time.Second)))
		require.NoError(t, err)
		assert.Len(t, beforeRange, 1, "WithFilterDequeuedBefore after completion should include the workflow")
		beforeEmpty, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(wfID), WithFilterDequeuedBefore(before.Add(-time.Hour)))
		require.NoError(t, err)
		assert.Len(t, beforeEmpty, 0, "WithFilterDequeuedBefore in the past should exclude the workflow")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueueWorkflowCustomName", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflowCustomName, "test-input", WithQueue(queue))
		require.NoError(t, err)

		_, ok := handle.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle to be of type workflowPollingHandle, got %T", handle)

		res, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "test-input", res)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after global concurrency test")
	})

	t.Run("EnqueuedWorkflowStartsChildWorkflow", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, queueWorkflowWithChild, "test-input", WithQueue(queue))
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
		handle, err := RunWorkflow(dbosCtx, queueWorkflowThatEnqueues, "test-input", WithQueue(queue))
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
		handle, err := RunWorkflow(dbosCtx, queueWorkflowCustomNameEnqueingAnotherCustomNameWorkflow, "test-input", WithQueue(queue))
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
		handle, err := RunWorkflow(dbosCtx, workflowEnqueuesAnother, "test-input", WithQueue(queue))
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

	t.Run("QueueWorkflowDLQ", func(t *testing.T) {
		workflowID := "queue-dlq-workflow-test"
		dlqCompleteEvent.Clear()

		// Enqueue once; workflow will run and block on dlqCompleteEvent
		originalHandle, err := RunWorkflow(dbosCtx, enqueueWorkflowDLQ, "test-input", WithQueue(dlqEnqueueQueue), WithWorkflowID(workflowID))
		require.NoError(t, err)

		// Wait for the workflow to start (blocked on dlqCompleteEvent)
		dlqStartEvent.Wait()
		dlqStartEvent.Clear()

		// Re-enqueue the same workflow ID many times; should not trigger DLQ (attempts stay 1)
		for i := range dlqMaxRetries * 2 {
			_, err := RunWorkflow(dbosCtx, enqueueWorkflowDLQ, "test-input", WithQueue(dlqEnqueueQueue), WithWorkflowID(workflowID))
			require.NoError(t, err, "failed to enqueue workflow attempt %d", i+1)
		}

		// ListWorkflows for this queue should show a single pending workflow
		workflows, err := ListWorkflows(dbosCtx, WithFilterQueueName(dlqEnqueueQueue.Name))
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

	t.Run("UnexpectedWorkflowOnDifferentQueues", func(t *testing.T) {
		workflowID := "conflicting-workflow-id"

		// Enqueue the same workflow ID on the first queue
		handle, err := RunWorkflow(dbosCtx, queueWorkflow, "test-input-1", WithQueue(conflictQueue1), WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to enqueue workflow on first queue")

		// Get the result from the first workflow to ensure it completes
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		assert.Equal(t, "test-input-1", result, "expected 'test-input-1'")

		// Now try to enqueue the same workflow ID on a different queue
		// This should trigger a ErrorCodeUnexpectedWorkflow
		_, err = RunWorkflow(dbosCtx, queueWorkflow, "test-input-2", WithQueue(conflictQueue2), WithWorkflowID(workflowID))
		require.Error(t, err, "expected ErrorCodeUnexpectedWorkflow when enqueueing same workflow ID on different queue, but got none")

		// Check that it's the correct error type
		require.True(t, errors.Is(err, ErrUnexpectedWorkflow), "expected error to be ErrorCodeUnexpectedWorkflow, got %T", err)

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
		handle1, err := RunWorkflow(dbosCtx, testWorkflow, "abc", WithQueue(dedupQueue), WithWorkflowID(wfid), WithDeduplicationID(dedupID))
		require.NoError(t, err, "failed to enqueue first workflow with deduplication ID")

		// Enqueue the same workflow with a different deduplication ID should be fine
		anotherHandle, err := RunWorkflow(dbosCtx, testWorkflow, "ghi", WithQueue(dedupQueue), WithDeduplicationID("my_other_dedup_id"))
		require.NoError(t, err, "failed to enqueue workflow with different deduplication ID")

		// Enqueue a workflow without deduplication ID should be fine
		nodedupHandle1, err := RunWorkflow(dbosCtx, testWorkflow, "jkl", WithQueue(dedupQueue))
		require.NoError(t, err, "failed to enqueue workflow without deduplication ID")

		// Enqueued multiple times without deduplication ID but with different inputs should be fine, but get the result of the first one
		nodedupHandle2, err := RunWorkflow(dbosCtx, testWorkflow, "mno", WithQueue(dedupQueue), WithWorkflowID(wfid))
		require.NoError(t, err, "failed to enqueue workflow with same workflow ID")

		// Enqueue the same workflow with the same deduplication ID should raise an exception.
		// Pass DeduplicationPolicyReject explicitly to confirm it matches the default behavior.
		wfid2 := uuid.NewString()
		_, err = RunWorkflow(dbosCtx, testWorkflow, "def", WithQueue(dedupQueue), WithWorkflowID(wfid2), WithDeduplicationID(dedupID), WithDeduplicationPolicy(DeduplicationPolicyReject))
		require.Error(t, err, "expected error when enqueueing workflow with same deduplication ID")

		// Check that it's the correct error type and message
		require.True(t, errors.Is(err, ErrQueueDeduplicated), "expected error to be ErrorCodeQueueDeduplicated, got %T", err)

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
		handle2, err := RunWorkflow(dbosCtx, testWorkflow, "def", WithQueue(dedupQueue), WithWorkflowID(wfid2), WithDeduplicationID(dedupID))
		require.NoError(t, err, "failed to enqueue workflow with same dedup ID after completion")
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second workflow with same dedup ID")
		assert.Equal(t, "def-c-p", result2, "expected second workflow result to be 'def-c-p'")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after deduplication test")
	})

	t.Run("QueueDeduplicationCancelAndRestart", func(t *testing.T) {
		// Verify that cancelling a workflow with a dedup ID clears the dedup constraint,
		// allowing a new workflow with the same dedup ID to be enqueued.
		workflowEvent := NewEvent()
		dedupWorkflowEvent = workflowEvent
		defer func() {
			dedupWorkflowEvent = nil
		}()

		dedupID := "cancel_dedup_id"
		wfid := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, testWorkflow, "cancel-me", WithQueue(dedupQueue), WithWorkflowID(wfid), WithDeduplicationID(dedupID))
		require.NoError(t, err, "failed to enqueue workflow with dedup ID")

		// Cancel the workflow before it completes
		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to cancel workflow")

		// Unblock any running workflow code
		workflowEvent.Set()

		// Wait for the workflow to reach a terminal state, then verify it was cancelled
		_, _ = handle.GetResult()
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get status of cancelled workflow")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be CANCELLED")

		// Enqueue a new workflow with the same dedup ID — should succeed because cancel cleared the constraint
		wfid2 := uuid.NewString()
		handle2, err := RunWorkflow(dbosCtx, testWorkflow, "restarted", WithQueue(dedupQueue), WithWorkflowID(wfid2), WithDeduplicationID(dedupID))
		require.NoError(t, err, "failed to enqueue workflow with same dedup ID after cancel")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from re-enqueued workflow")
		assert.Equal(t, "restarted-c-p", result2, "expected re-enqueued workflow to complete with correct result")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after cancel-and-restart test")
	})

	t.Run("QueueDeduplicationReturnExisting", func(t *testing.T) {
		// With DeduplicationPolicyReturnExisting, a collision returns a handle to the existing
		// workflow instead of erroring (singleton semantics).
		workflowEvent := NewEvent()
		dedupWorkflowEvent = workflowEvent
		defer func() {
			dedupWorkflowEvent = nil
		}()

		dedupID := "return_existing_dedup_id"
		wfid1 := uuid.NewString()
		handle1, err := RunWorkflow(dbosCtx, testWorkflow, "first", WithQueue(dedupQueue), WithWorkflowID(wfid1), WithDeduplicationID(dedupID), WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		require.NoError(t, err, "failed to enqueue first workflow")

		// Second enqueue with the same dedup ID returns a handle to the existing workflow instead of erroring
		handle2, err := RunWorkflow(dbosCtx, testWorkflow, "second", WithQueue(dedupQueue), WithDeduplicationID(dedupID), WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		require.NoError(t, err, "expected return-existing policy to not error on collision")
		assert.Equal(t, wfid1, handle2.GetWorkflowID(), "expected handle2 to point to the existing workflow")

		// Unblock and verify both handles resolve to the first workflow's result; the second input is discarded
		workflowEvent.Set()
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		assert.Equal(t, "first-c-p", result1)
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from existing-handle workflow")
		assert.Equal(t, "first-c-p", result2)

		// After the slot clears on completion, a new enqueue starts a fresh workflow
		wfid3 := uuid.NewString()
		handle3, err := RunWorkflow(dbosCtx, testWorkflow, "third", WithQueue(dedupQueue), WithWorkflowID(wfid3), WithDeduplicationID(dedupID), WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		require.NoError(t, err, "failed to enqueue after slot cleared")
		assert.Equal(t, wfid3, handle3.GetWorkflowID(), "expected a fresh workflow after the dedup slot cleared")
		result3, err := handle3.GetResult()
		require.NoError(t, err, "failed to get result from fresh workflow")
		assert.Equal(t, "third-c-p", result3)

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after return-existing test")
	})

	t.Run("QueueDeduplicationReturnExistingMultiParent", func(t *testing.T) {
		// Two parent workflows each spawn a singleton child with the same dedup ID + return-existing.
		// Both must attach to the same child, return that child's output (not their own input), and
		// record the attach in their operation outputs so replay resolves to the same child.
		workflowEvent := NewEvent()
		dedupWorkflowEvent = workflowEvent
		defer func() {
			dedupWorkflowEvent = nil
		}()

		// A directly-enqueued child holds the dedup slot
		firstChild, err := RunWorkflow(dbosCtx, childWorkflow, "first", WithQueue(dedupQueue), WithDeduplicationID(parentSingletonDedupID), WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		require.NoError(t, err, "failed to enqueue first child")
		childID := firstChild.GetWorkflowID()

		// Two parents each spawn a singleton child with the same dedup ID; both attach to firstChild
		parentA, err := RunWorkflow(dbosCtx, parentSpawnsSingletonChild, "second")
		require.NoError(t, err, "failed to start parent A")
		parentB, err := RunWorkflow(dbosCtx, parentSpawnsSingletonChild, "third")
		require.NoError(t, err, "failed to start parent B")

		// Both parents must attach to the still-running firstChild before we release it.
		attachedToChild := func(parentID string) bool {
			steps, err := GetWorkflowSteps(dbosCtx, parentID)
			if err != nil {
				return false
			}
			for _, s := range steps {
				if s.ChildWorkflowID == childID && s.StepID == 0 {
					return true
				}
			}
			return false
		}
		require.Eventually(t, func() bool {
			return attachedToChild(parentA.GetWorkflowID()) && attachedToChild(parentB.GetWorkflowID())
		}, 10*time.Second, 5*time.Millisecond, "both parents should attach to the shared child before it is released")

		// Unblock the child; everyone resolves to the child's output regardless of the parents' inputs
		workflowEvent.Set()
		childResult, err := firstChild.GetResult()
		require.NoError(t, err, "failed to get result from first child")
		assert.Equal(t, "first-c", childResult)
		resultA, err := parentA.GetResult()
		require.NoError(t, err, "failed to get result from parent A")
		assert.Equal(t, "first-c", resultA, "parent A should return the existing child's output")
		resultB, err := parentB.GetResult()
		require.NoError(t, err, "failed to get result from parent B")
		assert.Equal(t, "first-c", resultB, "parent B should return the existing child's output")

		// Each parent recorded the attach: the spawn step (step 0) maps to the shared child workflow ID
		for _, p := range []WorkflowHandle[string]{parentA, parentB} {
			steps, err := GetWorkflowSteps(dbosCtx, p.GetWorkflowID())
			require.NoError(t, err, "failed to get steps for parent %s", p.GetWorkflowID())
			attachedAtStep0 := false
			for _, s := range steps {
				if s.ChildWorkflowID == childID && s.StepID == 0 {
					attachedAtStep0 = true
				}
			}
			assert.True(t, attachedAtStep0, "parent %s should record a step-0 attach to the shared child %s", p.GetWorkflowID(), childID)
		}

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after multi-parent test")
	})

	t.Run("ReturnExistingValidation", func(t *testing.T) {
		// Missing queue name
		_, err := RunWorkflow(dbosCtx, testWorkflow, "x", WithDeduplicationID("id"), WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		require.Error(t, err, "expected error when queue name is missing")
		assert.Contains(t, err.Error(), "requires a queue name")

		// Missing deduplication ID
		_, err = RunWorkflow(dbosCtx, testWorkflow, "x", WithQueue(dedupQueue), WithDeduplicationPolicy(DeduplicationPolicyReturnExisting))
		require.Error(t, err, "expected error when deduplication ID is missing")
		assert.Contains(t, err.Error(), "requires a deduplication ID")
	})

	t.Run("ListQueues", func(t *testing.T) {
		// The test queues are database-backed, so they appear in ListQueues. The
		// internal queue is in-process only and therefore not included here.
		dbQueues, err := listWFQ(dbosCtx)
		require.NoError(t, err, "failed to list database-backed queues")

		expectedQueueNames := map[string]bool{
			queue.Name:           true,
			dlqEnqueueQueue.Name: true,
			conflictQueue1.Name:  true,
			conflictQueue2.Name:  true,
			dedupQueue.Name:      true,
		}

		assert.Equal(t, len(expectedQueueNames), len(dbQueues), "expected %d database-backed queues, got %d", len(expectedQueueNames), len(dbQueues))

		actualQueueNames := make(map[string]bool)
		for _, q := range dbQueues {
			actualQueueNames[q.Name] = true
			assert.True(t, expectedQueueNames[q.Name], "unexpected queue found: %s", q.Name)
		}
		for queueName := range expectedQueueNames {
			assert.True(t, actualQueueNames[queueName], "expected queue %s not found in database-backed queues", queueName)
		}

		// Verify specific queue properties for known queues
		for _, q := range dbQueues {
			switch q.Name {
			case queue.Name:
				assert.Nil(t, q.WorkerConcurrency, "expected queue to have nil WorkerConcurrency")
				assert.Nil(t, q.GlobalConcurrency, "expected queue to have nil GlobalConcurrency")
				assert.False(t, q.PriorityEnabled, "expected queue to have PriorityEnabled=false")
			case dedupQueue.Name:
				assert.Nil(t, q.WorkerConcurrency, "expected dedup queue to have nil WorkerConcurrency")
			}
		}
	})
}

func TestQueueRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	var recoveryQueue *workflowQueue // database-backed; registered after Launch
	var recoveryStepCounter int64

	recoveryStepWorkflowFunc := func(ctx Context, i int) (int, error) {
		atomic.AddInt64(&recoveryStepCounter, 1)
		return i, nil
	}
	RegisterWorkflow(dbosCtx, recoveryStepWorkflowFunc)

	recoveryWorkflowFunc := func(ctx Context, input string) ([]int, error) {
		handles := make([]WorkflowHandle[int], 0, 5)
		for i := range 5 {
			handle, err := RunWorkflow(ctx, recoveryStepWorkflowFunc, i, WithQueue(recoveryQueue))
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

	recoveryQueue, err = registerWFQ(dbosCtx, "recovery-queue")
	require.NoError(t, err)

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
	workflowsOnQueue, err := ListWorkflows(dbosCtx, WithFilterQueueName(recoveryQueue.Name))
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

	var globalConcurrencyQueue *workflowQueue // database-backed; registered after Launch
	workflowEvent1 := NewEvent()
	workflowEvent2 := NewEvent()
	workflowDoneEvent := NewEvent()

	// Create workflow with dbosContext
	globalConcurrencyWorkflowFunc := func(ctx Context, input string) (string, error) {
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

	globalConcurrencyQueue, err = registerWFQ(dbosCtx, "test-global-concurrency-queue", WithGlobalConcurrency(1))
	require.NoError(t, err)

	// Enqueue two workflows
	handle1, err := RunWorkflow(dbosCtx, globalConcurrencyWorkflowFunc, "workflow1", WithQueue(globalConcurrencyQueue))
	require.NoError(t, err, "failed to enqueue workflow1")

	handle2, err := RunWorkflow(dbosCtx, globalConcurrencyWorkflowFunc, "workflow2", WithQueue(globalConcurrencyQueue))
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

// TestVersionlessDequeueRequiresLatestVersion verifies that a worker only dequeues
// version-less (application_version IS NULL) workflows when it is running the latest
// registered application version. A worker whose version is not the latest still
// dequeues workflows tagged with its own version, but leaves version-less ones
// ENQUEUED until it becomes the latest version again (e.g. after a rolling deploy).
func TestVersionlessDequeueRequiresLatestVersion(t *testing.T) {
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	versionlessWorkflow := func(_ Context, input string) (string, error) {
		return input, nil
	}
	RegisterWorkflow(serverCtx, versionlessWorkflow, WithWorkflowName("VersionlessWorkflow"))

	require.NoError(t, Launch(serverCtx))

	// Database-backed queue registered after Launch, with a fast poll so the test is quick.
	queue, err := registerWFQ(serverCtx, "versionless-dequeue-queue", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)

	currentVersion := serverCtx.GetApplicationVersion()

	// A client is used to enqueue a version-less (NULL application_version) workflow.
	databaseURL := backendDatabaseURL(t)
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: databaseURL})
	require.NoError(t, err)
	t.Cleanup(func() {
		if client != nil {
			client.Shutdown(client, 30 * time.Second)
		}
	})

	// Register a newer application version and make it the latest, so this worker is no
	// longer running the latest version (simulating a rolling deploy).
	sysdb := serverCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
	require.NoError(t, sysdb.CreateApplicationVersion(context.Background(), "versionless-newer"))
	require.NoError(t, sysdb.UpdateApplicationVersionTimestamp(context.Background(), "versionless-newer", time.Now().Add(time.Hour).UnixMilli()))

	// Enqueue a version-less workflow: an empty application version is persisted as NULL.
	versionlessHandle, err := Enqueue[string](client, queue.Name, "VersionlessWorkflow", "versionless",
		WithEnqueueApplicationVersion(""))
	require.NoError(t, err)

	// Enqueue a workflow tagged with this worker's current version (the in-process default).
	taggedHandle, err := RunWorkflow(serverCtx, versionlessWorkflow, "tagged", WithQueue(queue))
	require.NoError(t, err)

	// The version-tagged workflow completes: the worker's version matches it even though the
	// worker is not running the latest version.
	taggedResult, err := taggedHandle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "tagged", taggedResult)

	// The version-less workflow must NOT be dequeued while this worker is not the latest.
	// Give the poller time to (wrongly) pick it up, then confirm it is still ENQUEUED.
	time.Sleep(2 * time.Second)
	status, err := versionlessHandle.GetStatus()
	require.NoError(t, err)
	assert.Equal(t, WorkflowStatusEnqueued, status.Status,
		"version-less workflow must stay ENQUEUED while this worker is not the latest version")

	// Promote this worker's version back to the latest.
	require.NoError(t, sysdb.UpdateApplicationVersionTimestamp(context.Background(), currentVersion, time.Now().Add(2*time.Hour).UnixMilli()))

	// Now that this worker is the latest again, the version-less workflow is dequeued and completes.
	versionlessResult, err := versionlessHandle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "versionless", versionlessResult)

	require.True(t, queueEntriesAreCleanedUp(serverCtx), "expected queue entries to be cleaned up after versionless dequeue test")
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

	var workerConcurrencyQueue *workflowQueue // database-backed; registered after Launch
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
		workflows, err := dbosCtx1.(*dbosContext).systemDB.ListWorkflows(context.Background(), sysdb.ListWorkflowsDBInput{
			QueueName: []string{workerConcurrencyQueue.Name},
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
	blockingWfFunc := func(ctx Context, i int) (int, error) {
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

	// Register the shared database-backed queue from both executors (the second
	// upserts the same configuration).
	workerConcurrencyQueue, err = registerWFQ(dbosCtx1, "test-worker-concurrency-queue", WithWorkerConcurrency(1))
	require.NoError(t, err)
	_, err = registerWFQ(dbosCtx2, "test-worker-concurrency-queue", WithWorkerConcurrency(1))
	require.NoError(t, err)

	// First enqueue four blocking workflows
	handle1, err := RunWorkflow(dbosCtx1, blockingWfFunc, 0, WithQueue(workerConcurrencyQueue), WithWorkflowID("worker-cc-wf-1"))
	require.NoError(t, err)
	handle2, err := RunWorkflow(dbosCtx1, blockingWfFunc, 1, WithQueue(workerConcurrencyQueue), WithWorkflowID("worker-cc-wf-2"))
	require.NoError(t, err)
	_, err = RunWorkflow(dbosCtx1, blockingWfFunc, 2, WithQueue(workerConcurrencyQueue), WithWorkflowID("worker-cc-wf-3"))
	require.NoError(t, err)
	_, err = RunWorkflow(dbosCtx1, blockingWfFunc, 3, WithQueue(workerConcurrencyQueue), WithWorkflowID("worker-cc-wf-4"))
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

func rateLimiterTestWorkflow(ctx Context, _ string) (time.Time, error) {
	return time.Now(), nil // Return current time
}

func TestQueueRateLimiter(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create workflow with dbosContext
	RegisterWorkflow(dbosCtx, rateLimiterTestWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	rateLimiterQueue, err := registerWFQ(dbosCtx, "test-rate-limiter-queue", WithRateLimiter(&RateLimiter{Limit: 5, Period: time.Duration(1800 * time.Millisecond)}))
	require.NoError(t, err)

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
		handle, err := RunWorkflow(dbosCtx, rateLimiterTestWorkflow, "", WithQueue(rateLimiterQueue))
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

	var timeoutQueue *workflowQueue // database-backed; registered after Launch

	queuedWaitForCancelWorkflow := func(ctx Context, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded), "workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, queuedWaitForCancelWorkflow)

	enqueuedWorkflowEnqueuesATimeoutWorkflow := func(ctx Context, childWorkflowID string) (string, error) {
		// This workflow will enqueue a workflow that waits indefinitely until it is cancelled
		handle, err := RunWorkflow(ctx, queuedWaitForCancelWorkflow, "enqueued-wait-for-cancel", WithQueue(timeoutQueue), WithWorkflowID(childWorkflowID))
		require.NoError(t, err, "failed to start enqueued wait for cancel workflow")
		// Workflow should get ErrorCodeAwaitedWorkflowCancelled Error
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when waiting for enqueued workflow to complete, but got none")
		dbosErr := &Error{Code: ErrorCodeAwaitedWorkflowCancelled}
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)
		return "", nil
	}
	RegisterWorkflow(dbosCtx, enqueuedWorkflowEnqueuesATimeoutWorkflow)

	detachedWorkflow := func(ctx Context, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
			return "detached-workflow-completed", nil
		}
	}

	enqueuedWorkflowEnqueuesADetachedWorkflow := func(ctx Context, timeout time.Duration) (string, error) {
		myId, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %v", err)
		}
		childID := fmt.Sprintf("%s-child", myId)
		// This workflow will enqueue a workflow that is not cancelable
		childCtx := WithoutCancel(ctx)
		handle, err := RunWorkflow(childCtx, detachedWorkflow, timeout*2, WithQueue(timeoutQueue), WithWorkflowID(childID))
		require.NoError(t, err, "failed to start enqueued detached workflow")

		// Wait for the enqueued workflow to complete
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from enqueued detached workflow")
		assert.Equal(t, "detached-workflow-completed", result, "expected result to be 'detached-workflow-completed'")
		return result, nil
	}

	RegisterWorkflow(dbosCtx, detachedWorkflow)
	RegisterWorkflow(dbosCtx, enqueuedWorkflowEnqueuesADetachedWorkflow)

	var timeoutOnDequeueQueue *workflowQueue // database-backed; registered after Launch
	blockingEvent := NewEvent()
	blockingWorkflow := func(ctx Context, _ string) (string, error) {
		blockingEvent.Wait()
		return "blocking-done", nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)
	fastWorkflow := func(ctx Context, _ string) (string, error) {
		return "done", nil
	}
	RegisterWorkflow(dbosCtx, fastWorkflow)

	Launch(dbosCtx)

	timeoutQueue, err := registerWFQ(dbosCtx, "timeout-queue")
	require.NoError(t, err)
	timeoutOnDequeueQueue, err = registerWFQ(dbosCtx, "timeout-on-dequeue-queue", WithGlobalConcurrency(1))
	require.NoError(t, err)

	t.Run("EnqueueWorkflowTimeout", func(t *testing.T) {
		// Start a workflow that will wait indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunWorkflow(cancelCtx, queuedWaitForCancelWorkflow, "enqueue-wait-for-cancel", WithQueue(timeoutQueue))
		require.NoError(t, err, "failed to enqueue wait for cancel workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.Error(t, err, "expected error but got none")

		// Check the error type
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)

		assert.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be ErrorCodeAwaitedWorkflowCancelled")

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
		handle, err := RunWorkflow(cancelCtx, enqueuedWorkflowEnqueuesATimeoutWorkflow, childWorkflowID, WithQueue(timeoutQueue))
		require.NoError(t, err, "failed to start enqueued workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.Error(t, err, "expected error but got none")

		// Check the error type
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)

		assert.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be ErrorCodeAwaitedWorkflowCancelled")

		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Wait for the child workflow status to become cancelled. The child is
		// enqueued by the parent only once the parent has been dequeued and run, so
		// it may not exist yet on the first polls: treat a missing child as the
		// condition not being met rather than a hard failure (a require here would
		// kill the polling goroutine on the first transient miss).
		require.Eventually(t, func() bool {
			childHandle, err := RetrieveWorkflow[string](dbosCtx, childWorkflowID)
			if err != nil {
				return false
			}

			status, err := childHandle.GetStatus()
			if err != nil {
				return false
			}
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "expected enqueued workflow status to be WorkflowStatusCancelled")

		require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up after workflow cancellation, but they are not")
	})

	t.Run("EnqueueWorkflowThatEnqueuesADetachedWorkflow", func(t *testing.T) {
		// Start a workflow that enqueues another workflow that is not cancelable.
		// The timeout must be long enough for the parent to enqueue the child and
		// start its getResult step (both DB roundtrips) before the deadline fires,
		// or the getResult pre-check sees the parent CANCELLED and errors.
		timeout := 1 * time.Second
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunWorkflow(cancelCtx, enqueuedWorkflowEnqueuesADetachedWorkflow, timeout, WithQueue(timeoutQueue))
		require.NoError(t, err, "failed to start enqueued detached workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.Error(t, err, "expected error but got none")

		// Check the error type
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)

		assert.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code, "expected error code to be ErrorCodeAwaitedWorkflowCancelled")

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
		blockingHandle, err := RunWorkflow(dbosCtx, blockingWorkflow, "blocking", WithQueue(timeoutOnDequeueQueue))
		require.NoError(t, err, "failed to enqueue blocking workflow")

		// Set a timeout that would expire if set on enqueue
		timeout := 2 * time.Second
		timeoutCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()

		// Enqueue second workflow with timeout
		handle, err := RunWorkflow(timeoutCtx, fastWorkflow, "timeout-test", WithQueue(timeoutOnDequeueQueue))
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

	// Priority-enabled queue with max concurrency of 1, plus a child queue.
	// Database-backed; registered after Launch.
	var priorityQueue, childQueue *workflowQueue

	workflowEvent := NewEvent()
	var wfPriorityList []int
	var mu sync.Mutex

	childWorkflow := func(ctx Context, p int) (int, error) {
		workflowEvent.Wait()
		return p, nil
	}
	RegisterWorkflow(dbosCtx, childWorkflow)

	testWorkflow := func(ctx Context, priority int) (int, error) {
		mu.Lock()
		wfPriorityList = append(wfPriorityList, priority)
		mu.Unlock()

		childHandle, err := RunWorkflow(ctx, childWorkflow, priority, WithQueue(childQueue))
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

	priorityQueue, err = registerWFQ(dbosCtx, "test_queue_priority", WithGlobalConcurrency(1), WithPriorityEnabled())
	require.NoError(t, err)
	childQueue, err = registerWFQ(dbosCtx, "test_queue_child")
	require.NoError(t, err)

	var wfHandles []WorkflowHandle[int]

	// First, enqueue a workflow without priority (default to priority 0)
	handle, err := RunWorkflow(dbosCtx, testWorkflow, 0, WithQueue(priorityQueue))
	require.NoError(t, err)
	wfHandles = append(wfHandles, handle)

	// Then, enqueue workflows with priority 5 to 1
	reversedPriorityHandles := make([]WorkflowHandle[int], 0, 5)
	for i := 5; i > 0; i-- {
		handle, err := RunWorkflow(dbosCtx, testWorkflow, i, WithQueue(priorityQueue), WithPriority(uint(i)))
		require.NoError(t, err)
		reversedPriorityHandles = append(reversedPriorityHandles, handle)
	}
	for i := 0; i < len(reversedPriorityHandles); i++ {
		wfHandles = append(wfHandles, reversedPriorityHandles[len(reversedPriorityHandles)-i-1])
	}

	// Finally, enqueue two workflows without priority again (default priority 0)
	handle6, err := RunWorkflow(dbosCtx, testWorkflow, 6, WithQueue(priorityQueue))
	require.NoError(t, err)
	wfHandles = append(wfHandles, handle6)

	time.Sleep(10 * time.Millisecond) // Avoid collisions in created_at...
	handle7, err := RunWorkflow(dbosCtx, testWorkflow, 7, WithQueue(priorityQueue))
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
	testWorkflow := func(ctx Context, input string) (string, error) {
		return "completed-" + input, nil
	}

	// Blocking workflow for testing pending/enqueued workflows
	startEvent := NewEvent()
	blockEvent := NewEvent()
	blockingWorkflow := func(ctx Context, input string) (string, error) {
		startEvent.Set()
		blockEvent.Wait()
		return "blocked-" + input, nil
	}

	RegisterWorkflow(dbosCtx, testWorkflow)
	RegisterWorkflow(dbosCtx, blockingWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	// Create database-backed queues for testing
	testQueue1, err := registerWFQ(dbosCtx, "list-test-queue", WithGlobalConcurrency(1))
	require.NoError(t, err)
	testQueue2, err := registerWFQ(dbosCtx, "list-test-queue2", WithGlobalConcurrency(1))
	require.NoError(t, err)

	t.Run("WithFilterQueuesOnly", func(t *testing.T) {
		blockEvent.Clear()
		startEvent.Clear()
		// Create a non-queued workflow (completed) - this should NOT appear in WithFilterQueuesOnly results
		nonQueuedHandle, err := RunWorkflow(dbosCtx, testWorkflow, "non-queued-test1")
		require.NoError(t, err, "failed to start non-queued workflow")
		_, err = nonQueuedHandle.GetResult()
		require.NoError(t, err, "failed to complete non-queued workflow")

		// Create queued workflows that will be pending/enqueued
		queuedHandle1, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-1-test1", WithQueue(testQueue1))
		require.NoError(t, err, "failed to start queued workflow 1")

		queuedHandle2, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-2-test1", WithQueue(testQueue1))
		require.NoError(t, err, "failed to start queued workflow 2")

		startEvent.Wait()

		// List workflows with WithFilterQueuesOnly - should only return queued workflows
		queuedWorkflows, err := ListWorkflows(dbosCtx, WithFilterQueuesOnly())
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
		completedQueuedHandle, err := RunWorkflow(dbosCtx, testWorkflow, "queued-completed", WithQueue(testQueue2))
		require.NoError(t, err, "failed to start queued workflow for completion")

		// Wait for it to complete
		_, err = completedQueuedHandle.GetResult()
		require.NoError(t, err, "failed to complete queued workflow")

		// Create pending queued workflows that will NOT have SUCCESS status
		pendingHandle1, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-pending-1", WithQueue(testQueue2))
		require.NoError(t, err, "failed to start pending queued workflow 1")

		pendingHandle2, err := RunWorkflow(dbosCtx, blockingWorkflow, "queued-pending-2", WithQueue(testQueue2))
		require.NoError(t, err, "failed to start pending queued workflow 2")

		startEvent.Wait()

		// List queued workflows with SUCCESS status filter
		successWorkflows, err := ListWorkflows(dbosCtx, WithFilterQueuesOnly(), WithFilterStatus(WorkflowStatusSuccess), WithFilterQueueName(testQueue2.Name))
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
		simpleWorkflow := func(ctx Context, input string) (string, error) {
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
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)

		// Verify the error carries the InvalidOption code
		assert.True(t, errors.Is(err, ErrInvalidOption), "expected error to be ErrorCodeInvalidOption")
		assert.Contains(t, dbosErr.Message, "partition key provided but queue name is missing")
	})

	t.Run("PartitionKeyWithDeduplicationID", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Register a simple workflow
		simpleWorkflow := func(ctx Context, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, simpleWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Create a partitioned database-backed queue
		partitionedQueue, err := registerWFQ(dbosCtx, "partitioned-queue-test", WithPartitionQueue())
		require.NoError(t, err)

		// Attempt to enqueue with both partition key and deduplication ID
		// This should return an error
		_, err = RunWorkflow(dbosCtx, simpleWorkflow, "test-input", WithQueue(partitionedQueue), WithQueuePartitionKey("partition-1"), WithDeduplicationID("dedup-id"))
		require.Error(t, err, "expected error when enqueueing with both partition key and deduplication ID")

		// Check that it's the correct error type
		var dbosErr *Error
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *Error, got %T", err)

		// Verify the error carries the InvalidOption code
		assert.True(t, errors.Is(err, ErrInvalidOption), "expected error to be ErrorCodeInvalidOption")
		assert.Contains(t, dbosErr.Message, "partition key and deduplication ID cannot be used together")
	})

	t.Run("Dequeue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Create events for blocking workflow on partition 1
		partition1StartEvent := NewEvent()
		partition1BlockEvent := NewEvent()

		// Create blocking workflow for partition 1
		blockingWorkflowP1 := func(ctx Context, input string) (string, error) {
			partition1StartEvent.Set()
			partition1BlockEvent.Wait()
			return "p1-" + input, nil
		}

		// Create non-blocking workflow (used for both partitions)
		nonBlockingWorkflow := func(ctx Context, input string) (string, error) {
			return input, nil
		}

		RegisterWorkflow(dbosCtx, blockingWorkflowP1)
		RegisterWorkflow(dbosCtx, nonBlockingWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Create a partitioned database-backed queue with concurrency limit of 1 per partition
		partitionedQueue, err := registerWFQ(dbosCtx, "partitioned-queue", WithPartitionQueue(), WithGlobalConcurrency(1))
		require.NoError(t, err)

		// Enqueue a blocking workflow on partition 1
		handleP1Blocked, err := RunWorkflow(dbosCtx, blockingWorkflowP1, "blocked", WithQueue(partitionedQueue), WithQueuePartitionKey("partition-1"))
		require.NoError(t, err, "failed to enqueue blocking workflow on partition 1")

		// Wait for the blocking workflow on partition 1 to start
		partition1StartEvent.Wait()

		// Enqueue a non-blocking workflow on partition 1 - this should be blocked behind the blocking one
		handleP1Normal, err := RunWorkflow(dbosCtx, nonBlockingWorkflow, "p1-normal", WithQueue(partitionedQueue), WithQueuePartitionKey("partition-1"))
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
			handle, err := RunWorkflow(dbosCtx, nonBlockingWorkflow, fmt.Sprintf("p2-workflow-%d", i), WithQueue(partitionedQueue), WithQueuePartitionKey("partition-2"))
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
		require.Equal(t, models.InternalQueueName, runner.internalQueue.Name)
	})
}

func TestQueuePollingIntervals(t *testing.T) {
	t.Run("queue uses default intervals when not specified", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: false})
		require.NoError(t, Launch(ctx))

		queue, err := registerWFQ(ctx, "polling-default-queue")
		require.NoError(t, err)
		require.Equal(t, models.DefaultBasePollingInterval, queue.basePollingInterval)
		// maxPollingInterval is not persisted: the DB-backed handle leaves it unset
		// and the queue worker derives the backoff ceiling from the base at runtime.
		require.Zero(t, queue.maxPollingInterval)
	})

	t.Run("base polling interval round-trips; max is worker-derived and not persisted", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: false})
		require.NoError(t, Launch(ctx))

		basePollingInterval := 2 * time.Second
		// Database-backed queues persist only the base polling interval. The max
		// polling interval is a worker-local backoff ceiling the queue worker derives
		// from the base at runtime, so RegisterQueue ignores WithQueueMaxPollingInterval
		// (logging a warning) and it never surfaces on the handle.
		queue, err := registerWFQ(ctx, "polling-custom-queue",
			WithQueueBasePollingInterval(basePollingInterval),
			WithQueueMaxPollingInterval(5*time.Second))
		require.NoError(t, err)
		require.Equal(t, basePollingInterval, queue.basePollingInterval)
		require.Zero(t, queue.maxPollingInterval)

		// Re-reading from the database returns the same base interval and no max.
		reloaded, err := retrieveWFQ(ctx, "polling-custom-queue")
		require.NoError(t, err)
		require.Equal(t, basePollingInterval, reloaded.basePollingInterval)
		require.Zero(t, reloaded.maxPollingInterval)
	})

	t.Run("SetPollingInterval updates the base interval at runtime", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: false})
		require.NoError(t, Launch(ctx))

		q, err := registerWFQ(ctx, "polling-setter-queue", WithQueueBasePollingInterval(time.Second))
		require.NoError(t, err)

		var qi Queue = q
		require.NoError(t, qi.SetPollingInterval(ctx, 250*time.Millisecond))
		require.Equal(t, 250*time.Millisecond, qi.GetPollingInterval())

		// The change is persisted and visible on a fresh reload.
		persisted, err := retrieveWFQ(ctx, "polling-setter-queue")
		require.NoError(t, err)
		require.Equal(t, 250*time.Millisecond, persisted.basePollingInterval)

		// A non-positive interval is rejected by config validation, leaving the
		// previous value in place.
		require.Error(t, qi.SetPollingInterval(ctx, 0))
		require.Equal(t, 250*time.Millisecond, qi.GetPollingInterval())

		// Setters only apply to database-backed queues.
		notDBBacked := &workflowQueue{Name: "not-db-backed"}
		require.Error(t, notDBBacked.SetPollingInterval(ctx, time.Second))
	})
}

func TestListenQueues(t *testing.T) {
	t.Run("ListenToSubsetOfQueues", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		// Register a simple workflow
		testWorkflow := func(ctx Context, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, testWorkflow)

		// Listen to only 2 out of 3 queues. Each ListenQueues call replaces the
		// whole set, so both names must be passed in one call.
		ListenQueues(dbosCtx, "listen-test-queue-1", "listen-test-queue-2")

		// Launch DBOS
		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		// Register 3 queues
		queue1, err := registerWFQ(dbosCtx, "listen-test-queue-1")
		require.NoError(t, err)
		queue2, err := registerWFQ(dbosCtx, "listen-test-queue-2")
		require.NoError(t, err)
		queue3, err := registerWFQ(dbosCtx, "listen-test-queue-3")
		require.NoError(t, err)

		// Enqueue workflows in all 3 queues
		handle1, err := RunWorkflow(dbosCtx, testWorkflow, "queue1-input", WithQueue(queue1))
		require.NoError(t, err, "failed to enqueue workflow to queue1")

		handle2, err := RunWorkflow(dbosCtx, testWorkflow, "queue2-input", WithQueue(queue2))
		require.NoError(t, err, "failed to enqueue workflow to queue2")

		handle3, err := RunWorkflow(dbosCtx, testWorkflow, "queue3-input", WithQueue(queue3))
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

		// Register a simple workflow
		testWorkflow := func(ctx Context, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, testWorkflow)

		// Call ListenQueues with only one queue (internal queue should still be listened to)
		ListenQueues(dbosCtx, "listen-internal-test-queue-1")

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
		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{OriginalWorkflowID: originalHandle.GetWorkflowID()})
		require.NoError(t, err, "failed to fork workflow")

		// Verify the forked workflow completes (proving the internal queue is being listened to)
		forkResult, err := forkHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked workflow")
		assert.Equal(t, "original-input", forkResult, "expected forked workflow to complete")

		// Verify the forked workflow was on the internal queue
		forkStatus, err := forkHandle.GetStatus()
		require.NoError(t, err, "failed to get status of forked workflow")
		assert.Equal(t, models.InternalQueueName, forkStatus.QueueName, "expected forked workflow to be on internal queue")

	})

	t.Run("ForkWorkflowToCustomQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		testWorkflow := func(ctx Context, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, testWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		forkTargetQueue, err := registerWFQ(dbosCtx, "fork-target-queue",
			WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)

		originalHandle, err := RunWorkflow(dbosCtx, testWorkflow, "fork-queue-input")
		require.NoError(t, err, "failed to run original workflow")
		_, err = originalHandle.GetResult()
		require.NoError(t, err, "failed to get result from original workflow")

		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowID: originalHandle.GetWorkflowID(),
			QueueName:          forkTargetQueue.GetName(),
		})
		require.NoError(t, err, "failed to fork workflow to custom queue")

		forkResult, err := forkHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked workflow")
		assert.Equal(t, "fork-queue-input", forkResult)

		status, err := forkHandle.GetStatus()
		require.NoError(t, err, "failed to get forked workflow status")
		assert.Equal(t, forkTargetQueue.Name, status.QueueName, "forked workflow should be attributed to the custom queue")
	})

	t.Run("ForkWorkflowToPartitionedQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		testWorkflow := func(ctx Context, input string) (string, error) {
			return input, nil
		}
		RegisterWorkflow(dbosCtx, testWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		forkPartitionedQueue, err := registerWFQ(dbosCtx, "fork-partitioned-queue",
			WithPartitionQueue(),
			WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)

		originalHandle, err := RunWorkflow(dbosCtx, testWorkflow, "fork-partition-input",
			WithQueue(forkPartitionedQueue), WithQueuePartitionKey("orig-partition"))
		require.NoError(t, err, "failed to run original workflow")
		_, err = originalHandle.GetResult()
		require.NoError(t, err, "failed to get result from original workflow")

		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowID: originalHandle.GetWorkflowID(),
			QueueName:          forkPartitionedQueue.GetName(),
			QueuePartitionKey:  "forked-partition",
		})
		require.NoError(t, err, "failed to fork workflow to partitioned queue")

		forkResult, err := forkHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked workflow")
		assert.Equal(t, "fork-partition-input", forkResult)

		status, err := forkHandle.GetStatus()
		require.NoError(t, err, "failed to get forked workflow status")
		assert.Equal(t, forkPartitionedQueue.Name, status.QueueName, "forked workflow should be attributed to the custom queue")
		assert.Equal(t, "forked-partition", status.QueuePartitionKey, "forked workflow should carry the supplied partition key")
	})

	t.Run("ResumeWorkflowToCustomQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		blockEvent := NewEvent()
		blockingWorkflow := func(ctx Context, input string) (string, error) {
			blockEvent.Wait()
			return input, nil
		}
		RegisterWorkflow(dbosCtx, blockingWorkflow)

		err := Launch(dbosCtx)
		require.NoError(t, err, "failed to launch DBOS instance")

		resumeTargetQueue, err := registerWFQ(dbosCtx, "resume-target-queue",
			WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)

		handle, err := RunWorkflow(dbosCtx, blockingWorkflow, "resume-queue-input")
		require.NoError(t, err, "failed to start blocking workflow")

		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to cancel workflow")
		blockEvent.Set()

		resumedHandle, err := ResumeWorkflow[string](dbosCtx, handle.GetWorkflowID(), WithResumeQueue(resumeTargetQueue.Name))
		require.NoError(t, err, "failed to resume workflow to custom queue")

		result, err := resumedHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed workflow")
		assert.Equal(t, "resume-queue-input", result)

		status, err := resumedHandle.GetStatus()
		require.NoError(t, err, "failed to get resumed workflow status")
		assert.Equal(t, resumeTargetQueue.Name, status.QueueName, "resumed workflow should be attributed to the custom queue")
	})

	t.Run("ListenToDatabaseBackedQueueByName", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
		RegisterWorkflow(dbosCtx, queueWorkflow)

		// Listen to a database-backed queue by name before launch, even though the
		// queue does not exist yet — listening is by name.
		ListenQueues(dbosCtx, "listened-db-queue")
		require.NoError(t, Launch(dbosCtx))

		// Register both the listened queue and an unlistened one after launch.
		listenedQueue, err := registerWFQ(dbosCtx, "listened-db-queue", WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)
		unlistenedQueue, err := registerWFQ(dbosCtx, "unlistened-db-queue", WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)

		// The listened queue dispatches.
		h1, err := RunWorkflow(dbosCtx, queueWorkflow, "a", WithQueue(listenedQueue))
		require.NoError(t, err)
		r1, err := h1.GetResult()
		require.NoError(t, err)
		require.Equal(t, "a", r1)

		// The unlistened queue does not: its workflow stays ENQUEUED.
		h2, err := RunWorkflow(dbosCtx, queueWorkflow, "b", WithQueue(unlistenedQueue))
		require.NoError(t, err)
		time.Sleep(2 * time.Second)
		st2, err := h2.GetStatus()
		require.NoError(t, err)
		require.Equal(t, WorkflowStatusEnqueued, st2.Status)
	})

	t.Run("DynamicallyChangeListenSetForDatabaseBackedQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
		RegisterWorkflow(dbosCtx, queueWorkflow)

		// A filter is active from launch (a name that never resolves), so the dynamic
		// queue is initially excluded.
		ListenQueues(dbosCtx, "placeholder")
		require.NoError(t, Launch(dbosCtx))

		dynamicQueue, err := registerWFQ(dbosCtx, "dynamic-db-queue", WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)
		h, err := RunWorkflow(dbosCtx, queueWorkflow, "c", WithQueue(dynamicQueue))
		require.NoError(t, err)

		// Not listened yet: stays ENQUEUED.
		time.Sleep(1500 * time.Millisecond)
		st, err := h.GetStatus()
		require.NoError(t, err)
		require.Equal(t, WorkflowStatusEnqueued, st.Status)

		// Replace the listen set after launch; the supervisor picks it up on its
		// next reconcile tick and the workflow completes.
		ListenQueues(dbosCtx, "dynamic-db-queue")
		r, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, "c", r)
	})

	t.Run("EachCallReplacesTheListenSet", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
		qr := dbosCtx.(*dbosContext).queueRunner

		snapshot := func() map[string]bool {
			qr.listenMu.Lock()
			defer qr.listenMu.Unlock()
			out := make(map[string]bool, len(qr.listenedQueues))
			for k, v := range qr.listenedQueues {
				out[k] = v
			}
			return out
		}

		ListenQueues(dbosCtx, "a", "b")
		require.Equal(t, map[string]bool{"a": true, "b": true}, snapshot())
		require.Equal(t, []string{"a", "b"}, ListenedQueues(dbosCtx))

		// A second call replaces the whole set: "a" and "b" are unlistened.
		ListenQueues(dbosCtx, "c")
		require.Equal(t, map[string]bool{"c": true}, snapshot())

		// Incremental add: read the current set, append, listen again.
		ListenQueues(dbosCtx, append(ListenedQueues(dbosCtx), "d")...)
		require.Equal(t, map[string]bool{"c": true, "d": true}, snapshot())

		// Zero names clears the filter (back to listening to every queue).
		ListenQueues(dbosCtx)
		require.Empty(t, snapshot())
		require.Empty(t, ListenedQueues(dbosCtx))
	})

	t.Run("ReplaceListenSetUnlistensQueue", func(t *testing.T) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
		RegisterWorkflow(dbosCtx, queueWorkflow)

		ListenQueues(dbosCtx, "replace-queue-a")
		require.NoError(t, Launch(dbosCtx))

		queueA, err := registerWFQ(dbosCtx, "replace-queue-a", WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)
		queueB, err := registerWFQ(dbosCtx, "replace-queue-b", WithQueueBasePollingInterval(50*time.Millisecond))
		require.NoError(t, err)

		// A is listened: its workflow completes.
		hA, err := RunWorkflow(dbosCtx, queueWorkflow, "a1", WithQueue(queueA))
		require.NoError(t, err)
		rA, err := hA.GetResult()
		require.NoError(t, err)
		require.Equal(t, "a1", rA)

		// Replace the set with B only: A is unlistened, B starts dispatching.
		ListenQueues(dbosCtx, "replace-queue-b")

		hB, err := RunWorkflow(dbosCtx, queueWorkflow, "b1", WithQueue(queueB))
		require.NoError(t, err)
		rB, err := hB.GetResult()
		require.NoError(t, err)
		require.Equal(t, "b1", rB)

		// A no longer dispatches: its workflow stays ENQUEUED.
		hA2, err := RunWorkflow(dbosCtx, queueWorkflow, "a2", WithQueue(queueA))
		require.NoError(t, err)
		time.Sleep(3 * time.Second)
		stA2, err := hA2.GetStatus()
		require.NoError(t, err)
		require.Equal(t, WorkflowStatusEnqueued, stA2.Status)
	})
}

func TestDelayedExecution(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	delayWorkflow := func(ctx Context, _ string) (string, error) {
		return "done", nil
	}

	RegisterWorkflow(dbosCtx, delayWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	delayQueue, err := registerWFQ(dbosCtx, "test-delay-queue", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)
	dedupDelayQueue, err := registerWFQ(dbosCtx, "test-delay-dedup-queue", WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)

	t.Run("BasicDelay", func(t *testing.T) {
		delayDuration := 2 * time.Second
		tBefore := time.Now()

		handle, err := RunWorkflow(dbosCtx, delayWorkflow, "", WithQueue(delayQueue), WithDelay(delayDuration))
		require.NoError(t, err, "failed to enqueue delayed workflow")

		tAfter := time.Now()

		// Check initial status is DELAYED
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusDelayed, status.Status)
		assert.False(t, status.DelayUntil.IsZero(), "delay_until should be set")
		// Allow 100ms tolerance for timing precision (DB stores milliseconds)
		tolerance := 100 * time.Millisecond
		assert.True(t, status.DelayUntil.After(tBefore.Add(delayDuration).Add(-tolerance)),
			"delay_until should be >= tBefore + delay (delay_until=%v, expected>=%v)", status.DelayUntil, tBefore.Add(delayDuration))
		assert.True(t, status.DelayUntil.Before(tAfter.Add(delayDuration).Add(tolerance)),
			"delay_until should be <= tAfter + delay (delay_until=%v, expected<=%v)", status.DelayUntil, tAfter.Add(delayDuration))

		// Wait for the workflow to complete
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get workflow result")
		assert.Equal(t, "done", result)

		// Verify it completed
		finalStatus, err := handle.GetStatus()
		require.NoError(t, err, "failed to get final status")
		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status)

		// Verify it wasn't dequeued before the delay expired
		assert.True(t, finalStatus.StartedAt.After(status.DelayUntil) || finalStatus.StartedAt.Equal(status.DelayUntil),
			"workflow should not have been dequeued before delay expired (started_at=%v, delay_until=%v)",
			finalStatus.StartedAt, status.DelayUntil)
	})

	t.Run("DelayedCancelAndResume", func(t *testing.T) {
		// Cancel a DELAYED workflow — it should never run
		cancelHandle, err := RunWorkflow(dbosCtx, delayWorkflow, "", WithQueue(delayQueue), WithDelay(60*time.Second))
		require.NoError(t, err)

		status, err := cancelHandle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusDelayed, status.Status)

		// Verify the delayed workflow appears in list queries before cancelling
		allWorkflows, err := ListWorkflows(dbosCtx, WithFilterStatus(WorkflowStatusDelayed))
		require.NoError(t, err)
		found := false
		for _, wf := range allWorkflows {
			if wf.ID == cancelHandle.GetWorkflowID() {
				found = true
				break
			}
		}
		assert.True(t, found, "delayed workflow should appear in list_workflows")

		queuedWorkflows, err := ListWorkflows(dbosCtx, WithFilterQueuesOnly())
		require.NoError(t, err)
		found = false
		for _, wf := range queuedWorkflows {
			if wf.ID == cancelHandle.GetWorkflowID() {
				found = true
				break
			}
		}
		assert.True(t, found, "delayed workflow should appear in list_queued_workflows")

		err = CancelWorkflow(dbosCtx, cancelHandle.GetWorkflowID())
		require.NoError(t, err)

		cancelledStatus, err := cancelHandle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusCancelled, cancelledStatus.Status)

		// Resume the cancelled workflow — should complete immediately, bypassing the delay
		tBefore := time.Now()
		_, err = ResumeWorkflow[string](dbosCtx, cancelHandle.GetWorkflowID())
		require.NoError(t, err)

		result, err := cancelHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "done", result)

		finalStatus, err := cancelHandle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusSuccess, finalStatus.Status)
		assert.Less(t, time.Since(tBefore), 10*time.Second, "resume should bypass the delay")
	})

	t.Run("DelayedDeduplication", func(t *testing.T) {
		dedupID := uuid.New().String()

		handle, err := RunWorkflow(dbosCtx, delayWorkflow, "",
			WithQueue(dedupDelayQueue), WithDelay(60*time.Second), WithDeduplicationID(dedupID))
		require.NoError(t, err)

		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusDelayed, status.Status)

		// Second enqueue with the same dedup ID should fail
		_, err = RunWorkflow(dbosCtx, delayWorkflow, "",
			WithQueue(dedupDelayQueue), WithDelay(60*time.Second), WithDeduplicationID(dedupID))
		require.Error(t, err, "expected deduplication error")
		assert.True(t, errors.Is(err, ErrQueueDeduplicated), "expected ErrorCodeQueueDeduplicated error, got: %v", err)

		// Clean up
		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
	})

	t.Run("SetWorkflowDelayDuration", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, delayWorkflow, "", WithQueue(delayQueue), WithDelay(600*time.Second))
		require.NoError(t, err)

		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusDelayed, status.Status)

		// Shorten the delay to 500ms
		err = SetWorkflowDelay(dbosCtx, handle.GetWorkflowID(), WithDelayDuration(500*time.Millisecond))
		require.NoError(t, err)

		status, err = handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusDelayed, status.Status)
		assert.True(t, status.DelayUntil.Before(time.Now().Add(5*time.Second)),
			"delay should have been shortened")

		tStart := time.Now()
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "done", result)
		assert.Less(t, time.Since(tStart), 30*time.Second, "workflow should complete shortly after shortened delay")
	})

	t.Run("SetWorkflowDelayUntil", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, delayWorkflow, "", WithQueue(delayQueue), WithDelay(600*time.Second))
		require.NoError(t, err)

		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusDelayed, status.Status)

		soon := time.Now().Add(500 * time.Millisecond)
		err = SetWorkflowDelay(dbosCtx, handle.GetWorkflowID(), WithDelayUntil(soon))
		require.NoError(t, err)

		status, err = handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, WorkflowStatusDelayed, status.Status)
		tolerance := 100 * time.Millisecond
		assert.True(t, status.DelayUntil.After(soon.Add(-tolerance)),
			"delay_until should be close to requested time (got=%v, expected~%v)", status.DelayUntil, soon)
		assert.True(t, status.DelayUntil.Before(soon.Add(tolerance)),
			"delay_until should be close to requested time (got=%v, expected~%v)", status.DelayUntil, soon)

		tStart := time.Now()
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "done", result)
		assert.Less(t, time.Since(tStart), 30*time.Second, "workflow should complete shortly after shortened delay")
	})

	t.Run("DelayWithoutQueueErrors", func(t *testing.T) {
		_, err := RunWorkflow(dbosCtx, delayWorkflow, "", WithDelay(5*time.Second))
		require.Error(t, err, "expected error when using delay without queue")
		assert.Contains(t, err.Error(), "delay provided but queue name is missing")
	})
}

func TestDatabaseBackedQueues(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, queueWorkflow)

	// Database-backed queues can be registered after launch.
	require.NoError(t, Launch(dbosCtx))

	// Create, get, list, update (upsert) and dispatch are exercised by the
	// migrated mechanics tests, ConflictPolicies, and the Mixed test. Delete is
	// only covered here.
	t.Run("DeleteQueue", func(t *testing.T) {
		_, err := registerWFQ(dbosCtx, "ephemeral-queue")
		require.NoError(t, err)
		require.NoError(t, DeleteQueue(dbosCtx, "ephemeral-queue"))

		// After deletion the queue is gone (RetrieveQueue returns ErrQueueNotFound).
		got, err := retrieveWFQ(dbosCtx, "ephemeral-queue")
		require.ErrorIs(t, err, ErrQueueNotFound)
		require.Nil(t, got)
	})

	t.Run("ConflictPolicies", func(t *testing.T) {
		_, err := registerWFQ(dbosCtx, "conflict-q", WithGlobalConcurrency(1))
		require.NoError(t, err)

		// never_update leaves the existing configuration unchanged.
		_, err = registerWFQ(dbosCtx, "conflict-q", WithGlobalConcurrency(99), WithQueueOnConflict(QueueConflictNeverUpdate))
		require.NoError(t, err)
		got, err := retrieveWFQ(dbosCtx, "conflict-q")
		require.NoError(t, err)
		require.Equal(t, 1, *got.GlobalConcurrency)

		// always_update overwrites it.
		_, err = registerWFQ(dbosCtx, "conflict-q", WithGlobalConcurrency(42), WithQueueOnConflict(QueueConflictAlwaysUpdate))
		require.NoError(t, err)
		got, err = retrieveWFQ(dbosCtx, "conflict-q")
		require.NoError(t, err)
		require.Equal(t, 42, *got.GlobalConcurrency)

		// update_if_latest_version (the default) updates while this process runs the
		// latest registered application version.
		_, err = registerWFQ(dbosCtx, "conflict-q", WithGlobalConcurrency(7))
		require.NoError(t, err)
		got, err = retrieveWFQ(dbosCtx, "conflict-q")
		require.NoError(t, err)
		require.Equal(t, 7, *got.GlobalConcurrency)

		// ...but not once a newer application version is the latest (rolling deploy).
		sysdb := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		require.NoError(t, sysdb.CreateApplicationVersion(context.Background(), "v-newer"))
		require.NoError(t, sysdb.UpdateApplicationVersionTimestamp(context.Background(), "v-newer", time.Now().Add(time.Hour).UnixMilli()))
		_, err = registerWFQ(dbosCtx, "conflict-q", WithGlobalConcurrency(1000))
		require.NoError(t, err)
		got, err = retrieveWFQ(dbosCtx, "conflict-q")
		require.NoError(t, err)
		require.Equal(t, 7, *got.GlobalConcurrency, "default policy must not overwrite when a newer version is latest")
	})

	t.Run("ValidationRejectsBadConcurrency", func(t *testing.T) {
		_, err := registerWFQ(dbosCtx, "bad-queue",
			WithGlobalConcurrency(1), WithWorkerConcurrency(5))
		require.Error(t, err)
	})

	t.Run("ValidationRejectsBadRateLimiter", func(t *testing.T) {
		// A non-positive limit is rejected and nothing is persisted.
		_, err := registerWFQ(dbosCtx, "bad-rate-limit-queue",
			WithRateLimiter(&RateLimiter{Limit: 0, Period: time.Second}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "rate limiter limit must be positive")
		got, err := retrieveWFQ(dbosCtx, "bad-rate-limit-queue")
		require.ErrorIs(t, err, ErrQueueNotFound)
		require.Nil(t, got)

		// A non-positive period is rejected too.
		_, err = registerWFQ(dbosCtx, "bad-rate-period-queue",
			WithRateLimiter(&RateLimiter{Limit: 5, Period: 0}))
		require.Error(t, err)
		require.Contains(t, err.Error(), "rate limiter period must be positive")
		got, err = retrieveWFQ(dbosCtx, "bad-rate-period-queue")
		require.ErrorIs(t, err, ErrQueueNotFound)
		require.Nil(t, got)
	})

	t.Run("RejectsCollisionWithInMemoryQueue", func(t *testing.T) {
		// The internal queue is an in-memory queue; registering a database-backed
		// queue with the same name must be rejected, and nothing persisted.
		_, err := registerWFQ(dbosCtx, models.InternalQueueName)
		require.Error(t, err)
		got, err := retrieveWFQ(dbosCtx, models.InternalQueueName)
		require.ErrorIs(t, err, ErrQueueNotFound)
		require.Nil(t, got)
	})
}

// TestDatabaseBackedQueueConfigReload verifies that a running queue worker picks
// up configuration changes persisted to the database without a restart. The
// first half raises the global concurrency of a live queue via a RegisterQueue
// upsert and asserts that a workflow which was held back by the old limit then
// starts running. The second half exercises the Queue interface setters: it
// updates several properties and checks both the handle and the persisted row
// reflect them, then drops worker concurrency to 1 and confirms the live worker
// runs only one workflow at a time.
func TestDatabaseBackedQueueConfigReload(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	startA := NewEvent()
	startB := NewEvent()
	release := NewEvent()
	reloadWorkflow := func(_ Context, input string) (string, error) {
		switch input {
		case "a":
			startA.Set()
		case "b":
			startB.Set()
		}
		release.Wait()
		return input, nil
	}
	RegisterWorkflow(dbosCtx, reloadWorkflow)

	// A second workflow used by the setter behavior check below: it records the
	// peak number of concurrently running invocations.
	var wcActive, wcMax atomic.Int64
	wcStarted := make(chan struct{}, 8)
	releaseWC := NewEvent()
	wcWorkflow := func(_ Context, input string) (string, error) {
		n := wcActive.Add(1)
		for {
			m := wcMax.Load()
			if n <= m || wcMax.CompareAndSwap(m, n) {
				break
			}
		}
		wcStarted <- struct{}{}
		releaseWC.Wait()
		wcActive.Add(-1)
		return input, nil
	}
	RegisterWorkflow(dbosCtx, wcWorkflow)

	require.NoError(t, Launch(dbosCtx))

	// Register with a global concurrency of 1.
	q, err := registerWFQ(dbosCtx, "reload-queue", WithGlobalConcurrency(1), WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)
	require.NotNil(t, q.GlobalConcurrency)
	require.Equal(t, 1, *q.GlobalConcurrency)

	hA, err := RunWorkflow(dbosCtx, reloadWorkflow, "a", WithQueue(q))
	require.NoError(t, err)
	hB, err := RunWorkflow(dbosCtx, reloadWorkflow, "b", WithQueue(q))
	require.NoError(t, err)

	// Under concurrency 1, "a" runs and "b" stays ENQUEUED behind it.
	startA.Wait()
	time.Sleep(1500 * time.Millisecond) // let the runner poll a few times
	stB, err := hB.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusEnqueued, stB.Status, "expected b to stay enqueued while a holds the only concurrency slot")

	// Raise the global concurrency to 2 at runtime. The live worker must reload
	// the new limit from the database (republished by the supervisor) without a
	// restart, after which "b" gets a slot and starts running.
	updated, err := registerWFQ(dbosCtx, "reload-queue",
		WithGlobalConcurrency(2),
		WithQueueBasePollingInterval(50*time.Millisecond),
		WithQueueOnConflict(QueueConflictAlwaysUpdate))
	require.NoError(t, err)
	require.NotNil(t, updated.GlobalConcurrency)
	require.Equal(t, 2, *updated.GlobalConcurrency)

	require.Eventually(t, func() bool {
		st, err := hB.GetStatus()
		return err == nil && st.Status == WorkflowStatusPending
	}, 5*time.Second, 50*time.Millisecond, "expected b to start running after the concurrency limit was raised at runtime")

	// Release both and confirm they complete.
	release.Set()
	rA, err := hA.GetResult()
	require.NoError(t, err)
	require.Equal(t, "a", rA)
	rB, err := hB.GetResult()
	require.NoError(t, err)
	require.Equal(t, "b", rB)

	// --- Dynamic setters via the Queue interface ---
	// Global concurrency is 2 at this point. Update several properties and check
	// the handle's getters and the persisted row both reflect the changes.
	var qi Queue = q
	require.NoError(t, qi.SetWorkerConcurrency(dbosCtx, intPtr(2)))
	require.NoError(t, qi.SetPriorityEnabled(dbosCtx, true))
	require.NoError(t, qi.SetRateLimit(dbosCtx, &RateLimiter{Limit: 50, Period: time.Second}))
	require.NoError(t, qi.SetPollingInterval(dbosCtx, 250*time.Millisecond))

	require.NotNil(t, qi.GetWorkerConcurrency())
	require.Equal(t, 2, *qi.GetWorkerConcurrency())
	require.True(t, qi.GetPriorityEnabled())
	require.Equal(t, 250*time.Millisecond, qi.GetPollingInterval())

	persisted, err := retrieveWFQ(dbosCtx, "reload-queue")
	require.NoError(t, err)
	require.NotNil(t, persisted.WorkerConcurrency)
	require.Equal(t, 2, *persisted.WorkerConcurrency)
	require.True(t, persisted.PriorityEnabled)
	require.NotNil(t, persisted.RateLimit)
	require.Equal(t, 50, persisted.RateLimit.Limit)
	require.Equal(t, 250*time.Millisecond, persisted.basePollingInterval)

	// Cross-field validation runs against the freshly persisted values: worker
	// concurrency may not exceed the global concurrency (2).
	require.Error(t, qi.SetWorkerConcurrency(dbosCtx, intPtr(3)),
		"expected worker concurrency above global concurrency to be rejected")

	// --- Behavior change: drop worker concurrency to 1 ---
	require.NoError(t, qi.SetWorkerConcurrency(dbosCtx, intPtr(1)))
	// Let the supervisor republish the new config (reconcile tick ~1s) before
	// enqueuing, so the worker dequeues under worker_concurrency=1.
	time.Sleep(1500 * time.Millisecond)

	hX, err := RunWorkflow(dbosCtx, wcWorkflow, "x", WithQueue(q))
	require.NoError(t, err)
	hY, err := RunWorkflow(dbosCtx, wcWorkflow, "y", WithQueue(q))
	require.NoError(t, err)

	// Exactly one starts; the other waits for the single worker slot. The live
	// worker reloads worker_concurrency=1 from the database on its next poll.
	<-wcStarted
	time.Sleep(1500 * time.Millisecond)
	require.Equal(t, int64(1), wcActive.Load(), "expected only one workflow to run with worker concurrency 1")

	releaseWC.Set()
	_, err = hX.GetResult()
	require.NoError(t, err)
	_, err = hY.GetResult()
	require.NoError(t, err)
	require.Equal(t, int64(1), wcMax.Load(), "peak observed concurrency should be 1")

	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up")
}

// TestDatabaseBackedQueueRespawnAfterDelete verifies that deleting a queue stops
// its worker, re-registering it under the same name respawns the worker, and a
// workflow that was left ENQUEUED across the delete is then dequeued and run.
// This works because dequeue matches by queue name, so the new queue row (with a
// fresh queue_id) still picks up the orphaned workflow.
func TestDatabaseBackedQueueRespawnAfterDelete(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	blockerStarted := NewEvent()
	releaseBlocker := NewEvent()
	// Release the blocker even if a require fails first: an unreleased blocker
	// leaks its workflow goroutine past Shutdown's timeout and fails the leak
	// check of every subsequent test.
	t.Cleanup(releaseBlocker.Set)
	respawnWorkflow := func(_ Context, input string) (string, error) {
		if input == "blocker" {
			blockerStarted.Set()
			releaseBlocker.Wait()
		}
		return input, nil
	}
	RegisterWorkflow(dbosCtx, respawnWorkflow)

	require.NoError(t, Launch(dbosCtx))

	const queueName = "respawn-queue"
	// Global concurrency of 1: a single blocking workflow holds the only slot,
	// keeping the target workflow ENQUEUED (undispatched) throughout the
	// delete/re-register cycle regardless of worker liveness.
	respawnQueue, err := registerWFQ(dbosCtx, queueName, WithGlobalConcurrency(1), WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)

	// Occupy the only concurrency slot.
	hBlocker, err := RunWorkflow(dbosCtx, respawnWorkflow, "blocker", WithQueue(respawnQueue))
	require.NoError(t, err)
	blockerStarted.Wait()

	// Enqueue the target; it stays ENQUEUED behind the blocker.
	hTarget, err := RunWorkflow(dbosCtx, respawnWorkflow, "target", WithQueue(respawnQueue))
	require.NoError(t, err)
	time.Sleep(1 * time.Second)
	stTarget, err := hTarget.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusEnqueued, stTarget.Status, "target should be enqueued behind the blocker")

	// Delete the queue. The supervisor drops it on its next reconcile and the
	// worker stops; the target's workflow_status row is untouched.
	require.NoError(t, DeleteQueue(dbosCtx, queueName))
	require.Eventually(t, func() bool {
		_, err := retrieveWFQ(dbosCtx, queueName)
		return errors.Is(err, ErrQueueNotFound)
	}, 3*time.Second, 50*time.Millisecond, "queue should be deleted")

	// Wait long enough for the worker to observe the deletion and stop, then
	// confirm the target is still enqueued (the delete did not orphan it).
	time.Sleep(1500 * time.Millisecond)
	stTarget, err = hTarget.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusEnqueued, stTarget.Status, "target should remain enqueued while the queue is gone")

	// Re-register the queue under the same name; the supervisor respawns a worker.
	_, err = registerWFQ(dbosCtx, queueName, WithGlobalConcurrency(1), WithQueueBasePollingInterval(50*time.Millisecond))
	require.NoError(t, err)

	// Free the slot. The respawned worker must dequeue the previously-enqueued
	// target and run it to completion.
	releaseBlocker.Set()
	rBlocker, err := hBlocker.GetResult()
	require.NoError(t, err)
	require.Equal(t, "blocker", rBlocker)

	rTarget, err := hTarget.GetResult()
	require.NoError(t, err)
	require.Equal(t, "target", rTarget)

	require.True(t, queueEntriesAreCleanedUp(dbosCtx), "expected queue entries to be cleaned up")
}

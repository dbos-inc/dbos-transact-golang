package dbos

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, true, true)

	// Create queue for communication between client and server
	queue := NewWorkflowQueue(serverCtx, "client-enqueue-queue")

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

	// Launch the server context to start processing tasks
	err := serverCtx.Launch()
	if err != nil {
		t.Fatalf("failed to launch server DBOS instance: %v", err)
	}

	// Setup client context - this will enqueue tasks
	clientCtx := setupDBOS(t, false, false) // Don't drop DB, don't check for leaks

	t.Run("EnqueueAndGetResult", func(t *testing.T) {
		// Client enqueues a task using the new Enqueue method
		handle, err := Enqueue[wfInput, string](clientCtx, GenericEnqueueOptions[wfInput]{
			WorkflowName:       "ServerWorkflow",
			QueueName:          queue.Name,
			WorkflowInput:      wfInput{Input: "test-input"},
			ApplicationVersion: serverCtx.GetApplicationVersion(),
		})
		if err != nil {
			t.Fatalf("failed to enqueue workflow from client: %v", err)
		}

		// Verify we got a polling handle
		_, ok := handle.(*workflowPollingHandle[string])
		if !ok {
			t.Fatalf("expected handle to be of type workflowPollingHandle, got %T", handle)
		}

		// Client retrieves the result
		result, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from enqueued workflow: %v", err)
		}

		expectedResult := "processed: test-input"
		if result != expectedResult {
			t.Fatalf("expected result to be '%s', got '%s'", expectedResult, result)
		}

		// Verify the workflow status
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}

		if status.Status != WorkflowStatusSuccess {
			t.Fatalf("expected workflow status to be SUCCESS, got %v", status.Status)
		}

		if status.Name != "ServerWorkflow" {
			t.Fatalf("expected workflow name to be 'ServerWorkflow', got '%s'", status.Name)
		}

		if status.QueueName != queue.Name {
			t.Fatalf("expected queue name to be '%s', got '%s'", queue.Name, status.QueueName)
		}

		if !queueEntriesAreCleanedUp(serverCtx) {
			t.Fatal("expected queue entries to be cleaned up after global concurrency test")
		}
	})

	t.Run("EnqueueWithCustomWorkflowID", func(t *testing.T) {
		customWorkflowID := "custom-client-workflow-id"

		// Client enqueues a task with a custom workflow ID
		_, err := Enqueue[wfInput, string](clientCtx, GenericEnqueueOptions[wfInput]{
			WorkflowName:  "ServerWorkflow",
			QueueName:     queue.Name,
			WorkflowID:    customWorkflowID,
			WorkflowInput: wfInput{Input: "test-input"},
		})
		if err != nil {
			t.Fatalf("failed to enqueue workflow with custom ID: %v", err)
		}

		// Verify the workflow ID is what we set
		retrieveHandle, err := RetrieveWorkflow[string](clientCtx, customWorkflowID)
		if err != nil {
			t.Fatalf("failed to retrieve workflow by custom ID: %v", err)
		}

		result, err := retrieveHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from retrieved workflow: %v", err)
		}

		if result != "processed: test-input" {
			t.Fatalf("expected retrieved workflow result to be 'processed: test-input', got '%s'", result)
		}

		if !queueEntriesAreCleanedUp(serverCtx) {
			t.Fatal("expected queue entries to be cleaned up after global concurrency test")
		}
	})

	t.Run("EnqueueWithTimeout", func(t *testing.T) {
		handle, err := Enqueue[string, string](clientCtx, GenericEnqueueOptions[string]{
			WorkflowName:    "BlockingWorkflow",
			QueueName:       queue.Name,
			WorkflowInput:   "blocking-input",
			WorkflowTimeout: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("failed to enqueue blocking workflow: %v", err)
		}

		// Should timeout when trying to get result
		_, err = handle.GetResult()
		if err == nil {
			t.Fatal("expected timeout error, but got none")
		}

		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != AwaitedWorkflowCancelled {
			t.Fatalf("expected error code to be AwaitedWorkflowCancelled, got %v", dbosErr.Code)
		}

		// Verify workflow is cancelled
		status, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}

		if status.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be CANCELLED, got %v", status.Status)
		}
	})

	// Verify all queue entries are cleaned up
	if !queueEntriesAreCleanedUp(serverCtx) {
		t.Fatal("expected queue entries to be cleaned up after client tests")
	}
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

	// Launch the server context to start processing tasks
	err := serverCtx.Launch()
	if err != nil {
		t.Fatalf("failed to launch server DBOS instance: %v", err)
	}

	// Setup client context - this will enqueue tasks
	clientCtx := setupDBOS(t, false, false) // Don't drop DB, don't check for leaks

	t.Run("CancelAndResume", func(t *testing.T) {
		// Reset the global counter
		stepsCompleted = 0
		input := 5
		workflowID := "test-cancel-resume-workflow"

		// Start the workflow - it will execute step one and then wait
		handle, err := Enqueue[int, int](clientCtx, GenericEnqueueOptions[int]{
			WorkflowName:       "CancelResumeWorkflow",
			QueueName:          queue.Name,
			WorkflowID:         workflowID,
			WorkflowInput:      input,
			ApplicationVersion: serverCtx.GetApplicationVersion(),
		})
		if err != nil {
			t.Fatalf("failed to enqueue workflow from client: %v", err)
		}

		// Wait for workflow to signal it has started and step one completed
		workflowStarted.Wait()

		// Verify step one completed but step two hasn't
		if stepsCompleted != 1 {
			t.Fatalf("expected steps completed to be 1, got %d", stepsCompleted)
		}

		// Cancel the workflow
		err = clientCtx.CancelWorkflow(workflowID)
		if err != nil {
			t.Fatalf("failed to cancel workflow: %v", err)
		}

		// Verify workflow is cancelled
		cancelStatus, err := handle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get workflow status: %v", err)
		}

		if cancelStatus.Status != WorkflowStatusCancelled {
			t.Fatalf("expected workflow status to be CANCELLED, got %v", cancelStatus.Status)
		}

		// Resume the workflow
		resumeHandle, err := ResumeWorkflow[int](clientCtx, workflowID)
		if err != nil {
			t.Fatalf("failed to resume workflow: %v", err)
		}

		// Wait for workflow completion
		proceedSignal.Set() // Allow the workflow to proceed to step two
		result, err := resumeHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from resumed workflow: %v", err)
		}

		// Verify the result
		if result != input {
			t.Fatalf("expected result to be %d, got %d", input, result)
		}

		// Verify both steps completed
		if stepsCompleted != 2 {
			t.Fatalf("expected steps completed to be 2, got %d", stepsCompleted)
		}

		// Check final status
		finalStatus, err := resumeHandle.GetStatus()
		if err != nil {
			t.Fatalf("failed to get final workflow status: %v", err)
		}

		if finalStatus.Status != WorkflowStatusSuccess {
			t.Fatalf("expected final workflow status to be SUCCESS, got %v", finalStatus.Status)
		}

		// After resume, the queue name should change to the internal queue name
		if finalStatus.QueueName != _DBOS_INTERNAL_QUEUE_NAME {
			t.Fatalf("expected queue name to be %s, got '%s'", _DBOS_INTERNAL_QUEUE_NAME, finalStatus.QueueName)
		}

		// Resume the workflow again - should not run again
		resumeAgainHandle, err := ResumeWorkflow[int](clientCtx, workflowID)
		if err != nil {
			t.Fatalf("failed to resume workflow again: %v", err)
		}

		resultAgain, err := resumeAgainHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from second resume: %v", err)
		}

		if resultAgain != input {
			t.Fatalf("expected second resume result to be %d, got %d", input, resultAgain)
		}

		// Verify steps didn't run again
		if stepsCompleted != 2 {
			t.Fatalf("expected steps completed to remain 2 after second resume, got %d", stepsCompleted)
		}

		if !queueEntriesAreCleanedUp(serverCtx) {
			t.Fatal("expected queue entries to be cleaned up after cancel/resume test")
		}
	})

	t.Run("CancelNonExistentWorkflow", func(t *testing.T) {
		nonExistentWorkflowID := "non-existent-workflow-id"

		// Try to cancel a non-existent workflow
		err := clientCtx.CancelWorkflow(nonExistentWorkflowID)
		if err == nil {
			t.Fatal("expected error when canceling non-existent workflow, but got none")
		}

		// Verify error type and code
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != NonExistentWorkflowError {
			t.Fatalf("expected error code to be NonExistentWorkflowError, got %v", dbosErr.Code)
		}

		if dbosErr.DestinationID != nonExistentWorkflowID {
			t.Fatalf("expected DestinationID to be %s, got %s", nonExistentWorkflowID, dbosErr.DestinationID)
		}
	})

	t.Run("ResumeNonExistentWorkflow", func(t *testing.T) {
		nonExistentWorkflowID := "non-existent-resume-workflow-id"

		// Try to resume a non-existent workflow
		_, err := ResumeWorkflow[int](clientCtx, nonExistentWorkflowID)
		fmt.Println(err)
		if err == nil {
			t.Fatal("expected error when resuming non-existent workflow, but got none")
		}

		// Verify error type and code
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != NonExistentWorkflowError {
			t.Fatalf("expected error code to be NonExistentWorkflowError, got %v", dbosErr.Code)
		}

		if dbosErr.DestinationID != nonExistentWorkflowID {
			t.Fatalf("expected DestinationID to be %s, got %s", nonExistentWorkflowID, dbosErr.DestinationID)
		}
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
		child1Handle, err := RunAsWorkflow(ctx, childWorkflow1, input)
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
		child2Handle, err := RunAsWorkflow(ctx, childWorkflow2, input)
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
	if err != nil {
		t.Fatalf("failed to launch server DBOS instance: %v", err)
	}

	// Setup client context
	clientCtx := setupDBOS(t, false, false)

	t.Run("ForkAtAllSteps", func(t *testing.T) {
		// Reset counters
		stepCount1, stepCount2, child1Count, child2Count = 0, 0, 0, 0

		originalWorkflowID := "original-workflow-fork-test"

		// 1. Run the entire workflow first and check counters are 1
		handle, err := Enqueue[string, string](clientCtx, GenericEnqueueOptions[string]{
			WorkflowName:       "ParentWorkflow",
			QueueName:          queue.Name,
			WorkflowID:         originalWorkflowID,
			WorkflowInput:      "test",
			ApplicationVersion: serverCtx.GetApplicationVersion(),
		})
		if err != nil {
			t.Fatalf("failed to enqueue original workflow: %v", err)
		}

		// Wait for the original workflow to complete
		result, err := handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from original workflow: %v", err)
		}

		expectedResult := "step1-test+step2-test+child1-test+child2-test"
		if result != expectedResult {
			t.Fatalf("expected result to be '%s', got '%s'", expectedResult, result)
		}

		// Verify all counters are 1 after original workflow
		if stepCount1 != 1 || stepCount2 != 1 || child1Count != 1 || child2Count != 1 {
			t.Fatalf("expected counters to be (step1:1, step2:1, child1:1, child2:1), got (step1:%d, step2:%d, child1:%d, child2:%d)", stepCount1, stepCount2, child1Count, child2Count)
		}

		// 2. Fork from each step 1 to 6 and verify results
		// Note: there's 6 steps: 2 steps 2 children and 2 GetResults
		for step := 1; step <= 6; step++ {
			t.Logf("Forking at step %d", step)

			customForkedWorkflowID := fmt.Sprintf("forked-workflow-step-%d", step)
			forkedHandle, err := ForkWorkflow[string](clientCtx, originalWorkflowID, WithForkWorkflowID(customForkedWorkflowID), WithForkStartStep(uint(step-1)))
			if err != nil {
				t.Fatalf("failed to fork workflow at step %d: %v", step, err)
			}

			forkedWorkflowID := forkedHandle.GetWorkflowID()
			if forkedWorkflowID != customForkedWorkflowID {
				t.Fatalf("expected forked workflow ID to be '%s', got '%s'", customForkedWorkflowID, forkedWorkflowID)
			}

			forkedResult, err := forkedHandle.GetResult()
			if err != nil {
				t.Fatalf("failed to get result from forked workflow at step %d: %v", step, err)
			}

			// 1) Verify workflow result is correct
			if forkedResult != expectedResult {
				t.Fatalf("forked workflow at step %d: expected result '%s', got '%s'", step, expectedResult, forkedResult)
			}

			// 2) Verify counters are at expected totals based on the step where we're forking
			t.Logf("Step %d: actual counters - step1:%d, step2:%d, child1:%d, child2:%d", step, stepCount1, stepCount2, child1Count, child2Count)

			// First step is executed only once
			if stepCount1 != 1+1 {
				t.Fatalf("forked workflow at step %d: step1 counter should be 2, got %d", step, stepCount1)
			}

			// First child will be executed twice
			if step < 3 {
				if child1Count != 1+step {
					t.Fatalf("forked workflow at step %d: child1 counter should be %d, got %d", step, 1+step, child1Count)
				}
			} else {
				if child1Count != 1+2 {
					t.Fatalf("forked workflow at step %d: child2 counter should be 3, got %d", step, child1Count)
				}
			}

			// Second step (in reality step 4) will be executed 4 times
			if step < 5 {
				if stepCount2 != 1+step {
					t.Fatalf("forked workflow at step %d: step2 counter should be %d, got %d", step, 1+step, stepCount2)
				}
			} else {
				if stepCount2 != 1+4 {
					t.Fatalf("forked workflow at step %d: step2 counter should be 5, got %d", step, stepCount2)
				}
			}

			// Second child will be executed 5 times
			if step < 6 {
				if child2Count != 1+step {
					t.Fatalf("forked workflow at step %d: child2 counter should be %d, got %d", step, 1+step, child2Count)
				}
			} else {
				if child2Count != 1+5 {
					t.Fatalf("forked workflow at step %d: child2 counter should be 6, got %d", step, child2Count)
				}
			}

			t.Logf("Step %d: all counter totals verified correctly", step)
		}

		t.Logf("Final counters after all forks - steps:%d, child1:%d, child2:%d", stepCount1, child1Count, child2Count)
	})

	t.Run("ForkNonExistentWorkflow", func(t *testing.T) {
		nonExistentWorkflowID := "non-existent-workflow-for-fork"

		// Try to fork a non-existent workflow
		_, err := clientCtx.ForkWorkflow(clientCtx, nonExistentWorkflowID, WithForkStartStep(1))
		if err == nil {
			t.Fatal("expected error when forking non-existent workflow, but got none")
		}

		// Verify error type
		dbosErr, ok := err.(*DBOSError)
		if !ok {
			t.Fatalf("expected error to be of type *DBOSError, got %T", err)
		}

		if dbosErr.Code != NonExistentWorkflowError {
			t.Fatalf("expected error code to be NonExistentWorkflowError, got %v", dbosErr.Code)
		}

		if dbosErr.DestinationID != nonExistentWorkflowID {
			t.Fatalf("expected DestinationID to be %s, got %s", nonExistentWorkflowID, dbosErr.DestinationID)
		}
	})

	t.Run("ForkWithInvalidStep", func(t *testing.T) {
		originalWorkflowID := "original-workflow-invalid-step"

		// Create an original workflow first
		handle, err := Enqueue[string, string](clientCtx, GenericEnqueueOptions[string]{
			WorkflowName:       "ParentWorkflow",
			QueueName:          queue.Name,
			WorkflowID:         originalWorkflowID,
			WorkflowInput:      "test",
			ApplicationVersion: serverCtx.GetApplicationVersion(),
		})
		if err != nil {
			t.Fatalf("failed to enqueue original workflow: %v", err)
		}

		// Wait for completion
		_, err = handle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from original workflow: %v", err)
		}

		// Try to fork at step 999 (beyond workflow's actual steps)
		_, err = clientCtx.ForkWorkflow(clientCtx, originalWorkflowID, WithForkStartStep(999))
		if err == nil {
			t.Fatal("expected error when forking at step 999, but got none")
		}
		// Verify the error message
		if !strings.Contains(err.Error(), "exceeds workflow's maximum step") {
			t.Fatalf("expected error message to contain 'exceeds workflow's maximum step', got: %v", err)
		}
	})

	// Verify all queue entries are cleaned up
	if !queueEntriesAreCleanedUp(serverCtx) {
		t.Fatal("expected queue entries to be cleaned up after fork workflow tests")
	}
}

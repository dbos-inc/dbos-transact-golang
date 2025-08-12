package dbos

import (
	"context"
	"fmt"
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

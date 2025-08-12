package dbos

import (
	"fmt"
	"testing"
	"time"
)

// Simple workflow that will be executed on the server
func serverWorkflow(ctx DBOSContext, input string) (string, error) {
	if input != "test-input" {
		return "", fmt.Errorf("unexpected input: %s", input)
	}
	return "processed: " + input, nil
}

func TestClientEnqueue(t *testing.T) {
	// Setup server context - this will process tasks
	serverCtx := setupDBOS(t, true, true)

	// Create queue for communication between client and server
	queue := NewWorkflowQueue(serverCtx, "client-enqueue-queue")

	// Register workflows with custom names so client can reference them
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
		handle, err := Enqueue[string, string](clientCtx, GenericEnqueueOptions[string]{
			WorkflowName:       "ServerWorkflow",
			QueueName:          queue.Name,
			WorkflowInput:      "test-input",
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
		_, err := Enqueue[string, string](clientCtx, GenericEnqueueOptions[string]{
			WorkflowName:  "ServerWorkflow",
			QueueName:     queue.Name,
			WorkflowID:    customWorkflowID,
			WorkflowInput: "test-input",
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

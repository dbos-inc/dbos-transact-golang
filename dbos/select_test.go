package dbos

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func simpleSelectWorkflow(ctx DBOSContext, input string) (string, error) {
	result, err := Select(ctx, []SelectCase[string]{
		{Topic: "test-topic-1", Timeout: timeoutPtr(1 * time.Second)},
		{Topic: "test-topic-2", Timeout: timeoutPtr(1 * time.Second)},
		{IsDefault: true},
	}, WithStepName("testSelect"))
	
	if err != nil {
		return "", err
	}
	
	switch result.CaseIndex {
	case 0:
		return "received-from-topic-1: " + result.Value, nil
	case 1:
		return "received-from-topic-2: " + result.Value, nil
	case 2:
		return "default-case", nil
	default:
		return "unknown-case", nil
	}
}

func selectReceiverWorkflow(ctx DBOSContext, topics []string) (string, error) {
	cases := make([]SelectCase[string], len(topics)+1)
	
	// Create receive cases for each topic
	for i, topic := range topics {
		timeout := 2 * time.Second
		cases[i] = SelectCase[string]{
			Topic:   topic,
			Timeout: &timeout,
			CaseID:  i,
		}
	}
	
	// Add default case
	cases[len(topics)] = SelectCase[string]{
		IsDefault: true,
		CaseID:    len(topics),
	}
	
	result, err := Select(ctx, cases, WithStepName("multiTopicSelect"))
	if err != nil {
		return "", err
	}
	
	if result.CaseIndex < len(topics) {
		return "received: " + result.Value + " from topic: " + topics[result.CaseIndex], nil
	}
	
	return "no-message-received", nil
}

type selectSenderInput struct {
	DestinationID string
	Topic         string
	Message       string
}

func selectSenderWorkflow(ctx DBOSContext, input selectSenderInput) (string, error) {
	err := Send(ctx, input.DestinationID, input.Message, input.Topic)
	if err != nil {
		return "", err
	}
	return "message-sent", nil
}

func selectValidationWorkflow1(ctx DBOSContext, input string) (string, error) {
	// Test empty cases
	_, err := Select(ctx, []SelectCase[string]{})
	if err != nil {
		return "empty-cases-error", nil
	}
	return "should-not-reach", nil
}

func selectValidationWorkflow2(ctx DBOSContext, input string) (string, error) {
	// Test multiple default cases
	_, err := Select(ctx, []SelectCase[string]{
		{IsDefault: true},
		{IsDefault: true},
	})
	if err != nil {
		return "multiple-defaults-error", nil
	}
	return "should-not-reach", nil
}

func TestDurableSelect(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dbosCtx := setupDBOS(t, true, false)

	// Register workflows
	RegisterWorkflow(dbosCtx, simpleSelectWorkflow)
	RegisterWorkflow(dbosCtx, selectReceiverWorkflow)
	RegisterWorkflow(dbosCtx, selectSenderWorkflow)
	RegisterWorkflow(dbosCtx, selectValidationWorkflow1)
	RegisterWorkflow(dbosCtx, selectValidationWorkflow2)

	// Launch DBOS
	err := dbosCtx.Launch()
	require.NoError(t, err, "failed to launch DBOS context")

	t.Run("SelectDefaultCase", func(t *testing.T) {
		// Test select with only default case (no messages available)
		handle, err := RunWorkflow(dbosCtx, simpleSelectWorkflow, "test-input")
		require.NoError(t, err, "failed to start simple select workflow")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from simple select workflow")
		assert.Equal(t, "default-case", result, "expected default case to be selected")
	})

	t.Run("SelectReceiveMessage", func(t *testing.T) {
		// Set up receiver workflow first
		receiverWorkflowID := uuid.NewString()
		receiverHandle, err := RunWorkflow(dbosCtx, selectReceiverWorkflow, []string{"select-topic-1", "select-topic-2"}, 
			WithWorkflowID(receiverWorkflowID))
		require.NoError(t, err, "failed to start receiver workflow")

		// Give the receiver time to start
		time.Sleep(100 * time.Millisecond)

		// Send a message to topic-2
		senderHandle, err := RunWorkflow(dbosCtx, selectSenderWorkflow, selectSenderInput{
			DestinationID: receiverWorkflowID,
			Topic:         "select-topic-2",
			Message:       "hello-from-sender",
		})
		require.NoError(t, err, "failed to start sender workflow")

		// Wait for sender to complete
		senderResult, err := senderHandle.GetResult()
		require.NoError(t, err, "failed to get sender result")
		assert.Equal(t, "message-sent", senderResult)

		// Wait for receiver to complete
		receiverResult, err := receiverHandle.GetResult()
		require.NoError(t, err, "failed to get receiver result")
		assert.Equal(t, "received: hello-from-sender from topic: select-topic-2", receiverResult)
	})

	t.Run("SelectDeterminism", func(t *testing.T) {
		// Test that select operations are deterministic during recovery
		workflowID := uuid.NewString()
		
		// First execution
		handle1, err := RunWorkflow(dbosCtx, simpleSelectWorkflow, "determinism-test", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start first workflow execution")

		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first execution")

		// Second execution with same ID should return the same result (from recorded step)
		handle2, err := RunWorkflow(dbosCtx, simpleSelectWorkflow, "determinism-test", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start second workflow execution")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second execution")

		assert.Equal(t, result1, result2, "select results should be deterministic across executions")
	})

	t.Run("SelectStepRecovery", func(t *testing.T) {
		// Test that select steps are properly recorded and recovered
		workflowID := uuid.NewString()
		
		handle, err := RunWorkflow(dbosCtx, simpleSelectWorkflow, "recovery-test", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start workflow")

		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result")

		// Verify the step was recorded
		steps, err := GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps")
		
		// Should have one step for the select operation
		require.Equal(t, 1, len(steps), "expected exactly one step")
		assert.Equal(t, "testSelect", steps[0].StepName, "expected step name to be 'testSelect'")
		assert.Equal(t, 0, steps[0].StepID, "expected step ID to be 0")
	})

	t.Run("SelectValidation", func(t *testing.T) {
		// Test empty cases
		handle, err := RunWorkflow(dbosCtx, selectValidationWorkflow1, "validation-test")
		require.NoError(t, err, "failed to start validation workflow")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get validation result")
		assert.Equal(t, "empty-cases-error", result, "expected error for empty cases")

		// Test multiple default cases
		handle2, err := RunWorkflow(dbosCtx, selectValidationWorkflow2, "validation-test-2")
		require.NoError(t, err, "failed to start validation workflow 2")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get validation result 2")
		assert.Equal(t, "multiple-defaults-error", result2, "expected error for multiple default cases")
	})
}

func selectValidationWorkflow3(ctx DBOSContext, input string) (string, error) {
	_, err := Select(ctx, []SelectCase[string]{
		{}, // Invalid case - no Topic, SendTopic, or IsDefault
	})
	if err != nil {
		return "invalid-case-error", nil
	}
	return "should-not-reach", nil
}

func TestSelectErrorHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	dbosCtx := setupDBOS(t, true, false)
	
	// Register workflows
	RegisterWorkflow(dbosCtx, selectValidationWorkflow3)

	err := dbosCtx.Launch()
	require.NoError(t, err, "failed to launch DBOS context")

	t.Run("NilContext", func(t *testing.T) {
		_, err := Select[string](nil, []SelectCase[string]{{IsDefault: true}})
		require.Error(t, err, "expected error for nil context")
		assert.Contains(t, err.Error(), "ctx cannot be nil")
	})

	t.Run("InvalidCase", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, selectValidationWorkflow3, "invalid-case-test")
		require.NoError(t, err, "failed to start workflow")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "invalid-case-error", result, "expected error for invalid case")
	})
}

// Helper function to create a timeout pointer
func timeoutPtr(d time.Duration) *time.Duration {
	return &d
}
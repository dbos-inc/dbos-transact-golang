package dbos

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Global debouncer variables for test workflows
var debouncer10sTimeout Debouncer[string, string]
var debouncer200msTimeout Debouncer[string, string]

// Helper test workflows
func debounceTestWorkflow(ctx DBOSContext, input string) (string, error) {
	return input, nil
}

// Helper workflow that calls Debounce from within a workflow
// Can handle both single and multiple debounce calls
type debounceCallInput struct {
	Key    string        // Debounce key
	Delay  time.Duration // Debounce delay
	Inputs []string      // Single element for single call, multiple for multiple calls
}

func workflowThatCallsDebounce(ctx DBOSContext, input debounceCallInput) (string, error) {
	var lastHandle WorkflowHandle[string]
	var err error

	for _, inp := range input.Inputs {
		lastHandle, err = (&debouncer10sTimeout).Debounce(ctx, input.Key, input.Delay, inp, WithAssumedRole("test-role"))
		if err != nil {
			return "", err
		}

		// Verify we get a polling handle
		_, ok := lastHandle.(*workflowPollingHandle[string])
		if !ok {
			return "", fmt.Errorf("expected handle to be of type workflowPollingHandle, got %T", lastHandle)
		}
	}

	// Get result from the last debounce call
	result, err := lastHandle.GetResult()
	if err != nil {
		return "", err
	}
	return result, nil
}

func TestDebouncer(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Set internal queue polling interval to 100ms
	internalQueue := dbosCtx.(*dbosContext).queueRunner.workflowQueueRegistry[_DBOS_INTERNAL_QUEUE_NAME]
	internalQueue.basePollingInterval = 10 * time.Millisecond
	dbosCtx.(*dbosContext).queueRunner.workflowQueueRegistry[_DBOS_INTERNAL_QUEUE_NAME] = internalQueue

	// Register test workflows
	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	RegisterWorkflow(dbosCtx, workflowThatCallsDebounce)

	// Create debouncers after Launch (each workflow debouncer can only be registered once)
	debouncer10sTimeout = NewDebouncer(dbosCtx, debounceTestWorkflow, 10*time.Second)
	debouncer200msTimeout = NewDebouncer(dbosCtx, debounceTestWorkflow, 200*time.Millisecond)

	Launch(dbosCtx)

	t.Run("TestSingleDebounceCall", func(t *testing.T) {
		// Create a workflow that calls Debounce
		parentInput := debounceCallInput{
			Key:    "test-key-1",
			Delay:  500 * time.Millisecond,
			Inputs: []string{"test-input-1"},
		}

		startTime := time.Now()
		handle, err := RunWorkflow(dbosCtx, workflowThatCallsDebounce, parentInput)
		require.NoError(t, err, "failed to start workflow that calls debounce")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "test-input-1", result, "result should match input")

		// Verify execution happened approximately 500ms after first call
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "execution should take at least 450ms")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")

		// Verify steps are generated for msg ID generation and wf ID generation
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")

		// Find the steps for DBOS.Debounce.assignWorkflowID and DBOS.Debounce.assignMessageID
		foundWorkflowIDStep := false
		foundMessageIDStep := false
		for _, step := range steps {
			if step.StepName == "DBOS.debounce.assignWorkflowID" {
				foundWorkflowIDStep = true
				assert.Nil(t, step.Error, "workflow ID step should not have error")
			}
			if step.StepName == "DBOS.debounce.assignMessageID" {
				foundMessageIDStep = true
				assert.Nil(t, step.Error, "message ID step should not have error")
			}
		}
		assert.True(t, foundWorkflowIDStep, "should have DBOS.debounce.assignWorkflowID step")
		assert.True(t, foundMessageIDStep, "should have DBOS.debounce.assignMessageID step")
	})

	t.Run("TestMultipleCallsPushBackAndLatestInput", func(t *testing.T) {
		// Create a workflow that calls Debounce 5 times with delay=200ms
		parentInput := debounceCallInput{
			Key:    "test-key-2",
			Delay:  200 * time.Millisecond,
			Inputs: []string{"input-1", "input-2", "input-3", "input-4", "input-5"},
		}

		startTime := time.Now()
		handle, err := RunWorkflow(dbosCtx, workflowThatCallsDebounce, parentInput)
		require.NoError(t, err, "failed to start workflow that calls debounce multiple times")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "input-5", result, "result should match latest input")

		// Verify execution happened approximately 1 second after first call
		// 5 calls × 200ms = 1s, plus some overhead, e.g., for the 10ms sleeps between calls and the workflow itself
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 1000*time.Millisecond, "execution should take at least 1.2s")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")
	})

	t.Run("TestDelayGreaterThanTimeout", func(t *testing.T) {
		// Call Debounce directly with delay=2s (greater than timeout of 200ms)
		startTime := time.Now()
		handle, err := debouncer200msTimeout.Debounce(dbosCtx, "test-key-4", 2*time.Second, "timeout-input")
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
		handle1, err := debouncer10sTimeout.Debounce(dbosCtx, "test-key-5", 10*time.Second, "first-input")
		require.NoError(t, err, "failed to call Debounce from outside workflow (first call)")

		// Second call: Debounce with delay=0 (should trigger immediate execution)
		startTime := time.Now()
		handle2, err := debouncer10sTimeout.Debounce(dbosCtx, "test-key-5", 0, "second-input")
		require.NoError(t, err, "failed to call Debounce from outside workflow (second call)")

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
		handle1, err := debouncer10sTimeout.Debounce(dbosCtx, "different-key-1", 200*time.Millisecond, "input-key-1")
		require.NoError(t, err, "failed to call Debounce with first key")

		handle2, err := debouncer10sTimeout.Debounce(dbosCtx, "different-key-2", 200*time.Millisecond, "input-key-2")
		require.NoError(t, err, "failed to call Debounce with second key")

		handle3, err := debouncer10sTimeout.Debounce(dbosCtx, "different-key-3", 200*time.Millisecond, "input-key-3")
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
		handle1, err := debouncer10sTimeout.Debounce(dbosCtx, "independent-key-1", 5*time.Second, "independent-1")
		require.NoError(t, err, "failed to call Debounce with first key")

		startTime2 := time.Now()
		handle2, err := debouncer10sTimeout.Debounce(dbosCtx, "independent-key-2", 200*time.Millisecond, "independent-2")
		require.NoError(t, err, "failed to call Debounce with second key")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second handle")
		assert.Equal(t, "independent-2", result2, "second handle should get its own input")

		// Verify key-2 executed independently (should complete before the 2s delay of key-1)
		elapsed2 := time.Since(startTime2)
		assert.GreaterOrEqual(t, elapsed2, 200*time.Millisecond, "key-2 should execute after its delay")
		assert.Less(t, elapsed2, 5*time.Second, "key-2 should not be affected by key-1's delay")

		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first handle")
		assert.Equal(t, "independent-1", result1, "first handle should get its own input")

	})
}

func TestDebouncerCannotBeCreatedAfterLaunch(t *testing.T) {
	// Set up a new DBOS context for this test (not launched)
	dbosCtx := setupDBOS(t, true, true)

	// Register a workflow for this test (reuse existing workflow)
	RegisterWorkflow(dbosCtx, debounceTestWorkflow)

	// Launch the context
	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS context")

	// Verify that creating a debouncer after launch panics
	assert.Panics(t, func() {
		NewDebouncer(dbosCtx, debounceTestWorkflow, 10*time.Second)
	}, "creating a debouncer after launch should panic")

	// Verify the panic is with the correct error type
	var panicErr *DBOSError
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				var ok bool
				panicErr, ok = r.(*DBOSError)
				if !ok {
					panic(r) // Re-panic if it's not the expected error type
				}
			}
		}()
		NewDebouncer(dbosCtx, debounceTestWorkflow, 10*time.Second)
	}()

	assert.True(t, panicked, "should have panicked")
	require.NotNil(t, panicErr, "panic error should not be nil")
	assert.Equal(t, InitializationError, panicErr.Code, "error code should be InitializationError")
	assert.Contains(t, panicErr.Message, "cannot create debouncer after DBOS has launched", "error message should mention debouncer creation after launch")
}

func TestDebouncerWorkflowOptions(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	testQueue := NewWorkflowQueue(dbosCtx, "debouncer-options-test-queue", WithPriorityEnabled(), WithPartitionQueue())

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)

	debouncer := NewDebouncer(dbosCtx, debounceTestWorkflow, 10*time.Second)

	Launch(dbosCtx)

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
		dbosCtx,
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
	workflows, err := ListWorkflows(dbosCtx, WithWorkflowIDs([]string{workflowID}))
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

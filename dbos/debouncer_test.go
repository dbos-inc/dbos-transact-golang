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

// Separate workflow for timeout test (needs different debouncer configuration)
func debounceTestWorkflowTimeout(ctx DBOSContext, input string) (string, error) {
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
			fmt.Println("error in debounce call", err)
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
	internalQueue.basePollingInterval = 100 * time.Millisecond
	dbosCtx.(*dbosContext).queueRunner.workflowQueueRegistry[_DBOS_INTERNAL_QUEUE_NAME] = internalQueue

	// Register test workflows
	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	RegisterWorkflow(dbosCtx, workflowThatCallsDebounce)
	RegisterWorkflow(dbosCtx, debounceTestWorkflowTimeout)

	// Create debouncers after Launch (each workflow debouncer can only be registered once)
	debouncer10sTimeout = NewDebouncer(dbosCtx, debounceTestWorkflow, 10*time.Second)
	debouncer200msTimeout = NewDebouncer(dbosCtx, debounceTestWorkflowTimeout, 200*time.Millisecond)

	Launch(dbosCtx)

	t.Run("Basic", func(t *testing.T) {
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
		})

		t.Run("TestDelayGreaterThanTimeout", func(t *testing.T) {
			// Temporarily use the timeout debouncer for this test
			originalDebouncer := debouncer10sTimeout
			debouncer10sTimeout = debouncer200msTimeout
			defer func() { debouncer10sTimeout = originalDebouncer }()

			// Create a workflow that calls Debounce with delay=2s (greater than timeout)
			parentInput := debounceCallInput{
				Key:    "test-key-4",
				Delay:  2 * time.Second,
				Inputs: []string{"timeout-input"},
			}

			startTime := time.Now()
			handle, err := RunWorkflow(dbosCtx, workflowThatCallsDebounce, parentInput)
			require.NoError(t, err, "failed to start workflow that calls debounce with delay > timeout")

			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result")
			assert.Equal(t, "timeout-input", result, "result should match input")

			// Verify execution happened at timeout (1s), not delay (2s)
			elapsed := time.Since(startTime)
			assert.Less(t, elapsed, 2*time.Second, "execution should take less than 2s")
		})

		t.Run("TestOutsideWorkflow", func(t *testing.T) {
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
			assert.LessOrEqual(t, elapsed, 500*time.Millisecond, "execution should happen immediately with delay=0")
		})
	})
}

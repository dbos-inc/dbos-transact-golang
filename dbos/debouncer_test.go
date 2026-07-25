package dbos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Global debouncer variables for test workflows
var debouncer10sTimeout *Debouncer[string, string]
var debouncer200msTimeout *Debouncer[string, string]

// TestDebouncerCustomSerializer verifies debouncing works end-to-end with a
// custom serializer configured: the bounce must encode the new inputs with the
// same serializer as the initial enqueue so the coalesced workflow decodes them.
func TestDebouncerCustomSerializer(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	deb, err := NewDebouncer(dbosCtx, debounceTestWorkflow)
	require.NoError(t, err, "failed to create the debouncer")
	require.NoError(t, Launch(dbosCtx))

	h1, err := deb.Debounce(dbosCtx, "gob-debounce-key", 2*time.Second, "input-1")
	require.NoError(t, err, "first debounce call should enqueue the debounced workflow")

	// Second call while the debounced workflow is pending: bounces it with
	// gob-encoded inputs.
	h2, err := deb.Debounce(dbosCtx, "gob-debounce-key", 500*time.Millisecond, "input-2")
	require.NoError(t, err, "bounce must handle custom-serializer inputs")
	require.Equal(t, h1.GetWorkflowID(), h2.GetWorkflowID(), "both handles must target the first call's workflow")

	result, err := h2.GetResult()
	require.NoError(t, err)
	require.Equal(t, "input-2", result, "debounced workflow must run with the latest input")
}

// debouncePortableEchoWorkflow takes a portable args envelope and echoes the
// first positional arg, so a debounce coalescing test can assert the latest
// input survived the bounce.
func debouncePortableEchoWorkflow(ctx Context, args PortableWorkflowArgs) (string, error) {
	if len(args.PositionalArgs) == 0 {
		return "", fmt.Errorf("expected a positional arg")
	}
	s, ok := args.PositionalArgs[0].(string)
	if !ok {
		return "", fmt.Errorf("expected string positional arg, got %T", args.PositionalArgs[0])
	}
	return s, nil
}

// TestDebouncerPortableSerializer is the portable (cross-language) analog of
// TestDebouncerCustomSerializer: with portable (PortableWorkflowArgs) inputs the
// bounce must re-encode the new envelope with the same serializer as the initial
// enqueue so the coalesced workflow decodes and runs with the latest input.
func TestDebouncerPortableSerializer(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	RegisterWorkflow(dbosCtx, debouncePortableEchoWorkflow)
	deb, err := NewDebouncer(dbosCtx, debouncePortableEchoWorkflow)
	require.NoError(t, err, "failed to create the debouncer")
	require.NoError(t, Launch(dbosCtx))

	args1 := PortableWorkflowArgs{PositionalArgs: []any{"input-1"}}
	args2 := PortableWorkflowArgs{PositionalArgs: []any{"input-2"}}

	h1, err := deb.Debounce(dbosCtx, "portable-debounce-key", 10*time.Second, args1)
	require.NoError(t, err, "first debounce call should enqueue the debounced workflow")

	// Second call while the debounced workflow is pending: bounces it with the
	// portable-encoded envelope.
	h2, err := deb.Debounce(dbosCtx, "portable-debounce-key", 500*time.Millisecond, args2)
	require.NoError(t, err, "bounce must handle portable-envelope inputs")
	require.Equal(t, h1.GetWorkflowID(), h2.GetWorkflowID(), "both handles must target the first call's workflow")

	result, err := h2.GetResult()
	require.NoError(t, err)
	require.Equal(t, "input-2", result, "debounced workflow must run with the latest input")
}

// Helper test workflows
func debounceTestWorkflow(ctx Context, input string) (string, error) {
	return input, nil
}

// enqueueDedupInput carries the target coordinates for workflowEnqueuesWithDedup.
type enqueueDedupInput struct {
	QueueName string
	WfName    string
	DedupID   string
}

// workflowEnqueuesWithDedup enqueues a workflow under a deduplication ID from
// within a workflow. When the ID is already held, the DBOS.enqueue step fails
// with ErrQueueDeduplicated and that outcome is checkpointed, so a recovery
// replay decodes the error from the database instead of re-running the insert.
func workflowEnqueuesWithDedup(ctx Context, in enqueueDedupInput) (string, error) {
	_, err := Enqueue[string](ctx, in.QueueName, in.WfName, "conflicting-input",
		WithEnqueueDeduplicationID(in.DedupID))
	if err != nil {
		if errors.Is(err, ErrQueueDeduplicated) {
			return "dedup-detected", nil
		}
		return "unexpected: " + err.Error(), nil
	}
	return "no-conflict", nil
}

// Helper workflow that calls Debounce from within a workflow
// Can handle both single and multiple debounce calls
type debounceCallInput struct {
	Key    string        // Debounce key
	Delay  time.Duration // Debounce delay
	Inputs []string      // Single element for single call, multiple for multiple calls
}

func workflowThatCallsDebounce(ctx Context, input debounceCallInput) (string, error) {
	var lastHandle WorkflowHandle[string]
	var err error

	for _, inp := range input.Inputs {
		lastHandle, err = debouncer10sTimeout.Debounce(ctx, input.Key, input.Delay, inp, WithAssumedRole("test-role"))
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

// workflowThatCallsDebounceInStep calls Debounce from within a step, which is not allowed.
func workflowThatCallsDebounceInStep(ctx Context, input debounceCallInput) (string, error) {
	return RunAsStep(ctx, func(stepCtx context.Context) (string, error) {
		h, err := debouncer10sTimeout.Debounce(stepCtx.(Context), input.Key, input.Delay, input.Inputs[0])
		if err != nil {
			return "", err
		}
		return h.GetWorkflowID(), nil
	})
}

// assertDebounceCallerSteps verifies the exact step sequence of a workflow that
// called Debounce numCalls times: the first call assigns the workflow ID, bounces,
// and enqueues the debounced workflow; subsequent calls only assign and bounce;
// then the caller awaits the result.
func assertDebounceCallerSteps(t *testing.T, ctx Context, workflowID string, numCalls int) {
	t.Helper()
	expected := []string{"DBOS.debounce.assignWorkflowID", "DBOS.debounceDelayedWorkflow", "DBOS.enqueue"}
	for range numCalls - 1 {
		expected = append(expected, "DBOS.debounce.assignWorkflowID", "DBOS.debounceDelayedWorkflow")
	}
	expected = append(expected, "DBOS.getResult")

	steps, err := GetWorkflowSteps(ctx, workflowID)
	require.NoError(t, err, "failed to get workflow steps")
	require.Len(t, steps, len(expected), "unexpected step count for %d debounce calls", numCalls)
	for i, name := range expected {
		assert.Equal(t, name, steps[i].StepName, "step %d", i)
		assert.Nil(t, steps[i].Error, "step %d should not have an error", i)
	}
}

func TestDebouncer(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Set internal queue polling interval to 100ms
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	// Register test workflows
	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	RegisterWorkflow(dbosCtx, workflowThatCallsDebounce)
	RegisterWorkflow(dbosCtx, workflowThatCallsDebounceInStep)

	// Create debouncers after Launch (each workflow debouncer can only be registered once)
	var err error
	debouncer10sTimeout, err = NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(10*time.Second))
	require.NoError(t, err, "failed to create the 10s debouncer")
	debouncer200msTimeout, err = NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(200*time.Millisecond))
	require.NoError(t, err, "failed to create the 200ms debouncer")
	debouncer2sTimeout, err := NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(2*time.Second))
	require.NoError(t, err, "failed to create the 2s debouncer")

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

		// Verify the exact step sequence checkpointed by the caller
		assertDebounceCallerSteps(t, dbosCtx, handle.GetWorkflowID(), 1)

		// The debounced workflow itself is the only workflow on the internal queue
		workflows, err := ListWorkflows(dbosCtx, WithFilterQueueName(models.InternalQueueName))
		require.NoError(t, err, "failed to list workflows")
		require.Len(t, workflows, 1, "should have exactly one workflow in the internal queue")
		assert.True(t, workflows[0].IsDebounced, "queued workflow should be marked debounced")

		// Debounced workflows can be filtered
		debounced, err := ListWorkflows(dbosCtx, WithFilterIsDebounced(true))
		require.NoError(t, err, "failed to list debounced workflows")
		require.Len(t, debounced, 1, "should have exactly one debounced workflow")
		assert.Equal(t, workflows[0].ID, debounced[0].ID)
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
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "execution should take at least 200ms")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")

		// Each of the five calls checkpoints its two steps
		assertDebounceCallerSteps(t, dbosCtx, handle.GetWorkflowID(), 5)
	})

	t.Run("TestDelayGreaterThanTimeout", func(t *testing.T) {
		// Call Debounce directly with delay=6s (greater than timeout of 200ms)
		startTime := time.Now()
		handle, err := debouncer200msTimeout.Debounce(dbosCtx, "test-key-4", 6*time.Second, "timeout-input")
		require.NoError(t, err, "failed to call Debounce with delay > timeout")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "timeout-input", result, "result should match input")

		// Verify execution happened at timeout (200ms), not delay (6s). The upper
		// bound leaves room for the 1s DELAYED->ENQUEUED transition tick and
		// polling-handle latency.
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "execution should take at least 200ms")
		assert.LessOrEqual(t, elapsed, 4*time.Second, "execution should happen at the timeout, not the delay")
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
		assert.LessOrEqual(t, elapsed, 4*time.Second, "execution should happen promptly with delay=0")
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

	t.Run("TestRecoverDebouncedWorkflow", func(t *testing.T) {
		// Call Debounce directly using the 2 second timeout debouncer
		handle1, err := debouncer2sTimeout.Debounce(dbosCtx, "recovery-test-key", 200*time.Millisecond, "recovery-input-1")
		require.NoError(t, err, "failed to call Debounce")

		// Wait for it to exit
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")
		assert.Equal(t, "recovery-input-1", result1, "result should match input")

		dbosCtxInstance, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtxInstance.systemDB)

		// updateWorkflowOutcome refuses to overwrite terminal rows, so reset the
		// completed debounced workflow with the raw-SQL test helper and verify it
		// recovers to completion like any queued workflow.
		setWorkflowStatusPending(t, dbosCtx, handle1.GetWorkflowID())

		cleared, err := dbosCtxInstance.systemDB.ClearQueueAssignment(context.Background(), handle1.GetWorkflowID())
		require.NoError(t, err, "failed to clear queue assignment")
		require.True(t, cleared, "should have cleared queue assignment")

		recoveredHandle := newWorkflowPollingHandle[any](dbosCtx, handle1.GetWorkflowID())
		_, err = recoveredHandle.GetResult()
		require.NoError(t, err, "shouldn't have errored")
	})

	t.Run("TestCallerRecoveryReplay", func(t *testing.T) {
		parentInput := debounceCallInput{
			Key:    "caller-replay-key",
			Delay:  200 * time.Millisecond,
			Inputs: []string{"replay-1", "replay-2", "replay-3"},
		}
		handle, err := RunWorkflow(dbosCtx, workflowThatCallsDebounce, parentInput)
		require.NoError(t, err, "failed to start workflow that calls debounce")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "replay-3", result, "result should match latest input")
		assertDebounceCallerSteps(t, dbosCtx, handle.GetWorkflowID(), 3)

		debouncedBefore, err := ListWorkflows(dbosCtx, WithFilterIsDebounced(true))
		require.NoError(t, err, "failed to list debounced workflows")

		// Replay the caller from scratch: every Debounce call must come back from
		// its checkpoints without minting new steps or a new debounced workflow.
		setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowID())
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		var recovered WorkflowHandle[any]
		for _, h := range recoveredHandles {
			if h.GetWorkflowID() == handle.GetWorkflowID() {
				recovered = h
			}
		}
		require.NotNil(t, recovered, "the caller workflow should have been recovered")

		replayResult, err := recovered.GetResult()
		require.NoError(t, err, "the replayed caller should complete")
		assert.Equal(t, "replay-3", replayResult, "the replay must return the recorded result")

		assertDebounceCallerSteps(t, dbosCtx, handle.GetWorkflowID(), 3)
		debouncedAfter, err := ListWorkflows(dbosCtx, WithFilterIsDebounced(true))
		require.NoError(t, err, "failed to list debounced workflows")
		assert.Len(t, debouncedAfter, len(debouncedBefore), "replay must not enqueue a new debounced workflow")
	})

	t.Run("DebounceCannotBeCalledWithinStep", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, workflowThatCallsDebounceInStep, debounceCallInput{
			Key:    "within-step-key",
			Delay:  200 * time.Millisecond,
			Inputs: []string{"within-step-input"},
		})
		require.NoError(t, err, "failed to start workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Debounce within a step")

		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected error to be of type *Error, got %T", err)
		require.Equal(t, ErrorCodeStepExecution, dbosErr.Code)
		require.Contains(t, err.Error(), "cannot call Debounce within a step")
	})

	t.Run("NegativeDelayRejected", func(t *testing.T) {
		_, err := debouncer10sTimeout.Debounce(dbosCtx, "negative-delay-key", -time.Second, "input")
		require.Error(t, err, "a negative debounce delay must be rejected")
		assert.ErrorIs(t, err, ErrInvalidOption)
		assert.Contains(t, err.Error(), "cannot be negative")
	})
}

func TestDebouncerCreatedAfterLaunch(t *testing.T) {
	// Set up a new DBOS context for this test (not launched)
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register a workflow for this test (reuse existing workflow)
	RegisterWorkflow(dbosCtx, debounceTestWorkflow)

	// Launch the context
	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS context")

	// Debouncers no longer register an internal workflow, so creating one after
	// launch works.
	deb, err := NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(10*time.Second))
	require.NoError(t, err, "failed to create a debouncer after launch")
	handle, err := deb.Debounce(dbosCtx, "after-launch-key", 100*time.Millisecond, "after-launch-input")
	require.NoError(t, err, "failed to debounce with a debouncer created after launch")
	result, err := handle.GetResult()
	require.NoError(t, err, "failed to get result")
	assert.Equal(t, "after-launch-input", result)

	// Creating a debouncer for an unregistered workflow fails
	_, err = NewDebouncer(dbosCtx, func(ctx Context, input string) (string, error) { return input, nil })
	require.Error(t, err, "creating a debouncer for an unregistered workflow should fail")
	assert.ErrorIs(t, err, ErrNonExistentWorkflow)

	// So does creating one for a queue that is not registered in the database
	_, err = NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerQueue("no-such-queue"))
	require.Error(t, err, "creating a debouncer for an unregistered queue should fail")
	assert.ErrorIs(t, err, ErrQueueNotFound)
}

func TestDebouncerWorkflowOptions(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	testQueue, err := RegisterQueue(dbosCtx, "debouncer-options-test-queue")
	require.NoError(t, err)

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)

	debouncer, err := NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(10*time.Second), WithDebouncerQueue(testQueue.GetName()))
	require.NoError(t, err, "failed to create the debouncer")

	Launch(dbosCtx)

	// Test workflow options
	expectedWorkflowID := "test-workflow-id-12345"
	expectedAssumedRole := "test-assumed-role"
	expectedAuthenticatedUser := "test-user"
	expectedAuthenticatedRoles := []string{"role1", "role2", "role3"}
	testInput := "test-input-with-options"

	// Call Debounce with supported workflow options
	handle, err := debouncer.Debounce(
		dbosCtx,
		"workflow-options-key",
		200*time.Millisecond,
		testInput,
		WithWorkflowID(expectedWorkflowID),
		WithAssumedRole(expectedAssumedRole),
		WithAuthenticatedUser(expectedAuthenticatedUser),
		WithAuthenticatedRoles(expectedAuthenticatedRoles...),
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
	workflows, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(workflowID))
	require.NoError(t, err, "failed to list workflows")
	require.Len(t, workflows, 1, "should find exactly one workflow")

	workflow := workflows[0]

	// Verify all workflow options are set correctly
	assert.Equal(t, expectedWorkflowID, workflow.ID, "workflow ID should match")
	assert.Equal(t, testQueue.GetName(), workflow.QueueName, "queue name should match")
	assert.Equal(t, expectedAssumedRole, workflow.AssumedRole, "assumed role should match")
	assert.Equal(t, expectedAuthenticatedUser, workflow.AuthenticatedUser, "authenticated user should match")
	assert.Equal(t, expectedAuthenticatedRoles, workflow.AuthenticatedRoles, "authenticated roles should match")
	assert.Equal(t, WorkflowStatusSuccess, workflow.Status, "workflow should have succeeded")

	// Options a debounce owns or cannot support are rejected
	for _, tc := range []struct {
		name string
		opt  WorkflowOption
	}{
		{"Queue", WithQueue(testQueue)},
		{"DeduplicationID", WithDeduplicationID("user-dedup")},
		{"Delay", WithDelay(time.Second)},
		{"Priority", WithPriority(5)},
		{"QueuePartitionKey", WithQueuePartitionKey("pk")},
		{"DeduplicationPolicy", WithDeduplicationPolicy(DeduplicationPolicyReturnExisting)},
	} {
		_, err := debouncer.Debounce(dbosCtx, "rejected-options-key", 200*time.Millisecond, testInput, tc.opt)
		assert.Error(t, err, "option %s should be rejected", tc.name)
	}
}

// TestDebouncerConfiguredInstance verifies a debouncer can target a workflow method
// registered on a configured instance via WithDebouncerInstance, and that each
// debouncer runs the target on its own instance.
func TestDebouncerConfiguredInstance(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	slack := &configuredNotifier{channel: "slack"}
	email := &configuredNotifier{channel: "email"}
	RegisterWorkflow(dbosCtx, slack.Send, WithInstance(slack))
	RegisterWorkflow(dbosCtx, email.Send, WithInstance(email))

	// Without the instance, the bare (colliding) FQN was never registered: fail loudly
	_, err := NewDebouncer(dbosCtx, slack.Send)
	require.Error(t, err, "creating a debouncer for an instance method without WithDebouncerInstance should fail")

	slackDebouncer, err := NewDebouncer(dbosCtx, slack.Send, WithDebouncerInstance(slack))
	require.NoError(t, err, "failed to create the slack debouncer")
	emailDebouncer, err := NewDebouncer(dbosCtx, email.Send, WithDebouncerInstance(email))
	require.NoError(t, err, "failed to create the email debouncer")

	require.NoError(t, Launch(dbosCtx))

	handle, err := slackDebouncer.Debounce(dbosCtx, "slack-key", 100*time.Millisecond, "hi")
	require.NoError(t, err, "failed to debounce on the slack instance")
	result, err := handle.GetResult()
	require.NoError(t, err, "failed to get result from the slack instance")
	assert.Equal(t, "slack: hi", result, "debounced workflow should run on the slack instance")

	handle, err = emailDebouncer.Debounce(dbosCtx, "email-key", 100*time.Millisecond, "hi")
	require.NoError(t, err, "failed to debounce on the email instance")
	result, err = handle.GetResult()
	require.NoError(t, err, "failed to get result from the email instance")
	assert.Equal(t, "email: hi", result, "debounced workflow should run on the email instance")
}

// TestClassifyBounce covers the four bounce classifications, including the
// transient-race retry that is impractical to reproduce end-to-end.
func TestClassifyBounce(t *testing.T) {
	id := "holder-id"
	assert.Equal(t, bounceReturn, classifyBounce(&sysdb.DebounceResult{BouncedWorkflowID: &id}, "wf"))
	assert.Equal(t, bounceEnqueue, classifyBounce(&sysdb.DebounceResult{}, "wf"))
	assert.Equal(t, bounceRaise, classifyBounce(&sysdb.DebounceResult{HolderWorkflowID: &id, HolderIsDebounced: false, HolderWorkflowName: "wf"}, "wf"),
		"a non-debounced holder of the key is a conflict")
	assert.Equal(t, bounceRaise, classifyBounce(&sysdb.DebounceResult{HolderWorkflowID: &id, HolderIsDebounced: true, HolderWorkflowName: "other"}, "wf"),
		"a debounced holder for another workflow is a key collision")
	assert.Equal(t, bounceRetry, classifyBounce(&sysdb.DebounceResult{HolderWorkflowID: &id, HolderIsDebounced: true, HolderWorkflowName: "wf"}, "wf"),
		"a same-name debounced holder that left DELAYED mid-bounce is retried")
}

// TestDebounceDeadlineCapsBounce verifies a bounce cannot push the start past
// the deadline captured when the workflow was first enqueued.
func TestDebounceDeadlineCapsBounce(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	deb, err := NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(2*time.Second))
	require.NoError(t, err, "failed to create the debouncer")
	require.NoError(t, Launch(dbosCtx))

	start := time.Now()
	h1, err := deb.Debounce(dbosCtx, "cap-key", 500*time.Millisecond, "first-input")
	require.NoError(t, err, "first debounce call should enqueue the workflow")

	// Bounce far past the deadline: the extension must be clamped to it.
	h2, err := deb.Debounce(dbosCtx, "cap-key", 30*time.Second, "second-input")
	require.NoError(t, err, "bounce past the deadline should succeed")
	require.Equal(t, h1.GetWorkflowID(), h2.GetWorkflowID(), "the second call must bounce the first call's workflow")

	result, err := h2.GetResult()
	require.NoError(t, err, "failed to get result")
	assert.Equal(t, "second-input", result, "the bounce must replace the inputs")

	// The workflow fired at the deadline (~2s in): later than the original 500ms
	// delay, far earlier than the requested 30s. The upper bound leaves room for
	// the 1s DELAYED->ENQUEUED transition tick and polling-handle latency.
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 1900*time.Millisecond, "the bounce should have extended the delay up to the deadline")
	assert.LessOrEqual(t, elapsed, 6*time.Second, "the bounce must be capped at the deadline, not honor the 30s delay")
}

func debounceCollideWorkflowA(ctx Context, input string) (string, error) {
	return input, nil
}

func debounceCollideWorkflowAX(ctx Context, input string) (string, error) {
	return input, nil
}

// TestDebounceKeyConflicts verifies conflicting holders of a debounce key are
// surfaced as deduplication errors rather than bounced.
func TestDebounceKeyConflicts(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	testQueue, err := RegisterQueue(dbosCtx, "debounce-conflict-queue")
	require.NoError(t, err)

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	// Names crafted so "debounce-collide-a" + key "x-k" and "debounce-collide-a-x"
	// + key "k" both yield the deduplication ID "debounce-collide-a-x-k"
	RegisterWorkflow(dbosCtx, debounceCollideWorkflowA, WithWorkflowName("debounce-collide-a"))
	RegisterWorkflow(dbosCtx, debounceCollideWorkflowAX, WithWorkflowName("debounce-collide-a-x"))
	require.NoError(t, Launch(dbosCtx))

	t.Run("NonDebouncedHolder", func(t *testing.T) {
		// A regular delayed enqueue holds the deduplication ID the debouncer would use
		fqn := resolveWorkflowFunctionName(debounceTestWorkflow)
		holder, err := Enqueue[string](dbosCtx, testQueue.GetName(), fqn, "holder-input",
			WithEnqueueDeduplicationID(fqn+"-conflict-key"),
			WithEnqueueDelay(time.Minute))
		require.NoError(t, err, "failed to enqueue the conflicting holder")

		deb, err := NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerQueue(testQueue.GetName()))
		require.NoError(t, err, "failed to create the debouncer")
		_, err = deb.Debounce(dbosCtx, "conflict-key", 100*time.Millisecond, "debounced-input")
		require.Error(t, err, "debouncing over a non-debounced holder of the key must fail")
		assert.True(t, errors.Is(err, ErrQueueDeduplicated), "expected ErrQueueDeduplicated, got %v", err)

		require.NoError(t, CancelWorkflow(dbosCtx, holder.GetWorkflowID()))
	})

	t.Run("CollidingDebouncers", func(t *testing.T) {
		debA, err := NewDebouncer(dbosCtx, debounceCollideWorkflowA)
		require.NoError(t, err, "failed to create the first debouncer")
		debAX, err := NewDebouncer(dbosCtx, debounceCollideWorkflowAX)
		require.NoError(t, err, "failed to create the second debouncer")

		holder, err := debA.Debounce(dbosCtx, "x-k", time.Minute, "a-input")
		require.NoError(t, err, "failed to debounce the first workflow")

		_, err = debAX.Debounce(dbosCtx, "k", 100*time.Millisecond, "ax-input")
		require.Error(t, err, "a debounce-key collision between different workflows must fail")
		assert.True(t, errors.Is(err, ErrQueueDeduplicated), "expected ErrQueueDeduplicated, got %v", err)

		require.NoError(t, CancelWorkflow(dbosCtx, holder.GetWorkflowID()))
	})
}

// TestRecoveryEnqueueWithDedupID exercises the Enqueue deduplication-conflict
// path where the error is read back from the database and deserialized rather
// than produced live. A workflow enqueues under a held dedup ID; the DBOS.enqueue
// step records the ErrQueueDeduplicated outcome. The holder is then removed and
// the caller recovered: on replay the enqueue step is served from its checkpoint,
// so a surviving "dedup-detected" result proves the persisted error decoded back
// into a value that still matches ErrQueueDeduplicated. A live re-run (holder
// gone) would instead succeed and return "no-conflict". Both the gob (Go<->Go)
// and portable (cross-language) error encodings are covered.
func TestRecoveryEnqueueWithDedupID(t *testing.T) {
	run := func(t *testing.T, portable bool) {
		dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

		queue, err := RegisterQueue(dbosCtx, "recovery-dedup-queue")
		require.NoError(t, err)
		RegisterWorkflow(dbosCtx, debounceTestWorkflow)
		RegisterWorkflow(dbosCtx, workflowEnqueuesWithDedup)
		require.NoError(t, Launch(dbosCtx))

		fqn := resolveWorkflowFunctionName(debounceTestWorkflow)
		dedupID := "recovery-dedup-id"

		// A delayed enqueue holds the dedup slot the workflow will collide with.
		holder, err := Enqueue[string](dbosCtx, queue.GetName(), fqn, "holder-input",
			WithEnqueueDeduplicationID(dedupID),
			WithEnqueueDelay(time.Minute))
		require.NoError(t, err, "failed to enqueue the holder")

		var wfOpts []WorkflowOption
		if portable {
			wfOpts = append(wfOpts, WithPortableWorkflow())
		}
		in := enqueueDedupInput{QueueName: queue.GetName(), WfName: fqn, DedupID: dedupID}
		handle, err := RunWorkflow(dbosCtx, workflowEnqueuesWithDedup, in, wfOpts...)
		require.NoError(t, err, "failed to start the enqueueing workflow")

		result, err := handle.GetResult()
		require.NoError(t, err, "the workflow itself should not error")
		require.Equal(t, "dedup-detected", result, "the live enqueue should hit the dedup conflict")

		// Free the dedup slot: a live re-execution of the enqueue step would now
		// succeed, so any surviving "dedup-detected" after recovery must come from
		// the checkpointed (and decoded) step error, not a fresh insert.
		require.NoError(t, CancelWorkflow(dbosCtx, holder.GetWorkflowID()))

		// Recover the caller from scratch; the enqueue step replays from its
		// checkpoint, decoding the persisted ErrQueueDeduplicated.
		setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowID())
		recovered, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		var replayed WorkflowHandle[any]
		for _, h := range recovered {
			if h.GetWorkflowID() == handle.GetWorkflowID() {
				replayed = h
			}
		}
		require.NotNil(t, replayed, "the caller workflow should have been recovered")

		replayResult, err := replayed.GetResult()
		require.NoError(t, err, "the replayed caller should complete")
		assert.Equal(t, "dedup-detected", replayResult,
			"the checkpointed dedup error must decode back into an ErrQueueDeduplicated match")
	}

	t.Run("Gob", func(t *testing.T) { run(t, false) })
	t.Run("Portable", func(t *testing.T) { run(t, true) })
}

// TestConcurrentDebounce races concurrent Debounce calls on one key: every call
// must land on a single workflow, including callers that lose the enqueue race
// and loop around to bounce the winner's workflow.
func TestConcurrentDebounce(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	deb, err := NewDebouncer(dbosCtx, debounceTestWorkflow)
	require.NoError(t, err, "failed to create the debouncer")
	require.NoError(t, Launch(dbosCtx))

	const callers = 8
	handles := make([]WorkflowHandle[string], callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			handles[i], errs[i] = deb.Debounce(dbosCtx, "concurrent-key", 2*time.Second, fmt.Sprintf("input-%d", i))
		})
	}
	wg.Wait()

	for i := range callers {
		require.NoError(t, errs[i], "concurrent debounce call %d failed", i)
	}
	for i := 1; i < callers; i++ {
		assert.Equal(t, handles[0].GetWorkflowID(), handles[i].GetWorkflowID(), "all concurrent calls must coalesce on one workflow")
	}

	result, err := handles[0].GetResult()
	require.NoError(t, err, "failed to get result")
	assert.True(t, strings.HasPrefix(result, "input-"), "result should be one of the submitted inputs, got %q", result)

	// The key was released when the workflow started: a new call starts a new generation
	h, err := deb.Debounce(dbosCtx, "concurrent-key", 100*time.Millisecond, "next-generation")
	require.NoError(t, err, "failed to debounce after the previous generation ran")
	assert.NotEqual(t, handles[0].GetWorkflowID(), h.GetWorkflowID(), "a debounce after the workflow ran must start a new workflow")
	result, err = h.GetResult()
	require.NoError(t, err, "failed to get result of the new generation")
	assert.Equal(t, "next-generation", result)
}

func debounceBlockingWorkflow(ctx Context, input string) (string, error) {
	<-ctx.Done()
	return "", ctx.Err()
}

// TestDebouncerWorkflowTimeout verifies a deadline on the Debounce context is
// recorded as the debounced workflow's execution timeout (never an absolute
// deadline) and cancels the workflow once it runs.
func TestDebouncerWorkflowTimeout(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	RegisterWorkflow(dbosCtx, debounceBlockingWorkflow)
	deb, err := NewDebouncer(dbosCtx, debounceBlockingWorkflow)
	require.NoError(t, err, "failed to create the debouncer")
	require.NoError(t, Launch(dbosCtx))

	timeoutCtx, cancelFunc := WithTimeout(dbosCtx, time.Second)
	defer cancelFunc()
	handle, err := deb.Debounce(timeoutCtx, "wf-timeout-key", 100*time.Millisecond, "blocking-input")
	require.NoError(t, err, "failed to debounce")

	// The deadline is recorded as a timeout whose clock starts at dequeue, so
	// the debounce delay does not count against it. No absolute deadline is set.
	status, err := handle.GetStatus()
	require.NoError(t, err, "failed to get status")
	assert.Greater(t, status.Timeout, time.Duration(0), "the context deadline should be recorded as a timeout")
	assert.LessOrEqual(t, status.Timeout, time.Second, "the recorded timeout cannot exceed the context deadline")
	assert.True(t, status.Deadline.IsZero(), "no absolute deadline should be set on the debounced workflow")

	// Poll from the parent context: timeoutCtx expires before the workflow ends.
	pollingHandle := newWorkflowPollingHandle[string](dbosCtx, handle.GetWorkflowID())
	_, err = pollingHandle.GetResult()
	require.Error(t, err, "the debounced workflow should be cancelled by its execution timeout")
	dbosErr, ok := err.(*Error)
	require.True(t, ok, "expected *Error, got %T (%v)", err, err)
	assert.Equal(t, ErrorCodeAwaitedWorkflowCancelled, dbosErr.Code)

	status, err = pollingHandle.GetStatus()
	require.NoError(t, err, "failed to get status")
	assert.Equal(t, WorkflowStatusCancelled, status.Status, "the debounced workflow should be cancelled")

	// An already-expired context deadline is rejected
	expiredCtx, expiredCancel := WithTimeout(dbosCtx, time.Nanosecond)
	defer expiredCancel()
	time.Sleep(time.Millisecond)
	_, err = deb.Debounce(expiredCtx, "wf-timeout-expired-key", 100*time.Millisecond, "input")
	require.Error(t, err, "an expired context deadline must be rejected")
}

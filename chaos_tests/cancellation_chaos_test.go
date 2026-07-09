package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestCancellationChaos exercises durable execution under randomly-timed
// context cancellation: a workflow with a random number of sequential steps is
// cancelled at a random point across up to maxCancels rounds. Across every subsequent
// round, steps recorded before a resume never execute again. The final round
// completes with the correct result and every step recorded exactly once.
//
// The first execution runs under a per-lifecycle cancellable context and is
// cancelled by cancelling that context. Resumed executions derive their
// context from the DBOS root context, unreachable from the enqueuer, so later
// rounds cancel through the CancelWorkflow API, which takes effect at the next
// step boundary and lets the in-flight step checkpoint.
func TestCancellationChaos(t *testing.T) {
	dbosCtx := setupDBOS(t)

	const stepDuration = 20 * time.Millisecond

	// Per-workflow step execution counts, keyed by workflow ID. The entry is
	// stored before the workflow starts and survives across resume rounds.
	var execCounts sync.Map

	workflow := func(ctx dbos.DBOSContext, numSteps int) (int, error) {
		wfID, err := dbos.GetWorkflowID(ctx)
		if err != nil {
			return 0, err
		}
		v, ok := execCounts.Load(wfID)
		if !ok {
			return 0, fmt.Errorf("no exec counts for workflow %s", wfID)
		}
		counts := v.([]atomic.Int64)
		sum := 0
		for i := range numSteps {
			out, err := dbos.RunAsStep(ctx, func(stepCtx context.Context) (int, error) {
				counts[i].Add(1)
				select {
				case <-stepCtx.Done():
					return 0, stepCtx.Err()
				case <-time.After(stepDuration):
					return i, nil
				}
			}, dbos.WithStepName(fmt.Sprintf("step-%d", i)))
			if err != nil {
				return 0, err
			}
			sum += out
		}
		return sum, nil
	}
	dbos.RegisterWorkflow(dbosCtx, workflow, dbos.WithWorkflowName("CancellationChaos"))

	require.NoError(t, dbos.Launch(dbosCtx))

	isCancellationErr := func(err error) bool {
		return errors.Is(err, context.Canceled) ||
			errors.Is(err, &dbos.DBOSError{Code: dbos.WorkflowCancelled}) ||
			errors.Is(err, &dbos.DBOSError{Code: dbos.AwaitedWorkflowCancelled})
	}

	// Recorded steps must be unique and form a contiguous prefix 0..k-1.
	checkPrefix := func(steps []dbos.StepInfo, numSteps int) error {
		seen := make([]bool, numSteps)
		for _, s := range steps {
			if s.StepID < 0 || s.StepID >= numSteps || seen[s.StepID] {
				return fmt.Errorf("invalid or duplicate recorded step ID %d (numSteps=%d)", s.StepID, numSteps)
			}
			seen[s.StepID] = true
		}
		for i := range len(steps) {
			if !seen[i] {
				return fmt.Errorf("recorded steps are not a contiguous prefix: %d steps recorded but step %d missing", len(steps), i)
			}
		}
		return nil
	}

	lifecycle := func(rng *rand.Rand) error {
		numSteps := 2 + rng.IntN(4)
		wantSum := numSteps * (numSteps - 1) / 2
		wfID := uuid.NewString()
		counts := make([]atomic.Int64, numSteps)
		execCounts.Store(wfID, counts)
		defer execCounts.Delete(wfID)

		cancelCtx, cancelFunc := dbos.WithCancel(dbosCtx)
		defer cancelFunc()

		handle, err := dbos.RunWorkflow(cancelCtx, workflow, numSteps, dbos.WithWorkflowID(wfID))
		if err != nil {
			return fmt.Errorf("start workflow: %w", err)
		}

		const maxCancels = 3
		cancels := 0
		// Exec counts of the steps recorded before the last resume: these
		// replay and must never execute again.
		replayed := make(map[int]int64)
		for round := 0; ; round++ {
			if cancels < maxCancels && rng.Float64() < 0.7 {
				cancels++
				time.Sleep(time.Duration(rng.IntN(numSteps)) * stepDuration)
				if round == 0 {
					cancelFunc()
				} else if err := dbos.CancelWorkflow(dbosCtx, wfID); err != nil {
					return fmt.Errorf("cancel workflow: %w", err)
				}
			}

			result, err := handle.GetResult()

			// An API cancel takes effect at the next step boundary, so GetResult
			// can return while the last step is still finishing (and
			// checkpointing). Wait it out before inspecting counts and steps,
			// and before resuming, so the next round doesn't race the tail.
			time.Sleep(2 * stepDuration)

			for id, count := range replayed {
				if got := counts[id].Load(); got != count {
					return fmt.Errorf("step %d re-executed after being recorded: %d -> %d executions", id, count, got)
				}
			}

			steps, stepsErr := dbos.GetWorkflowSteps(dbosCtx, wfID)
			if stepsErr != nil {
				return fmt.Errorf("get workflow steps: %w", stepsErr)
			}
			if prefixErr := checkPrefix(steps, numSteps); prefixErr != nil {
				return prefixErr
			}

			if err == nil {
				if result != wantSum {
					return fmt.Errorf("unexpected result %d, want %d", result, wantSum)
				}
				if len(steps) != numSteps {
					return fmt.Errorf("expected all %d steps recorded, got %d", numSteps, len(steps))
				}
				return nil
			}

			if !isCancellationErr(err) {
				return fmt.Errorf("expected cancellation error, got: %v", err)
			}
			status, statusErr := handle.GetStatus()
			if statusErr != nil {
				return fmt.Errorf("get status after cancel: %w", statusErr)
			}
			if status.Status != dbos.WorkflowStatusCancelled {
				return fmt.Errorf("expected status CANCELLED after cancel, got %s", status.Status)
			}

			replayed = make(map[int]int64, len(steps))
			for _, s := range steps {
				replayed[s.StepID] = counts[s.StepID].Load()
			}
			if handle, err = dbos.ResumeWorkflow[int](dbosCtx, wfID); err != nil {
				return fmt.Errorf("resume cancelled workflow: %w", err)
			}
		}
	}

	seed := uint64(time.Now().UnixNano())
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		parsed, err := strconv.ParseUint(s, 10, 64)
		require.NoError(t, err, "invalid CHAOS_SEED")
		seed = parsed
	}
	t.Logf("cancellation chaos seed: %d", seed)

	const numWorkers = 10
	const lifecyclesPerWorker = 30

	errs := make(chan error, numWorkers)
	var wg sync.WaitGroup
	for w := range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(w)))
			for i := range lifecyclesPerWorker {
				if err := lifecycle(rng); err != nil {
					errs <- fmt.Errorf("worker %d lifecycle %d: %w", w, i, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

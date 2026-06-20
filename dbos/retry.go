package dbos

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// retry.go: the shared backoff-retry loop primitive.
//
// Two distinct retry policies are built on top of it:
//   - retry / retryWithResult (system_database.go): infinite-by-default retries
//     of DBOS system-database operations on transient connection errors (and,
//     opted in per call site, transaction conflicts). Configured via retryConfig.
//   - executeStepWithRetry (workflow.go): the user-facing step retry policy —
//     bounded maxRetries, an optional retry predicate, and exhaustion wrapping.
//
// retryLoop owns only the mechanics the two share: the attempt loop, the backoff
// delay schedule, and the cancel-aware wait. Each policy supplies its own
// retryability decision, error shaping, and logging through callbacks, so the
// two semantics stay separate while the timing/loop code lives in one place.

// backoffSchedule computes the delay before the next attempt. delayFor(n) returns
// min(base*factor^(n-1), max), optionally scaled by a random jitter factor drawn
// from [jitterMin, jitterMax]. Jitter is disabled when jitterMax == 0.
type backoffSchedule struct {
	base      time.Duration
	max       time.Duration
	factor    float64
	jitterMin float64
	jitterMax float64
}

func (b backoffSchedule) delayFor(attempt int) time.Duration {
	d := float64(b.base) * math.Pow(b.factor, float64(attempt-1))
	d = math.Min(d, float64(b.max))
	if b.jitterMax > 0 {
		d *= b.jitterMin + rand.Float64()*(b.jitterMax-b.jitterMin) // #nosec G404 -- jitter, not security-sensitive
	}
	return time.Duration(d)
}

// connectionRetryBackoff is the exponential backoff schedule for re-establishing
// the notification listener's database connection: 1s base, ×2 per attempt,
// capped at 120s, with ±25% jitter. delayFor is 1-based, so callers that count
// attempts from 0 pass retryAttempt+1. (The previous fixed attempt-count cap was
// redundant: with these constants the delay already saturates at the 120s cap by
// attempt 7.)
var connectionRetryBackoff = backoffSchedule{
	base:      _DB_CONNECTION_RETRY_BASE_DELAY,
	max:       _DB_CONNECTION_MAX_DELAY,
	factor:    _DB_CONNECTION_RETRY_FACTOR,
	jitterMin: 0.75,
	jitterMax: 1.25,
}

// waitForRetry sleeps for d, returning false if ctx is cancelled first.
func waitForRetry(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

// retryLoop runs work() until it returns nil (success → returns nil) or decide()
// stops it. After each failed run it calls decide(err, runs), where runs is the
// number of completed runs (>=1). decide returns (true, nil) to schedule another
// run after a backoff wait, or (false, finalErr) to stop and return finalErr.
// onRetry (if non-nil) is called just before each backoff wait, for logging.
// onCancel maps a context cancellation during a wait into the returned error.
func retryLoop(
	ctx context.Context,
	sched backoffSchedule,
	work func() error,
	decide func(err error, runs int) (bool, error),
	onRetry func(err error, runs int, delay time.Duration),
	onCancel func() error,
) error {
	runs := 0
	for {
		err := work()
		if err == nil {
			return nil
		}
		runs++
		retry, finalErr := decide(err, runs)
		if !retry {
			return finalErr
		}
		delay := sched.delayFor(runs)
		if onRetry != nil {
			onRetry(err, runs, delay)
		}
		if !waitForRetry(ctx, delay) {
			return onCancel()
		}
	}
}

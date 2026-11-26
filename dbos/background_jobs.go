package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

const (
	// How often one of an application's processes runs its background jobs.
	_BACKGROUND_JOBS_INTERVAL = time.Second
	// How long a holder keeps one lock transaction before retaking the lock: a peer can win it then.
	_BACKGROUND_JOBS_HOLD = 60 * time.Second
	// Most workflows one sweep transaction cancels.
	_WORKFLOW_TIMEOUTS_SWEEP_BATCH_SIZE = 1000
)

type backgroundJob struct {
	name string
	run  func() error
}

// runBackgroundJobs runs this application version's background jobs while it holds their lock.
func (c *dbosContext) runBackgroundJobs() {
	jobs := []backgroundJob{
		{name: "delayed_workflows", run: c.transitionDelayedWorkflows},
		{name: "workflow_timeout", run: c.cancelTimedOutWorkflows},
	}
	if c.suspend != nil {
		jobs = append(jobs, backgroundJob{name: "suspended_workflows", run: c.transitionSuspendedWorkflows})
	}
	// One holder per application version, so the latest code always runs them; they tolerate overlap.
	lockName := "background_jobs." + c.applicationVersion
	runBackgroundJobLoop(c, c.systemDB, c.logger, lockName, _BACKGROUND_JOBS_INTERVAL, _BACKGROUND_JOBS_HOLD, jobs)
}

// runBackgroundJobLoop runs jobs on whichever process holds lockName, until stop is done.
func runBackgroundJobLoop(stop context.Context, db sysdb.SystemDatabase, logger *slog.Logger, lockName string,
	interval, hold time.Duration, jobs []backgroundJob) {
	for stop.Err() == nil {
		held := holdBackgroundJobs(stop, db, logger, lockName, interval, hold, jobs)
		// The holder tries again at once and so keeps the jobs; the rest poll for them.
		wait := time.Duration(0)
		if !held {
			wait = time.Duration(float64(interval) * (0.9 + 0.2*rand.Float64())) // #nosec G404 -- non-crypto jitter
		}
		select {
		case <-stop.Done():
			return
		case <-time.After(wait):
		}
	}
}

// holdBackgroundJobs runs the jobs once per interval while it holds the lock, up to hold.
// It reports whether the hold ran its course.
func holdBackgroundJobs(stop context.Context, db sysdb.SystemDatabase, logger *slog.Logger, lockName string,
	interval, hold time.Duration, jobs []backgroundJob) bool {
	lock, err := db.BackgroundJobLock(stop, lockName)
	if err != nil {
		if stop.Err() == nil {
			logger.Warn(fmt.Sprintf("Exception taking the %s lock", lockName), "error", err)
		}
		return false
	}
	if lock == nil {
		return false
	}
	defer lock.Release()
	holdUntil := time.Now().Add(hold)
	for {
		// Timed from the pass's start, so slow jobs can't shorten the interval.
		started := time.Now()
		for _, job := range jobs {
			if err := job.run(); err != nil && stop.Err() == nil {
				logger.Warn(fmt.Sprintf("Exception in background job %s", job.name), "error", err)
			}
		}
		select {
		case <-stop.Done():
			return true
		case <-time.After(max(0, interval-time.Since(started))):
		}
		if !time.Now().Before(holdUntil) {
			return true
		}
		// In place, so the lock is never free mid-hold.
		if err := lock.Renew(stop); err != nil {
			if stop.Err() == nil {
				logger.Warn(fmt.Sprintf("Exception taking the %s lock", lockName), "error", err)
			}
			return false
		}
	}
}

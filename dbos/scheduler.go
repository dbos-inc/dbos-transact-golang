package dbos

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/robfig/cron/v3"
)

/*******************************/
/******* SCHEDULE TYPES ********/
/*******************************/

// ScheduleSpec describes a workflow schedule. It is the input to
// CreateSchedule and ApplySchedules, usable from both a Context and a
// Client.
//
// The target workflow is identified by WorkflowName — the name under which the
// workflow is registered — allowing schedules to be managed for workflows
// owned by any process or language. From a Context, Workflow can be set
// instead to reference a registered Go workflow function directly; when set,
// it takes precedence over WorkflowName.
type ScheduleSpec struct {
	ScheduleName      string // Required: unique name of the schedule
	Schedule          string // Required: cron expression driving the schedule
	WorkflowName      string // Name of the target workflow (required unless Workflow is set)
	Workflow          any    // Registered scheduled workflow function (Context only; takes precedence over WorkflowName)
	WorkflowClassName string // Optional class/namespace name for cross-language dispatch
	Context           any    // Optional user-defined context (serialized as JSON) passed to each scheduled invocation; decode with DecodeScheduleContext
	AutomaticBackfill bool   // Backfill missed ticks when the schedule is reloaded after downtime
	CronTimezone      string // Optional IANA timezone used to interpret the cron expression
	QueueName         string // Optional queue to route scheduled invocations to (defaults to the internal queue)
}

const (
	_DEFAULT_SCHEDULE_POLL_INTERVAL = 30 * time.Second
	_SCHEDULE_MAX_JITTER            = 10 * time.Second
)

func validateCronSchedule(spec, cronTimezone string) error {
	if spec == "" {
		return models.NewInvalidOptionError("schedule is required")
	}
	full := spec
	if cronTimezone != "" {
		full = "CRON_TZ=" + cronTimezone + " " + spec
	}
	if _, err := models.NewScheduleCronParser().Parse(full); err != nil {
		return models.NewInvalidOptionError(fmt.Sprintf("invalid cron schedule %q: %v", spec, err))
	}
	return nil
}

func jitterCap(sched cron.Schedule, scheduledTime time.Time) time.Duration {
	if sched == nil {
		return 0
	}
	interval := sched.Next(scheduledTime).Sub(scheduledTime)
	if interval <= 0 {
		return 0
	}
	return min(interval/10, _SCHEDULE_MAX_JITTER)
}

// ScheduledWorkflowFunc is the signature DB-backed scheduled workflow
// functions must conform to. Each tick the scheduler invokes the function
// with a ScheduledWorkflowInput carrying the cron tick time and the
// user-defined context attached to the schedule.
type ScheduledWorkflowFunc func(ctx Context, input ScheduledWorkflowInput) (any, error)

// DecodeScheduleContext decodes the schedule's user-defined context carried by
// a ScheduledWorkflowInput into T — typically the same type that was set as
// ScheduleSpec.Context when the schedule was created. Returns the zero value
// of T if the schedule has no context.
//
// Example:
//
//	type ReportConfig struct {
//	    Region    string `json:"region"`
//	    BatchSize int    `json:"batch_size"`
//	}
//
//	func reportWorkflow(ctx dbos.Context, input dbos.ScheduledWorkflowInput) (any, error) {
//	    cfg, err := dbos.DecodeScheduleContext[ReportConfig](input)
//	    ...
//	}
func DecodeScheduleContext[T any](input ScheduledWorkflowInput) (T, error) {
	var v T
	if len(input.Context) == 0 {
		return v, nil
	}
	if err := json.Unmarshal(input.Context, &v); err != nil {
		return v, fmt.Errorf("failed to decode schedule context: %w", err)
	}
	return v, nil
}

/************************************/
/******* SCHEDULE MANAGEMENT ********/
/************************************/

// manage AddFunc to the cron
func (c *dbosContext) addScheduleCronEntry(
	scheduleName, cronSchedule string,
	fn ScheduledWorkflowFunc,
	scheduleContext json.RawMessage,
) (cron.EntryID, error) {
	// A tick can fire before the entryID is published below; wait for it so the
	// Entry lookup never runs with a bogus zero ID.
	var entryID cron.EntryID
	ready := make(chan struct{})
	assigned, err := c.getWorkflowScheduler().AddFunc(cronSchedule, func() {
		if !c.launched.Load() {
			return
		}
		select {
		case <-ready:
		case <-c.Done():
			return
		}
		entry := c.getWorkflowScheduler().Entry(entryID)
		scheduledTime := entry.Prev
		if scheduledTime.IsZero() {
			scheduledTime = entry.Next
		}

		// Jitter up to 10% of the interval, capped at _SCHEDULE_MAX_JITTER, to
		// spread load when many executors share the same schedule.
		if cap := jitterCap(entry.Schedule, scheduledTime); cap > 0 {
			select {
			case <-time.After(rand.N(cap)): // #nosec G404 -- jitter is non-security; weak RNG is fine
			case <-c.Done():
				return
			}
		}

		input := ScheduledWorkflowInput{ScheduledTime: scheduledTime, Context: scheduleContext}
		if _, runErr := fn(c, input); runErr != nil {
			c.logger.Error("failed to run scheduled workflow", "schedule", scheduleName, "error", runErr)
		}
	})
	if err != nil {
		return 0, err
	}
	entryID = assigned
	close(ready)
	return assigned, nil
}

// buildDBScheduleFunc returns a ScheduledWorkflowFunc that enqueues the
// schedule's workflow by name, client-style. The workflow does not need to be
// registered on this executor: name -> FQN resolution happens at dequeue time
// on a worker that has the function.
func (c *dbosContext) buildDBScheduleFunc(schedule WorkflowSchedule) ScheduledWorkflowFunc {
	if _, ok := c.workflowCustomNametoFQN.Load(schedule.WorkflowName); !ok {
		c.logger.Debug("scheduled workflow not registered on this executor; ticks will enqueue by name", "schedule", schedule.ScheduleName, "workflow", schedule.WorkflowName)
	}
	scheduleName := schedule.ScheduleName
	queueName := schedule.QueueName
	if queueName == "" {
		queueName = models.InternalQueueName
	}

	return func(ctx Context, input ScheduledWorkflowInput) (any, error) {
		wfID := fmt.Sprintf("sched-%s-%s", scheduleName, input.ScheduledTime.Format(time.RFC3339))

		// Skip if this tick's workflow already exists. Another executor may have enqueued it.
		existing, err := sysdb.RetryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.systemDB.ListWorkflows(c, sysdb.ListWorkflowsDBInput{WorkflowIDs: []string{wfID}})
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("failed to check existing scheduled workflow", "schedule", scheduleName, "workflow_id", wfID, "error", err)
			return nil, err
		}
		if len(existing) > 0 {
			c.logger.Debug("skipping schedule tick", "schedule", scheduleName, "scheduled_time", input.ScheduledTime)
			return nil, nil
		}

		ser := resolveEncoder(ctx)
		encodedInput, err := ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input: %w", err)
		}

		// Scheduled workflows always run against the latest registered application version, so a stale executor does not pick them up after a new deploy.
		// If lookup fails, leave the version unset: NULL rows are only dequeued by executors on the latest version.
		var appVersion string
		latest, err := sysdb.RetryWithResult(c, func() (*VersionInfo, error) {
			return c.systemDB.GetLatestApplicationVersion(c, nil)
		}, sysdb.WithRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("failed to fetch latest application version for scheduled workflow", "schedule", scheduleName, "workflow_id", wfID, "error", err)
		} else if latest != nil {
			appVersion = latest.Name
		}

		status := WorkflowStatus{
			Name:               schedule.WorkflowName,
			ClassName:          schedule.WorkflowClassName,
			ApplicationVersion: appVersion,
			ApplicationID:      c.GetApplicationID(),
			ExecutorID:         c.GetExecutorID(),
			Status:             WorkflowStatusEnqueued,
			ID:                 wfID,
			CreatedAt:          time.Now(),
			Input:              encodedInput,
			QueueName:          queueName,
			Serialization:      ser.Name(),
			ScheduleName:       scheduleName,
		}

		uncancellableCtx := WithoutCancel(c)
		if err := sysdb.Retry(c, func() error {
			tx, err := c.systemDB.Pool().BeginTx(uncancellableCtx, TxOptions{})
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback(uncancellableCtx)
			if _, err := c.systemDB.InsertWorkflowStatus(uncancellableCtx, sysdb.InsertWorkflowStatusDBInput{Status: status, Tx: tx}); err != nil {
				return err
			}
			return tx.Commit(uncancellableCtx)
		}, sysdb.WithRetrierLogger(c.logger), sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction)); err != nil {
			c.logger.Error("failed to enqueue scheduled workflow", "schedule", scheduleName, "workflow_id", wfID, "error", err)
			return nil, err
		}

		if err := sysdb.Retry(c, func() error {
			return c.systemDB.UpdateScheduleLastFiredAt(uncancellableCtx, scheduleName, time.Now())
		}, sysdb.WithRetrierLogger(c.logger), sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction)); err != nil {
			c.logger.Error("failed to update schedule last fired time after retries", "schedule", scheduleName, "error", err)
		}

		return nil, nil
	}
}

func (c *dbosContext) addDBScheduleToScheduler(schedule WorkflowSchedule) {
	sig := c.calculateSignature(schedule)

	fn := c.buildDBScheduleFunc(schedule)

	spec := schedule.Schedule
	if schedule.CronTimezone != "" {
		spec = "CRON_TZ=" + schedule.CronTimezone + " " + spec
	}

	entryID, err := c.addScheduleCronEntry(schedule.ScheduleName, spec, fn, schedule.Context)
	if err != nil {
		c.logger.Error("failed to add schedule to scheduler", "schedule", schedule.ScheduleName, "error", err)
		return
	}

	c.scheduleMu.Lock()
	c.scheduleEntryIDs[schedule.ScheduleName] = entryID
	c.scheduleInstalledSignatures[schedule.ScheduleName] = sig
	c.scheduleMu.Unlock()
	c.logger.Info("Added schedule to scheduler", "schedule", schedule.ScheduleName, "workflow", schedule.WorkflowName)
}

func (c *dbosContext) installedScheduleEntryID(scheduleName string) (cron.EntryID, bool) {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()
	id, ok := c.scheduleEntryIDs[scheduleName]
	return id, ok
}

func (c *dbosContext) removeDBScheduleFromScheduler(scheduleName string) {
	c.scheduleMu.Lock()
	entryID, exists := c.scheduleEntryIDs[scheduleName]
	if exists {
		delete(c.scheduleEntryIDs, scheduleName)
		delete(c.scheduleInstalledSignatures, scheduleName)
	}
	c.scheduleMu.Unlock()
	if !exists {
		c.logger.Warn("attempted to remove non-existent schedule from scheduler", "schedule", scheduleName)
		return
	}
	c.getWorkflowScheduler().Remove(entryID)
	c.logger.Info("Removed schedule from scheduler", "schedule", scheduleName)
}

// Periodically lists schedules from the system database and reconciles the cron scheduler's entries
// New active schedules are added (with optional automatic backfill), paused or deleted schedules are removed.
func (c *dbosContext) runScheduleReconciler() {
	interval := c.config.SchedulerPollingInterval
	if interval <= 0 {
		interval = _DEFAULT_SCHEDULE_POLL_INTERVAL
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		c.reconcileSchedules()

		select {
		case <-c.Done():
			return
		case <-ticker.C:
		}
	}
}

// scheduleSignature holds definition fields used to detect when an installed
// cron entry must be restarted after ApplySchedules / reconciler updates.
// Identity, lifecycle, and runtime fields (schedule_id, status, last_fired_at,
// automatic_backfill) are intentionally omitted.
type scheduleSignature struct {
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	ContextJSON       string
	CronTimezone      string
	QueueName         string
}

func (c *dbosContext) calculateSignature(s WorkflowSchedule) scheduleSignature {
	return scheduleSignature{
		WorkflowName:      s.WorkflowName,
		WorkflowClassName: s.WorkflowClassName,
		Schedule:          s.Schedule,
		ContextJSON:       string(s.Context),
		CronTimezone:      s.CronTimezone,
		QueueName:         s.QueueName,
	}
}

func (c *dbosContext) maybeAutomaticBackfill(sched *WorkflowSchedule) {
	if !sched.AutomaticBackfill || sched.LastFiredAt == nil {
		return
	}
	start := sched.LastFiredAt.Add(time.Second)
	end := time.Now()
	if !start.Before(end) {
		return
	}
	c.logger.Info("performing automatic backfill", "schedule", sched.ScheduleName, "start", start, "end", end)
	if _, err := c.systemDB.BackfillSchedule(c, sysdb.BackfillScheduleDBInput{
		ScheduleName: sched.ScheduleName,
		Schedule:     sched.Schedule,
		StartTime:    start,
		EndTime:      end,
	}); err != nil {
		c.logger.Error("automatic backfill failed", "schedule", sched.ScheduleName, "error", err)
	}
}

func (c *dbosContext) reconcileSchedules() {
	schedules, err := c.systemDB.ListSchedules(c, sysdb.ListSchedulesDBInput{})
	if err != nil {
		c.logger.Warn("failed to list schedules for reconciler", "error", err)
		return
	}

	current := make(map[string]*WorkflowSchedule, len(schedules))
	for i := range schedules {
		current[schedules[i].ScheduleName] = &schedules[i]
	}

	// Remove entries that were deleted or paused. Collect names first to avoid
	// mutating the map while iterating.
	var toRemove []string
	c.scheduleMu.Lock()
	for name := range c.scheduleEntryIDs {
		sched, ok := current[name]
		if !ok || sched.Status != ScheduleStatusActive {
			toRemove = append(toRemove, name)
		}
	}
	c.scheduleMu.Unlock()
	for _, name := range toRemove {
		c.removeDBScheduleFromScheduler(name)
	}

	// Start, restart, or leave running based on definition signature.
	for name, sched := range current {
		if sched.Status != ScheduleStatusActive {
			continue
		}

		c.scheduleMu.Lock()
		_, exists := c.scheduleEntryIDs[name]
		installedSig := c.scheduleInstalledSignatures[name]
		c.scheduleMu.Unlock()

		if exists {
			// Running — restart on a changed definition; no backfill needed.
			sig := c.calculateSignature(*sched)
			if installedSig == sig {
				continue
			}
			c.removeDBScheduleFromScheduler(name)
			c.addDBScheduleToScheduler(*sched)
			continue
		}

		// Not running — start it, backfilling missed executions first if enabled.
		c.maybeAutomaticBackfill(sched)
		c.addDBScheduleToScheduler(*sched)
	}
}

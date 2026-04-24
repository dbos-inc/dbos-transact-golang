package dbos

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/robfig/cron/v3"
)

/*******************************/
/******* SCHEDULE TYPES ********/
/*******************************/

type ScheduleStatus string

const (
	ScheduleStatusActive ScheduleStatus = "ACTIVE"
	ScheduleStatusPaused ScheduleStatus = "PAUSED"
)

type WorkflowSchedule struct {
	ScheduleID        string         `json:"schedule_id"`
	ScheduleName      string         `json:"schedule_name"`
	WorkflowName      string         `json:"workflow_name"`
	WorkflowClassName string         `json:"workflow_class_name,omitempty"`
	Schedule          string         `json:"schedule"`
	Status            ScheduleStatus `json:"status"`
	Context           any            `json:"context"`
	LastFiredAt       *time.Time     `json:"last_fired_at,omitempty"`
	AutomaticBackfill bool           `json:"automatic_backfill"`
	CronTimezone      string         `json:"cron_timezone,omitempty"`
	QueueName         string         `json:"queue_name,omitempty"`
}

// ScheduledWorkflowInput is the input type that DB-backed scheduled workflow
// functions must accept. ScheduledTime is the cron tick time; Context carries
// the user-defined value attached to the schedule (nil if none).
type ScheduledWorkflowInput struct {
	ScheduledTime time.Time `json:"scheduled_time"`
	Context       any       `json:"context,omitempty"`
}

type ApplySchedulesRequest struct {
	ScheduleName      string
	WorkflowFn        any
	Schedule          string
	Context           any
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
}

const (
	_DEFAULT_SCHEDULE_POLL_INTERVAL = 30 * time.Second
	_SCHEDULE_MAX_JITTER            = 10 * time.Second
)

func newScheduleCronParser() cron.Parser {
	return cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}

func validateCronSchedule(spec, cronTimezone string) error {
	if spec == "" {
		return fmt.Errorf("schedule is required")
	}
	full := spec
	if cronTimezone != "" {
		full = "CRON_TZ=" + cronTimezone + " " + spec
	}
	if _, err := newScheduleCronParser().Parse(full); err != nil {
		return fmt.Errorf("invalid cron schedule %q: %w", spec, err)
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
type ScheduledWorkflowFunc func(ctx DBOSContext, input ScheduledWorkflowInput) (any, error)

/************************************/
/******* SCHEDULE MANAGEMENT ********/
/************************************/

// manage AddFunc to the cron
func (c *dbosContext) addScheduleCronEntry(
	scheduleName, cronSchedule string,
	fn ScheduledWorkflowFunc,
	scheduleContext any,
) (cron.EntryID, error) {
	var entryID cron.EntryID
	var err error
	entryID, err = c.getWorkflowScheduler().AddFunc(cronSchedule, func() {
		if !c.launched.Load() {
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
	return entryID, err
}

// wraps the registry's type-erased workflow wrapper into a ScheduledWorkflowFunc
// that also checks if the schedule already fired for this interval
func (c *dbosContext) buildDBScheduleFunc(schedule WorkflowSchedule) (ScheduledWorkflowFunc, error) {
	fqn, ok := c.workflowCustomNametoFQN.Load(schedule.WorkflowName)
	if !ok {
		return nil, fmt.Errorf("workflow not found: %s", schedule.WorkflowName)
	}
	value, ok := c.workflowRegistry.Load(fqn)
	if !ok {
		return nil, fmt.Errorf("workflow not found: %s", schedule.WorkflowName)
	}
	entry, ok := value.(WorkflowRegistryEntry)
	if !ok {
		return nil, fmt.Errorf("invalid workflow registry entry for: %s", schedule.WorkflowName)
	}
	wrappedFn := entry.wrappedFunction
	scheduleName := schedule.ScheduleName
	queueName := schedule.QueueName
	if queueName == "" {
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	return func(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
		wfID := fmt.Sprintf("sched-%s-%s", scheduleName, input.ScheduledTime)

		// Skip if this tick's workflow already exists. Another executor may have enqueued it.
		existing, err := c.systemDB.listWorkflows(c, listWorkflowsDBInput{workflowIDs: []string{wfID}})
		if err != nil {
			c.logger.Error("failed to check existing scheduled workflow", "schedule", scheduleName, "workflow_id", wfID, "error", err)
			return nil, err
		}
		if len(existing) > 0 {
			c.logger.Debug("skipping schedule tick", "schedule", scheduleName, "scheduledTime", input.ScheduledTime)
			return nil, nil
		}

		// The registry wrapper expects encoded inputs, so encode the ScheduledWorkflowInput, using the DBOS Context serializer, before invoking it.
		ser := resolveEncoder(ctx)
		encodedInput, err := ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input: %w", err)
		}

		opts := []WorkflowOption{
			WithWorkflowID(wfID),
			WithQueue(queueName),
			withWorkflowName(entry.FQN),
		}
		result, runErr := wrappedFn(ctx, encodedInput, ser.Name(), opts...)

		now := time.Now()
		if err := c.systemDB.updateSchedule(c, updateScheduleDBInput{
			ScheduleName: scheduleName,
			Status:       ScheduleStatusActive,
			LastFiredAt:  &now,
		}); err != nil {
			c.logger.Error("failed to update schedule last fired time", "schedule", scheduleName, "error", err)
		}

		return result, runErr
	}, nil
}

func (c *dbosContext) addDBScheduleToScheduler(schedule WorkflowSchedule) {
	fn, err := c.buildDBScheduleFunc(schedule)
	if err != nil {
		c.logger.Error("failed to get workflow for schedule", "schedule", schedule.ScheduleName, "error", err)
		return
	}

	spec := schedule.Schedule
	if schedule.CronTimezone != "" {
		spec = "CRON_TZ=" + schedule.CronTimezone + " " + spec
	}

	entryID, err := c.addScheduleCronEntry(schedule.ScheduleName, spec, fn, schedule.Context)
	if err != nil {
		c.logger.Error("failed to add schedule to scheduler", "schedule", schedule.ScheduleName, "error", err)
		return
	}

	c.scheduleEntryIDs[schedule.ScheduleName] = entryID
	c.scheduleInstalledIDs[schedule.ScheduleName] = schedule.ScheduleID
	c.logger.Info("Added schedule to scheduler", "schedule", schedule.ScheduleName, "workflow", schedule.WorkflowName)
}

func (c *dbosContext) removeDBScheduleFromScheduler(scheduleName string) {
	entryID, exists := c.scheduleEntryIDs[scheduleName]
	if !exists {
		c.logger.Warn("attempted to remove non-existent schedule from scheduler", "schedule", scheduleName)
		return
	}
	c.getWorkflowScheduler().Remove(entryID)
	delete(c.scheduleEntryIDs, scheduleName)
	delete(c.scheduleInstalledIDs, scheduleName)
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

func (c *dbosContext) reconcileSchedules() {
	schedules, err := c.systemDB.listSchedules(c, listSchedulesDBInput{})
	if err != nil {
		c.logger.Warn("failed to list schedules for reconciler", "error", err)
		return
	}

	current := make(map[string]*WorkflowSchedule, len(schedules))
	for i := range schedules {
		current[schedules[i].ScheduleName] = &schedules[i]
	}

	// Remove entries that were deleted, paused, or replaced (re-applied with a
	// new ScheduleID — e.g. a changed cron spec, queue, context, or timezone).
	// Collect names first to avoid mutating the map while iterating.
	var toRemove []string
	for name := range c.scheduleEntryIDs {
		sched, ok := current[name]
		if !ok || sched.Status != ScheduleStatusActive {
			toRemove = append(toRemove, name)
			continue
		}
		if c.scheduleInstalledIDs[name] != sched.ScheduleID {
			toRemove = append(toRemove, name)
		}
	}
	for _, name := range toRemove {
		c.removeDBScheduleFromScheduler(name)
	}

	// Add new active schedules.
	for name, sched := range current {
		if sched.Status != ScheduleStatusActive {
			continue
		}
		if _, exists := c.scheduleEntryIDs[name]; exists {
			continue
		}

		if sched.AutomaticBackfill && sched.LastFiredAt != nil {
			start := sched.LastFiredAt.Add(time.Second)
			end := time.Now()
			if start.Before(end) {
				c.logger.Info("performing automatic backfill", "schedule", sched.ScheduleName, "start", start, "end", end)
				if err := c.systemDB.backfillSchedule(c, backfillScheduleDBInput{
					ScheduleName: sched.ScheduleName,
					Schedule:     sched.Schedule,
					StartTime:    start,
					EndTime:      end,
				}); err != nil {
					c.logger.Error("automatic backfill failed", "schedule", sched.ScheduleName, "error", err)
				}
			}
		}

		c.addDBScheduleToScheduler(*sched)
	}
}

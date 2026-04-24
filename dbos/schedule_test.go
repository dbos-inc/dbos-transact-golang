package dbos

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestScheduleCRUD(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflows
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	const customWorkflowName = "custom-schedule-workflow"
	RegisterWorkflow(dbosCtx, testWorkflowForScheduleCustomName, WithWorkflowName(customWorkflowName))

	// Custom queue used by CreateDelete to verify WithScheduleQueueName routes
	// scheduled workflows to the configured queue.
	customQueue := NewWorkflowQueue(dbosCtx, "schedule-crud-custom-queue")

	require.NoError(t, dbosCtx.Launch())

	c := dbosCtx.(*dbosContext)

	const workflowFQN = "github.com/dbos-inc/dbos-transact-golang/dbos.testWorkflowForSchedule"

	t.Run("CreateDelete", func(t *testing.T) {
		const name = "create-delete-schedule"
		err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: name,
			Schedule:     "*/1 * * * * *",
		}, WithScheduleContext("test-context"), WithScheduleQueueName(customQueue.Name))
		require.NoError(t, err)

		schedule, err := GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.NotNil(t, schedule)
		require.Equal(t, name, schedule.ScheduleName)
		require.Equal(t, workflowFQN, schedule.WorkflowName)
		require.Equal(t, "*/1 * * * * *", schedule.Schedule)
		require.Equal(t, ScheduleStatusActive, schedule.Status)
		require.Equal(t, customQueue.Name, schedule.QueueName)

		// Reconciler should install a cron entry for the new schedule.
		require.Eventually(t, func() bool {
			id, ok := c.scheduleEntryIDs[name]
			if !ok {
				return false
			}
			return c.getWorkflowScheduler().Entry(id).Schedule != nil
		}, 3*time.Second, 50*time.Millisecond, "reconciler should install the cron entry")

		// Scheduled ticks should enqueue workflows on the custom queue.
		require.Eventually(t, func() bool {
			wfs, err := ListWorkflows(dbosCtx,
				WithWorkflowIDPrefix("sched-"+name+"-"),
				WithQueueName(customQueue.Name),
			)
			return err == nil && len(wfs) > 0
		}, 5*time.Second, 100*time.Millisecond, "scheduled ticks should land on the custom queue")

		err = DeleteSchedule(dbosCtx, name)
		require.NoError(t, err)

		schedule, err = GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.Nil(t, schedule)

		// Reconciler should drop the cron entry once the schedule is gone.
		require.Eventually(t, func() bool {
			_, ok := c.scheduleEntryIDs[name]
			return !ok
		}, 3*time.Second, 50*time.Millisecond, "reconciler should remove the cron entry")
	})

	t.Run("ListSchedules", func(t *testing.T) {
		const nameA = "list-schedule-a"
		const nameB = "list-schedule-b"
		const nameC = "list-schedule-c"

		err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: nameA,
			Schedule:     "0 0 * * * *",
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, nameA) })

		err = CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: nameB,
			Schedule:     "0 0 * * * *",
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, nameB) })

		err = CreateSchedule(dbosCtx, testWorkflowForScheduleCustomName, CreateScheduleRequest{
			ScheduleName: nameC,
			Schedule:     "0 0 * * * *",
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, nameC) })

		// No filter: all three schedules visible
		all, err := ListSchedules(dbosCtx)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(all), 3)

		// Schedules created without a queue should report the internal queue as
		// their effective default.
		for _, want := range []string{nameA, nameB, nameC} {
			var found *WorkflowSchedule
			for i := range all {
				if all[i].ScheduleName == want {
					found = &all[i]
					break
				}
			}
			require.NotNil(t, found, "schedule %s should be listed", want)
			require.Equal(t, _DBOS_INTERNAL_QUEUE_NAME, found.QueueName, "schedule %s should default to the internal queue", want)
		}

		// Filter by status
		active, err := ListSchedules(dbosCtx, WithScheduleStatuses(ScheduleStatusActive))
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(active), 3)

		// Filter by FQN workflow name → only the two schedules using the FQN-registered workflow
		byWorkflow, err := ListSchedules(dbosCtx, WithScheduleWorkflowNames(workflowFQN))
		require.NoError(t, err)
		require.Len(t, byWorkflow, 2)
		for _, s := range byWorkflow {
			require.NotEqual(t, nameC, s.ScheduleName)
		}

		// Filter by custom workflow name → only the schedule registered under that name
		byCustom, err := ListSchedules(dbosCtx, WithScheduleWorkflowNames(customWorkflowName))
		require.NoError(t, err)
		require.Len(t, byCustom, 1)
		require.Equal(t, nameC, byCustom[0].ScheduleName)

		// Filter by shared schedule name prefix → all three matches
		byPrefix, err := ListSchedules(dbosCtx, WithScheduleNamePrefixes("list-schedule-"))
		require.NoError(t, err)
		require.Len(t, byPrefix, 3)

		// Filter by schedule name prefix only → exactly one match
		byName, err := ListSchedules(dbosCtx, WithScheduleNamePrefixes(nameA))
		require.NoError(t, err)
		require.Len(t, byName, 1)
		require.Equal(t, nameA, byName[0].ScheduleName)

		// Filter by workflow name + schedule name → exactly one match
		filtered, err := ListSchedules(dbosCtx,
			WithScheduleWorkflowNames(workflowFQN),
			WithScheduleNamePrefixes(nameA),
		)
		require.NoError(t, err)
		require.Len(t, filtered, 1)
		require.Equal(t, nameA, filtered[0].ScheduleName)

		// Non-existing workflow name → empty
		none, err := ListSchedules(dbosCtx, WithScheduleWorkflowNames("does.not.exist"))
		require.NoError(t, err)
		require.Empty(t, none)

		// Non-existing schedule name → empty
		none, err = ListSchedules(dbosCtx, WithScheduleNamePrefixes("does-not-exist"))
		require.NoError(t, err)
		require.Empty(t, none)
	})

	t.Run("PauseResumeSchedule", func(t *testing.T) {
		const name = "pause-resume-schedule"
		err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: name,
			Schedule:     "0 0 * * * *",
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, name) })

		err = PauseSchedule(dbosCtx, name)
		require.NoError(t, err)

		schedule, err := GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusPaused, schedule.Status)

		err = ResumeSchedule(dbosCtx, name)
		require.NoError(t, err)

		schedule, err = GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusActive, schedule.Status)
	})
}

func TestApplySchedules(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	// Two queues so we can verify that re-applying a schedule with a different
	// QueueName routes future ticks to the new queue.
	queueA := NewWorkflowQueue(dbosCtx, "apply-queue-a")
	queueB := NewWorkflowQueue(dbosCtx, "apply-queue-b")

	require.NoError(t, dbosCtx.Launch())

	c := dbosCtx.(*dbosContext)

	const (
		toPause = "applied-schedule-pause"
		toKeep  = "applied-schedule-keep"
		toDrop  = "applied-schedule-drop"
	)

	hasEntry := func(name string) bool {
		id, ok := c.scheduleEntryIDs[name]
		if !ok {
			return false
		}
		return c.getWorkflowScheduler().Entry(id).Schedule != nil
	}

	// Round 1: apply three active schedules. toKeep fires every second on
	// queueA so we can observe that a queue change takes effect on re-apply.
	err := ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: toPause, WorkflowFn: testWorkflowForSchedule, Schedule: "*/10 * * * * *"},
		{ScheduleName: toKeep, WorkflowFn: testWorkflowForSchedule, Schedule: "*/1 * * * * *", QueueName: queueA.Name},
		{ScheduleName: toDrop, WorkflowFn: testWorkflowForSchedule, Schedule: "0 30 * * * *"},
	})
	require.NoError(t, err)

	schedules, err := ListSchedules(dbosCtx, WithScheduleStatuses(ScheduleStatusActive))
	require.NoError(t, err)
	require.Equal(t, 3, len(schedules))

	for _, name := range []string{toPause, toKeep, toDrop} {
		require.Eventually(t, func() bool { return hasEntry(name) },
			3*time.Second, 50*time.Millisecond, "reconciler should install the cron entry for %s", name)
	}

	// toKeep should enqueue at least one workflow on queueA before the re-apply.
	require.Eventually(t, func() bool {
		wfs, err := ListWorkflows(dbosCtx,
			WithWorkflowIDPrefix("sched-"+toKeep+"-"),
			WithQueueName(queueA.Name),
		)
		return err == nil && len(wfs) > 0
	}, 5*time.Second, 100*time.Millisecond, "toKeep should enqueue on queueA before re-apply")

	// Round 2: pause one, delete one, re-apply the third to change its queue.
	require.NoError(t, PauseSchedule(dbosCtx, toPause))
	require.NoError(t, DeleteSchedule(dbosCtx, toDrop))
	require.NoError(t, ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: toKeep, WorkflowFn: testWorkflowForSchedule, Schedule: "*/1 * * * * *", QueueName: queueB.Name},
	}))

	// Paused: schedule still exists but its cron entry is removed.
	paused, err := GetSchedule(dbosCtx, toPause)
	require.NoError(t, err)
	require.NotNil(t, paused)
	require.Equal(t, ScheduleStatusPaused, paused.Status)
	require.Eventually(t, func() bool { return !hasEntry(toPause) },
		3*time.Second, 50*time.Millisecond, "reconciler should drop the cron entry for paused %s", toPause)

	// Deleted: schedule is gone and its cron entry is removed.
	dropped, err := GetSchedule(dbosCtx, toDrop)
	require.NoError(t, err)
	require.Nil(t, dropped)
	require.Eventually(t, func() bool { return !hasEntry(toDrop) },
		3*time.Second, 50*time.Millisecond, "reconciler should drop the cron entry for deleted %s", toDrop)

	// Kept: still active, cron entry installed, and queue was updated to queueB.
	kept, err := GetSchedule(dbosCtx, toKeep)
	require.NoError(t, err)
	require.NotNil(t, kept)
	require.Equal(t, ScheduleStatusActive, kept.Status)
	require.Equal(t, queueB.Name, kept.QueueName)
	require.Eventually(t, func() bool { return hasEntry(toKeep) },
		3*time.Second, 50*time.Millisecond, "re-applied toKeep should have a cron entry")

	// Ticks fired after the re-apply should enqueue on queueB.
	require.Eventually(t, func() bool {
		wfs, err := ListWorkflows(dbosCtx,
			WithWorkflowIDPrefix("sched-"+toKeep+"-"),
			WithQueueName(queueB.Name),
		)
		return err == nil && len(wfs) > 0
	}, 5*time.Second, 100*time.Millisecond, "re-applied toKeep should enqueue on queueB")

	active, err := ListSchedules(dbosCtx, WithScheduleStatuses(ScheduleStatusActive))
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, toKeep, active[0].ScheduleName)
}

func TestApplySchedulesInvalidSignature(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	require.NoError(t, dbosCtx.Launch())

	// Second argument is not ScheduledWorkflowInput.
	badInputType := func(ctx DBOSContext, input string) (any, error) { return nil, nil }
	err := ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "bad-input", WorkflowFn: badInputType, Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ScheduledWorkflowInput")

	// Not a function at all.
	err = ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "not-a-func", WorkflowFn: "not a function", Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)

	// Too few parameters.
	tooFewParams := func(ctx DBOSContext) (any, error) { return nil, nil }
	err = ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{ScheduleName: "too-few", WorkflowFn: tooFewParams, Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)

	// None of the above schedules should have been persisted.
	for _, name := range []string{"bad-input", "not-a-func", "too-few"} {
		s, err := GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.Nil(t, s, "schedule %s should not have been created", name)
	}
}

func TestBackfillSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "backfill-schedule",
		Schedule:     "*/1 * * * * *", // Every second for testing
	})
	require.NoError(t, err)

	// Backfill last minute
	start := time.Now().Add(-1 * time.Minute)
	end := time.Now()

	err = BackfillSchedule(dbosCtx, "backfill-schedule", start, end)
	require.NoError(t, err)

	// A `*/1 * * * * *` schedule over a one-minute window should enqueue
	// roughly 60 workflows; allow some slack for clock alignment.
	backfilled, err := ListWorkflows(dbosCtx, WithWorkflowIDPrefix("sched-backfill-schedule-"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(backfilled), 50, "backfill should have enqueued ~60 workflows, got %d", len(backfilled))
	for _, wf := range backfilled {
		require.Equal(t, WorkflowStatusEnqueued, wf.Status)
	}
}

func TestTriggerSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	// Launch so the internal queue can dequeue and run the triggered workflow.
	require.NoError(t, dbosCtx.Launch())

	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "trigger-schedule",
		Schedule:     "0 0 * * * *",
	})
	require.NoError(t, err)

	workflowID, err := TriggerSchedule(dbosCtx, "trigger-schedule")
	require.NoError(t, err)
	require.NotEmpty(t, workflowID)
	require.Contains(t, workflowID, "trigger-schedule")

	handle, err := RetrieveWorkflow[any](dbosCtx, workflowID)
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "completed", result)
}

func TestScheduleWithOptions(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "full-options-schedule",
		Schedule:     "0 0 * * * *",
	},
		WithScheduleContext(map[string]string{"key": "value"}),
		WithAutomaticBackfill(true),
		WithCronTimezone("America/New_York"),
		WithScheduleQueueName("my-queue"),
	)
	require.NoError(t, err)

	schedule, err := GetSchedule(dbosCtx, "full-options-schedule")
	require.NoError(t, err)
	require.True(t, schedule.AutomaticBackfill)
	require.Equal(t, "America/New_York", schedule.CronTimezone)
	require.Equal(t, "my-queue", schedule.QueueName)
}

func testWorkflowForSchedule(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

func testWorkflowForScheduleCustomName(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

var backfillRestartFiredEvent *Event

func testWorkflowForBackfillRestart(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	if backfillRestartFiredEvent != nil {
		backfillRestartFiredEvent.Set()
	}
	return "completed", nil
}

func TestAutomaticBackfillOnRestart(t *testing.T) {
	backfillRestartFiredEvent = NewEvent()

	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	RegisterWorkflow(dbosCtx, testWorkflowForBackfillRestart)
	require.NoError(t, dbosCtx.Launch())

	const scheduleName = "test-backfill-restart"
	const wfFQN = "github.com/dbos-inc/dbos-transact-golang/dbos.testWorkflowForBackfillRestart"

	err := CreateSchedule(dbosCtx, testWorkflowForBackfillRestart, CreateScheduleRequest{
		ScheduleName: scheduleName,
		Schedule:     "*/1 * * * * *", // Every second
	}, WithAutomaticBackfill(true))
	require.NoError(t, err)

	// Wait for the schedule to fire at least once so LastFiredAt is set.
	backfillRestartFiredEvent.Wait()

	// Snapshot how many runs have succeeded before the restart.
	var before []WorkflowStatus
	require.Eventually(t, func() bool {
		before, err = ListWorkflows(dbosCtx,
			WithName(wfFQN),
			WithStatus([]WorkflowStatusType{WorkflowStatusSuccess}),
		)
		return err == nil && len(before) >= 1
	}, 3*time.Second, 50*time.Millisecond, "expected at least one successful run before shutdown")

	dbosCtx.Shutdown(5 * time.Second)

	// Reset the event so the next Wait only returns after a post-restart fire.
	backfillRestartFiredEvent.Clear()

	// Simulate missed schedules while the context is down.
	time.Sleep(2 * time.Second)

	dbosCtx2 := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})
	defer dbosCtx2.Shutdown(5 * time.Second)

	RegisterWorkflow(dbosCtx2, testWorkflowForBackfillRestart)
	require.NoError(t, dbosCtx2.Launch())

	// Launch should backfill the missed runs; wait for one to execute.
	backfillRestartFiredEvent.Wait()

	// After backfill, the success count should have grown by more than one.
	require.Eventually(t, func() bool {
		after, err := ListWorkflows(dbosCtx2,
			WithName(wfFQN),
			WithStatus([]WorkflowStatusType{WorkflowStatusSuccess}),
		)
		return err == nil && len(after)-len(before) > 2
	}, 5*time.Second, 100*time.Millisecond, "expected backfill to produce more than one additional successful workflow")
}

// TestScheduleCronTimezone verifies that a non-empty CronTimezone is applied
// to the installed cron entry via the CRON_TZ= prefix: Next() from a known
// wall-clock reference should fall at the configured hour in that tz.
func TestScheduleCronTimezone(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(5 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	const scheduleName = "tz-schedule"
	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: scheduleName,
		Schedule:     "0 0 9 * * *", // 09:00:00 every day
	}, WithCronTimezone("America/New_York"))
	require.NoError(t, err)

	c := dbosCtx.(*dbosContext)
	var entry cron.Entry
	require.Eventually(t, func() bool {
		id, ok := c.scheduleEntryIDs[scheduleName]
		if !ok {
			return false
		}
		entry = c.getWorkflowScheduler().Entry(id)
		return entry.Schedule != nil
	}, 3*time.Second, 50*time.Millisecond, "reconciler should install the cron entry")

	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	// 06:00 NY → next fire should be 09:00 NY the same day, regardless of
	// where the test host's local time sits.
	ref := time.Date(2025, 1, 15, 6, 0, 0, 0, loc)
	next := entry.Schedule.Next(ref).In(loc)
	require.Equal(t, 9, next.Hour(), "next fire should be 09:00 NY, got %v", next)
	require.Equal(t, 2025, next.Year())
	require.Equal(t, time.January, next.Month())
	require.Equal(t, 15, next.Day())
}

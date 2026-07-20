package dbos

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestScheduleCRUD(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	// First register the workflows
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	RegisterWorkflow(dbosCtx, testCapturingScheduledWorkflow)
	const customWorkflowName = "custom-schedule-workflow"
	RegisterWorkflow(dbosCtx, testWorkflowForScheduleCustomName, WithWorkflowName(customWorkflowName))

	// Custom queue used by CreateDelete to verify WithScheduleQueueName routes
	// scheduled workflows to the configured queue.
	customQueue, err := RegisterQueue(dbosCtx, "schedule-crud-custom-queue")
	require.NoError(t, err)

	require.NoError(t, dbosCtx.Launch())

	c := dbosCtx.(*dbosContext)

	const workflowFQN = "github.com/dbos-inc/dbos-transact-golang/dbos.testWorkflowForSchedule"

	t.Run("CreateDelete", func(t *testing.T) {
		scheduledInputCapture = sync.Map{}
		const name = "create-delete-schedule"
		const ctxValue = "test-context"
		capturingFQN := "github.com/dbos-inc/dbos-transact-golang/dbos.testCapturingScheduledWorkflow"
		err := CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: name,
			Schedule:     "*/1 * * * * *",
			Workflow:     testCapturingScheduledWorkflow,
			Context:      ctxValue,
			QueueName:    customQueue.GetName(),
		})
		require.NoError(t, err)

		schedule, err := GetSchedule(dbosCtx, name)
		require.NoError(t, err)
		require.NotZero(t, schedule)
		require.Equal(t, name, schedule.ScheduleName)
		require.Equal(t, capturingFQN, schedule.WorkflowName)
		require.Equal(t, "*/1 * * * * *", schedule.Schedule)
		require.Equal(t, ScheduleStatusActive, schedule.Status)
		require.Equal(t, customQueue.GetName(), schedule.QueueName)

		// Reconciler should install a cron entry for the new schedule.
		require.Eventually(t, func() bool {
			id, ok := c.installedScheduleEntryID(name)
			if !ok {
				return false
			}
			return c.getWorkflowScheduler().Entry(id).Schedule != nil
		}, 3*time.Second, 50*time.Millisecond, "reconciler should install the cron entry")

		// Scheduled ticks should enqueue workflows on the custom queue and the
		// fired workflow should receive the configured ScheduledTime + Context.
		var firedWfID string
		require.Eventually(t, func() bool {
			wfs, err := ListWorkflows(dbosCtx,
				WithFilterWorkflowIDPrefix("sched-"+name+"-"),
				WithFilterQueueName(customQueue.GetName()),
			)
			if err != nil || len(wfs) == 0 {
				return false
			}
			for _, wf := range wfs {
				if _, ok := scheduledInputCapture.Load(wf.ID); ok {
					firedWfID = wf.ID
					return true
				}
			}
			return false
		}, 10*time.Second, 100*time.Millisecond, "scheduled tick should land on the custom queue and execute")

		captured, _ := scheduledInputCapture.Load(firedWfID)
		got := captured.(ScheduledWorkflowInput)
		decodedCtx, err := DecodeScheduleContext[string](got)
		require.NoError(t, err)
		require.Equal(t, ctxValue, decodedCtx)
		require.False(t, got.ScheduledTime.IsZero())

		err = DeleteSchedule(dbosCtx, name)
		require.NoError(t, err)

		schedule, err = GetSchedule(dbosCtx, name)
		require.ErrorIs(t, err, ErrScheduleNotFound)
		require.Zero(t, schedule)

		// Reconciler should drop the cron entry once the schedule is gone.
		require.Eventually(t, func() bool {
			_, ok := c.installedScheduleEntryID(name)
			return !ok
		}, 3*time.Second, 50*time.Millisecond, "reconciler should remove the cron entry")
	})

	t.Run("ListSchedules", func(t *testing.T) {
		const nameA = "list-schedule-a"
		const nameB = "list-schedule-b"
		const nameC = "list-schedule-c"

		err := CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: nameA,
			Schedule:     "0 0 * * * *",
			Workflow:     testWorkflowForSchedule,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, nameA) })

		err = CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: nameB,
			Schedule:     "0 0 * * * *",
			Workflow:     testWorkflowForSchedule,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, nameB) })

		err = CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: nameC,
			Schedule:     "0 0 * * * *",
			Workflow:     testWorkflowForScheduleCustomName,
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
			require.Equal(t, models.InternalQueueName, found.QueueName, "schedule %s should default to the internal queue", want)
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

	t.Run("DuplicateName", func(t *testing.T) {
		const name = "duplicate-name-schedule"
		require.NoError(t, CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: name,
			Schedule:     "0 0 * * * *",
			Workflow:     testWorkflowForSchedule,
		}))
		t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, name) })

		err := CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: name,
			Schedule:     "0 0 * * * *",
			Workflow:     testWorkflowForSchedule,
		})
		require.Error(t, err, "creating a schedule with a duplicate name must fail")
	})

	t.Run("PauseResumeSchedule", func(t *testing.T) {
		const name = "pause-resume-schedule"
		err := CreateSchedule(dbosCtx, ScheduleSpec{
			ScheduleName: name,
			Schedule:     "0 0 * * * *",
			Workflow:     testWorkflowForSchedule,
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

		// Pausing or resuming a non-existent schedule must error.
		err = PauseSchedule(dbosCtx, "does-not-exist")
		require.ErrorIs(t, err, ErrScheduleNotFound)

		err = ResumeSchedule(dbosCtx, "does-not-exist")
		require.ErrorIs(t, err, ErrScheduleNotFound)
	})
}

func TestApplySchedules(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	// Two queues so we can verify that re-applying a schedule with a different
	// QueueName routes future ticks to the new queue.
	queueA, err := RegisterQueue(dbosCtx, "apply-queue-a")
	require.NoError(t, err)
	queueB, err := RegisterQueue(dbosCtx, "apply-queue-b")
	require.NoError(t, err)

	require.NoError(t, dbosCtx.Launch())

	c := dbosCtx.(*dbosContext)

	const (
		toPause = "applied-schedule-pause"
		toKeep  = "applied-schedule-keep"
		toDrop  = "applied-schedule-drop"
	)

	hasEntry := func(name string) bool {
		id, ok := c.installedScheduleEntryID(name)
		if !ok {
			return false
		}
		return c.getWorkflowScheduler().Entry(id).Schedule != nil
	}

	// Round 1: apply three active schedules. toKeep fires every second on
	// queueA so we can observe that a queue change takes effect on re-apply.
	err = ApplySchedules(dbosCtx, []ScheduleSpec{
		{ScheduleName: toPause, Workflow: testWorkflowForSchedule, Schedule: "*/10 * * * * *"},
		{ScheduleName: toKeep, Workflow: testWorkflowForSchedule, Schedule: "*/1 * * * * *", QueueName: queueA.GetName()},
		{ScheduleName: toDrop, Workflow: testWorkflowForSchedule, Schedule: "0 30 * * * *"},
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
			WithFilterWorkflowIDPrefix("sched-"+toKeep+"-"),
			WithFilterQueueName(queueA.GetName()),
		)
		return err == nil && len(wfs) > 0
	}, 5*time.Second, 100*time.Millisecond, "toKeep should enqueue on queueA before re-apply")

	// Snapshot schedule_id: re-apply must update definition in place, not replace the row.
	beforeKeep, err := GetSchedule(dbosCtx, toKeep)
	require.NoError(t, err)
	require.NotZero(t, beforeKeep)
	keepScheduleID := beforeKeep.ScheduleID

	// Round 2: pause one, delete one, re-apply the third to change its queue.
	require.NoError(t, PauseSchedule(dbosCtx, toPause))
	require.NoError(t, DeleteSchedule(dbosCtx, toDrop))
	require.NoError(t, ApplySchedules(dbosCtx, []ScheduleSpec{
		{ScheduleName: toKeep, Workflow: testWorkflowForSchedule, Schedule: "*/1 * * * * *", QueueName: queueB.GetName()},
	}))

	// Paused: schedule still exists but its cron entry is removed.
	paused, err := GetSchedule(dbosCtx, toPause)
	require.NoError(t, err)
	require.NotZero(t, paused)
	require.Equal(t, ScheduleStatusPaused, paused.Status)
	require.Eventually(t, func() bool { return !hasEntry(toPause) },
		3*time.Second, 50*time.Millisecond, "reconciler should drop the cron entry for paused %s", toPause)

	// Deleted: schedule is gone and its cron entry is removed.
	dropped, err := GetSchedule(dbosCtx, toDrop)
	require.ErrorIs(t, err, ErrScheduleNotFound)
	require.Zero(t, dropped)
	require.Eventually(t, func() bool { return !hasEntry(toDrop) },
		3*time.Second, 50*time.Millisecond, "reconciler should drop the cron entry for deleted %s", toDrop)

	// Kept: still active, same schedule_id, cron entry installed, queue updated to queueB.
	kept, err := GetSchedule(dbosCtx, toKeep)
	require.NoError(t, err)
	require.NotZero(t, kept)
	require.Equal(t, ScheduleStatusActive, kept.Status)
	require.Equal(t, keepScheduleID, kept.ScheduleID, "upsert must preserve schedule_id on re-apply")
	require.Equal(t, queueB.GetName(), kept.QueueName)
	require.Eventually(t, func() bool { return hasEntry(toKeep) },
		3*time.Second, 50*time.Millisecond, "re-applied toKeep should have a cron entry")

	// Ticks fired after the re-apply should enqueue on queueB.
	require.Eventually(t, func() bool {
		wfs, err := ListWorkflows(dbosCtx,
			WithFilterWorkflowIDPrefix("sched-"+toKeep+"-"),
			WithFilterQueueName(queueB.GetName()),
		)
		return err == nil && len(wfs) > 0
	}, 5*time.Second, 100*time.Millisecond, "re-applied toKeep should enqueue on queueB")

	active, err := ListSchedules(dbosCtx, WithScheduleStatuses(ScheduleStatusActive))
	require.NoError(t, err)
	require.Len(t, active, 1)
	require.Equal(t, toKeep, active[0].ScheduleName)
}

// TestApplySchedulesConcurrent checks that concurrent ApplySchedules of the same
// name are idempotent (upsert): one row, no error, and re-apply preserves schedule_id.
func TestApplySchedulesConcurrent(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	const (
		name       = "shared-schedule"
		numWorkers = 8
	)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, numWorkers)

	for range numWorkers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- ApplySchedules(dbosCtx, []ScheduleSpec{
				{
					ScheduleName: name,
					Workflow:     testWorkflowForSchedule,
					Schedule:     "0 0 * * * *",
					Context:      map[string]any{"region": "us"},
				},
			})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	schedules, err := ListSchedules(dbosCtx, WithScheduleNamePrefixes(name))
	require.NoError(t, err)
	require.Len(t, schedules, 1)
	require.Equal(t, name, schedules[0].ScheduleName)
	require.Equal(t, "0 0 * * * *", schedules[0].Schedule)
	require.JSONEq(t, `{"region":"us"}`, string(schedules[0].Context))
	scheduleID := schedules[0].ScheduleID

	// Re-applying updates definition in place and preserves schedule_id.
	require.NoError(t, ApplySchedules(dbosCtx, []ScheduleSpec{
		{
			ScheduleName: name,
			Workflow:     testWorkflowForSchedule,
			Schedule:     "0 0 0 * * *",
			Context:      map[string]any{"region": "eu"},
		},
	}))
	schedules, err = ListSchedules(dbosCtx, WithScheduleNamePrefixes(name))
	require.NoError(t, err)
	require.Len(t, schedules, 1)
	require.Equal(t, scheduleID, schedules[0].ScheduleID)
	require.Equal(t, "0 0 0 * * *", schedules[0].Schedule)
	require.JSONEq(t, `{"region":"eu"}`, string(schedules[0].Context))

	require.NoError(t, DeleteSchedule(dbosCtx, name))
	schedules, err = ListSchedules(dbosCtx, WithScheduleNamePrefixes(name))
	require.NoError(t, err)
	require.Empty(t, schedules)
}

// TestApplySchedulesLiveUpdate verifies that re-applying a changed definition is
// picked up by the reconciler (signature change → restart) and new context is used.
func TestApplySchedulesLiveUpdate(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	resetLiveUpdateVersionCounts()
	RegisterWorkflow(dbosCtx, testLiveUpdateScheduledWorkflow)
	require.NoError(t, dbosCtx.Launch())

	const name = "live-update"
	require.NoError(t, ApplySchedules(dbosCtx, []ScheduleSpec{
		{
			ScheduleName: name,
			Workflow:     testLiveUpdateScheduledWorkflow,
			Schedule:     "*/1 * * * * *",
			Context:      map[string]any{"version": 1},
		},
	}))
	t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, name) })

	before, err := GetSchedule(dbosCtx, name)
	require.NoError(t, err)
	require.NotZero(t, before)

	require.Eventually(t, func() bool {
		return liveUpdateVersionCount(1) >= 1
	}, 10*time.Second, 100*time.Millisecond, "schedule should fire with context version 1")

	require.NoError(t, ApplySchedules(dbosCtx, []ScheduleSpec{
		{
			ScheduleName: name,
			Workflow:     testLiveUpdateScheduledWorkflow,
			Schedule:     "*/1 * * * * *",
			Context:      map[string]any{"version": 2},
		},
	}))

	after, err := GetSchedule(dbosCtx, name)
	require.NoError(t, err)
	require.NotZero(t, after)
	require.Equal(t, before.ScheduleID, after.ScheduleID, "live update must preserve schedule_id")

	// Reconciler should restart the entry and fire with the new context.
	// Version 2 fires can only come from the re-applied definition.
	require.Eventually(t, func() bool {
		return liveUpdateVersionCount(2) >= 2
	}, 10*time.Second, 100*time.Millisecond, "re-applied schedule should fire with context version 2")
}

// TestApplySchedulesPreservesRuntimeState checks that re-apply updates definition
// fields without clobbering status or last_fired_at.
func TestApplySchedulesPreservesRuntimeState(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	// No Launch needed: this test only exercises the DB upsert path.
	c := dbosCtx.(*dbosContext)

	const name = "state-keep"
	require.NoError(t, ApplySchedules(dbosCtx, []ScheduleSpec{
		{
			ScheduleName: name,
			Workflow:     testWorkflowForSchedule,
			Schedule:     "0 0 0 * * *", // rare fire
			Context:      map[string]any{"version": 1},
		},
	}))
	t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, name) })

	require.NoError(t, PauseSchedule(dbosCtx, name))
	lastFired := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, c.systemDB.UpdateScheduleLastFiredAt(c, name, lastFired))

	require.NoError(t, ApplySchedules(dbosCtx, []ScheduleSpec{
		{
			ScheduleName: name,
			Workflow:     testWorkflowForSchedule,
			Schedule:     "0 0 0 * * *",
			Context:      map[string]any{"version": 2},
		},
	}))

	sched, err := GetSchedule(dbosCtx, name)
	require.NoError(t, err)
	require.NotZero(t, sched)
	require.Equal(t, ScheduleStatusPaused, sched.Status, "status must be preserved")
	require.NotNil(t, sched.LastFiredAt)
	require.True(t, sched.LastFiredAt.Equal(lastFired), "last_fired_at must be preserved, got %v", sched.LastFiredAt)
	require.JSONEq(t, `{"version":2}`, string(sched.Context), "definition context must still update")
}

// TestCalculateScheduleSignature ensures definition fields affect the signature
// and identity/lifecycle/runtime fields do not.
func TestCalculateScheduleSignature(t *testing.T) {
	c := &dbosContext{}
	base := WorkflowSchedule{
		ScheduleID:        "id-1",
		ScheduleName:      "sig",
		WorkflowName:      "wf",
		WorkflowClassName: "",
		Schedule:          "* * * * *",
		Status:            ScheduleStatusActive,
		Context:           json.RawMessage(`"ctx"`),
		LastFiredAt:       nil,
		AutomaticBackfill: false,
		CronTimezone:      "",
		QueueName:         "",
	}
	sig := c.calculateSignature(base)

	// Identity / lifecycle / runtime fields must NOT change the signature.
	lastFired := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	unchanged := []WorkflowSchedule{
		{ScheduleID: "id-2", ScheduleName: base.ScheduleName, WorkflowName: base.WorkflowName, Schedule: base.Schedule, Status: base.Status, Context: base.Context},
		{ScheduleID: base.ScheduleID, ScheduleName: "other-name", WorkflowName: base.WorkflowName, Schedule: base.Schedule, Status: base.Status, Context: base.Context},
		{ScheduleID: base.ScheduleID, ScheduleName: base.ScheduleName, WorkflowName: base.WorkflowName, Schedule: base.Schedule, Status: ScheduleStatusPaused, Context: base.Context},
		{ScheduleID: base.ScheduleID, ScheduleName: base.ScheduleName, WorkflowName: base.WorkflowName, Schedule: base.Schedule, Status: base.Status, Context: base.Context, LastFiredAt: &lastFired},
		{ScheduleID: base.ScheduleID, ScheduleName: base.ScheduleName, WorkflowName: base.WorkflowName, Schedule: base.Schedule, Status: base.Status, Context: base.Context, AutomaticBackfill: true},
	}
	for i, s := range unchanged {
		got := c.calculateSignature(s)
		require.Equal(t, sig, got, "case %d should not change signature", i)
	}

	// Context compares as raw JSON bytes: identical bytes, equal signatures.
	rawA := base
	rawA.Context = json.RawMessage(`{"a":1,"b":"x"}`)
	rawB := base
	rawB.Context = json.RawMessage(`{"a":1,"b":"x"}`)
	require.Equal(t, c.calculateSignature(rawA), c.calculateSignature(rawB))

	// Definition fields MUST change the signature.
	changed := []WorkflowSchedule{
		{WorkflowName: "wf2", Schedule: base.Schedule, Context: base.Context},
		{WorkflowName: base.WorkflowName, WorkflowClassName: "SomeClass", Schedule: base.Schedule, Context: base.Context},
		{WorkflowName: base.WorkflowName, Schedule: "0 * * * *", Context: base.Context},
		{WorkflowName: base.WorkflowName, Schedule: base.Schedule, Context: json.RawMessage(`"ctx2"`)},
		{WorkflowName: base.WorkflowName, Schedule: base.Schedule, Context: base.Context, CronTimezone: "America/Los_Angeles"},
		{WorkflowName: base.WorkflowName, Schedule: base.Schedule, Context: base.Context, QueueName: "q"},
	}
	for i, s := range changed {
		got := c.calculateSignature(s)
		require.NotEqual(t, sig, got, "case %d should change signature", i)
	}
}

func TestApplySchedulesInvalidSignature(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	require.NoError(t, dbosCtx.Launch())

	// Second argument is not ScheduledWorkflowInput.
	badInputType := func(ctx Context, input string) (any, error) { return nil, nil }
	err := ApplySchedules(dbosCtx, []ScheduleSpec{
		{ScheduleName: "bad-input", Workflow: badInputType, Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "ScheduledWorkflowInput")

	// Not a function at all.
	err = ApplySchedules(dbosCtx, []ScheduleSpec{
		{ScheduleName: "not-a-func", Workflow: "not a function", Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)

	// Too few parameters.
	tooFewParams := func(ctx Context) (any, error) { return nil, nil }
	err = ApplySchedules(dbosCtx, []ScheduleSpec{
		{ScheduleName: "too-few", Workflow: tooFewParams, Schedule: "0 0 * * * *"},
	})
	require.Error(t, err)

	// None of the above schedules should have been persisted.
	for _, name := range []string{"bad-input", "not-a-func", "too-few"} {
		s, err := GetSchedule(dbosCtx, name)
		require.ErrorIs(t, err, ErrScheduleNotFound, "schedule %s should not have been created", name)
		require.Zero(t, s)
	}
}

func TestScheduleCronValidation(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	// CreateSchedule rejects a garbage cron expression up-front.
	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: "bad-cron-create",
		Schedule:     "not a cron",
		Workflow:     testWorkflowForSchedule,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cron schedule")
	got, err := GetSchedule(dbosCtx, "bad-cron-create")
	require.ErrorIs(t, err, ErrScheduleNotFound, "invalid-cron schedule must not be persisted")
	require.Zero(t, got)

	// ApplySchedules rejects invalid cron before writing any row (atomicity).
	err = ApplySchedules(dbosCtx, []ScheduleSpec{
		{ScheduleName: "apply-good", Workflow: testWorkflowForSchedule, Schedule: "0 0 * * * *"},
		{ScheduleName: "apply-bad", Workflow: testWorkflowForSchedule, Schedule: "garbage"},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cron schedule")
	for _, name := range []string{"apply-good", "apply-bad"} {
		s, err := GetSchedule(dbosCtx, name)
		require.ErrorIs(t, err, ErrScheduleNotFound, "schedule %s should not have been created", name)
		require.Zero(t, s)
	}

	// Invalid timezone also surfaces at validate time.
	err = CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: "bad-tz",
		Schedule:     "0 0 * * * *",
		Workflow:     testWorkflowForSchedule,
		CronTimezone: "Not/A_Zone",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cron schedule")
}

func TestBackfillSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: "backfill-schedule",
		Schedule:     "*/1 * * * * *", // Every second for testing,
		Workflow:     testWorkflowForSchedule,
	})
	require.NoError(t, err)

	// Backfill last minute
	start := time.Now().Add(-1 * time.Minute)
	end := time.Now()

	ids, err := BackfillSchedule(dbosCtx, "backfill-schedule", start, end)
	require.NoError(t, err)

	// A `*/1 * * * * *` schedule over a one-minute window should enqueue
	// roughly 60 workflows; allow some slack for clock alignment.
	require.GreaterOrEqual(t, len(ids), 50, "backfill should have returned ~60 IDs, got %d", len(ids))
	backfilled, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDPrefix("sched-backfill-schedule-"))
	require.NoError(t, err)
	require.Equal(t, len(ids), len(backfilled), "returned IDs should match enqueued workflows")
	for _, wf := range backfilled {
		require.Equal(t, WorkflowStatusEnqueued, wf.Status)
	}

	// Idempotency: re-running the same backfill should not create duplicate rows
	// or bump recovery_attempts on the existing ones. Returned IDs should still
	// match the existing rows so callers can poll them.
	idsAgain, err := BackfillSchedule(dbosCtx, "backfill-schedule", start, end)
	require.NoError(t, err)
	require.Equal(t, len(ids), len(idsAgain), "second backfill must return the same IDs")
	again, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDPrefix("sched-backfill-schedule-"))
	require.NoError(t, err)
	require.Equal(t, len(backfilled), len(again), "second backfill must not enqueue duplicates")
	for _, wf := range again {
		require.Equal(t, 0, wf.Attempts, "second backfill must not bump recovery_attempts")
	}
}

// TestBackfillScheduleRecovery exercises the path where a backfilled workflow
// row is flipped to PENDING (simulating an executor crash mid-run) and then
// recovered via recoverPendingWorkflows. The recovered workflow must decode
// the ScheduledWorkflowInput written at backfill time and run it correctly.
func TestBackfillScheduleRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	scheduledInputCapture = sync.Map{}
	RegisterWorkflow(dbosCtx, testCapturingScheduledWorkflow)
	require.NoError(t, dbosCtx.Launch())

	// Use a far-future cron so the live scheduler doesn't fire while the test runs.
	const ctxValue = "backfill-recovery-context"
	const scheduleName = "backfill-recovery-schedule"
	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: scheduleName,
		Schedule:     "0 0 0 1 1 *", // Once a year,
		Workflow:     testCapturingScheduledWorkflow,
		Context:      ctxValue,
	})
	require.NoError(t, err)

	// Backfill a 5-second window of every-second ticks.
	start := time.Now().Add(-5 * time.Second).Truncate(time.Second)
	end := time.Now()
	c := dbosCtx.(*dbosContext)
	ids, err := c.systemDB.BackfillSchedule(c, sysdb.BackfillScheduleDBInput{
		ScheduleName: scheduleName,
		Schedule:     "*/1 * * * * *",
		StartTime:    start,
		EndTime:      end,
	})
	require.NoError(t, err)
	require.NotEmpty(t, ids, "backfill should have enqueued at least one workflow")

	target := ids[0]
	require.Eventually(t, func() bool {
		statuses, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(target))
		return err == nil && len(statuses) == 1 && statuses[0].Status == WorkflowStatusSuccess
	}, 10*time.Second, 50*time.Millisecond, "queue runner should run the backfilled workflow before recovery")

	// Drop the captured input from the first run so we can assert recovery's run populates it.
	scheduledInputCapture.Delete(target)

	setWorkflowStatusPending(t, dbosCtx, target)

	handles, err := recoverPendingWorkflows(c, []string{"local"})
	require.NoError(t, err)
	var recovered WorkflowHandle[any]
	for _, h := range handles {
		if h.GetWorkflowID() == target {
			recovered = h
			break
		}
	}
	require.NotNil(t, recovered, "recovery should have produced a handle for %s", target)

	result, err := recovered.GetResult()
	require.NoError(t, err)
	require.Equal(t, "completed", result)

	captured, ok := scheduledInputCapture.Load(target)
	require.True(t, ok, "workflow should have captured its input on recovery")
	got := captured.(ScheduledWorkflowInput)
	decodedCtx, err := DecodeScheduleContext[string](got)
	require.NoError(t, err)
	require.Equal(t, ctxValue, decodedCtx, "Context should round-trip through DB-encoded inputs")
	require.False(t, got.ScheduledTime.IsZero(), "ScheduledTime should be populated from DB-encoded inputs")
	require.False(t, got.ScheduledTime.Before(start.Add(-time.Second)), "ScheduledTime should be within the backfill window")
	require.False(t, got.ScheduledTime.After(end.Add(time.Second)), "ScheduledTime should be within the backfill window")

	// CreateSchedule inside the workflow is step-wrapped: must exist exactly once after recovery.
	inner, err := ListSchedules(dbosCtx, WithScheduleNamePrefixes(target+"-inner"))
	require.NoError(t, err)
	require.Len(t, inner, 1)
}

func TestTriggerSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	scheduledInputCapture = sync.Map{}
	RegisterWorkflow(dbosCtx, testCapturingScheduledWorkflow)

	require.NoError(t, dbosCtx.Launch())

	const ctxValue = "trigger-context-value"
	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: "trigger-schedule",
		Schedule:     "0 0 * * * *",
		Workflow:     testCapturingScheduledWorkflow,
		Context:      ctxValue,
	})
	require.NoError(t, err)

	beforeTrigger := time.Now()
	handle, err := TriggerSchedule[any](dbosCtx, "trigger-schedule")
	afterTrigger := time.Now()
	require.NoError(t, err)
	require.NotNil(t, handle)
	workflowID := handle.GetWorkflowID()
	require.NotEmpty(t, workflowID)
	require.Contains(t, workflowID, "trigger-schedule")

	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "completed", result)

	captured, ok := scheduledInputCapture.Load(workflowID)
	require.True(t, ok, "workflow should have captured its input")
	got := captured.(ScheduledWorkflowInput)
	decodedCtx, err := DecodeScheduleContext[string](got)
	require.NoError(t, err)
	require.Equal(t, ctxValue, decodedCtx, "Context should match the schedule's configured context")
	require.False(t, got.ScheduledTime.Before(beforeTrigger.Add(-time.Second)), "ScheduledTime should be at or after the trigger call")
	require.False(t, got.ScheduledTime.After(afterTrigger.Add(time.Second)), "ScheduledTime should be at or before the trigger call returns")

	// A second schedule sharing the same workflow function: ScheduleName is what
	// distinguishes their runs, since both have the same workflow name.
	err = CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: "trigger-schedule-b",
		Schedule:     "0 0 * * * *",
		Workflow:     testCapturingScheduledWorkflow,
		Context:      ctxValue,
	})
	require.NoError(t, err)
	handleB, err := TriggerSchedule[any](dbosCtx, "trigger-schedule-b")
	require.NoError(t, err)
	_, err = handleB.GetResult()
	require.NoError(t, err)

	// Filter by a single schedule name: contains that schedule's run, tagged with
	// its name, and excludes the other schedule's run. (Assert on membership, not
	// exact counts, so a cron tick firing mid-test cannot flake the assertions.)
	runsA, err := ListWorkflows(dbosCtx, WithFilterScheduleName("trigger-schedule"))
	require.NoError(t, err)
	idsA := make(map[string]bool, len(runsA))
	for _, wf := range runsA {
		require.Equal(t, "trigger-schedule", wf.ScheduleName)
		idsA[wf.ID] = true
	}
	require.True(t, idsA[workflowID], "triggered run should match its schedule name filter")
	require.False(t, idsA[handleB.GetWorkflowID()], "other schedule's run must not match")

	// Filter by a list of schedule names matches runs from both.
	runsBoth, err := ListWorkflows(dbosCtx, WithFilterScheduleName("trigger-schedule", "trigger-schedule-b"))
	require.NoError(t, err)
	idsBoth := make(map[string]bool, len(runsBoth))
	for _, wf := range runsBoth {
		require.Contains(t, []string{"trigger-schedule", "trigger-schedule-b"}, wf.ScheduleName)
		idsBoth[wf.ID] = true
	}
	require.True(t, idsBoth[workflowID])
	require.True(t, idsBoth[handleB.GetWorkflowID()])

	// A schedule name that produced no runs returns nothing.
	neverFired, err := ListWorkflows(dbosCtx, WithFilterScheduleName("never-fired"))
	require.NoError(t, err)
	require.Empty(t, neverFired)
}

func TestScheduleWithOptions(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName:      "full-options-schedule",
		Schedule:          "0 0 * * * *",
		Workflow:          testWorkflowForSchedule,
		Context:           map[string]string{"key": "value"},
		AutomaticBackfill: true,
		CronTimezone:      "America/New_York",
		QueueName:         "my-queue",
	})
	require.NoError(t, err)

	schedule, err := GetSchedule(dbosCtx, "full-options-schedule")
	require.NoError(t, err)
	require.True(t, schedule.AutomaticBackfill)
	require.Equal(t, "America/New_York", schedule.CronTimezone)
	require.Equal(t, "my-queue", schedule.QueueName)
}

func testWorkflowForSchedule(ctx Context, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

func testWorkflowForScheduleCustomName(ctx Context, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

var scheduledInputCapture sync.Map

// liveUpdateVersionCounts counts fires of testLiveUpdateScheduledWorkflow by the
// "version" value in the schedule context.
var (
	liveUpdateMu            sync.Mutex
	liveUpdateVersionCounts = map[int]int{}
)

func resetLiveUpdateVersionCounts() {
	liveUpdateMu.Lock()
	liveUpdateVersionCounts = map[int]int{}
	liveUpdateMu.Unlock()
}

func liveUpdateVersionCount(version int) int {
	liveUpdateMu.Lock()
	defer liveUpdateMu.Unlock()
	return liveUpdateVersionCounts[version]
}

type liveUpdateScheduleContext struct {
	Version int `json:"version"`
}

func testLiveUpdateScheduledWorkflow(ctx Context, input ScheduledWorkflowInput) (any, error) {
	cfg, err := DecodeScheduleContext[liveUpdateScheduleContext](input)
	if err != nil {
		return nil, err
	}
	if cfg.Version != 0 {
		liveUpdateMu.Lock()
		liveUpdateVersionCounts[cfg.Version]++
		liveUpdateMu.Unlock()
	}
	return "completed", nil
}

func testCapturingScheduledWorkflow(ctx Context, input ScheduledWorkflowInput) (any, error) {
	wfID, _ := GetWorkflowID(ctx)
	scheduledInputCapture.Store(wfID, input)
	// CreateSchedule is wrapped as a step via runAsTxn when called inside a
	// workflow. The inner cron never fires during tests.
	if err := CreateSchedule(ctx, ScheduleSpec{
		ScheduleName: wfID + "-inner",
		Schedule:     "0 0 0 1 1 *",
		Workflow:     testCapturingScheduledWorkflow,
	}); err != nil {
		return nil, err
	}
	return "completed", nil
}

var backfillRestartFiredEvent *Event

func testWorkflowForBackfillRestart(ctx Context, input ScheduledWorkflowInput) (any, error) {
	if backfillRestartFiredEvent != nil {
		backfillRestartFiredEvent.Set()
	}
	return "completed", nil
}

func TestAutomaticBackfillOnRestart(t *testing.T) {
	backfillRestartFiredEvent = NewEvent()

	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})

	RegisterWorkflow(dbosCtx, testWorkflowForBackfillRestart)
	require.NoError(t, dbosCtx.Launch())

	const scheduleName = "test-backfill-restart"
	const wfFQN = "github.com/dbos-inc/dbos-transact-golang/dbos.testWorkflowForBackfillRestart"

	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName:      scheduleName,
		Schedule:          "*/1 * * * * *", // Every second,
		Workflow:          testWorkflowForBackfillRestart,
		AutomaticBackfill: true,
	})
	require.NoError(t, err)

	// Wait for the schedule to fire at least once so LastFiredAt is set.
	backfillRestartFiredEvent.Wait()

	// Snapshot how many runs have succeeded before the restart.
	var before []WorkflowStatus
	require.Eventually(t, func() bool {
		before, err = ListWorkflows(dbosCtx,
			WithFilterName(wfFQN),
			WithFilterStatus(WorkflowStatusSuccess),
		)
		return err == nil && len(before) >= 1
	}, 3*time.Second, 50*time.Millisecond, "expected at least one successful run before shutdown")

	dbosCtx.Shutdown(dbosCtx, 5 * time.Second)

	// Reset the event so the next Wait only returns after a post-restart fire.
	backfillRestartFiredEvent.Clear()

	// Simulate missed schedules while the context is down.
	time.Sleep(2 * time.Second)

	dbosCtx2 := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx2.Shutdown(dbosCtx2, 5 * time.Second)

	RegisterWorkflow(dbosCtx2, testWorkflowForBackfillRestart)
	require.NoError(t, dbosCtx2.Launch())

	// Launch should backfill the missed runs; wait for one to execute.
	backfillRestartFiredEvent.Wait()

	// After backfill, the success count should have grown by more than one.
	require.Eventually(t, func() bool {
		after, err := ListWorkflows(dbosCtx2,
			WithFilterName(wfFQN),
			WithFilterStatus(WorkflowStatusSuccess),
		)
		return err == nil && len(after)-len(before) > 2
	}, 5*time.Second, 100*time.Millisecond, "expected backfill to produce more than one additional successful workflow")
}

func testWorkflowExpectingApplySchedulesError(ctx Context, _ string) (string, error) {
	err := ApplySchedules(ctx, []ScheduleSpec{
		{ScheduleName: "x", Workflow: testWorkflowForSchedule, Schedule: "0 0 * * * *"},
	})
	if err == nil {
		return "", nil
	}
	return err.Error(), nil
}

func testWorkflowExpectingBackfillScheduleError(ctx Context, _ string) (string, error) {
	_, err := BackfillSchedule(ctx, "any", time.Now().Add(-time.Minute), time.Now())
	if err == nil {
		return "", nil
	}
	return err.Error(), nil
}

func testWorkflowExpectingTriggerScheduleError(ctx Context, _ string) (string, error) {
	_, err := TriggerSchedule[any](ctx, "any")
	if err == nil {
		return "", nil
	}
	return err.Error(), nil
}

// TestScheduleWorkflowInternalRejections checks that ApplySchedules,
// BackfillSchedule, and TriggerSchedule reject calls from within a workflow.
func TestScheduleWorkflowInternalRejections(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	RegisterWorkflow(dbosCtx, testWorkflowExpectingApplySchedulesError)
	RegisterWorkflow(dbosCtx, testWorkflowExpectingBackfillScheduleError)
	RegisterWorkflow(dbosCtx, testWorkflowExpectingTriggerScheduleError)
	require.NoError(t, dbosCtx.Launch())

	cases := []struct {
		name string
		fn   Workflow[string, string]
		want string
	}{
		{"ApplySchedules", testWorkflowExpectingApplySchedulesError, "ApplySchedules cannot be called from within a workflow"},
		{"BackfillSchedule", testWorkflowExpectingBackfillScheduleError, "BackfillSchedule cannot be called from within a workflow"},
		{"TriggerSchedule", testWorkflowExpectingTriggerScheduleError, "TriggerSchedule cannot be called from within a workflow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handle, err := RunWorkflow(dbosCtx, tc.fn, "")
			require.NoError(t, err)
			result, err := handle.GetResult()
			require.NoError(t, err)
			require.Contains(t, result, tc.want)
		})
	}
}

// TestScheduleCronTimezone verifies that a non-empty CronTimezone is applied
// to the installed cron entry via the CRON_TZ= prefix: Next() from a known
// wall-clock reference should fall at the configured hour in that tz.
func TestScheduleCronTimezone(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 5 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	const scheduleName = "tz-schedule"
	err := CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: scheduleName,
		Schedule:     "0 0 9 * * *", // 09:00:00 every day,
		Workflow:     testWorkflowForSchedule,
		CronTimezone: "America/New_York",
	})
	require.NoError(t, err)

	c := dbosCtx.(*dbosContext)
	var entry cron.Entry
	require.Eventually(t, func() bool {
		id, ok := c.installedScheduleEntryID(scheduleName)
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

func TestScheduleNameSurvivesExportImport(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, dbosCtx.Launch())

	require.NoError(t, CreateSchedule(dbosCtx, ScheduleSpec{
		ScheduleName: "export-test",
		Schedule:     "0 0 0 * * *", // daily, won't fire during the test,
		Workflow:     testWorkflowForSchedule,
	}))
	t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, "export-test") })

	handle, err := TriggerSchedule[any](dbosCtx, "export-test")
	require.NoError(t, err)
	_, err = handle.GetResult()
	require.NoError(t, err)
	workflowID := handle.GetWorkflowID()

	original, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(workflowID))
	require.NoError(t, err)
	require.Len(t, original, 1)
	require.Equal(t, "export-test", original[0].ScheduleName)

	// Export, delete, then reimport: schedule_name must survive the round-trip.
	sdb := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
	exported, err := sdb.ExportWorkflow(dbosCtx, workflowID, true)
	require.NoError(t, err)
	require.NoError(t, DeleteWorkflows(dbosCtx, []string{workflowID}))
	gone, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(workflowID))
	require.NoError(t, err)
	require.Empty(t, gone)

	require.NoError(t, sdb.ImportWorkflow(dbosCtx, exported))
	imported, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(workflowID))
	require.NoError(t, err)
	require.Len(t, imported, 1)
	require.Equal(t, "export-test", imported[0].ScheduleName)

	// The reimported run is still found by the schedule name filter.
	byName, err := ListWorkflows(dbosCtx, WithFilterScheduleName("export-test"))
	require.NoError(t, err)
	require.Len(t, byName, 1)
	require.Equal(t, workflowID, byName[0].ID)
}

// A schedule can fire on an executor that does not have the target workflow
// registered: the tick enqueues by name and name resolution happens at dequeue
// time on a worker that has the function.
func TestScheduleFiresWithoutLocalRegistration(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, schedulerPollingInterval: 100 * time.Millisecond})
	defer dbosCtx.Shutdown(dbosCtx, 10 * time.Second)
	require.NoError(t, dbosCtx.Launch())

	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30 * time.Second) })

	const scheduleName = "unregistered-workflow-schedule"
	const workflowName = "workflowRegisteredOnAnotherWorker"
	const queueName = "queue-listened-elsewhere"
	require.NoError(t, client.CreateSchedule(client, ScheduleSpec{
		ScheduleName: scheduleName,
		WorkflowName: workflowName,
		Schedule:     "*/1 * * * * *",
		QueueName:    queueName,
	}))
	t.Cleanup(func() { _ = client.DeleteSchedule(client, scheduleName) })

	var enqueued WorkflowStatus
	require.Eventually(t, func() bool {
		wfs, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDPrefix("sched-"+scheduleName+"-"))
		if err != nil || len(wfs) == 0 {
			return false
		}
		enqueued = wfs[0]
		return true
	}, 15*time.Second, 100*time.Millisecond, "tick should enqueue even though the workflow is not registered locally")

	require.Equal(t, workflowName, enqueued.Name)
	require.Equal(t, queueName, enqueued.QueueName)
	require.Equal(t, scheduleName, enqueued.ScheduleName)
	require.Equal(t, WorkflowStatusEnqueued, enqueued.Status)

	sched, err := client.GetSchedule(client, scheduleName)
	require.NoError(t, err)
	require.NotZero(t, sched)
	require.NotNil(t, sched.LastFiredAt, "last_fired_at should be updated after the tick")
}

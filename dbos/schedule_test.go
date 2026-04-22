package dbos

import (
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestScheduleCRUD(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	t.Run("CreateSchedule", func(t *testing.T) {
		err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: "test-schedule-1",
			Schedule:     "*/5 * * * * *",
		}, WithScheduleContext("test-context"))
		require.NoError(t, err)

		// Verify schedule was created
		schedule, err := GetSchedule(dbosCtx, "test-schedule-1")
		require.NoError(t, err)
		require.NotNil(t, schedule)
		require.Equal(t, "test-schedule-1", schedule.ScheduleName)
		require.Equal(t, "github.com/dbos-inc/dbos-transact-golang/dbos.testWorkflowForSchedule", schedule.WorkflowName)
		require.Equal(t, "*/5 * * * * *", schedule.Schedule)
		require.Equal(t, ScheduleStatusActive, schedule.Status)
	})

	t.Run("ListSchedules", func(t *testing.T) {
		schedules, err := ListSchedules(dbosCtx, "")
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(schedules), 1)

		// Filter by status
		activeSchedules, err := ListSchedules(dbosCtx, ScheduleStatusActive)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(activeSchedules), 1)
	})

	t.Run("PauseResumeSchedule", func(t *testing.T) {
		err := PauseSchedule(dbosCtx, "test-schedule-1")
		require.NoError(t, err)

		schedule, err := GetSchedule(dbosCtx, "test-schedule-1")
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusPaused, schedule.Status)

		err = ResumeSchedule(dbosCtx, "test-schedule-1")
		require.NoError(t, err)

		schedule, err = GetSchedule(dbosCtx, "test-schedule-1")
		require.NoError(t, err)
		require.Equal(t, ScheduleStatusActive, schedule.Status)
	})

	t.Run("DeleteSchedule", func(t *testing.T) {
		err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
			ScheduleName: "test-schedule-delete",
			Schedule:     "0 0 * * * *",
		})
		require.NoError(t, err)

		err = DeleteSchedule(dbosCtx, "test-schedule-delete")
		require.NoError(t, err)

		schedule, err := GetSchedule(dbosCtx, "test-schedule-delete")
		require.NoError(t, err)
		require.Nil(t, schedule)
	})
}

func TestApplySchedules(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := ApplySchedules(dbosCtx, []ApplySchedulesRequest{
		{
			ScheduleName: "applied-schedule-1",
			WorkflowFn:   testWorkflowForSchedule,
			Schedule:     "*/10 * * * * *",
		},
		{
			ScheduleName: "applied-schedule-2",
			WorkflowFn:   testWorkflowForSchedule,
			Schedule:     "0 0 * * * *",
		},
	})
	require.NoError(t, err)

	schedules, err := ListSchedules(dbosCtx, ScheduleStatusActive)
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))
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
}

func TestTriggerSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "trigger-schedule",
		Schedule:     "0 0 * * * *",
	})
	require.NoError(t, err)

	workflowID, err := TriggerSchedule(dbosCtx, "trigger-schedule")
	require.NoError(t, err)
	require.NotEmpty(t, workflowID)
	require.Contains(t, workflowID, "trigger-schedule")
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

// Helper workflow for testing schedules — DB-backed schedules require the
// workflow input to be a ScheduledWorkflowInput.
func testWorkflowForSchedule(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
	return "completed", nil
}

func TestAutomaticBackfillOnRestart(t *testing.T) {
	config := setupDBOSOptions{dropDB: true, checkLeaks: true}
	dbosCtx := setupDBOS(t, config)

	// Register workflow and launch
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	err := dbosCtx.Launch()
	require.NoError(t, err)

	scheduleName := "test-backfill-restart"
	err = CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: scheduleName,
		Schedule:     "*/1 * * * * *", // Every second
	}, WithAutomaticBackfill(true))
	require.NoError(t, err)

	// Wait for it to fire once to set LastFiredAt
	time.Sleep(2 * time.Second)

	// Shutdown the context
	dbosCtx.Shutdown(5 * time.Second)

	// Wait for a few seconds to simulate "missed" schedules
	time.Sleep(3 * time.Second)

	// Restart DBOS
	dbosCtx2 := setupDBOS(t, setupDBOSOptions{dropDB: false, checkLeaks: true})
	defer dbosCtx2.Shutdown(5 * time.Second)

	// Register the same workflow again
	RegisterWorkflow(dbosCtx2, testWorkflowForSchedule)

	// Launch should trigger backfill
	err = dbosCtx2.Launch()
	require.NoError(t, err)

	// Wait for backfill to process
	time.Sleep(2 * time.Second)

	// Verify that multiple workflows were fired (backfilled)
	statuses, err := dbosCtx2.ListWorkflows(dbosCtx2)
	require.NoError(t, err)

	firedCount := 0
	for _, status := range statuses {
		if strings.Contains(status.Name, "testWorkflowForSchedule") {
			firedCount++
		}
	}
	require.Greater(t, firedCount, 0, "Should have backfilled some workflows")
}

func TestCronTriggering(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(5 * time.Second)

	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := dbosCtx.Launch()
	require.NoError(t, err)

	// Create a schedule that fires every second
	err = CreateSchedule(dbosCtx, testWorkflowForSchedule, CreateScheduleRequest{
		ScheduleName: "test-cron-trigger",
		Schedule:     "*/1 * * * * *",
	})
	require.NoError(t, err)

	// Wait for it to fire at least once
	time.Sleep(2500 * time.Millisecond)

	statuses, err := dbosCtx.ListWorkflows(dbosCtx)
	require.NoError(t, err)

	firedCount := 0
	for _, status := range statuses {
		if strings.Contains(status.Name, "testWorkflowForSchedule") {
			firedCount++
		}
	}
	require.GreaterOrEqual(t, firedCount, 1, "Schedule should have fired at least once")
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

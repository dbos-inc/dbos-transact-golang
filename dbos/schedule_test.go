package dbos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestScheduleCRUD(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	t.Run("CreateSchedule", func(t *testing.T) {
		err := CreateSchedule(dbosCtx, CreateScheduleRequest{
			ScheduleName: "test-schedule-1",
			WorkflowFn:   testWorkflowForSchedule,
			Schedule:     "*/5 * * * * *",
			Context:      "test-context",
		})
		require.NoError(t, err)

		// Verify schedule was created
		schedule, err := GetSchedule(dbosCtx, "test-schedule-1")
		require.NoError(t, err)
		require.NotNil(t, schedule)
		require.Equal(t, "test-schedule-1", schedule.ScheduleName)
		require.Equal(t, "testWorkflowForSchedule", schedule.WorkflowName)
		require.Equal(t, "*/5 * * * * *", schedule.Schedule)
		require.Equal(t, ScheduleStatusActive, schedule.Status)
	})

	t.Run("ListSchedules", func(t *testing.T) {
		schedules, err := ListSchedules(dbosCtx, "")
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(schedules), 1)

		// Filter by status
		activeSchedules, err := ListSchedules(dbosCtx, "ACTIVE")
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
		err := CreateSchedule(dbosCtx, CreateScheduleRequest{
			ScheduleName: "test-schedule-delete",
			WorkflowFn:   testWorkflowForSchedule,
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

	schedules, err := ListSchedules(dbosCtx, "ACTIVE")
	require.NoError(t, err)
	require.Equal(t, 2, len(schedules))
}

func TestBackfillSchedule(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, CreateScheduleRequest{
		ScheduleName: "backfill-schedule",
		WorkflowFn:   testWorkflowForSchedule,
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

	err := CreateSchedule(dbosCtx, CreateScheduleRequest{
		ScheduleName: "trigger-schedule",
		WorkflowFn:   testWorkflowForSchedule,
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

	err := CreateSchedule(dbosCtx, CreateScheduleRequest{
		ScheduleName:      "full-options-schedule",
		WorkflowFn:        testWorkflowForSchedule,
		Schedule:          "0 0 * * * *",
		Context:           map[string]string{"key": "value"},
		AutomaticBackfill: true,
		CronTimezone:      "America/New_York",
	})
	require.NoError(t, err)

	schedule, err := GetSchedule(dbosCtx, "full-options-schedule")
	require.NoError(t, err)
	require.True(t, schedule.AutomaticBackfill)
	require.Equal(t, "America/New_York", schedule.CronTimezone)
}

// Helper workflow for testing schedules - must match WorkflowFunc signature
func testWorkflowForSchedule(ctx DBOSContext, input any) (any, error) {
	return "completed", nil
}

func TestAutomaticBackfillOnRestart(t *testing.T) {
	config := setupDBOSOptions{dropDB: true, checkLeaks: true}
	dbosCtx := setupDBOS(t, config)

	// Register workflow and create a schedule
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	scheduleName := "test-backfill-restart"
	err := CreateSchedule(dbosCtx, CreateScheduleRequest{
		ScheduleName:      scheduleName,
		WorkflowFn:        testWorkflowForSchedule,
		Schedule:          "*/1 * * * * *", // Every second
		AutomaticBackfill: true,
	})
	require.NoError(t, err)

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
		if status.Name == "testWorkflowForSchedule" {
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
	err = CreateSchedule(dbosCtx, CreateScheduleRequest{
		ScheduleName: "test-cron-trigger",
		WorkflowFn:   testWorkflowForSchedule,
		Schedule:     "*/1 * * * * *",
	})
	require.NoError(t, err)

	// Wait for it to fire at least once
	time.Sleep(2500 * time.Millisecond)

	statuses, err := dbosCtx.ListWorkflows(dbosCtx)
	require.NoError(t, err)

	firedCount := 0
	for _, status := range statuses {
		if status.Name == "testWorkflowForSchedule" {
			firedCount++
		}
	}
	require.GreaterOrEqual(t, firedCount, 1, "Schedule should have fired at least once")
}

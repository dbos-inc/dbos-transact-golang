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

func TestScheduleWithQueue(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(10 * time.Second)

	// First register the workflow
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)

	err := CreateSchedule(dbosCtx, CreateScheduleRequest{
		ScheduleName: "queue-schedule",
		WorkflowFn:   testWorkflowForSchedule,
		Schedule:     "0 0 * * * *",
		QueueName:    "test-queue",
	})
	require.NoError(t, err)

	schedule, err := GetSchedule(dbosCtx, "queue-schedule")
	require.NoError(t, err)
	require.Equal(t, "test-queue", schedule.QueueName)
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
		QueueName:         "priority-queue",
	})
	require.NoError(t, err)

	schedule, err := GetSchedule(dbosCtx, "full-options-schedule")
	require.NoError(t, err)
	require.True(t, schedule.AutomaticBackfill)
	require.Equal(t, "America/New_York", schedule.CronTimezone)
	require.Equal(t, "priority-queue", schedule.QueueName)
}

// Helper workflow for testing schedules - must match WorkflowFunc signature
func testWorkflowForSchedule(ctx DBOSContext, input any) (any, error) {
	return "completed", nil
}

package dbos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func prefixLiteralWorkflow(_ Context, v string) (string, error) { return v, nil }

// Workflow ID and schedule name prefixes are literal: "_" and "%" must not act as
// LIKE wildcards, and a backslash must not act as an escape character.
func TestLiteralPrefixFilters(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	defer dbosCtx.Shutdown(dbosCtx, 10*time.Second)
	RegisterWorkflow(dbosCtx, prefixLiteralWorkflow)
	RegisterWorkflow(dbosCtx, testWorkflowForSchedule)
	require.NoError(t, Launch(dbosCtx))

	t.Run("WorkflowIDPrefix", func(t *testing.T) {
		ids := []string{`customer_1-job`, `customerA1-job`, `customer%2-job`, `customerZZ2-job`, `back\slash-job`, `backXslash-job`}
		for _, id := range ids {
			h, err := RunWorkflow(dbosCtx, prefixLiteralWorkflow, id, WithWorkflowID(id))
			require.NoError(t, err)
			_, err = h.GetResult()
			require.NoError(t, err)
		}
		for prefix, want := range map[string]string{
			`customer_1`: `customer_1-job`,
			`customer%2`: `customer%2-job`,
			`back\slash`: `back\slash-job`,
		} {
			rows, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDPrefix(prefix))
			require.NoError(t, err)
			require.Len(t, rows, 1, "prefix %q", prefix)
			require.Equal(t, want, rows[0].ID)
		}
	})

	t.Run("ScheduleNamePrefix", func(t *testing.T) {
		for _, name := range []string{`sched_1-a`, `schedA1-a`, `sched%2-a`, `schedZZ2-a`} {
			require.NoError(t, CreateSchedule(dbosCtx, ScheduleSpec{
				ScheduleName: name,
				Schedule:     "0 0 * * * *",
				Workflow:     testWorkflowForSchedule,
			}))
			name := name
			t.Cleanup(func() { _ = DeleteSchedule(dbosCtx, name) })
		}
		for prefix, want := range map[string]string{`sched_1`: `sched_1-a`, `sched%2`: `sched%2-a`} {
			got, err := ListSchedules(dbosCtx, WithScheduleNamePrefixes(prefix))
			require.NoError(t, err)
			require.Len(t, got, 1, "prefix %q", prefix)
			require.Equal(t, want, got[0].ScheduleName)
		}
	})
}

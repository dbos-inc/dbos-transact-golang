package dbos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowApplicationName reads a workflow_status row's owner directly.
func rowApplicationName(t *testing.T, ctx Context, workflowID string) *string {
	t.Helper()
	sdb := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := sdb.Dialect().RewriteQuery(fmt.Sprintf(
		`SELECT application_name FROM %sworkflow_status WHERE workflow_uuid = $1`,
		sdb.Dialect().SchemaPrefix(sdb.Schema())))
	var owner *string
	require.NoError(t, sdb.Pool().QueryRow(context.Background(), query, workflowID).Scan(&owner))
	return owner
}

// stepApplicationNames reads the owners of a workflow's recorded steps.
func stepApplicationNames(t *testing.T, ctx Context, workflowID string) []*string {
	t.Helper()
	sdb := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := sdb.Dialect().RewriteQuery(fmt.Sprintf(
		`SELECT application_name FROM %soperation_outputs WHERE workflow_uuid = $1 ORDER BY function_id`,
		sdb.Dialect().SchemaPrefix(sdb.Schema())))
	rows, err := sdb.Pool().Query(context.Background(), query, workflowID)
	require.NoError(t, err)
	defer rows.Close()
	var owners []*string
	for rows.Next() {
		var owner *string
		require.NoError(t, rows.Scan(&owner))
		owners = append(owners, owner)
	}
	require.NoError(t, rows.Err())
	return owners
}

// TestApplicationNameQueueIsolation verifies two applications sharing the
// internal queue only dequeue their own workflows, and that registered queue
// names are per-application addresses.
func TestApplicationNameQueueIsolation(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	// Each closure tags its output with the application that ran it.
	tagged := func(app string) func(ctx Context, input string) (string, error) {
		return func(ctx Context, input string) (string, error) {
			return app + ":" + input, nil
		}
	}
	RegisterWorkflow(ctxA, tagged("app-a"), WithWorkflowName("shared-workflow"))
	RegisterWorkflow(ctxB, tagged("app-b"), WithWorkflowName("shared-workflow"))

	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	// A queue name is a global address, so each application registers its own.
	_, err := RegisterQueue(ctxA, "app-a-queue")
	require.NoError(t, err)
	_, err = RegisterQueue(ctxB, "app-b-queue")
	require.NoError(t, err)

	// All on the internal queue, the one name every application shares, so
	// only ownership routes the workflows.
	var handlesA []WorkflowHandle[string]
	for range 5 {
		handle, err := Enqueue[string, string](ctxA, models.InternalQueueName, "shared-workflow", "x")
		require.NoError(t, err)
		handlesA = append(handlesA, handle)
	}
	handleB, err := Enqueue[string, string](ctxB, models.InternalQueueName, "shared-workflow", "x")
	require.NoError(t, err)

	for _, handle := range handlesA {
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "app-a:x", result, "app-a's workflow must be run by app-a")
		owner := rowApplicationName(t, ctxA, handle.GetWorkflowID())
		require.NotNil(t, owner)
		assert.Equal(t, "app-a", *owner)
	}
	resultB, err := handleB.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "app-b:x", resultB, "app-b's workflow must be run by app-b")

	// Registration made each queue owned, so neither application enumerates the other's.
	queuesA, err := ListQueues(ctxA)
	require.NoError(t, err)
	namesA := make(map[string]bool, len(queuesA))
	for _, q := range queuesA {
		namesA[q.GetName()] = true
	}
	assert.True(t, namesA["app-a-queue"])
	assert.False(t, namesA["app-b-queue"])
}

// TestApplicationNameClaimsUnclaimed verifies a named application runs and
// claims workflows enqueued by a nameless client.
func TestApplicationNameClaimsUnclaimed(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})

	stepped := func(ctx Context, input string) (string, error) {
		return RunAsStep(ctx, func(context.Context) (string, error) {
			return input + "-stepped", nil
		}, WithStepName("tagStep"))
	}
	RegisterWorkflow(ctxA, stepped, WithWorkflowName("claimable-workflow"))
	require.NoError(t, Launch(ctxA))

	const queueName = "app-name-claim-queue"
	_, err := RegisterQueue(ctxA, queueName)
	require.NoError(t, err)

	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })

	handle, err := Enqueue[string](client, queueName, "claimable-workflow", "x")
	require.NoError(t, err)

	// The client wrote an unclaimed row.
	require.Nil(t, rowApplicationName(t, ctxA, handle.GetWorkflowID()))

	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "x-stepped", result)

	owner := rowApplicationName(t, ctxA, handle.GetWorkflowID())
	require.NotNil(t, owner, "the dequeue must claim the unclaimed row")
	assert.Equal(t, "app-a", *owner)

	// Steps are stamped with the running application.
	stepOwners := stepApplicationNames(t, ctxA, handle.GetWorkflowID())
	require.NotEmpty(t, stepOwners)
	for _, stepOwner := range stepOwners {
		require.NotNil(t, stepOwner)
		assert.Equal(t, "app-a", *stepOwner)
	}
}

// TestApplicationNameRecoveryIsolation verifies recovery never re-enqueues a
// peer's PENDING workflows, despite the shared "local" executor ID.
func TestApplicationNameRecoveryIsolation(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	simple := func(ctx Context, input string) (string, error) { return input, nil }
	RegisterWorkflow(ctxA, simple, WithWorkflowName("recovery-workflow"))
	RegisterWorkflow(ctxB, simple, WithWorkflowName("recovery-workflow"))

	// Nothing is launched, so rows stay where the test puts them.
	handle, err := Enqueue[string](ctxA, "recovery-isolation-queue", "recovery-workflow", "x")
	require.NoError(t, err)
	workflowID := handle.GetWorkflowID()

	// Simulate a crashed executor: PENDING, owned by app-a, executor "local".
	sdb := ctxA.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := sdb.Dialect().RewriteQuery(fmt.Sprintf(
		`UPDATE %sworkflow_status SET status = $1, executor_id = $2 WHERE workflow_uuid = $3`,
		sdb.Dialect().SchemaPrefix(sdb.Schema())))
	_, err = sdb.Pool().Exec(context.Background(), query, models.WorkflowStatusPending, "local", workflowID)
	require.NoError(t, err)

	// app-b's recovery must not touch app-a's workflow.
	recovered, err := recoverPendingWorkflows(ctxB.(*dbosContext), []string{"local"})
	require.NoError(t, err)
	assert.Empty(t, recovered, "app-b must not recover app-a's workflows")

	// app-a's recovery re-enqueues it.
	recovered, err = recoverPendingWorkflows(ctxA.(*dbosContext), []string{"local"})
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, workflowID, recovered[0].GetWorkflowID())
}

// TestApplicationNameListScoping verifies lists default to the caller's own
// application plus unclaimed rows, while ID-keyed reads address any workflow.
func TestApplicationNameListScoping(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	simple := func(ctx Context, input string) (string, error) { return input, nil }
	RegisterWorkflow(ctxA, simple, WithWorkflowName("list-workflow"))
	RegisterWorkflow(ctxB, simple, WithWorkflowName("list-workflow"))
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	handleA, err := RunWorkflow(ctxA, simple, "a")
	require.NoError(t, err)
	_, err = handleA.GetResult()
	require.NoError(t, err)
	handleB, err := RunWorkflow(ctxB, simple, "b")
	require.NoError(t, err)
	_, err = handleB.GetResult()
	require.NoError(t, err)

	listedA, err := ListWorkflows(ctxA)
	require.NoError(t, err)
	require.Len(t, listedA, 1, "app-a must only list its own workflows")
	assert.Equal(t, handleA.GetWorkflowID(), listedA[0].ID)
	assert.Equal(t, "app-a", listedA[0].ApplicationName)

	// app-b can address app-a's workflow by ID.
	crossListed, err := ListWorkflows(ctxB, WithFilterWorkflowIDs(handleA.GetWorkflowID()))
	require.NoError(t, err)
	require.Len(t, crossListed, 1)
	assert.Equal(t, "app-a", crossListed[0].ApplicationName)

	// A nameless client lists every application's workflows.
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })
	listedAll, err := ListWorkflows(client)
	require.NoError(t, err)
	assert.Len(t, listedAll, 2)
}

// TestApplicationNameForkInherits verifies a fork inherits the source's
// owner, whoever forks it.
func TestApplicationNameForkInherits(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	stepped := func(ctx Context, input string) (string, error) {
		return RunAsStep(ctx, func(context.Context) (string, error) {
			return input + "-stepped", nil
		}, WithStepName("forkStep"))
	}
	RegisterWorkflow(ctxA, stepped, WithWorkflowName("fork-workflow"))
	RegisterWorkflow(ctxB, stepped, WithWorkflowName("fork-workflow"))
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	handle, err := RunWorkflow(ctxA, stepped, "x")
	require.NoError(t, err)
	_, err = handle.GetResult()
	require.NoError(t, err)

	// app-b forks app-a's workflow: the fork belongs to app-a and runs there.
	forkHandle, err := ForkWorkflow[string](ctxB, ForkWorkflowInput{
		OriginalWorkflowID: handle.GetWorkflowID(),
		StartStep:          1,
	})
	require.NoError(t, err)
	forkOwner := rowApplicationName(t, ctxA, forkHandle.GetWorkflowID())
	require.NotNil(t, forkOwner)
	assert.Equal(t, "app-a", *forkOwner)

	result, err := forkHandle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "x-stepped", result)

	// The copied checkpoints carry the fork's owner too.
	stepOwners := stepApplicationNames(t, ctxA, forkHandle.GetWorkflowID())
	require.NotEmpty(t, stepOwners)
	require.NotNil(t, stepOwners[0])
	assert.Equal(t, "app-a", *stepOwners[0])
}

// TestApplicationNameGarbageCollectionScoping verifies one application's
// retention policy never deletes a peer's rows.
func TestApplicationNameGarbageCollectionScoping(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	simple := func(ctx Context, input string) (string, error) { return input, nil }
	RegisterWorkflow(ctxA, simple, WithWorkflowName("gc-workflow"))
	RegisterWorkflow(ctxB, simple, WithWorkflowName("gc-workflow"))
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	// Three app-a rows against a batch size of two: one bounded batch, then the tail.
	for range 3 {
		handleA, err := RunWorkflow(ctxA, simple, "a")
		require.NoError(t, err)
		_, err = handleA.GetResult()
		require.NoError(t, err)
	}
	handleB, err := RunWorkflow(ctxB, simple, "b")
	require.NoError(t, err)
	_, err = handleB.GetResult()
	require.NoError(t, err)

	batchSize := 2
	cutoff := time.Now().Add(time.Hour).UnixMilli()
	gcInput := sysdb.GarbageCollectWorkflowsInput{CutoffEpochTimestampMs: &cutoff, BatchSize: &batchSize}

	// app-b's GC collects its own row and spares every app-a row.
	require.NoError(t, ctxB.(*dbosContext).systemDB.GarbageCollectWorkflows(ctxB, gcInput))
	assert.Equal(t, 3, ownedRowCount(t, ctxA, "app-a"))
	assert.Equal(t, 0, ownedRowCount(t, ctxA, "app-b"))

	// app-a's GC then collects its own three, across two batches.
	require.NoError(t, ctxA.(*dbosContext).systemDB.GarbageCollectWorkflows(ctxA, gcInput))
	assert.Equal(t, 0, ownedRowCount(t, ctxA, "app-a"))
}

// ownedRowCount counts the workflow_status rows an application owns.
func ownedRowCount(t *testing.T, ctx Context, appName string) int {
	t.Helper()
	sdb := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := sdb.Dialect().RewriteQuery(fmt.Sprintf(
		`SELECT COUNT(*) FROM %sworkflow_status WHERE application_name = $1`,
		sdb.Dialect().SchemaPrefix(sdb.Schema())))
	var count int
	require.NoError(t, sdb.Pool().QueryRow(context.Background(), query, appName).Scan(&count))
	return count
}

// TestApplicationVersionIncludesAppName verifies same-binary peers under
// different names get distinct computed versions.
func TestApplicationVersionIncludesAppName(t *testing.T) {
	versionA := computeApplicationVersion("app-a")
	versionB := computeApplicationVersion("app-b")
	require.NotEmpty(t, versionA)
	assert.NotEqual(t, versionA, versionB)
	assert.Equal(t, versionA, computeApplicationVersion("app-a"))
}

// rawExec runs a statement directly against the system database; %s in the
// query is replaced with the schema prefix.
func rawExec(t *testing.T, ctx Context, query string, args ...any) {
	t.Helper()
	sdb := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	q := sdb.Dialect().RewriteQuery(fmt.Sprintf(query, sdb.Dialect().SchemaPrefix(sdb.Schema())))
	_, err := sdb.Pool().Exec(context.Background(), q, args...)
	require.NoError(t, err)
}

// rawQueryString reads a single nullable string from the system database.
func rawQueryString(t *testing.T, ctx Context, query string, args ...any) *string {
	t.Helper()
	sdb := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
	q := sdb.Dialect().RewriteQuery(fmt.Sprintf(query, sdb.Dialect().SchemaPrefix(sdb.Schema())))
	var value *string
	require.NoError(t, sdb.Pool().QueryRow(context.Background(), q, args...).Scan(&value))
	return value
}

// TestApplicationNameRegistryConflicts verifies queue and schedule names stay
// globally unique: a peer's name is a conflict, not a silent overwrite, while
// a nameless writer administers any row without taking it.
func TestApplicationNameRegistryConflicts(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})

	rawExec(t, ctxA, `INSERT INTO %squeues (queue_id, name, created_at, updated_at, application_name) VALUES ($1, $2, $3, $4, $5)`,
		"conflict-queue-id", "conflict-queue", int64(1), int64(1), "other-app")
	rawExec(t, ctxA, `INSERT INTO %sworkflow_schedules (schedule_id, schedule_name, workflow_name, schedule, status, context, cron_timezone, application_name) VALUES ($1, $2, $3, $4, $5, $6, '', $7)`,
		"conflict-schedule-id", "conflict-schedule", "some-workflow", "0 0 0 1 1 *", "ACTIVE", "null", "other-app")

	_, err := RegisterQueue(ctxA, "conflict-queue")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered by application 'other-app'")

	// Declining to update is still a collision: only the owner ever polls the queue.
	_, err = RegisterQueue(ctxA, "conflict-queue", WithQueueOnConflict(QueueConflictNeverUpdate))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered by application 'other-app'")

	err = CreateSchedule(ctxA, ScheduleSpec{ScheduleName: "conflict-schedule", WorkflowName: "some-workflow", Schedule: "0 0 0 1 1 *"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered by application 'other-app'")

	// A nameless writer updates the row without stripping the owner off it.
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })
	_, err = RegisterQueue(client, "conflict-queue", WithGlobalConcurrency(7), WithQueueOnConflict(QueueConflictAlwaysUpdate))
	require.NoError(t, err)
	owner := rawQueryString(t, ctxA, `SELECT application_name FROM %squeues WHERE name = $1`, "conflict-queue")
	require.NotNil(t, owner)
	assert.Equal(t, "other-app", *owner)
	concurrency := rawQueryString(t, ctxA, `SELECT CAST(concurrency AS TEXT) FROM %squeues WHERE name = $1`, "conflict-queue")
	require.NotNil(t, concurrency)
	assert.Equal(t, "7", *concurrency)

	// The identity read still resolves a peer's queue by name.
	theirs, err := RetrieveQueue(ctxA, "conflict-queue")
	require.NoError(t, err)
	assert.Equal(t, "conflict-queue", theirs.GetName())
}

// TestApplicationNameRegistryUnclaimed verifies re-registering an unclaimed
// queue or schedule claims it in place, and list reads scope to the caller's
// own application plus unclaimed rows.
func TestApplicationNameRegistryUnclaimed(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})

	scheduled := func(ctx Context, input ScheduledWorkflowInput) (any, error) { return nil, nil }
	RegisterWorkflow(ctxA, scheduled, WithWorkflowName("scheduled-wf"))

	rawExec(t, ctxA, `INSERT INTO %squeues (queue_id, name, created_at, updated_at, application_name) VALUES ($1, $2, $3, $4, $5)`,
		"legacy-queue-id", "unclaimed-queue", int64(1), int64(1), nil)
	rawExec(t, ctxA, `INSERT INTO %squeues (queue_id, name, created_at, updated_at, application_name) VALUES ($1, $2, $3, $4, $5)`,
		"foreign-queue-id", "theirs-queue", int64(1), int64(1), "other-app")
	rawExec(t, ctxA, `INSERT INTO %sworkflow_schedules (schedule_id, schedule_name, workflow_name, schedule, status, context, cron_timezone, application_name) VALUES ($1, $2, $3, $4, $5, $6, '', $7)`,
		"legacy-schedule-id", "unclaimed-sched", "scheduled-wf", "0 0 0 1 1 *", "ACTIVE", "null", nil)
	rawExec(t, ctxA, `INSERT INTO %sworkflow_schedules (schedule_id, schedule_name, workflow_name, schedule, status, context, cron_timezone, application_name) VALUES ($1, $2, $3, $4, $5, $6, '', $7)`,
		"foreign-schedule-id", "theirs-sched", "scheduled-wf", "0 0 0 1 1 *", "ACTIVE", "null", "other-app")

	// Re-registering claims the unclaimed rows without recreating them.
	_, err := RegisterQueue(ctxA, "unclaimed-queue")
	require.NoError(t, err)
	require.NoError(t, ApplySchedules(ctxA, []ScheduleSpec{{
		ScheduleName: "unclaimed-sched",
		WorkflowName: "scheduled-wf",
		Schedule:     "0 0 0 1 1 *",
	}}))
	queueOwner := rawQueryString(t, ctxA, `SELECT application_name FROM %squeues WHERE name = $1`, "unclaimed-queue")
	require.NotNil(t, queueOwner)
	assert.Equal(t, "app-a", *queueOwner)
	queueID := rawQueryString(t, ctxA, `SELECT queue_id FROM %squeues WHERE name = $1`, "unclaimed-queue")
	require.NotNil(t, queueID)
	assert.Equal(t, "legacy-queue-id", *queueID, "claiming must not recreate the row")
	scheduleOwner := rawQueryString(t, ctxA, `SELECT application_name FROM %sworkflow_schedules WHERE schedule_name = $1`, "unclaimed-sched")
	require.NotNil(t, scheduleOwner)
	assert.Equal(t, "app-a", *scheduleOwner)
	scheduleID := rawQueryString(t, ctxA, `SELECT schedule_id FROM %sworkflow_schedules WHERE schedule_name = $1`, "unclaimed-sched")
	require.NotNil(t, scheduleID)
	assert.Equal(t, "legacy-schedule-id", *scheduleID, "claiming must not recreate the row")

	// Lists scope to this application's rows plus unclaimed ones.
	queues, err := ListQueues(ctxA)
	require.NoError(t, err)
	queueNames := make(map[string]bool, len(queues))
	for _, q := range queues {
		queueNames[q.GetName()] = true
	}
	assert.True(t, queueNames["unclaimed-queue"])
	assert.False(t, queueNames["theirs-queue"])

	schedules, err := ListSchedules(ctxA)
	require.NoError(t, err)
	scheduleNames := make(map[string]bool, len(schedules))
	for _, s := range schedules {
		scheduleNames[s.ScheduleName] = true
	}
	assert.True(t, scheduleNames["unclaimed-sched"])
	assert.False(t, scheduleNames["theirs-sched"])

	// A nameless client lists every application's rows.
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })
	allQueues, err := ListQueues(client)
	require.NoError(t, err)
	allNames := make(map[string]bool, len(allQueues))
	for _, q := range allQueues {
		allNames[q.GetName()] = true
	}
	assert.True(t, allNames["theirs-queue"])

	// An exact-name filter composes with the app scope: a peer's schedule is
	// invisible unless its application is named explicitly.
	scoped, err := ListSchedules(ctxA, WithScheduleNames("theirs-sched"))
	require.NoError(t, err)
	assert.Empty(t, scoped)
	theirs, err := ListSchedules(ctxA, WithScheduleNames("theirs-sched"), WithScheduleApplicationNames("other-app"))
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.Equal(t, "other-app", theirs[0].ApplicationName)

	// GetSchedule is a name-addressed identity read: it stays global.
	sched, err := GetSchedule(ctxA, "theirs-sched")
	require.NoError(t, err)
	assert.Equal(t, "other-app", sched.ApplicationName)
}

// TestApplicationVersionsPerApplication verifies version rows record their
// registrar and the latest-version lookup is scoped to it.
func TestApplicationVersionsPerApplication(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	require.NoError(t, Launch(ctxA))
	futureMs := time.Now().Add(24 * time.Hour).UnixMilli()

	// A peer's newer version is not this application's latest.
	rawExec(t, ctxA, `INSERT INTO %sapplication_versions (version_id, version_name, version_timestamp, created_at, application_name) VALUES ($1, $2, $3, $4, $5)`,
		"foreign-version-id", "foreign-version", futureMs, futureMs, "other-app")
	latest, err := GetLatestApplicationVersion(ctxA)
	require.NoError(t, err)
	assert.Equal(t, ctxA.GetApplicationVersion(), latest.Name)

	// An unclaimed newer version is every application's latest.
	rawExec(t, ctxA, `INSERT INTO %sapplication_versions (version_id, version_name, version_timestamp, created_at, application_name) VALUES ($1, $2, $3, $4, $5)`,
		"unclaimed-version-id", "unclaimed-version", futureMs+1, futureMs+1, nil)
	latest, err = GetLatestApplicationVersion(ctxA)
	require.NoError(t, err)
	assert.Equal(t, "unclaimed-version", latest.Name)

	// Listing scopes to own plus unclaimed versions.
	versions, err := ListApplicationVersions(ctxA)
	require.NoError(t, err)
	names := make(map[string]bool, len(versions))
	for _, v := range versions {
		names[v.Name] = true
	}
	assert.True(t, names["unclaimed-version"])
	assert.False(t, names["foreign-version"])

	// Registering a pinned version claims a pre-upgrade row without recreating or retiming it.
	rawExec(t, ctxA, `INSERT INTO %sapplication_versions (version_id, version_name, version_timestamp, created_at, application_name) VALUES ($1, $2, $3, $4, $5)`,
		"pinned-version-id", "1.0.0", int64(7), int64(7), nil)
	sdb := ctxA.(*dbosContext).systemDB.(*sysdb.SysDB)
	require.NoError(t, sdb.CreateApplicationVersion(context.Background(), "1.0.0", ctxA.(*dbosContext).requestedOwner("")))
	pinnedOwner := rawQueryString(t, ctxA, `SELECT application_name FROM %sapplication_versions WHERE version_name = $1`, "1.0.0")
	require.NotNil(t, pinnedOwner)
	assert.Equal(t, "app-a", *pinnedOwner)
	pinnedID := rawQueryString(t, ctxA, `SELECT version_id FROM %sapplication_versions WHERE version_name = $1`, "1.0.0")
	require.NotNil(t, pinnedID)
	assert.Equal(t, "pinned-version-id", *pinnedID)

	// Promoting a peer's version is a collision, not a retiming.
	err = SetLatestApplicationVersion(ctxA, "foreign-version")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered by application 'other-app'")

	// This application owns the row, so it promotes it, past the unclaimed one.
	require.NoError(t, SetLatestApplicationVersion(ctxA, "1.0.0"))
	latest, err = GetLatestApplicationVersion(ctxA)
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", latest.Name)
	assert.Equal(t, "app-a", latest.ApplicationName)

	// Promotion claims an unclaimed row, which would otherwise be every peer's latest.
	require.NoError(t, SetLatestApplicationVersion(ctxA, "unclaimed-version"))
	unclaimedOwner := rawQueryString(t, ctxA, `SELECT application_name FROM %sapplication_versions WHERE version_name = $1`, "unclaimed-version")
	require.NotNil(t, unclaimedOwner)
	assert.Equal(t, "app-a", *unclaimedOwner)
}

// TestScheduleOwnerRoutesTrigger verifies a schedule's runs go to the owning
// application, whoever fires them.
func TestScheduleOwnerRoutesTrigger(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	ran := func(ctx Context, input ScheduledWorkflowInput) (any, error) { return "ran-on-app-a", nil }
	RegisterWorkflow(ctxA, ran, WithWorkflowName("owned-sched-wf"))
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	require.NoError(t, CreateSchedule(ctxA, ScheduleSpec{
		ScheduleName: "owned-sched",
		WorkflowName: "owned-sched-wf",
		Schedule:     "0 0 0 1 1 *",
	}))

	// app-b triggers app-a's schedule: the run belongs to app-a and runs there.
	handle, err := TriggerSchedule[any](ctxB, "owned-sched")
	require.NoError(t, err)
	owner := rowApplicationName(t, ctxA, handle.GetWorkflowID())
	require.NotNil(t, owner)
	assert.Equal(t, "app-a", *owner)

	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "ran-on-app-a", result)
}

// TestScheduleSpecApplicationName verifies ScheduleSpec.ApplicationName routes
// ownership and WithScheduleApplicationNames widens the list scope.
func TestScheduleSpecApplicationName(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	// app-a creates a schedule owned by app-b.
	require.NoError(t, CreateSchedule(ctxA, ScheduleSpec{
		ScheduleName:    "delegated-sched",
		WorkflowName:    "delegated-wf",
		Schedule:        "0 0 0 1 1 *",
		ApplicationName: "app-b",
	}))

	mine, err := ListSchedules(ctxA, WithScheduleNames("delegated-sched"))
	require.NoError(t, err)
	assert.Empty(t, mine)

	theirs, err := ListSchedules(ctxB, WithScheduleNames("delegated-sched"))
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.Equal(t, "app-b", theirs[0].ApplicationName)

	widened, err := ListSchedules(ctxA, WithScheduleNames("delegated-sched"), WithScheduleApplicationNames("app-b"))
	require.NoError(t, err)
	require.Len(t, widened, 1)

	// ApplySchedules honors the spec's ApplicationName too.
	require.NoError(t, ApplySchedules(ctxB, []ScheduleSpec{{
		ScheduleName:    "applied-sched",
		WorkflowName:    "applied-wf",
		Schedule:        "0 0 0 1 1 *",
		ApplicationName: "app-a",
	}}))
	applied, err := ListSchedules(ctxA, WithScheduleNames("applied-sched"))
	require.NoError(t, err)
	require.Len(t, applied, 1)
	assert.Equal(t, "app-a", applied[0].ApplicationName)

	// A nameless client leaving the field empty creates an unclaimed schedule,
	// which every application lists by default.
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })
	require.NoError(t, CreateSchedule(client, ScheduleSpec{
		ScheduleName: "unclaimed-spec-sched",
		WorkflowName: "unclaimed-wf",
		Schedule:     "0 0 0 1 1 *",
	}))
	for _, ctx := range []Context{ctxA, ctxB} {
		listed, err := ListSchedules(ctx, WithScheduleNames("unclaimed-spec-sched"))
		require.NoError(t, err)
		require.Len(t, listed, 1)
		assert.Equal(t, "", listed[0].ApplicationName)
	}
}

// TestQueueApplicationName verifies WithQueueApplicationName routes ownership
// and WithQueueApplicationNames widens the list scope.
func TestQueueApplicationName(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	findQueue := func(qs []Queue, name string) Queue {
		for _, q := range qs {
			if q.GetName() == name {
				return q
			}
		}
		return nil
	}

	// app-a registers a queue owned by app-b.
	q, err := RegisterQueue(ctxA, "delegated-queue", WithQueueApplicationName("app-b"))
	require.NoError(t, err)
	assert.Equal(t, "app-b", q.GetApplicationName())

	mine, err := ListQueues(ctxA)
	require.NoError(t, err)
	assert.Nil(t, findQueue(mine, "delegated-queue"))

	theirs, err := ListQueues(ctxB)
	require.NoError(t, err)
	require.NotNil(t, findQueue(theirs, "delegated-queue"))

	widened, err := ListQueues(ctxA, WithListQueuesApplicationNames("app-b"))
	require.NoError(t, err)
	require.NotNil(t, findQueue(widened, "delegated-queue"))

	// Registering the same name under this application's own name is a collision.
	_, err = RegisterQueue(ctxA, "delegated-queue")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered by application 'app-b'")

	// A nameless client registers an unclaimed queue, listed by every application.
	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })
	unclaimed, err := RegisterQueue(client, "unclaimed-opt-queue")
	require.NoError(t, err)
	assert.Equal(t, "", unclaimed.GetApplicationName())
	for _, ctx := range []Context{ctxA, ctxB} {
		listed, err := ListQueues(ctx)
		require.NoError(t, err)
		require.NotNil(t, findQueue(listed, "unclaimed-opt-queue"))
	}
}

// TestEnqueueApplicationName verifies WithEnqueueApplicationName routes the
// workflow to the owning application, which runs it at its own latest version.
func TestEnqueueApplicationName(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	tagged := func(app string) func(ctx Context, input string) (string, error) {
		return func(ctx Context, input string) (string, error) { return app + ":" + input, nil }
	}
	RegisterWorkflow(ctxA, tagged("app-a"), WithWorkflowName("owned-enqueue-wf"))
	RegisterWorkflow(ctxB, tagged("app-b"), WithWorkflowName("owned-enqueue-wf"))
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	// app-b enqueues a workflow owned by app-a. The default version stays
	// unset (app-b's own would never match), so app-a dequeues it at its
	// latest version and runs it.
	handle, err := Enqueue[string, string](ctxB, models.InternalQueueName, "owned-enqueue-wf", "x",
		WithEnqueueApplicationName("app-a"))
	require.NoError(t, err)
	owner := rowApplicationName(t, ctxA, handle.GetWorkflowID())
	require.NotNil(t, owner)
	assert.Equal(t, "app-a", *owner)
	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "app-a:x", result)

	// The run belongs to app-a: app-b's default list scope excludes it, and
	// naming the application widens the scope. (An exact-ID lookup would stay
	// an unscoped identity read.)
	mine, err := ListWorkflows(ctxB, WithFilterName("owned-enqueue-wf"))
	require.NoError(t, err)
	assert.Empty(t, mine)
	theirs, err := ListWorkflows(ctxB, WithFilterName("owned-enqueue-wf"), WithFilterApplicationName("app-a"))
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.Equal(t, "app-a", theirs[0].ApplicationName)
}

// TestClientAppName verifies a client configured with an application name acts
// on its behalf: registry writes claim for it and its enqueues run there.
func TestClientAppName(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	echo := func(ctx Context, input string) (string, error) { return input + "-ran", nil }
	RegisterWorkflow(ctxA, echo, WithWorkflowName("client-owned-wf"))
	require.NoError(t, Launch(ctxA))

	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t), AppName: "app-a"})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })

	q, err := RegisterQueue(client, "client-owned-queue", WithQueueOnConflict(QueueConflictAlwaysUpdate))
	require.NoError(t, err)
	assert.Equal(t, "app-a", q.GetApplicationName())

	// Client enqueues carry the client's application and no version, so app-a
	// runs them at its latest version.
	handle, err := Enqueue[string, string](client, "client-owned-queue", "client-owned-wf", "x")
	require.NoError(t, err)
	owner := rowApplicationName(t, ctxA, handle.GetWorkflowID())
	require.NotNil(t, owner)
	assert.Equal(t, "app-a", *owner)
	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "x-ran", result)
}

// TestRenameApplication verifies RenameApplication re-owns every table's rows —
// atomically for the registries and in-flight workflows, in batches for the
// history — leaving other applications' rows alone.
func TestRenameApplication(t *testing.T) {
	ctxA := setupDBOS(t, setupDBOSOptions{dropDB: true, appName: "app-a"})
	ctxB := setupDBOS(t, setupDBOSOptions{appName: "app-b"})

	twoSteps := func(app string) func(ctx Context, input string) (string, error) {
		return func(ctx Context, input string) (string, error) {
			for range 2 {
				if _, err := RunAsStep(ctx, func(context.Context) (string, error) { return "s", nil }); err != nil {
					return "", err
				}
			}
			return app + ":" + input, nil
		}
	}
	RegisterWorkflow(ctxA, twoSteps("app-a"), WithWorkflowName("rename-wf"))
	RegisterWorkflow(ctxB, twoSteps("app-b"), WithWorkflowName("rename-wf"))
	require.NoError(t, Launch(ctxA))
	require.NoError(t, Launch(ctxB))

	_, err := RegisterQueue(ctxA, "rename-queue")
	require.NoError(t, err)
	require.NoError(t, CreateSchedule(ctxA, ScheduleSpec{
		ScheduleName: "rename-sched",
		Schedule:     "0 0 0 1 1 *",
		WorkflowName: "rename-wf",
	}))

	// Three completed workflows with two steps each, and one of app-b's for contrast.
	var done []WorkflowHandle[string]
	for range 3 {
		h, err := Enqueue[string, string](ctxA, "rename-queue", "rename-wf", "x")
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
		done = append(done, h)
	}
	hB, err := Enqueue[string, string](ctxB, models.InternalQueueName, "rename-wf", "x")
	require.NoError(t, err)
	_, err = hB.GetResult()
	require.NoError(t, err)

	// Stop the application being renamed: its dequeues race the rename.
	require.NoError(t, Shutdown(ctxA, 30*time.Second))

	client, err := NewClient(context.Background(), ClientConfig{DatabaseURL: backendDatabaseURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30*time.Second) })

	// An ENQUEUED workflow left behind by the stopped app-a: the atomic path.
	pending, err := Enqueue[string, string](client, "rename-queue", "rename-wf", "x",
		WithEnqueueApplicationName("app-a"))
	require.NoError(t, err)

	// Batch size 1 forces the range loop over the terminal history.
	counts, err := RenameApplication(client, RenameApplicationInput{OldName: "app-a", NewName: "app-c", BatchSize: 1})
	require.NoError(t, err)
	assert.Equal(t, ApplicationRowCounts{Queues: 1, Schedules: 1, Versions: 1, Workflows: 4, Steps: 6}, counts)

	for _, h := range done {
		owner := rowApplicationName(t, ctxB, h.GetWorkflowID())
		require.NotNil(t, owner)
		assert.Equal(t, "app-c", *owner)
		for _, stepOwner := range stepApplicationNames(t, ctxB, h.GetWorkflowID()) {
			require.NotNil(t, stepOwner)
			assert.Equal(t, "app-c", *stepOwner)
		}
	}
	ownerB := rowApplicationName(t, ctxB, hB.GetWorkflowID())
	require.NotNil(t, ownerB)
	assert.Equal(t, "app-b", *ownerB)

	// Adoption moves only unclaimed rows, and only when asked.
	unclaimed, err := Enqueue[string, string](client, "rename-queue", "unclaimed-wf", "y")
	require.NoError(t, err)
	noAdopt, err := RenameApplication(client, RenameApplicationInput{OldName: "no-such-app", NewName: "app-d"})
	require.NoError(t, err)
	assert.Equal(t, ApplicationRowCounts{}, noAdopt)
	adopted, err := RenameApplication(client, RenameApplicationInput{NewName: "app-d", AdoptUnclaimedRows: true})
	require.NoError(t, err)
	assert.Equal(t, ApplicationRowCounts{Workflows: 1}, adopted)
	adoptedOwner := rowApplicationName(t, ctxB, unclaimed.GetWorkflowID())
	require.NotNil(t, adoptedOwner)
	assert.Equal(t, "app-d", *adoptedOwner)

	_, err = RenameApplication(client, RenameApplicationInput{NewName: "app-e"})
	require.ErrorContains(t, err, "nothing to re-own")
	_, err = RenameApplication(client, RenameApplicationInput{OldName: "app-e", NewName: "app-e"})
	require.ErrorContains(t, err, "already holds that name")
	_, err = RenameApplication(client, RenameApplicationInput{OldName: "app-x", NewName: ""})
	require.ErrorContains(t, err, "new application name is required")
	_, err = RenameApplication(client, RenameApplicationInput{OldName: "app-x", NewName: "App-E"})
	require.ErrorContains(t, err, "invalid application name")
	_, err = RenameApplication(client, RenameApplicationInput{OldName: "app-x", NewName: "app-e", BatchSize: -1})
	require.ErrorContains(t, err, "batch size")

	// app-c owns the moved registries and runs the moved in-flight workflow,
	// whose version was left unset by the client enqueue.
	ctxC := setupDBOS(t, setupDBOSOptions{appName: "app-c"})
	RegisterWorkflow(ctxC, twoSteps("app-c"), WithWorkflowName("rename-wf"))
	require.NoError(t, Launch(ctxC))

	q, err := RetrieveQueue(ctxC, "rename-queue")
	require.NoError(t, err)
	assert.Equal(t, "app-c", q.GetApplicationName())
	sched, err := GetSchedule(ctxC, "rename-sched")
	require.NoError(t, err)
	assert.Equal(t, "app-c", sched.ApplicationName)

	result, err := pending.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "app-c:x", result)
}

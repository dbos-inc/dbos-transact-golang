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

// TestApplicationNameQueueIsolation verifies two applications sharing a
// system database and a queue only dequeue their own workflows.
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

	const queueName = "app-name-isolation-queue"
	_, err := RegisterQueue(ctxA, queueName)
	require.NoError(t, err)

	var handlesA []WorkflowHandle[string]
	for range 5 {
		handle, err := Enqueue[string, string](ctxA, queueName, "shared-workflow", "x")
		require.NoError(t, err)
		handlesA = append(handlesA, handle)
	}
	handleB, err := Enqueue[string, string](ctxB, queueName, "shared-workflow", "x")
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

	require.True(t, queueEntriesAreCleanedUp(ctxA))
	require.True(t, queueEntriesAreCleanedUp(ctxB))
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

	handle, err := Enqueue[string, string](client, queueName, "claimable-workflow", "x")
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
	handle, err := Enqueue[string, string](ctxA, "recovery-isolation-queue", "recovery-workflow", "x")
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

	handleA, err := RunWorkflow(ctxA, simple, "a")
	require.NoError(t, err)
	_, err = handleA.GetResult()
	require.NoError(t, err)

	cutoff := time.Now().Add(time.Hour).UnixMilli()
	gcInput := sysdb.GarbageCollectWorkflowsInput{CutoffEpochTimestampMs: &cutoff}

	// app-b's GC spares app-a's completed workflow.
	require.NoError(t, ctxB.(*dbosContext).systemDB.GarbageCollectWorkflows(ctxB, gcInput))
	require.NotNil(t, rowApplicationName(t, ctxA, handleA.GetWorkflowID()))

	// app-a's GC deletes it.
	require.NoError(t, ctxA.(*dbosContext).systemDB.GarbageCollectWorkflows(ctxA, gcInput))
	sdb := ctxA.(*dbosContext).systemDB.(*sysdb.SysDB)
	query := sdb.Dialect().RewriteQuery(fmt.Sprintf(
		`SELECT COUNT(*) FROM %sworkflow_status WHERE workflow_uuid = $1`,
		sdb.Dialect().SchemaPrefix(sdb.Schema())))
	var count int
	require.NoError(t, sdb.Pool().QueryRow(context.Background(), query, handleA.GetWorkflowID()).Scan(&count))
	assert.Equal(t, 0, count)
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

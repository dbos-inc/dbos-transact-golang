package dbos

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errCancelFailedWorkflow = errors.New("the workflow failed")

// failedWorkflow runs a workflow that records one step and then fails, and returns its ID.
func failedWorkflow(t *testing.T, dbosCtx Context) string {
	t.Helper()
	handle, err := RunWorkflow(dbosCtx, cancelFailedWorkflow, "input")
	require.NoError(t, err)
	_, err = handle.GetResult()
	require.ErrorContains(t, err, errCancelFailedWorkflow.Error())
	return handle.GetWorkflowID()
}

func cancelFailedWorkflow(ctx Context, input string) (string, error) {
	if _, err := RunAsStep(ctx, func(context.Context) (string, error) { return input, nil }, WithStepName("record")); err != nil {
		return "", err
	}
	return "", errCancelFailedWorkflow
}

// launchCancelFailed starts DBOS with the failing workflow registered.
func launchCancelFailed(t *testing.T) (Context, *sysdb.SysDB) {
	t.Helper()
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, cancelFailedWorkflow)
	require.NoError(t, Launch(dbosCtx))
	return dbosCtx, dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
}

// statusOf reads one workflow with its output.
func statusOf(t *testing.T, dbosCtx Context, id string) WorkflowStatus {
	t.Helper()
	rows, err := ListWorkflows(dbosCtx, WithFilterWorkflowIDs(id), WithFilterLoadOutput(true))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0]
}

// insertWorkflows writes a workflow row for each status, each a child of parentID.
func insertWorkflows(t *testing.T, sysDB *sysdb.SysDB, parentID string, statuses map[string]WorkflowStatusType) {
	t.Helper()
	ctx := context.Background()
	tx, err := sysDB.Pool().BeginTx(ctx, sysdb.TxOptions{})
	require.NoError(t, err)
	defer tx.Rollback(ctx)
	for id, status := range statuses {
		_, err := sysDB.InsertWorkflowStatus(ctx, sysdb.InsertWorkflowStatusDBInput{
			Status: models.WorkflowStatus{ID: id, Name: "cancel-failed-tree", Status: status, ParentWorkflowID: parentID, CreatedAt: time.Now(), UpdatedAt: time.Now()},
			Tx:     tx,
		})
		require.NoError(t, err, "insert workflow %s", id)
	}
	require.NoError(t, tx.Commit(ctx))
}

func TestCancelWorkflow_WithCancelFailedEndsAnErrorWorkflowAndKeepsItsOutput(t *testing.T) {
	dbosCtx, _ := launchCancelFailed(t)
	id := failedWorkflow(t, dbosCtx)
	stepsBefore, err := GetWorkflowSteps(dbosCtx, id)
	require.NoError(t, err)
	require.Len(t, stepsBefore, 1, "the failed workflow recorded its step")

	require.NoError(t, CancelWorkflow(dbosCtx, id, WithCancelFailed()))

	after := statusOf(t, dbosCtx, id)
	assert.Equal(t, WorkflowStatusCancelled, after.Status)
	require.Error(t, after.Error, "the cancel keeps the recorded error")
	assert.Contains(t, after.Error.Error(), errCancelFailedWorkflow.Error())
	assert.Empty(t, after.QueueName)
	stepsAfter, err := GetWorkflowSteps(dbosCtx, id)
	require.NoError(t, err)
	assert.Equal(t, stepsBefore, stepsAfter, "the cancel keeps the recorded steps")
}

func TestCancelWorkflow_WithoutTheOptionLeavesAnErrorWorkflowUntouched(t *testing.T) {
	dbosCtx, _ := launchCancelFailed(t)
	id := failedWorkflow(t, dbosCtx)
	before := statusOf(t, dbosCtx, id)

	require.NoError(t, CancelWorkflow(dbosCtx, id, WithCancelChildren()))
	require.NoError(t, CancelWorkflows(dbosCtx, []string{id}))

	after := statusOf(t, dbosCtx, id)
	assert.Equal(t, WorkflowStatusError, after.Status)
	assert.Equal(t, before.UpdatedAt, after.UpdatedAt, "the cancel wrote nothing")
}

func TestCancelWorkflow_WithCancelFailedLeavesAFailedChildFailed(t *testing.T) {
	dbosCtx, sysDB := launchCancelFailed(t)
	parentID, openID, failedID, doneID := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	insertWorkflows(t, sysDB, "", map[string]WorkflowStatusType{parentID: WorkflowStatusError})
	insertWorkflows(t, sysDB, parentID, map[string]WorkflowStatusType{openID: WorkflowStatusPending, failedID: WorkflowStatusError, doneID: WorkflowStatusSuccess})

	require.NoError(t, CancelWorkflow(dbosCtx, parentID, WithCancelChildren(), WithCancelFailed()))

	assert.Equal(t, WorkflowStatusCancelled, statusOf(t, dbosCtx, parentID).Status, "the failed root ends cancelled")
	assert.Equal(t, WorkflowStatusCancelled, statusOf(t, dbosCtx, openID).Status, "the open child stops with its parent")
	assert.Equal(t, WorkflowStatusError, statusOf(t, dbosCtx, failedID).Status, "a failed child stays failed")
	assert.Equal(t, WorkflowStatusSuccess, statusOf(t, dbosCtx, doneID).Status)
}

func TestCancelWorkflow_WithCancelFailedLeavesASuccessWorkflowUntouched(t *testing.T) {
	dbosCtx, sysDB := launchCancelFailed(t)
	doneID, failedID := uuid.NewString(), uuid.NewString()
	insertWorkflows(t, sysDB, "", map[string]WorkflowStatusType{doneID: WorkflowStatusSuccess, failedID: WorkflowStatusError})
	before := statusOf(t, dbosCtx, doneID)

	require.NoError(t, CancelWorkflow(dbosCtx, doneID, WithCancelFailed()))
	require.NoError(t, CancelWorkflows(dbosCtx, []string{doneID, failedID}, WithCancelFailed()))

	after := statusOf(t, dbosCtx, doneID)
	assert.Equal(t, WorkflowStatusSuccess, after.Status)
	assert.Equal(t, before.UpdatedAt, after.UpdatedAt, "the cancel wrote nothing")
	assert.Equal(t, WorkflowStatusCancelled, statusOf(t, dbosCtx, failedID).Status, "the batch cancel ends the failed workflow")
}

func TestCancelWorkflow_WithCancelFailedInsideAWorkflowEndsAnErrorWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	RegisterWorkflow(dbosCtx, cancelFailedWorkflow)
	cancelOne := func(ctx Context, id string) (string, error) {
		return id, CancelWorkflow(ctx, id, WithCancelFailed())
	}
	RegisterWorkflow(dbosCtx, cancelOne)
	cancelMany := func(ctx Context, id string) (string, error) {
		return id, CancelWorkflows(ctx, []string{id}, WithCancelFailed())
	}
	RegisterWorkflow(dbosCtx, cancelMany)
	require.NoError(t, Launch(dbosCtx))

	for name, canceller := range map[string]Workflow[string, string]{"cancel_workflow": cancelOne, "cancel_workflows": cancelMany} {
		t.Run(name, func(t *testing.T) {
			id := failedWorkflow(t, dbosCtx)
			handle, err := RunWorkflow(dbosCtx, canceller, id)
			require.NoError(t, err)
			_, err = handle.GetResult()
			require.NoError(t, err)

			after := statusOf(t, dbosCtx, id)
			assert.Equal(t, WorkflowStatusCancelled, after.Status, "the cancel inside a workflow ends the failed workflow")
			require.ErrorContains(t, after.Error, errCancelFailedWorkflow.Error())
		})
	}
}

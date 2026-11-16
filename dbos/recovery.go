package dbos

import (
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// recoverPendingWorkflows re-enqueues pending workflows owned by the specified executors and returns handles to them.
func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	recovered, err := sysdb.RetryWithResult(ctx, func() ([]sysdb.RecoveredWorkflow, error) {
		return ctx.systemDB.ReenqueueForRecovery(ctx, executorIDs, ctx.applicationVersion, models.InternalQueueName)
	}, sysdb.WithRetrierLogger(ctx.logger))
	if err != nil {
		return nil, err
	}

	workflowHandles := make([]WorkflowHandle[any], 0, len(recovered))
	for _, workflow := range recovered {
		workflowHandles = append(workflowHandles, newWorkflowPollingHandle[any](ctx, workflow.ID, workflow.ShardKey))
	}
	return workflowHandles, nil
}

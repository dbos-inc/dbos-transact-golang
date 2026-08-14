package dbos

import (
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// recoverPendingWorkflows re-enqueues pending workflows owned by the specified executors and returns handles to them.
func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error) {
	recoveredIDs, err := sysdb.RetryWithResult(ctx, func() ([]string, error) {
		return ctx.systemDB.ReenqueueForRecovery(ctx, executorIDs, ctx.applicationVersion, models.InternalQueueName)
	}, sysdb.WithRetrierLogger(ctx.logger))
	if err != nil {
		return nil, err
	}

	workflowHandles := make([]WorkflowHandle[any], 0, len(recoveredIDs))
	for _, workflowID := range recoveredIDs {
		workflowHandles = append(workflowHandles, newWorkflowPollingHandle[any](ctx, workflowID))
	}
	return workflowHandles, nil
}

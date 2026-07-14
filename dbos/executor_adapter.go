package dbos

import (
	"context"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// executorAdapter exposes the narrow surface of dbosContext that the
// internal admin server and conductor need (their Executor interfaces),
// without letting them depend on the dbos package.
type executorAdapter struct {
	ctx *dbosContext
}

func (a executorAdapter) ListWorkflows(_ context.Context, opts ...models.ListWorkflowsOption) ([]models.WorkflowStatus, error) {
	return a.ctx.ListWorkflows(a.ctx, opts...)
}

func (a executorAdapter) GetWorkflowSteps(_ context.Context, workflowID string) ([]models.StepInfo, error) {
	return a.ctx.GetWorkflowSteps(a.ctx, workflowID)
}

func (a executorAdapter) CancelWorkflow(_ context.Context, workflowID string) error {
	return a.ctx.CancelWorkflow(a.ctx, workflowID)
}

func (a executorAdapter) ResumeWorkflow(_ context.Context, workflowID string) error {
	_, err := a.ctx.ResumeWorkflow(a.ctx, workflowID)
	return err
}

func (a executorAdapter) ForkWorkflow(_ context.Context, input models.ForkWorkflowInput) (string, error) {
	handle, err := a.ctx.ForkWorkflow(a.ctx, input)
	if err != nil {
		return "", err
	}
	return handle.GetWorkflowID(), nil
}

func (a executorAdapter) RecoverPendingWorkflows(_ context.Context, executorIDs []string) ([]string, error) {
	handles, err := recoverPendingWorkflows(a.ctx, executorIDs)
	if err != nil {
		return nil, err
	}
	workflowIDs := make([]string, len(handles))
	for i, handle := range handles {
		workflowIDs[i] = handle.GetWorkflowID()
	}
	return workflowIDs, nil
}

func (a executorAdapter) CancelAllBefore(_ context.Context, cutoff time.Time) error {
	return sysdb.Retry(a.ctx, func() error {
		return a.ctx.systemDB.CancelAllBefore(a.ctx, cutoff)
	}, sysdb.WithRetrierLogger(a.ctx.logger))
}

func (a executorAdapter) QueueMetadata() []models.QueueConfig {
	queues := a.ctx.queueRunner.listQueues()
	configs := make([]models.QueueConfig, 0, len(queues))
	for _, q := range queues {
		configs = append(configs, q.toConfig())
	}
	return configs
}

func (a executorAdapter) Deactivate() {
	a.ctx.logger.Info("Deactivating DBOS executor", "executor_id", a.ctx.executorID, "app_version", a.ctx.applicationVersion)
	// Stop the workflow scheduler. Note we don't wait for running jobs to complete
	if a.ctx.workflowScheduler != nil {
		a.ctx.workflowScheduler.Stop()
	}
}

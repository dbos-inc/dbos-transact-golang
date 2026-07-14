package dbos

import (
	"context"
	"encoding/json"
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

func (a executorAdapter) GetWorkflowSteps(_ context.Context, workflowID string, opts ...models.GetWorkflowStepsOption) ([]models.StepInfo, error) {
	return a.ctx.GetWorkflowSteps(a.ctx, workflowID, opts...)
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

func (a executorAdapter) SystemDB() sysdb.SystemDatabase {
	return a.ctx.systemDB
}

func (a executorAdapter) GetExecutorID() string {
	return a.ctx.GetExecutorID()
}

func (a executorAdapter) GetApplicationVersion() string {
	return a.ctx.GetApplicationVersion()
}

func (a executorAdapter) AlertHandler() models.AlertHandler {
	return a.ctx.alertHandler
}

func (a executorAdapter) DecodeStoredValue(_ context.Context, value, serialization string) (string, error) {
	decoder, err := resolveDecoder[any](serialization, getCustomSerializerFromCtx(a.ctx))
	if err != nil {
		return "", err
	}
	decoded, err := decoder.Decode(&value)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (a executorAdapter) CancelWorkflows(_ context.Context, workflowIDs []string, opts ...CancelWorkflowOptions) error {
	return a.ctx.CancelWorkflows(a.ctx, workflowIDs, opts...)
}

func (a executorAdapter) ResumeWorkflows(_ context.Context, workflowIDs []string, opts ...ResumeWorkflowOption) error {
	_, err := a.ctx.ResumeWorkflows(a.ctx, workflowIDs, opts...)
	return err
}

func (a executorAdapter) GetWorkflowAggregates(_ context.Context, input models.GetWorkflowAggregatesInput) ([]sysdb.WorkflowAggregateRow, error) {
	return a.ctx.GetWorkflowAggregates(a.ctx, input)
}

func (a executorAdapter) GetStepAggregates(_ context.Context, input models.GetStepAggregatesInput) ([]sysdb.StepAggregateRow, error) {
	return a.ctx.GetStepAggregates(a.ctx, input)
}

func (a executorAdapter) ListSchedules(_ context.Context, opts ...models.ListSchedulesOption) ([]models.WorkflowSchedule, error) {
	return a.ctx.ListSchedules(a.ctx, opts...)
}

func (a executorAdapter) GetSchedule(_ context.Context, scheduleName string) (*models.WorkflowSchedule, error) {
	return a.ctx.GetSchedule(a.ctx, scheduleName)
}

func (a executorAdapter) PauseSchedule(_ context.Context, scheduleName string) error {
	return a.ctx.PauseSchedule(a.ctx, scheduleName)
}

func (a executorAdapter) ResumeSchedule(_ context.Context, scheduleName string) error {
	return a.ctx.ResumeSchedule(a.ctx, scheduleName)
}

func (a executorAdapter) ListQueues(_ context.Context) ([]models.QueueConfig, error) {
	queues, err := a.ctx.ListQueues(a.ctx)
	if err != nil {
		return nil, err
	}
	configs := make([]models.QueueConfig, 0, len(queues))
	for _, q := range queues {
		if wq, ok := q.(*WorkflowQueue); ok {
			configs = append(configs, wq.toConfig())
		}
	}
	return configs, nil
}

func (a executorAdapter) RetrieveQueue(_ context.Context, name string) (*models.QueueConfig, error) {
	q, err := a.ctx.RetrieveQueue(a.ctx, name)
	if err != nil {
		return nil, err
	}
	wq, ok := q.(*WorkflowQueue)
	if !ok || wq == nil {
		return nil, nil
	}
	cfg := wq.toConfig()
	return &cfg, nil
}

package dbos

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingSystemDB struct {
	lastInterval time.Duration
	result       *string
	err          error
}

func (r *recordingSystemDB) launch(ctx context.Context)                          {}
func (r *recordingSystemDB) shutdown(ctx context.Context, timeout time.Duration) {}
func (r *recordingSystemDB) resetSystemDB(ctx context.Context) error             { return nil }
func (r *recordingSystemDB) insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) awaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*string, error) {
	r.lastInterval = pollInterval
	return r.result, r.err
}
func (r *recordingSystemDB) cancelWorkflow(ctx context.Context, workflowID string) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) cancelAllBefore(ctx context.Context, cutoffTime time.Time) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) resumeWorkflow(ctx context.Context, workflowID string) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) forkWorkflow(ctx context.Context, input forkWorkflowDBInput) (string, error) {
	return "", errors.New("not implemented")
}
func (r *recordingSystemDB) recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) checkChildWorkflow(ctx context.Context, workflowUUID string, functionID int) (*string, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) recordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) send(ctx context.Context, input WorkflowSendInput) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) recv(ctx context.Context, input recvInput) (*string, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) setEvent(ctx context.Context, input WorkflowSetEventInput) error {
	return errors.New("not implemented")
}
func (r *recordingSystemDB) getEvent(ctx context.Context, input getEventInput) (*string, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) sleep(ctx context.Context, input sleepInput) (time.Duration, error) {
	return 0, errors.New("not implemented")
}
func (r *recordingSystemDB) dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) clearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	return false, errors.New("not implemented")
}
func (r *recordingSystemDB) getQueuePartitions(ctx context.Context, queueName string) ([]string, error) {
	return nil, errors.New("not implemented")
}
func (r *recordingSystemDB) garbageCollectWorkflows(ctx context.Context, input garbageCollectWorkflowsInput) error {
	return errors.New("not implemented")
}

func newTestDBOSContext(systemDB systemDatabase) *dbosContext {
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	return &dbosContext{
		ctx:                     context.Background(),
		logger:                  logger,
		systemDB:                systemDB,
		workflowsWg:             &sync.WaitGroup{},
		workflowRegistry:        &sync.Map{},
		workflowCustomNametoFQN: &sync.Map{},
		queueRunner:             newQueueRunner(logger),
	}
}

func TestGetResultUsesDefaultPollingInterval(t *testing.T) {
	serializer := newJSONSerializer[string]()
	encoded, err := serializer.Encode("ok")
	require.NoError(t, err)

	sysDB := &recordingSystemDB{result: encoded}
	ctx := newTestDBOSContext(sysDB)
	handle := newWorkflowPollingHandle[string](ctx, "workflow-id")

	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "ok", result)
	assert.Equal(t, _DB_RETRY_INTERVAL, sysDB.lastInterval)
}

func TestGetResultUsesCustomPollingInterval(t *testing.T) {
	serializer := newJSONSerializer[int]()
	encoded, err := serializer.Encode(42)
	require.NoError(t, err)

	sysDB := &recordingSystemDB{result: encoded}
	ctx := newTestDBOSContext(sysDB)
	handle := newWorkflowPollingHandle[int](ctx, "workflow-id")

	customInterval := 25 * time.Millisecond
	result, err := handle.GetResult(WithPollingInterval(customInterval))
	require.NoError(t, err)
	require.Equal(t, 42, result)
	assert.Equal(t, customInterval, sysDB.lastInterval)
}

func TestGetResultIgnoresNonPositivePollingInterval(t *testing.T) {
	serializer := newJSONSerializer[string]()
	encoded, err := serializer.Encode("value")
	require.NoError(t, err)

	sysDB := &recordingSystemDB{result: encoded}
	ctx := newTestDBOSContext(sysDB)
	handle := newWorkflowPollingHandle[string](ctx, "workflow-id")

	result, err := handle.GetResult(WithPollingInterval(0))
	require.NoError(t, err)
	require.Equal(t, "value", result)
	assert.Equal(t, _DB_RETRY_INTERVAL, sysDB.lastInterval)
}

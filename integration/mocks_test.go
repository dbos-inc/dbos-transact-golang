package dbos_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/integration/mocks"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
)

func step(ctx context.Context) (int, error) {
	return 1, nil
}

func childWorkflow(ctx dbos.DBOSContext, i int) (int, error) {
	return i + 1, nil
}

func workflow(ctx dbos.DBOSContext, i int) (int, error) {
	// Test RunAsStep
	a, err := dbos.RunAsStep(ctx, step)
	if err != nil {
		return 0, err
	}

	// Child wf
	ch, err := dbos.RunWorkflow(ctx, childWorkflow, i)
	if err != nil {
		return 0, err
	}
	b, err := ch.GetResult()
	if err != nil {
		return 0, err
	}

	// Test messaging operations
	c, err := dbos.Recv[int](ctx, "chan1", 1*time.Second)
	if err != nil {
		return 0, err
	}
	d, err := dbos.GetEvent[int](ctx, "tgw", "event1", 1*time.Second)
	if err != nil {
		return 0, err
	}
	err = dbos.Send(ctx, "dst", 1, "topic")
	if err != nil {
		return 0, err
	}

	// Test SetEvent
	err = dbos.SetEvent(ctx, "test_key", "test_value")
	if err != nil {
		return 0, err
	}

	// Test Sleep
	_, err = dbos.Sleep(ctx, 100*time.Millisecond)
	if err != nil {
		return 0, err
	}

	// Test ID retrieval methods
	workflowID, err := ctx.GetWorkflowID()
	if err != nil {
		return 0, err
	}
	stepID, err := ctx.GetStepID()
	if err != nil {
		return 0, err
	}

	// Test workflow management
	_, err = dbos.RetrieveWorkflow[int](ctx, workflowID)
	if err != nil {
		return 0, err
	}

	err = dbos.CancelWorkflow(ctx, workflowID)
	if err != nil {
		return 0, err
	}

	_, err = dbos.ResumeWorkflow[int](ctx, workflowID)
	if err != nil {
		return 0, err
	}

	forkInput := dbos.ForkWorkflowInput{
		OriginalWorkflowID: workflowID,
		StartStep:          uint(stepID),
	}
	_, err = dbos.ForkWorkflow[int](ctx, forkInput)
	if err != nil {
		return 0, err
	}

	_, err = dbos.ListWorkflows(ctx)
	if err != nil {
		return 0, err
	}

	_, err = dbos.GetWorkflowSteps(ctx, workflowID)
	if err != nil {
		return 0, err
	}

	// Test accessor methods
	appVersion := ctx.GetApplicationVersion()
	executorID := ctx.GetExecutorID()
	appID := ctx.GetApplicationID()

	// Use some values to avoid compiler warnings
	_ = appVersion
	_ = executorID
	_ = appID

	// Test Go and Select methods (using stepAny to match Select signature)
	stepAny := func(ctx context.Context) (any, error) {
		return 1, nil
	}
	outcomeChan, err := dbos.Go(ctx, stepAny)
	if err != nil {
		return 0, err
	}

	// Test Select method
	e, err := dbos.Select(ctx, []<-chan dbos.StepOutcome[any]{outcomeChan})
	if err != nil {
		return 0, err
	}

	return a + b + c + d + e.(int), nil
}

func aRealProgramFunction(dbosCtx dbos.DBOSContext) error {

	dbos.RegisterWorkflow(dbosCtx, workflow)

	err := dbos.Launch(dbosCtx)
	if err != nil {
		return err
	}
	defer dbos.Shutdown(dbosCtx, 1*time.Second)

	res, err := workflow(dbosCtx, 2)
	if err != nil {
		return err
	}
	if res != 5 {
		return fmt.Errorf("unexpected result: %v", res)
	}

	// Test WithValue
	valCtx := dbos.WithValue(dbosCtx, "key", "val")
	if valCtx == nil {
		return fmt.Errorf("WithValue returned nil")
	}

	// Test WithCancelCause
	cancelCtx, cf := dbos.WithCancelCause(dbosCtx)
	if cancelCtx == nil {
		return fmt.Errorf("WithCancelCause returned nil context")
	}
	if cf == nil {
		return fmt.Errorf("WithCancelCause returned nil cancel function")
	}

	return nil
}

// clientMethodsFunction exercises remaining DBOSContext methods
func clientMethodsFunction(ctx dbos.DBOSContext) error {
	// WriteStream
	err := dbos.WriteStream(ctx, "stream-key", "stream-value")
	if err != nil {
		return fmt.Errorf("WriteStream failed: %w", err)
	}

	// CloseStream
	err = dbos.CloseStream(ctx, "stream-key")
	if err != nil {
		return fmt.Errorf("CloseStream failed: %w", err)
	}

	// ReadStream
	_, _, err = dbos.ReadStream[any](ctx, "wf-id-123", "stream-key")
	if err != nil {
		return fmt.Errorf("ReadStream failed: %w", err)
	}

	// ReadStreamAsync
	asyncChan, err := dbos.ReadStreamAsync[any](ctx, "wf-id-456", "async-key")
	if err != nil {
		return fmt.Errorf("ReadStreamAsync failed: %w", err)
	}
	for range asyncChan {
	}

	// Patch
	enabled, err := dbos.Patch(ctx, "feature-patch")
	if err != nil || !enabled {
		return fmt.Errorf("Patch failed: enabled=%v, err=%v", enabled, err)
	}

	// DeprecatePatch
	err = dbos.DeprecatePatch(ctx, "old-patch")
	if err != nil {
		return fmt.Errorf("DeprecatePatch failed: %w", err)
	}

	// ListRegisteredWorkflows
	entries, err := dbos.ListRegisteredWorkflows(ctx)
	if err != nil || len(entries) != 1 {
		return fmt.Errorf("ListRegisteredWorkflows failed: entries=%v, err=%v", entries, err)
	}

	// ListRegisteredQueues
	queues, err := dbos.ListRegisteredQueues(ctx)
	if err != nil || len(queues) != 1 {
		return fmt.Errorf("ListRegisteredQueues failed: queues=%v, err=%v", queues, err)
	}

	// From
	fromCtx := dbos.From(ctx, context.Background())
	if fromCtx == nil {
		return fmt.Errorf("From returned nil")
	}

	// WithoutCancel
	noCancelCtx := dbos.WithoutCancel(ctx)
	if noCancelCtx == nil {
		return fmt.Errorf("WithoutCancel returned nil")
	}

	// WithTimeout
	timeoutCtx, timeoutCancel := dbos.WithTimeout(ctx, 5*time.Minute)
	if timeoutCtx == nil || timeoutCancel == nil {
		return fmt.Errorf("WithTimeout returned nil")
	}

	// ListenQueues
	dbos.ListenQueues(ctx, dbos.WorkflowQueue{Name: "queue1"}, dbos.WorkflowQueue{Name: "queue2"})

	// DeleteWorkflows
	err = dbos.DeleteWorkflows(ctx, []string{"wf-to-delete"})
	if err != nil {
		return fmt.Errorf("DeleteWorkflows failed: %w", err)
	}

	return nil
}

// clientUsingFunction demonstrates Client usage with specific values
func clientUsingFunction(client dbos.Client) error {
	handle, err := client.Enqueue("my-queue", "my-workflow", "input-data")
	if err != nil {
		return err
	}

	status, err := handle.GetStatus()
	if err != nil {
		return err
	}

	if status.ID == "" {
		return fmt.Errorf("expected workflow ID")
	}

	client.Shutdown(1 * time.Second)
	return nil
}

func TestMocks(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockCtx := mocks.NewMockDBOSContext(t)

	// Context lifecycle
	mockCtx.On("Launch").Return(nil)
	mockCtx.On("Shutdown", mock.Anything).Return()

	// Context methods
	mockCtx.On("Done").Return((<-chan struct{})(nil))

	// Basic workflow operations (existing)
	mockCtx.On("RunAsStep", mockCtx, mock.Anything, mock.Anything).Return(1, nil)

	// Child workflow
	mockChildHandle := mocks.NewMockWorkflowHandle[any](t)
	mockCtx.On("RunWorkflow", mockCtx, mock.Anything, 2, mock.Anything).Return(mockChildHandle, nil).Once()
	mockChildHandle.On("GetResult").Return(1, nil)

	// Messaging
	mockCtx.On("Recv", mockCtx, "chan1", 1*time.Second).Return(1, nil)
	mockCtx.On("GetEvent", mockCtx, "tgw", "event1", 1*time.Second).Return(1, nil)
	mockCtx.On("Send", mockCtx, "dst", 1, "topic").Return(nil)
	mockCtx.On("SetEvent", mockCtx, "test_key", "test_value").Return(nil)

	mockCtx.On("Sleep", mockCtx, 100*time.Millisecond).Return(100*time.Millisecond, nil)

	// ID retrieval methods
	mockCtx.On("GetWorkflowID").Return("test-workflow-id", nil)
	mockCtx.On("GetStepID").Return(1, nil)

	// Workflow management
	mockGenericHandle := mocks.NewMockWorkflowHandle[any](t)
	mockGenericHandle.On("GetWorkflowID").Return("generic-workflow-id")

	mockCtx.On("RetrieveWorkflow", mockCtx, "test-workflow-id").Return(mockGenericHandle, nil)
	mockCtx.On("CancelWorkflow", mockCtx, "test-workflow-id").Return(nil)
	mockCtx.On("ResumeWorkflow", mockCtx, "test-workflow-id").Return(mockGenericHandle, nil)
	mockCtx.On("ForkWorkflow", mockCtx, mock.Anything).Return(mockGenericHandle, nil)
	mockCtx.On("ListWorkflows", mockCtx).Return([]dbos.WorkflowStatus{}, nil)
	mockCtx.On("GetWorkflowSteps", mockCtx, "test-workflow-id").Return([]dbos.StepInfo{}, nil)

	// Accessor methods
	mockCtx.On("GetApplicationVersion").Return("test-version")
	mockCtx.On("GetExecutorID").Return("test-executor")
	mockCtx.On("GetApplicationID").Return("test-app-id")

	// Go and Select expectations
	outcomeChan := make(chan dbos.StepOutcome[any], 1)
	outcomeChan <- dbos.StepOutcome[any]{Result: 1, Err: nil}
	close(outcomeChan)

	mockCtx.On("Go", mockCtx, mock.MatchedBy(func(fn interface{}) bool {
		return fn != nil
	}), mock.Anything).Return(outcomeChan, nil).Once()

	mockCtx.On("Select", mockCtx, mock.MatchedBy(func(chans []<-chan dbos.StepOutcome[any]) bool {
		return len(chans) == 1 && chans != nil
	})).Return(1, nil).Once()

	// Context management
	mockValCtx := mocks.NewMockDBOSContext(t)
	mockCtx.On("WithValue", "key", "val").Return(mockValCtx)

	mockCancelCtx := mocks.NewMockDBOSContext(t)
	var cancelFunc context.CancelCauseFunc = func(error) {}
	mockCtx.On("WithCancelCause").Return(mockCancelCtx, cancelFunc)

	err := aRealProgramFunction(mockCtx)
	if err != nil {
		t.Fatal(err)
	}

	// Test MockClient with specific values
	mockClient := mocks.NewMockClient(t)
	mockClientHandle := mocks.NewMockWorkflowHandle[any](t)

	// Enqueue with specific values
	mockClientHandle.On("GetStatus").Return(dbos.WorkflowStatus{ID: "wf-123"}, nil).Once()
	mockClient.On("Enqueue", "my-queue", "my-workflow", "input-data", mock.Anything).Return(mockClientHandle, nil).Once()
	mockClient.On("Shutdown", 1*time.Second).Return()

	err = clientUsingFunction(mockClient)
	if err != nil {
		t.Fatalf("clientUsingFunction failed: %v", err)
	}

	// Test remaining DBOSContext methods
	mockCtx2 := mocks.NewMockDBOSContext(t)

	// WriteStream
	mockCtx2.On("WriteStream", mockCtx2, "stream-key", "stream-value").Return(nil).Once()

	// CloseStream
	mockCtx2.On("CloseStream", mockCtx2, "stream-key").Return(nil).Once()

	// ReadStream
	mockCtx2.On("ReadStream", mockCtx2, "wf-id-123", "stream-key").Return([]any{"InZhbDEi", "InZhbDIi", "InZhbDMi"}, true, nil).Once()

	// ReadStreamAsync
	streamChan := make(chan dbos.StreamValue[any], 1)
	close(streamChan)
	mockCtx2.On("ReadStreamAsync", mockCtx2, "wf-id-456", "async-key").Return((<-chan dbos.StreamValue[any])(streamChan), nil).Once()

	// Patch
	mockCtx2.On("Patch", mockCtx2, "feature-patch").Return(true, nil).Once()

	// DeprecatePatch
	mockCtx2.On("DeprecatePatch", mockCtx2, "old-patch").Return(nil).Once()

	// ListRegisteredWorkflows
	mockCtx2.On("ListRegisteredWorkflows", mockCtx2).Return([]dbos.WorkflowRegistryEntry{
		{Name: "Workflow1", FQN: "workflow1"},
	}, nil).Once()

	// ListRegisteredQueues
	mockCtx2.On("ListRegisteredQueues", mockCtx2).Return([]dbos.WorkflowQueue{
		{Name: "queue1"},
	}, nil).Once()

	// From
	mockFromCtx := mocks.NewMockDBOSContext(t)
	mockCtx2.On("From", mockCtx2, mock.Anything).Return(mockFromCtx, nil).Once()

	// WithoutCancel
	mockNoCancelCtx := mocks.NewMockDBOSContext(t)
	mockCtx2.On("WithoutCancel", mockCtx2).Return(mockNoCancelCtx, nil).Once()

	// WithTimeout
	mockTimeoutCtx := mocks.NewMockDBOSContext(t)
	var timeoutCancelFunc context.CancelFunc = func() {}
	mockCtx2.On("WithTimeout", mockCtx2, 5*time.Minute).Return(mockTimeoutCtx, timeoutCancelFunc, nil).Once()

	// ListenQueues
	mockCtx2.On("ListenQueues", mockCtx2, mock.MatchedBy(func(qs []dbos.WorkflowQueue) bool {
		return len(qs) == 2
	})).Return(nil).Once()

	// DeleteWorkflows
	mockCtx2.On("DeleteWorkflows", mockCtx2, []string{"wf-to-delete"}, mock.Anything).Return(nil).Once()

	err = clientMethodsFunction(mockCtx2)
	if err != nil {
		t.Fatalf("clientMethodsFunction failed: %v", err)
	}
}

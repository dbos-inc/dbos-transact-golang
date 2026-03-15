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

	return a + b + c + d, nil
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
	if res != 4 {
		return fmt.Errorf("unexpected result: %v", res)
	}
	return nil
}

func TestMocks(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockCtx := mocks.NewMockDBOSContext(t)

	// Context lifecycle
	mockCtx.On("Launch").Return(nil)
	mockCtx.On("Shutdown", mock.Anything).Return()

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
	mockGenericHandle.On("GetWorkflowID").Return("generic-workflow-id").Maybe()
	mockGenericHandle.On("GetResult").Return(42, nil).Maybe()
	mockGenericHandle.On("GetStatus").Return(dbos.WorkflowStatus{}, nil).Maybe()

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

	// Context management
	mockValCtx := mocks.NewMockDBOSContext(t)
	mockCtx.On("WithValue", "key", "val").Return(mockValCtx)
	valCtx := dbos.WithValue(mockCtx, "key", "val")
	if valCtx != dbos.DBOSContext(mockValCtx) {
		t.Fatal("WithValue did not return mock context")
	}

	mockCancelCtx := mocks.NewMockDBOSContext(t)
	var cancelFunc context.CancelCauseFunc = func(error) {}
	mockCtx.On("WithCancelCause").Return(mockCancelCtx, cancelFunc)
	cancelCtx, cf := dbos.WithCancelCause(mockCtx)
	if cancelCtx != dbos.DBOSContext(mockCancelCtx) {
		t.Fatal("WithCancelCause did not return mock context")
	}
	if cf == nil {
		t.Fatal("WithCancelCause did not return cancel function")
	}

	err := aRealProgramFunction(mockCtx)
	if err != nil {
		t.Fatal(err)
	}

	// Test MockClient
	mockClient := mocks.NewMockClient(t)
	mockClient.On("Enqueue", "q", "wf", "input", mock.Anything).Return(mockGenericHandle, nil)
	mockClient.On("Shutdown", 1*time.Second).Return()

	h, err := mockClient.Enqueue("q", "wf", "input")
	if err != nil || h != mockGenericHandle {
		t.Fatal("MockClient Enqueue failed")
	}
	mockClient.Shutdown(1 * time.Second)

	// Test remaining DBOSContext methods (optional expectations)
	mockCtx2 := mocks.NewMockDBOSContext(t)

	// Go
	outcomeChan := make(chan dbos.StepOutcome[any], 1)
	mockCtx2.On("Go", mockCtx2, mock.Anything, mock.Anything).Return(outcomeChan, nil).Maybe()

	// Select
	mockCtx2.On("Select", mockCtx2, mock.Anything).Return(42, nil).Maybe()

	// WriteStream
	mockCtx2.On("WriteStream", mockCtx2, "key", "value").Return(nil).Maybe()

	// CloseStream
	mockCtx2.On("CloseStream", mockCtx2, "key").Return(nil).Maybe()

	// ReadStream
	mockCtx2.On("ReadStream", mockCtx2, "wf-id", "key").Return([]any{1, 2, 3}, true, nil).Maybe()

	// ReadStreamAsync
	streamChan := make(chan dbos.StreamValue[any], 1)
	mockCtx2.On("ReadStreamAsync", mockCtx2, "wf-id", "key").Return((<-chan dbos.StreamValue[any])(streamChan), nil).Maybe()

	// Patch
	mockCtx2.On("Patch", mockCtx2, "patch-name").Return(true, nil).Maybe()

	// DeprecatePatch
	mockCtx2.On("DeprecatePatch", mockCtx2, "patch-name").Return(nil).Maybe()

	// ListRegisteredWorkflows
	mockCtx2.On("ListRegisteredWorkflows", mockCtx2).Return([]dbos.WorkflowRegistryEntry{}, nil).Maybe()

	// ListRegisteredQueues
	mockCtx2.On("ListRegisteredQueues", mockCtx2).Return([]dbos.WorkflowQueue{}, nil).Maybe()

	// From
	mockFromCtx := mocks.NewMockDBOSContext(t)
	mockCtx2.On("From", mockCtx2, mock.Anything).Return(mockFromCtx).Maybe()

	// WithoutCancel
	mockNoCancelCtx := mocks.NewMockDBOSContext(t)
	mockCtx2.On("WithoutCancel", mockCtx2).Return(mockNoCancelCtx).Maybe()

	// WithTimeout
	mockTimeoutCtx := mocks.NewMockDBOSContext(t)
	var cancel context.CancelFunc = func() {}
	mockCtx2.On("WithTimeout", mockCtx2, mock.Anything).Return(mockTimeoutCtx, cancel).Maybe()

	// ListenQueues
	mockCtx2.On("ListenQueues", mockCtx2, mock.Anything).Maybe()
}

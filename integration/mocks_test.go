package dbos_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/integration/internal/mocks"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
)

func step(ctx context.Context) (int, error) {
	return 1, nil
}

func childWorkflow(ctx dbos.Context, i int) (int, error) {
	return i + 1, nil
}

func workflow(ctx dbos.Context, i int) (int, error) {
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

	_, err = dbos.ForkWorkflow[int](ctx, dbos.ForkWorkflowInput{OriginalWorkflowID: workflowID, StartStep: uint(stepID)})
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

func aRealProgramFunction(dbosCtx dbos.Context) error {

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

	// Test WithCancel
	plainCancelCtx, plainCancel := dbos.WithCancel(dbosCtx)
	if plainCancelCtx == nil {
		return fmt.Errorf("WithCancel returned nil context")
	}
	if plainCancel == nil {
		return fmt.Errorf("WithCancel returned nil cancel function")
	}

	return nil
}

// clientMethodsFunction exercises remaining Context methods
func clientMethodsFunction(ctx dbos.Context) error {
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
	entries := dbos.ListRegisteredWorkflows(ctx)
	if len(entries) != 1 {
		return fmt.Errorf("ListRegisteredWorkflows failed: entries=%v", entries)
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
	dbos.ListenQueues(ctx, "queue1", "queue2")

	// DeleteWorkflows
	err = dbos.DeleteWorkflows(ctx, []string{"wf-to-delete"})
	if err != nil {
		return fmt.Errorf("DeleteWorkflows failed: %w", err)
	}

	// RunAsTransaction
	txnResult, err := dbos.RunAsTransaction(ctx, &dbos.DataSource{}, func(ctx context.Context, tx dbos.Tx) (int, error) {
		return 21, nil
	})
	if err != nil || txnResult != 21 {
		return fmt.Errorf("RunAsTransaction failed: result=%v, err=%v", txnResult, err)
	}

	return nil
}

// clientUsingFunction demonstrates Client usage with specific values
func clientUsingFunction(client dbos.Client) error {
	handle, err := client.Enqueue(client, "my-queue", "my-workflow", "input-data")
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

	client.Shutdown(client, 1 * time.Second)
	return nil
}

func TestMocks(t *testing.T) {
	defer goleak.VerifyNone(t)
	mockCtx := mocks.NewMockContext(t)

	// Context lifecycle
	mockCtx.On("Launch").Return(nil)
	mockCtx.On("Shutdown", mock.Anything, mock.Anything).Return(nil)

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
	mockGenericHandle.On("GetWorkflowID").Return("generic-workflow-id").Maybe()

	mockCtx.On("RetrieveWorkflow", mockCtx, "test-workflow-id").Return(mockGenericHandle, nil)
	mockCtx.On("CancelWorkflow", mockCtx, "test-workflow-id").Return(nil)
	mockCtx.On("ResumeWorkflow", mockCtx, "test-workflow-id").Return(mockGenericHandle, nil)
	mockCtx.On("ForkWorkflow", mockCtx, dbos.ForkWorkflowInput{
		OriginalWorkflowID: "test-workflow-id",
		StartStep:          1,
	}).Return(mockGenericHandle, nil)
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

	// The mock type-asserts the stored return value to the interface's
	// receive-only channel type, so hand it a <-chan.
	var outcomeRecv <-chan dbos.StepOutcome[any] = outcomeChan
	mockCtx.On("Go", mockCtx, mock.MatchedBy(func(fn interface{}) bool {
		return fn != nil
	}), mock.Anything).Return(outcomeRecv, nil).Once()

	mockCtx.On("Select", mockCtx, mock.MatchedBy(func(chans []<-chan dbos.StepOutcome[any]) bool {
		return len(chans) == 1 && chans != nil
	})).Return(1, nil).Once()

	// Context management
	mockValCtx := mocks.NewMockContext(t)
	mockCtx.On("WithValue", "key", "val").Return(mockValCtx)

	mockCancelCtx := mocks.NewMockContext(t)
	var cancelFunc context.CancelCauseFunc = func(error) {}
	mockCtx.On("WithCancelCause").Return(mockCancelCtx, cancelFunc)

	var plainCancelFunc context.CancelFunc = func() {}
	mockCtx.On("WithCancel").Return(mockCancelCtx, plainCancelFunc)

	err := aRealProgramFunction(mockCtx)
	if err != nil {
		t.Fatal(err)
	}

	// Test MockClient with specific values
	mockClient := mocks.NewMockClient(t)
	mockClientHandle := mocks.NewMockWorkflowHandle[any](t)

	// Enqueue with specific values
	mockClientHandle.On("GetStatus").Return(dbos.WorkflowStatus{ID: "wf-123"}, nil).Once()
	mockClient.On("Enqueue", mockClient, "my-queue", "my-workflow", "input-data", mock.Anything).Return(mockClientHandle, nil).Once()
	mockClient.On("Shutdown", mock.Anything, 1*time.Second).Return(nil)

	err = clientUsingFunction(mockClient)
	if err != nil {
		t.Fatalf("clientUsingFunction failed: %v", err)
	}

	// Test remaining Context methods
	mockCtx2 := mocks.NewMockContext(t)

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
	}).Once()

	// From
	mockFromCtx := mocks.NewMockContext(t)
	mockCtx2.On("From", mockCtx2, mock.Anything).Return(mockFromCtx, nil).Once()

	// WithoutCancel
	mockNoCancelCtx := mocks.NewMockContext(t)
	mockCtx2.On("WithoutCancel", mockCtx2).Return(mockNoCancelCtx, nil).Once()

	// WithTimeout
	mockTimeoutCtx := mocks.NewMockContext(t)
	var timeoutCancelFunc context.CancelFunc = func() {}
	mockCtx2.On("WithTimeout", mockCtx2, 5*time.Minute).Return(mockTimeoutCtx, timeoutCancelFunc, nil).Once()

	// ListenQueues
	mockCtx2.On("ListenQueues", mockCtx2, []string{"queue1", "queue2"}).Return().Once()

	// DeleteWorkflows
	mockCtx2.On("DeleteWorkflows", mockCtx2, []string{"wf-to-delete"}, mock.Anything).Return(nil).Once()

	// RunAsTransaction (args: ds, fn, WithStepName option appended by the generic)
	mockCtx2.On("RunAsTransaction", mockCtx2, mock.Anything, mock.Anything, mock.Anything).Return(21, nil).Once()

	err = clientMethodsFunction(mockCtx2)
	if err != nil {
		t.Fatalf("clientMethodsFunction failed: %v", err)
	}
}

// TestClientTypedHelpersWithMock verifies the typed, package-level helpers
// (dbos.GetEvent, dbos.Enqueue, dbos.RetrieveWorkflow, etc.) route through the
// Client interface methods and therefore work with a mocked Client, rather
// than requiring the concrete built-in implementation.
func TestClientTypedHelpersWithMock(t *testing.T) {
	defer goleak.VerifyNone(t)

	mockClient := mocks.NewMockClient(t)

	// GetEvent decodes via the interface GetEvent on a mocked client.
	mockClient.On("GetEvent", mockClient, "wf-evt", "key", 1*time.Second).Return(42, nil).Once()
	evt, err := dbos.GetEvent[int](mockClient, "wf-evt", "key", 1*time.Second)
	if err != nil {
		t.Fatalf("GetEvent failed: %v", err)
	}
	if evt != 42 {
		t.Fatalf("GetEvent = %d, want 42", evt)
	}

	// Enqueue returns a typed handle backed by the mocked handle.
	enqHandle := mocks.NewMockWorkflowHandle[any](t)
	enqHandle.On("GetResult").Return(7, nil).Once()
	mockClient.On("Enqueue", mockClient, "q", "wf", "in", mock.Anything).Return(enqHandle, nil).Once()
	eh, err := dbos.Enqueue[int](mockClient, "q", "wf", "in")
	if err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if res, err := eh.GetResult(); err != nil || res != 7 {
		t.Fatalf("Enqueue handle GetResult = (%d, %v), want (7, nil)", res, err)
	}

	// RetrieveWorkflow returns a typed handle.
	retHandle := mocks.NewMockWorkflowHandle[any](t)
	retHandle.On("GetResult").Return(9, nil).Once()
	mockClient.On("RetrieveWorkflow", mockClient, "wf-ret").Return(retHandle, nil).Once()
	rh, err := dbos.RetrieveWorkflow[int](mockClient, "wf-ret")
	if err != nil {
		t.Fatalf("RetrieveWorkflow failed: %v", err)
	}
	if res, err := rh.GetResult(); err != nil || res != 9 {
		t.Fatalf("RetrieveWorkflow handle GetResult = (%d, %v), want (9, nil)", res, err)
	}

	// ForkWorkflow returns a typed handle.
	forkHandle := mocks.NewMockWorkflowHandle[any](t)
	forkHandle.On("GetResult").Return(11, nil).Once()
	mockClient.On("ForkWorkflow", mockClient, dbos.ForkWorkflowInput{OriginalWorkflowID: "wf-ret"}).Return(forkHandle, nil).Once()
	fh, err := dbos.ForkWorkflow[int](mockClient, dbos.ForkWorkflowInput{OriginalWorkflowID: "wf-ret"})
	if err != nil {
		t.Fatalf("ForkWorkflow failed: %v", err)
	}
	if res, err := fh.GetResult(); err != nil || res != 11 {
		t.Fatalf("ForkWorkflow handle GetResult = (%d, %v), want (11, nil)", res, err)
	}

	// ListWorkflows is an interface method, so it mocks directly.
	mockClient.On("ListWorkflows", mockClient, mock.Anything).Return([]dbos.WorkflowStatus{{ID: "wf-ret"}}, nil).Once()
	list, err := mockClient.ListWorkflows(mockClient)
	if err != nil || len(list) != 1 || list[0].ID != "wf-ret" {
		t.Fatalf("ListWorkflows = (%v, %v), want one status with ID wf-ret", list, err)
	}

	// TriggerSchedule returns a typed handle through the proxy path.
	trigHandle := mocks.NewMockWorkflowHandle[any](t)
	trigHandle.On("GetResult").Return(13, nil).Once()
	mockClient.On("TriggerSchedule", mockClient, "sched").Return(trigHandle, nil).Once()
	th, err := dbos.TriggerSchedule[int](mockClient, "sched")
	if err != nil {
		t.Fatalf("TriggerSchedule failed: %v", err)
	}
	if res, err := th.GetResult(); err != nil || res != 13 {
		t.Fatalf("TriggerSchedule handle GetResult = (%d, %v), want (13, nil)", res, err)
	}

	// ResumeWorkflows wraps each returned handle in a typed proxy.
	batchIDs := []string{"wf-1", "wf-2"}
	res1 := mocks.NewMockWorkflowHandle[any](t)
	res1.On("GetResult").Return(1, nil).Once()
	res2 := mocks.NewMockWorkflowHandle[any](t)
	res2.On("GetResult").Return(2, nil).Once()
	mockClient.On("ResumeWorkflows", mockClient, batchIDs).Return([]dbos.WorkflowHandle[any]{res1, res2}, nil).Once()
	resumed, err := dbos.ResumeWorkflows[int](mockClient, batchIDs)
	if err != nil {
		t.Fatalf("ResumeWorkflows failed: %v", err)
	}
	if len(resumed) != 2 {
		t.Fatalf("ResumeWorkflows returned %d handles, want 2", len(resumed))
	}
	for i, want := range []int{1, 2} {
		if res, err := resumed[i].GetResult(); err != nil || res != want {
			t.Fatalf("ResumeWorkflows handle %d GetResult = (%d, %v), want (%d, nil)", i, res, err, want)
		}
	}

	// ForkWorkflows wraps each returned handle in a typed proxy.
	fork1 := mocks.NewMockWorkflowHandle[any](t)
	fork1.On("GetResult").Return(3, nil).Once()
	fork2 := mocks.NewMockWorkflowHandle[any](t)
	fork2.On("GetResult").Return(4, nil).Once()
	mockClient.On("ForkWorkflows", mockClient, mock.Anything).Return([]dbos.WorkflowHandle[any]{fork1, fork2}, nil).Once()
	forked, err := dbos.ForkWorkflows[int](mockClient, dbos.ForkWorkflowsInput{
		Workflows: []dbos.ForkWorkflowSpec{{OriginalWorkflowID: "wf-1"}, {OriginalWorkflowID: "wf-2"}},
	})
	if err != nil {
		t.Fatalf("ForkWorkflows failed: %v", err)
	}
	if len(forked) != 2 {
		t.Fatalf("ForkWorkflows returned %d handles, want 2", len(forked))
	}
	for i, want := range []int{3, 4} {
		if res, err := forked[i].GetResult(); err != nil || res != want {
			t.Fatalf("ForkWorkflows handle %d GetResult = (%d, %v), want (%d, nil)", i, res, err, want)
		}
	}

	// CancelWorkflows passes through to the interface method.
	mockClient.On("CancelWorkflows", mockClient, batchIDs).Return(nil).Once()
	if err := dbos.CancelWorkflows(mockClient, batchIDs); err != nil {
		t.Fatalf("CancelWorkflows failed: %v", err)
	}

	// Queue management round-trips through a MockQueue.
	mockQueue := mocks.NewMockQueue(t)
	mockQueue.On("GetName").Return("mock-queue").Once()
	mockClient.On("RegisterQueue", mockClient, "mock-queue").Return(mockQueue, nil).Once()
	q, err := dbos.RegisterQueue(mockClient, "mock-queue")
	if err != nil {
		t.Fatalf("RegisterQueue failed: %v", err)
	}
	if q.GetName() != "mock-queue" {
		t.Fatalf("RegisterQueue name = %q, want mock-queue", q.GetName())
	}

	limit := 5
	mockQueue.On("SetGlobalConcurrency", mockClient, &limit).Return(nil).Once()
	if err := q.SetGlobalConcurrency(mockClient, &limit); err != nil {
		t.Fatalf("SetGlobalConcurrency failed: %v", err)
	}

	mockClient.On("RetrieveQueue", mockClient, "mock-queue").Return(mockQueue, nil).Once()
	if _, err := dbos.RetrieveQueue(mockClient, "mock-queue"); err != nil {
		t.Fatalf("RetrieveQueue failed: %v", err)
	}
}

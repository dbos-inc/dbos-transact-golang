package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func isInterfaceType(v any) bool {
	t := reflect.TypeOf(v)
	if t == nil {
		return true // nil can be any type
	}
	return t.Kind() == reflect.Interface
}

// isTestNilValue checks if a value is nil (for pointer types, slice, map, interface, etc.)
func isTestNilValue(v any) bool {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return true
	}
	switch val.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface, reflect.Chan, reflect.Func:
		return val.IsNil()
	}
	return false
}

// Unified helper: test round-trip across all read paths for typed workflows.
// Also handles nil values when expected is nil.
func testRoundTrip[T any, H any](
	t *testing.T,
	executor DBOSContext,
	handle WorkflowHandle[H],
	expected T,
) {
	t.Helper()

	isNilExpected := isTestNilValue(expected)

	t.Run("HandleGetResult", func(t *testing.T) {
		gotAny, err := handle.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, gotAny, "Nil result should be preserved")
		} else {
			assert.Equal(t, expected, gotAny)
		}
	})

	t.Run("ListWorkflows", func(t *testing.T) {
		wfs, err := ListWorkflows(executor,
			WithWorkflowIDs([]string{handle.GetWorkflowID()}),
			WithLoadInput(true), WithLoadOutput(true))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		wf := wfs[0]
		if isNilExpected {
			assert.Nil(t, wf.Input, "Workflow input should be nil")
			assert.Nil(t, wf.Output, "Workflow output should be nil")
		} else {
			require.NotNil(t, wf.Input)
			require.NotNil(t, wf.Output)
			assert.Equal(t, expected, wf.Input)
			assert.Equal(t, expected, wf.Output)
		}
	})

	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
		require.NoError(t, err)
		require.Len(t, steps, 1)
		step := steps[0]
		if isNilExpected {
			assert.Nil(t, step.Output, "Step output should be nil")
		} else {
			require.NotNil(t, step.Output)
			assert.Equal(t, expected, step.Output)
		}
		assert.Nil(t, step.Error)
	})

	t.Run("RetrieveWorkflow", func(t *testing.T) {
		h2, err := RetrieveWorkflow[H](executor, handle.GetWorkflowID())
		require.NoError(t, err)
		gotAny, err := h2.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, gotAny, "Retrieved workflow result should be nil")
		} else {
			assert.Equal(t, expected, gotAny)
		}
	})
}

// Helper function to test Send/Recv communication
func testSendRecv[T any](
	t *testing.T,
	executor DBOSContext,
	senderWorkflow Workflow[T, T],
	receiverWorkflow Workflow[T, T],
	input T,
	senderID string,
) {
	t.Helper()

	// Start receiver workflow first (it will wait for the message)
	receiverHandle, err := RunWorkflow(executor, receiverWorkflow, input, WithWorkflowID(senderID+"-receiver"))
	require.NoError(t, err, "Receiver workflow execution failed")

	// Start sender workflow (it will send the message)
	senderHandle, err := RunWorkflow(executor, senderWorkflow, input, WithWorkflowID(senderID))
	require.NoError(t, err, "Sender workflow execution failed")

	// Get sender result
	senderResult, err := senderHandle.GetResult()
	require.NoError(t, err, "Sender workflow should complete")

	// Get receiver result
	receiverResult, err := receiverHandle.GetResult()
	require.NoError(t, err, "Receiver workflow should complete")

	// Verify the received data matches what was sent
	assert.Equal(t, input, senderResult, "Sender result should match input")
	assert.Equal(t, input, receiverResult, "Received data should match sent data")
}

// Helper function to test SetEvent/GetEvent communication
func testSetGetEvent[T any](
	t *testing.T,
	executor DBOSContext,
	setEventWorkflow Workflow[T, T],
	getEventWorkflow Workflow[string, T],
	input T,
	setEventID string,
	getEventID string,
) {
	t.Helper()

	// Start setEvent workflow
	setEventHandle, err := RunWorkflow(executor, setEventWorkflow, input, WithWorkflowID(setEventID))
	require.NoError(t, err, "SetEvent workflow execution failed")

	// Wait for setEvent to complete
	setResult, err := setEventHandle.GetResult()
	require.NoError(t, err, "SetEvent workflow should complete")

	// Start getEvent workflow (will retrieve the event)
	getEventHandle, err := RunWorkflow(executor, getEventWorkflow, setEventID, WithWorkflowID(getEventID))
	require.NoError(t, err, "GetEvent workflow execution failed")

	// Get the event result
	getResult, err := getEventHandle.GetResult()
	require.NoError(t, err, "GetEvent workflow should complete")

	// Verify the event data matches what was set
	assert.Equal(t, input, setResult, "SetEvent result should match input")
	assert.Equal(t, input, getResult, "GetEvent data should match what was set")
}

// Helper function to test workflow recovery
func testWorkflowRecovery[T any](
	t *testing.T,
	executor DBOSContext,
	recoveryWorkflow func(DBOSContext, T) (T, error),
	startEvent *Event,
	blockingEvent *Event,
	input T,
	workflowID string,
) {
	t.Helper()

	// Start the blocking workflow
	handle, err := RunWorkflow(executor, recoveryWorkflow, input, WithWorkflowID(workflowID))
	require.NoError(t, err, "failed to start blocking workflow")

	// Wait for the workflow to reach the blocking step
	startEvent.Wait()

	// Recover the pending workflow
	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	recoveredHandles, err := recoverPendingWorkflows(dbosCtx, []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")

	// Find our workflow in the recovered handles
	var recoveredHandle WorkflowHandle[any]
	for _, h := range recoveredHandles {
		if h.GetWorkflowID() == handle.GetWorkflowID() {
			recoveredHandle = h
			break
		}
	}
	require.NotNil(t, recoveredHandle, "expected to find recovered handle")

	// Verify it's a polling handle
	_, ok = recoveredHandle.(*workflowPollingHandle[any])
	require.True(t, ok, "recovered handle should be of type workflowPollingHandle, got %T", recoveredHandle)

	// Unblock the workflow
	blockingEvent.Set()

	// Get result from the original handle
	originalResult, err := handle.GetResult()
	require.NoError(t, err, "original handle should complete successfully")

	// Get result from the recovered handle
	recoveredResult, err := recoveredHandle.GetResult()
	require.NoError(t, err, "recovered handle should complete successfully")

	// Verify results match input
	assert.Equal(t, input, originalResult, "original handle result should match input")
	assert.Equal(t, input, recoveredResult, "recovered handle result should match input")
}

// Readability helpers: group related subtests behind concise functions
func runScalarsTests(t *testing.T, executor DBOSContext) {
	t.Run("Scalars", func(t *testing.T) {
		// Test int as representative scalar type
		h2, err := RunWorkflow(executor, serializerIntWorkflow, 42)
		require.NoError(t, err)
		testRoundTrip[int, int](t, executor, h2, 42)
	})

	t.Run("EmptyString", func(t *testing.T) {
		emptyStr := ""
		h3, err := RunWorkflow(executor, serializerStringWorkflow, emptyStr)
		require.NoError(t, err)
		testRoundTrip[string, string](t, executor, h3, emptyStr)
	})
}

// testRoundTrip doesn't work super well for pointer types and ListWorkflows/GetWorkflowSteps
func runPointerTests(t *testing.T, executor DBOSContext) {
	t.Run("Pointers", func(t *testing.T) {
		v := 123
		expected := &v
		h2, err := RunWorkflow(executor, serializerIntPtrWorkflow, expected)
		require.NoError(t, err)

		t.Run("HandleGetResult", func(t *testing.T) {
			gotAny, err := h2.GetResult()
			require.NoError(t, err)
			assert.Equal(t, expected, gotAny)
		})

		t.Run("RetrieveWorkflow", func(t *testing.T) {
			h3, err := RetrieveWorkflow[*int](executor, h2.GetWorkflowID())
			require.NoError(t, err)
			gotAny, err := h3.GetResult()
			require.NoError(t, err)
			assert.Equal(t, expected, gotAny)
		})

		// For ListWorkflows and GetWorkflowSteps, the decoded value will be the underlying type,
		// not the pointer, because they use serializer.Decode() without pointer reconstruction
		t.Run("ListWorkflows", func(t *testing.T) {
			wfs, err := ListWorkflows(executor,
				WithWorkflowIDs([]string{h2.GetWorkflowID()}),
				WithLoadInput(true), WithLoadOutput(true))
			require.NoError(t, err)
			require.Len(t, wfs, 1)
			wf := wfs[0]
			require.NotNil(t, wf.Input)
			require.NotNil(t, wf.Output)
			// Decoded value should be the underlying type (int), not the pointer
			assert.Equal(t, v, wf.Input, "Input should be dereferenced value")
			assert.Equal(t, v, wf.Output, "Output should be dereferenced value")
		})

		t.Run("GetWorkflowSteps", func(t *testing.T) {
			steps, err := GetWorkflowSteps(executor, h2.GetWorkflowID())
			require.NoError(t, err)
			require.Len(t, steps, 1)
			step := steps[0]
			require.NotNil(t, step.Output)
			// Decoded value should be the underlying type (int), not the pointer
			assert.Equal(t, v, step.Output, "Step output should be dereferenced value")
			assert.Nil(t, step.Error)
		})
	})
}

func runSlicesAndArraysTests(t *testing.T, executor DBOSContext) {
	t.Run("SlicesAndArrays", func(t *testing.T) {
		// Non-empty slice - tests collection round-trip
		s1 := []int{1, 2, 3}
		h2, err := RunWorkflow(executor, serializerIntSliceWorkflow, s1)
		require.NoError(t, err)
		testRoundTrip[[]int, []int](t, executor, h2, s1)

		// Nil slice - tests nil handling
		var s2 []int
		h4, err := RunWorkflow(executor, serializerIntSliceWorkflow, s2)
		require.NoError(t, err)
		testRoundTrip[[]int, []int](t, executor, h4, s2)
	})
}

func runMapsTests(t *testing.T, executor DBOSContext) {
	t.Run("Maps", func(t *testing.T) {
		// Non-empty map - tests map round-trip
		m1 := map[string]int{"x": 1, "y": 2}
		h2, err := RunWorkflow(executor, serializerStringIntMapWorkflow, m1)
		require.NoError(t, err)
		testRoundTrip[map[string]int, map[string]int](t, executor, h2, m1)

		// Nil map - tests nil handling
		var m2 map[string]int
		h4, err := RunWorkflow(executor, serializerStringIntMapWorkflow, m2)
		require.NoError(t, err)
		testRoundTrip[map[string]int, map[string]int](t, executor, h4, m2)
	})
}

func runInterfaceFieldsTests(t *testing.T, executor DBOSContext) {
	t.Run("InterfaceFieldsStruct", func(t *testing.T) {
		inp := WithInterfaces{A: map[string]any{"k": "v"}}
		h2, err := RunWorkflow(executor, serializerWithInterfacesWorkflow, inp)
		require.NoError(t, err)
		testRoundTrip[WithInterfaces, WithInterfaces](t, executor, h2, inp)
	})
}

func runCustomTypesTests(t *testing.T, executor DBOSContext) {
	t.Run("CustomTypes", func(t *testing.T) {
		mi := MyInt(7)
		h2, err := RunWorkflow(executor, serializerMyIntWorkflow, mi)
		require.NoError(t, err)
		testRoundTrip[MyInt, MyInt](t, executor, h2, mi)

		ms := MyString("zeta")
		h4, err := RunWorkflow(executor, serializerMyStringWorkflow, ms)
		require.NoError(t, err)
		testRoundTrip[MyString, MyString](t, executor, h4, ms)

		msl := []MyString{"a", "b"}
		h6, err := RunWorkflow(executor, serializerMyStringSliceWorkflow, msl)
		require.NoError(t, err)
		testRoundTrip[[]MyString, []MyString](t, executor, h6, msl)

		mm := map[string]MyInt{"k": 9}
		h8, err := RunWorkflow(executor, serializerStringMyIntMapWorkflow, mm)
		require.NoError(t, err)
		testRoundTrip[map[string]MyInt, map[string]MyInt](t, executor, h8, mm)
	})
}

func runCustomMarshalerTests(t *testing.T, executor DBOSContext) {
	t.Run("CustomMarshaler", func(t *testing.T) {
		tw := TwiceInt(11)
		h2, err := RunWorkflow(executor, serializerTwiceIntWorkflow, tw)
		require.NoError(t, err)
		testRoundTrip[TwiceInt, TwiceInt](t, executor, h2, tw)
	})
}

type MyInt int
type MyString string

// Custom marshaler that doubles on marshal and halves on unmarshal
type TwiceInt int

func (t TwiceInt) MarshalJSON() ([]byte, error) {
	v := int(t) * 2
	return json.Marshal(v)
}

func (t *TwiceInt) UnmarshalJSON(b []byte) error {
	var v int
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	*t = TwiceInt(v / 2)
	return nil
}

// Struct with interface fields
type WithInterfaces struct {
	A any
}

// Test data structures for DBOS integration testing
type TestData struct {
	Message string
	Value   int
	Active  bool
}

type TestWorkflowData struct {
	ID       string
	Message  string
	Value    int
	Active   bool
	Data     TestData
	Metadata map[string]string
}

// Test workflows and steps
func serializerTestStep(_ context.Context, input TestWorkflowData) (TestWorkflowData, error) {
	return input, nil
}

func serializerWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (TestWorkflowData, error) {
		return serializerTestStep(context, input)
	})
}

func serializerPointerValueWorkflow(ctx DBOSContext, input *TestWorkflowData) (*TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (*TestWorkflowData, error) {
		return input, nil
	})
}

// makeTestWorkflow creates a generic workflow that simply returns the input.
func makeTestWorkflow[T any]() Workflow[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		return RunAsStep(ctx, func(context context.Context) (T, error) {
			return input, nil
		})
	}
}

// Typed workflow functions for testing concrete signatures
// These are now generated using makeTestWorkflow to reduce boilerplate
var (
	serializerIntWorkflow            = makeTestWorkflow[int]()
	serializerIntPtrWorkflow         = makeTestWorkflow[*int]()
	serializerIntSliceWorkflow       = makeTestWorkflow[[]int]()
	serializerStringIntMapWorkflow   = makeTestWorkflow[map[string]int]()
	serializerWithInterfacesWorkflow = makeTestWorkflow[WithInterfaces]()
	serializerMyIntWorkflow          = makeTestWorkflow[MyInt]()
	serializerMyStringWorkflow       = makeTestWorkflow[MyString]()
	serializerMyStringSliceWorkflow  = makeTestWorkflow[[]MyString]()
	serializerStringMyIntMapWorkflow = makeTestWorkflow[map[string]MyInt]()
	serializerTwiceIntWorkflow       = makeTestWorkflow[TwiceInt]()
	serializerStringWorkflow         = makeTestWorkflow[string]()
	serializerBoolWorkflow           = makeTestWorkflow[bool]()
)

// makeSenderWorkflow creates a generic sender workflow that sends a message to a receiver workflow.
func makeSenderWorkflow[T any]() Workflow[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		receiverWorkflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return *new(T), fmt.Errorf("failed to get workflow ID: %w", err)
		}
		destID := receiverWorkflowID + "-receiver"
		err = Send(ctx, destID, input, "test-topic")
		if err != nil {
			return *new(T), fmt.Errorf("send failed: %w", err)
		}
		return input, nil
	}
}

// makeReceiverWorkflow creates a generic receiver workflow that receives a message.
func makeReceiverWorkflow[T any]() Workflow[T, T] {
	return func(ctx DBOSContext, _ T) (T, error) {
		received, err := Recv[T](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("recv failed: %w", err)
		}
		return received, nil
	}
}

// makeSetEventWorkflow creates a generic workflow that sets an event.
func makeSetEventWorkflow[T any]() Workflow[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		err := SetEvent(ctx, "test-key", input)
		if err != nil {
			return *new(T), fmt.Errorf("set event failed: %w", err)
		}
		return input, nil
	}
}

// makeGetEventWorkflow creates a generic workflow that gets an event.
func makeGetEventWorkflow[T any]() Workflow[string, T] {
	return func(ctx DBOSContext, targetWorkflowID string) (T, error) {
		event, err := GetEvent[T](ctx, targetWorkflowID, "test-key", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("get event failed: %w", err)
		}
		return event, nil
	}
}

// Typed Send/Recv workflows for various types
var (
	serializerIntSenderWorkflow      = makeSenderWorkflow[int]()
	serializerIntReceiverWorkflow    = makeReceiverWorkflow[int]()
	serializerIntPtrSenderWorkflow   = makeSenderWorkflow[*int]()
	serializerIntPtrReceiverWorkflow = makeReceiverWorkflow[*int]()
	serializerMyIntSenderWorkflow    = makeSenderWorkflow[MyInt]()
	serializerMyIntReceiverWorkflow  = makeReceiverWorkflow[MyInt]()
)

// Typed SetEvent/GetEvent workflows for various types
var (
	serializerIntSetEventWorkflow    = makeSetEventWorkflow[int]()
	serializerIntGetEventWorkflow    = makeGetEventWorkflow[int]()
	serializerIntPtrSetEventWorkflow = makeSetEventWorkflow[*int]()
	serializerIntPtrGetEventWorkflow = makeGetEventWorkflow[*int]()
	serializerMyIntSetEventWorkflow  = makeSetEventWorkflow[MyInt]()
	serializerMyIntGetEventWorkflow  = makeGetEventWorkflow[MyInt]()
)

func serializerErrorStep(_ context.Context, _ TestWorkflowData) (TestWorkflowData, error) {
	return TestWorkflowData{}, fmt.Errorf("step error")
}

func serializerErrorWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (TestWorkflowData, error) {
		return serializerErrorStep(context, input)
	})
}

// Workflows for testing Send/Recv with non-basic types
func serializerSenderWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	receiverWorkflowID, err := GetWorkflowID(ctx)
	if err != nil {
		return TestWorkflowData{}, fmt.Errorf("failed to get workflow ID: %w", err)
	}
	// Add a suffix to create receiver workflow ID
	destID := receiverWorkflowID + "-receiver"

	err = Send(ctx, destID, input, "test-topic")
	if err != nil {
		return TestWorkflowData{}, fmt.Errorf("send failed: %w", err)
	}
	return input, nil
}

func serializerReceiverWorkflow(ctx DBOSContext, _ TestWorkflowData) (TestWorkflowData, error) {
	// Receive a message with the expected type
	received, err := Recv[TestWorkflowData](ctx, "test-topic", 10*time.Second)
	if err != nil {
		return TestWorkflowData{}, fmt.Errorf("recv failed: %w", err)
	}
	return received, nil
}

// Workflows for testing SetEvent/GetEvent with non-basic types
func serializerSetEventWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	err := SetEvent(ctx, "test-key", input)
	if err != nil {
		return TestWorkflowData{}, fmt.Errorf("set event failed: %w", err)
	}
	return input, nil
}

func serializerGetEventWorkflow(ctx DBOSContext, targetWorkflowID string) (TestWorkflowData, error) {
	// Get the event with the expected type
	event, err := GetEvent[TestWorkflowData](ctx, targetWorkflowID, "test-key", 10*time.Second)
	if err != nil {
		return TestWorkflowData{}, fmt.Errorf("get event failed: %w", err)
	}
	return event, nil
}

// Workflows for testing recovery with TestWorkflowData type
var (
	serializerRecoveryStartEvent *Event
	serializerRecoveryEvent      *Event
)

func serializerRecoveryWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	// Single blocking step
	return RunAsStep(ctx, func(context context.Context) (TestWorkflowData, error) {
		serializerRecoveryStartEvent.Set()
		serializerRecoveryEvent.Wait()
		return input, nil
	}, WithStepName("BlockingStep"))
}

// init registers all custom types with gob for GobSerializer
// Note: gob requires concrete types to be registered. Interface types cannot be registered
// directly - only their concrete implementations. When encoding interface{} fields,
// gob needs the concrete type to be registered.
func init() {
	// Register wrapper type
	safeGobRegister(gobValue{})

	// Register test data types (concrete structs)
	safeGobRegister(TestData{})
	safeGobRegister(TestWorkflowData{})

	// Register custom type aliases (must register with concrete value)
	safeGobRegister(MyInt(0))
	safeGobRegister(MyString(""))
	safeGobRegister(TwiceInt(0))

	// Register struct with interface fields (the struct itself is concrete)
	safeGobRegister(WithInterfaces{})

	// Register slices of custom types
	safeGobRegister([]MyString(nil))
	safeGobRegister([]MyInt(nil))
	safeGobRegister([]int(nil))
	safeGobRegister([]string(nil))
	safeGobRegister([]bool(nil))

	// Register maps with custom types
	safeGobRegister(map[string]MyInt(nil))
	safeGobRegister(map[string]string(nil))
	safeGobRegister(map[string]int(nil))
	safeGobRegister(map[string]bool(nil))
	safeGobRegister(map[string]any(nil))

	// Register pointer types
	safeGobRegister((*int)(nil))
	safeGobRegister((*string)(nil))
	safeGobRegister((*bool)(nil))
	safeGobRegister((*TestWorkflowData)(nil))
	safeGobRegister((*TestData)(nil))
	safeGobRegister((*MyInt)(nil))
	safeGobRegister((*MyString)(nil))

	// Register time.Time (used in workflow timeouts and sleep operations)
	safeGobRegister(time.Time{})
}

// Test that workflows use the configured serializer for input/output
func TestSerializer(t *testing.T) {
	t.Run("Gob", func(t *testing.T) {
		executor := setupDBOS(t, true, true)

		// Create a test queue for queued workflow tests
		testQueue := NewWorkflowQueue(executor, "serializer-test-queue")

		// Register workflows
		RegisterWorkflow(executor, serializerWorkflow)
		RegisterWorkflow(executor, serializerPointerValueWorkflow)
		RegisterWorkflow(executor, serializerErrorWorkflow)
		RegisterWorkflow(executor, serializerSenderWorkflow)
		RegisterWorkflow(executor, serializerReceiverWorkflow)
		RegisterWorkflow(executor, serializerSetEventWorkflow)
		RegisterWorkflow(executor, serializerGetEventWorkflow)
		RegisterWorkflow(executor, serializerRecoveryWorkflow)
		// Register typed workflows for concrete signatures
		RegisterWorkflow(executor, serializerIntWorkflow)
		RegisterWorkflow(executor, serializerIntPtrWorkflow)
		RegisterWorkflow(executor, serializerIntSliceWorkflow)
		RegisterWorkflow(executor, serializerStringIntMapWorkflow)
		RegisterWorkflow(executor, serializerWithInterfacesWorkflow)
		RegisterWorkflow(executor, serializerMyIntWorkflow)
		RegisterWorkflow(executor, serializerMyStringWorkflow)
		RegisterWorkflow(executor, serializerMyStringSliceWorkflow)
		RegisterWorkflow(executor, serializerStringMyIntMapWorkflow)
		RegisterWorkflow(executor, serializerTwiceIntWorkflow)
		RegisterWorkflow(executor, serializerStringWorkflow)
		RegisterWorkflow(executor, serializerBoolWorkflow)
		// Register typed Send/Recv workflows
		RegisterWorkflow(executor, serializerIntSenderWorkflow)
		RegisterWorkflow(executor, serializerIntReceiverWorkflow)
		RegisterWorkflow(executor, serializerIntPtrSenderWorkflow)
		RegisterWorkflow(executor, serializerIntPtrReceiverWorkflow)
		RegisterWorkflow(executor, serializerMyIntSenderWorkflow)
		RegisterWorkflow(executor, serializerMyIntReceiverWorkflow)
		// Register typed SetEvent/GetEvent workflows
		RegisterWorkflow(executor, serializerIntSetEventWorkflow)
		RegisterWorkflow(executor, serializerIntGetEventWorkflow)
		RegisterWorkflow(executor, serializerIntPtrSetEventWorkflow)
		RegisterWorkflow(executor, serializerIntPtrGetEventWorkflow)
		RegisterWorkflow(executor, serializerMyIntSetEventWorkflow)
		RegisterWorkflow(executor, serializerMyIntGetEventWorkflow)

		err := Launch(executor)
		require.NoError(t, err)
		defer Shutdown(executor, 10*time.Second)

		// Test workflow with comprehensive data structure
		t.Run("ComprehensiveValues", func(t *testing.T) {
			input := TestWorkflowData{
				ID:       "test-id",
				Message:  "test message",
				Value:    42,
				Active:   true,
				Data:     TestData{Message: "embedded", Value: 123, Active: false},
				Metadata: map[string]string{"key": "value"},
			}

			handle, err := RunWorkflow(executor, serializerWorkflow, input)
			require.NoError(t, err, "Workflow execution failed")

			testRoundTrip[TestWorkflowData, TestWorkflowData](t, executor, handle, input)
		})

		// Test nil values with pointer type workflow
		t.Run("NilValuesPointer", func(t *testing.T) {
			handle, err := RunWorkflow(executor, serializerPointerValueWorkflow, (*TestWorkflowData)(nil))
			require.NoError(t, err, "Nil pointer workflow execution failed")

			testRoundTrip[*TestWorkflowData, *TestWorkflowData](t, executor, handle, (*TestWorkflowData)(nil))
		})

		// Test error values
		t.Run("ErrorValues", func(t *testing.T) {
			input := TestWorkflowData{
				ID:       "error-test-id",
				Message:  "error test",
				Value:    123,
				Active:   true,
				Data:     TestData{Message: "error data", Value: 456, Active: false},
				Metadata: map[string]string{"type": "error"},
			}

			handle, err := RunWorkflow(executor, serializerErrorWorkflow, input)
			require.NoError(t, err, "Error workflow execution failed")

			// 1. Test with handle.GetResult()
			t.Run("HandleGetResult", func(t *testing.T) {
				_, err := handle.GetResult()
				require.Error(t, err, "Should get step error")
				assert.Contains(t, err.Error(), "step error", "Error message should be preserved")
			})

			// 2. Test with GetWorkflowSteps
			t.Run("GetWorkflowSteps", func(t *testing.T) {
				steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
				require.NoError(t, err, "Failed to get workflow steps")
				require.Len(t, steps, 1, "Expected 1 step")

				step := steps[0]
				require.NotNil(t, step.Error, "Step should have error")
				assert.Contains(t, step.Error.Error(), "step error", "Step error should be preserved")
			})
		})

		// Test Send/Recv with non-basic types
		t.Run("SendRecv", func(t *testing.T) {
			input := TestWorkflowData{
				ID:       "sendrecv-test-id",
				Message:  "test message",
				Value:    99,
				Active:   true,
				Data:     TestData{Message: "nested", Value: 200, Active: true},
				Metadata: map[string]string{"comm": "sendrecv"},
			}

			testSendRecv(t, executor, serializerSenderWorkflow, serializerReceiverWorkflow, input, "sender-wf")
		})

		// Test SetEvent/GetEvent with non-basic types
		t.Run("SetGetEvent", func(t *testing.T) {
			input := TestWorkflowData{
				ID:       "event-test-id",
				Message:  "event message",
				Value:    77,
				Active:   false,
				Data:     TestData{Message: "event nested", Value: 333, Active: true},
				Metadata: map[string]string{"type": "event"},
			}

			testSetGetEvent(t, executor, serializerSetEventWorkflow, serializerGetEventWorkflow, input, "setevent-wf", "getevent-wf")
		})

		// Test typed Send/Recv and SetEvent/GetEvent with various types
		t.Run("TypedSendRecvAndSetGetEvent", func(t *testing.T) {
			// Test int (scalar type)
			t.Run("Int", func(t *testing.T) {
				input := 42
				testSendRecv(t, executor, serializerIntSenderWorkflow, serializerIntReceiverWorkflow, input, "typed-int-sender-wf")
				testSetGetEvent(t, executor, serializerIntSetEventWorkflow, serializerIntGetEventWorkflow, input, "typed-int-setevent-wf", "typed-int-getevent-wf")
			})

			// Test MyInt (user defined type)
			t.Run("MyInt", func(t *testing.T) {
				input := MyInt(73)
				testSendRecv(t, executor, serializerMyIntSenderWorkflow, serializerMyIntReceiverWorkflow, input, "typed-myint-sender-wf")
				testSetGetEvent(t, executor, serializerMyIntSetEventWorkflow, serializerMyIntGetEventWorkflow, input, "typed-myint-setevent-wf", "typed-myint-getevent-wf")
			})

			// Test *int (pointer type, set)
			t.Run("IntPtrSet", func(t *testing.T) {
				v := 99
				input := &v
				testSendRecv(t, executor, serializerIntPtrSenderWorkflow, serializerIntPtrReceiverWorkflow, input, "typed-intptr-set-sender-wf")
				testSetGetEvent(t, executor, serializerIntPtrSetEventWorkflow, serializerIntPtrGetEventWorkflow, input, "typed-intptr-set-setevent-wf", "typed-intptr-set-getevent-wf")
			})

			// Test *int (pointer type, nil)
			t.Run("IntPtrNil", func(t *testing.T) {
				var input *int = nil
				testSendRecv(t, executor, serializerIntPtrSenderWorkflow, serializerIntPtrReceiverWorkflow, input, "typed-intptr-nil-sender-wf")
				testSetGetEvent(t, executor, serializerIntPtrSetEventWorkflow, serializerIntPtrGetEventWorkflow, input, "typed-intptr-nil-setevent-wf", "typed-intptr-nil-getevent-wf")
			})
		})

		// Test workflow recovery with TestWorkflowData type
		t.Run("WorkflowRecovery", func(t *testing.T) {
			serializerRecoveryStartEvent = NewEvent()
			serializerRecoveryEvent = NewEvent()

			input := TestWorkflowData{
				ID:       "recovery-test-id",
				Message:  "recovery test message",
				Value:    123,
				Active:   true,
				Data:     TestData{Message: "recovery nested", Value: 456, Active: false},
				Metadata: map[string]string{"type": "recovery"},
			}

			testWorkflowRecovery(t, executor, serializerRecoveryWorkflow, serializerRecoveryStartEvent, serializerRecoveryEvent, input, "serializer-recovery-wf")
		})

		// Test queued workflow with TestWorkflowData type
		t.Run("QueuedWorkflow", func(t *testing.T) {
			input := TestWorkflowData{
				ID:       "queued-test-id",
				Message:  "queued test message",
				Value:    456,
				Active:   false,
				Data:     TestData{Message: "queued nested", Value: 789, Active: true},
				Metadata: map[string]string{"type": "queued"},
			}

			// Start workflow with queue option
			handle, err := RunWorkflow(executor, serializerWorkflow, input, WithWorkflowID("serializer-queued-wf"), WithQueue(testQueue.Name))
			require.NoError(t, err, "failed to start queued workflow")

			// Get result from the handle
			result, err := handle.GetResult()
			require.NoError(t, err, "queued workflow should complete successfully")
			assert.Equal(t, input, result, "queued workflow result should match input")
		})

		// Additional coverage: Scalars
		runScalarsTests(t, executor)

		// Pointer variants (non-nil)
		runPointerTests(t, executor)

		// Slices and arrays, including nil vs empty and nested
		runSlicesAndArraysTests(t, executor)

		// Maps, including non-string keys
		runMapsTests(t, executor)

		// Struct with interface fields
		runInterfaceFieldsTests(t, executor)

		// Custom defined types
		runCustomTypesTests(t, executor)

		// Custom marshaler/unmarshaler
		runCustomMarshalerTests(t, executor)

	})
}

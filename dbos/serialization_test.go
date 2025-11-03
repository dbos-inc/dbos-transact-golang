package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
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

// needsJSONRecast checks if JSON recast is needed for the given types and executor.
// JSON recast is needed when using JSON serializer with an any-typed handle.
func needsJSONRecast[T any, H any](executor DBOSContext) bool {
	dbosCtx, ok := executor.(*dbosContext)
	if !ok {
		return false
	}
	isJSON := isJSONSerializer(dbosCtx.serializer)
	var zeroH H
	isAnyHandle := isInterfaceType(zeroH)
	return isJSON && isAnyHandle
}

// Unified helper: test round-trip across all read paths for both typed and any workflows.
// Handles JSON recast automatically when needed (when handle is WorkflowHandle[any] and JSON serializer).
// Also handles nil values when expected is nil.
func testRoundTrip[T any, H any](
	t *testing.T,
	executor DBOSContext,
	handle WorkflowHandle[H],
	expected T,
) {
	t.Helper()

	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	isJSON := isJSONSerializer(dbosCtx.serializer)
	needsRecast := needsJSONRecast[T, H](executor)
	isNilExpected := isTestNilValue(expected)

	t.Run("HandleGetResult", func(t *testing.T) {
		gotAny, err := handle.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, gotAny, "Nil result should be preserved")
		} else if needsRecast {
			got, err := convertJSONToType[T](gotAny)
			require.NoError(t, err)
			assert.Equal(t, expected, got)
		} else {
			assert.Equal(t, expected, gotAny)
		}
	})

	// ListWorkflows returns typeless input and output values. Needs recast by the caller if serializer is JSON.
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
			if isJSON {
				inVal, err := convertJSONToType[T](wf.Input)
				require.NoError(t, err)
				outVal, err := convertJSONToType[T](wf.Output)
				require.NoError(t, err)
				assert.Equal(t, expected, inVal)
				assert.Equal(t, expected, outVal)
			} else {
				assert.Equal(t, expected, wf.Input)
				assert.Equal(t, expected, wf.Output)
			}
		}
	})

	// GetWorkflowSteps returns typeless output values. Needs recast by the caller if serializer is JSON.
	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
		require.NoError(t, err)
		require.Len(t, steps, 1)
		step := steps[0]
		if isNilExpected {
			assert.Nil(t, step.Output, "Step output should be nil")
		} else {
			require.NotNil(t, step.Output)
			if isJSON {
				outVal, err := convertJSONToType[T](step.Output)
				require.NoError(t, err)
				assert.Equal(t, expected, outVal)
			} else {
				assert.Equal(t, expected, step.Output)
			}
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
		} else if needsRecast {
			got, err := convertJSONToType[T](gotAny)
			require.NoError(t, err)
			assert.Equal(t, expected, got)
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
	if needsJSONRecast[T, T](executor) {
		// For JSON serializer with any type, convert map[string]interface{} to typed struct
		typedSenderResult, err := convertJSONToType[TestWorkflowData](senderResult)
		require.NoError(t, err, "Failed to convert sender result")
		typedReceiverResult, err := convertJSONToType[TestWorkflowData](receiverResult)
		require.NoError(t, err, "Failed to convert receiver result")
		typedInput, err := convertJSONToType[TestWorkflowData](input)
		require.NoError(t, err, "Failed to convert input")
		assert.Equal(t, typedInput, typedSenderResult, "Sender result should match input")
		assert.Equal(t, typedInput, typedReceiverResult, "Received data should match sent data")
	} else {
		assert.Equal(t, input, senderResult, "Sender result should match input")
		assert.Equal(t, input, receiverResult, "Received data should match sent data")
	}
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
	if needsJSONRecast[T, T](executor) {
		// For JSON serializer with any type, convert map[string]interface{} to typed struct
		typedSetResult, err := convertJSONToType[TestWorkflowData](setResult)
		require.NoError(t, err, "Failed to convert set result")
		typedGetResult, err := convertJSONToType[TestWorkflowData](getResult)
		require.NoError(t, err, "Failed to convert get result")
		typedInput, err := convertJSONToType[TestWorkflowData](input)
		require.NoError(t, err, "Failed to convert input")
		assert.Equal(t, typedInput, typedSetResult, "SetEvent result should match input")
		assert.Equal(t, typedInput, typedGetResult, "GetEvent data should match what was set")
	} else {
		assert.Equal(t, input, setResult, "SetEvent result should match input")
		assert.Equal(t, input, getResult, "GetEvent data should match what was set")
	}
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
	isJSON := isJSONSerializer(dbosCtx.serializer)
	needsRecast := needsJSONRecast[T, T](executor)
	if isJSON {
		// Recovery handles are always "any", so always convert recovered result
		typedRecoveredResult, err := convertJSONToType[TestWorkflowData](recoveredResult)
		require.NoError(t, err, "Failed to convert recovered result")
		if needsRecast {
			// For JSON serializer with any type, convert original result too
			typedOriginalResult, err := convertJSONToType[TestWorkflowData](originalResult)
			require.NoError(t, err, "Failed to convert original result")
			typedInput, err := convertJSONToType[TestWorkflowData](input)
			require.NoError(t, err, "Failed to convert input")
			assert.Equal(t, typedInput, typedOriginalResult, "original handle result should match input")
			assert.Equal(t, typedInput, typedRecoveredResult, "recovered handle result should match input")
		} else {
			assert.Equal(t, input, originalResult, "original handle result should match input")
			assert.Equal(t, input, typedRecoveredResult, "recovered handle result should match input")
		}
	} else {
		assert.Equal(t, input, originalResult, "original handle result should match input")
		assert.Equal(t, input, recoveredResult, "recovered handle result should match input")
	}
}

// Readability helpers: group related subtests behind concise functions
func runScalarsTests(t *testing.T, executor DBOSContext) {
	t.Run("Scalars", func(t *testing.T) {
		// Test int as representative scalar type
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(int(42)))
		require.NoError(t, err)
		testRoundTrip[int, any](t, executor, h1, 42)

		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerIntWorkflow, 42)
		require.NoError(t, err)
		testRoundTrip[int, int](t, executor, h2, 42)
	})
}

func runPointerTests(t *testing.T, executor DBOSContext) {
	t.Run("Pointers", func(t *testing.T) {
		v := 123
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(&v))
		require.NoError(t, err)
		testRoundTrip[*int, any](t, executor, h1, &v)

		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerIntPtrWorkflow, &v)
		require.NoError(t, err)
		testRoundTrip[*int, *int](t, executor, h2, &v)
	})
}

func runSlicesAndArraysTests(t *testing.T, executor DBOSContext) {
	t.Run("SlicesAndArrays", func(t *testing.T) {
		// Non-empty slice - tests collection round-trip
		s1 := []int{1, 2, 3}
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(s1))
		require.NoError(t, err)
		testRoundTrip[[]int, any](t, executor, h1, s1)

		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerIntSliceWorkflow, s1)
		require.NoError(t, err)
		testRoundTrip[[]int, []int](t, executor, h2, s1)

		// Nil slice - tests nil handling
		var s2 []int
		// Test with any-typed workflow
		h3, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(s2))
		require.NoError(t, err)
		testRoundTrip[[]int, any](t, executor, h3, s2)

		// Test with typed workflow
		h4, err := RunWorkflow(executor, serializerIntSliceWorkflow, s2)
		require.NoError(t, err)
		testRoundTrip[[]int, []int](t, executor, h4, s2)
	})
}

func runMapsTests(t *testing.T, executor DBOSContext) {
	t.Run("Maps", func(t *testing.T) {
		// Non-empty map - tests map round-trip
		m1 := map[string]int{"x": 1, "y": 2}
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(m1))
		require.NoError(t, err)
		testRoundTrip[map[string]int, any](t, executor, h1, m1)

		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerStringIntMapWorkflow, m1)
		require.NoError(t, err)
		testRoundTrip[map[string]int, map[string]int](t, executor, h2, m1)

		// Nil map - tests nil handling
		var m2 map[string]int
		// Test with any-typed workflow
		h3, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(m2))
		require.NoError(t, err)
		testRoundTrip[map[string]int, any](t, executor, h3, m2)

		// Test with typed workflow
		h4, err := RunWorkflow(executor, serializerStringIntMapWorkflow, m2)
		require.NoError(t, err)
		testRoundTrip[map[string]int, map[string]int](t, executor, h4, m2)
	})
}

func runInterfaceFieldsTests(t *testing.T, executor DBOSContext) {
	t.Run("InterfaceFieldsStruct", func(t *testing.T) {
		inp := WithInterfaces{A: map[string]any{"k": "v"}}
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(inp))
		require.NoError(t, err)
		testRoundTrip[WithInterfaces, any](t, executor, h1, inp)

		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerWithInterfacesWorkflow, inp)
		require.NoError(t, err)
		testRoundTrip[WithInterfaces, WithInterfaces](t, executor, h2, inp)
	})
}

func runCustomTypesTests(t *testing.T, executor DBOSContext) {
	t.Run("CustomTypes", func(t *testing.T) {
		mi := MyInt(7)
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(mi))
		require.NoError(t, err)
		testRoundTrip[MyInt, any](t, executor, h1, mi)
		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerMyIntWorkflow, mi)
		require.NoError(t, err)
		testRoundTrip[MyInt, MyInt](t, executor, h2, mi)

		ms := MyString("zeta")
		// Test with any-typed workflow
		h3, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(ms))
		require.NoError(t, err)
		testRoundTrip[MyString, any](t, executor, h3, ms)
		// Test with typed workflow
		h4, err := RunWorkflow(executor, serializerMyStringWorkflow, ms)
		require.NoError(t, err)
		testRoundTrip[MyString, MyString](t, executor, h4, ms)

		msl := []MyString{"a", "b"}
		// Test with any-typed workflow
		h5, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(msl))
		require.NoError(t, err)
		testRoundTrip[[]MyString, any](t, executor, h5, msl)
		// Test with typed workflow
		h6, err := RunWorkflow(executor, serializerMyStringSliceWorkflow, msl)
		require.NoError(t, err)
		testRoundTrip[[]MyString, []MyString](t, executor, h6, msl)

		mm := map[string]MyInt{"k": 9}
		// Test with any-typed workflow
		h7, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(mm))
		require.NoError(t, err)
		testRoundTrip[map[string]MyInt, any](t, executor, h7, mm)
		// Test with typed workflow
		h8, err := RunWorkflow(executor, serializerStringMyIntMapWorkflow, mm)
		require.NoError(t, err)
		testRoundTrip[map[string]MyInt, map[string]MyInt](t, executor, h8, mm)
	})
}

func runCustomMarshalerTests(t *testing.T, executor DBOSContext) {
	t.Run("CustomMarshaler", func(t *testing.T) {
		tw := TwiceInt(11)
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(tw))
		require.NoError(t, err)
		testRoundTrip[TwiceInt, any](t, executor, h1, tw)

		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerTwiceIntWorkflow, tw)
		require.NoError(t, err)
		testRoundTrip[TwiceInt, TwiceInt](t, executor, h2, tw)
	})
}

func runJSONEdgeTests(t *testing.T, executor DBOSContext) {
	t.Run("JSONEdgeCases", func(t *testing.T) {
		// Empty string
		// Test with any-typed workflow
		h1, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(""))
		require.NoError(t, err)
		testRoundTrip[string, any](t, executor, h1, "")
		// Test with typed workflow
		h2, err := RunWorkflow(executor, serializerStringWorkflow, "")
		require.NoError(t, err)
		testRoundTrip[string, string](t, executor, h2, "")

		// Zero int
		// Test with any-typed workflow
		h3, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(0))
		require.NoError(t, err)
		testRoundTrip[int, any](t, executor, h3, 0)
		// Test with typed workflow
		h4, err := RunWorkflow(executor, serializerIntWorkflow, 0)
		require.NoError(t, err)
		testRoundTrip[int, int](t, executor, h4, 0)

		// False bool
		// Test with any-typed workflow
		h5, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(false))
		require.NoError(t, err)
		testRoundTrip[bool, any](t, executor, h5, false)
		// Test with typed workflow
		h6, err := RunWorkflow(executor, serializerBoolWorkflow, false)
		require.NoError(t, err)
		testRoundTrip[bool, bool](t, executor, h6, false)
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

// Interface for testing interface-typed workflows
type DataProvider interface {
	GetMessage() string
	GetValue() int
}

// Concrete implementation of DataProvider
type ConcreteDataProvider struct {
	Message string
	Value   int
}

func (c ConcreteDataProvider) GetMessage() string {
	return c.Message
}

func (c ConcreteDataProvider) GetValue() int {
	return c.Value
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

func serializerNilValueWorkflow(ctx DBOSContext, input *TestWorkflowData) (*TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (*TestWorkflowData, error) {
		return input, nil
	})
}

func serializerAnyValueWorkflow(ctx DBOSContext, input any) (any, error) {
	return RunAsStep(ctx, func(context context.Context) (any, error) {
		if input == nil {
			return nil, nil
		}
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

func serializerInterfaceValueWorkflow(ctx DBOSContext, input DataProvider) (DataProvider, error) {
	return RunAsStep(ctx, func(context context.Context) (DataProvider, error) {
		return input, nil
	})
}

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

// Workflows for testing Send/Recv with any type
func serializerAnySenderWorkflow(ctx DBOSContext, input any) (any, error) {
	receiverWorkflowID, err := GetWorkflowID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow ID: %w", err)
	}
	// Add a suffix to create receiver workflow ID
	destID := receiverWorkflowID + "-receiver"

	err = Send(ctx, destID, input, "test-topic")
	if err != nil {
		return nil, fmt.Errorf("send failed: %w", err)
	}
	return input, nil
}

func serializerAnyReceiverWorkflow(ctx DBOSContext, _ any) (any, error) {
	// Receive a message with any type
	received, err := Recv[any](ctx, "test-topic", 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("recv failed: %w", err)
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

// Workflows for testing SetEvent/GetEvent with any type
func serializerAnySetEventWorkflow(ctx DBOSContext, input any) (any, error) {
	err := SetEvent(ctx, "test-key", input)
	if err != nil {
		return nil, fmt.Errorf("set event failed: %w", err)
	}
	return input, nil
}

func serializerAnyGetEventWorkflow(ctx DBOSContext, targetWorkflowID string) (any, error) {
	// Get the event with any type
	event, err := GetEvent[any](ctx, targetWorkflowID, "test-key", 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("get event failed: %w", err)
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

// Workflows for testing recovery with any type
var (
	serializerAnyRecoveryStartEvent *Event
	serializerAnyRecoveryEvent      *Event
)

func serializerAnyRecoveryWorkflow(ctx DBOSContext, input any) (any, error) {
	// Single blocking step
	return RunAsStep(ctx, func(context context.Context) (any, error) {
		serializerAnyRecoveryStartEvent.Set()
		serializerAnyRecoveryEvent.Wait()
		return input, nil
	}, WithStepName("BlockingStep"))
}

// gobValue is a wrapper type for gob encoding/decoding of any value
type gobValue struct {
	Value any
}

// safeGobRegister attempts to register a type with gob, recovering only from
// panics caused by duplicate type/name registrations (e.g., registering both T and *T).
// These specific conflicts don't affect encoding/decoding correctness, so they're safe to ignore.
// Other panics (like registering `any`) are real errors and will propagate.
func safeGobRegister(value any, logger *slog.Logger) {
	defer func() {
		if r := recover(); r != nil {
			if errStr, ok := r.(string); ok {
				// Check if this is one of the two specific duplicate registration errors we want to ignore
				// See https://cs.opensource.google/go/go/+/refs/tags/go1.25.1:src/encoding/gob/type.go;l=832
				if strings.Contains(errStr, "gob: registering duplicate types for") ||
					strings.Contains(errStr, "gob: registering duplicate names for") {
					if logger != nil {
						logger.Debug("gob registration conflict", "type", fmt.Sprintf("%T", value), "error", r)
					}
					return
				}
			}
			// Re-panic for any other errors
			panic(r)
		}
	}()
	gob.Register(value)
}

// init registers all custom types with gob for GobSerializer
// Note: gob requires concrete types to be registered. Interface types cannot be registered
// directly - only their concrete implementations. When encoding interface{} fields,
// gob needs the concrete type to be registered.
func init() {
	// Register wrapper type
	safeGobRegister(gobValue{}, nil)

	// Register test data types (concrete structs)
	safeGobRegister(TestData{}, nil)
	safeGobRegister(TestWorkflowData{}, nil)

	// Register custom type aliases (must register with concrete value)
	safeGobRegister(MyInt(0), nil)
	safeGobRegister(MyString(""), nil)
	safeGobRegister(TwiceInt(0), nil)

	// Register struct with interface fields (the struct itself is concrete)
	safeGobRegister(WithInterfaces{}, nil)

	// Register concrete implementation of interface (cannot register DataProvider interface itself)
	safeGobRegister(ConcreteDataProvider{}, nil)

	// Register slices of custom types
	safeGobRegister([]MyString(nil), nil)
	safeGobRegister([]MyInt(nil), nil)
	safeGobRegister([]int(nil), nil)
	safeGobRegister([]string(nil), nil)
	safeGobRegister([]bool(nil), nil)

	// Register maps with custom types
	safeGobRegister(map[string]MyInt(nil), nil)
	safeGobRegister(map[string]string(nil), nil)
	safeGobRegister(map[string]int(nil), nil)
	safeGobRegister(map[string]bool(nil), nil)
	safeGobRegister(map[string]any(nil), nil)

	// Register pointer types
	safeGobRegister((*int)(nil), nil)
	safeGobRegister((*string)(nil), nil)
	safeGobRegister((*bool)(nil), nil)
	safeGobRegister((*TestWorkflowData)(nil), nil)
	safeGobRegister((*TestData)(nil), nil)
	safeGobRegister((*MyInt)(nil), nil)
	safeGobRegister((*MyString)(nil), nil)

	// Register time.Time (used in workflow timeouts and sleep operations)
	safeGobRegister(time.Time{}, nil)
}

// GobSerializer implements Serializer using encoding/gob
type GobSerializer struct{}

func NewGobSerializer() *GobSerializer {
	return &GobSerializer{}
}

func (g *GobSerializer) Encode(data any) (string, error) {
	// Check if data is nil (for pointer types, slice, map, interface, chan, func)
	if isTestNilValue(data) {
		// For nil values, encode an empty byte slice directly to base64
		return base64.StdEncoding.EncodeToString([]byte{}), nil
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	wrapper := gobValue{Value: data}
	if err := encoder.Encode(wrapper); err != nil {
		return "", fmt.Errorf("failed to encode data with gob: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func (g *GobSerializer) Decode(data *string) (any, error) {
	if data == nil || *data == "" {
		return nil, nil
	}

	dataBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64 data: %w", err)
	}

	// If decoded data is empty, it represents a nil value
	if len(dataBytes) == 0 {
		return nil, nil
	}

	var wrapper gobValue
	decoder := gob.NewDecoder(bytes.NewReader(dataBytes))
	if err := decoder.Decode(&wrapper); err != nil {
		return nil, fmt.Errorf("failed to decode gob data: %w", err)
	}

	return wrapper.Value, nil
}

// Test that workflows use the configured serializer for input/output
func TestSerializer(t *testing.T) {
	serializers := map[string]func() Serializer{
		//	"JSON": func() Serializer { return NewJSONSerializer() },
		"Gob": func() Serializer { return NewGobSerializer() },
	}

	for serializerName, serializerFactory := range serializers {
		t.Run(serializerName, func(t *testing.T) {
			executor := setupDBOS(t, true, true, serializerFactory())

			// Create a test queue for queued workflow tests
			testQueue := NewWorkflowQueue(executor, "serializer-test-queue")

			// Register workflows
			RegisterWorkflow(executor, serializerWorkflow)
			RegisterWorkflow(executor, serializerNilValueWorkflow)
			RegisterWorkflow(executor, serializerErrorWorkflow)
			RegisterWorkflow(executor, serializerSenderWorkflow)
			RegisterWorkflow(executor, serializerReceiverWorkflow)
			RegisterWorkflow(executor, serializerSetEventWorkflow)
			RegisterWorkflow(executor, serializerGetEventWorkflow)
			RegisterWorkflow(executor, serializerRecoveryWorkflow)
			RegisterWorkflow(executor, serializerInterfaceValueWorkflow)
			// Register any-type workflows (JSON-only runtime)
			RegisterWorkflow(executor, serializerAnyValueWorkflow)
			RegisterWorkflow(executor, serializerAnySenderWorkflow)
			RegisterWorkflow(executor, serializerAnyReceiverWorkflow)
			RegisterWorkflow(executor, serializerAnySetEventWorkflow)
			RegisterWorkflow(executor, serializerAnyGetEventWorkflow)
			RegisterWorkflow(executor, serializerAnyRecoveryWorkflow)
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

			// Test workflow with any type and comprehensive data structure
			t.Run("ComprehensiveAnyValues", func(t *testing.T) {
				input := TestWorkflowData{
					ID:       "any-test-id",
					Message:  "any test message",
					Value:    99,
					Active:   true,
					Data:     TestData{Message: "any embedded", Value: 777, Active: true},
					Metadata: map[string]string{"type": "any"},
				}

				// Pass input as any to match serializerNilValueAnyWorkflow signature
				handle, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(input))
				require.NoError(t, err, "Any workflow execution failed")

				// For any-typed workflow we keep using any-specific helper elsewhere
				testRoundTrip[TestWorkflowData, any](t, executor, handle, input)
			})

			// Test workflow with interface type
			t.Run("ComprehensiveInterfaceValues", func(t *testing.T) {
				input := ConcreteDataProvider{
					Message: "interface test message",
					Value:   123,
				}

				handle, err := RunWorkflow(executor, serializerInterfaceValueWorkflow, DataProvider(input))
				require.NoError(t, err, "Interface workflow execution failed")

				// Get the result
				result, err := handle.GetResult()
				require.NoError(t, err, "Failed to get workflow result")

				// For interface types, we need to check the concrete type
				concreteResult, ok := result.(ConcreteDataProvider)
				require.True(t, ok, "Result should be ConcreteDataProvider type")
				assert.Equal(t, input.Message, concreteResult.Message, "Message should match")
				assert.Equal(t, input.Value, concreteResult.Value, "Value should match")

				// Test with ListWorkflows
				workflows, err := ListWorkflows(executor,
					WithWorkflowIDs([]string{handle.GetWorkflowID()}),
					WithLoadInput(true),
					WithLoadOutput(true),
				)
				require.NoError(t, err, "Failed to list workflows")
				require.Len(t, workflows, 1, "Expected 1 workflow")

				workflow := workflows[0]
				require.NotNil(t, workflow.Input, "Workflow input should not be nil")
				require.NotNil(t, workflow.Output, "Workflow output should not be nil")

				// For Gob serializer, the concrete type is preserved
				// For JSON serializer, we get a map that needs conversion
				dbosCtx, ok := executor.(*dbosContext)
				require.True(t, ok, "expected dbosContext")
				isJSON := isJSONSerializer(dbosCtx.serializer)

				if isJSON {
					// JSON serializer returns map[string]any. We need to convert it to a _concrete_ type
					inputConcrete, err := convertJSONToType[ConcreteDataProvider](workflow.Input)
					require.NoError(t, err, "Failed to convert workflow input to ConcreteDataProvider")
					assert.Equal(t, input, inputConcrete, "Workflow input should match input")

					outputConcrete, err := convertJSONToType[ConcreteDataProvider](workflow.Output)
					require.NoError(t, err, "Failed to convert workflow output to ConcreteDataProvider")
					assert.Equal(t, input, outputConcrete, "Workflow output should match input")
				} else {
					inputConcrete, ok := workflow.Input.(ConcreteDataProvider)
					require.True(t, ok, "Input should be ConcreteDataProvider for Gob")
					assert.Equal(t, input, inputConcrete, "Input should match")

					outputConcrete, ok := workflow.Output.(ConcreteDataProvider)
					require.True(t, ok, "Output should be ConcreteDataProvider for Gob")
					assert.Equal(t, input, outputConcrete, "Output should match")
				}

				// Test GetWorkflowSteps for interface types
				t.Run("GetWorkflowSteps", func(t *testing.T) {
					steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
					require.NoError(t, err, "Failed to get workflow steps")
					require.Len(t, steps, 1, "Expected 1 step")

					step := steps[0]
					require.NotNil(t, step.Output, "Step output should not be nil")
					assert.Nil(t, step.Error, "Step should not have error")

					if isJSON {
						// JSON serializer returns map[string]any, convert to ConcreteDataProvider
						outputConcrete, err := convertJSONToType[ConcreteDataProvider](step.Output)
						require.NoError(t, err, "Failed to convert step output to ConcreteDataProvider")
						assert.Equal(t, input, outputConcrete, "Step output should match input")
					} else {
						outputConcrete, ok := step.Output.(ConcreteDataProvider)
						require.True(t, ok, "Output should be ConcreteDataProvider for Gob")
						assert.Equal(t, input, outputConcrete, "Step output should match input")
					}
				})

				// Test RetrieveWorkflow for interface types
				// TODO: not supported for interface types w/o storing the type information in the DB
				/*
					t.Run("RetrieveWorkflow", func(t *testing.T) {
						h2, err := RetrieveWorkflow[DataProvider](executor, handle.GetWorkflowID())
						require.NoError(t, err, "Failed to retrieve workflow")

						retrievedResult, err := h2.GetResult()
						require.NoError(t, err, "Failed to get retrieved workflow result")

						// For interface types, we need to check the concrete type
						concreteRetrievedResult, ok := retrievedResult.(ConcreteDataProvider)
						require.True(t, ok, "Retrieved result should be ConcreteDataProvider type")
						assert.Equal(t, input.Message, concreteRetrievedResult.Message, "Message should match")
						assert.Equal(t, input.Value, concreteRetrievedResult.Value, "Value should match")
					})
				*/
			})

			// Test nil values with pointer type workflow
			t.Run("NilValuesPointer", func(t *testing.T) {
				handle, err := RunWorkflow(executor, serializerNilValueWorkflow, (*TestWorkflowData)(nil))
				require.NoError(t, err, "Nil pointer workflow execution failed")

				testRoundTrip[*TestWorkflowData, *TestWorkflowData](t, executor, handle, (*TestWorkflowData)(nil))
			})

			// Test nil values with any type workflow
			t.Run("NilValuesAny", func(t *testing.T) {
				handle, err := RunWorkflow(executor, serializerAnyValueWorkflow, nil)
				require.NoError(t, err, "Nil any workflow execution failed")

				testRoundTrip[any, any](t, executor, handle, nil)
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

			// Test Send/Recv with any type
			t.Run("SendRecvAny", func(t *testing.T) {
				input := TestWorkflowData{
					ID:       "sendrecv-any-test-id",
					Message:  "any test message",
					Value:    888,
					Active:   true,
					Data:     TestData{Message: "any nested", Value: 999, Active: false},
					Metadata: map[string]string{"comm": "sendrecv-any"},
				}

				testSendRecv(t, executor, serializerAnySenderWorkflow, serializerAnyReceiverWorkflow, any(input), "any-sender-wf")
			})

			// Test SetEvent/GetEvent with any type
			t.Run("SetGetEventAny", func(t *testing.T) {
				input := TestWorkflowData{
					ID:       "event-any-test-id",
					Message:  "any event message",
					Value:    555,
					Active:   true,
					Data:     TestData{Message: "any event nested", Value: 666, Active: false},
					Metadata: map[string]string{"type": "event-any"},
				}

				testSetGetEvent(t, executor, serializerAnySetEventWorkflow, serializerAnyGetEventWorkflow, any(input), "any-setevent-wf", "any-getevent-wf")
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

			// Test workflow recovery with any type
			t.Run("WorkflowRecoveryAny", func(t *testing.T) {

				serializerAnyRecoveryStartEvent = NewEvent()
				serializerAnyRecoveryEvent = NewEvent()

				input := TestWorkflowData{
					ID:       "recovery-any-test-id",
					Message:  "recovery any test message",
					Value:    789,
					Active:   false,
					Data:     TestData{Message: "recovery any nested", Value: 987, Active: true},
					Metadata: map[string]string{"type": "recovery-any"},
				}

				testWorkflowRecovery(t, executor, serializerAnyRecoveryWorkflow, serializerAnyRecoveryStartEvent, serializerAnyRecoveryEvent, any(input), "serializer-any-recovery-wf")
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

			// Test queued workflow with any type
			t.Run("QueuedWorkflowAny", func(t *testing.T) {

				input := TestWorkflowData{
					ID:       "queued-any-test-id",
					Message:  "queued any test message",
					Value:    321,
					Active:   true,
					Data:     TestData{Message: "queued any nested", Value: 654, Active: false},
					Metadata: map[string]string{"type": "queued-any"},
				}

				// Start workflow with queue option
				handle, err := RunWorkflow(executor, serializerAnyValueWorkflow, any(input), WithWorkflowID("serializer-queued-any-wf"), WithQueue(testQueue.Name))
				require.NoError(t, err, "failed to start queued workflow")

				// Get result from the handle
				result, err := handle.GetResult()
				require.NoError(t, err, "queued workflow should complete successfully")

				// Convert the result from any type
				typedResult, err := convertJSONToType[TestWorkflowData](result)
				require.NoError(t, err, "Failed to convert result")
				assert.Equal(t, input, typedResult, "queued workflow result should match input")
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

			// JSON edge cases
			runJSONEdgeTests(t, executor)

		})
	}
}

// Test serializer interface compliance
func TestSerializerInterface(t *testing.T) {
	// Test that both serializers implement the Serializer interface
	var _ Serializer = (*JSONSerializer)(nil)
}

// Test that DBOS uses the configured serializer
func TestSerializerConfiguration(t *testing.T) {
	// Test JSON serializer configuration
	t.Run("JSONSerializer", func(t *testing.T) {
		config := Config{
			DatabaseURL: getDatabaseURL(),
			AppName:     "test-app",
			Serializer:  NewJSONSerializer(),
		}

		executor, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err, "Failed to create DBOS context with JSON serializer")

		// Verify the serializer is set in the context
		if dbosCtx, ok := executor.(*dbosContext); ok {
			assert.NotNil(t, dbosCtx.serializer, "JSON serializer should be configured")
			assert.IsType(t, &JSONSerializer{}, dbosCtx.serializer, "Should be JSONSerializer type")
		}
	})

	// Removed Gob serializer configuration test

	// Test default serializer (should be JSON)
	t.Run("DefaultSerializer", func(t *testing.T) {
		config := Config{
			DatabaseURL: getDatabaseURL(),
			AppName:     "test-app",
			// No serializer specified - should default to Gob
		}

		executor, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err, "Failed to create DBOS context with default serializer")

		// Verify the default serializer is Gob
		if dbosCtx, ok := executor.(*dbosContext); ok {
			assert.NotNil(t, dbosCtx.serializer, "Default serializer should be configured")
			assert.IsType(t, &JSONSerializer{}, dbosCtx.serializer, "Default should be JSONSerializer")
		}
	})
}

package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeAnyToType converts an any value (which is map[string]interface{} after JSON decode)
// back to the expected type T by re-encoding to JSON and decoding into T.
func decodeAnyToType[T any](value any) (T, error) {
	var result T
	if value == nil {
		return result, nil
	}
	// Re-encode the any value to JSON
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return result, fmt.Errorf("failed to marshal any value: %w", err)
	}
	// Decode JSON into the expected type T
	if err := json.Unmarshal(jsonBytes, &result); err != nil {
		return result, fmt.Errorf("failed to unmarshal into type T: %w", err)
	}
	return result, nil
}

// testAllSerializationPaths tests workflow recovery and verifies all read paths.
// This is the unified test function that exercises:
// 1. Workflow recovery: starts a workflow, blocks it, recovers it, then verifies completion
// 2. All read paths: HandleGetResult, GetWorkflowSteps, ListWorkflows, RetrieveWorkflow
// This ensures recovery paths exercise all encoding/decoding scenarios that normal workflows do.
// If input is nil, the test expects the output to be nil too.
func testAllSerializationPaths[T any](
	t *testing.T,
	executor DBOSContext,
	recoveryWorkflow Workflow[T, T],
	input T,
	workflowID string,
) {
	t.Helper()

	isNilExpected := isNilValue(input)

	// Setup events for recovery
	startEvent := NewEvent()
	blockingEvent := NewEvent()
	recoveryEventRegistry[workflowID] = struct {
		startEvent    *Event
		blockingEvent *Event
	}{startEvent, blockingEvent}
	defer delete(recoveryEventRegistry, workflowID)

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

	// Unblock the workflow
	blockingEvent.Set()

	// Expected output - workflow returns input, so output equals input
	expectedOutput := input

	// Test read paths after completion
	t.Run("HandleGetResult", func(t *testing.T) {
		output, err := handle.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, output, "Nil result should be preserved")
		} else {
			assert.Equal(t, expectedOutput, output)
		}
	})

	t.Run("RetrieveWorkflow", func(t *testing.T) {
		h2, err := RetrieveWorkflow[T](executor, handle.GetWorkflowID())
		require.NoError(t, err)
		output, err := h2.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, output, "Retrieved workflow result should be nil")
		} else {
			assert.Equal(t, expectedOutput, output, "Retrieved workflow result should match expected output")
		}
	})

	// Check the last step output (the workflow result)
	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(steps), 1, "Should have at least one step")
		if len(steps) > 0 {
			lastStep := steps[len(steps)-1]
			if isNilExpected {
				assert.Nil(t, lastStep.Output, "Step output should be nil")
			} else {
				require.NotNil(t, lastStep.Output)
				// GetWorkflowSteps returns any (map[string]interface{} after JSON decode)
				// We need to re-encode to JSON and decode into type T
				decodedOutput, err := decodeAnyToType[T](lastStep.Output)
				require.NoError(t, err, "Failed to decode step output to type T")
				assert.Equal(t, expectedOutput, decodedOutput, "Step output should match expected output")
			}
			assert.Nil(t, lastStep.Error)
		}
	})

	// Verify final state via ListWorkflows
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

			// ListWorkflows returns any (map[string]interface{} after JSON decode)
			// We need to re-encode to JSON and decode into type T
			decodedInput, err := decodeAnyToType[T](wf.Input)
			require.NoError(t, err, "Failed to decode workflow input to type T")
			decodedOutput, err := decodeAnyToType[T](wf.Output)
			require.NoError(t, err, "Failed to decode workflow output to type T")
			assert.Equal(t, input, decodedInput, "Workflow input should match input")
			assert.Equal(t, expectedOutput, decodedOutput, "Workflow output should match expected output")
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

type MyInt int
type MyString string
type IntSliceSlice [][]int

type TestData struct {
	Message string
	Value   int
	Active  bool
}

type NestedTestData struct {
	Key   string
	Count int
}

type TestWorkflowData struct {
	ID           string
	Message      string
	Value        int
	Active       bool
	Data         TestData
	Metadata     map[string]string
	NestedSlice  []NestedTestData
	NestedMap    map[string]MyInt
	StringPtr    *string
	StringPtrPtr **string
}

// Typed workflow functions for testing concrete signatures
var (
	serializerWorkflow             = makeTestWorkflow[TestWorkflowData]()
	recoveryStructPtrWorkflow      = makeRecoveryWorkflow[*TestWorkflowData]()
	serializerStructWorkflow       = makeRecoveryWorkflow[TestWorkflowData]()
	recoveryIntWorkflow            = makeRecoveryWorkflow[int]()
	recoveryStringWorkflow         = makeRecoveryWorkflow[string]()
	recoveryIntPtrWorkflow         = makeRecoveryWorkflow[*int]()
	recoveryNestedIntPtrWorkflow   = makeRecoveryWorkflow[**int]()
	recoveryIntSliceWorkflow       = makeRecoveryWorkflow[[]int]()
	recoveryIntArrayWorkflow       = makeRecoveryWorkflow[[3]int]()
	recoveryByteSliceWorkflow      = makeRecoveryWorkflow[[]byte]()
	recoveryStringIntMapWorkflow   = makeRecoveryWorkflow[map[string]int]()
	recoveryMyIntWorkflow          = makeRecoveryWorkflow[MyInt]()
	recoveryMyStringWorkflow       = makeRecoveryWorkflow[MyString]()
	recoveryMyStringSliceWorkflow  = makeRecoveryWorkflow[[]MyString]()
	recoveryStringMyIntMapWorkflow = makeRecoveryWorkflow[map[string]MyInt]()
	// Additional types: empty struct, nested collections, slices of pointers
	recoveryEmptyStructWorkflow   = makeRecoveryWorkflow[struct{}]()
	recoveryIntSliceSliceWorkflow = makeRecoveryWorkflow[IntSliceSlice]()
	recoveryNestedMapWorkflow     = makeRecoveryWorkflow[map[string]map[string]int]()
	recoveryIntPtrSliceWorkflow   = makeRecoveryWorkflow[[]*int]()
	recoveryAnyWorkflow           = makeRecoveryWorkflow[any]()
)

// Typed Send/Recv workflows for various types
var (
	serializerSenderWorkflow         = makeSenderWorkflow[TestWorkflowData]()
	serializerReceiverWorkflow       = makeReceiverWorkflow[TestWorkflowData]()
	serializerIntSenderWorkflow      = makeSenderWorkflow[int]()
	serializerIntReceiverWorkflow    = makeReceiverWorkflow[int]()
	serializerIntPtrSenderWorkflow   = makeSenderWorkflow[*int]()
	serializerIntPtrReceiverWorkflow = makeReceiverWorkflow[*int]()
	serializerMyIntSenderWorkflow    = makeSenderWorkflow[MyInt]()
	serializerMyIntReceiverWorkflow  = makeReceiverWorkflow[MyInt]()
)

// Typed SetEvent/GetEvent workflows for various types
var (
	serializerSetEventWorkflow       = makeSetEventWorkflow[TestWorkflowData]()
	serializerGetEventWorkflow       = makeGetEventWorkflow[TestWorkflowData]()
	serializerIntSetEventWorkflow    = makeSetEventWorkflow[int]()
	serializerIntGetEventWorkflow    = makeGetEventWorkflow[int]()
	serializerIntPtrSetEventWorkflow = makeSetEventWorkflow[*int]()
	serializerIntPtrGetEventWorkflow = makeGetEventWorkflow[*int]()
	serializerMyIntSetEventWorkflow  = makeSetEventWorkflow[MyInt]()
	serializerMyIntGetEventWorkflow  = makeGetEventWorkflow[MyInt]()
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

// makeTestWorkflow creates a generic workflow that simply returns the input.
func makeTestWorkflow[T any]() Workflow[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		return RunAsStep(ctx, func(context context.Context) (T, error) {
			return input, nil
		})
	}
}

func serializerErrorStep(_ context.Context, _ TestWorkflowData) (TestWorkflowData, error) {
	return TestWorkflowData{}, fmt.Errorf("step error")
}

func serializerErrorWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (TestWorkflowData, error) {
		return serializerErrorStep(context, input)
	})
}

// recoveryEventRegistry stores events for recovery workflows by workflow ID
var recoveryEventRegistry = make(map[string]struct {
	startEvent    *Event
	blockingEvent *Event
})

// makeRecoveryWorkflow creates a generic recovery workflow that has an initial step
// and then a blocking step that uses the output of the first step.
// This is used to test workflow recovery with various types.
// The workflow looks up events from recoveryEventRegistry using the workflow ID.
func makeRecoveryWorkflow[T any]() Workflow[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		// First step: return the input (tests encoding/decoding of type T)
		firstStepOutput, err := RunAsStep(ctx, func(context context.Context) (T, error) {
			return input, nil
		}, WithStepName("FirstStep"))
		if err != nil {
			fmt.Printf("makeRecoveryWorkflow: FirstStep error: %v\n", err)
			return *new(T), err
		}

		// Second step: blocking step that uses the first step's output
		// This tests that the first step's output is correctly decoded
		// If decoding fails or is incorrect, this step will fail
		return RunAsStep(ctx, func(context context.Context) (T, error) {
			workflowID, err := GetWorkflowID(ctx)
			if err != nil {
				return *new(T), fmt.Errorf("failed to get workflow ID: %w", err)
			}
			events, ok := recoveryEventRegistry[workflowID]
			if !ok {
				return *new(T), fmt.Errorf("no events registered for workflow ID: %s", workflowID)
			}
			events.startEvent.Set()
			events.blockingEvent.Wait()
			// Return the first step's output - this verifies correct decoding
			// If the type was decoded incorrectly, this assignment/return will fail
			return firstStepOutput, nil
		}, WithStepName("BlockingStep"))
	}
}

// TestDataProcessor is an interface for testing workflows with interface signatures
type TestDataProcessor interface {
	Process(data string) string
}

// TestStringProcessor is a concrete implementation of TestDataProcessor
type TestStringProcessor struct {
	Prefix string
}

// Process implements the TestDataProcessor interface
func (p *TestStringProcessor) Process(data string) string {
	return p.Prefix + data
}

// TestSerializer tests that workflows use the configured serializer for input/output.
//
// This test suite uses recovery-based testing as the primary approach. All tests exercise
// workflow recovery paths because:
//  1. Recovery paths exercise all encoding/decoding scenarios that normal workflows do
//  2. Recovery paths additionally test decoding from persisted state (database)
//  3. This ensures that serialization works correctly even when workflows are recovered
//     after a process restart or failure
//
// Each test:
// - Starts a workflow with a blocking step
// - Recovers the pending workflow from the database
// - Verifies all read paths: HandleGetResult, ListWorkflows, GetWorkflowSteps, RetrieveWorkflow
// - Ensures that both original and recovered handles produce correct results
//
// The suite covers: scalars, pointers, nested pointers
// slices, arrays, byte slices, maps, and custom types. It also tests Send/Recv and
// SetEvent/GetEvent communication patterns.
func TestSerializer(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Create a test queue for queued workflow tests
	testQueue := NewWorkflowQueue(executor, "serializer-test-queue")

	// Register workflows
	RegisterWorkflow(executor, serializerWorkflow)
	RegisterWorkflow(executor, recoveryStructPtrWorkflow)
	RegisterWorkflow(executor, serializerErrorWorkflow)
	RegisterWorkflow(executor, serializerSenderWorkflow)
	RegisterWorkflow(executor, serializerReceiverWorkflow)
	RegisterWorkflow(executor, serializerSetEventWorkflow)
	RegisterWorkflow(executor, serializerGetEventWorkflow)
	RegisterWorkflow(executor, serializerStructWorkflow)

	// Register recovery workflows for all types
	RegisterWorkflow(executor, recoveryIntWorkflow)
	RegisterWorkflow(executor, recoveryStringWorkflow)
	RegisterWorkflow(executor, recoveryIntPtrWorkflow)
	RegisterWorkflow(executor, recoveryNestedIntPtrWorkflow)
	RegisterWorkflow(executor, recoveryIntSliceWorkflow)
	RegisterWorkflow(executor, recoveryIntArrayWorkflow)
	RegisterWorkflow(executor, recoveryByteSliceWorkflow)
	RegisterWorkflow(executor, recoveryStringIntMapWorkflow)
	RegisterWorkflow(executor, recoveryMyIntWorkflow)
	RegisterWorkflow(executor, recoveryMyStringWorkflow)
	RegisterWorkflow(executor, recoveryMyStringSliceWorkflow)
	RegisterWorkflow(executor, recoveryStringMyIntMapWorkflow)
	// Register additional recovery workflows
	RegisterWorkflow(executor, recoveryEmptyStructWorkflow)
	RegisterWorkflow(executor, recoveryIntSliceSliceWorkflow)
	RegisterWorkflow(executor, recoveryNestedMapWorkflow)
	RegisterWorkflow(executor, recoveryIntPtrSliceWorkflow)
	RegisterWorkflow(executor, recoveryAnyWorkflow)
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
	t.Run("StructValues", func(t *testing.T) {
		strPtr := "pointer value"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "test-id",
			Message:  "test message",
			Value:    42,
			Active:   true,
			Data:     TestData{Message: "embedded", Value: 123, Active: false},
			Metadata: map[string]string{"key": "value"},
			NestedSlice: []NestedTestData{
				{Key: "nested1", Count: 10},
				{Key: "nested2", Count: 20},
			},
			NestedMap: map[string]MyInt{
				"map-key1": MyInt(100),
				"map-key2": MyInt(200),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		testAllSerializationPaths(t, executor, serializerStructWorkflow, input, "struct-values-wf")
	})

	// Test nil values with pointer type workflow
	t.Run("NilStructPointer", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryStructPtrWorkflow, (*TestWorkflowData)(nil), "nil-pointer-wf")
	})

	t.Run("Int", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryIntWorkflow, 42, "recovery-int-wf")
	})

	t.Run("EmptyString", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryStringWorkflow, "", "recovery-empty-string-wf")
	})

	// Pointer variants (single level only, nested pointers not supported)
	t.Run("Pointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v := 123
			input := &v
			testAllSerializationPaths(t, executor, recoveryIntPtrWorkflow, input, "recovery-int-ptr-wf")

		})

		t.Run("Nil", func(t *testing.T) {
			var input *int = nil
			testAllSerializationPaths(t, executor, recoveryIntPtrWorkflow, input, "recovery-int-ptr-nil-wf")
		})
	})

	t.Run("NestedPointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v := 123
			ptr := &v
			ptrPtr := &ptr
			testAllSerializationPaths(t, executor, recoveryNestedIntPtrWorkflow, ptrPtr, "recovery-nested-int-ptr-wf")

		})

		t.Run("Nil", func(t *testing.T) {
			var ptrPtr **int = nil
			testAllSerializationPaths(t, executor, recoveryNestedIntPtrWorkflow, ptrPtr, "recovery-nested-int-ptr-nil-wf")
		})
	})

	t.Run("SlicesAndArrays", func(t *testing.T) {
		t.Run("NonEmptySlice", func(t *testing.T) {
			input := []int{1, 2, 3}
			testAllSerializationPaths(t, executor, recoveryIntSliceWorkflow, input, "recovery-int-slice-wf")
		})

		t.Run("NilSlice", func(t *testing.T) {
			var input []int = nil
			testAllSerializationPaths(t, executor, recoveryIntSliceWorkflow, input, "recovery-int-slice-nil-wf")
		})

		t.Run("Array", func(t *testing.T) {
			input := [3]int{1, 2, 3}
			testAllSerializationPaths(t, executor, recoveryIntArrayWorkflow, input, "recovery-int-array-wf")
		})
	})

	t.Run("ByteSlices", func(t *testing.T) {
		t.Run("NonEmpty", func(t *testing.T) {
			input := []byte{1, 2, 3, 4, 5}
			testAllSerializationPaths(t, executor, recoveryByteSliceWorkflow, input, "recovery-byte-slice-wf")
		})

		t.Run("Nil", func(t *testing.T) {
			var input []byte = nil
			testAllSerializationPaths(t, executor, recoveryByteSliceWorkflow, input, "recovery-byte-slice-nil-wf")
		})
	})

	t.Run("Maps", func(t *testing.T) {
		t.Run("NonEmptyMap", func(t *testing.T) {
			input := map[string]int{"x": 1, "y": 2}
			testAllSerializationPaths(t, executor, recoveryStringIntMapWorkflow, input, "recovery-string-int-map-wf")
		})

		t.Run("NilMap", func(t *testing.T) {
			var input map[string]int = nil
			testAllSerializationPaths(t, executor, recoveryStringIntMapWorkflow, input, "recovery-string-int-map-nil-wf")
		})
	})

	t.Run("CustomTypes", func(t *testing.T) {
		t.Run("MyInt", func(t *testing.T) {
			input := MyInt(7)
			testAllSerializationPaths(t, executor, recoveryMyIntWorkflow, input, "recovery-myint-wf")
		})

		t.Run("MyString", func(t *testing.T) {
			input := MyString("zeta")
			testAllSerializationPaths(t, executor, recoveryMyStringWorkflow, input, "recovery-mystring-wf")
		})

		t.Run("MyStringSlice", func(t *testing.T) {
			input := []MyString{"a", "b"}
			testAllSerializationPaths(t, executor, recoveryMyStringSliceWorkflow, input, "recovery-mystring-slice-wf")
		})

		t.Run("StringMyIntMap", func(t *testing.T) {
			input := map[string]MyInt{"k": 9}
			testAllSerializationPaths(t, executor, recoveryStringMyIntMapWorkflow, input, "recovery-string-myint-map-wf")
		})
	})

	// Empty struct
	t.Run("EmptyStruct", func(t *testing.T) {
		input := struct{}{}
		testAllSerializationPaths(t, executor, recoveryEmptyStructWorkflow, input, "recovery-empty-struct-wf")
	})

	// Nested collections
	t.Run("NestedCollections", func(t *testing.T) {
		t.Run("SliceOfSlices", func(t *testing.T) {
			input := IntSliceSlice{{1, 2}, {3, 4, 5}}
			testAllSerializationPaths(t, executor, recoveryIntSliceSliceWorkflow, input, "recovery-int-slice-slice-wf")
		})

		t.Run("NestedMap", func(t *testing.T) {
			input := map[string]map[string]int{
				"outer1": {"inner1": 1, "inner2": 2},
				"outer2": {"inner3": 3},
			}
			testAllSerializationPaths(t, executor, recoveryNestedMapWorkflow, input, "recovery-nested-map-wf")
		})
	})

	// Slices of pointers
	t.Run("SliceOfPointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v1 := 10
			v2 := 20
			v3 := 30
			input := []*int{&v1, &v2, &v3}
			testAllSerializationPaths(t, executor, recoveryIntPtrSliceWorkflow, input, "recovery-int-ptr-slice-wf")
		})

		t.Run("NilSlice", func(t *testing.T) {
			var input []*int = nil
			testAllSerializationPaths(t, executor, recoveryIntPtrSliceWorkflow, input, "recovery-int-ptr-slice-nil-wf")
		})
	})

	// Test workflow with any signature using testAllSerializationPaths
	t.Run("Any", func(t *testing.T) {
		// Test with a string value (avoids JSON number type conversion issues)
		input := any("test-value")
		testAllSerializationPaths(t, executor, recoveryAnyWorkflow, input, "recovery-any-string-wf")
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
			NestedSlice: []NestedTestData{
				{Key: "error-nested", Count: 99},
			},
			NestedMap: map[string]MyInt{
				"error-key": MyInt(999),
			},
			StringPtr:    nil,
			StringPtrPtr: nil,
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
		strPtr := "sendrecv pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "sendrecv-test-id",
			Message:  "test message",
			Value:    99,
			Active:   true,
			Data:     TestData{Message: "nested", Value: 200, Active: true},
			Metadata: map[string]string{"comm": "sendrecv"},
			NestedSlice: []NestedTestData{
				{Key: "sendrecv-nested", Count: 50},
			},
			NestedMap: map[string]MyInt{
				"sendrecv-key": MyInt(500),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		testSendRecv(t, executor, serializerSenderWorkflow, serializerReceiverWorkflow, input, "sender-wf")
	})

	// Test SetEvent/GetEvent with non-basic types
	t.Run("SetGetEvent", func(t *testing.T) {
		strPtr := "event pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "event-test-id",
			Message:  "event message",
			Value:    77,
			Active:   false,
			Data:     TestData{Message: "event nested", Value: 333, Active: true},
			Metadata: map[string]string{"type": "event"},
			NestedSlice: []NestedTestData{
				{Key: "event-nested1", Count: 30},
				{Key: "event-nested2", Count: 40},
			},
			NestedMap: map[string]MyInt{
				"event-key1": MyInt(300),
				"event-key2": MyInt(400),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
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

	// Test queued workflow with TestWorkflowData type
	t.Run("QueuedWorkflow", func(t *testing.T) {
		strPtr := "queued pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "queued-test-id",
			Message:  "queued test message",
			Value:    456,
			Active:   false,
			Data:     TestData{Message: "queued nested", Value: 789, Active: true},
			Metadata: map[string]string{"type": "queued"},
			NestedSlice: []NestedTestData{
				{Key: "queued-nested", Count: 222},
			},
			NestedMap: map[string]MyInt{
				"queued-key": MyInt(2222),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		// Start workflow with queue option
		handle, err := RunWorkflow(executor, serializerWorkflow, input, WithWorkflowID("serializer-queued-wf"), WithQueue(testQueue.Name))
		require.NoError(t, err, "failed to start queued workflow")

		// Get result from the handle
		result, err := handle.GetResult()
		require.NoError(t, err, "queued workflow should complete successfully")
		assert.Equal(t, input, result, "queued workflow result should match input")
	})
}

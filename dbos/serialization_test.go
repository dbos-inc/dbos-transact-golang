package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAllSerializationPaths tests workflow recovery and verifies all read paths.
// This is the unified test function that exercises:
// 1. Workflow recovery: starts a workflow, blocks it, recovers it, then verifies completion
// 2. All read paths: HandleGetResult, GetWorkflowSteps, ListWorkflows, RetrieveWorkflow
// This ensures recovery paths exercise all encoding/decoding scenarios that normal workflows do.
// If input is nil, the test expects the output to be nil too.
func testAllSerializationPaths[T any](
	t *testing.T,
	executor Context,
	recoveryWorkflow Workflow[T, T],
	input T,
	workflowID string,
) {
	t.Helper()

	// Check if input is nil (for pointer types, slice, map, etc.)
	val := reflect.ValueOf(input)
	isNilExpected := false
	if !val.IsValid() {
		isNilExpected = true
	} else {
		switch val.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
			isNilExpected = val.IsNil()
		}
	}

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
	customSer := getCustomSerializerFromCtx(executor)
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
				if customSer != nil {
					// Custom serializer: output is already decoded to concrete type
					assert.Equal(t, expectedOutput, lastStep.Output, "Step output should match expected output")
				} else {
					// Default JSON: output is the raw JSON string (base64-decoded)
					strValue, ok := lastStep.Output.(string)
					require.True(t, ok, "Step output should be a string")
					if strValue == "" {
						var zero T
						assert.Equal(t, zero, expectedOutput, "Step output should be the zero value of type T")
					} else {
						var decodedOutput T
						err := json.Unmarshal([]byte(strValue), &decodedOutput)
						require.NoError(t, err, "Failed to unmarshal step output to type T")
						assert.Equal(t, expectedOutput, decodedOutput, "Step output should match expected output")
					}
				}
			}
			assert.Nil(t, lastStep.Error)
		}
	})

	// Verify final state via ListWorkflows
	t.Run("ListWorkflows", func(t *testing.T) {
		wfs, err := ListWorkflows(executor,
			WithFilterWorkflowIDs(handle.GetWorkflowID()),
			WithFilterLoadInput(true), WithFilterLoadOutput(true))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		wf := wfs[0]

		// GetStatus on a launched context must load input/output too
		status, err := handle.GetStatus()
		require.NoError(t, err)
		assert.Equal(t, wf.Input, status.Input, "GetStatus input should match ListWorkflows input")
		assert.Equal(t, wf.Output, status.Output, "GetStatus output should match ListWorkflows output")
		if isNilExpected {
			require.Nil(t, wf.Input, "Workflow input should be nil")
			require.Nil(t, wf.Output, "Workflow output should be nil")
		} else {
			require.NotNil(t, wf.Input)
			require.NotNil(t, wf.Output)

			if customSer != nil {
				// Custom serializer: input/output are already decoded to concrete types
				assert.Equal(t, input, wf.Input, "Workflow input should match input")
				assert.Equal(t, expectedOutput, wf.Output, "Workflow output should match expected output")
			} else {
				// Default JSON: input/output are raw JSON strings (base64-decoded)
				inputStr, ok := wf.Input.(string)
				require.True(t, ok, "Workflow input should be a string")
				outputStr, ok := wf.Output.(string)
				require.True(t, ok, "Workflow output should be a string")

				if inputStr == "" {
					var zero T
					assert.Equal(t, zero, input, "Workflow input should be the zero value of type T")
				} else {
					var decodedInput T
					err := json.Unmarshal([]byte(inputStr), &decodedInput)
					require.NoError(t, err, "Failed to unmarshal workflow input to type T")
					assert.Equal(t, input, decodedInput, "Workflow input should match input")
				}

				if outputStr == "" {
					var zero T
					assert.Equal(t, zero, expectedOutput, "Workflow output should be the zero value of type T")
				} else {
					var decodedOutput T
					err = json.Unmarshal([]byte(outputStr), &decodedOutput)
					require.NoError(t, err, "Failed to unmarshal workflow output to type T")
					assert.Equal(t, expectedOutput, decodedOutput, "Workflow output should match expected output")
				}
			}
		}
	})

	// If nil is expected, verify the nil marker is stored in the database
	if isNilExpected {
		t.Run("DatabaseNilMarker", func(t *testing.T) {
			// Get the database pool to query directly
			dbosCtx, ok := executor.(*dbosContext)
			require.True(t, ok, "expected dbosContext")
			sysDB, ok := dbosCtx.systemDB.(*sysdb.SysDB)
			require.True(t, ok, "expected sysDB")

			// Query the database directly to check for the marker
			ctx := context.Background()
			schemaPrefix := sysDB.Dialect().SchemaPrefix(sysDB.Schema())
			query := sysDB.RenderSQL(`SELECT inputs, output FROM %sworkflow_status WHERE workflow_uuid = $1`, schemaPrefix)

			var inputString, outputString *string
			err := sysDB.Pool().QueryRow(ctx, query, workflowID).Scan(&inputString, &outputString)
			require.NoError(t, err, "failed to query workflow status")

			// Both input and output should be the nil marker
			require.NotNil(t, inputString, "input should not be NULL in database")
			assert.Equal(t, nilMarker, *inputString, "input should be the nil marker")

			require.NotNil(t, outputString, "output should not be NULL in database")
			assert.Equal(t, nilMarker, *outputString, "output should be the nil marker")

			// Also check the step output in operation_outputs
			stepQuery := sysDB.RenderSQL(`SELECT output FROM %soperation_outputs WHERE workflow_uuid = $1 ORDER BY function_id LIMIT 1`, schemaPrefix)
			var stepOutputString *string
			err = sysDB.Pool().QueryRow(ctx, stepQuery, workflowID).Scan(&stepOutputString)
			require.NoError(t, err, "failed to query step output")
			require.NotNil(t, stepOutputString, "step output should not be NULL in database")
			assert.Equal(t, nilMarker, *stepOutputString, "step output should be the nil marker")
		})
	}
}

// Helper function to test Send/Recv communication
func testSendRecv[T any](
	t *testing.T,
	executor Context,
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
	executor Context,
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

// Stream workflows
var serializerStreamWorkflow = makeStreamWorkflow[TestWorkflowData]()

// makeSenderWorkflow creates a generic sender workflow that sends a message to a receiver workflow.
func makeSenderWorkflow[T any]() Workflow[T, T] {
	return func(ctx Context, input T) (T, error) {
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
	return func(ctx Context, _ T) (T, error) {
		received, err := Recv[T](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("recv failed: %w", err)
		}
		return received, nil
	}
}

// makeSetEventWorkflow creates a generic workflow that sets an event.
func makeSetEventWorkflow[T any]() Workflow[T, T] {
	return func(ctx Context, input T) (T, error) {
		err := SetEvent(ctx, "test-key", input)
		if err != nil {
			return *new(T), fmt.Errorf("set event failed: %w", err)
		}
		return input, nil
	}
}

// makeGetEventWorkflow creates a generic workflow that gets an event.
func makeGetEventWorkflow[T any]() Workflow[string, T] {
	return func(ctx Context, targetWorkflowID string) (T, error) {
		event, err := GetEvent[T](ctx, targetWorkflowID, "test-key", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("get event failed: %w", err)
		}
		return event, nil
	}
}

// makeTestWorkflow creates a generic workflow that simply returns the input.
func makeTestWorkflow[T any]() Workflow[T, T] {
	return func(ctx Context, input T) (T, error) {
		return RunAsStep(ctx, func(context context.Context) (T, error) {
			return input, nil
		})
	}
}

func serializerErrorStep(_ context.Context, _ TestWorkflowData) (TestWorkflowData, error) {
	return TestWorkflowData{}, fmt.Errorf("step error")
}

func serializerErrorWorkflow(ctx Context, input TestWorkflowData) (TestWorkflowData, error) {
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
	return func(ctx Context, input T) (T, error) {
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
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Create a test queue for queued workflow tests
	testQueue, err := RegisterQueue(executor, "serializer-test-queue")
	require.NoError(t, err)

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
	RegisterWorkflow(executor, serializerStreamWorkflow)

	err = Launch(executor)
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
		testAllSerializationPaths(t, executor, recoveryIntWorkflow, 0, "recovery-int-wf")
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
		handle, err := RunWorkflow(executor, serializerWorkflow, input, WithWorkflowID("serializer-queued-wf"), WithQueue(testQueue))
		require.NoError(t, err, "failed to start queued workflow")

		// Get result from the handle
		result, err := handle.GetResult()
		require.NoError(t, err, "queued workflow should complete successfully")
		assert.Equal(t, input, result, "queued workflow result should match input")
	})

	// Test WriteStream/ReadStream
	t.Run("WriteReadStream", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "stream-test", Message: "stream data", Value: 111,
			Data:     TestData{Message: "streamed", Value: 222},
			Metadata: map[string]string{"stream": "json"},
		}
		handle, err := RunWorkflow(executor, serializerStreamWorkflow, input, WithWorkflowID("json-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[TestWorkflowData](executor, "json-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})
}

// ===== Gob Serializer Tests =====

// GobOnlyType is a type that uses GobEncoder/GobDecoder for custom binary encoding.
// JSON cannot handle this because it has unexported fields and custom encoding logic.
type GobOnlyType struct {
	real float64
	imag float64
	tag  string
}

func (g GobOnlyType) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(g.real); err != nil {
		return nil, err
	}
	if err := enc.Encode(g.imag); err != nil {
		return nil, err
	}
	if err := enc.Encode(g.tag); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *GobOnlyType) GobDecode(data []byte) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&g.real); err != nil {
		return err
	}
	if err := dec.Decode(&g.imag); err != nil {
		return err
	}
	return dec.Decode(&g.tag)
}

func init() {
	// Register types for gob encoding/decoding through any interface
	gob.Register(TestWorkflowData{})
	gob.Register(TestData{})
	gob.Register(NestedTestData{})
	gob.Register(map[string]string{})
	gob.Register([]NestedTestData{})
	gob.Register(map[string]MyInt{})
	gob.Register(MyInt(0))
	gob.Register(MyString(""))
	gob.Register([]MyString{})
	gob.Register(map[string]int{})
	gob.Register([]int{})
	gob.Register([3]int{})
	gob.Register([]byte{})
	gob.Register(GobOnlyType{})
	gob.Register(Chicken{})
}

var (
	gobRecoveryStructWorkflow   = makeRecoveryWorkflow[TestWorkflowData]()
	gobRecoveryIntWorkflow      = makeRecoveryWorkflow[int]()
	gobRecoveryStringWorkflow   = makeRecoveryWorkflow[string]()
	gobRecoveryIntSliceWorkflow = makeRecoveryWorkflow[[]int]()
	gobRecoveryMapWorkflow      = makeRecoveryWorkflow[map[string]int]()
	gobRecoveryMyIntWorkflow    = makeRecoveryWorkflow[MyInt]()
	gobRecoveryGobOnlyWorkflow  = makeRecoveryWorkflow[GobOnlyType]()
	gobSenderWorkflow           = makeSenderWorkflow[TestWorkflowData]()
	gobReceiverWorkflow         = makeReceiverWorkflow[TestWorkflowData]()
	gobSetEventWorkflow         = makeSetEventWorkflow[TestWorkflowData]()
	gobGetEventWorkflow         = makeGetEventWorkflow[TestWorkflowData]()
	gobGobOnlyWorkflow          = makeTestWorkflow[GobOnlyType]()
	gobGobOnlySenderWorkflow    = makeSenderWorkflow[GobOnlyType]()
	gobGobOnlyReceiverWorkflow  = makeReceiverWorkflow[GobOnlyType]()
	gobGobOnlySetEventWorkflow  = makeSetEventWorkflow[GobOnlyType]()
	gobGobOnlyGetEventWorkflow  = makeGetEventWorkflow[GobOnlyType]()
	gobStreamWorkflow           = makeStreamWorkflow[TestWorkflowData]()
	gobGobOnlyStreamWorkflow    = makeStreamWorkflow[GobOnlyType]()
	gobQueuedWorkflow           = makeTestWorkflow[TestWorkflowData]()
)

// TestGobSerializer tests the built-in gob serializer through all workflow paths.
func TestGobScheduledWorkflowInput(t *testing.T) {
	// ScheduledWorkflowInput is gob-registered by the SDK itself; a gob
	// serializer must round-trip it without any user-side registration.
	ser := NewGobSerializer()
	in := ScheduledWorkflowInput{
		ScheduledTime: time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC),
		Context:       json.RawMessage(`"schedule-context"`),
	}
	encoded, err := ser.Encode(in)
	require.NoError(t, err)
	decoded, err := ser.Decode(encoded)
	require.NoError(t, err)
	out, ok := decoded.(ScheduledWorkflowInput)
	require.True(t, ok, "decoded value has type %T", decoded)
	require.True(t, out.ScheduledTime.Equal(in.ScheduledTime))
	require.Equal(t, in.Context, out.Context)
}

func TestGobSerializer(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})

	RegisterWorkflow(executor, gobRecoveryStructWorkflow)
	RegisterWorkflow(executor, gobRecoveryIntWorkflow)
	RegisterWorkflow(executor, gobRecoveryStringWorkflow)
	RegisterWorkflow(executor, gobRecoveryIntSliceWorkflow)
	RegisterWorkflow(executor, gobRecoveryMapWorkflow)
	RegisterWorkflow(executor, gobRecoveryMyIntWorkflow)
	RegisterWorkflow(executor, gobRecoveryGobOnlyWorkflow)
	RegisterWorkflow(executor, gobSenderWorkflow)
	RegisterWorkflow(executor, gobReceiverWorkflow)
	RegisterWorkflow(executor, gobSetEventWorkflow)
	RegisterWorkflow(executor, gobGetEventWorkflow)
	RegisterWorkflow(executor, gobGobOnlyWorkflow)
	RegisterWorkflow(executor, gobGobOnlySenderWorkflow)
	RegisterWorkflow(executor, gobGobOnlyReceiverWorkflow)
	RegisterWorkflow(executor, gobGobOnlySetEventWorkflow)
	RegisterWorkflow(executor, gobGobOnlyGetEventWorkflow)
	RegisterWorkflow(executor, gobStreamWorkflow)
	RegisterWorkflow(executor, gobGobOnlyStreamWorkflow)
	RegisterWorkflow(executor, gobQueuedWorkflow)

	gobTestQueue, err := RegisterQueue(executor, "gob-serializer-test-queue")
	require.NoError(t, err)

	err = Launch(executor)
	require.NoError(t, err)
	defer Shutdown(executor, 10*time.Second)

	t.Run("Struct", func(t *testing.T) {
		input := TestWorkflowData{
			ID:       "gob-test",
			Message:  "gob message",
			Value:    42,
			Active:   true,
			Data:     TestData{Message: "embedded", Value: 123, Active: false},
			Metadata: map[string]string{"key": "value"},
			NestedSlice: []NestedTestData{
				{Key: "nested1", Count: 10},
			},
			NestedMap: map[string]MyInt{"k": MyInt(100)},
		}
		testAllSerializationPaths(t, executor, gobRecoveryStructWorkflow, input, "gob-struct-wf")
	})

	t.Run("Int", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryIntWorkflow, 42, "gob-int-wf")
	})

	t.Run("String", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryStringWorkflow, "hello gob", "gob-string-wf")
	})

	t.Run("IntSlice", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryIntSliceWorkflow, []int{1, 2, 3}, "gob-int-slice-wf")
	})

	t.Run("Map", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryMapWorkflow, map[string]int{"x": 1, "y": 2}, "gob-map-wf")
	})

	t.Run("MyInt", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryMyIntWorkflow, MyInt(7), "gob-myint-wf")
	})

	// Test gob-only type: uses GobEncoder/GobDecoder with unexported fields.
	// JSON cannot serialize this type. Uses a simple workflow (not recovery-based)
	// because recovery involves step output re-encoding which differs for GobOnly types.
	t.Run("GobOnlyType", func(t *testing.T) {
		input := GobOnlyType{real: 3.14, imag: 2.71, tag: "complex-value"}
		handle, err := RunWorkflow(executor, gobGobOnlyWorkflow, input, WithWorkflowID("gob-only-type-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result, "gob-only type should roundtrip correctly")

		// Verify RetrieveWorkflow also works (reads from DB, decodes with gob)
		h2, err := RetrieveWorkflow[GobOnlyType](executor, "gob-only-type-wf")
		require.NoError(t, err)
		result2, err := h2.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result2, "gob-only type should roundtrip via RetrieveWorkflow")
	})

	t.Run("SendRecv", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-sendrecv", Message: "gob msg", Value: 99,
			Data:     TestData{Message: "nested", Value: 200},
			Metadata: map[string]string{"comm": "gob"},
		}
		testSendRecv(t, executor, gobSenderWorkflow, gobReceiverWorkflow, input, "gob-sender-wf")
	})

	t.Run("SetGetEvent", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-event", Message: "gob event", Value: 77,
			Data:     TestData{Message: "event nested", Value: 333},
			Metadata: map[string]string{"type": "gob-event"},
		}
		testSetGetEvent(t, executor, gobSetEventWorkflow, gobGetEventWorkflow, input, "gob-setevent-wf", "gob-getevent-wf")
	})

	// Test gob-only type through Send/Recv
	t.Run("GobOnlySendRecv", func(t *testing.T) {
		input := GobOnlyType{real: 1.5, imag: 2.5, tag: "sendrecv"}
		testSendRecv(t, executor, gobGobOnlySenderWorkflow, gobGobOnlyReceiverWorkflow, input, "gob-only-sender-wf")
	})

	// Test gob-only type through SetEvent/GetEvent
	t.Run("GobOnlySetGetEvent", func(t *testing.T) {
		input := GobOnlyType{real: 9.8, imag: 6.7, tag: "event"}
		testSetGetEvent(t, executor, gobGobOnlySetEventWorkflow, gobGobOnlyGetEventWorkflow, input, "gob-only-setevent-wf", "gob-only-getevent-wf")
	})

	// Test WriteStream/ReadStream with struct
	t.Run("WriteReadStream", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-stream", Message: "stream data", Value: 55,
			Data:     TestData{Message: "streamed", Value: 555},
			Metadata: map[string]string{"stream": "gob"},
		}
		handle, err := RunWorkflow(executor, gobStreamWorkflow, input, WithWorkflowID("gob-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[TestWorkflowData](executor, "gob-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})

	// Test WriteStream/ReadStream with gob-only type
	t.Run("GobOnlyWriteReadStream", func(t *testing.T) {
		input := GobOnlyType{real: 7.7, imag: 8.8, tag: "streamed"}
		handle, err := RunWorkflow(executor, gobGobOnlyStreamWorkflow, input, WithWorkflowID("gob-only-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[GobOnlyType](executor, "gob-only-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})

	// Test queued workflow
	t.Run("QueuedWorkflow", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-queued", Message: "queued msg", Value: 88,
			Data:     TestData{Message: "queued", Value: 888},
			Metadata: map[string]string{"type": "gob-queued"},
		}
		handle, err := RunWorkflow(executor, gobQueuedWorkflow, input, WithWorkflowID("gob-queued-wf"), WithQueue(gobTestQueue))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	// Test recovery with gob-only type
	t.Run("GobOnlyRecovery", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryGobOnlyWorkflow, GobOnlyType{real: 5.5, imag: 6.6, tag: "recovered"}, "gob-only-recovery-wf")
	})
}

// TestGobDebounceInsideWorkflow verifies Debounce called from within a workflow
// works with the gob serializer, live and on replay. The bounce step checkpoints
// an internal result type, so the SDK must pre-register it with gob: users cannot
// register internal types themselves.
func TestGobDebounceInsideWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})
	dbosCtx.(*dbosContext).queueRunner.internalQueue.basePollingInterval = 10 * time.Millisecond

	RegisterWorkflow(dbosCtx, debounceTestWorkflow)
	deb, err := NewDebouncer(dbosCtx, debounceTestWorkflow, WithDebouncerTimeout(10*time.Second))
	require.NoError(t, err, "failed to create the debouncer")

	parent := func(ctx Context, in string) (string, error) {
		h, err := deb.Debounce(ctx, "gob-in-wf-key", 100*time.Millisecond, in)
		if err != nil {
			return "", err
		}
		return h.GetResult()
	}
	RegisterWorkflow(dbosCtx, parent, WithWorkflowName("gob-debounce-parent"))
	require.NoError(t, Launch(dbosCtx))

	handle, err := RunWorkflow(dbosCtx, parent, "gob-input")
	require.NoError(t, err, "failed to start the caller workflow")
	result, err := handle.GetResult()
	require.NoError(t, err, "debounce inside a workflow must work with the gob serializer")
	assert.Equal(t, "gob-input", result)

	// Replay the caller: the gob-encoded bounce checkpoint must decode back
	setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowID())
	recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")
	require.Len(t, recoveredHandles, 1, "the caller workflow should have been recovered")
	replayResult, err := recoveredHandles[0].GetResult()
	require.NoError(t, err, "the replayed caller must decode the gob-encoded bounce checkpoint")
	assert.Equal(t, "gob-input", replayResult)
}

// ===== Chicken Serializer Tests =====

// TestClientCustomSerializer tests that a Client created with a custom serializer
// correctly uses it for Enqueue, Send, GetEvent, and ClientReadStream.
func TestClientCustomSerializer(t *testing.T) {
	gob.Register(Chicken{})

	customSer := &chickenSerializer{}

	// Server uses the same custom serializer so it can decode what the client encodes
	serverCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: customSer})

	queue, err := RegisterQueue(serverCtx, "client-ser-queue")
	require.NoError(t, err)

	// Workflow that returns its input — on the server side the deserialized input
	// will be fixedChicken because the chickenSerializer always decodes to that.
	echoWorkflow := func(ctx Context, input Chicken) (Chicken, error) {
		return input, nil
	}
	RegisterWorkflow(serverCtx, echoWorkflow, WithWorkflowName("ClientSerEchoWorkflow"))

	// Workflow that writes to a stream
	streamWorkflow := func(ctx Context, input Chicken) (Chicken, error) {
		if err := WriteStream(ctx, "client-ser-stream", input); err != nil {
			return Chicken{}, err
		}
		if err := CloseStream(ctx, "client-ser-stream"); err != nil {
			return Chicken{}, err
		}
		return input, nil
	}
	RegisterWorkflow(serverCtx, streamWorkflow, WithWorkflowName("ClientSerStreamWorkflow"))

	// Workflow that waits for a message via Recv then returns it
	recvWorkflow := func(ctx Context, _ Chicken) (Chicken, error) {
		msg, err := Recv[Chicken](ctx, "client-topic", 10*time.Second)
		if err != nil {
			return Chicken{}, err
		}
		return msg, nil
	}
	RegisterWorkflow(serverCtx, recvWorkflow, WithWorkflowName("ClientSerRecvWorkflow"))

	// Workflow that sets an event
	setEventWorkflow := func(ctx Context, input Chicken) (Chicken, error) {
		if err := SetEvent(ctx, "client-event-key", input); err != nil {
			return Chicken{}, err
		}
		return input, nil
	}
	RegisterWorkflow(serverCtx, setEventWorkflow, WithWorkflowName("ClientSerSetEventWorkflow"))

	err = Launch(serverCtx)
	require.NoError(t, err)
	defer Shutdown(serverCtx, 10*time.Second)

	// Create client with the same custom serializer
	databaseURL := backendDatabaseURL(t)
	client, err := NewClient(context.Background(), ClientConfig{
		DatabaseURL: databaseURL,
		Serializer:  customSer,
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(client, 30 * time.Second) })

	t.Run("EnqueueWithCustomSerializer", func(t *testing.T) {
		// The chicken serializer always encodes to fixedChicken, so regardless
		// of what we pass in, the server should decode fixedChicken.
		handle, err := Enqueue[Chicken, Chicken](client, queue.GetName(), "ClientSerEchoWorkflow",
			Chicken{Name: "ignored", Noise: "ignored", Legs: 99},
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, fixedChicken, result)
	})

	t.Run("SendWithCustomSerializer", func(t *testing.T) {
		// Enqueue a workflow that waits for a message
		handle, err := Enqueue[Chicken, Chicken](client, queue.GetName(), "ClientSerRecvWorkflow",
			Chicken{},
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err)

		// Send a message via client — the serializer encodes it to fixedChicken
		err = client.Send(client, handle.GetWorkflowID(), Chicken{Name: "ignored"}, "client-topic")
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, fixedChicken, result)
	})

	t.Run("GetEventWithCustomSerializer", func(t *testing.T) {
		// Enqueue a workflow that sets an event
		handle, err := Enqueue[Chicken, Chicken](client, queue.GetName(), "ClientSerSetEventWorkflow",
			fixedChicken,
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err)

		// Wait for the workflow to complete so the event is set
		_, err = handle.GetResult()
		require.NoError(t, err)

		// GetEvent decodes using the serialization recorded with the event,
		// verifying the custom serializer was used for the round-trip.
		event, err := GetEvent[Chicken](client, handle.GetWorkflowID(), "client-event-key", 10*time.Second)
		require.NoError(t, err)
		assert.Equal(t, fixedChicken, event)
	})

	t.Run("ClientReadStreamWithCustomSerializer", func(t *testing.T) {
		// Enqueue a workflow that writes to a stream
		handle, err := Enqueue[Chicken, Chicken](client, queue.GetName(), "ClientSerStreamWorkflow",
			fixedChicken,
			WithEnqueueApplicationVersion(serverCtx.GetApplicationVersion()))
		require.NoError(t, err)

		// Wait for completion
		_, err = handle.GetResult()
		require.NoError(t, err)

		// Read stream via typed client API
		values, closed, err := ReadStream[Chicken](client, handle.GetWorkflowID(), "client-ser-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, fixedChicken, values[0])
	})
}

// Chicken is a whimsical struct used to test fully custom serializers.
type Chicken struct {
	Name  string
	Noise string
	Legs  int
}

// fixedChicken is the canonical chicken that the chickenSerializer always returns.
var fixedChicken = Chicken{Name: "Poulet", Noise: "cotcotcodet", Legs: 2}

// chickenSerializer is a custom serializer that always encodes/decodes to a fixed Chicken value,
// regardless of the actual input. This tests that the custom serializer plumbing is actually used:
// the workflow will always receive fixedChicken as input, no matter what was originally provided.
type chickenSerializer struct{}

func (s *chickenSerializer) Name() string { return "chicken" }

func (s *chickenSerializer) Encode(data any) (*string, error) {
	if isNilValue(data) {
		marker := string(nilMarker)
		return &marker, nil
	}
	// Always encode the fixed chicken, ignoring the actual data
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	chicken := any(fixedChicken)
	if err := enc.Encode(&chicken); err != nil {
		return nil, fmt.Errorf("chicken encode failed: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(buf.Bytes())
	return &encoded, nil
}

func (s *chickenSerializer) Decode(data *string) (any, error) {
	if data == nil || *data == nilMarker {
		return nil, nil
	}
	decodedBytes, err := base64.StdEncoding.DecodeString(*data)
	if err != nil {
		return nil, fmt.Errorf("chicken base64 decode failed: %w", err)
	}
	var result any
	dec := gob.NewDecoder(bytes.NewReader(decodedBytes))
	if err := dec.Decode(&result); err != nil {
		return nil, fmt.Errorf("chicken gob decode failed: %w", err)
	}
	return result, nil
}

// makeStreamWorkflow creates a workflow that writes a value to a stream, then closes it.
func makeStreamWorkflow[T any]() Workflow[T, T] {
	return func(ctx Context, input T) (T, error) {
		if err := WriteStream(ctx, "test-stream", input); err != nil {
			return *new(T), fmt.Errorf("write stream failed: %w", err)
		}
		if err := CloseStream(ctx, "test-stream"); err != nil {
			return *new(T), fmt.Errorf("close stream failed: %w", err)
		}
		return input, nil
	}
}

var (
	chickenRecoveryWorkflow = makeRecoveryWorkflow[Chicken]()
	chickenSenderWorkflow   = makeSenderWorkflow[Chicken]()
	chickenReceiverWorkflow = makeReceiverWorkflow[Chicken]()
	chickenSetEventWorkflow = makeSetEventWorkflow[Chicken]()
	chickenGetEventWorkflow = makeGetEventWorkflow[Chicken]()
	chickenStreamWorkflow   = makeStreamWorkflow[Chicken]()
)

// TestChickenSerializer tests a fully custom user-provided serializer.
// The chicken serializer always encodes/decodes a fixed Chicken value.
// On first execution the workflow receives the original input (in-memory, no serializer roundtrip).
// On recovery, the input is read from DB through the serializer, so it becomes fixedChicken.
// testAllSerializationPaths exercises recovery, proving the custom serializer is used.
func TestChickenSerializer(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: &chickenSerializer{}})

	RegisterWorkflow(executor, chickenRecoveryWorkflow)
	RegisterWorkflow(executor, chickenSenderWorkflow)
	RegisterWorkflow(executor, chickenReceiverWorkflow)
	RegisterWorkflow(executor, chickenSetEventWorkflow)
	RegisterWorkflow(executor, chickenGetEventWorkflow)
	RegisterWorkflow(executor, chickenStreamWorkflow)

	err := Launch(executor)
	require.NoError(t, err)
	defer Shutdown(executor, 10*time.Second)

	// On recovery, the serializer decodes the DB input to fixedChicken.
	// The recovery workflow returns its input, so the result should be fixedChicken.
	t.Run("RecoveryReturnsFixedChicken", func(t *testing.T) {
		testAllSerializationPaths(t, executor, chickenRecoveryWorkflow, fixedChicken, "chicken-wf")
	})

	t.Run("SendRecv", func(t *testing.T) {
		// Send/Recv encode through the serializer, so both sides get fixedChicken
		testSendRecv(t, executor, chickenSenderWorkflow, chickenReceiverWorkflow, fixedChicken, "chicken-sender-wf")
	})

	t.Run("SetGetEvent", func(t *testing.T) {
		testSetGetEvent(t, executor, chickenSetEventWorkflow, chickenGetEventWorkflow, fixedChicken, "chicken-setevent-wf", "chicken-getevent-wf")
	})

	t.Run("WriteReadStream", func(t *testing.T) {
		handle, err := RunWorkflow(executor, chickenStreamWorkflow, fixedChicken, WithWorkflowID("chicken-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, fixedChicken, result)

		// Read the stream values back
		values, closed, err := ReadStream[Chicken](executor, "chicken-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, fixedChicken, values[0])
	})
}

// timeHostileSerializer behaves like the default JSON serializer but refuses to
// encode a time.Time. It models a realistic custom serializer (schema/protobuf
// based, or one with a type allowlist) that has no mapping for time.Time. It is
// used to guard against DBOS routing an internal deadline through the user
// serializer as a time.Time: the special steps (Sleep, and the timeout sleep of
// Recv/GetEvent) must checkpoint the deadline as epoch millis (int64), which any
// serializer can round-trip, rather than a time.Time.
type timeHostileSerializer struct{}

func (timeHostileSerializer) Name() string { return "TIME_HOSTILE" }

func (timeHostileSerializer) Encode(data any) (*string, error) {
	if _, ok := data.(time.Time); ok {
		return nil, fmt.Errorf("timeHostileSerializer refuses to encode time.Time")
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

func (timeHostileSerializer) Decode(data *string) (any, error) {
	if data == nil {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(*data)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	// Return whole numbers as int64 so DBOS's typed decode of the epoch-millis
	// deadline round-trips instead of surfacing a float64.
	if n, ok := v.(json.Number); ok {
		if i, ierr := n.Int64(); ierr == nil {
			return i, nil
		}
		f, ferr := n.Float64()
		if ferr != nil {
			return nil, ferr
		}
		return f, nil
	}
	return v, nil
}

// TestTimeHostileSerializer verifies the special steps whose durable deadline is
// a timestamp (Sleep, and the internal timeout sleep of Recv/GetEvent) encode that deadline as int64.
func TestTimeHostileSerializer(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: timeHostileSerializer{}})

	sleepWorkflow := func(ctx Context, ms int) (time.Duration, error) {
		return Sleep(ctx, time.Duration(ms)*time.Millisecond)
	}
	recvTimeoutWorkflow := func(ctx Context, topic string) (string, error) {
		return Recv[string](ctx, topic, 500*time.Millisecond)
	}
	getEventTimeoutWorkflow := func(ctx Context, targetID string) (string, error) {
		return GetEvent[string](ctx, targetID, "no-such-key", 500*time.Millisecond)
	}
	RegisterWorkflow(dbosCtx, sleepWorkflow)
	RegisterWorkflow(dbosCtx, recvTimeoutWorkflow)
	RegisterWorkflow(dbosCtx, getEventTimeoutWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err)

	t.Run("Sleep", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, sleepWorkflow, 100)
		require.NoError(t, err, "failed to start sleep workflow")
		slept, err := handle.GetResult()
		require.NoError(t, err, "Sleep must succeed: the deadline is stored as epoch millis, not a time.Time the serializer rejects")
		require.LessOrEqual(t, slept, 100*time.Millisecond)

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, "DBOS.sleep", steps[0].StepName)
		require.Nil(t, steps[0].Error, "the sleep deadline must checkpoint cleanly")
	})

	t.Run("RecvTimeout", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, recvTimeoutWorkflow, "time-hostile-topic")
		require.NoError(t, err, "failed to start recv workflow")
		_, err = handle.GetResult()
		require.Error(t, err, "expected a timeout")
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected *Error, got %T (a serialization failure here would mean the deadline was routed through the serializer as a time.Time)", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout, not a serialization error")
	})

	t.Run("GetEventTimeout", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, getEventTimeoutWorkflow, "time-hostile-nonexistent-target")
		require.NoError(t, err, "failed to start getEvent workflow")
		_, err = handle.GetResult()
		require.Error(t, err, "expected a timeout")
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected *Error, got %T", err)
		require.Equal(t, ErrorCodeTimeout, dbosErr.Code, "expected ErrorCodeTimeout, not a serialization error")
	})
}

// TestPortableInterop tests cross-language interoperability using the portable JSON format.
// It simulates another language inserting a workflow into the DB with portable_json serialization,
// and verifies that Go can recover and execute it correctly.
func TestPortableInterop(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// InteropInput exercises the full portable JSON value space:
	// strings, integers, floats, booleans, nulls, arrays, nested objects, RFC3339 timestamps
	type NestedObj struct {
		Deep bool `json:"deep"`
	}
	type MapArg struct {
		Key1   string    `json:"key1"`
		Key2   int       `json:"key2"`
		Nested NestedObj `json:"nested"`
	}

	// The workflow accepts 7 positional args matching the golden JSON below.
	// Go workflows take a single input param, so we use a struct that mirrors the positional args.
	type InteropArgs struct {
		Str       string   `json:"str"`
		Num       int      `json:"num"`
		Timestamp string   `json:"timestamp"`
		Arr       []string `json:"arr"`
		Obj       MapArg   `json:"obj"`
		Flag      bool     `json:"flag"`
		Nullable  *string  `json:"nullable"`
	}

	expectedArgs := InteropArgs{
		Str:       "hello-interop",
		Num:       42,
		Timestamp: "2025-06-15T10:30:00.000Z",
		Arr:       []string{"alpha", "beta", "gamma"},
		Obj:       MapArg{Key1: "value1", Key2: 99, Nested: NestedObj{Deep: true}},
		Flag:      true,
		Nullable:  nil,
	}

	// Golden JSON matching what Python/TS would produce, including namedArgs (ignored by Go)
	goldenInputsJSON := `{"positionalArgs":[{"str":"hello-interop","num":42,"timestamp":"2025-06-15T10:30:00.000Z","arr":["alpha","beta","gamma"],"obj":{"key1":"value1","key2":99,"nested":{"deep":true}},"flag":true,"nullable":null}],"namedArgs":{"unused_kwarg":"should_be_ignored","another":123}}`

	// InteropResult captures intermediate results to prove each encode/decode path works.
	type InteropResult struct {
		Input        InteropArgs `json:"input"`
		StepOutput   InteropArgs `json:"stepOutput"`
		RecvOutput   InteropArgs `json:"recvOutput"`
		EventOutput  InteropArgs `json:"eventOutput"`
		StreamOutput InteropArgs `json:"streamOutput"`
	}

	// A single workflow that exercises all serialization paths:
	// step output, send/recv, set_event/get_event, and write_stream/read_stream.
	portableWf := func(ctx Context, input InteropArgs) (InteropResult, error) {
		// 1. Step: encode/decode step output
		stepOut, err := RunAsStep(ctx, func(_ context.Context) (InteropArgs, error) {
			return input, nil
		})
		if err != nil {
			return InteropResult{}, fmt.Errorf("step failed: %w", err)
		}

		// 2. Send to self, then Recv
		wfID, err := GetWorkflowID(ctx)
		if err != nil {
			return InteropResult{}, err
		}
		if err := Send(ctx, wfID, input, "test-topic"); err != nil {
			return InteropResult{}, fmt.Errorf("send failed: %w", err)
		}
		recvOut, err := Recv[InteropArgs](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return InteropResult{}, fmt.Errorf("recv failed: %w", err)
		}

		// 3. SetEvent then GetEvent (from own workflow)
		if err := SetEvent(ctx, "test-key", input); err != nil {
			return InteropResult{}, fmt.Errorf("set event failed: %w", err)
		}
		eventOut, err := GetEvent[InteropArgs](ctx, wfID, "test-key", 10*time.Second)
		if err != nil {
			return InteropResult{}, fmt.Errorf("get event failed: %w", err)
		}

		// 4. Stream: write, close, then read back
		if err := WriteStream(ctx, "test-stream", input); err != nil {
			return InteropResult{}, fmt.Errorf("write stream failed: %w", err)
		}
		if err := CloseStream(ctx, "test-stream"); err != nil {
			return InteropResult{}, fmt.Errorf("close stream failed: %w", err)
		}
		streamValues, closed, err := ReadStream[InteropArgs](ctx, wfID, "test-stream")
		if err != nil {
			return InteropResult{}, fmt.Errorf("read stream failed: %w", err)
		}
		if !closed {
			return InteropResult{}, fmt.Errorf("expected stream to be closed")
		}
		if len(streamValues) != 1 {
			return InteropResult{}, fmt.Errorf("expected 1 stream value, got %d", len(streamValues))
		}

		return InteropResult{
			Input:        input,
			StepOutput:   stepOut,
			RecvOutput:   recvOut,
			EventOutput:  eventOut,
			StreamOutput: streamValues[0],
		}, nil
	}
	RegisterWorkflow(executor, portableWf, WithWorkflowName("interop_workflow"))
	_, err := RegisterQueue(executor, "portable-interop-queue")
	require.NoError(t, err)

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	// Helper to insert a portable workflow directly into the DB (simulating another language).
	insertPortableWorkflow := func(t *testing.T, workflowID, status string, queueName *string) {
		t.Helper()
		c := executor.(*dbosContext)
		sysDB := c.systemDB.(*sysdb.SysDB)
		insertQuery := sysDB.RenderSQL(`INSERT INTO %sworkflow_status (
			workflow_uuid, status, name, inputs, serialization, queue_name,
			created_at, updated_at, recovery_attempts, executor_id, priority,
			application_version, application_id, authenticated_user, assumed_role, authenticated_roles
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		now := time.Now().UnixMilli()
		_, err := sysDB.Pool().Exec(context.Background(), insertQuery,
			workflowID, status, "interop_workflow", goldenInputsJSON, PortableSerializerName, queueName,
			now, now, 0, "local", 0, c.applicationVersion, "", "", "", "[]")
		require.NoError(t, err)
	}

	// verifyResult checks all intermediate results match expectedArgs.
	verifyResult := func(t *testing.T, result InteropResult) {
		t.Helper()
		assert.Equal(t, expectedArgs, result.Input, "workflow input")
		assert.Equal(t, expectedArgs, result.StepOutput, "step output")
		assert.Equal(t, expectedArgs, result.RecvOutput, "recv output")
		assert.Equal(t, expectedArgs, result.EventOutput, "event output")
		assert.Equal(t, expectedArgs, result.StreamOutput, "stream output")
	}

	// 1. Recovery path: direct DB insert with status=PENDING, then recover.
	t.Run("DirectDBInsertRecovery", func(t *testing.T) {
		workflowID := "interop-recovery-" + t.Name()
		insertPortableWorkflow(t, workflowID, string(WorkflowStatusPending), nil)

		c := executor.(*dbosContext)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)

		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowID)
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)

		// Verify ListWorkflows returns portable inputs/outputs correctly
		wfs, err := ListWorkflows(executor,
			WithFilterWorkflowIDs(workflowID),
			WithFilterLoadInput(true), WithFilterLoadOutput(true))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		wf := wfs[0]
		assert.Equal(t, PortableSerializerName, wf.Serialization)

		require.NotNil(t, wf.Input)
		inputMap, ok := wf.Input.(map[string]any)
		require.True(t, ok, "expected decoded map for portable input, got %T", wf.Input)
		posArgs, ok := inputMap["positionalArgs"].([]any)
		require.True(t, ok, "positionalArgs should decode as a JSON array, got %T", inputMap["positionalArgs"])
		assert.Len(t, posArgs, 1)

		require.NotNil(t, wf.Output)
		outputMap, ok := wf.Output.(map[string]any)
		require.True(t, ok, "expected decoded map for portable output, got %T", wf.Output)
		assert.Contains(t, outputMap, "input")
		assert.Contains(t, outputMap, "stepOutput")
		assert.Contains(t, outputMap, "recvOutput")
		assert.Contains(t, outputMap, "eventOutput")
	})

	// 2. Queue path: direct DB insert with status=ENQUEUED + queue_name, let the queue runner dequeue.
	t.Run("DirectDBInsertQueue", func(t *testing.T) {
		workflowID := "interop-queue-" + t.Name()
		queueName := "portable-interop-queue"
		insertPortableWorkflow(t, workflowID, string(WorkflowStatusEnqueued), &queueName)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowID)
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)
	})

	// 3. Client enqueue path: Go client with PortableWorkflowArgs.
	t.Run("ClientEnqueuePortable", func(t *testing.T) {
		client, err := NewClient(context.Background(), ClientConfig{
			DatabaseURL: executor.(*dbosContext).config.DatabaseURL,
		})
		require.NoError(t, err)
		t.Cleanup(func() { client.Shutdown(client, 5 * time.Second) })

		portableArgs := PortableWorkflowArgs{
			PositionalArgs: []any{expectedArgs, "extra-positional", 99},
			NamedArgs:      map[string]any{"lang": "python", "debug": true},
		}
		handle, err := Enqueue[InteropResult, PortableWorkflowArgs](client, "portable-interop-queue", "interop_workflow", portableArgs)
		require.NoError(t, err)
		require.NotEmpty(t, handle.GetWorkflowID())

		// Verify the DB has portable_json serialization and the correct envelope
		c := executor.(*dbosContext)
		sysDB := c.systemDB.(*sysdb.SysDB)
		var storedInputs, storedSerialization string
		selectQuery := sysDB.RenderSQL(`SELECT inputs, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		err = sysDB.Pool().QueryRow(context.Background(), selectQuery, handle.GetWorkflowID()).Scan(&storedInputs, &storedSerialization)
		require.NoError(t, err)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		// Verify envelope format — extra positional/named args are preserved in the DB
		var envelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 3)
		assert.Len(t, envelope.NamedArgs, 2)

		// Wait for execution and verify all paths
		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, handle.GetWorkflowID())
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)
	})

	// 4. Wrong-type input: enqueue with a string where InteropArgs is expected.
	// Go's type system catches this during deserialization; the workflow should fail.
	t.Run("WrongTypeInput", func(t *testing.T) {
		workflowID := "interop-wrongtype-" + t.Name()
		queueName := "portable-interop-queue"
		badInputsJSON := `{"positionalArgs":["not-an-object"],"namedArgs":{}}`

		c := executor.(*dbosContext)
		sysDB := c.systemDB.(*sysdb.SysDB)
		insertQuery := sysDB.RenderSQL(`INSERT INTO %sworkflow_status (
			workflow_uuid, status, name, inputs, serialization, queue_name,
			created_at, updated_at, recovery_attempts, executor_id, priority,
			application_version, application_id, authenticated_user, assumed_role, authenticated_roles
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		now := time.Now().UnixMilli()
		_, err := sysDB.Pool().Exec(context.Background(), insertQuery,
			workflowID, string(WorkflowStatusEnqueued), "interop_workflow", badInputsJSON, PortableSerializerName, &queueName,
			now, now, 0, "local", 0, c.applicationVersion, "", "", "", "[]")
		require.NoError(t, err)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowID)
		require.NoError(t, err)
		_, err = retrievedHandle.GetResult()
		require.Error(t, err)
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		// A DBOS *Error now projects its symbolic code into the portable envelope's
		// Name (instead of the generic "Portable Error"), so errors.Is still matches.
		assert.Equal(t, ErrorCodeWorkflowExecution.String(), pe.Name)
		assert.Contains(t, err.Error(), fmt.Sprintf("DBOS Error %s", ErrorCodeWorkflowExecution))
	})
}

// TestPortablePerOperationOptions verifies that WithPortableSend, WithPortableSetEvent, and
// WithPortableWriteStream force portable_json serialization for individual operations,
// even when the calling workflow uses the default serializer.
func TestPortablePerOperationOptions(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	type Payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	payload := Payload{Name: "portable-op", Count: 7}

	c := executor.(*dbosContext)
	sysDB := c.systemDB.(*sysdb.SysDB)

	// Helper: fetch the serialization recorded in operation_outputs for the Recv step of a workflow.
	// The Recv step stores the serialization of the message it consumed, which reflects what the sender used.
	recvStepSerialization := func(t *testing.T, workflowID string) string {
		t.Helper()
		var ser string
		q := sysDB.RenderSQL(`SELECT serialization FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_name = 'DBOS.recv' ORDER BY function_id ASC LIMIT 1`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.NoError(t, sysDB.Pool().QueryRow(context.Background(), q, workflowID).Scan(&ser))
		return ser
	}

	// Helper: fetch the serialization column for a workflow event.
	eventSerialization := func(t *testing.T, workflowID, key string) string {
		t.Helper()
		var ser string
		q := sysDB.RenderSQL(`SELECT serialization FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.NoError(t, sysDB.Pool().QueryRow(context.Background(), q, workflowID, key).Scan(&ser))
		return ser
	}

	// Helper: fetch the serialization column for the first stream entry (non-sentinel).
	streamSerialization := func(t *testing.T, workflowID, key string) string {
		t.Helper()
		var ser string
		q := sysDB.RenderSQL(`SELECT serialization FROM %sstreams WHERE workflow_uuid = $1 AND key = $2 AND value != $3 ORDER BY "offset" LIMIT 1`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.NoError(t, sysDB.Pool().QueryRow(context.Background(), q, workflowID, key, sysdb.StreamClosedSentinel).Scan(&ser))
		return ser
	}

	// Workflows must be registered before Launch.
	var (
		portableSendSenderWf   Workflow[string, string]
		portableSendReceiverWf Workflow[string, Payload]
		portableSetterWf       Workflow[string, string]
		portableGetterWf       Workflow[string, Payload]
		portableWriterWf       Workflow[string, string]
	)

	portableSendSenderWf = func(ctx Context, receiverID string) (string, error) {
		return "", Send(ctx, receiverID, payload, "topic", WithPortableSend())
	}
	portableSendReceiverWf = func(ctx Context, _ string) (Payload, error) {
		return Recv[Payload](ctx, "topic", 10*time.Second)
	}
	portableSetterWf = func(ctx Context, _ string) (string, error) {
		return "", SetEvent(ctx, "evt-key", payload, WithPortableSetEvent())
	}
	portableGetterWf = func(ctx Context, targetID string) (Payload, error) {
		return GetEvent[Payload](ctx, targetID, "evt-key", 10*time.Second)
	}
	portableWriterWf = func(ctx Context, _ string) (string, error) {
		if err := WriteStream(ctx, "stream-key", payload, WithPortableWriteStream()); err != nil {
			return "", err
		}
		return "", CloseStream(ctx, "stream-key")
	}

	RegisterWorkflow(executor, portableSendSenderWf, WithWorkflowName("portable-op-send-sender"))
	RegisterWorkflow(executor, portableSendReceiverWf, WithWorkflowName("portable-op-send-receiver"))
	RegisterWorkflow(executor, portableSetterWf, WithWorkflowName("portable-op-setter"))
	RegisterWorkflow(executor, portableGetterWf, WithWorkflowName("portable-op-getter"))
	RegisterWorkflow(executor, portableWriterWf, WithWorkflowName("portable-op-writer"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	// WithPortableSend: a standard workflow sends with portable serialization; a standard
	// workflow Recvs it and gets the correct value back. The serialization recorded in the
	// receiver's Recv step output reflects what the sender used.
	t.Run("WithPortableSend", func(t *testing.T) {
		receiverID := "portable-op-send-receiver-" + t.Name()
		senderID := "portable-op-send-sender-" + t.Name()

		receiverHandle, err := RunWorkflow(executor, portableSendReceiverWf, "", WithWorkflowID(receiverID))
		require.NoError(t, err)

		_, err = RunWorkflow(executor, portableSendSenderWf, receiverID, WithWorkflowID(senderID))
		require.NoError(t, err)

		result, err := receiverHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, payload, result)

		// The Recv step records the serialization of the consumed message in operation_outputs.
		assert.Equal(t, PortableSerializerName, recvStepSerialization(t, receiverID))
	})

	// WithPortableSetEvent: a standard workflow sets an event with portable serialization;
	// a standard GetEvent reads it back correctly.
	t.Run("WithPortableSetEvent", func(t *testing.T) {
		setterID := "portable-op-setter-" + t.Name()
		getterID := "portable-op-getter-" + t.Name()

		setHandle, err := RunWorkflow(executor, portableSetterWf, "", WithWorkflowID(setterID))
		require.NoError(t, err)
		_, err = setHandle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, PortableSerializerName, eventSerialization(t, setterID, "evt-key"))

		getHandle, err := RunWorkflow(executor, portableGetterWf, setterID, WithWorkflowID(getterID))
		require.NoError(t, err)
		result, err := getHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, payload, result)
	})

	// WithPortableWriteStream: a standard workflow writes with portable serialization;
	// ReadStream reads it back correctly.
	t.Run("WithPortableWriteStream", func(t *testing.T) {
		wfID := "portable-op-writer-" + t.Name()

		handle, err := RunWorkflow(executor, portableWriterWf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, PortableSerializerName, streamSerialization(t, wfID, "stream-key"))

		values, closed, err := ReadStream[Payload](executor, wfID, "stream-key")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, payload, values[0])
	})
}

// TestDirectRunPortableWorkflow tests starting a workflow in portable mode via RunWorkflow,
// verifying the DB contains the correct portable JSON envelope, then recovering it.
func TestDirectRunPortableWorkflow(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	type InteropInput struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	expectedInput := InteropInput{Name: "direct-portable", Value: 99}

	// Simple workflow that returns its input through a step (exercises encode/decode).
	portableEchoWf := func(ctx Context, input InteropInput) (InteropInput, error) {
		stepOut, err := RunAsStep(ctx, func(_ context.Context) (InteropInput, error) {
			return input, nil
		})
		if err != nil {
			return InteropInput{}, err
		}
		return stepOut, nil
	}
	RegisterWorkflow(executor, portableEchoWf, WithWorkflowName("portable_echo"))

	// Workflow that accepts the full PortableWorkflowArgs envelope directly.
	portableEnvelopeWf := func(ctx Context, input PortableWorkflowArgs) (PortableWorkflowArgs, error) {
		stepOut, err := RunAsStep(ctx, func(_ context.Context) (PortableWorkflowArgs, error) {
			return input, nil
		})
		if err != nil {
			return PortableWorkflowArgs{}, err
		}
		return stepOut, nil
	}
	RegisterWorkflow(executor, portableEnvelopeWf, WithWorkflowName("portable_envelope"))

	// Workflows for primitive input tests (int, string).
	portableIntEchoWf := func(ctx Context, input int) (int, error) {
		return RunAsStep(ctx, func(_ context.Context) (int, error) { return input, nil })
	}
	RegisterWorkflow(executor, portableIntEchoWf, WithWorkflowName("portable_int_echo"))

	portableStringEchoWf := func(ctx Context, input string) (string, error) {
		return RunAsStep(ctx, func(_ context.Context) (string, error) { return input, nil })
	}
	RegisterWorkflow(executor, portableStringEchoWf, WithWorkflowName("portable_string_echo"))

	// Multi-step workflow for partial recovery test (must register before Launch).
	type PartialRecoveryResult struct {
		StepOut  InteropInput `json:"stepOut"`
		RecvOut  InteropInput `json:"recvOut"`
		EventOut InteropInput `json:"eventOut"`
	}
	multiStepWf := func(ctx Context, input InteropInput) (PartialRecoveryResult, error) {
		stepOut, err := RunAsStep(ctx, func(_ context.Context) (InteropInput, error) {
			return input, nil
		})
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("step: %w", err)
		}

		wfID, err := GetWorkflowID(ctx)
		if err != nil {
			return PartialRecoveryResult{}, err
		}
		if err := Send(ctx, wfID, input, "partial-topic"); err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("send: %w", err)
		}
		recvOut, err := Recv[InteropInput](ctx, "partial-topic", 10*time.Second)
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("recv: %w", err)
		}

		if err := SetEvent(ctx, "partial-key", input); err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("setEvent: %w", err)
		}
		eventOut, err := GetEvent[InteropInput](ctx, wfID, "partial-key", 10*time.Second)
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("getEvent: %w", err)
		}

		return PartialRecoveryResult{StepOut: stepOut, RecvOut: recvOut, EventOut: eventOut}, nil
	}
	RegisterWorkflow(executor, multiStepWf, WithWorkflowName("partial_recovery_wf"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	c := executor.(*dbosContext)
	sysDB := c.systemDB.(*sysdb.SysDB)

	// Helper: read the stored inputs and serialization from the DB.
	readStoredInputs := func(t *testing.T, workflowID string) (string, string) {
		t.Helper()
		var storedInputs, storedSerialization string
		q := sysDB.RenderSQL(`SELECT inputs, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		err := sysDB.Pool().QueryRow(context.Background(), q, workflowID).Scan(&storedInputs, &storedSerialization)
		require.NoError(t, err)
		return storedInputs, storedSerialization
	}

	// Helper: flip a completed workflow back to PENDING for recovery.
	resetToPending := func(t *testing.T, workflowID string) {
		t.Helper()
		schemaPrefix := sysDB.Dialect().SchemaPrefix(sysDB.Schema())
		q := sysDB.RenderSQL(`UPDATE %sworkflow_status SET status = $1, output = NULL, error = NULL WHERE workflow_uuid = $2`, schemaPrefix)
		_, err := sysDB.Pool().Exec(context.Background(), q, string(WorkflowStatusPending), workflowID)
		require.NoError(t, err)
		// Also clear operation outputs so the workflow re-executes its steps.
		dq := sysDB.RenderSQL(`DELETE FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix)
		_, err = sysDB.Pool().Exec(context.Background(), dq, workflowID)
		require.NoError(t, err)
	}

	// 1. Normal struct input → WithPortableWorkflow → run, verify DB envelope, recover.
	t.Run("NormalInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-normal-" + t.Name()
		handle, err := RunWorkflow(executor, portableEchoWf, expectedInput,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, result)

		// Verify the DB has portable_json serialization with the correct envelope.
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		// The first positional arg should unmarshal to expectedInput.
		var decoded InteropInput
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, expectedInput, decoded)

		// Reset workflow to PENDING and recover — should re-execute and produce the same result.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[InteropInput](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, recoveredResult)
	})

	// 2. PortableWorkflowArgs envelope input → WithPortableWorkflow → run, verify, recover.
	t.Run("EnvelopeInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-envelope-" + t.Name()
		envelopeInput := PortableWorkflowArgs{
			PositionalArgs: []any{expectedInput, "extra", 42},
			NamedArgs:      map[string]any{"lang": "go", "debug": false},
		}
		handle, err := RunWorkflow(executor, portableEnvelopeWf, envelopeInput,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		// The workflow receives and returns the full envelope.
		assert.Len(t, result.PositionalArgs, 3)
		assert.Len(t, result.NamedArgs, 2)

		// Verify DB: the stored inputs should be the envelope itself (not double-wrapped).
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var storedEnvelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &storedEnvelope))
		assert.Len(t, storedEnvelope.PositionalArgs, 3, "envelope should not be double-wrapped")
		assert.Len(t, storedEnvelope.NamedArgs, 2)

		// Reset and recover.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[PortableWorkflowArgs](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Len(t, recoveredResult.PositionalArgs, 3)
		assert.Len(t, recoveredResult.NamedArgs, 2)
	})

	// 3. Primitive int input → WithPortableWorkflow → run, verify DB envelope, recover.
	t.Run("PrimitiveIntInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-int-" + t.Name()
		handle, err := RunWorkflow(executor, portableIntEchoWf, 42,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 42, result)

		// Verify DB envelope wraps the primitive as a single positional arg.
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		var decoded int
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, 42, decoded)

		// Recover.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[int](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 42, recoveredResult)
	})

	// 4. Primitive string input → WithPortableWorkflow → run, verify DB envelope, recover.
	t.Run("PrimitiveStringInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-str-" + t.Name()
		handle, err := RunWorkflow(executor, portableStringEchoWf, "hello-portable",
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "hello-portable", result)

		// Verify DB envelope wraps the string as a single positional arg.
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		var decoded string
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, "hello-portable", decoded)

		// Recover.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[string](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "hello-portable", recoveredResult)
	})

	// 5. Partial recovery: run a multi-step portable workflow, keep operation_outputs,
	// reset to PENDING. On recovery every step is replayed from stored results using
	// the serialization column in operation_outputs — NOT re-executed.
	t.Run("PartialRecoveryFromStoredSteps", func(t *testing.T) {
		workflowID := "partial-recovery-" + t.Name()
		handle, err := RunWorkflow(executor, multiStepWf, expectedInput,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)
		firstResult, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, firstResult.StepOut)
		assert.Equal(t, expectedInput, firstResult.RecvOut)
		assert.Equal(t, expectedInput, firstResult.EventOut)

		// Verify operation_outputs exist for this workflow.
		var stepCount int
		schemaPrefix := sysDB.Dialect().SchemaPrefix(sysDB.Schema())
		countQ := sysDB.RenderSQL(`SELECT count(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix)
		require.NoError(t, sysDB.Pool().QueryRow(context.Background(), countQ, workflowID).Scan(&stepCount))
		require.Greater(t, stepCount, 0, "expected operation_outputs rows from first execution")

		// Reset to PENDING but KEEP operation_outputs — steps will be replayed from DB.
		resetQ := sysDB.RenderSQL(`UPDATE %sworkflow_status SET status = $1, output = NULL, error = NULL WHERE workflow_uuid = $2`, schemaPrefix)
		_, err = sysDB.Pool().Exec(context.Background(), resetQ, string(WorkflowStatusPending), workflowID)
		require.NoError(t, err)

		// Recover — each step hits checkOperationExecution and decodes from stored serialization.
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[PartialRecoveryResult](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, recoveredResult.StepOut, "step replayed from DB")
		assert.Equal(t, expectedInput, recoveredResult.RecvOut, "recv replayed from DB")
		assert.Equal(t, expectedInput, recoveredResult.EventOut, "getEvent replayed from DB")
	})
}

func TestPortableWorkflowError(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Workflow that runs a step then raises a PortableWorkflowError with all fields set.
	portableErrWf := func(ctx Context, input string) (string, error) {
		_, err := RunAsStep(ctx, func(_ context.Context) (string, error) {
			return input, nil
		})
		if err != nil {
			return "", err
		}
		return "", &PortableWorkflowError{
			Name:    "ValidationError",
			Message: "invalid input: " + input,
			Code:    400,
			Data:    map[string]any{"field": "input"},
		}
	}
	RegisterWorkflow(executor, portableErrWf, WithWorkflowName("portable_err_wf"))

	// Workflow that runs a step that itself fails with a PortableWorkflowError.
	portableStepErrWf := func(ctx Context, input string) (string, error) {
		return RunAsStep(ctx, func(_ context.Context) (string, error) {
			return "", &PortableWorkflowError{
				Name:    "StepError",
				Message: "step failed: " + input,
				Code:    500,
			}
		})
	}
	RegisterWorkflow(executor, portableStepErrWf, WithWorkflowName("portable_step_err_wf"))

	// Workflow that runs a step then raises a plain Go error (triggers best-effort conversion).
	plainErrWf := func(ctx Context, input string) (string, error) {
		_, err := RunAsStep(ctx, func(_ context.Context) (string, error) {
			return input, nil
		})
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("something went wrong: %s", input)
	}
	RegisterWorkflow(executor, plainErrWf, WithWorkflowName("plain_err_portable_wf"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	c := executor.(*dbosContext)
	sysDB := c.systemDB.(*sysdb.SysDB)

	readStoredError := func(t *testing.T, workflowID string) string {
		t.Helper()
		var storedError *string
		q := sysDB.RenderSQL(`SELECT error FROM %sworkflow_status WHERE workflow_uuid = $1`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.NoError(t, sysDB.Pool().QueryRow(context.Background(), q, workflowID).Scan(&storedError))
		require.NotNil(t, storedError)
		return *storedError
	}

	readStoredStepError := func(t *testing.T, workflowID string, stepID int) string {
		t.Helper()
		var storedError *string
		q := sysDB.RenderSQL(`SELECT error FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
			sysDB.Dialect().SchemaPrefix(sysDB.Schema()))
		require.NoError(t, sysDB.Pool().QueryRow(context.Background(), q, workflowID, stepID).Scan(&storedError))
		require.NotNil(t, storedError)
		return *storedError
	}

	t.Run("PortableWorkflowErrorFields", func(t *testing.T) {
		wfID := "portable-err-fields"
		handle, err := RunWorkflow(executor, portableErrWf, "test-value",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// Direct handle returns the raw *PortableWorkflowError from the goroutine.
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "ValidationError", pe.Name)
		assert.Equal(t, "invalid input: test-value", pe.Message)

		// Stored in DB as portable JSON.
		var errData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfID)), &errData))
		assert.Equal(t, "ValidationError", errData["name"])
		assert.Equal(t, "invalid input: test-value", errData["message"])
		assert.Equal(t, float64(400), errData["code"])

		// RetrieveWorkflow goes through DB deserialization — returns *PortableWorkflowError.
		retrieved, err := RetrieveWorkflow[string](executor, wfID)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		var dbPe *PortableWorkflowError
		require.ErrorAs(t, err, &dbPe)
		assert.Equal(t, "ValidationError", dbPe.Name)
		assert.Equal(t, "invalid input: test-value", dbPe.Message)
		assert.Equal(t, float64(400), dbPe.Code) // JSON numbers unmarshal to float64
		dbData, ok := dbPe.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "input", dbData["field"])
	})

	t.Run("PlainErrorBestEffortConversion", func(t *testing.T) {
		wfID := "portable-err-plain"
		handle, err := RunWorkflow(executor, plainErrWf, "oops",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// Stored in DB as portable JSON with best-effort name and message.
		var errData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfID)), &errData))
		assert.Equal(t, "something went wrong: oops", errData["message"])
		assert.Equal(t, "Portable Error", errData["name"])

		// The stored JSON deserializes directly into *PortableWorkflowError.
		var storedPe PortableWorkflowError
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfID)), &storedPe))
		assert.Equal(t, "something went wrong: oops", storedPe.Message)
		assert.Equal(t, "Portable Error", storedPe.Name)

		// RetrieveWorkflow deserializes the portable JSON back to *PortableWorkflowError.
		retrieved, err := RetrieveWorkflow[string](executor, wfID)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "something went wrong: oops", pe.Message)
		assert.Equal(t, "Portable Error", pe.Name)
	})

	t.Run("StepPortableWorkflowError", func(t *testing.T) {
		wfID := "portable-step-err"
		handle, err := RunWorkflow(executor, portableStepErrWf, "step-input",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// Step error stored as portable JSON in operation_outputs (step 0).
		var stepErrData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredStepError(t, wfID, 0)), &stepErrData))
		assert.Equal(t, "StepError", stepErrData["name"])
		assert.Equal(t, "step failed: step-input", stepErrData["message"])
		assert.Equal(t, float64(500), stepErrData["code"])

		// GetWorkflowSteps deserializes the step error as *PortableWorkflowError.
		steps, err := GetWorkflowSteps(executor, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		var stepPe *PortableWorkflowError
		require.ErrorAs(t, steps[0].Error, &stepPe)
		assert.Equal(t, "StepError", stepPe.Name)
		assert.Equal(t, "step failed: step-input", stepPe.Message)
		assert.Equal(t, float64(500), stepPe.Code)
	})

	t.Run("ListWorkflowsAndGetWorkflowSteps", func(t *testing.T) {
		wfID := "portable-list-wf"
		handle, err := RunWorkflow(executor, portableErrWf, "list-test",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// ListWorkflows: error goes through errors.New → .Error() → deserializeWorkflowError.
		wfs, err := ListWorkflows(executor, WithFilterWorkflowIDs(wfID))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		var listPe *PortableWorkflowError
		require.ErrorAs(t, wfs[0].Error, &listPe)
		assert.Equal(t, "ValidationError", listPe.Name)
		assert.Equal(t, "invalid input: list-test", listPe.Message)
		assert.Equal(t, float64(400), listPe.Code)

		// GetWorkflowSteps: first step succeeds (just echoes input), workflow error is separate.
		steps, err := GetWorkflowSteps(executor, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		assert.Nil(t, steps[0].Error) // step succeeded; error is on the workflow, not the step
		// Portable step output decodes into its JSON value.
		require.NotNil(t, steps[0].Output)
		assert.Equal(t, "list-test", steps[0].Output)
	})
}

// TestWorkflowErrorSerializationRoundTrip covers the pure serialize/deserialize logic:
// Go <-> Go errors are gob-encoded (preserving the concrete type, e.g. *Error),
// portable workflows use the cross-language JSON envelope, and decode is self-describing.
func TestWorkflowErrorSerializationRoundTrip(t *testing.T) {
	t.Run("ErrorPreservedGoToGo", func(t *testing.T) {
		orig := models.NewQueueDeduplicatedError("wf-1", "q-1", "dedup-1")
		s := serializeWorkflowError(nil, orig, "DBOS_JSON")

		got := deserializeWorkflowError(&s)
		var de *Error
		require.ErrorAs(t, got, &de)
		assert.Equal(t, ErrorCodeQueueDeduplicated, de.Code)
		assert.Equal(t, "wf-1", de.WorkflowID)
		assert.Equal(t, "q-1", de.QueueName)
		assert.Equal(t, "dedup-1", de.DeduplicationID)
		assert.Equal(t, orig.Message, de.Message)
		assert.Equal(t, orig.Error(), got.Error())
		require.ErrorIs(t, got, ErrQueueDeduplicated)
	})

	t.Run("GobWireNamePinned", func(t *testing.T) {
		// Stored errors reference the registered gob name; it must stay
		// "*dbos.DBOSError" (see the RegisterName in serialization.go) or
		// errors persisted by earlier versions become undecodable.
		s := serializeWorkflowError(nil, models.NewQueueDeduplicatedError("wf-1", "q-1", "dedup-1"), "DBOS_JSON")
		raw, err := base64.StdEncoding.DecodeString(s)
		require.NoError(t, err)
		require.Contains(t, string(raw), "*dbos.DBOSError")
	})

	t.Run("PlainErrorGoToGo", func(t *testing.T) {
		// errors.New/fmt.Errorf types are not gob-encodable → plain-string fallback.
		s := serializeWorkflowError(nil, fmt.Errorf("boom"), "DBOS_JSON")
		got := deserializeWorkflowError(&s)
		require.Error(t, got)
		assert.Equal(t, "boom", got.Error())
		var de *Error
		assert.NotErrorAs(t, got, &de)
	})

	t.Run("DBOSErrorPreservedPortable", func(t *testing.T) {
		// Portable (cross-language JSON) is the lossy path: the concrete *Error type
		// is not carried, but its code and context are projected into the envelope so
		// errors.Is against a DBOS sentinel still matches after the round trip.
		orig := models.NewMaxStepRetriesExceededError("wf-7", "my-step", 3, fmt.Errorf("underlying"))
		s := serializeWorkflowError(nil, orig, PortableSerializerName)

		// Wire format is the JSON envelope, not gob.
		var envelope PortableWorkflowError
		require.NoError(t, json.Unmarshal([]byte(s), &envelope))
		assert.Equal(t, ErrorCodeMaxStepRetriesExceeded.String(), envelope.Name)
		assert.EqualValues(t, int(ErrorCodeMaxStepRetriesExceeded), envelope.Code)
		data, ok := envelope.Data.(map[string]any)
		require.True(t, ok, "context fields must be captured in Data")
		assert.Equal(t, "wf-7", data["workflowID"])
		assert.Equal(t, "my-step", data["stepName"])
		assert.EqualValues(t, 3, data["maxRetries"])

		// Decode yields a *PortableWorkflowError (not *Error), but errors.Is still matches.
		got := deserializeWorkflowError(&s)
		require.Error(t, got)
		var pe *PortableWorkflowError
		require.ErrorAs(t, got, &pe)
		assert.Equal(t, orig.Error(), got.Error())
		require.ErrorIs(t, got, ErrMaxStepRetriesExceeded)

		// It matches only its own code, not an unrelated DBOS sentinel.
		assert.False(t, errors.Is(got, ErrAwaitedWorkflowCancelled))
	})

	t.Run("PlainErrorPortableDoesNotMatchSentinel", func(t *testing.T) {
		// A non-DBOS error keeps the generic name and matches no DBOS sentinel.
		s := serializeWorkflowError(nil, fmt.Errorf("boom"), PortableSerializerName)
		got := deserializeWorkflowError(&s)
		require.Error(t, got)
		var pe *PortableWorkflowError
		require.ErrorAs(t, got, &pe)
		assert.Equal(t, "Portable Error", pe.Name)
		assert.False(t, errors.Is(got, ErrAwaitedWorkflowCancelled))
	})

	t.Run("WarnsOnPlainStringFallback", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, nil))

		// Non-gob-encodable type → plain-string fallback must warn
		serializeWorkflowError(logger, fmt.Errorf("boom"), "DBOS_JSON")
		assert.Contains(t, buf.String(), "cannot be gob-encoded", "expected a warning for the plain-string fallback")

		// Gob-encodable error must not warn
		buf.Reset()
		serializeWorkflowError(logger, models.NewWorkflowCancelledError("wf-1", context.Canceled), "DBOS_JSON")
		assert.Empty(t, buf.String(), "expected no warning for a gob-encodable error")
	})

	t.Run("LegacyPlainStringDecodes", func(t *testing.T) {
		s := "DBOS Error WorkflowCancelled: legacy string, not encoded"
		got := deserializeWorkflowError(&s)
		require.Error(t, got)
		assert.Equal(t, s, got.Error())
		var pe *PortableWorkflowError
		assert.NotErrorAs(t, got, &pe)
	})

	t.Run("NilAndEmpty", func(t *testing.T) {
		assert.NoError(t, deserializeWorkflowError(nil))
		empty := ""
		assert.NoError(t, deserializeWorkflowError(&empty))
		assert.Equal(t, "", serializeWorkflowError(nil, nil, "DBOS_JSON"))
	})
}

// TestGoToGoErrorTypePreservation verifies end-to-end (through the DB) that a *Error
// returned by a default (non-portable) workflow is reconstructed with its concrete type
// and code when read back via a fresh handle, while a plain error keeps its message.
func TestGoToGoErrorTypePreservation(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	dbosErrWf := func(ctx Context, _ string) (string, error) {
		return "", &Error{Code: ErrorCodeWorkflowExecution, Message: "boom", WorkflowID: "inner"}
	}
	RegisterWorkflow(executor, dbosErrWf, WithWorkflowName("go_dbos_err_wf"))

	plainErrWf := func(ctx Context, _ string) (string, error) {
		return "", fmt.Errorf("plain boom")
	}
	RegisterWorkflow(executor, plainErrWf, WithWorkflowName("go_plain_err_wf"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	t.Run("ErrorReconstructedViaRetrieve", func(t *testing.T) {
		wfID := "go-dbos-err"
		h, err := RunWorkflow(executor, dbosErrWf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.Error(t, err)

		// Fresh handle → error comes from the DB round-trip, not the live goroutine.
		retrieved, err := RetrieveWorkflow[string](executor, wfID)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)

		var de *Error
		require.ErrorAs(t, err, &de)
		assert.Equal(t, ErrorCodeWorkflowExecution, de.Code)
		assert.Equal(t, "boom", de.Message)
		assert.Equal(t, "inner", de.WorkflowID)
		require.ErrorIs(t, err, &Error{Code: ErrorCodeWorkflowExecution})
	})

	t.Run("PlainErrorMessagePreservedViaRetrieve", func(t *testing.T) {
		wfID := "go-plain-err"
		h, err := RunWorkflow(executor, plainErrWf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.Error(t, err)

		retrieved, err := RetrieveWorkflow[string](executor, wfID)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		assert.Equal(t, "plain boom", err.Error())
		var de *Error
		assert.NotErrorAs(t, err, &de)
	})
}

// TestListWorkflowsAndGetWorkflowStepsIsolateDecodeErrors verifies that a single
// workflow's or step's undecodable input/output does not fail the entire
// ListWorkflows / GetWorkflowSteps call. Other items in the batch must still be
// returned and correctly decoded; the corrupted item's field carries the raw
// stored payload (with a logged warning) instead of aborting the whole request.
func TestListWorkflowsAndGetWorkflowStepsIsolateDecodeErrors(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	echoWf := func(ctx Context, input string) (string, error) {
		return RunAsStep(ctx, func(_ context.Context) (string, error) {
			return input, nil
		})
	}
	RegisterWorkflow(executor, echoWf, WithWorkflowName("decode_isolation_echo_wf"))

	multiStepWf := func(ctx Context, input string) (string, error) {
		for i := 0; i < 3; i++ {
			idx := i
			if _, err := RunAsStep(ctx, func(_ context.Context) (string, error) {
				return fmt.Sprintf("%s-step-%d", input, idx), nil
			}); err != nil {
				return "", err
			}
		}
		return input, nil
	}
	RegisterWorkflow(executor, multiStepWf, WithWorkflowName("decode_isolation_multi_step_wf"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	c := executor.(*dbosContext)
	sysDB := c.systemDB.(*sysdb.SysDB)
	schemaPrefix := sysDB.Dialect().SchemaPrefix(sysDB.Schema())

	const garbage = "not-valid-base64!!!"

	// corruptWorkflowColumn overwrites a workflow_status column (output or inputs)
	// with a value that cannot be base64-decoded.
	corruptWorkflowColumn := func(t *testing.T, column, workflowID string) {
		t.Helper()
		q := sysDB.RenderSQL(`UPDATE %sworkflow_status SET `+column+` = $1 WHERE workflow_uuid = $2`, schemaPrefix)
		_, err := sysDB.Pool().Exec(context.Background(), q, garbage, workflowID)
		require.NoError(t, err)
	}

	// corruptStepOutput overwrites a single operation_outputs row's output with a
	// value that cannot be base64-decoded.
	corruptStepOutput := func(t *testing.T, workflowID string, functionID int) {
		t.Helper()
		q := sysDB.RenderSQL(`UPDATE %soperation_outputs SET output = $1 WHERE workflow_uuid = $2 AND function_id = $3`, schemaPrefix)
		_, err := sysDB.Pool().Exec(context.Background(), q, garbage, workflowID, functionID)
		require.NoError(t, err)
	}

	t.Run("ListWorkflowsOutputDecodeErrorIsolated", func(t *testing.T) {
		goodID1 := "decode-good-output-1-" + t.Name()
		corruptID := "decode-corrupt-output-" + t.Name()
		goodID2 := "decode-good-output-2-" + t.Name()

		for _, id := range []string{goodID1, corruptID, goodID2} {
			handle, err := RunWorkflow(executor, echoWf, id, WithWorkflowID(id))
			require.NoError(t, err)
			result, err := handle.GetResult()
			require.NoError(t, err)
			assert.Equal(t, id, result)
		}

		corruptWorkflowColumn(t, "output", corruptID)

		wfs, err := ListWorkflows(executor, WithFilterWorkflowIDs(goodID1, corruptID, goodID2))
		require.NoError(t, err, "one workflow's undecodable output should not fail the whole ListWorkflows call")
		require.Len(t, wfs, 3)

		byID := make(map[string]WorkflowStatus, len(wfs))
		for _, wf := range wfs {
			byID[wf.ID] = wf
		}

		// Good entries decode to their raw JSON representation.
		assert.Equal(t, fmt.Sprintf("%q", goodID1), byID[goodID1].Output)
		assert.Equal(t, fmt.Sprintf("%q", goodID2), byID[goodID2].Output)
		// The corrupted entry surfaces the raw stored payload instead of
		// failing the whole call.
		corruptOutput, ok := byID[corruptID].Output.(string)
		require.True(t, ok, "corrupted output should be a string")
		assert.Equal(t, garbage, corruptOutput)
	})

	t.Run("ListWorkflowsInputDecodeErrorIsolated", func(t *testing.T) {
		goodID := "decode-good-input-" + t.Name()
		corruptID := "decode-corrupt-input-" + t.Name()

		for _, id := range []string{goodID, corruptID} {
			handle, err := RunWorkflow(executor, echoWf, id, WithWorkflowID(id))
			require.NoError(t, err)
			_, err = handle.GetResult()
			require.NoError(t, err)
		}

		corruptWorkflowColumn(t, "inputs", corruptID)

		wfs, err := ListWorkflows(executor, WithFilterWorkflowIDs(goodID, corruptID))
		require.NoError(t, err, "one workflow's undecodable input should not fail the whole ListWorkflows call")
		require.Len(t, wfs, 2)

		byID := make(map[string]WorkflowStatus, len(wfs))
		for _, wf := range wfs {
			byID[wf.ID] = wf
		}

		assert.Equal(t, fmt.Sprintf("%q", goodID), byID[goodID].Input)
		corruptInput, ok := byID[corruptID].Input.(string)
		require.True(t, ok, "corrupted input should be a string")
		assert.Equal(t, garbage, corruptInput)
	})

	t.Run("GetWorkflowStepsOutputDecodeErrorIsolated", func(t *testing.T) {
		workflowID := "decode-steps-" + t.Name()
		handle, err := RunWorkflow(executor, multiStepWf, "payload", WithWorkflowID(workflowID))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		corruptStepOutput(t, workflowID, 1)

		steps, err := GetWorkflowSteps(executor, workflowID)
		require.NoError(t, err, "one step's undecodable output should not fail the whole GetWorkflowSteps call")
		require.Len(t, steps, 3)

		assert.Equal(t, `"payload-step-0"`, steps[0].Output)
		corruptStepOutputVal, ok := steps[1].Output.(string)
		require.True(t, ok, "corrupted step output should be a string")
		assert.Equal(t, garbage, corruptStepOutputVal)
		assert.Equal(t, `"payload-step-2"`, steps[2].Output)
	})
}

// TestForkPreservesSerialization: forking with StartStep > 0 must copy the
// serialization column on checkpoints, events, and streams. If the copies
// drop it, the forked replay decodes gob payloads with the default JSON
// decoder and fails.
func TestForkPreservesSerialization(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})

	wf := func(ctx Context, input TestWorkflowData) (TestWorkflowData, error) {
		out, err := RunAsStep(ctx, func(ctx context.Context) (TestWorkflowData, error) {
			return input, nil
		}, WithStepName("checkpointStep"))
		if err != nil {
			return TestWorkflowData{}, err
		}
		if err := SetEvent(ctx, "fork-event", out); err != nil {
			return TestWorkflowData{}, err
		}
		if err := WriteStream(ctx, "fork-stream", out); err != nil {
			return TestWorkflowData{}, err
		}
		return out, nil
	}
	RegisterWorkflow(executor, wf, WithWorkflowName("fork-serialization-wf"))
	require.NoError(t, Launch(executor))

	input := TestWorkflowData{
		ID: "fork-serialization", Message: "gob payload", Value: 7,
		Data:     TestData{Message: "nested", Value: 14},
		Metadata: map[string]string{"path": "fork"},
	}
	handle, err := RunWorkflow(executor, wf, input, WithWorkflowID("fork-serialization-orig"))
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, input, result)

	// Fork past all recorded steps (0=checkpointStep, 1=SetEvent, 2=WriteStream)
	// so every copied row must carry its serialization to replay correctly.
	forkHandle, err := ForkWorkflow[TestWorkflowData](executor, ForkWorkflowInput{OriginalWorkflowID: "fork-serialization-orig", StartStep: 3})
	require.NoError(t, err)
	forkResult, err := forkHandle.GetResult()
	require.NoError(t, err, "forked replay must decode copied checkpoints with their recorded serializer")
	assert.Equal(t, input, forkResult)

	forkID := forkHandle.GetWorkflowID()
	event, err := GetEvent[TestWorkflowData](executor, forkID, "fork-event", 10*time.Second)
	require.NoError(t, err, "copied event must decode with its recorded serializer")
	assert.Equal(t, input, event)

	values, closed, err := ReadStream[TestWorkflowData](executor, forkID, "fork-stream")
	require.NoError(t, err, "copied stream entry must decode with its recorded serializer")
	assert.True(t, closed)
	require.Len(t, values, 1)
	assert.Equal(t, input, values[0])
}

// TestExportImportPreservesSerialization: export/import must round-trip the
// serialization column on checkpoints, events, events history, and streams —
// not just workflow_status. If import writes NULL serialization, every reader
// of the reimported rows falls back to the default JSON decoder and fails on
// gob payloads.
func TestExportImportPreservesSerialization(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})

	wf := func(ctx Context, input TestWorkflowData) (TestWorkflowData, error) {
		out, err := RunAsStep(ctx, func(ctx context.Context) (TestWorkflowData, error) {
			return input, nil
		}, WithStepName("checkpointStep"))
		if err != nil {
			return TestWorkflowData{}, err
		}
		if err := SetEvent(ctx, "export-event", out); err != nil {
			return TestWorkflowData{}, err
		}
		if err := WriteStream(ctx, "export-stream", out); err != nil {
			return TestWorkflowData{}, err
		}
		return out, nil
	}
	RegisterWorkflow(executor, wf, WithWorkflowName("export-serialization-wf"))
	require.NoError(t, Launch(executor))

	input := TestWorkflowData{
		ID: "export-serialization", Message: "gob payload", Value: 7,
		Data:     TestData{Message: "nested", Value: 14},
		Metadata: map[string]string{"path": "export"},
	}
	workflowID := "export-serialization-orig"
	handle, err := RunWorkflow(executor, wf, input, WithWorkflowID(workflowID))
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, input, result)

	sdb := executor.(*dbosContext).systemDB.(*sysdb.SysDB)

	exported, err := sdb.ExportWorkflow(executor, workflowID, false)
	require.NoError(t, err)
	require.Len(t, exported, 1)

	// The exported payload itself must carry serialization on every table.
	requireSerialization := func(table string, rows []map[string]any) {
		require.NotEmpty(t, rows, "expected exported %s rows", table)
		for _, row := range rows {
			ser, ok := row["serialization"].(*string)
			require.True(t, ok, "%s row missing serialization key", table)
			require.NotNil(t, ser, "%s row exported with NULL serialization", table)
		}
	}
	requireSerialization("operation_outputs", exported[0].OperationOutputs)
	requireSerialization("workflow_events", exported[0].WorkflowEvents)
	requireSerialization("workflow_events_history", exported[0].WorkflowEventsHistory)
	requireSerialization("streams", exported[0].Streams)

	require.NoError(t, sdb.DeleteWorkflows(executor, sysdb.DeleteWorkflowsDBInput{
		WorkflowIDs: []string{workflowID},
	}))
	require.NoError(t, sdb.ImportWorkflow(executor, exported))

	// Readers of the reimported rows must decode with the recorded serializer.
	event, err := GetEvent[TestWorkflowData](executor, workflowID, "export-event", 10*time.Second)
	require.NoError(t, err, "reimported event must decode with its recorded serializer")
	assert.Equal(t, input, event)

	values, closed, err := ReadStream[TestWorkflowData](executor, workflowID, "export-stream")
	require.NoError(t, err, "reimported stream entry must decode with its recorded serializer")
	assert.True(t, closed)
	require.Len(t, values, 1)
	assert.Equal(t, input, values[0])

	// Fork past all steps: replay of the reimported checkpoints must decode
	// with the serialization the import round-tripped.
	forkHandle, err := ForkWorkflow[TestWorkflowData](executor, ForkWorkflowInput{OriginalWorkflowID: workflowID, StartStep: 3})
	require.NoError(t, err)
	forkResult, err := forkHandle.GetResult()
	require.NoError(t, err, "replay of reimported checkpoints must decode with their recorded serializer")
	assert.Equal(t, input, forkResult)
}

// listingSpySerializer records whether its Decode was consulted.
type listingSpySerializer struct{ decodeCalls int }

func (s *listingSpySerializer) Name() string { return "listing-spy" }
func (s *listingSpySerializer) Encode(data any) (*string, error) {
	str := fmt.Sprintf("%v", data)
	return &str, nil
}
func (s *listingSpySerializer) Decode(data *string) (any, error) {
	s.decodeCalls++
	return "spy:" + *data, nil
}

// TestListingDecodeUsesStoredSerialization verifies the listing decode path picks
// the decoder from each row's persisted serialization format, not from the currently
// configured serializer: a DBOS_JSON row must never be routed through a custom serializer.
func TestListingDecodeUsesStoredSerialization(t *testing.T) {
	spy := &listingSpySerializer{}
	c := &dbosContext{
		logger:     slog.Default(),
		serializer: spy,
	}

	defaultJSON := `{"count":3,"name":"pre-serializer-era"}`
	b64 := base64.StdEncoding.EncodeToString([]byte(defaultJSON))
	spyPayload := "spy-payload"
	portableJSON := `{"positionalArgs":["x"]}`

	workflows := []WorkflowStatus{
		{ID: "wf-json", Serialization: "DBOS_JSON", Input: &b64, Output: &b64},
		{ID: "wf-empty", Serialization: "", Input: &b64, Output: &b64},
		{ID: "wf-custom", Serialization: "listing-spy", Input: &spyPayload, Output: &spyPayload},
		{ID: "wf-portable", Serialization: PortableSerializerName, Input: &portableJSON, Output: &portableJSON},
		{ID: "wf-unknown", Serialization: "DBOS_GOB", Input: &b64, Output: &b64},
	}

	require.NoError(t, c.decodeWorkflowsInputOutput(workflows, true, true))

	// DBOS_JSON and legacy empty-format rows: raw JSON text, spy not consulted.
	assert.Equal(t, defaultJSON, workflows[0].Input)
	assert.Equal(t, defaultJSON, workflows[0].Output)
	assert.Equal(t, defaultJSON, workflows[1].Input)
	assert.Equal(t, defaultJSON, workflows[1].Output)

	// Row whose stored format matches the custom serializer: decoded by it.
	assert.Equal(t, "spy:"+spyPayload, workflows[2].Input)
	assert.Equal(t, "spy:"+spyPayload, workflows[2].Output)

	// Portable rows: decoded JSON value.
	decodedPortable := map[string]any{"positionalArgs": []any{"x"}}
	assert.Equal(t, decodedPortable, workflows[3].Input)
	assert.Equal(t, decodedPortable, workflows[3].Output)

	// Unknown format: raw stored string kept as-is, spy not consulted.
	assert.Equal(t, b64, workflows[4].Input)
	assert.Equal(t, b64, workflows[4].Output)

	assert.Equal(t, 2, spy.decodeCalls, "custom serializer must only decode rows tagged with its format")
}

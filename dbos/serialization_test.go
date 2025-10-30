package dbos

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// isInterfaceType checks if the given value is of interface{} (any) type
func isInterfaceType(v any) bool {
	t := reflect.TypeOf(v)
	if t == nil {
		return true // nil can be any type
	}
	return t.Kind() == reflect.Interface
}

// Helper function to test comprehensive workflow values across different retrieval methods
func testComprehensiveWorkflowValues[T any](
	t *testing.T,
	executor DBOSContext,
	handle WorkflowHandle[T],
	expectedInput TestWorkflowData,
) {
	t.Helper()

	// Get the serializer to determine how to compare
	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	isJSON := isJSONSerializer(dbosCtx.serializer)

	// 1. Test with handle.GetResult()
	t.Run("HandleGetResult", func(t *testing.T) {
		result, err := handle.GetResult()
		require.NoError(t, err, "Failed to get workflow result")
		assert.Equal(t, expectedInput, result, "Workflow result should match input")
	})

	// 2. Test with ListWorkflows
	t.Run("ListWorkflows", func(t *testing.T) {
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

		if isJSON {
			// For JSON serializer with any type, convert map[string]interface{} to typed struct
			typedInput, err := convertJSONToType[TestWorkflowData](workflow.Input)
			require.NoError(t, err, "Failed to convert workflow input")
			typedOutput, err := convertJSONToType[TestWorkflowData](workflow.Output)
			require.NoError(t, err, "Failed to convert workflow output")
			assert.Equal(t, expectedInput, typedInput, "Workflow input from ListWorkflows should match original")
			assert.Equal(t, expectedInput, typedOutput, "Workflow output from ListWorkflows should match original")
		} else {
			assert.Equal(t, expectedInput, workflow.Input, "Workflow input from ListWorkflows should match original")
			assert.Equal(t, expectedInput, workflow.Output, "Workflow output from ListWorkflows should match original")
		}
	})

	// 3. Test with GetWorkflowSteps
	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
		require.NoError(t, err, "Failed to get workflow steps")
		require.Len(t, steps, 1, "Expected 1 step")

		step := steps[0]
		require.NotNil(t, step.Output, "Step output should not be nil")

		if isJSON {
			// For JSON serializer with any type, convert map[string]interface{} to typed struct
			typedOutput, err := convertJSONToType[TestWorkflowData](step.Output)
			require.NoError(t, err, "Failed to convert step output")
			assert.Equal(t, expectedInput, typedOutput, "Step output should match original")
		} else {
			assert.Equal(t, expectedInput, step.Output, "Step output should match original")
		}
		assert.Nil(t, step.Error, "Step should not have error")
	})

	// 4. Test with RetrieveWorkflow (polling handle)

	// Check if T is 'any' - only then we need JSON recast for RetrieveWorkflow (generic otherwise)
	var zeroT T
	needsJSONRecast := isJSON && isInterfaceType(zeroT)

	t.Run("RetrieveWorkflow", func(t *testing.T) {
		retrievedHandle, err := RetrieveWorkflow[T](executor, handle.GetWorkflowID())
		require.NoError(t, err, "Failed to retrieve workflow")

		result, err := retrievedHandle.GetResult()
		require.NoError(t, err, "Failed to get result from retrieved handle")

		if needsJSONRecast {
			// For JSON serializer with any type, convert map[string]interface{} to typed struct
			typedResult, err := convertJSONToType[TestWorkflowData](result)
			require.NoError(t, err, "Failed to convert retrieved workflow result")
			assert.Equal(t, expectedInput, typedResult, "Retrieved workflow result should match input")
		} else {
			assert.Equal(t, expectedInput, result, "Retrieved workflow result should match input")
		}
	})
}

// Helper function to test nil workflow values across different retrieval methods
func testNilWorkflowValues[T any](
	t *testing.T,
	executor DBOSContext,
	handle WorkflowHandle[T],
) {
	t.Helper()

	// 1. Test with handle.GetResult()
	t.Run("HandleGetResult", func(t *testing.T) {
		result, err := handle.GetResult()
		require.NoError(t, err, "Failed to get workflow result")
		assert.Nil(t, result, "Nil result should be preserved")
	})

	// 2. Test with ListWorkflows
	t.Run("ListWorkflows", func(t *testing.T) {
		workflows, err := ListWorkflows(executor,
			WithWorkflowIDs([]string{handle.GetWorkflowID()}),
			WithLoadInput(true),
			WithLoadOutput(true),
		)
		require.NoError(t, err, "Failed to list workflows")
		require.Len(t, workflows, 1, "Expected 1 workflow")

		workflow := workflows[0]
		assert.Nil(t, workflow.Input, "Workflow input from ListWorkflows should be nil")
		assert.Nil(t, workflow.Output, "Workflow output from ListWorkflows should be nil")
	})

	// 3. Test with GetWorkflowSteps
	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
		require.NoError(t, err, "Failed to get workflow steps")
		require.Len(t, steps, 1, "Expected 1 step")

		step := steps[0]
		assert.Nil(t, step.Output, "Step output should be nil")
		assert.Nil(t, step.Error, "Step should not have error")
	})

	// 4. Test with RetrieveWorkflow (polling handle)
	t.Run("RetrieveWorkflow", func(t *testing.T) {
		retrievedHandle, err := RetrieveWorkflow[T](executor, handle.GetWorkflowID())
		require.NoError(t, err, "Failed to retrieve workflow")

		result, err := retrievedHandle.GetResult()
		require.NoError(t, err, "Failed to get result from retrieved handle")
		assert.Nil(t, result, "Retrieved workflow result should be nil")
	})

	// 5. Test database storage (nil values stored as empty strings)
	t.Run("DatabaseStorage", func(t *testing.T) {
		dbosCtx, ok := executor.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		sysDB, ok := dbosCtx.systemDB.(*sysDB)
		require.True(t, ok, "expected sysDB")

		var inputs string
		var output string
		err := sysDB.pool.QueryRow(context.Background(),
			fmt.Sprintf("SELECT inputs, output FROM %s.workflow_status WHERE workflow_uuid = $1", sysDB.schema),
			handle.GetWorkflowID()).Scan(&inputs, &output)
		require.NoError(t, err, "Failed to query workflow_status")
		assert.Equal(t, "", inputs, "Nil inputs should be stored as empty string in database")
		assert.Equal(t, "", output, "Nil output should be stored as empty string in database")
	})
}

// Helper function to test Send/Recv communication
func testSendRecv[T any](
	t *testing.T,
	executor DBOSContext,
	senderWorkflow func(DBOSContext, T) (T, error),
	receiverWorkflow func(DBOSContext, T) (T, error),
	input T,
	senderID string,
) {
	t.Helper()

	// Start sender workflow first
	senderHandle, err := RunWorkflow(executor, senderWorkflow, input, WithWorkflowID(senderID))
	require.NoError(t, err, "Sender workflow execution failed")

	// Start receiver workflow (it will wait for the message)
	receiverHandle, err := RunWorkflow(executor, receiverWorkflow, input, WithWorkflowID(senderID+"-receiver"))
	require.NoError(t, err, "Receiver workflow execution failed")

	// Get sender result
	senderResult, err := senderHandle.GetResult()
	require.NoError(t, err, "Sender workflow should complete")

	// Get receiver result
	receiverResult, err := receiverHandle.GetResult()
	require.NoError(t, err, "Receiver workflow should complete")

	// Verify the received data matches what was sent

	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	var zeroT T
	needsJSONRecast := isJSONSerializer(dbosCtx.serializer) && isInterfaceType(zeroT)
	if needsJSONRecast {
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
	setEventWorkflow func(DBOSContext, T) (T, error),
	getEventWorkflow func(DBOSContext, string) (T, error),
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
	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	var zeroT T
	needsJSONRecast := isJSONSerializer(dbosCtx.serializer) && isInterfaceType(zeroT)
	if needsJSONRecast {
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
	if isJSON {
		// Recovery handle are always "any"
		typedRecoveredResult, err := convertJSONToType[TestWorkflowData](recoveredResult)
		require.NoError(t, err, "Failed to convert recovered result")
		var zeroT T
		if isInterfaceType(zeroT) {
			// For JSON serializer with any type, convert results
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

// Test that workflows use the configured serializer for input/output
func TestSerializer(t *testing.T) {
	serializers := map[string]func() Serializer{
		"JSON": func() Serializer { return NewJSONSerializer() },
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

				testComprehensiveWorkflowValues(t, executor, handle, input)
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

				testComprehensiveWorkflowValues(t, executor, handle, input)
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
					// JSON serializer returns map[string]any
					inputMap, ok := workflow.Input.(map[string]any)
					require.True(t, ok, "Input should be map[string]any for JSON")
					assert.Equal(t, input.Message, inputMap["Message"], "Message should match in input")
					assert.Equal(t, float64(input.Value), inputMap["Value"], "Value should match in input")

					outputMap, ok := workflow.Output.(map[string]any)
					require.True(t, ok, "Output should be map[string]any for JSON")
					assert.Equal(t, input.Message, outputMap["Message"], "Message should match in output")
					assert.Equal(t, float64(input.Value), outputMap["Value"], "Value should match in output")
				} else {
					// Gob serializer preserves the concrete type
					inputConcrete, ok := workflow.Input.(ConcreteDataProvider)
					require.True(t, ok, "Input should be ConcreteDataProvider for Gob")
					assert.Equal(t, input, inputConcrete, "Input should match")

					outputConcrete, ok := workflow.Output.(ConcreteDataProvider)
					require.True(t, ok, "Output should be ConcreteDataProvider for Gob")
					assert.Equal(t, input, outputConcrete, "Output should match")
				}
			})

			// Test nil values with pointer type workflow
			t.Run("NilValuesPointer", func(t *testing.T) {
				handle, err := RunWorkflow(executor, serializerNilValueWorkflow, (*TestWorkflowData)(nil))
				require.NoError(t, err, "Nil pointer workflow execution failed")

				testNilWorkflowValues(t, executor, handle)
			})

			// Test nil values with any type workflow
			t.Run("NilValuesAny", func(t *testing.T) {
				handle, err := RunWorkflow(executor, serializerAnyValueWorkflow, nil)
				require.NoError(t, err, "Nil any workflow execution failed")

				testNilWorkflowValues(t, executor, handle)
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

package dbos

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			// For JSON serializer, convert map[string]interface{} to typed struct
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
			// For JSON serializer, convert map[string]interface{} to typed struct
			typedOutput, err := convertJSONToType[TestWorkflowData](step.Output)
			require.NoError(t, err, "Failed to convert step output")
			assert.Equal(t, expectedInput, typedOutput, "Step output should match original")
		} else {
			assert.Equal(t, expectedInput, step.Output, "Step output should match original")
		}
		assert.Nil(t, step.Error, "Step should not have error")
	})

	// 4. Test with RetrieveWorkflow (polling handle)
	t.Run("RetrieveWorkflow", func(t *testing.T) {
		retrievedHandle, err := RetrieveWorkflow[T](executor, handle.GetWorkflowID())
		require.NoError(t, err, "Failed to retrieve workflow")

		result, err := retrievedHandle.GetResult()
		require.NoError(t, err, "Failed to get result from retrieved handle")

		if isJSON {
			// For JSON serializer, convert map[string]interface{} to typed struct
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

	// Get the serializer to determine how to compare
	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	isJSON := isJSONSerializer(dbosCtx.serializer)

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
	if isJSON {
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

	// Get the serializer to determine how to compare
	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	isJSON := isJSONSerializer(dbosCtx.serializer)

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
	if isJSON {
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

func serializerTestWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (TestWorkflowData, error) {
		return serializerTestStep(context, input)
	})
}

func serializerNilValueWorkflow(ctx DBOSContext, input *TestWorkflowData) (*TestWorkflowData, error) {
	return RunAsStep(ctx, func(context context.Context) (*TestWorkflowData, error) {
		return input, nil
	})
}

func serializerNilValueAnyWorkflow(ctx DBOSContext, input any) (any, error) {
	return RunAsStep(ctx, func(context context.Context) (any, error) {
		if input == nil {
			return nil, nil
		}
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

// Test that workflows use the configured serializer for input/output
func TestSerializer(t *testing.T) {
	serializers := map[string]func() Serializer{
		"JSON": func() Serializer { return NewJSONSerializer() },
		"Gob":  func() Serializer { return NewGobSerializer() },
	}

	for serializerName, serializerFactory := range serializers {
		t.Run(serializerName, func(t *testing.T) {
			executor := setupDBOS(t, true, true, serializerFactory())

			// Register workflows
			RegisterWorkflow(executor, serializerTestWorkflow)
			RegisterWorkflow(executor, serializerNilValueWorkflow)
			RegisterWorkflow(executor, serializerErrorWorkflow)
			RegisterWorkflow(executor, serializerSenderWorkflow)
			RegisterWorkflow(executor, serializerReceiverWorkflow)
			RegisterWorkflow(executor, serializerSetEventWorkflow)
			RegisterWorkflow(executor, serializerGetEventWorkflow)
			if serializerName == "JSON" {
				// Cannot register "any" workflow with Gob serializer
				RegisterWorkflow(executor, serializerNilValueAnyWorkflow)
				RegisterWorkflow(executor, serializerAnySenderWorkflow)
				RegisterWorkflow(executor, serializerAnyReceiverWorkflow)
				RegisterWorkflow(executor, serializerAnySetEventWorkflow)
				RegisterWorkflow(executor, serializerAnyGetEventWorkflow)
			}

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

				handle, err := RunWorkflow(executor, serializerTestWorkflow, input)
				require.NoError(t, err, "Workflow execution failed")

				testComprehensiveWorkflowValues(t, executor, handle, input)
			})

			// Test workflow with any type and comprehensive data structure
			t.Run("ComprehensiveAnyValues", func(t *testing.T) {
				if serializerName == "Gob" {
					t.Skip("Skipping test for Gob serializer due to Gob limitations with interface types")
				}
				input := TestWorkflowData{
					ID:       "any-test-id",
					Message:  "any test message",
					Value:    99,
					Active:   true,
					Data:     TestData{Message: "any embedded", Value: 777, Active: true},
					Metadata: map[string]string{"type": "any"},
				}

				// Pass input as any to match serializerNilValueAnyWorkflow signature
				handle, err := RunWorkflow(executor, serializerNilValueAnyWorkflow, any(input))
				require.NoError(t, err, "Any workflow execution failed")

				testComprehensiveWorkflowValues(t, executor, handle, input)
			})

			// Test nil values with pointer type workflow
			t.Run("NilValuesPointer", func(t *testing.T) {
				handle, err := RunWorkflow(executor, serializerNilValueWorkflow, (*TestWorkflowData)(nil))
				require.NoError(t, err, "Nil pointer workflow execution failed")

				testNilWorkflowValues(t, executor, handle)
			})

			// Test nil values with any type workflow
			t.Run("NilValuesAny", func(t *testing.T) {
				if serializerName == "Gob" {
					t.Skip("Skipping test for Gob serializer due to Gob limitations with interface types")
				}
				handle, err := RunWorkflow(executor, serializerNilValueAnyWorkflow, nil)
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
				if serializerName == "Gob" {
					t.Skip("Skipping test for Gob serializer due to Gob limitations with interface types")
				}
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
				if serializerName == "Gob" {
					t.Skip("Skipping test for Gob serializer due to Gob limitations with interface types")
				}
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

		})
	}
}

// Test serializer interface compliance
func TestSerializerInterface(t *testing.T) {
	// Test that both serializers implement the Serializer interface
	var _ Serializer = (*GobSerializer)(nil)
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

	// Test Gob serializer configuration
	t.Run("GobSerializer", func(t *testing.T) {
		config := Config{
			DatabaseURL: getDatabaseURL(),
			AppName:     "test-app",
			Serializer:  NewGobSerializer(),
		}

		executor, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err, "Failed to create DBOS context with Gob serializer")

		// Verify the serializer is set in the context
		if dbosCtx, ok := executor.(*dbosContext); ok {
			assert.NotNil(t, dbosCtx.serializer, "Gob serializer should be configured")
			assert.IsType(t, &GobSerializer{}, dbosCtx.serializer, "Should be GobSerializer type")
		}
	})

	// Test default serializer (should be Gob)
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
			assert.IsType(t, &GobSerializer{}, dbosCtx.serializer, "Default should be GobSerializer")
		}
	})
}

// Global events for controlling concurrent workflow test execution
var (
	concurrentConflictFirstInStep     *Event
	concurrentConflictSecondInStep    *Event
	concurrentConflictProceed         *Event
	concurrentConflictInvocationCount int32
)

// Test concurrent workflow invocations with step recording conflict
// This tests the conflict resolution path in workflow.go where a workflow
// detects a conflict during recordOperationResult and waits for the existing workflow
func TestConcurrentWorkflowInvocationConflict(t *testing.T) {
	serializers := map[string]Serializer{
		"Gob":  NewGobSerializer(),
		"JSON": NewJSONSerializer(),
	}

	for serializerName, serializer := range serializers {
		t.Run(serializerName, func(t *testing.T) {
			executor := setupDBOS(t, true, true, serializer)

			// Register a workflow that has a step and blocks on an event
			RegisterWorkflow(executor, concurrentConflictWorkflow)

			// Create events to control workflow execution
			concurrentConflictFirstInStep = NewEvent()
			concurrentConflictSecondInStep = NewEvent()
			concurrentConflictProceed = NewEvent()
			concurrentConflictInvocationCount = 0

			// Use a fixed workflow ID for both executions
			workflowID := "concurrent-conflict-test-id"
			input := TestWorkflowData{
				ID:       workflowID,
				Message:  "concurrent test",
				Value:    999,
				Active:   true,
				Data:     TestData{Message: "nested", Value: 111, Active: true},
				Metadata: map[string]string{"test": "concurrent"},
			}

			// Start first workflow - it will enter the step and wait
			handle1, err := RunWorkflow(executor, concurrentConflictWorkflow, input, WithWorkflowID(workflowID))
			require.NoError(t, err, "First workflow should start successfully")

			// Wait for the first workflow to enter the step
			concurrentConflictFirstInStep.Wait()

			// Start second workflow with the same ID - it will also enter the step and wait
			handle2, err := RunWorkflow(executor, concurrentConflictWorkflow, input, WithWorkflowID(workflowID))
			require.NoError(t, err, "Second workflow should start successfully")

			// Wait for the second workflow to enter the step
			concurrentConflictSecondInStep.Wait()

			// Now both workflows are inside the step, waiting to proceed
			// Let them both try to commit - one will succeed, the other will get a conflict
			concurrentConflictProceed.Set()

			// Both handles should return the same result
			result1, err := handle1.GetResult()
			require.NoError(t, err, "First workflow should complete successfully")

			fmt.Printf("type of handle2: %T\n", handle2)

			result2, err := handle2.GetResult()
			require.NoError(t, err, "Second workflow should get result from first workflow")

			// Get the serializer to determine how to compare
			dbosCtx, ok := executor.(*dbosContext)
			require.True(t, ok, "expected dbosContext")
			isJSON := isJSONSerializer(dbosCtx.serializer)

			// Verify results are equal
			if isJSON {
				typedResult1, err := convertJSONToType[TestWorkflowData](result1)
				require.NoError(t, err, "Failed to convert first result")
				typedResult2, err := convertJSONToType[TestWorkflowData](result2)
				require.NoError(t, err, "Failed to convert second result")
				assert.Equal(t, typedResult1, typedResult2, "Both results should be equal")
				assert.Equal(t, input, typedResult1, "Result should match input")
			} else {
				assert.Equal(t, result1, result2, "Both results should be equal")
				assert.Equal(t, input, result1, "Result should match input")
			}

			// Verify both handles point to the same workflow ID
			assert.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID())
		})
	}
}

// Workflow that executes a step with concurrent access control
func concurrentConflictWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	// Execute a step - both invocations will enter here simultaneously
	result, err := RunAsStep(ctx, func(ctx context.Context) (TestWorkflowData, error) {
		// Atomically increment and determine which invocation this is
		invocationNum := atomic.AddInt32(&concurrentConflictInvocationCount, 1)

		// Signal that this invocation has entered the step
		switch invocationNum {
		case 1:
			concurrentConflictFirstInStep.Set()
		case 2:
			concurrentConflictSecondInStep.Set()
		}

		// Wait for the test to tell both invocations to proceed
		concurrentConflictProceed.Wait()

		// Now both will try to record their result - one will succeed, one will conflict
		return input, nil
	})
	fmt.Println("step error:", err)
	if err != nil {
		return TestWorkflowData{}, err
	}

	return result, nil
}

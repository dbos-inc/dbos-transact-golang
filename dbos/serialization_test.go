package dbos

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Unit Tests for Serialization Functions
// ============================================================================

func TestSerialize(t *testing.T) {
	logger := slog.Default()

	t.Run("NilValues", func(t *testing.T) {
		tests := []struct {
			name  string
			value any
		}{
			{"nil", nil},
			{"nil pointer", (*int)(nil)},
			{"nil slice", ([]int)(nil)},
			{"nil map", (map[string]int)(nil)},
			{"nil interface", (interface{})(nil)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := serialize(tt.value, logger)
				require.NoError(t, err)
				// Nil values should serialize to base64-encoded empty bytes
				expected := base64.StdEncoding.EncodeToString([]byte{})
				assert.Equal(t, expected, result)
			})
		}
	})

	t.Run("BuiltinTypes", func(t *testing.T) {
		tests := []struct {
			name  string
			value any
		}{
			{"int", 42},
			{"string", "test"},
			{"bool", true},
			{"float64", 3.14},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := serialize(tt.value, logger)
				require.NoError(t, err)
				assert.NotEmpty(t, result)
				// Should be valid base64
				_, decodeErr := base64.StdEncoding.DecodeString(result)
				assert.NoError(t, decodeErr)
			})
		}
	})

	t.Run("StructTypes", func(t *testing.T) {
		type TestStruct struct {
			A string
			B int
		}

		tests := []struct {
			name  string
			value any
		}{
			{"struct", TestStruct{A: "test", B: 42}},
			{"pointer to struct", &TestStruct{A: "test", B: 42}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := serialize(tt.value, logger)
				require.NoError(t, err)
				assert.NotEmpty(t, result)
				// Should be valid base64
				_, decodeErr := base64.StdEncoding.DecodeString(result)
				assert.NoError(t, decodeErr)
			})
		}
	})

	t.Run("CollectionTypes", func(t *testing.T) {
		tests := []struct {
			name  string
			value any
		}{
			{"slice", []int{1, 2, 3}},
			{"map", map[string]int{"a": 1, "b": 2}},
			{"empty slice", []int{}},
			{"empty map", map[string]int{}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := serialize(tt.value, logger)
				require.NoError(t, err)
				assert.NotEmpty(t, result)
				// Should be valid base64
				_, decodeErr := base64.StdEncoding.DecodeString(result)
				assert.NoError(t, decodeErr)
			})
		}
	})
}

func TestDeserialize(t *testing.T) {
	t.Run("NilOrEmptyString", func(t *testing.T) {
		tests := []struct {
			name  string
			input *string
		}{
			{"nil pointer", nil},
			{"empty string", stringPtr("")},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result, err := deserialize(tt.input)
				require.NoError(t, err)
				assert.Nil(t, result)
			})
		}
	})

	t.Run("InvalidBase64", func(t *testing.T) {
		invalidBase64 := "not-valid-base64!!!"
		result, err := deserialize(&invalidBase64)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode data")
		assert.Nil(t, result)
	})

	t.Run("ValidBase64ButInvalidGob", func(t *testing.T) {
		// Valid base64 but not a valid gob encoding
		invalidGob := base64.StdEncoding.EncodeToString([]byte{1, 2, 3, 4, 5})
		result, err := deserialize(&invalidGob)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode data")
		assert.Nil(t, result)
	})
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	logger := slog.Default()

	type ComplexStruct struct {
		Name   string
		Value  int
		Nested struct {
			Data []string
		}
	}

	tests := []struct {
		name     string
		value    any
		expected any // expected value after round trip (gob may not preserve pointer vs value)
	}{
		{"int", 42, 42},
		{"string", "test string", "test string"},
		{"bool", true, true},
		{"float", 3.14159, 3.14159},
		{"slice", []int{1, 2, 3, 4, 5}, []int{1, 2, 3, 4, 5}},
		{"map", map[string]int{"a": 1, "b": 2, "c": 3}, map[string]int{"a": 1, "b": 2, "c": 3}},
		{"struct", ComplexStruct{
			Name:  "test",
			Value: 42,
			Nested: struct {
				Data []string
			}{
				Data: []string{"a", "b", "c"},
			},
		}, ComplexStruct{
			Name:  "test",
			Value: 42,
			Nested: struct {
				Data []string
			}{
				Data: []string{"a", "b", "c"},
			},
		}},
		// Note: gob doesn't preserve pointer vs value semantics, so a pointer becomes a value after round trip
		{"pointer to struct", &ComplexStruct{Name: "ptr-test", Value: 99}, ComplexStruct{Name: "ptr-test", Value: 99}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			serialized, err := serialize(tt.value, logger)
			require.NoError(t, err)
			assert.NotEmpty(t, serialized)

			// Deserialize
			deserialized, err := deserialize(&serialized)
			require.NoError(t, err)

			// Verify round trip
			assert.Equal(t, tt.expected, deserialized)
		})
	}
}

func TestIsNilValue(t *testing.T) {
	t.Run("NilValues", func(t *testing.T) {
		tests := []struct {
			name  string
			value any
		}{
			{"nil", nil},
			{"nil pointer", (*int)(nil)},
			{"nil slice", ([]int)(nil)},
			{"nil map", (map[string]int)(nil)},
			{"nil interface", (interface{})(nil)},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.True(t, isNilValue(tt.value))
			})
		}
	})

	t.Run("NonNilValues", func(t *testing.T) {
		val := 42
		tests := []struct {
			name  string
			value any
		}{
			{"int", 42},
			{"string", "test"},
			{"bool", true},
			{"non-nil pointer", &val},
			{"empty slice", []int{}},
			{"empty map", map[string]int{}},
			{"struct", struct{ A int }{A: 1}},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				assert.False(t, isNilValue(tt.value))
			})
		}
	})
}

func TestSafeGobRegister(t *testing.T) {
	logger := slog.Default()

	t.Run("DuplicateTypeRegistration", func(t *testing.T) {
		type TestType1 struct {
			Value string
		}

		// First registration should succeed
		assert.NotPanics(t, func() {
			safeGobRegister(TestType1{}, logger)
		})

		// Duplicate registration should not panic (handled by safeGobRegister)
		assert.NotPanics(t, func() {
			safeGobRegister(TestType1{}, logger)
		})
	})

	t.Run("TypeAndPointerConflict", func(t *testing.T) {
		type TestType2 struct {
			Value string
		}

		// Register value type
		assert.NotPanics(t, func() {
			safeGobRegister(TestType2{}, logger)
		})

		// Register pointer type (should handle the conflict gracefully)
		assert.NotPanics(t, func() {
			safeGobRegister(&TestType2{}, logger)
		})
	})
}

// Helper function
func stringPtr(s string) *string {
	return &s
}

// ============================================================================
// Integration Tests for Workflow Serialization
// ============================================================================

// Builtin types
func encodingStepBuiltinTypes(_ context.Context, input int) (int, error) {
	return input, errors.New("step error")
}

func encodingWorkflowBuiltinTypes(ctx DBOSContext, input string) (string, error) {
	stepResult, err := RunAsStep(ctx, func(context context.Context) (int, error) {
		return encodingStepBuiltinTypes(context, 123)
	})
	return fmt.Sprintf("%d", stepResult), fmt.Errorf("workflow error: %v", err)
}

// Struct types
type StepOutputStruct struct {
	A StepInputStruct
	B string
}

type StepInputStruct struct {
	A SimpleStruct
	B string
}

type WorkflowInputStruct struct {
	A SimpleStruct
	B int
}

type SimpleStruct struct {
	A string
	B int
}

type PointerResultStruct struct {
	Value string
	Count int
}

func encodingWorkflowStruct(ctx DBOSContext, input WorkflowInputStruct) (StepOutputStruct, error) {
	return RunAsStep(ctx, func(context context.Context) (StepOutputStruct, error) {
		return encodingStepStruct(context, StepInputStruct{
			A: input.A,
			B: fmt.Sprintf("%d", input.B),
		})
	})
}

func encodingStepStruct(_ context.Context, input StepInputStruct) (StepOutputStruct, error) {
	return StepOutputStruct{
		A: input,
		B: "processed by encodingStepStruct",
	}, nil
}

// Test nil pointer is ignored during encode
func encodingWorkflowNilReturn(ctx DBOSContext, shouldReturnNil bool) (string, error) {
	pointerResult, err := RunAsStep(ctx, func(context context.Context) (*PointerResultStruct, error) {
		if shouldReturnNil {
			return nil, nil
		}
		return &PointerResultStruct{
			Value: "pointer result",
			Count: 42,
		}, nil
	})
	if err != nil {
		return "", fmt.Errorf("pointer step failed: %w", err)
	}
	// Build result summary
	var summary []string
	if shouldReturnNil {
		summary = append(summary, "All nil types handled successfully")
	} else {
		if pointerResult != nil {
			summary = append(summary, fmt.Sprintf("ptr:%s", pointerResult.Value))
		}
	}
	return strings.Join(summary, ", "), nil
}

// Interface types for testing manual interface registration
type ResponseInterface interface {
	GetMessage() string
	GetCode() int
}

type ConcreteResponse struct {
	Message string
	Code    int
}

func (c ConcreteResponse) GetMessage() string {
	return c.Message
}

func (c ConcreteResponse) GetCode() int {
	return c.Code
}

func encodingWorkflowInterface(ctx DBOSContext, input string) (ResponseInterface, error) {
	result, err := RunAsStep(ctx, func(context context.Context) (ResponseInterface, error) {
		return encodingStepInterface(context, input)
	})
	if err != nil {
		return nil, fmt.Errorf("interface step failed: %w", err)
	}
	return result, nil
}

func encodingStepInterface(_ context.Context, input string) (ResponseInterface, error) {
	return ConcreteResponse{
		Message: fmt.Sprintf("Processed: %s", input),
		Code:    200,
	}, nil
}

func TestWorkflowSerializationIntegration(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflows with executor
	RegisterWorkflow(executor, encodingWorkflowBuiltinTypes)
	RegisterWorkflow(executor, encodingWorkflowStruct)
	RegisterWorkflow(executor, encodingWorkflowNilReturn)
	RegisterWorkflow(executor, encodingWorkflowInterface)

	err := Launch(executor)
	require.NoError(t, err)

	t.Run("BuiltinTypes", func(t *testing.T) {
		// Test a workflow that uses a built-in type (string)
		directHandle, err := RunWorkflow(executor, encodingWorkflowBuiltinTypes, "test")
		require.NoError(t, err)

		// Test result and error from direct handle
		directHandleResult, err := directHandle.GetResult()
		assert.Equal(t, "123", directHandleResult)
		require.Error(t, err)
		assert.Equal(t, "workflow error: step error", err.Error())

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[string](executor.(*dbosContext), directHandle.GetWorkflowID())
		require.NoError(t, err)
		retrievedResult, err := retrieveHandler.GetResult()
		assert.Equal(t, "123", retrievedResult)
		require.Error(t, err)
		assert.Equal(t, "workflow error: step error", err.Error())

		// Test results from ListWorkflows
		workflows, err := ListWorkflows(
			executor,
			WithWorkflowIDs([]string{directHandle.GetWorkflowID()}),
			WithLoadInput(true),
			WithLoadOutput(true),
		)
		require.NoError(t, err)
		require.Len(t, workflows, 1)
		workflow := workflows[0]
		require.NotNil(t, workflow.Input)
		workflowInput, ok := workflow.Input.(string)
		require.True(t, ok, "expected workflow input to be of type string, got %T", workflow.Input)
		assert.Equal(t, "test", workflowInput)
		require.NotNil(t, workflow.Output)
		workflowOutput, ok := workflow.Output.(string)
		require.True(t, ok, "expected workflow output to be of type string, got %T", workflow.Output)
		assert.Equal(t, "123", workflowOutput)
		require.NotNil(t, workflow.Error)
		assert.Equal(t, "workflow error: step error", workflow.Error.Error())

		// Test results from GetWorkflowSteps
		steps, err := GetWorkflowSteps(executor, directHandle.GetWorkflowID())
		require.NoError(t, err)
		require.Len(t, steps, 1)
		step := steps[0]
		require.NotNil(t, step.Output)
		stepOutput, ok := step.Output.(int)
		require.True(t, ok, "expected step output to be of type int, got %T", step.Output)
		assert.Equal(t, 123, stepOutput)
		require.NotNil(t, step.Error)
		assert.Equal(t, "step error", step.Error.Error())
	})

	t.Run("StructType", func(t *testing.T) {
		// Test a workflow that calls a step with struct types to verify serialization/deserialization
		input := WorkflowInputStruct{
			A: SimpleStruct{A: "test", B: 123},
			B: 456,
		}

		directHandle, err := RunWorkflow(executor, encodingWorkflowStruct, input)
		require.NoError(t, err)

		// Test result from direct handle
		directResult, err := directHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input.A.A, directResult.A.A.A)
		assert.Equal(t, input.A.B, directResult.A.A.B)
		assert.Equal(t, fmt.Sprintf("%d", input.B), directResult.A.B)
		assert.Equal(t, "processed by encodingStepStruct", directResult.B)

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[StepOutputStruct](executor.(*dbosContext), directHandle.GetWorkflowID())
		require.NoError(t, err)
		retrievedResult, err := retrieveHandler.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input.A.A, retrievedResult.A.A.A)
		assert.Equal(t, input.A.B, retrievedResult.A.A.B)
		assert.Equal(t, fmt.Sprintf("%d", input.B), retrievedResult.A.B)
		assert.Equal(t, "processed by encodingStepStruct", retrievedResult.B)

		// Test results from ListWorkflows
		workflows, err := ListWorkflows(executor,
			WithWorkflowIDs([]string{directHandle.GetWorkflowID()}),
			WithLoadInput(true),
			WithLoadOutput(true),
		)
		require.Len(t, workflows, 1)
		require.NoError(t, err)
		workflow := workflows[0]
		require.NotNil(t, workflow.Input)
		workflowInput, ok := workflow.Input.(WorkflowInputStruct)
		require.True(t, ok, "expected workflow input to be of type WorkflowInputStruct, got %T", workflow.Input)
		assert.Equal(t, input.A.A, workflowInput.A.A)
		assert.Equal(t, input.A.B, workflowInput.A.B)
		assert.Equal(t, input.B, workflowInput.B)

		workflowOutput, ok := workflow.Output.(StepOutputStruct)
		require.True(t, ok, "expected workflow output to be of type StepOutputStruct, got %T", workflow.Output)
		assert.Equal(t, input.A.A, workflowOutput.A.A.A)
		assert.Equal(t, input.A.B, workflowOutput.A.A.B)
		assert.Equal(t, fmt.Sprintf("%d", input.B), workflowOutput.A.B)
		assert.Equal(t, "processed by encodingStepStruct", workflowOutput.B)

		// Test results from GetWorkflowSteps
		steps, err := GetWorkflowSteps(executor, directHandle.GetWorkflowID())
		require.NoError(t, err)
		require.Len(t, steps, 1)
		step := steps[0]
		require.NotNil(t, step.Output)
		stepOutput, ok := step.Output.(StepOutputStruct)
		require.True(t, ok, "expected step output to be of type StepOutputStruct, got %T", step.Output)
		assert.Equal(t, input.A.A, stepOutput.A.A.A)
		assert.Equal(t, input.A.B, stepOutput.A.A.B)
		assert.Equal(t, fmt.Sprintf("%d", input.B), stepOutput.A.B)
		assert.Equal(t, "processed by encodingStepStruct", stepOutput.B)
		assert.Nil(t, step.Error)
	})

	t.Run("NilableTypes", func(t *testing.T) {
		// Test with non-nil values for all types
		directHandle, err := RunWorkflow(executor, encodingWorkflowNilReturn, false)
		require.NoError(t, err)

		// Test result from direct handle
		directResult, err := directHandle.GetResult()
		require.NoError(t, err)
		require.NotNil(t, directResult)
		// Verify that we got results for all types
		assert.Contains(t, directResult, "ptr:pointer result")

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[string](executor.(*dbosContext), directHandle.GetWorkflowID())
		require.NoError(t, err)
		retrievedResult, err := retrieveHandler.GetResult()
		require.NoError(t, err)
		assert.Contains(t, retrievedResult, "ptr:pointer result")

		// Test with nil values for all types
		nilHandle, err := RunWorkflow(executor, encodingWorkflowNilReturn, true)
		require.NoError(t, err)

		// Test nil result from direct handle
		nilDirectResult, err := nilHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "All nil types handled successfully", nilDirectResult)

		// Test nil result from polling handle
		nilRetrieveHandler, err := RetrieveWorkflow[string](executor.(*dbosContext), nilHandle.GetWorkflowID())
		require.NoError(t, err)
		nilRetrievedResult, err := nilRetrieveHandler.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "All nil types handled successfully", nilRetrievedResult)

		// Test results from GetWorkflowSteps to ensure all steps executed
		steps, err := GetWorkflowSteps(executor, directHandle.GetWorkflowID())
		require.NoError(t, err)
		assert.Equal(t, 1, len(steps), "Expected 1 step for nil-able types")
		for _, step := range steps {
			assert.Nil(t, step.Error, "No step should have errors")
		}
	})

	t.Run("LazyInterfaceRegistration", func(t *testing.T) {
		// Test a workflow that returns an interface with lazily registered concrete type
		directHandle, err := RunWorkflow(executor, encodingWorkflowInterface, "test-interface")
		require.NoError(t, err)

		// Test result from direct handle
		directResult, err := directHandle.GetResult()
		require.NoError(t, err)
		require.NotNil(t, directResult)
		assert.Equal(t, "Processed: test-interface", directResult.GetMessage())
		assert.Equal(t, 200, directResult.GetCode())

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[ResponseInterface](executor.(*dbosContext), directHandle.GetWorkflowID())
		require.NoError(t, err)
		retrievedResult, err := retrieveHandler.GetResult()
		require.NoError(t, err)
		require.NotNil(t, retrievedResult)
		assert.Equal(t, "Processed: test-interface", retrievedResult.GetMessage())
		assert.Equal(t, 200, retrievedResult.GetCode())

		// Test results from ListWorkflows
		workflows, err := ListWorkflows(
			executor,
			WithWorkflowIDs([]string{directHandle.GetWorkflowID()}),
			WithLoadInput(true),
			WithLoadOutput(true),
		)
		require.NoError(t, err)
		require.Len(t, workflows, 1)
		workflow := workflows[0]
		require.NotNil(t, workflow.Input)
		workflowInput, ok := workflow.Input.(string)
		require.True(t, ok, "expected workflow input to be of type string, got %T", workflow.Input)
		assert.Equal(t, "test-interface", workflowInput)
		require.NotNil(t, workflow.Output)
		// The output should be deserialized as ConcreteResponse since we registered it
		workflowOutput, ok := workflow.Output.(ConcreteResponse)
		require.True(t, ok, "expected workflow output to be of type ConcreteResponse, got %T", workflow.Output)
		assert.Equal(t, "Processed: test-interface", workflowOutput.Message)
		assert.Equal(t, 200, workflowOutput.Code)

		// Test results from GetWorkflowSteps
		steps, err := GetWorkflowSteps(executor, directHandle.GetWorkflowID())
		require.NoError(t, err)
		require.Len(t, steps, 1)
		step := steps[0]
		require.NotNil(t, step.Output)
		// The step output should also be ConcreteResponse
		stepOutput, ok := step.Output.(ConcreteResponse)
		require.True(t, ok, "expected step output to be of type ConcreteResponse, got %T", step.Output)
		assert.Equal(t, "Processed: test-interface", stepOutput.Message)
		assert.Equal(t, 200, stepOutput.Code)
		assert.Nil(t, step.Error)
	})
}

type UserDefinedEventData struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Details struct {
		Description string   `json:"description"`
		Tags        []string `json:"tags"`
	} `json:"details"`
}

func setEventUserDefinedTypeWorkflow(ctx DBOSContext, input string) (string, error) {
	eventData := UserDefinedEventData{
		ID:   42,
		Name: "test-event",
		Details: struct {
			Description string   `json:"description"`
			Tags        []string `json:"tags"`
		}{
			Description: "This is a test event with user-defined data",
			Tags:        []string{"test", "user-defined", "serialization"},
		},
	}

	err := SetEvent(ctx, input, eventData)
	if err != nil {
		return "", err
	}
	return "user-defined-event-set", nil
}

func TestSetEventSerialize(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflow with executor
	RegisterWorkflow(executor, setEventUserDefinedTypeWorkflow)

	t.Run("SetEventUserDefinedType", func(t *testing.T) {
		// Start a workflow that sets an event with a user-defined type
		setHandle, err := RunWorkflow(executor, setEventUserDefinedTypeWorkflow, "user-defined-key")
		require.NoError(t, err)

		// Wait for the workflow to complete
		result, err := setHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "user-defined-event-set", result)

		// Retrieve the event to verify it was properly serialized and can be deserialized
		retrievedEvent, err := GetEvent[UserDefinedEventData](executor, setHandle.GetWorkflowID(), "user-defined-key", 3*time.Second)
		require.NoError(t, err)

		// Verify the retrieved data matches what we set
		assert.Equal(t, 42, retrievedEvent.ID)
		assert.Equal(t, "test-event", retrievedEvent.Name)
		assert.Equal(t, "This is a test event with user-defined data", retrievedEvent.Details.Description)
		require.Len(t, retrievedEvent.Details.Tags, 3)
		expectedTags := []string{"test", "user-defined", "serialization"}
		assert.Equal(t, expectedTags, retrievedEvent.Details.Tags)
	})
}

func sendUserDefinedTypeWorkflow(ctx DBOSContext, destinationID string) (string, error) {
	// Create an instance of our user-defined type inside the workflow
	sendData := UserDefinedEventData{
		ID:   42,
		Name: "test-send-message",
		Details: struct {
			Description string   `json:"description"`
			Tags        []string `json:"tags"`
		}{
			Description: "This is a test send message with user-defined data",
			Tags:        []string{"test", "user-defined", "serialization", "send"},
		},
	}

	// Send should automatically register this type with gob
	err := Send(ctx, destinationID, sendData, "user-defined-topic")
	if err != nil {
		return "", err
	}
	return "user-defined-message-sent", nil
}

func recvUserDefinedTypeWorkflow(ctx DBOSContext, input string) (UserDefinedEventData, error) {
	// Receive the user-defined type message
	result, err := Recv[UserDefinedEventData](ctx, "user-defined-topic", 3*time.Second)
	return result, err
}

func TestSendSerialize(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflows with executor
	RegisterWorkflow(executor, sendUserDefinedTypeWorkflow)
	RegisterWorkflow(executor, recvUserDefinedTypeWorkflow)

	t.Run("SendUserDefinedType", func(t *testing.T) {
		// Start a receiver workflow first
		recvHandle, err := RunWorkflow(executor, recvUserDefinedTypeWorkflow, "recv-input")
		require.NoError(t, err)

		// Start a sender workflow that sends a message with a user-defined type
		sendHandle, err := RunWorkflow(executor, sendUserDefinedTypeWorkflow, recvHandle.GetWorkflowID())
		require.NoError(t, err)

		// Wait for the sender workflow to complete
		sendResult, err := sendHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "user-defined-message-sent", sendResult)

		// Wait for the receiver workflow to complete and get the message
		receivedData, err := recvHandle.GetResult()
		require.NoError(t, err)

		// Verify the received data matches what we sent
		assert.Equal(t, 42, receivedData.ID)
		assert.Equal(t, "test-send-message", receivedData.Name)
		assert.Equal(t, "This is a test send message with user-defined data", receivedData.Details.Description)

		// Verify tags
		expectedTags := []string{"test", "user-defined", "serialization", "send"}
		assert.Equal(t, expectedTags, receivedData.Details.Tags)
	})
}

func TestGobRegistrationWithConflictingTypes(t *testing.T) {
	// Create a fresh DBOS context for this test
	executor := setupDBOS(t, false, true) // Don't reset DB but do check for leaks

	// Test type definitions
	type TestType struct {
		Value string
	}

	type InnerType struct {
		ID int
	}
	type OuterType struct {
		Inner InnerType
		Name  string
	}

	// Test workflows with various type combinations
	workflow1 := func(ctx DBOSContext, input TestType) (TestType, error) {
		return input, nil
	}
	workflow2 := func(ctx DBOSContext, input *TestType) (*TestType, error) {
		return input, nil
	}
	workflow3 := func(ctx DBOSContext, input TestType) (TestType, error) {
		return TestType{Value: input.Value + "-modified"}, nil
	}
	workflow5 := func(ctx DBOSContext, input OuterType) (OuterType, error) {
		return input, nil
	}
	workflow6 := func(ctx DBOSContext, input *OuterType) (*OuterType, error) {
		return input, nil
	}
	workflow7 := func(ctx DBOSContext, input []TestType) ([]TestType, error) {
		return input, nil
	}
	workflow8 := func(ctx DBOSContext, input []*TestType) ([]*TestType, error) {
		return input, nil
	}
	workflow9 := func(ctx DBOSContext, input map[string]TestType) (map[string]TestType, error) {
		return input, nil
	}
	workflow10 := func(ctx DBOSContext, input map[string]*TestType) (map[string]*TestType, error) {
		return input, nil
	}

	// Register all workflows - this will trigger gob registration conflicts that should be handled gracefully
	RegisterWorkflow(executor, workflow1)
	RegisterWorkflow(executor, workflow2)
	RegisterWorkflow(executor, workflow3)
	RegisterWorkflow(executor, workflow5)
	RegisterWorkflow(executor, workflow6)
	RegisterWorkflow(executor, workflow7)
	RegisterWorkflow(executor, workflow8)
	RegisterWorkflow(executor, workflow9)
	RegisterWorkflow(executor, workflow10)

	// Launch and verify the system works
	err := Launch(executor)
	require.NoError(t, err, "failed to launch DBOS after gob conflict handling")
	defer Shutdown(executor, 10*time.Second)

	// Test workflows with value type and pointer type
	t.Run("ValueType", func(t *testing.T) {
		testValue := TestType{Value: "test"}
		handle, err := RunWorkflow(executor, workflow1, testValue)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testValue, result)
	})

	t.Run("PointerType", func(t *testing.T) {
		testPointer := &TestType{Value: "pointer"}
		handle, err := RunWorkflow(executor, workflow2, testPointer)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testPointer, result)
	})

	t.Run("ModifiedOutput", func(t *testing.T) {
		testValue := TestType{Value: "test"}
		handle, err := RunWorkflow(executor, workflow3, testValue)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, TestType{Value: "test-modified"}, result)
	})

	t.Run("NestedStruct", func(t *testing.T) {
		testOuter := OuterType{Inner: InnerType{ID: 42}, Name: "test"}
		handle, err := RunWorkflow(executor, workflow5, testOuter)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testOuter, result)
	})

	t.Run("NestedStructPointer", func(t *testing.T) {
		testOuterPtr := &OuterType{Inner: InnerType{ID: 43}, Name: "test-ptr"}
		handle, err := RunWorkflow(executor, workflow6, testOuterPtr)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testOuterPtr, result)
	})

	t.Run("SliceType", func(t *testing.T) {
		testSlice := []TestType{{Value: "a"}, {Value: "b"}}
		handle, err := RunWorkflow(executor, workflow7, testSlice)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testSlice, result)
	})

	t.Run("PointerSliceType", func(t *testing.T) {
		testPtrSlice := []*TestType{{Value: "a"}, {Value: "b"}}
		handle, err := RunWorkflow(executor, workflow8, testPtrSlice)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testPtrSlice, result)
	})

	t.Run("MapType", func(t *testing.T) {
		testMap := map[string]TestType{"key1": {Value: "value1"}}
		handle, err := RunWorkflow(executor, workflow9, testMap)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testMap, result)
	})

	t.Run("PointerMapType", func(t *testing.T) {
		testPtrMap := map[string]*TestType{"key1": {Value: "value1"}}
		handle, err := RunWorkflow(executor, workflow10, testPtrMap)
		require.NoError(t, err)
		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, testPtrMap, result)
	})
}

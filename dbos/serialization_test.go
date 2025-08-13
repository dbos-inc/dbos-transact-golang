package dbos

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/** Test serialization and deserialization
[x] Built in types
[x] User defined types (structs)
[x] Workflow inputs/outputs
[x] Step inputs/outputs
[x] Direct handlers, polling handler, list workflows results, get step infos
[x] Set/get event with user defined types
*/

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

func TestWorkflowEncoding(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflows with executor
	RegisterWorkflow(executor, encodingWorkflowBuiltinTypes)
	RegisterWorkflow(executor, encodingWorkflowStruct)

	t.Run("BuiltinTypes", func(t *testing.T) {
		// Test a workflow that uses a built-in type (string)
		directHandle, err := RunAsWorkflow(executor, encodingWorkflowBuiltinTypes, "test")
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
		workflows, err := ListWorkflows(executor, WithWorkflowIDs(
			[]string{directHandle.GetWorkflowID()},
		))
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
		steps, err := executor.(*dbosContext).systemDB.getWorkflowSteps(context.Background(), directHandle.GetWorkflowID())
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

		directHandle, err := RunAsWorkflow(executor, encodingWorkflowStruct, input)
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
		workflows, err := ListWorkflows(executor, WithWorkflowIDs(
			[]string{directHandle.GetWorkflowID()},
		))
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
		steps, err := executor.(*dbosContext).systemDB.getWorkflowSteps(context.Background(), directHandle.GetWorkflowID())
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

	err := SetEvent(ctx, GenericWorkflowSetEventInput[UserDefinedEventData]{Key: input, Message: eventData})
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
		setHandle, err := RunAsWorkflow(executor, setEventUserDefinedTypeWorkflow, "user-defined-key")
		require.NoError(t, err)

		// Wait for the workflow to complete
		result, err := setHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "user-defined-event-set", result)

		// Retrieve the event to verify it was properly serialized and can be deserialized
		retrievedEvent, err := GetEvent[UserDefinedEventData](executor, WorkflowGetEventInput{
			TargetWorkflowID: setHandle.GetWorkflowID(),
			Key:              "user-defined-key",
			Timeout:          3 * time.Second,
		})
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
	// Note the explicit type parameter since compiler cannot infer UserDefinedEventData from string input
	err := Send(ctx, GenericWorkflowSendInput[UserDefinedEventData]{
		DestinationID: destinationID,
		Topic:         "user-defined-topic",
		Message:       sendData,
	})
	if err != nil {
		return "", err
	}
	return "user-defined-message-sent", nil
}

func recvUserDefinedTypeWorkflow(ctx DBOSContext, input string) (UserDefinedEventData, error) {
	// Receive the user-defined type message
	result, err := Recv[UserDefinedEventData](ctx, WorkflowRecvInput{
		Topic:   "user-defined-topic",
		Timeout: 3 * time.Second,
	})
	return result, err
}

func TestSendSerialize(t *testing.T) {
	executor := setupDBOS(t, true, true)

	// Register workflows with executor
	RegisterWorkflow(executor, sendUserDefinedTypeWorkflow)
	RegisterWorkflow(executor, recvUserDefinedTypeWorkflow)

	t.Run("SendUserDefinedType", func(t *testing.T) {
		// Start a receiver workflow first
		recvHandle, err := RunAsWorkflow(executor, recvUserDefinedTypeWorkflow, "recv-input")
		require.NoError(t, err)

		// Start a sender workflow that sends a message with a user-defined type
		sendHandle, err := RunAsWorkflow(executor, sendUserDefinedTypeWorkflow, recvHandle.GetWorkflowID())
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

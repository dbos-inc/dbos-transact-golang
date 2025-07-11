package dbos

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

/** Test serialization and deserialization
[] Built in types
[] User defined types (structs)
[] Workflow inputs/outputs
[] Step inputs/outputs
[] Direct handlers, polling handler, list workflows results, get step infos
*/

var (
	builtinWf = WithWorkflow(encodingWorkflowBuiltinTypes)
	structWf  = WithWorkflow(encodingWorkflowStruct)
)

// Builtin types
func encodingStepBuiltinTypes(_ context.Context, input int) (int, error) {
	return input, errors.New("step error")
}

func encodingWorkflowBuiltinTypes(ctx context.Context, input string) (string, error) {
	stepResult, err := RunAsStep(ctx, encodingStepBuiltinTypes, 123)
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

func encodingWorkflowStruct(ctx context.Context, input WorkflowInputStruct) (StepOutputStruct, error) {
	return RunAsStep(ctx, encodingStepStruct, StepInputStruct{
		A: input.A,
		B: fmt.Sprintf("%d", input.B),
	})
}

func encodingStepStruct(ctx context.Context, input StepInputStruct) (StepOutputStruct, error) {
	return StepOutputStruct{
		A: input,
		B: "processed by encodingStepStruct",
	}, nil
}

func TestWorkflowEncoding(t *testing.T) {
	setupDBOS(t)

	t.Run("BuiltinTypes", func(t *testing.T) {
		// Test a workflow that uses a built-in type (string)
		directHandle, err := builtinWf(context.Background(), "test")
		if err != nil {
			t.Fatalf("failed to execute workflow: %v", err)
		}

		// Test result and error from direct handle
		directHandleResult, err := directHandle.GetResult(context.Background())
		if directHandleResult != "123" {
			t.Fatalf("expected direct handle result to be '123', got %v", directHandleResult)
		}
		if err.Error() != "workflow error: step error" {
			t.Fatalf("expected error to be 'workflow error: step error', got %v", err)
		}

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[string](directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to retrieve workflow: %v", err)
		}
		retrievedResult, err := retrieveHandler.GetResult(context.Background())
		if retrievedResult != "123" {
			t.Fatalf("expected retrieved result to be '123', got %v", retrievedResult)
		}
		if err.Error() != "workflow error: step error" {
			t.Fatalf("expected error to be 'workflow error: step error', got %v", err)
		}

		// Test results from ListWorkflows
		workflows, err := getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{directHandle.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}
		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow, got %d", len(workflows))
		}
		workflow := workflows[0]
		if workflow.Input == nil {
			t.Fatal("expected workflow input to be non-nil")
		}
		workflowInput, ok := workflow.Input.(string)
		if !ok {
			t.Fatalf("expected workflow input to be of type string, got %T", workflow.Input)
		}
		if workflowInput != "test" {
			t.Fatalf("expected workflow input to be 'test', got %v", workflowInput)
		}
		if workflow.Output == nil {
			t.Fatal("expected workflow output to be non-nil")
		}
		workflowOutput, ok := workflow.Output.(string)
		if !ok {
			t.Fatalf("expected workflow output to be of type string, got %T", workflow.Output)
		}
		if workflowOutput != "123" {
			t.Fatalf("expected workflow output to be '123', got %v", workflowOutput)
		}
		if workflow.Error == nil {
			t.Fatal("expected workflow error to be non-nil")
		}
		if workflow.Error.Error() != "workflow error: step error" {
			t.Fatalf("expected workflow error to be 'workflow error: step error', got %v", workflow.Error.Error())
		}

		// Test results from GetWorkflowSteps
		steps, err := getExecutor().systemDB.GetWorkflowSteps(context.Background(), directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step, got %d", len(steps))
		}
		step := steps[0]
		if step.Output == nil {
			t.Fatal("expected step output to be non-nil")
		}
		stepOutput, ok := step.Output.(int)
		if !ok {
			t.Fatalf("expected step output to be of type int, got %T", step.Output)
		}
		if stepOutput != 123 {
			t.Fatalf("expected step output to be 123, got %v", stepOutput)
		}
		if step.Error == nil {
			t.Fatal("expected step error to be non-nil")
		}
		if step.Error.Error() != "step error" {
			t.Fatalf("expected step error to be 'step error', got %v", step.Error.Error())
		}
	})

	t.Run("StructType", func(t *testing.T) {
		// Test a workflow that calls a step with struct types to verify serialization/deserialization
		input := WorkflowInputStruct{
			A: SimpleStruct{A: "test", B: 123},
			B: 456,
		}

		directHandle, err := structWf(context.Background(), input)
		if err != nil {
			t.Fatalf("failed to execute step workflow: %v", err)
		}

		// Test result from direct handle
		directResult, err := directHandle.GetResult(context.Background())
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if directResult.A.A.A != input.A.A {
			t.Fatalf("expected direct result input data name to be %v, got %v", input.A.A, directResult.A.A.A)
		}
		if directResult.A.A.B != input.A.B {
			t.Fatalf("expected direct result input data value to be %v, got %v", input.A.B, directResult.A.A.B)
		}
		if directResult.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected direct result input ID to be %v, got %v", fmt.Sprintf("%d", input.B), directResult.A.B)
		}
		if directResult.B != "processed by encodingStepStruct" {
			t.Fatalf("expected direct result step info to be 'processed by encodingStepStruct', got %v", directResult.B)
		}

		// Test result from polling handle
		retrieveHandler, err := RetrieveWorkflow[StepOutputStruct](directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to retrieve step workflow: %v", err)
		}
		retrievedResult, err := retrieveHandler.GetResult(context.Background())
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if retrievedResult.A.A.A != input.A.A {
			t.Fatalf("expected retrieved result input data name to be %v, got %v", input.A.A, retrievedResult.A.A.A)
		}
		if retrievedResult.A.A.B != input.A.B {
			t.Fatalf("expected retrieved result input data value to be %v, got %v", input.A.B, retrievedResult.A.A.B)
		}
		if retrievedResult.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected retrieved result input ID to be %v, got %v", fmt.Sprintf("%d", input.B), retrievedResult.A.B)
		}
		if retrievedResult.B != "processed by encodingStepStruct" {
			t.Fatalf("expected retrieved result step info to be 'processed by encodingStepStruct', got %v", retrievedResult.B)
		}

		// Test results from ListWorkflows
		workflows, err := getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{directHandle.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}
		workflow := workflows[0]
		if workflow.Input == nil {
			t.Fatal("expected workflow input to be non-nil")
		}
		workflowInput, ok := workflow.Input.(WorkflowInputStruct)
		if !ok {
			t.Fatalf("expected workflow input to be of type WorkflowInputStruct, got %T", workflow.Input)
		}
		if workflowInput.A.A != input.A.A {
			t.Fatalf("expected workflow input data name to be %v, got %v", input.A.A, workflowInput.A.A)
		}
		if workflowInput.A.B != input.A.B {
			t.Fatalf("expected workflow input data value to be %v, got %v", input.A.B, workflowInput.A.B)
		}
		if workflowInput.B != input.B {
			t.Fatalf("expected workflow input ID to be %v, got %v", input.B, workflowInput.B)
		}

		workflowOutput, ok := workflow.Output.(StepOutputStruct)
		if !ok {
			t.Fatalf("expected workflow output to be of type StepOutputStruct, got %T", workflow.Output)
		}
		if workflowOutput.A.A.A != input.A.A {
			t.Fatalf("expected workflow output input data name to be %v, got %v", input.A.A, workflowOutput.A.A.A)
		}
		if workflowOutput.A.A.B != input.A.B {
			t.Fatalf("expected workflow output input data value to be %v, got %v", input.A.B, workflowOutput.A.A.B)
		}
		if workflowOutput.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected workflow output input ID to be %v, got %v", fmt.Sprintf("%d", input.B), workflowOutput.A.B)
		}
		if workflowOutput.B != "processed by encodingStepStruct" {
			t.Fatalf("expected workflow output step info to be 'processed by encodingStepStruct', got %v", workflowOutput.B)
		}

		// Test results from GetWorkflowSteps
		steps, err := getExecutor().systemDB.GetWorkflowSteps(context.Background(), directHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps: %v", err)
		}
		if len(steps) != 1 {
			t.Fatalf("expected 1 step, got %d", len(steps))
		}
		step := steps[0]
		if step.Output == nil {
			t.Fatal("expected step output to be non-nil")
		}
		stepOutput, ok := step.Output.(StepOutputStruct)
		if !ok {
			t.Fatalf("expected step output to be of type StepOutputStruct, got %T", step.Output)
		}
		if stepOutput.A.A.A != input.A.A {
			t.Fatalf("expected step output input data name to be %v, got %v", input.A.A, stepOutput.A.A.A)
		}
		if stepOutput.A.A.B != input.A.B {
			t.Fatalf("expected step output input data value to be %v, got %v", input.A.B, stepOutput.A.A.B)
		}
		if stepOutput.A.B != fmt.Sprintf("%d", input.B) {
			t.Fatalf("expected step output input ID to be %v, got %v", fmt.Sprintf("%d", input.B), stepOutput.A.B)
		}
		if stepOutput.B != "processed by encodingStepStruct" {
			t.Fatalf("expected step output step info to be 'processed by encodingStepStruct', got %v", stepOutput.B)
		}
		if step.Error != nil {
			t.Fatalf("expected step error to be nil, got %v", step.Error)
		}
	})
}

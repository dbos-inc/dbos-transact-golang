package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Global counter for idempotency testing
var idempotencyCounter int64

func simpleWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(dbosCtx DBOSContext, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStep(ctx)
	})
}

func slowWorkflow(dbosCtx DBOSContext, sleepTime time.Duration) (string, error) {
	Sleep(dbosCtx, sleepTime)
	return "done", nil
}

func simpleStep(_ context.Context) (string, error) {
	return "from step", nil
}

func simpleStepError(_ context.Context) (string, error) {
	return "", fmt.Errorf("step failure")
}

func simpleWorkflowWithStepError(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStepError(ctx)
	})
}

func simpleWorkflowWithSchedule(dbosCtx DBOSContext, scheduledTime time.Time) (time.Time, error) {
	return scheduledTime, nil
}

// idempotencyWorkflow increments a global counter and returns the input
func incrementCounter(_ context.Context, value int64) (int64, error) {
	idempotencyCounter += value
	return idempotencyCounter, nil
}

// Unified struct that demonstrates both pointer and value receiver methods
type workflowStruct struct{}

// Pointer receiver method
func (w *workflowStruct) simpleWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	return simpleWorkflow(dbosCtx, input)
}

// Value receiver method on the same struct
func (w workflowStruct) simpleWorkflowValue(dbosCtx DBOSContext, input string) (string, error) {
	return input + "-value", nil
}

// interface for workflow methods
type TestWorkflowInterface interface {
	Execute(dbosCtx DBOSContext, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(dbosCtx DBOSContext, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

// Generic workflow function
func Identity[T any](dbosCtx DBOSContext, in T) (T, error) {
	return in, nil
}

func TestWorkflowsRegistration(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Setup workflows with executor
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflowError)
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStep)
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStepError)
	// struct methods
	s := workflowStruct{}
	RegisterWorkflow(dbosCtx, s.simpleWorkflow)
	RegisterWorkflow(dbosCtx, s.simpleWorkflowValue)
	// interface method workflow
	workflowIface := TestWorkflowInterface(&workflowImplementation{
		field: "example",
	})
	RegisterWorkflow(dbosCtx, workflowIface.Execute)
	// Generic workflow
	RegisterWorkflow(dbosCtx, Identity[int])
	// Closure with captured state
	prefix := "hello-"
	closureWorkflow := func(dbosCtx DBOSContext, in string) (string, error) {
		return prefix + in, nil
	}
	RegisterWorkflow(dbosCtx, closureWorkflow)
	// Anonymous workflow
	anonymousWorkflow := func(dbosCtx DBOSContext, in string) (string, error) {
		return "anonymous-" + in, nil
	}
	RegisterWorkflow(dbosCtx, anonymousWorkflow)

	type testCase struct {
		name           string
		workflowFunc   func(DBOSContext, string, ...WorkflowOption) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				result, err := handle.GetResult()
				_, err2 := handle.GetResult()
				if err2 == nil {
					return nil, fmt.Errorf("Second call to GetResult should return an error")
				}
				expectedErrorMsg := "workflow result channel is already closed. Did you call GetResult() twice on the same workflow handle?"
				if err2.Error() != expectedErrorMsg {
					return nil, fmt.Errorf("Unexpected error message: %v, expected: %s", err2, expectedErrorMsg)
				}
				return result, err
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflowError, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflowWithStep, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, s.simpleWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, s.simpleWorkflowValue, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, workflowIface.Execute, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, Identity, 42, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "42", // input not used in this case
			expectedResult: 42,
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, closureWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, anonymousWorkflow, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(dbosCtx DBOSContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := RunWorkflow(dbosCtx, simpleWorkflowWithStepError, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(dbosCtx, tc.input, WithWorkflowID(uuid.NewString()))

			if tc.expectError {
				require.Error(t, err, "expected error but got none")
				if tc.expectedError != "" {
					assert.Equal(t, tc.expectedError, err.Error())
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)
			}
		})
	}

	t.Run("DoubleRegistrationWithoutName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, true) // Don't reset DB but do check for leaks

		// First registration should work
		RegisterWorkflow(freshCtx, simpleWorkflow)

		// Second registration of the same workflow should panic with ConflictingRegistrationError
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration but got none")
			dbosErr, ok := r.(*DBOSError)
			require.True(t, ok, "expected panic to be *DBOSError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow)
	})

	t.Run("DoubleRegistrationWithCustomName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, true) // Don't reset DB but do check for leaks

		// First registration with custom name should work
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))

		// Second registration with same custom name should panic with ConflictingRegistrationError
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration with custom name but got none")
			dbosErr, ok := r.(*DBOSError)
			require.True(t, ok, "expected panic to be *DBOSError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))
	})

	t.Run("DifferentWorkflowsSameCustomName", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, true) // Don't reset DB but do check for leaks

		// First registration with custom name should work
		RegisterWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("same-name"))

		// Second registration of different workflow with same custom name should panic with ConflictingRegistrationError
		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from registering different workflows with same custom name but got none")
			dbosErr, ok := r.(*DBOSError)
			require.True(t, ok, "expected panic to be *DBOSError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		RegisterWorkflow(freshCtx, simpleWorkflowError, WithWorkflowName("same-name"))
	})

	t.Run("RegisterAfterLaunchPanics", func(t *testing.T) {
		// Create a fresh DBOS context for this test
		freshCtx := setupDBOS(t, false, true) // Don't reset DB but do check for leaks

		// Launch DBOS context
		err := Launch(freshCtx)
		require.NoError(t, err)
		defer Shutdown(freshCtx, 10*time.Second)

		// Attempting to register after launch should panic
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from registration after launch but got none")
			}
		}()
		RegisterWorkflow(freshCtx, simpleWorkflow)
	})
}

func stepWithinAStep(ctx context.Context) (string, error) {
	return simpleStep(ctx)
}

func stepWithinAStepWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepWithinAStep(ctx)
	})
}

// Global counter for retry testing
var stepRetryAttemptCount int

func stepRetryAlwaysFailsStep(_ context.Context) (string, error) {
	stepRetryAttemptCount++
	return "", fmt.Errorf("always fails - attempt %d", stepRetryAttemptCount)
}

var stepIdempotencyCounter int

func stepIdempotencyTest(_ context.Context) (string, error) {
	stepIdempotencyCounter++
	return "", nil
}

func stepRetryWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepIdempotencyTest(ctx)
	})

	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepRetryAlwaysFailsStep(ctx)
	}, WithStepMaxRetries(5), WithBaseInterval(1*time.Millisecond), WithMaxInterval(10*time.Millisecond))
}

func step1(_ context.Context) (string, error) {
	return "", nil
}

func testStepWf1(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, step1)
}

func step2(_ context.Context) (string, error) {
	return "", nil
}

func testStepWf2(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, step2)
}

func TestSteps(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Create workflows with executor
	RegisterWorkflow(dbosCtx, stepWithinAStepWorkflow)
	RegisterWorkflow(dbosCtx, stepRetryWorkflow)
	RegisterWorkflow(dbosCtx, testStepWf1)
	RegisterWorkflow(dbosCtx, testStepWf2)
	// Create a workflow that uses custom step names
	customNameWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
		// Run a step with a custom name
		result1, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return "custom-step-1-result", nil
		}, WithStepName("MyCustomStep1"))
		if err != nil {
			return "", err
		}

		// Run another step with a different custom name
		result2, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return "custom-step-2-result", nil
		}, WithStepName("MyCustomStep2"))
		if err != nil {
			return "", err
		}

		return result1 + "-" + result2, nil
	}

	RegisterWorkflow(dbosCtx, customNameWorkflow)

	// Define user-defined types for testing serialization
	type StepInput struct {
		Name      string            `json:"name"`
		Count     int               `json:"count"`
		Active    bool              `json:"active"`
		Metadata  map[string]string `json:"metadata"`
		CreatedAt time.Time         `json:"created_at"`
	}

	type StepOutput struct {
		ProcessedName string    `json:"processed_name"`
		TotalCount    int       `json:"total_count"`
		Success       bool      `json:"success"`
		ProcessedAt   time.Time `json:"processed_at"`
		Details       []string  `json:"details"`
	}

	// Create a step function that accepts StepInput and returns StepOutput
	processUserObjectStep := func(_ context.Context, input StepInput) (StepOutput, error) {
		// Process the input and create output
		output := StepOutput{
			ProcessedName: fmt.Sprintf("Processed_%s", input.Name),
			TotalCount:    input.Count * 2,
			Success:       input.Active,
			ProcessedAt:   time.Now(),
			Details:       []string{"step1", "step2", "step3"},
		}

		// Verify input was correctly deserialized
		if input.Metadata == nil {
			return StepOutput{}, fmt.Errorf("metadata map was not properly deserialized")
		}

		return output, nil
	}

	// Create a workflow that uses the step with user-defined objects
	userObjectWorkflow := func(dbosCtx DBOSContext, workflowInput string) (string, error) {
		// Create input for the step
		stepInput := StepInput{
			Name:   workflowInput,
			Count:  42,
			Active: true,
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			CreatedAt: time.Now(),
		}

		// Run the step with user-defined input and output
		output, err := RunAsStep(dbosCtx, func(ctx context.Context) (StepOutput, error) {
			return processUserObjectStep(ctx, stepInput)
		})
		if err != nil {
			return "", fmt.Errorf("step failed: %w", err)
		}

		// Verify the output was correctly returned
		if output.ProcessedName == "" {
			return "", fmt.Errorf("output ProcessedName is empty")
		}
		if output.TotalCount != 84 {
			return "", fmt.Errorf("expected TotalCount to be 84, got %d", output.TotalCount)
		}
		if len(output.Details) != 3 {
			return "", fmt.Errorf("expected 3 details, got %d", len(output.Details))
		}

		return "", nil
	}
	// Register the workflow
	RegisterWorkflow(dbosCtx, userObjectWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("StepsMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run a step outside of a workflow context
		_, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
		require.Error(t, err, "expected error when running step outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		require.Equal(t, StepExecutionError, dbosErr.Code, "expected error code to be StepExecutionError, got %v", dbosErr.Code)

		// Test the specific message from the 3rd argument
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("StepWithinAStepAreJustFunctions", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, stepWithinAStepWorkflow, "test")
		require.NoError(t, err, "failed to run step within a step")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from step within a step")
		assert.Equal(t, "from step", result)

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to list steps")
		require.Len(t, steps, 1, "expected 1 step, got %d", len(steps))
	})

	t.Run("StepRetryWithExponentialBackoff", func(t *testing.T) {
		// Reset the global counters before test
		stepRetryAttemptCount = 0
		stepIdempotencyCounter = 0

		// Execute the workflow
		handle, err := RunWorkflow(dbosCtx, stepRetryWorkflow, "test")
		require.NoError(t, err, "failed to start retry workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from failing workflow but got none")

		// Verify the step was called exactly 6 times (max attempts + 1 initial attempt)
		assert.Equal(t, 6, stepRetryAttemptCount, "expected 6 attempts")

		// Verify the error is a MaxStepRetriesExceeded error
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)

		assert.Equal(t, MaxStepRetriesExceeded, dbosErr.Code, "expected error code to be MaxStepRetriesExceeded")

		// Verify the error contains the step name and max retries
		expectedErrorMessage := "has exceeded its maximum of 5 retries"
		assert.Contains(t, dbosErr.Message, expectedErrorMessage, "expected error message to contain expected text")

		// Verify each error message is present in the joined error
		for i := 1; i <= 5; i++ {
			expectedMsg := fmt.Sprintf("always fails - attempt %d", i)
			assert.Contains(t, dbosErr.Error(), expectedMsg, "expected joined error to contain expected message")
		}

		// Verify that the failed step was still recorded in the database
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")

		require.Len(t, steps, 2, "expected 2 recorded steps")

		// Verify the second step has the error
		step := steps[1]
		require.NotNil(t, step.Error, "expected error in recorded step, got none")

		assert.Equal(t, dbosErr.Error(), step.Error.Error(), "expected recorded step error to match joined error")

		// Verify the idempotency step was executed only once
		assert.Equal(t, 1, stepIdempotencyCounter, "expected idempotency step to be executed only once")
	})

	t.Run("checkStepName", func(t *testing.T) {
		// Run first workflow with custom step name
		handle1, err := RunWorkflow(dbosCtx, testStepWf1, "test-input-1")
		require.NoError(t, err, "failed to run testStepWf1")
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from testStepWf1")

		// Run second workflow with custom step name
		handle2, err := RunWorkflow(dbosCtx, testStepWf2, "test-input-2")
		require.NoError(t, err, "failed to run testStepWf2")
		_, err = handle2.GetResult()
		require.NoError(t, err, "failed to get result from testStepWf2")

		// Get workflow steps for first workflow and check step name
		steps1, err := GetWorkflowSteps(dbosCtx, handle1.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for testStepWf1")
		require.Len(t, steps1, 1, "expected 1 step in testStepWf1")
		s1 := steps1[0]
		expectedStepName1 := runtime.FuncForPC(reflect.ValueOf(step1).Pointer()).Name()
		assert.Equal(t, expectedStepName1, s1.StepName, "expected step name to match runtime function name")

		// Get workflow steps for second workflow and check step name
		steps2, err := GetWorkflowSteps(dbosCtx, handle2.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for testStepWf2")
		require.Len(t, steps2, 1, "expected 1 step in testStepWf2")
		s2 := steps2[0]
		expectedStepName2 := runtime.FuncForPC(reflect.ValueOf(step2).Pointer()).Name()
		assert.Equal(t, expectedStepName2, s2.StepName, "expected step name to match runtime function name")
	})

	t.Run("customStepNames", func(t *testing.T) {

		// Execute the workflow
		handle, err := RunWorkflow(dbosCtx, customNameWorkflow, "test-input")
		require.NoError(t, err, "failed to run workflow with custom step names")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow with custom step names")
		assert.Equal(t, "custom-step-1-result-custom-step-2-result", result)

		// Verify the custom step names were recorded
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps")

		// Check that the first step has the custom name
		assert.Equal(t, "MyCustomStep1", steps[0].StepName, "expected first step to have custom name")
		assert.Equal(t, 0, steps[0].StepID)

		// Check that the second step has the custom name
		assert.Equal(t, "MyCustomStep2", steps[1].StepName, "expected second step to have custom name")
		assert.Equal(t, 1, steps[1].StepID)
	})

	t.Run("stepsOutputEncoding", func(t *testing.T) {
		// Execute the workflow
		handle, err := RunWorkflow(dbosCtx, userObjectWorkflow, "TestObject")
		require.NoError(t, err, "failed to run workflow with user-defined objects")

		// Get the result
		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")

		// Verify the step was recorded
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step")

		// Verify step output was properly serialized and stored
		step := steps[0]
		require.NotNil(t, step.Output, "step output should not be nil")
		assert.Nil(t, step.Error)

		// Deserialize the output from the database to verify proper encoding
		// Use json.Unmarshal to handle JSON encode/decode round-trip
		var storedOutput StepOutput
		err = json.Unmarshal([]byte(step.Output.(string)), &storedOutput)
		require.NoError(t, err, "failed to decode step output to StepOutput")

		// Verify all fields were correctly serialized and deserialized
		assert.Equal(t, "Processed_TestObject", storedOutput.ProcessedName, "ProcessedName not correctly serialized")
		assert.Equal(t, 84, storedOutput.TotalCount, "TotalCount not correctly serialized")
		assert.True(t, storedOutput.Success, "Success flag not correctly serialized")
		assert.Len(t, storedOutput.Details, 3, "Details array length incorrect")
		assert.Equal(t, []string{"step1", "step2", "step3"}, storedOutput.Details, "Details array not correctly serialized")
		assert.False(t, storedOutput.ProcessedAt.IsZero(), "ProcessedAt timestamp should not be zero")
	})
}

func TestChildWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	type Inheritance struct {
		ParentID string
		Index    int
	}

	// Create child workflows with executor
	childWf := func(ctx DBOSContext, input Inheritance) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}
		expectedCurrentID := fmt.Sprintf("%s-0", input.ParentID)
		if workflowID != expectedCurrentID {
			return "", fmt.Errorf("expected childWf workflow ID to be %s, got %s", expectedCurrentID, workflowID)
		}
		// Steps of a child workflow start with an incremented step ID, because the first step ID is allocated to the child workflow
		return RunAsStep(ctx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	RegisterWorkflow(dbosCtx, childWf)

	parentWf := func(ctx DBOSContext, input Inheritance) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}

		childHandle, err := RunWorkflow(ctx, childWf, Inheritance{ParentID: workflowID})
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		// Check this wf ID is built correctly
		expectedParentID := fmt.Sprintf("%s-%d", input.ParentID, input.Index)
		if workflowID != expectedParentID {
			return "", fmt.Errorf("expected parentWf workflow ID to be %s, got %s", expectedParentID, workflowID)
		}
		res, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get result from child workflow: %w", err)
		}

		// Check the steps from this workflow
		steps, err := GetWorkflowSteps(ctx, workflowID)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow steps: %w", err)
		}
		if len(steps) != 2 {
			return "", fmt.Errorf("expected 2 recorded steps, got %d", len(steps))
		}
		// Verify the first step is the child workflow
		if steps[0].StepID != 0 {
			return "", fmt.Errorf("expected first step ID to be 0, got %d", steps[0].StepID)
		}
		if steps[0].StepName != runtime.FuncForPC(reflect.ValueOf(childWf).Pointer()).Name() {
			return "", fmt.Errorf("expected first step to be child workflow, got %s", steps[0].StepName)
		}
		if steps[0].Output != nil {
			return "", fmt.Errorf("expected first step output to be nil, got %s", steps[0].Output)
		}
		if steps[1].Error != nil {
			return "", fmt.Errorf("expected second step error to be nil, got %s", steps[1].Error)
		}
		if steps[0].ChildWorkflowID != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("expected first step child workflow ID to be %s, got %s", childHandle.GetWorkflowID(), steps[0].ChildWorkflowID)
		}

		// The second step is the result from the child workflow
		if steps[1].StepID != 1 {
			return "", fmt.Errorf("expected second step ID to be 1, got %d", steps[1].StepID)
		}
		if steps[1].StepName != "DBOS.getResult" {
			return "", fmt.Errorf("expected second step name to be getResult, got %s", steps[1].StepName)
		}
		var stepOutput string
		err = json.Unmarshal([]byte(steps[1].Output.(string)), &stepOutput)
		if err != nil {
			return "", fmt.Errorf("failed to unmarshal step output: %w", err)
		}
		if stepOutput != "from step" {
			return "", fmt.Errorf("expected second step output to be 'from step', got %s", steps[1].Output)
		}
		if steps[1].Error != nil {
			return "", fmt.Errorf("expected second step error to be nil, got %s", steps[1].Error)
		}
		if steps[1].ChildWorkflowID != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("expected second step child workflow ID to be %s, got %s", childHandle.GetWorkflowID(), steps[1].ChildWorkflowID)
		}

		return res, nil
	}
	RegisterWorkflow(dbosCtx, parentWf)

	grandParentWf := func(ctx DBOSContext, r int) (string, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}

		// 2 steps per loop: spawn child and get result
		for i := range r {
			expectedStepID := (2 * i)
			parentHandle, err := RunWorkflow(ctx, parentWf, Inheritance{ParentID: workflowID, Index: expectedStepID})
			if err != nil {
				return "", fmt.Errorf("failed to run parent workflow: %w", err)
			}

			// Verify parent (this workflow's child) ID follows the pattern: parentID-functionID
			parentWorkflowID := parentHandle.GetWorkflowID()

			expectedParentID := fmt.Sprintf("%s-%d", workflowID, expectedStepID)
			if parentWorkflowID != expectedParentID {
				return "", fmt.Errorf("expected parent workflow ID to be %s, got %s", expectedParentID, parentWorkflowID)
			}

			result, err := parentHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from parent workflow: %w", err)
			}
			if result != "from step" {
				return "", fmt.Errorf("expected result from parent workflow to be 'from step', got %s", result)
			}

		}
		// Check the steps from this workflow
		steps, err := GetWorkflowSteps(ctx, workflowID)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow steps: %w", err)
		}
		if len(steps) != r*2 {
			return "", fmt.Errorf("expected 2 recorded steps, got %d", len(steps))
		}

		// We do expect the steps to be returned in the order of execution, which seems to be the case even without an ORDER BY function_id ASC clause in the SQL query
		for i := 0; i < r; i += 2 {
			expectedStepID := i
			expectedChildID := fmt.Sprintf("%s-%d", workflowID, i)
			childWfStep := steps[i]
			getResultStep := steps[i+1]

			if childWfStep.StepID != expectedStepID {
				return "", fmt.Errorf("expected child wf step ID to be %d, got %d", expectedStepID, childWfStep.StepID)
			}
			if getResultStep.StepID != expectedStepID+1 {
				return "", fmt.Errorf("expected get result step ID to be %d, got %d", expectedStepID+1, getResultStep.StepID)
			}
			expectedName := runtime.FuncForPC(reflect.ValueOf(parentWf).Pointer()).Name()
			if childWfStep.StepName != expectedName {
				return "", fmt.Errorf("expected child wf step name to be %s, got %s", expectedName, childWfStep.StepName)
			}
			expectedName = "DBOS.getResult"
			if getResultStep.StepName != expectedName {
				return "", fmt.Errorf("expected get result step name to be %s, got %s", expectedName, getResultStep.StepName)
			}

			if childWfStep.Output != nil {
				return "", fmt.Errorf("expected child wf step output to be nil, got %s", childWfStep.Output)
			}
			var stepOutput string
			err = json.Unmarshal([]byte(getResultStep.Output.(string)), &stepOutput)
			if err != nil {
				return "", fmt.Errorf("failed to unmarshal step output: %w", err)
			}
			if stepOutput != "from step" {
				return "", fmt.Errorf("expected get result step output to be 'from step', got %s", getResultStep.Output)
			}

			if childWfStep.Error != nil {
				return "", fmt.Errorf("expected child wf step error to be nil, got %s", childWfStep.Error)
			}
			if getResultStep.Error != nil {
				return "", fmt.Errorf("expected get result step error to be nil, got %s", getResultStep.Error)
			}
			if childWfStep.ChildWorkflowID != expectedChildID {
				return "", fmt.Errorf("expected step child workflow ID to be %s, got %s", expectedChildID, childWfStep.ChildWorkflowID)
			}
			if getResultStep.ChildWorkflowID != expectedChildID {
				return "", fmt.Errorf("expected step child workflow ID to be %s, got %s", expectedChildID, getResultStep.ChildWorkflowID)
			}
		}

		return "", nil
	}
	RegisterWorkflow(dbosCtx, grandParentWf)

	// Register workflows needed for ChildWorkflowWithCustomID test
	simpleChildWf := func(dbosCtx DBOSContext, input string) (string, error) {
		return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	RegisterWorkflow(dbosCtx, simpleChildWf)

	// Register workflows needed for RecoveredChildWorkflowPollingHandle test
	var pollingHandleCompleteEvent *Event
	pollingHandleChildWf := func(dbosCtx DBOSContext, input string) (string, error) {
		// Wait if event is set
		if pollingHandleCompleteEvent != nil {
			pollingHandleCompleteEvent.Wait()
		}
		return input + "-result", nil
	}
	RegisterWorkflow(dbosCtx, pollingHandleChildWf)

	var pollingCounter int
	var pollingHandleStartEvent *Event
	pollingHandleParentWf := func(ctx DBOSContext, input string) (string, error) {
		pollingCounter++

		// Run child workflow with a known ID
		childHandle, err := RunWorkflow(ctx, pollingHandleChildWf, "child-input", WithWorkflowID("known-child-workflow-id"))
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		switch pollingCounter {
		case 1:
			// First handle will be a direct handle
			_, ok := childHandle.(*workflowHandle[string])
			if !ok {
				return "", fmt.Errorf("expected child handle to be of type workflowDirectHandle, got %T", childHandle)
			}
			// Signal the child workflow is started
			if pollingHandleStartEvent != nil {
				pollingHandleStartEvent.Set()
			}

			result, err := childHandle.GetResult()
			if err != nil {
				return "", fmt.Errorf("failed to get result from child workflow: %w", err)
			}
			return result, nil
		case 2:
			// Second handle will be a polling handle
			_, ok := childHandle.(*workflowPollingHandle[string])
			if !ok {
				return "", fmt.Errorf("expected recovered child handle to be of type workflowPollingHandle, got %T", childHandle)
			}
		}
		return "", nil
	}
	RegisterWorkflow(dbosCtx, pollingHandleParentWf)

	// Register workflows needed for ChildWorkflowCannotBeSpawnedFromStep test
	childWfForStepTest := func(dbosCtx DBOSContext, input string) (string, error) {
		return "child-result", nil
	}
	RegisterWorkflow(dbosCtx, childWfForStepTest)

	parentWfForStepTest := func(ctx DBOSContext, input string) (string, error) {
		return RunAsStep(ctx, func(context context.Context) (string, error) {
			dbosCtx := context.(DBOSContext)
			_, err := RunWorkflow(dbosCtx, childWfForStepTest, input)
			if err != nil {
				return "", err
			}
			return "should-not-reach", nil
		})
	}
	RegisterWorkflow(dbosCtx, parentWfForStepTest)
	// Simple parent that starts one child with a custom workflow ID
	simpleParentWf := func(ctx DBOSContext, customChildID string) (string, error) {
		childHandle, err := RunWorkflow(ctx, simpleChildWf, "test-child-input", WithWorkflowID(customChildID))
		if err != nil {
			return "", fmt.Errorf("failed to run child workflow: %w", err)
		}

		result, err := childHandle.GetResult()
		if err != nil {
			return "", fmt.Errorf("failed to get result from child workflow: %w", err)
		}

		return result, nil
	}

	RegisterWorkflow(dbosCtx, simpleParentWf)

	// Launch the context once for all subtests
	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ChildWorkflowIDGeneration", func(t *testing.T) {
		r := 3
		h, err := RunWorkflow(dbosCtx, grandParentWf, r)
		require.NoError(t, err, "failed to execute grand parent workflow")
		_, err = h.GetResult()
		require.NoError(t, err, "failed to get result from grand parent workflow")
	})

	t.Run("ChildWorkflowWithCustomID", func(t *testing.T) {
		customChildID := uuid.NewString()

		parentHandle, err := RunWorkflow(dbosCtx, simpleParentWf, customChildID)
		require.NoError(t, err, "failed to start parent workflow")

		result, err := parentHandle.GetResult()
		require.NoError(t, err, "failed to get result from parent workflow")
		require.Equal(t, "from step", result)

		// Verify the child workflow was recorded as step 0
		steps, err := GetWorkflowSteps(dbosCtx, parentHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 recorded steps, got %d", len(steps))

		// Verify first step is the child workflow with stepID=0
		require.Equal(t, 0, steps[0].StepID)
		require.Equal(t, runtime.FuncForPC(reflect.ValueOf(simpleChildWf).Pointer()).Name(), steps[0].StepName)
		require.Equal(t, customChildID, steps[0].ChildWorkflowID)

		// Verify second step is the getResult call with stepID=1
		require.Equal(t, 1, steps[1].StepID)
		require.Equal(t, "DBOS.getResult", steps[1].StepName)
		require.Equal(t, customChildID, steps[1].ChildWorkflowID)
	})

	t.Run("RecoveredChildWorkflowPollingHandle", func(t *testing.T) {
		// Reset counter and set up events for this test
		pollingCounter = 0
		pollingHandleStartEvent = NewEvent()
		pollingHandleCompleteEvent = NewEvent()
		knownChildID := "known-child-workflow-id"
		knownParentID := "known-parent-workflow-id"

		// Execute parent workflow - it will block after starting the child
		parentHandle, err := RunWorkflow(dbosCtx, pollingHandleParentWf, "parent-input", WithWorkflowID(knownParentID))
		require.NoError(t, err, "failed to start parent workflow")

		// Wait for the workflows to start
		pollingHandleStartEvent.Wait()

		// Recover pending workflows - this should give us both parent and child handles
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")

		// Should have recovered both parent and child workflows
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles (parent and child), got %d", len(recoveredHandles))

		// Find the child handle and verify it's a polling handle with the correct ID
		var childRecoveredHandle WorkflowHandle[any]
		for _, handle := range recoveredHandles {
			if handle.GetWorkflowID() == knownChildID {
				childRecoveredHandle = handle
				break
			}
		}

		require.NotNil(t, childRecoveredHandle, "failed to find recovered child workflow handle with ID %s", knownChildID)

		// Complete both workflows
		pollingHandleCompleteEvent.Set()
		result, err := parentHandle.GetResult()
		require.NoError(t, err, "failed to get result from original parent workflow")
		require.Equal(t, "child-input-result", result)
		childResult, err := childRecoveredHandle.GetResult()
		require.NoError(t, err, "failed to get result from recovered child handle")
		require.Equal(t, result, childResult)
	})

	t.Run("ChildWorkflowCannotBeSpawnedFromStep", func(t *testing.T) {
		// Execute the workflow - should fail when step tries to spawn child workflow
		handle, err := RunWorkflow(dbosCtx, parentWfForStepTest, "test-input")
		require.NoError(t, err, "failed to start parent workflow")

		// Expect the workflow to fail
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when spawning child workflow from step, but got none")

		// Check the error type and message
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code, "expected error code to be StepExecutionError, got %v", dbosErr.Code)

		expectedMessagePart := "cannot spawn child workflow from within a step"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})
}

// Idempotency workflows moved to test functions

func idempotencyWorkflow(dbosCtx DBOSContext, input string) (string, error) {
	RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
		return incrementCounter(ctx, int64(1))
	})
	return input, nil
}

func TestWorkflowIdempotency(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, idempotencyWorkflow)

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "idempotency-test"

		// Execute the same workflow twice with the same ID
		// First execution
		handle1, err := RunWorkflow(dbosCtx, idempotencyWorkflow, input, WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to execute workflow first time")
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first execution")

		// Second execution with the same workflow ID
		handle2, err := RunWorkflow(dbosCtx, idempotencyWorkflow, input, WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to execute workflow second time")
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second execution")

		require.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID())

		// Verify the second handle is a polling handle
		_, ok := handle2.(*workflowPollingHandle[string])
		require.True(t, ok, "expected handle2 to be of type workflowPollingHandle, got %T", handle2)

		// Verify both executions return the same result
		require.Equal(t, result1, result2)

		// Verify the counter was only incremented once (idempotency)
		require.Equal(t, int64(1), idempotencyCounter, "expected counter to be 1 (workflow executed only once)")
	})
}

func TestWorkflowRecovery(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	var (
		recoveryCounters   []int64
		recoveryEvents     []*Event
		blockingEvents     []*Event
		secondStepErrors   []error
		secondStepErrorsMu sync.Mutex
	)

	recoveryWorkflow := func(dbosCtx DBOSContext, index int) (int64, error) {
		// First step with custom name - increments the counter
		_, err := RunAsStep(dbosCtx, func(ctx context.Context) (int64, error) {
			recoveryCounters[index]++
			return recoveryCounters[index], nil
		}, WithStepName(fmt.Sprintf("IncrementStep-%d", index)))
		if err != nil {
			return 0, err
		}

		// Signal that first step is complete
		recoveryEvents[index].Set()

		// Second step with custom name - blocks until signaled
		_, err = RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
			blockingEvents[index].Wait()
			return fmt.Sprintf("completed-%d", index), nil
		}, WithStepName(fmt.Sprintf("BlockingStep-%d", index)))
		if err != nil {
			secondStepErrorsMu.Lock()
			secondStepErrors = append(secondStepErrors, err)
			secondStepErrorsMu.Unlock()
			return 0, err
		}

		return recoveryCounters[index], nil
	}

	RegisterWorkflow(dbosCtx, recoveryWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("WorkflowRecovery", func(t *testing.T) {
		const numWorkflows = 5

		// Initialize slices for multiple workflows
		recoveryCounters = make([]int64, numWorkflows)
		recoveryEvents = make([]*Event, numWorkflows)
		blockingEvents = make([]*Event, numWorkflows)
		secondStepErrors = make([]error, 0)

		// Create events for each workflow
		for i := range numWorkflows {
			recoveryEvents[i] = NewEvent()
			blockingEvents[i] = NewEvent()
		}

		// Start all workflows
		handles := make([]WorkflowHandle[int64], numWorkflows)
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, recoveryWorkflow, i, WithWorkflowID(fmt.Sprintf("recovery-test-%d", i)))
			require.NoError(t, err, "failed to start workflow %d", i)
			handles[i] = handle
		}

		// Wait for all first steps to complete
		for i := range numWorkflows {
			recoveryEvents[i].Wait()
		}

		// Verify step states before recovery
		for i := range numWorkflows {
			steps, err := GetWorkflowSteps(dbosCtx, handles[i].GetWorkflowID())
			require.NoError(t, err, "failed to get steps for workflow %d", i)
			require.Len(t, steps, 1, "expected 1 completed step for workflow %d before recovery", i)

			// Verify first step has custom name and completed
			assert.Equal(t, fmt.Sprintf("IncrementStep-%d", i), steps[0].StepName, "workflow %d first step name mismatch", i)
			assert.Equal(t, 0, steps[0].StepID, "workflow %d first step ID should be 0", i)
			assert.NotNil(t, steps[0].Output, "workflow %d first step should have output", i)
			assert.Nil(t, steps[0].Error, "workflow %d first step should not have error", i)
		}

		// Verify counters are all 1 (executed once)
		for i := range numWorkflows {
			require.Equal(t, int64(1), recoveryCounters[i], "workflow %d counter should be 1 before recovery", i)
		}

		// Run recovery
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, numWorkflows, "expected %d recovered handles, got %d", numWorkflows, len(recoveredHandles))

		// Create a map for easy lookup of recovered handles
		recoveredMap := make(map[string]WorkflowHandle[any])
		for _, h := range recoveredHandles {
			recoveredMap[h.GetWorkflowID()] = h
		}

		// Verify all original workflows were recovered
		for i := range numWorkflows {
			originalID := handles[i].GetWorkflowID()
			recoveredHandle, found := recoveredMap[originalID]
			require.True(t, found, "workflow %d with ID %s not found in recovered handles", i, originalID)

			_, ok := recoveredHandle.(*workflowPollingHandle[any])
			require.True(t, ok, "recovered handle %d should be of type workflowPollingHandle, got %T", i, recoveredHandle)
		}

		// Verify first steps were NOT re-executed (counters should still be 1)
		for i := range numWorkflows {
			require.Equal(t, int64(1), recoveryCounters[i], "workflow %d counter should remain 1 after recovery (idempotent)", i)
		}

		// Verify workflow attempts increased to 2
		for i := range numWorkflows {
			workflows, err := dbosCtx.(*dbosContext).systemDB.listWorkflows(context.Background(), listWorkflowsDBInput{
				workflowIDs: []string{handles[i].GetWorkflowID()},
			})
			require.NoError(t, err, "failed to list workflow %d", i)
			require.Len(t, workflows, 1, "expected 1 workflow entry for workflow %d", i)
			assert.Equal(t, 2, workflows[0].Attempts, "workflow %d should have 2 attempts after recovery", i)
		}

		// Unblock all workflows and verify they complete
		for i := range numWorkflows {
			blockingEvents[i].Set()
		}

		// Get results from all recovered workflows
		for i := range numWorkflows {
			recoveredHandle := recoveredMap[handles[i].GetWorkflowID()]
			result, err := recoveredHandle.GetResult()
			require.NoError(t, err, "failed to get result from recovered workflow %d", i)

			// Result should be the counter value (1) as float64
			require.Equal(t, float64(1), result, "workflow %d result should be 1", i)
		}

		// Final verification of step states
		for i := range numWorkflows {
			steps, err := GetWorkflowSteps(dbosCtx, handles[i].GetWorkflowID())
			require.NoError(t, err, "failed to get final steps for workflow %d", i)
			require.Len(t, steps, 2, "expected 2 steps for workflow %d", i)

			// Both steps should now be completed
			assert.NotNil(t, steps[0].Output, "workflow %d first step should have output", i)
			assert.NotNil(t, steps[1].Output, "workflow %d second step should have output", i)
			assert.Nil(t, steps[0].Error, "workflow %d first step should not have error", i)
			assert.Nil(t, steps[1].Error, "workflow %d second step should not have error", i)
		}

		// At least 5 of the 2nd steps should have errored due to execution race
		// Check they are DBOSErrors with StepExecutionError wrapping a ConflictingIDError
		var errorsCopy []error
		require.Eventually(t, func() bool {
			secondStepErrorsMu.Lock()
			errorsCopy := make([]error, len(secondStepErrors))
			copy(errorsCopy, secondStepErrors)
			secondStepErrorsMu.Unlock()
			return len(errorsCopy) >= 5
		}, 10*time.Second, 100*time.Millisecond)
		for _, err := range errorsCopy {
			dbosErr, ok := err.(*DBOSError)
			require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
			require.Equal(t, StepExecutionError, dbosErr.Code, "expected error code to be StepExecutionError, got %v", dbosErr.Code)
			require.True(t, errors.Is(dbosErr.Unwrap(), &DBOSError{Code: ConflictingIDError}), "expected underlying error to be ConflictingIDError, got %T", dbosErr.Unwrap())
		}
	})
}

var (
	maxRecoveryAttempts       = 20
	deadLetterQueueStartEvent *Event
	deadLetterQueueEvent      *Event
	recoveryCount             int64
)

func deadLetterQueueWorkflow(ctx DBOSContext, input string) (int, error) {
	recoveryCount++
	wfid, err := GetWorkflowID(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get workflow ID: %v", err)
	}
	fmt.Printf("Dead letter queue workflow %s started, recovery count: %d\n", wfid, recoveryCount)
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}

func infiniteDeadLetterQueueWorkflow(ctx DBOSContext, input string) (int, error) {
	deadLetterQueueStartEvent.Set()
	deadLetterQueueEvent.Wait()
	return 0, nil
}
func TestWorkflowDeadLetterQueue(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, deadLetterQueueWorkflow, WithMaxRetries(maxRecoveryAttempts))
	RegisterWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, WithMaxRetries(-1)) // A negative value means infinite retries

	t.Run("DeadLetterQueueBehavior", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()
		recoveryCount = 0

		// Start a workflow that blocks forever
		wfID := uuid.NewString()
		handle, err := RunWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.NoError(t, err, "failed to start dead letter queue workflow")
		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()

		// Attempt to recover the blocked workflow the maximum number of times
		for i := range maxRecoveryAttempts {
			_, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
			expectedCount := int64(i + 2) // +1 for initial execution, +1 for each recovery
			require.Equal(t, expectedCount, recoveryCount, "expected recovery count to be %d, got %d", expectedCount, recoveryCount)
		}

		// Verify an additional attempt throws a DLQ error and puts the workflow in the DLQ status
		_, err = recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.Error(t, err, "expected dead letter queue error but got none")

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)
		require.Equal(t, DeadLetterQueueError, dbosErr.Code)

		// Verify workflow status is MAX_RECOVERY_ATTEMPTS_EXCEEDED
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusMaxRecoveryAttemptsExceeded, status.Status)

		// Verify that attempting to start a workflow with the same ID throws a DLQ error
		_, err = RunWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.Error(t, err, "expected dead letter queue error when restarting workflow with same ID but got none")

		dbosErr, ok = err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, dbosErr.Code, DeadLetterQueueError, "expected error code to be DeadLetterQueueError")

		// Now resume the workflow -- this clears the DLQ status
		resumedHandle, err := ResumeWorkflow[int](dbosCtx, wfID)
		require.NoError(t, err, "failed to resume workflow")

		// Recover pending workflows again - should work without error
		_, err = recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows after resume")

		// Complete the blocked workflow
		deadLetterQueueEvent.Set()

		// Wait for both handles to complete
		result1, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from original handle")

		result2, err := resumedHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed handle")

		require.Equal(t, result1, result2)

		// Verify workflow status is SUCCESS
		status, err = handle.GetStatus()
		require.NoError(t, err, "failed to get final workflow status")
		require.Equal(t, WorkflowStatusSuccess, status.Status)

		// Verify that retries of a completed workflow do not raise the DLQ exception
		for i := 0; i < maxRecoveryAttempts*2; i++ {
			_, err = RunWorkflow(dbosCtx, deadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
			require.NoError(t, err, "unexpected error when retrying completed workflow")
		}
	})

	t.Run("InfiniteRetriesWorkflow", func(t *testing.T) {
		deadLetterQueueEvent = NewEvent()
		deadLetterQueueStartEvent = NewEvent()

		// Verify that a workflow with MaxRetries=0 (infinite retries) is retried infinitely
		wfID := uuid.NewString()

		handle, err := RunWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, "test", WithWorkflowID(wfID))
		require.NoError(t, err, "failed to start infinite dead letter queue workflow")

		deadLetterQueueStartEvent.Wait()
		deadLetterQueueStartEvent.Clear()
		// Attempt to recover the blocked workflow many times (should never fail)
		handles := []WorkflowHandle[any]{}
		for i := range _DEFAULT_MAX_RECOVERY_ATTEMPTS * 2 {
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			handles = append(handles, recoveredHandles...)
			deadLetterQueueStartEvent.Wait()
			deadLetterQueueStartEvent.Clear()
		}

		// Complete the workflow
		deadLetterQueueEvent.Set()

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from infinite dead letter queue workflow")
		require.Equal(t, 0, result)

		// Wait for all handles to complete
		for i, h := range handles {
			resultAny, err := h.GetResult()
			require.NoError(t, err, "failed to get result from handle %d", i)
			// Decode the result from any (which may be float64 after JSON decode) to int
			// Marshal to JSON then unmarshal into the expected type
			jsonBytes, err := json.Marshal(resultAny)
			require.NoError(t, err, "failed to marshal result to JSON")
			var result int
			err = json.Unmarshal(jsonBytes, &result)
			require.NoError(t, err, "failed to decode result to int")
			require.Equal(t, 0, result)
		}
	})
}

var (
	counter    atomic.Int64
	counter1Ch = make(chan time.Time, 100)
)

func TestScheduledWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	RegisterWorkflow(dbosCtx, func(ctx DBOSContext, scheduledTime time.Time) (string, error) {
		startTime := time.Now()
		if counter.Add(1) == 10 {
			return "", fmt.Errorf("counter reached 10, stopping workflow")
		}
		select {
		case counter1Ch <- startTime:
		default:
		}
		return fmt.Sprintf("Scheduled workflow scheduled at time %v and executed at time %v", scheduledTime, startTime), nil
	}, WithSchedule("* * * * * *")) // Every second

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	// Helper function to collect execution times
	collectExecutionTimes := func(ch chan time.Time, target int, timeout time.Duration) ([]time.Time, error) {
		var executionTimes []time.Time
		for len(executionTimes) < target {
			select {
			case execTime := <-ch:
				executionTimes = append(executionTimes, execTime)
			case <-time.After(timeout):
				return nil, fmt.Errorf("timeout waiting for %d executions, got %d", target, len(executionTimes))
			}
		}
		return executionTimes, nil
	}

	t.Run("ScheduledWorkflowExecution", func(t *testing.T) {
		// Wait for workflow to execute at least 10 times (should take ~9-10 seconds)
		executionTimes, err := collectExecutionTimes(counter1Ch, 10, 10*time.Second)
		require.NoError(t, err, "Failed to collect scheduled workflow execution times")
		require.GreaterOrEqual(t, len(executionTimes), 10)

		// Verify timing - each execution should be approximately 1 second apart
		scheduleInterval := 1 * time.Second
		allowedSlack := 3 * time.Second

		for i, execTime := range executionTimes {
			// Calculate expected execution time based on schedule interval
			expectedTime := executionTimes[0].Add(time.Duration(i+1) * scheduleInterval)

			// Calculate the delta between actual and expected execution time
			delta := execTime.Sub(expectedTime)
			if delta < 0 {
				delta = -delta // Get absolute value
			}

			// Check if delta is within acceptable slack
			require.LessOrEqual(t, delta, allowedSlack, "Execution %d timing deviation too large: expected around %v, got %v (delta: %v, allowed slack: %v)", i+1, expectedTime, execTime, delta, allowedSlack)

			t.Logf("Execution %d: expected %v, actual %v, delta %v", i+1, expectedTime, execTime, delta)
		}

		// Stop the workflowScheduler and check if it stops executing
		dbosCtx.(*dbosContext).getWorkflowScheduler().Stop()
		time.Sleep(3 * time.Second) // Wait a bit to ensure no more executions
		currentCounter := counter.Load()
		require.Less(t, counter.Load(), currentCounter+2, "Scheduled workflow continued executing after stopping scheduler")
	})
}

var (
	sendIdempotencyEvent         = NewEvent()
	receiveIdempotencyStartEvent = NewEvent()
	receiveIdempotencyStopEvent  = NewEvent()
	sendRecvSyncEvent            = NewEvent() // Event to synchronize send/recv in tests
	numConcurrentRecvWfs         = 5
	concurrentRecvReadyEvents    = make([]*Event, numConcurrentRecvWfs)
	concurrentRecvStartEvent     = NewEvent()
)

type sendWorkflowInput struct {
	DestinationID string
	Topic         string
}

func sendWorkflow(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	err := Send(ctx, input.DestinationID, "message1", input.Topic)
	if err != nil {
		return "", err
	}
	err = Send(ctx, input.DestinationID, "message2", input.Topic)
	if err != nil {
		return "", err
	}
	err = Send(ctx, input.DestinationID, "message3", input.Topic)
	if err != nil {
		return "", err
	}
	return "", nil
}

func receiveWorkflow(ctx DBOSContext, topic string) (string, error) {
	// Wait for the test to signal it's ready
	sendRecvSyncEvent.Wait()

	msg1, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil {
		return "", err
	}
	msg2, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil {
		return "", err
	}
	msg3, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil {
		return "", err
	}
	return msg1 + "-" + msg2 + "-" + msg3, nil
}

func receiveWorkflowCoordinated(ctx DBOSContext, input struct {
	Topic string
	i     int
}) (string, error) {
	// Signal that this workflow has started and is ready
	concurrentRecvReadyEvents[input.i].Set()

	// Wait for the coordination event before starting to receive

	concurrentRecvStartEvent.Wait()

	// Do a single Recv call with timeout
	msg, err := Recv[string](ctx, input.Topic, 3*time.Second)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func sendStructWorkflow(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	testStruct := sendRecvType{Value: "test-struct-value"}
	err := Send(ctx, input.DestinationID, testStruct, input.Topic)
	return "", err
}

func receiveStructWorkflow(ctx DBOSContext, topic string) (sendRecvType, error) {
	return Recv[sendRecvType](ctx, topic, 3*time.Second)
}

func sendIdempotencyWorkflow(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	err := Send(ctx, input.DestinationID, "m1", input.Topic)
	if err != nil {
		return "", err
	}
	sendIdempotencyEvent.Wait()
	return "idempotent-send-completed", nil
}

func receiveIdempotencyWorkflow(ctx DBOSContext, topic string) (string, error) {
	msg, err := Recv[string](ctx, topic, 3*time.Second)
	if err != nil {
		// Unlock the test in this case
		receiveIdempotencyStartEvent.Set()
		return "", err
	}
	receiveIdempotencyStartEvent.Set()
	receiveIdempotencyStopEvent.Wait()
	return msg, nil
}

func durableRecvSleepWorkflow(ctx DBOSContext, topic string) (string, error) {
	// First Recv with 2-second timeout (will timeout)
	msg1, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil {
		return "", fmt.Errorf("unexpected error in first recv: %w", err)
	}

	// Second Recv with 2-second timeout (will also timeout)
	msg2, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil {
		return "", fmt.Errorf("unexpected error in second recv: %w", err)
	}

	// Signal that both Recv calls completed
	receiveIdempotencyStartEvent.Set()

	// Wait for test to signal completion
	receiveIdempotencyStopEvent.Wait()

	// Return result - will be empty strings since both timeout
	return msg1 + msg2, nil
}

func stepThatCallsSend(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx.(DBOSContext), input.DestinationID, "message-from-step", input.Topic)
	if err != nil {
		return "", err
	}
	return "send-completed", nil
}

func workflowThatCallsSendInStep(ctx DBOSContext, input sendWorkflowInput) (string, error) {
	return RunAsStep(ctx, func(context context.Context) (string, error) {
		return stepThatCallsSend(context, input)
	})
}

type sendRecvType struct {
	Value string
}

func recvContextCancelWorkflow(ctx DBOSContext, topic string) (string, error) {
	// Try to receive with a 5 second timeout, but context will cancel before that
	msg, err := Recv[string](ctx, topic, 5*time.Second)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func TestSendRecv(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register all send/recv workflows with executor
	RegisterWorkflow(dbosCtx, sendWorkflow)
	RegisterWorkflow(dbosCtx, receiveWorkflow)
	RegisterWorkflow(dbosCtx, receiveWorkflowCoordinated)
	RegisterWorkflow(dbosCtx, sendStructWorkflow)
	RegisterWorkflow(dbosCtx, receiveStructWorkflow)
	RegisterWorkflow(dbosCtx, sendIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, receiveIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, durableRecvSleepWorkflow)
	RegisterWorkflow(dbosCtx, workflowThatCallsSendInStep)
	RegisterWorkflow(dbosCtx, recvContextCancelWorkflow)

	Launch(dbosCtx)

	t.Run("SendRecvSuccess", func(t *testing.T) {
		// Clear the sync event before starting
		sendRecvSyncEvent.Clear()

		// Start the receive workflow - it will wait for sendRecvSyncEvent before calling Recv
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, "test-topic")
		require.NoError(t, err, "failed to start receive workflow")

		// Send messages to the receive workflow
		sendHandle, err := RunWorkflow(dbosCtx, sendWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "test-topic",
		})
		require.NoError(t, err, "failed to send message")

		// Wait for send workflow to complete
		_, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send workflow")

		// Now that the send workflow has completed, signal the receive workflow to proceed
		sendRecvSyncEvent.Set()

		// Wait for receive workflow to complete
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "message1-message2-message3", result)

		// Verify step counting for send workflow (sendWorkflow calls Send 3 times)
		sendSteps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for send workflow")
		require.Len(t, sendSteps, 3, "expected 3 steps in send workflow (3 Send calls), got %d", len(sendSteps))
		for i, step := range sendSteps {
			require.Equal(t, i, step.StepID, "expected step %d to have correct StepID", i)
			require.Equal(t, "DBOS.send", step.StepName, "expected step %d to have StepName 'DBOS.send'", i)
			require.False(t, step.StartedAt.IsZero(), "expected step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected step %d CompletedAt to be after or equal to StartedAt", i)
		}

		// Verify step counting for receive workflow (receiveWorkflow calls Recv 3 times)
		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for receive workflow")
		require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[1].StepName, "expected step 1 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[2].StepName, "expected step 2 to have StepName 'DBOS.recv'")
		for i, step := range receiveSteps {
			require.False(t, step.StartedAt.IsZero(), "expected recv step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected recv step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected recv step %d CompletedAt to be after or equal to StartedAt", i)
		}
	})

	t.Run("SendRecvCustomStruct", func(t *testing.T) {
		// Start the receive workflow
		receiveHandle, err := RunWorkflow(dbosCtx, receiveStructWorkflow, "struct-topic")
		require.NoError(t, err, "failed to start receive workflow")

		// Send the struct to the receive workflow
		sendHandle, err := RunWorkflow(dbosCtx, sendStructWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "struct-topic",
		})
		require.NoError(t, err, "failed to send struct")

		_, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send workflow")

		// Get the result from receive workflow
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")

		// Verify the struct was received correctly
		require.Equal(t, "test-struct-value", result.Value)

		// Verify step counting for sendStructWorkflow (calls Send 1 time)
		sendSteps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for send struct workflow")
		require.Len(t, sendSteps, 1, "expected 1 step in send struct workflow (1 Send call), got %d", len(sendSteps))
		require.Equal(t, 0, sendSteps[0].StepID)
		require.Equal(t, "DBOS.send", sendSteps[0].StepName)
		require.False(t, sendSteps[0].StartedAt.IsZero(), "expected send step to have StartedAt set")
		require.False(t, sendSteps[0].CompletedAt.IsZero(), "expected send step to have CompletedAt set")
		require.True(t, sendSteps[0].CompletedAt.After(sendSteps[0].StartedAt) || sendSteps[0].CompletedAt.Equal(sendSteps[0].StartedAt),
			"expected send step CompletedAt to be after or equal to StartedAt")

		// Verify step counting for receiveStructWorkflow (calls Recv 1 time, with sleep)
		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for receive struct workflow")
		require.Len(t, receiveSteps, 2, "expected 2 steps in receive struct workflow (1 Recv call + 1 sleep call), got %d", len(receiveSteps))
		// First step should be recv
		require.Equal(t, 0, receiveSteps[0].StepID)
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName)
		require.False(t, receiveSteps[0].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, receiveSteps[0].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, receiveSteps[0].CompletedAt.After(receiveSteps[0].StartedAt) || receiveSteps[0].CompletedAt.Equal(receiveSteps[0].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")
		// Second step should be sleep
		require.Equal(t, 1, receiveSteps[1].StepID)
		require.Equal(t, "DBOS.sleep", receiveSteps[1].StepName)
		require.False(t, receiveSteps[1].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, receiveSteps[1].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, receiveSteps[1].CompletedAt.After(receiveSteps[1].StartedAt) || receiveSteps[1].CompletedAt.Equal(receiveSteps[1].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("SendToNonExistentUUID", func(t *testing.T) {
		// Generate a non-existent UUID
		destUUID := uuid.NewString()

		// Send to non-existent UUID should fail
		handle, err := RunWorkflow(dbosCtx, sendWorkflow, sendWorkflowInput{
			DestinationID: destUUID,
			Topic:         "testtopic",
		})
		require.NoError(t, err, "failed to start send workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error when sending to non-existent UUID but got none")

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, NonExistentWorkflowError, dbosErr.Code)

		expectedErrorMsg := fmt.Sprintf("workflow %s does not exist", destUUID)
		require.Contains(t, err.Error(), expectedErrorMsg)
	})

	t.Run("RecvTimeout", func(t *testing.T) {
		// Set the event so the receive workflow can proceed immediately
		sendRecvSyncEvent.Set()

		// Create a receive workflow that tries to receive a message but no send happens
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, "timeout-test-topic")
		require.NoError(t, err, "failed to start receive workflow")
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "expected no error on timeout")
		assert.Equal(t, "--", result, "expected -- result on timeout")
		// Check that six steps were recorded: recv, sleep, recv, sleep, recv, sleep
		steps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 6, "expected 6 steps in receive workflow, got %d", len(steps))
		require.Equal(t, "DBOS.recv", steps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected step 1 to have StepName 'DBOS.sleep'")
		require.Equal(t, "DBOS.recv", steps[2].StepName, "expected step 2 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected step 3 to have StepName 'DBOS.sleep'")
		require.Equal(t, "DBOS.recv", steps[4].StepName, "expected step 4 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.sleep", steps[5].StepName, "expected step 5 to have StepName 'DBOS.sleep'")
		for i, step := range steps {
			require.False(t, step.StartedAt.IsZero(), "expected step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected step %d CompletedAt to be after or equal to StartedAt", i)
		}
	})

	t.Run("RecvMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run Recv outside of a workflow context
		_, err := Recv[string](dbosCtx, "test-topic", 1*time.Second)
		require.Error(t, err, "expected error when running Recv outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SendOutsideWorkflow", func(t *testing.T) {
		// Set the event so the receive workflow can proceed immediately
		sendRecvSyncEvent.Set()

		// Start a receive workflow to have a valid destination
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, "outside-workflow-topic")
		require.NoError(t, err, "failed to start receive workflow")

		time.Sleep(500 * time.Millisecond) // Ensure receive workflow gets to the sleep part

		// Send messages from outside a workflow context
		for i := range 3 {
			err = Send(dbosCtx, receiveHandle.GetWorkflowID(), fmt.Sprintf("message%d", i+1), "outside-workflow-topic")
			require.NoError(t, err, "failed to send message%d from outside workflow", i+1)
		}

		// Verify the receive workflow gets all messages
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		assert.Equal(t, "message1-message2-message3", result, "expected correct result from receive workflow")

		// Verify step counting for receive workflow (calls Recv 3 times, each with sleep)
		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for receive workflow")
		require.Len(t, receiveSteps, 4, "expected 4 steps in receive workflow (3 Recv calls + 1 sleep calls), got %d", len(receiveSteps))
		// Steps 0, 2 and 4 are recv
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.sleep", receiveSteps[1].StepName, "expected step 1 to have StepName 'DBOS.sleep'")
		require.Equal(t, "DBOS.recv", receiveSteps[2].StepName, "expected step 2 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[3].StepName, "expected step 3 to have StepName 'DBOS.recv'")
		for i, step := range receiveSteps {
			require.False(t, step.StartedAt.IsZero(), "expected step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected step %d CompletedAt to be after or equal to StartedAt", i)
		}

	})
	t.Run("SendRecvIdempotency", func(t *testing.T) {
		// Start the receive workflow and wait for it to be ready
		receiveHandle, err := RunWorkflow(dbosCtx, receiveIdempotencyWorkflow, "idempotency-topic")
		require.NoError(t, err, "failed to start receive idempotency workflow")

		// Send the message to the receive workflow
		sendHandle, err := RunWorkflow(dbosCtx, sendIdempotencyWorkflow, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "idempotency-topic",
		})
		require.NoError(t, err, "failed to send idempotency message")

		// Wait for the receive workflow to have received the message
		receiveIdempotencyStartEvent.Wait()

		// Attempt recovering both workflows. There should be only 1 and 2 steps recorded for send and receive, respectively, after recovery.
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))
		steps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step in send idempotency workflow, got %d", len(steps))
		assert.Equal(t, 0, steps[0].StepID, "expected send idempotency step to have StepID 0")
		assert.Equal(t, "DBOS.send", steps[0].StepName, "expected send idempotency step to have StepName 'DBOS.send'")
		require.False(t, steps[0].StartedAt.IsZero(), "expected send step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected send step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected send step CompletedAt to be after or equal to StartedAt")

		steps, err = GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get steps for receive idempotency workflow")
		require.Len(t, steps, 2, "expected 2 steps in receive idempotency workflow (recv + sleep), got %d", len(steps))
		assert.Equal(t, 0, steps[0].StepID, "expected receive idempotency step to have StepID 0")
		assert.Equal(t, "DBOS.recv", steps[0].StepName, "expected receive idempotency step to have StepName 'DBOS.recv'")
		assert.Equal(t, 1, steps[1].StepID, "expected receive idempotency sleep step to have StepID 1")
		assert.Equal(t, "DBOS.sleep", steps[1].StepName, "expected receive idempotency sleep step to have StepName 'DBOS.sleep'")
		for i, step := range steps {
			require.False(t, step.StartedAt.IsZero(), "expected step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected step %d CompletedAt to be after or equal to StartedAt", i)
		}

		// Unblock the workflows to complete
		receiveIdempotencyStopEvent.Set()
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive idempotency workflow")
		assert.Equal(t, "m1", result, "expected result to be 'm1'")

		sendIdempotencyEvent.Set()
		result, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send idempotency workflow")
		assert.Equal(t, "idempotent-send-completed", result, "expected result to be 'idempotent-send-completed'")
	})

	t.Run("SendCannotBeCalledWithinStep", func(t *testing.T) {
		// Set the event so the receive workflow can proceed immediately
		sendRecvSyncEvent.Set()

		// Start a receive workflow to have a valid destination
		receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflow, "send-within-step-topic")
		require.NoError(t, err, "failed to start receive workflow")

		// Execute the workflow that tries to call Send within a step
		handle, err := RunWorkflow(dbosCtx, workflowThatCallsSendInStep, sendWorkflowInput{
			DestinationID: receiveHandle.GetWorkflowID(),
			Topic:         "send-within-step-topic",
		})
		require.NoError(t, err, "failed to start workflow")

		// Expect the workflow to fail with the specific error
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Send within a step, but got none")

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "cannot call Send within a step"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain expected text")

		// Wait for the receive workflow to time out
		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		assert.Equal(t, "--", result, "expected receive workflow result to be '--' (timeout)")
	})

	t.Run("TestConcurrentRecvs", func(t *testing.T) {
		// Test concurrent receivers - all should return valid results
		receiveTopic := "concurrent-recv-topic"

		// Start multiple concurrent receive workflows
		numReceivers := 5
		receiverHandles := make([]WorkflowHandle[string], numReceivers)

		// Start all receivers - they will signal when ready and wait for coordination
		for i := range numReceivers {
			concurrentRecvReadyEvents[i] = NewEvent()
			receiveHandle, err := RunWorkflow(dbosCtx, receiveWorkflowCoordinated, struct {
				Topic string
				i     int
			}{
				Topic: receiveTopic,
				i:     i,
			}, WithWorkflowID("concurrent-recv-wfid"))
			require.NoError(t, err, "failed to start receive workflow %d", i)
			receiverHandles[i] = receiveHandle
		}

		// Wait for all workflows to signal they are ready
		for i := range numReceivers {
			concurrentRecvReadyEvents[i].Wait()
		}

		// Now unblock all receivers simultaneously so they race to the Recv call
		concurrentRecvStartEvent.Set()

		// Collect results from all receivers
		for i := range numReceivers {
			result, err := receiverHandles[i].GetResult()
			require.NoError(t, err, "receiver %d should not error", i)
			require.Equal(t, result, "", "receiver %d should have an empty string result", i)
		}
	})

	t.Run("durableSleep", func(t *testing.T) {
		// Clear events before starting
		receiveIdempotencyStartEvent.Clear()
		receiveIdempotencyStopEvent.Clear()

		// First execution: Start workflow that will timeout after 4 seconds total (2x 2-second timeouts) and then block
		workflowID := uuid.NewString()
		startTime := time.Now()

		handle1, err := RunWorkflow(dbosCtx, durableRecvSleepWorkflow, "durable-sleep-topic", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start first receive workflow")

		// Wait for the workflow to signal it has completed the Recv call (which includes the sleep)
		receiveIdempotencyStartEvent.Wait()
		receiveIdempotencyStartEvent.Clear()

		// Verify it took at least close to 4 seconds to reach this point (2x 2-second Recv timeouts)
		elapsed := time.Since(startTime)

		require.GreaterOrEqual(t, elapsed, 3900*time.Millisecond, "expected workflow to sleep for close to 4 seconds, but elapsed time was %v", elapsed)

		// Check workflow steps after first execution - recv outputs should be nil (timeout)
		steps, err := GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps after first execution")
		require.Len(t, steps, 4, "expected 4 steps after first execution")

		// First recv (step 0) - should have nil output due to timeout
		require.Equal(t, 0, steps[0].StepID, "expected first step ID to be 0")
		require.Equal(t, "DBOS.recv", steps[0].StepName, "expected first step to be recv")
		require.Nil(t, steps[0].Output, "expected first recv step output to be nil (timeout)")
		require.False(t, steps[0].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")

		// First sleep (step 1)
		require.Equal(t, 1, steps[1].StepID, "expected second step ID to be 1")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected second step to be sleep")
		require.False(t, steps[1].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, steps[1].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, steps[1].CompletedAt.After(steps[1].StartedAt) || steps[1].CompletedAt.Equal(steps[1].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")

		// Second recv (step 2) - should have nil output due to timeout
		require.Equal(t, 2, steps[2].StepID, "expected third step ID to be 2")
		require.Equal(t, "DBOS.recv", steps[2].StepName, "expected third step to be recv")
		require.Nil(t, steps[2].Output, "expected second recv step output to be nil (timeout)")
		require.False(t, steps[2].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, steps[2].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, steps[2].CompletedAt.After(steps[2].StartedAt) || steps[2].CompletedAt.Equal(steps[2].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")

		// Second sleep (step 3)
		require.Equal(t, 3, steps[3].StepID, "expected fourth step ID to be 3")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected fourth step to be sleep")
		require.False(t, steps[3].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, steps[3].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, steps[3].CompletedAt.After(steps[3].StartedAt) || steps[3].CompletedAt.Equal(steps[3].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")

		// Now the workflow is blocked on receiveIdempotencyStopEvent.Wait()
		// Let's recover it to test that the sleep is not repeated
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))

		// The recovered workflow should proceed quickly since it already completed the sleep
		receiveIdempotencyStartEvent.Wait()
		receiveIdempotencyStartEvent.Clear()

		// Verify that the recovery was fast (no additional 4-second sleep)
		secondElapsed := time.Since(startTime)
		additionalTime := secondElapsed - elapsed
		require.Less(t, additionalTime, 500*time.Millisecond, "expected recovery to be fast (additional time less than 500ms), but additional time was %v", additionalTime)

		// Check workflow steps after first recovery - recv outputs should still be nil (timeout)
		steps, err = GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps after first recovery")
		require.Len(t, steps, 4, "expected 4 steps after first recovery")

		// First recv (step 0) - should still have nil output due to timeout
		require.Equal(t, 0, steps[0].StepID, "expected first step ID to be 0")
		require.Equal(t, "DBOS.recv", steps[0].StepName, "expected first step to be recv")
		require.Nil(t, steps[0].Output, "expected first recv step output to still be nil (timeout)")

		// First sleep (step 1)
		require.Equal(t, 1, steps[1].StepID, "expected second step ID to be 1")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected second step to be sleep")

		// Second recv (step 2) - should still have nil output due to timeout
		require.Equal(t, 2, steps[2].StepID, "expected third step ID to be 2")
		require.Equal(t, "DBOS.recv", steps[2].StepName, "expected third step to be recv")
		require.Nil(t, steps[2].Output, "expected second recv step output to still be nil (timeout)")

		// Second sleep (step 3)
		require.Equal(t, 3, steps[3].StepID, "expected fourth step ID to be 3")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected fourth step to be sleep")

		// Now send values so the workflow can receive. Recover again and verify that all steps have been generated correctly (even tho we didn't need to sleep)
		err = Send(dbosCtx, workflowID, "msg1", "durable-sleep-topic")
		require.NoError(t, err, "failed to send message to durable sleep workflow")
		err = Send(dbosCtx, workflowID, "msg2", "durable-sleep-topic")
		require.NoError(t, err, "failed to send second message to durable sleep workflow")

		// Recover again to ensure the workflow processes the message
		recoveredHandles, err = recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows on second attempt")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle on second attempt, got %d", len(recoveredHandles))

		receiveIdempotencyStartEvent.Wait()

		// Complete the workflow
		receiveIdempotencyStopEvent.Set()
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from workflow")

		// Check workflow steps after second recovery - recv outputs should now contain the messages
		steps, err = GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps after second recovery")
		require.Len(t, steps, 4, "expected 4 steps after second recovery")

		// First recv (step 0) - should still have nil as output because recv() is idempotent
		require.Equal(t, 0, steps[0].StepID, "expected first step ID to be 0")
		require.Equal(t, "DBOS.recv", steps[0].StepName, "expected first step to be recv")
		require.Nil(t, steps[0].Output, "expected first recv step output to still be nil (timeout)")

		// First sleep (step 1)
		require.Equal(t, 1, steps[1].StepID, "expected second step ID to be 1")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected second step to be sleep")

		// Second recv (step 2) - should still have nil as output because recv() is idempotent
		require.Equal(t, 2, steps[2].StepID, "expected third step ID to be 2")
		require.Equal(t, "DBOS.recv", steps[2].StepName, "expected third step to be recv")
		require.Nil(t, steps[2].Output, "expected second recv step output to still be nil (timeout)")

		// Second sleep (step 3)
		require.Equal(t, 3, steps[3].StepID, "expected fourth step ID to be 3")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected fourth step to be sleep")
	})

	t.Run("RecvContextCancellation", func(t *testing.T) {
		// Create a context with a shorter timeout than the Recv timeout (1s < 5s)
		timeoutCtx, cancel := WithTimeout(dbosCtx, 1*time.Second)
		defer cancel()

		// Start the workflow with the timeout context
		handle, err := RunWorkflow(timeoutCtx, recvContextCancelWorkflow, "context-cancel-topic")
		require.NoError(t, err, "failed to start recv context cancel workflow")

		// Get the result - should fail with context deadline exceeded
		result, err := handle.GetResult()
		require.Error(t, err, "expected error from context cancellation")
		require.True(t, errors.Is(err, context.DeadlineExceeded), "expected context.DeadlineExceeded error, got: %v", err)
		require.Equal(t, "", result, "expected empty result when context cancelled")

		// Verify the workflow status is cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})
}

var (
	setEventStart                 = NewEvent()
	setEventStartIdempotencyEvent = NewEvent()
	setEvenStopIdempotencyEvent   = NewEvent()
	getEventStartIdempotencyEvent = NewEvent()
	getEventStopIdempotencyEvent  = NewEvent()
	setSecondEventSignal          = NewEvent()
	setThirdEventSignal           = NewEvent()
	getEventWorkflowStartedSignal = NewEvent()
)

type setEventWorkflowInput struct {
	Key     string
	Message string
}

func setEventWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, input.Key, input.Message)
	if err != nil {
		return "", err
	}
	setEventStart.Set()
	return "event-set", nil
}

type getEventWorkflowInput struct {
	TargetWorkflowID string
	Key              string
}

func getEventWorkflow(ctx DBOSContext, input getEventWorkflowInput) (string, error) {
	getEventWorkflowStartedSignal.Set()
	result, err := GetEvent[string](ctx, input.TargetWorkflowID, input.Key, 3*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func setTwoEventsWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	// Set the first event
	err := SetEvent(ctx, "event", "first-event-message")
	if err != nil {
		return "", err
	}

	// Wait for external signal before setting the second event
	setSecondEventSignal.Wait()

	// Set the second event
	err = SetEvent(ctx, "event", "second-event-message")
	if err != nil {
		return "", err
	}

	setThirdEventSignal.Wait()

	// Set the third event
	err = SetEvent(ctx, "anotherevent", "third-event-message")
	if err != nil {
		return "", err
	}

	return "two-events-set", nil
}

func setEventIdempotencyWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, input.Key, input.Message)
	if err != nil {
		return "", err
	}
	setEventStartIdempotencyEvent.Set()
	setEvenStopIdempotencyEvent.Wait()
	return "idempotent-set-completed", nil
}

func getEventIdempotencyWorkflow(ctx DBOSContext, input setEventWorkflowInput) (string, error) {
	result, err := GetEvent[string](ctx, input.Key, input.Message, 3*time.Second)
	if err != nil {
		return "", err
	}
	getEventStartIdempotencyEvent.Set()
	getEventStopIdempotencyEvent.Wait()
	return result, nil
}

func durableGetEventSleepWorkflow(ctx DBOSContext, targetWorkflowID string) (string, error) {
	// First GetEvent with 2-second timeout (will timeout)
	val1, err := GetEvent[string](ctx, targetWorkflowID, "key1", 2*time.Second)
	if err != nil {
		return "", fmt.Errorf("unexpected error in first getEvent: %w", err)
	}

	// Second GetEvent with 2-second timeout (will not timeout)
	val2, err := GetEvent[string](ctx, targetWorkflowID, "key2", 2*time.Second)
	if err != nil {
		return "", fmt.Errorf("unexpected error in second getEvent: %w", err)
	}

	// Signal that both GetEvent calls completed
	getEventStartIdempotencyEvent.Set()

	// Wait for test to signal completion
	getEventStopIdempotencyEvent.Wait()

	return val1 + val2, nil
}

// Test workflows and steps for parameter mismatch validation
func conflictWorkflowA(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
}

func conflictWorkflowB(dbosCtx DBOSContext, input string) (string, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
}

func conflictStepA(_ context.Context) (string, error) {
	return "step-a-result", nil
}

func conflictStepB(_ context.Context) (string, error) {
	return "step-b-result", nil
}

func workflowWithMultipleSteps(dbosCtx DBOSContext, input string) (string, error) {
	// First step
	result1, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
	if err != nil {
		return "", err
	}

	// Second step - this is where we'll test step name conflicts
	result2, err := RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
	if err != nil {
		return "", err
	}

	return result1 + "-" + result2, nil
}

func TestWorkflowExecutionMismatch(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register workflows for testing
	RegisterWorkflow(dbosCtx, conflictWorkflowA)
	RegisterWorkflow(dbosCtx, conflictWorkflowB)
	RegisterWorkflow(dbosCtx, workflowWithMultipleSteps)

	t.Run("WorkflowNameConflict", func(t *testing.T) {
		workflowID := uuid.NewString()

		// First, run conflictWorkflowA with a specific workflow ID
		handle, err := RunWorkflow(dbosCtx, conflictWorkflowA, "test-input", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start first workflow")

		// Get the result to ensure it completes
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from first workflow")
		require.Equal(t, "step-a-result", result)

		// Now try to run conflictWorkflowB with the same workflow ID
		// This should return a ConflictingWorkflowError
		_, err = RunWorkflow(dbosCtx, conflictWorkflowB, "test-input", WithWorkflowID(workflowID))
		require.Error(t, err, "expected ConflictingWorkflowError when running different workflow with same ID, but got none")

		// Check that it's the correct error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, ConflictingWorkflowError, dbosErr.Code)

		// Check that the error message contains the workflow names
		expectedMsgPart := "Workflow already exists with a different name"
		require.Contains(t, err.Error(), expectedMsgPart)
	})

	t.Run("StepNameConflict", func(t *testing.T) {
		handle, err := RunWorkflow(dbosCtx, workflowWithMultipleSteps, "test-input")
		require.NoError(t, err, "failed to start workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")
		require.Equal(t, "step-a-result-step-b-result", result)

		// Check operation execution with a different step name for the same step ID
		workflowID := handle.GetWorkflowID()

		// This directly tests the CheckOperationExecution method with mismatched step name
		wrongStepName := "wrong-step-name"
		_, err = dbosCtx.(*dbosContext).systemDB.checkOperationExecution(dbosCtx, checkOperationExecutionDBInput{
			workflowID: workflowID,
			stepID:     0,
			stepName:   wrongStepName,
		})

		require.Error(t, err, "expected UnexpectedStep error when checking operation with wrong step name, but got none")

		// Check that it's the correct error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, UnexpectedStep, dbosErr.Code)

		// Check that the error message contains step information
		require.Contains(t, err.Error(), "Check that your workflow is deterministic")
		require.Contains(t, err.Error(), wrongStepName)
	})
}

func TestSetGetEvent(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register all set/get event workflows with executor
	RegisterWorkflow(dbosCtx, setEventWorkflow)
	RegisterWorkflow(dbosCtx, getEventWorkflow)
	RegisterWorkflow(dbosCtx, setTwoEventsWorkflow)
	RegisterWorkflow(dbosCtx, setEventIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, getEventIdempotencyWorkflow)
	RegisterWorkflow(dbosCtx, durableGetEventSleepWorkflow)

	Launch(dbosCtx)

	t.Run("SetGetEventFromWorkflow", func(t *testing.T) {
		// Clear the signal event before starting
		setSecondEventSignal.Clear()

		setWorkflowID := uuid.NewString()
		// Start a workflow to get the first event
		getFirstEventHandle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
			TargetWorkflowID: setWorkflowID, // Target workflow ID
			Key:              "event",       // Event key
		})
		require.NoError(t, err, "failed to start get first event workflow")
		getEventWorkflowStartedSignal.Wait()
		getEventWorkflowStartedSignal.Clear()

		time.Sleep(500 * time.Millisecond)

		// Start the workflow that sets two events
		setHandle, err := RunWorkflow(dbosCtx, setTwoEventsWorkflow, setEventWorkflowInput{
			Key:     setWorkflowID,
			Message: "unused",
		}, WithWorkflowID(setWorkflowID))
		require.NoError(t, err, "failed to start set two events workflow")

		// Verify we can get the first event
		firstMessage, err := getFirstEventHandle.GetResult()
		require.NoError(t, err, "failed to get result from first event workflow")
		assert.Equal(t, "first-event-message", firstMessage, "expected first message to be 'first-event-message'")

		// Signal the workflow to set the second event
		setSecondEventSignal.Set()

		time.Sleep(500 * time.Millisecond)

		// Start a workflow to get the second event
		getSecondEventHandle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
			TargetWorkflowID: setWorkflowID, // Target workflow ID
			Key:              "event",       // Event key
		})
		require.NoError(t, err, "failed to start get second event workflow")
		getEventWorkflowStartedSignal.Wait()
		getEventWorkflowStartedSignal.Clear()

		// Verify we can get the second event
		secondMessage, err := getSecondEventHandle.GetResult()
		require.NoError(t, err, "failed to get result from second event workflow")
		assert.Equal(t, "second-event-message", secondMessage, "expected second message to be 'second-event-message'")

		// Start a workflow to get the third event
		getThirdEventHandle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
			TargetWorkflowID: setWorkflowID,  // Target workflow ID
			Key:              "anotherevent", // Event key
		})
		require.NoError(t, err, "failed to start get third event workflow")
		getEventWorkflowStartedSignal.Wait() // So we know we're waiting on GetEvent

		// Signal the workflow to set the third event and wait until it's done
		setThirdEventSignal.Set()

		// Verify we can get the third event
		thirdMessage, err := getThirdEventHandle.GetResult()
		require.NoError(t, err, "failed to get result from third event workflow")
		assert.Equal(t, "third-event-message", thirdMessage, "expected third message to be 'third-event-message'")

		// Wait for the set workflow to complete
		result, err := setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set two events workflow")
		assert.Equal(t, "two-events-set", result, "expected result to be 'two-events-set'")

		// Verify step counting for setTwoEventsWorkflow (calls SetEvent 3 times)
		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for set two events workflow")
		require.Len(t, setSteps, 3, "expected 3 steps in set two events workflow (3 SetEvent calls), got %d", len(setSteps))
		for i, step := range setSteps {
			assert.Equal(t, i, step.StepID, "expected step %d to have StepID %d", i, i)
			assert.Equal(t, "DBOS.setEvent", step.StepName, "expected step %d to have StepName 'DBOS.setEvent'", i)
			require.False(t, step.StartedAt.IsZero(), "expected setEvent step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected setEvent step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected setEvent step %d CompletedAt to be after or equal to StartedAt", i)
		}

		// Verify step counting for the first get event workflow
		getFirstSteps, err := GetWorkflowSteps(dbosCtx, getFirstEventHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for get first event workflow")
		require.Len(t, getFirstSteps, 2, "expected 2 steps in get first event workflow (getEvent + sleep), got %d", len(getFirstSteps))
		// First step should be the getEvent step with stepID 0
		assert.Equal(t, 0, getFirstSteps[0].StepID, "expected first step to have StepID 0")
		assert.Equal(t, "DBOS.getEvent", getFirstSteps[0].StepName, "expected first step to have StepName 'DBOS.getEvent'")
		require.False(t, getFirstSteps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
		require.False(t, getFirstSteps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
		require.True(t, getFirstSteps[0].CompletedAt.After(getFirstSteps[0].StartedAt) || getFirstSteps[0].CompletedAt.Equal(getFirstSteps[0].StartedAt),
			"expected getEvent step CompletedAt to be after or equal to StartedAt")
		// Second step should be the sleep step with stepID 1
		assert.Equal(t, 1, getFirstSteps[1].StepID, "expected second step to have StepID 1")
		assert.Equal(t, "DBOS.sleep", getFirstSteps[1].StepName, "expected second step to have StepName 'DBOS.sleep'")
		require.False(t, getFirstSteps[1].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, getFirstSteps[1].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, getFirstSteps[1].CompletedAt.After(getFirstSteps[1].StartedAt) || getFirstSteps[1].CompletedAt.Equal(getFirstSteps[1].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")

		// Verify step counting for the second get event workflow
		// This one does not sleep because the event was already set
		getSecondSteps, err := GetWorkflowSteps(dbosCtx, getSecondEventHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for get second event workflow")
		require.Len(t, getSecondSteps, 1, "expected 1 step in get second event workflow (getEvent only), got %d", len(getSecondSteps))
		// First step should be the getEvent step with stepID 0
		assert.Equal(t, 0, getSecondSteps[0].StepID, "expected first step to have StepID 0")
		assert.Equal(t, "DBOS.getEvent", getSecondSteps[0].StepName, "expected first step to have StepName 'DBOS.getEvent'")
		require.False(t, getSecondSteps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
		require.False(t, getSecondSteps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
		require.True(t, getSecondSteps[0].CompletedAt.After(getSecondSteps[0].StartedAt) || getSecondSteps[0].CompletedAt.Equal(getSecondSteps[0].StartedAt),
			"expected getEvent step CompletedAt to be after or equal to StartedAt")

		// Verify step counting for the third get event workflow
		// This one sleeps because the event wasn't set yet
		getThirdSteps, err := GetWorkflowSteps(dbosCtx, getThirdEventHandle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps for get third event workflow")
		require.Len(t, getThirdSteps, 2, "expected 2 steps in get third event workflow (getEvent + sleep), got %d", len(getThirdSteps))
		// First step should be the getEvent step with stepID 0
		assert.Equal(t, 0, getThirdSteps[0].StepID, "expected first step to have StepID 0")
		assert.Equal(t, "DBOS.getEvent", getThirdSteps[0].StepName, "expected first step to have StepName 'DBOS.getEvent'")
		require.False(t, getThirdSteps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
		require.False(t, getThirdSteps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
		require.True(t, getThirdSteps[0].CompletedAt.After(getThirdSteps[0].StartedAt) || getThirdSteps[0].CompletedAt.Equal(getThirdSteps[0].StartedAt),
			"expected getEvent step CompletedAt to be after or equal to StartedAt")
		// Second step should be the sleep step with stepID 1
		assert.Equal(t, 1, getThirdSteps[1].StepID, "expected second step to have StepID")
		assert.Equal(t, "DBOS.sleep", getThirdSteps[1].StepName, "expected second step to have StepName 'DBOS.sleep'")
		require.False(t, getThirdSteps[1].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, getThirdSteps[1].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, getThirdSteps[1].CompletedAt.After(getThirdSteps[1].StartedAt) || getThirdSteps[1].CompletedAt.Equal(getThirdSteps[1].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("GetEventFromOutsideWorkflow", func(t *testing.T) {
		// Start a workflow that sets an event
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the event to be set
		_, err = setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}

		// Start a workflow that gets the event from outside the original workflow
		message, err := GetEvent[string](dbosCtx, setHandle.GetWorkflowID(), "test-key", 3*time.Second)
		if err != nil {
			t.Fatalf("failed to get event from outside workflow: %v", err)
		}
		if message != "test-message" {
			t.Fatalf("expected received message to be 'test-message', got '%s'", message)
		}

		// Verify step counting for setEventWorkflow (calls SetEvent 1 time)
		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get workflow steps for set event workflow: %v", err)
		}
		require.Len(t, setSteps, 1, "expected 1 step in set event workflow (1 SetEvent call), got %d", len(setSteps))
		if setSteps[0].StepID != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", setSteps[0].StepID)
		}
		if setSteps[0].StepName != "DBOS.setEvent" {
			t.Fatalf("expected step to have StepName 'DBOS.setEvent', got '%s'", setSteps[0].StepName)
		}
		require.False(t, setSteps[0].StartedAt.IsZero(), "expected setEvent step to have StartedAt set")
		require.False(t, setSteps[0].CompletedAt.IsZero(), "expected setEvent step to have CompletedAt set")
		require.True(t, setSteps[0].CompletedAt.After(setSteps[0].StartedAt) || setSteps[0].CompletedAt.Equal(setSteps[0].StartedAt),
			"expected setEvent step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("GetEventTimeout", func(t *testing.T) {
		// Try to get an event from a non-existent workflow
		nonExistentID := uuid.NewString()
		message, err := GetEvent[string](dbosCtx, nonExistentID, "test-key", 3*time.Second)
		require.NoError(t, err, "failed to get event from non-existent workflow")
		if message != "" {
			t.Fatalf("expected empty result on timeout, got '%s'", message)
		}

		// Try to get an event from an existing workflow but with a key that doesn't exist
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		require.NoError(t, err, "failed to set event")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")
		message, err = GetEvent[string](dbosCtx, setHandle.GetWorkflowID(), "non-existent-key", 3*time.Second)
		require.NoError(t, err, "failed to get event with non-existent key")
		if message != "" {
			t.Fatalf("expected empty result on timeout with non-existent key, got '%s'", message)
		}
	})

	t.Run("SetGetEventMustRunInsideWorkflows", func(t *testing.T) {
		// Attempt to run SetEvent outside of a workflow context
		err := SetEvent(dbosCtx, "test-key", "test-message")
		require.Error(t, err, "expected error when running SetEvent outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SetGetEventIdempotency", func(t *testing.T) {
		// Start the set event workflow
		setHandle, err := RunWorkflow(dbosCtx, setEventIdempotencyWorkflow, setEventWorkflowInput{
			Key:     "idempotency-key",
			Message: "idempotency-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event idempotency workflow: %v", err)
		}

		time.Sleep(500 * time.Millisecond) // Ensure the value is in the database...

		// Start the get event workflow
		getHandle, err := RunWorkflow(dbosCtx, getEventIdempotencyWorkflow, setEventWorkflowInput{
			Key:     setHandle.GetWorkflowID(),
			Message: "idempotency-key",
		})
		if err != nil {
			t.Fatalf("failed to start get event idempotency workflow: %v", err)
		}

		// Wait for the workflows to signal it has received the event
		getEventStartIdempotencyEvent.Wait()
		getEventStartIdempotencyEvent.Clear()
		setEventStartIdempotencyEvent.Wait()
		setEventStartIdempotencyEvent.Clear()

		// Attempt recovering both workflows. Each should have exactly 1 step.
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))

		getEventStartIdempotencyEvent.Wait()
		setEventStartIdempotencyEvent.Wait()

		// Verify step counts
		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for set event idempotency workflow: %v", err)
		}
		require.Len(t, setSteps, 1, "expected 1 step in set event idempotency workflow, got %d", len(setSteps))
		if setSteps[0].StepID != 0 {
			t.Fatalf("expected set event idempotency step to have StepID 0, got %d", setSteps[0].StepID)
		}
		if setSteps[0].StepName != "DBOS.setEvent" {
			t.Fatalf("expected set event idempotency step to have StepName 'DBOS.setEvent', got '%s'", setSteps[0].StepName)
		}
		require.False(t, setSteps[0].StartedAt.IsZero(), "expected setEvent step to have StartedAt set")
		require.False(t, setSteps[0].CompletedAt.IsZero(), "expected setEvent step to have CompletedAt set")
		require.True(t, setSteps[0].CompletedAt.After(setSteps[0].StartedAt) || setSteps[0].CompletedAt.Equal(setSteps[0].StartedAt),
			"expected setEvent step CompletedAt to be after or equal to StartedAt")

		getSteps, err := GetWorkflowSteps(dbosCtx, getHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to get steps for get event idempotency workflow: %v", err)
		}
		require.Len(t, getSteps, 1, "expected 1 step in get event idempotency workflow, got %d", len(getSteps))
		if getSteps[0].StepID != 0 {
			t.Fatalf("expected get event idempotency step to have StepID 0, got %d", getSteps[0].StepID)
		}
		if getSteps[0].StepName != "DBOS.getEvent" {
			t.Fatalf("expected get event idempotency step to have StepName 'DBOS.getEvent', got '%s'", getSteps[0].StepName)
		}
		require.False(t, getSteps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
		require.False(t, getSteps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
		require.True(t, getSteps[0].CompletedAt.After(getSteps[0].StartedAt) || getSteps[0].CompletedAt.Equal(getSteps[0].StartedAt),
			"expected getEvent step CompletedAt to be after or equal to StartedAt")

		// Complete the workflows
		setEvenStopIdempotencyEvent.Set()
		getEventStopIdempotencyEvent.Set()

		setResult, err := setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event idempotency workflow: %v", err)
		}
		if setResult != "idempotent-set-completed" {
			t.Fatalf("expected result to be 'idempotent-set-completed', got '%s'", setResult)
		}

		getResult, err := getHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from get event idempotency workflow: %v", err)
		}
		if getResult != "idempotency-message" {
			t.Fatalf("expected result to be 'idempotency-message', got '%s'", getResult)
		}

		// Check the recovered handle returns the same result
		for _, recoveredHandle := range recoveredHandles {
			if recoveredHandle.GetWorkflowID() == setHandle.GetWorkflowID() {
				recoveredSetResult, err := recoveredHandle.GetResult()
				if err != nil {
					t.Fatalf("failed to get result from recovered set event idempotency workflow: %v", err)
				}
				if recoveredSetResult != "idempotent-set-completed" {
					t.Fatalf("expected recovered result to be 'idempotent-set-completed', got '%s'", recoveredSetResult)

				}
			}
			if recoveredHandle.GetWorkflowID() == getHandle.GetWorkflowID() {
				recoveredGetResult, err := recoveredHandle.GetResult()
				if err != nil {
					t.Fatalf("failed to get result from recovered get event idempotency workflow: %v", err)
				}
				if recoveredGetResult != "idempotency-message" {
					t.Fatalf("expected recovered result to be 'idempotency-message', got '%s'", recoveredGetResult)
				}
			}
		}
	})

	t.Run("ConcurrentGetEvent", func(t *testing.T) {
		// Set event
		setHandle, err := RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "concurrent-event-key",
			Message: "concurrent-event-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		// Wait for the set event workflow to complete
		_, err = setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}
		// Start a few goroutines that'll concurrently get the event
		numGoroutines := 5
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)
		wg.Add(numGoroutines)
		for range numGoroutines {
			go func() {
				defer wg.Done()
				res, err := GetEvent[string](dbosCtx, setHandle.GetWorkflowID(), "concurrent-event-key", 10*time.Second)
				if err != nil {
					errors <- fmt.Errorf("failed to get event in goroutine: %v", err)
					return
				}
				if res != "concurrent-event-message" {
					errors <- fmt.Errorf("expected result in goroutine to be 'concurrent-event-message', got '%s'", res)
					return
				}
			}()
		}
		wg.Wait()
		close(errors)

		// Check for any errors from goroutines
		for err := range errors {
			require.FailNow(t, "goroutine error: %v", err)
		}
	})

	t.Run("durableSleep", func(t *testing.T) {
		// Clear events before starting
		getEventStartIdempotencyEvent.Clear()
		getEventStopIdempotencyEvent.Clear()

		// First execution: Start workflow that will timeout after 4 seconds total (2x 2-second timeouts) and then block
		sendWorkflowID := uuid.NewString()
		getWorkflowID := uuid.NewString()
		startTime := time.Now()

		handle1, err := RunWorkflow(dbosCtx, durableGetEventSleepWorkflow, sendWorkflowID, WithWorkflowID(getWorkflowID))
		require.NoError(t, err, "failed to start first get event workflow")

		// Wait for the workflow to signal it has completed the GetEvent calls (which includes the sleeps)
		getEventStartIdempotencyEvent.Wait()
		getEventStartIdempotencyEvent.Clear()

		// Verify it took at least close to 4 seconds to reach this point (2x Getevents with 2s timeout each)
		elapsed := time.Since(startTime)
		require.GreaterOrEqual(t, elapsed, 3900*time.Millisecond, "expected workflow to sleep for close to 4 seconds, but elapsed time was %v", elapsed)

		// Check workflow steps after first execution - getEvent outputs should be empty strings (timeout)
		steps, err := GetWorkflowSteps(dbosCtx, getWorkflowID)
		require.NoError(t, err, "failed to get workflow steps after first execution")
		require.Len(t, steps, 4, "expected 4 steps after first execution")

		// First getEvent (step 0) - should have empty string output due to timeout
		require.Equal(t, 0, steps[0].StepID, "expected first step ID to be 0")
		require.Equal(t, "DBOS.getEvent", steps[0].StepName, "expected first step to be getEvent")
		require.Nil(t, steps[0].Output, "expected first getEvent step output to be empty string (timeout)")
		require.False(t, steps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected getEvent step CompletedAt to be after or equal to StartedAt")

		// First sleep (step 1)
		require.Equal(t, 1, steps[1].StepID, "expected second step ID to be 1")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected second step to be sleep")
		require.False(t, steps[1].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, steps[1].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, steps[1].CompletedAt.After(steps[1].StartedAt) || steps[1].CompletedAt.Equal(steps[1].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")

		// Second getEvent (step 2) - should have empty string output due to timeout
		require.Equal(t, 2, steps[2].StepID, "expected third step ID to be 2")
		require.Equal(t, "DBOS.getEvent", steps[2].StepName, "expected third step to be getEvent")
		require.Nil(t, steps[2].Output, "expected second getEvent step output to be empty string (timeout)")
		require.False(t, steps[2].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
		require.False(t, steps[2].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
		require.True(t, steps[2].CompletedAt.After(steps[2].StartedAt) || steps[2].CompletedAt.Equal(steps[2].StartedAt),
			"expected getEvent step CompletedAt to be after or equal to StartedAt")

		// Second sleep (step 3)
		require.Equal(t, 3, steps[3].StepID, "expected fourth step ID to be 3")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected fourth step to be sleep")
		require.False(t, steps[3].StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, steps[3].CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, steps[3].CompletedAt.After(steps[3].StartedAt) || steps[3].CompletedAt.Equal(steps[3].StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")

		// Now the workflow is blocked on getEventStopIdempotencyEvent.Wait()
		// Let's recover it to test that the sleep is not repeated
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))

		// The recovered workflow should proceed quickly since it already completed the sleeps
		getEventStartIdempotencyEvent.Wait()
		getEventStartIdempotencyEvent.Clear()

		// Verify that the recovery was fast (no additional 4-second sleep)
		secondElapsed := time.Since(startTime)
		additionalTime := secondElapsed - elapsed
		require.Less(t, additionalTime, 500*time.Millisecond, "expected recovery to be fast (additional time less than 500ms), but additional time was %v", additionalTime)

		// Check workflow steps after first recovery - getEvent outputs should still be empty strings (timeout)
		steps, err = GetWorkflowSteps(dbosCtx, getWorkflowID)
		require.NoError(t, err, "failed to get workflow steps after first recovery")
		require.Len(t, steps, 4, "expected 4 steps after first recovery")

		// First getEvent (step 0) - should still have empty string output due to timeout
		require.Equal(t, 0, steps[0].StepID, "expected first step ID to be 0")
		require.Equal(t, "DBOS.getEvent", steps[0].StepName, "expected first step to be getEvent")
		require.Nil(t, steps[0].Output, "expected first getEvent step output to still be empty string (timeout)")

		// First sleep (step 1)
		require.Equal(t, 1, steps[1].StepID, "expected second step ID to be 1")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected second step to be sleep")

		// Second getEvent (step 2) - should still have empty string output due to timeout
		require.Equal(t, 2, steps[2].StepID, "expected third step ID to be 2")
		require.Equal(t, "DBOS.getEvent", steps[2].StepName, "expected third step to be getEvent")
		require.Nil(t, steps[2].Output, "expected second getEvent step output to still be empty string (timeout)")

		// Second sleep (step 3)
		require.Equal(t, 3, steps[3].StepID, "expected fourth step ID to be 3")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected fourth step to be sleep")

		// Now start a workflow that sets the event to unblock the getEvent call
		_, err = RunWorkflow(dbosCtx, setEventWorkflow, setEventWorkflowInput{
			Key:     "key1",
			Message: "message",
		}, WithWorkflowID(sendWorkflowID))
		require.NoError(t, err, "failed to start set event workflow to unblock getEvent")
		setEventStart.Wait()

		// Run the getEvent workflow again to check the no-sleep path
		_, err = RunWorkflow(dbosCtx, durableGetEventSleepWorkflow, sendWorkflowID, WithWorkflowID(getWorkflowID))
		require.NoError(t, err, "failed to start second get event workflow")

		getEventStartIdempotencyEvent.Wait()

		// Complete the workflow
		getEventStopIdempotencyEvent.Set()

		// Get results from both handles - they should be the same (empty string due to timeout)
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from first get event workflow")

		// Check workflow steps after event is set - first getEvent should have "message", second should be empty
		steps, err = GetWorkflowSteps(dbosCtx, getWorkflowID)
		require.NoError(t, err, "failed to get workflow steps after event set")
		require.Len(t, steps, 4, "expected 4 steps after event set")

		// First getEvent (step 0) - should still be nil because getEvent is idempotent
		require.Equal(t, 0, steps[0].StepID, "expected first step ID to be 0")
		require.Equal(t, "DBOS.getEvent", steps[0].StepName, "expected first step to be getEvent")
		require.Nil(t, steps[0].Output, "expected first getEvent step output to be nil")

		// First sleep (step 1)
		require.Equal(t, 1, steps[1].StepID, "expected second step ID to be 1")
		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected second step to be sleep")

		// Second getEvent (step 2) - should still be nil because getEvent is idempotent
		require.Equal(t, 2, steps[2].StepID, "expected third step ID to be 2")
		require.Equal(t, "DBOS.getEvent", steps[2].StepName, "expected third step to be getEvent")
		require.Nil(t, steps[2].Output, "expected second getEvent step output to be nil (no event for key2)")

		// Second sleep (step 3)
		require.Equal(t, 3, steps[3].StepID, "expected fourth step ID to be 3")
		require.Equal(t, "DBOS.sleep", steps[3].StepName, "expected fourth step to be sleep")
	})
}

var (
	sleepStartEvent *Event
	sleepStopEvent  *Event
)

func sleepRecoveryWorkflow(dbosCtx DBOSContext, duration time.Duration) (time.Duration, error) {
	result, err := Sleep(dbosCtx, duration)
	if err != nil {
		return 0, err
	}
	// Block after sleep so we can recover a pending workflow
	sleepStartEvent.Set()
	sleepStopEvent.Wait()
	return result, nil
}

func TestSleep(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, sleepRecoveryWorkflow)

	t.Run("SleepDurableRecovery", func(t *testing.T) {
		sleepStartEvent = NewEvent()
		sleepStopEvent = NewEvent()

		// Start a workflow that sleeps for 2 seconds then blocks
		sleepDuration := 2 * time.Second

		handle, err := RunWorkflow(dbosCtx, sleepRecoveryWorkflow, sleepDuration)
		require.NoError(t, err, "failed to start sleep recovery workflow")

		sleepStartEvent.Wait()
		sleepStartEvent.Clear()

		// Run the workflow again and check the return time was less than the durable sleep
		startTime := time.Now()
		_, err = RunWorkflow(dbosCtx, sleepRecoveryWorkflow, sleepDuration, WithWorkflowID(handle.GetWorkflowID()))
		require.NoError(t, err, "failed to start second sleep recovery workflow")

		sleepStartEvent.Wait()
		// Time elapsed should be at most the sleep duration
		elapsed := time.Since(startTime)
		assert.Less(t, elapsed, sleepDuration, "expected elapsed time to be less than sleep duration")

		// Verify the sleep step was recorded correctly
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")

		require.Len(t, steps, 1, "expected 1 step (the sleep), got %d", len(steps))

		step := steps[0]
		assert.Equal(t, 0, step.StepID, "expected step to have StepID 0")
		assert.Equal(t, "DBOS.sleep", step.StepName, "expected step name to be 'DBOS.sleep'")
		assert.Nil(t, step.Error, "expected step to have no error")
		require.False(t, step.StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, step.CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")

		sleepStopEvent.Set()

		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get sleep workflow result")
	})

	t.Run("SleepCannotBeCalledOutsideWorkflow", func(t *testing.T) {
		// Attempt to call Sleep outside of a workflow context
		_, err := Sleep(dbosCtx, 1*time.Second)
		require.Error(t, err, "expected error when calling Sleep outside of workflow context, but got none")

		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		// Test the specific message from the error
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})
}

func TestWorkflowTimeout(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	waitForCancelWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded),
			"workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflow)

	t.Run("WorkflowTimeout", func(t *testing.T) {
		// The reason this sequence works is that the timeout is so fast that the workflow AfterFunc
		// triggers as soon as it is set, likely even before the workflow goroutine is started
		// So we are almost guaranteed that the workflow will be cancelled before returning, hence GetStatus will show it as cancelled
		// Start a workflow that will wait indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflow, "wait-for-cancel")
		require.NoError(t, err, "failed to start wait for cancel workflow")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	wfcStart := NewEvent()
	wfcStop := NewEvent()
	waitForCancelWorkflowManual := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will wait indefinitely until it is cancelled
		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded),
			"workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		wfcStart.Set()
		wfcStop.Wait()
		return "", ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowManual)

	t.Run("ManuallyCancelWorkflow", func(t *testing.T) {
		// This test requires an event to prevent the workflow for returning before we GetStatus
		// This is because direct cancellation through the cancel function can happen faster than the timeout context AfterFunc
		// This is even more likely in contended environments with few CPU resources
		// When this happens, the workflow will complete first with an error status, and the AfterFunc cancelWorkflow will be a no-op
		// Thus the workflow status will be "Error" instead of "Cancelled" and the test fail
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Hour)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflowManual, "manual-cancel")
		require.NoError(t, err, "failed to start manual cancel workflow")

		// Cancel the workflow manually
		cancelFunc()
		wfcStart.Wait()

		// Check the workflow status: should be cancelled
		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "workflow did not reach cancelled status in time")

		wfcStop.Set()

		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")
	})

	waitForCancelStep := func(ctx context.Context) (string, error) {
		// This step will trigger cancellation of the entire workflow context
		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("step was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}
		return "", ctx.Err()
	}

	waitForCancelWorkflowWithStep := func(ctx DBOSContext, _ string) (string, error) {
		return RunAsStep(ctx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowWithStep)

	t.Run("WorkflowWithStepTimeout", func(t *testing.T) {
		// Start a workflow that will run a step that triggers cancellation
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 100*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflowWithStep, "wf-with-step-timeout")
		require.NoError(t, err, "failed to start workflow with step timeout")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	waitForCancelWorkflowWithStepAfterCancel := func(ctx DBOSContext, _ string) (string, error) {
		// Wait for cancellation
		<-ctx.Done()
		// Check that we have the correct cancellation error
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}
		// The status of this workflow should transition to cancelled
		maxtries := 10
		for range maxtries {
			isCancelled, err := checkWfStatus(ctx, WorkflowStatusCancelled)
			if err != nil {
				return "", err
			}
			if isCancelled {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		// After cancellation, try to run a simple step
		// This should return a WorkflowCancelled error
		return RunAsStep(ctx, simpleStep)
	}
	RegisterWorkflow(dbosCtx, waitForCancelWorkflowWithStepAfterCancel)

	t.Run("WorkflowWithStepAfterTimeout", func(t *testing.T) {
		// Start a workflow that waits for cancellation then tries to run a step
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflowWithStepAfterCancel, "wf-with-step-after-timeout")
		require.NoError(t, err, "failed to start workflow with step after timeout")

		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		// The workflow should return a WorkflowCancelled error from the step
		require.Error(t, err, "expected error from workflow")

		// Check if the error is a DBOSError with WorkflowCancelled code
		var dbosErr *DBOSError
		if errors.As(err, &dbosErr) {
			assert.Equal(t, WorkflowCancelled, dbosErr.Code, "expected WorkflowCancelled error code, got: %v", dbosErr.Code)
		} else {
			// If not a DBOSError, check if it's a context error
			assert.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded),
				"expected context.Canceled or context.DeadlineExceeded error, got: %v", err)
		}
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	shorterStepTimeoutWorkflow := func(ctx DBOSContext, _ string) (string, error) {
		// This workflow will run a step that has a shorter timeout than the workflow itself
		// The timeout will trigger a step error, the workflow can do whatever it wants with that error
		stepCtx, stepCancelFunc := WithTimeout(ctx, 1*time.Millisecond)
		defer stepCancelFunc() // Ensure we clean up the context
		_, err := RunAsStep(stepCtx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected step to timeout, got: %v", err)
		return "step-timed-out", nil
	}
	RegisterWorkflow(dbosCtx, shorterStepTimeoutWorkflow)

	t.Run("ShorterStepTimeout", func(t *testing.T) {
		// Start a workflow that runs a step with a shorter timeout than the workflow itself
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Second)
		defer cancelFunc() // Ensure we clean up the context
		handle, err := RunWorkflow(cancelCtx, shorterStepTimeoutWorkflow, "shorter-step-timeout")
		require.NoError(t, err, "failed to start shorter step timeout workflow")
		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from shorter step timeout workflow")
		assert.Equal(t, "step-timed-out", result, "expected result to be 'step-timed-out'")
		// Status is SUCCESS
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected workflow status to be WorkflowStatusSuccess")
	})

	detachedStep := func(ctx context.Context, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}

	detachedStepWorkflow := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		// This workflow will run a step that is not cancelable.
		// What this means is the workflow *will* be cancelled, but the step will run normally
		stepCtx := WithoutCancel(ctx)
		res, err := RunAsStep(stepCtx, func(context context.Context) (string, error) {
			return detachedStep(context, timeout*2)
		})
		require.NoError(t, err, "failed to run detached step")
		assert.Equal(t, "detached-step-completed", res, "expected detached step result to be 'detached-step-completed'")
		return res, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, detachedStepWorkflow)

	t.Run("DetachedStepWorkflow", func(t *testing.T) {
		// Start a workflow that runs a step that is not cancelable
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		handle, err := RunWorkflow(cancelCtx, detachedStepWorkflow, 1*time.Second)
		require.NoError(t, err, "failed to start detached step workflow")
		// Wait for the workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "detached-step-completed", result, "expected result to be 'detached-step-completed'")
		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	waitForCancelParent := func(ctx DBOSContext, childWorkflowID string) (string, error) {
		// This workflow will run a child workflow that waits indefinitely until it is cancelled
		childHandle, err := RunWorkflow(ctx, waitForCancelWorkflow, "child-wait-for-cancel", WithWorkflowID(childWorkflowID))
		require.NoError(t, err, "failed to start child workflow")

		// Wait for the child workflow to complete
		result, err := childHandle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected child workflow to be cancelled, got: %v", err)
		return result, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, waitForCancelParent)

	t.Run("ChildWorkflowTimesout", func(t *testing.T) {
		// Start a parent workflow that runs a child workflow that waits indefinitely
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc() // Ensure we clean up the context

		childWorkflowID := "child-wait-for-cancel-" + uuid.NewString()
		handle, err := RunWorkflow(cancelCtx, waitForCancelParent, childWorkflowID)
		require.NoError(t, err, "failed to start parent workflow")

		// Wait for the parent workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Check the child workflow status: should be cancelled
		childHandle, err := RetrieveWorkflow[string](dbosCtx, childWorkflowID)
		require.NoError(t, err, "failed to get child workflow handle")
		status, err = childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected child workflow status to be WorkflowStatusCancelled")
	})

	detachedChild := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}
	RegisterWorkflow(dbosCtx, detachedChild)

	detachedChildWorkflowParent := func(ctx DBOSContext, timeout time.Duration) (string, error) {
		childCtx := WithoutCancel(ctx)
		myID, err := GetWorkflowID(ctx)
		require.NoError(t, err, "failed to get parent workflow ID")
		childWorkflowID := fmt.Sprintf("%s-detached-child", myID)
		childHandle, err := RunWorkflow(childCtx, detachedChild, timeout*2, WithWorkflowID(childWorkflowID))
		require.NoError(t, err, "failed to start child workflow")

		// Wait for the child workflow to complete
		result, err := childHandle.GetResult()
		require.NoError(t, err, "failed to get result from child workflow")
		// The child spun for timeout*2 so ctx.Err() should be context.DeadlineExceeded
		return result, ctx.Err()
	}
	RegisterWorkflow(dbosCtx, detachedChildWorkflowParent)

	t.Run("ChildWorkflowDetached", func(t *testing.T) {
		timeout := 500 * time.Millisecond
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, detachedChildWorkflowParent, timeout)
		require.NoError(t, err, "failed to start parent workflow with detached child")

		// Wait for the parent workflow to complete and get the result
		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "Expected deadline exceeded error, got: %v", err)
		assert.Equal(t, "detached-step-completed", result, "expected result to be 'detached-step-completed'")

		// Check the workflow status: should be cancelled
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		// Check the child workflow status: should be cancelled
		childHandle, err := RetrieveWorkflow[string](dbosCtx, fmt.Sprintf("%s-detached-child", handle.GetWorkflowID()))
		require.NoError(t, err, "failed to get child workflow handle")
		status, err = childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected child workflow status to be WorkflowStatusSuccess")
	})

	t.Run("RecoverWaitForCancelWorkflow", func(t *testing.T) {
		start := time.Now()
		timeout := 1 * time.Second
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := RunWorkflow(cancelCtx, waitForCancelWorkflow, "recover-wait-for-cancel")
		require.NoError(t, err, "failed to start wait for cancel workflow")

		// Recover the pending workflow
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))
		recoveredHandle := recoveredHandles[0]
		assert.Equal(t, handle.GetWorkflowID(), recoveredHandle.GetWorkflowID(), "expected recovered handle to have same ID")

		// Wait for the workflow to complete and check the result. Should we AwaitedWorkflowCancelled
		result, err := recoveredHandle.GetResult()
		assert.Equal(t, "", result, "expected result to be an empty string")
		// Check the error type
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected error to be of type *DBOSError, got %T", err)
		require.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code)

		// Check the workflow status: should be cancelled
		status, err := recoveredHandle.GetStatus()
		require.NoError(t, err, "failed to get recovered workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected recovered workflow status to be WorkflowStatusCancelled")

		// Check the deadline on the status was is within an expected range (start time + timeout * .1)
		// FIXME this might be flaky and frankly not super useful
		expectedDeadline := start.Add(timeout * 10 / 100)
		assert.True(t, status.Deadline.After(expectedDeadline) && status.Deadline.Before(start.Add(timeout)),
			"expected workflow deadline to be within %v and %v, got %v", expectedDeadline, start.Add(timeout), status.Deadline)
	})
}

func notificationWaiterWorkflow(ctx DBOSContext, pairID int) (string, error) {
	result, err := GetEvent[string](ctx, fmt.Sprintf("notification-setter-%d", pairID), "event-key", 10*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func notificationSetterWorkflow(ctx DBOSContext, pairID int) (string, error) {
	err := SetEvent(ctx, "event-key", fmt.Sprintf("notification-message-%d", pairID))
	if err != nil {
		return "", err
	}
	return "event-set", nil
}

func sendRecvReceiverWorkflow(ctx DBOSContext, pairID int) (string, error) {
	result, err := Recv[string](ctx, "send-recv-topic", 10*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func sendRecvSenderWorkflow(ctx DBOSContext, pairID int) (string, error) {
	err := Send(ctx, fmt.Sprintf("send-recv-receiver-%d", pairID), fmt.Sprintf("send-recv-message-%d", pairID), "send-recv-topic")
	if err != nil {
		return "", err
	}
	return "message-sent", nil
}

func concurrentSimpleWorkflow(dbosCtx DBOSContext, input int) (int, error) {
	return RunAsStep(dbosCtx, func(ctx context.Context) (int, error) {
		return input * 2, nil
	})
}

func TestConcurrentWorkflows(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, concurrentSimpleWorkflow)
	RegisterWorkflow(dbosCtx, notificationWaiterWorkflow)
	RegisterWorkflow(dbosCtx, notificationSetterWorkflow)
	RegisterWorkflow(dbosCtx, sendRecvReceiverWorkflow)
	RegisterWorkflow(dbosCtx, sendRecvSenderWorkflow)

	t.Run("SimpleWorkflow", func(t *testing.T) {
		const numGoroutines = 500
		var wg sync.WaitGroup
		results := make(chan int, numGoroutines)
		errors := make(chan error, numGoroutines)

		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(input int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, concurrentSimpleWorkflow, input)
				if err != nil {
					errors <- fmt.Errorf("failed to start workflow %d: %w", input, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for workflow %d: %w", input, err)
					return
				}
				expectedResult := input * 2
				if result != expectedResult {
					errors <- fmt.Errorf("workflow %d: expected result %d, got %d", input, expectedResult, result)
					return
				}
				results <- result
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		require.Equal(t, 0, len(errors), "Expected no errors from concurrent workflows")

		resultCount := 0
		receivedResults := make(map[int]bool)
		for result := range results {
			resultCount++
			if result < 0 || result >= numGoroutines*2 || result%2 != 0 {
				t.Errorf("Unexpected result %d", result)
			} else {
				receivedResults[result] = true
			}
		}

		assert.Equal(t, numGoroutines, resultCount, "Expected correct number of results")
	})

	t.Run("NotificationWorkflows", func(t *testing.T) {
		const numPairs = 500
		var wg sync.WaitGroup
		waiterResults := make(chan string, numPairs)
		setterResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, notificationSetterWorkflow, pairID, WithWorkflowID(fmt.Sprintf("notification-setter-%d", pairID)))
				if err != nil {
					errors <- fmt.Errorf("failed to start setter workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for setter workflow %d: %w", pairID, err)
					return
				}
				setterResults <- result
			}(i)

			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, notificationWaiterWorkflow, pairID)
				if err != nil {
					errors <- fmt.Errorf("failed to start waiter workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for waiter workflow %d: %w", pairID, err)
					return
				}
				expectedMessage := fmt.Sprintf("notification-message-%d", pairID)
				if result != expectedMessage {
					errors <- fmt.Errorf("waiter workflow %d: expected message '%s', got '%s'", pairID, expectedMessage, result)
					return
				}
				waiterResults <- result
			}(i)
		}

		wg.Wait()
		close(waiterResults)
		close(setterResults)
		close(errors)

		require.Equal(t, 0, len(errors), "Expected no errors from notification workflows")

		waiterCount := 0
		receivedWaiterResults := make(map[string]bool)
		for result := range waiterResults {
			waiterCount++
			receivedWaiterResults[result] = true
		}

		setterCount := 0
		for result := range setterResults {
			setterCount++
			assert.Equal(t, "event-set", result, "Expected setter result to be 'event-set'")
		}

		assert.Equal(t, numPairs, waiterCount, "Expected correct number of waiter results")
		assert.Equal(t, numPairs, setterCount, "Expected correct number of setter results")

		for i := range numPairs {
			expectedWaiterResult := fmt.Sprintf("notification-message-%d", i)
			assert.True(t, receivedWaiterResults[expectedWaiterResult], "Expected waiter result '%s' not found", expectedWaiterResult)
		}
	})

	t.Run("SendRecvWorkflows", func(t *testing.T) {
		const numPairs = 500
		var wg sync.WaitGroup
		receiverResults := make(chan string, numPairs)
		senderResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, sendRecvReceiverWorkflow, pairID, WithWorkflowID(fmt.Sprintf("send-recv-receiver-%d", pairID)))
				if err != nil {
					errors <- fmt.Errorf("failed to start receiver workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for receiver workflow %d: %w", pairID, err)
					return
				}
				expectedMessage := fmt.Sprintf("send-recv-message-%d", pairID)
				if result != expectedMessage {
					errors <- fmt.Errorf("receiver workflow %d: expected message '%s', got '%s'", pairID, expectedMessage, result)
					return
				}
				receiverResults <- result
			}(i)

			go func(pairID int) {
				defer wg.Done()
				handle, err := RunWorkflow(dbosCtx, sendRecvSenderWorkflow, pairID)
				if err != nil {
					errors <- fmt.Errorf("failed to start sender workflow %d: %w", pairID, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for sender workflow %d: %w", pairID, err)
					return
				}
				senderResults <- result
			}(i)
		}

		wg.Wait()
		close(receiverResults)
		close(senderResults)
		close(errors)

		require.Equal(t, 0, len(errors), "Expected no errors from send/recv workflows")

		receiverCount := 0
		receivedReceiverResults := make(map[string]bool)
		for result := range receiverResults {
			receiverCount++
			receivedReceiverResults[result] = true
		}

		senderCount := 0
		for result := range senderResults {
			senderCount++
			assert.Equal(t, "message-sent", result, "Expected sender result to be 'message-sent'")
		}

		assert.Equal(t, numPairs, receiverCount, "Expected correct number of receiver results")
		assert.Equal(t, numPairs, senderCount, "Expected correct number of sender results")

		for i := range numPairs {
			expectedReceiverResult := fmt.Sprintf("send-recv-message-%d", i)
			assert.True(t, receivedReceiverResults[expectedReceiverResult], "Expected receiver result '%s' not found", expectedReceiverResult)
		}
	})
}

func TestWorkflowAtVersion(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	RegisterWorkflow(dbosCtx, simpleWorkflow)

	version := "test-app-version-12345"
	handle, err := RunWorkflow(dbosCtx, simpleWorkflow, "input", WithApplicationVersion(version))
	require.NoError(t, err, "failed to start workflow")

	_, err = handle.GetResult()
	require.NoError(t, err, "failed to get workflow result")

	retrieved, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
	require.NoError(t, err, "failed to retrieve workflow")

	status, err := retrieved.GetStatus()
	require.NoError(t, err, "failed to get workflow status")
	assert.Equal(t, version, status.ApplicationVersion, "expected correct application version")
}

func TestWorkflowCancel(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	blockingEvent := NewEvent()

	// Workflow that waits for an event, then calls Recv(). Returns raw error if Recv fails
	blockingWorkflow := func(ctx DBOSContext, topic string) (string, error) {
		// Wait for the event
		blockingEvent.Wait()

		// Now call Recv() - this should fail if the workflow is cancelled
		msg, err := Recv[string](ctx, topic, 5*time.Second)
		if err != nil {
			return "", err // Return the raw error from Recv
		}
		return msg, nil
	}
	RegisterWorkflow(dbosCtx, blockingWorkflow)

	t.Run("TestWorkflowCancelWithRecvError", func(t *testing.T) {
		topic := "cancel-test-topic"

		// Start the blocking workflow
		handle, err := RunWorkflow(dbosCtx, blockingWorkflow, topic)
		require.NoError(t, err, "failed to start blocking workflow")

		// Cancel the workflow using DBOS.CancelWorkflow
		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to cancel workflow")

		// Signal the event so the workflow can move on to Recv()
		blockingEvent.Set()

		// Check the return values of the workflow
		result, err := handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		// Check that we get a DBOSError with WorkflowCancelled code
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)
		assert.Equal(t, WorkflowCancelled, dbosErr.Code, "expected AwaitedWorkflowCancelled error code, got: %v", dbosErr.Code)

		// Ensure the workflow status is of an error type
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	t.Run("TestWorkflowCancelWithSuccess", func(t *testing.T) {
		blockingEventNoError := NewEvent()

		// Workflow that waits for an event, then calls Recv(). Does NOT return error when Recv times out
		blockingWorkflowNoError := func(ctx DBOSContext, topic string) (string, error) {
			// Wait for the event
			blockingEventNoError.Wait()
			Recv[string](ctx, topic, 5*time.Second)
			// Ignore the error
			return "", nil
		}
		RegisterWorkflow(dbosCtx, blockingWorkflowNoError)

		topic := "cancel-no-error-test-topic"

		// Start the blocking workflow
		handle, err := RunWorkflow(dbosCtx, blockingWorkflowNoError, topic)
		require.NoError(t, err, "failed to start blocking workflow")

		// Cancel the workflow using DBOS.CancelWorkflow
		err = CancelWorkflow(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to cancel workflow")

		// Signal the event so the workflow can move on to Recv()
		blockingEventNoError.Set()

		// Check the return values of the workflow
		// Because this is a direct handle it'll not return an error
		result, err := handle.GetResult()
		require.NoError(t, err, "expected no error from direct handle")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		// Now use a polling handle to get result -- observe the error
		pollingHandle, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to retrieve workflow with polling handle")

		result, err = pollingHandle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow even when workflow returns success")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		// Check that we still get a DBOSError with AwaitedWorkflowCancelled code
		// The gate prevents CANCELLED -> SUCCESS transition
		var dbosErr *DBOSError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DBOSError, got %T", err)
		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected AwaitedWorkflowCancelled error code, got: %v", dbosErr.Code)

		// Ensure the workflow status remains CANCELLED
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to remain WorkflowStatusCancelled due to gate")
	})
}

var cancelAllBeforeBlockEvent = NewEvent()

func cancelAllBeforeBlockingWorkflow(ctx DBOSContext, input string) (string, error) {
	cancelAllBeforeBlockEvent.Wait()
	return input, nil
}

func TestCancelAllBefore(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	RegisterWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflow)

	// Create a queue for testing enqueued workflows
	queue := NewWorkflowQueue(dbosCtx, "test-cancel-queue")

	t.Run("CancelAllBefore", func(t *testing.T) {
		now := time.Now()
		cutoffTime := now.Add(3 * time.Second)

		// Create workflows that should be cancelled (PENDING/ENQUEUED before cutoff)
		shouldBeCancelledIDs := make([]string, 0)

		// Create 2 PENDING workflows before cutoff time
		for i := range 2 {
			handle, err := RunWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow, fmt.Sprintf("pending-before-%d", i))
			require.NoError(t, err, "failed to start pending workflow %d", i)
			shouldBeCancelledIDs = append(shouldBeCancelledIDs, handle.GetWorkflowID())
		}

		// Create 2 ENQUEUED workflows before cutoff time
		for i := range 2 {
			handle, err := RunWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow, fmt.Sprintf("enqueued-before-%d", i), WithQueue(queue.Name))
			require.NoError(t, err, "failed to start enqueued workflow %d", i)
			shouldBeCancelledIDs = append(shouldBeCancelledIDs, handle.GetWorkflowID())
		}

		// Create workflows that should NOT be cancelled

		// Create 1 SUCCESS workflow before cutoff time (but complete it)
		successHandle, err := RunWorkflow(dbosCtx, simpleWorkflow, "success-before")
		require.NoError(t, err, "failed to start success workflow")
		_, err = successHandle.GetResult()
		require.NoError(t, err, "failed to complete success workflow")
		shouldNotBeCancelledIDs := []string{successHandle.GetWorkflowID()}

		// Sleep to ensure we pass the cutoff time
		time.Sleep(4 * time.Second)

		// Create 2 PENDING/ENQUEUED workflows after cutoff time
		for i := range 2 {
			handle, err := RunWorkflow(dbosCtx, cancelAllBeforeBlockingWorkflow, fmt.Sprintf("pending-after-%d", i))
			require.NoError(t, err, "failed to start pending workflow after cutoff %d", i)
			shouldNotBeCancelledIDs = append(shouldNotBeCancelledIDs, handle.GetWorkflowID())
		}

		// Call cancelAllBefore
		err = dbosCtx.(*dbosContext).systemDB.cancelAllBefore(dbosCtx, cutoffTime)
		require.NoError(t, err, "failed to call cancelAllBefore")

		// Verify workflows that should be cancelled
		for _, wfID := range shouldBeCancelledIDs {
			handle, err := RetrieveWorkflow[string](dbosCtx, wfID)
			require.NoError(t, err, "failed to retrieve workflow %s", wfID)

			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status for workflow %s", wfID)
			assert.Equal(t, WorkflowStatusCancelled, status.Status, "workflow %s should be cancelled", wfID)
		}

		// Verify workflows that should NOT be cancelled
		for _, wfID := range shouldNotBeCancelledIDs {
			handle, err := RetrieveWorkflow[string](dbosCtx, wfID)
			require.NoError(t, err, "failed to retrieve workflow %s", wfID)

			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status for workflow %s", wfID)
			assert.NotEqual(t, WorkflowStatusCancelled, status.Status, "workflow %s should NOT be cancelled", wfID)
		}

		// Unblock any remaining workflows
		cancelAllBeforeBlockEvent.Set()

		// Wait for workflows to complete and verify they were cancelled
		for _, wfID := range shouldBeCancelledIDs {
			handle, err := RetrieveWorkflow[string](dbosCtx, wfID)
			require.NoError(t, err, "failed to retrieve cancelled workflow %s", wfID)

			_, err = handle.GetResult()
			if err != nil {
				// Should get a DBOSError with AwaitedWorkflowCancelled code
				var dbosErr *DBOSError
				if errors.As(err, &dbosErr) {
					assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected AwaitedWorkflowCancelled error code for workflow %s, got: %v", wfID, dbosErr.Code)
				} else {
					// Fallback: check if error message contains "cancelled"
					assert.Contains(t, err.Error(), "cancelled", "expected cancellation error for workflow %s", wfID)
				}
			}
		}
	})
}

func gcTestStep(_ context.Context, x int) (int, error) {
	return x, nil
}

func gcTestWorkflow(dbosCtx DBOSContext, x int) (int, error) {
	result, err := RunAsStep(dbosCtx, func(ctx context.Context) (int, error) {
		return gcTestStep(ctx, x)
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func gcBlockedWorkflow(dbosCtx DBOSContext, event *Event) (string, error) {
	event.Wait()
	workflowID, err := GetWorkflowID(dbosCtx)
	if err != nil {
		return "", err
	}
	return workflowID, nil
}

func TestGarbageCollect(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("GarbageCollectWithOffset", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, false, true)
		gcTestEvent := NewEvent()

		// Ensure the event is set at the end to unblock any remaining workflows
		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		gcTestEvent.Clear()
		numWorkflows := 10

		// Start one blocked workflow and 10 normal workflows
		blockedHandle, err := RunWorkflow(dbosCtx, gcBlockedWorkflow, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		var completedHandles []WorkflowHandle[int]
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			completedHandles = append(completedHandles, handle)
		}

		// Verify exactly 11 workflows exist before GC (1 blocked + 10 completed)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows+1, len(workflows), "expected exactly %d workflows before GC", numWorkflows+1)

		// Garbage collect keeping only the 5 newest workflows
		// The blocked workflow won't be deleted because it's pending
		threshold := 5
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold: &threshold,
		})
		require.NoError(t, err, "failed to garbage collect workflows")

		// Verify workflows after GC - should have 6 workflows:
		// - 5 newest workflows (by creation time cutoff determined by threshold)
		// - 1 blocked workflow (preserved because it's pending)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after GC")
		require.Equal(t, 6, len(workflows), "expected exactly 6 workflows after GC (5 from threshold + 1 pending)")

		// Create a map of remaining workflow IDs for easy lookup
		remainingIDs := make(map[string]bool)
		for _, wf := range workflows {
			remainingIDs[wf.ID] = true
		}

		// Verify blocked workflow still exists (since it's pending)
		require.True(t, remainingIDs[blockedHandle.GetWorkflowID()], "blocked workflow should still exist after GC")

		// Find status of blocked workflow
		for _, wf := range workflows {
			if wf.ID == blockedHandle.GetWorkflowID() {
				require.Equal(t, WorkflowStatusPending, wf.Status, "blocked workflow should still be pending")
				break
			}
		}

		// Verify that the 5 newest completed workflows are preserved
		// The completedHandles slice is in order of creation (0 is oldest, 9 is newest)
		// So indices 5-9 (the last 5) should be preserved
		for i := range numWorkflows {
			wfID := completedHandles[i].GetWorkflowID()
			if i < numWorkflows-threshold {
				// Older workflows (indices 0-4) should be deleted
				require.False(t, remainingIDs[wfID], "older workflow at index %d (ID: %s) should have been deleted", i, wfID)
			} else {
				// Newer workflows (indices 5-9) should be preserved
				require.True(t, remainingIDs[wfID], "newer workflow at index %d (ID: %s) should have been preserved", i, wfID)
			}
		}

		// Complete the blocked workflow
		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowID(), result, "expected blocked workflow to return its ID")
	})

	t.Run("GarbageCollectWithCutoffTime", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, false, true)
		gcTestEvent := NewEvent()

		// Ensure the event is set at the end to unblock any remaining workflows
		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		gcTestEvent.Clear()
		numWorkflows := 10

		// Start blocked workflow BEFORE cutoff to verify pending workflows are preserved
		blockedHandle, err := RunWorkflow(dbosCtx, gcBlockedWorkflow, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		// Execute first batch of workflows (before cutoff)
		var beforeCutoffHandles []WorkflowHandle[int]
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			beforeCutoffHandles = append(beforeCutoffHandles, handle)
		}

		// Wait to ensure clear time separation between batches
		time.Sleep(500 * time.Millisecond)
		cutoffTime := time.Now()
		// Additional small delay to ensure cutoff is after all first batch workflows
		time.Sleep(100 * time.Millisecond)

		// Execute second batch of workflows after cutoff
		var afterCutoffHandles []WorkflowHandle[int]
		for i := numWorkflows; i < numWorkflows*2; i++ {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			afterCutoffHandles = append(afterCutoffHandles, handle)
		}

		// Verify exactly 21 workflows exist before GC (1 blocked + 10 old + 10 new)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, 21, len(workflows), "expected exactly 21 workflows before GC (1 blocked + 10 old + 10 new)")

		// Garbage collect workflows completed before cutoff time
		cutoffTimestamp := cutoffTime.UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &cutoffTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect workflows by time")

		// Verify exactly 11 workflows remain after GC (1 blocked + 10 new completed)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after time-based GC")
		require.Equal(t, 11, len(workflows), "expected exactly 11 workflows after time-based GC (1 blocked + 10 new)")

		// Create a map of remaining workflow IDs for easy lookup
		remainingIDs := make(map[string]bool)
		for _, wf := range workflows {
			remainingIDs[wf.ID] = true
		}

		// Verify blocked workflow still exists (even though it was created before cutoff)
		require.True(t, remainingIDs[blockedHandle.GetWorkflowID()], "blocked workflow should still exist after GC")

		// Verify that all workflows created before cutoff were deleted (except the blocked one)
		for _, handle := range beforeCutoffHandles {
			wfID := handle.GetWorkflowID()
			require.False(t, remainingIDs[wfID], "workflow created before cutoff (ID: %s) should have been deleted", wfID)
		}

		// Verify that all workflows created after cutoff were preserved
		for _, handle := range afterCutoffHandles {
			wfID := handle.GetWorkflowID()
			require.True(t, remainingIDs[wfID], "workflow created after cutoff (ID: %s) should have been preserved", wfID)
		}

		// Complete the blocked workflow
		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowID(), result, "expected blocked workflow to return its ID")

		// Wait a moment to ensure the completed workflow timestamp is after creation
		time.Sleep(100 * time.Millisecond)

		// Garbage collect all workflows - use a future cutoff to catch everything
		futureTimestamp := time.Now().Add(1 * time.Hour).UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &futureTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect all completed workflows")

		// Verify exactly 0 workflows remain
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after final GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after final GC")
	})

	t.Run("GarbageCollectEmptyDatabase", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, false, true)

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		// Verify exactly 0 workflows exist initially
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows in empty database")

		// Verify GC runs without errors on a blank table
		threshold := 1
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold: &threshold,
		})
		require.NoError(t, err, "garbage collect should work on empty database")

		// Verify still 0 workflows after row-based GC
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after row-based GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after row-based GC on empty database")

		currentTimestamp := time.Now().UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &currentTimestamp,
		})
		require.NoError(t, err, "time-based garbage collect should work on empty database")

		// Verify still 0 workflows after time-based GC
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after time-based GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after time-based GC on empty database")
	})

	t.Run("GarbageCollectOnlyCompletedWorkflows", func(t *testing.T) {
		// Start with clean database for precise workflow counting
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, false, true)
		gcTestEvent := NewEvent()

		// Ensure the event is set at the end to unblock any remaining workflows
		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		RegisterWorkflow(dbosCtx, gcTestWorkflow)
		RegisterWorkflow(dbosCtx, gcBlockedWorkflow)

		gcTestEvent.Clear()
		numWorkflows := 5

		// Start blocked workflow that will remain pending
		blockedHandle, err := RunWorkflow(dbosCtx, gcBlockedWorkflow, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		// Execute normal workflows to completion
		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
		}

		// Verify exactly 6 workflows exist (1 blocked + 5 completed)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows+1, len(workflows), "expected exactly %d workflows", numWorkflows+1)

		// Count pending vs completed workflows
		pendingCount := 0
		completedCount := 0
		for _, wf := range workflows {
			switch wf.Status {
			case WorkflowStatusPending:
				pendingCount++
			case WorkflowStatusSuccess:
				completedCount++
			}
		}
		require.Equal(t, 1, pendingCount, "expected exactly 1 pending workflow")
		require.Equal(t, numWorkflows, completedCount, "expected exactly %d completed workflows", numWorkflows)

		// GC keeping only the 1 newest workflow
		// The blocked workflow is the oldest but won't be deleted because it's pending
		// So we should have 2 workflows: 1 newest completed + 1 pending
		threshold := 1
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold: &threshold,
		})
		require.NoError(t, err, "failed to garbage collect workflows")

		// Verify exactly 2 workflows remain (1 newest + 1 pending)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after GC")
		require.Equal(t, 2, len(workflows), "expected exactly 2 workflows after GC (1 newest + 1 pending)")

		// Verify pending workflow still exists
		found := false
		pendingCount = 0
		completedCount = 0
		for _, wf := range workflows {
			if wf.ID == blockedHandle.GetWorkflowID() {
				found = true
				require.Equal(t, WorkflowStatusPending, wf.Status, "blocked workflow should still be pending")
			}
			switch wf.Status {
			case WorkflowStatusPending:
				pendingCount++
			case WorkflowStatusSuccess:
				completedCount++
			}
		}
		require.True(t, found, "pending workflow should remain")
		require.Equal(t, 1, pendingCount, "expected exactly 1 pending workflow after GC")
		require.Equal(t, 1, completedCount, "expected exactly 1 completed workflow after GC")

		// Complete the blocked workflow and verify GC works
		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowID(), result, "expected blocked workflow to return its ID")

		// Wait a moment to ensure the completed workflow timestamp is after creation
		time.Sleep(100 * time.Millisecond)

		// Now GC everything using future timestamp
		futureTimestamp := time.Now().Add(1 * time.Hour).UnixMilli()
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &futureTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect all workflows")

		// Verify exactly 0 workflows remain
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after final GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after final GC")
	})

	t.Run("ThresholdAndCutoffTimestampInteraction", func(t *testing.T) {
		// Reset database for clean test environment
		resetTestDatabase(t, databaseURL)
		dbosCtx := setupDBOS(t, false, true)

		// Register the test workflow
		RegisterWorkflow(dbosCtx, gcTestWorkflow)

		// This test verifies that when both threshold and cutoff timestamp are provided,
		// the more stringent (restrictive) one applies - i.e., the one that keeps more workflows

		// Create 10 workflows with different timestamps
		numWorkflows := 10
		handles := make([]WorkflowHandle[int], numWorkflows)

		for i := range numWorkflows {
			handle, err := RunWorkflow(dbosCtx, gcTestWorkflow, i)
			require.NoError(t, err, "failed to start workflow %d", i)
			handles[i] = handle

			// Add small delay to ensure distinct timestamps
			time.Sleep(10 * time.Millisecond)
		}

		// Wait for all workflows to complete
		for i, handle := range handles {
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from workflow %d", i)
			require.Equal(t, i, result)
		}

		// Get timestamps for testing
		workflows, err := ListWorkflows(dbosCtx, WithSortDesc())
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows, len(workflows))

		// Workflows are ordered newest first in ListWorkflows
		var cutoff1 int64 // Will keep 5 newest when used as cutoff
		var cutoff2 int64 // Will keep 8 newest when used as cutoff

		cutoff1 = workflows[7].CreatedAt.UnixMilli() // 3rd oldest workflow
		cutoff2 = workflows[1].CreatedAt.UnixMilli() // 9th oldest workflow

		// Case 1: Threshold is more restrictive (higher/more recent cutoff)
		// Threshold would keep 6 newest, timestamp would keep 8 newest
		// Result: threshold wins (higher timestamp), only 6 workflows remain
		threshold := 6
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold:          &threshold,
			cutoffEpochTimestampMs: &cutoff1,
		})
		require.NoError(t, err, "failed to garbage collect with threshold 6 and 7th newest timestamp")

		workflows, err = ListWorkflows(dbosCtx, WithSortDesc())
		require.NoError(t, err, "failed to list workflows after first GC")
		require.Equal(t, threshold, len(workflows), "expected 6 workflows when threshold has more recent cutoff than timestamp")

		for i := 0; i < len(workflows)-threshold; i++ {
			require.Equal(t, workflows[i].ID, handles[i].GetWorkflowID(), "expected workflow %d to remain", i)
		}

		// Case2: Threshold is less restrictive (lower cutoff)
		threshold = 3
		err = dbosCtx.(*dbosContext).systemDB.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold:          &threshold,
			cutoffEpochTimestampMs: &cutoff2,
		})
		require.NoError(t, err, "failed to garbage collect with threshold 3 and 2nd newest timestamp")

		workflows, err = ListWorkflows(dbosCtx, WithSortDesc())
		require.NoError(t, err, "failed to list workflows after second GC")
		require.Equal(t, 2, len(workflows), "expected 2 workflows after second GC")
		require.Equal(t, workflows[0].ID, handles[numWorkflows-1].GetWorkflowID(), "expected newest workflow to remain")
		require.Equal(t, workflows[1].ID, handles[numWorkflows-2].GetWorkflowID(), "expected 2nd newest workflow to remain")
	})
}

// TestSpecialSteps tests that special workflow functions (ListWorkflows, CancelWorkflow,
// ResumeWorkflow, ForkWorkflow, GetWorkflowSteps) work correctly as durable steps
func TestSpecialSteps(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	specialStepsEvent := NewEvent()
	blockingEvent := NewEvent()
	childEvent := NewEvent()

	// Child workflow that blocks on an event (for cancellation testing)
	childWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
		// Wait for event to be set (will be cancelled before this happens)
		childEvent.Wait()
		return fmt.Sprintf("auxiliary-result-%s", input), nil
	}

	// Main workflow that uses all special steps
	specialStepsWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
		defer specialStepsEvent.Set()

		currentWorkflowID, err := GetWorkflowID(dbosCtx)
		if err != nil {
			return "", fmt.Errorf("failed to get current workflow ID: %w", err)
		}

		// Step 0: Start a child workflow to use in other operations
		childHandle, err := RunWorkflow(dbosCtx, childWorkflow, "test")
		if err != nil {
			return "", fmt.Errorf("failed to start child workflow: %w", err)
		}

		// Step 1: Use CancelWorkflow on the child workflow (should be cancelled while waiting)
		err = CancelWorkflow(dbosCtx, childHandle.GetWorkflowID())
		if err != nil {
			return "", fmt.Errorf("CancelWorkflow failed: %w", err)
		}

		// Step 2: Use RetrieveWorkflow (list workflows under the hood)
		retrievedHandle, err := RetrieveWorkflow[string](dbosCtx, childHandle.GetWorkflowID())
		if err != nil {
			return "", fmt.Errorf("RetrieveWorkflow failed: %w", err)
		}
		if retrievedHandle.GetWorkflowID() != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("RetrieveWorkflow returned wrong workflow ID")
		}

		// Step 3: Check status of cancelled workflow (calls listWorkflows under the hood)
		status, err := retrievedHandle.GetStatus()
		if err != nil {
			return "", fmt.Errorf("failed to get status of retrieved workflow: %w", err)
		}
		if status.Status != WorkflowStatusCancelled {
			return "", fmt.Errorf("expected cancelled workflow status, got %v", status.Status)
		}

		// Step 4: resume the cancelled workflow
		resumeHandle, err := ResumeWorkflow[string](dbosCtx, childHandle.GetWorkflowID())
		if err != nil {
			fmt.Println("error in ResumeWorkflow:", err)
			return "", fmt.Errorf("ResumeWorkflow failed: %w", err)
		}
		if resumeHandle.GetWorkflowID() != childHandle.GetWorkflowID() {
			return "", fmt.Errorf("ResumeWorkflow returned wrong workflow ID")
		}

		// Step 5: Use ForkWorkflow
		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowID: currentWorkflowID,
			StartStep:          0,
		})
		if err != nil {
			return "", fmt.Errorf("ForkWorkflow failed: %w", err)
		}
		if forkHandle.GetWorkflowID() == "" {
			return "", fmt.Errorf("ForkWorkflow returned empty workflow ID")
		}

		// Step 6: Use GetWorkflowSteps on current workflow
		steps, err := GetWorkflowSteps(dbosCtx, currentWorkflowID)
		if err != nil {
			return "", fmt.Errorf("GetWorkflowSteps failed: %w", err)
		}
		if len(steps) != 6 {
			t.Logf("Expected 6 steps so far, got %d", len(steps))
			for step := range steps {
				t.Logf("Step %d: %s (Error: %v)\n", steps[step].StepID, steps[step].StepName, steps[step].Error)
			}
			return "", fmt.Errorf("Expected 6 steps so far, got %d", len(steps))
		}

		// Step 7: Use ListWorkflows at the end to check expected count
		workflows, err := ListWorkflows(dbosCtx, WithLimit(100))
		if err != nil {
			return "", fmt.Errorf("ListWorkflows failed: %w", err)
		}
		// We should have at least 3 workflows: main, child, and forked
		foundMain := false
		foundChild := false
		foundForked := false
		for _, wf := range workflows {
			if wf.ID == currentWorkflowID {
				foundMain = true
			}
			if wf.ID == childHandle.GetWorkflowID() {
				foundChild = true
			}
			if wf.ID == forkHandle.GetWorkflowID() {
				foundForked = true
			}
		}
		if !foundMain || !foundChild || !foundForked {
			return "", fmt.Errorf("ListWorkflows did not return expected workflows. Found main: %v, child: %v, forked: %v", foundMain, foundChild, foundForked)
		}

		// Signal that all special steps are complete
		specialStepsEvent.Set()
		// Unblock the child
		childEvent.Set()
		// Wait for test to unblock
		blockingEvent.Wait()

		return "success", nil
	}

	RegisterWorkflow(dbosCtx, childWorkflow, WithWorkflowName("child-workflow"))
	RegisterWorkflow(dbosCtx, specialStepsWorkflow)

	t.Run("SpecialStepsExecution", func(t *testing.T) {
		// Start the main workflow
		workflowID := uuid.NewString()
		_, err := RunWorkflow(dbosCtx, specialStepsWorkflow, "test-input", WithWorkflowID(workflowID))
		require.NoError(t, err, "failed to start special steps workflow")

		// Wait for all special steps to complete
		specialStepsEvent.Wait()

		// Test recovery - recover the pending workflow before unblocking
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")

		// Find our workflow in the recovered handles
		var recoveredHandle WorkflowHandle[any]
		for _, h := range recoveredHandles {
			if h.GetWorkflowID() == workflowID {
				recoveredHandle = h
				break
			}
		}
		require.NotNil(t, recoveredHandle, "workflow should be recovered")

		// Verify that the special steps were recorded properly (and once, idempotently)
		steps, err := GetWorkflowSteps(dbosCtx, workflowID)
		require.NoError(t, err, "failed to get workflow steps")

		// We should have at least 8 steps for the special functions
		require.Equal(t, len(steps), 8, "expected 8 steps")
		require.Equal(t, "child-workflow", steps[0].StepName, "first step should be child-workflow")
		require.Equal(t, "DBOS.cancelWorkflow", steps[1].StepName, "second step should be DBOS.cancelWorkflow")
		require.Equal(t, "DBOS.retrieveWorkflow", steps[2].StepName, "third step should be DBOS.retrieveWorkflow")
		require.Equal(t, "DBOS.getStatus", steps[3].StepName, "fourth step should be DBOS.getStatus")
		require.Equal(t, "DBOS.resumeWorkflow", steps[4].StepName, "fifth step should be DBOS.resumeWorkflow")
		require.Equal(t, "DBOS.forkWorkflow", steps[5].StepName, "sixth step should be DBOS.forkWorkflow")
		require.Equal(t, "DBOS.getWorkflowSteps", steps[6].StepName, "seventh step should be DBOS.getWorkflowSteps")
		require.Equal(t, "DBOS.listWorkflows", steps[7].StepName, "eighth step should be DBOS.listWorkflows")

		// Unblock the main workflow
		blockingEvent.Set()

		// Get final result
		result, err := recoveredHandle.GetResult()
		require.NoError(t, err, "workflow should complete successfully")
		require.Equal(t, "success", result, "workflow should return success")
	})
}

func TestRegisteredWorkflowListing(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)

	// Register some regular workflows
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	RegisterWorkflow(dbosCtx, simpleWorkflowError, WithMaxRetries(5))
	RegisterWorkflow(dbosCtx, simpleWorkflowWithStep, WithWorkflowName("CustomStepWorkflow"))
	RegisterWorkflow(dbosCtx, simpleWorkflowWithSchedule, WithWorkflowName("ScheduledWorkflow"), WithSchedule("0 0 * * * *"))

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ListRegisteredWorkflows", func(t *testing.T) {
		workflows, err := ListRegisteredWorkflows(dbosCtx)
		require.NoError(t, err, "ListRegisteredWorkflows should not return an error")

		// Should have 4 workflows (3 regular + 1 scheduled)
		require.GreaterOrEqual(t, len(workflows), 4, "Should have 4 registered workflows")

		// Create a map for easier lookup
		workflowMap := make(map[string]WorkflowRegistryEntry)
		for _, wf := range workflows {
			workflowMap[wf.FQN] = wf
		}

		// Check that simpleWorkflow is registered
		simpleWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflow).Pointer()).Name()
		simpleWf, exists := workflowMap[simpleWorkflowFQN]
		require.True(t, exists, "simpleWorkflow should be registered")
		require.Equal(t, _DEFAULT_MAX_RECOVERY_ATTEMPTS, simpleWf.MaxRetries, "simpleWorkflow should have default max retries")
		require.Empty(t, simpleWf.CronSchedule, "simpleWorkflow should not have cron schedule")

		// Check that simpleWorkflowError is registered with custom max retries
		simpleWorkflowErrorFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowError).Pointer()).Name()
		errorWf, exists := workflowMap[simpleWorkflowErrorFQN]
		require.True(t, exists, "simpleWorkflowError should be registered")
		require.Equal(t, 5, errorWf.MaxRetries, "simpleWorkflowError should have custom max retries")
		require.Empty(t, errorWf.CronSchedule, "simpleWorkflowError should not have cron schedule")

		// Check that custom named workflow is registered
		customStepWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithStep).Pointer()).Name()
		customWf, exists := workflowMap[customStepWorkflowFQN]
		require.True(t, exists, "CustomStepWorkflow should be found")
		require.Equal(t, "CustomStepWorkflow", customWf.Name, "CustomStepWorkflow should have the correct name")
		require.Empty(t, customWf.CronSchedule, "CustomStepWorkflow should not have cron schedule")

		// Check that scheduled workflow is registered
		scheduledWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithSchedule).Pointer()).Name()
		scheduledWf, exists := workflowMap[scheduledWorkflowFQN]
		require.True(t, exists, "ScheduledWorkflow should be found")
		require.Equal(t, "ScheduledWorkflow", scheduledWf.Name, "ScheduledWorkflow should have the correct name")
		require.Equal(t, "0 0 * * * *", scheduledWf.CronSchedule, "ScheduledWorkflow should have the correct cron schedule")
	})

	t.Run("ListRegisteredWorkflowsWithScheduledOnly", func(t *testing.T) {
		scheduledWorkflows, err := ListRegisteredWorkflows(dbosCtx, WithScheduledOnly())
		require.NoError(t, err, "ListRegisteredWorkflows with WithScheduledOnly should not return an error")
		require.Equal(t, 1, len(scheduledWorkflows), "Should have exactly 1 scheduled workflow")

		entry := scheduledWorkflows[0]
		scheduledWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithSchedule).Pointer()).Name()
		require.Equal(t, scheduledWorkflowFQN, entry.FQN, "ScheduledWorkflow should have the correct FQN")
		require.Equal(t, "0 0 * * * *", entry.CronSchedule, "ScheduledWorkflow should have the correct cron schedule")
	})
}

func TestWorkflowIdentity(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, simpleWorkflow)
	handle, err := RunWorkflow(
		dbosCtx,
		simpleWorkflow,
		"test",
		WithWorkflowID("my-workflow-id"),
		WithAuthenticatedUser("user123"),
		WithAssumedRole("admin"),
		WithAuthenticatedRoles([]string{"reader", "writer"}))
	require.NoError(t, err, "failed to start workflow")

	// Retrieve the workflow's status.
	status, err := handle.GetStatus()
	require.NoError(t, err)

	t.Run("CheckAuthenticatedUser", func(t *testing.T) {
		assert.Equal(t, "user123", status.AuthenticatedUser)
	})

	t.Run("CheckAssumedRole", func(t *testing.T) {
		assert.Equal(t, "admin", status.AssumedRole)
	})

	t.Run("CheckAuthenticatedRoles", func(t *testing.T) {
		assert.Equal(t, []string{"reader", "writer"}, status.AuthenticatedRoles)
	})
}

func TestWorkflowHandles(t *testing.T) {
dbosCtx := setupDBOS(t, true, true)
RegisterWorkflow(dbosCtx, slowWorkflow)

t.Run("WorkflowHandleTimeout", func(t *testing.T) {
handle, err := RunWorkflow(dbosCtx, slowWorkflow, 10*time.Second)
require.NoError(t, err, "failed to start workflow")

start := time.Now()
_, err = handle.GetResult(WithHandleTimeout(10*time.Millisecond), WithPollingInterval(1*time.Millisecond))
duration := time.Since(start)

require.Error(t, err, "expected timeout error")
assert.Contains(t, err.Error(), "workflow result timeout")
assert.True(t, duration < 100*time.Millisecond, "timeout should occur quickly")
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected error to be detectable as context.DeadlineExceeded, got: %v", err)
	})

	t.Run("WorkflowPollingHandleTimeout", func(t *testing.T) {
		// Start a workflow that will block on the first signal
		originalHandle, err := RunWorkflow(dbosCtx, slowWorkflow, 10*time.Second)
		require.NoError(t, err, "failed to start workflow")

pollingHandle, err := RetrieveWorkflow[string](dbosCtx, originalHandle.GetWorkflowID())
require.NoError(t, err, "failed to retrieve workflow")

_, ok := pollingHandle.(*workflowPollingHandle[string])
require.True(t, ok, "expected polling handle, got %T", pollingHandle)

_, err = pollingHandle.GetResult(WithHandleTimeout(10*time.Millisecond), WithPollingInterval(1*time.Millisecond))

require.Error(t, err, "expected timeout error")
assert.True(t, errors.Is(err, context.DeadlineExceeded),
"expected error to be detectable as context.DeadlineExceeded, got: %v", err)
})
}

func TestWorkflowHandleContextCancel(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	RegisterWorkflow(dbosCtx, getEventWorkflow)

	t.Run("WorkflowHandleContextCancel", func(t *testing.T) {
		getEventWorkflowStartedSignal.Clear()
		handle, err := RunWorkflow(dbosCtx, getEventWorkflow, getEventWorkflowInput{
			TargetWorkflowID: "test-workflow-id",
			Key:              "test-key",
		})
		require.NoError(t, err, "failed to start workflow")

		resultChan := make(chan error)
		go func() {
			_, err := handle.GetResult()
			resultChan <- err
		}()

		getEventWorkflowStartedSignal.Wait()
		getEventWorkflowStartedSignal.Clear()

		dbosCtx.Shutdown(1 * time.Second)

		err = <-resultChan
		require.Error(t, err, "expected error from cancelled context")
		assert.True(t, errors.Is(err, context.Canceled),
			"expected error to be detectable as context.Canceled, got: %v", err)
	})
}

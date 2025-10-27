package dbos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	Created  time.Time
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

func serializerNilValueWorkflow(ctx DBOSContext, input any) (any, error) {
	return RunAsStep(ctx, func(context context.Context) (any, error) {
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

	// Test default serializer (should be JSON)
	t.Run("DefaultSerializer", func(t *testing.T) {
		config := Config{
			DatabaseURL: getDatabaseURL(),
			AppName:     "test-app",
			// No serializer specified - should default to JSON
		}

		executor, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err, "Failed to create DBOS context with default serializer")

		// Verify the default serializer is JSON
		if dbosCtx, ok := executor.(*dbosContext); ok {
			assert.NotNil(t, dbosCtx.serializer, "Default serializer should be configured")
			assert.IsType(t, &JSONSerializer{}, dbosCtx.serializer, "Default should be JSONSerializer")
		}
	})
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
					Created:  time.Now(),
					Metadata: map[string]string{"key": "value"},
				}

				handle, err := RunWorkflow(executor, serializerTestWorkflow, input)
				require.NoError(t, err, "Workflow execution failed")

				// Wait for completion
				_, err = handle.GetResult()
				require.NoError(t, err, "Failed to get workflow result")

				// 1. Test with handle.GetResult()
				t.Run("HandleGetResult", func(t *testing.T) {
					result, err := handle.GetResult()
					require.NoError(t, err, "Failed to get workflow result")
					assert.Equal(t, input, result, "Workflow result should match input")
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
					assert.Equal(t, input, workflow.Input, "Workflow input from ListWorkflows should match original")
					assert.Equal(t, input, workflow.Output, "Workflow output from ListWorkflows should match original")
				})

				// 3. Test with GetWorkflowSteps
				t.Run("GetWorkflowSteps", func(t *testing.T) {
					steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
					require.NoError(t, err, "Failed to get workflow steps")
					require.Len(t, steps, 1, "Expected 1 step")

					step := steps[0]
					require.NotNil(t, step.Output, "Step output should not be nil")
					assert.Equal(t, input, step.Output, "Step output should match original")
					assert.Nil(t, step.Error, "Step should not have error")
				})
			})

			// Test nil values
			t.Run("NilValues", func(t *testing.T) {
				handle, err := RunWorkflow(executor, serializerNilValueWorkflow, nil)
				require.NoError(t, err, "Nil workflow execution failed")

				// Wait for completion
				_, err = handle.GetResult()
				require.NoError(t, err, "Failed to get nil workflow result")

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

				// 4. Test database storage (nil values stored as empty strings)
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
			})

			// Test error values
			t.Run("ErrorValues", func(t *testing.T) {
				input := TestWorkflowData{
					ID:       "error-test-id",
					Message:  "error test",
					Value:    123,
					Active:   true,
					Data:     TestData{Message: "error data", Value: 456, Active: false},
					Created:  time.Now(),
					Metadata: map[string]string{"type": "error"},
				}

				handle, err := RunWorkflow(executor, serializerErrorWorkflow, input)
				require.NoError(t, err, "Error workflow execution failed")

				// Wait for completion (will error)
				_, err = handle.GetResult()
				require.Error(t, err, "Should get step error")

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

		})
	}
}

// Test serializer interface compliance
func TestSerializerInterface(t *testing.T) {
	// Test that both serializers implement the Serializer interface
	var _ Serializer = (*GobSerializer)(nil)
	var _ Serializer = (*JSONSerializer)(nil)
}

package dbos

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMetrics(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	defer Shutdown(dbosCtx, 1*time.Minute)

	// Get the internal systemDB instance
	sysDB, ok := dbosCtx.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	require.NotNil(t, sysDB.systemDB)

	// Define test workflows
	testWorkflowA := func(ctx DBOSContext, input string) (string, error) {
		_, err := RunAsStep(ctx, func(_ context.Context) (string, error) {
			return "x", nil
		}, WithStepName("testStepX"))
		if err != nil {
			return "", err
		}
		_, err = RunAsStep(ctx, func(_ context.Context) (string, error) {
			return "x", nil
		}, WithStepName("testStepX"))
		if err != nil {
			return "", err
		}
		return "a", nil
	}

	testWorkflowB := func(ctx DBOSContext, input string) (string, error) {
		_, err := RunAsStep(ctx, func(_ context.Context) (string, error) {
			return "y", nil
		}, WithStepName("testStepY"))
		if err != nil {
			return "", err
		}
		return "b", nil
	}

	// Register workflows with custom names
	RegisterWorkflow(dbosCtx, testWorkflowA, WithWorkflowName("testWorkflowA"))
	RegisterWorkflow(dbosCtx, testWorkflowB, WithWorkflowName("testWorkflowB"))

	// Record start time before creating workflows
	startTime := time.Now()

	// Execute workflows to create metrics data
	handle1, err := RunWorkflow(dbosCtx, testWorkflowA, "input1")
	require.NoError(t, err)
	_, err = handle1.GetResult()
	require.NoError(t, err)

	handle2, err := RunWorkflow(dbosCtx, testWorkflowA, "input2")
	require.NoError(t, err)
	_, err = handle2.GetResult()
	require.NoError(t, err)

	handle3, err := RunWorkflow(dbosCtx, testWorkflowB, "input3")
	require.NoError(t, err)
	_, err = handle3.GetResult()
	require.NoError(t, err)

	// Query metrics from start to now + 10 hours
	endTime := time.Now().Add(10 * time.Hour)
	metrics, err := sysDB.systemDB.getMetrics(context.Background(), startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(metrics), 4, "Expected at least 4 metrics (2 workflow counts + 2 step counts)")

	// Convert to map for easier assertion
	metricsMap := make(map[string]float64)
	for _, m := range metrics {
		key := m.MetricType + ":" + m.MetricName
		metricsMap[key] = m.Value
	}

	// Verify workflow counts
	workflowCountAFound := false
	workflowCountBFound := false
	stepCountXFound := false
	stepCountYFound := false

	for _, m := range metrics {
		if m.MetricType == "workflow_count" && m.MetricName == "testWorkflowA" {
			workflowCountAFound = true
			assert.Equal(t, float64(2), m.Value, "testWorkflowA should have 2 executions")
		}
		if m.MetricType == "workflow_count" && m.MetricName == "testWorkflowB" {
			workflowCountBFound = true
			assert.Equal(t, float64(1), m.Value, "testWorkflowB should have 1 execution")
		}
		if m.MetricType == "step_count" && m.MetricName == "testStepX" {
			stepCountXFound = true
			assert.Equal(t, float64(4), m.Value, "testStepX should have 4 executions (2 workflows * 2 steps)")
		}
		if m.MetricType == "step_count" && m.MetricName == "testStepY" {
			stepCountYFound = true
			assert.Equal(t, float64(1), m.Value, "testStepY should have 1 execution")
		}
	}

	assert.True(t, workflowCountAFound, "Should have testWorkflowA workflow_count metric")
	assert.True(t, workflowCountBFound, "Should have testWorkflowB workflow_count metric")
	assert.True(t, stepCountXFound, "Should have testStepX step_count metric")
	assert.True(t, stepCountYFound, "Should have testStepY step_count metric")
}

func TestGetMetricsEmptyTimeRange(t *testing.T) {
	dbosCtx := setupDBOS(t, true, true)
	defer Shutdown(dbosCtx, 1*time.Minute)

	sysDB, ok := dbosCtx.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	require.NotNil(t, sysDB.systemDB)

	// Query metrics for a time range with no data
	futureTime := time.Now().Add(24 * time.Hour)
	futureTime2 := futureTime.Add(1 * time.Hour)

	metrics, err := sysDB.systemDB.getMetrics(context.Background(), futureTime.Format(time.RFC3339), futureTime2.Format(time.RFC3339))
	require.NoError(t, err)
	assert.Equal(t, 0, len(metrics), "Should return empty metrics for future time range")
}

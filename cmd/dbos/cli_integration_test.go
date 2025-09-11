package main

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed cli_test_app.go.test
var testAppContent []byte

// Test configuration
const (
	testProjectName = "test-project"
	testServerPort  = "8080"
	testTimeout     = 30 * time.Second
	// From dbos/queue.go
	dbosInternalQueueName = "_dbos_internal_queue"
)

// getDatabaseURL returns a default database URL if none is configured, following dbos/utils_test.go pattern
func getDatabaseURL() string {
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	return databaseURL
}

// TestCLIWorkflow provides comprehensive integration testing of the DBOS CLI
func TestCLIWorkflow(t *testing.T) {
	// Build the CLI once at the beginning
	cliPath := buildCLI(t)
	t.Logf("Built CLI at: %s", cliPath)
	t.Cleanup(func() {
		os.Remove(cliPath)
	})

	// Create temporary directory for test
	tempDir := t.TempDir()
	originalDir, err := os.Getwd()
	require.NoError(t, err)

	// Change to temp directory for test isolation
	err = os.Chdir(tempDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		os.Chdir(originalDir)
	})

	t.Run("ResetDatabase", func(t *testing.T) {
		cmd := exec.Command(cliPath, "reset", "-y")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Reset database command failed: %s", string(output))

		assert.Contains(t, string(output), "System database has been reset successfully", "Output should confirm database reset")
		assert.Contains(t, string(output), "database\":\"dbos", "Output should confirm database reset")

		// log in the database and ensure the dbos schema does not exist anymore
		db, err := sql.Open("pgx", getDatabaseURL())
		require.NoError(t, err)
		defer db.Close()

		var exists bool
		err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dbos')").Scan(&exists)
		require.NoError(t, err)

		assert.False(t, exists, "DBOS schema should not exist")
	})

	t.Run("ProjectInitialization", func(t *testing.T) {
		testProjectInitialization(t, cliPath)
	})

	t.Run("ApplicationLifecycle", func(t *testing.T) {
		cmd := testApplicationLifecycle(t, cliPath)
		t.Cleanup(func() {
			if cmd.Process != nil {
				/*
					fmt.Println(cmd.Stderr)
					fmt.Println(cmd.Stdout)
				*/
				cmd.Process.Kill()
			}
		})
	})

	t.Run("WorkflowCommands", func(t *testing.T) {
		testWorkflowCommands(t, cliPath)
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		testErrorHandling(t, cliPath)
	})
}

// testProjectInitialization verifies project initialization
func testProjectInitialization(t *testing.T, cliPath string) {

	// Initialize project
	cmd := exec.Command(cliPath, "init", testProjectName)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Init command failed: %s", string(output))

	outputStr := string(output)
	assert.Contains(t, outputStr, fmt.Sprintf("Created new DBOS application: %s", testProjectName))

	// Verify project structure
	projectDir := filepath.Join(".", testProjectName)
	require.DirExists(t, projectDir)

	// Check required files
	requiredFiles := []string{"go.mod", "main.go", "dbos-config.yaml"}
	for _, file := range requiredFiles {
		filePath := filepath.Join(projectDir, file)
		require.FileExists(t, filePath, "Required file %s should exist", file)
	}

	// Verify go.mod content
	goModContent, err := os.ReadFile(filepath.Join(projectDir, "go.mod"))
	require.NoError(t, err)
	assert.Contains(t, string(goModContent), testProjectName, "go.mod should contain project name")

	// Verify main.go content
	mainGoContent, err := os.ReadFile(filepath.Join(projectDir, "main.go"))
	require.NoError(t, err)
	assert.Contains(t, string(mainGoContent), testProjectName, "main.go should contain project name")

	// Replace the template main.go with our embedded test app
	mainGoPath := filepath.Join(projectDir, "main.go")
	err = os.WriteFile(mainGoPath, testAppContent, 0644)
	require.NoError(t, err, "Failed to write test app to main.go")

	// Run go mod tidy to prepare for build
	err = os.Chdir(projectDir)
	require.NoError(t, err)

	modCmd := exec.Command("go", "mod", "tidy")
	modOutput, err := modCmd.CombinedOutput()
	require.NoError(t, err, "go mod tidy failed: %s", string(modOutput))
}

// testApplicationLifecycle starts the application and triggers workflows
func testApplicationLifecycle(t *testing.T, cliPath string) *exec.Cmd {
	// Should already be in project directory from previous test

	// Start the application in background
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, cliPath, "start")
	cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

	// Capture output for debugging
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Start()
	require.NoError(t, err, "Failed to start application")

	// Wait for server to be ready
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://localhost:" + testServerPort)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 10*time.Second, 500*time.Millisecond, "Server should start within 10 seconds")

	// Trigger workflows via HTTP endpoints
	t.Run("TriggerExampleWorkflow", func(t *testing.T) {
		resp, err := http.Get("http://localhost:" + testServerPort + "/workflow")
		require.NoError(t, err, "Failed to trigger workflow")
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Workflow endpoint should return 200")
		assert.Contains(t, string(body), "Workflow result", "Should contain workflow result")
	})

	t.Run("TriggerQueueWorkflow", func(t *testing.T) {
		resp, err := http.Get("http://localhost:" + testServerPort + "/queue")
		require.NoError(t, err, "Failed to trigger queue workflow")
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue endpoint should return 200")

		// Parse JSON response to get workflow ID
		var response map[string]string
		err = json.Unmarshal(body, &response)
		require.NoError(t, err, "Should be valid JSON response")

		workflowID, exists := response["workflow_id"]
		assert.True(t, exists, "Response should contain workflow_id")
		assert.NotEmpty(t, workflowID, "Workflow ID should not be empty")
	})

	return cmd
}

// testWorkflowCommands comprehensively tests all workflow CLI commands
func testWorkflowCommands(t *testing.T, cliPath string) {

	t.Run("ListWorkflows", func(t *testing.T) {
		testListWorkflows(t, cliPath)
	})

	t.Run("GetWorkflow", func(t *testing.T) {
		testGetWorkflow(t, cliPath)
	})

	t.Run("CancelResumeWorkflow", func(t *testing.T) {
		testCancelResumeWorkflow(t, cliPath)
	})

	t.Run("ForkWorkflow", func(t *testing.T) {
		testForkWorkflow(t, cliPath)
	})

	t.Run("GetWorkflowSteps", func(t *testing.T) {
		testGetWorkflowSteps(t, cliPath)
	})
}

// testListWorkflows tests various workflow listing scenarios
func testListWorkflows(t *testing.T, cliPath string) {
	// Create some test workflows first to ensure we have data to filter
	// The previous test functions have already created workflows that we can query

	// Get the current time for time-based filtering
	currentTime := time.Now()

	testCases := []struct {
		name              string
		args              []string
		expectWorkflows   bool
		expectQueuedCount int
		checkQueueNames   bool
		maxCount          int
		minCount          int
		checkStatus       dbos.WorkflowStatusType
	}{
		{
			name:            "BasicList",
			args:            []string{"workflow", "list"},
			expectWorkflows: true,
		},
		{
			name:            "LimitedList",
			args:            []string{"workflow", "list", "--limit", "5"},
			expectWorkflows: true,
			maxCount:        5,
		},
		{
			name:     "OffsetPagination",
			args:     []string{"workflow", "list", "--limit", "3", "--offset", "1"},
			maxCount: 3,
		},
		{
			name:            "SortDescending",
			args:            []string{"workflow", "list", "--sort-desc", "--limit", "10"},
			expectWorkflows: true,
			maxCount:        10,
		},
		{
			name:              "QueueNameFilter",
			args:              []string{"workflow", "list", "--queue", "example-queue"},
			expectWorkflows:   true,
			expectQueuedCount: 10, // From QueueWorkflow which enqueues 10 workflows
			checkQueueNames:   true,
		},
		{
			name:            "StatusFilterSuccess",
			args:            []string{"workflow", "list", "--status", "SUCCESS"},
			expectWorkflows: true,
			checkStatus:     dbos.WorkflowStatusSuccess,
		},
		{
			name:        "StatusFilterError",
			args:        []string{"workflow", "list", "--status", "ERROR"},
			checkStatus: dbos.WorkflowStatusError,
		},
		{
			name:        "StatusFilterPending",
			args:        []string{"workflow", "list", "--status", "PENDING"},
			checkStatus: dbos.WorkflowStatusPending,
		},
		{
			name:        "StatusFilterCancelled",
			args:        []string{"workflow", "list", "--status", "CANCELLED"},
			checkStatus: dbos.WorkflowStatusCancelled,
		},
		{
			name:            "TimeRangeFilter",
			args:            []string{"workflow", "list", "--start-time", currentTime.Add(-1 * time.Hour).Format(time.RFC3339), "--end-time", currentTime.Add(1 * time.Hour).Format(time.RFC3339)},
			expectWorkflows: true,
		},
		{
			name:     "CurrentTimeStartFilter",
			args:     []string{"workflow", "list", "--start-time", currentTime.Format(time.RFC3339)},
			maxCount: 0, // Should return no workflows as all were created before currentTime
		},
		{
			name:     "FutureTimeFilter",
			args:     []string{"workflow", "list", "--start-time", currentTime.Add(1 * time.Hour).Format(time.RFC3339)},
			maxCount: 0, // Should return no workflows
		},
		{
			name:            "PastTimeFilter",
			args:            []string{"workflow", "list", "--end-time", currentTime.Add(1 * time.Hour).Format(time.RFC3339)},
			expectWorkflows: true, // Should return all workflows created before now + 1 hour
		},
		{
			name:            "WorkflowNameFilter",
			args:            []string{"workflow", "list", "--name", "main.QueueWorkflow"},
			expectWorkflows: true,
			minCount:        1, // Should find at least the QueueWorkflow
		},
		{
			name:            "QueuesOnlyFilter",
			args:            []string{"workflow", "list", "--queues-only"},
			expectWorkflows: true,
			minCount:        10, // Should find at least the enqueued workflows
		},
		{
			name:            "UserFilter",
			args:            []string{"workflow", "list", "--user", "test-user"},
			expectWorkflows: false, // No workflows with test-user in this test
		},
		{
			name:     "LargeLimit",
			args:     []string{"workflow", "list", "--limit", "100"},
			maxCount: 100,
		},
		{
			name:            "CombinedTimeAndStatus",
			args:            []string{"workflow", "list", "--status", "SUCCESS", "--start-time", currentTime.Add(-2 * time.Hour).Format(time.RFC3339)},
			expectWorkflows: true,
			checkStatus:     dbos.WorkflowStatusSuccess,
		},
		{
			name:            "QueueAndTimeFilter",
			args:            []string{"workflow", "list", "--queue", "example-queue", "--end-time", currentTime.Add(1 * time.Hour).Format(time.RFC3339)},
			expectWorkflows: true,
			checkQueueNames: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(cliPath, tc.args...)
			cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "List command failed: %s", string(output))

			// Parse JSON output
			var workflows []dbos.WorkflowStatus
			err = json.Unmarshal(output, &workflows)
			require.NoError(t, err, "JSON output should be valid")

			if tc.expectWorkflows {
				assert.Greater(t, len(workflows), 0, "Should have workflows")
			}

			if tc.expectQueuedCount > 0 {
				assert.Equal(t, tc.expectQueuedCount, len(workflows), "Should have expected number of queued workflows")
			}

			if tc.maxCount > 0 {
				assert.LessOrEqual(t, len(workflows), tc.maxCount, "Should not exceed max count")
			}

			if tc.minCount > 0 {
				assert.GreaterOrEqual(t, len(workflows), tc.minCount, "Should have at least min count")
			}

			if tc.checkQueueNames {
				for _, wf := range workflows {
					assert.NotEmpty(t, wf.QueueName, "Queued workflows should have queue name")
				}
			}

			if tc.checkStatus != "" {
				for _, wf := range workflows {
					assert.Equal(t, tc.checkStatus, wf.Status, "All workflows should have status %s", tc.checkStatus)
				}
			}
		})
	}
}

// testGetWorkflow tests retrieving individual workflow details
func testGetWorkflow(t *testing.T, cliPath string) {
	resp, err := http.Get("http://localhost:" + testServerPort + "/queue")
	require.NoError(t, err, "Failed to trigger queue workflow")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Parse JSON response to get workflow ID
	var response map[string]string
	err = json.Unmarshal(body, &response)
	require.NoError(t, err, "Should be valid JSON response")

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue endpoint should return 200")

	workflowID, exists := response["workflow_id"]
	assert.True(t, exists, "Response should contain workflow_id")
	assert.NotEmpty(t, workflowID, "Workflow ID should not be empty")

	t.Run("GetWorkflowJSON", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "get", workflowID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Get workflow JSON command failed: %s", string(output))

		// Verify valid JSON
		var status dbos.WorkflowStatus
		err = json.Unmarshal(output, &status)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, workflowID, status.ID, "JSON should contain correct workflow ID")
		assert.NotEmpty(t, status.Status, "Should have workflow status")
		assert.NotEmpty(t, status.Name, "Should have workflow name")

		// Redo the test with the db url in flags
		cmd2 := exec.Command(cliPath, "workflow", "get", workflowID, "--db-url", getDatabaseURL())

		output2, err2 := cmd2.CombinedOutput()
		require.NoError(t, err2, "Get workflow JSON command failed: %s", string(output2))

		// Verify valid JSON
		var status2 dbos.WorkflowStatus
		err = json.Unmarshal(output2, &status2)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, workflowID, status2.ID, "JSON should contain correct workflow ID")
		assert.NotEmpty(t, status2.Status, "Should have workflow status")
		assert.NotEmpty(t, status2.Name, "Should have workflow name")
	})
}

// testCancelResumeWorkflow tests workflow state management
func testCancelResumeWorkflow(t *testing.T, cliPath string) {
	resp, err := http.Get("http://localhost:" + testServerPort + "/queue")
	require.NoError(t, err, "Failed to trigger queue workflow")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Parse JSON response to get workflow ID
	var response map[string]string
	err = json.Unmarshal(body, &response)
	require.NoError(t, err, "Should be valid JSON response")

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue endpoint should return 200")

	workflowID, exists := response["workflow_id"]
	assert.True(t, exists, "Response should contain workflow_id")
	assert.NotEmpty(t, workflowID, "Workflow ID should not be empty")

	t.Run("CancelWorkflow", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "cancel", workflowID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Cancel workflow command failed: %s", string(output))

		assert.Contains(t, string(output), "Successfully cancelled", "Should confirm cancellation")

		// Verify workflow is actually cancelled
		getCmd := exec.Command(cliPath, "workflow", "get", workflowID)
		getCmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		getOutput, err := getCmd.CombinedOutput()
		require.NoError(t, err, "Get workflow status failed: %s", string(getOutput))

		var status dbos.WorkflowStatus
		err = json.Unmarshal(getOutput, &status)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, "CANCELLED", string(status.Status), fmt.Sprintf("Workflow %s should be cancelled", workflowID))
	})

	t.Run("ResumeWorkflow", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "resume", workflowID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Resume workflow command failed: %s", string(output))

		// Parse JSON response from resume command
		var resumeStatus dbos.WorkflowStatus
		err = json.Unmarshal(output, &resumeStatus)
		require.NoError(t, err, "Resume JSON output should be valid")

		assert.Equal(t, workflowID, resumeStatus.ID, "Should be the same workflow ID")
		assert.Equal(t, "ENQUEUED", string(resumeStatus.Status), "Resumed workflow should be enqueued")
		assert.Equal(t, dbosInternalQueueName, resumeStatus.QueueName, "Should be on internal queue")
	})
}

// testForkWorkflow tests workflow forking functionality
func testForkWorkflow(t *testing.T, cliPath string) {
	resp, err := http.Get("http://localhost:" + testServerPort + "/queue")
	require.NoError(t, err, "Failed to trigger queue workflow")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Parse JSON response to get workflow ID
	var response map[string]string
	err = json.Unmarshal(body, &response)
	require.NoError(t, err, "Should be valid JSON response")

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue endpoint should return 200")

	workflowID, exists := response["workflow_id"]
	assert.True(t, exists, "Response should contain workflow_id")
	assert.NotEmpty(t, workflowID, "Workflow ID should not be empty")

	t.Run("ForkWorkflow", func(t *testing.T) {
		newID := uuid.NewString()
		targetVersion := "1.0.0"
		cmd := exec.Command(cliPath, "workflow", "fork", workflowID, "--forked-workflow-id", newID, "--application-version", targetVersion)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Fork workflow command failed: %s", string(output))

		// Parse JSON response
		var forkedStatus dbos.WorkflowStatus
		err = json.Unmarshal(output, &forkedStatus)
		require.NoError(t, err, "Fork JSON output should be valid")

		assert.NotEqual(t, workflowID, forkedStatus.ID, "Forked workflow should have different ID")
		assert.Equal(t, "ENQUEUED", string(forkedStatus.Status), "Forked workflow should be enqueued")
		assert.Equal(t, dbosInternalQueueName, forkedStatus.QueueName, "Should be on internal queue")
		assert.Equal(t, newID, forkedStatus.ID, "Forked workflow should have specified ID")
		assert.Equal(t, targetVersion, forkedStatus.ApplicationVersion, "Forked workflow should have specified application version")
	})

	t.Run("ForkWorkflowFromStep", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "fork", workflowID, "--step", "2")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Fork workflow from step command failed: %s", string(output))

		// Parse JSON response
		var forkedStatus dbos.WorkflowStatus
		err = json.Unmarshal(output, &forkedStatus)
		require.NoError(t, err, "Fork from step JSON output should be valid")

		assert.NotEqual(t, workflowID, forkedStatus.ID, "Forked workflow should have different ID")
		assert.Equal(t, "ENQUEUED", string(forkedStatus.Status), "Forked workflow should be enqueued")
		assert.Equal(t, dbosInternalQueueName, forkedStatus.QueueName, "Should be on internal queue")
	})

	t.Run("ForkWorkflowFromNegativeStep", func(t *testing.T) {
		// Test fork with invalid step number (0 should be converted to 1)
		cmd := exec.Command(cliPath, "workflow", "fork", workflowID, "--step", "-1")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Fork workflow from step command failed: %s", string(output))

		// Parse JSON response
		var forkedStatus dbos.WorkflowStatus
		err = json.Unmarshal(output, &forkedStatus)
		require.NoError(t, err, "Fork from step JSON output should be valid")

		assert.NotEqual(t, workflowID, forkedStatus.ID, "Forked workflow should have different ID")
		assert.Equal(t, "ENQUEUED", string(forkedStatus.Status), "Forked workflow should be enqueued")
		assert.Equal(t, dbosInternalQueueName, forkedStatus.QueueName, "Should be on internal queue")
	})
}

// testGetWorkflowSteps tests retrieving workflow steps
func testGetWorkflowSteps(t *testing.T, cliPath string) {
	resp, err := http.Get("http://localhost:" + testServerPort + "/queue")
	require.NoError(t, err, "Failed to trigger queue workflow")
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Parse JSON response to get workflow ID
	var response map[string]string
	err = json.Unmarshal(body, &response)
	require.NoError(t, err, "Should be valid JSON response")

	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue endpoint should return 200")

	workflowID, exists := response["workflow_id"]
	assert.True(t, exists, "Response should contain workflow_id")
	assert.NotEmpty(t, workflowID, "Workflow ID should not be empty")

	t.Run("GetStepsJSON", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "steps", workflowID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Get workflow steps JSON command failed: %s", string(output))

		// Verify valid JSON
		var steps []dbos.StepInfo
		err = json.Unmarshal(output, &steps)
		require.NoError(t, err, "JSON output should be valid")

		// Steps array should be valid (could be empty for simple workflows)
		assert.NotNil(t, steps, "Steps should not be nil")

		// If steps exist, verify structure
		for _, step := range steps {
			assert.Greater(t, step.StepID, -1, fmt.Sprintf("Step ID should be positive for workflow %s", workflowID))
			assert.NotEmpty(t, step.StepName, fmt.Sprintf("Step name should not be empty for workflow %s", workflowID))
		}
	})
}

// testErrorHandling tests various error conditions and edge cases
func testErrorHandling(t *testing.T, cliPath string) {

	t.Run("InvalidWorkflowID", func(t *testing.T) {
		invalidID := "invalid-workflow-id-12345"

		// Test get with invalid ID
		cmd := exec.Command(cliPath, "workflow", "get", invalidID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid workflow ID")
		assert.Contains(t, string(output), "workflow not found", "Should contain error message")
	})

	t.Run("MissingWorkflowID", func(t *testing.T) {
		// Test get without workflow ID
		cmd := exec.Command(cliPath, "workflow", "get")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail without workflow ID")
		assert.Contains(t, string(output), "accepts 1 arg(s), received 0", "Should show usage error")
	})

	t.Run("InvalidStatusFilter", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "list", "--status", "INVALID_STATUS")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid status")
		assert.Contains(t, string(output), "invalid status", "Should contain status error message")
	})

	t.Run("InvalidTimeFormat", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "list", "--start-time", "invalid-time-format")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid time format")
		assert.Contains(t, string(output), "invalid start-time format", "Should contain time format error")
	})

	t.Run("MissingDatabaseURL", func(t *testing.T) {
		// Test without system DB url in the flags or env var
		cmd := exec.Command(cliPath, "workflow", "list")

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail without database URL")
		// Error message will vary based on implementation
		outputStr := string(output)
		assert.True(t,
			assert.Contains(t, outputStr, "database") ||
				assert.Contains(t, outputStr, "connection") ||
				assert.Contains(t, outputStr, "url"),
			"Should contain database-related error")
	})
}

// Helper functions

func buildCLI(t *testing.T) string {
	// Get the directory where this test file is located
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "Failed to get current file path")

	// The cmd directory is where this test file is located
	cmdDir := filepath.Dir(filename)

	// Build output path in the cmd directory
	cliPath := filepath.Join(cmdDir, "dbos-cli-test")

	// Check if already built
	if _, err := os.Stat(cliPath); os.IsNotExist(err) {
		// Build the CLI from the cmd directory
		buildCmd := exec.Command("go", "build", "-o", "dbos-cli-test", ".")
		buildCmd.Dir = cmdDir
		buildOutput, buildErr := buildCmd.CombinedOutput()
		require.NoError(t, buildErr, "Failed to build CLI: %s", string(buildOutput))
	}

	// Return absolute path
	absPath, err := filepath.Abs(cliPath)
	require.NoError(t, err, "Failed to get absolute path")
	return absPath
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	// Always have a database URL available
	dbURL := getDatabaseURL()

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

	t.Run("DatabaseReset", func(t *testing.T) {
		testDatabaseReset(t, dbURL)
	})
	
	t.Run("DatabaseURLPrecedence", func(t *testing.T) {
		testDatabaseURLPrecedence(t)
	})

	t.Run("ProjectInitialization", func(t *testing.T) {
		testProjectInitialization(t)
	})

	t.Run("ApplicationLifecycle", func(t *testing.T) {
		testApplicationLifecycle(t)
	})

	t.Run("WorkflowCommands", func(t *testing.T) {
		testWorkflowCommands(t)
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		testErrorHandling(t)
	})
}

// testDatabaseReset verifies the reset command works correctly
func testDatabaseReset(t *testing.T, dbURL string) {
	cliPath := getCliPath(t)

	cmd := exec.Command(cliPath, "reset", "--yes")
	cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+dbURL)

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Reset command failed: %s", string(output))

	outputStr := string(output)
	assert.Contains(t, outputStr, "has been reset successfully", "Reset should confirm success")
}

// testDatabaseURLPrecedence tests that database URL precedence is respected
func testDatabaseURLPrecedence(t *testing.T) {
	cliPath := getCliPath(t)
	
	// Test 1: Environment variable should be used when available
	testDB1 := "postgres://postgres:dbos@localhost:5432/dbos_test1?sslmode=disable"
	cmd := exec.Command(cliPath, "workflow", "list", "--json")
	cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+testDB1)
	
	output, err := cmd.CombinedOutput()
	// We don't expect this to succeed since dbos_test1 likely doesn't exist, 
	// but we should get a database-related error, confirming it tried to use our URL
	assert.Error(t, err, "Should get error when connecting to non-existent test database")
	assert.Contains(t, string(output), "test1", "Error should reference the test database name")
	
	// Test 2: CLI flag should override environment variable 
	testDB2 := "postgres://postgres:dbos@localhost:5432/dbos_test2?sslmode=disable"
	cmd2 := exec.Command(cliPath, "workflow", "list", "--json", "--database-url", testDB2)
	cmd2.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+testDB1) // Different from flag
	
	output2, err2 := cmd2.CombinedOutput()
	// Again, expect error but should try the flag URL, not env URL
	assert.Error(t, err2, "Should get error when connecting to non-existent test database")
	assert.Contains(t, string(output2), "test2", "Error should reference the flag database name, not env")
}

// testProjectInitialization verifies project initialization
func testProjectInitialization(t *testing.T) {
	cliPath := getCliPath(t)

	// Initialize project
	cmd := exec.Command(cliPath, "init", testProjectName)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Init command failed: %s", string(output))

	outputStr := string(output)
	assert.Contains(t, outputStr, fmt.Sprintf("Created new DBOS application in '%s'", testProjectName))

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

	// Run go mod tidy to prepare for build
	err = os.Chdir(projectDir)
	require.NoError(t, err)

	modCmd := exec.Command("go", "mod", "tidy")
	modOutput, err := modCmd.CombinedOutput()
	require.NoError(t, err, "go mod tidy failed: %s", string(modOutput))
}

// testApplicationLifecycle starts the application and triggers workflows
func testApplicationLifecycle(t *testing.T) {
	// Should already be in project directory from previous test
	projectDir := filepath.Join(".", testProjectName)
	err := os.Chdir(projectDir)
	require.NoError(t, err)

	// Start the application in background
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Get CLI path for dbos start command
	cliPath := getCliPath(t)
	
	cmd := exec.CommandContext(ctx, cliPath, "start")
	cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

	// Capture output for debugging
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Start()
	require.NoError(t, err, "Failed to start application")

	// Ensure process cleanup
	t.Cleanup(func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
	})

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
		assert.Contains(t, string(body), "Successfully completed", "Should contain success message")
	})

	// Give workflows time to complete
	time.Sleep(2 * time.Second)
}

// testWorkflowCommands comprehensively tests all workflow CLI commands
func testWorkflowCommands(t *testing.T) {
	cliPath := getCliPath(t)

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
}

// testListWorkflows tests various workflow listing scenarios
func testListWorkflows(t *testing.T, cliPath string) {
	testCases := []struct {
		name               string
		args               []string
		expectWorkflows    bool
		expectQueuedCount  int
		checkQueueNames    bool
	}{
		{
			name:            "BasicList",
			args:            []string{"workflow", "list", "--json"},
			expectWorkflows: true,
		},
		{
			name:            "LimitedList",
			args:            []string{"workflow", "list", "--json", "--limit", "5"},
			expectWorkflows: true,
		},
		{
			name:              "QueueOnlyList",
			args:              []string{"workflow", "list", "--json", "--queues-only"},
			expectWorkflows:   true,
			expectQueuedCount: 10, // From QueueWorkflow which enqueues 10 workflows
			checkQueueNames:   true,
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

			if tc.checkQueueNames {
				for _, wf := range workflows {
					assert.NotEmpty(t, wf.QueueName, "Queued workflows should have queue name")
				}
			}

			if tc.name == "LimitedList" {
				assert.LessOrEqual(t, len(workflows), 5, "Limited list should respect limit")
			}
		})
	}
}

// testGetWorkflow tests retrieving individual workflow details
func testGetWorkflow(t *testing.T, cliPath string) {
	// First get a workflow ID from the list
	workflowID := getFirstWorkflowID(t, cliPath)
	require.NotEmpty(t, workflowID, "Should have at least one workflow ID available")

	t.Run("GetWorkflowJSON", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "get", workflowID, "--json")
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
	})
}

// testCancelResumeWorkflow tests workflow state management
func testCancelResumeWorkflow(t *testing.T, cliPath string) {
	// Get a workflow ID that's not already cancelled
	workflowID := getFirstWorkflowID(t, cliPath)
	if workflowID == "" {
		t.Skip("No workflows found, skipping cancel/resume test")
	}

	t.Run("CancelWorkflow", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "cancel", workflowID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Cancel workflow command failed: %s", string(output))

		assert.Contains(t, string(output), "Successfully cancelled", "Should confirm cancellation")
		
		// Verify workflow is actually cancelled
		getCmd := exec.Command(cliPath, "workflow", "get", workflowID, "--json")
		getCmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())
		
		getOutput, err := getCmd.CombinedOutput()
		require.NoError(t, err, "Get workflow status failed: %s", string(getOutput))
		
		var status dbos.WorkflowStatus
		err = json.Unmarshal(getOutput, &status)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, "CANCELLED", string(status.Status), "Workflow should be cancelled")
	})

	t.Run("ResumeWorkflow", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "resume", workflowID, "--json")
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
	// Get a workflow ID to fork
	workflowID := getFirstWorkflowID(t, cliPath)
	if workflowID == "" {
		t.Skip("No workflows found, skipping fork test")
	}

	t.Run("ForkWorkflow", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "fork", workflowID, "--json")
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
	})

	t.Run("ForkWorkflowFromStep", func(t *testing.T) {
		cmd := exec.Command(cliPath, "workflow", "fork", workflowID, "--step", "2", "--json")
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

// Helper functions

// getCliPath returns the path to the CLI executable
func getCliPath(t *testing.T) string {
	// Assume CLI is built and available in the cmd directory
	cliPath := filepath.Join("..", "dbos")
	if _, err := os.Stat(cliPath); os.IsNotExist(err) {
		// Try building it
		buildCmd := exec.Command("go", "build", "-o", "dbos", ".")
		buildCmd.Dir = ".."
		buildOutput, buildErr := buildCmd.CombinedOutput()
		require.NoError(t, buildErr, "Failed to build CLI: %s", string(buildOutput))
	}
	return cliPath
}

// getFirstWorkflowID retrieves the first workflow ID from the list
func getFirstWorkflowID(t *testing.T, cliPath string) string {
	cmd := exec.Command(cliPath, "workflow", "list", "--json", "--limit", "1")
	cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to list workflows: %s", string(output))

	var workflows []dbos.WorkflowStatus
	err = json.Unmarshal(output, &workflows)
	require.NoError(t, err, "Failed to parse workflow JSON")

	if len(workflows) == 0 {
		return ""
	}

	return workflows[0].ID
}

// testErrorHandling tests various error conditions and edge cases
func testErrorHandling(t *testing.T) {
	cliPath := getCliPath(t)

	t.Run("InvalidWorkflowID", func(t *testing.T) {
		invalidID := "invalid-workflow-id-12345"

		// Test get with invalid ID
		cmd := exec.Command(cliPath, "workflow", "get", invalidID)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid workflow ID")
		assert.Contains(t, string(output), "failed to retrieve workflow", "Should contain error message")
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
		// Test without DBOS_SYSTEM_DATABASE_URL
		cmd := exec.Command(cliPath, "workflow", "list")
		// Don't set DBOS_SYSTEM_DATABASE_URL

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

	t.Run("InitExistingDirectory", func(t *testing.T) {
		// Try to init in already existing directory
		cmd := exec.Command(cliPath, "init", testProjectName)
		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail when directory already exists")
		assert.Contains(t, string(output), "already exists", "Should mention directory exists")
	})

	t.Run("ForkWithInvalidStep", func(t *testing.T) {
		workflowID := getFirstWorkflowID(t, cliPath)
		if workflowID == "" {
			t.Skip("No workflows found for fork test")
		}

		// Test fork with invalid step number (0 should be converted to 1)
		cmd := exec.Command(cliPath, "workflow", "fork", workflowID, "--step", "0")
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL())

		output, err := cmd.CombinedOutput()
		// This should not error as step 0 gets converted to 1
		require.NoError(t, err, "Fork with step 0 should succeed: %s", string(output))
		assert.Contains(t, string(output), "Starting from step: 1", "Step 0 should be converted to 1")
	})
}

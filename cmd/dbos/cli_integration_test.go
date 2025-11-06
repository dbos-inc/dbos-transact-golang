package main

import (
	"bufio"
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
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

//go:embed cli_test_app.go.test
var testAppContent []byte

// Test configuration
const (
	testProjectName = "test-project"
	testServerPort  = "8080"
	// From dbos/queue.go
	dbosInternalQueueName = "_dbos_internal_queue"
)

// getDatabaseURL returns a default database URL if none is configured, following dbos/utils_test.go pattern
func getDatabaseURL(dbRole string) string {
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = "dbos"
	}
	dsn := &url.URL{
		Scheme:   "postgres",
		Host:     "localhost:5432",
		Path:     "/dbos",
		RawQuery: "sslmode=disable",
	}
	dsn.User = url.UserPassword(dbRole, password)
	return dsn.String()
}

// getDatabaseURLKeyValue returns the same connection string in libpq key-value format
// This is useful for testing that commands handle both URL and key-value formats
func getDatabaseURLKeyValue(dbRole string) string {
	password := os.Getenv("PGPASSWORD")
	if password == "" {
		password = "dbos"
	}
	return fmt.Sprintf("user='%s' password='%s' database=dbos host=localhost port=5432 sslmode=disable", dbRole, password)
}

// TestCLIWorkflow provides comprehensive integration testing of the DBOS CLI
func TestCLIWorkflow(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
	)
	// Build the CLI once at the beginning
	cliPath := buildCLI(t)
	t.Logf("Built CLI at: %s", cliPath)
	t.Cleanup(func() {
		os.Remove(cliPath)
	})

	// Test configurations for schema
	testConfigs := []struct {
		name       string
		schemaName string
		dbRole     string
		args       []string
	}{
		{
			name:       "CustomSchema",
			schemaName: "test_schema",
			dbRole:     "postgres",
			args:       []string{"--schema", "test_schema"},
		},
		{
			name:       "DefaultSchema",
			schemaName: "dbos",
			dbRole:     "postgres",
			args:       []string{},
		},
		{
			name:       "FunnySchema",
			schemaName: "F8nny_sCHem@-n@m3",
			dbRole:     "User Name-123@acme.com#$%&!",
			args:       []string{"--schema", "F8nny_sCHem@-n@m3"},
		},
	}

	for _, config := range testConfigs {
		t.Run(config.name, func(t *testing.T) {
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
				args := append([]string{"reset", "-y"}, config.args...)
				cmd := exec.Command(cliPath, args...)
				cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL("postgres"))

				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "Reset database command failed: %s", string(output))

				assert.Contains(t, string(output), "System database has been reset successfully", "Output should confirm database reset")

				// If db role is specified, attempt to remove it from postgres
				if config.dbRole != "postgres" {
					db, err := sql.Open("pgx", getDatabaseURL("postgres"))
					require.NoError(t, err)
					defer db.Close()

					_, err = db.Exec(fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{config.dbRole}.Sanitize()))
					require.NoError(t, err)
				}

				// log in the database and ensure schema and role do not exist anymore
				db, err := sql.Open("pgx", getDatabaseURL("postgres"))
				require.NoError(t, err)
				defer db.Close()

				var exists bool
				err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", config.schemaName).Scan(&exists)
				require.NoError(t, err)
				assert.False(t, exists, fmt.Sprintf("Schema %s should not exist", config.schemaName))

				if config.dbRole != "postgres" {
					err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)", config.dbRole).Scan(&exists)
					require.NoError(t, err)
					assert.False(t, exists, fmt.Sprintf("Role %s should not exist", config.dbRole))
				}
			})

			t.Run("ResetDatabaseWithKeyValueFormat", func(t *testing.T) {
				// Test reset command with key-value format connection string
				args := append([]string{"reset", "-y", "--db-url", getDatabaseURLKeyValue("postgres")}, config.args...)
				cmd := exec.Command(cliPath, args...)

				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "Reset database command with key-value format failed: %s", string(output))

				assert.Contains(t, string(output), "System database has been reset successfully", "Output should confirm database reset")

				// Verify the database was reset by checking schema doesn't exist
				db, err := sql.Open("pgx", getDatabaseURL("postgres"))
				require.NoError(t, err)
				defer db.Close()

				var exists bool
				err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", config.schemaName).Scan(&exists)
				require.NoError(t, err)
				assert.False(t, exists, fmt.Sprintf("Schema %s should not exist after reset", config.schemaName))
			})

			t.Run("ProjectInitialization", func(t *testing.T) {
				testProjectInitialization(t, cliPath)
			})

			t.Run("MigrateCommand", func(t *testing.T) {
				testMigrateCommand(t, cliPath, config.args, config.dbRole)
			})

			// Start a test application using dbos start
			startArgs := append([]string{"start"}, config.args...)
			cmd := exec.CommandContext(context.Background(), cliPath, startArgs...)
			envVars := append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(config.dbRole))
			// Pass the schema to the test app if using custom schema
			if config.schemaName != "dbos" {
				envVars = append(envVars, "DBOS_SCHEMA="+config.schemaName)
			}
			cmd.Env = envVars
			stdout, _ := cmd.StdoutPipe()
			stderr, _ := cmd.StderrPipe()
			require.NoError(t, cmd.Start(), "Failed to start application")
			go func() {
				scanner := bufio.NewScanner(stdout)
				for scanner.Scan() {
					t.Logf("[app stdout] %s", scanner.Text())
				}
			}()
			go func() {
				scanner := bufio.NewScanner(stderr)
				for scanner.Scan() {
					t.Logf("[app stderr] %s", scanner.Text())
				}
			}()

			// Wait for server to be ready
			require.Eventually(t, func() bool {
				resp, err := http.Get("http://localhost:" + testServerPort)
				if err != nil {
					return false
				}
				resp.Body.Close()
				return resp.StatusCode == http.StatusOK
			}, 10*time.Second, 500*time.Millisecond, "Server should start within 10 seconds")

			t.Cleanup(func() {
				fmt.Printf("Cleaning up application process %d\n", cmd.Process.Pid)
				err := syscall.Kill(cmd.Process.Pid, syscall.SIGTERM)
				require.NoError(t, err, "Failed to send interrupt signal to application process")
				_ = cmd.Wait()
			})

			t.Run("WorkflowCommands", func(t *testing.T) {
				testWorkflowCommands(t, cliPath, config.args, config.dbRole)
			})

			t.Run("ErrorHandling", func(t *testing.T) {
				testErrorHandling(t, cliPath, config.args, config.dbRole)
			})
		})
	}
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

	// Replace the go.mod to use the local dbos directory
	err = os.Chdir(projectDir)
	require.NoError(t, err)

	// Get the absolute path to the dbos-transact-golang repository root
	// We need to find the actual repository location, not the temp test directory
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "Failed to get current file path")

	// Navigate from cmd/dbos/cli_integration_test.go to the repo root
	repoRoot := filepath.Dir(filepath.Dir(filepath.Dir(filename)))

	// Use go mod edit to replace the dependency with the local path
	replaceCmd := exec.Command("go", "mod", "edit", "-replace",
		fmt.Sprintf("github.com/dbos-inc/dbos-transact-golang=%s", repoRoot))
	replaceOutput, err := replaceCmd.CombinedOutput()
	require.NoError(t, err, "go mod edit -replace failed: %s", string(replaceOutput))

	// Run go mod tidy to prepare for build
	modCmd := exec.Command("go", "mod", "tidy")
	modOutput, err := modCmd.CombinedOutput()
	require.NoError(t, err, "go mod tidy failed: %s", string(modOutput))
}

func testMigrateCommand(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
	// If the role is set, create it in Postgres first
	if dbRole != "postgres" {
		db, err := sql.Open("pgx", getDatabaseURL("postgres"))
		require.NoError(t, err)
		defer db.Close()
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		query := fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", pgx.Identifier{dbRole}.Sanitize(), password)
		_, err = db.Exec(query)
		require.NoError(t, err)
	}

	args := append([]string{"--verbose", "migrate"}, baseArgs...)
	if dbRole != "postgres" {
		args = append(args, "--app-role", dbRole)
	}
	cmd := exec.Command(cliPath, args...)
	cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL("postgres"))
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Migrate command failed: %s", string(output))
	assert.Contains(t, string(output), "DBOS migrations completed successfully", "Output should confirm migration")
	fmt.Println(string(output))
}

// testWorkflowCommands comprehensively tests all workflow CLI commands
func testWorkflowCommands(t *testing.T, cliPath string, baseArgs []string, dbRole string) {

	t.Run("ListWorkflows", func(t *testing.T) {
		testListWorkflows(t, cliPath, baseArgs, dbRole)
	})

	t.Run("GetWorkflow", func(t *testing.T) {
		testGetWorkflow(t, cliPath, baseArgs, dbRole)
	})

	t.Run("CancelResumeWorkflow", func(t *testing.T) {
		testCancelResumeWorkflow(t, cliPath, baseArgs, dbRole)
	})

	t.Run("ForkWorkflow", func(t *testing.T) {
		testForkWorkflow(t, cliPath, baseArgs, dbRole)
	})

	t.Run("GetWorkflowSteps", func(t *testing.T) {
		testGetWorkflowSteps(t, cliPath, baseArgs, dbRole)
	})
}

// testListWorkflows tests various workflow listing scenarios
func testListWorkflows(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
	// Create some test workflows first to ensure we have data to filter
	resp, err := http.Get("http://localhost:" + testServerPort + "/workflow")
	require.NoError(t, err, "Failed to trigger workflow")
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Workflow endpoint should return 200")
	assert.Contains(t, string(body), "Workflow result", "Should contain workflow result")

	resp, err = http.Get("http://localhost:" + testServerPort + "/queue")
	require.NoError(t, err, "Failed to trigger queue workflow")
	defer resp.Body.Close()
	body, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Queue endpoint should return 200")

	// Parse JSON response to get workflow ID
	var response map[string]string
	err = json.Unmarshal(body, &response)
	require.NoError(t, err, "Should be valid JSON response")

	workflowID, exists := response["workflow_id"]
	assert.True(t, exists, "Response should contain workflow_id")
	assert.NotEmpty(t, workflowID, "Workflow ID should not be empty")
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
			args := append(tc.args, baseArgs...)
			cmd := exec.Command(cliPath, args...)
			cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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

	// Test list command with key-value format connection string
	t.Run("ListWithKeyValueFormat", func(t *testing.T) {
		args := append([]string{"workflow", "list", "--db-url", getDatabaseURLKeyValue(dbRole)}, baseArgs...)
		fmt.Println(args)
		cmd := exec.Command(cliPath, args...)

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "List command with key-value format failed: %s", string(output))

		// Parse JSON output
		var workflows []dbos.WorkflowStatus
		err = json.Unmarshal(output, &workflows)
		require.NoError(t, err, "JSON output should be valid")
		assert.Greater(t, len(workflows), 0, "Should have workflows when using key-value format")
	})
}

// testGetWorkflow tests retrieving individual workflow details
func testGetWorkflow(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
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
		args := append([]string{"workflow", "get", workflowID}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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
		args2 := append([]string{"workflow", "get", workflowID, "--db-url", getDatabaseURL(dbRole)}, baseArgs...)
		cmd2 := exec.Command(cliPath, args2...)

		output2, err2 := cmd2.CombinedOutput()
		require.NoError(t, err2, "Get workflow JSON command failed: %s", string(output2))

		// Verify valid JSON
		var status2 dbos.WorkflowStatus
		err = json.Unmarshal(output2, &status2)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, workflowID, status2.ID, "JSON should contain correct workflow ID")
		assert.NotEmpty(t, status2.Status, "Should have workflow status")
		assert.NotEmpty(t, status2.Name, "Should have workflow name")

		// Test with key-value format connection string (libpq format)
		argsKeyValue := append([]string{"workflow", "get", workflowID, "--db-url", getDatabaseURLKeyValue(dbRole)}, baseArgs...)
		cmdKeyValue := exec.Command(cliPath, argsKeyValue...)

		outputKeyValue, errKeyValue := cmdKeyValue.CombinedOutput()
		require.NoError(t, errKeyValue, "Get workflow JSON command with key-value format failed: %s", string(outputKeyValue))

		// Verify valid JSON
		var statusKeyValue dbos.WorkflowStatus
		err = json.Unmarshal(outputKeyValue, &statusKeyValue)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, workflowID, statusKeyValue.ID, "JSON should contain correct workflow ID")
		assert.NotEmpty(t, statusKeyValue.Status, "Should have workflow status")
		assert.NotEmpty(t, statusKeyValue.Name, "Should have workflow name")

		// Test with config file containing environment variable
		configPath := "dbos-config.yaml"

		// Read the existing config file
		originalConfig, err := os.ReadFile(configPath)
		require.NoError(t, err, "Should be able to read existing config file")

		// Create backup and modify config to use ${D} variable
		originalConfigStr := string(originalConfig)
		modifiedConfig := strings.ReplaceAll(originalConfigStr, "${DBOS_SYSTEM_DATABASE_URL}", "${D}")

		err = os.WriteFile(configPath, []byte(modifiedConfig), 0644)
		require.NoError(t, err, "Should be able to write modified config")

		// Cleanup: restore original config
		t.Cleanup(func() {
			os.WriteFile(configPath, originalConfig, 0644)
		})

		// Test with environment variable D
		args3 := append([]string{"workflow", "get", workflowID}, baseArgs...)
		cmd3 := exec.Command(cliPath, args3...)
		cmd3.Env = append(os.Environ(), "D="+getDatabaseURL(dbRole))

		output3, err3 := cmd3.CombinedOutput()
		require.NoError(t, err3, "Get workflow JSON command with config env var failed: %s", string(output3))

		// Verify valid JSON
		var status3 dbos.WorkflowStatus
		err = json.Unmarshal(output3, &status3)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, workflowID, status3.ID, "JSON should contain correct workflow ID")
		assert.NotEmpty(t, status3.Status, "Should have workflow status")
		assert.NotEmpty(t, status3.Name, "Should have workflow name")
	})
}

// testCancelResumeWorkflow tests workflow state management
func testCancelResumeWorkflow(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
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
		args := append([]string{"workflow", "cancel", workflowID}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "Cancel workflow command failed: %s", string(output))

		assert.Contains(t, string(output), "Successfully cancelled", "Should confirm cancellation")

		// Verify workflow is actually cancelled
		getArgs := append([]string{"workflow", "get", workflowID}, baseArgs...)
		getCmd := exec.Command(cliPath, getArgs...)
		getCmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

		getOutput, err := getCmd.CombinedOutput()
		require.NoError(t, err, "Get workflow status failed: %s", string(getOutput))

		var status dbos.WorkflowStatus
		err = json.Unmarshal(getOutput, &status)
		require.NoError(t, err, "JSON output should be valid")
		assert.Equal(t, "CANCELLED", string(status.Status), fmt.Sprintf("Workflow %s should be cancelled", workflowID))
	})

	t.Run("ResumeWorkflow", func(t *testing.T) {
		args := append([]string{"workflow", "resume", workflowID}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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
func testForkWorkflow(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
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
		args := append([]string{"workflow", "fork", workflowID, "--forked-workflow-id", newID, "--application-version", targetVersion}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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
		args := append([]string{"workflow", "fork", workflowID, "--step", "2"}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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
		args := append([]string{"workflow", "fork", workflowID, "--step", "-1"}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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
func testGetWorkflowSteps(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
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
		args := append([]string{"workflow", "steps", workflowID}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

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
func testErrorHandling(t *testing.T, cliPath string, baseArgs []string, dbRole string) {
	t.Run("InvalidWorkflowID", func(t *testing.T) {
		invalidID := "invalid-workflow-id-12345"

		// Test get with invalid ID
		args := append([]string{"workflow", "get", invalidID}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid workflow ID")
		assert.Contains(t, string(output), "workflow not found", "Should contain error message")
	})

	t.Run("MissingWorkflowID", func(t *testing.T) {
		// Test get without workflow ID
		args := append([]string{"workflow", "get"}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail without workflow ID")
		assert.Contains(t, string(output), "accepts 1 arg(s), received 0", "Should show usage error")
	})

	t.Run("InvalidStatusFilter", func(t *testing.T) {
		args := append([]string{"workflow", "list", "--status", "INVALID_STATUS"}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid status")
		assert.Contains(t, string(output), "invalid status", "Should contain status error message")
	})

	t.Run("InvalidTimeFormat", func(t *testing.T) {
		args := append([]string{"workflow", "list", "--start-time", "invalid-time-format"}, baseArgs...)
		cmd := exec.Command(cliPath, args...)
		cmd.Env = append(os.Environ(), "DBOS_SYSTEM_DATABASE_URL="+getDatabaseURL(dbRole))

		output, err := cmd.CombinedOutput()
		assert.Error(t, err, "Should fail with invalid time format")
		assert.Contains(t, string(output), "invalid start-time format", "Should contain time format error")
	})

	t.Run("MissingDatabaseURL", func(t *testing.T) {
		// Test without system DB url in the flags or env var
		args := append([]string{"workflow", "list"}, baseArgs...)
		cmd := exec.Command(cliPath, args...)

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

	// Delete any existing binary before building
	os.Remove(cliPath)

	// Build the CLI from the cmd directory
	buildCmd := exec.Command("go", "build", "-o", "dbos-cli-test", ".")
	buildCmd.Dir = cmdDir
	buildOutput, buildErr := buildCmd.CombinedOutput()
	require.NoError(t, buildErr, "Failed to build CLI: %s", string(buildOutput))

	// Return absolute path
	absPath, err := filepath.Abs(cliPath)
	require.NoError(t, err, "Failed to get absolute path")
	return absPath
}

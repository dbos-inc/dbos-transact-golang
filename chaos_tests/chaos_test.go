package chaos_test

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCLIPath string

// Event struct provides a simple synchronization primitive that can be used to signal between goroutines.
type Event struct {
	mu    sync.Mutex
	cond  *sync.Cond
	IsSet bool
}

func NewEvent() *Event {
	e := &Event{}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *Event) Wait() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for !e.IsSet {
		e.cond.Wait()
	}
}

func (e *Event) Set() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = true
	e.cond.Broadcast()
}

func isCockroachDB(ctx context.Context, conn *pgx.Conn) bool {
	var version string
	err := conn.QueryRow(ctx, "SHOW CLUSTER SETTING version").Scan(&version)
	return err == nil
}

// dropDatabaseIfExists drops a database in a way that works with both PostgreSQL and CockroachDB.
// For CockroachDB, it terminates active connections first, then drops the database.
// For PostgreSQL, it uses the WITH (FORCE) syntax.
func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	crdb := isCockroachDB(ctx, conn)

	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()

	var err error
	if crdb {
		// In CockroachDB, we can't force drop, so we terminate connections manually
		// Try to terminate connections to the target database
		terminateQuery := `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid != pg_backend_pid()`
		_, _ = conn.Exec(ctx, terminateQuery, dbName) // Ignore errors, proceed anyway

		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitizedDBName)
		_, err = conn.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop database %s: %w", dbName, err)
		}
	} else {
		// For PostgreSQL, use WITH (FORCE) to drop even with active connections
		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
		_, err = conn.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop database %s: %w", dbName, err)
		}
	}

	return nil
}

func (e *Event) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = false
}

// TestMain builds the CLI once for all tests
func TestMain(m *testing.M) {
	// Get the directory where this test file is located
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintf(os.Stderr, "Failed to get current file path\n")
		os.Exit(1)
	}

	// Navigate to the project root then to cmd/dbos
	testDir := filepath.Dir(filename)
	projectRoot := filepath.Dir(testDir) // Go up from integration/ to project root
	cmdDir := filepath.Join(projectRoot, "cmd", "dbos")

	// Build output path in the integration directory (where test is)
	cliPath := filepath.Join(testDir, "dbos-cli-test")

	// Delete any existing binary before building
	os.Remove(cliPath)

	// Build the CLI from the cmd/dbos directory
	buildCmd := exec.Command("go", "build", "-o", cliPath, ".")
	buildCmd.Dir = cmdDir
	buildOutput, buildErr := buildCmd.CombinedOutput()
	if buildErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to build CLI: %s\n", string(buildOutput))
		os.Exit(1)
	}

	// Set the global CLI path
	absPath, err := filepath.Abs(cliPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get absolute path: %v\n", err)
		os.Exit(1)
	}
	testCLIPath = absPath

	// Start postgres
	startPostgresCmd := exec.Command(cliPath, "postgres", "start")
	startOutput, startErr := startPostgresCmd.CombinedOutput()
	if startErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to start postgres: %s\n", string(startOutput))
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Clean up CLI binary
	os.Remove(cliPath)

	os.Exit(code)
}

// Use the DBOS CLI to start postgres
func startPostgres(t *testing.T, cliPath string) {
	cmd := exec.Command(cliPath, "postgres", "start")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to start postgres: %s", string(output))
}

// Use the DBOS CLI to stop postgres
func stopPostgres(t *testing.T, cliPath string) {
	cmd := exec.Command(cliPath, "postgres", "stop")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "Failed to stop postgres: %s", string(output))
}

// PostgresChaosMonkey starts a goroutine that randomly stops and starts PostgreSQL
func PostgresChaosMonkey(t *testing.T, ctx context.Context, wg *sync.WaitGroup) {
	cliPath := testCLIPath

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer t.Logf("Chaos Monkey: Exiting")

		for {
			// Check for context cancellation first
			select {
			case <-ctx.Done():
				require.Eventually(t, func() bool {
					startPostgres(t, cliPath)
					return true
				}, 5*time.Second, 100*time.Millisecond)
				return
			default:
			}

			// Random down time between 0 and 2 seconds
			downTime := time.Duration(rand.Float64()*2) * time.Second

			// Stop PostgreSQL
			require.Eventually(t, func() bool {
				stopPostgres(t, cliPath)
				return true
			}, 5*time.Second, 100*time.Millisecond)
			t.Logf("🐒 Chaos Monkey: Stopped PostgreSQL")

			// Sleep for random down time
			select {
			case <-time.After(downTime):
				// Start PostgreSQL again
				require.Eventually(t, func() bool {
					startPostgres(t, cliPath)
					return true
				}, 5*time.Second, 100*time.Millisecond)
				t.Logf("🐒 Chaos Monkey: Starting PostgreSQL")
			case <-ctx.Done():
				// Ensure PostgreSQL is started before exiting
				require.Eventually(t, func() bool {
					startPostgres(t, cliPath)
					return true
				}, 5*time.Second, 100*time.Millisecond)
				return
			}

			// Wait a bit before next chaos event (between 5 and 40 seconds)
			upTime := time.Duration(5+rand.Float64()*35) * time.Second
			select {
			case <-time.After(upTime):
				// Continue to next iteration
			case <-ctx.Done():
				t.Logf("Chaos Monkey: Context cancelled during uptime")
				return
			}
		}
	}()
}

// setupDBOS sets up a DBOS context for integration testing
func setupDBOS(t *testing.T) dbos.DBOSContext {
	t.Helper()

	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}

	// Clean up the test database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	require.NoError(t, err)

	dbName := parsedURL.Database
	postgresURL := parsedURL.Copy()
	postgresURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresURL)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	err = dropDatabaseIfExists(context.Background(), conn, dbName)
	require.NoError(t, err)

	dbosCtx, err := dbos.NewDBOSContext(context.Background(), dbos.Config{
		DatabaseURL: databaseURL,
		AppName:     "chaos-test",
		Logger:      slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)

	// Register cleanup to run after test completes
	t.Cleanup(func() {
		if dbosCtx != nil {
			dbos.Shutdown(dbosCtx, 30*time.Second)
		}
	})

	return dbosCtx
}

// Test workflow with multiple steps and transactions
func TestChaosWorkflow(t *testing.T) {
	dbosCtx := setupDBOS(t)

	// Start chaos monkey
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	// Define scheduled workflow that runs every second
	scheduledWorkflow := func(ctx dbos.DBOSContext, scheduledTime time.Time) (struct{}, error) {
		return struct{}{}, nil
	}

	// Define step functions
	stepOne := func(_ context.Context, x int) (int, error) {
		return x + 1, nil
	}

	stepTwo := func(_ context.Context, x int) (int, error) {
		return x + 2, nil
	}

	// Define workflow function
	workflow := func(ctx dbos.DBOSContext, x int) (int, error) {
		// Execute step one
		x, err := dbos.RunAsStep(ctx, func(context context.Context) (int, error) {
			return stepOne(context, x)
		})
		if err != nil {
			return 0, fmt.Errorf("step one failed: %w", err)
		}

		// Execute step two
		x, err = dbos.RunAsStep(ctx, func(context context.Context) (int, error) {
			return stepTwo(context, x)
		})
		if err != nil {
			return 0, fmt.Errorf("step two failed: %w", err)
		}

		return x, nil
	}

	// Register the workflows
	dbos.RegisterWorkflow(dbosCtx, workflow)
	// Register scheduled workflow to run every second for chaos testing
	dbos.RegisterWorkflow(dbosCtx, scheduledWorkflow, dbos.WithSchedule("* * * * * *"), dbos.WithWorkflowName("ScheduledChaosTest"))

	err := dbos.Launch(dbosCtx)
	require.NoError(t, err)

	// Run multiple workflows
	numWorkflows := 10000
	for i := range numWorkflows {
		if i%100 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}
		handle, err := dbos.RunWorkflow(dbosCtx, workflow, i)
		require.NoError(t, err, "failed to start workflow %d", i)

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)
		assert.Equal(t, i+3, result, "unexpected result for workflow %d", i)
	}

	// Validate scheduled workflow executions using ListWorkflows
	scheduledWorkflows, err := dbos.ListWorkflows(dbosCtx,
		dbos.WithName("ScheduledChaosTest"),
		dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess}),
		dbos.WithSortDesc(),
		dbos.WithLimit(1),
		dbos.WithLoadInput(false),
		dbos.WithLoadOutput(false),
	)
	require.NoError(t, err, "failed to list scheduled workflows")

	assert.Equal(t, len(scheduledWorkflows), 1, "Expected exactly one scheduled workflow execution")

	// Check the last execution was within 10 seconds -- reasonable for a 1 second schedule and 2 seconds postgres downtime
	latestWorkflow := scheduledWorkflows[0] // Sorted descending
	timeSinceLastExecution := time.Since(latestWorkflow.CreatedAt)
	assert.Less(t, timeSinceLastExecution, 10*time.Second,
		"Last scheduled execution was %v ago, expected less than 60 seconds", timeSinceLastExecution)
}

// Test send/recv functionality
func TestChaosRecv(t *testing.T) {
	dbosCtx := setupDBOS(t)

	// Start chaos monkey
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	topic := "test_topic"

	// Pre-allocate signals for all workflows, indexed by workflow index
	numWorkflows := 10000
	signals := make([]*Event, numWorkflows)
	for i := range numWorkflows {
		signals[i] = NewEvent()
	}

	// Define recv workflow - takes index as parameter
	recvWorkflow := func(ctx dbos.DBOSContext, index int) (string, error) {
		// Signal that we've started
		signals[index].Set()

		// Receive from topic with timeout
		value, err := dbos.Recv[string](ctx, topic, 10*time.Minute)
		if err != nil {
			return "", fmt.Errorf("failed to receive: %w", err)
		}
		return value, nil
	}

	// Register the workflow
	dbos.RegisterWorkflow(dbosCtx, recvWorkflow)

	err := dbos.Launch(dbosCtx)
	require.NoError(t, err)

	// Run multiple workflows with send/recv
	for i := range numWorkflows {
		if i%100 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}
		handle, err := dbos.RunWorkflow(dbosCtx, recvWorkflow, i)
		require.NoError(t, err, "failed to start workflow %d", i)

		// Wait for the workflow to actually start before calling Recv
		signals[i].Wait()

		// Generate a random value
		value := uuid.NewString()

		// Send the value to the workflow
		workflowID := handle.GetWorkflowID()
		err = dbos.Send(dbosCtx, workflowID, value, topic)
		require.NoError(t, err, "failed to send value for workflow %d", i)

		// Get the result and verify it matches
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)
		assert.Equal(t, value, result, "unexpected result for workflow %d", i)
	}
}

// Test event functionality
func TestChaosEvents(t *testing.T) {
	dbosCtx := setupDBOS(t)

	// Start chaos monkey
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	key := "test_key"

	// Define event workflow
	eventWorkflow := func(ctx dbos.DBOSContext, _ string) (string, error) {
		value := uuid.NewString()
		err := dbos.SetEvent(ctx, key, value)
		if err != nil {
			return "", fmt.Errorf("failed to set event: %w", err)
		}
		return value, nil
	}

	// Register the workflow
	dbos.RegisterWorkflow(dbosCtx, eventWorkflow)

	err := dbos.Launch(dbosCtx)
	require.NoError(t, err)

	// Run multiple workflows with events
	numWorkflows := 5000
	for i := range numWorkflows {
		if i%100 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}
		wfID := uuid.NewString()

		// Start workflow with specific ID
		handle, err := dbos.RunWorkflow(dbosCtx, eventWorkflow, "", dbos.WithWorkflowID(wfID))
		require.NoError(t, err, "failed to start workflow %d", i)

		// Get the workflow result
		value, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)

		// Retrieve the event and verify it matches
		retrievedValue, err := dbos.GetEvent[string](dbosCtx, wfID, key, 10*time.Minute)
		require.NoError(t, err, "failed to get event for workflow %d", i)
		assert.Equal(t, value, retrievedValue, "unexpected event value for workflow %d", i)
	}
}

// Test queue functionality
func TestChaosQueues(t *testing.T) {
	dbosCtx := setupDBOS(t)

	// Start chaos monkey
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	PostgresChaosMonkey(t, ctx, &wg)

	queue := dbos.NewWorkflowQueue(dbosCtx, "test_queue")

	// Define step functions
	stepOne := func(ctx dbos.DBOSContext, x int) (int, error) {
		// Run as a step
		result, err := dbos.RunAsStep(ctx, func(context context.Context) (int, error) {
			return x + 1, nil
		})
		if err != nil {
			return 0, fmt.Errorf("step one failed: %w", err)
		}
		return result, nil
	}

	stepTwo := func(ctx dbos.DBOSContext, x int) (int, error) {
		// Run as a step
		result, err := dbos.RunAsStep(ctx, func(context context.Context) (int, error) {
			return x + 2, nil
		})
		if err != nil {
			return 0, fmt.Errorf("step two failed: %w", err)
		}
		return result, nil
	}

	// Define main workflow that enqueues other workflows
	workflow := func(ctx dbos.DBOSContext, x int) (int, error) {
		// Enqueue step one
		handle1, err := dbos.RunWorkflow(ctx, stepOne, x, dbos.WithQueue(queue.Name))
		if err != nil {
			return 0, fmt.Errorf("failed to enqueue step one: %w", err)
		}
		x, err = handle1.GetResult()
		if err != nil {
			return 0, fmt.Errorf("failed to get result from step one: %w", err)
		}

		// Enqueue step two
		handle2, err := dbos.RunWorkflow(ctx, stepTwo, x, dbos.WithQueue(queue.Name))
		if err != nil {
			return 0, fmt.Errorf("failed to enqueue step two: %w", err)
		}
		x, err = handle2.GetResult()
		if err != nil {
			return 0, fmt.Errorf("failed to get result from step two: %w", err)
		}
		return x, nil
	}

	// Register all workflows
	dbos.RegisterWorkflow(dbosCtx, stepOne)
	dbos.RegisterWorkflow(dbosCtx, stepTwo)
	dbos.RegisterWorkflow(dbosCtx, workflow)

	err := dbos.Launch(dbosCtx)
	require.NoError(t, err)

	// Run multiple workflows
	numWorkflows := 30
	for i := range numWorkflows {
		if i%10 == 0 {
			t.Logf("Starting workflow %d/%d", i+1, numWorkflows)
		}
		// Enqueue the main workflow
		handle, err := dbos.RunWorkflow(dbosCtx, workflow, i, dbos.WithQueue(queue.Name))
		require.NoError(t, err, "failed to enqueue workflow %d", i)

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result for workflow %d", i)
		assert.Equal(t, i+3, result, "unexpected result for workflow %d", i)
	}
}

package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestConfig(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
	)
	databaseURL := getDatabaseURL()

	t.Run("CreatesDBOSContext", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-app-id")
		t.Setenv("DBOS__VMID", "test-executor-id")
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-initialize",
		})
		require.NoError(t, err)
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}() // Clean up executor

		require.NotNil(t, ctx)

		// Test that executor implements DBOSContext interface
		var _ DBOSContext = ctx

		// Test that we can call methods on the executor
		appVersion := ctx.GetApplicationVersion()
		assert.Equal(t, "v1.0.0", appVersion)
		executorID := ctx.GetExecutorID()
		assert.Equal(t, "test-executor-id", executorID)
		appID := ctx.GetApplicationID()
		assert.Equal(t, "test-app-id", appID)
	})

	t.Run("FailsWithoutAppName", func(t *testing.T) {
		config := Config{
			DatabaseURL: databaseURL,
		}

		_, err := NewDBOSContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("FailsWithoutDatabaseURLOrSystemDBPool", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewDBOSContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: either databaseURL or systemDBPool must be provided"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("ConfigApplicationVersionAndExecutorID", func(t *testing.T) {
		t.Run("UsesConfigValues", func(t *testing.T) {
			// Clear env vars to ensure we're testing config values
			t.Setenv("DBOS__APPVERSION", "")
			t.Setenv("DBOS__VMID", "")

			ctx, err := NewDBOSContext(context.Background(), Config{
				DatabaseURL:        databaseURL,
				AppName:            "test-config-values",
				ApplicationVersion: "config-v1.2.3",
				ExecutorID:         "config-executor-123",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			assert.Equal(t, "config-v1.2.3", ctx.GetApplicationVersion())
			assert.Equal(t, "config-executor-123", ctx.GetExecutorID())
		})

		t.Run("EnvVarsOverrideConfigValues", func(t *testing.T) {
			t.Setenv("DBOS__APPVERSION", "env-v2.0.0")
			t.Setenv("DBOS__VMID", "env-executor-456")

			ctx, err := NewDBOSContext(context.Background(), Config{
				DatabaseURL:        databaseURL,
				AppName:            "test-env-override",
				ApplicationVersion: "config-v1.2.3",
				ExecutorID:         "config-executor-123",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			// Env vars should override config values
			assert.Equal(t, "env-v2.0.0", ctx.GetApplicationVersion())
			assert.Equal(t, "env-executor-456", ctx.GetExecutorID())
		})

		t.Run("UsesDefaultsWhenEmpty", func(t *testing.T) {
			// Clear env vars and don't set config values
			t.Setenv("DBOS__APPVERSION", "")
			t.Setenv("DBOS__VMID", "")

			ctx, err := NewDBOSContext(context.Background(), Config{
				DatabaseURL: databaseURL,
				AppName:     "test-defaults",
				// ApplicationVersion and ExecutorID left empty
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			// Should use computed application version (hash) and "local" executor ID
			appVersion := ctx.GetApplicationVersion()
			assert.NotEmpty(t, appVersion, "ApplicationVersion should not be empty")
			assert.NotEqual(t, "", appVersion, "ApplicationVersion should have a default value")

			executorID := ctx.GetExecutorID()
			assert.Equal(t, "local", executorID)
		})

		t.Run("EnvVarsOverrideEmptyConfig", func(t *testing.T) {
			t.Setenv("DBOS__APPVERSION", "env-only-v3.0.0")
			t.Setenv("DBOS__VMID", "env-only-executor")

			ctx, err := NewDBOSContext(context.Background(), Config{
				DatabaseURL: databaseURL,
				AppName:     "test-env-only",
				// ApplicationVersion and ExecutorID left empty
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			// Should use env vars even when config is empty
			assert.Equal(t, "env-only-v3.0.0", ctx.GetApplicationVersion())
			assert.Equal(t, "env-only-executor", ctx.GetExecutorID())
		})
	})

	t.Run("SystemDBMigration", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-migration")
		t.Setenv("DBOS__VMID", "test-executor-id")

		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-migration",
		})
		require.NoError(t, err)
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		require.NotNil(t, ctx)

		// Get the internal systemDB instance to check tables directly
		dbosCtx, ok := ctx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtx.systemDB)

		sysDB, ok := dbosCtx.systemDB.(*sysDB)
		require.True(t, ok, "expected sysDB")

		// Verify all expected tables exist and have correct structure
		dbCtx := context.Background()

		// Test workflow_status table
		var exists bool
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_status table should exist")

		// Test operation_outputs table
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "operation_outputs table should exist")

		// Test workflow_events table
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_events')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_events table should exist")

		// Test notifications table
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'notifications')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "notifications table should exist")

		// Test that all tables can be queried (empty results expected)
		rows, err := sysDB.pool.Query(dbCtx, "SELECT workflow_uuid FROM dbos.workflow_status LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.pool.Query(dbCtx, "SELECT workflow_uuid FROM dbos.operation_outputs LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.pool.Query(dbCtx, "SELECT workflow_uuid FROM dbos.workflow_events LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.pool.Query(dbCtx, "SELECT destination_uuid FROM dbos.notifications LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		// Check that the dbos_migrations table exists and has one row with the correct version
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'dbos_migrations')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "dbos_migrations table should exist")

		// Verify migration version is 7 (after initial migration, queue partition key migration, workflow status index migration, forked_from migration, step timestamps migration, workflow events history migration, and owner_xid migration)
		var version int64
		var count int
		err = sysDB.pool.QueryRow(dbCtx, "SELECT COUNT(*) FROM dbos.dbos_migrations").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = sysDB.pool.QueryRow(dbCtx, "SELECT version FROM dbos.dbos_migrations").Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(7), version, "migration version should be 7 (after initial migration, queue partition key migration, workflow status index migration, forked_from migration, step timestamps migration, workflow events history migration, and owner_xid migration)")

		// Test manual shutdown and recreate
		Shutdown(ctx, 1*time.Minute)

		// Recreate context - should have no error since DB is already migrated
		ctx2, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-migration-recreate",
		})
		require.NoError(t, err)
		defer func() {
			if ctx2 != nil {
				Shutdown(ctx2, 1*time.Minute)
			}
		}()

		require.NotNil(t, ctx2)
	})

	t.Run("KeyValueFormatConnectionString", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-keyvalue-format")
		t.Setenv("DBOS__VMID", "test-executor-id")

		// Get base connection parameters
		originalURL := databaseURL
		parsedURL, err := pgxpool.ParseConfig(originalURL)
		require.NoError(t, err)

		user := parsedURL.ConnConfig.User
		database := parsedURL.ConnConfig.Database
		host := parsedURL.ConnConfig.Host
		port := parsedURL.ConnConfig.Port

		// Use a unique test password that won't match other connection parameters
		testPassword := "TEST_PASSWORD_UNIQUE_12345!@#$%"

		// Test password masking with various spacing formats
		maskingTestCases := []struct {
			name    string
			connStr string
		}{
			{"NoSpaces", fmt.Sprintf("user=%s password=%s database=%s host=%s", user, testPassword, database, host)},
			{"SpaceBeforeEquals", fmt.Sprintf("user=%s password =%s database=%s host=%s", user, testPassword, database, host)},
			{"SpaceAfterEquals", fmt.Sprintf("user=%s password= %s database=%s host=%s", user, testPassword, database, host)},
			{"SpacesBothSides", fmt.Sprintf("user=%s password = %s database=%s host=%s", user, testPassword, database, host)},
			{"UppercaseKey", fmt.Sprintf("user=%s PASSWORD=%s database=%s host=%s", user, testPassword, database, host)},
			{"MixedCaseKey", fmt.Sprintf("user=%s Password=%s database=%s host=%s", user, testPassword, database, host)},
		}

		// Add port and sslmode if needed
		portSSL := ""
		if port != 0 {
			portSSL += fmt.Sprintf(" port=%d", port)
		}
		if strings.Contains(originalURL, "sslmode=disable") {
			portSSL += " sslmode=disable"
		}
		for i := range maskingTestCases {
			maskingTestCases[i].connStr += portSSL
		}

		for _, tc := range maskingTestCases {
			t.Run("Masking_"+tc.name, func(t *testing.T) {
				masked, err := maskPassword(tc.connStr)
				require.NoError(t, err)
				assert.Contains(t, masked, "***", "password should be masked")
				passwordPattern := fmt.Sprintf("password=%s", testPassword)
				assert.NotContains(t, strings.ToLower(masked), strings.ToLower(passwordPattern), "password should not appear in plaintext")
			})
		}

		// Integration test: verify DBOS context works with key-value format
		t.Run("DBOSContextCreation", func(t *testing.T) {
			// Use the actual password from config for integration test
			actualPassword := parsedURL.ConnConfig.Password
			var keyValueConnStr string
			if actualPassword == "" {
				keyValueConnStr = fmt.Sprintf("user='%s' database=%s host=%s%s", user, database, host, portSSL)
			} else {
				keyValueConnStr = fmt.Sprintf("user='%s' password='%s' database=%s host=%s%s", user, actualPassword, database, host, portSSL)
			}

			ctx, err := NewDBOSContext(context.Background(), Config{
				DatabaseURL: keyValueConnStr,
				AppName:     "test-keyvalue-format",
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			require.NotNil(t, ctx)

			// Verify system DB is functional
			dbosCtx, ok := ctx.(*dbosContext)
			require.True(t, ok)
			sysDB, ok := dbosCtx.systemDB.(*sysDB)
			require.True(t, ok)

			var exists bool
			err = sysDB.pool.QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')").Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists)

			// Verify masking works
			poolConnStr := sysDB.pool.Config().ConnString()
			maskedConnStr, err := maskPassword(poolConnStr)
			require.NoError(t, err)
			if actualPassword == "" {
				assert.NotContains(t, maskedConnStr, "password=")
			} else {
				assert.Contains(t, maskedConnStr, "password=***")
				assert.NotContains(t, maskedConnStr, fmt.Sprintf("password=%s", actualPassword))

			}
		})
	})

}

func TestContext(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("PreservesContextValues", func(t *testing.T) {
		// Define test keys and values
		type contextKey string
		key1 := contextKey("test-key-1")
		key2 := contextKey("test-key-2")
		value1 := "test-value-1"
		value2 := 42

		// Create a context with seeded values
		baseCtx := context.Background()
		ctxWithValues := context.WithValue(baseCtx, key1, value1)
		ctxWithValues = context.WithValue(ctxWithValues, key2, value2)

		// Create DBOSContext with the seeded context
		dbosCtx, err := NewDBOSContext(ctxWithValues, Config{
			DatabaseURL: databaseURL,
			AppName:     "test-context-values",
		})
		require.NoError(t, err)
		defer func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 1*time.Minute)
			}
		}()

		require.NotNil(t, dbosCtx)

		// Verify that the context values are preserved in DBOSContext
		assert.Equal(t, value1, dbosCtx.Value(key1), "DBOSContext should preserve context value for key1")
		assert.Equal(t, value2, dbosCtx.Value(key2), "DBOSContext should preserve context value for key2")

		// Verify that non-existent keys return nil
		nonExistentKey := contextKey("non-existent-key")
		assert.Nil(t, dbosCtx.Value(nonExistentKey), "DBOSContext should return nil for non-existent keys")
	})

	t.Run("FromPreservesDerivedContextValues", func(t *testing.T) {
		type contextKey string
		key1 := contextKey("from-test-key-1")
		key2 := contextKey("from-test-key-2")
		key3 := contextKey("from-test-key-3")
		value1 := "old-value-1"
		value2 := 100
		value3 := "new-value-3"

		// Build a context chain: base has key1, key2; derived adds key3
		baseCtx := context.Background()
		baseCtx = context.WithValue(baseCtx, key1, value1)
		baseCtx = context.WithValue(baseCtx, key2, value2)
		derivedCtx := context.WithValue(baseCtx, key3, value3)

		// Create DBOSContext with the base context
		dbosCtx, err := NewDBOSContext(baseCtx, Config{
			DatabaseURL: databaseURL,
			AppName:     "test-context-from",
		})
		require.NoError(t, err)
		defer func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 1*time.Minute)
			}
		}()
		require.NotNil(t, dbosCtx)

		// From(dbosCtx, derivedCtx) returns a DBOS context that wraps the derived context
		fromCtx := From(dbosCtx, derivedCtx)
		require.NotNil(t, fromCtx)

		// Value must return all values: from the base (old) and from the derived (new)
		assert.Equal(t, value1, fromCtx.Value(key1), "From DBOS context should return value from ancestor context")
		assert.Equal(t, value2, fromCtx.Value(key2), "From DBOS context should return value from ancestor context")
		assert.Equal(t, value3, fromCtx.Value(key3), "From DBOS context should return value from derived context")
	})
}

func TestCustomSystemDBSchema(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
	)
	t.Setenv("DBOS__APPVERSION", "v1.0.0")
	t.Setenv("DBOS__APPID", "test-custom-schema")
	t.Setenv("DBOS__VMID", "test-executor-id")

	databaseURL := getDatabaseURL()
	customSchema := "dbos_custom_test"

	ctx, err := NewDBOSContext(context.Background(), Config{
		DatabaseURL:    databaseURL,
		AppName:        "test-custom-schema-migration",
		DatabaseSchema: customSchema,
	})
	require.NoError(t, err)
	defer func() {
		if ctx != nil {
			Shutdown(ctx, 1*time.Minute)
		}
	}()

	require.NotNil(t, ctx)

	t.Run("CustomSchemaSetup", func(t *testing.T) {
		// Get the internal systemDB instance to check tables directly
		dbosCtx, ok := ctx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtx.systemDB)

		sysDB, ok := dbosCtx.systemDB.(*sysDB)
		require.True(t, ok, "expected sysDB")

		// Verify schema name was set correctly
		assert.Equal(t, customSchema, sysDB.schema, "schema name should match custom schema")

		// Verify all expected tables exist in the custom schema
		dbCtx := context.Background()

		// Test workflow_status table in custom schema
		var exists bool
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_status')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_status table should exist in custom schema")

		// Test operation_outputs table in custom schema
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'operation_outputs')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "operation_outputs table should exist in custom schema")

		// Test workflow_events table in custom schema
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_events')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_events table should exist in custom schema")

		// Test notifications table in custom schema
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'notifications')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "notifications table should exist in custom schema")

		// Test that all tables can be queried using custom schema (empty results expected)
		rows, err := sysDB.pool.Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.workflow_status LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.pool.Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.operation_outputs LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.pool.Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.workflow_events LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.pool.Query(dbCtx, fmt.Sprintf("SELECT destination_uuid FROM %s.notifications LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		// Check that the dbos_migrations table exists in custom schema
		err = sysDB.pool.QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'dbos_migrations')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "dbos_migrations table should exist in custom schema")

		// Verify migration version is 7 (after initial migration, queue partition key migration, workflow status index migration, forked_from migration, step timestamps migration, workflow events history migration, and owner_xid migration)
		var version int64
		var count int
		err = sysDB.pool.QueryRow(dbCtx, fmt.Sprintf("SELECT COUNT(*) FROM %s.dbos_migrations", customSchema)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = sysDB.pool.QueryRow(dbCtx, fmt.Sprintf("SELECT version FROM %s.dbos_migrations", customSchema)).Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(7), version, "migration version should be 7 (after initial migration, queue partition key migration, workflow status index migration, forked_from migration, step timestamps migration, workflow events history migration, and owner_xid migration)")
	})

	// Test workflows for exercising Send/Recv and SetEvent/GetEvent
	type testWorkflowInput struct {
		PartnerWorkflowID string
		Message           string
	}

	// Event to signal when workflow B is ready to receive
	var workflowBReadyEvent *Event

	// Workflow A: Uses Send() and GetEvent() - waits for workflow B
	sendGetEventWorkflow := func(ctx DBOSContext, input testWorkflowInput) (string, error) {
		// Send a message to the partner workflow
		err := Send(ctx, input.PartnerWorkflowID, input.Message, "test-topic")
		if err != nil {
			return "", err
		}

		// Wait for an event from the partner workflow
		result, err := GetEvent[string](ctx, input.PartnerWorkflowID, "response-key", 5*time.Hour)
		if err != nil {
			return "", err
		}

		return result, nil
	}

	// Workflow B: Uses Recv() and SetEvent() - waits for workflow A
	recvSetEventWorkflow := func(ctx DBOSContext, input testWorkflowInput) (string, error) {
		// Signal that this workflow has started and is ready to receive
		if workflowBReadyEvent != nil {
			workflowBReadyEvent.Set()
		}

		// Receive a message from the partner workflow
		receivedMsg, err := Recv[string](ctx, "test-topic", 5*time.Hour)
		if err != nil {
			return "", err
		}

		// Set an event for the partner workflow
		err = SetEvent(ctx, "response-key", "response-from-workflow-b")
		if err != nil {
			return "", err
		}

		return receivedMsg, nil
	}

	t.Run("CustomSchemaUsage", func(t *testing.T) {
		// Initialize the event to signal when workflow B is ready to receive
		workflowBReadyEvent = NewEvent()

		// Register the test workflows
		RegisterWorkflow(ctx, sendGetEventWorkflow)
		RegisterWorkflow(ctx, recvSetEventWorkflow)

		// Launch the DBOS context
		Launch(ctx)

		// Test RunWorkflow - start both workflows that will communicate with each other
		workflowAID := uuid.NewString()
		workflowBID := uuid.NewString()

		// Start workflow B first (receiver)
		handleB, err := RunWorkflow(ctx, recvSetEventWorkflow, testWorkflowInput{
			PartnerWorkflowID: workflowAID,
			Message:           "test-message-from-b",
		}, WithWorkflowID(workflowBID))
		require.NoError(t, err, "failed to start recvSetEventWorkflow")

		// Wait for workflow B to be ready to receive
		workflowBReadyEvent.Wait()

		// Start workflow A (sender)
		handleA, err := RunWorkflow(ctx, sendGetEventWorkflow, testWorkflowInput{
			PartnerWorkflowID: workflowBID,
			Message:           "test-message-from-a",
		}, WithWorkflowID(workflowAID))
		require.NoError(t, err, "failed to start sendGetEventWorkflow")

		// Wait for both workflows to complete
		resultA, err := handleA.GetResult()
		require.NoError(t, err, "failed to get result from workflow A")
		assert.Equal(t, "response-from-workflow-b", resultA, "workflow A should receive response from workflow B")

		resultB, err := handleB.GetResult()
		require.NoError(t, err, "failed to get result from workflow B")
		assert.Equal(t, "test-message-from-a", resultB, "workflow B should receive message from workflow A")

		// Test GetWorkflowSteps
		stepsA, err := GetWorkflowSteps(ctx, workflowAID)
		require.NoError(t, err, "failed to get workflow A steps")
		require.GreaterOrEqual(t, len(stepsA), 2, "workflow A should have at least 2 steps")
		require.LessOrEqual(t, len(stepsA), 3, "workflow A should have at most 3 steps")
		assert.Equal(t, "DBOS.send", stepsA[0].StepName, "first step should be Send")
		// Verify GetEvent step is present (required)
		foundGetEvent := false
		for i := 1; i < len(stepsA); i++ {
			if stepsA[i].StepName == "DBOS.getEvent" {
				foundGetEvent = true
				break
			}
		}
		assert.True(t, foundGetEvent, "workflow A should have GetEvent step")

		stepsB, err := GetWorkflowSteps(ctx, workflowBID)
		require.NoError(t, err, "failed to get workflow B steps")
		require.GreaterOrEqual(t, len(stepsB), 2, "workflow B should have at least 2 steps")
		require.LessOrEqual(t, len(stepsB), 3, "workflow B should have at most 3 steps")
		assert.Equal(t, "DBOS.recv", stepsB[0].StepName, "first step should be Recv")
		// Verify SetEvent step is present (required)
		foundSetEvent := false
		for i := 1; i < len(stepsB); i++ {
			if stepsB[i].StepName == "DBOS.setEvent" {
				foundSetEvent = true
				break
			}
		}
		assert.True(t, foundSetEvent, "workflow B should have SetEvent step")
	})
}

func TestCustomPool(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
	)
	// Test workflows for custom pool testing
	type customPoolWorkflowInput struct {
		PartnerWorkflowID string
		Message           string
	}

	// Workflow A: Uses Send() and GetEvent() - waits for workflow B
	sendGetEventWorkflowCustom := func(ctx DBOSContext, input customPoolWorkflowInput) (string, error) {
		// Send a message to the partner workflow
		err := Send(ctx, input.PartnerWorkflowID, input.Message, "custom-pool-topic")
		if err != nil {
			return "", err
		}

		// Wait for an event from the partner workflow
		result, err := GetEvent[string](ctx, input.PartnerWorkflowID, "custom-response-key", 5*time.Hour)
		if err != nil {
			return "", err
		}

		return result, nil
	}

	// Workflow B: Uses Recv() and SetEvent() - waits for workflow A
	recvSetEventWorkflowCustom := func(ctx DBOSContext, input customPoolWorkflowInput) (string, error) {
		// Receive a message from the partner workflow
		receivedMsg, err := Recv[string](ctx, "custom-pool-topic", 5*time.Hour)
		if err != nil {
			return "", err
		}

		time.Sleep(1 * time.Second)

		// Set an event for the partner workflow
		err = SetEvent(ctx, "custom-response-key", "response-from-custom-pool-workflow")
		if err != nil {
			return "", err
		}

		return receivedMsg, nil
	}

	t.Run("CustomPool", func(t *testing.T) {
		// Custom Pool
		databaseURL := getDatabaseURL()
		poolConfig, err := pgxpool.ParseConfig(databaseURL)
		require.NoError(t, err)

		poolConfig.MaxConns = 10
		poolConfig.MinConns = 5
		poolConfig.MaxConnLifetime = 2 * time.Hour
		poolConfig.MaxConnIdleTime = time.Minute * 2

		poolConfig.ConnConfig.ConnectTimeout = 10 * time.Second

		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)

		config := Config{
			AppName:      "test-custom-pool",
			SystemDBPool: pool,
		}

		customdbosContext, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, customdbosContext)

		dbosCtx, ok := customdbosContext.(*dbosContext)
		defer Shutdown(dbosCtx, 10*time.Second)
		require.True(t, ok)

		sysDB, ok := dbosCtx.systemDB.(*sysDB)
		require.True(t, ok)
		assert.Same(t, pool, sysDB.pool, "The pool in dbosContext should be the same as the custom pool provided")

		stats := sysDB.pool.Stat()
		assert.Equal(t, int32(10), stats.MaxConns(), "MaxConns should match custom pool config")

		sysdbConfig := sysDB.pool.Config()
		assert.Equal(t, int32(10), sysdbConfig.MaxConns)
		assert.Equal(t, int32(5), sysdbConfig.MinConns)
		assert.Equal(t, 2*time.Hour, sysdbConfig.MaxConnLifetime)
		assert.Equal(t, 2*time.Minute, sysdbConfig.MaxConnIdleTime)
		assert.Equal(t, 10*time.Second, sysdbConfig.ConnConfig.ConnectTimeout)

		// Register the test workflows
		RegisterWorkflow(customdbosContext, sendGetEventWorkflowCustom)
		RegisterWorkflow(customdbosContext, recvSetEventWorkflowCustom)

		// Launch the DBOS context
		err = Launch(customdbosContext)
		require.NoError(t, err)
		defer Shutdown(dbosCtx, 1*time.Minute)

		// Test RunWorkflow - start both workflows that will communicate with each other
		workflowAID := uuid.NewString()
		workflowBID := uuid.NewString()

		// Start workflow B first (receiver)
		handleB, err := RunWorkflow(customdbosContext, recvSetEventWorkflowCustom, customPoolWorkflowInput{
			PartnerWorkflowID: workflowAID,
			Message:           "custom-pool-message-from-b",
		}, WithWorkflowID(workflowBID))
		require.NoError(t, err, "failed to start recvSetEventWorkflowCustom")

		// Small delay to ensure workflow B is ready to receive
		time.Sleep(100 * time.Millisecond)

		// Start workflow A (sender)
		handleA, err := RunWorkflow(customdbosContext, sendGetEventWorkflowCustom, customPoolWorkflowInput{
			PartnerWorkflowID: workflowBID,
			Message:           "custom-pool-message-from-a",
		}, WithWorkflowID(workflowAID))
		require.NoError(t, err, "failed to start sendGetEventWorkflowCustom")

		// Wait for both workflows to complete
		resultA, err := handleA.GetResult()
		require.NoError(t, err, "failed to get result from workflow A")
		assert.Equal(t, "response-from-custom-pool-workflow", resultA, "workflow A should receive response from workflow B")

		resultB, err := handleB.GetResult()
		require.NoError(t, err, "failed to get result from workflow B")
		assert.Equal(t, "custom-pool-message-from-a", resultB, "workflow B should receive message from workflow A")

		// Test GetWorkflowSteps
		stepsA, err := GetWorkflowSteps(customdbosContext, workflowAID)
		require.NoError(t, err, "failed to get workflow A steps")
		require.Len(t, stepsA, 3, "workflow A should have 3 steps (Send + GetEvent + Sleep)")
		assert.Equal(t, "DBOS.send", stepsA[0].StepName, "first step should be Send")
		assert.Equal(t, "DBOS.getEvent", stepsA[1].StepName, "second step should be GetEvent")
		assert.Equal(t, "DBOS.sleep", stepsA[2].StepName, "third step should be Sleep")

		stepsB, err := GetWorkflowSteps(customdbosContext, workflowBID)
		require.NoError(t, err, "failed to get workflow B steps")
		require.Len(t, stepsB, 3, "workflow B should have 3 steps (Recv + Sleep + SetEvent)")
		assert.Equal(t, "DBOS.recv", stepsB[0].StepName, "first step should be Recv")
		assert.Equal(t, "DBOS.sleep", stepsB[1].StepName, "second step should be Sleep")
		assert.Equal(t, "DBOS.setEvent", stepsB[2].StepName, "third step should be SetEvent")
	})

	wf := func(ctx DBOSContext, input string) (string, error) {
		return input, nil
	}

	t.Run("CustomPoolTakesPrecedence", func(t *testing.T) {
		invalidDatabaseURL := "postgres://invalid:invalid@localhost:5432/invaliddb"
		databaseURL := getDatabaseURL()
		poolConfig, err := pgxpool.ParseConfig(databaseURL)
		require.NoError(t, err)
		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)

		config := Config{
			DatabaseURL:  invalidDatabaseURL,
			AppName:      "test-invalid-db-url",
			SystemDBPool: pool,
		}
		dbosCtx, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err)

		RegisterWorkflow(dbosCtx, wf)

		// Launch the DBOS context
		err = Launch(dbosCtx)
		require.NoError(t, err)
		defer Shutdown(dbosCtx, 1*time.Minute)

		// Run a workflow
		_, err = RunWorkflow(dbosCtx, wf, "test-input")
		require.NoError(t, err)
	})

	t.Run("InvalidCustomPool", func(t *testing.T) {
		databaseURL := getDatabaseURL()
		poolConfig, err := pgxpool.ParseConfig(databaseURL)
		require.NoError(t, err)
		poolConfig.ConnConfig.Host = "invalid-host"
		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)

		config := Config{
			DatabaseURL:  databaseURL,
			AppName:      "test-invalid-custom-pool",
			SystemDBPool: pool,
		}
		_, err = NewDBOSContext(context.Background(), config)
		require.Error(t, err)
		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)
		assert.Equal(t, InitializationError, dbosErr.Code)
		expectedMsg := "Error initializing DBOS Transact: failed to validate custom pool"
		assert.Contains(t, dbosErr.Message, expectedMsg)
	})

	t.Run("DirectSystemDatabase", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		databaseURL := getDatabaseURL()
		logger := slog.Default()

		// Create custom pool
		poolConfig, err := pgxpool.ParseConfig(databaseURL)
		require.NoError(t, err)
		poolConfig.MaxConns = 15
		poolConfig.MinConns = 3
		customPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
		require.NoError(t, err)
		defer customPool.Close()

		// Create system database with custom pool
		sysDBInput := newSystemDatabaseInput{
			databaseURL:    databaseURL,
			databaseSchema: "dbos_test_custom_direct",
			customPool:     customPool,
			logger:         logger,
		}

		systemDB, err := newSystemDatabase(ctx, sysDBInput)
		require.NoError(t, err, "failed to create system database with custom pool")
		require.NotNil(t, systemDB)

		// Launch the system database
		systemDB.launch(ctx)

		require.Eventually(t, func() bool {
			conn, err := systemDB.(*sysDB).pool.Acquire(ctx)
			require.NoError(t, err)
			defer conn.Release()
			err = conn.Ping(ctx)
			require.NoError(t, err)
			return true
		}, 5*time.Second, 100*time.Millisecond, "system database should be reachable")

		// Shutdown the system database
		cancel() // Cancel context
		shutdownTimeout := 2 * time.Second
		systemDB.shutdown(ctx, shutdownTimeout)
		assert.False(t, systemDB.(*sysDB).launched)
	})
}

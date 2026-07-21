package dbos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

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
		// database/sql's connectionOpener/connectionCleaner exit just after
		// Close, sometimes after goleak's check fires under the sqlite backend.
		goleak.IgnoreAnyFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreAnyFunction("database/sql.(*DB).connectionCleaner"),
	)
	databaseURL := backendDatabaseURL(t)

	t.Run("CreatesContext", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-app-id")
		t.Setenv("DBOS__VMID", "test-executor-id")
		ctx, err := NewContext(context.Background(), Config{
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

		// Test that executor implements Context interface
		var _ Context = ctx

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

		_, err := NewContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected Error, got %T", err)

		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("FailsWithoutDatabaseURLOrSystemDBPool", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected Error, got %T", err)

		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: one of databaseURL, systemDBPool, or sqliteSystemDB must be provided"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("ConfigApplicationVersionAndExecutorID", func(t *testing.T) {
		t.Run("UsesConfigValues", func(t *testing.T) {
			// Clear env vars to ensure we're testing config values
			t.Setenv("DBOS__APPVERSION", "")
			t.Setenv("DBOS__VMID", "")

			ctx, err := NewContext(context.Background(), Config{
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

			ctx, err := NewContext(context.Background(), Config{
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

			ctx, err := NewContext(context.Background(), Config{
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

			ctx, err := NewContext(context.Background(), Config{
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

	t.Run("ConductorExecutorMetadata", func(t *testing.T) {
		t.Run("AcceptsJSONSerializable", func(t *testing.T) {
			ctx, err := NewContext(context.Background(), Config{
				DatabaseURL: databaseURL,
				AppName:     "test-conductor-metadata-valid",
				ConductorExecutorMetadata: map[string]any{
					"region":   "us-east-1",
					"instance": 42,
				},
			})
			require.NoError(t, err)
			defer func() {
				if ctx != nil {
					Shutdown(ctx, 1*time.Minute)
				}
			}()

			dbosCtx, ok := ctx.(*dbosContext)
			require.True(t, ok)
			assert.Equal(t, map[string]any{
				"region":   "us-east-1",
				"instance": 42,
			}, dbosCtx.config.ConductorExecutorMetadata)
		})

		t.Run("RejectsNonSerializable", func(t *testing.T) {
			_, err := NewContext(context.Background(), Config{
				DatabaseURL: databaseURL,
				AppName:     "test-conductor-metadata-invalid",
				ConductorExecutorMetadata: map[string]any{
					"bad": make(chan int),
				},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "conductorExecutorMetadata must be JSON-serializable")
		})
	})

	t.Run("SystemDBMigration", func(t *testing.T) {
		skipIfSqlite(t, "queries pg information_schema; TestSQLiteFoundation covers the sqlite migration path")
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-migration")
		t.Setenv("DBOS__VMID", "test-executor-id")

		ctx, err := NewContext(context.Background(), Config{
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

		sysDB, ok := dbosCtx.systemDB.(*sysdb.SysDB)
		require.True(t, ok, "expected sysDB")

		// Verify all expected tables exist and have correct structure
		dbCtx := context.Background()

		// Test workflow_status table
		var exists bool
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_status table should exist")

		// Test operation_outputs table
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'operation_outputs')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "operation_outputs table should exist")

		// Test workflow_events table
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_events')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_events table should exist")

		// Test notifications table
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'notifications')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "notifications table should exist")

		// Test that all tables can be queried (empty results expected)
		rows, err := sysDB.Pool().Query(dbCtx, "SELECT workflow_uuid FROM dbos.workflow_status LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.Pool().Query(dbCtx, "SELECT workflow_uuid FROM dbos.operation_outputs LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.Pool().Query(dbCtx, "SELECT workflow_uuid FROM dbos.workflow_events LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.Pool().Query(dbCtx, "SELECT destination_uuid FROM dbos.notifications LIMIT 1")
		require.NoError(t, err)
		rows.Close()

		// Check that the dbos_migrations table exists and has one row with the correct version
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'dbos_migrations')").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "dbos_migrations table should exist")

		// Verify migration version is 14 (after initial migration through pgsql_client_functions)
		var version int64
		var count int
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT COUNT(*) FROM dbos.dbos_migrations").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = sysDB.Pool().QueryRow(dbCtx, "SELECT version FROM dbos.dbos_migrations").Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(42), version, "migration version should be 42 (after all migrations including the debounce columns)")

		// Test manual shutdown and recreate
		Shutdown(ctx, 1*time.Minute)

		// Recreate context - should have no error since DB is already migrated
		ctx2, err := NewContext(context.Background(), Config{
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
		skipIfSqlite(t, "libpq key=value DSN form is pg-specific")
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
				masked, err := sysdb.MaskPassword(tc.connStr)
				require.NoError(t, err)
				assert.Contains(t, masked, "***", "password should be masked")
				passwordPattern := fmt.Sprintf("password=%s", testPassword)
				assert.NotContains(t, strings.ToLower(masked), strings.ToLower(passwordPattern), "password should not appear in plaintext")
			})
		}

		// Integration test: verify DBOS context works with key-value format
		t.Run("ContextCreation", func(t *testing.T) {
			// Use the actual password from config for integration test
			actualPassword := parsedURL.ConnConfig.Password
			var keyValueConnStr string
			if actualPassword == "" {
				keyValueConnStr = fmt.Sprintf("user='%s' database=%s host=%s%s", user, database, host, portSSL)
			} else {
				keyValueConnStr = fmt.Sprintf("user='%s' password='%s' database=%s host=%s%s", user, actualPassword, database, host, portSSL)
			}

			ctx, err := NewContext(context.Background(), Config{
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
			sysDB, ok := dbosCtx.systemDB.(*sysdb.SysDB)
			require.True(t, ok)

			var exists bool
			err = sysDB.Pool().QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dbos' AND table_name = 'workflow_status')").Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists)

			// Verify masking works
			poolConnStr := PgxPool(sysDB.Pool()).Config().ConnString()
			maskedConnStr, err := sysdb.MaskPassword(poolConnStr)
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

func TestSystemDBStartupTimeoutConfig(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		config, err := processConfig(&Config{AppName: "test", DatabaseURL: "sqlite::memory:"})
		require.NoError(t, err)
		assert.Equal(t, 2*time.Minute, config.SystemDBStartupTimeout)
	})

	t.Run("Explicit", func(t *testing.T) {
		config, err := processConfig(&Config{
			AppName:                "test",
			DatabaseURL:            "sqlite::memory:",
			SystemDBStartupTimeout: 17 * time.Second,
		})
		require.NoError(t, err)
		assert.Equal(t, 17*time.Second, config.SystemDBStartupTimeout)
	})

	t.Run("Negative", func(t *testing.T) {
		_, err := processConfig(&Config{
			AppName:                "test",
			DatabaseURL:            "sqlite::memory:",
			SystemDBStartupTimeout: -time.Second,
		})
		require.EqualError(t, err, "systemDBStartupTimeout cannot be negative")
	})
}

func TestSystemDBStartupTimeoutBoundsSQLitePoolWait(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	const timeout = 50 * time.Millisecond
	started := time.Now()
	_, err = NewContext(context.Background(), Config{
		AppName:                "startup-timeout-sqlite",
		SQLiteSystemDB:         db,
		SystemDBStartupTimeout: timeout,
	})
	elapsed := time.Since(started)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system database startup timed out after 50ms while initializing the SQLite system database")
	assert.Less(t, elapsed, time.Second)
}

func TestSystemDBStartupTimeoutDiagnosesExhaustedPostgresPool(t *testing.T) {
	skipIfSqlite(t, "PostgreSQL pool statistics")
	poolConfig, err := pgxpool.ParseConfig(backendDatabaseURL(t))
	require.NoError(t, err)
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	require.NoError(t, err)
	defer pool.Close()

	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()

	_, err = NewContext(context.Background(), Config{
		AppName:                "startup-timeout-exhausted-pool",
		SystemDBPool:           pool,
		SystemDBStartupTimeout: 50 * time.Millisecond,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "while acquiring a connection from the custom pool: connection pool has no free connections (acquired=1, max=1)")
}

type launchRecoveryFaultPool struct {
	sysdb.Pool
}

func (p *launchRecoveryFaultPool) Query(ctx context.Context, query string, args ...any) (sysdb.Rows, error) {
	if strings.Contains(query, "SELECT workflow_uuid, status, name") {
		return nil, errors.New("injected recovery failure")
	}
	return p.Pool.Query(ctx, query, args...)
}

func TestLaunchFailureCleansUpStartedComponents(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-launch-cleanup",
		DatabaseURL: "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
	})
	require.NoError(t, err)

	dbosCtx := ctx.(*dbosContext)
	systemDB := dbosCtx.systemDB.(*sysdb.SysDB)
	systemDB.SetPool(&launchRecoveryFaultPool{Pool: systemDB.Pool()})

	err = dbosCtx.Launch()
	require.ErrorContains(t, err, "failed to recover pending workflows during launch")
	require.ErrorContains(t, err, "injected recovery failure")

	assert.False(t, dbosCtx.launched.Load())
	assert.False(t, dbosCtx.queueRunnerStarted.Load())
	assert.False(t, dbosCtx.workflowSchedulerStarted.Load())
	assert.NotNil(t, dbosCtx.workflowScheduler)
	assert.ErrorIs(t, dbosCtx.Err(), context.Canceled)
	assert.False(t, systemDB.Launched())
	require.Error(t, systemDB.Pool().Ping(context.Background()))
}

func TestConcurrentLaunchOnlyStartsOnce(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-concurrent-launch",
		DatabaseURL: "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
	})
	require.NoError(t, err)

	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- Launch(ctx)
		}()
	}
	close(start)

	var successes int
	for range 2 {
		if err := <-results; err == nil {
			successes++
		} else {
			assert.ErrorContains(t, err, "DBOS is already launched")
		}
	}
	assert.Equal(t, 1, successes)
	Shutdown(ctx, 5*time.Second)
}

func TestRunWorkflowBeforeLaunchFails(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-run-before-launch",
		DatabaseURL: "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
	})
	require.NoError(t, err)
	defer Shutdown(ctx, 5*time.Second)

	wf := func(ctx Context, in string) (string, error) { return in, nil }
	RegisterWorkflow(ctx, wf)

	_, err = RunWorkflow(ctx, wf, "hello")
	require.Error(t, err)
	dbosErr := &Error{}
	require.ErrorAs(t, err, &dbosErr)
	assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
	assert.Contains(t, err.Error(), "DBOS must be launched before running workflows")

	require.NoError(t, Launch(ctx))

	handle, err := RunWorkflow(ctx, wf, "hello")
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestConcurrentShutdownDoesNotWaitTwice(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-concurrent-shutdown",
		DatabaseURL: "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
	})
	require.NoError(t, err)
	dbosCtx := ctx.(*dbosContext)
	// Simulate a queue runner that cannot complete so the first caller remains
	// in Shutdown long enough for the second caller to enter.
	dbosCtx.queueRunnerStarted.Store(true)

	const timeout = 200 * time.Millisecond
	start := make(chan struct{})
	durations := make(chan time.Duration, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			began := time.Now()
			Shutdown(ctx, timeout)
			durations <- time.Since(began)
		}()
	}
	ready.Wait()
	close(start)

	first := <-durations
	second := <-durations
	assert.Less(t, min(first, second), timeout/2)
}

type blockingScheduleListDB struct {
	sysdb.SystemDatabase
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingScheduleListDB) ListSchedules(context.Context, sysdb.ListSchedulesDBInput) ([]WorkflowSchedule, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return nil, nil
}

func TestShutdownJoinsScheduleReconciler(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{
		AppName:                  "test-reconciler-shutdown",
		DatabaseURL:              "sqlite:" + filepath.Join(t.TempDir(), "dbos.db"),
		SchedulerPollingInterval: time.Hour,
	})
	require.NoError(t, err)
	dbosCtx := ctx.(*dbosContext)
	blockingDB := &blockingScheduleListDB{
		SystemDatabase: dbosCtx.systemDB,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	dbosCtx.systemDB = blockingDB
	dbosCtx.workflowScheduler.Start()
	dbosCtx.workflowSchedulerStarted.Store(true)
	dbosCtx.scheduleReconcilerWg.Add(1)
	go func() {
		defer dbosCtx.scheduleReconcilerWg.Done()
		dbosCtx.runScheduleReconciler()
	}()
	<-blockingDB.entered

	shutdownDone := make(chan struct{})
	go func() {
		Shutdown(ctx, time.Second)
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before the schedule reconciler exited")
	case <-time.After(50 * time.Millisecond):
	}
	assert.True(t, dbosCtx.workflowSchedulerStarted.Load())

	close(blockingDB.release)
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not complete after the schedule reconciler exited")
	}
	assert.False(t, dbosCtx.workflowSchedulerStarted.Load())
}

func TestContext(t *testing.T) {
	databaseURL := backendDatabaseURL(t)

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

		// Create Context with the seeded context
		dbosCtx, err := NewContext(ctxWithValues, Config{
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

		// Verify that the context values are preserved in Context
		assert.Equal(t, value1, dbosCtx.Value(key1), "Context should preserve context value for key1")
		assert.Equal(t, value2, dbosCtx.Value(key2), "Context should preserve context value for key2")

		// Verify that non-existent keys return nil
		nonExistentKey := contextKey("non-existent-key")
		assert.Nil(t, dbosCtx.Value(nonExistentKey), "Context should return nil for non-existent keys")
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

		// Create Context with the base context
		dbosCtx, err := NewContext(baseCtx, Config{
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
	skipIfSqlite(t, "queries pg information_schema; sqlite has no per-schema isolation")
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

	ctx, err := NewContext(context.Background(), Config{
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

		sysDB, ok := dbosCtx.systemDB.(*sysdb.SysDB)
		require.True(t, ok, "expected sysDB")

		// Verify schema name was set correctly
		assert.Equal(t, customSchema, sysDB.Schema(), "schema name should match custom schema")

		// Verify all expected tables exist in the custom schema
		dbCtx := context.Background()

		// Test workflow_status table in custom schema
		var exists bool
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_status')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_status table should exist in custom schema")

		// Test operation_outputs table in custom schema
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'operation_outputs')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "operation_outputs table should exist in custom schema")

		// Test workflow_events table in custom schema
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'workflow_events')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "workflow_events table should exist in custom schema")

		// Test notifications table in custom schema
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'notifications')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "notifications table should exist in custom schema")

		// Test that all tables can be queried using custom schema (empty results expected)
		rows, err := sysDB.Pool().Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.workflow_status LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.Pool().Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.operation_outputs LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.Pool().Query(dbCtx, fmt.Sprintf("SELECT workflow_uuid FROM %s.workflow_events LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		rows, err = sysDB.Pool().Query(dbCtx, fmt.Sprintf("SELECT destination_uuid FROM %s.notifications LIMIT 1", customSchema))
		require.NoError(t, err)
		rows.Close()

		// Check that the dbos_migrations table exists in custom schema
		err = sysDB.Pool().QueryRow(dbCtx, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'dbos_migrations')", customSchema).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "dbos_migrations table should exist in custom schema")

		// Verify migration version is 14 (after initial migration through pgsql_client_functions)
		var version int64
		var count int
		err = sysDB.Pool().QueryRow(dbCtx, fmt.Sprintf("SELECT COUNT(*) FROM %s.dbos_migrations", customSchema)).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = sysDB.Pool().QueryRow(dbCtx, fmt.Sprintf("SELECT version FROM %s.dbos_migrations", customSchema)).Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(42), version, "migration version should be 42 (after all migrations including the debounce columns)")
	})

	// Test workflows for exercising Send/Recv and SetEvent/GetEvent
	type testWorkflowInput struct {
		PartnerWorkflowID string
		Message           string
	}

	// Event to signal when workflow B is ready to receive
	var workflowBReadyEvent *Event

	// Workflow A: Uses Send() and GetEvent() - waits for workflow B
	sendGetEventWorkflow := func(ctx Context, input testWorkflowInput) (string, error) {
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
	recvSetEventWorkflow := func(ctx Context, input testWorkflowInput) (string, error) {
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
	skipIfSqlite(t, "test *pgxpool.Pool only")
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
	sendGetEventWorkflowCustom := func(ctx Context, input customPoolWorkflowInput) (string, error) {
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
	recvSetEventWorkflowCustom := func(ctx Context, input customPoolWorkflowInput) (string, error) {
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

		customdbosContext, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, customdbosContext)

		dbosCtx, ok := customdbosContext.(*dbosContext)
		defer Shutdown(dbosCtx, 10*time.Second)
		require.True(t, ok)

		sysDB, ok := dbosCtx.systemDB.(*sysdb.SysDB)
		require.True(t, ok)
		assert.Same(t, pool, PgxPool(sysDB.Pool()), "The pool in dbosContext should be the same as the custom pool provided")

		stats := PgxPool(sysDB.Pool()).Stat()
		assert.Equal(t, int32(10), stats.MaxConns(), "MaxConns should match custom pool config")

		sysdbConfig := PgxPool(sysDB.Pool()).Config()
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

	wf := func(ctx Context, input string) (string, error) {
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
		dbosCtx, err := NewContext(context.Background(), config)
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
		_, err = NewContext(context.Background(), config)
		require.Error(t, err)
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected Error, got %T", err)
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
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
		sysDBInput := sysdb.NewSystemDatabaseInput{
			DatabaseURL:    databaseURL,
			DatabaseSchema: "dbos_test_custom_direct",
			CustomPool:     customPool,
			Logger:         logger,
		}

		systemDB, err := sysdb.NewSystemDatabase(ctx, sysDBInput)
		require.NoError(t, err, "failed to create system database with custom pool")
		require.NotNil(t, systemDB)

		// Launch the system database
		systemDB.Launch(ctx)

		require.Eventually(t, func() bool {
			conn, err := PgxPool(systemDB.(*sysdb.SysDB).Pool()).Acquire(ctx)
			require.NoError(t, err)
			defer conn.Release()
			err = conn.Ping(ctx)
			require.NoError(t, err)
			return true
		}, 5*time.Second, 100*time.Millisecond, "system database should be reachable")

		// Shutdown the system database
		cancel() // Cancel context
		shutdownTimeout := 2 * time.Second
		systemDB.Shutdown(ctx, shutdownTimeout)
		assert.False(t, systemDB.(*sysdb.SysDB).Launched())
	})
}

// TestSystemDBShutdownReportsPending verifies SysDB.Shutdown returns the
// sub-components that failed to stop within the timeout.
func TestSystemDBShutdownReportsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := filepath.Join(t.TempDir(), "dbos.db")
	customDB, err := sql.Open("sqlite", dbPath)
	require.NoError(t, err)
	defer customDB.Close()

	systemDB, err := sysdb.NewSystemDatabase(ctx, sysdb.NewSystemDatabaseInput{
		DatabaseSchema: "dbos",
		CustomSqliteDB: customDB,
		Logger:         logger,
	})
	require.NoError(t, err)
	systemDB.Launch(ctx)

	// ctx is deliberately not cancelled, so the notification loop cannot exit
	pending := systemDB.Shutdown(ctx, 100*time.Millisecond)
	require.Contains(t, pending, "notification listener")
	require.True(t, systemDB.(*sysdb.SysDB).Launched(), "a timed-out shutdown must leave the listener tracked so later calls re-check it")

	cancel()
	require.Eventually(t, func() bool {
		return len(systemDB.Shutdown(ctx, 100*time.Millisecond)) == 0
	}, 5*time.Second, 100*time.Millisecond, "notification loop should exit after cancel")
	require.False(t, systemDB.(*sysdb.SysDB).Launched())
}

// TestClientShutdownReportsSystemDBTimeout verifies Client.Shutdown returns an
// error when the system database fails to shut down within the timeout.
func TestClientShutdownReportsSystemDBTimeout(t *testing.T) {
	skipIfSqlite(t, "holds a pgx pool connection to block pool close")
	ctx := context.Background()

	poolConfig, err := pgxpool.ParseConfig(getDatabaseURL())
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	require.NoError(t, err)

	client, err := NewClient(ctx, ClientConfig{
		SystemDBPool:   pool,
		DatabaseSchema: "dbos_test_shutdown_client",
	})
	require.NoError(t, err)

	// A held connection blocks pool.Close() past the timeout
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)

	err = client.Shutdown(client, 500 * time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "system database connection pool")

	conn.Release()
	require.Eventually(t, func() bool {
		return pool.Stat().TotalConns() == 0
	}, 5*time.Second, 100*time.Millisecond, "pool should close after connection release")
}

// -----------------------------------------------------------------------------
// SQLite-specific suite. These tests construct sqlite handles directly so they
// run on every test invocation, regardless of DBOS_TEST_BACKEND. They cover
// the sqlite-only paths (DSN parsing, migration application, UDFs/PRAGMAs,
// dialect error classification) that the cross-backend tests above can't
// reach via the high-level API.
// -----------------------------------------------------------------------------

// TestSQLiteFoundation verifies the SQLite path of newSystemDatabase: URL
// dispatch, migration application, UDF availability, pragma settings.
func TestSQLiteFoundation(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dbos.db")
	url := "sqlite:" + dbPath
	logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	sd, err := sysdb.NewSystemDatabase(context.Background(), sysdb.NewSystemDatabaseInput{
		DatabaseURL:    url,
		DatabaseSchema: "dbos",
		Logger:         logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { sd.Shutdown(context.Background(), 0) })

	s, ok := sd.(*sysdb.SysDB)
	require.True(t, ok, "expected *sysDB concrete type")
	require.Nil(t, PgxPool(s.Pool()), "pg pool should be nil for sqlite")
	require.NotNil(t, SQLDB(s.Pool()), "sqlite handle should be non-nil")
	require.Equal(t, DialectSQLite, s.Dialect().Name())

	// Migrations table should be at the latest version.
	migs := sysdb.BuildSqliteMigrations()
	latest := migs[len(migs)-1].Version
	var got int64
	require.NoError(t, SQLDB(s.Pool()).QueryRow(`SELECT version FROM dbos_migrations`).Scan(&got))
	assert.Equal(t, latest, got)

	// Re-opening the same file is a no-op (migrations already applied).
	sd2, err := sysdb.NewSystemDatabase(context.Background(), sysdb.NewSystemDatabaseInput{
		DatabaseURL:    url,
		DatabaseSchema: "dbos",
		Logger:         logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { sd2.Shutdown(context.Background(), 0) })

	s2 := sd2.(*sysdb.SysDB)
	require.NoError(t, SQLDB(s2.Pool()).QueryRow(`SELECT version FROM dbos_migrations`).Scan(&got))
	assert.Equal(t, latest, got, "version should remain at latest on re-open")

	// PRAGMAs we set should stick.
	var jm string
	require.NoError(t, SQLDB(s2.Pool()).QueryRow(`PRAGMA journal_mode`).Scan(&jm))
	assert.Equal(t, "wal", jm, "WAL journal mode should be enabled")

	var fk int
	require.NoError(t, SQLDB(s2.Pool()).QueryRow(`PRAGMA foreign_keys`).Scan(&fk))
	assert.Equal(t, 1, fk, "foreign_keys pragma should be on")

	// Core schema should exist.
	for _, table := range []string{
		"workflow_status", "operation_outputs", "notifications",
		"workflow_events", "workflow_events_history", "streams",
		"event_dispatch_kv", "workflow_schedules", "application_versions", "queues",
	} {
		var name string
		err := SQLDB(s2.Pool()).QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
		assert.NoErrorf(t, err, "table %q missing after migrations", table)
	}

	// A BeginTx interrupted by context cancellation must surface an error
	// detectable as context.Canceled. modernc/sqlite substitutes ctx.Err()
	// for interrupted statements (stmt.exec) but NOT for the transaction
	// control path (tx.exec: begin/commit/rollback), which returns the raw
	// SQLite code (`interrupted (9)` or `database is locked (5)`). The dbos
	// layer must map it back so callers (e.g. handle.GetResult after
	// Shutdown) can errors.Is it.
	// Every dbos sqlite connection is opened with _txlock=immediate, so BEGIN
	// itself acquires the write lock: blocker holds it from BeginTx on, and
	// tx2's BEGIN IMMEDIATE blocks in the busy handler for the full
	// busy_timeout (5s) — the interrupt does not shorten the wait, only
	// poisons the result. The 100ms cancel therefore lands mid-BEGIN unless
	// the timer goroutine is starved for >5s; retry the scenario in that
	// pathological case rather than mis-assert.
	blocker, err := s.Pool().BeginTx(context.Background(), TxOptions{})
	require.NoError(t, err)
	cancelLanded := false
	for attempt := 0; attempt < 3 && !cancelLanded; attempt++ {
		cctx, cancelBegin := context.WithCancel(context.Background())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancelBegin()
		}()
		var tx2 Tx
		tx2, err = s.Pool().BeginTx(cctx, TxOptions{})
		cancelLanded = cctx.Err() != nil
		if tx2 != nil {
			_ = tx2.Rollback(context.Background())
		}
		cancelBegin()
	}
	_ = blocker.Rollback(context.Background())
	require.True(t, cancelLanded, "cancel never landed while BEGIN was in flight")
	require.Error(t, err, "BeginTx should fail when its context is cancelled mid-flight")
	assert.True(t, errors.Is(err, context.Canceled),
		"expected error to be detectable as context.Canceled, got: %v", err)
}

// TestSQLiteURLParsing checks the DSN extraction for common URL forms.
func TestSQLiteURLParsing(t *testing.T) {
	cases := []struct {
		url, want string
	}{
		// Hierarchical absolute paths: u.Path land
		{"sqlite:/tmp/x.db", "/tmp/x.db"},
		{"sqlite:///tmp/x.db", "/tmp/x.db"},
		// Opaque forms: u.Opaque land
		{"sqlite::memory:", ":memory:"},
		{"sqlite:relative.db", "relative.db"},
		// sqlite3: scheme is accepted too
		{"sqlite3:relative.db", "relative.db"},
		// Modernc URI-mode flags embedded inside a sqlite: URL; the inner
		// file: URI is preserved verbatim and the query string is re-appended.
		{"sqlite:file:/abs/x.db?_pragma=foreign_keys(1)", "file:/abs/x.db?_pragma=foreign_keys(1)"},
		// Fragment is preserved verbatim.
		{"sqlite:/tmp/x.db#vfs=unix", "/tmp/x.db#vfs=unix"},
		{"sqlite:relative.db#fragment", "relative.db#fragment"},
		// Fragment combines with a query string in the obvious order: path
		// then ?query then #fragment.
		{"sqlite:file:/abs/x.db?_pragma=foreign_keys(1)#tag", "file:/abs/x.db?_pragma=foreign_keys(1)#tag"},
	}
	for _, c := range cases {
		got, err := sysdb.SqliteDSN(c.url)
		require.NoErrorf(t, err, "sqliteDSN(%q)", c.url)
		assert.Equalf(t, c.want, got, "sqliteDSN(%q)", c.url)
	}

	bads := []struct {
		url    string
		errMsg string
	}{
		{"postgres://x", "not a sqlite URL"},
		{"sqlite:", "has no path"},
		{"sqlite://", "has no path"},
		{"sqlite://host/path", "host component"},
	}
	for _, b := range bads {
		_, err := sysdb.SqliteDSN(b.url)
		require.Errorf(t, err, "sqliteDSN(%q) should error", b.url)
		assert.Containsf(t, err.Error(), b.errMsg, "sqliteDSN(%q)", b.url)
	}
}

// TestDetectDialect covers scheme → dialect mapping including the libpq
// key=value DSN heuristic and error paths. Pure parsing, no DB connection.
func TestDetectDialect(t *testing.T) {
	cases := []struct {
		name   string
		url    string
		want   DialectName
		errMsg string // substring; empty means no error
	}{
		// sqlite forms
		{"sqlite-abs-single-slash", "sqlite:/tmp/x.db", DialectSQLite, ""},
		{"sqlite-abs-triple-slash", "sqlite:///tmp/x.db", DialectSQLite, ""},
		{"sqlite-memory", "sqlite::memory:", DialectSQLite, ""},
		{"sqlite3-scheme", "sqlite3:relative.db", DialectSQLite, ""},
		{"sqlite-uppercase-scheme", "SQLITE:/tmp/x.db", DialectSQLite, ""},
		// modernc URI-mode flags embedded inside a sqlite: URL — outer scheme
		// wins despite the inner file: token.
		{"sqlite-modernc-uri-mode", "sqlite:file:/abs/x.db?_pragma=foreign_keys(1)", DialectSQLite, ""},
		// sqlite URL with fragment.
		{"sqlite-with-fragment", "sqlite:/tmp/x.db#vfs=unix", DialectSQLite, ""},

		// postgres / postgresql URLs
		{"pg-url", "postgres://u:p@h:5432/d", DialectPostgres, ""},
		{"postgresql-url", "postgresql://u:p@h/d", DialectPostgres, ""},
		// CRDB Cloud–style URL with sslmode and options query params.
		{"crdb-cloud-style", "postgresql://u:p@h:26257/d?sslmode=verify-full&options=--cluster%3Dfoo-1234", DialectPostgres, ""},
		// IPv6 host literal.
		{"pg-ipv6-host", "postgres://[::1]:5432/dbos", DialectPostgres, ""},
		// Bare host, no path.
		{"pg-bare-host", "postgres://localhost", DialectPostgres, ""},
		// Unix-socket form (host carried in query string, empty authority).
		{"pg-unix-socket", "postgres:///dbos?host=/var/run/postgresql", DialectPostgres, ""},
		// Percent-encoded special char in password.
		{"pg-percent-encoded-pw", "postgres://u:p%40ss@h/d", DialectPostgres, ""},
		// postgresql: scheme with KV-looking userinfo — scheme wins over the
		// KV-DSN heuristic (which only applies when scheme is empty).
		{"postgresql-with-kv-shaped-userinfo", "postgresql://user=foo:5432/db", DialectPostgres, ""},
		// Empty body after scheme — still a recognised scheme.
		{"pg-empty-body", "postgres://", DialectPostgres, ""},

		// libpq key=value DSN — accepted as Postgres.
		{"kv-quoted-spaces", "host=localhost user='postgres' application_name='dbos worker'", DialectPostgres, ""},
		{"kv-user-first", "user='postgres' password='x' database=dbos host=localhost", DialectPostgres, ""},
		{"kv-host-first", "host=localhost port=5432 dbname=dbos", DialectPostgres, ""},
		// libpq KV DSN with characters that look like url.Parse escape sequences
		// in quoted values (#$%&!). url.Parse would reject "%&!"; we must detect
		// the KV form first.
		{"kv-quoted-funny-chars", "user='User Name-123@acme.com#$%&!' password='a!b@c$d()e*_,/:;=?@ff[]22' database=dbos host=localhost port=5432 sslmode=disable", DialectPostgres, ""},

		// Failure paths.
		{"empty", "", "", "empty"},
		{"unsupported-file-scheme", "file:/abs/x.db", "", "unsupported database scheme"},
		{"postgres-typo", "postgress://typo", "", "unsupported database scheme"},
		{"mysql", "mysql://h/d", "", "unsupported database scheme"},
		{"no-scheme-plain-string", "justastring", "", "no scheme"},
		// KV-shaped but doesn't start with a canonical pg keyword: must not be
		// mistakenly routed to pg.
		{"kv-with-non-canonical-prefix", "foo=bar host=localhost", "", "no scheme"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := sysdb.DetectDialect(c.url)
			if c.errMsg == "" {
				require.NoErrorf(t, err, "detectDialect(%q)", c.url)
				assert.Equalf(t, c.want, got, "detectDialect(%q)", c.url)
			} else {
				require.Errorf(t, err, "detectDialect(%q) should error", c.url)
				assert.Containsf(t, err.Error(), c.errMsg, "detectDialect(%q) error message", c.url)
			}
		})
	}
}

// TestPostgresConnectionStringForms derives DSN variants from the live
// DBOS_SYSTEM_DATABASE_URL (or default getDatabaseURL()) and confirms each
// variant survives parse → pool → Ping. Detection alone is unit-tested by
// TestDetectDialect; this guards the integration claim that we accept these
// DSN shapes against the actual pgx parser and a live server.
func TestPostgresConnectionStringForms(t *testing.T) {
	skipIfSqlite(t, "Postgres-only DSN forms")
	canonical := getDatabaseURL()

	// Extract connection params from the canonical URL via pgx, then rebuild
	// equivalent DSNs in alternative shapes. This avoids hard-coding host/port
	// and lets the test follow whatever env the rest of the suite uses.
	cfg, err := pgxpool.ParseConfig(canonical)
	require.NoError(t, err, "parse canonical URL")
	host := cfg.ConnConfig.Host
	port := cfg.ConnConfig.Port
	user := cfg.ConnConfig.User
	password := cfg.ConnConfig.Password
	dbname := cfg.ConnConfig.Database

	// postgresql:// equivalent of the canonical postgres:// URL.
	postgresqlScheme := strings.Replace(canonical, "postgres://", "postgresql://", 1)

	// libpq key=value forms — both unquoted and single-quoted variants. pgx
	// supports either. Password is quoted to tolerate special characters.
	kvUnquoted := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)
	kvQuoted := fmt.Sprintf(
		"host='%s' port=%d user='%s' password='%s' dbname='%s' sslmode=disable",
		host, port, user, password, dbname,
	)

	cases := []struct {
		name string
		url  string
	}{
		{"postgres-url-canonical", canonical},
		{"postgresql-scheme", postgresqlScheme},
		{"kv-dsn-unquoted", kvUnquoted},
		{"kv-dsn-quoted", kvQuoted},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := sysdb.DetectDialect(c.url)
			require.NoErrorf(t, err, "detectDialect(%q)", c.url)
			assert.Equal(t, DialectPostgres, got)

			pool, err := pgxpool.New(context.Background(), c.url)
			require.NoErrorf(t, err, "pgxpool.New(%q)", c.url)
			t.Cleanup(pool.Close)
			require.NoErrorf(t, pool.Ping(context.Background()), "Ping(%q)", c.url)
		})
	}
}

// TestSQLiteConnectionStringForms opens an actual database connection through
// each supported sqlite: URL form, verifying the parse → DSN → driver chain.
// Pure URL extraction is covered by TestSQLiteURLParsing; this confirms each
// form actually opens and round-trips data.
func TestSQLiteConnectionStringForms(t *testing.T) {
	dir := t.TempDir()
	abs := filepath.Join(dir, "dbos.db")
	// Separate path for the fragment case: modernc treats the DSN as a literal
	// file path when it doesn't start with "file:", so "/p/dbos.db#frag" would
	// create a file literally named "dbos.db#frag" — colliding with abs above
	// if reused.
	absFrag := filepath.Join(dir, "frag.db")

	cases := []struct {
		name string
		url  string
	}{
		{"sqlite-abs-single-slash", "sqlite:" + abs},
		{"sqlite-abs-triple-slash", "sqlite://" + abs},
		// Memory-backed DB: ":memory:" creates a private in-memory database
		// for the lifetime of the connection. Used by tests that want full
		// isolation without touching the filesystem.
		{"sqlite-memory", "sqlite::memory:"},
		{"sqlite3-scheme-abs", "sqlite3:" + abs},
		// modernc URI-mode flags inside a sqlite: URL. The inner file: URI is
		// passed verbatim to the driver, which supports the _pragma query arg.
		{"sqlite-modernc-uri-mode", "sqlite:file:" + abs + "?_pragma=foreign_keys(1)"},
		// Uppercase scheme: detectDialect lowercases before matching, and
		// sqliteDSN does the same, so the open path is identical to the
		// canonical sqlite: form.
		{"sqlite-uppercase-scheme", "SQLITE:" + abs},
		// Fragment is carried through sqliteDSN verbatim. modernc doesn't parse
		// fragments for non-file: DSNs, so it ends up as part of the filename.
		// This confirms the chain doesn't reject the URL outright.
		{"sqlite-with-fragment", "sqlite:" + absFrag + "#vfs=unix"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := sysdb.DetectDialect(c.url)
			require.NoErrorf(t, err, "detectDialect(%q)", c.url)
			assert.Equal(t, DialectSQLite, got)

			dsn, err := sysdb.SqliteDSN(c.url)
			require.NoErrorf(t, err, "sqliteDSN(%q)", c.url)

			db, err := sql.Open("sqlite", dsn)
			require.NoErrorf(t, err, "sql.Open(%q → %q)", c.url, dsn)
			t.Cleanup(func() { _ = db.Close() })
			require.NoErrorf(t, db.Ping(), "Ping(%q → %q)", c.url, dsn)

			// Round-trip a row: confirms the driver is fully functional, not
			// just that Ping reports OK. Each subtest uses its own table name
			// so file-backed runs don't see each other's writes.
			tbl := "rt_" + strings.ReplaceAll(c.name, "-", "_")
			_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s (k INTEGER PRIMARY KEY, v TEXT)`, tbl))
			require.NoError(t, err)
			_, err = db.Exec(fmt.Sprintf(`INSERT INTO %s (k, v) VALUES (1, 'hello')`, tbl))
			require.NoError(t, err)
			var v string
			require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT v FROM %s WHERE k = 1`, tbl)).Scan(&v))
			assert.Equal(t, "hello", v)
		})
	}

	// Relative path: modernc resolves it against the process CWD. t.Chdir
	// scopes the directory change to this subtest so the relative.db ends up
	// inside the tempdir (auto-cleaned at test end) and other tests are
	// unaffected.
	t.Run("sqlite3-relative-path", func(t *testing.T) {
		t.Chdir(t.TempDir())
		const url = "sqlite3:relative.db"

		got, err := sysdb.DetectDialect(url)
		require.NoError(t, err)
		assert.Equal(t, DialectSQLite, got)

		dsn, err := sysdb.SqliteDSN(url)
		require.NoError(t, err)
		assert.Equal(t, "relative.db", dsn)

		db, err := sql.Open("sqlite", dsn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		require.NoError(t, db.Ping())

		_, err = db.Exec(`CREATE TABLE rt_relative (k INTEGER PRIMARY KEY, v TEXT)`)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO rt_relative (k, v) VALUES (1, 'rel')`)
		require.NoError(t, err)
		var v string
		require.NoError(t, db.QueryRow(`SELECT v FROM rt_relative WHERE k = 1`).Scan(&v))
		assert.Equal(t, "rel", v)
	})
}

// TestSQLiteMemoryBackedFile confirms that a sqlite::memory: URL really gives
// us a memory-backed database (no file on disk) and that the data lives only
// for the lifetime of the connection.
func TestSQLiteMemoryBackedFile(t *testing.T) {
	const url = "sqlite::memory:"

	got, err := sysdb.DetectDialect(url)
	require.NoError(t, err)
	assert.Equal(t, DialectSQLite, got)

	dsn, err := sysdb.SqliteDSN(url)
	require.NoError(t, err)
	assert.Equal(t, ":memory:", dsn)

	db, err := sql.Open("sqlite", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())

	// Force a single connection so the in-memory DB persists across statements
	// in this test (each new conn to ":memory:" gets its own private DB).
	db.SetMaxOpenConns(1)

	_, err = db.Exec(`CREATE TABLE t (k INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO t (k, v) VALUES (1, 'memory-backed')`)
	require.NoError(t, err)

	var v string
	require.NoError(t, db.QueryRow(`SELECT v FROM t WHERE k = 1`).Scan(&v))
	assert.Equal(t, "memory-backed", v)

	// SQLite exposes the backing storage via PRAGMA database_list. For an
	// in-memory DB the file column is empty — that's the actual signal that
	// the data lives in memory, not on disk.
	rows, err := db.Query(`PRAGMA database_list`)
	require.NoError(t, err)
	defer rows.Close()
	var sawMain bool
	for rows.Next() {
		var seq int
		var name, file string
		require.NoError(t, rows.Scan(&seq, &name, &file))
		if name == "main" {
			sawMain = true
			assert.Empty(t, file, "main DB file path should be empty for :memory:")
		}
	}
	require.NoError(t, rows.Err())
	require.True(t, sawMain, "expected a 'main' entry in PRAGMA database_list")
}

// TestSQLiteDialectClassification sanity-checks the sqlite error helpers
// against real driver errors. Uses the live driver (not regex on strings).
func TestSQLiteDialectClassification(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	_, err = db.ExecContext(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT UNIQUE);
		CREATE TABLE child (parent_id INTEGER REFERENCES t(id)); PRAGMA foreign_keys = ON;`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO t VALUES (1, 'x')`)
	require.NoError(t, err)

	// Unique violation.
	_, err = db.ExecContext(ctx, `INSERT INTO t VALUES (2, 'x')`)
	require.Error(t, err)
	assert.True(t, sysdb.SqliteDialect{}.IsUniqueViolation(err), "expected unique-violation: %v", err)

	// Foreign key enforcement on :memory: is per-connection; the PRAGMA above
	// only sticks on the conn that executed it. Skip if not enforced here.
	_, err = db.ExecContext(ctx, `INSERT INTO child VALUES (999)`)
	if err == nil {
		t.Log("foreign_keys not enforced on this connection; skipping FK assertion")
	} else {
		assert.True(t, sysdb.SqliteDialect{}.IsForeignKeyViolation(err), "expected fk-violation: %v", err)
	}
}

// TestNewContextSQLiteRoundtrip exercises the user-facing Config path with
// a sqlite: URL. NewContext + Shutdown should succeed.
func TestNewContextSQLiteRoundtrip(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dbos.db")
	logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, err := NewContext(context.Background(), Config{
		AppName:     "test-sqlite",
		DatabaseURL: "sqlite:" + dbPath,
		Logger:      logger,
	})
	require.NoError(t, err, "NewContext with sqlite URL should succeed")
	ctx.Shutdown(ctx, 0)
}

// TestNewContextRejectsUnknownScheme verifies up-front URL validation
// fires for unsupported / mistyped schemes (backend-agnostic).
func TestNewContextRejectsUnknownScheme(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelError}))
	cases := []string{
		"mysql://h/d",
		"postgress://typo",
		"justastring",
	}
	for _, bad := range cases {
		_, err := NewContext(context.Background(), Config{
			AppName:     "test",
			DatabaseURL: bad,
			Logger:      logger,
		})
		assert.Errorf(t, err, "expected NewContext to reject %q", bad)
	}
}

// testWriter is a slog-compatible writer that forwards to t.Log.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(b []byte) (int, error) {
	w.t.Log(string(b))
	return len(b), nil
}

// TestCustomSqlitePool mirrors TestCustomPool for the SQLiteSystemDB config
// field: caller-supplied *sql.DB instead of *pgxpool.Pool. Always runs (does
// not depend on DBOS_TEST_BACKEND) because every subtest constructs its own
// sqlite handle.
func TestCustomSqlitePool(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreAnyFunction("database/sql.(*DB).connectionOpener"),
		goleak.IgnoreAnyFunction("database/sql.(*DB).connectionCleaner"),
		// MutuallyExclusivePools spins up a pgxpool just to exercise validation;
		// its background goroutines may outlive pool.Close().
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
	)

	type customPoolWorkflowInput struct {
		PartnerWorkflowID string
		Message           string
	}

	sendGetEventWorkflowCustom := func(ctx Context, input customPoolWorkflowInput) (string, error) {
		if err := Send(ctx, input.PartnerWorkflowID, input.Message, "sqlite-custom-pool-topic"); err != nil {
			return "", err
		}
		return GetEvent[string](ctx, input.PartnerWorkflowID, "sqlite-custom-response-key", 5*time.Hour)
	}
	recvSetEventWorkflowCustom := func(ctx Context, input customPoolWorkflowInput) (string, error) {
		msg, err := Recv[string](ctx, "sqlite-custom-pool-topic", 5*time.Hour)
		if err != nil {
			return "", err
		}
		time.Sleep(100 * time.Millisecond)
		if err := SetEvent(ctx, "sqlite-custom-response-key", "response-from-sqlite-custom-pool"); err != nil {
			return "", err
		}
		return msg, nil
	}

	t.Run("CustomSqliteDB", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "dbos.db")
		db, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(4)
		db.SetConnMaxLifetime(time.Hour)

		config := Config{
			AppName:        "test-custom-sqlite-db",
			SQLiteSystemDB: db,
		}
		customdbosContext, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, customdbosContext)

		dbosCtx, ok := customdbosContext.(*dbosContext)
		require.True(t, ok)
		defer Shutdown(dbosCtx, 10*time.Second)

		sysDB, ok := dbosCtx.systemDB.(*sysdb.SysDB)
		require.True(t, ok)
		assert.Same(t, db, SQLDB(sysDB.Pool()), "sysDB should use the caller's *sql.DB instance")
		require.Equal(t, DialectSQLite, sysDB.Dialect().Name())

		RegisterWorkflow(customdbosContext, sendGetEventWorkflowCustom)
		RegisterWorkflow(customdbosContext, recvSetEventWorkflowCustom)
		require.NoError(t, Launch(customdbosContext))

		workflowAID := uuid.NewString()
		workflowBID := uuid.NewString()

		handleB, err := RunWorkflow(customdbosContext, recvSetEventWorkflowCustom, customPoolWorkflowInput{
			PartnerWorkflowID: workflowAID,
			Message:           "sqlite-custom-pool-message-from-b",
		}, WithWorkflowID(workflowBID))
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		handleA, err := RunWorkflow(customdbosContext, sendGetEventWorkflowCustom, customPoolWorkflowInput{
			PartnerWorkflowID: workflowBID,
			Message:           "sqlite-custom-pool-message-from-a",
		}, WithWorkflowID(workflowAID))
		require.NoError(t, err)

		resultA, err := handleA.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "response-from-sqlite-custom-pool", resultA)

		resultB, err := handleB.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "sqlite-custom-pool-message-from-a", resultB)
	})

	t.Run("CustomSqliteDBTakesPrecedence", func(t *testing.T) {
		// An invalid DatabaseURL is ignored when SQLiteSystemDB is set.
		dbPath := filepath.Join(t.TempDir(), "dbos.db")
		db, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)

		config := Config{
			DatabaseURL:    "postgres://invalid:invalid@localhost:5432/invaliddb",
			AppName:        "test-sqlite-pool-precedence",
			SQLiteSystemDB: db,
		}
		dbosCtx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, dbosCtx)
		defer Shutdown(dbosCtx, 5*time.Second)

		sysDB, ok := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
		require.True(t, ok)
		assert.Equal(t, DialectSQLite, sysDB.Dialect().Name(), "sqlite custom DB should win over postgres URL")
	})

	t.Run("MutuallyExclusivePools", func(t *testing.T) {
		// Setting both SystemDBPool and SQLiteSystemDB must be rejected by
		// processConfig before any connection attempt.
		dbPath := filepath.Join(t.TempDir(), "dbos.db")
		db, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)
		defer db.Close()

		// pgxpool.NewWithConfig with MinConns=0 does not dial up-front; this
		// gives us a non-nil *pgxpool.Pool without requiring a live pg server.
		poolConfig, err := pgxpool.ParseConfig("postgres://localhost:5432/dbos?sslmode=disable")
		require.NoError(t, err)
		poolConfig.MinConns = 0
		pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
		require.NoError(t, err)
		defer pool.Close()

		_, err = NewContext(context.Background(), Config{
			AppName:        "test-mutually-exclusive",
			SystemDBPool:   pool,
			SQLiteSystemDB: db,
		})
		require.Error(t, err)
		dbosErr, ok := err.(*Error)
		require.True(t, ok, "expected Error, got %T", err)
		assert.Contains(t, dbosErr.Message, "mutually exclusive")
	})

	t.Run("DirectSqliteSystemDatabase", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		logger := slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelInfo}))

		dbPath := filepath.Join(t.TempDir(), "dbos.db")
		customDB, err := sql.Open("sqlite", dbPath)
		require.NoError(t, err)
		defer customDB.Close()

		systemDB, err := sysdb.NewSystemDatabase(ctx, sysdb.NewSystemDatabaseInput{
			DatabaseSchema: "dbos",
			CustomSqliteDB: customDB,
			Logger:         logger,
		})
		require.NoError(t, err)
		require.NotNil(t, systemDB)

		systemDB.Launch(ctx)

		require.Eventually(t, func() bool {
			return SQLDB(systemDB.(*sysdb.SysDB).Pool()).PingContext(ctx) == nil
		}, 5*time.Second, 100*time.Millisecond, "system database should be reachable")

		cancel()
		systemDB.Shutdown(ctx, 2*time.Second)
		assert.False(t, systemDB.(*sysdb.SysDB).Launched())
	})
}

package dbos

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
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
				ctx.Shutdown(1 * time.Minute)
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

	t.Run("NewSystemDatabaseWithCustomPool", func(t *testing.T) {

		// Logger
		var buf bytes.Buffer
		slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		slogLogger = slogLogger.With("service", "dbos-test", "environment", "test")

		// Custom Pool
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
			DatabaseURL:  databaseURL,
			AppName:      "test-custom-pool",
			Logger:       slogLogger,
			SystemDBPool: pool,
		}

		customdbosContext, err := NewDBOSContext(context.Background(), config)
		require.NoError(t, err)
		require.NotNil(t, customdbosContext)

		dbosCtx, ok := customdbosContext.(*dbosContext)
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
	})

	t.Run("FailsWithoutDatabaseURL", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewDBOSContext(context.Background(), config)
		require.Error(t, err)

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: databaseURL"
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
					ctx.Shutdown(1 * time.Minute)
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
					ctx.Shutdown(1 * time.Minute)
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
					ctx.Shutdown(1 * time.Minute)
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
					ctx.Shutdown(1 * time.Minute)
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
				ctx.Shutdown(1 * time.Minute)
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

		// Verify migration version is 1 (after initial migration)
		var version int64
		var count int
		err = sysDB.pool.QueryRow(dbCtx, "SELECT COUNT(*) FROM dbos.dbos_migrations").Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "dbos_migrations table should have exactly one row")

		err = sysDB.pool.QueryRow(dbCtx, "SELECT version FROM dbos.dbos_migrations").Scan(&version)
		require.NoError(t, err)
		assert.Equal(t, int64(1), version, "migration version should be 1 (after initial migration)")

		// Test manual shutdown and recreate
		ctx.Shutdown(1 * time.Minute)

		// Recreate context - should have no error since DB is already migrated
		ctx2, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-migration-recreate",
		})
		require.NoError(t, err)
		defer func() {
			if ctx2 != nil {
				ctx2.Shutdown(1 * time.Minute)
			}
		}()

		require.NotNil(t, ctx2)
	})
}

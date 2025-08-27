package dbos

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	databaseURL := getDatabaseURL()

	t.Run("CreatesDBOSContext", func(t *testing.T) {
		t.Setenv("DBOS__APPVERSION", "v1.0.0")
		t.Setenv("DBOS__APPID", "test-app-id")
		t.Setenv("DBOS__VMID", "test-executor-id")
		ctx, err := NewDBOSContext(Config{
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

		_, err := NewDBOSContext(config)
		require.Error(t, err)

		dbosErr, ok := err.(*DBOSError)
		require.True(t, ok, "expected DBOSError, got %T", err)

		assert.Equal(t, InitializationError, dbosErr.Code)

		expectedMsg := "Error initializing DBOS Transact: missing required config field: appName"
		assert.Equal(t, expectedMsg, dbosErr.Message)
	})

	t.Run("FailsWithoutDatabaseURL", func(t *testing.T) {
		config := Config{
			AppName: "test-app",
		}

		_, err := NewDBOSContext(config)
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

			ctx, err := NewDBOSContext(Config{
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

			ctx, err := NewDBOSContext(Config{
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

			ctx, err := NewDBOSContext(Config{
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

			ctx, err := NewDBOSContext(Config{
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
}

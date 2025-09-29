package dbos

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestLogger(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
		goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
	)
	databaseURL := getDatabaseURL()

	t.Run("Default logger", func(t *testing.T) {
		dbosCtx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
		}) // Create executor with default logger
		require.NoError(t, err)
		err = Launch(dbosCtx)
		require.NoError(t, err)
		t.Cleanup(func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 10*time.Second)
			}
		})

		ctx, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "Expected dbosCtx to be of type *dbosContext")
		require.NotNil(t, ctx.logger)

		// Test logger access
		ctx.logger.Info("Test message from default logger")

	})

	t.Run("Custom logger", func(t *testing.T) {
		// Test with custom slog logger
		var buf bytes.Buffer
		slogLogger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		// Add some context to the slog logger
		slogLogger = slogLogger.With("service", "dbos-test", "environment", "test")

		dbosCtx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			Logger:      slogLogger,
		})
		require.NoError(t, err)
		err = Launch(dbosCtx)
		require.NoError(t, err)
		t.Cleanup(func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 10*time.Second)
			}
		})

		ctx := dbosCtx.(*dbosContext)
		require.NotNil(t, ctx.logger)

		// Test that we can use the logger and it maintains context
		ctx.logger.Info("Test message from custom logger", "test_key", "test_value")

		// Check that our custom logger was used and captured the output
		logOutput := buf.String()
		assert.Contains(t, logOutput, "service=dbos-test", "Expected log output to contain service=dbos-test")
		assert.Contains(t, logOutput, "environment=test", "Expected log output to contain environment=test")
		assert.Contains(t, logOutput, "test_key=test_value", "Expected log output to contain test_key=test_value")
	})
}

package dbos

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// fakeConductor stands in for the enterprise client.
type fakeConductor struct {
	launched  atomic.Int32
	shutdowns atomic.Int32
	timeout   time.Duration
}

func (f *fakeConductor) Launch() { f.launched.Add(1) }
func (f *fakeConductor) Shutdown(timeout time.Duration) error {
	f.shutdowns.Add(1)
	f.timeout = timeout
	return nil
}

// fakeConductorFactory records what core passes and returns conn.
func fakeConductorFactory(conn ConductorConnection) ConductorFactory {
	return func(ctx Context, opts ConductorOptions) (ConductorConnection, error) {
		return conn, nil
	}
}

func sqliteURL(t *testing.T) string {
	t.Helper()
	return "sqlite:" + filepath.Join(t.TempDir(), "dbos.db")
}

func TestEnterprise(t *testing.T) {
	t.Cleanup(func() { RegisterConductorFactory(nil) })

	t.Run("AbsentModuleGetsTheImportHint", func(t *testing.T) {
		RegisterConductorFactory(nil)
		_, err := NewContext(context.Background(), Config{
			AppName:         "test-app",
			DatabaseURL:     sqliteURL(t),
			ConductorAPIKey: "test-key",
		})
		require.Error(t, err)
		var dbosErr *Error
		require.True(t, errors.As(err, &dbosErr), "expected *Error, got %T", err)
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
		assert.Contains(t, dbosErr.Message, `import _ "go.dbos.dev/dbos-enterprise-go"`)
		assert.Contains(t, dbosErr.Message, "go get go.dbos.dev/dbos-enterprise-go")
	})

	t.Run("RefusedConstructionLeavesNothingBehind", func(t *testing.T) {
		RegisterConductorFactory(nil)
		databaseURL := sqliteURL(t)
		_, err := NewContext(context.Background(), Config{
			AppName:         "test-app",
			DatabaseURL:     databaseURL,
			ConductorAPIKey: "test-key",
		})
		require.Error(t, err)

		// A fresh context on the same database launches without Conductor.
		ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: databaseURL})
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		require.NoError(t, ctx.Launch())
		assert.Equal(t, "local", ctx.GetExecutorID())
	})

	t.Run("CloudAlwaysRequiresTheModule", func(t *testing.T) {
		RegisterConductorFactory(nil)
		t.Setenv("DBOS__CLOUD", "true")
		t.Setenv("DBOS__CONDUCTOR_APP_NAME", "cloud-app")
		t.Setenv("DBOS__CONDUCTOR_KEY", "cloud-key")
		t.Setenv("DBOS__CONDUCTOR_URL", "wss://conductor.invalid/v1alpha1")
		// No key in code: DBOS Cloud connects to Conductor regardless.
		_, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: sqliteURL(t)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "go.dbos.dev/dbos-enterprise-go")
	})

	t.Run("FactoryReceivesResolvedOptions", func(t *testing.T) {
		var got ConductorOptions
		var gotCtx Context
		conn := &fakeConductor{}
		RegisterConductorFactory(func(ctx Context, opts ConductorOptions) (ConductorConnection, error) {
			gotCtx, got = ctx, opts
			return conn, nil
		})
		ctx, err := NewContext(context.Background(), Config{
			AppName:                   "test-app",
			DatabaseURL:               sqliteURL(t),
			ConductorAPIKey:           "test-key",
			ConductorExecutorMetadata: map[string]any{"region": "us-east-1"},
			ConductorMetadataOnlyMode: true,
		})
		require.NoError(t, err)

		assert.Same(t, ctx, gotCtx, "the factory gets the root context")
		assert.Equal(t, ConductorOptions{
			AppName:          "test-app",
			URL:              "wss://cloud.dbos.dev/conductor/v1alpha1",
			APIKey:           "test-key",
			ExecutorMetadata: map[string]any{"region": "us-east-1"},
			MetadataOnlyMode: true,
		}, got)
		assert.NotEqual(t, "local", ctx.GetExecutorID(), "a Conductor-connected executor gets a unique ID")

		// Not launched yet, so not connected yet.
		assert.EqualValues(t, 0, conn.launched.Load())
		require.NoError(t, ctx.Launch())
		assert.EqualValues(t, 1, conn.launched.Load())
		require.NoError(t, Shutdown(ctx, 7*time.Second))
		assert.EqualValues(t, 1, conn.shutdowns.Load())
		assert.Equal(t, 7*time.Second, conn.timeout)
	})

	t.Run("ExplicitURLAndCloudEnvironmentPassThrough", func(t *testing.T) {
		var got ConductorOptions
		RegisterConductorFactory(func(ctx Context, opts ConductorOptions) (ConductorConnection, error) {
			got = opts
			return &fakeConductor{}, nil
		})
		ctx, err := NewContext(context.Background(), Config{
			AppName:         "test-app",
			DatabaseURL:     sqliteURL(t),
			ConductorAPIKey: "test-key",
			ConductorURL:    "ws://localhost:8090/",
		})
		require.NoError(t, err)
		require.NoError(t, Shutdown(ctx, 5*time.Second))
		assert.Equal(t, "ws://localhost:8090/", got.URL)

		t.Setenv("DBOS__CLOUD", "true")
		t.Setenv("DBOS__CONDUCTOR_APP_NAME", "cloud-app")
		t.Setenv("DBOS__CONDUCTOR_KEY", "cloud-key")
		t.Setenv("DBOS__CONDUCTOR_URL", "wss://conductor.invalid/v1alpha1")
		ctx, err = NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: sqliteURL(t)})
		require.NoError(t, err)
		require.NoError(t, Shutdown(ctx, 5*time.Second))
		assert.Equal(t, ConductorOptions{AppName: "cloud-app", URL: "wss://conductor.invalid/v1alpha1", APIKey: "cloud-key"}, got)
	})

	t.Run("FactoryErrorIsAnInitializationError", func(t *testing.T) {
		RegisterConductorFactory(func(ctx Context, opts ConductorOptions) (ConductorConnection, error) {
			return nil, errors.New("bad key")
		})
		before := goleak.IgnoreCurrent()
		_, err := NewContext(context.Background(), Config{
			AppName:         "test-app",
			DatabaseURL:     sqliteURL(t),
			ConductorAPIKey: "test-key",
		})
		require.Error(t, err)
		var dbosErr *Error
		require.True(t, errors.As(err, &dbosErr))
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
		assert.Contains(t, dbosErr.Message, "failed to initialize conductor: bad key")
		// The system database was already open; the refusal must close it.
		require.Eventually(t, func() bool { return goleak.Find(before) == nil }, 5*time.Second, 50*time.Millisecond,
			"a refused context must not leave database goroutines behind")
	})

	t.Run("NoKeyNeedsNoModule", func(t *testing.T) {
		RegisterConductorFactory(nil)
		ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: sqliteURL(t)})
		require.NoError(t, err)
		require.Nil(t, ctx.(*dbosContext).conductor)
		require.NoError(t, Shutdown(ctx, 5*time.Second))
	})
}

// The accessors behind dbos/internals.Runtime.
func TestEnterpriseRuntimeAccessors(t *testing.T) {
	ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: sqliteURL(t)})
	require.NoError(t, err)
	t.Cleanup(func() { Shutdown(ctx, 5*time.Second) })
	c := ctx.(*dbosContext)

	assert.NotNil(t, c.SystemDB())
	assert.Same(t, c.logger, c.Logger())
	assert.NotEmpty(t, c.DBOSVersion())

	assert.Nil(t, c.AlertHandler())
	handler := func(name, message string, metadata map[string]string) {}
	SetAlertHandler(ctx, handler)
	assert.NotNil(t, c.AlertHandler())

	// Default JSON rows are stored base64-encoded; the decoder returns the raw JSON text.
	encoded, err := newJSONSerializer[any]().Encode(map[string]any{"a": 1})
	require.NoError(t, err)
	decoded, err := c.DecodeStoredValue(*encoded, "")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": float64(1)}, decoded)

	_, err = c.DecodeStoredValue("x", "NOT_A_FORMAT")
	assert.Error(t, err)

	handles, err := c.RecoverPendingWorkflows([]string{"nobody"})
	require.NoError(t, err)
	assert.Empty(t, handles)
}

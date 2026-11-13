package dbos

// Suspend mode's wiring: Config.SuspendMode is entitlement-gated at construction, needs
// the commercial module, and gets its backend from the factory that module registers.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSuspendBackend stands in for the enterprise backend.
type fakeSuspendBackend struct{}

func (fakeSuspendBackend) ParkRecv(context.Context, SuspendRecvInput) (bool, error) {
	return false, nil
}
func (fakeSuspendBackend) ParkSleep(context.Context, SuspendSleepInput) error { return nil }
func (fakeSuspendBackend) TransitionSuspendedWorkflows(context.Context) error { return nil }

// registerSuspendBackend registers factory until the test ends.
func registerSuspendBackend(t *testing.T, factory SuspendBackendFactory) {
	t.Helper()
	RegisterSuspendBackend(factory)
	t.Cleanup(func() { RegisterSuspendBackend(nil) })
}

// suspendConfig turns suspend mode on, its key checked against the stand-in.
func suspendConfig(t *testing.T, conductor *entitlementsStandIn) Config {
	t.Helper()
	return Config{
		AppName:               "test-app",
		DatabaseURL:           sqliteURL(t),
		SuspendMode:           true,
		TransactEnterpriseKey: "enterprise-key",
		ConductorURL:          conductor.url(),
	}
}

func TestSuspendModeWiring(t *testing.T) {
	t.Setenv("DBOS__CLOUD", "")
	t.Setenv("DBOS_DOMAIN", "")

	// launchesWithout checks a refused construction left nothing behind on databaseURL.
	launchesWithout := func(t *testing.T, databaseURL string) {
		t.Helper()
		ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: databaseURL})
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		require.NoError(t, ctx.Launch())
		assert.Nil(t, ctx.(*dbosContext).suspend)
	}

	t.Run("SuspendModeRequiresAKey", func(t *testing.T) {
		built := 0
		registerSuspendBackend(t, func(Context) (SuspendBackend, error) { built++; return fakeSuspendBackend{}, nil })
		databaseURL := sqliteURL(t)
		_, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: databaseURL, SuspendMode: true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Suspending workflows on recv and sleep requires a DBOS key")
		assert.Zero(t, built)
		launchesWithout(t, databaseURL)
	})

	t.Run("TheFlagNeedsTheEnterpriseModule", func(t *testing.T) {
		RegisterSuspendBackend(nil)
		conductor := newEntitlementsStandIn(t)
		_, err := NewContext(context.Background(), suspendConfig(t, conductor))
		require.Error(t, err)
		var dbosErr *Error
		require.True(t, errors.As(err, &dbosErr), "expected *Error, got %T", err)
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
		assert.Contains(t, dbosErr.Message, "Suspending workflows on recv and sleep requires go.dbos.dev/dbos-enterprise-go")
		assert.Contains(t, dbosErr.Message, `import _ "go.dbos.dev/dbos-enterprise-go"`)
		// The key was checked first.
		assert.Equal(t, []string{"Bearer enterprise-key"}, conductor.bearers())
	})

	t.Run("ConstructionRefusesWhenNotEntitled", func(t *testing.T) {
		built := 0
		registerSuspendBackend(t, func(Context) (SuspendBackend, error) { built++; return fakeSuspendBackend{}, nil })
		conductor := newEntitlementsStandIn(t)
		conductor.entitle(map[string]any{"sharding": true, "autoUpgrade": true, "suspendWorkflows": false})
		config := suspendConfig(t, conductor)
		_, err := NewContext(context.Background(), config)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not entitled to suspendWorkflows (Suspending workflows on recv and sleep)")
		assert.Contains(t, err.Error(), "plan does not include it")
		assert.Zero(t, built)
		launchesWithout(t, config.DatabaseURL)
	})

	t.Run("TheFactoryBuildsTheBackendOnTheRootContext", func(t *testing.T) {
		backend := &fakeSuspendBackend{}
		var gotCtx Context
		var hadSystemDB bool
		registerSuspendBackend(t, func(ctx Context) (SuspendBackend, error) {
			gotCtx = ctx
			hadSystemDB = ctx.(*dbosContext).SystemDB() != nil
			return backend, nil
		})
		conductor := newEntitlementsStandIn(t)
		ctx, err := NewContext(context.Background(), suspendConfig(t, conductor))
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })

		assert.Same(t, ctx, gotCtx, "the factory gets the root context")
		assert.True(t, hadSystemDB, "the system database is open when the factory runs")
		assert.Same(t, backend, ctx.(*dbosContext).suspend)
		assert.Equal(t, []string{"Bearer enterprise-key"}, conductor.bearers())
		require.NoError(t, ctx.Launch())
	})

	t.Run("AFactoryErrorIsAnInitializationError", func(t *testing.T) {
		registerSuspendBackend(t, func(Context) (SuspendBackend, error) { return nil, errors.New("no backend") })
		conductor := newEntitlementsStandIn(t)
		config := suspendConfig(t, conductor)
		_, err := NewContext(context.Background(), config)
		require.Error(t, err)
		var dbosErr *Error
		require.True(t, errors.As(err, &dbosErr), "expected *Error, got %T", err)
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
		assert.Contains(t, dbosErr.Message, "failed to initialize suspend mode: no backend")
		launchesWithout(t, config.DatabaseURL)
	})

	t.Run("OffByDefault", func(t *testing.T) {
		built := 0
		registerSuspendBackend(t, func(Context) (SuspendBackend, error) { built++; return fakeSuspendBackend{}, nil })
		conductor := newEntitlementsStandIn(t)
		config := suspendConfig(t, conductor)
		config.SuspendMode = false
		ctx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		assert.Nil(t, ctx.(*dbosContext).suspend)
		assert.Zero(t, built)
		assert.Empty(t, conductor.seen(), "no feature is on, so nothing is checked")
	})

}

// A suspending Recv or Sleep is refused when suspend mode is off, and the workflow keeps running.
func TestSuspendRequiresSuspendMode(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	suspendWorkflow := func(ctx Context, _ string) (string, error) {
		if _, err := Recv[string](ctx, "topic", time.Minute, WithRecvSuspend()); !errors.Is(err, ErrInvalidOption) {
			return "", fmt.Errorf("Recv returned %v", err)
		}
		if _, err := Sleep(ctx, time.Minute, WithSleepSuspend()); !errors.Is(err, ErrInvalidOption) {
			return "", fmt.Errorf("Sleep returned %v", err)
		}
		return "refused", nil
	}
	RegisterWorkflow(dbosCtx, suspendWorkflow)
	Launch(dbosCtx)

	handle, err := RunWorkflow(dbosCtx, suspendWorkflow, "")
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "refused", result)
	steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
	require.NoError(t, err)
	assert.Empty(t, steps)
}

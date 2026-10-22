package dbos

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingPass satisfies AutoUpgradePass, recording every call.
type recordingPass struct {
	ctx  Context
	opts AutoUpgradeOptions

	mu                sync.Mutex
	runs              []context.Context
	closes            int
	events            []string
	runErr            error
	closeErr          error
	blockUntilStopped bool
	block             chan struct{} // when set, Run blocks on it instead of ctx
}

func (p *recordingPass) Run(ctx context.Context) (int, error) {
	p.mu.Lock()
	p.events = append(p.events, "run-start")
	p.runs = append(p.runs, ctx)
	runErr, blockUntilStopped, block := p.runErr, p.blockUntilStopped, p.block
	p.mu.Unlock()
	if runErr != nil {
		return 0, runErr
	}
	if block != nil {
		<-block
	} else if blockUntilStopped {
		<-ctx.Done()
	}
	p.mu.Lock()
	p.events = append(p.events, "run-end")
	p.mu.Unlock()
	return 0, nil
}

func (p *recordingPass) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, "close")
	p.closes++
	return p.closeErr
}

func (p *recordingPass) runCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.runs)
}

func (p *recordingPass) eventLog() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

func (p *recordingPass) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}

// recordingFactory registers a factory that builds recording passes and returns where
// they are collected.
func recordingFactory(t *testing.T) *[]*recordingPass {
	t.Helper()
	var mu sync.Mutex
	instances := &[]*recordingPass{}
	RegisterAutoUpgradeFactory(func(ctx Context, opts AutoUpgradeOptions) (AutoUpgradePass, error) {
		p := &recordingPass{ctx: ctx, opts: opts}
		mu.Lock()
		*instances = append(*instances, p)
		mu.Unlock()
		return p, nil
	})
	t.Cleanup(func() { RegisterAutoUpgradeFactory(nil) })
	return instances
}

// conductorConfig configures a Conductor connection (stood in for) whose key and URL
// the entitlement check uses, with a fast auto-upgrade tick.
func conductorConfig(t *testing.T, conductor *entitlementsStandIn) Config {
	t.Helper()
	return Config{
		AppName:                "test-app",
		DatabaseURL:            sqliteURL(t),
		ConductorAPIKey:        "test-key",
		ConductorURL:           conductor.url(),
		AutoUpgradeInterval:    50 * time.Millisecond,
		AutoUpgradeGracePeriod: 7500 * time.Millisecond,
	}
}

func waitForRuns(t *testing.T, pass *recordingPass, count int) {
	t.Helper()
	require.Eventually(t, func() bool { return pass.runCount() >= count }, 5*time.Second, 5*time.Millisecond)
}

func TestAutoUpgradeWiring(t *testing.T) {
	t.Setenv("DBOS__CLOUD", "")
	t.Setenv("DBOS_DOMAIN", "")
	RegisterConductorFactory(fakeConductorFactory(&fakeConductor{}))
	t.Cleanup(func() { RegisterConductorFactory(nil) })

	noop := func(ctx Context, _ string) (string, error) { return "", nil }

	// --- Gating ---

	t.Run("AutoUpgradeRequiresAKey", func(t *testing.T) {
		recordingFactory(t)
		databaseURL := sqliteURL(t)
		_, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: databaseURL, AutoUpgrade: true})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-upgrading workflows requires a DBOS key")

		// The refused instance leaves nothing behind.
		ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: databaseURL})
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		require.NoError(t, ctx.Launch())
	})

	t.Run("TheFlagNeedsTheEnterpriseModule", func(t *testing.T) {
		RegisterAutoUpgradeFactory(nil)
		conductor := newEntitlementsStandIn(t)
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		_, err := NewContext(context.Background(), config)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-upgrading workflows requires go.dbos.dev/dbos-enterprise-go")
		assert.Contains(t, err.Error(), `import _ "go.dbos.dev/dbos-enterprise-go"`)
	})

	t.Run("OptedInWorkflowRequiresAKeyAtLaunch", func(t *testing.T) {
		instances := recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		// The config is off and no key is given, so construction passes...
		ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: sqliteURL(t)})
		require.NoError(t, err)
		RegisterWorkflow(ctx, noop, WithWorkflowName("wantsUpgrade"), WithAutoUpgrade(true))
		// ...but the workflow's opt-in needs a key, which only launch can tell.
		err = ctx.Launch()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-upgrading workflows requires a DBOS key")
		assert.Empty(t, conductor.seen())
		assert.Empty(t, *instances)
	})

	t.Run("OptedInWorkflowNeedsTheEnterpriseModuleAtLaunch", func(t *testing.T) {
		RegisterAutoUpgradeFactory(nil)
		conductor := newEntitlementsStandIn(t)
		ctx, err := NewContext(context.Background(), conductorConfig(t, conductor))
		require.NoError(t, err)
		RegisterWorkflow(ctx, noop, WithWorkflowName("wantsUpgrade"), WithAutoUpgrade(true))
		err = ctx.Launch()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-upgrading workflows requires go.dbos.dev/dbos-enterprise-go")
		// The key was checked first.
		assert.Equal(t, []string{"Bearer test-key"}, conductor.bearers())
	})

	t.Run("ConstructionRefusesWhenTheConfigFlagIsNotEntitled", func(t *testing.T) {
		instances := recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		conductor.entitle(map[string]any{"autoUpgrade": false})
		// The config turns the feature on, so the constructor checks it, before any state exists.
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		_, err := NewContext(context.Background(), config)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "plan does not include it")
		assert.Equal(t, []string{"Bearer test-key"}, conductor.bearers())
		assert.Empty(t, *instances)

		// The refused instance leaves nothing behind.
		ctx, err := NewContext(context.Background(), Config{AppName: "test-app", DatabaseURL: config.DatabaseURL})
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		require.NoError(t, ctx.Launch())
	})

	t.Run("LaunchRefusesWhenAnOptedInWorkflowIsNotEntitled", func(t *testing.T) {
		instances := recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		// The config is off, so construction checks nothing...
		ctx, err := NewContext(context.Background(), conductorConfig(t, conductor))
		require.NoError(t, err)
		assert.Empty(t, conductor.seen())
		RegisterWorkflow(ctx, noop, WithWorkflowName("wantsUpgrade"), WithAutoUpgrade(true))
		// ...and the workflow's opt-in is checked at launch, which refuses.
		conductor.entitle(map[string]any{"autoUpgrade": false})
		err = ctx.Launch()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "plan does not include it")
		assert.Equal(t, []string{"Bearer test-key"}, conductor.bearers())
		assert.Nil(t, ctx.(*dbosContext).autoUpgrader)
		assert.Empty(t, *instances)
	})

	t.Run("TheEntitlementIsCheckedWithTheEnterpriseKeyFirst", func(t *testing.T) {
		recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		// Both keys given: the enterprise key is the one checked.
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		config.TransactEnterpriseKey = "enterprise-key"
		ctx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		RegisterWorkflow(ctx, noop, WithWorkflowName("inherits"))
		require.NoError(t, ctx.Launch())
		// Checked once, at construction: launch does not ask again for the config flag.
		assert.Equal(t, []string{"Bearer enterprise-key"}, conductor.bearers())
		assert.NotNil(t, ctx.(*dbosContext).autoUpgrader)
		require.NoError(t, Shutdown(ctx, 10*time.Second))

		// The enterprise key alone is enough: no Conductor connection is needed, only
		// the Conductor to ask about the entitlement.
		ctx, err = NewContext(context.Background(), Config{
			AppName:               "test-app",
			DatabaseURL:           sqliteURL(t),
			AutoUpgrade:           true,
			TransactEnterpriseKey: "enterprise-key",
			ConductorURL:          conductor.url(),
			AutoUpgradeInterval:   50 * time.Millisecond,
		})
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		RegisterWorkflow(ctx, noop, WithWorkflowName("standalone"))
		require.NoError(t, ctx.Launch())
		assert.Equal(t, []string{"Bearer enterprise-key", "Bearer enterprise-key"}, conductor.bearers())
		assert.Nil(t, ctx.(*dbosContext).conductor)
		assert.NotNil(t, ctx.(*dbosContext).autoUpgrader)
	})

	// --- Enrollment ---

	t.Run("WorkflowsEnrollByFlagThenByConfig", func(t *testing.T) {
		recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		ctx, err := NewContext(context.Background(), conductorConfig(t, conductor))
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		c := ctx.(*dbosContext)

		optedIn := func(ctx Context, _ string) (string, error) { return "", nil }
		optedOut := func(ctx Context, _ string) (string, error) { return "", nil }
		inherits := func(ctx Context, _ string) (string, error) { return "", nil }
		RegisterWorkflow(ctx, optedIn, WithWorkflowName("optedIn"), WithAutoUpgrade(true))
		RegisterWorkflow(ctx, optedOut, WithWorkflowName("optedOut"), WithAutoUpgrade(false))
		RegisterWorkflow(ctx, inherits, WithWorkflowName("inherits"))
		// A workflow without a custom name is recorded under its function name.
		RegisterWorkflow(ctx, noop)
		noopName, err := c.resolveWorkflowName(noop)
		require.NoError(t, err)

		// The config is off, so only the explicit opt-in enrolls.
		assert.Equal(t, []string{"optedIn"}, c.autoUpgradableWorkflows())
		assert.Equal(t, []string{"optedIn"}, c.AutoUpgradableWorkflows(), "the runtime accessor behind dbos/internals")
		// With it on, everything that did not opt out enrolls, under the recorded names.
		c.config.AutoUpgrade = true
		assert.Equal(t, []string{noopName, "inherits", "optedIn"}, c.autoUpgradableWorkflows())
	})

	t.Run("InstancesOfOneMethodEnrollOnce", func(t *testing.T) {
		recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		ctx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		a, b := &upgradeInstance{name: "a"}, &upgradeInstance{name: "b"}
		RegisterWorkflow(ctx, a.Run, WithInstance(a), WithWorkflowName("instanceRun"))
		RegisterWorkflow(ctx, b.Run, WithInstance(b), WithWorkflowName("instanceRun"))
		assert.Equal(t, []string{"instanceRun"}, ctx.(*dbosContext).autoUpgradableWorkflows())
	})

	t.Run("NoLoopWithoutEnrolledWorkflows", func(t *testing.T) {
		instances := recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		// A key is given, but nothing enrolls: the config is off and the one workflow opts out.
		ctx, err := NewContext(context.Background(), conductorConfig(t, conductor))
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		RegisterWorkflow(ctx, noop, WithWorkflowName("optedOut"), WithAutoUpgrade(false))
		require.NoError(t, ctx.Launch())
		assert.Nil(t, ctx.(*dbosContext).autoUpgrader)
		assert.Empty(t, *instances)
		// So no entitlement is checked either.
		assert.Empty(t, conductor.seen())
	})

	// --- Loop lifecycle through the Context ---

	t.Run("TheLoopRunsThePassUntilShutdown", func(t *testing.T) {
		instances := recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		ctx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		RegisterWorkflow(ctx, noop, WithWorkflowName("inherits"))
		require.NoError(t, ctx.Launch())
		c := ctx.(*dbosContext)
		// The config's flag reaches the context: the unflagged workflow enrolls, and the
		// Conductor key stands in for the enterprise key.
		assert.Equal(t, []string{"inherits"}, c.autoUpgradableWorkflows())
		assert.Equal(t, []string{"Bearer test-key"}, conductor.bearers())
		upgrader := c.autoUpgrader
		require.NotNil(t, upgrader)
		assert.Equal(t, 50*time.Millisecond, upgrader.interval)

		// The pass was built from this context with the configured grace, and ticks on
		// the configured interval.
		require.Len(t, *instances, 1)
		pass := (*instances)[0]
		assert.Same(t, ctx, pass.ctx)
		assert.Equal(t, AutoUpgradeOptions{GracePeriod: 7500 * time.Millisecond}, pass.opts)
		waitForRuns(t, pass, 3)
		for _, runCtx := range pass.runs {
			assert.Same(t, upgrader.ctx, runCtx)
		}

		require.NoError(t, Shutdown(ctx, 10*time.Second))
		assert.Equal(t, 1, pass.closeCount())
		// Nothing ticks after shutdown.
		runs := pass.runCount()
		time.Sleep(200 * time.Millisecond)
		assert.Equal(t, runs, pass.runCount())
	})

	t.Run("DefaultInterval", func(t *testing.T) {
		instances := recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		config.AutoUpgradeInterval = 0
		config.AutoUpgradeGracePeriod = 0
		ctx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		RegisterWorkflow(ctx, noop, WithWorkflowName("inherits"))
		require.NoError(t, ctx.Launch())
		require.NotNil(t, ctx.(*dbosContext).autoUpgrader)
		assert.Equal(t, 30*time.Second, ctx.(*dbosContext).autoUpgrader.interval)
		require.Len(t, *instances, 1)
		assert.Equal(t, AutoUpgradeOptions{GracePeriod: 0}, (*instances)[0].opts)
	})

	t.Run("AFailedFactoryIsAnInitializationError", func(t *testing.T) {
		RegisterAutoUpgradeFactory(func(ctx Context, opts AutoUpgradeOptions) (AutoUpgradePass, error) {
			return nil, errors.New("no lease")
		})
		t.Cleanup(func() { RegisterAutoUpgradeFactory(nil) })
		conductor := newEntitlementsStandIn(t)
		config := conductorConfig(t, conductor)
		config.AutoUpgrade = true
		ctx, err := NewContext(context.Background(), config)
		require.NoError(t, err)
		RegisterWorkflow(ctx, noop, WithWorkflowName("inherits"))
		err = ctx.Launch()
		require.Error(t, err)
		var dbosErr *Error
		require.True(t, errors.As(err, &dbosErr))
		assert.Equal(t, ErrorCodeInitialization, dbosErr.Code)
		assert.Contains(t, dbosErr.Message, "failed to initialize auto-upgrade: no lease")
	})

	t.Run("IntervalsCannotBeNegative", func(t *testing.T) {
		recordingFactory(t)
		conductor := newEntitlementsStandIn(t)
		config := conductorConfig(t, conductor)
		config.AutoUpgradeInterval = -time.Second
		_, err := NewContext(context.Background(), config)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "autoUpgradeInterval cannot be negative")
		config = conductorConfig(t, conductor)
		config.AutoUpgradeGracePeriod = -time.Second
		_, err = NewContext(context.Background(), config)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "autoUpgradeGracePeriod cannot be negative")
	})
}

// upgradeInstance is a configured instance whose method registers as a workflow.
type upgradeInstance struct{ name string }

func (i *upgradeInstance) ConfigName() string { return i.name }
func (i *upgradeInstance) Run(ctx Context, _ string) (string, error) {
	return i.name, nil
}

// --- Loop lifecycle in isolation ---

func TestAutoUpgradeLoop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

	t.Run("APassFailureDoesNotStopTheLoop", func(t *testing.T) {
		pass := &recordingPass{runErr: errors.New("boom")}
		loop := newAutoUpgradeLoop(context.Background(), pass, 10*time.Millisecond, logger)
		loop.start()
		waitForRuns(t, pass, 3)
		require.NoError(t, loop.stop(5*time.Second))
		assert.Equal(t, 1, pass.closeCount())
	})

	t.Run("StopCancelsThePassInFlightThenCloses", func(t *testing.T) {
		pass := &recordingPass{blockUntilStopped: true}
		loop := newAutoUpgradeLoop(context.Background(), pass, 10*time.Millisecond, logger)
		loop.start()
		waitForRuns(t, pass, 1)
		require.NoError(t, loop.stop(5*time.Second))
		// The pass saw the cancellation, returned, and only then was closed.
		assert.Equal(t, []string{"run-start", "run-end", "close"}, pass.eventLog())
		// Stopping again is a no-op.
		require.NoError(t, loop.stop(5*time.Second))
		assert.Equal(t, 1, pass.closeCount())
	})

	t.Run("CancellingTheParentContextEndsTheLoop", func(t *testing.T) {
		pass := &recordingPass{blockUntilStopped: true}
		parent, cancel := context.WithCancel(context.Background())
		loop := newAutoUpgradeLoop(parent, pass, 10*time.Millisecond, logger)
		loop.start()
		waitForRuns(t, pass, 1)
		cancel()
		select {
		case <-loop.done:
		case <-time.After(5 * time.Second):
			t.Fatal("the loop did not exit after its parent context was cancelled")
		}
		// The pass is closed by stop, not by the cancellation.
		assert.Equal(t, 0, pass.closeCount())
		require.NoError(t, loop.stop(5*time.Second))
		assert.Equal(t, 1, pass.closeCount())
	})

	t.Run("APassThatIgnoresTheCancellationTimesOut", func(t *testing.T) {
		pass := &recordingPass{block: make(chan struct{})}
		loop := newAutoUpgradeLoop(context.Background(), pass, 10*time.Millisecond, logger)
		loop.start()
		waitForRuns(t, pass, 1)
		err := loop.stop(50 * time.Millisecond)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "did not stop within")
		// Closed regardless, as the context will not come back.
		assert.Equal(t, 1, pass.closeCount())
		close(pass.block)
		<-loop.done
	})
}

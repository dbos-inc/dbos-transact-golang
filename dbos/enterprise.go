package dbos

// The contract between DBOS Transact and go.dbos.dev/dbos-enterprise-go, the
// commercial extension that connects an application to DBOS Conductor.
//
// Applications opt in with a blank import:
//
//	import _ "go.dbos.dev/dbos-enterprise-go"
//
// The commercial module's init() function registers a Conductor constructor and
// an auto-upgrade pass constructor.
//
// The other direction, what the enterprise module may use from OSS beyond the
// public API, is declared in package dbos/internals.

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

const EnterpriseModule = "go.dbos.dev/dbos-enterprise-go"

type ConductorOptions struct {
	AppName          string         // Application name Conductor addresses this executor by
	URL              string         // Conductor WebSocket URL, e.g. wss://cloud.dbos.dev/conductor/v1alpha1
	APIKey           string         // Conductor API key
	ExecutorMetadata map[string]any // Config.ConductorExecutorMetadata
	MetadataOnlyMode bool           // Config.ConductorMetadataOnlyMode
}

// ConductorConnection is the Conductor client, run for the life of a launched Context.
type ConductorConnection interface {
	Launch()                              // Start connecting; called by Context.Launch after the runtime is up
	Shutdown(timeout time.Duration) error // Disconnect; returns an error if the client did not stop within timeout
}

// ConductorFactory builds the Conductor client for a Context. ctx is the root
// Context returned by NewContext, before Launch. ctx must implement the
// dbos/internals.Runtime interface.
type ConductorFactory func(ctx Context, opts ConductorOptions) (ConductorConnection, error)

var (
	conductorFactoryMu sync.RWMutex
	conductorFactory   ConductorFactory
)

// RegisterConductorFactory makes Conductor available to every Context created
// afterwards. The commercial module calls it from its init function.
func RegisterConductorFactory(f ConductorFactory) {
	conductorFactoryMu.Lock()
	defer conductorFactoryMu.Unlock()
	conductorFactory = f
}

func registeredConductorFactory() ConductorFactory {
	conductorFactoryMu.RLock()
	defer conductorFactoryMu.RUnlock()
	return conductorFactory
}

// AutoUpgradeOptions configures the auto-upgrade pass the commercial module builds.
type AutoUpgradeOptions struct {
	GracePeriod time.Duration // Config.AutoUpgradeGracePeriod
}

// AutoUpgradePass is one tick of auto-upgrade: it hands the work left on older
// application versions to the latest one. The commercial module builds it, and core
// runs it on a timer from Launch until Shutdown.
type AutoUpgradePass interface {
	// Run moves the workflows left on older application versions and returns how many
	// it moved. A cancelled ctx stops it at its next chunk.
	Run(ctx context.Context) (int, error)
	// Close releases what the pass holds, once it is no longer run.
	Close() error
}

// AutoUpgradeFactory builds the auto-upgrade pass for a Context. ctx is the root
// Context returned by NewContext, being launched. ctx must implement the
// dbos/internals.Runtime interface.
type AutoUpgradeFactory func(ctx Context, opts AutoUpgradeOptions) (AutoUpgradePass, error)

var (
	autoUpgradeFactoryMu sync.RWMutex
	autoUpgradeFactory   AutoUpgradeFactory
)

// RegisterAutoUpgradeFactory makes auto-upgrade available to every Context created
// afterwards. The commercial module calls it from its init function.
func RegisterAutoUpgradeFactory(f AutoUpgradeFactory) {
	autoUpgradeFactoryMu.Lock()
	defer autoUpgradeFactoryMu.Unlock()
	autoUpgradeFactory = f
}

func registeredAutoUpgradeFactory() AutoUpgradeFactory {
	autoUpgradeFactoryMu.RLock()
	defer autoUpgradeFactoryMu.RUnlock()
	return autoUpgradeFactory
}

// enterpriseUnavailableError says what needed the commercial module, and how to add it.
func enterpriseUnavailableError(feature string) error {
	return models.NewInitializationError(fmt.Sprintf(
		"%s requires %s at the same minor version as github.com/dbos-inc/dbos-transact-golang (%s). "+
			"Add `import _ %q` to your program and run `go get %s`",
		feature, EnterpriseModule, getDBOSVersion(), EnterpriseModule, EnterpriseModule))
}

// Add methods to dbosContext to implement dbos/internals.Runtime.

func (c *dbosContext) SystemDB() sysdb.SystemDatabase { return c.systemDB }
func (c *dbosContext) Logger() *slog.Logger           { return c.logger }
func (c *dbosContext) AlertHandler() AlertHandler     { return c.alertHandler }
func (c *dbosContext) DBOSVersion() string            { return getDBOSVersion() }
func (c *dbosContext) AutoUpgradableWorkflows() []string {
	return c.autoUpgradableWorkflows()
}
func (c *dbosContext) RecoverPendingWorkflows(executorIDs []string) ([]WorkflowHandle[any], error) {
	return recoverPendingWorkflows(c, executorIDs)
}

func (c *dbosContext) DecodeStoredValue(value, serialization string) (any, error) {
	decoder, err := resolveDecoder[any](serialization, getCustomSerializerFromCtx(c))
	if err != nil {
		return nil, err
	}
	return decoder.Decode(&value)
}

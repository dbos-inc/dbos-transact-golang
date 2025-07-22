package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sort"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	_APP_VERSION               string
	_EXECUTOR_ID               string
	_APP_ID                    string
	_DEFAULT_ADMIN_SERVER_PORT = 3001
)

func computeApplicationVersion() string {
	if len(registry) == 0 {
		fmt.Println("DBOS: No registered workflows found, cannot compute application version")
		return ""
	}

	// Collect all function names and sort them for consistent hashing
	var functionNames []string
	for fqn := range registry {
		functionNames = append(functionNames, fqn)
	}
	sort.Strings(functionNames)

	hasher := sha256.New()

	for _, fqn := range functionNames {
		workflowEntry := registry[fqn]

		// Try to get function source location and other identifying info
		if pc := runtime.FuncForPC(reflect.ValueOf(workflowEntry.wrappedFunction).Pointer()); pc != nil {
			// Get the function's entry point - this reflects the actual compiled code
			entry := pc.Entry()
			fmt.Fprintf(hasher, "%x", entry)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil))

}

var workflowScheduler *cron.Cron // Global because accessed during workflow registration before the dbos singleton is initialized

var logger *slog.Logger // Global because accessed everywhere inside the library

func getLogger() *slog.Logger {
	if dbos == nil || logger == nil {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return logger
}

type config struct {
	logger      *slog.Logger
	adminServer bool
	databaseURL string
	appName     string
}

// NewConfig merges configuration from two sources in order of precedence:
// 1. programmatic configuration
// 2. environment variables
// Finally, it applies default values if needed.
func NewConfig(programmaticConfig config) *config {
	dbosConfig := &config{}

	// Start with environment variables (lowest precedence)
	if dbURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL"); dbURL != "" {
		dbosConfig.databaseURL = dbURL
	}

	// Override with programmatic configuration (highest precedence)
	if len(programmaticConfig.databaseURL) > 0 {
		dbosConfig.databaseURL = programmaticConfig.databaseURL
	}
	if len(programmaticConfig.appName) > 0 {
		dbosConfig.appName = programmaticConfig.appName
	}
	// Copy over parameters that can only be set programmatically
	dbosConfig.logger = programmaticConfig.logger
	dbosConfig.adminServer = programmaticConfig.adminServer

	// Load defaults
	if len(dbosConfig.databaseURL) == 0 {
		getLogger().Info("Using default database URL: postgres://postgres:${PGPASSWORD}@localhost:5432/dbos?sslmode=disable")
		password := url.QueryEscape(os.Getenv("PGPASSWORD"))
		dbosConfig.databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", password)
	}
	return dbosConfig
}

var dbos *Executor // DBOS singleton instance

type Executor struct {
	systemDB              SystemDatabase
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
	queueRunnerDone       chan struct{}
	adminServer           *adminServer
	config                *config
}

type executorOption func(*config)

func WithLogger(logger *slog.Logger) executorOption {
	return func(config *config) {
		config.logger = logger
	}
}

func WithAdminServer() executorOption {
	return func(config *config) {
		config.adminServer = true
	}
}

func WithDatabaseURL(url string) executorOption {
	return func(config *config) {
		config.databaseURL = url
	}
}

func WithAppName(name string) executorOption {
	return func(config *config) {
		config.appName = name
	}
}

func NewExecutor(options ...executorOption) (*Executor, error) {
	if dbos != nil {
		fmt.Println("warning: DBOS instance already initialized, skipping re-initialization")
		return nil, newInitializationError("DBOS already initialized")
	}

	// Start with default configuration
	config := &config{
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}

	// Apply options
	for _, option := range options {
		option(config)
	}

	// Load & process the configuration
	config = NewConfig(*config)

	// Set global logger
	logger = config.logger

	// Initialize global variables with environment variables, providing defaults if not set
	_APP_VERSION = os.Getenv("DBOS__APPVERSION")
	if _APP_VERSION == "" {
		_APP_VERSION = computeApplicationVersion()
		logger.Info("DBOS__APPVERSION not set, using computed hash")
	}

	_EXECUTOR_ID = os.Getenv("DBOS__VMID")
	if _EXECUTOR_ID == "" {
		_EXECUTOR_ID = "local"
		logger.Info("DBOS__VMID not set, using default", "executor_id", _EXECUTOR_ID)
	}

	_APP_ID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := NewSystemDatabase(config.databaseURL)
	if err != nil {
		return nil, newInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	logger.Info("System database initialized")

	// Create the executor instance
	executor := &Executor{
		config:   config,
		systemDB: systemDB,
	}

	// Set the global dbos instance
	dbos = executor

	return executor, nil
}

func (e *Executor) Launch() error {
	// Start the system database
	e.systemDB.Launch(context.Background())

	// Start the admin server if configured
	if e.config.adminServer {
		adminServer := newAdminServer(_DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		logger.Info("Admin server started", "port", _DEFAULT_ADMIN_SERVER_PORT)
		e.adminServer = adminServer
	}

	// Create context with cancel function for queue runner
	ctx, cancel := context.WithCancel(context.Background())
	e.queueRunnerCtx = ctx
	e.queueRunnerCancelFunc = cancel
	e.queueRunnerDone = make(chan struct{})

	// Start the queue runner in a goroutine
	go func() {
		defer close(e.queueRunnerDone)
		queueRunner(ctx)
	}()
	logger.Info("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if workflowScheduler != nil {
		workflowScheduler.Start()
		logger.Info("Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	_, err := recoverPendingWorkflows(context.Background(), []string{_EXECUTOR_ID}) // XXX maybe use the queue runner context here to allow Shutdown to cancel it?
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}

	logger.Info("DBOS initialized", "app_version", _APP_VERSION, "executor_id", _EXECUTOR_ID)
	return nil
}

func (e *Executor) Shutdown() {
	if e == nil {
		fmt.Println("Executor instance is nil, cannot shutdown")
		return
	}

	// XXX is there a way to ensure all workflows goroutine are done before closing?

	// Cancel the context to stop the queue runner
	if e.queueRunnerCancelFunc != nil {
		e.queueRunnerCancelFunc()
		// Wait for queue runner to finish
		<-e.queueRunnerDone
		getLogger().Info("Queue runner stopped")
	}

	if workflowScheduler != nil {
		ctx := workflowScheduler.Stop()
		// Wait for all running jobs to complete with 5-second timeout
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			getLogger().Info("All scheduled jobs completed")
		case <-timeoutCtx.Done():
			getLogger().Warn("Timeout waiting for jobs to complete", "timeout", "5s")
		}
	}

	if e.systemDB != nil {
		e.systemDB.Shutdown()
		e.systemDB = nil
	}

	if e.adminServer != nil {
		err := e.adminServer.Shutdown()
		if err != nil {
			getLogger().Error("Failed to shutdown admin server", "error", err)
		} else {
			getLogger().Info("Admin server shutdown complete")
		}
		e.adminServer = nil
	}

	// Clear global references if this is the global instance
	if dbos == e {
		if logger != nil {
			logger = nil
		}
		dbos = nil
	}
}

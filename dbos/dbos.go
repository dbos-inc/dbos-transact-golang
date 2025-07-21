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
	APP_VERSION               string
	EXECUTOR_ID               string
	APP_ID                    string
	DEFAULT_ADMIN_SERVER_PORT = 3001
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

type Executor interface {
	Shutdown()
}

var workflowScheduler *cron.Cron

type executor struct {
	systemDB              SystemDatabase
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
	queueRunnerDone       chan struct{}
	adminServer           *AdminServer
}

var dbos *executor

func getExecutor() *executor {
	if dbos == nil {
		return nil
	}
	return dbos
}

var logger *slog.Logger

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

type LaunchOption func(*config)

func WithLogger(logger *slog.Logger) LaunchOption {
	return func(config *config) {
		config.logger = logger
	}
}

func WithAdminServer() LaunchOption {
	return func(config *config) {
		config.adminServer = true
	}
}

func WithDatabaseURL(url string) LaunchOption {
	return func(config *config) {
		config.databaseURL = url
	}
}

func Launch(options ...LaunchOption) error {
	if dbos != nil {
		fmt.Println("warning: DBOS instance already initialized, skipping re-initialization")
		return NewInitializationError("DBOS already initialized")
	}

	// Load & process the configuration
	config := &config{
		logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	for _, option := range options {
		option(config)
	}
	config = NewConfig(*config)

	logger = config.logger

	// Initialize with environment variables, providing defaults if not set
	APP_VERSION = os.Getenv("DBOS__APPVERSION")
	if APP_VERSION == "" {
		APP_VERSION = computeApplicationVersion()
		logger.Info("DBOS__APPVERSION not set, using computed hash")
	}

	EXECUTOR_ID = os.Getenv("DBOS__VMID")
	if EXECUTOR_ID == "" {
		EXECUTOR_ID = "local"
		logger.Info("DBOS__VMID not set, using default", "executor_id", EXECUTOR_ID)
	}

	APP_ID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := NewSystemDatabase(config.databaseURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	logger.Info("System database initialized")

	systemDB.Launch(context.Background())

	// Start the admin server if configured
	var adminServer *AdminServer
	if config.adminServer {
		adminServer = NewAdminServer(DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			logger.Error("Failed to start admin server", "error", err)
			return NewInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		logger.Info("Admin server started", "port", DEFAULT_ADMIN_SERVER_PORT)
	}

	// Create context with cancel function for queue runner
	ctx, cancel := context.WithCancel(context.Background())

	dbos = &executor{
		systemDB:              systemDB,
		queueRunnerCtx:        ctx,
		queueRunnerCancelFunc: cancel,
		queueRunnerDone:       make(chan struct{}),
		adminServer:           adminServer,
	}

	// Start the queue runner in a goroutine
	go func() {
		defer close(dbos.queueRunnerDone)
		queueRunner(ctx)
	}()
	logger.Info("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if workflowScheduler != nil {
		workflowScheduler.Start()
		logger.Info("Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	_, err = recoverPendingWorkflows(context.Background(), []string{EXECUTOR_ID}) // XXX maybe use the queue runner context here to allow Shutdown to cancel it?
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}

	logger.Info("DBOS initialized", "app_version", APP_VERSION, "executor_id", EXECUTOR_ID)
	return nil
}

// Close closes the DBOS instance and its resources
func Shutdown() {
	if dbos == nil {
		fmt.Println("DBOS instance is nil, cannot destroy")
		return
	}

	// XXX is there a way to ensure all workflows goroutine are done before closing?

	// Cancel the context to stop the queue runner
	if dbos.queueRunnerCancelFunc != nil {
		dbos.queueRunnerCancelFunc()
		// Wait for queue runner to finish
		<-dbos.queueRunnerDone
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

	if dbos.systemDB != nil {
		dbos.systemDB.Shutdown()
		dbos.systemDB = nil
	}

	if dbos.adminServer != nil {
		err := dbos.adminServer.Shutdown()
		if err != nil {
			getLogger().Error("Failed to shutdown admin server", "error", err)
		} else {
			getLogger().Info("Admin server shutdown complete")
		}
		dbos.adminServer = nil
	}

	if logger != nil {
		logger = nil
	}

	dbos = nil
}

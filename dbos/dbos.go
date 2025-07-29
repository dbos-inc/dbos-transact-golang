package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	_DEFAULT_ADMIN_SERVER_PORT = 3001
)

var logger *slog.Logger // Global because accessed everywhere inside the library

func getLogger() *slog.Logger {
	if dbos == nil || logger == nil {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return logger
}

type Config struct {
	DatabaseURL string
	AppName     string
	Logger      *slog.Logger
	AdminServer bool
}

// processConfig enforces mandatory fields and applies defaults.
func processConfig(inputConfig *Config) (*Config, error) {
	// First check required fields
	if len(inputConfig.DatabaseURL) == 0 {
		return nil, fmt.Errorf("missing required config field: databaseURL")
	}
	if len(inputConfig.AppName) == 0 {
		return nil, fmt.Errorf("missing required config field: appName")
	}

	dbosConfig := &Config{
		DatabaseURL: inputConfig.DatabaseURL,
		AppName:     inputConfig.AppName,
		Logger:      inputConfig.Logger,
		AdminServer: inputConfig.AdminServer,
	}

	// Load defaults
	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	return dbosConfig, nil
}

type DBOSExecutor interface {
	Launch() error
	Shutdown()

	RegisterWorkflow(fqn string, fn typedErasedWorkflowWrapperFunc, maxRetries int)
	RegisterScheduledWorkflow(fqn string, fn typedErasedWorkflowWrapperFunc, cronSchedule string, maxRetries int)

	GetWorkflowScheduler() *cron.Cron
	GetApplicationVersion() string
}

var dbos *executor // DBOS singleton instance

type executor struct {
	systemDB    SystemDatabase
	adminServer *adminServer
	config      *Config
	// Queue runner context and cancel function
	queueRunnerCtx        context.Context
	queueRunnerCancelFunc context.CancelFunc
	queueRunnerDone       chan struct{}
	// Application metadata
	applicationVersion string
	applicationID      string
	executorID         string
	// Wait group for workflow goroutines
	workflowsWg *sync.WaitGroup
	// Workflow registry
	workflowRegistry map[string]workflowRegistryEntry
	workflowRegMutex sync.RWMutex
	// Workflow scheduler
	workflowScheduler *cron.Cron
}

func (e *executor) GetWorkflowScheduler() *cron.Cron {
	if e.workflowScheduler == nil {
		e.workflowScheduler = cron.New(cron.WithSeconds())
	}
	return e.workflowScheduler
}

func (e *executor) GetApplicationVersion() string {
	return e.applicationVersion
}

// TODO: use a normal builder pattern name (NewDBOSExecutor)
func Initialize(inputConfig Config) (DBOSExecutor, error) {
	if dbos != nil {
		fmt.Println("warning: DBOS instance already initialized, skipping re-initialization")
		return nil, newInitializationError("DBOS already initialized")
	}

	initExecutor := &executor{
		workflowsWg: &sync.WaitGroup{},
	}

	// Load & process the configuration
	config, err := processConfig(&inputConfig)
	if err != nil {
		return nil, newInitializationError(err.Error())
	}
	initExecutor.config = config

	// Set global logger
	logger = config.Logger

	// Register types we serialize with gob
	var t time.Time
	gob.Register(t)

	// Initialize global variables with environment variables, providing defaults if not set
	initExecutor.applicationVersion = os.Getenv("DBOS__APPVERSION")
	if initExecutor.applicationVersion == "" {
		initExecutor.applicationVersion = computeApplicationVersion()
		logger.Info("DBOS__APPVERSION not set, using computed hash")
	}

	initExecutor.executorID = os.Getenv("DBOS__VMID")
	if initExecutor.executorID == "" {
		initExecutor.executorID = "local"
		logger.Info("DBOS__VMID not set, using default", "executor_id", initExecutor.executorID)
	}

	initExecutor.applicationID = os.Getenv("DBOS__APPID")

	// Create the system database
	systemDB, err := NewSystemDatabase(config.DatabaseURL)
	if err != nil {
		return nil, newInitializationError(fmt.Sprintf("failed to create system database: %v", err))
	}
	initExecutor.systemDB = systemDB
	logger.Info("System database initialized")

	// Initialize the workflow registry
	initExecutor.workflowRegistry = make(map[string]workflowRegistryEntry)

	// Set the global dbos instance
	dbos = initExecutor

	return initExecutor, nil
}

func (e *executor) Launch() error {
	// Start the system database
	e.systemDB.Launch(context.Background())

	// Start the admin server if configured
	if e.config.AdminServer {
		adminServer := newAdminServer(_DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		logger.Info("Admin server started", "port", _DEFAULT_ADMIN_SERVER_PORT)
		dbos.adminServer = adminServer
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
	if e.workflowScheduler != nil {
		e.workflowScheduler.Start()
		logger.Info("Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(context.Background(), []string{e.executorID}) // XXX maybe use the queue runner context here to allow Shutdown to cancel it?
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	}

	logger.Info("DBOS initialized", "app_version", e.applicationVersion, "executor_id", e.executorID)
	return nil
}

func (e *executor) Shutdown() {
	if e == nil {
		fmt.Println("DBOS instance is nil, cannot shutdown")
		return
	}

	// XXX is there a way to ensure all workflows goroutine are done before closing?
	e.workflowsWg.Wait()

	// Cancel the context to stop the queue runner
	if e.queueRunnerCancelFunc != nil {
		e.queueRunnerCancelFunc()
		// Wait for queue runner to finish
		<-e.queueRunnerDone
		getLogger().Info("Queue runner stopped")
	}

	if e.workflowScheduler != nil {
		ctx := e.workflowScheduler.Stop()
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

	if logger != nil {
		logger = nil
	}
}

func GetBinaryHash() (string, error) {
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	file, err := os.Open(execPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func computeApplicationVersion() string {
	hash, err := GetBinaryHash()
	if err != nil {
		fmt.Printf("DBOS: Failed to compute binary hash: %v\n", err)
		return ""
	}
	return hash
}

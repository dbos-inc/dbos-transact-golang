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
	if logger == nil {
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

type DBOSContext interface {
	context.Context // Standard Go context behavior

	// Context Lifecycle
	Launch() error
	Shutdown()
	WithValue(key, val any) DBOSContext

	// Workflow registration
	RegisterWorkflow(fqn string, fn typedErasedWorkflowWrapperFunc, maxRetries int)
	RegisterScheduledWorkflow(fqn string, fn typedErasedWorkflowWrapperFunc, cronSchedule string, maxRetries int)

	// Workflow operations
	RunAsStep(fn TypeErasedStepFunc, input any, stepName string, opts ...stepOption) (any, error)
	Send(input WorkflowSendInputInternal) error
	Recv(input WorkflowRecvInput) (any, error)
	SetEvent(input WorkflowSetEventInputInternal) error
	GetEvent(input WorkflowGetEventInput) (any, error)
	Sleep(duration time.Duration) (time.Duration, error)

	// Workflow management
	RetrieveWorkflow(workflowIDs []string) ([]WorkflowStatus, error)
	CheckChildWorkflow(parentWorkflowID string, stepCounter int) (*string, error)
	InsertWorkflowStatus(status WorkflowStatus, maxRetries int) (*insertWorkflowResult, error)
	RecordChildWorkflow(input recordChildWorkflowDBInput) error
	UpdateWorkflowOutcome(workflowID string, status WorkflowStatusType, err error, output any) error

	// Context operations
	GetWorkflowID() (string, error)

	// Accessors
	GetWorkflowScheduler() *cron.Cron
	GetApplicationVersion() string
	GetSystemDB() SystemDatabase
	GetContext() context.Context
	GetExecutorID() string
	GetApplicationID() string
	GetWorkflowWg() *sync.WaitGroup
}

type dbosContext struct {
	ctx         context.Context // Embedded context for standard behavior
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
	workflowRegMutex *sync.RWMutex

	// Workflow scheduler
	workflowScheduler *cron.Cron
}

// Implement contex.Context interface methods
func (e *dbosContext) Deadline() (deadline time.Time, ok bool) {
	return e.ctx.Deadline()
}

func (e *dbosContext) Done() <-chan struct{} {
	return e.ctx.Done()
}

func (e *dbosContext) Err() error {
	return e.ctx.Err()
}

func (e *dbosContext) Value(key any) any {
	return e.ctx.Value(key)
}

// Create a new context
// This is intended for workflow contexts and step contexts
// Hence we only set the relevant fields
func (e *dbosContext) WithValue(key, val any) DBOSContext {
	return &dbosContext{
		ctx:                context.WithValue(e.ctx, key, val),
		systemDB:           e.systemDB,
		applicationVersion: e.applicationVersion,
		executorID:         e.executorID,
		applicationID:      e.applicationID,
		workflowsWg:        e.workflowsWg,
	}
}

func (e *dbosContext) GetContext() context.Context {
	return e.ctx
}

func (e *dbosContext) GetWorkflowScheduler() *cron.Cron {
	if e.workflowScheduler == nil {
		e.workflowScheduler = cron.New(cron.WithSeconds())
	}
	return e.workflowScheduler
}

func (e *dbosContext) GetApplicationVersion() string {
	return e.applicationVersion
}

func (e *dbosContext) GetSystemDB() SystemDatabase {
	return e.systemDB
}

func (e *dbosContext) GetExecutorID() string {
	return e.executorID
}

func (e *dbosContext) GetApplicationID() string {
	return e.applicationID
}

func (e *dbosContext) GetWorkflowWg() *sync.WaitGroup {
	return e.workflowsWg
}

func NewDBOSContext(inputConfig Config) (DBOSContext, error) {
	initExecutor := &dbosContext{
		workflowsWg: &sync.WaitGroup{},
		ctx:         context.Background(),
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

	return initExecutor, nil
}

func (e *dbosContext) Launch() error {
	// Start the system database
	e.systemDB.Launch(context.Background())

	// Start the admin server if configured
	if e.config.AdminServer {
		adminServer := newAdminServer(e, _DEFAULT_ADMIN_SERVER_PORT)
		err := adminServer.Start()
		if err != nil {
			logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		logger.Info("Admin server started", "port", _DEFAULT_ADMIN_SERVER_PORT)
		e.adminServer = adminServer
	}

	// Create context with cancel function for queue runner
	// XXX this can now be a cancel function on the executor itself?
	ctx, cancel := context.WithCancel(context.Background())
	e.queueRunnerCtx = ctx
	e.queueRunnerCancelFunc = cancel
	e.queueRunnerDone = make(chan struct{})

	// Start the queue runner in a goroutine
	go func() {
		defer close(e.queueRunnerDone)
		queueRunner(e)
	}()
	logger.Info("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if e.workflowScheduler != nil {
		e.workflowScheduler.Start()
		logger.Info("Workflow scheduler started")
	}

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(e, []string{e.executorID})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	}

	logger.Info("DBOS initialized", "app_version", e.applicationVersion, "executor_id", e.executorID)
	return nil
}

func (e *dbosContext) Shutdown() {
	// Wait for all workflows to finish
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

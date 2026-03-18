package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	_DEFAULT_ADMIN_SERVER_PORT = 3001
	_DEFAULT_SYSTEM_DB_SCHEMA  = "dbos"
	_DBOS_DOMAIN               = "cloud.dbos.dev"
)

// Config holds configuration parameters for initializing a DBOS context.
// DatabaseURL and AppName are required.
type Config struct {
	AppName            string        // Application name for identification (required)
	DatabaseURL        string        // DatabaseURL is a PostgreSQL connection string. Either this or SystemDBPool is required.
	SystemDBPool       *pgxpool.Pool // SystemDBPool is a custom System Database Pool. It's optional and takes precedence over DatabaseURL if both are provided.
	DatabaseSchema     string        // Database schema name (defaults to "dbos")
	Logger             *slog.Logger  // Custom logger instance (defaults to a new slog logger)
	AdminServer        bool          // Enable Transact admin HTTP server (disabled by default)
	AdminServerPort    int           // Port for the admin HTTP server (default: 3001)
	ConductorURL       string        // DBOS conductor service URL (optional)
	ConductorAPIKey    string        // DBOS conductor API key (optional)
	ApplicationVersion string        // Application version (optional, overridden by DBOS__APPVERSION env var)
	ExecutorID         string        // Executor ID (optional, overridden by DBOS__VMID env var)
	EnablePatching     bool          // Enable the patching system for Patch and DeprecatePatch (default: false)
}

func processConfig(inputConfig *Config) (*Config, error) {
	// First check required fields
	if len(inputConfig.DatabaseURL) == 0 && inputConfig.SystemDBPool == nil {
		return nil, fmt.Errorf("either databaseURL or systemDBPool must be provided")
	}
	if len(inputConfig.AppName) == 0 {
		return nil, fmt.Errorf("missing required config field: appName")
	}
	if inputConfig.AdminServerPort == 0 {
		inputConfig.AdminServerPort = _DEFAULT_ADMIN_SERVER_PORT
	}

	dbosConfig := &Config{
		DatabaseURL:        inputConfig.DatabaseURL,
		AppName:            inputConfig.AppName,
		DatabaseSchema:     inputConfig.DatabaseSchema,
		Logger:             inputConfig.Logger,
		AdminServer:        inputConfig.AdminServer,
		AdminServerPort:    inputConfig.AdminServerPort,
		ConductorURL:       inputConfig.ConductorURL,
		ConductorAPIKey:    inputConfig.ConductorAPIKey,
		ApplicationVersion: inputConfig.ApplicationVersion,
		ExecutorID:         inputConfig.ExecutorID,
		SystemDBPool:       inputConfig.SystemDBPool,
		EnablePatching:     inputConfig.EnablePatching,
	}

	// Load defaults
	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if dbosConfig.DatabaseSchema == "" {
		dbosConfig.DatabaseSchema = _DEFAULT_SYSTEM_DB_SCHEMA
	}

	// If patching is enabled and application version is not set, fix the application version
	if dbosConfig.EnablePatching && dbosConfig.ApplicationVersion == "" {
		dbosConfig.ApplicationVersion = "PATCHING_ENABLED"
	}

	// Override with environment variables if set
	if envAppVersion := os.Getenv("DBOS__APPVERSION"); envAppVersion != "" {
		dbosConfig.ApplicationVersion = envAppVersion
	}
	if envExecutorID := os.Getenv("DBOS__VMID"); envExecutorID != "" {
		dbosConfig.ExecutorID = envExecutorID
	}

	// Apply defaults for empty values
	if dbosConfig.ApplicationVersion == "" {
		dbosConfig.ApplicationVersion = computeApplicationVersion()
	}
	if dbosConfig.ExecutorID == "" {
		dbosConfig.ExecutorID = "local"
	}

	return dbosConfig, nil
}

// DBOSContext represents a DBOS execution context that provides workflow orchestration capabilities.
// It extends the standard Go context.Context and adds methods for running workflows and steps,
// inter-workflow communication, and state management.
//
// The context manages the lifecycle of workflows, provides durability guarantees, and enables
// recovery of interrupted workflows.
type DBOSContext interface {
	context.Context

	// Context Lifecycle
	Launch() error                  // Launch the DBOS runtime including system database, queues, and perform a workflow recovery for the local executor
	Shutdown(timeout time.Duration) // Gracefully shutdown all DBOS resources

	// Workflow operations
	RunAsStep(_ DBOSContext, fn StepFunc, opts ...StepOption) (any, error)                                      // Execute a function as a durable step within a workflow
	RunWorkflow(_ DBOSContext, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) // Start a new workflow execution
	Go(_ DBOSContext, fn StepFunc, opts ...StepOption) (chan StepOutcome[any], error)                           // Starts a step inside a Go routine and returns a channel to receive the result
	Select(_ DBOSContext, channels []<-chan StepOutcome[any]) (any, error)                                      // Performs a durable select over a slice of channels, checkpointing the selected channel and value
	Send(_ DBOSContext, destinationID string, message any, topic string) error                                  // Send a message to another workflow
	Recv(_ DBOSContext, topic string, timeout time.Duration) (any, error)                                       // Receive a message sent to this workflow
	SetEvent(_ DBOSContext, key string, message any) error                                                      // Set a key-value event for this workflow
	GetEvent(_ DBOSContext, targetWorkflowID string, key string, timeout time.Duration) (any, error)            // Get a key-value event from a target workflow
	WriteStream(_ DBOSContext, key string, value any) error                                                     // Write a value to a durable stream
	CloseStream(_ DBOSContext, key string) error                                                                // Close a durable stream
	ReadStream(_ DBOSContext, workflowID string, key string) ([]any, bool, error)                               // Read values from a durable stream (blocks until workflow inactive or stream closed)
	ReadStreamAsync(_ DBOSContext, workflowID string, key string) (<-chan StreamValue[any], error)              // Read values from a durable stream asynchronously
	Sleep(_ DBOSContext, duration time.Duration) (time.Duration, error)                                         // Durable sleep that survives workflow recovery
	Patch(_ DBOSContext, patchName string) (bool, error)                                                        // Check if workflow should use patched code
	DeprecatePatch(_ DBOSContext, patchName string) error                                                       // Deprecate a patch
	GetWorkflowID() (string, error)                                                                             // Get the current workflow ID (only available within workflows)
	GetStepID() (int, error)                                                                                    // Get the current step ID (only available within workflows)

	// Workflow management
	RetrieveWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error)                                // Get a handle to an existing workflow
	CancelWorkflow(_ DBOSContext, workflowID string) error                                                         // Cancel a workflow by setting its status to CANCELLED
	ResumeWorkflow(_ DBOSContext, workflowID string) (WorkflowHandle[any], error)                                  // Resume a cancelled workflow
	ForkWorkflow(_ DBOSContext, input ForkWorkflowInput) (WorkflowHandle[any], error)                              // Fork a workflow from a specific step
	ListWorkflows(_ DBOSContext, opts ...ListWorkflowsOption) ([]WorkflowStatus, error)                            // List workflows based on filtering criteria
	GetWorkflowSteps(_ DBOSContext, workflowID string) ([]StepInfo, error)                                         // Get the execution steps of a workflow
	ListRegisteredWorkflows(_ DBOSContext, opts ...ListRegisteredWorkflowsOption) ([]WorkflowRegistryEntry, error) // List registered workflows with filtering options
	ListRegisteredQueues(_ DBOSContext) ([]WorkflowQueue, error)                                                   // List all registered workflow queues

	// Accessors
	GetApplicationVersion() string // Get the application version for this context
	GetExecutorID() string         // Get the executor ID for this context
	GetApplicationID() string      // Get the application ID for this context

	// Context management
	From(_ DBOSContext, ctx context.Context) DBOSContext                                                        // Returns a copy of the current DBOSContext wrapping the provided context.Context
	WithoutCancel(_ DBOSContext) DBOSContext                                                                     // Returns a copy that is not canceled when the parent is canceled
	WithTimeout(_ DBOSContext, timeout time.Duration) (DBOSContext, context.CancelFunc)                            // Returns a copy that is canceled after the timeout

	// Queue configuration
	ListenQueues(_ DBOSContext, queues ...WorkflowQueue) // Configure which queues this process should listen to
}

type dbosContext struct {
	ctx           context.Context
	ctxCancelFunc context.CancelCauseFunc

	launched atomic.Bool

	systemDB    systemDatabase
	adminServer *adminServer
	config      *Config

	// Queue runner
	queueRunner *queueRunner

	// Conductor client
	conductor *conductor

	// Application metadata
	applicationVersion string
	applicationID      string
	executorID         string

	// Wait group for workflow goroutines
	workflowsWg *sync.WaitGroup

	// Workflow registry - read-mostly sync.Map since registration happens only before launch
	workflowRegistry        *sync.Map // map[string]WorkflowRegistryEntry
	workflowCustomNametoFQN *sync.Map // Maps fully qualified workflow names to custom names. Usefor when client enqueues a workflow by name because registry is indexed by FQN.

	// Set of workflow IDs currently running on this context (key = workflow ID, value = struct{}{})
	activeWorkflowIDs *sync.Map

	// Workflow scheduler
	workflowScheduler *cron.Cron

	// logger
	logger *slog.Logger
}

func (c *dbosContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *dbosContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *dbosContext) Err() error {
	return c.ctx.Err()
}

func (c *dbosContext) Value(key any) any {
	return c.ctx.Value(key)
}

// From returns a copy of the current DBOSContext with the underlying context.Context set to the provided ctx.
// The provided context must be a child of a context.Context that was provided by DBOS (e.g., the first argument of RunWorkflow or RunAsStep)
// That is because such context embeds important metadata necessary for DBOS to function correctly.
func (c *dbosContext) From(_ DBOSContext, ctx context.Context) DBOSContext {
	if ctx == nil {
		return nil
	}
	launched := c.launched.Load()
	childCtx := &dbosContext{
		ctx:                     ctx, // Use the provided context
		config:                  c.config,
		logger:                  c.logger,
		systemDB:                c.systemDB,
		workflowsWg:             c.workflowsWg,
		workflowRegistry:        c.workflowRegistry,
		workflowCustomNametoFQN: c.workflowCustomNametoFQN,
		activeWorkflowIDs:       c.activeWorkflowIDs,
		applicationVersion:      c.applicationVersion,
		executorID:              c.executorID,
		applicationID:           c.applicationID,
		queueRunner:             c.queueRunner,
	}
	childCtx.launched.Store(launched)
	return childCtx
}

func From(dbosCtx DBOSContext, ctx context.Context) DBOSContext {
	if dbosCtx == nil {
		return nil
	}
	return dbosCtx.From(dbosCtx, ctx)
}

// WithValue returns a copy of the DBOS context with the given key-value pair.
// This is similar to context.WithValue but maintains DBOS context capabilities.
// No-op if the provided context is not a concrete dbos.dbosContext.
func WithValue(ctx DBOSContext, key, val any) DBOSContext {
	if ctx == nil {
		return nil
	}
	// Will do nothing if the concrete type is not dbosContext
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		launched := dbosCtx.launched.Load()
		childCtx := &dbosContext{
			ctx:                     context.WithValue(dbosCtx.ctx, key, val), // Spawn a new child context with the value set
			config:                  dbosCtx.config,
			logger:                  dbosCtx.logger,
			systemDB:                dbosCtx.systemDB,
			workflowsWg:             dbosCtx.workflowsWg,
			workflowRegistry:        dbosCtx.workflowRegistry,
			workflowCustomNametoFQN: dbosCtx.workflowCustomNametoFQN,
			activeWorkflowIDs:       dbosCtx.activeWorkflowIDs,
			applicationVersion:      dbosCtx.applicationVersion,
			executorID:              dbosCtx.executorID,
			applicationID:           dbosCtx.applicationID,
			queueRunner:             dbosCtx.queueRunner,
		}
		childCtx.launched.Store(launched)
		return childCtx
	}
	return nil
}

func (c *dbosContext) WithoutCancel(_ DBOSContext) DBOSContext {
	launched := c.launched.Load()
	childCtx := &dbosContext{
		ctx:                     context.WithoutCancel(c.ctx),
		config:                  c.config,
		logger:                  c.logger,
		systemDB:                c.systemDB,
		workflowsWg:             c.workflowsWg,
		workflowRegistry:        c.workflowRegistry,
		workflowCustomNametoFQN: c.workflowCustomNametoFQN,
		activeWorkflowIDs:       c.activeWorkflowIDs,
		applicationVersion:      c.applicationVersion,
		executorID:              c.executorID,
		applicationID:           c.applicationID,
		queueRunner:             c.queueRunner,
	}
	childCtx.launched.Store(launched)
	return childCtx
}

// WithoutCancel returns a copy of the DBOS context that is not canceled when the parent context is canceled.
// This can be used to detach a child workflow.
func WithoutCancel(ctx DBOSContext) DBOSContext {
	if ctx == nil {
		return nil
	}
	return ctx.WithoutCancel(ctx)
}

// WithCancelCause returns a copy of the DBOS context that can be canceled with a cause.
// The returned context will be canceled when the returned CancelCauseFunc is called with a cause.
// No-op if the provided context is not a concrete dbos.dbosContext.
func WithCancelCause(ctx DBOSContext) (DBOSContext, context.CancelCauseFunc) {
	if ctx == nil {
		return nil, func(error) {}
	}
	if dbosCtx, ok := ctx.(*dbosContext); ok {
		launched := dbosCtx.launched.Load()
		newCtx, cancelCauseFunc := context.WithCancelCause(dbosCtx.ctx)
		childCtx := &dbosContext{
			ctx:                     newCtx,
			logger:                  dbosCtx.logger,
			systemDB:                dbosCtx.systemDB,
			workflowsWg:             dbosCtx.workflowsWg,
			workflowRegistry:        dbosCtx.workflowRegistry,
			workflowCustomNametoFQN: dbosCtx.workflowCustomNametoFQN,
			activeWorkflowIDs:       dbosCtx.activeWorkflowIDs,
			applicationVersion:      dbosCtx.applicationVersion,
			executorID:              dbosCtx.executorID,
			applicationID:           dbosCtx.applicationID,
			queueRunner:             dbosCtx.queueRunner,
		}
		childCtx.launched.Store(launched)
		return childCtx, cancelCauseFunc
	}
	return nil, func(error) {}
}

func (c *dbosContext) WithTimeout(_ DBOSContext, timeout time.Duration) (DBOSContext, context.CancelFunc) {
	launched := c.launched.Load()
	newCtx, cancelFunc := context.WithTimeoutCause(c.ctx, timeout, errors.New("DBOS context timeout"))
	childCtx := &dbosContext{
		ctx:                     newCtx,
		config:                  c.config,
		logger:                  c.logger,
		systemDB:                c.systemDB,
		workflowsWg:             c.workflowsWg,
		workflowRegistry:        c.workflowRegistry,
		workflowCustomNametoFQN: c.workflowCustomNametoFQN,
		activeWorkflowIDs:       c.activeWorkflowIDs,
		applicationVersion:      c.applicationVersion,
		executorID:              c.executorID,
		applicationID:           c.applicationID,
		queueRunner:             c.queueRunner,
	}
	childCtx.launched.Store(launched)
	return childCtx, cancelFunc
}

// WithTimeout returns a copy of the DBOS context that is canceled after the given timeout.
func WithTimeout(ctx DBOSContext, timeout time.Duration) (DBOSContext, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return ctx.WithTimeout(ctx, timeout)
}

func (c *dbosContext) getWorkflowScheduler() *cron.Cron {
	if c.workflowScheduler == nil {
		c.workflowScheduler = cron.New(cron.WithSeconds())
	}
	return c.workflowScheduler
}

func (c *dbosContext) GetApplicationVersion() string {
	return c.applicationVersion
}

func (c *dbosContext) GetExecutorID() string {
	return c.executorID
}

func (c *dbosContext) GetApplicationID() string {
	return c.applicationID
}

// ListRegisteredQueues returns all registered workflow queues.
func (c *dbosContext) ListRegisteredQueues(_ DBOSContext) ([]WorkflowQueue, error) {
	if c.queueRunner == nil {
		return []WorkflowQueue{}, nil
	}
	return c.queueRunner.listQueues(), nil
}

// ListRegisteredWorkflows returns information about registered workflows with their registration parameters.
// Supports filtering using functional options.
func (c *dbosContext) ListRegisteredWorkflows(_ DBOSContext, opts ...ListRegisteredWorkflowsOption) ([]WorkflowRegistryEntry, error) {
	// Initialize parameters with defaults
	params := &listRegisteredWorkflowsOptions{}

	// Apply all provided options
	for _, opt := range opts {
		opt(params)
	}

	// Get all registered workflows and apply filters
	var filteredWorkflows []WorkflowRegistryEntry
	c.workflowRegistry.Range(func(key, value any) bool {
		workflow := value.(WorkflowRegistryEntry)

		// Filter by scheduled only
		if params.scheduledOnly && workflow.CronSchedule == "" {
			return true
		}

		filteredWorkflows = append(filteredWorkflows, workflow)
		return true
	})

	return filteredWorkflows, nil
}

// NewDBOSContext creates a new DBOS context with the provided configuration.
// The context must be launched with Launch() for workflow execution and should be shut down with Shutdown().
// This function initializes the DBOS system database, sets up the queue sub-system, and prepares the workflow registry.
//
// Example:
//
//	config := dbos.Config{
//	    DatabaseURL: "postgres://user:pass@localhost:5432/dbname",
//	    AppName:     "my-app",
//	}
//	ctx, err := dbos.NewDBOSContext(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer ctx.Shutdown(30*time.Second)
//
//	if err := ctx.Launch(); err != nil {
//	    log.Fatal(err)
//	}
func NewDBOSContext(ctx context.Context, inputConfig Config) (DBOSContext, error) {
	dbosBaseCtx, cancelFunc := context.WithCancelCause(ctx)
	initExecutor := &dbosContext{
		workflowsWg:             &sync.WaitGroup{},
		ctx:                     dbosBaseCtx,
		ctxCancelFunc:           cancelFunc,
		workflowRegistry:        &sync.Map{},
		workflowCustomNametoFQN: &sync.Map{},
		activeWorkflowIDs:       &sync.Map{},
	}

	// Load and process the configuration
	config, err := processConfig(&inputConfig)
	if err != nil {
		return nil, newInitializationError(err.Error())
	}
	initExecutor.config = config

	// Set global logger
	initExecutor.logger = config.Logger
	initExecutor.logger.Info("Initializing DBOS context", "app_name", config.AppName, "dbos_version", getDBOSVersion())

	// Initialize global variables from processed config (already handles env vars and defaults)
	initExecutor.applicationVersion = config.ApplicationVersion
	initExecutor.executorID = config.ExecutorID

	initExecutor.applicationID = os.Getenv("DBOS__APPID")

	newSystemDatabaseInputs := newSystemDatabaseInput{
		databaseURL:     config.DatabaseURL,
		databaseSchema:  config.DatabaseSchema,
		customPool:      config.SystemDBPool,
		logger:          initExecutor.logger,
		applicationName: config.AppName,
	}

	// Create the system database
	systemDB, err := newSystemDatabase(initExecutor, newSystemDatabaseInputs)
	if err != nil {
		initExecutor.logger.Error("failed to create system database", "error", err)
		return nil, newInitializationError(err.Error())
	}
	initExecutor.systemDB = systemDB
	initExecutor.logger.Debug("System database initialized")

	// Initialize the queue runner and register DBOS internal queue
	initExecutor.queueRunner = newQueueRunner(initExecutor.logger)
	NewWorkflowQueue(initExecutor, _DBOS_INTERNAL_QUEUE_NAME)

	// Register the any,any internal debouncer workflow so it's always available for execution
	// This allows a client to debounce workflow and the server side to run them, even without knowing the actual workflow types
	RegisterWorkflow(initExecutor, internalDebouncerWF[any, any])

	// Initialize conductor if API key is provided
	if config.ConductorAPIKey != "" {
		initExecutor.executorID = uuid.NewString()
		if config.ConductorURL == "" {
			dbosDomain := os.Getenv("DBOS_DOMAIN")
			if dbosDomain == "" {
				dbosDomain = _DBOS_DOMAIN
			}
			config.ConductorURL = fmt.Sprintf("wss://%s/conductor/v1alpha1", dbosDomain)
		}
		conductorConfig := conductorConfig{
			url:     config.ConductorURL,
			apiKey:  config.ConductorAPIKey,
			appName: config.AppName,
		}
		conductor, err := newConductor(initExecutor, conductorConfig)
		if err != nil {
			return nil, newInitializationError(fmt.Sprintf("failed to initialize conductor: %v", err))
		}
		initExecutor.conductor = conductor
		initExecutor.logger.Debug("Conductor initialized")
	}

	return initExecutor, nil
}

// Launch initializes and starts the DBOS runtime components including the system database,
// admin server (if enabled), queue runner, workflow scheduler, and performs recovery
// of any pending workflows on this executor.
//
// Returns an error if the context is already launched or if any component fails to start.
func (c *dbosContext) Launch() error {
	if c.launched.Load() {
		return newInitializationError("DBOS is already launched")
	}

	// Start the system database
	c.systemDB.launch(c)

	// Start the admin server if enabled
	if c.config.AdminServer {
		adminServer := newAdminServer(c, c.config.AdminServerPort)
		err := adminServer.Start()
		if err != nil {
			c.logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		c.logger.Debug("Admin server started", "port", c.config.AdminServerPort)
		c.adminServer = adminServer
	}

	// Start the queue runner in a goroutine
	go func() {
		c.queueRunner.run(c)
	}()
	c.logger.Debug("Queue runner started")

	// Start the workflow scheduler if it has been initialized
	if c.workflowScheduler != nil {
		c.workflowScheduler.Start()
		c.logger.Debug("Workflow scheduler started")
	}

	// Start the conductor if it has been initialized
	if c.conductor != nil {
		c.conductor.launch()
		c.logger.Debug("Conductor started")
	}

	// Run a round of recovery on the local executor
	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorID})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		c.logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	} else {
		c.logger.Debug("No pending workflows to recover")
	}

	c.logger.Info("DBOS launched", "app_version", c.applicationVersion, "executor_id", c.executorID)
	c.launched.Store(true)
	return nil
}

// Shutdown gracefully shuts down the DBOS runtime by performing a complete, ordered cleanup
// of all system components. The shutdown sequence includes:
//
// 1. Calls Cancel to stop workflows and cancel the context
// 2. Waits for the queue runner to complete processing
// 3. Stops the workflow scheduler and waits for scheduled jobs to finish
// 4. Shuts down the system database connection pool and notification listener
// 5. Shuts down conductor
// 6. Shuts down the admin server
// 7. Marks the context as not launched
//
// Each step respects the provided timeout. If any component doesn't shut down within the timeout,
// a warning is logged and the shutdown continues to the next component.
//
// Shutdown is a permanent operation and should be called when the application is terminating.
func (c *dbosContext) Shutdown(timeout time.Duration) {
	c.logger.Debug("Shutting down DBOS context")

	// Cancel the context to signal all resources to stop
	c.ctxCancelFunc(errors.New("DBOS cancellation initiated"))

	// Wait for all workflows to finish
	c.logger.Debug("Waiting for all workflows to finish")
	done := make(chan struct{})
	go func() {
		c.workflowsWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		c.logger.Debug("All workflows completed")
	case <-time.After(timeout):
		c.logger.Warn("Timeout waiting for workflows to complete", "timeout", timeout)
	}

	// Wait for queue runner to finish
	if c.queueRunner != nil && c.launched.Load() {
		c.logger.Debug("Waiting for queue runner to complete")
		select {
		case <-c.queueRunner.completionChan:
			c.logger.Debug("Queue runner completed")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for queue runner to complete", "timeout", timeout)
		}
	}

	// Stop the workflow scheduler and wait until all scheduled workflows are done
	if c.workflowScheduler != nil && c.launched.Load() {
		c.logger.Debug("Stopping workflow scheduler")
		ctx := c.workflowScheduler.Stop()

		select {
		case <-ctx.Done():
			c.logger.Debug("All scheduled jobs completed")
			c.workflowScheduler = nil
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for jobs to complete. Moving on", "timeout", timeout)
		}
	}

	// Shutdown the conductor
	if c.conductor != nil {
		c.logger.Debug("Shutting down conductor")
		c.conductor.shutdown(timeout)
	}

	// Shutdown the admin server
	if c.adminServer != nil && c.launched.Load() {
		c.logger.Debug("Shutting down admin server")
		err := c.adminServer.Shutdown(timeout)
		if err != nil {
			c.logger.Error("Failed to shutdown admin server", "error", err)
		} else {
			c.logger.Debug("Admin server shutdown complete")
		}
	}

	// Close the system database
	if c.systemDB != nil {
		c.logger.Debug("Shutting down system database")
		c.systemDB.shutdown(c, timeout)
	}

	c.launched.Store(false)
}

// getBinaryHash computes and returns the SHA-256 hash of the current executable.
// This is used for application versioning to ensure workflow compatibility across deployments.
// Returns the hexadecimal representation of the hash or an error if the executable cannot be read.
func getBinaryHash() (string, error) {
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return "", fmt.Errorf("resolve self path: %w", err)
	}

	fi, err := os.Lstat(execPath)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("executable is not a regular file")
	}

	file, err := os.Open(execPath) // #nosec G304 -- opening our own executable, not user-supplied
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
	hash, err := getBinaryHash()
	if err != nil {
		fmt.Printf("DBOS: Failed to compute binary hash: %v\n", err)
		return ""
	}
	return hash
}

// getDBOSVersion returns the version of the DBOS module
func getDBOSVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/dbos-inc/dbos-transact-golang" {
				return dep.Version
			}
		}
		// If running as main module, return main module version
		if info.Main.Path == "github.com/dbos-inc/dbos-transact-golang" {
			return info.Main.Version
		}
	}
	return "unknown"
}

// Launch launches the DBOS runtime using the provided DBOSContext.
// This is a package-level wrapper for the DBOSContext.Launch() method.
//
// Example:
//
//	ctx, err := dbos.NewDBOSContext(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err := dbos.Launch(ctx); err != nil {
//	    log.Fatal(err)
//	}
func Launch(ctx DBOSContext) error {
	if ctx == nil {
		return fmt.Errorf("ctx cannot be nil")
	}
	return ctx.Launch()
}

// Shutdown gracefully shuts down the DBOS runtime using the provided DBOSContext and timeout.
// This is a package-level wrapper for the DBOSContext.Shutdown() method.
//
// Example:
//
//	ctx, err := dbos.NewDBOSContext(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer dbos.Shutdown(ctx, 30*time.Second)
func Shutdown(ctx DBOSContext, timeout time.Duration) {
	if ctx == nil {
		return
	}
	ctx.Shutdown(timeout)
}

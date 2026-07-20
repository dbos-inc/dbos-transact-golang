package dbos

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	_DEFAULT_ADMIN_SERVER_PORT         = 3001
	_DEFAULT_SYSTEM_DB_SCHEMA          = "dbos"
	_DEFAULT_SYSTEM_DB_STARTUP_TIMEOUT = 2 * time.Minute
	_DBOS_DOMAIN                       = "cloud.dbos.dev"
	_LAUNCH_ROLLBACK_TIMEOUT           = 30 * time.Second
)

// Config holds configuration parameters for initializing a DBOS context.
// AppName is required, along with exactly one of DatabaseURL, SystemDBPool,
// or SQLiteSystemDB.
type Config struct {
	AppName string // Application name for identification (required)
	// DatabaseURL is the system-database connection string. Accepts Postgres URLs or
	// key=value DSNs (e.g. postgres://user:pass@host:5432/dbname; CockroachDB uses the
	// same form) and sqlite URLs (sqlite:/path/to.db, sqlite:relative.db, or
	// sqlite::memory:). Exactly one of DatabaseURL, SystemDBPool, or SQLiteSystemDB must be set.
	DatabaseURL               string
	SystemDBPool              *pgxpool.Pool   // SystemDBPool is a custom pg/CRDB pool. Optional; takes precedence over DatabaseURL. Mutually exclusive with SQLiteSystemDB.
	SQLiteSystemDB            *sql.DB         // SQLiteSystemDB is a custom sqlite handle (e.g. from modernc.org/sqlite). Optional; takes precedence over DatabaseURL. Mutually exclusive with SystemDBPool.
	DatabaseSchema            string          // Database schema name (defaults to "dbos")
	Logger                    *slog.Logger    // Custom logger instance (defaults to a new slog logger)
	AdminServer               bool            // Enable Transact admin HTTP server (disabled by default)
	AdminServerPort           int             // Port for the admin HTTP server (default: 3001)
	ConductorURL              string          // DBOS conductor service URL (optional)
	ConductorAPIKey           string          // DBOS conductor API key (optional)
	ConductorExecutorMetadata map[string]any  // Metadata associated with this executor that may be used to identify it on the Conductor dashboard. Must be JSON-serializable.
	ApplicationVersion        string          // Application version (optional, overridden by DBOS__APPVERSION env var)
	ExecutorID                string          // Executor ID (optional, overridden by DBOS__VMID env var)
	EnablePatching            bool            // Enable the patching system for Patch and DeprecatePatch (default: false)
	Serializer                Serializer[any] // Custom serializer for encoding/decoding workflow inputs, outputs, and events (defaults to JSON serializer)
	SchedulerPollingInterval  time.Duration   // controls how often dynamic schedules are reconciled with the database (defaults to 30 seconds)
	SystemDBStartupTimeout    time.Duration   // Maximum time for system-database connection and migrations (defaults to 2 minutes)
}

func processConfig(inputConfig *Config) (*Config, error) {
	// First check required fields
	if len(inputConfig.DatabaseURL) == 0 && inputConfig.SystemDBPool == nil && inputConfig.SQLiteSystemDB == nil {
		return nil, fmt.Errorf("one of databaseURL, systemDBPool, or sqliteSystemDB must be provided")
	}
	if inputConfig.SystemDBPool != nil && inputConfig.SQLiteSystemDB != nil {
		return nil, fmt.Errorf("systemDBPool and sqliteSystemDB are mutually exclusive")
	}
	if len(inputConfig.AppName) == 0 {
		return nil, fmt.Errorf("missing required config field: appName")
	}
	if inputConfig.SystemDBPool == nil && inputConfig.SQLiteSystemDB == nil {
		if _, err := sysdb.DetectDialect(inputConfig.DatabaseURL); err != nil {
			return nil, err
		}
	}
	if inputConfig.AdminServerPort == 0 {
		inputConfig.AdminServerPort = _DEFAULT_ADMIN_SERVER_PORT
	}
	if inputConfig.SystemDBStartupTimeout < 0 {
		return nil, fmt.Errorf("systemDBStartupTimeout cannot be negative")
	}

	dbosConfig := &Config{
		DatabaseURL:               inputConfig.DatabaseURL,
		AppName:                   inputConfig.AppName,
		DatabaseSchema:            inputConfig.DatabaseSchema,
		Logger:                    inputConfig.Logger,
		AdminServer:               inputConfig.AdminServer,
		AdminServerPort:           inputConfig.AdminServerPort,
		ConductorURL:              inputConfig.ConductorURL,
		ConductorAPIKey:           inputConfig.ConductorAPIKey,
		ConductorExecutorMetadata: inputConfig.ConductorExecutorMetadata,
		ApplicationVersion:        inputConfig.ApplicationVersion,
		ExecutorID:                inputConfig.ExecutorID,
		SystemDBPool:              inputConfig.SystemDBPool,
		SQLiteSystemDB:            inputConfig.SQLiteSystemDB,
		EnablePatching:            inputConfig.EnablePatching,
		Serializer:                inputConfig.Serializer,
		SchedulerPollingInterval:  inputConfig.SchedulerPollingInterval,
		SystemDBStartupTimeout:    inputConfig.SystemDBStartupTimeout,
	}

	if dbosConfig.ConductorExecutorMetadata != nil {
		if _, err := json.Marshal(dbosConfig.ConductorExecutorMetadata); err != nil {
			return nil, fmt.Errorf("conductorExecutorMetadata must be JSON-serializable: %w", err)
		}
	}

	// Load defaults
	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if dbosConfig.DatabaseSchema == "" {
		dbosConfig.DatabaseSchema = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	if dbosConfig.SystemDBStartupTimeout == 0 {
		dbosConfig.SystemDBStartupTimeout = _DEFAULT_SYSTEM_DB_STARTUP_TIMEOUT
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

// AlertHandler is a function that handles alerts received from DBOS Conductor.
type AlertHandler = models.AlertHandler

// Client is the subset of a Context that works without Launch: it needs
// only a connection to the system database — established by NewClient — and
// none of the launched runtime resources (queue runner, scheduler, conductor,
// workflow recovery)
//
// It provides a programmatic way to interact with your DBOS application from
// external code: enqueueing workflows, workflow management, queue management,
// schedule management, and application version management. Every Context
// is a Client, so a launched Context can be passed anywhere a Client is
// accepted.
//
// Create a standalone Client with NewClient. Use the
// package-level functions (dbos.Enqueue, dbos.ListWorkflows, ...) to get
// compile-time type checking.
type Client interface {
	context.Context

	// Workflow operations
	Enqueue(_ Client, queueName string, workflowName string, input any, opts ...EnqueueOption) (WorkflowHandle[any], error) // Enqueue a workflow by name to a named queue
	Send(_ Client, destinationID string, message any, topic string, opts ...SendOption) error                               // Send a message to a workflow
	GetEvent(_ Client, targetWorkflowID string, key string, timeout time.Duration) (any, error)                             // Get a key-value event from a target workflow. Returns an opaque envelope; call dbos.GetEvent[R] instead to receive the decoded value
	ReadStream(_ Client, workflowID string, key string, opts ...ReadStreamOption) ([]any, bool, error)                      // Read values from a durable stream (blocks until workflow inactive or stream closed)
	ReadStreamAsync(_ Client, workflowID string, key string) (<-chan StreamValue[any], error)                               // Read values from a durable stream asynchronously

	// Workflow management
	RetrieveWorkflow(_ Client, workflowID string) (WorkflowHandle[any], error)                                   // Get a handle to an existing workflow
	CancelWorkflow(_ Client, workflowID string, opts ...CancelWorkflowOption) error                              // Cancel a workflow by setting its status to CANCELLED
	CancelWorkflows(_ Client, workflowIDs []string, opts ...CancelWorkflowOption) error                          // Cancel multiple workflows in a single DB round-trip
	SetWorkflowAttributes(_ Client, workflowID string, attributes map[string]any) error                          // Replace the custom attributes on an existing workflow (nil clears them)
	SetWorkflowDelay(_ Client, workflowID string, opts ...SetWorkflowDelayOption) error                          // Set or update the delay on a DELAYED workflow
	ResumeWorkflow(_ Client, workflowID string, opts ...ResumeWorkflowOption) (WorkflowHandle[any], error)       // Resume a cancelled workflow
	ResumeWorkflows(_ Client, workflowIDs []string, opts ...ResumeWorkflowOption) ([]WorkflowHandle[any], error) // Resume multiple workflows in a single DB round-trip
	ForkWorkflow(_ Client, input ForkWorkflowInput) (WorkflowHandle[any], error)                                 // Fork a workflow from a specific step
	ForkWorkflows(_ Client, input ForkWorkflowsInput) ([]WorkflowHandle[any], error)                             // Fork multiple workflows in a single DB round-trip
	ListWorkflows(_ Client, opts ...ListWorkflowsOption) ([]WorkflowStatus, error)                               // List workflows based on filtering criteria
	GetWorkflowSteps(_ Client, workflowID string, opts ...GetWorkflowStepsOption) ([]StepInfo, error)            // Get the execution steps of a workflow
	GetWorkflowAggregates(_ Client, input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error)            // Aggregate counts of workflows by one or more grouping columns
	GetStepAggregates(_ Client, input GetStepAggregatesInput) ([]StepAggregateRow, error)                        // Aggregate counts/durations of steps by function name and/or status
	DeleteWorkflows(_ Client, workflowIDs []string, opts ...DeleteWorkflowOption) error                          // Delete workflows and all their associated data

	// Queue management
	RegisterQueue(_ Client, name string, options ...QueueOption) (Queue, error) // Register and persist a database-backed queue
	RetrieveQueue(_ Client, name string) (Queue, error)                         // Retrieve a database-backed queue by name (ErrQueueNotFound if absent)
	ListQueues(_ Client) ([]Queue, error)                                       // List all database-backed queues
	DeleteQueue(_ Client, name string) error                                    // Delete a database-backed queue

	// Schedule management
	CreateSchedule(_ Client, spec ScheduleSpec) error                                       // Create a new schedule
	ApplySchedules(_ Client, schedules []ScheduleSpec) error                                // Apply schedules (create or update)
	PauseSchedule(_ Client, scheduleName string) error                                      // Pause a schedule
	ResumeSchedule(_ Client, scheduleName string) error                                     // Resume a paused schedule
	DeleteSchedule(_ Client, scheduleName string) error                                     // Delete a schedule
	GetSchedule(_ Client, scheduleName string) (WorkflowSchedule, error)                    // Get a schedule by name (ErrScheduleNotFound if absent)
	ListSchedules(_ Client, opts ...ListSchedulesOption) ([]WorkflowSchedule, error)        // List schedules with optional filters
	BackfillSchedule(_ Client, scheduleName string, start, end time.Time) ([]string, error) // Backfill a schedule, returning the IDs of the enqueued workflows
	TriggerSchedule(_ Client, scheduleName string) (WorkflowHandle[any], error)             // Trigger a schedule immediately, returning a handle to the enqueued workflow

	// Application version management
	ListApplicationVersions(_ Client) ([]VersionInfo, error)        // List all registered application versions, newest first
	GetLatestApplicationVersion(_ Client) (VersionInfo, error)      // Get the latest registered application version
	SetLatestApplicationVersion(_ Client, versionName string) error // Mark the named version as latest by bumping its timestamp to now

	Shutdown(_ Client, timeout time.Duration) error // Gracefully shutdown all DBOS resources; returns an error if the timeout expired before they all stopped
}

// Context represents a DBOS execution context that provides workflow orchestration capabilities.
// It extends the standard Go context.Context and adds methods for running workflows and steps,
// inter-workflow communication, and state management.
//
// The context manages the lifecycle of workflows, provides durability guarantees, and enables
// recovery of interrupted workflows.
type Context interface {
	Client

	// Context Lifecycle
	Launch() error // Launch the DBOS runtime (system database, queues, scheduler) and recover this executor's PENDING workflows: interrupted workflows resume from their last completed step; queued ones are returned to their queue

	// Workflow operations
	RunAsStep(_ Context, fn StepFunc, opts ...StepOption) (any, error)                                      // Execute a function as a durable step within a workflow
	RunAsTransaction(_ Context, ds *DataSource, fn TxnFunc, opts ...StepOption) (any, error)                // Execute a function as a durable transaction against a registered data source
	RunWorkflow(_ Context, fn WorkflowFunc, input any, opts ...WorkflowOption) (WorkflowHandle[any], error) // Start a new workflow execution
	Go(_ Context, fn StepFunc, opts ...StepOption) (<-chan StepOutcome[any], error)                         // Starts a step inside a Go routine and returns a channel to receive the result
	Select(_ Context, channels []<-chan StepOutcome[any]) (any, error)                                      // Performs a durable select over a slice of channels, checkpointing the selected channel and value
	Recv(_ Context, topic string, timeout time.Duration) (any, error)                                       // Receive a message sent to this workflow
	SetEvent(_ Context, key string, message any, opts ...SetEventOption) error                              // Set a key-value event for this workflow
	WriteStream(_ Context, key string, value any, opts ...WriteStreamOption) error                          // Write a value to a durable stream
	CloseStream(_ Context, key string) error                                                                // Close a durable stream
	Sleep(_ Context, duration time.Duration) (time.Duration, error)                                         // Durable sleep that survives workflow recovery
	Patch(_ Context, patchName string) (bool, error)                                                        // Check if workflow should use patched code
	DeprecatePatch(_ Context, patchName string) error                                                       // Deprecate a patch
	GetWorkflowID() (string, error)                                                                         // Get the current workflow ID (only available within workflows)
	GetStepID() (int, error)                                                                                // Get the current step ID (only available within workflows)

	// Registration
	ListRegisteredWorkflows(_ Context) []WorkflowRegistryEntry // List workflows registered in this process
	ListenQueues(_ Context, names ...string)                   // Replace the set of queues this process listens to (each call replaces the whole set)
	ListenedQueues(_ Context) []string                         // Get the current listen set (empty means every queue)

	// Accessors
	GetApplicationVersion() string // Get the application version for this context
	GetExecutorID() string         // Get the executor ID for this context
	GetApplicationID() string      // Get the application ID for this context

	// Context management
	From(_ Context, ctx context.Context) Context                                // Returns a copy of the current Context wrapping the provided context.Context
	WithoutCancel(_ Context) Context                                            // Returns a copy that is not canceled when the parent is canceled
	WithTimeout(_ Context, timeout time.Duration) (Context, context.CancelFunc) // Returns a copy that is canceled after the timeout
	WithValue(key, val any) Context                                             // Returns a copy of the DBOS context with the given key-value pair
	WithCancel() (Context, context.CancelFunc)                                  // Returns a copy that can be manually canceled
	WithCancelCause() (Context, context.CancelCauseFunc)                        // Returns a copy of the DBOS context that can be canceled with a cause

	// Alert handling
	SetAlertHandler(handler AlertHandler) // Register a handler for alerts from DBOS Conductor (must be called before Launch)
}

type dbosContext struct {
	ctx           context.Context
	ctxCancelFunc context.CancelCauseFunc

	launched atomic.Bool
	// Launch and shutdown are permanent, one-shot lifecycle transitions.
	launchStarted   atomic.Bool
	shutdownStarted atomic.Bool

	systemDB    sysdb.SystemDatabase
	adminServer *adminServer
	config      *Config

	// Queue runner
	queueRunner        *queueRunner
	queueRunnerStarted atomic.Bool

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

	// Set of workflow IDs currently running on this context (key = workflow ID, value = activeWorkflowEntry)
	activeWorkflowIDs *sync.Map

	// Workflow scheduler
	workflowScheduler        *cron.Cron
	workflowSchedulerStarted atomic.Bool
	scheduleReconcilerWg     sync.WaitGroup

	scheduleMu sync.Mutex
	// Schedule entry ID mapping (scheduleName -> cron.EntryID)
	scheduleEntryIDs map[string]cron.EntryID
	// Definition signature of each installed cron entry (scheduleName -> sig).
	// Used by the reconciler to detect definition changes and reinstall the entry.
	scheduleInstalledSignatures map[string]scheduleSignature

	// logger
	logger *slog.Logger

	serializer Serializer[any]

	// Alert handler
	alertHandler AlertHandler
}

// SetAlertHandler registers a handler function for alerts received from DBOS Conductor.
// Must be called before Launch(). Only one handler is allowed per context.
func (c *dbosContext) SetAlertHandler(handler AlertHandler) {
	if handler == nil {
		panic("alert handler cannot be nil")
	}
	if c.launched.Load() {
		panic("cannot set alert handler after Launch()")
	}
	if c.alertHandler != nil {
		panic("alert handler is already registered")
	}
	c.alertHandler = handler
}

// SetAlertHandler registers a handler function for alerts received from DBOS Conductor.
// Must be called before Launch(). Only one handler is allowed per context.
func SetAlertHandler(ctx Context, handler AlertHandler) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	ctx.SetAlertHandler(handler)
}

// ClearRegistries clears the workflow registry,
// allowing re-registration of workflows. Intended for testing only.
func (c *dbosContext) ClearRegistries() {
	c.workflowRegistry.Clear()
	c.workflowCustomNametoFQN.Clear()
	c.alertHandler = nil
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

// clone returns a copy of the DBOS context with the underlying context.Context replaced by ctx.
// Root-only lifecycle fields (cancel func, admin server, conductor, scheduler state, alert handler)
// are deliberately not propagated to derived contexts.
func (c *dbosContext) clone(ctx context.Context) *dbosContext {
	childCtx := &dbosContext{
		ctx:                     ctx,
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
		serializer:              c.serializer,
	}
	childCtx.launched.Store(c.launched.Load())
	return childCtx
}

func (c *dbosContext) From(_ Context, ctx context.Context) Context {
	if ctx == nil {
		return nil
	}
	return c.clone(ctx)
}

// From returns a copy of dbosCtx whose embedded context.Context is replaced by ctx.
//
// The returned Context takes its Deadline, cancellation (Done/Err), and Values
// entirely from ctx; dbosCtx contributes only the DBOS runtime state (system
// database, registries, configuration, logger). ctx must descend from a
// context.Context provided by DBOS (e.g., the first argument of RunWorkflow or
// RunAsStep), because DBOS metadata such as the current workflow state travels
// in context values.
//
// From returns nil if dbosCtx or ctx is nil.
func From(dbosCtx Context, ctx context.Context) Context {
	if dbosCtx == nil {
		return nil
	}
	return dbosCtx.From(dbosCtx, ctx)
}

// WithValue returns a copy of the DBOS context with the given key-value pair.
// This is similar to context.WithValue but maintains DBOS context capabilities.
func WithValue(ctx Context, key, val any) Context {
	if ctx == nil {
		return nil
	}
	return ctx.WithValue(key, val)
}

func (c *dbosContext) WithValue(key, val any) Context {
	return c.clone(context.WithValue(c.ctx, key, val))
}

func (c *dbosContext) WithoutCancel(_ Context) Context {
	return c.clone(context.WithoutCancel(c.ctx))
}

// WithoutCancel returns a copy of the DBOS context that is not canceled when the parent context is canceled.
// This can be used to detach a child workflow.
func WithoutCancel(ctx Context) Context {
	if ctx == nil {
		return nil
	}
	return ctx.WithoutCancel(ctx)
}

func (c *dbosContext) WithCancel() (Context, context.CancelFunc) {
	newCtx, cancelFunc := context.WithCancel(c.ctx)
	return c.clone(newCtx), cancelFunc
}

// WithCancel returns a copy of the DBOS context that can be manually canceled.
// The returned CancelFunc must be called when the derived context is no longer needed,
// to release resources associated with it.
func WithCancel(ctx Context) (Context, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return ctx.WithCancel()
}

func (c *dbosContext) WithCancelCause() (Context, context.CancelCauseFunc) {
	newCtx, cancelCauseFunc := context.WithCancelCause(c.ctx)
	return c.clone(newCtx), cancelCauseFunc
}

// WithCancelCause returns a copy of the DBOS context that can be canceled with a cause.
// The returned context will be canceled when the returned CancelCauseFunc is called with a cause.
func WithCancelCause(ctx Context) (Context, context.CancelCauseFunc) {
	if ctx == nil {
		return nil, func(error) {}
	}
	return ctx.WithCancelCause()
}

func (c *dbosContext) WithTimeout(_ Context, timeout time.Duration) (Context, context.CancelFunc) {
	newCtx, cancelFunc := context.WithTimeoutCause(c.ctx, timeout, errors.New("DBOS context timeout"))
	return c.clone(newCtx), cancelFunc
}

// WithTimeout returns a copy of the DBOS context that is canceled after the given timeout.
func WithTimeout(ctx Context, timeout time.Duration) (Context, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return ctx.WithTimeout(ctx, timeout)
}

func (c *dbosContext) getWorkflowScheduler() *cron.Cron {
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

// GetApplicationVersion is the package-level wrapper for Context.GetApplicationVersion.
// It returns the application version of the provided context, or an empty string if ctx is nil.
func GetApplicationVersion(ctx Context) string {
	if ctx == nil {
		return ""
	}
	return ctx.GetApplicationVersion()
}

// GetExecutorID is the package-level wrapper for Context.GetExecutorID.
// It returns the executor ID of the provided context, or an empty string if ctx is nil.
func GetExecutorID(ctx Context) string {
	if ctx == nil {
		return ""
	}
	return ctx.GetExecutorID()
}

// GetApplicationID is the package-level wrapper for Context.GetApplicationID.
// It returns the application ID of the provided context, or an empty string if ctx is nil.
func GetApplicationID(ctx Context) string {
	if ctx == nil {
		return ""
	}
	return ctx.GetApplicationID()
}

// ListRegisteredWorkflows returns information about registered workflows with their registration parameters.
func (c *dbosContext) ListRegisteredWorkflows(_ Context) []WorkflowRegistryEntry {
	var workflows []WorkflowRegistryEntry
	c.workflowRegistry.Range(func(key, value any) bool {
		workflows = append(workflows, value.(WorkflowRegistryEntry))
		return true
	})
	return workflows
}

// NewContext creates a new DBOS context with the provided configuration.
// The context must be launched with Launch() for workflow execution and should be shut down with Shutdown().
// This function initializes the DBOS system database, sets up the queue sub-system, and prepares the workflow registry.
//
// Example:
//
//	config := dbos.Config{
//	    DatabaseURL: "postgres://user:pass@localhost:5432/dbname",
//	    AppName:     "my-app",
//	}
//	ctx, err := dbos.NewContext(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer dbos.Shutdown(ctx, 30*time.Second)
//
//	if err := ctx.Launch(); err != nil {
//	    log.Fatal(err)
//	}
func NewContext(ctx context.Context, inputConfig Config) (Context, error) {
	dbosBaseCtx, cancelFunc := context.WithCancelCause(ctx)
	initExecutor := &dbosContext{
		workflowsWg:                 &sync.WaitGroup{},
		ctx:                         dbosBaseCtx,
		ctxCancelFunc:               cancelFunc,
		workflowRegistry:            &sync.Map{},
		workflowCustomNametoFQN:     &sync.Map{},
		activeWorkflowIDs:           &sync.Map{},
		workflowScheduler:           cron.New(cron.WithSeconds()),
		scheduleEntryIDs:            make(map[string]cron.EntryID),
		scheduleInstalledSignatures: make(map[string]scheduleSignature),
	}

	// Load and process the configuration
	config, err := processConfig(&inputConfig)
	if err != nil {
		return nil, models.NewInitializationError(err.Error())
	}
	initExecutor.config = config

	// Set global logger
	initExecutor.logger = config.Logger
	initExecutor.logger.Info("Initializing DBOS context", "app_name", config.AppName, "dbos_version", getDBOSVersion())

	// Initialize global variables from processed config (already handles env vars and defaults)
	initExecutor.applicationVersion = config.ApplicationVersion
	initExecutor.executorID = config.ExecutorID

	initExecutor.applicationID = os.Getenv("DBOS__APPID")
	initExecutor.serializer = config.Serializer

	newSystemDatabaseInputs := sysdb.NewSystemDatabaseInput{
		DatabaseURL:     config.DatabaseURL,
		DatabaseSchema:  config.DatabaseSchema,
		CustomPool:      config.SystemDBPool,
		CustomSqliteDB:  config.SQLiteSystemDB,
		Logger:          initExecutor.logger,
		ApplicationName: config.AppName,
		EncodeScheduledInput: func(ctx context.Context, scheduledTime time.Time, scheduleContext json.RawMessage) (*string, string, error) {
			ser := resolveEncoder(ctx)
			encoded, err := ser.Encode(ScheduledWorkflowInput{
				ScheduledTime: scheduledTime,
				Context:       scheduleContext,
			})
			return encoded, ser.Name(), err
		},
	}

	// Create the system database within a bounded startup window. This covers
	// pool acquisition, database creation, migrations, and the final ping.
	startupCtx, cancelStartup := context.WithTimeout(initExecutor, config.SystemDBStartupTimeout)
	defer cancelStartup()
	newSystemDatabaseInputs.StartupTimeout = config.SystemDBStartupTimeout
	systemDB, err := sysdb.NewSystemDatabase(startupCtx, newSystemDatabaseInputs)
	if err != nil {
		initExecutor.logger.Error("failed to create system database", "error", err)
		return nil, models.NewInitializationError(err.Error())
	}
	initExecutor.systemDB = systemDB
	initExecutor.logger.Debug("System database initialized")

	// Initialize the queue runner (which owns the DBOS internal queue)
	initExecutor.queueRunner = newQueueRunner(initExecutor.logger)

	// Register the any,any internal debouncer workflow so it's always available for execution
	// This allows a client to debounce workflow and the server side to run them, even without knowing the actual workflow types
	RegisterWorkflow(initExecutor, internalDebouncerWF[any, any])

	// Initialize conductor. In DBOS Cloud, connect to Conductor for observability
	// using the cloud-provided environment variables. Otherwise, connect if a
	// Conductor API key was configured.
	var conductorCfg *conductorConfig
	if os.Getenv("DBOS__CLOUD") == "true" {
		cloudAppName := os.Getenv("DBOS__CONDUCTOR_APP_NAME")
		cloudConductorKey := os.Getenv("DBOS__CONDUCTOR_KEY")
		cloudConductorURL := os.Getenv("DBOS__CONDUCTOR_URL")
		if cloudAppName != "" && cloudConductorKey != "" && cloudConductorURL != "" {
			conductorCfg = &conductorConfig{
				url:              cloudConductorURL,
				apiKey:           cloudConductorKey,
				appName:          cloudAppName,
				executorMetadata: config.ConductorExecutorMetadata,
			}
		}
	} else if config.ConductorAPIKey != "" {
		initExecutor.executorID = uuid.NewString()
		if config.ConductorURL == "" {
			dbosDomain := os.Getenv("DBOS_DOMAIN")
			if dbosDomain == "" {
				dbosDomain = _DBOS_DOMAIN
			}
			config.ConductorURL = fmt.Sprintf("wss://%s/conductor/v1alpha1", dbosDomain)
		}
		conductorCfg = &conductorConfig{
			url:              config.ConductorURL,
			apiKey:           config.ConductorAPIKey,
			appName:          config.AppName,
			executorMetadata: config.ConductorExecutorMetadata,
		}
	}

	if conductorCfg != nil {
		conductor, err := newConductor(initExecutor, *conductorCfg)
		if err != nil {
			return nil, models.NewInitializationError(fmt.Sprintf("failed to initialize conductor: %v", err))
		}
		initExecutor.conductor = conductor
		initExecutor.logger.Debug("Conductor initialized")
	}

	return initExecutor, nil
}

// ClientConfig holds configuration parameters for NewClient. Exactly one of
// DatabaseURL, SystemDBPool, or SQLiteSystemDB must be set.
type ClientConfig struct {
	// DatabaseURL is the system-database connection string: a Postgres URL or key=value
	// DSN, or a sqlite URL (sqlite:/path/to.db, sqlite:relative.db, or sqlite::memory:).
	// Exactly one of DatabaseURL, SystemDBPool, or SQLiteSystemDB must be set.
	DatabaseURL            string
	SystemDBPool           *pgxpool.Pool   // SystemDBPool is a custom pg/CRDB pool. Optional; takes precedence over DatabaseURL. Mutually exclusive with SQLiteSystemDB.
	SQLiteSystemDB         *sql.DB         // SQLiteSystemDB is a custom sqlite handle (e.g. from modernc.org/sqlite). Optional; takes precedence over DatabaseURL. Mutually exclusive with SystemDBPool.
	DatabaseSchema         string          // Database schema name (defaults to "dbos")
	Logger                 *slog.Logger    // Optional custom logger
	Serializer             Serializer[any] // Optional custom serializer (defaults to JSON)
	SystemDBStartupTimeout time.Duration   // Maximum time for system-database connection and migrations (defaults to 2 minutes)
}

// NewClient creates a new DBOS client with the provided configuration.
// It connects to the system database and starts its notification listener —
// or a poller on backends without listen/notify support — so every Client
// operation, including blocking ones like GetEvent, works without launching
// the DBOS runtime.
//
// Example:
//
//	config := dbos.ClientConfig{
//	    DatabaseURL: "postgres://user:pass@localhost:5432/dbname",
//	}
//	client, err := dbos.NewClient(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewClient(ctx context.Context, config ClientConfig) (Client, error) {
	dbosCtx, err := NewContext(ctx, Config{
		DatabaseURL:            config.DatabaseURL,
		DatabaseSchema:         config.DatabaseSchema,
		AppName:                "dbos-client",
		Logger:                 config.Logger,
		SystemDBPool:           config.SystemDBPool,
		SQLiteSystemDB:         config.SQLiteSystemDB,
		Serializer:             config.Serializer,
		SystemDBStartupTimeout: config.SystemDBStartupTimeout,
	})
	if err != nil {
		return nil, err
	}

	asDBOSCtx, ok := dbosCtx.(*dbosContext)
	if ok {
		asDBOSCtx.systemDB.Launch(asDBOSCtx)
	}

	return dbosCtx, nil
}

// Launch initializes and starts the DBOS runtime components including the system database
// and admin server (if enabled), recovers any pending workflows on this executor, then
// starts the queue runner and workflow scheduler.
//
// Returns an error if the context is already launched or if any component fails to start.
// A failed Launch is terminal: the context is torn down (system database closed) and
// cannot be relaunched — create a new context with NewContext and Launch that instead.
func (c *dbosContext) Launch() error {
	if !c.launchStarted.CompareAndSwap(false, true) {
		return models.NewInitializationError("DBOS is already launched")
	}
	launchCompleted := false
	defer func() {
		if !launchCompleted {
			if err := c.Shutdown(c, _LAUNCH_ROLLBACK_TIMEOUT); err != nil {
				c.logger.Error("Failed to roll back launch", "error", err)
			}
		}
	}()

	// Start the system database
	c.systemDB.Launch(c)

	// Register the current application version and warn if it is not the latest.
	if err := sysdb.Retry(c, func() error {
		return c.systemDB.CreateApplicationVersion(c, c.applicationVersion)
	}, sysdb.WithRetrierLogger(c.logger)); err != nil {
		c.logger.Warn("Failed to register application version", "version", c.applicationVersion, "error", err)
	} else if latest, err := sysdb.RetryWithResult(c, func() (*VersionInfo, error) {
		return c.systemDB.GetLatestApplicationVersion(c, nil)
	}, sysdb.WithRetrierLogger(c.logger)); err != nil {
		c.logger.Warn("Failed to fetch latest application version", "error", err)
	} else if latest.Name != c.applicationVersion {
		c.logger.Warn("Current application version is not the latest",
			"current", c.applicationVersion, "latest", latest.Name)
	}

	// Start the admin server if enabled
	if c.config.AdminServer {
		c.adminServer = newAdminServer(c, c.config.AdminServerPort)
		err := c.adminServer.Start()
		if err != nil {
			c.logger.Error("Failed to start admin server", "error", err)
			return models.NewInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		c.logger.Debug("Admin server started", "port", c.config.AdminServerPort)
	}

	// Recover local pending workflows before starting the queue runner so
	// recovered workflows are not racing a fresh dequeue pass.
	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorID})
	if err != nil {
		return models.NewInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		c.logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	} else {
		c.logger.Debug("No pending workflows to recover")
	}

	// Start the queue runner in a goroutine
	go func() {
		c.queueRunner.run(c)
	}()
	c.queueRunnerStarted.Store(true)
	c.logger.Debug("Queue runner started")

	// Start the cron scheduler.
	c.getWorkflowScheduler().Start()
	c.workflowSchedulerStarted.Store(true)
	c.logger.Debug("Workflow scheduler started")

	// Start the dynamic schedule reconciler. It polls the schedules table every
	// _SCHEDULE_POLL_INTERVAL and reconciles cron entries against DB state.
	c.scheduleReconcilerWg.Add(1)
	go func() {
		defer c.scheduleReconcilerWg.Done()
		c.runScheduleReconciler()
	}()

	// Start the conductor if it has been initialized
	if c.conductor != nil {
		c.conductor.launch()
		c.logger.Debug("Conductor started")
	}

	c.logger.Info("DBOS launched", "app_version", c.applicationVersion, "executor_id", c.executorID)
	c.launched.Store(true)
	launchCompleted = true
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
// Shutdown is a permanent, one-shot operation and should be called once, when the
// application is terminating: only the first call performs the shutdown and reports
// its result; subsequent calls return nil immediately without waiting for it.
func (c *dbosContext) Shutdown(_ Client, timeout time.Duration) error {
	if !c.shutdownStarted.CompareAndSwap(false, true) {
		return nil
	}
	c.logger.Debug("Shutting down DBOS context")

	// Resources still running when their timeout expired.
	var pending []string

	// Cancel the context to signal all resources to stop
	c.ctxCancelFunc(errors.New("DBOS cancellation initiated"))

	// Stop workflow producers before draining in-flight workflows. Producers
	// (.e.g, queue runner) call RunWorkflow, which calls workflowsWg.Add(1);
	// waiting on the WaitGroup before they finish races with those Adds.
	reconcilerDone := make(chan struct{})
	go func() {
		c.scheduleReconcilerWg.Wait()
		close(reconcilerDone)
	}()
	select {
	case <-reconcilerDone:
		c.logger.Debug("Schedule reconciler completed")
	case <-time.After(timeout):
		c.logger.Warn("Timeout waiting for schedule reconciler to complete", "timeout", timeout)
		pending = append(pending, "schedule reconciler")
	}

	// Wait for queue runner to finish
	if c.queueRunner != nil && c.queueRunnerStarted.Load() {
		c.logger.Debug("Waiting for queue runner to complete")
		select {
		case <-c.queueRunner.completionChan:
			c.logger.Debug("Queue runner completed")
			c.queueRunnerStarted.Store(false)
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for queue runner to complete", "timeout", timeout)
			pending = append(pending, "queue runner")
		}
	}

	// Stop the workflow scheduler and wait until all scheduled workflows are done
	if c.workflowScheduler != nil && c.workflowSchedulerStarted.Load() {
		c.logger.Debug("Stopping workflow scheduler")
		ctx := c.workflowScheduler.Stop()
		c.workflowSchedulerStarted.Store(false)

		select {
		case <-ctx.Done():
			c.logger.Debug("All scheduled jobs completed")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for jobs to complete. Moving on", "timeout", timeout)
			pending = append(pending, "workflow scheduler")
		}
	}

	// Shutdown the conductor
	if c.conductor != nil {
		c.logger.Debug("Shutting down conductor")
		if err := c.conductor.shutdown(timeout); err != nil {
			pending = append(pending, "conductor")
		}
	}

	// Shutdown the admin server
	if c.adminServer != nil {
		c.logger.Debug("Shutting down admin server")
		err := c.adminServer.Shutdown(timeout)
		if err != nil {
			c.logger.Error("Failed to shutdown admin server", "error", err)
			pending = append(pending, "admin server")
		} else {
			c.logger.Debug("Admin server shutdown complete")
		}
	}

	// Now that all producers are stopped, wait for in-flight workflows to finish
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
		pending = append(pending, "workflows")
	}

	// Close the system database
	if c.systemDB != nil {
		c.logger.Debug("Shutting down system database")
		for _, p := range c.systemDB.Shutdown(c, timeout) {
			pending = append(pending, "system database "+p)
		}
	}

	c.launched.Store(false)

	if len(pending) > 0 {
		return fmt.Errorf("shutdown timed out after %v waiting for: %s", timeout, strings.Join(pending, ", "))
	}
	return nil
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

// Launch launches the DBOS runtime using the provided Context.
// This is a package-level wrapper for the Context.Launch() method.
//
// Example:
//
//	ctx, err := dbos.NewContext(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	if err := dbos.Launch(ctx); err != nil {
//	    log.Fatal(err)
//	}
func Launch(ctx Context) error {
	if ctx == nil {
		return fmt.Errorf("ctx cannot be nil")
	}
	return ctx.Launch()
}

// Shutdown gracefully shuts down all DBOS resources using the provided Client
// or Context and timeout.
// This is a package-level wrapper for the Client.Shutdown() method.
//
// Example:
//
//	ctx, err := dbos.NewContext(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer dbos.Shutdown(ctx, 30*time.Second)
//
// It returns an error if the timeout expired before all dependent resources
// (queue runner, workflow scheduler, in-flight workflows, ...) stopped; those
// resources may still be running when it returns.
func Shutdown(c Client, timeout time.Duration) error {
	if c == nil {
		return nil
	}
	return c.Shutdown(c, timeout)
}

// clearRegistries clears the workflow registry,
// allowing re-registration of workflows. Intended for testing only.
func clearRegistries(ctx Context) {
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}
	c.ClearRegistries()
}

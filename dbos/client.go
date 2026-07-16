package dbos

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ClientConfig struct {
	DatabaseURL    string          // DatabaseURL is the system-database connection string. Exactly one of DatabaseURL, SystemDBPool, or SqliteSystemDB must be set.
	SystemDBPool   *pgxpool.Pool   // SystemDBPool is a custom pg/CRDB pool. Optional; takes precedence over DatabaseURL. Mutually exclusive with SqliteSystemDB.
	SqliteSystemDB *sql.DB         // SqliteSystemDB is a custom sqlite handle (e.g. from modernc.org/sqlite). Optional; takes precedence over DatabaseURL. Mutually exclusive with SystemDBPool.
	DatabaseSchema string          // Database schema name (defaults to "dbos")
	Logger         *slog.Logger    // Optional custom logger
	Serializer     Serializer[any] // Optional custom serializer (defaults to JSON)
}

// Client is the subset of a DBOSContext that works without Launch: it needs
// only a connection to the system database — established by NewClient — and
// none of the launched runtime resources (queue runner, scheduler, conductor,
// workflow recovery)
//
// It provides a programmatic way to interact with your DBOS application from
// external code: enqueueing workflows, workflow management, queue management,
// schedule management, and application version management. Every DBOSContext
// is a Client, so a launched DBOSContext can be passed anywhere a Client is
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
	GetEvent(_ Client, targetWorkflowID string, key string, timeout time.Duration) (any, error)                             // Get a key-value event from a target workflow
	ReadStream(_ Client, workflowID string, key string, opts ...ReadStreamOption) ([]any, bool, error)                      // Read values from a durable stream (blocks until workflow inactive or stream closed)
	ReadStreamAsync(_ Client, workflowID string, key string) (<-chan StreamValue[any], error)                               // Read values from a durable stream asynchronously

	// Workflow management
	RetrieveWorkflow(_ Client, workflowID string) (WorkflowHandle[any], error)                                   // Get a handle to an existing workflow
	CancelWorkflow(_ Client, workflowID string, opts ...CancelWorkflowOptions) error                             // Cancel a workflow by setting its status to CANCELLED
	CancelWorkflows(_ Client, workflowIDs []string, opts ...CancelWorkflowOptions) error                         // Cancel multiple workflows in a single DB round-trip
	UpdateWorkflowAttributes(_ Client, workflowID string, attributes map[string]any) error                       // Replace the custom attributes on an existing workflow (nil clears them)
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
	RetrieveQueue(_ Client, name string) (Queue, error)                         // Retrieve a database-backed queue by name (nil if absent)
	ListQueues(_ Client) ([]Queue, error)                                       // List all database-backed queues
	DeleteQueue(_ Client, name string) error                                    // Delete a database-backed queue

	// Schedule management
	CreateSchedule(_ Client, spec ScheduleSpec) error                                       // Create a new schedule
	ApplySchedules(_ Client, schedules []ScheduleSpec) error                                // Apply schedules (create or update)
	PauseSchedule(_ Client, scheduleName string) error                                      // Pause a schedule
	ResumeSchedule(_ Client, scheduleName string) error                                     // Resume a paused schedule
	DeleteSchedule(_ Client, scheduleName string) error                                     // Delete a schedule
	GetSchedule(_ Client, scheduleName string) (*WorkflowSchedule, error)                   // Get a schedule by name
	ListSchedules(_ Client, opts ...ListSchedulesOption) ([]WorkflowSchedule, error)        // List schedules with optional filters
	BackfillSchedule(_ Client, scheduleName string, start, end time.Time) ([]string, error) // Backfill a schedule, returning the IDs of the enqueued workflows
	TriggerSchedule(_ Client, scheduleName string) (WorkflowHandle[any], error)             // Trigger a schedule immediately, returning a handle to the enqueued workflow

	// Application version management
	ListApplicationVersions(_ Client) ([]VersionInfo, error)        // List all registered application versions, newest first
	GetLatestApplicationVersion(_ Client) (*VersionInfo, error)     // Get the latest registered application version
	SetLatestApplicationVersion(_ Client, versionName string) error // Mark the named version as latest by bumping its timestamp to now

	Shutdown(timeout time.Duration) // Gracefully shutdown all DBOS resources
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
	dbosCtx, err := NewDBOSContext(ctx, Config{
		DatabaseURL:    config.DatabaseURL,
		DatabaseSchema: config.DatabaseSchema,
		AppName:        "dbos-client",
		Logger:         config.Logger,
		SystemDBPool:   config.SystemDBPool,
		SqliteSystemDB: config.SqliteSystemDB,
		Serializer:     config.Serializer,
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

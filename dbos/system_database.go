package dbos

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*******************************/
/******* INTERFACE ********/
/*******************************/

type systemDatabase interface {
	// SysDB management
	launch(ctx context.Context)
	shutdown(ctx context.Context, timeout time.Duration)
	resetSystemDB(ctx context.Context) error

	// Workflows
	insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error)
	listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error)
	updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error
	awaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*string, error)
	cancelWorkflow(ctx context.Context, input cancelWorkflowDBInput) error
	cancelAllBefore(ctx context.Context, cutoffTime time.Time) error
	resumeWorkflow(ctx context.Context, input resumeWorkflowDBInput) error
	forkWorkflow(ctx context.Context, input forkWorkflowDBInput) (string, error)

	// Child workflows
	recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error
	checkChildWorkflow(ctx context.Context, workflowUUID string, functionID int) (*string, error)
	recordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error

	// Steps
	recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error)
	getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error)

	// Communication (special steps)
	send(ctx context.Context, input WorkflowSendInput) error
	recv(ctx context.Context, input recvInput) (*string, error)
	setEvent(ctx context.Context, input WorkflowSetEventInput) error
	getEvent(ctx context.Context, input getEventInput) (*string, error)

	// Streams
	writeStream(ctx context.Context, input writeStreamDBInput) error
	readStream(ctx context.Context, input readStreamDBInput) ([]streamEntry, bool, error)

	// Timers (special steps)
	sleep(ctx context.Context, input sleepInput) (time.Duration, error)

	// Patches
	patch(ctx context.Context, input patchDBInput) (bool, error)
	doesPatchExists(ctx context.Context, input patchDBInput) (string, error)

	// Queues
	dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error)
	clearQueueAssignment(ctx context.Context, workflowID string) (bool, error)
	getQueuePartitions(ctx context.Context, queueName string) ([]string, error)

	// Garbage collection
	garbageCollectWorkflows(ctx context.Context, input garbageCollectWorkflowsInput) error

	// Metrics
	getMetrics(ctx context.Context, startTime string, endTime string) ([]metricData, error)
}

type sysDB struct {
	pool                          *pgxpool.Pool
	notificationLoopDone          chan struct{}
	workflowNotificationsMap      *sync.Map
	workflowNotificationRepollMap *sync.Map
	workflowEventsMap             *sync.Map
	workflowEventsRepollMap       *sync.Map
	logger                        *slog.Logger
	schema                        string
	launched                      bool
	isCockroachDB                 bool
}

/*******************************/
/******* INITIALIZATION ********/
/*******************************/

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	// Get the database name from the pool config
	poolConfig := pool.Config()
	dbName := poolConfig.ConnConfig.Database
	if dbName == "" {
		return errors.New("database name not found in pool configuration")
	}

	// Create a connection to the postgres database to create the target database
	serverConfig := poolConfig.ConnConfig.Copy()
	serverConfig.Database = "postgres"
	conn, err := pgx.ConnectConfig(ctx, serverConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %v", err)
	}
	defer conn.Close(ctx)

	// Create the system database if it doesn't exist
	var exists bool
	err = conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if database exists: %v", err)
	}
	if !exists {
		createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(ctx, createSQL)
		if err != nil {
			return fmt.Errorf("failed to create database %s: %v", dbName, err)
		}
		logger.Debug("Database created", "name", dbName)
	}

	return nil
}

//go:embed migrations/1_initial_dbos_schema.sql
var migration1SQL string

//go:embed migrations/1_initial_dbos_schema_listen_notify.sql
var migration1ListenNotifySQL string

//go:embed migrations/2_add_queue_partition_key.sql
var migration2SQL string

//go:embed migrations/3_add_workflow_status_index.sql
var migration3SQL string

//go:embed migrations/4_add_forked_from.sql
var migration4SQL string

//go:embed migrations/5_add_step_timestamps.sql
var migration5SQL string

//go:embed migrations/6_add_workflow_events_history.sql
var migration6SQL string

//go:embed migrations/7_add_owner_xid.sql
var migration7SQL string

//go:embed migrations/8_add_parent_workflow_id.sql
var migration8SQL string

type migrationFile struct {
	version int64
	sql     string
}

const (
	_DBOS_MIGRATION_TABLE = "dbos_migrations"

	// PostgreSQL error codes
	_PG_ERROR_UNIQUE_VIOLATION      = "23505"
	_PG_ERROR_FOREIGN_KEY_VIOLATION = "23503"

	// Notification channels
	_DBOS_NOTIFICATIONS_CHANNEL   = "dbos_notifications_channel"
	_DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel"

	// Stream sentinel value for closure
	_DBOS_STREAM_CLOSED_SENTINEL = "__DBOS_STREAM_CLOSED__"

	// Database retry timeouts
	_DB_CONNECTION_RETRY_BASE_DELAY  = 1 * time.Second
	_DB_CONNECTION_RETRY_FACTOR      = 2
	_DB_CONNECTION_RETRY_MAX_RETRIES = 10
	_DB_CONNECTION_MAX_DELAY         = 120 * time.Second
	_DB_RETRY_INTERVAL               = 1 * time.Second
)

func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, isCockroach bool) error {

	// Process the migration SQL with fmt.Sprintf
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	migration1SQLProcessed := fmt.Sprintf(migration1SQL,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)

	// If not CockroachDB, merge the listen/notify triggers with the main migration
	if !isCockroach {
		migration1ListenNotifySQLProcessed := fmt.Sprintf(migration1ListenNotifySQL,
			sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
		migration1SQLProcessed = migration1SQLProcessed + "\n" + migration1ListenNotifySQLProcessed
	}

	migration2SQLProcessed := fmt.Sprintf(migration2SQL, sanitizedSchema)

	migration3SQLProcessed := fmt.Sprintf(migration3SQL, sanitizedSchema)

	migration4SQLProcessed := fmt.Sprintf(migration4SQL, sanitizedSchema, sanitizedSchema)

	migration5SQLProcessed := fmt.Sprintf(migration5SQL, sanitizedSchema)

	migration6SQLProcessed := fmt.Sprintf(migration6SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)

	migration7SQLProcessed := fmt.Sprintf(migration7SQL, sanitizedSchema)

	migration8SQLProcessed := fmt.Sprintf(migration8SQL, sanitizedSchema, sanitizedSchema)

	// Build migrations list with processed SQL
	migrations := []migrationFile{
		{version: 1, sql: migration1SQLProcessed},
		{version: 2, sql: migration2SQLProcessed},
		{version: 3, sql: migration3SQLProcessed},
		{version: 4, sql: migration4SQLProcessed},
		{version: 5, sql: migration5SQLProcessed},
		{version: 6, sql: migration6SQLProcessed},
		{version: 7, sql: migration7SQLProcessed},
		{version: 8, sql: migration8SQLProcessed},
	}

	// Begin transaction for atomic migration execution
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// Check if the schema exists
	var schemaExists bool
	checkSchemaQuery := `SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`
	err = tx.QueryRow(ctx, checkSchemaQuery, schema).Scan(&schemaExists)
	if err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %v", schema, err)
	}

	// Create the schema if it doesn't exist
	if !schemaExists {
		createSchemaQuery := fmt.Sprintf("CREATE SCHEMA %s", pgx.Identifier{schema}.Sanitize())
		_, err = tx.Exec(ctx, createSchemaQuery)
		if err != nil {
			return fmt.Errorf("failed to create schema %s: %v", schema, err)
		}
	}

	// Create the migrations table if it doesn't exist
	checkMigrationTableExistsQuery := `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`

	var migrationTableExists bool
	err = tx.QueryRow(ctx, checkMigrationTableExistsQuery, schema, _DBOS_MIGRATION_TABLE).Scan(&migrationTableExists)
	if err != nil {
		return fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !migrationTableExists {
		createTableQuery := fmt.Sprintf(`CREATE TABLE %s.%s (version BIGINT NOT NULL PRIMARY KEY)`, pgx.Identifier{schema}.Sanitize(), _DBOS_MIGRATION_TABLE)
		_, err = tx.Exec(ctx, createTableQuery)
		if err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}

	// Get current migration version
	var currentVersion int64 = 0
	query := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", pgx.Identifier{schema}.Sanitize(), _DBOS_MIGRATION_TABLE)
	err = tx.QueryRow(ctx, query).Scan(&currentVersion)
	if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to get current migration version: %v", err)
	}

	// Apply migrations starting from the next version
	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}

		// Execute the migration SQL
		_, err = tx.Exec(ctx, migration.sql)
		if err != nil {
			return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
		}

		// Update the migration version
		if currentVersion == 0 {
			// Insert first migration record
			insertQuery := fmt.Sprintf("INSERT INTO %s.%s (version) VALUES ($1)", pgx.Identifier{schema}.Sanitize(), _DBOS_MIGRATION_TABLE)
			_, err = tx.Exec(ctx, insertQuery, migration.version)
		} else {
			// Update existing migration record
			updateQuery := fmt.Sprintf("UPDATE %s.%s SET version = $1", pgx.Identifier{schema}.Sanitize(), _DBOS_MIGRATION_TABLE)
			_, err = tx.Exec(ctx, updateQuery, migration.version)
		}
		if err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", migration.version, err)
		}

		currentVersion = migration.version
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration transaction: %v", err)
	}

	return nil
}

type newSystemDatabaseInput struct {
	databaseURL     string
	databaseSchema  string
	customPool      *pgxpool.Pool
	logger          *slog.Logger
	applicationName string
}

// New creates a new SystemDatabase instance and runs migrations
func newSystemDatabase(ctx context.Context, inputs newSystemDatabaseInput) (systemDatabase, error) {
	// Dereference fields from inputs
	databaseURL := inputs.databaseURL
	databaseSchema := inputs.databaseSchema
	customPool := inputs.customPool
	logger := inputs.logger

	// Validate that schema is provided
	if databaseSchema == "" {
		return nil, fmt.Errorf("database schema cannot be empty")
	}

	// Configure a connection pool
	var pool *pgxpool.Pool
	if customPool != nil {
		logger.Info("Using custom database connection pool")
		// Verify the pool is valid
		poolConn, err := customPool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to validate custom pool: %v", err)
		}
		defer poolConn.Release()
		err = poolConn.Ping(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to validate custom pool: %v", err)
		}
		pool = customPool
	} else {
		// Parse the connection string to get a config
		config, err := pgxpool.ParseConfig(databaseURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database URL: %v", err)
		}

		// Set pool configuration
		config.MaxConns = 20
		config.MinConns = 0
		config.MaxConnLifetime = time.Hour
		config.MaxConnIdleTime = time.Minute * 5

		// Add acquire timeout to prevent indefinite blocking
		config.ConnConfig.ConnectTimeout = 10 * time.Second

		// Set application_name parameter if provided
		if inputs.applicationName != "" {
			if config.ConnConfig.RuntimeParams == nil {
				config.ConnConfig.RuntimeParams = make(map[string]string)
			}
			config.ConnConfig.RuntimeParams["application_name"] = inputs.applicationName
		}

		// Create pool with configuration
		newPool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pool: %v", err)
		}
		pool = newPool
	}

	// Displaying Masked Database URL
	maskedDatabaseURL, err := maskPassword(pool.Config().ConnString())
	if err != nil {
		logger.Error("Failed to parse database URL", "error", err)
		return nil, fmt.Errorf("failed to parse database URL: %v", err)
	}
	logger.Info("Connecting to system database", "database_url", maskedDatabaseURL, "schema", databaseSchema)

	if customPool == nil {
		// Create the database if it doesn't exist
		if err := retry(ctx, func() error {
			return createDatabaseIfNotExists(ctx, pool, logger)
		}, withRetrierLogger(logger)); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to create database: %v", err)
		}
	}

	// Detect if we're running CockroachDB
	// This must happen after we ensured the database exist
	conn, err := pool.Acquire(ctx)
	if err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to acquire connection to detect database type: %v", err)
	}
	defer conn.Release()
	isCockroach := isCockroachDB(ctx, conn.Conn())
	if isCockroach {
		logger.Info("Detected CockroachDB")
	}

	// Run migrations
	if err := retry(ctx, func() error {
		return runMigrations(ctx, pool, databaseSchema, isCockroach)
	}, withRetrierLogger(logger)); err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to run migrations: %v", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	// Create a map of notification payloads to channels
	workflowNotificationsMap := &sync.Map{}
	workflowNotificationRepollMap := &sync.Map{}
	workflowEventsMap := &sync.Map{}
	workflowEventsRepollMap := &sync.Map{}

	return &sysDB{
		pool:                          pool,
		workflowNotificationsMap:      workflowNotificationsMap,
		workflowNotificationRepollMap: workflowNotificationRepollMap,
		workflowEventsMap:             workflowEventsMap,
		workflowEventsRepollMap:       workflowEventsRepollMap,
		notificationLoopDone:          make(chan struct{}),
		logger:                        logger.With("service", "system_database"),
		schema:                        databaseSchema,
		isCockroachDB:                 isCockroach,
	}, nil
}

func (s *sysDB) launch(ctx context.Context) {
	// Start the appropriate notification loop based on database type
	if s.isCockroachDB {
		// Start the polling-based notification poller loop (CockroachDB)
		go s.notificationPollerLoop(ctx)
	} else {
		// Otherwise start the LISTEN/NOTIFY-based notification listener loop
		go s.notificationListenerLoop(ctx)
	}
	s.launched = true
}

func (s *sysDB) shutdown(ctx context.Context, timeout time.Duration) {
	s.logger.Debug("Closing system database connection pool")

	if s.launched {
		// Wait for the notification loop to exit
		// The context should be cancelled prior to calling shutdown
		select {
		case <-s.notificationLoopDone:
		case <-time.After(timeout):
			s.logger.Warn("Notification listener loop did not finish in time", "timeout", timeout)
		}
	}

	if s.pool != nil {
		poolClose := make(chan struct{})
		go func() {
			// Will block until every acquired connection is released
			s.pool.Close()
			close(poolClose)
		}()
		select {
		case <-poolClose:
		case <-time.After(timeout):
			s.logger.Warn("System database connection pool did not close in time", "timeout", timeout)
		}
	}

	s.workflowNotificationsMap.Clear()
	s.workflowEventsMap.Clear()

	s.launched = false
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type insertWorkflowResult struct {
	attempts         int
	status           WorkflowStatusType
	name             string
	queueName        *string
	timeout          time.Duration
	workflowDeadline time.Time
	ownerXID         string
}

type insertWorkflowStatusDBInput struct {
	status            WorkflowStatus
	maxRetries        int
	tx                pgx.Tx
	ownerXID          *string
	incrementAttempts bool
}

func (s *sysDB) insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error) {
	if input.tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

	// Set default values
	attempts := 1
	if input.status.Status == WorkflowStatusEnqueued {
		attempts = 0
	}

	updatedAt := time.Now()
	if !input.status.UpdatedAt.IsZero() {
		updatedAt = input.status.UpdatedAt
	}

	var deadline *int64 = nil
	if !input.status.Deadline.IsZero() {
		millis := input.status.Deadline.UnixMilli()
		deadline = &millis
	}

	var timeoutMs *int64 = nil
	if input.status.Timeout > 0 {
		millis := input.status.Timeout.Round(time.Millisecond).Milliseconds()
		timeoutMs = &millis
	}

	// Our DB works with NULL values
	var applicationVersion *string
	if len(input.status.ApplicationVersion) > 0 {
		applicationVersion = &input.status.ApplicationVersion
	}

	var deduplicationID *string
	if len(input.status.DeduplicationID) > 0 {
		deduplicationID = &input.status.DeduplicationID
	}

	var queuePartitionKey *string
	if len(input.status.QueuePartitionKey) > 0 {
		queuePartitionKey = &input.status.QueuePartitionKey
	}

	var parentWorkflowID *string
	if len(input.status.ParentWorkflowID) > 0 {
		parentWorkflowID = &input.status.ParentWorkflowID
	}

	query := fmt.Sprintf(`INSERT INTO %s.workflow_status (
        workflow_uuid,
        status,
        name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        executor_id,
        application_version,
        application_id,
        created_at,
        recovery_attempts,
        updated_at,
        workflow_timeout_ms,
        workflow_deadline_epoch_ms,
        inputs,
        deduplication_id,
        priority,
        queue_partition_key,
        owner_xid,
        parent_workflow_id
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
			recovery_attempts = CASE
                WHEN EXCLUDED.status != $22 THEN workflow_status.recovery_attempts + $24
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status = $23 THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid`, pgx.Identifier{s.schema}.Sanitize())

	var result insertWorkflowResult
	var timeoutMSResult *int64
	var workflowDeadlineEpochMS *int64
	var ownerXIDReturn *string

	// Marshal authenticated roles (slice of strings) to JSON for TEXT column
	authenticatedRoles, err := json.Marshal(input.status.AuthenticatedRoles)

	if err != nil {
		return nil, fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	recoveryIncrement := 0
	if input.incrementAttempts {
		recoveryIncrement = 1
	}
	err = input.tx.QueryRow(ctx, query,
		input.status.ID,
		input.status.Status,
		input.status.Name,
		input.status.QueueName,
		input.status.AuthenticatedUser,
		input.status.AssumedRole,
		authenticatedRoles,
		input.status.ExecutorID,
		applicationVersion,
		input.status.ApplicationID,
		input.status.CreatedAt.Round(time.Millisecond).UnixMilli(), // slightly reduce the likelihood of collisions
		attempts,
		updatedAt.UnixMilli(),
		timeoutMs,
		deadline,
		input.status.Input, // encoded input (already *string)
		deduplicationID,
		input.status.Priority,
		queuePartitionKey,
		input.ownerXID,
		parentWorkflowID,
		WorkflowStatusEnqueued,
		WorkflowStatusEnqueued,
		recoveryIncrement,
	).Scan(
		&result.attempts,
		&result.status,
		&result.name,
		&result.queueName,
		&timeoutMSResult,
		&workflowDeadlineEpochMS,
		&ownerXIDReturn,
	)
	if ownerXIDReturn != nil {
		result.ownerXID = *ownerXIDReturn
	}
	if err != nil {
		// Handle unique constraint violation for the deduplication ID (this should be the only case for a 23505)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_UNIQUE_VIOLATION {
			return nil, newQueueDeduplicatedError(
				input.status.ID,
				input.status.QueueName,
				input.status.DeduplicationID,
			)
		}
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	// Convert timeout milliseconds to time.Duration
	if timeoutMSResult != nil && *timeoutMSResult > 0 {
		result.timeout = time.Duration(*timeoutMSResult) * time.Millisecond
	}

	// Convert deadline milliseconds to time.Time
	if workflowDeadlineEpochMS != nil {
		result.workflowDeadline = time.Unix(0, *workflowDeadlineEpochMS*int64(time.Millisecond))
	}

	if len(input.status.Name) > 0 && result.name != input.status.Name {
		return nil, newConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.name, input.status.Name))
	}
	if len(input.status.QueueName) > 0 && result.queueName != nil && input.status.QueueName != *result.queueName {
		return nil, newConflictingWorkflowError(input.status.ID, fmt.Sprintf("Workflow already exists in a different queue: %s, but the provided queue is: %s", *result.queueName, input.status.QueueName))
	}

	// Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
	// When this number becomes equal to `maxRetries + 1`, we mark the workflow as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
	if result.status != WorkflowStatusSuccess && result.status != WorkflowStatusError &&
		input.maxRetries > 0 && result.attempts > input.maxRetries+1 {

		// Update workflow status to MAX_RECOVERY_ATTEMPTS_EXCEEDED and clear queue-related fields
		dlqQuery := fmt.Sprintf(`UPDATE %s.workflow_status
					 SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
					 WHERE workflow_uuid = $2 AND status = $3`, pgx.Identifier{s.schema}.Sanitize())

		_, err = input.tx.Exec(ctx, dlqQuery,
			WorkflowStatusMaxRecoveryAttemptsExceeded,
			input.status.ID,
			WorkflowStatusPending)

		if err != nil {
			return nil, fmt.Errorf("failed to update workflow to %s: %w", WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		// Commit the transaction before throwing the error
		if err := input.tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction after marking workflow as %s: %w", WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		return nil, newDeadLetterQueueError(input.status.ID, input.maxRetries)
	}

	return &result, nil
}

// listWorkflowsDBInput represents the input parameters for listing workflows.
type listWorkflowsDBInput struct {
	workflowName       []string
	queueName          []string
	queuesOnly         bool
	workflowIDPrefix   []string
	workflowIDs        []string
	authenticatedUser  []string
	startTime          time.Time
	endTime            time.Time
	status             []WorkflowStatusType
	applicationVersion []string
	executorIDs        []string
	forkedFrom         []string
	parentWorkflowID   []string
	deduplicationID    []string
	limit              *int
	offset             *int
	sortDesc           bool
	loadInput          bool
	loadOutput         bool
	tx                 pgx.Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *sysDB) listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error) {
	qb := newQueryBuilder()

	// Build the base query with conditional column selection
	loadColumns := []string{
		"workflow_uuid", "status", "name", "authenticated_user", "assumed_role", "authenticated_roles",
		"executor_id", "created_at", "updated_at", "application_version", "application_id",
		"recovery_attempts", "queue_name", "workflow_timeout_ms", "workflow_deadline_epoch_ms", "started_at_epoch_ms",
		"deduplication_id", "priority", "queue_partition_key", "forked_from", "parent_workflow_id",
	}

	if input.loadOutput {
		loadColumns = append(loadColumns, "output", "error")
	}
	if input.loadInput {
		loadColumns = append(loadColumns, "inputs")
	}

	baseQuery := fmt.Sprintf("SELECT %s FROM %s.workflow_status", strings.Join(loadColumns, ", "), pgx.Identifier{s.schema}.Sanitize())

	// Add filters using query builder
	if len(input.workflowName) > 0 {
		qb.addWhereAny("name", input.workflowName)
	}
	if len(input.queueName) > 0 {
		qb.addWhereAny("queue_name", input.queueName)
	}
	if input.queuesOnly {
		qb.addWhereIsNotNull("queue_name")
	}
	if len(input.workflowIDPrefix) > 0 {
		qb.addWhereLikeAny("workflow_uuid", input.workflowIDPrefix, "%")
	}
	if len(input.workflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.workflowIDs)
	}
	if len(input.authenticatedUser) > 0 {
		qb.addWhereAny("authenticated_user", input.authenticatedUser)
	}
	if !input.startTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.startTime.UnixMilli())
	}
	if !input.endTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.endTime.UnixMilli())
	}
	if len(input.status) > 0 {
		qb.addWhereAny("status", input.status)
	}
	if len(input.applicationVersion) > 0 {
		qb.addWhereAny("application_version", input.applicationVersion)
	}
	if len(input.executorIDs) > 0 {
		qb.addWhereAny("executor_id", input.executorIDs)
	}
	if len(input.forkedFrom) > 0 {
		qb.addWhereAny("forked_from", input.forkedFrom)
	}
	if len(input.parentWorkflowID) > 0 {
		qb.addWhereAny("parent_workflow_id", input.parentWorkflowID)
	}
	if len(input.deduplicationID) > 0 {
		qb.addWhereAny("deduplication_id", input.deduplicationID)
	}

	// Build complete query
	var query string
	if len(qb.whereClauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", baseQuery, strings.Join(qb.whereClauses, " AND "))
	} else {
		query = baseQuery
	}

	// Add sorting
	if input.sortDesc {
		query += " ORDER BY created_at DESC"
	} else {
		query += " ORDER BY created_at ASC"
	}

	// Add limit and offset
	if input.limit != nil {
		qb.argCounter++
		query += fmt.Sprintf(" LIMIT $%d", qb.argCounter)
		qb.args = append(qb.args, *input.limit)
	}

	if input.offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.offset)
	}

	// Execute the query
	var rows pgx.Rows
	var err error

	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute ListWorkflows query: %w", err)
	}
	defer rows.Close()

	var workflows []WorkflowStatus
	for rows.Next() {
		var wf WorkflowStatus
		var queueName *string
		var createdAtMs, updatedAtMs int64
		var timeoutMs *int64
		var deadlineMs, startedAtMs *int64
		var outputString, inputString *string
		var errorStr *string
		var deduplicationID *string
		var applicationVersion *string
		var executorID *string
		var authenticatedRoles *string
		var queuePartitionKey *string
		var forkedFrom *string
		var parentWorkflowID *string

		// Build scan arguments dynamically based on loaded columns
		scanArgs := []any{
			&wf.ID, &wf.Status, &wf.Name, &wf.AuthenticatedUser, &wf.AssumedRole,
			&authenticatedRoles, &executorID, &createdAtMs,
			&updatedAtMs, &applicationVersion, &wf.ApplicationID,
			&wf.Attempts, &queueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &deduplicationID, &wf.Priority, &queuePartitionKey, &forkedFrom, &parentWorkflowID,
		}

		if input.loadOutput {
			scanArgs = append(scanArgs, &outputString, &errorStr)
		}
		if input.loadInput {
			scanArgs = append(scanArgs, &inputString)
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow row: %w", err)
		}

		if authenticatedRoles != nil && *authenticatedRoles != "" {
			if err := json.Unmarshal([]byte(*authenticatedRoles), &wf.AuthenticatedRoles); err != nil {
				return nil, fmt.Errorf("failed to unmarshal authenticated_roles: %w", err)
			}
		}

		if queueName != nil && len(*queueName) > 0 {
			wf.QueueName = *queueName
		}

		if executorID != nil && len(*executorID) > 0 {
			wf.ExecutorID = *executorID
		}

		if applicationVersion != nil && len(*applicationVersion) > 0 {
			wf.ApplicationVersion = *applicationVersion
		}

		if deduplicationID != nil && len(*deduplicationID) > 0 {
			wf.DeduplicationID = *deduplicationID
		}

		if queuePartitionKey != nil && len(*queuePartitionKey) > 0 {
			wf.QueuePartitionKey = *queuePartitionKey
		}

		if forkedFrom != nil && len(*forkedFrom) > 0 {
			wf.ForkedFrom = *forkedFrom
		}

		if parentWorkflowID != nil && len(*parentWorkflowID) > 0 {
			wf.ParentWorkflowID = *parentWorkflowID
		}

		// Convert milliseconds to time.Time
		wf.CreatedAt = time.Unix(0, createdAtMs*int64(time.Millisecond))
		wf.UpdatedAt = time.Unix(0, updatedAtMs*int64(time.Millisecond))

		// Convert timeout milliseconds to time.Duration
		if timeoutMs != nil && *timeoutMs > 0 {
			wf.Timeout = time.Duration(*timeoutMs) * time.Millisecond
		}

		// Convert deadline milliseconds to time.Time
		if deadlineMs != nil {
			wf.Deadline = time.Unix(0, *deadlineMs*int64(time.Millisecond))
		}

		// Convert started at milliseconds to time.Time
		if startedAtMs != nil {
			wf.StartedAt = time.Unix(0, *startedAtMs*int64(time.Millisecond))
		}

		// Handle output and error only if loadOutput is true
		if input.loadOutput {
			// Convert error string to error type if present
			if errorStr != nil && *errorStr != "" {
				wf.Error = errors.New(*errorStr)
			}

			// Return output as encoded *string
			wf.Output = outputString
		}

		// Return input as encoded *string
		if input.loadInput {
			wf.Input = inputString
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type updateWorkflowOutcomeDBInput struct {
	workflowID string
	status     WorkflowStatusType
	output     *string
	err        error
	tx         pgx.Tx
}

// updateWorkflowOutcome updates the status, output, and error of a workflow
// Note that transitions from CANCELLED to SUCCESS or ERROR are forbidden
func (s *sysDB) updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error {
	query := fmt.Sprintf(`UPDATE %s.workflow_status
			  SET status = $1, output = $2, error = $3, updated_at = $4, deduplication_id = NULL
			  WHERE workflow_uuid = $5 AND NOT (status = $6 AND $1::TEXT IN ($7, $8))`, pgx.Identifier{s.schema}.Sanitize())

	var errorStr string
	if input.err != nil {
		errorStr = input.err.Error()
	}

	// input.output is already a *string from the database layer
	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.status, input.output, errorStr, time.Now().UnixMilli(), input.workflowID, WorkflowStatusCancelled, WorkflowStatusSuccess, WorkflowStatusError)
	} else {
		_, err = s.pool.Exec(ctx, query, input.status, input.output, errorStr, time.Now().UnixMilli(), input.workflowID, WorkflowStatusCancelled, WorkflowStatusSuccess, WorkflowStatusError)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

type cancelWorkflowDBInput struct {
	workflowID string
	tx         pgx.Tx
}

func (s *sysDB) cancelWorkflow(ctx context.Context, input cancelWorkflowDBInput) error {
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{input.workflowID},
		loadInput:   true,
		loadOutput:  true,
		tx:          input.tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		return err
	}
	if len(wfs) == 0 {
		return newNonExistentWorkflowError(input.workflowID)
	}

	wf := wfs[0]
	switch wf.Status {
	case WorkflowStatusSuccess, WorkflowStatusError, WorkflowStatusCancelled:
		// Workflow is already in a terminal state, rollback and return
		return nil
	}

	updateStatusQuery := fmt.Sprintf(`UPDATE %s.workflow_status
						  SET status = $1, updated_at = $2, started_at_epoch_ms = NULL
						  WHERE workflow_uuid = $3`, pgx.Identifier{s.schema}.Sanitize())

	if input.tx != nil {
		_, err = input.tx.Exec(ctx, updateStatusQuery, WorkflowStatusCancelled, time.Now().UnixMilli(), input.workflowID)
	} else {
		_, err = s.pool.Exec(ctx, updateStatusQuery, WorkflowStatusCancelled, time.Now().UnixMilli(), input.workflowID)
	}
	if err != nil {
		return fmt.Errorf("failed to update workflow status to CANCELLED: %w", err)
	}

	return nil
}

func (s *sysDB) cancelAllBefore(ctx context.Context, cutoffTime time.Time) error {
	// List all workflows in PENDING or ENQUEUED state ending at cutoffTime
	listInput := listWorkflowsDBInput{
		endTime: cutoffTime,
		status:  []WorkflowStatusType{WorkflowStatusPending, WorkflowStatusEnqueued},
	}

	workflows, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list workflows for cancellation: %w", err)
	}

	// Cancel each workflow
	for _, workflow := range workflows {
		if err := s.cancelWorkflow(ctx, cancelWorkflowDBInput{workflowID: workflow.ID}); err != nil {
			s.logger.Error("Failed to cancel workflow during cancelAllBefore", "workflowID", workflow.ID, "error", err)
			// Continue with other workflows even if one fails
			// If desired we could funnel the errors back the caller (conductor, admin server)
		}
	}
	return nil
}

type garbageCollectWorkflowsInput struct {
	cutoffEpochTimestampMs *int64
	rowsThreshold          *int
}

func (s *sysDB) garbageCollectWorkflows(ctx context.Context, input garbageCollectWorkflowsInput) error {
	// Validate input parameters
	if input.rowsThreshold != nil && *input.rowsThreshold <= 0 {
		return fmt.Errorf("rowsThreshold must be greater than 0, got %d", *input.rowsThreshold)
	}

	cutoffTimestamp := input.cutoffEpochTimestampMs

	// If rowsThreshold is provided, get the timestamp of the Nth newest workflow
	if input.rowsThreshold != nil {
		query := fmt.Sprintf(`SELECT created_at
				  FROM %s.workflow_status
				  ORDER BY created_at DESC
				  LIMIT 1 OFFSET $1`, pgx.Identifier{s.schema}.Sanitize())

		var rowsBasedCutoff int64
		err := s.pool.QueryRow(ctx, query, *input.rowsThreshold-1).Scan(&rowsBasedCutoff)
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("failed to query cutoff timestamp by rows threshold: %w", err)
		}
		// If we don't have a provided cutoffTimestamp and found one in the database
		// Or if the found cutoffTimestamp is more restrictive (higher timestamp = more recent = less deletion)
		// Use the cutoff timestamp found in the database
		if rowsBasedCutoff > 0 && cutoffTimestamp == nil || (cutoffTimestamp != nil && rowsBasedCutoff > *cutoffTimestamp) {
			cutoffTimestamp = &rowsBasedCutoff
		}
	}

	// If no cutoff is determined, no garbage collection is needed
	if cutoffTimestamp == nil {
		return nil
	}

	// Delete all workflows older than cutoff that are NOT PENDING or ENQUEUED
	query := fmt.Sprintf(`DELETE FROM %s.workflow_status
			  WHERE created_at < $1
			    AND status NOT IN ($2, $3)`, pgx.Identifier{s.schema}.Sanitize())

	commandTag, err := s.pool.Exec(ctx, query,
		*cutoffTimestamp,
		WorkflowStatusPending,
		WorkflowStatusEnqueued)

	if err != nil {
		return fmt.Errorf("failed to garbage collect workflows: %w", err)
	}

	s.logger.Info("Garbage collected workflows",
		"cutoff_timestamp", *cutoffTimestamp,
		"deleted_count", commandTag.RowsAffected())

	return nil
}

type resumeWorkflowDBInput struct {
	workflowID string
	tx         pgx.Tx
}

func (s *sysDB) resumeWorkflow(ctx context.Context, input resumeWorkflowDBInput) error {
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{input.workflowID},
		loadInput:   true,
		loadOutput:  true,
		tx:          input.tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		s.logger.Error("ResumeWorkflow: failed to list workflows", "error", err)
		return err
	}
	if len(wfs) == 0 {
		return newNonExistentWorkflowError(input.workflowID)
	}

	wf := wfs[0]
	if wf.Status == WorkflowStatusSuccess || wf.Status == WorkflowStatusError {
		return nil // Workflow is complete, do nothing
	}

	// Set the workflow's status to ENQUEUED and clear its recovery attempts, set new deadline
	updateStatusQuery := fmt.Sprintf(`UPDATE %s.workflow_status
						  SET status = $1, queue_name = $2, recovery_attempts = $3, 
						      workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
						      started_at_epoch_ms = NULL, updated_at = $4
						  WHERE workflow_uuid = $5`, pgx.Identifier{s.schema}.Sanitize())

	if input.tx != nil {
		_, err = input.tx.Exec(ctx, updateStatusQuery,
			WorkflowStatusEnqueued,
			_DBOS_INTERNAL_QUEUE_NAME,
			0,
			time.Now().UnixMilli(),
			input.workflowID)
	} else {
		_, err = s.pool.Exec(ctx, updateStatusQuery,
			WorkflowStatusEnqueued,
			_DBOS_INTERNAL_QUEUE_NAME,
			0,
			time.Now().UnixMilli(),
			input.workflowID)
	}
	if err != nil {
		return fmt.Errorf("failed to update workflow status to ENQUEUED: %w", err)
	}

	return nil
}

type forkWorkflowDBInput struct {
	originalWorkflowID string
	forkedWorkflowID   string
	startStep          int
	applicationVersion string
	tx                 pgx.Tx
}

func (s *sysDB) forkWorkflow(ctx context.Context, input forkWorkflowDBInput) (string, error) {
	// Generate new workflow ID if not provided
	forkedWorkflowID := input.forkedWorkflowID
	if forkedWorkflowID == "" {
		forkedWorkflowID = uuid.New().String()
	}

	// Validate startStep
	if input.startStep < 0 {
		return "", fmt.Errorf("startStep must be >= 0, got %d", input.startStep)
	}

	// When no transaction is provided, run queries on the pool directly (no transaction).
	execCtx := func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		if input.tx != nil {
			return input.tx.Exec(ctx, sql, args...)
		}
		return s.pool.Exec(ctx, sql, args...)
	}

	// Get the original workflow status
	listInput := listWorkflowsDBInput{
		workflowIDs: []string{input.originalWorkflowID},
		loadInput:   true,
		tx:          input.tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		return "", fmt.Errorf("failed to list workflows: %w", err)
	}
	if len(wfs) == 0 {
		return "", newNonExistentWorkflowError(input.originalWorkflowID)
	}

	originalWorkflow := wfs[0]

	// Determine the application version to use
	appVersion := originalWorkflow.ApplicationVersion
	if input.applicationVersion != "" {
		appVersion = input.applicationVersion
	}

	// Create an entry for the forked workflow with the same initial values as the original
	insertQuery := fmt.Sprintf(`INSERT INTO %s.workflow_status (
		workflow_uuid,
		status,
		name,
		authenticated_user,
		assumed_role,
		authenticated_roles,
		application_version,
		application_id,
		queue_name,
		inputs,
		created_at,
		updated_at,
		recovery_attempts,
		forked_from
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`, pgx.Identifier{s.schema}.Sanitize())

	// Marshal authenticated roles (slice of strings) to JSON for TEXT column
	authenticatedRoles, err := json.Marshal(originalWorkflow.AuthenticatedRoles)
	if err != nil {
		return "", fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	_, err = execCtx(ctx, insertQuery,
		forkedWorkflowID,
		WorkflowStatusEnqueued,
		originalWorkflow.Name,
		originalWorkflow.AuthenticatedUser,
		originalWorkflow.AssumedRole,
		authenticatedRoles,
		&appVersion,
		originalWorkflow.ApplicationID,
		_DBOS_INTERNAL_QUEUE_NAME,
		originalWorkflow.Input, // encoded
		time.Now().UnixMilli(),
		time.Now().UnixMilli(),
		0,
		input.originalWorkflowID) // forked_from

	if err != nil {
		return "", fmt.Errorf("failed to insert forked workflow status: %w", err)
	}

	// If startStep > 0, copy the original workflow's outputs into the forked workflow
	if input.startStep > 0 {
		copyOutputsQuery := fmt.Sprintf(`INSERT INTO %s.operation_outputs
			(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
			SELECT $1, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
			FROM %s.operation_outputs
			WHERE workflow_uuid = $2 AND function_id < $3`, pgx.Identifier{s.schema}.Sanitize(), pgx.Identifier{s.schema}.Sanitize())

		_, err = execCtx(ctx, copyOutputsQuery, forkedWorkflowID, input.originalWorkflowID, input.startStep)
		if err != nil {
			return "", fmt.Errorf("failed to copy operation outputs: %w", err)
		}

		// Copy workflow events history for steps before startStep
		copyEventsHistoryQuery := fmt.Sprintf(`INSERT INTO %s.workflow_events_history
			(workflow_uuid, function_id, key, value)
			SELECT $1, function_id, key, value
			FROM %s.workflow_events_history
			WHERE workflow_uuid = $2 AND function_id < $3`, pgx.Identifier{s.schema}.Sanitize(), pgx.Identifier{s.schema}.Sanitize())

		_, err = execCtx(ctx, copyEventsHistoryQuery, forkedWorkflowID, input.originalWorkflowID, input.startStep)
		if err != nil {
			return "", fmt.Errorf("failed to copy workflow events history: %w", err)
		}

		// Copy the latest version of each event (highest function_id for each key) into workflow_events
		copyLatestEventsQuery := fmt.Sprintf(`INSERT INTO %s.workflow_events (workflow_uuid, key, value)
			SELECT $1, key, value
			FROM (
				SELECT DISTINCT ON (key) key, value
				FROM %s.workflow_events_history
				WHERE workflow_uuid = $2 AND function_id < $3
				ORDER BY key, function_id DESC
			) AS latest_events`, pgx.Identifier{s.schema}.Sanitize(), pgx.Identifier{s.schema}.Sanitize())

		_, err = execCtx(ctx, copyLatestEventsQuery, forkedWorkflowID, input.originalWorkflowID, input.startStep)
		if err != nil {
			return "", fmt.Errorf("failed to copy latest workflow events: %w", err)
		}

		// Copy streams for steps before startStep
		copyStreamsQuery := fmt.Sprintf(`INSERT INTO %s.streams
			(workflow_uuid, key, value, "offset", function_id)
			SELECT $1, key, value, "offset", function_id
			FROM %s.streams
			WHERE workflow_uuid = $2 AND function_id < $3`, pgx.Identifier{s.schema}.Sanitize(), pgx.Identifier{s.schema}.Sanitize())

		_, err = execCtx(ctx, copyStreamsQuery, forkedWorkflowID, input.originalWorkflowID, input.startStep)
		if err != nil {
			return "", fmt.Errorf("failed to copy streams: %w", err)
		}
	}

	return forkedWorkflowID, nil
}

func (s *sysDB) awaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*string, error) {
	query := fmt.Sprintf(`SELECT status, output, error, recovery_attempts FROM %s.workflow_status WHERE workflow_uuid = $1`, pgx.Identifier{s.schema}.Sanitize())
	var status WorkflowStatusType
	if pollInterval <= 0 {
		pollInterval = _DB_RETRY_INTERVAL
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		row := s.pool.QueryRow(ctx, query, workflowID)
		var outputString *string
		var errorStr *string
		var attempts int
		err := row.Scan(&status, &outputString, &errorStr, &attempts)
		if err != nil {
			if err == pgx.ErrNoRows {
				time.Sleep(pollInterval)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		switch status {
		case WorkflowStatusSuccess, WorkflowStatusError:
			if errorStr == nil || len(*errorStr) == 0 {
				return outputString, nil
			}
			return outputString, errors.New(*errorStr)
		case WorkflowStatusCancelled:
			return outputString, newAwaitedWorkflowCancelledError(workflowID)
		case WorkflowStatusMaxRecoveryAttemptsExceeded:
			return outputString, newDeadLetterQueueError(workflowID, attempts-2)
		default:
			time.Sleep(pollInterval)
		}
	}
}

type recordOperationResultDBInput struct {
	workflowID  string
	stepID      int
	stepName    string
	output      *string
	err         error
	tx          pgx.Tx
	startedAt   time.Time
	completedAt time.Time
}

func (s *sysDB) recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	startedAtMs := input.startedAt.UnixMilli()
	completedAtMs := input.completedAt.UnixMilli()

	query := fmt.Sprintf(`INSERT INTO %s.operation_outputs
            (workflow_uuid, function_id, output, error, function_name, started_at_epoch_ms, completed_at_epoch_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7)`, pgx.Identifier{s.schema}.Sanitize())

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query,
			input.workflowID,
			input.stepID,
			input.output,
			errorString,
			input.stepName,
			startedAtMs,
			completedAtMs,
		)
	} else {
		_, err = s.pool.Exec(ctx, query,
			input.workflowID,
			input.stepID,
			input.output,
			errorString,
			input.stepName,
			startedAtMs,
			completedAtMs,
		)
	}

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_UNIQUE_VIOLATION {
			return newWorkflowConflictIDError(input.workflowID)
		}
		return err
	}

	return nil
}

/*******************************/
/******* CHILD WORKFLOWS ********/
/*******************************/

type recordChildWorkflowDBInput struct {
	parentWorkflowID string
	childWorkflowID  string
	stepID           int
	stepName         string
	tx               pgx.Tx
}

func (s *sysDB) recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error {
	query := fmt.Sprintf(`INSERT INTO %s.operation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)`, pgx.Identifier{s.schema}.Sanitize())

	var commandTag pgconn.CommandTag
	var err error

	if input.tx != nil {
		commandTag, err = input.tx.Exec(ctx, query,
			input.parentWorkflowID,
			input.stepID,
			input.stepName,
			input.childWorkflowID,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.parentWorkflowID,
			input.stepID,
			input.stepName,
			input.childWorkflowID,
		)
	}

	if err != nil {
		// Check for unique constraint violation (conflict ID error)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_UNIQUE_VIOLATION {
			return fmt.Errorf(
				"child workflow %s already registered for parent workflow %s (operation ID: %d). Is your workflow deterministic?",
				input.childWorkflowID, input.parentWorkflowID, input.stepID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		s.logger.Warn("RecordChildWorkflow No rows were affected by the insert")
	}

	return nil
}

func (s *sysDB) checkChildWorkflow(ctx context.Context, workflowID string, functionID int) (*string, error) {
	query := fmt.Sprintf(`SELECT child_workflow_id
              FROM %s.operation_outputs
              WHERE workflow_uuid = $1 AND function_id = $2`, pgx.Identifier{s.schema}.Sanitize())

	var childWorkflowID *string
	err := s.pool.QueryRow(ctx, query, workflowID, functionID).Scan(&childWorkflowID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to check child workflow: %w", err)
	}

	return childWorkflowID, nil
}

type recordChildGetResultDBInput struct {
	parentWorkflowID string
	childWorkflowID  string
	stepID           int
	output           *string
	err              error
	startedAt        time.Time
	completedAt      time.Time
}

func (s *sysDB) recordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error {
	startedAtMs := input.startedAt.UnixMilli()
	completedAtMs := input.completedAt.UnixMilli()

	query := fmt.Sprintf(`INSERT INTO %s.operation_outputs
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT DO NOTHING`, pgx.Identifier{s.schema}.Sanitize())

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	_, err := s.pool.Exec(ctx, query,
		input.parentWorkflowID,
		input.stepID,
		"DBOS.getResult",
		input.output,
		errorString,
		input.childWorkflowID,
		startedAtMs,
		completedAtMs,
	)
	if err != nil {
		return fmt.Errorf("failed to record get result: %w", err)
	}
	return nil
}

/*******************************/
/******* STEPS ********/
/*******************************/

type recordedResult struct {
	output *string
	err    error
}

type checkOperationExecutionDBInput struct {
	workflowID string
	stepID     int
	stepName   string
	tx         pgx.Tx
}

func (s *sysDB) checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error) {
	var tx pgx.Tx
	var err error

	// Use provided transaction or create a new one
	if input.tx != nil {
		tx = input.tx
	} else {
		tx, err = s.pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) // We don't need to commit this transaction -- it is just useful for having READ COMMITTED across the reads
	}

	// First query: Retrieve the workflow status
	workflowStatusQuery := fmt.Sprintf(`SELECT status FROM %s.workflow_status WHERE workflow_uuid = $1`, pgx.Identifier{s.schema}.Sanitize())

	// Second query: Retrieve operation outputs if they exist
	stepOutputQuery := fmt.Sprintf(`SELECT output, error, function_name
							 FROM %s.operation_outputs
							 WHERE workflow_uuid = $1 AND function_id = $2`, pgx.Identifier{s.schema}.Sanitize())

	var workflowStatus WorkflowStatusType

	// Execute first query to get workflow status
	err = tx.QueryRow(ctx, workflowStatusQuery, input.workflowID).Scan(&workflowStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, newNonExistentWorkflowError(input.workflowID)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}

	// If the workflow is cancelled, raise the exception
	if workflowStatus == WorkflowStatusCancelled {
		return nil, newWorkflowCancelledError(input.workflowID)
	}

	// Execute second query to get operation outputs
	var outputString *string
	var errorStr *string
	var recordedFunctionName string

	err = tx.QueryRow(ctx, stepOutputQuery, input.workflowID, input.stepID).Scan(&outputString, &errorStr, &recordedFunctionName)

	// If there are no operation outputs, return nil
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	// If the provided and recorded function name are different, return an error
	if input.stepName != recordedFunctionName {
		return nil, newUnexpectedStepError(input.workflowID, input.stepID, input.stepName, recordedFunctionName)
	}

	var recordedError error
	if errorStr != nil && *errorStr != "" {
		recordedError = errors.New(*errorStr)
	}
	result := &recordedResult{
		output: outputString,
		err:    recordedError,
	}
	return result, nil
}

// StepInfo contains information about a workflow step execution.
type stepInfo struct {
	StepID          int       // The sequential ID of the step within the workflow
	StepName        string    // The name of the step function
	Output          *string   // The output returned by the step (if any)
	Error           error     // The error returned by the step (if any)
	ChildWorkflowID string    // The ID of a child workflow spawned by this step (if applicable)
	StartedAt       time.Time // When the step execution started
	CompletedAt     time.Time // When the step execution completed
}

type getWorkflowStepsInput struct {
	workflowID string
	loadOutput bool
}

func (s *sysDB) getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error) {
	query := fmt.Sprintf(`SELECT function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
			  FROM %s.operation_outputs
			  WHERE workflow_uuid = $1
			  ORDER BY function_id ASC`, pgx.Identifier{s.schema}.Sanitize())

	rows, err := s.pool.Query(ctx, query, input.workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []stepInfo
	for rows.Next() {
		var step stepInfo
		var outputString *string
		var errorString *string
		var childWorkflowID *string
		var startedAtMs, completedAtMs *int64

		err := rows.Scan(&step.StepID, &step.StepName, &outputString, &errorString, &childWorkflowID, &startedAtMs, &completedAtMs)
		if err != nil {
			return nil, fmt.Errorf("failed to scan step row: %w", err)
		}

		// Convert timestamps from milliseconds to time.Time
		if startedAtMs != nil {
			step.StartedAt = time.Unix(0, *startedAtMs*int64(time.Millisecond))
		}
		if completedAtMs != nil {
			step.CompletedAt = time.Unix(0, *completedAtMs*int64(time.Millisecond))
		}

		// Return output as encoded string if loadOutput is true
		if input.loadOutput {
			step.Output = outputString
		}

		// Convert error string to error if present
		if errorString != nil && *errorString != "" {
			step.Error = errors.New(*errorString)
		}

		// Set child workflow ID if present
		if childWorkflowID != nil {
			step.ChildWorkflowID = *childWorkflowID
		}

		steps = append(steps, step)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over step rows: %w", err)
	}

	return steps, nil
}

type sleepInput struct {
	duration  time.Duration // Duration to sleep
	skipSleep bool          // If true, the function will not actually sleep and just return the remaining sleep duration
	stepID    *int          // Optional step ID to use instead of generating a new one (for internal use)
}

// Sleep is a special type of step that sleeps for a specified duration
// A wakeup time is computed and recorded in the database
// If we sleep is re-executed, it will only sleep for the remaining duration until the wakeup time
// sleep can be called within other special steps (e.g., getEvent, recv) to provide durable sleep

func (s *sysDB) sleep(ctx context.Context, input sleepInput) (time.Duration, error) {
	functionName := "DBOS.sleep"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, newStepExecutionError("", functionName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	// Determine step ID
	var stepID int
	if input.stepID != nil && *input.stepID >= 0 {
		stepID = *input.stepID
	} else {
		stepID = wfState.nextStepID()
	}

	startTime := time.Now()

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: wfState.workflowID,
		stepID:     stepID,
		stepName:   functionName,
	}
	recordedResult, err := s.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return 0, fmt.Errorf("failed to check operation execution: %w", err)
	}

	var endTime time.Time

	if recordedResult != nil {
		if recordedResult.output == nil { // This should never happen
			return 0, fmt.Errorf("no recorded end time for recorded sleep operation")
		}

		// Decode the recorded end time directly into time.Time
		// recordedResult.output is an encoded *string
		serializer := newJSONSerializer[time.Time]()
		endTime, err = serializer.Decode(recordedResult.output)
		if err != nil {
			return 0, fmt.Errorf("failed to decode sleep end time: %w", err)
		}

		if recordedResult.err != nil { // This should never happen
			return 0, recordedResult.err
		}
	} else {
		// First execution: calculate and record the end time
		endTime = time.Now().Add(input.duration)

		// Serialize the end time before recording
		serializer := newJSONSerializer[time.Time]()
		encodedEndTime, serErr := serializer.Encode(endTime)
		if serErr != nil {
			return 0, fmt.Errorf("failed to serialize sleep end time: %w", serErr)
		}

		// Record the operation result with the calculated end time
		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowID:  wfState.workflowID,
			stepID:      stepID,
			stepName:    functionName,
			output:      encodedEndTime,
			err:         nil,
			startedAt:   startTime,
			completedAt: completedTime,
		}

		err = s.recordOperationResult(ctx, recordInput)
		if err != nil {
			// Check if this is a ConflictingWorkflowError (operation already recorded by another process)
			if dbosErr, ok := err.(*DBOSError); ok && dbosErr.Code == ConflictingIDError {
			} else {
				return 0, fmt.Errorf("failed to record sleep operation result: %w", err)
			}
		}
	}

	// Calculate remaining duration until wake up time
	remainingDuration := max(0, time.Until(endTime))

	if !input.skipSleep {
		// Actually sleep for the remaining duration
		time.Sleep(remainingDuration)
	}

	return remainingDuration, nil
}

/****************************************/
/******* PATCHES ********/
/****************************************/

type patchDBInput struct {
	workflowID string
	stepID     int
	patchName  string
}

func (s *sysDB) doesPatchExists(ctx context.Context, input patchDBInput) (string, error) {
	var functionName string
	query := fmt.Sprintf(`SELECT function_name FROM %s.operation_outputs WHERE workflow_uuid = $1 AND function_id = $2`, pgx.Identifier{s.schema}.Sanitize())
	return functionName, s.pool.QueryRow(ctx, query, input.workflowID, input.stepID).Scan(&functionName)
}

func (s *sysDB) patch(ctx context.Context, input patchDBInput) (bool, error) {
	functionName, err := s.doesPatchExists(ctx, input)
	if err != nil {
		// No result means this is a new workflow, or an existing workflow that has not reached this step yet
		// Insert the patch marker and return true
		if err == pgx.ErrNoRows {
			insertQuery := fmt.Sprintf(`INSERT INTO %s.operation_outputs (workflow_uuid, function_id, function_name) VALUES ($1, $2, $3)`, pgx.Identifier{s.schema}.Sanitize())
			_, err = s.pool.Exec(ctx, insertQuery, input.workflowID, input.stepID, input.patchName)
			if err != nil {
				return false, fmt.Errorf("failed to insert patch marker: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("failed to check for patch: %w", err)
	}

	// If functionName != patchName, this is a workflow that existed before the patch was applied
	// Else this a new (patched) workflow that is being re-executed (e.g., recovery, or forked at a later step)
	return functionName == input.patchName, nil
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

func (s *sysDB) notificationListenerLoop(ctx context.Context) {
	defer func() {
		s.logger.Debug("Notification listener loop exiting")
		s.notificationLoopDone <- struct{}{}
	}()

	acquire := func(ctx context.Context) (*pgxpool.Conn, error) {
		// Acquire a connection from the pool and set up LISTEN on the notifications channels
		pc, err := s.pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := pc.Begin(ctx)
		if err != nil {
			pc.Release()
			return nil, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", _DBOS_NOTIFICATIONS_CHANNEL)); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				s.logger.Error("Failed to rollback transaction after LISTEN error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", _DBOS_WORKFLOW_EVENTS_CHANNEL)); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				s.logger.Error("Failed to rollback transaction after LISTEN error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		if err = tx.Commit(ctx); err != nil {
			rErr := tx.Rollback(ctx)
			if rErr != nil {
				s.logger.Error("Failed to rollback transaction after COMMIT error", "error", rErr)
			}
			pc.Release()
			return nil, err
		}
		return pc, nil
	}

	s.logger.Debug("DBOS: Starting notification listener loop")

	poolConn, err := retryWithResult(ctx, func() (*pgxpool.Conn, error) {
		return acquire(ctx)
	}, withRetrierLogger(s.logger))
	if err != nil {
		s.logger.Error("Failed to acquire listener connection", "error", err)
		return
	}
	defer poolConn.Release()

	retryAttempt := 0
	for {
		// Block until a notification is received. OnNotification will be called when a notification is received.
		// WaitForNotification handles context cancellation: https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgconn/pgconn.go#L1050
		n, err := poolConn.Conn().WaitForNotification(ctx)
		if err != nil {
			// Context cancellation -> graceful exit
			if ctx.Err() != nil {
				s.logger.Debug("Notification listener exiting (context canceled", "cause", context.Cause(ctx), "error", err)
				poolConn.Release()
				return
			}
			// If the underlying connection is closed, attempt to re-acquire a new one
			if poolConn.Conn().IsClosed() {
				s.logger.Debug("Notification listener connection closed. re-acquiring")
				poolConn.Release()
				for {
					if ctx.Err() != nil {
						s.logger.Debug("Notification listener exiting (context canceled)", "cause", context.Cause(ctx), "error", err)
						return
					}
					poolConn, err = acquire(ctx)
					if err == nil {
						retryAttempt = 0
						break
					}
					s.logger.Debug("failed to re-acquire connection for notification listener", "error", err)
					time.Sleep(backoffWithJitter(retryAttempt))
					retryAttempt++
				}
				// The connection is re-aquired. Signal to all waiters they should poll the database for a potentially missed value.
				s.workflowNotificationRepollMap.Range(func(key, value any) bool {
					repollChannel := value.(chan struct{})
					repollChannel <- struct{}{}
					return true
				})
				s.workflowEventsRepollMap.Range(func(key, value any) bool {
					repollChannel := value.(chan struct{})
					repollChannel <- struct{}{}
					return true
				})
				continue
			}
			// Other transient errors. Backoff and continue on same conn
			s.logger.Error("Error waiting for notification", "error", err)
			time.Sleep(backoffWithJitter(retryAttempt))
			retryAttempt++
			continue
		}

		// Success: reduce backoff pressure
		if retryAttempt > 0 {
			retryAttempt--
		}

		switch n.Channel {
		case _DBOS_NOTIFICATIONS_CHANNEL:
			if cond, ok := s.workflowNotificationsMap.Load(n.Payload); ok {
				cond.(*sync.Cond).L.Lock()
				cond.(*sync.Cond).Broadcast()
				cond.(*sync.Cond).L.Unlock()
			}
		case _DBOS_WORKFLOW_EVENTS_CHANNEL:
			if cond, ok := s.workflowEventsMap.Load(n.Payload); ok {
				cond.(*sync.Cond).L.Lock()
				cond.(*sync.Cond).Broadcast()
				cond.(*sync.Cond).L.Unlock()
			}
		}
	}
}

func (s *sysDB) notificationPollerLoop(ctx context.Context) {
	defer func() {
		s.logger.Debug("Notification poller loop exiting")
		s.notificationLoopDone <- struct{}{}
	}()

	s.logger.Debug("DBOS: Starting notification poller loop")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Debug("Notification poller exiting (context canceled)", "cause", context.Cause(ctx))
			return
		case <-ticker.C:
			s.pollNotifications(ctx)
			s.pollEvents(ctx)
		}
	}
}

func (s *sysDB) pollNotifications(ctx context.Context) {
	// Iterate through all registered notification payloads
	s.workflowNotificationsMap.Range(func(key, value any) bool {
		payload, ok := key.(string)
		if !ok {
			return true // Continue to next item
		}

		// Parse payload: format is "destinationID::topic"
		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			s.logger.Warn("Invalid notification payload format", "payload", payload)
			return true // Continue to next item
		}

		destinationID := parts[0]
		topic := parts[1]

		// Query database to check if notification exists
		query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s.notifications WHERE destination_uuid = $1 AND topic = $2)`, pgx.Identifier{s.schema}.Sanitize())
		var exists bool
		err := s.pool.QueryRow(ctx, query, destinationID, topic).Scan(&exists)
		if err != nil {
			s.logger.Warn("Failed to poll notification", "payload", payload, "error", err)
			return true // Continue to next item
		}

		// If notification exists, signal the condition variable
		if exists {
			if cond, ok := value.(*sync.Cond); ok {
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
			}
		}

		return true // Continue to next item
	})
}

func (s *sysDB) pollEvents(ctx context.Context) {
	// Iterate through all registered event payloads
	s.workflowEventsMap.Range(func(key, value any) bool {
		payload, ok := key.(string)
		if !ok {
			return true // Continue to next item
		}

		// Parse payload: format is "targetWorkflowID::key"
		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			s.logger.Warn("Invalid event payload format", "payload", payload)
			return true // Continue to next item
		}

		targetWorkflowID := parts[0]
		eventKey := parts[1]

		// Query database to check if event exists
		query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s.workflow_events WHERE workflow_uuid = $1 AND key = $2)`, pgx.Identifier{s.schema}.Sanitize())
		var exists bool
		err := s.pool.QueryRow(ctx, query, targetWorkflowID, eventKey).Scan(&exists)
		if err != nil {
			s.logger.Warn("Failed to poll event", "payload", payload, "error", err)
			return true // Continue to next item
		}

		// If event exists, signal the condition variable
		if exists {
			if cond, ok := value.(*sync.Cond); ok {
				cond.L.Lock()
				cond.Broadcast()
				cond.L.Unlock()
			}
		}

		return true // Continue to next item
	})
}

const _DBOS_NULL_TOPIC = "__null__topic__"

type WorkflowSendInput struct {
	DestinationID string
	Message       any
	Topic         string
	tx            pgx.Tx
}

// Send is a special type of step that sends a message to another workflow.
// Can be called both within a workflow (as a step) or outside a workflow (directly).
// When called within a workflow: durability and the function run in the same transaction, and we forbid nested step execution
func (s *sysDB) send(ctx context.Context, input WorkflowSendInput) error {
	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// Set default topic if not provided
	topic := _DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	insertQuery := fmt.Sprintf(`INSERT INTO %s.notifications (destination_uuid, topic, message) VALUES ($1, $2, $3)`, pgx.Identifier{s.schema}.Sanitize())
	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, insertQuery, input.DestinationID, topic, input.Message)
	} else {
		_, err = s.pool.Exec(ctx, insertQuery, input.DestinationID, topic, input.Message)
	}
	if err != nil {
		s.logger.Error("failed to insert notification", "error", err, "query", insertQuery, "destination_id", input.DestinationID, "topic", topic, "message", input.Message)
		// Check for foreign key violation (destination workflow doesn't exist)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == _PG_ERROR_FOREIGN_KEY_VIOLATION {
			return newNonExistentWorkflowError(input.DestinationID)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}
	return nil
}

// Recv is a special type of step that receives a message destined for a given workflow
func (s *sysDB) recv(ctx context.Context, input recvInput) (*string, error) {
	functionName := "DBOS.recv"

	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", functionName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	stepID := wfState.nextStepID()
	sleepStepID := wfState.nextStepID() // We will use a sleep step to implement the timeout
	destinationID := wfState.workflowID

	// Set default topic if not provided
	topic := _DBOS_NULL_TOPIC
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: destinationID,
		stepID:     stepID,
		stepName:   functionName,
	}
	recordedResult, err := s.checkOperationExecution(ctx, checkInput)
	if err != nil {
		return nil, err
	}
	if recordedResult != nil {
		return recordedResult.output, recordedResult.err
	}

	// First check if there's already a receiver for this workflow/topic to avoid unnecessary database load
	payload := fmt.Sprintf("%s::%s", destinationID, topic)
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	_, loaded := s.workflowNotificationsMap.LoadOrStore(payload, cond)
	if loaded {
		cond.L.Unlock()
		s.logger.Error("Receive already called for workflow", "destination_id", destinationID)
		return nil, newWorkflowConflictIDError(destinationID)
	}
	repollChannel := make(chan struct{}, 1)
	s.workflowNotificationRepollMap.LoadOrStore(payload, repollChannel)
	defer func() {
		// Clean up the condition variable after we're done and broadcast to wake up any waiting goroutines
		cond.Broadcast()
		s.workflowNotificationsMap.Delete(payload)
		s.workflowNotificationRepollMap.Delete(payload)
	}()

	// Now check if there is already a message available in the database.
	// If not, we'll wait for a notification and timeout
	var exists bool
	query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s.notifications WHERE destination_uuid = $1 AND topic = $2)`, pgx.Identifier{s.schema}.Sanitize())
	err = s.pool.QueryRow(ctx, query, destinationID, topic).Scan(&exists)
	if err != nil {
		cond.L.Unlock()
		return nil, fmt.Errorf("failed to check message: %w", err)
	}
	var timeoutOccurred bool

	// Create the waiting goroutine once (only if !exists, so we don't attempt to unlock twice)
	done := make(chan struct{})
	if !exists {
		go func() {
			// This is the only place we unlock the condition variable if the value did not exist
			// Because of the deferred Broadcast, we'll eventually hit this before returning
			defer cond.L.Unlock()
			cond.Wait()
			close(done)
		}()
	} else {
		cond.L.Unlock()
	}

loop:
	for !exists {
		timeout, err := s.sleep(ctx, sleepInput{
			duration:  input.Timeout,
			skipSleep: true,
			stepID:    &sleepStepID,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to sleep before recv timeout: %w", err)
		}

		select {
		case <-done:
			break loop
		case <-time.After(timeout):
			timeoutOccurred = true
			s.logger.Warn("Recv() timeout reached", "payload", payload, "timeout", input.Timeout)
			break loop
		case <-repollChannel:
			s.logger.Warn("Receive polling after repoll channel signal", "payload", payload)
			// We were instructed to poll again because the connection was disconnected
			err = s.pool.QueryRow(ctx, query, destinationID, topic).Scan(&exists)
			if err != nil {
				return nil, fmt.Errorf("failed to check message: %w", err)
			}
			// Restart at the beginning of the loop. If the value was found, we'll exit the loop and process to consuming the value.
			continue
		case <-ctx.Done():
			s.logger.Warn("Recv() context cancelled", "payload", payload, "cause", context.Cause(ctx))
			return nil, ctx.Err()
		}
	}

	// Capture start time before finding and deleting the message
	startTime := time.Now()

	// Find the oldest message and delete it atomically
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	// Use message_uuid so we delete exactly one row; created_at_epoch_ms can match multiple rows when inserts occur in the same millisecond.
	query = fmt.Sprintf(`
    WITH oldest_entry AS (
        SELECT message_uuid, message
        FROM %s.notifications
        WHERE destination_uuid = $1 AND topic = $2
        ORDER BY created_at_epoch_ms ASC
        LIMIT 1
    )
    DELETE FROM %s.notifications
    WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
    RETURNING message`, pgx.Identifier{s.schema}.Sanitize(), pgx.Identifier{s.schema}.Sanitize())

	var messageString *string
	err = tx.QueryRow(ctx, query, destinationID, topic).Scan(&messageString)
	if err != nil {
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("failed to consume message: %w", err)
		}
	}

	// Record the operation result (with encoded message string)
	completedTime := time.Now()
	recordInput := recordOperationResultDBInput{
		workflowID:  destinationID,
		stepID:      stepID,
		stepName:    functionName,
		output:      messageString,
		tx:          tx,
		startedAt:   startTime,
		completedAt: completedTime,
	}

	// Record an error if no message found and timeout occurred
	if timeoutOccurred && messageString == nil {
		recordInput.err = newTimeoutError(destinationID, functionName, fmt.Sprintf("no message received within %v", input.Timeout))
	}

	err = s.recordOperationResult(ctx, recordInput)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return the message string pointer and the timeout error if any
	return messageString, recordInput.err
}

type WorkflowSetEventInput struct {
	Key     string
	Message any
	tx      pgx.Tx
}

func (s *sysDB) setEvent(ctx context.Context, input WorkflowSetEventInput) error {
	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return newStepExecutionError("", "DBOS.setEvent", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// input.Message is already encoded *string from the typed layer
	// Insert or update the event using UPSERT
	insertQuery := fmt.Sprintf(`INSERT INTO %s.workflow_events (workflow_uuid, key, value)
					VALUES ($1, $2, $3)
					ON CONFLICT (workflow_uuid, key)
					DO UPDATE SET value = EXCLUDED.value`, pgx.Identifier{s.schema}.Sanitize())

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, insertQuery, wfState.workflowID, input.Key, input.Message)
	} else {
		_, err = s.pool.Exec(ctx, insertQuery, wfState.workflowID, input.Key, input.Message)
	}
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Record event in workflow_events_history
	insertHistoryQuery := fmt.Sprintf(`INSERT INTO %s.workflow_events_history (workflow_uuid, function_id, key, value)
					VALUES ($1, $2, $3, $4)
					ON CONFLICT (workflow_uuid, function_id, key)
					DO UPDATE SET value = EXCLUDED.value`, pgx.Identifier{s.schema}.Sanitize())

	if input.tx != nil {
		_, err = input.tx.Exec(ctx, insertHistoryQuery, wfState.workflowID, wfState.stepID, input.Key, input.Message)
	} else {
		_, err = s.pool.Exec(ctx, insertHistoryQuery, wfState.workflowID, wfState.stepID, input.Key, input.Message)
	}
	return err
}

func (s *sysDB) getEvent(ctx context.Context, input getEventInput) (*string, error) {
	functionName := "DBOS.getEvent"

	// Get workflow state from context (optional for GetEvent as we can get an event from outside a workflow)
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	var stepID int
	var sleepStepID int
	var isInWorkflow bool

	startTime := time.Now()
	if ok && wfState != nil {
		isInWorkflow = true
		if wfState.isWithinStep {
			return nil, newStepExecutionError(wfState.workflowID, functionName, fmt.Errorf("cannot call GetEvent within a step"))
		}
		stepID = wfState.nextStepID()
		sleepStepID = wfState.nextStepID() // We will use a sleep step to implement the timeout

		// Check if operation was already executed (only if in workflow)
		checkInput := checkOperationExecutionDBInput{
			workflowID: wfState.workflowID,
			stepID:     stepID,
			stepName:   functionName,
		}
		recordedResult, err := s.checkOperationExecution(ctx, checkInput)
		if err != nil {
			return nil, err
		}
		if recordedResult != nil {
			return recordedResult.output, recordedResult.err
		}
	}

	// Create notification payload and condition variable
	payload := fmt.Sprintf("%s::%s", input.TargetWorkflowID, input.Key)
	cond := sync.NewCond(&sync.Mutex{})
	cond.L.Lock()
	existingCond, loaded := s.workflowEventsMap.LoadOrStore(payload, cond)
	if loaded {
		cond.L.Unlock()
		// Reuse the existing condition variable
		cond = existingCond.(*sync.Cond)
	}
	repollChannel := make(chan struct{}, 1)
	s.workflowEventsRepollMap.LoadOrStore(payload, repollChannel)

	// Defer broadcast to ensure any waiting goroutines eventually unlock
	defer func() {
		cond.Broadcast()
		// Clean up the condition variable after we're done (Delete is a no-op if the key doesn't exist)
		s.workflowEventsMap.Delete(payload)
		s.workflowEventsRepollMap.Delete(payload)
	}()

	// Check if the event already exists in the database
	query := fmt.Sprintf(`SELECT value FROM %s.workflow_events WHERE workflow_uuid = $1 AND key = $2`, pgx.Identifier{s.schema}.Sanitize())
	var valueString *string
	var row pgx.Row
	var err error

	// Helper function to query the event and handle errors
	queryEvent := func() error {
		row = s.pool.QueryRow(ctx, query, input.TargetWorkflowID, input.Key)
		err = row.Scan(&valueString)
		if err != nil && err != pgx.ErrNoRows {
			if !loaded {
				cond.L.Unlock()
			}
			return fmt.Errorf("failed to query workflow event: %w", err)
		}
		return nil
	}

	if err := queryEvent(); err != nil {
		return nil, err
	}

	var timeoutOccurred bool
	if valueString == nil {
		// Start a goroutine to wait for the event to be set
		// This goroutine is responsible for unlocking the CV, which will happen whenever the event is set (through either the deferred Broadcast or from the notification listener)
		done := make(chan struct{})
		go func() {
			if !loaded {
				defer cond.L.Unlock()
			}
			cond.Wait()
			close(done)
		}()

	loop:
		for valueString == nil {
			// Wait for notification with timeout using condition variable
			timeout := input.Timeout
			if isInWorkflow {
				timeout, err = s.sleep(ctx, sleepInput{
					duration:  input.Timeout,
					skipSleep: true,
					stepID:    &sleepStepID,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to sleep before getEvent timeout: %w", err)
				}
			}

			select {
			case <-done:
				// Received notification
				if err := queryEvent(); err != nil {
					return nil, err
				}
				break loop
			case <-time.After(timeout):
				timeoutOccurred = true
				s.logger.Warn("GetEvent() timeout reached", "target_workflow_id", input.TargetWorkflowID, "key", input.Key, "timeout", input.Timeout)
				// Check if the event exists in the database -- we never know
				if err := queryEvent(); err != nil {
					return nil, err
				}
				break loop
			case <-repollChannel:
				// We were instructed to poll again because the connection was disconnected
				if err := queryEvent(); err != nil {
					return nil, err
				}
				// Restart at the beginning of the loop.
				// If the value was found, we'll exit the loop
				continue
			case <-ctx.Done():
				s.logger.Warn("GetEvent() context cancelled", "target_workflow_id", input.TargetWorkflowID, "key", input.Key, "cause", context.Cause(ctx))
				if !loaded {
					cond.L.Unlock()
				}
				return nil, ctx.Err()
			}
		}
	} else {
		if !loaded {
			cond.L.Unlock()
		}
	}

	// Record the operation result if this is called within a workflow
	var timeoutErr error
	if isInWorkflow {
		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowID:  wfState.workflowID,
			stepID:      stepID,
			stepName:    functionName,
			output:      valueString,
			err:         nil,
			startedAt:   startTime,
			completedAt: completedTime,
		}

		// Record an error if no event found and timeout occurred
		if timeoutOccurred && valueString == nil {
			recordInput.err = newTimeoutError(wfState.workflowID, functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
			timeoutErr = recordInput.err
		}

		err = s.recordOperationResult(ctx, recordInput)
		if err != nil {
			return nil, err
		}
	} else {
		// If not in workflow and timeout occurred with no event found, return error
		if timeoutOccurred && valueString == nil {
			timeoutErr = newTimeoutError("", functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
		}
	}

	// Return the value string pointer and the timeout error if any
	return valueString, timeoutErr
}

/*******************************/
/******* STREAMS ********/
/*******************************/

type writeStreamDBInput struct {
	Key   string
	Value *string // Already serialized
	tx    pgx.Tx
}

type readStreamDBInput struct {
	WorkflowID string
	Key        string
	FromOffset int
}

type streamEntry struct {
	Value  string
	Offset int
}

func (s *sysDB) writeStream(ctx context.Context, input writeStreamDBInput) error {
	// Get workflow state from context
	wfState, ok := ctx.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return fmt.Errorf("workflow state not found in context: are you running this within a workflow?")
	}

	// When no transaction is provided, run queries on the pool directly (no transaction).
	tx := input.tx
	queryRow := func(ctx context.Context, sql string, args ...any) pgx.Row {
		if tx != nil {
			return tx.QueryRow(ctx, sql, args...)
		}
		return s.pool.QueryRow(ctx, sql, args...)
	}

	exec := func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		if tx != nil {
			return tx.Exec(ctx, sql, args...)
		}
		return s.pool.Exec(ctx, sql, args...)
	}

	checkClosedQuery := fmt.Sprintf(`SELECT 1 FROM %s.streams 
		WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1`,
		pgx.Identifier{s.schema}.Sanitize())

	getOffsetQuery := fmt.Sprintf(`SELECT COALESCE(MAX("offset"), -1) + 1 FROM %s.streams 
		WHERE workflow_uuid = $1 AND key = $2`,
		pgx.Identifier{s.schema}.Sanitize())

	insertQuery := fmt.Sprintf(`INSERT INTO %s.streams (workflow_uuid, key, value, "offset", function_id)
		VALUES ($1, $2, $3, $4, $5)`,
		pgx.Identifier{s.schema}.Sanitize())

	var err error
	var exists int
	var nextOffset int

	err = queryRow(ctx, checkClosedQuery, wfState.workflowID, input.Key, _DBOS_STREAM_CLOSED_SENTINEL).Scan(&exists)
	if err == nil && exists == 1 {
		return fmt.Errorf("stream '%s' is already closed", input.Key)
	} else if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check stream status: %w", err)
	}

	err = queryRow(ctx, getOffsetQuery, wfState.workflowID, input.Key).Scan(&nextOffset)
	if err != nil {
		return fmt.Errorf("failed to get next offset: %w", err)
	}

	_, err = exec(ctx, insertQuery, wfState.workflowID, input.Key, input.Value, nextOffset, wfState.stepID)
	if err != nil {
		return fmt.Errorf("failed to insert stream entry: %w", err)
	}

	return nil
}

// readStream reads stream entries starting from a given offset.
// Returns the entries, whether the stream is closed, and any error.
func (s *sysDB) readStream(ctx context.Context, input readStreamDBInput) ([]streamEntry, bool, error) {
	query := fmt.Sprintf(`SELECT value, "offset" FROM %s.streams 
		WHERE workflow_uuid = $1 AND key = $2 AND "offset" >= $3
		ORDER BY "offset" ASC`,
		pgx.Identifier{s.schema}.Sanitize())

	rows, err := s.pool.Query(ctx, query, input.WorkflowID, input.Key, input.FromOffset)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query stream: %w", err)
	}
	defer rows.Close()

	var entries []streamEntry
	closed := false

	for rows.Next() {
		var value string
		var offset int
		if err := rows.Scan(&value, &offset); err != nil {
			return nil, false, fmt.Errorf("failed to scan stream entry: %w", err)
		}

		if value == _DBOS_STREAM_CLOSED_SENTINEL {
			closed = true
			break
		}

		entries = append(entries, streamEntry{
			Value:  value,
			Offset: offset,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("error iterating stream entries: %w", err)
	}

	return entries, closed, nil
}

/*******************************/
/******* QUEUES ********/
/*******************************/

type dequeuedWorkflow struct {
	id    string
	name  string
	input *string
}

type dequeueWorkflowsInput struct {
	queue              WorkflowQueue
	executorID         string
	applicationVersion string
	queuePartitionKey  string
}

func (s *sysDB) dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error) {
	// Begin transaction with snapshot isolation
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Set transaction isolation level to repeatable read (similar to snapshot isolation)
	_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		return nil, fmt.Errorf("failed to set transaction isolation level: %w", err)
	}

	// First check the rate limiter
	var numRecentQueries int
	if input.queue.RateLimit != nil {
		// Calculate the cutoff time: current time minus limiter period
		cutoffTimeMs := time.Now().Add(-input.queue.RateLimit.Period).UnixMilli()

		// Count workflows that have started in the limiter period
		limiterQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %s.workflow_status
		WHERE queue_name = $1
		  AND status != $2
		  AND started_at_epoch_ms > $3`, pgx.Identifier{s.schema}.Sanitize())

		limiterArgs := []any{input.queue.Name, WorkflowStatusEnqueued, cutoffTimeMs}
		if len(input.queuePartitionKey) > 0 {
			limiterQuery += ` AND queue_partition_key = $4`
			limiterArgs = append(limiterArgs, input.queuePartitionKey)
		}

		err := tx.QueryRow(ctx, limiterQuery, limiterArgs...).Scan(&numRecentQueries)
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}

		if numRecentQueries >= input.queue.RateLimit.Limit {
			return []dequeuedWorkflow{}, nil
		}
	}

	// Calculate max_tasks based on concurrency limits
	maxTasks := input.queue.MaxTasksPerIteration

	if input.queue.WorkerConcurrency != nil || input.queue.GlobalConcurrency != nil {
		// Count pending workflows by executor
		pendingQuery := fmt.Sprintf(`
			SELECT executor_id, COUNT(*) as task_count
			FROM %s.workflow_status
			WHERE queue_name = $1 AND status = $2`, pgx.Identifier{s.schema}.Sanitize())

		pendingArgs := []any{input.queue.Name, WorkflowStatusPending}
		if len(input.queuePartitionKey) > 0 {
			pendingQuery += ` AND queue_partition_key = $3`
			pendingArgs = append(pendingArgs, input.queuePartitionKey)
		}
		pendingQuery += ` GROUP BY executor_id`

		rows, err := tx.Query(ctx, pendingQuery, pendingArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to query pending workflows: %w", err)
		}
		defer rows.Close()

		pendingWorkflowsDict := make(map[string]int)
		for rows.Next() {
			var executorIDRow string
			var taskCount int
			if err := rows.Scan(&executorIDRow, &taskCount); err != nil {
				return nil, fmt.Errorf("failed to scan pending workflow row: %w", err)
			}
			pendingWorkflowsDict[executorIDRow] = taskCount
		}

		localPendingWorkflows := pendingWorkflowsDict[input.executorID]

		// Check worker concurrency limit
		if input.queue.WorkerConcurrency != nil {
			workerConcurrency := *input.queue.WorkerConcurrency
			if localPendingWorkflows > workerConcurrency {
				s.logger.Warn("Local pending workflows on queue exceeds worker concurrency limit", "local_pending", localPendingWorkflows, "queue_name", input.queue.Name, "concurrency_limit", workerConcurrency)
			}
			availableWorkerTasks := max(workerConcurrency-localPendingWorkflows, 0)
			maxTasks = availableWorkerTasks
		}

		// Check global concurrency limit
		if input.queue.GlobalConcurrency != nil {
			globalPendingWorkflows := 0
			for _, count := range pendingWorkflowsDict {
				globalPendingWorkflows += count
			}

			concurrency := *input.queue.GlobalConcurrency
			if globalPendingWorkflows > concurrency {
				s.logger.Warn("Total pending workflows on queue exceeds global concurrency limit", "total_pending", globalPendingWorkflows, "queue_name", input.queue.Name, "concurrency_limit", concurrency)
			}
			availableTasks := max(concurrency-globalPendingWorkflows, 0)
			if availableTasks < maxTasks {
				maxTasks = availableTasks
			}
		}
	}

	if maxTasks <= 0 {
		return nil, nil
	}

	// Build the query to select workflows for dequeueing
	var query string
	queryArgs := []any{input.queue.Name, WorkflowStatusEnqueued, input.applicationVersion}
	query = fmt.Sprintf(`
			SELECT workflow_uuid
			FROM %s.workflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND (application_version = $3 OR application_version IS NULL)`, pgx.Identifier{s.schema}.Sanitize())

	// Add partition key filter if provided
	if len(input.queuePartitionKey) > 0 {
		query += ` AND queue_partition_key = $4`
		queryArgs = append(queryArgs, input.queuePartitionKey)
	}

	if input.queue.PriorityEnabled {
		query += ` ORDER BY priority ASC, created_at ASC`
	} else {
		query += ` ORDER BY created_at ASC`
	}

	// Use SKIP LOCKED when no global concurrency is set to avoid blocking,
	// otherwise use NOWAIT to ensure consistent view across processes
	skipLocks := input.queue.GlobalConcurrency == nil
	var lockClause string
	if skipLocks {
		lockClause = "FOR UPDATE SKIP LOCKED"
	} else {
		lockClause = "FOR UPDATE NOWAIT"
	}
	query += fmt.Sprintf(" %s", lockClause)

	if maxTasks >= 0 {
		query += fmt.Sprintf(" LIMIT %d", int(maxTasks))
	}

	// Execute the query to get workflow IDs
	rows, err := tx.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}
	defer rows.Close()

	var dequeuedIDs []string
	for rows.Next() {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			s.logger.Warn("DequeueWorkflows context cancelled while reading dequeue results", "cause", context.Cause(ctx))
			return nil, ctx.Err()
		default:
		}
		var workflowID string
		if err := rows.Scan(&workflowID); err != nil {
			return nil, fmt.Errorf("failed to scan workflow ID: %w", err)
		}
		dequeuedIDs = append(dequeuedIDs, workflowID)
	}

	if len(dequeuedIDs) > 0 {
		s.logger.Debug("attempting to dequeue task(s)", "queueName", input.queue.Name, "numTasks", len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIDs {
		// If we have a limiter, stop dequeueing workflows when the number of workflows started this period exceeds the limit.
		if input.queue.RateLimit != nil {
			if len(retWorkflows)+numRecentQueries >= input.queue.RateLimit.Limit {
				break
			}
		}
		retWorkflow := dequeuedWorkflow{
			id: id,
		}

		// Update workflow status to PENDING and return name and inputs
		updateQuery := fmt.Sprintf(`
			UPDATE %s.workflow_status
			SET status = $1,
			    application_version = $2,
			    executor_id = $3,
			    started_at_epoch_ms = $4,
			    workflow_deadline_epoch_ms = CASE
			        WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
			        THEN (EXTRACT(epoch FROM NOW()) * 1000)::BIGINT + workflow_timeout_ms
			        ELSE workflow_deadline_epoch_ms
			    END
			WHERE workflow_uuid = $5
			RETURNING name, inputs`, pgx.Identifier{s.schema}.Sanitize())

		err := tx.QueryRow(ctx, updateQuery,
			WorkflowStatusPending,
			input.applicationVersion,
			input.executorID,
			time.Now().UnixMilli(),
			id).Scan(&retWorkflow.name, &retWorkflow.input)
		if err != nil {
			return nil, fmt.Errorf("failed to update workflow %s during dequeue: %w", id, err)
		}

		retWorkflows = append(retWorkflows, retWorkflow)
	}

	// Commit only if workflows were dequeued. Avoids WAL bloat and XID advancement.
	if len(retWorkflows) > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return retWorkflows, nil
}

func (s *sysDB) clearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	query := fmt.Sprintf(`UPDATE %s.workflow_status
			  SET status = $1, started_at_epoch_ms = NULL
			  WHERE workflow_uuid = $2
			    AND queue_name IS NOT NULL
			    AND status = $3`, pgx.Identifier{s.schema}.Sanitize())

	commandTag, err := s.pool.Exec(ctx, query,
		WorkflowStatusEnqueued,
		workflowID,
		WorkflowStatusPending)

	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment for workflow %s: %w", workflowID, err)
	}

	// If no rows were affected, the workflow is not anymore in the queue or was already completed
	return commandTag.RowsAffected() > 0, nil
}

// getQueuePartitions returns all unique partition keys for enqueued workflows in a queue.
func (s *sysDB) getQueuePartitions(ctx context.Context, queueName string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT queue_partition_key
		FROM %s.workflow_status
		WHERE queue_name = $1
		  AND status = $2
		  AND queue_partition_key IS NOT NULL`, pgx.Identifier{s.schema}.Sanitize())

	rows, err := s.pool.Query(ctx, query, queueName, WorkflowStatusEnqueued)
	if err != nil {
		return nil, fmt.Errorf("failed to query queue partitions: %w", err)
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var partitionKey string
		if err := rows.Scan(&partitionKey); err != nil {
			return nil, fmt.Errorf("failed to scan partition key: %w", err)
		}
		partitions = append(partitions, partitionKey)
	}

	return partitions, nil
}

/*******************************/
/******* METRICS ********/
/*******************************/

type metricData struct {
	MetricName string  `json:"metric_name"` // step name or workflow name
	MetricType string  `json:"metric_type"` // workflow_count, step_count, etc
	Value      float64 `json:"value"`
}

func (s *sysDB) getMetrics(ctx context.Context, startTime, endTime string) ([]metricData, error) {
	// Parse ISO timestamp strings to time.Time
	startTimeParsed, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		return nil, fmt.Errorf("invalid start_time format: %w", err)
	}
	endTimeParsed, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		return nil, fmt.Errorf("invalid end_time format: %w", err)
	}

	// Convert to epoch milliseconds
	startEpochMs := startTimeParsed.UnixMilli()
	endEpochMs := endTimeParsed.UnixMilli()

	var metrics []metricData

	// Query workflow metrics
	workflowMetrics, err := s.getMetricWorkflowCount(ctx, startEpochMs, endEpochMs)
	if err != nil {
		return nil, err
	}
	metrics = append(metrics, workflowMetrics...)

	// Query step metrics
	stepMetrics, err := s.getMetricStepCount(ctx, startEpochMs, endEpochMs)
	if err != nil {
		return nil, err
	}
	metrics = append(metrics, stepMetrics...)

	return metrics, nil
}

func (s *sysDB) getMetricWorkflowCount(ctx context.Context, startEpochMs, endEpochMs int64) ([]metricData, error) {
	workflowQuery := fmt.Sprintf(`
		SELECT name, COUNT(workflow_uuid) as count
		FROM %s.workflow_status
		WHERE created_at >= $1 AND created_at < $2
		GROUP BY name
	`, pgx.Identifier{s.schema}.Sanitize())

	rows, err := s.pool.Query(ctx, workflowQuery, startEpochMs, endEpochMs)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow metrics: %w", err)
	}
	defer rows.Close()

	var metrics []metricData
	for rows.Next() {
		var workflowName string
		var workflowCount int64
		if err := rows.Scan(&workflowName, &workflowCount); err != nil {
			return nil, fmt.Errorf("failed to scan workflow metric: %w", err)
		}
		metrics = append(metrics, metricData{
			MetricType: "workflow_count",
			MetricName: workflowName,
			Value:      float64(workflowCount),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workflow metrics: %w", err)
	}

	return metrics, nil
}

func (s *sysDB) getMetricStepCount(ctx context.Context, startEpochMs, endEpochMs int64) ([]metricData, error) {
	stepQuery := fmt.Sprintf(`
		SELECT function_name, COUNT(*) as count
		FROM %s.operation_outputs
		WHERE completed_at_epoch_ms >= $1 AND completed_at_epoch_ms < $2
		GROUP BY function_name
	`, pgx.Identifier{s.schema}.Sanitize())

	rows, err := s.pool.Query(ctx, stepQuery, startEpochMs, endEpochMs)
	if err != nil {
		return nil, fmt.Errorf("failed to query step metrics: %w", err)
	}
	defer rows.Close()

	var metrics []metricData
	for rows.Next() {
		var stepName string
		var stepCount int64
		if err := rows.Scan(&stepName, &stepCount); err != nil {
			return nil, fmt.Errorf("failed to scan step metric: %w", err)
		}
		metrics = append(metrics, metricData{
			MetricType: "step_count",
			MetricName: stepName,
			Value:      float64(stepCount),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating step metrics: %w", err)
	}

	return metrics, nil
}

/*******************************/
/******* UTILS ********/
/*******************************/

func isCockroachDB(ctx context.Context, conn *pgx.Conn) bool {
	var version string
	err := conn.QueryRow(ctx, "SHOW CLUSTER SETTING version").Scan(&version)
	return err == nil
}

// dropDatabaseIfExists drops a database in a way that works with both PostgreSQL and CockroachDB.
// For CockroachDB, it terminates active connections first, then drops the database.
// For PostgreSQL, it uses the WITH (FORCE) syntax.
func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	crdb := isCockroachDB(ctx, conn)

	sanitizedDBName := pgx.Identifier{dbName}.Sanitize()

	var err error
	if crdb {
		// In CockroachDB, we can't force drop, so we terminate connections manually
		// Try to terminate connections to the target database
		terminateQuery := `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1 AND pid != pg_backend_pid()`
		_, _ = conn.Exec(ctx, terminateQuery, dbName) // Ignore errors, proceed anyway

		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s", sanitizedDBName)
		_, err = conn.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop database %s: %w", dbName, err)
		}
	} else {
		// For PostgreSQL, use WITH (FORCE) to drop even with active connections
		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", sanitizedDBName)
		_, err = conn.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop database %s: %w", dbName, err)
		}
	}

	return nil
}

func (s *sysDB) resetSystemDB(ctx context.Context) error {
	// Get the current database configuration from the pool
	config := s.pool.Config()
	if config == nil || config.ConnConfig == nil {
		return fmt.Errorf("failed to get pool configuration")
	}

	// Extract the database name before closing the pool
	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in pool configuration")
	}

	// Close the current pool before dropping the database
	s.pool.Close()

	// Create a new connection configuration pointing to the postgres database
	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	// Connect to the postgres database
	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	// Drop the database using the helper function
	err = dropDatabaseIfExists(ctx, conn, dbName)
	if err != nil {
		return err
	}

	return nil
}

type queryBuilder struct {
	setClauses   []string
	whereClauses []string
	args         []any
	argCounter   int
}

func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		setClauses:   make([]string, 0),
		whereClauses: make([]string, 0),
		args:         make([]any, 0),
		argCounter:   0,
	}
}

func (qb *queryBuilder) addSet(column string, value any) {
	qb.argCounter++
	qb.setClauses = append(qb.setClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addSetRaw(clause string) {
	qb.setClauses = append(qb.setClauses, clause)
}

func (qb *queryBuilder) addWhere(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereIsNotNull(column string) {
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s IS NOT NULL", column))
}

func (qb *queryBuilder) addWhereLike(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s LIKE $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereAny(column string, values any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s = ANY($%d)", column, qb.argCounter))
	qb.args = append(qb.args, values)
}

// addWhereLikeAny adds (column LIKE $n OR column LIKE $n+1 OR ...) for each prefix+suffix pattern.
func (qb *queryBuilder) addWhereLikeAny(column string, prefixes []string, suffix string) {
	if len(prefixes) == 0 {
		return
	}
	ors := make([]string, len(prefixes))
	for i, p := range prefixes {
		qb.argCounter++
		ors[i] = fmt.Sprintf("%s LIKE $%d", column, qb.argCounter)
		qb.args = append(qb.args, p+suffix)
	}
	qb.whereClauses = append(qb.whereClauses, "("+strings.Join(ors, " OR ")+")")
}

func (qb *queryBuilder) addWhereGreaterEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s >= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLessEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s <= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func backoffWithJitter(retryAttempt int) time.Duration {
	exp := float64(_DB_CONNECTION_RETRY_BASE_DELAY) * math.Pow(_DB_CONNECTION_RETRY_FACTOR, float64(retryAttempt))
	// cap backoff to max number of retries, then do a fixed time delay
	// expected retryAttempt to initially be 0, so >= used
	// cap delay to maximum of _DB_CONNECTION_MAX_DELAY milliseconds
	if retryAttempt >= _DB_CONNECTION_RETRY_MAX_RETRIES || exp > float64(_DB_CONNECTION_MAX_DELAY) {
		exp = float64(_DB_CONNECTION_MAX_DELAY)
	}

	// want randomization between +-25% of exp
	jitter := 0.75 + rand.Float64()*0.5 // #nosec G404 -- trivial use of math/rand
	return time.Duration(exp * jitter)
}

// maskPassword replaces the password in a database URL with asterisks
func maskPassword(dbURL string) (string, error) {
	parsedURL, err := url.Parse(dbURL)
	if err == nil && parsedURL.Scheme != "" {

		// Check if there is user info with a password
		if parsedURL.User != nil {
			username := parsedURL.User.Username()
			_, hasPassword := parsedURL.User.Password()
			if hasPassword {
				// Manually construct the URL with masked password to avoid encoding
				maskedURL := parsedURL.Scheme + "://" + username + ":***@" + parsedURL.Host + parsedURL.Path
				if parsedURL.RawQuery != "" {
					maskedURL += "?" + parsedURL.RawQuery
				}
				if parsedURL.Fragment != "" {
					maskedURL += "#" + parsedURL.Fragment
				}
				return maskedURL, nil
			}
		}

		return parsedURL.String(), nil
	}

	// If URL parsing failed or no scheme, try key-value format (libpq connection string)
	return maskPasswordInKeyValueFormat(dbURL), nil
}

// maskPasswordInKeyValueFormat masks password in libpq-style key-value connection strings
// Format: "user=foo password=bar database=db host=localhost"
// Supports all spacing variations: password=value, password =value, password= value, password = value
func maskPasswordInKeyValueFormat(connStr string) string {
	// Match password=value (case insensitive, handles spaces around =)
	// Pattern matches: password (case insensitive), optional spaces, =, optional spaces, then value until next space or end
	re := regexp.MustCompile(`(?i)password\s*=\s*[^\s]+`)
	return re.ReplaceAllString(connStr, "password=***")
}

/*******************************/
/******* RETRIER ********/
/*******************************/

func isRetryablePGError(err error, logger *slog.Logger) bool {
	if err == nil {
		return false
	}

	// If tx is closed (because failure happened between pgx trying to commit/rollback and setting tx.closed)
	// pgx will always return pgx.ErrTxClosed again.
	// This is only retryable if the caller retries with a new transaction object.
	// Otherwise, retrying with the same closed transaction will always fail.
	if errors.Is(err, pgx.ErrTxClosed) {
		if logger != nil {
			logger.Warn("Transaction is closed, retrying requires a new transaction object", "error", err)
		}
		return true
	}

	// PostgreSQL codes indicating connection/admin shutdown etc.
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		switch pgerr.Code {
		case pgerrcode.ConnectionException,
			pgerrcode.ConnectionDoesNotExist,
			pgerrcode.ConnectionFailure,
			pgerrcode.SQLClientUnableToEstablishSQLConnection,
			pgerrcode.SQLServerRejectedEstablishmentOfSQLConnection,
			pgerrcode.AdminShutdown,
			pgerrcode.CrashShutdown,
			pgerrcode.CannotConnectNow:
			return true
		}
	}

	// pgx aggregate for connect attempts:
	var cerr *pgconn.ConnectError
	if errors.As(err, &cerr) {
		return true
	}

	// Match most "connection closed" cases
	if errors.Is(err, io.EOF) || strings.Contains(err.Error(), "conn closed") {
		return true
	}

	// Net-level errors
	var nerr net.Error
	return errors.As(err, &nerr)
}

// isRetryableTransaction returns true for PG error 40001 (SerializationFailure).
// Used when chaining with isRetryablePGError for CockroachDB transaction retries.
func isRetryableTransaction(err error, _ *slog.Logger) bool {
	if err == nil {
		return false
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == pgerrcode.SerializationFailure {
		return true
	}
	return false
}

// retryConfig holds the configuration for a retry operation
type retryConfig struct {
	maxRetries          int // -1 for infinite retries
	baseDelay           time.Duration
	maxDelay            time.Duration
	backoffFactor       float64
	jitterMin           float64
	jitterMax           float64
	retryConditionChain []func(error, *slog.Logger) bool
	logger              *slog.Logger
}

// retryOption is a functional option for configuring retry behavior
type retryOption func(*retryConfig)

// withRetrierLogger sets the logger for the retrier
func withRetrierLogger(logger *slog.Logger) retryOption {
	return func(c *retryConfig) {
		c.logger = logger
	}
}

// withRetryCondition appends the given condition functions to the retry condition chain.
// An error is retryable if any function in the chain returns true.
func withRetryCondition(fns ...func(error, *slog.Logger) bool) retryOption {
	return func(c *retryConfig) {
		c.retryConditionChain = append(c.retryConditionChain, fns...)
	}
}

// retry executes a function with retry logic using functional options
func retry(ctx context.Context, fn func() error, options ...retryOption) error {
	// Start with default configuration: chain of one condition (isRetryablePGError)
	config := &retryConfig{
		maxRetries:          -1,
		baseDelay:           100 * time.Millisecond,
		maxDelay:            30 * time.Second,
		backoffFactor:       2.0,
		jitterMin:           0.95,
		jitterMax:           1.05,
		retryConditionChain: []func(error, *slog.Logger) bool{isRetryablePGError},
	}

	// Apply options
	for _, opt := range options {
		opt(config)
	}

	var lastErr error
	delay := config.baseDelay
	attempt := 0

	for {
		lastErr = fn()

		// Success and rollback case
		if lastErr == nil {
			return nil
		}

		// Check if error is retryable (any condition in the chain returns true)
		retryable := false
		for _, cond := range config.retryConditionChain {
			if cond(lastErr, config.logger) {
				retryable = true
				break
			}
		}
		if !retryable {
			if config.logger != nil {
				config.logger.Debug("Non-retryable error encountered", "error", lastErr)
			}
			return lastErr
		}

		// Check if we should continue retrying
		// If maxRetries is -1, retry indefinitely
		if config.maxRetries >= 0 && attempt >= config.maxRetries {
			return lastErr
		}

		// Log retry attempt if logger is provided
		if config.logger != nil {
			config.logger.Debug("Retrying operation",
				"attempt", attempt+1,
				"max_retries", config.maxRetries,
				"delay", delay,
				"error", lastErr)
		}

		// Apply jitter to the delay
		jitterRange := config.jitterMax - config.jitterMin
		jitterFactor := config.jitterMin + rand.Float64()*jitterRange // #nosec G404 -- trivial use of math/rand
		jitteredDelay := time.Duration(float64(delay) * jitterFactor)

		// Wait before retrying with context cancellation support
		select {
		case <-time.After(jitteredDelay):
		case <-ctx.Done():
			if config.logger != nil {
				config.logger.Debug("Retry operation cancelled", "error", ctx.Err())
			}
			return ctx.Err()
		}

		// Calculate next delay with exponential backoff
		delay = min(time.Duration(float64(delay)*config.backoffFactor), config.maxDelay)

		attempt++
	}
}

// retryWithResult executes a function that returns a value with retry logic
// It uses the non-generic retry function under the hood
func retryWithResult[T any](ctx context.Context, fn func() (T, error), options ...retryOption) (T, error) {
	var result T
	var capturedErr error

	// Wrap the generic function to work with the non-generic retry
	wrappedFn := func() error {
		var err error
		result, err = fn()
		capturedErr = err
		return err
	}

	// Use the non-generic retry function
	err := retry(ctx, wrappedFn, options...)

	// Return the last result and error
	if err != nil {
		return result, capturedErr
	}
	return result, nil
}

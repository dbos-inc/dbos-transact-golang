package sysdb

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*******************************/
/******* INTERFACE ********/
/*******************************/

type SystemDatabase interface {
	// SysDB management
	Launch(ctx context.Context)
	Pool() Pool
	Dialect() Dialect
	// IsContentionError reports whether err is a lock/serialization contention
	// error for the active backend. See Dialect.IsContentionError.
	IsContentionError(err error) bool
	// Shutdown returns the names of sub-components still running when timeout expired.
	Shutdown(ctx context.Context, timeout time.Duration) []string
	ResetSystemDB(ctx context.Context) error

	// Workflows
	InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error)
	ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]models.WorkflowStatus, error)
	UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error
	SetWorkflowAttributes(ctx context.Context, input SetWorkflowAttributesDBInput) error
	AwaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*AwaitWorkflowResultOutput, error)
	CancelWorkflows(ctx context.Context, input CancelWorkflowsDBInput) ([]string, error)
	CancelAllBefore(ctx context.Context, cutoffTime time.Time) error
	DeleteWorkflows(ctx context.Context, input DeleteWorkflowsDBInput) error
	ResumeWorkflows(ctx context.Context, input ResumeWorkflowsDBInput) ([]string, error)
	ForkWorkflows(ctx context.Context, input ForkWorkflowsDBInput) ([]string, error)
	ForkFrom(ctx context.Context, input ForkFromDBInput) ([]string, error)

	GetDeduplicatedWorkflow(ctx context.Context, queueName, deduplicationID string) (*string, error)

	// Child workflows
	GetWorkflowChildren(ctx context.Context, input GetWorkflowChildrenDBInput) ([]models.WorkflowStatus, error)
	RecordChildWorkflow(ctx context.Context, input RecordChildWorkflowDBInput) error
	CheckChildWorkflow(ctx context.Context, workflowUUID string, functionID int, functionName string) (*string, error)

	// Steps
	RecordOperationResult(ctx context.Context, input RecordOperationResultDBInput) error
	CheckOperationExecution(ctx context.Context, input CheckOperationExecutionDBInput) (*RecordedResult, error)
	GetWorkflowSteps(ctx context.Context, input GetWorkflowStepsInput) ([]StepRow, error)

	// Aggregates
	GetWorkflowAggregates(ctx context.Context, input GetWorkflowAggregatesDBInput) ([]WorkflowAggregateRow, error)
	GetStepAggregates(ctx context.Context, input GetStepAggregatesDBInput) ([]StepAggregateRow, error)

	// Communication (special steps)
	Send(ctx context.Context, input WorkflowSendInput) error
	StartRecvListener(ctx context.Context, destinationID, topic string) (*NotificationWaiter, error)
	ConsumeMessage(ctx context.Context, tx Tx, destinationID, topic string) (*string, *string, error)
	SetEvent(ctx context.Context, input WorkflowSetEventInput) error
	StartEventListener(ctx context.Context, targetWorkflowID, key string) (*NotificationWaiter, error)
	GetEventValue(ctx context.Context, q Querier, targetWorkflowID, key string) (*string, *string, error)

	// Communication observability
	GetAllEvents(ctx context.Context, workflowID string) ([]EventRecord, error)
	GetAllNotifications(ctx context.Context, workflowID string) ([]NotificationRecord, error)
	GetAllStreamEntries(ctx context.Context, workflowID string) ([]StreamEntry, error)

	// Streams
	WriteStream(ctx context.Context, input WriteStreamDBInput) error
	ReadStream(ctx context.Context, input ReadStreamDBInput) ([]StreamEntry, bool, error)
	// StreamWakeChannel returns a channel signaled when new rows are written to
	// the given workflow's stream, plus a cleanup func to drop the registration.
	StreamWakeChannel(workflowID, key string) (chan struct{}, func())

	// Patches
	Patch(ctx context.Context, input PatchDBInput) (bool, error)
	DoesPatchExists(ctx context.Context, input PatchDBInput) (string, error)

	// Queues
	SetWorkflowDelay(ctx context.Context, input SetWorkflowDelayDBInput) error
	TransitionDelayedWorkflows(ctx context.Context) error
	DebounceDelayedWorkflow(ctx context.Context, input DebounceDelayedWorkflowDBInput) (*DebounceResult, error)
	DequeueWorkflows(ctx context.Context, input DequeueWorkflowsInput) ([]DequeuedWorkflow, error)
	ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error)
	GetQueuePartitions(ctx context.Context, queueName string) ([]string, error)

	// Database-backed queue registry (the queues table)
	GetQueue(ctx context.Context, name string) (*models.QueueConfig, error) // returns nil if the queue does not exist
	ListQueues(ctx context.Context) ([]models.QueueConfig, error)
	UpsertQueue(ctx context.Context, input UpsertQueueDBInput) (bool, error)
	UpdateQueueConfig(ctx context.Context, name string, mutate func(*models.QueueConfig) error) (*models.QueueConfig, error)
	DeleteQueue(ctx context.Context, name string) error

	// Garbage collection
	GarbageCollectWorkflows(ctx context.Context, input GarbageCollectWorkflowsInput) error

	// Metrics
	GetMetrics(ctx context.Context, startTime string, endTime string) ([]MetricData, error)

	// Schedules
	CreateSchedule(ctx context.Context, input CreateScheduleDBInput) error
	UpsertSchedule(ctx context.Context, input UpsertScheduleDBInput) error
	ListSchedules(ctx context.Context, input ListSchedulesDBInput) ([]models.WorkflowSchedule, error)
	UpdateSchedule(ctx context.Context, input UpdateScheduleDBInput) error
	UpdateScheduleLastFiredAt(ctx context.Context, scheduleName string, lastFiredAt time.Time) error
	DeleteSchedule(ctx context.Context, input DeleteScheduleDBInput) error
	BackfillSchedule(ctx context.Context, input BackfillScheduleDBInput) ([]string, error)
	TriggerSchedule(ctx context.Context, scheduleName string) (string, error)

	// Application versions
	CreateApplicationVersion(ctx context.Context, versionName string) error
	UpdateApplicationVersionTimestamp(ctx context.Context, versionName string, newTimestamp int64) error
	ListApplicationVersions(ctx context.Context) ([]VersionInfo, error)
	GetLatestApplicationVersion(ctx context.Context, tx Tx) (*VersionInfo, error)

	// Workflow export/import
	ExportWorkflow(ctx context.Context, workflowID string, exportChildren bool) ([]ExportedWorkflow, error)
	ImportWorkflow(ctx context.Context, workflows []ExportedWorkflow) error
}

// ExportedWorkflow contains all data for a single workflow, in a portable format suitable for
// exporting from one environment and importing into another.
type ExportedWorkflow struct {
	WorkflowStatus        map[string]any   `json:"workflow_status"`
	OperationOutputs      []map[string]any `json:"operation_outputs"`
	WorkflowEvents        []map[string]any `json:"workflow_events"`
	WorkflowEventsHistory []map[string]any `json:"workflow_events_history"`
	Streams               []map[string]any `json:"streams"`
}

type SysDB struct {
	pool                 Pool
	dialect              Dialect
	notificationLoopMu   sync.Mutex
	notificationLoopDone chan struct{}
	RecvNotifier         *notifyRegistry // recv waiters, keyed by "destinationID::topic"
	EventNotifier        *notifyRegistry // getEvent waiters, keyed by "targetWorkflowID::key"
	streamNotifier       *notifyRegistry // stream readers, keyed by "workflowID::key"
	logger               *slog.Logger
	encodeScheduledInput func(ctx context.Context, scheduledTime time.Time, scheduleContext json.RawMessage) (*string, string, error)
	schema               string
	launched             bool
	isCockroachDB        bool
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

//go:embed migrations/9_add_workflow_schedules.sql
var migration9SQL string

//go:embed migrations/10_add_notifications_pkey.sql
var migration10SQL string

//go:embed migrations/10_check_notifications_pkey_cockroach.sql
var migration10CheckCockroachSQL string

//go:embed migrations/10_add_notifications_pkey_cockroach.sql
var migration10AddCockroachSQL string

//go:embed migrations/11_add_serialization_columns.sql
var migration11SQL string

//go:embed migrations/12_add_notifications_consumed.sql
var migration12SQL string

//go:embed migrations/13_add_application_versions.sql
var migration13SQL string

//go:embed migrations/14_add_pgsql_client_functions.sql
var migration14SQL string

//go:embed migrations/15_add_workflow_schedule_columns.sql
var migration15SQL string

//go:embed migrations/16_add_delay_until.sql
var migration16SQL string

//go:embed migrations/17_add_workflow_schedule_queue_name.sql
var migration17SQL string

//go:embed migrations/18_add_was_forked_from.sql
var migration18SQL string

//go:embed migrations/19_add_operation_outputs_completed_at_index.sql
var migration19SQL string

//go:embed migrations/20_set_function_search_path.sql
var migration20SQL string

//go:embed migrations/21_create_queues_table.sql
var migration21SQL string

//go:embed migrations/22_drop_forked_from_index.sql
var migration22SQL string

//go:embed migrations/23_create_partial_forked_from_index.sql
var migration23SQL string

//go:embed migrations/24_drop_parent_workflow_id_index.sql
var migration24SQL string

//go:embed migrations/25_create_partial_parent_workflow_id_index.sql
var migration25SQL string

//go:embed migrations/26_drop_executor_id_index.sql
var migration26SQL string

//go:embed migrations/27_create_partial_dedup_id_index.sql
var migration27SQL string

//go:embed migrations/28_drop_dedup_id_constraint.sql
var migration28SQL string

//go:embed migrations/28_drop_dedup_id_constraint_cockroach.sql
var migration28CockroachSQL string

//go:embed migrations/29_create_pending_index.sql
var migration29SQL string

//go:embed migrations/30_create_failed_index.sql
var migration30SQL string

//go:embed migrations/31_drop_status_index.sql
var migration31SQL string

//go:embed migrations/32_create_in_flight_index.sql
var migration32SQL string

//go:embed migrations/33_add_rate_limited.sql
var migration33SQL string

//go:embed migrations/34_create_rate_limited_index.sql
var migration34SQL string

//go:embed migrations/35_drop_queue_status_started_index.sql
var migration35SQL string

//go:embed migrations/36_add_completed_at.sql
var migration36SQL string

//go:embed migrations/37_create_started_at_index.sql
var migration37SQL string

//go:embed migrations/38_update_enqueue_workflow.sql
var migration38SQL string

//go:embed migrations/38_set_enqueue_workflow_search_path.sql
var migration38SearchPathSQL string

//go:embed migrations/39_create_streams_trigger.sql
var migration39SQL string

//go:embed migrations/40_add_attributes.sql
var migration40SQL string

//go:embed migrations/41_add_schedule_name.sql
var migration41SQL string

//go:embed migrations/42_add_debounce_columns.sql
var migration42SQL string

type MigrationFile struct {
	Version int64
	SQL     string
	Online  bool
}

const (
	MigrationTable = "dbos_migrations"

	// Notification channels
	_DBOS_NOTIFICATIONS_CHANNEL   = "dbos_notifications_channel"
	_DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel"
	_DBOS_STREAMS_CHANNEL         = "dbos_streams_channel"

	// Stream sentinel value for closure
	StreamClosedSentinel = "__DBOS_STREAM_CLOSED__"

	// Database retry timeouts
	_DB_CONNECTION_RETRY_BASE_DELAY = 1 * time.Second
	_DB_CONNECTION_RETRY_FACTOR     = 2
	_DB_CONNECTION_MAX_DELAY        = 120 * time.Second
	DBRetryInterval                 = 1 * time.Second
)

// returns the CONCURRENTLY keyword for online index DDL.
func concurrentlyKw(isCockroach bool) string {
	if isCockroach {
		return ""
	}
	return "CONCURRENTLY"
}

// BuildMigrations renders the full list of migrations against the target schema.
func BuildMigrations(schema string, isCockroach bool) []MigrationFile {
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()

	migration1SQLProcessed := fmt.Sprintf(migration1SQL,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema,
		sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	if !isCockroach {
		migration1ListenNotifySQLProcessed := fmt.Sprintf(migration1ListenNotifySQL,
			sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
		migration1SQLProcessed = migration1SQLProcessed + "\n" + migration1ListenNotifySQLProcessed
	}

	c := concurrentlyKw(isCockroach)

	// Migration 20 is a Postgres-only function-hardening pass; on CockroachDB
	// it is a no-op (the version row still advances).
	migration20SQLProcessed := ""
	if !isCockroach {
		migration20SQLProcessed = fmt.Sprintf(migration20SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	}

	// Migration 28 drops the legacy uq_workflow_status_queue_name_dedup_id
	// constraint. CockroachDB exposes it as an index (DROP INDEX ... CASCADE);
	// Postgres exposes it as a table constraint (ALTER TABLE DROP CONSTRAINT).
	// This is a fast catalog op, so CONCURRENTLY is not used in either path.
	migration28File := migration28SQL
	if isCockroach {
		migration28File = migration28CockroachSQL
	}
	migration28SQLProcessed := fmt.Sprintf(migration28File, sanitizedSchema)

	// Migration 38 replaces enqueue_workflow with a signature adding
	// authenticated_user, authenticated_roles, and delay_until_epoch_ms. The
	// DROP/CREATE base runs everywhere; the trailing search_path hardening is
	// Postgres-only (CockroachDB rejects ALTER FUNCTION ... SET).
	migration38SQLProcessed := fmt.Sprintf(migration38SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	if !isCockroach {
		migration38SQLProcessed = migration38SQLProcessed + "\n" + fmt.Sprintf(migration38SearchPathSQL, sanitizedSchema)
	}

	// Migration 39 installs the streams notification trigger. Gated on
	// LISTEN/NOTIFY support, mirroring the migration 1 triggers; on CockroachDB
	// it is a no-op (the version row still advances).
	migration39SQLProcessed := ""
	if !isCockroach {
		migration39SQLProcessed = fmt.Sprintf(migration39SQL,
			sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)
	}

	return []MigrationFile{
		{Version: 1, SQL: migration1SQLProcessed},
		{Version: 2, SQL: fmt.Sprintf(migration2SQL, sanitizedSchema)},
		{Version: 3, SQL: fmt.Sprintf(migration3SQL, sanitizedSchema)},
		{Version: 4, SQL: fmt.Sprintf(migration4SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 5, SQL: fmt.Sprintf(migration5SQL, sanitizedSchema)},
		{Version: 6, SQL: fmt.Sprintf(migration6SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{Version: 7, SQL: fmt.Sprintf(migration7SQL, sanitizedSchema)},
		{Version: 8, SQL: fmt.Sprintf(migration8SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 9, SQL: fmt.Sprintf(migration9SQL, sanitizedSchema)},
		{Version: 10, SQL: fmt.Sprintf(migration10SQL, schema, sanitizedSchema)},
		{Version: 11, SQL: fmt.Sprintf(migration11SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{Version: 12, SQL: fmt.Sprintf(migration12SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 13, SQL: fmt.Sprintf(migration13SQL, sanitizedSchema)},
		{Version: 14, SQL: fmt.Sprintf(migration14SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{Version: 15, SQL: fmt.Sprintf(migration15SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{Version: 16, SQL: fmt.Sprintf(migration16SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 17, SQL: fmt.Sprintf(migration17SQL, sanitizedSchema)},
		{Version: 18, SQL: fmt.Sprintf(migration18SQL, sanitizedSchema)},
		{Version: 19, SQL: fmt.Sprintf(migration19SQL, sanitizedSchema)},
		{Version: 20, SQL: migration20SQLProcessed},
		{Version: 21, SQL: fmt.Sprintf(migration21SQL, sanitizedSchema)},
		{Version: 22, SQL: fmt.Sprintf(migration22SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 23, SQL: fmt.Sprintf(migration23SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 24, SQL: fmt.Sprintf(migration24SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 25, SQL: fmt.Sprintf(migration25SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 26, SQL: fmt.Sprintf(migration26SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 27, SQL: fmt.Sprintf(migration27SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 28, SQL: migration28SQLProcessed},
		{Version: 29, SQL: fmt.Sprintf(migration29SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 30, SQL: fmt.Sprintf(migration30SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 31, SQL: fmt.Sprintf(migration31SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 32, SQL: fmt.Sprintf(migration32SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 33, SQL: fmt.Sprintf(migration33SQL, sanitizedSchema)},
		{Version: 34, SQL: fmt.Sprintf(migration34SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 35, SQL: fmt.Sprintf(migration35SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 36, SQL: fmt.Sprintf(migration36SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 37, SQL: fmt.Sprintf(migration37SQL, c, sanitizedSchema), Online: !isCockroach},
		{Version: 38, SQL: migration38SQLProcessed},
		{Version: 39, SQL: migration39SQLProcessed},
		{Version: 40, SQL: fmt.Sprintf(migration40SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 41, SQL: fmt.Sprintf(migration41SQL, sanitizedSchema, sanitizedSchema)},
		{Version: 42, SQL: fmt.Sprintf(migration42SQL, sanitizedSchema, sanitizedSchema)},
	}
}

// ShouldMigrate reports whether any migration work remains for the schema.
// Returns true if the schema is missing, the dbos_migrations table is missing,
// or the recorded version is behind the latest.
func ShouldMigrate(ctx context.Context, pool *pgxpool.Pool, schema string, isCockroach bool) (bool, error) {
	var schemaExists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schema).Scan(&schemaExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if schema %s exists: %v", schema, err)
	}
	if !schemaExists {
		return true, nil
	}

	var tableExists bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, MigrationTable).Scan(&tableExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !tableExists {
		return true, nil
	}

	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", pgx.Identifier{schema}.Sanitize(), MigrationTable)
	err = pool.QueryRow(ctx, q).Scan(&currentVersion)
	if err != nil && err != pgx.ErrNoRows {
		return false, fmt.Errorf("failed to get current migration version: %v", err)
	}
	migrations := BuildMigrations(schema, isCockroach)
	return currentVersion < migrations[len(migrations)-1].Version, nil
}

// CleanupInvalidIndexes drops indexes left in an INVALID state by a prior
// failed CREATE INDEX CONCURRENTLY. Such indexes are not used by the planner
// but block recreating an index of the same name. Must be called before
// retrying an online migration.
func CleanupInvalidIndexes(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) error {
	q := `SELECT i.relname FROM pg_index ix
	      JOIN pg_class i ON i.oid = ix.indexrelid
	      JOIN pg_class t ON t.oid = ix.indrelid
	      JOIN pg_namespace n ON n.oid = t.relnamespace
	      WHERE NOT ix.indisvalid AND n.nspname = $1`
	rows, err := pool.Query(ctx, q, schema)
	if err != nil {
		return fmt.Errorf("failed to list invalid indexes: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan invalid index name: %v", err)
		}
		names = append(names, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to iterate invalid indexes: %v", err)
	}
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	for _, name := range names {
		if logger != nil {
			logger.Warn("dropping invalid index left by a prior failed migration", "schema", schema, "index", name)
		}
		dropQ := fmt.Sprintf(`DROP INDEX CONCURRENTLY IF EXISTS %s.%s`, sanitizedSchema, pgx.Identifier{name}.Sanitize())
		if _, err := pool.Exec(ctx, dropQ); err != nil {
			return fmt.Errorf("failed to drop invalid index %s.%s: %v", schema, name, err)
		}
	}
	return nil
}

func writeMigrationVersion(ctx context.Context, exec interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, schema string, version int64, lastApplied int64) error {
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()
	if lastApplied == 0 {
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (version) VALUES ($1)", sanitizedSchema, MigrationTable)
		if _, err := exec.Exec(ctx, insertQuery, version); err != nil {
			return fmt.Errorf("failed to insert migration version %d: %v", version, err)
		}
	} else {
		updateQuery := fmt.Sprintf("UPDATE %s.%s SET version = $1", sanitizedSchema, MigrationTable)
		if _, err := exec.Exec(ctx, updateQuery, version); err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", version, err)
		}
	}
	return nil
}

func RunMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, isCockroach bool, logger *slog.Logger) error {
	migrations := BuildMigrations(schema, isCockroach)
	sanitizedSchema := pgx.Identifier{schema}.Sanitize()

	// Schema + migrations table setup in a single short transaction.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)
	var schemaExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`,
		schema).Scan(&schemaExists); err != nil {
		return fmt.Errorf("failed to check if schema %s exists: %v", schema, err)
	}
	if !schemaExists {
		createSchemaQuery := fmt.Sprintf("CREATE SCHEMA %s", sanitizedSchema)
		if _, err := tx.Exec(ctx, createSchemaQuery); err != nil {
			return fmt.Errorf("failed to create schema %s: %v", schema, err)
		}
	}
	var migrationTableExists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		schema, MigrationTable).Scan(&migrationTableExists); err != nil {
		return fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !migrationTableExists {
		createTableQuery := fmt.Sprintf(`CREATE TABLE %s.%s (version BIGINT NOT NULL PRIMARY KEY)`,
			sanitizedSchema, MigrationTable)
		if _, err := tx.Exec(ctx, createTableQuery); err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}
	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", sanitizedSchema, MigrationTable)
	if err := tx.QueryRow(ctx, q).Scan(&currentVersion); err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to get current migration version: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration setup transaction: %v", err)
	}

	// Apply pending migrations one at a time.
	invalidIndexesCleaned := false
	for _, migration := range migrations {
		if migration.Version <= currentVersion {
			continue
		}

		if migration.Online {
			// Online migrations must run outside a transaction so PostgreSQL will accept CREATE/DROP INDEX CONCURRENTLY.
			// Before the first online migration, sweep up any indexes left INVALID by a prior crashed run.
			// The version bump is necessarily a second, non-atomic round-trip. If it fails and must re-run, re-executing the migration has to be safe.
			if !invalidIndexesCleaned {
				if err := CleanupInvalidIndexes(ctx, pool, schema, logger); err != nil {
					return err
				}
				invalidIndexesCleaned = true
			}
			if _, err := pool.Exec(ctx, migration.SQL); err != nil {
				return fmt.Errorf("failed to execute migration %d: %v", migration.Version, err)
			}
			if err := writeMigrationVersion(ctx, pool, schema, migration.Version, currentVersion); err != nil {
				return err
			}
			currentVersion = migration.Version
			continue
		}

		if err := applyCatalogMigration(ctx, pool, schema, sanitizedSchema, migration, isCockroach, currentVersion); err != nil {
			return err
		}
		currentVersion = migration.Version
	}

	return nil
}

// applyCatalogMigration runs a single non-online migration and its version bump in one transaction.
func applyCatalogMigration(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema, sanitizedSchema string,
	migration MigrationFile,
	isCockroach bool,
	currentVersion int64,
) error {
	mtx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for migration %d: %v", migration.Version, err)
	}
	defer mtx.Rollback(ctx)

	switch {
	case migration.Version == 10 && isCockroach:
		// CockroachDB does not support the DO block used by the Postgres
		// migration file; run the equivalent logic at the application layer
		// inside the same transaction.
		if err := applyCockroachMigration10(ctx, mtx, schema, sanitizedSchema); err != nil {
			return err
		}
	case strings.TrimSpace(migration.SQL) == "":
		// No-op migration (e.g. migration 20 on CockroachDB). Still advance
		// the version row so we don't re-evaluate it next time.
	default:
		if _, err := mtx.Exec(ctx, migration.SQL); err != nil {
			return fmt.Errorf("failed to execute migration %d: %v", migration.Version, err)
		}
	}

	if err := writeMigrationVersion(ctx, mtx, schema, migration.Version, currentVersion); err != nil {
		return err
	}
	if err := mtx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration %d: %v", migration.Version, err)
	}
	return nil
}

// applyCockroachMigration10 applies migration 10 on CockroachDB, which does
// not support the DO block used by the Postgres migration file.
func applyCockroachMigration10(ctx context.Context, tx pgx.Tx, schema, sanitizedSchema string) error {
	rows, err := tx.Query(ctx, migration10CheckCockroachSQL, schema)
	if err != nil {
		return fmt.Errorf("failed to check notifications primary key for migration 10: %v", err)
	}
	hasPK := rows.Next()
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to check notifications primary key for migration 10: %v", err)
	}
	if !hasPK {
		alterQuery := fmt.Sprintf(migration10AddCockroachSQL, sanitizedSchema)
		if _, err := tx.Exec(ctx, alterQuery); err != nil {
			return fmt.Errorf("failed to execute migration 10: %v", err)
		}
	}
	return nil
}

type NewSystemDatabaseInput struct {
	DatabaseURL     string
	DatabaseSchema  string
	CustomPool      *pgxpool.Pool
	CustomSqliteDB  *sql.DB
	Logger          *slog.Logger
	ApplicationName string
	StartupTimeout  time.Duration
	// EncodeScheduledInput serializes the input of a schedule-created workflow
	// (backfill/trigger). Injected by the caller to keep serialization concerns
	// out of the system database.
	EncodeScheduledInput func(ctx context.Context, scheduledTime time.Time, scheduleContext json.RawMessage) (encoded *string, serialization string, err error)
}

func startupError(ctx context.Context, timeout time.Duration, phase string, pool *pgxpool.Pool, err error) error {
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return err
	}
	if pool != nil {
		stat := pool.Stat()
		if stat.MaxConns() > 0 && stat.AcquiredConns() >= stat.MaxConns() {
			return fmt.Errorf("system database startup timed out after %s while %s: connection pool has no free connections (acquired=%d, max=%d); increase pool capacity or release checked-out connections: %w",
				timeout, phase, stat.AcquiredConns(), stat.MaxConns(), context.DeadlineExceeded)
		}
	}
	return fmt.Errorf("system database startup timed out after %s while %s; check database connectivity, pool capacity, and blocking database locks: %w", timeout, phase, context.DeadlineExceeded)
}

// RenderSQL formats a canonical pg-style query string with sprintf and runs
// it through the dialect's rewrite pass. Use this for every sysDB query that
// must work on both pg and sqlite — it converts $N placeholders to ?N for
// sqlite while leaving pg unchanged.
func (s *SysDB) RenderSQL(format string, args ...any) string {
	return s.dialect.RewriteQuery(fmt.Sprintf(format, args...))
}

// NewSystemDatabase creates a new SystemDatabase instance and runs migrations.
func NewSystemDatabase(ctx context.Context, inputs NewSystemDatabaseInput) (SystemDatabase, error) {
	// Dereference fields from inputs
	databaseURL := inputs.DatabaseURL
	databaseSchema := inputs.DatabaseSchema
	customPool := inputs.CustomPool
	customSqliteDB := inputs.CustomSqliteDB
	logger := inputs.Logger

	// Validate that schema is provided
	if databaseSchema == "" {
		return nil, fmt.Errorf("database schema cannot be empty")
	}
	if customPool != nil && customSqliteDB != nil {
		return nil, fmt.Errorf("customPool and customSqliteDB are mutually exclusive")
	}

	// Dispatch sqlite first
	if customSqliteDB != nil {
		systemDB, err := newSqliteSystemDatabase(inputs.EncodeScheduledInput, ctx, databaseURL, databaseSchema, customSqliteDB, logger)
		if err != nil {
			return nil, startupError(ctx, inputs.StartupTimeout, "initializing the SQLite system database", nil, err)
		}
		return systemDB, nil
	}
	if customPool == nil {
		dialectName, err := DetectDialect(databaseURL)
		if err != nil {
			return nil, err
		}
		if dialectName == DialectSQLite {
			systemDB, err := newSqliteSystemDatabase(inputs.EncodeScheduledInput, ctx, databaseURL, databaseSchema, nil, logger)
			if err != nil {
				return nil, startupError(ctx, inputs.StartupTimeout, "initializing the SQLite system database", nil, err)
			}
			return systemDB, nil
		}
	}

	// Configure a connection pool
	var pool *pgxpool.Pool
	if customPool != nil {
		logger.Info("Using custom database connection pool")
		// Verify the pool is valid
		poolConn, err := customPool.Acquire(ctx)
		if err != nil {
			return nil, startupError(ctx, inputs.StartupTimeout, "acquiring a connection from the custom pool", customPool, fmt.Errorf("failed to validate custom pool: %w", err))
		}
		err = poolConn.Ping(ctx)
		poolConn.Release()
		if err != nil {
			return nil, startupError(ctx, inputs.StartupTimeout, "validating the custom pool", customPool, fmt.Errorf("failed to validate custom pool: %w", err))
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
		if inputs.ApplicationName != "" {
			if config.ConnConfig.RuntimeParams == nil {
				config.ConnConfig.RuntimeParams = make(map[string]string)
			}
			config.ConnConfig.RuntimeParams["application_name"] = inputs.ApplicationName
		}

		// Create pool with configuration
		newPool, err := pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pool: %v", err)
		}
		pool = newPool
	}

	// Displaying Masked Database URL
	maskedDatabaseURL, err := MaskPassword(pool.Config().ConnString())
	if err != nil {
		logger.Error("Failed to parse database URL", "error", err)
		return nil, fmt.Errorf("failed to parse database URL: %v", err)
	}
	logger.Info("Connecting to system database", "database_url", maskedDatabaseURL, "schema", databaseSchema)

	if customPool == nil {
		// Create the database if it doesn't exist
		if err := Retry(ctx, func() error {
			return createDatabaseIfNotExists(ctx, pool, logger)
		}, WithRetrierLogger(logger)); err != nil {
			pool.Close()
			return nil, startupError(ctx, inputs.StartupTimeout, "connecting to or creating the system database", pool, fmt.Errorf("failed to create database: %w", err))
		}
	}

	// Detect if we're running CockroachDB
	// This must happen after we ensured the database exist
	conn, err := pool.Acquire(ctx)
	if err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, startupError(ctx, inputs.StartupTimeout, "acquiring a connection to detect database type", pool, fmt.Errorf("failed to acquire connection to detect database type: %w", err))
	}
	isCockroach := IsCockroachDB(conn.Conn())
	// Release before any error path calls pool.Close(): Close blocks until all
	// acquired connections are returned, so a deferred Release would deadlock.
	conn.Release()
	if isCockroach {
		logger.Info("Detected CockroachDB")
	}

	needsMigration, smErr := ShouldMigrate(ctx, pool, databaseSchema, isCockroach)
	if smErr != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, startupError(ctx, inputs.StartupTimeout, "checking system database migration status", pool, fmt.Errorf("failed to determine migration status: %w", smErr))
	}
	if needsMigration {
		if err := Retry(ctx, func() error {
			return RunMigrations(ctx, pool, databaseSchema, isCockroach, logger)
		}, WithRetrierLogger(logger)); err != nil {
			if customPool == nil {
				pool.Close()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			return nil, startupError(ctx, inputs.StartupTimeout, "running system database migrations", pool, fmt.Errorf("failed to run migrations: %w", err))
		}
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, startupError(ctx, inputs.StartupTimeout, "pinging the system database", pool, fmt.Errorf("failed to ping database: %w", err))
	}

	dialect := Dialect(PostgresDialect{})
	if isCockroach {
		dialect = CockroachDialect{}
	}

	return &SysDB{
		pool:                 NewPgxPool(pool),
		dialect:              dialect,
		RecvNotifier:         newNotifyRegistry(),
		EventNotifier:        newNotifyRegistry(),
		streamNotifier:       newNotifyRegistry(),
		encodeScheduledInput: inputs.EncodeScheduledInput,
		notificationLoopDone: make(chan struct{}),
		logger:               logger.With("service", "system_database"),
		schema:               databaseSchema,
		isCockroachDB:        isCockroach,
	}, nil
}

func (s *SysDB) ListenNotifyPool() *pgxpool.Pool {
	if s.dialect == nil || !s.dialect.SupportsListenNotify() {
		return nil
	}
	return PgxPool(s.pool)
}

func (s *SysDB) Schema() string {
	return s.schema
}

// SetPool swaps the underlying pool. Test support only (fault injection);
// must not be called after Launch.
func (s *SysDB) SetPool(p Pool) {
	s.pool = p
}

func (s *SysDB) Launched() bool {
	s.notificationLoopMu.Lock()
	defer s.notificationLoopMu.Unlock()
	return s.launched
}

func (s *SysDB) Pool() Pool {
	return s.pool
}

func (s *SysDB) Dialect() Dialect {
	return s.dialect
}

func (s *SysDB) IsContentionError(err error) bool {
	return s.dialect.IsContentionError(err)
}

func (s *SysDB) StreamWakeChannel(workflowID, key string) (chan struct{}, func()) {
	payload := fmt.Sprintf("%s::%s", workflowID, key)
	ch := s.streamNotifier.subscribe(payload)
	return ch, func() { s.streamNotifier.unsubscribe(payload, ch) }
}

func (s *SysDB) Launch(ctx context.Context) {
	done := make(chan struct{})
	s.notificationLoopMu.Lock()
	s.notificationLoopDone = done
	s.launched = true
	s.notificationLoopMu.Unlock()

	if s.ListenNotifyPool() == nil {
		go func() {
			s.notificationPollerLoop(ctx)
			close(done)
		}()
	} else {
		go func() {
			s.notificationListenerLoop(ctx)
			close(done)
		}()
	}
}

func (s *SysDB) Shutdown(ctx context.Context, timeout time.Duration) []string {
	s.logger.Debug("Closing system database connection pool")
	var pending []string

	s.notificationLoopMu.Lock()
	launched := s.launched
	done := s.notificationLoopDone
	s.notificationLoopMu.Unlock()
	listenerStopped := true
	if launched {
		// Wait for the notification loop to exit
		// The context should be cancelled prior to calling shutdown
		select {
		case <-done:
		case <-time.After(timeout):
			s.logger.Warn("Notification listener loop did not finish in time", "timeout", timeout)
			pending = append(pending, "notification listener")
			listenerStopped = false
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
			pending = append(pending, "connection pool")
		}
	}

	s.RecvNotifier.clear()
	s.EventNotifier.clear()
	s.streamNotifier.clear()

	s.notificationLoopMu.Lock()
	// Stay launched while the notification loop is still running so a later
	// Shutdown call waits for it again instead of skipping the check.
	if listenerStopped {
		s.launched = false
	}
	s.notificationLoopMu.Unlock()
	return pending
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type InsertWorkflowResult struct {
	Attempts          int
	Status            models.WorkflowStatusType
	Name              string
	QueueName         *string
	QueuePartitionKey *string
	Timeout           time.Duration
	WorkflowDeadline  time.Time
	OwnerXID          string
}

type InsertWorkflowStatusDBInput struct {
	Status            models.WorkflowStatus
	MaxRetries        int
	Tx                Tx
	OwnerXID          *string
	IncrementAttempts bool
}

func (s *SysDB) InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error) {
	if input.Tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

	// Set default values
	attempts := 1
	if input.Status.Status == models.WorkflowStatusEnqueued || input.Status.Status == models.WorkflowStatusDelayed {
		attempts = 0
	}

	var delayUntilEpochMs *int64
	if !input.Status.DelayUntil.IsZero() {
		millis := input.Status.DelayUntil.UnixMilli()
		delayUntilEpochMs = &millis
	}

	updatedAt := time.Now()
	if !input.Status.UpdatedAt.IsZero() {
		updatedAt = input.Status.UpdatedAt
	}

	var deadline *int64 = nil
	if !input.Status.Deadline.IsZero() {
		millis := input.Status.Deadline.UnixMilli()
		deadline = &millis
	}

	var timeoutMs *int64 = nil
	if input.Status.Timeout > 0 {
		millis := input.Status.Timeout.Round(time.Millisecond).Milliseconds()
		timeoutMs = &millis
	}

	// Our DB works with NULL values
	var applicationVersion *string
	if len(input.Status.ApplicationVersion) > 0 {
		applicationVersion = &input.Status.ApplicationVersion
	}

	var deduplicationID *string
	if len(input.Status.DeduplicationID) > 0 {
		deduplicationID = &input.Status.DeduplicationID
	}

	var queuePartitionKey *string
	if len(input.Status.QueuePartitionKey) > 0 {
		queuePartitionKey = &input.Status.QueuePartitionKey
	}

	var parentWorkflowID *string
	if len(input.Status.ParentWorkflowID) > 0 {
		parentWorkflowID = &input.Status.ParentWorkflowID
	}

	var className *string
	if len(input.Status.ClassName) > 0 {
		className = &input.Status.ClassName
	}

	var attributesJSON *string
	if len(input.Status.Attributes) > 0 {
		marshaled, err := json.Marshal(input.Status.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal workflow attributes: %w", err)
		}
		attributesStr := string(marshaled)
		attributesJSON = &attributesStr
	}

	var scheduleName *string
	if len(input.Status.ScheduleName) > 0 {
		scheduleName = &input.Status.ScheduleName
	}

	var debounceDeadlineEpochMs *int64
	if !input.Status.DebounceDeadline.IsZero() {
		millis := input.Status.DebounceDeadline.UnixMilli()
		debounceDeadlineEpochMs = &millis
	}

	query := s.RenderSQL(`INSERT INTO %sworkflow_status (
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
        parent_workflow_id,
        class_name,
        config_name,
        serialization,
        delay_until_epoch_ms,
        attributes,
        schedule_name,
        debounce_deadline_epoch_ms,
        is_debounced
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
			recovery_attempts = CASE
                WHEN EXCLUDED.status NOT IN ($30, $31) THEN workflow_status.recovery_attempts + $32
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status IN ($30, $31) THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, queue_partition_key, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid`, s.dialect.SchemaPrefix(s.schema))

	var result InsertWorkflowResult
	var timeoutMSResult *int64
	var workflowDeadlineEpochMS *int64
	var ownerXIDReturn *string

	// Marshal authenticated roles (slice of strings) to JSON for TEXT column
	authenticatedRoles, err := json.Marshal(input.Status.AuthenticatedRoles)

	if err != nil {
		return nil, fmt.Errorf("failed to marshal the authenticated roles: %w", err)
	}

	recoveryIncrement := 0
	if input.IncrementAttempts {
		recoveryIncrement = 1
	}
	err = input.Tx.QueryRow(ctx, query,
		input.Status.ID,
		input.Status.Status,
		input.Status.Name,
		input.Status.QueueName,
		input.Status.AuthenticatedUser,
		input.Status.AssumedRole,
		authenticatedRoles,
		input.Status.ExecutorID,
		applicationVersion,
		input.Status.ApplicationID,
		input.Status.CreatedAt.Round(time.Millisecond).UnixMilli(), // slightly reduce the likelihood of collisions
		attempts,
		updatedAt.UnixMilli(),
		timeoutMs,
		deadline,
		input.Status.Input,
		deduplicationID,
		input.Status.Priority,
		queuePartitionKey,
		input.OwnerXID,
		parentWorkflowID,
		className,
		input.Status.ConfigName,
		input.Status.Serialization,
		delayUntilEpochMs,
		attributesJSON,
		scheduleName,
		debounceDeadlineEpochMs,
		input.Status.IsDebounced,
		models.WorkflowStatusEnqueued,
		models.WorkflowStatusDelayed,
		recoveryIncrement,
	).Scan(
		&result.Attempts,
		&result.Status,
		&result.Name,
		&result.QueueName,
		&result.QueuePartitionKey,
		&timeoutMSResult,
		&workflowDeadlineEpochMS,
		&ownerXIDReturn,
	)
	if ownerXIDReturn != nil {
		result.OwnerXID = *ownerXIDReturn
	}
	ownerXIDMatches := (input.OwnerXID == nil && ownerXIDReturn == nil) ||
		(input.OwnerXID != nil && ownerXIDReturn != nil && *input.OwnerXID == *ownerXIDReturn)
	if err != nil {
		// Handle unique constraint violation for the deduplication ID (this should be the only case)
		if s.dialect.IsUniqueViolation(err) {
			return nil, models.NewQueueDeduplicatedError(
				input.Status.ID,
				input.Status.QueueName,
				input.Status.DeduplicationID,
			)
		}
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	// Convert timeout milliseconds to time.Duration
	if timeoutMSResult != nil && *timeoutMSResult > 0 {
		result.Timeout = time.Duration(*timeoutMSResult) * time.Millisecond
	}

	// Convert deadline milliseconds to time.Time
	if workflowDeadlineEpochMS != nil {
		result.WorkflowDeadline = time.Unix(0, *workflowDeadlineEpochMS*int64(time.Millisecond))
	}

	if len(input.Status.Name) > 0 && result.Name != input.Status.Name {
		return nil, models.NewUnexpectedWorkflowError(input.Status.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.Name, input.Status.Name))
	}
	if len(input.Status.QueueName) > 0 && result.QueueName != nil && input.Status.QueueName != *result.QueueName {
		return nil, models.NewUnexpectedWorkflowError(input.Status.ID, fmt.Sprintf("Workflow already exists in a different queue: %s, but the provided queue is: %s", *result.QueueName, input.Status.QueueName))
	}

	// Every time we start executing a workflow (and thus attempt to insert its status), we increment `recovery_attempts` by 1.
	// When this number becomes equal to `maxRetries + 1`, we mark the workflow as `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
	if result.Status != models.WorkflowStatusSuccess && result.Status != models.WorkflowStatusError &&
		input.MaxRetries > 0 && result.Attempts > input.MaxRetries+1 && !ownerXIDMatches {

		// Update workflow status to MAX_RECOVERY_ATTEMPTS_EXCEEDED and clear queue-related fields
		dlqQuery := s.RenderSQL(`UPDATE %sworkflow_status
					 SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
					 WHERE workflow_uuid = $2 AND status = $3`, s.dialect.SchemaPrefix(s.schema))

		_, err = input.Tx.Exec(ctx, dlqQuery,
			models.WorkflowStatusMaxRecoveryAttemptsExceeded,
			input.Status.ID,
			models.WorkflowStatusPending)

		if err != nil {
			return nil, fmt.Errorf("failed to update workflow to %s: %w", models.WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		// Commit the transaction before throwing the error
		if err := input.Tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction after marking workflow as %s: %w", models.WorkflowStatusMaxRecoveryAttemptsExceeded, err)
		}

		return nil, models.NewDeadLetterQueueError(input.Status.ID, input.MaxRetries)
	}

	return &result, nil
}

// ListWorkflowsDBInput represents the input parameters for listing workflows.
type ListWorkflowsDBInput struct {
	WorkflowName       []string
	QueueName          []string
	QueuesOnly         bool
	WorkflowIDPrefix   []string
	WorkflowIDs        []string
	AuthenticatedUser  []string
	StartTime          time.Time
	EndTime            time.Time
	Status             []models.WorkflowStatusType
	ApplicationVersion []string
	ExecutorIDs        []string
	ForkedFrom         []string
	ParentWorkflowID   []string
	DeduplicationID    []string
	CompletedAfter     time.Time
	CompletedBefore    time.Time
	DequeuedAfter      time.Time
	DequeuedBefore     time.Time
	WasForkedFrom      *bool
	HasParent          *bool
	Attributes         map[string]any
	ScheduleName       []string
	IsDebounced        *bool
	Limit              *int
	Offset             *int
	SortDesc           bool
	LoadInput          bool
	LoadOutput         bool
	Tx                 Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *SysDB) ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]models.WorkflowStatus, error) {
	qb := newQueryBuilder(s.dialect)

	// Build the base query with conditional column selection
	loadColumns := []string{
		"workflow_uuid", "status", "name", "authenticated_user", "assumed_role", "authenticated_roles",
		"executor_id", "created_at", "updated_at", "application_version", "application_id",
		"recovery_attempts", "queue_name", "workflow_timeout_ms", "workflow_deadline_epoch_ms", "started_at_epoch_ms",
		"deduplication_id", "priority", "queue_partition_key", "forked_from", "parent_workflow_id",
		"serialization", "delay_until_epoch_ms", "was_forked_from", "completed_at", "class_name", "config_name",
		"attributes", "schedule_name", "debounce_deadline_epoch_ms", "is_debounced",
	}

	if input.LoadOutput {
		loadColumns = append(loadColumns, "output", "error")
	}
	if input.LoadInput {
		loadColumns = append(loadColumns, "inputs")
	}

	baseQuery := fmt.Sprintf("SELECT %s FROM %sworkflow_status", strings.Join(loadColumns, ", "), s.dialect.SchemaPrefix(s.schema))

	// Add filters using query builder
	if len(input.WorkflowName) > 0 {
		qb.addWhereAny("name", input.WorkflowName)
	}
	if len(input.QueueName) > 0 {
		qb.addWhereAny("queue_name", input.QueueName)
	}
	if input.QueuesOnly {
		qb.addWhereIsNotNull("queue_name")
	}
	if len(input.WorkflowIDPrefix) > 0 {
		qb.addWhereLikeAny("workflow_uuid", input.WorkflowIDPrefix, "%")
	}
	if len(input.WorkflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.WorkflowIDs)
	}
	if len(input.AuthenticatedUser) > 0 {
		qb.addWhereAny("authenticated_user", input.AuthenticatedUser)
	}
	if !input.StartTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.StartTime.UnixMilli())
	}
	if !input.EndTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.EndTime.UnixMilli())
	}
	if len(input.Status) > 0 {
		qb.addWhereAny("status", input.Status)
	}
	if len(input.ApplicationVersion) > 0 {
		qb.addWhereAny("application_version", input.ApplicationVersion)
	}
	if len(input.ExecutorIDs) > 0 {
		qb.addWhereAny("executor_id", input.ExecutorIDs)
	}
	if len(input.ForkedFrom) > 0 {
		qb.addWhereAny("forked_from", input.ForkedFrom)
	}
	if len(input.ParentWorkflowID) > 0 {
		qb.addWhereAny("parent_workflow_id", input.ParentWorkflowID)
	}
	if len(input.DeduplicationID) > 0 {
		qb.addWhereAny("deduplication_id", input.DeduplicationID)
	}
	if len(input.ScheduleName) > 0 {
		qb.addWhereAny("schedule_name", input.ScheduleName)
	}
	if !input.CompletedAfter.IsZero() {
		qb.addWhereGreaterEqual("completed_at", input.CompletedAfter.UnixMilli())
	}
	if !input.CompletedBefore.IsZero() {
		qb.addWhereLessEqual("completed_at", input.CompletedBefore.UnixMilli())
	}
	// dequeued_after/before filter on started_at_epoch_ms: that column records
	// when a workflow was dequeued and began executing.
	if !input.DequeuedAfter.IsZero() {
		qb.addWhereGreaterEqual("started_at_epoch_ms", input.DequeuedAfter.UnixMilli())
	}
	if !input.DequeuedBefore.IsZero() {
		qb.addWhereLessEqual("started_at_epoch_ms", input.DequeuedBefore.UnixMilli())
	}
	if input.WasForkedFrom != nil {
		qb.addWhere("was_forked_from", *input.WasForkedFrom)
	}
	if input.IsDebounced != nil {
		qb.addWhere("is_debounced", *input.IsDebounced)
	}
	if input.HasParent != nil {
		if *input.HasParent {
			qb.addWhereIsNotNull("parent_workflow_id")
		} else {
			qb.addWhereIsNull("parent_workflow_id")
		}
	}
	if len(input.Attributes) > 0 {
		if !s.dialect.SupportsAttributesContainment() {
			return nil, fmt.Errorf("filtering workflows by attributes is not supported on %s; use a Postgres system database to filter by attributes", s.dialect.Name())
		}
		attributesJSON, err := json.Marshal(input.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal attributes filter: %w", err)
		}
		// JSONB containment (@>), served by the GIN index on the attributes column
		qb.argCounter++
		qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("attributes @> $%d::jsonb", qb.argCounter))
		qb.args = append(qb.args, string(attributesJSON))
	}

	// Build complete query
	var query string
	if len(qb.whereClauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", baseQuery, strings.Join(qb.whereClauses, " AND "))
	} else {
		query = baseQuery
	}

	// Add sorting
	if input.SortDesc {
		query += " ORDER BY created_at DESC"
	} else {
		query += " ORDER BY created_at ASC"
	}

	// Add limit and offset
	if input.Limit != nil {
		qb.argCounter++
		query += fmt.Sprintf(" LIMIT $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Limit)
	} else if input.Offset != nil {
		query += dialectNoLimitClause(s.dialect)
	}

	if input.Offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Offset)
	}

	// Execute the query against the input tx if provided, else the pool.
	query = s.dialect.RewriteQuery(query)
	var rows Rows
	var err error
	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute ListWorkflows query: %w", err)
	}
	defer rows.Close()

	var workflows []models.WorkflowStatus
	for rows.Next() {
		var wf models.WorkflowStatus
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
		var serialization *string
		var authenticatedUser *string
		var assumedRole *string
		var applicationID *string
		var delayUntilEpochMs *int64
		var completedAtMs *int64
		var className *string
		var attributesJSON *string
		var scheduleName *string
		var debounceDeadlineEpochMs *int64

		// Build scan arguments dynamically based on loaded columns.
		scanArgs := []any{
			&wf.ID, &wf.Status, &wf.Name, &authenticatedUser, &assumedRole,
			&authenticatedRoles, &executorID, &createdAtMs,
			&updatedAtMs, &applicationVersion, &applicationID,
			&wf.Attempts, &queueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &deduplicationID, &wf.Priority, &queuePartitionKey, &forkedFrom, &parentWorkflowID,
			&serialization, &delayUntilEpochMs, &wf.WasForkedFrom, &completedAtMs, &className, &wf.ConfigName,
			&attributesJSON, &scheduleName, &debounceDeadlineEpochMs, &wf.IsDebounced,
		}

		if input.LoadOutput {
			scanArgs = append(scanArgs, &outputString, &errorStr)
		}
		if input.LoadInput {
			scanArgs = append(scanArgs, &inputString)
		}

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow row: %w", err)
		}

		if authenticatedUser != nil {
			wf.AuthenticatedUser = *authenticatedUser
		}
		if className != nil {
			wf.ClassName = *className
		}
		if assumedRole != nil {
			wf.AssumedRole = *assumedRole
		}
		if applicationID != nil {
			wf.ApplicationID = *applicationID
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

		if serialization != nil && len(*serialization) > 0 {
			wf.Serialization = *serialization
		}

		if attributesJSON != nil && len(*attributesJSON) > 0 {
			if err := json.Unmarshal([]byte(*attributesJSON), &wf.Attributes); err != nil {
				return nil, fmt.Errorf("failed to unmarshal attributes: %w", err)
			}
		}

		if scheduleName != nil && len(*scheduleName) > 0 {
			wf.ScheduleName = *scheduleName
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

		// Convert delay_until_epoch_ms to time.Time
		if delayUntilEpochMs != nil {
			wf.DelayUntil = time.Unix(0, *delayUntilEpochMs*int64(time.Millisecond))
		}

		// Convert debounce_deadline_epoch_ms to time.Time
		if debounceDeadlineEpochMs != nil {
			wf.DebounceDeadline = time.Unix(0, *debounceDeadlineEpochMs*int64(time.Millisecond))
		}

		// Convert completed_at milliseconds to time.Time
		if completedAtMs != nil {
			wf.CompletedAt = time.Unix(0, *completedAtMs*int64(time.Millisecond))
		}

		// Handle output and error only if loadOutput is true
		if input.LoadOutput {
			// Convert error string to error type if present
			if errorStr != nil && *errorStr != "" {
				wf.Error = errors.New(*errorStr)
			}

			// Return output as encoded *string
			wf.Output = outputString
		}

		// Return input as encoded *string
		if input.LoadInput {
			wf.Input = inputString
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type UpdateWorkflowOutcomeDBInput struct {
	WorkflowID string
	Status     models.WorkflowStatusType
	Output     *string
	ErrStr     string
	Tx         Tx
}

// UpdateWorkflowOutcome records a workflow's terminal outcome. The write is refused
// when the row is already terminal (CANCELLED/SUCCESS/ERROR). A refusal is an error
// only when the workflow was cancelled, so the caller ends as cancelled instead of
// reporting a completion that was never recorded; other refusals are silent no-ops.
func (s *SysDB) UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error {
	query := s.RenderSQL(`UPDATE %sworkflow_status
			  SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4, deduplication_id = NULL
			  WHERE workflow_uuid = $5 AND status NOT IN ($6, $7, $8)`, s.dialect.SchemaPrefix(s.schema))

	var runner Querier = s.pool
	if input.Tx != nil {
		runner = input.Tx
	}

	// input.output is already a *string from the database layer
	res, err := runner.Exec(ctx, query, input.Status, input.Output, input.ErrStr, time.Now().UnixMilli(), input.WorkflowID, models.WorkflowStatusCancelled, models.WorkflowStatusSuccess, models.WorkflowStatusError)
	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check workflow status update: %w", err)
	}
	if rowsAffected == 0 {
		// The guarded UPDATE matched no rows. Re-read the status (only on this rare
		// no-op path): only a cancelled workflow surfaces an error to the caller.
		statusQuery := s.RenderSQL(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
		var currentStatus models.WorkflowStatusType
		if err := runner.QueryRow(ctx, statusQuery, input.WorkflowID).Scan(&currentStatus); err != nil {
			if errors.Is(err, ErrNoRows) {
				return nil
			}
			return fmt.Errorf("failed to read workflow status after refused outcome update: %w", err)
		}
		if currentStatus == models.WorkflowStatusCancelled {
			return models.NewWorkflowCancelledError(input.WorkflowID, nil)
		}
	}
	return nil
}

type SetWorkflowAttributesDBInput struct {
	WorkflowID string
	Attributes map[string]any
	Tx         Tx
}

// SetWorkflowAttributes replaces the custom attributes attached to an existing
// workflow. A nil/empty attributes map clears them (stored as NULL). Returns a
// non-existent workflow error if no workflow with the given ID exists.
func (s *SysDB) SetWorkflowAttributes(ctx context.Context, input SetWorkflowAttributesDBInput) error {
	var attributesJSON *string
	if len(input.Attributes) > 0 {
		marshaled, err := json.Marshal(input.Attributes)
		if err != nil {
			return fmt.Errorf("failed to marshal workflow attributes: %w", err)
		}
		attributesStr := string(marshaled)
		attributesJSON = &attributesStr
	}

	query := s.RenderSQL(`UPDATE %sworkflow_status SET attributes = $1, updated_at = $2 WHERE workflow_uuid = $3`, s.dialect.SchemaPrefix(s.schema))

	var res Result
	var err error
	if input.Tx != nil {
		res, err = input.Tx.Exec(ctx, query, attributesJSON, time.Now().UnixMilli(), input.WorkflowID)
	} else {
		res, err = s.pool.Exec(ctx, query, attributesJSON, time.Now().UnixMilli(), input.WorkflowID)
	}
	if err != nil {
		return fmt.Errorf("failed to update workflow attributes: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return models.NewNonExistentWorkflowError(input.WorkflowID)
	}
	return nil
}

type CancelWorkflowsDBInput struct {
	CancelChildren bool
	WorkflowIDs    []string
	Tx             Tx
}

// CancelWorkflows cancels the given workflows in a single round-trip. Workflows that
// are already in a terminal state (SUCCESS, ERROR, CANCELLED) are left untouched.
// Returns the subset of input IDs that existed in workflow_status (including terminal
// ones, which are considered existing even though they are not updated).
func (s *SysDB) CancelWorkflows(ctx context.Context, input CancelWorkflowsDBInput) ([]string, error) {
	if len(input.WorkflowIDs) == 0 {
		return nil, nil
	}

	workflowIDs := make([]string, len(input.WorkflowIDs))
	copy(workflowIDs, input.WorkflowIDs)

	if input.CancelChildren {
		for _, workflowID := range workflowIDs {
			children, err := s.GetWorkflowChildren(ctx, GetWorkflowChildrenDBInput{
				WorkflowID: workflowID,
				Tx:         input.Tx,
			})
			if err != nil {
				return nil, err
			}
			for _, child := range children {
				workflowIDs = append(workflowIDs, child.ID)
			}
		}
	}

	schemaPrefix := s.dialect.SchemaPrefix(s.schema)
	anyClause := dialectAnyClause(s.dialect, "workflow_uuid", 3)
	encodedIDs, err := encodeArrayParam(s.dialect, workflowIDs)
	if err != nil {
		return nil, fmt.Errorf("cancel workflows: %w", err)
	}

	// Dialects without data-modifying CTEs (sqlite) split the pg
	// single-statement CTE into two statements (UPDATE then SELECT).
	// Needs repeatable read. Reuse the caller's tx when supplied.
	if !s.dialect.SupportsDataModifyingCTE() {
		updateQuery := s.RenderSQL(`UPDATE %sworkflow_status
			SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
			    queue_name = NULL, deduplication_id = NULL
			WHERE %s AND status NOT IN ($4, $5, $6)`, schemaPrefix, anyClause)
		selectAnyClause := dialectAnyClause(s.dialect, "workflow_uuid", 1)
		selectQuery := s.RenderSQL(`SELECT workflow_uuid FROM %sworkflow_status WHERE %s`, schemaPrefix, selectAnyClause)
		args := []any{
			models.WorkflowStatusCancelled,
			time.Now().UnixMilli(),
			encodedIDs,
			models.WorkflowStatusSuccess,
			models.WorkflowStatusError,
			models.WorkflowStatusCancelled,
		}

		var runner Querier
		var localTx Tx
		if input.Tx != nil {
			runner = input.Tx
		} else {
			tx, err := s.pool.BeginTx(ctx, TxOptions{IsoLevel: s.dialect.SnapshotIsolation()})
			if err != nil {
				return nil, fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback(ctx)
			localTx = tx
			runner = tx
		}

		if _, err := runner.Exec(ctx, updateQuery, args...); err != nil {
			return nil, fmt.Errorf("failed to cancel workflows: %w", err)
		}
		rows, err := runner.Query(ctx, selectQuery, args[2])
		if err != nil {
			return nil, fmt.Errorf("failed to list cancelled workflow ids: %w", err)
		}
		found := make([]string, 0, len(input.WorkflowIDs))
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				scanErr := fmt.Errorf("failed to scan cancelled workflow id: %w", err)
				if cerr := rows.Close(); cerr != nil {
					return nil, errors.Join(scanErr, fmt.Errorf("close rows: %w", cerr))
				}
				return nil, scanErr
			}
			found = append(found, id)
		}
		if cerr := rows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close cancelled workflow rows: %w", cerr)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to read cancelled workflow ids: %w", err)
		}
		if localTx != nil {
			if err := localTx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("failed to commit cancel workflows tx: %w", err)
			}
		}
		return found, nil
	}

	query := s.RenderSQL(`WITH existing AS (
			SELECT workflow_uuid FROM %sworkflow_status WHERE %s
		), updated AS (
			UPDATE %sworkflow_status
			SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
			    queue_name = NULL, deduplication_id = NULL
			WHERE %s AND status NOT IN ($4, $5, $6)
			RETURNING workflow_uuid
		)
		SELECT workflow_uuid FROM existing`, schemaPrefix, anyClause, schemaPrefix, anyClause)

	args := []any{
		models.WorkflowStatusCancelled,
		time.Now().UnixMilli(),
		encodedIDs,
		models.WorkflowStatusSuccess,
		models.WorkflowStatusError,
		models.WorkflowStatusCancelled,
	}

	var rows Rows
	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, args...)
	} else {
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to cancel workflows: %w", err)
	}
	defer rows.Close()

	found := make([]string, 0, len(input.WorkflowIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan cancelled workflow id: %w", err)
		}
		found = append(found, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read cancelled workflow ids: %w", err)
	}
	return found, nil
}

type DeleteWorkflowsDBInput struct {
	WorkflowIDs    []string
	DeleteChildren bool
	Tx             Tx
}

func (s *SysDB) DeleteWorkflows(ctx context.Context, input DeleteWorkflowsDBInput) error {
	// If no transaction is provided, create one so the entire operation is atomic
	tx := input.Tx
	if tx == nil {
		var err error
		tx, err = s.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction for deleteWorkflows: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	// Collect all workflow IDs to delete
	workflowIDs := make([]string, len(input.WorkflowIDs))
	copy(workflowIDs, input.WorkflowIDs)

	if input.DeleteChildren {
		for _, wfID := range input.WorkflowIDs {
			children, err := s.GetWorkflowChildren(ctx, GetWorkflowChildrenDBInput{
				WorkflowID: wfID,
				Tx:         tx,
			})
			if err != nil {
				return err
			}
			for _, child := range children {
				workflowIDs = append(workflowIDs, child.ID)
			}
		}
	}

	// Delete all matching workflows regardless of their state
	anyClause := dialectAnyClause(s.dialect, "workflow_uuid", 1)
	deleteQuery := s.RenderSQL(
		`DELETE FROM %sworkflow_status WHERE %s`,
		s.dialect.SchemaPrefix(s.schema), anyClause)
	encodedIDs, err := encodeArrayParam(s.dialect, workflowIDs)
	if err != nil {
		return fmt.Errorf("delete workflows: %w", err)
	}
	if _, err := tx.Exec(ctx, deleteQuery, encodedIDs); err != nil {
		return fmt.Errorf("failed to delete workflow(s): %w", err)
	}

	// If we created the transaction internally, commit it
	if input.Tx == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit deleteWorkflows transaction: %w", err)
		}
	}

	return nil
}

type GetWorkflowChildrenDBInput struct {
	WorkflowID string
	Tx         Tx
}

// GetWorkflowChildren retrieves all descendant workflows of the given parent workflow
// (breadth-first) within the same transaction.
func (s *SysDB) GetWorkflowChildren(ctx context.Context, input GetWorkflowChildrenDBInput) ([]models.WorkflowStatus, error) {

	children, err := s.ListWorkflows(ctx, ListWorkflowsDBInput{
		ParentWorkflowID: []string{input.WorkflowID},
		Tx:               input.Tx,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get children of workflow %s: %w", input.WorkflowID, err)
	}

	queue := make([]string, 0, len(children))
	for _, child := range children {
		queue = append(queue, child.ID)
	}
	for len(queue) > 0 {
		parentID := queue[0]
		queue = queue[1:]

		grandchildren, err := s.ListWorkflows(ctx, ListWorkflowsDBInput{
			ParentWorkflowID: []string{parentID},
			Tx:               input.Tx,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get children of workflow %s: %w", parentID, err)
		}
		for _, gc := range grandchildren {
			children = append(children, gc)
			queue = append(queue, gc.ID)
		}
	}

	return children, nil
}

func (s *SysDB) CancelAllBefore(ctx context.Context, cutoffTime time.Time) error {
	// List all workflows in PENDING, ENQUEUED, or DELAYED state ending at cutoffTime
	listInput := ListWorkflowsDBInput{
		EndTime: cutoffTime,
		Status:  []models.WorkflowStatusType{models.WorkflowStatusPending, models.WorkflowStatusEnqueued, models.WorkflowStatusDelayed},
	}

	workflows, err := s.ListWorkflows(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list workflows for cancellation: %w", err)
	}

	if len(workflows) == 0 {
		return nil
	}

	ids := make([]string, len(workflows))
	for i, workflow := range workflows {
		ids[i] = workflow.ID
	}
	if _, err := s.CancelWorkflows(ctx, CancelWorkflowsDBInput{WorkflowIDs: ids}); err != nil {
		return fmt.Errorf("failed to cancel workflows during cancelAllBefore: %w", err)
	}
	return nil
}

type GarbageCollectWorkflowsInput struct {
	CutoffEpochTimestampMs *int64
	RowsThreshold          *int
}

func (s *SysDB) GarbageCollectWorkflows(ctx context.Context, input GarbageCollectWorkflowsInput) error {
	// Validate input parameters
	if input.RowsThreshold != nil && *input.RowsThreshold <= 0 {
		return fmt.Errorf("rowsThreshold must be greater than 0, got %d", *input.RowsThreshold)
	}

	cutoffTimestamp := input.CutoffEpochTimestampMs

	// If rowsThreshold is provided, get the timestamp of the Nth newest workflow
	if input.RowsThreshold != nil {
		query := s.RenderSQL(`SELECT created_at
				  FROM %sworkflow_status
				  ORDER BY created_at DESC
				  LIMIT 1 OFFSET $1`, s.dialect.SchemaPrefix(s.schema))

		var rowsBasedCutoff int64
		err := s.pool.QueryRow(ctx, query, *input.RowsThreshold-1).Scan(&rowsBasedCutoff)
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("failed to query cutoff timestamp by rows threshold: %w", err)
		}
		// If we don't have a provided cutoffTimestamp and found one in the database
		// Or if the found cutoffTimestamp deletes more (higher timestamp = more recent cutoff = more rows deleted), as needed to enforce the rows threshold
		// Use the cutoff timestamp found in the database
		if rowsBasedCutoff > 0 && cutoffTimestamp == nil || (cutoffTimestamp != nil && rowsBasedCutoff > *cutoffTimestamp) {
			cutoffTimestamp = &rowsBasedCutoff
		}
	}

	// If no cutoff is determined, no garbage collection is needed
	if cutoffTimestamp == nil {
		return nil
	}

	// Delete all workflows older than cutoff that are NOT PENDING, ENQUEUED, or DELAYED
	query := s.RenderSQL(`DELETE FROM %sworkflow_status
			  WHERE created_at < $1
			    AND status NOT IN ($2, $3, $4)`, s.dialect.SchemaPrefix(s.schema))

	commandTag, err := s.pool.Exec(ctx, query,
		*cutoffTimestamp,
		models.WorkflowStatusPending,
		models.WorkflowStatusEnqueued,
		models.WorkflowStatusDelayed)

	if err != nil {
		return fmt.Errorf("failed to garbage collect workflows: %w", err)
	}

	deletedCount, _ := commandTag.RowsAffected()
	s.logger.Info("Garbage collected workflows",
		"cutoff_timestamp", *cutoffTimestamp,
		"deleted_count", deletedCount)

	return nil
}

type ResumeWorkflowsDBInput struct {
	WorkflowIDs []string
	QueueName   string
	Tx          Tx
}

// ResumeWorkflows re-enqueues the given workflows onto the specified queue (or the internal
// queue if unset). It returns the subset of IDs that existed in workflow_status; IDs in
// terminal states are considered existing even though they are not updated.
func (s *SysDB) ResumeWorkflows(ctx context.Context, input ResumeWorkflowsDBInput) ([]string, error) {
	if len(input.WorkflowIDs) == 0 {
		return nil, nil
	}

	schemaPrefix := s.dialect.SchemaPrefix(s.schema)
	anyClause := dialectAnyClause(s.dialect, "workflow_uuid", 5)

	queueName := input.QueueName
	if queueName == "" {
		queueName = models.InternalQueueName
	}

	encodedIDs, err := encodeArrayParam(s.dialect, input.WorkflowIDs)
	if err != nil {
		return nil, fmt.Errorf("resume workflows: %w", err)
	}

	args := []any{
		models.WorkflowStatusEnqueued,
		queueName,
		0,
		time.Now().UnixMilli(),
		encodedIDs,
		models.WorkflowStatusSuccess,
		models.WorkflowStatusError,
	}

	// Dialects without data-modifying CTEs (sqlite) split the pg
	// single-statement CTE into two statements (UPDATE then SELECT).
	// Needs repeatable read. Reuse the caller's tx when supplied.
	if !s.dialect.SupportsDataModifyingCTE() {
		updateQuery := s.RenderSQL(`UPDATE %sworkflow_status
			SET status = $1, queue_name = $2, recovery_attempts = $3,
			    workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
			    started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
			WHERE %s AND status NOT IN ($6, $7)`, schemaPrefix, anyClause)
		selectAnyClause := dialectAnyClause(s.dialect, "workflow_uuid", 1)
		selectQuery := s.RenderSQL(`SELECT workflow_uuid FROM %sworkflow_status WHERE %s`, schemaPrefix, selectAnyClause)

		var runner Querier
		var localTx Tx
		if input.Tx != nil {
			runner = input.Tx
		} else {
			tx, err := s.pool.BeginTx(ctx, TxOptions{IsoLevel: s.dialect.SnapshotIsolation()})
			if err != nil {
				return nil, fmt.Errorf("failed to begin transaction: %w", err)
			}
			defer tx.Rollback(ctx)
			localTx = tx
			runner = tx
		}

		if _, err := runner.Exec(ctx, updateQuery, args...); err != nil {
			return nil, fmt.Errorf("failed to resume workflows: %w", err)
		}
		rows, err := runner.Query(ctx, selectQuery, args[4])
		if err != nil {
			return nil, fmt.Errorf("failed to list resumed workflow ids: %w", err)
		}
		found := make([]string, 0, len(input.WorkflowIDs))
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				scanErr := fmt.Errorf("failed to scan resumed workflow id: %w", err)
				if cerr := rows.Close(); cerr != nil {
					return nil, errors.Join(scanErr, fmt.Errorf("close rows: %w", cerr))
				}
				return nil, scanErr
			}
			found = append(found, id)
		}
		if cerr := rows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close resumed workflow rows: %w", cerr)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to read resumed workflow ids: %w", err)
		}
		if localTx != nil {
			if err := localTx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("failed to commit resume workflows tx: %w", err)
			}
		}
		return found, nil
	}

	query := s.RenderSQL(`WITH existing AS (
			SELECT workflow_uuid FROM %sworkflow_status WHERE %s
		), updated AS (
			UPDATE %sworkflow_status
			SET status = $1, queue_name = $2, recovery_attempts = $3,
			    workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
			    started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
			WHERE %s AND status NOT IN ($6, $7)
			RETURNING workflow_uuid
		)
		SELECT workflow_uuid FROM existing`, schemaPrefix, anyClause, schemaPrefix, anyClause)

	var rows Rows
	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, args...)
	} else {
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to resume workflows: %w", err)
	}
	defer rows.Close()

	found := make([]string, 0, len(input.WorkflowIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan resumed workflow id: %w", err)
		}
		found = append(found, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read resumed workflow ids: %w", err)
	}
	return found, nil
}

type ForkWorkflowsDBInput struct {
	OriginalWorkflowIDs []string
	ForkedWorkflowIDs   []string // Optional: must match originalWorkflowIDs in length if set; empty entries are auto-generated
	StartSteps          []int
	ApplicationVersion  string
	QueueName           string
	QueuePartitionKey   string
	Tx                  Tx
}

func (s *SysDB) ForkWorkflows(ctx context.Context, input ForkWorkflowsDBInput) ([]string, error) {
	if len(input.OriginalWorkflowIDs) == 0 {
		return []string{}, nil
	}
	if len(input.StartSteps) != len(input.OriginalWorkflowIDs) {
		return nil, errors.New("originalWorkflowIDs and startSteps must have the same length")
	}
	if len(input.ForkedWorkflowIDs) > 0 && len(input.ForkedWorkflowIDs) != len(input.OriginalWorkflowIDs) {
		return nil, errors.New("originalWorkflowIDs and forkedWorkflowIDs must have the same length")
	}

	// Validate start steps and generate forked workflow IDs where not provided
	forkedWorkflowIDs := make([]string, len(input.OriginalWorkflowIDs))
	for i := range input.OriginalWorkflowIDs {
		if input.StartSteps[i] < 0 {
			return nil, fmt.Errorf("startStep must be >= 0, got %d", input.StartSteps[i])
		}
		if len(input.ForkedWorkflowIDs) > 0 && input.ForkedWorkflowIDs[i] != "" {
			forkedWorkflowIDs[i] = input.ForkedWorkflowIDs[i]
		} else {
			forkedWorkflowIDs[i] = uuid.New().String()
		}
	}

	tx := input.Tx
	ownTx := tx == nil
	if ownTx {
		var err error
		tx, err = s.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to begin fork transaction: %w", err)
		}
		defer tx.Rollback(ctx)
	}
	execCtx := tx.Exec

	// Get the original workflow statuses in one query. Use the same tx so the
	// read sees the pre-fork state consistently with the writes below.
	listInput := ListWorkflowsDBInput{
		WorkflowIDs: input.OriginalWorkflowIDs,
		LoadInput:   true,
		Tx:          tx,
	}
	wfs, err := s.ListWorkflows(ctx, listInput)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}
	statusByID := make(map[string]models.WorkflowStatus, len(wfs))
	for _, wf := range wfs {
		statusByID[wf.ID] = wf
	}
	for _, id := range input.OriginalWorkflowIDs {
		if _, ok := statusByID[id]; !ok {
			return nil, models.NewNonExistentWorkflowError(id)
		}
	}

	// Determine the queue to place the forked workflows on
	queueName := input.QueueName
	if queueName == "" {
		queueName = models.InternalQueueName
	}

	var queuePartitionKey any
	if input.QueuePartitionKey != "" {
		queuePartitionKey = input.QueuePartitionKey
	}

	// Bulk insert all forked workflow status rows in one statement, each with
	// the same initial values as its original.
	insertColumns := []string{
		"workflow_uuid", "status", "name", "authenticated_user", "assumed_role",
		"authenticated_roles", "application_version", "application_id", "queue_name",
		"queue_partition_key", "inputs", "created_at", "updated_at", "recovery_attempts",
		"forked_from", "serialization", "class_name", "config_name", "attributes",
	}
	valueRows := make([]string, len(input.OriginalWorkflowIDs))
	insertArgs := make([]any, 0, len(input.OriginalWorkflowIDs)*len(insertColumns))
	nowMs := time.Now().UnixMilli()
	for i, originalWorkflowID := range input.OriginalWorkflowIDs {
		originalWorkflow := statusByID[originalWorkflowID]

		// Determine the application version to use
		appVersion := originalWorkflow.ApplicationVersion
		if input.ApplicationVersion != "" {
			appVersion = input.ApplicationVersion
		}

		// Marshal authenticated roles (slice of strings) to JSON for TEXT column
		authenticatedRoles, err := json.Marshal(originalWorkflow.AuthenticatedRoles)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal the authenticated roles: %w", err)
		}

		var className any
		if originalWorkflow.ClassName != "" {
			className = originalWorkflow.ClassName
		}

		var attributesJSON any
		if len(originalWorkflow.Attributes) > 0 {
			marshaled, err := json.Marshal(originalWorkflow.Attributes)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal workflow attributes: %w", err)
			}
			attributesJSON = string(marshaled)
		}

		placeholders := make([]string, len(insertColumns))
		for j := range placeholders {
			placeholders[j] = fmt.Sprintf("$%d", i*len(insertColumns)+j+1)
		}
		valueRows[i] = "(" + strings.Join(placeholders, ", ") + ")"
		insertArgs = append(insertArgs,
			forkedWorkflowIDs[i],
			models.WorkflowStatusEnqueued,
			originalWorkflow.Name,
			originalWorkflow.AuthenticatedUser,
			originalWorkflow.AssumedRole,
			authenticatedRoles,
			appVersion,
			originalWorkflow.ApplicationID,
			queueName,
			queuePartitionKey,
			originalWorkflow.Input, // encoded
			nowMs,
			nowMs,
			0,
			originalWorkflowID, // forked_from
			originalWorkflow.Serialization,
			className,
			originalWorkflow.ConfigName,
			attributesJSON)
	}
	insertQuery := s.RenderSQL(`INSERT INTO %sworkflow_status (`+strings.Join(insertColumns, ", ")+`)
		VALUES `+strings.Join(valueRows, ", "), s.dialect.SchemaPrefix(s.schema))
	if _, err = execCtx(ctx, insertQuery, insertArgs...); err != nil {
		return nil, fmt.Errorf("failed to insert forked workflow statuses: %w", err)
	}

	// For workflows forked from a step > 0, copy checkpoints, events, and streams.
	// A UNION ALL mapping of (orig_id, fork_id, start_step) makes each table copy
	// a single statement regardless of batch size.
	mappingBranches := make([]string, 0, len(input.OriginalWorkflowIDs))
	mappingArgs := make([]any, 0, len(input.OriginalWorkflowIDs)*3)
	for i, originalWorkflowID := range input.OriginalWorkflowIDs {
		if input.StartSteps[i] <= 0 {
			continue
		}
		base := len(mappingArgs)
		mappingBranches = append(mappingBranches, fmt.Sprintf(
			"SELECT CAST($%d AS TEXT) AS orig_id, CAST($%d AS TEXT) AS fork_id, CAST($%d AS INTEGER) AS start_step",
			base+1, base+2, base+3))
		mappingArgs = append(mappingArgs, originalWorkflowID, forkedWorkflowIDs[i], input.StartSteps[i])
	}

	if len(mappingBranches) > 0 {
		mapping := "(" + strings.Join(mappingBranches, " UNION ALL ") + ") AS m"

		copyOutputsQuery := s.RenderSQL(`INSERT INTO %soperation_outputs
			(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
			SELECT m.fork_id, oo.function_id, oo.output, oo.error, oo.function_name, oo.child_workflow_id, oo.started_at_epoch_ms, oo.completed_at_epoch_ms, oo.serialization
			FROM `+mapping+`
			JOIN %soperation_outputs oo ON oo.workflow_uuid = m.orig_id AND oo.function_id < m.start_step`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyOutputsQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy operation outputs: %w", err)
		}

		copyEventsHistoryQuery := s.RenderSQL(`INSERT INTO %sworkflow_events_history
			(workflow_uuid, function_id, key, value, serialization)
			SELECT m.fork_id, h.function_id, h.key, h.value, h.serialization
			FROM `+mapping+`
			JOIN %sworkflow_events_history h ON h.workflow_uuid = m.orig_id AND h.function_id < m.start_step`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyEventsHistoryQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy workflow events history: %w", err)
		}

		// Copy only the latest version of each event (highest function_id per key) into workflow_events.
		copyLatestEventsQuery := s.RenderSQL(`INSERT INTO %sworkflow_events (workflow_uuid, key, value, serialization)
			SELECT workflow_uuid, key, value, serialization FROM (
				SELECT m.fork_id AS workflow_uuid, h.key AS key, h.value AS value, h.serialization AS serialization,
					ROW_NUMBER() OVER (PARTITION BY m.fork_id, h.key ORDER BY h.function_id DESC) AS rn
				FROM `+mapping+`
				JOIN %sworkflow_events_history h ON h.workflow_uuid = m.orig_id AND h.function_id < m.start_step
			) ranked WHERE rn = 1`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyLatestEventsQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy latest workflow events: %w", err)
		}

		copyStreamsQuery := s.RenderSQL(`INSERT INTO %sstreams
			(workflow_uuid, key, value, "offset", function_id, serialization)
			SELECT m.fork_id, st.key, st.value, st."offset", st.function_id, st.serialization
			FROM `+mapping+`
			JOIN %sstreams st ON st.workflow_uuid = m.orig_id AND st.function_id < m.start_step`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyStreamsQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy streams: %w", err)
		}
	}

	// Mark the original workflows as having been forked from.
	markIDs, err := encodeArrayParam(s.dialect, input.OriginalWorkflowIDs)
	if err != nil {
		return nil, err
	}
	markForkedQuery := s.RenderSQL(`UPDATE %sworkflow_status SET was_forked_from = TRUE WHERE `+dialectAnyClause(s.dialect, "workflow_uuid", 1), s.dialect.SchemaPrefix(s.schema))
	if _, err = execCtx(ctx, markForkedQuery, markIDs); err != nil {
		return nil, fmt.Errorf("failed to mark original workflows as forked: %w", err)
	}

	if ownTx {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit fork transaction: %w", err)
		}
	}
	return forkedWorkflowIDs, nil
}

type ForkFromDBInput struct {
	WorkflowIDs        []string
	ApplicationVersion string
	QueueName          string
	QueuePartitionKey  string
	FromLastFailure    bool
	FromLastStep       bool
	FromStep           *int
	FromStepName       *string
}

// ForkFrom forks a batch of workflows, computing each workflow's start step
// from its recorded checkpoints according to exactly one of four modes:
// fromLastFailure (last step that recorded an error, falling back to the last step),
// fromLastStep, fromStep (explicit step), or fromStepName (last occurrence of a named step).
func (s *SysDB) ForkFrom(ctx context.Context, input ForkFromDBInput) ([]string, error) {
	modes := 0
	for _, set := range []bool{input.FromLastFailure, input.FromLastStep, input.FromStep != nil, input.FromStepName != nil} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return nil, errors.New("exactly one of fromLastFailure, fromLastStep, fromStep, or fromStepName must be specified")
	}
	if len(input.WorkflowIDs) == 0 {
		return []string{}, nil
	}

	startSteps := make(map[string]int, len(input.WorkflowIDs))
	if input.FromStep != nil {
		for _, id := range input.WorkflowIDs {
			startSteps[id] = *input.FromStep
		}
	} else {
		idsParam, err := encodeArrayParam(s.dialect, input.WorkflowIDs)
		if err != nil {
			return nil, err
		}
		args := []any{idsParam}

		var stepExpr string
		switch {
		case input.FromLastFailure:
			stepExpr = "COALESCE(MAX(CASE WHEN error IS NOT NULL THEN function_id END), MAX(function_id))"
		default: // fromLastStep and fromStepName
			stepExpr = "MAX(function_id)"
		}
		nameFilter := ""
		if input.FromStepName != nil {
			nameFilter = " AND function_name = $2"
			args = append(args, *input.FromStepName)
		}

		query := s.RenderSQL(`SELECT workflow_uuid, `+stepExpr+`
			FROM %soperation_outputs
			WHERE `+dialectAnyClause(s.dialect, "workflow_uuid", 1)+nameFilter+`
			GROUP BY workflow_uuid`, s.dialect.SchemaPrefix(s.schema))

		rows, err := s.pool.Query(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to query start steps: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var workflowID string
			var startStep int
			if err := rows.Scan(&workflowID, &startStep); err != nil {
				return nil, fmt.Errorf("failed to scan start step: %w", err)
			}
			startSteps[workflowID] = startStep
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to read start steps: %w", err)
		}

		for _, id := range input.WorkflowIDs {
			if _, ok := startSteps[id]; !ok {
				if input.FromStepName != nil {
					return nil, fmt.Errorf("workflow %s has no step named '%s'", id, *input.FromStepName)
				}
				return nil, fmt.Errorf("workflow %s has no steps", id)
			}
		}
	}

	orderedStartSteps := make([]int, len(input.WorkflowIDs))
	for i, id := range input.WorkflowIDs {
		orderedStartSteps[i] = startSteps[id]
	}
	return s.ForkWorkflows(ctx, ForkWorkflowsDBInput{
		OriginalWorkflowIDs: input.WorkflowIDs,
		StartSteps:          orderedStartSteps,
		ApplicationVersion:  input.ApplicationVersion,
		QueueName:           input.QueueName,
		QueuePartitionKey:   input.QueuePartitionKey,
	})
}

type AwaitWorkflowResultOutput struct {
	Output        *string
	Serialization string
	ErrStr        *string
}

func (s *SysDB) AwaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*AwaitWorkflowResultOutput, error) {
	query := s.RenderSQL(`SELECT status, output, error, recovery_attempts, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
	var status models.WorkflowStatusType
	if pollInterval <= 0 {
		pollInterval = DBRetryInterval
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
		var serialization *string
		err := row.Scan(&status, &outputString, &errorStr, &attempts, &serialization)
		if err != nil {
			if err == pgx.ErrNoRows {
				time.Sleep(pollInterval)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		var storedSerialization string
		if serialization != nil {
			storedSerialization = *serialization
		}
		result := &AwaitWorkflowResultOutput{Output: outputString, Serialization: storedSerialization}

		switch status {
		case models.WorkflowStatusSuccess, models.WorkflowStatusError:
			if errorStr != nil && len(*errorStr) > 0 {
				result.ErrStr = errorStr
			}
			return result, nil
		case models.WorkflowStatusCancelled:
			return result, models.NewAwaitedWorkflowCancelledError(workflowID)
		case models.WorkflowStatusMaxRecoveryAttemptsExceeded:
			return result, models.NewDeadLetterQueueError(workflowID, attempts-2)
		default:
			time.Sleep(pollInterval)
		}
	}
}

type RecordOperationResultDBInput struct {
	WorkflowID      string
	ChildWorkflowID string
	StepID          int
	StepName        string
	Output          *string
	ErrStr          *string
	Tx              Tx
	StartedAt       time.Time
	CompletedAt     time.Time
	Serialization   string
	ExecutorID      string
}

// RecordOperationResult checkpoints a step outcome. A checkpoint already
// existing at (workflow_uuid, function_id) is disambiguated by content:
//   - identical to input (including the caller's timestamps) → our own earlier
//     write whose commit ack was lost; the retry is a no-op success.
//   - different function name → determinism violation (ErrorCodeUnexpectedStep).
//   - anything else → a concurrent execution of this workflow checkpointed the
//     step first → ErrorCodeConflictingID. Callers must surface it as the step
//     error so the workflow-level handler parks this run in polling mode
//     rather than racing the other execution step by step.
//
// ON CONFLICT DO NOTHING (instead of letting the unique violation surface)
// keeps a caller-owned transaction healthy so it can still be used or rolled
// back cleanly after the conflict.
func (s *SysDB) RecordOperationResult(ctx context.Context, input RecordOperationResultDBInput) error {
	startedAtMs := input.StartedAt.UnixMilli()
	completedAtMs := input.CompletedAt.UnixMilli()

	columns := []string{"workflow_uuid", "function_id", "output", "error", "function_name", "started_at_epoch_ms", "completed_at_epoch_ms", "serialization"}
	placeholders := []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"}
	args := []any{input.WorkflowID, input.StepID, input.Output, input.ErrStr, input.StepName, startedAtMs, completedAtMs, input.Serialization}
	argCounter := 8

	if input.ChildWorkflowID != "" {
		columns = append(columns, "child_workflow_id")
		argCounter++
		placeholders = append(placeholders, fmt.Sprintf("$%d", argCounter))
		args = append(args, input.ChildWorkflowID)
	}

	query := s.RenderSQL(`INSERT INTO %soperation_outputs (%s) VALUES (%s)
		ON CONFLICT (workflow_uuid, function_id) DO NOTHING`,
		s.dialect.SchemaPrefix(s.schema), strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	var querier Querier = s.pool
	if input.Tx != nil {
		querier = input.Tx
	}

	result, err := querier.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected after recording operation result: %w", err)
	}
	if n > 0 {
		s.refreshExecutorID(ctx, querier, input.WorkflowID, input.ExecutorID)
		return nil
	}

	selectQuery := s.RenderSQL(`SELECT output, error, function_name, serialization, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
		FROM %soperation_outputs
		WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))
	var storedOutput *string
	var storedError *string
	var storedFunctionName string
	var storedSerialization *string
	var storedChildID *string
	var storedStartedAtMs *int64
	var storedCompletedAtMs *int64
	err = querier.QueryRow(ctx, selectQuery, input.WorkflowID, input.StepID).Scan(
		&storedOutput, &storedError, &storedFunctionName, &storedSerialization, &storedChildID, &storedStartedAtMs, &storedCompletedAtMs)
	if err != nil {
		if err == pgx.ErrNoRows {
			// This should only happen if the conflicting row was deleted, e.g., during GC
			return models.NewWorkflowConflictIDError(input.WorkflowID)
		}
		return fmt.Errorf("failed to read existing operation result: %w", err)
	}
	// Our own earlier write (commit succeeded but its ack was lost) is identical
	// to the input, including the caller-supplied timestamps: the retry already
	// happened, report success.
	sameWrite := input.StepName == storedFunctionName &&
		nullableStrEq(storedOutput, input.Output) &&
		nullableStrEq(storedError, input.ErrStr) &&
		nullableStrEq(storedSerialization, &input.Serialization) &&
		derefStr(storedChildID) == input.ChildWorkflowID &&
		storedStartedAtMs != nil && *storedStartedAtMs == startedAtMs &&
		storedCompletedAtMs != nil && *storedCompletedAtMs == completedAtMs
	if sameWrite {
		return nil
	}
	if input.StepName != storedFunctionName {
		return models.NewUnexpectedStepError(input.WorkflowID, input.StepID, input.StepName, storedFunctionName)
	}
	// A concurrent execution's row differs (at minimum in its timestamps):
	// report the conflict so the caller parks this run.
	return models.NewWorkflowConflictIDError(input.WorkflowID)
}

func (s *SysDB) refreshExecutorID(ctx context.Context, querier Querier, workflowID, executorID string) {
	if executorID == "" { // Shouldn't happen!
		return
	}
	query := s.RenderSQL(`UPDATE %sworkflow_status SET executor_id = $1
		WHERE workflow_uuid = $2 AND (executor_id IS NULL OR executor_id <> $1)`,
		s.dialect.SchemaPrefix(s.schema))
	if _, err := querier.Exec(ctx, query, executorID, workflowID); err != nil {
		s.logger.Warn("failed to refresh workflow executor ID after checkpoint",
			"workflow_id", workflowID, "executor_id", executorID, "error", err)
	}
}

// nullableStrEq compares two nullable strings, treating NULL and "" as equal.
func nullableStrEq(a, b *string) bool {
	return derefStr(a) == derefStr(b)
}

func derefStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

/*******************************/
/******* CHILD WORKFLOWS ********/
/*******************************/

type RecordChildWorkflowDBInput struct {
	ParentWorkflowID string
	ChildWorkflowID  string
	StepID           int
	StepName         string
	Tx               Tx
}

func (s *SysDB) RecordChildWorkflow(ctx context.Context, input RecordChildWorkflowDBInput) error {
	// Idempotent: a retry after a lost commit ack (or a concurrent recovery of
	// the parent) re-inserts the same row; only a *different* child at the same
	// step is a determinism violation. ON CONFLICT DO NOTHING raises no error,
	// so a duplicate never aborts the caller's transaction; on conflict, read
	// back the recorded child and compare.
	query := s.RenderSQL(`INSERT INTO %soperation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (workflow_uuid, function_id) DO NOTHING`, s.dialect.SchemaPrefix(s.schema))

	var querier Querier = s.pool
	if input.Tx != nil {
		querier = input.Tx
	}

	result, err := querier.Exec(ctx, query,
		input.ParentWorkflowID, input.StepID, input.StepName, input.ChildWorkflowID)
	if err != nil {
		return fmt.Errorf("failed to record child workflow: %w", err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected after recording child workflow: %w", err)
	}
	if n == 0 {
		selectQuery := s.RenderSQL(`SELECT child_workflow_id
              FROM %soperation_outputs
              WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))
		var recordedChildID *string
		if err := querier.QueryRow(ctx, selectQuery, input.ParentWorkflowID, input.StepID).Scan(&recordedChildID); err != nil {
			return fmt.Errorf("failed to check existing child workflow record: %w", err)
		}
		if recordedChildID == nil || *recordedChildID != input.ChildWorkflowID {
			recorded := "<nil>"
			if recordedChildID != nil {
				recorded = *recordedChildID
			}
			return models.NewUnexpectedStepError(input.ParentWorkflowID, input.StepID, input.ChildWorkflowID, recorded)
		}
	}

	return nil
}

func (s *SysDB) CheckChildWorkflow(ctx context.Context, workflowID string, functionID int, functionName string) (*string, error) {
	query := s.RenderSQL(`SELECT child_workflow_id, function_name
              FROM %soperation_outputs
              WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))

	var childWorkflowID *string
	var recordedFunctionName string
	err := s.pool.QueryRow(ctx, query, workflowID, functionID).Scan(&childWorkflowID, &recordedFunctionName)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to check child workflow: %w", err)
	}

	// A function is already recorded at this step ID. If it was invoked under a
	// different name than on the original execution, the workflow is
	// non-deterministic (a different child workflow or step is being called).
	if functionName != recordedFunctionName {
		return nil, models.NewUnexpectedStepError(workflowID, functionID, functionName, recordedFunctionName)
	}

	return childWorkflowID, nil
}

// GetDeduplicatedWorkflow returns the ID of the workflow currently holding the
// deduplication slot for (queueName, deduplicationID), or nil if the slot is free.
func (s *SysDB) GetDeduplicatedWorkflow(ctx context.Context, queueName, deduplicationID string) (*string, error) {
	query := s.RenderSQL(`SELECT workflow_uuid
              FROM %sworkflow_status
              WHERE queue_name = $1 AND deduplication_id = $2`, s.dialect.SchemaPrefix(s.schema))

	var workflowID *string
	err := s.pool.QueryRow(ctx, query, queueName, deduplicationID).Scan(&workflowID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get deduplicated workflow: %w", err)
	}

	return workflowID, nil
}

/*******************************/
/******* STEPS ********/
/*******************************/

type RecordedResult struct {
	Output        *string
	ErrStr        *string
	Serialization string
}

type CheckOperationExecutionDBInput struct {
	WorkflowID string
	StepID     int
	StepName   string
	Tx         Tx
}

func (s *SysDB) CheckOperationExecution(ctx context.Context, input CheckOperationExecutionDBInput) (*RecordedResult, error) {
	var tx Tx
	var err error

	// Use provided transaction or create a new one
	if input.Tx != nil {
		tx = input.Tx
	} else {
		tx, err = s.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) // We don't need to commit this transaction -- it is just useful for having READ COMMITTED across the reads
	}

	// First query: Retrieve the workflow status
	workflowStatusQuery := s.RenderSQL(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

	// Second query: Retrieve operation outputs if they exist
	stepOutputQuery := s.RenderSQL(`SELECT output, error, function_name, serialization
							 FROM %soperation_outputs
							 WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))

	var workflowStatus models.WorkflowStatusType

	// Execute first query to get workflow status
	err = tx.QueryRow(ctx, workflowStatusQuery, input.WorkflowID).Scan(&workflowStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, models.NewNonExistentWorkflowError(input.WorkflowID)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}

	// If the workflow is cancelled, raise the exception
	if workflowStatus == models.WorkflowStatusCancelled {
		return nil, models.NewWorkflowCancelledError(input.WorkflowID, nil)
	}

	// Execute second query to get operation outputs
	var outputString *string
	var errorStr *string
	var recordedFunctionName string
	var serialization *string

	err = tx.QueryRow(ctx, stepOutputQuery, input.WorkflowID, input.StepID).Scan(&outputString, &errorStr, &recordedFunctionName, &serialization)

	// If there are no operation outputs, return nil
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	// If the provided and recorded function name are different, return an error
	if input.StepName != recordedFunctionName {
		return nil, models.NewUnexpectedStepError(input.WorkflowID, input.StepID, input.StepName, recordedFunctionName)
	}

	var storedSerialization string
	if serialization != nil {
		storedSerialization = *serialization
	}
	var recordedErrStr *string
	if errorStr != nil && *errorStr != "" {
		recordedErrStr = errorStr
	}
	result := &RecordedResult{
		Output:        outputString,
		ErrStr:        recordedErrStr,
		Serialization: storedSerialization,
	}
	return result, nil
}

// StepInfo contains information about a workflow step execution.
type StepRow struct {
	StepID          int       // The sequential ID of the step within the workflow
	StepName        string    // The name of the step function
	Output          *string   // The output returned by the step (if any)
	Error           error     // The error returned by the step (if any)
	ChildWorkflowID string    // The ID of a child workflow spawned by this step (if applicable)
	StartedAt       time.Time // When the step execution started
	CompletedAt     time.Time // When the step execution completed
	Serialization   string    // The serialization format used for this step
}

type GetWorkflowStepsInput struct {
	WorkflowID string
	LoadOutput bool
	Limit      *int
	Offset     *int
}

func (s *SysDB) GetWorkflowSteps(ctx context.Context, input GetWorkflowStepsInput) ([]StepRow, error) {
	loadColumns := []string{"function_id", "function_name", "error", "child_workflow_id", "started_at_epoch_ms", "completed_at_epoch_ms", "serialization"}
	if input.LoadOutput {
		loadColumns = append(loadColumns, "output")
	}
	query := s.RenderSQL(`SELECT `+strings.Join(loadColumns, ", ")+`
			  FROM %soperation_outputs
			  WHERE workflow_uuid = $1
			  ORDER BY function_id ASC`, s.dialect.SchemaPrefix(s.schema))

	args := []any{input.WorkflowID}
	if input.Limit != nil {
		args = append(args, *input.Limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	} else if input.Offset != nil {
		query += dialectNoLimitClause(s.dialect)
	}
	if input.Offset != nil {
		args = append(args, *input.Offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow steps: %w", err)
	}
	defer rows.Close()

	var steps []StepRow
	for rows.Next() {
		var step StepRow
		var outputString *string
		var errorString *string
		var childWorkflowID *string
		var startedAtMs, completedAtMs *int64
		var serialization *string

		scanArgs := []any{&step.StepID, &step.StepName, &errorString, &childWorkflowID, &startedAtMs, &completedAtMs, &serialization}
		if input.LoadOutput {
			scanArgs = append(scanArgs, &outputString)
		}
		err := rows.Scan(scanArgs...)
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
		if input.LoadOutput {
			step.Output = outputString
		}

		var storedSerialization string
		if serialization != nil {
			storedSerialization = *serialization
		}
		step.Serialization = storedSerialization
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

// WorkflowAggregateRow is a single row of a workflow aggregate query result.
// Group maps each grouping column name (e.g. "status", "name", "time_bucket") to its
// stringified value, with nil entries for grouping columns that were NULL for that row.
// Count, MinCreatedAt, MaxQueueWaitMs and MaxTotalLatencyMs are pointers because the caller
// selects which aggregates to compute; an unselected aggregate is nil (serialized as null,
// matching the other SDKs). MinCreatedAt is an epoch-ms timestamp; the latency fields are
// in milliseconds.
type WorkflowAggregateRow struct {
	Group             map[string]*string `json:"group"`
	Count             *int64             `json:"count"`
	MinCreatedAt      *int64             `json:"min_created_at"`
	MaxQueueWaitMs    *int64             `json:"max_queue_wait_ms"`
	MaxTotalLatencyMs *int64             `json:"max_total_latency_ms"`
}

// _DEFAULT_AGGREGATES_LIMIT caps the number of group rows returned by getWorkflowAggregates
// when the caller does not provide an override.
const _DEFAULT_AGGREGATES_LIMIT = 10_000_000

// GetWorkflowAggregatesDBInput represents the input parameters for getting workflow aggregates.
type GetWorkflowAggregatesDBInput struct {
	GroupByStatus             bool
	GroupByName               bool
	GroupByQueueName          bool
	GroupByExecutorID         bool
	GroupByApplicationVersion bool
	SelectCount               bool
	SelectMinCreatedAt        bool
	SelectMaxQueueWaitMs      bool
	SelectMaxTotalLatencyMs   bool
	TimeBucketSizeMs          int64 // 0 disables time bucketing
	Status                    []models.WorkflowStatusType
	StartTime                 time.Time
	EndTime                   time.Time
	CompletedAfter            time.Time
	CompletedBefore           time.Time
	DequeuedAfter             time.Time
	DequeuedBefore            time.Time
	WorkflowName              []string
	ApplicationVersion        []string
	ExecutorID                []string
	QueueName                 []string
	WorkflowIDPrefix          []string
	WorkflowIDs               []string
	AuthenticatedUser         []string
	ForkedFrom                []string
	ParentWorkflowID          []string
	WasForkedFrom             *bool
	HasParent                 *bool
	Attributes                map[string]any
	Limit                     int64 // 0 means use _DEFAULT_AGGREGATES_LIMIT
	Tx                        Tx
}

func (s *SysDB) GetWorkflowAggregates(ctx context.Context, input GetWorkflowAggregatesDBInput) ([]WorkflowAggregateRow, error) {
	if input.TimeBucketSizeMs < 0 {
		return nil, errors.New("timeBucketSizeMs must be > 0")
	}

	// Build group columns from boolean flags
	type groupCol struct {
		name string
		expr string
	}
	var groups []groupCol
	if input.GroupByStatus {
		groups = append(groups, groupCol{name: "status", expr: "status"})
	}
	if input.GroupByName {
		groups = append(groups, groupCol{name: "name", expr: "name"})
	}
	if input.GroupByQueueName {
		groups = append(groups, groupCol{name: "queue_name", expr: "queue_name"})
	}
	if input.GroupByExecutorID {
		groups = append(groups, groupCol{name: "executor_id", expr: "executor_id"})
	}
	if input.GroupByApplicationVersion {
		groups = append(groups, groupCol{name: "application_version", expr: "application_version"})
	}

	qb := newQueryBuilder(s.dialect)

	if input.TimeBucketSizeMs > 0 {
		// CockroachDB infers a placeholder's type from its first use and refuses
		// to reuse the same $n in two contexts with different types (here decimal
		// for the division, then int for the multiplication). Bind the bucket size
		// twice so each occurrence gets its own placeholder.
		qb.argCounter++
		divArg := qb.argCounter
		qb.args = append(qb.args, input.TimeBucketSizeMs)
		qb.argCounter++
		mulArg := qb.argCounter
		qb.args = append(qb.args, input.TimeBucketSizeMs)
		var expr string
		if s.dialect.SupportsArrayParameters() {
			// pg/CRDB: cast to numeric so FLOOR returns a true floor (not int trunc).
			expr = fmt.Sprintf("(CAST(FLOOR(created_at::numeric / $%d) AS BIGINT) * $%d)", divArg, mulArg)
		} else {
			// sqlite: created_at is INTEGER; INT/INT already truncates toward zero
			// which is FLOOR for non-negative epoch ms.
			expr = fmt.Sprintf("((created_at / $%d) * $%d)", divArg, mulArg)
		}
		groups = append(groups, groupCol{name: "time_bucket", expr: expr})
	}

	if len(groups) == 0 {
		return nil, errors.New("at least one group_by flag must be set, or a time bucket size provided")
	}

	// Apply filters using the query builder
	if len(input.Status) > 0 {
		qb.addWhereAny("status", input.Status)
	}
	if !input.StartTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.StartTime.UnixMilli())
	}
	if !input.EndTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.EndTime.UnixMilli())
	}
	if len(input.WorkflowName) > 0 {
		qb.addWhereAny("name", input.WorkflowName)
	}
	if len(input.ApplicationVersion) > 0 {
		qb.addWhereAny("application_version", input.ApplicationVersion)
	}
	if len(input.ExecutorID) > 0 {
		qb.addWhereAny("executor_id", input.ExecutorID)
	}
	if len(input.QueueName) > 0 {
		qb.addWhereAny("queue_name", input.QueueName)
	}
	if len(input.WorkflowIDPrefix) > 0 {
		qb.addWhereLikeAny("workflow_uuid", input.WorkflowIDPrefix, "%")
	}
	if len(input.WorkflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.WorkflowIDs)
	}
	if len(input.AuthenticatedUser) > 0 {
		qb.addWhereAny("authenticated_user", input.AuthenticatedUser)
	}
	if len(input.ForkedFrom) > 0 {
		qb.addWhereAny("forked_from", input.ForkedFrom)
	}
	if len(input.ParentWorkflowID) > 0 {
		qb.addWhereAny("parent_workflow_id", input.ParentWorkflowID)
	}
	if input.WasForkedFrom != nil {
		qb.addWhere("was_forked_from", *input.WasForkedFrom)
	}
	if input.HasParent != nil {
		if *input.HasParent {
			qb.addWhereIsNotNull("parent_workflow_id")
		} else {
			qb.addWhereIsNull("parent_workflow_id")
		}
	}
	if len(input.Attributes) > 0 {
		if !s.dialect.SupportsAttributesContainment() {
			return nil, fmt.Errorf("filtering workflows by attributes is not supported on %s; use a Postgres system database to filter by attributes", s.dialect.Name())
		}
		attributesJSON, err := json.Marshal(input.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal attributes filter: %w", err)
		}
		// JSONB containment (@>), served by the GIN index on the attributes column
		qb.argCounter++
		qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("attributes @> $%d::jsonb", qb.argCounter))
		qb.args = append(qb.args, string(attributesJSON))
	}
	// completed_after/before filter on completed_at; dequeued_after/before on
	// started_at_epoch_ms (the dequeue timestamp). Both are epoch-ms columns.
	if !input.CompletedAfter.IsZero() {
		qb.addWhereGreaterEqual("completed_at", input.CompletedAfter.UnixMilli())
	}
	if !input.CompletedBefore.IsZero() {
		qb.addWhereLessEqual("completed_at", input.CompletedBefore.UnixMilli())
	}
	if !input.DequeuedAfter.IsZero() {
		qb.addWhereGreaterEqual("started_at_epoch_ms", input.DequeuedAfter.UnixMilli())
	}
	if !input.DequeuedBefore.IsZero() {
		qb.addWhereLessEqual("started_at_epoch_ms", input.DequeuedBefore.UnixMilli())
	}

	// Build select aggregates. MAX/MIN ignore NULLs, so workflows missing a
	// started_at_epoch_ms or completed_at drop out of the queue-wait / latency maxima.
	type selectCol struct {
		name string
		expr string
	}
	var selects []selectCol
	if input.SelectCount {
		selects = append(selects, selectCol{name: "count", expr: "COUNT(*)"})
	}
	if input.SelectMinCreatedAt {
		selects = append(selects, selectCol{name: "min_created_at", expr: "MIN(created_at)"})
	}
	if input.SelectMaxQueueWaitMs {
		selects = append(selects, selectCol{name: "max_queue_wait_ms", expr: "MAX(started_at_epoch_ms - created_at)"})
	}
	if input.SelectMaxTotalLatencyMs {
		selects = append(selects, selectCol{name: "max_total_latency_ms", expr: "MAX(completed_at - created_at)"})
	}
	if len(selects) == 0 {
		return nil, errors.New("at least one select_ flag must be set")
	}

	// Build SELECT clause: each group expression aliased to "g0", "g1", ... so the position is stable
	// regardless of whether the expression is a column or a CAST(...) expression.
	selectParts := make([]string, 0, len(groups)+len(selects))
	groupParts := make([]string, 0, len(groups))
	for i, g := range groups {
		alias := fmt.Sprintf("g%d", i)
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", g.expr, alias))
		groupParts = append(groupParts, g.expr)
	}
	for i, sel := range selects {
		selectParts = append(selectParts, fmt.Sprintf("%s AS s%d", sel.expr, i))
	}

	query := fmt.Sprintf("SELECT %s FROM %sworkflow_status",
		strings.Join(selectParts, ", "),
		s.dialect.SchemaPrefix(s.schema))
	if len(qb.whereClauses) > 0 {
		query += " WHERE " + strings.Join(qb.whereClauses, " AND ")
	}
	query += " GROUP BY " + strings.Join(groupParts, ", ")
	limit := input.Limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	query += fmt.Sprintf(" LIMIT %d", limit)

	var rows Rows
	var err error
	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute getWorkflowAggregates query: %w", err)
	}
	defer rows.Close()

	results := make([]WorkflowAggregateRow, 0)
	for rows.Next() {
		// Scan each group column as nullable string, plus each selected aggregate as nullable int64.
		groupVals := make([]any, len(groups))
		for i := range groups {
			var v *string
			groupVals[i] = &v
		}
		selectVals := make([]any, len(selects))
		for i := range selects {
			var v *int64
			selectVals[i] = &v
		}
		scanArgs := append(append([]any{}, groupVals...), selectVals...)
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan workflow aggregate row: %w", err)
		}
		groupMap := make(map[string]*string, len(groups))
		for i, g := range groups {
			groupMap[g.name] = *(groupVals[i].(**string))
		}
		row := WorkflowAggregateRow{Group: groupMap}
		for i, sel := range selects {
			val := *(selectVals[i].(**int64))
			switch sel.name {
			case "count":
				row.Count = val
			case "min_created_at":
				row.MinCreatedAt = val
			case "max_queue_wait_ms":
				row.MaxQueueWaitMs = val
			case "max_total_latency_ms":
				row.MaxTotalLatencyMs = val
			}
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow aggregate rows: %w", err)
	}

	return results, nil
}

// StepAggregateRow is a single row of a step aggregate query result.
// Group maps each grouping column name (e.g. "function_name", "status", "time_bucket") to its
// stringified value, with nil entries for grouping columns that were NULL for that row.
// Count and MaxDurationMs are pointers because the caller selects which aggregates to compute;
// an unselected aggregate is nil (serialized as null, matching the other SDKs).
type StepAggregateRow struct {
	Group         map[string]*string `json:"group"`
	Count         *int64             `json:"count"`
	MaxDurationMs *int64             `json:"max_duration_ms"`
}

// GetStepAggregatesDBInput represents the input parameters for getting step aggregates.
type GetStepAggregatesDBInput struct {
	GroupByFunctionName bool
	GroupByStatus       bool
	SelectCount         bool
	SelectMaxDurationMs bool
	TimeBucketSizeMs    int64 // 0 disables time bucketing
	Status              []string
	FunctionName        []string
	WorkflowIDPrefix    []string
	CompletedAfter      time.Time
	CompletedBefore     time.Time
	Limit               int64 // 0 means use _DEFAULT_AGGREGATES_LIMIT
	Tx                  Tx
}

// statusExpr derives a step's status from operation_outputs: rows with a NULL error are
// SUCCESS, otherwise ERROR. operation_outputs has no explicit status column.
const stepStatusExpr = "(CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END)"

func (s *SysDB) GetStepAggregates(ctx context.Context, input GetStepAggregatesDBInput) ([]StepAggregateRow, error) {
	if input.TimeBucketSizeMs < 0 {
		return nil, errors.New("timeBucketSizeMs must be > 0")
	}

	type groupCol struct {
		name string
		expr string
	}
	var groups []groupCol
	if input.GroupByFunctionName {
		groups = append(groups, groupCol{name: "function_name", expr: "function_name"})
	}
	if input.GroupByStatus {
		groups = append(groups, groupCol{name: "status", expr: stepStatusExpr})
	}

	qb := newQueryBuilder(s.dialect)

	if input.TimeBucketSizeMs > 0 {
		// Bucket on completed_at_epoch_ms, the indexed timestamp on this table.
		// Bind the bucket size twice: see getWorkflowAggregates for why CockroachDB
		// requires a distinct placeholder per type context.
		qb.argCounter++
		divArg := qb.argCounter
		qb.args = append(qb.args, input.TimeBucketSizeMs)
		qb.argCounter++
		mulArg := qb.argCounter
		qb.args = append(qb.args, input.TimeBucketSizeMs)
		var expr string
		if s.dialect.SupportsArrayParameters() {
			expr = fmt.Sprintf("(CAST(FLOOR(completed_at_epoch_ms::numeric / $%d) AS BIGINT) * $%d)", divArg, mulArg)
		} else {
			expr = fmt.Sprintf("((completed_at_epoch_ms / $%d) * $%d)", divArg, mulArg)
		}
		groups = append(groups, groupCol{name: "time_bucket", expr: expr})
	}

	if len(groups) == 0 {
		return nil, errors.New("at least one group_by flag must be set, or a time bucket size provided")
	}

	// Build select aggregates. MAX ignores NULLs, so rows without start/complete
	// timestamps (child-workflow and getResult markers) drop out of the duration max.
	type selectCol struct {
		name string
		expr string
	}
	var selects []selectCol
	if input.SelectCount {
		selects = append(selects, selectCol{name: "count", expr: "COUNT(*)"})
	}
	if input.SelectMaxDurationMs {
		selects = append(selects, selectCol{name: "max_duration_ms", expr: "MAX(completed_at_epoch_ms - started_at_epoch_ms)"})
	}
	if len(selects) == 0 {
		return nil, errors.New("at least one select_ flag must be set")
	}

	// Apply filters
	if len(input.Status) > 0 {
		qb.addWhereAny(stepStatusExpr, input.Status)
	}
	if len(input.FunctionName) > 0 {
		qb.addWhereAny("function_name", input.FunctionName)
	}
	if len(input.WorkflowIDPrefix) > 0 {
		qb.addWhereLikeAny("workflow_uuid", input.WorkflowIDPrefix, "%")
	}
	if !input.CompletedAfter.IsZero() {
		qb.addWhereGreaterEqual("completed_at_epoch_ms", input.CompletedAfter.UnixMilli())
	}
	if !input.CompletedBefore.IsZero() {
		qb.addWhereLessEqual("completed_at_epoch_ms", input.CompletedBefore.UnixMilli())
	}

	// Build SELECT clause: group expressions aliased to "g0", "g1", ... so position is stable.
	selectParts := make([]string, 0, len(groups)+len(selects))
	groupParts := make([]string, 0, len(groups))
	for i, g := range groups {
		alias := fmt.Sprintf("g%d", i)
		selectParts = append(selectParts, fmt.Sprintf("%s AS %s", g.expr, alias))
		groupParts = append(groupParts, g.expr)
	}
	for i, sel := range selects {
		selectParts = append(selectParts, fmt.Sprintf("%s AS s%d", sel.expr, i))
	}

	query := fmt.Sprintf("SELECT %s FROM %soperation_outputs",
		strings.Join(selectParts, ", "),
		s.dialect.SchemaPrefix(s.schema))
	if len(qb.whereClauses) > 0 {
		query += " WHERE " + strings.Join(qb.whereClauses, " AND ")
	}
	query += " GROUP BY " + strings.Join(groupParts, ", ")
	limit := input.Limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	query += fmt.Sprintf(" LIMIT %d", limit)

	var rows Rows
	var err error
	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute getStepAggregates query: %w", err)
	}
	defer rows.Close()

	results := make([]StepAggregateRow, 0)
	for rows.Next() {
		groupVals := make([]any, len(groups))
		for i := range groups {
			var v *string
			groupVals[i] = &v
		}
		selectVals := make([]any, len(selects))
		for i := range selects {
			var v *int64
			selectVals[i] = &v
		}
		scanArgs := append(append([]any{}, groupVals...), selectVals...)
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan step aggregate row: %w", err)
		}
		groupMap := make(map[string]*string, len(groups))
		for i, g := range groups {
			groupMap[g.name] = *(groupVals[i].(**string))
		}
		row := StepAggregateRow{Group: groupMap}
		for i, sel := range selects {
			val := *(selectVals[i].(**int64))
			switch sel.name {
			case "count":
				row.Count = val
			case "max_duration_ms":
				row.MaxDurationMs = val
			}
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over step aggregate rows: %w", err)
	}

	return results, nil
}

/****************************************/
/******* PATCHES ********/
/****************************************/

type PatchDBInput struct {
	WorkflowID string
	StepID     int
	PatchName  string
}

func (s *SysDB) DoesPatchExists(ctx context.Context, input PatchDBInput) (string, error) {
	var functionName string
	query := s.RenderSQL(`SELECT function_name FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))
	return functionName, s.pool.QueryRow(ctx, query, input.WorkflowID, input.StepID).Scan(&functionName)
}

func (s *SysDB) Patch(ctx context.Context, input PatchDBInput) (bool, error) {
	functionName, err := s.DoesPatchExists(ctx, input)
	if err != nil {
		// No result means this is a new workflow, or an existing workflow that has not reached this step yet
		// Insert the patch marker and return true
		if err == pgx.ErrNoRows {
			insertQuery := s.RenderSQL(`INSERT INTO %soperation_outputs (workflow_uuid, function_id, function_name) VALUES ($1, $2, $3)`, s.dialect.SchemaPrefix(s.schema))
			_, err = s.pool.Exec(ctx, insertQuery, input.WorkflowID, input.StepID, input.PatchName)
			if err != nil {
				return false, fmt.Errorf("failed to insert patch marker: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("failed to check for patch: %w", err)
	}

	// If functionName != patchName, this is a workflow that existed before the patch was applied
	// Else this a new (patched) workflow that is being re-executed (e.g., recovery, or forked at a later step)
	return functionName == input.PatchName, nil
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

func (s *SysDB) notificationListenerLoop(ctx context.Context) {
	defer func() {
		s.logger.Debug("Notification listener loop exiting")
	}()

	pgxPool := s.ListenNotifyPool()
	if pgxPool == nil {
		s.logger.Error("Notification listener loop started without a pgx-backed pool; aborting")
		return
	}

	acquire := func(ctx context.Context) (*pgxpool.Conn, error) {
		// Acquire a connection from the pool and set up LISTEN on the notifications channels
		pc, err := pgxPool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := pc.Begin(ctx)
		if err != nil {
			pc.Release()
			return nil, err
		}
		for _, channel := range []string{_DBOS_NOTIFICATIONS_CHANNEL, _DBOS_WORKFLOW_EVENTS_CHANNEL, _DBOS_STREAMS_CHANNEL} {
			if _, err = tx.Exec(ctx, fmt.Sprintf("LISTEN %s", channel)); err != nil {
				rErr := tx.Rollback(ctx)
				if rErr != nil {
					s.logger.Error("Failed to rollback transaction after LISTEN error", "error", rErr)
				}
				pc.Release()
				return nil, err
			}
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

	poolConn, err := RetryWithResult(ctx, func() (*pgxpool.Conn, error) {
		return acquire(ctx)
	}, WithRetrierLogger(s.logger))
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
					if !WaitForRetry(ctx, ConnectionRetryBackoff.DelayFor(retryAttempt+1)) {
						return
					}
					retryAttempt++
				}
				// The connection is re-acquired. Wake all waiters so they re-poll the
				// database for a value whose notification may have been missed.
				s.RecvNotifier.notifyAll()
				s.EventNotifier.notifyAll()
				continue
			}
			// Other transient errors. Backoff and continue on same conn
			s.logger.Error("Error waiting for notification", "error", err)
			if !WaitForRetry(ctx, ConnectionRetryBackoff.DelayFor(retryAttempt+1)) {
				return
			}
			retryAttempt++
			continue
		}

		// Success: reduce backoff pressure
		if retryAttempt > 0 {
			retryAttempt--
		}

		switch n.Channel {
		case _DBOS_NOTIFICATIONS_CHANNEL:
			s.RecvNotifier.notify(n.Payload)
		case _DBOS_WORKFLOW_EVENTS_CHANNEL:
			s.EventNotifier.notify(n.Payload)
		case _DBOS_STREAMS_CHANNEL:
			s.streamNotifier.notify(n.Payload)
		}
	}
}

func (s *SysDB) notificationPollerLoop(ctx context.Context) {
	defer func() {
		s.logger.Debug("Notification poller loop exiting")
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

func (s *SysDB) pollNotifications(ctx context.Context) {
	// Iterate through all registered notification payloads
	for _, payload := range s.RecvNotifier.payloads() {
		// Parse payload: format is "destinationID::topic"
		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			s.logger.Warn("Invalid notification payload format", "payload", payload)
			continue
		}

		destinationID := parts[0]
		topic := parts[1]

		// Query database to check if an unconsumed notification exists
		query := s.RenderSQL(`SELECT EXISTS (SELECT 1 FROM %snotifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)`, s.dialect.SchemaPrefix(s.schema))
		var exists bool
		err := s.pool.QueryRow(ctx, query, destinationID, topic).Scan(&exists)
		if err != nil {
			s.logger.Warn("Failed to poll notification", "payload", payload, "error", err)
			continue
		}

		// If a notification exists, wake the waiters so they re-check.
		if exists {
			s.RecvNotifier.notify(payload)
		}
	}
}

func (s *SysDB) pollEvents(ctx context.Context) {
	// Iterate through all registered event payloads
	for _, payload := range s.EventNotifier.payloads() {
		// Parse payload: format is "targetWorkflowID::key"
		parts := strings.SplitN(payload, "::", 2)
		if len(parts) != 2 {
			s.logger.Warn("Invalid event payload format", "payload", payload)
			continue
		}

		targetWorkflowID := parts[0]
		eventKey := parts[1]

		// Query database to check if event exists
		query := s.RenderSQL(`SELECT EXISTS (SELECT 1 FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2)`, s.dialect.SchemaPrefix(s.schema))
		var exists bool
		err := s.pool.QueryRow(ctx, query, targetWorkflowID, eventKey).Scan(&exists)
		if err != nil {
			s.logger.Warn("Failed to poll event", "payload", payload, "error", err)
			continue
		}

		// If the event exists, wake the waiters so they re-check.
		if exists {
			s.EventNotifier.notify(payload)
		}
	}
}

const NullTopic = "__null__topic__"

type WorkflowSendInput struct {
	DestinationID  string
	Message        any
	Topic          string
	Tx             Tx
	Serialization  string
	IdempotencyKey string
}

// Send is a special type of step that sends a message to another workflow.
// Can be called both within a workflow (as a step) or outside a workflow (directly).
// When called within a workflow: durability and the function run in the same transaction, and we forbid nested step execution
func (s *SysDB) Send(ctx context.Context, input WorkflowSendInput) error {
	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// Set default topic if not provided
	topic := NullTopic
	if len(input.Topic) > 0 {
		topic = input.Topic
	}

	// ON CONFLICT DO NOTHING makes Send idempotent: with an idempotency key the
	// message_uuid is deterministic, so a retried Send inserts at most once. Without
	// a key the random UUID never collides, so the clause is a no-op.
	insertQuery := s.RenderSQL(`INSERT INTO %snotifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (message_uuid) DO NOTHING`, s.dialect.SchemaPrefix(s.schema))
	messageUUID := uuid.NewString()
	if input.IdempotencyKey != "" {
		messageUUID = fmt.Sprintf("%s::%s", input.IdempotencyKey, input.DestinationID)
	}
	createdAtMs := time.Now().UnixMilli()
	var err error
	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, insertQuery, input.DestinationID, topic, input.Message, input.Serialization, messageUUID, createdAtMs)
	} else {
		_, err = s.pool.Exec(ctx, insertQuery, input.DestinationID, topic, input.Message, input.Serialization, messageUUID, createdAtMs)
	}
	if err != nil {
		s.logger.Error("failed to insert notification", "error", err, "query", insertQuery, "destination_id", input.DestinationID, "topic", topic, "message", input.Message)
		// Check for foreign key violation (destination workflow doesn't exist)
		if s.dialect.IsForeignKeyViolation(err) {
			return models.NewNonExistentWorkflowError(input.DestinationID)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}
	return nil
}

// notifyRegistry delivers per-payload wake-ups to notification waiters (recv and
// getEvent).
type notifyRegistry struct {
	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{} // payload -> set of waiter channels
}

func newNotifyRegistry() *notifyRegistry {
	return &notifyRegistry{subs: make(map[string]map[chan struct{}]struct{})}
}

func (n *notifyRegistry) addLocked(payload string, ch chan struct{}) {
	set := n.subs[payload]
	if set == nil {
		set = make(map[chan struct{}]struct{})
		n.subs[payload] = set
	}
	set[ch] = struct{}{}
}

// subscribe registers a new waiter for payload and returns its wake channel.
func (n *notifyRegistry) subscribe(payload string) chan struct{} {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.addLocked(payload, ch)
	n.mu.Unlock()
	return ch
}

// subscribeExclusive registers the sole waiter for payload, returning false if one
// already exists.
func (n *notifyRegistry) subscribeExclusive(payload string) (chan struct{}, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.subs[payload]) > 0 {
		return nil, false
	}
	ch := make(chan struct{}, 1)
	n.addLocked(payload, ch)
	return ch, true
}

// unsubscribe removes a waiter; the payload entry is dropped once its last waiter leaves.
func (n *notifyRegistry) unsubscribe(payload string, ch chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	set := n.subs[payload]
	delete(set, ch)
	if len(set) == 0 {
		delete(n.subs, payload)
	}
}

// notify wakes every waiter for payload.
func (n *notifyRegistry) notify(payload string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ch := range n.subs[payload] {
		select {
		case ch <- struct{}{}:
		default: // Do not block (coalesce multiple notifications into one)
		}
	}
}

// notifyAll wakes every waiter regardless of payload; used after a listener
// reconnect so waiters re-poll for a value whose notification may have been missed.
func (n *notifyRegistry) notifyAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, set := range n.subs {
		for ch := range set {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

// payloads returns a snapshot of the currently registered payloads (used by the
// polling fallback to know which rows to check).
func (n *notifyRegistry) payloads() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.subs))
	for payload := range n.subs {
		out = append(out, payload)
	}
	return out
}

// has reports whether any waiter is registered for payload.
func (n *notifyRegistry) Has(payload string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.subs[payload]) > 0
}

// waiterCount reports the number of waiters registered for payload.
func (n *notifyRegistry) WaiterCount(payload string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.subs[payload])
}

// clear drops all registrations (used on shutdown).
func (n *notifyRegistry) clear() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.subs = make(map[string]map[chan struct{}]struct{})
}

// NotificationWaiter tracks a waiter registered for a notification (recv message or workflow event).
type NotificationWaiter struct {
	Pending bool                                   // the awaited row already existed at registration time
	Wait    func(deadline time.Time) (bool, error) // block until the row is pending or the deadline passes; true means timeout
	Release func()                                 // unregister the waiter; must be called after the result is read (or on abandonment)
}

func (s *SysDB) notificationWait(ctx context.Context, opName, payload string, ch <-chan struct{}, recheck func(context.Context) (bool, error)) func(deadline time.Time) (bool, error) {
	return func(deadline time.Time) (bool, error) {
		// The caller has already probed and found nothing; any notify since then is
		// buffered in ch, so wait for a wake before rechecking. The deadline bounds
		// recheck's retries so a DB outage cannot block past the timeout.
		waitCtx, cancel := context.WithDeadline(ctx, deadline)
		defer cancel()
		for {
			select {
			case <-ch:
				// A notification or reconnect repoll fired; re-check.
			case <-waitCtx.Done():
				if err := ctx.Err(); err != nil {
					s.logger.Warn(opName+" context cancelled", "payload", payload, "cause", context.Cause(ctx))
					return false, err
				}
				s.logger.Warn(opName+" timeout reached", "payload", payload, "deadline", deadline)
				return true, nil
			}
			found, err := recheck(waitCtx)
			if err != nil {
				if cerr := ctx.Err(); cerr != nil {
					s.logger.Warn(opName+" context cancelled", "payload", payload, "cause", context.Cause(ctx))
					return false, cerr
				}
				if waitCtx.Err() != nil {
					s.logger.Warn(opName+" timeout reached", "payload", payload, "deadline", deadline)
					return true, nil
				}
				return false, err
			}
			if found {
				return false, nil
			}
		}
	}
}

// StartRecvListener registers the calling workflow as the sole receiver for
// (destinationID, topic) and checks whether a message is already pending.
func (s *SysDB) StartRecvListener(ctx context.Context, destinationID, topic string) (*NotificationWaiter, error) {
	// A destination/topic may have only one receiver at a time.
	payload := fmt.Sprintf("%s::%s", destinationID, topic)
	ch, ok := s.RecvNotifier.subscribeExclusive(payload)
	if !ok {
		s.logger.Error("Receive already called for workflow", "destination_id", destinationID)
		return nil, models.NewWorkflowConflictIDError(destinationID)
	}
	release := func() { s.RecvNotifier.unsubscribe(payload, ch) }

	// recheck reports whether an unconsumed message is pending; it is used both for
	// the initial "already pending?" probe and by the wait loop after each wake.
	query := s.RenderSQL(`SELECT EXISTS (SELECT 1 FROM %snotifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)`, s.dialect.SchemaPrefix(s.schema))
	recheck := func(ctx context.Context) (bool, error) {
		return RetryWithResult(ctx, func() (bool, error) {
			var found bool
			if err := s.pool.QueryRow(ctx, query, destinationID, topic).Scan(&found); err != nil {
				return false, fmt.Errorf("failed to check message: %w", err)
			}
			return found, nil
		}, WithRetrierLogger(s.logger))
	}
	exists, err := recheck(ctx)
	if err != nil {
		release()
		return nil, err
	}
	wait := s.notificationWait(ctx, "Recv()", payload, ch, recheck)

	return &NotificationWaiter{Pending: exists, Wait: wait, Release: release}, nil
}

// ConsumeMessage finds the oldest unconsumed message for (destinationID, topic) and
// atomically marks it consumed. Returns a nil message if none is pending.
func (s *SysDB) ConsumeMessage(ctx context.Context, tx Tx, destinationID, topic string) (*string, *string, error) {
	// Use message_uuid so we update exactly one row; created_at_epoch_ms can match multiple rows when inserts occur in the same millisecond.
	query := s.RenderSQL(`
    WITH oldest_entry AS (
        SELECT message_uuid
        FROM %snotifications
        WHERE destination_uuid = $1 AND topic = $2 AND consumed = false
        ORDER BY created_at_epoch_ms ASC
        LIMIT 1
    )
    UPDATE %snotifications
    SET consumed = true
    WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
    RETURNING message, serialization`, s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))

	var messageString *string
	var msgSerialization *string
	err := tx.QueryRow(ctx, query, destinationID, topic).Scan(&messageString, &msgSerialization)
	if err != nil && err != pgx.ErrNoRows {
		return nil, nil, fmt.Errorf("failed to consume message: %w", err)
	}
	return messageString, msgSerialization, nil
}

type WorkflowSetEventInput struct {
	Key           string
	Message       any
	Tx            Tx
	Serialization string
	WorkflowID    string // Workflow that owns the event (resolved by the caller from context)
	StepID        int    // Step ID for this setEvent (the enclosing transaction step's ID)
}

func (s *SysDB) SetEvent(ctx context.Context, input WorkflowSetEventInput) error {
	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// input.Message is already encoded *string from the typed layer
	// Insert or update the event using UPSERT
	insertQuery := s.RenderSQL(`INSERT INTO %sworkflow_events (workflow_uuid, key, value, serialization)
					VALUES ($1, $2, $3, $4)
					ON CONFLICT (workflow_uuid, key)
					DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization`, s.dialect.SchemaPrefix(s.schema))

	var err error
	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, insertQuery, input.WorkflowID, input.Key, input.Message, input.Serialization)
	} else {
		_, err = s.pool.Exec(ctx, insertQuery, input.WorkflowID, input.Key, input.Message, input.Serialization)
	}
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Record event in workflow_events_history
	insertHistoryQuery := s.RenderSQL(`INSERT INTO %sworkflow_events_history (workflow_uuid, function_id, key, value, serialization)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (workflow_uuid, function_id, key)
					DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization`, s.dialect.SchemaPrefix(s.schema))

	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, insertHistoryQuery, input.WorkflowID, input.StepID, input.Key, input.Message, input.Serialization)
	} else {
		_, err = s.pool.Exec(ctx, insertHistoryQuery, input.WorkflowID, input.StepID, input.Key, input.Message, input.Serialization)
	}
	return err
}

// StartEventListener registers the caller as a waiter for the (targetWorkflowID, key)
// event and checks whether the event is already set. Unlike recv, multiple waiters
// may listen for the same event.
func (s *SysDB) StartEventListener(ctx context.Context, targetWorkflowID, key string) (*NotificationWaiter, error) {
	payload := fmt.Sprintf("%s::%s", targetWorkflowID, key)
	ch := s.EventNotifier.subscribe(payload)
	release := func() { s.EventNotifier.unsubscribe(payload, ch) }

	// recheck reports whether the event is set; it is used both for the initial
	// "already set?" probe and by the wait loop after each wake.
	query := s.RenderSQL(`SELECT EXISTS (SELECT 1 FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2)`, s.dialect.SchemaPrefix(s.schema))
	recheck := func(ctx context.Context) (bool, error) {
		return RetryWithResult(ctx, func() (bool, error) {
			var found bool
			if err := s.pool.QueryRow(ctx, query, targetWorkflowID, key).Scan(&found); err != nil {
				return false, fmt.Errorf("failed to check event: %w", err)
			}
			return found, nil
		}, WithRetrierLogger(s.logger))
	}
	exists, err := recheck(ctx)
	if err != nil {
		release()
		return nil, err
	}
	wait := s.notificationWait(ctx, "GetEvent()", payload, ch, recheck)

	return &NotificationWaiter{Pending: exists, Wait: wait, Release: release}, nil
}

// GetEventValue reads the current value and serialization for (targetWorkflowID, key)
// from the workflow_events table. Returns a nil value if the event is not set.
// A nil Querier defaults to the pool (for callers outside a transaction).
func (s *SysDB) GetEventValue(ctx context.Context, q Querier, targetWorkflowID, key string) (*string, *string, error) {
	if q == nil {
		q = s.pool
	}
	query := s.RenderSQL(`SELECT value, serialization FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2`, s.dialect.SchemaPrefix(s.schema))
	var value *string
	var serialization *string
	err := q.QueryRow(ctx, query, targetWorkflowID, key).Scan(&value, &serialization)
	if err != nil && err != pgx.ErrNoRows {
		return nil, nil, fmt.Errorf("failed to query workflow event: %w", err)
	}
	return value, serialization, nil
}

/*******************************/
/******* STREAMS ********/
/*******************************/

type WriteStreamDBInput struct {
	Key           string
	Value         *string // Already serialized
	Tx            Tx
	Serialization string
	WorkflowID    string // Workflow that owns the stream (resolved by the caller from context)
	StepID        int    // Step ID for this write (the enclosing transaction step's ID)
}

type ReadStreamDBInput struct {
	WorkflowID string
	Key        string
	FromOffset int
}

type StreamEntry struct {
	Key           string
	Value         string
	Offset        int
	Serialization string
}

func (s *SysDB) WriteStream(ctx context.Context, input WriteStreamDBInput) error {
	// When no transaction is provided, run queries on the pool directly (no transaction).
	tx := input.Tx
	queryRow := func(ctx context.Context, sql string, args ...any) Row {
		if tx != nil {
			return tx.QueryRow(ctx, sql, args...)
		}
		return s.pool.QueryRow(ctx, sql, args...)
	}

	exec := func(ctx context.Context, sql string, args ...any) (Result, error) {
		if tx != nil {
			return tx.Exec(ctx, sql, args...)
		}
		return s.pool.Exec(ctx, sql, args...)
	}

	schema := s.dialect.SchemaPrefix(s.schema)

	checkClosedQuery := s.RenderSQL(`SELECT 1 FROM %sstreams
		WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1`,
		schema)

	insertQuery := s.RenderSQL(`INSERT INTO %sstreams (workflow_uuid, key, value, "offset", function_id, serialization)
		SELECT $1, $2, $3, COALESCE(
			(SELECT MAX("offset") FROM %sstreams WHERE workflow_uuid = $1 AND key = $2), -1
		) + 1, $4, $5`,
		schema, schema)

	var err error
	var exists int

	err = queryRow(ctx, checkClosedQuery, input.WorkflowID, input.Key, StreamClosedSentinel).Scan(&exists)
	if err == nil && exists == 1 {
		return fmt.Errorf("stream '%s' is already closed", input.Key)
	} else if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check stream status: %w", err)
	}

	_, err = exec(ctx, insertQuery, input.WorkflowID, input.Key, input.Value, input.StepID, input.Serialization)
	if err != nil {
		return fmt.Errorf("failed to insert stream entry: %w", err)
	}

	return nil
}

// ReadStream reads stream entries starting from a given offset.
// Returns the entries, whether the stream is closed, and any error.
func (s *SysDB) ReadStream(ctx context.Context, input ReadStreamDBInput) ([]StreamEntry, bool, error) {
	query := s.RenderSQL(`SELECT value, "offset", serialization FROM %sstreams
		WHERE workflow_uuid = $1 AND key = $2 AND "offset" >= $3
		ORDER BY "offset" ASC`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, input.WorkflowID, input.Key, input.FromOffset)
	if err != nil {
		return nil, false, fmt.Errorf("failed to query stream: %w", err)
	}
	defer rows.Close()

	var entries []StreamEntry
	closed := false

	for rows.Next() {
		var value string
		var offset int
		var serialization *string
		if err := rows.Scan(&value, &offset, &serialization); err != nil {
			return nil, false, fmt.Errorf("failed to scan stream entry: %w", err)
		}

		if value == StreamClosedSentinel {
			closed = true
			break
		}

		var ser string
		if serialization != nil {
			ser = *serialization
		}
		entries = append(entries, StreamEntry{
			Value:         value,
			Offset:        offset,
			Serialization: ser,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("error iterating stream entries: %w", err)
	}

	return entries, closed, nil
}

// EventRecord is one row from the workflow_events table.
type EventRecord struct {
	Key           string
	Value         string
	Serialization string
}

// GetAllEvents returns every event row currently set on the workflow.
func (s *SysDB) GetAllEvents(ctx context.Context, workflowID string) ([]EventRecord, error) {
	query := s.RenderSQL(`SELECT key, value, serialization FROM %sworkflow_events WHERE workflow_uuid = $1`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow events: %w", err)
	}
	defer rows.Close()

	var events []EventRecord
	for rows.Next() {
		var rec EventRecord
		var serialization *string
		if err := rows.Scan(&rec.Key, &rec.Value, &serialization); err != nil {
			return nil, fmt.Errorf("failed to scan event row: %w", err)
		}
		if serialization != nil {
			rec.Serialization = *serialization
		}
		events = append(events, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating event rows: %w", err)
	}
	return events, nil
}

// NotificationRecord is one row from the notifications table.
// Topic is nil when the row stored the __null__topic__ sentinel.
type NotificationRecord struct {
	Topic            *string
	Message          string
	Serialization    string
	CreatedAtEpochMs int64
	Consumed         bool
}

// GetAllNotifications returns every notification sent to the workflow, ordered by arrival time.
// The __null__topic__ sentinel is normalized back to a nil Topic.
func (s *SysDB) GetAllNotifications(ctx context.Context, workflowID string) ([]NotificationRecord, error) {
	query := s.RenderSQL(`SELECT topic, message, serialization, created_at_epoch_ms, consumed
		FROM %snotifications
		WHERE destination_uuid = $1
		ORDER BY created_at_epoch_ms`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query notifications: %w", err)
	}
	defer rows.Close()

	var results []NotificationRecord
	for rows.Next() {
		var rec NotificationRecord
		var serialization *string
		if err := rows.Scan(&rec.Topic, &rec.Message, &serialization, &rec.CreatedAtEpochMs, &rec.Consumed); err != nil {
			return nil, fmt.Errorf("failed to scan notification row: %w", err)
		}
		if rec.Topic != nil && *rec.Topic == NullTopic {
			rec.Topic = nil
		}
		if serialization != nil {
			rec.Serialization = *serialization
		}
		results = append(results, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating notification rows: %w", err)
	}
	return results, nil
}

// GetAllStreamEntries returns every stream entry for the workflow, ordered by (key, offset).
// Rows holding the stream-closed sentinel are filtered out; callers may group by Key.
func (s *SysDB) GetAllStreamEntries(ctx context.Context, workflowID string) ([]StreamEntry, error) {
	query := s.RenderSQL(`SELECT key, value, "offset", serialization FROM %sstreams
		WHERE workflow_uuid = $1
		ORDER BY key, "offset"`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query streams: %w", err)
	}
	defer rows.Close()

	var records []StreamEntry
	for rows.Next() {
		var rec StreamEntry
		var serialization *string
		if err := rows.Scan(&rec.Key, &rec.Value, &rec.Offset, &serialization); err != nil {
			return nil, fmt.Errorf("failed to scan stream row: %w", err)
		}
		if rec.Value == StreamClosedSentinel {
			continue
		}
		if serialization != nil {
			rec.Serialization = *serialization
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating stream rows: %w", err)
	}
	return records, nil
}

/*******************************/
/******* QUEUES ********/
/*******************************/

type SetWorkflowDelayDBInput struct {
	WorkflowID string
	DelayUntil time.Time
	Tx         Tx
}

// SetWorkflowDelay updates the delay on a DELAYED workflow.
func (s *SysDB) SetWorkflowDelay(ctx context.Context, input SetWorkflowDelayDBInput) error {
	query := s.RenderSQL(`UPDATE %sworkflow_status
		SET delay_until_epoch_ms = $1, updated_at = $2
		WHERE workflow_uuid = $3
		  AND status = $4`, s.dialect.SchemaPrefix(s.schema))

	nowMs := time.Now().UnixMilli()
	delayMs := input.DelayUntil.UnixMilli()

	if input.Tx != nil {
		_, err := input.Tx.Exec(ctx, query, delayMs, nowMs, input.WorkflowID, models.WorkflowStatusDelayed)
		if err != nil {
			return fmt.Errorf("failed to set workflow delay: %w", err)
		}
	} else {
		_, err := s.pool.Exec(ctx, query, delayMs, nowMs, input.WorkflowID, models.WorkflowStatusDelayed)
		if err != nil {
			return fmt.Errorf("failed to set workflow delay: %w", err)
		}
	}
	return nil
}

// TransitionDelayedWorkflows transitions DELAYED workflows whose delay has expired to ENQUEUED.
// For debounced workflows, the deduplication ID is cleared in the same atomic update: it is a
// debounce key held only while DELAYED, so a later same-key debounce starts a fresh workflow.
func (s *SysDB) TransitionDelayedWorkflows(ctx context.Context) error {
	nowMs := time.Now().UnixMilli()
	query := s.RenderSQL(`UPDATE %sworkflow_status
		SET status = $1, updated_at = $2,
		    deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END
		WHERE status = $3
		  AND delay_until_epoch_ms <= $2`, s.dialect.SchemaPrefix(s.schema))

	_, err := s.pool.Exec(ctx, query, models.WorkflowStatusEnqueued, nowMs, models.WorkflowStatusDelayed)
	if err != nil {
		return fmt.Errorf("failed to transition delayed workflows: %w", err)
	}
	return nil
}

// DebounceDelayedWorkflowDBInput identifies a debounced workflow by
// (name, queue, deduplication ID) and carries the new delay and inputs.
type DebounceDelayedWorkflowDBInput struct {
	WorkflowName    string
	QueueName       string
	DeduplicationID string
	DelayUntil      time.Time
	Input           *string // encoded
	Serialization   string
	Tx              Tx
}

// DebounceResult reports the outcome of a bounce attempt.
type DebounceResult struct {
	BouncedWorkflowID  *string // The extended workflow's ID if an existing debounced DELAYED workflow was bounced; nil if no bounce occurred
	HolderWorkflowID   *string // The workflow currently holding the deduplication ID, if any, when no bounce occurred
	HolderIsDebounced  bool    // Whether the holder is itself a debounced workflow
	HolderWorkflowName string  // The holder's workflow name; a mismatch with the caller's means a debounce-key collision between workflows
}

// DebounceDelayedWorkflow extends an existing debounced DELAYED workflow's delay and
// updates its inputs, atomically. The new delay is capped at the workflow's
// debounce_deadline_epoch_ms, if one is set. Matching on workflow name ensures a
// debounce-key collision between different workflows (e.g. "a"+"b-c" vs "a-b"+"c")
// never overwrites another workflow's inputs. If nothing matched, returns the current
// holder (or that the key is unheld) so the caller can start fresh or surface a conflict.
// Runs on input.Tx if given, joining its transaction (e.g. a checkpointed step's);
// otherwise in its own retried transaction.
func (s *SysDB) DebounceDelayedWorkflow(ctx context.Context, input DebounceDelayedWorkflowDBInput) (*DebounceResult, error) {
	if input.Tx != nil {
		return s.debounceDelayedWorkflowInternal(ctx, input.Tx, input)
	}
	return RetryWithResult(ctx, func() (*DebounceResult, error) {
		tx, err := s.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to begin debounce transaction: %w", err)
		}
		defer tx.Rollback(ctx)
		result, err := s.debounceDelayedWorkflowInternal(ctx, tx, input)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit debounce transaction: %w", err)
		}
		return result, nil
	}, WithRetrierLogger(s.logger), WithRetryCondition(s.dialect.IsRetryableTransaction))
}

func (s *SysDB) debounceDelayedWorkflowInternal(ctx context.Context, tx Tx, input DebounceDelayedWorkflowDBInput) (*DebounceResult, error) {
	// Cap the new delay at the debounce deadline, if any (CASE not LEAST, for Postgres/SQLite portability).
	updateQuery := s.RenderSQL(`UPDATE %sworkflow_status
		SET delay_until_epoch_ms = CASE
		      WHEN debounce_deadline_epoch_ms IS NOT NULL AND debounce_deadline_epoch_ms < $1
		      THEN debounce_deadline_epoch_ms
		      ELSE $1
		    END,
		    inputs = $2, serialization = $3, updated_at = $4
		WHERE name = $5 AND queue_name = $6 AND deduplication_id = $7
		  AND status = $8 AND is_debounced = TRUE
		RETURNING workflow_uuid`, s.dialect.SchemaPrefix(s.schema))

	var bouncedWorkflowID string
	err := tx.QueryRow(ctx, updateQuery,
		input.DelayUntil.UnixMilli(),
		input.Input,
		input.Serialization,
		time.Now().UnixMilli(),
		input.WorkflowName,
		input.QueueName,
		input.DeduplicationID,
		models.WorkflowStatusDelayed,
	).Scan(&bouncedWorkflowID)
	if err == nil {
		// We updated a debounced workflow
		return &DebounceResult{BouncedWorkflowID: &bouncedWorkflowID}, nil
	}
	if !errors.Is(err, ErrNoRows) {
		return nil, fmt.Errorf("failed to bounce delayed workflow: %w", err)
	}

	// We didn't update any row. We want to distinguish which situation led to this:
	// 1. A workflow exists but is_debounced is false: the user is using the same dedup key than the debouncer on the queue. We report the conflict.
	// 2. A workflow exists, is_debounced is true: this can be a deduplication key collision, maybe due a name + key collision or some other rare situation.
	holderQuery := s.RenderSQL(`SELECT workflow_uuid, is_debounced, name
		FROM %sworkflow_status
		WHERE queue_name = $1 AND deduplication_id = $2`, s.dialect.SchemaPrefix(s.schema))

	var holderWorkflowID, holderWorkflowName string
	var holderIsDebounced bool
	err = tx.QueryRow(ctx, holderQuery, input.QueueName, input.DeduplicationID).Scan(&holderWorkflowID, &holderIsDebounced, &holderWorkflowName)
	// No result means we should create a new debounced workflow.
	if errors.Is(err, ErrNoRows) {
		return &DebounceResult{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query deduplication ID holder: %w", err)
	}
	return &DebounceResult{
		HolderWorkflowID:   &holderWorkflowID,
		HolderIsDebounced:  holderIsDebounced,
		HolderWorkflowName: holderWorkflowName,
	}, nil
}

type DequeuedWorkflow struct {
	Id            string
	Name          string
	Input         *string
	Serialization string
	ConfigName    *string
}

type DequeueWorkflowsInput struct {
	Queue              models.QueueConfig
	ExecutorID         string
	ApplicationVersion string
	QueuePartitionKey  string
	LocalRunningCount  int
}

func (s *SysDB) DequeueWorkflows(ctx context.Context, input DequeueWorkflowsInput) ([]DequeuedWorkflow, error) {
	// Snapshot isolation is only required for global concurrency or rate limiting.
	// Otherwise read committed suffices: worker concurrency is enforced in-memory.
	snapshot := input.Queue.GlobalConcurrency != nil || input.Queue.RateLimit != nil
	tx, err := s.pool.BeginTx(ctx, TxOptions{IsoLevel: s.dialect.QueueDequeueIsolation(snapshot)})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	schemaPrefix := s.dialect.SchemaPrefix(s.schema)

	// Rate limiter: count workflows started within the limiter period.
	var numRecentQueries int
	if input.Queue.RateLimit != nil {
		cutoffTimeMs := time.Now().Add(-input.Queue.RateLimit.Period).UnixMilli()

		limiterQuery := s.RenderSQL(`
		SELECT COUNT(*)
		FROM %sworkflow_status
		WHERE queue_name = $1
		  AND rate_limited = TRUE
		  AND status NOT IN ($2, $3)
		  AND started_at_epoch_ms > $4`, schemaPrefix)

		limiterArgs := []any{input.Queue.Name, models.WorkflowStatusEnqueued, models.WorkflowStatusDelayed, cutoffTimeMs}
		if len(input.QueuePartitionKey) > 0 {
			limiterQuery += ` AND queue_partition_key = $5`
			limiterArgs = append(limiterArgs, input.QueuePartitionKey)
		}

		err := tx.QueryRow(ctx, s.dialect.RewriteQuery(limiterQuery), limiterArgs...).Scan(&numRecentQueries)
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}

		if numRecentQueries >= input.Queue.RateLimit.Limit {
			return []DequeuedWorkflow{}, nil
		}
	}

	// Calculate max_tasks based on concurrency limits
	// maxTasks < 0 means this dequeue is unbounded.
	maxTasks := -1
	if input.Queue.WorkerConcurrency != nil {
		workerConcurrency := *input.Queue.WorkerConcurrency
		if input.LocalRunningCount > workerConcurrency {
			s.logger.Warn("Local running workflows on queue exceeds worker concurrency limit", "local_running", input.LocalRunningCount, "queue_name", input.Queue.Name, "concurrency_limit", workerConcurrency)
		}
		if available := max(workerConcurrency-input.LocalRunningCount, 0); maxTasks < 0 || available < maxTasks {
			maxTasks = available
		}
	}

	if input.Queue.GlobalConcurrency != nil {
		pendingQuery := s.RenderSQL(`
			SELECT COUNT(*)
			FROM %sworkflow_status
			WHERE queue_name = $1 AND status = $2`, schemaPrefix)

		pendingArgs := []any{input.Queue.Name, models.WorkflowStatusPending}
		if len(input.QueuePartitionKey) > 0 {
			pendingQuery += ` AND queue_partition_key = $3`
			pendingArgs = append(pendingArgs, input.QueuePartitionKey)
		}

		var globalCount int
		if err := tx.QueryRow(ctx, s.dialect.RewriteQuery(pendingQuery), pendingArgs...).Scan(&globalCount); err != nil {
			return nil, fmt.Errorf("failed to query pending workflows: %w", err)
		}

		concurrency := *input.Queue.GlobalConcurrency
		if globalCount > concurrency {
			s.logger.Warn("Total pending workflows on queue exceeds global concurrency limit", "total_pending", globalCount, "queue_name", input.Queue.Name, "concurrency_limit", concurrency)
		}
		if availableTasks := max(concurrency-globalCount, 0); maxTasks < 0 || availableTasks < maxTasks {
			maxTasks = availableTasks
		}
	}

	if maxTasks == 0 {
		return nil, nil
	}

	// Build the SELECT for candidate workflow IDs. Always order by
	// (priority, created_at) so the planner can satisfy the dequeue scan from
	// idx_workflow_status_in_flight (queue_name, status, priority, created_at).
	isLatestVersion := true
	switch latest, err := s.GetLatestApplicationVersion(ctx, tx); {
	case err == nil:
		isLatestVersion = latest.Name == input.ApplicationVersion
	case errors.Is(err, &models.Error{Code: models.ErrorCodeNoApplicationVersions}):
		// No versions registered yet: treat this worker as the latest.
	default:
		return nil, fmt.Errorf("failed to query latest application version: %w", err)
	}

	versionClause := `application_version = $3`
	if isLatestVersion {
		versionClause = `(application_version = $3 OR application_version IS NULL)`
	}

	queryArgs := []any{input.Queue.Name, models.WorkflowStatusEnqueued, input.ApplicationVersion}
	query := s.RenderSQL(`
			SELECT workflow_uuid
			FROM %sworkflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND `+versionClause, schemaPrefix)

	if len(input.QueuePartitionKey) > 0 {
		query += ` AND queue_partition_key = $4`
		queryArgs = append(queryArgs, input.QueuePartitionKey)
	}

	query += ` ORDER BY priority ASC, created_at ASC`

	// Use SKIP LOCKED when no global concurrency is set to avoid blocking,
	// otherwise use NOWAIT to ensure consistent view across processes
	if input.Queue.GlobalConcurrency == nil {
		if lock := s.dialect.LockSkipLocked(); lock != "" {
			query += " " + lock
		}
	} else {
		if lock := s.dialect.LockNoWait(); lock != "" {
			query += " " + lock
		}
	}

	if maxTasks >= 0 {
		query += fmt.Sprintf(" LIMIT %d", int(maxTasks))
	}

	rows, err := tx.Query(ctx, s.dialect.RewriteQuery(query), queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}
	defer rows.Close()

	var dequeuedIDs []string
	for rows.Next() {
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
		s.logger.Debug("attempting to dequeue task(s)", "queue_name", input.Queue.Name, "num_tasks", len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	updateQuery := s.RenderSQL(`
		UPDATE %sworkflow_status
		SET status = $1,
		    application_version = $2,
		    executor_id = $3,
		    started_at_epoch_ms = $4,
		    rate_limited = $5,
		    workflow_deadline_epoch_ms = CASE
		        WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
		        THEN $4 + workflow_timeout_ms
		        ELSE workflow_deadline_epoch_ms
		    END
		WHERE workflow_uuid = $6 AND status = $7
		RETURNING name, inputs, serialization, config_name`, schemaPrefix)

	var retWorkflows []DequeuedWorkflow
	for _, id := range dequeuedIDs {
		if input.Queue.RateLimit != nil {
			if len(retWorkflows)+numRecentQueries >= input.Queue.RateLimit.Limit {
				break
			}
		}
		retWorkflow := DequeuedWorkflow{Id: id}

		var serialization *string
		err := tx.QueryRow(ctx, updateQuery,
			models.WorkflowStatusPending,
			input.ApplicationVersion,
			input.ExecutorID,
			time.Now().UnixMilli(),
			input.Queue.RateLimit != nil,
			id,
			models.WorkflowStatusEnqueued).Scan(&retWorkflow.Name, &retWorkflow.Input, &serialization, &retWorkflow.ConfigName)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			return nil, fmt.Errorf("failed to update workflow %s during dequeue: %w", id, err)
		}
		if serialization != nil {
			retWorkflow.Serialization = *serialization
		}

		retWorkflows = append(retWorkflows, retWorkflow)
	}

	// Commit only if workflows were dequeued. Avoids WAL bloat / XID advance.
	if len(retWorkflows) > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return retWorkflows, nil
}

func (s *SysDB) ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	query := s.RenderSQL(`UPDATE %sworkflow_status
			  SET status = $1, started_at_epoch_ms = NULL
			  WHERE workflow_uuid = $2
			    AND queue_name IS NOT NULL
			    AND status = $3`, s.dialect.SchemaPrefix(s.schema))

	commandTag, err := s.pool.Exec(ctx, query,
		models.WorkflowStatusEnqueued,
		workflowID,
		models.WorkflowStatusPending)

	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment for workflow %s: %w", workflowID, err)
	}

	// If no rows were affected, the workflow is not anymore in the queue or was already completed
	n, err := commandTag.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to read rows affected after clearing queue assignment for workflow %s: %w", workflowID, err)
	}
	return n > 0, nil
}

// GetQueuePartitions returns all unique partition keys for enqueued workflows in a queue.
func (s *SysDB) GetQueuePartitions(ctx context.Context, queueName string) ([]string, error) {
	query := s.RenderSQL(`
		SELECT DISTINCT queue_partition_key
		FROM %sworkflow_status
		WHERE queue_name = $1
		  AND status = $2
		  AND queue_partition_key IS NOT NULL`, s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, queueName, models.WorkflowStatusEnqueued)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read queue partitions: %w", err)
	}

	return partitions, nil
}

/*******************************/
/******* QUEUE REGISTRY ********/
/*******************************/

const _QUEUE_SELECT_COLUMNS = "name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec, priority_enabled, partition_queue, polling_interval_sec"

type UpsertQueueDBInput struct {
	Queue          models.QueueConfig
	UpdateExisting bool
}

// scanQueueRow builds a database-backed models.QueueConfig from a row selecting
// _QUEUE_SELECT_COLUMNS, in order.
func scanQueueRow(row Row) (*models.QueueConfig, error) {
	var (
		name                            string
		concurrency, workerConcurrency  *int
		rateLimitMax                    *int
		rateLimitPeriodSec              *float64
		priorityEnabled, partitionQueue bool
		pollingIntervalSec              float64
	)
	if err := row.Scan(&name, &concurrency, &workerConcurrency, &rateLimitMax, &rateLimitPeriodSec, &priorityEnabled, &partitionQueue, &pollingIntervalSec); err != nil {
		return nil, err
	}
	q := &models.QueueConfig{
		Name:              name,
		GlobalConcurrency: concurrency,
		WorkerConcurrency: workerConcurrency,
		PriorityEnabled:   priorityEnabled,
		PartitionQueue:    partitionQueue,
		DatabaseBacked:    true,
	}
	if rateLimitMax != nil {
		var period time.Duration
		if rateLimitPeriodSec != nil {
			period = time.Duration(*rateLimitPeriodSec * float64(time.Second))
		}
		q.RateLimit = &models.RateLimiter{Limit: *rateLimitMax, Period: period}
	}
	base := time.Duration(pollingIntervalSec * float64(time.Second))
	if base <= 0 {
		base = models.DefaultBasePollingInterval
	}
	q.BasePollingInterval = base
	return q, nil
}

// GetQueue returns the database-backed queue with the given name, or nil (with a
// nil error) when no such queue exists.
func (s *SysDB) GetQueue(ctx context.Context, name string) (*models.QueueConfig, error) {
	return s.getQueueRow(ctx, s.pool, name)
}

func (s *SysDB) getQueueRow(ctx context.Context, db Querier, name string) (*models.QueueConfig, error) {
	query := s.RenderSQL(`SELECT `+_QUEUE_SELECT_COLUMNS+` FROM %squeues WHERE name = $1`, s.dialect.SchemaPrefix(s.schema))
	q, err := scanQueueRow(db.QueryRow(ctx, s.dialect.RewriteQuery(query), name))
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get queue %s: %w", name, err)
	}
	return q, nil
}

// ListQueues returns all database-backed queues registered in the queues table.
func (s *SysDB) ListQueues(ctx context.Context) ([]models.QueueConfig, error) {
	query := s.RenderSQL(`SELECT `+_QUEUE_SELECT_COLUMNS+` FROM %squeues`, s.dialect.SchemaPrefix(s.schema))
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []models.QueueConfig
	for rows.Next() {
		q, err := scanQueueRow(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan queue: %w", err)
		}
		queues = append(queues, *q)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read queues: %w", err)
	}
	return queues, nil
}

// DeleteQueue removes a database-backed queue's row, if it exists. Workflows
// still enqueued on it become unrecoverable.
func (s *SysDB) DeleteQueue(ctx context.Context, name string) error {
	query := s.RenderSQL(`DELETE FROM %squeues WHERE name = $1`, s.dialect.SchemaPrefix(s.schema))
	if _, err := s.pool.Exec(ctx, s.dialect.RewriteQuery(query), name); err != nil {
		return fmt.Errorf("failed to delete queue %s: %w", name, err)
	}
	return nil
}

// UpsertQueue inserts a queue row or, when updateExisting is set, overwrites the
// existing configuration. It returns true iff a new row was inserted.
func (s *SysDB) UpsertQueue(ctx context.Context, input UpsertQueueDBInput) (bool, error) {
	q := input.Queue
	var rateLimitMax *int
	var rateLimitPeriodSec *float64
	if q.RateLimit != nil {
		rateLimitMax = &q.RateLimit.Limit
		sec := q.RateLimit.Period.Seconds()
		rateLimitPeriodSec = &sec
	}
	pollingSec := q.BasePollingInterval.Seconds()
	if pollingSec <= 0 {
		pollingSec = models.DefaultBasePollingInterval.Seconds()
	}
	nowMs := time.Now().UnixMilli()
	schemaPrefix := s.dialect.SchemaPrefix(s.schema)

	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Supply queue_id and created_at explicitly: the SQLite schema has no
	// defaults for them (only the Postgres schema does).
	insertQuery := s.RenderSQL(`INSERT INTO %squeues
		(queue_id, name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec, priority_enabled, partition_queue, polling_interval_sec, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (name) DO NOTHING`, schemaPrefix)
	res, err := tx.Exec(ctx, s.dialect.RewriteQuery(insertQuery),
		uuid.New().String(), q.Name, q.GlobalConcurrency, q.WorkerConcurrency, rateLimitMax, rateLimitPeriodSec, q.PriorityEnabled, q.PartitionQueue, pollingSec, nowMs, nowMs)
	if err != nil {
		return false, fmt.Errorf("failed to insert queue %s: %w", q.Name, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to read rows affected for queue %s: %w", q.Name, err)
	}
	inserted := affected > 0

	if !inserted && input.UpdateExisting {
		if err := s.updateQueueRow(ctx, tx, q); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return inserted, nil
}

func (s *SysDB) updateQueueQuery(schemaPrefix string) string {
	return s.RenderSQL(`UPDATE %squeues SET
		concurrency = $2, worker_concurrency = $3, rate_limit_max = $4, rate_limit_period_sec = $5,
		priority_enabled = $6, partition_queue = $7, polling_interval_sec = $8, updated_at = $9
		WHERE name = $1`, schemaPrefix)
}

// updateQueueRow overwrites the configuration columns of an existing queue row
// using the given Querier (a pool or a transaction). It returns an error if no
// row with the queue's name exists.
func (s *SysDB) updateQueueRow(ctx context.Context, db Querier, q models.QueueConfig) error {
	var rateLimitMax *int
	var rateLimitPeriodSec *float64
	if q.RateLimit != nil {
		rateLimitMax = &q.RateLimit.Limit
		sec := q.RateLimit.Period.Seconds()
		rateLimitPeriodSec = &sec
	}
	pollingSec := q.BasePollingInterval.Seconds()
	if pollingSec <= 0 {
		pollingSec = models.DefaultBasePollingInterval.Seconds()
	}
	nowMs := time.Now().UnixMilli()
	schemaPrefix := s.dialect.SchemaPrefix(s.schema)

	res, err := db.Exec(ctx, s.dialect.RewriteQuery(s.updateQueueQuery(schemaPrefix)),
		q.Name, q.GlobalConcurrency, q.WorkerConcurrency, rateLimitMax, rateLimitPeriodSec, q.PriorityEnabled, q.PartitionQueue, pollingSec, nowMs)
	if err != nil {
		return fmt.Errorf("failed to update queue %s: %w", q.Name, err)
	}
	if affected, err := res.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("queue %s does not exist", q.Name)
	}
	return nil
}

// UpdateQueueConfig applies a single configuration change to a database-backed
// queue within one transaction: it reads the current row, passes it to mutate
// (which applies and validates the change against the freshly-read values),
// persists the row, and returns the updated queue. Run with snapshot isolation.
func (s *SysDB) UpdateQueueConfig(ctx context.Context, name string, mutate func(*models.QueueConfig) error) (*models.QueueConfig, error) {
	tx, err := s.pool.BeginTx(ctx, TxOptions{IsoLevel: s.dialect.SnapshotIsolation()})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	q, err := s.getQueueRow(ctx, tx, name)
	if err != nil {
		return nil, err
	}
	if q == nil {
		return nil, fmt.Errorf("queue %s no longer exists", name)
	}
	if err := mutate(q); err != nil {
		return nil, err
	}
	if err := s.updateQueueRow(ctx, tx, *q); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return q, nil
}

/*******************************/
/******* METRICS ********/
/*******************************/

type MetricData struct {
	MetricName string  `json:"metric_name"` // step name or workflow name
	MetricType string  `json:"metric_type"` // workflow_count, step_count, etc
	Value      float64 `json:"value"`
}

func (s *SysDB) GetMetrics(ctx context.Context, startTime, endTime string) ([]MetricData, error) {
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

	var metrics []MetricData

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

func (s *SysDB) getMetricWorkflowCount(ctx context.Context, startEpochMs, endEpochMs int64) ([]MetricData, error) {
	workflowQuery := s.RenderSQL(`
		SELECT name, COUNT(workflow_uuid) as count
		FROM %sworkflow_status
		WHERE created_at >= $1 AND created_at < $2
		GROUP BY name
	`, s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, workflowQuery, startEpochMs, endEpochMs)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow metrics: %w", err)
	}
	defer rows.Close()

	var metrics []MetricData
	for rows.Next() {
		var workflowName string
		var workflowCount int64
		if err := rows.Scan(&workflowName, &workflowCount); err != nil {
			return nil, fmt.Errorf("failed to scan workflow metric: %w", err)
		}
		metrics = append(metrics, MetricData{
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

func (s *SysDB) getMetricStepCount(ctx context.Context, startEpochMs, endEpochMs int64) ([]MetricData, error) {
	stepQuery := s.RenderSQL(`
		SELECT function_name, COUNT(*) as count
		FROM %soperation_outputs
		WHERE completed_at_epoch_ms >= $1 AND completed_at_epoch_ms < $2
		GROUP BY function_name
	`, s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, stepQuery, startEpochMs, endEpochMs)
	if err != nil {
		return nil, fmt.Errorf("failed to query step metrics: %w", err)
	}
	defer rows.Close()

	var metrics []MetricData
	for rows.Next() {
		var stepName string
		var stepCount int64
		if err := rows.Scan(&stepName, &stepCount); err != nil {
			return nil, fmt.Errorf("failed to scan step metric: %w", err)
		}
		metrics = append(metrics, MetricData{
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
/******* SCHEDULES ********/
/*******************************/

type UpsertScheduleDBInput struct {
	ScheduleID        string
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           string // JSON serialized
	Status            models.ScheduleStatus
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
	Tx                Tx // optional: run inside an existing transaction
}

func (s *SysDB) UpsertSchedule(ctx context.Context, input UpsertScheduleDBInput) error {
	query := s.RenderSQL(`
		INSERT INTO %sworkflow_schedules (
			schedule_id, schedule_name, workflow_name, workflow_class_name,
			schedule, context, status, automatic_backfill, cron_timezone, queue_name
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (schedule_name) DO UPDATE SET
			workflow_name = EXCLUDED.workflow_name,
			workflow_class_name = EXCLUDED.workflow_class_name,
			schedule = EXCLUDED.schedule,
			context = EXCLUDED.context,
			cron_timezone = EXCLUDED.cron_timezone,
			queue_name = EXCLUDED.queue_name,
			automatic_backfill = EXCLUDED.automatic_backfill
	`, s.dialect.SchemaPrefix(s.schema))

	var queueNameVal any
	if input.QueueName != "" {
		queueNameVal = input.QueueName
	}

	var workflowClassNameVal any
	if input.WorkflowClassName != "" {
		workflowClassNameVal = input.WorkflowClassName
	}

	args := []any{
		input.ScheduleID,
		input.ScheduleName,
		input.WorkflowName,
		workflowClassNameVal,
		input.Schedule,
		input.Context,
		input.Status,
		input.AutomaticBackfill,
		input.CronTimezone,
		queueNameVal,
	}

	var err error
	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, query, args...)
	} else {
		_, err = s.pool.Exec(ctx, query, args...)
	}
	if err != nil {
		return fmt.Errorf("failed to upsert schedule: %w", err)
	}
	return nil
}

type CreateScheduleDBInput struct {
	ScheduleID        string
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           string // JSON serialized
	Status            models.ScheduleStatus
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
	Tx                Tx // optional: run inside an existing transaction
}

func (s *SysDB) CreateSchedule(ctx context.Context, input CreateScheduleDBInput) error {
	query := s.RenderSQL(`
		INSERT INTO %sworkflow_schedules (
			schedule_id, schedule_name, workflow_name, workflow_class_name,
			schedule, context, status, automatic_backfill, cron_timezone, queue_name
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, s.dialect.SchemaPrefix(s.schema))

	var queueNameVal any
	if input.QueueName != "" {
		queueNameVal = input.QueueName
	}

	var workflowClassNameVal any
	if input.WorkflowClassName != "" {
		workflowClassNameVal = input.WorkflowClassName
	}

	args := []any{
		input.ScheduleID,
		input.ScheduleName,
		input.WorkflowName,
		workflowClassNameVal,
		input.Schedule,
		input.Context,
		input.Status,
		input.AutomaticBackfill,
		input.CronTimezone,
		queueNameVal,
	}

	var err error
	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, query, args...)
	} else {
		_, err = s.pool.Exec(ctx, query, args...)
	}
	if err != nil {
		return fmt.Errorf("failed to create schedule: %w", err)
	}
	return nil
}

type ListSchedulesDBInput struct {
	Statuses             []models.ScheduleStatus
	WorkflowNames        []string
	ScheduleNamePrefixes []string
	Tx                   Tx // optional: run inside an existing transaction
}

func (s *SysDB) ListSchedules(ctx context.Context, input ListSchedulesDBInput) ([]models.WorkflowSchedule, error) {
	query := s.RenderSQL(`
		SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
		       schedule, status, context, last_fired_at, automatic_backfill,
		       cron_timezone, queue_name
		FROM %sworkflow_schedules
	`, s.dialect.SchemaPrefix(s.schema))

	var args []any
	var conds []string

	if len(input.Statuses) > 0 {
		statuses := make([]string, len(input.Statuses))
		for i, st := range input.Statuses {
			statuses[i] = string(st)
		}
		encoded, err := encodeArrayParam(s.dialect, statuses)
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
		args = append(args, encoded)
		conds = append(conds, dialectAnyClause(s.dialect, "status", len(args)))
	}
	if len(input.WorkflowNames) > 0 {
		encoded, err := encodeArrayParam(s.dialect, input.WorkflowNames)
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
		args = append(args, encoded)
		conds = append(conds, dialectAnyClause(s.dialect, "workflow_name", len(args)))
	}
	if len(input.ScheduleNamePrefixes) > 0 {
		patterns := make([]string, len(input.ScheduleNamePrefixes))
		for i, p := range input.ScheduleNamePrefixes {
			patterns[i] = p + "%"
		}
		encoded, err := encodeArrayParam(s.dialect, patterns)
		if err != nil {
			return nil, fmt.Errorf("list schedules: %w", err)
		}
		args = append(args, encoded)
		conds = append(conds, dialectLikeAnyClause(s.dialect, "schedule_name", len(args)))
	}
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}

	var rows Rows
	var err error
	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, args...)
	} else {
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}
	defer rows.Close()

	var schedules []models.WorkflowSchedule
	for rows.Next() {
		var schedule models.WorkflowSchedule
		var lastFiredAtStr *string
		var contextJSON string

		var queueName *string
		var workflowClassName *string
		err := rows.Scan(
			&schedule.ScheduleID,
			&schedule.ScheduleName,
			&schedule.WorkflowName,
			&workflowClassName,
			&schedule.Schedule,
			&schedule.Status,
			&contextJSON,
			&lastFiredAtStr,
			&schedule.AutomaticBackfill,
			&schedule.CronTimezone,
			&queueName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schedule: %w", err)
		}
		if queueName != nil {
			schedule.QueueName = *queueName
		} else {
			schedule.QueueName = models.InternalQueueName
		}
		if workflowClassName != nil {
			schedule.WorkflowClassName = *workflowClassName
		}

		if lastFiredAtStr != nil {
			t, err := time.Parse(time.RFC3339Nano, *lastFiredAtStr)
			if err != nil {
				t, err = time.Parse(time.RFC3339, *lastFiredAtStr)
			}
			if err == nil {
				schedule.LastFiredAt = &t
			} else {
				// A nil LastFiredAt disables automatic backfill for this schedule
				s.logger.Warn("failed to parse schedule last_fired_at; automatic backfill will not run for this schedule",
					"schedule_name", schedule.ScheduleName, "last_fired_at", *lastFiredAtStr, "error", err)
			}
		}
		if raw := strings.TrimSpace(contextJSON); raw != "" && raw != "null" {
			schedule.Context = json.RawMessage(contextJSON)
		}

		schedules = append(schedules, schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}

	return schedules, nil
}

type UpdateScheduleDBInput struct {
	ScheduleName string
	Status       models.ScheduleStatus
	LastFiredAt  *time.Time
	Tx           Tx // optional: run inside an existing transaction
}

func (s *SysDB) UpdateSchedule(ctx context.Context, input UpdateScheduleDBInput) error {
	query := s.RenderSQL(`
		UPDATE %sworkflow_schedules
		SET status = $1, last_fired_at = $2
		WHERE schedule_name = $3
	`, s.dialect.SchemaPrefix(s.schema))

	var lastFiredAtVal any
	if input.LastFiredAt != nil {
		lastFiredAtVal = input.LastFiredAt.Format(time.RFC3339Nano)
	}

	var err error
	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, query, input.Status, lastFiredAtVal, input.ScheduleName)
	} else {
		_, err = s.pool.Exec(ctx, query, input.Status, lastFiredAtVal, input.ScheduleName)
	}
	if err != nil {
		return fmt.Errorf("failed to update schedule: %w", err)
	}
	return nil
}

func (s *SysDB) UpdateScheduleLastFiredAt(ctx context.Context, scheduleName string, lastFiredAt time.Time) error {
	query := s.RenderSQL(`
		UPDATE %sworkflow_schedules
		SET last_fired_at = $1
		WHERE schedule_name = $2
	`, s.dialect.SchemaPrefix(s.schema))
	_, err := s.pool.Exec(ctx, query, lastFiredAt.Format(time.RFC3339Nano), scheduleName)
	if err != nil {
		return fmt.Errorf("failed to update schedule last_fired_at: %w", err)
	}
	return nil
}

type DeleteScheduleDBInput struct {
	ScheduleName string
	Tx           Tx // optional: run inside an existing transaction
}

func (s *SysDB) DeleteSchedule(ctx context.Context, input DeleteScheduleDBInput) error {
	query := s.RenderSQL(`DELETE FROM %sworkflow_schedules WHERE schedule_name = $1`, s.dialect.SchemaPrefix(s.schema))

	var err error
	if input.Tx != nil {
		_, err = input.Tx.Exec(ctx, query, input.ScheduleName)
	} else {
		_, err = s.pool.Exec(ctx, query, input.ScheduleName)
	}
	if err != nil {
		return fmt.Errorf("failed to delete schedule: %w", err)
	}
	return nil
}

type BackfillScheduleDBInput struct {
	ScheduleName string
	Schedule     string
	StartTime    time.Time
	EndTime      time.Time
}

func (s *SysDB) BackfillSchedule(ctx context.Context, input BackfillScheduleDBInput) ([]string, error) {
	if s.encodeScheduledInput == nil {
		return nil, errors.New("scheduled input encoder is not configured")
	}
	schedules, err := s.ListSchedules(ctx, ListSchedulesDBInput{ScheduleNamePrefixes: []string{input.ScheduleName}})
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}
	var schedule *models.WorkflowSchedule
	for i := range schedules {
		if schedules[i].ScheduleName == input.ScheduleName {
			schedule = &schedules[i]
			break
		}
	}
	if schedule == nil {
		return nil, fmt.Errorf("schedule not found: %s", input.ScheduleName)
	}

	spec := input.Schedule
	if schedule.CronTimezone != "" {
		spec = "CRON_TZ=" + schedule.CronTimezone + " " + spec
	}

	scheduleEntry, err := models.NewScheduleCronParser().Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cron schedule: %w", err)
	}

	queueName := models.InternalQueueName
	if schedule.QueueName != "" {
		queueName = schedule.QueueName
	}

	// Backfilled workflows always run against the latest registered application
	// version. If lookup fails (e.g. no versions registered yet) leave it unset.
	var backfillAppVersion string
	backfillLatest, err := RetryWithResult(ctx, func() (*VersionInfo, error) {
		return s.GetLatestApplicationVersion(ctx, nil)
	}, WithRetrierLogger(s.logger))
	if err != nil {
		s.logger.Error("failed to fetch latest application version for schedule backfill", "schedule", input.ScheduleName, "error", err)
	} else if backfillLatest != nil {
		backfillAppVersion = backfillLatest.Name
	}

	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	checkQuery := s.RenderSQL(`SELECT 1 FROM %sworkflow_status WHERE workflow_uuid = $1 LIMIT 1`, s.dialect.SchemaPrefix(s.schema))

	nextTime := scheduleEntry.Next(input.StartTime)
	now := time.Now()
	var workflowIDs []string

	for nextTime.Before(input.EndTime) {
		workflowID := fmt.Sprintf("sched-%s-%s", input.ScheduleName, nextTime.Format(time.RFC3339))
		workflowIDs = append(workflowIDs, workflowID)

		var dummy int
		err := tx.QueryRow(ctx, checkQuery, workflowID).Scan(&dummy)
		if err == nil {
			nextTime = scheduleEntry.Next(nextTime)
			continue
		}
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("failed to check workflow existence for %s: %w", workflowID, err)
		}

		encodedInput, serName, encErr := s.encodeScheduledInput(ctx, nextTime, schedule.Context)
		if encErr != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input for %s: %w", workflowID, encErr)
		}

		status := models.WorkflowStatus{
			ID:                 workflowID,
			Status:             models.WorkflowStatusEnqueued,
			Name:               schedule.WorkflowName,
			ClassName:          schedule.WorkflowClassName,
			QueueName:          queueName,
			CreatedAt:          now,
			Input:              encodedInput,
			Serialization:      serName,
			ApplicationVersion: backfillAppVersion,
			ScheduleName:       input.ScheduleName,
		}
		if _, err := s.InsertWorkflowStatus(ctx, InsertWorkflowStatusDBInput{Status: status, Tx: tx}); err != nil {
			return nil, fmt.Errorf("failed to enqueue backfill workflow %s: %w", workflowID, err)
		}

		nextTime = scheduleEntry.Next(nextTime)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit backfill transaction: %w", err)
	}
	return workflowIDs, nil
}

// TriggerSchedule immediately enqueues the named schedule's workflow at the
// current time, using the schedule's queue (or the internal queue by default)
// and preserving its workflow_class_name and context. Returns the workflow ID.
func (s *SysDB) TriggerSchedule(ctx context.Context, scheduleName string) (string, error) {
	if scheduleName == "" {
		return "", errors.New("schedule_name is required")
	}
	if s.encodeScheduledInput == nil {
		return "", errors.New("scheduled input encoder is not configured")
	}

	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	schedules, err := s.ListSchedules(ctx, ListSchedulesDBInput{
		ScheduleNamePrefixes: []string{scheduleName},
		Tx:                   tx,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get schedule: %w", err)
	}
	var schedule *models.WorkflowSchedule
	for i := range schedules {
		if schedules[i].ScheduleName == scheduleName {
			schedule = &schedules[i]
			break
		}
	}
	if schedule == nil {
		return "", fmt.Errorf("schedule not found: %s", scheduleName)
	}

	queueName := schedule.QueueName
	if queueName == "" {
		queueName = models.InternalQueueName
	}

	now := time.Now()
	workflowID := fmt.Sprintf("sched-%s-trigger-%s", scheduleName, now.Format(time.RFC3339Nano))

	encodedInput, serName, err := s.encodeScheduledInput(ctx, now, schedule.Context)
	if err != nil {
		return "", fmt.Errorf("failed to encode scheduled workflow input: %w", err)
	}

	// Triggered scheduled workflows run against the latest registered application
	// version. If lookup fails (e.g. no versions registered yet) leave it unset.
	var triggerAppVersion string
	triggerLatest, err := RetryWithResult(ctx, func() (*VersionInfo, error) {
		return s.GetLatestApplicationVersion(ctx, nil)
	}, WithRetrierLogger(s.logger))
	if err != nil {
		s.logger.Error("failed to fetch latest application version for schedule trigger", "schedule", scheduleName, "error", err)
	} else if triggerLatest != nil {
		triggerAppVersion = triggerLatest.Name
	}

	status := models.WorkflowStatus{
		ID:                 workflowID,
		Status:             models.WorkflowStatusEnqueued,
		Name:               schedule.WorkflowName,
		ClassName:          schedule.WorkflowClassName,
		QueueName:          queueName,
		CreatedAt:          now,
		Input:              encodedInput,
		Serialization:      serName,
		ApplicationVersion: triggerAppVersion,
		ScheduleName:       scheduleName,
	}

	if _, err := s.InsertWorkflowStatus(ctx, InsertWorkflowStatusDBInput{Status: status, Tx: tx}); err != nil {
		return "", fmt.Errorf("failed to enqueue triggered workflow: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("failed to commit transaction: %w", err)
	}

	return workflowID, nil
}

/*******************************/
/******* APPLICATION VERSIONS **/
/*******************************/

// VersionInfo describes a registered application version.
type VersionInfo struct {
	ID        string `json:"version_id"`
	Name      string `json:"version_name"`
	Timestamp int64  `json:"version_timestamp"` // epoch milliseconds
	CreatedAt int64  `json:"created_at"`        // epoch milliseconds
}

func (s *SysDB) CreateApplicationVersion(ctx context.Context, versionName string) error {
	query := s.RenderSQL(`
		INSERT INTO %sapplication_versions (version_id, version_name, version_timestamp, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (version_name) DO NOTHING
	`, s.dialect.SchemaPrefix(s.schema))
	nowMs := time.Now().UnixMilli()
	if _, err := s.pool.Exec(ctx, query, uuid.New().String(), versionName, nowMs, nowMs); err != nil {
		return fmt.Errorf("failed to create application version: %w", err)
	}
	return nil
}

func (s *SysDB) UpdateApplicationVersionTimestamp(ctx context.Context, versionName string, newTimestamp int64) error {
	query := s.RenderSQL(`
		UPDATE %sapplication_versions
		SET version_timestamp = $1
		WHERE version_name = $2
	`, s.dialect.SchemaPrefix(s.schema))
	if _, err := s.pool.Exec(ctx, query, newTimestamp, versionName); err != nil {
		return fmt.Errorf("failed to update application version timestamp: %w", err)
	}
	return nil
}

func (s *SysDB) ListApplicationVersions(ctx context.Context) ([]VersionInfo, error) {
	query := s.RenderSQL(`
		SELECT version_id, version_name, version_timestamp, created_at
		FROM %sapplication_versions
		ORDER BY version_timestamp DESC
	`, s.dialect.SchemaPrefix(s.schema))
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list application versions: %w", err)
	}
	defer rows.Close()

	var versions []VersionInfo
	for rows.Next() {
		var v VersionInfo
		if err := rows.Scan(&v.ID, &v.Name, &v.Timestamp, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan application version: %w", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read application versions: %w", err)
	}
	return versions, nil
}

func (s *SysDB) GetLatestApplicationVersion(ctx context.Context, tx Tx) (*VersionInfo, error) {
	query := s.RenderSQL(`
		SELECT version_id, version_name, version_timestamp, created_at
		FROM %sapplication_versions
		ORDER BY version_timestamp DESC
		LIMIT 1
	`, s.dialect.SchemaPrefix(s.schema))
	var q Querier = s.pool
	if tx != nil {
		q = tx
	}
	var v VersionInfo
	err := q.QueryRow(ctx, query).Scan(&v.ID, &v.Name, &v.Timestamp, &v.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, models.NewNoApplicationVersionsError()
		}
		return nil, fmt.Errorf("failed to get latest application version: %w", err)
	}
	return &v, nil
}

/*******************************/
/******* UTILS ********/
/*******************************/

func IsCockroachDB(conn *pgx.Conn) bool {
	return conn.PgConn().ParameterStatus("crdb_version") != ""
}

// DropDatabaseIfExists drops a database in a way that works with both PostgreSQL and CockroachDB.
// For CockroachDB, it terminates active connections first, then drops the database.
// For PostgreSQL, it uses the WITH (FORCE) syntax.
func DropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	crdb := IsCockroachDB(conn)

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

func (s *SysDB) ResetSystemDB(ctx context.Context) error {
	// Get the current database configuration from the pool
	config := PgxPool(s.pool).Config()
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
	err = DropDatabaseIfExists(ctx, conn, dbName)
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
	dialect      Dialect
}

func newQueryBuilder(dialect Dialect) *queryBuilder {
	return &queryBuilder{
		setClauses:   make([]string, 0),
		whereClauses: make([]string, 0),
		args:         make([]any, 0),
		argCounter:   0,
		dialect:      dialect,
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

func (qb *queryBuilder) addWhereIsNull(column string) {
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s IS NULL", column))
}

func (qb *queryBuilder) addWhereLike(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s LIKE $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

// Manually expand array parameters for databases that don't support them
func (qb *queryBuilder) addWhereAny(column string, values any) {
	if qb.dialect != nil && !qb.dialect.SupportsArrayParameters() {
		v := reflect.ValueOf(values)
		if v.Kind() == reflect.Slice {
			placeholders := make([]string, v.Len())
			for i := 0; i < v.Len(); i++ {
				qb.argCounter++
				placeholders[i] = fmt.Sprintf("$%d", qb.argCounter)
				// Unwrap named primitive types to their underlying kind so
				// database/sql's positional binding accepts them.
				item := v.Index(i)
				var bound any
				switch item.Kind() {
				case reflect.String:
					bound = item.String()
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					bound = item.Int()
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					bound = item.Uint()
				case reflect.Float32, reflect.Float64:
					bound = item.Float()
				case reflect.Bool:
					bound = item.Bool()
				default:
					bound = item.Interface()
				}
				qb.args = append(qb.args, bound)
			}
			qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", ")))
			return
		}
	}
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

// MaskPassword replaces the password in a database URL with asterisks
func MaskPassword(dbURL string) (string, error) {
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

// RetryOption is a functional option for configuring retry behavior
type RetryOption func(*retryConfig)

// WithRetrierLogger sets the logger for the retrier
func WithRetrierLogger(logger *slog.Logger) RetryOption {
	return func(c *retryConfig) {
		c.logger = logger
	}
}

// WithRetryCondition appends the given condition functions to the retry condition chain.
// An error is retryable if any function in the chain returns true.
func WithRetryCondition(fns ...func(error, *slog.Logger) bool) RetryOption {
	return func(c *retryConfig) {
		c.retryConditionChain = append(c.retryConditionChain, fns...)
	}
}

// Retry executes a function with Retry logic using functional optionsr
func Retry(ctx context.Context, fn func() error, options ...RetryOption) error {
	config := &retryConfig{
		maxRetries:    -1,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      30 * time.Second,
		backoffFactor: 2.0,
		jitterMin:     0.95,
		jitterMax:     1.05,
		retryConditionChain: []func(error, *slog.Logger) bool{
			PostgresDialect{}.IsRetryable,
			SqliteDialect{}.IsRetryable,
		},
	}

	// Apply options
	for _, opt := range options {
		opt(config)
	}

	sched := BackoffSchedule{
		Base:      config.baseDelay,
		Max:       config.maxDelay,
		Factor:    config.backoffFactor,
		JitterMin: config.jitterMin,
		JitterMax: config.jitterMax,
	}

	// decide: retryable if any chain condition matches, until the (optional)
	// maxRetries budget is spent. runs is the number of completed runs, so
	// runs > maxRetries means the last allowed run has just failed.
	decide := func(err error, runs int) (bool, error) {
		retryable := false
		for _, cond := range config.retryConditionChain {
			if cond(err, config.logger) {
				retryable = true
				break
			}
		}
		if !retryable {
			if config.logger != nil {
				config.logger.Debug("Non-retryable error encountered", "error", err)
			}
			return false, err
		}
		if config.maxRetries >= 0 && runs > config.maxRetries {
			return false, err
		}
		return true, nil
	}

	onRetry := func(err error, runs int, delay time.Duration) {
		if config.logger != nil {
			config.logger.Debug("Retrying operation",
				"attempt", runs,
				"max_retries", config.maxRetries,
				"delay", delay,
				"error", err)
		}
	}

	onCancel := func() error {
		if config.logger != nil {
			config.logger.Debug("Retry operation cancelled", "error", ctx.Err())
		}
		return ctx.Err()
	}

	return RetryLoop(ctx, sched, fn, decide, onRetry, onCancel)
}

// RetryWithResult executes a function that returns a value with retry logic
// It uses the non-generic retry function under the hood
func RetryWithResult[T any](ctx context.Context, fn func() (T, error), options ...RetryOption) (T, error) {
	var result T

	wrappedFn := func() error {
		var err error
		result, err = fn()
		return err
	}

	// Return retry's error directly: it is the final fn() error, or ctx.Err()
	// when the context is cancelled during a backoff wait.
	return result, Retry(ctx, wrappedFn, options...)
}

func (s *SysDB) ExportWorkflow(ctx context.Context, workflowID string, exportChildren bool) ([]ExportedWorkflow, error) {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for exportWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	workflowIDs := []string{workflowID}
	if exportChildren {
		children, err := s.GetWorkflowChildren(ctx, GetWorkflowChildrenDBInput{
			WorkflowID: workflowID,
			Tx:         tx,
		})
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			workflowIDs = append(workflowIDs, child.ID)
		}
	}

	exported := make([]ExportedWorkflow, 0, len(workflowIDs))

	for _, wfID := range workflowIDs {
		// Export workflow_status
		statusQuery := s.RenderSQL(`SELECT
				workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
				output, error, executor_id, created_at, updated_at, application_version, application_id,
				class_name, config_name, recovery_attempts, queue_name, workflow_timeout_ms,
				workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs, priority,
				queue_partition_key, forked_from, parent_workflow_id, delay_until_epoch_ms, serialization,
				was_forked_from, rate_limited, completed_at, attributes, schedule_name,
				debounce_deadline_epoch_ms, is_debounced
			FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		row := tx.QueryRow(ctx, statusQuery, wfID)
		var (
			wfUUID, status, name                                         *string
			authUser, assumedRole, authRoles, output, errStr, executorID *string
			appVersion, appID, className, configName, queueName          *string
			dedupID, inputs, queuePartitionKey, forkedFrom               *string
			parentWorkflowID                                             *string
			createdAt, updatedAt, recoveryAttempts                       *int64
			workflowTimeoutMs, workflowDeadlineEpochMs, startedAtEpochMs *int64
			priority                                                     *int
			delayUntilEpochMs                                            *int64
			serialization                                                *string
			wasForkedFrom, rateLimited, isDebounced                      *bool
			completedAt, debounceDeadlineEpochMs                         *int64
			attributes, wfScheduleName                                   *string
		)
		err := row.Scan(
			&wfUUID, &status, &name, &authUser, &assumedRole, &authRoles,
			&output, &errStr, &executorID, &createdAt, &updatedAt, &appVersion, &appID,
			&className, &configName, &recoveryAttempts, &queueName, &workflowTimeoutMs,
			&workflowDeadlineEpochMs, &startedAtEpochMs, &dedupID, &inputs, &priority,
			&queuePartitionKey, &forkedFrom, &parentWorkflowID, &delayUntilEpochMs, &serialization,
			&wasForkedFrom, &rateLimited, &completedAt, &attributes, &wfScheduleName,
			&debounceDeadlineEpochMs, &isDebounced,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, models.NewNonExistentWorkflowError(wfID)
			}
			return nil, fmt.Errorf("failed to export workflow_status for %s: %w", wfID, err)
		}

		workflowStatus := map[string]any{
			"workflow_uuid":              wfUUID,
			"status":                     status,
			"name":                       name,
			"authenticated_user":         authUser,
			"assumed_role":               assumedRole,
			"authenticated_roles":        authRoles,
			"output":                     output,
			"error":                      errStr,
			"executor_id":                executorID,
			"created_at":                 createdAt,
			"updated_at":                 updatedAt,
			"application_version":        appVersion,
			"application_id":             appID,
			"class_name":                 className,
			"config_name":                configName,
			"recovery_attempts":          recoveryAttempts,
			"queue_name":                 queueName,
			"workflow_timeout_ms":        workflowTimeoutMs,
			"workflow_deadline_epoch_ms": workflowDeadlineEpochMs,
			"started_at_epoch_ms":        startedAtEpochMs,
			"deduplication_id":           dedupID,
			"inputs":                     inputs,
			"priority":                   priority,
			"queue_partition_key":        queuePartitionKey,
			"forked_from":                forkedFrom,
			"parent_workflow_id":         parentWorkflowID,
			"delay_until_epoch_ms":       delayUntilEpochMs,
			"serialization":              serialization,
			"was_forked_from":            wasForkedFrom,
			"rate_limited":               rateLimited,
			"completed_at":               completedAt,
			"attributes":                 attributes,
			"schedule_name":              wfScheduleName,
			"debounce_deadline_epoch_ms": debounceDeadlineEpochMs,
			"is_debounced":               isDebounced,
		}

		// Export operation_outputs
		outputsQuery := s.RenderSQL(`SELECT workflow_uuid, function_id, function_name, output, error,
				child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
			FROM %soperation_outputs WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		outputRows, err := tx.Query(ctx, outputsQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export operation_outputs for %s: %w", wfID, err)
		}
		var operationOutputs []map[string]any
		for outputRows.Next() {
			var opWfUUID, opFuncName *string
			var opFuncID *int
			var opOutput, opError, opChildWfID *string
			var opStartedAt, opCompletedAt *int64
			var opSerialization *string
			if err := outputRows.Scan(&opWfUUID, &opFuncID, &opFuncName, &opOutput, &opError, &opChildWfID, &opStartedAt, &opCompletedAt, &opSerialization); err != nil {
				scanErr := fmt.Errorf("failed to scan operation_outputs row for %s: %w", wfID, err)
				if cerr := outputRows.Close(); cerr != nil {
					return nil, errors.Join(scanErr, fmt.Errorf("close operation_outputs rows: %w", cerr))
				}
				return nil, scanErr
			}
			operationOutputs = append(operationOutputs, map[string]any{
				"workflow_uuid":         opWfUUID,
				"function_id":           opFuncID,
				"function_name":         opFuncName,
				"output":                opOutput,
				"error":                 opError,
				"child_workflow_id":     opChildWfID,
				"started_at_epoch_ms":   opStartedAt,
				"completed_at_epoch_ms": opCompletedAt,
				"serialization":         opSerialization,
			})
		}
		if cerr := outputRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close operation_outputs rows for %s: %w", wfID, cerr)
		}
		if err := outputRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating operation_outputs for %s: %w", wfID, err)
		}

		// Export workflow_events
		eventsQuery := s.RenderSQL(`SELECT workflow_uuid, key, value, serialization
			FROM %sworkflow_events WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		eventRows, err := tx.Query(ctx, eventsQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events for %s: %w", wfID, err)
		}
		var workflowEvents []map[string]any
		for eventRows.Next() {
			var evWfUUID, evKey, evValue, evSerialization *string
			if err := eventRows.Scan(&evWfUUID, &evKey, &evValue, &evSerialization); err != nil {
				scanErr := fmt.Errorf("failed to scan workflow_events row for %s: %w", wfID, err)
				if cerr := eventRows.Close(); cerr != nil {
					return nil, errors.Join(scanErr, fmt.Errorf("close workflow_events rows: %w", cerr))
				}
				return nil, scanErr
			}
			workflowEvents = append(workflowEvents, map[string]any{
				"workflow_uuid": evWfUUID,
				"key":           evKey,
				"value":         evValue,
				"serialization": evSerialization,
			})
		}
		if cerr := eventRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close workflow_events rows for %s: %w", wfID, cerr)
		}
		if err := eventRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating workflow_events for %s: %w", wfID, err)
		}

		// Export workflow_events_history
		historyQuery := s.RenderSQL(`SELECT workflow_uuid, function_id, key, value, serialization
			FROM %sworkflow_events_history WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		historyRows, err := tx.Query(ctx, historyQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events_history for %s: %w", wfID, err)
		}
		var workflowEventsHistory []map[string]any
		for historyRows.Next() {
			var hWfUUID, hKey, hValue, hSerialization *string
			var hFuncID *int
			if err := historyRows.Scan(&hWfUUID, &hFuncID, &hKey, &hValue, &hSerialization); err != nil {
				scanErr := fmt.Errorf("failed to scan workflow_events_history row for %s: %w", wfID, err)
				if cerr := historyRows.Close(); cerr != nil {
					return nil, errors.Join(scanErr, fmt.Errorf("close workflow_events_history rows: %w", cerr))
				}
				return nil, scanErr
			}
			workflowEventsHistory = append(workflowEventsHistory, map[string]any{
				"workflow_uuid": hWfUUID,
				"function_id":   hFuncID,
				"key":           hKey,
				"value":         hValue,
				"serialization": hSerialization,
			})
		}
		if cerr := historyRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close workflow_events_history rows for %s: %w", wfID, cerr)
		}
		if err := historyRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating workflow_events_history for %s: %w", wfID, err)
		}

		// Export streams
		streamsQuery := s.RenderSQL(`SELECT workflow_uuid, key, value, "offset", function_id, serialization
			FROM %sstreams WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		streamRows, err := tx.Query(ctx, streamsQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export streams for %s: %w", wfID, err)
		}
		var streams []map[string]any
		for streamRows.Next() {
			var sWfUUID, sKey, sValue, sSerialization *string
			var sOffset, sFuncID *int
			if err := streamRows.Scan(&sWfUUID, &sKey, &sValue, &sOffset, &sFuncID, &sSerialization); err != nil {
				scanErr := fmt.Errorf("failed to scan streams row for %s: %w", wfID, err)
				if cerr := streamRows.Close(); cerr != nil {
					return nil, errors.Join(scanErr, fmt.Errorf("close streams rows: %w", cerr))
				}
				return nil, scanErr
			}
			streams = append(streams, map[string]any{
				"workflow_uuid": sWfUUID,
				"key":           sKey,
				"value":         sValue,
				"offset":        sOffset,
				"function_id":   sFuncID,
				"serialization": sSerialization,
			})
		}
		if cerr := streamRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close streams rows for %s: %w", wfID, cerr)
		}
		if err := streamRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating streams for %s: %w", wfID, err)
		}

		exported = append(exported, ExportedWorkflow{
			WorkflowStatus:        workflowStatus,
			OperationOutputs:      operationOutputs,
			WorkflowEvents:        workflowEvents,
			WorkflowEventsHistory: workflowEventsHistory,
			Streams:               streams,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit exportWorkflow transaction: %w", err)
	}
	return exported, nil
}

func (s *SysDB) ImportWorkflow(ctx context.Context, workflows []ExportedWorkflow) error {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction for importWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, wf := range workflows {
		status := wf.WorkflowStatus

		// Import workflow_status
		insertStatusQuery := s.RenderSQL(`INSERT INTO %sworkflow_status (
				workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
				output, error, executor_id, created_at, updated_at, application_version, application_id,
				class_name, config_name, recovery_attempts, queue_name, workflow_timeout_ms,
				workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs, priority,
				queue_partition_key, forked_from, parent_workflow_id, delay_until_epoch_ms, serialization,
				was_forked_from, rate_limited, completed_at, attributes, schedule_name,
				debounce_deadline_epoch_ms, is_debounced
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)`,
			s.dialect.SchemaPrefix(s.schema))

		// was_forked_from and rate_limited are NOT NULL; default them to false
		// for payloads exported before these fields were included (older exports,
		// or ones from an SDK that omits them), so importing them doesn't violate
		// the constraint.
		boolOrFalse := func(v any) bool {
			switch b := v.(type) {
			case bool:
				return b
			case *bool:
				if b != nil {
					return *b
				}
			}
			return false
		}
		wasForkedFrom := boolOrFalse(status["was_forked_from"])
		rateLimited := boolOrFalse(status["rate_limited"])
		isDebounced := boolOrFalse(status["is_debounced"])

		_, err := tx.Exec(ctx, insertStatusQuery,
			status["workflow_uuid"], status["status"], status["name"],
			status["authenticated_user"], status["assumed_role"], status["authenticated_roles"],
			status["output"], status["error"], status["executor_id"],
			status["created_at"], status["updated_at"], status["application_version"], status["application_id"],
			status["class_name"], status["config_name"], status["recovery_attempts"], status["queue_name"],
			status["workflow_timeout_ms"], status["workflow_deadline_epoch_ms"], status["started_at_epoch_ms"],
			status["deduplication_id"], status["inputs"], status["priority"],
			status["queue_partition_key"], status["forked_from"], status["parent_workflow_id"],
			status["delay_until_epoch_ms"], status["serialization"], wasForkedFrom,
			rateLimited, status["completed_at"], status["attributes"], status["schedule_name"],
			status["debounce_deadline_epoch_ms"], isDebounced,
		)
		if err != nil {
			return fmt.Errorf("failed to import workflow_status: %w", err)
		}

		// Import operation_outputs
		for _, op := range wf.OperationOutputs {
			insertOpQuery := s.RenderSQL(`INSERT INTO %soperation_outputs (
					workflow_uuid, function_id, function_name, output, error,
					child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertOpQuery,
				op["workflow_uuid"], op["function_id"], op["function_name"],
				op["output"], op["error"], op["child_workflow_id"],
				op["started_at_epoch_ms"], op["completed_at_epoch_ms"], op["serialization"],
			)
			if err != nil {
				return fmt.Errorf("failed to import operation_outputs: %w", err)
			}
		}

		// Import workflow_events
		for _, ev := range wf.WorkflowEvents {
			insertEvQuery := s.RenderSQL(`INSERT INTO %sworkflow_events (
					workflow_uuid, key, value, serialization
				) VALUES ($1, $2, $3, $4)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertEvQuery,
				ev["workflow_uuid"], ev["key"], ev["value"], ev["serialization"],
			)
			if err != nil {
				return fmt.Errorf("failed to import workflow_events: %w", err)
			}
		}

		// Import workflow_events_history
		for _, h := range wf.WorkflowEventsHistory {
			insertHistQuery := s.RenderSQL(`INSERT INTO %sworkflow_events_history (
					workflow_uuid, function_id, key, value, serialization
				) VALUES ($1, $2, $3, $4, $5)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertHistQuery,
				h["workflow_uuid"], h["function_id"], h["key"], h["value"], h["serialization"],
			)
			if err != nil {
				return fmt.Errorf("failed to import workflow_events_history: %w", err)
			}
		}

		// Import streams
		for _, st := range wf.Streams {
			insertStreamQuery := s.RenderSQL(`INSERT INTO %sstreams (
					workflow_uuid, key, value, "offset", function_id, serialization
				) VALUES ($1, $2, $3, $4, $5, $6)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertStreamQuery,
				st["workflow_uuid"], st["key"], st["value"], st["offset"], st["function_id"], st["serialization"],
			)
			if err != nil {
				return fmt.Errorf("failed to import streams: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit importWorkflow transaction: %w", err)
	}
	return nil
}

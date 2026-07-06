package dbos

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

	"github.com/google/uuid"
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
	updateWorkflowAttributes(ctx context.Context, input updateWorkflowAttributesDBInput) error
	awaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*awaitWorkflowResultOutput, error)
	cancelWorkflows(ctx context.Context, input cancelWorkflowsDBInput) ([]string, error)
	cancelAllBefore(ctx context.Context, cutoffTime time.Time) error
	deleteWorkflows(ctx context.Context, input deleteWorkflowsDBInput) error
	resumeWorkflows(ctx context.Context, input resumeWorkflowsDBInput) ([]string, error)
	forkWorkflows(ctx context.Context, input forkWorkflowsDBInput) ([]string, error)
	forkFrom(ctx context.Context, input forkFromDBInput) ([]string, error)

	getDeduplicatedWorkflow(ctx context.Context, queueName, deduplicationID string) (*string, error)

	// Child workflows
	getWorkflowChildren(ctx context.Context, input getWorkflowChildrenDBInput) ([]WorkflowStatus, error)
	recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error
	checkChildWorkflow(ctx context.Context, workflowUUID string, functionID int, functionName string) (*string, error)

	// Steps
	recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error)
	getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error)

	// Aggregates
	getWorkflowAggregates(ctx context.Context, input getWorkflowAggregatesDBInput) ([]WorkflowAggregateRow, error)
	getStepAggregates(ctx context.Context, input getStepAggregatesDBInput) ([]StepAggregateRow, error)

	// Communication (special steps)
	send(ctx context.Context, input WorkflowSendInput) error
	recv(ctx context.Context, input recvInput) (*recvResult, error)
	setEvent(ctx context.Context, input WorkflowSetEventInput) error
	getEvent(ctx context.Context, input getEventInput) (*getEventResult, error)

	// Communication observability
	getAllEvents(ctx context.Context, workflowID string) ([]eventRecord, error)
	getAllNotifications(ctx context.Context, workflowID string) ([]notificationRecord, error)
	getAllStreamEntries(ctx context.Context, workflowID string) ([]streamEntry, error)

	// Streams
	writeStream(ctx context.Context, input writeStreamDBInput) error
	readStream(ctx context.Context, input readStreamDBInput) ([]streamEntry, bool, error)

	// Timers (special steps)
	sleep(ctx context.Context, input sleepInput) (time.Duration, error)

	// Patches
	patch(ctx context.Context, input patchDBInput) (bool, error)
	doesPatchExists(ctx context.Context, input patchDBInput) (string, error)

	// Queues
	setWorkflowDelay(ctx context.Context, input setWorkflowDelayDBInput) error
	transitionDelayedWorkflows(ctx context.Context) error
	dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error)
	clearQueueAssignment(ctx context.Context, workflowID string) (bool, error)
	getQueuePartitions(ctx context.Context, queueName string) ([]string, error)

	// Database-backed queue registry (the queues table)
	getQueue(ctx context.Context, name string) (*WorkflowQueue, error) // returns nil if the queue does not exist
	listQueues(ctx context.Context) ([]WorkflowQueue, error)
	upsertQueue(ctx context.Context, input upsertQueueDBInput) (bool, error)
	updateQueueConfig(ctx context.Context, name string, mutate func(*WorkflowQueue) error) (*WorkflowQueue, error)
	deleteQueue(ctx context.Context, name string) error

	// Garbage collection
	garbageCollectWorkflows(ctx context.Context, input garbageCollectWorkflowsInput) error

	// Metrics
	getMetrics(ctx context.Context, startTime string, endTime string) ([]metricData, error)

	// Schedules
	createSchedule(ctx context.Context, input createScheduleDBInput) error
	listSchedules(ctx context.Context, input listSchedulesDBInput) ([]WorkflowSchedule, error)
	updateSchedule(ctx context.Context, input updateScheduleDBInput) error
	updateScheduleLastFiredAt(ctx context.Context, scheduleName string, lastFiredAt time.Time) error
	deleteSchedule(ctx context.Context, input deleteScheduleDBInput) error
	backfillSchedule(ctx context.Context, input backfillScheduleDBInput) ([]string, error)
	triggerSchedule(ctx context.Context, scheduleName string) (string, error)

	// Application versions
	createApplicationVersion(ctx context.Context, versionName string) error
	updateApplicationVersionTimestamp(ctx context.Context, versionName string, newTimestamp int64) error
	listApplicationVersions(ctx context.Context) ([]VersionInfo, error)
	getLatestApplicationVersion(ctx context.Context, tx Tx) (*VersionInfo, error)

	// Workflow export/import
	exportWorkflow(ctx context.Context, workflowID string, exportChildren bool) ([]ExportedWorkflow, error)
	importWorkflow(ctx context.Context, workflows []ExportedWorkflow) error
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

type sysDB struct {
	pool                          Pool
	dialect                       Dialect
	notificationLoopDone          chan struct{}
	workflowNotificationsMap      *sync.Map
	workflowNotificationRepollMap *sync.Map
	workflowEventsMap             *sync.Map
	workflowEventsRepollMap       *sync.Map
	streamsMap                    *sync.Map
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

type migrationFile struct {
	version int64
	sql     string
	online  bool
}

const (
	_DBOS_MIGRATION_TABLE = "dbos_migrations"

	// Notification channels
	_DBOS_NOTIFICATIONS_CHANNEL   = "dbos_notifications_channel"
	_DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel"
	_DBOS_STREAMS_CHANNEL         = "dbos_streams_channel"

	// Stream sentinel value for closure
	_DBOS_STREAM_CLOSED_SENTINEL = "__DBOS_STREAM_CLOSED__"

	// Database retry timeouts
	_DB_CONNECTION_RETRY_BASE_DELAY = 1 * time.Second
	_DB_CONNECTION_RETRY_FACTOR     = 2
	_DB_CONNECTION_MAX_DELAY        = 120 * time.Second
	_DB_RETRY_INTERVAL              = 1 * time.Second
)

// returns the CONCURRENTLY keyword for online index DDL.
func concurrentlyKw(isCockroach bool) string {
	if isCockroach {
		return ""
	}
	return "CONCURRENTLY"
}

// buildMigrations renders the full list of migrations against the target schema.
func buildMigrations(schema string, isCockroach bool) []migrationFile {
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

	return []migrationFile{
		{version: 1, sql: migration1SQLProcessed},
		{version: 2, sql: fmt.Sprintf(migration2SQL, sanitizedSchema)},
		{version: 3, sql: fmt.Sprintf(migration3SQL, sanitizedSchema)},
		{version: 4, sql: fmt.Sprintf(migration4SQL, sanitizedSchema, sanitizedSchema)},
		{version: 5, sql: fmt.Sprintf(migration5SQL, sanitizedSchema)},
		{version: 6, sql: fmt.Sprintf(migration6SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 7, sql: fmt.Sprintf(migration7SQL, sanitizedSchema)},
		{version: 8, sql: fmt.Sprintf(migration8SQL, sanitizedSchema, sanitizedSchema)},
		{version: 9, sql: fmt.Sprintf(migration9SQL, sanitizedSchema)},
		{version: 10, sql: fmt.Sprintf(migration10SQL, schema, sanitizedSchema)},
		{version: 11, sql: fmt.Sprintf(migration11SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 12, sql: fmt.Sprintf(migration12SQL, sanitizedSchema, sanitizedSchema)},
		{version: 13, sql: fmt.Sprintf(migration13SQL, sanitizedSchema)},
		{version: 14, sql: fmt.Sprintf(migration14SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 15, sql: fmt.Sprintf(migration15SQL, sanitizedSchema, sanitizedSchema, sanitizedSchema)},
		{version: 16, sql: fmt.Sprintf(migration16SQL, sanitizedSchema, sanitizedSchema)},
		{version: 17, sql: fmt.Sprintf(migration17SQL, sanitizedSchema)},
		{version: 18, sql: fmt.Sprintf(migration18SQL, sanitizedSchema)},
		{version: 19, sql: fmt.Sprintf(migration19SQL, sanitizedSchema)},
		{version: 20, sql: migration20SQLProcessed},
		{version: 21, sql: fmt.Sprintf(migration21SQL, sanitizedSchema)},
		{version: 22, sql: fmt.Sprintf(migration22SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 23, sql: fmt.Sprintf(migration23SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 24, sql: fmt.Sprintf(migration24SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 25, sql: fmt.Sprintf(migration25SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 26, sql: fmt.Sprintf(migration26SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 27, sql: fmt.Sprintf(migration27SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 28, sql: migration28SQLProcessed},
		{version: 29, sql: fmt.Sprintf(migration29SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 30, sql: fmt.Sprintf(migration30SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 31, sql: fmt.Sprintf(migration31SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 32, sql: fmt.Sprintf(migration32SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 33, sql: fmt.Sprintf(migration33SQL, sanitizedSchema)},
		{version: 34, sql: fmt.Sprintf(migration34SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 35, sql: fmt.Sprintf(migration35SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 36, sql: fmt.Sprintf(migration36SQL, sanitizedSchema, sanitizedSchema)},
		{version: 37, sql: fmt.Sprintf(migration37SQL, c, sanitizedSchema), online: !isCockroach},
		{version: 38, sql: migration38SQLProcessed},
		{version: 39, sql: migration39SQLProcessed},
		{version: 40, sql: fmt.Sprintf(migration40SQL, sanitizedSchema, sanitizedSchema)},
	}
}

// shouldMigrate reports whether any migration work remains for the schema.
// Returns true if the schema is missing, the dbos_migrations table is missing,
// or the recorded version is behind the latest.
func shouldMigrate(ctx context.Context, pool *pgxpool.Pool, schema string, isCockroach bool) (bool, error) {
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
		schema, _DBOS_MIGRATION_TABLE).Scan(&tableExists)
	if err != nil {
		return false, fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !tableExists {
		return true, nil
	}

	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", pgx.Identifier{schema}.Sanitize(), _DBOS_MIGRATION_TABLE)
	err = pool.QueryRow(ctx, q).Scan(&currentVersion)
	if err != nil && err != pgx.ErrNoRows {
		return false, fmt.Errorf("failed to get current migration version: %v", err)
	}
	migrations := buildMigrations(schema, isCockroach)
	return currentVersion < migrations[len(migrations)-1].version, nil
}

// cleanupInvalidIndexes drops indexes left in an INVALID state by a prior
// failed CREATE INDEX CONCURRENTLY. Such indexes are not used by the planner
// but block recreating an index of the same name. Must be called before
// retrying an online migration.
func cleanupInvalidIndexes(ctx context.Context, pool *pgxpool.Pool, schema string, logger *slog.Logger) error {
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
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (version) VALUES ($1)", sanitizedSchema, _DBOS_MIGRATION_TABLE)
		if _, err := exec.Exec(ctx, insertQuery, version); err != nil {
			return fmt.Errorf("failed to insert migration version %d: %v", version, err)
		}
	} else {
		updateQuery := fmt.Sprintf("UPDATE %s.%s SET version = $1", sanitizedSchema, _DBOS_MIGRATION_TABLE)
		if _, err := exec.Exec(ctx, updateQuery, version); err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", version, err)
		}
	}
	return nil
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string, isCockroach bool, logger *slog.Logger) error {
	migrations := buildMigrations(schema, isCockroach)
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
		schema, _DBOS_MIGRATION_TABLE).Scan(&migrationTableExists); err != nil {
		return fmt.Errorf("failed to check if migration table exists: %v", err)
	}
	if !migrationTableExists {
		createTableQuery := fmt.Sprintf(`CREATE TABLE %s.%s (version BIGINT NOT NULL PRIMARY KEY)`,
			sanitizedSchema, _DBOS_MIGRATION_TABLE)
		if _, err := tx.Exec(ctx, createTableQuery); err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}
	var currentVersion int64
	q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", sanitizedSchema, _DBOS_MIGRATION_TABLE)
	if err := tx.QueryRow(ctx, q).Scan(&currentVersion); err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to get current migration version: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration setup transaction: %v", err)
	}

	// Apply pending migrations one at a time.
	invalidIndexesCleaned := false
	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}

		if migration.online {
			// Online migrations must run outside a transaction so PostgreSQL will accept CREATE/DROP INDEX CONCURRENTLY.
			// Before the first online migration, sweep up any indexes left INVALID by a prior crashed run.
			// The version bump is necessarily a second, non-atomic round-trip. If it fails and must re-run, re-executing the migration has to be safe.
			if !invalidIndexesCleaned {
				if err := cleanupInvalidIndexes(ctx, pool, schema, logger); err != nil {
					return err
				}
				invalidIndexesCleaned = true
			}
			if _, err := pool.Exec(ctx, migration.sql); err != nil {
				return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
			}
			if err := writeMigrationVersion(ctx, pool, schema, migration.version, currentVersion); err != nil {
				return err
			}
			currentVersion = migration.version
			continue
		}

		if err := applyCatalogMigration(ctx, pool, schema, sanitizedSchema, migration, isCockroach, currentVersion); err != nil {
			return err
		}
		currentVersion = migration.version
	}

	return nil
}

// applyCatalogMigration runs a single non-online migration and its version bump in one transaction.
func applyCatalogMigration(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema, sanitizedSchema string,
	migration migrationFile,
	isCockroach bool,
	currentVersion int64,
) error {
	mtx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for migration %d: %v", migration.version, err)
	}
	defer mtx.Rollback(ctx)

	switch {
	case migration.version == 10 && isCockroach:
		// CockroachDB does not support the DO block used by the Postgres
		// migration file; run the equivalent logic at the application layer
		// inside the same transaction.
		if err := applyCockroachMigration10(ctx, mtx, schema, sanitizedSchema); err != nil {
			return err
		}
	case strings.TrimSpace(migration.sql) == "":
		// No-op migration (e.g. migration 20 on CockroachDB). Still advance
		// the version row so we don't re-evaluate it next time.
	default:
		if _, err := mtx.Exec(ctx, migration.sql); err != nil {
			return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
		}
	}

	if err := writeMigrationVersion(ctx, mtx, schema, migration.version, currentVersion); err != nil {
		return err
	}
	if err := mtx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit migration %d: %v", migration.version, err)
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

type newSystemDatabaseInput struct {
	databaseURL     string
	databaseSchema  string
	customPool      *pgxpool.Pool
	customSqliteDB  *sql.DB
	logger          *slog.Logger
	applicationName string
}

// New creates a new SystemDatabase instance and runs migrations
// renderSQL formats a canonical pg-style query string with sprintf and runs
// it through the dialect's rewrite pass. Use this for every sysDB query that
// must work on both pg and sqlite — it converts $N placeholders to ?N for
// sqlite while leaving pg unchanged.
func (s *sysDB) renderSQL(format string, args ...any) string {
	return s.dialect.RewriteQuery(fmt.Sprintf(format, args...))
}

func newSystemDatabase(ctx context.Context, inputs newSystemDatabaseInput) (systemDatabase, error) {
	// Dereference fields from inputs
	databaseURL := inputs.databaseURL
	databaseSchema := inputs.databaseSchema
	customPool := inputs.customPool
	customSqliteDB := inputs.customSqliteDB
	logger := inputs.logger

	// Validate that schema is provided
	if databaseSchema == "" {
		return nil, fmt.Errorf("database schema cannot be empty")
	}
	if customPool != nil && customSqliteDB != nil {
		return nil, fmt.Errorf("customPool and customSqliteDB are mutually exclusive")
	}

	// Dispatch sqlite first
	if customSqliteDB != nil {
		return newSqliteSystemDatabase(ctx, databaseURL, databaseSchema, customSqliteDB, logger)
	}
	if customPool == nil {
		dialectName, err := detectDialect(databaseURL)
		if err != nil {
			return nil, err
		}
		if dialectName == DialectSQLite {
			return newSqliteSystemDatabase(ctx, databaseURL, databaseSchema, nil, logger)
		}
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
	isCockroach := isCockroachDB(conn.Conn())
	if isCockroach {
		logger.Info("Detected CockroachDB")
	}

	needsMigration, smErr := shouldMigrate(ctx, pool, databaseSchema, isCockroach)
	if smErr != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to determine migration status: %v", smErr)
	}
	if needsMigration {
		if err := retry(ctx, func() error {
			return runMigrations(ctx, pool, databaseSchema, isCockroach, logger)
		}, withRetrierLogger(logger)); err != nil {
			if customPool == nil {
				pool.Close()
			}
			return nil, fmt.Errorf("failed to run migrations: %v", err)
		}
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		if customPool == nil {
			pool.Close()
		}
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	dialect := Dialect(postgresDialect{})
	if isCockroach {
		dialect = cockroachDialect{}
	}

	return &sysDB{
		pool:                          newPgxPool(pool),
		dialect:                       dialect,
		workflowNotificationsMap:      &sync.Map{},
		workflowNotificationRepollMap: &sync.Map{},
		workflowEventsMap:             &sync.Map{},
		workflowEventsRepollMap:       &sync.Map{},
		streamsMap:                    &sync.Map{},
		notificationLoopDone:          make(chan struct{}),
		logger:                        logger.With("service", "system_database"),
		schema:                        databaseSchema,
		isCockroachDB:                 isCockroach,
	}, nil
}

func (s *sysDB) listenNotifyPool() *pgxpool.Pool {
	if s.dialect == nil || !s.dialect.SupportsListenNotify() {
		return nil
	}
	return PgxPool(s.pool)
}

func (s *sysDB) launch(ctx context.Context) {
	if s.listenNotifyPool() == nil {
		go s.notificationPollerLoop(ctx)
	} else {
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
	s.streamsMap.Clear()

	s.launched = false
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type insertWorkflowResult struct {
	attempts          int
	status            WorkflowStatusType
	name              string
	queueName         *string
	queuePartitionKey *string
	timeout           time.Duration
	workflowDeadline  time.Time
	ownerXID          string
}

type insertWorkflowStatusDBInput struct {
	status            WorkflowStatus
	maxRetries        int
	tx                Tx
	ownerXID          *string
	incrementAttempts bool
}

func (s *sysDB) insertWorkflowStatus(ctx context.Context, input insertWorkflowStatusDBInput) (*insertWorkflowResult, error) {
	if input.tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

	// Set default values
	attempts := 1
	if input.status.Status == WorkflowStatusEnqueued || input.status.Status == WorkflowStatusDelayed {
		attempts = 0
	}

	var delayUntilEpochMs *int64
	if !input.status.DelayUntil.IsZero() {
		millis := input.status.DelayUntil.UnixMilli()
		delayUntilEpochMs = &millis
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

	var className *string
	if len(input.status.ClassName) > 0 {
		className = &input.status.ClassName
	}

	var attributesJSON *string
	if len(input.status.Attributes) > 0 {
		marshaled, err := json.Marshal(input.status.Attributes)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal workflow attributes: %w", err)
		}
		attributesStr := string(marshaled)
		attributesJSON = &attributesStr
	}

	query := s.renderSQL(`INSERT INTO %sworkflow_status (
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
        attributes
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
			recovery_attempts = CASE
                WHEN EXCLUDED.status NOT IN ($27, $28) THEN workflow_status.recovery_attempts + $29
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status IN ($27, $28) THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, queue_partition_key, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid`, s.dialect.SchemaPrefix(s.schema))

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
		input.status.Input,
		deduplicationID,
		input.status.Priority,
		queuePartitionKey,
		input.ownerXID,
		parentWorkflowID,
		className,
		input.status.ConfigName,
		input.status.Serialization,
		delayUntilEpochMs,
		attributesJSON,
		WorkflowStatusEnqueued,
		WorkflowStatusDelayed,
		recoveryIncrement,
	).Scan(
		&result.attempts,
		&result.status,
		&result.name,
		&result.queueName,
		&result.queuePartitionKey,
		&timeoutMSResult,
		&workflowDeadlineEpochMS,
		&ownerXIDReturn,
	)
	if ownerXIDReturn != nil {
		result.ownerXID = *ownerXIDReturn
	}
	if err != nil {
		// Handle unique constraint violation for the deduplication ID (this should be the only case)
		if s.dialect.IsUniqueViolation(err) {
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
		dlqQuery := s.renderSQL(`UPDATE %sworkflow_status
					 SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
					 WHERE workflow_uuid = $2 AND status = $3`, s.dialect.SchemaPrefix(s.schema))

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
	completedAfter     time.Time
	completedBefore    time.Time
	dequeuedAfter      time.Time
	dequeuedBefore     time.Time
	wasForkedFrom      *bool
	hasParent          *bool
	attributes         map[string]any
	limit              *int
	offset             *int
	sortDesc           bool
	loadInput          bool
	loadOutput         bool
	tx                 Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *sysDB) listWorkflows(ctx context.Context, input listWorkflowsDBInput) ([]WorkflowStatus, error) {
	qb := newQueryBuilder(s.dialect)

	// Build the base query with conditional column selection
	loadColumns := []string{
		"workflow_uuid", "status", "name", "authenticated_user", "assumed_role", "authenticated_roles",
		"executor_id", "created_at", "updated_at", "application_version", "application_id",
		"recovery_attempts", "queue_name", "workflow_timeout_ms", "workflow_deadline_epoch_ms", "started_at_epoch_ms",
		"deduplication_id", "priority", "queue_partition_key", "forked_from", "parent_workflow_id",
		"serialization", "delay_until_epoch_ms", "was_forked_from", "completed_at", "class_name", "config_name",
		"attributes",
	}

	if input.loadOutput {
		loadColumns = append(loadColumns, "output", "error")
	}
	if input.loadInput {
		loadColumns = append(loadColumns, "inputs")
	}

	baseQuery := fmt.Sprintf("SELECT %s FROM %sworkflow_status", strings.Join(loadColumns, ", "), s.dialect.SchemaPrefix(s.schema))

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
	if !input.completedAfter.IsZero() {
		qb.addWhereGreaterEqual("completed_at", input.completedAfter.UnixMilli())
	}
	if !input.completedBefore.IsZero() {
		qb.addWhereLessEqual("completed_at", input.completedBefore.UnixMilli())
	}
	// dequeued_after/before filter on started_at_epoch_ms: that column records
	// when a workflow was dequeued and began executing.
	if !input.dequeuedAfter.IsZero() {
		qb.addWhereGreaterEqual("started_at_epoch_ms", input.dequeuedAfter.UnixMilli())
	}
	if !input.dequeuedBefore.IsZero() {
		qb.addWhereLessEqual("started_at_epoch_ms", input.dequeuedBefore.UnixMilli())
	}
	if input.wasForkedFrom != nil {
		qb.addWhere("was_forked_from", *input.wasForkedFrom)
	}
	if input.hasParent != nil {
		if *input.hasParent {
			qb.addWhereIsNotNull("parent_workflow_id")
		} else {
			qb.addWhereIsNull("parent_workflow_id")
		}
	}
	if len(input.attributes) > 0 {
		if !s.dialect.SupportsAttributesContainment() {
			return nil, fmt.Errorf("filtering workflows by attributes is not supported on %s; use a Postgres system database to filter by attributes", s.dialect.Name())
		}
		attributesJSON, err := json.Marshal(input.attributes)
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
	} else if input.offset != nil {
		query += dialectNoLimitClause(s.dialect)
	}

	if input.offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.offset)
	}

	// Execute the query against the input tx if provided, else the pool.
	query = s.dialect.RewriteQuery(query)
	var rows Rows
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
		var serialization *string
		var authenticatedUser *string
		var assumedRole *string
		var applicationID *string
		var delayUntilEpochMs *int64
		var completedAtMs *int64
		var className *string
		var attributesJSON *string

		// Build scan arguments dynamically based on loaded columns.
		scanArgs := []any{
			&wf.ID, &wf.Status, &wf.Name, &authenticatedUser, &assumedRole,
			&authenticatedRoles, &executorID, &createdAtMs,
			&updatedAtMs, &applicationVersion, &applicationID,
			&wf.Attempts, &queueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &deduplicationID, &wf.Priority, &queuePartitionKey, &forkedFrom, &parentWorkflowID,
			&serialization, &delayUntilEpochMs, &wf.WasForkedFrom, &completedAtMs, &className, &wf.ConfigName,
			&attributesJSON,
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

		// Convert completed_at milliseconds to time.Time
		if completedAtMs != nil {
			wf.CompletedAt = time.Unix(0, *completedAtMs*int64(time.Millisecond))
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
	errStr     string
	tx         Tx
}

// updateWorkflowOutcome updates the status, output, and error of a workflow
// Note that transitions from CANCELLED to SUCCESS or ERROR are forbidden
func (s *sysDB) updateWorkflowOutcome(ctx context.Context, input updateWorkflowOutcomeDBInput) error {
	query := s.renderSQL(`UPDATE %sworkflow_status
			  SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4, deduplication_id = NULL
			  WHERE workflow_uuid = $5 AND NOT (status = $6 AND CAST($1 AS TEXT) IN ($7, $8))`, s.dialect.SchemaPrefix(s.schema))

	// input.output is already a *string from the database layer
	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.status, input.output, input.errStr, time.Now().UnixMilli(), input.workflowID, WorkflowStatusCancelled, WorkflowStatusSuccess, WorkflowStatusError)
	} else {
		_, err = s.pool.Exec(ctx, query, input.status, input.output, input.errStr, time.Now().UnixMilli(), input.workflowID, WorkflowStatusCancelled, WorkflowStatusSuccess, WorkflowStatusError)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

type updateWorkflowAttributesDBInput struct {
	workflowID string
	attributes map[string]any
	tx         Tx
}

// updateWorkflowAttributes replaces the custom attributes attached to an existing
// workflow. A nil/empty attributes map clears them (stored as NULL). Returns a
// non-existent workflow error if no workflow with the given ID exists.
func (s *sysDB) updateWorkflowAttributes(ctx context.Context, input updateWorkflowAttributesDBInput) error {
	var attributesJSON *string
	if len(input.attributes) > 0 {
		marshaled, err := json.Marshal(input.attributes)
		if err != nil {
			return fmt.Errorf("failed to marshal workflow attributes: %w", err)
		}
		attributesStr := string(marshaled)
		attributesJSON = &attributesStr
	}

	query := s.renderSQL(`UPDATE %sworkflow_status SET attributes = $1, updated_at = $2 WHERE workflow_uuid = $3`, s.dialect.SchemaPrefix(s.schema))

	var res Result
	var err error
	if input.tx != nil {
		res, err = input.tx.Exec(ctx, query, attributesJSON, time.Now().UnixMilli(), input.workflowID)
	} else {
		res, err = s.pool.Exec(ctx, query, attributesJSON, time.Now().UnixMilli(), input.workflowID)
	}
	if err != nil {
		return fmt.Errorf("failed to update workflow attributes: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return newNonExistentWorkflowError(input.workflowID)
	}
	return nil
}

type cancelWorkflowsDBInput struct {
	cancelChildren bool
	workflowIDs    []string
	tx             Tx
}

// cancelWorkflows cancels the given workflows in a single round-trip. Workflows that
// are already in a terminal state (SUCCESS, ERROR, CANCELLED) are left untouched.
// Returns the subset of input IDs that existed in workflow_status (including terminal
// ones, which are considered existing even though they are not updated).
func (s *sysDB) cancelWorkflows(ctx context.Context, input cancelWorkflowsDBInput) ([]string, error) {
	if len(input.workflowIDs) == 0 {
		return nil, nil
	}

	workflowIDs := make([]string, len(input.workflowIDs))
	copy(workflowIDs, input.workflowIDs)

	if input.cancelChildren {
		for _, workflowID := range workflowIDs {
			children, err := s.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
				workflowID: workflowID,
				tx:         input.tx,
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
		updateQuery := s.renderSQL(`UPDATE %sworkflow_status
			SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
			    queue_name = NULL, deduplication_id = NULL
			WHERE %s AND status NOT IN ($4, $5, $6)`, schemaPrefix, anyClause)
		selectAnyClause := dialectAnyClause(s.dialect, "workflow_uuid", 1)
		selectQuery := s.renderSQL(`SELECT workflow_uuid FROM %sworkflow_status WHERE %s`, schemaPrefix, selectAnyClause)
		args := []any{
			WorkflowStatusCancelled,
			time.Now().UnixMilli(),
			encodedIDs,
			WorkflowStatusSuccess,
			WorkflowStatusError,
			WorkflowStatusCancelled,
		}

		var runner Querier
		var localTx Tx
		if input.tx != nil {
			runner = input.tx
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
		found := make([]string, 0, len(input.workflowIDs))
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

	query := s.renderSQL(`WITH existing AS (
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
		WorkflowStatusCancelled,
		time.Now().UnixMilli(),
		encodedIDs,
		WorkflowStatusSuccess,
		WorkflowStatusError,
		WorkflowStatusCancelled,
	}

	var rows Rows
	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, args...)
	} else {
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to cancel workflows: %w", err)
	}
	defer rows.Close()

	found := make([]string, 0, len(input.workflowIDs))
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

type deleteWorkflowsDBInput struct {
	workflowIDs    []string
	deleteChildren bool
	tx             Tx
}

func (s *sysDB) deleteWorkflows(ctx context.Context, input deleteWorkflowsDBInput) error {
	// If no transaction is provided, create one so the entire operation is atomic
	tx := input.tx
	if tx == nil {
		var err error
		tx, err = s.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction for deleteWorkflows: %w", err)
		}
		defer tx.Rollback(ctx)
	}

	// Collect all workflow IDs to delete
	workflowIDs := make([]string, len(input.workflowIDs))
	copy(workflowIDs, input.workflowIDs)

	if input.deleteChildren {
		for _, wfID := range input.workflowIDs {
			children, err := s.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
				workflowID: wfID,
				tx:         tx,
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
	deleteQuery := s.renderSQL(
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
	if input.tx == nil {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit deleteWorkflows transaction: %w", err)
		}
	}

	return nil
}

type getWorkflowChildrenDBInput struct {
	workflowID string
	tx         Tx
}

// getWorkflowChildren retrieves all descendant workflows of the given parent workflow
// (breadth-first) within the same transaction.
func (s *sysDB) getWorkflowChildren(ctx context.Context, input getWorkflowChildrenDBInput) ([]WorkflowStatus, error) {

	children, err := s.listWorkflows(ctx, listWorkflowsDBInput{
		parentWorkflowID: []string{input.workflowID},
		tx:               input.tx,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get children of workflow %s: %w", input.workflowID, err)
	}

	queue := make([]string, 0, len(children))
	for _, child := range children {
		queue = append(queue, child.ID)
	}
	for len(queue) > 0 {
		parentID := queue[0]
		queue = queue[1:]

		grandchildren, err := s.listWorkflows(ctx, listWorkflowsDBInput{
			parentWorkflowID: []string{parentID},
			tx:               input.tx,
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

func (s *sysDB) cancelAllBefore(ctx context.Context, cutoffTime time.Time) error {
	// List all workflows in PENDING, ENQUEUED, or DELAYED state ending at cutoffTime
	listInput := listWorkflowsDBInput{
		endTime: cutoffTime,
		status:  []WorkflowStatusType{WorkflowStatusPending, WorkflowStatusEnqueued, WorkflowStatusDelayed},
	}

	workflows, err := s.listWorkflows(ctx, listInput)
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
	if _, err := s.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIDs: ids}); err != nil {
		return fmt.Errorf("failed to cancel workflows during cancelAllBefore: %w", err)
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
		query := s.renderSQL(`SELECT created_at
				  FROM %sworkflow_status
				  ORDER BY created_at DESC
				  LIMIT 1 OFFSET $1`, s.dialect.SchemaPrefix(s.schema))

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

	// Delete all workflows older than cutoff that are NOT PENDING, ENQUEUED, or DELAYED
	query := s.renderSQL(`DELETE FROM %sworkflow_status
			  WHERE created_at < $1
			    AND status NOT IN ($2, $3, $4)`, s.dialect.SchemaPrefix(s.schema))

	commandTag, err := s.pool.Exec(ctx, query,
		*cutoffTimestamp,
		WorkflowStatusPending,
		WorkflowStatusEnqueued,
		WorkflowStatusDelayed)

	if err != nil {
		return fmt.Errorf("failed to garbage collect workflows: %w", err)
	}

	deletedCount, _ := commandTag.RowsAffected()
	s.logger.Info("Garbage collected workflows",
		"cutoff_timestamp", *cutoffTimestamp,
		"deleted_count", deletedCount)

	return nil
}

type resumeWorkflowsDBInput struct {
	workflowIDs []string
	queueName   string
	tx          Tx
}

// resumeWorkflows re-enqueues the given workflows onto the specified queue (or the internal
// queue if unset). It returns the subset of IDs that existed in workflow_status; IDs in
// terminal states are considered existing even though they are not updated.
func (s *sysDB) resumeWorkflows(ctx context.Context, input resumeWorkflowsDBInput) ([]string, error) {
	if len(input.workflowIDs) == 0 {
		return nil, nil
	}

	schemaPrefix := s.dialect.SchemaPrefix(s.schema)
	anyClause := dialectAnyClause(s.dialect, "workflow_uuid", 5)

	queueName := input.queueName
	if queueName == "" {
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	encodedIDs, err := encodeArrayParam(s.dialect, input.workflowIDs)
	if err != nil {
		return nil, fmt.Errorf("resume workflows: %w", err)
	}

	args := []any{
		WorkflowStatusEnqueued,
		queueName,
		0,
		time.Now().UnixMilli(),
		encodedIDs,
		WorkflowStatusSuccess,
		WorkflowStatusError,
	}

	// Dialects without data-modifying CTEs (sqlite) split the pg
	// single-statement CTE into two statements (UPDATE then SELECT).
	// Needs repeatable read. Reuse the caller's tx when supplied.
	if !s.dialect.SupportsDataModifyingCTE() {
		updateQuery := s.renderSQL(`UPDATE %sworkflow_status
			SET status = $1, queue_name = $2, recovery_attempts = $3,
			    workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
			    started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
			WHERE %s AND status NOT IN ($6, $7)`, schemaPrefix, anyClause)
		selectAnyClause := dialectAnyClause(s.dialect, "workflow_uuid", 1)
		selectQuery := s.renderSQL(`SELECT workflow_uuid FROM %sworkflow_status WHERE %s`, schemaPrefix, selectAnyClause)

		var runner Querier
		var localTx Tx
		if input.tx != nil {
			runner = input.tx
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
		found := make([]string, 0, len(input.workflowIDs))
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

	query := s.renderSQL(`WITH existing AS (
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
	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, args...)
	} else {
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to resume workflows: %w", err)
	}
	defer rows.Close()

	found := make([]string, 0, len(input.workflowIDs))
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

type forkWorkflowsDBInput struct {
	originalWorkflowIDs []string
	forkedWorkflowIDs   []string // Optional: must match originalWorkflowIDs in length if set; empty entries are auto-generated
	startSteps          []int
	applicationVersion  string
	queueName           string
	queuePartitionKey   string
	tx                  Tx
}

func (s *sysDB) forkWorkflows(ctx context.Context, input forkWorkflowsDBInput) ([]string, error) {
	if len(input.originalWorkflowIDs) == 0 {
		return []string{}, nil
	}
	if len(input.startSteps) != len(input.originalWorkflowIDs) {
		return nil, errors.New("originalWorkflowIDs and startSteps must have the same length")
	}
	if len(input.forkedWorkflowIDs) > 0 && len(input.forkedWorkflowIDs) != len(input.originalWorkflowIDs) {
		return nil, errors.New("originalWorkflowIDs and forkedWorkflowIDs must have the same length")
	}

	// Validate start steps and generate forked workflow IDs where not provided
	forkedWorkflowIDs := make([]string, len(input.originalWorkflowIDs))
	for i := range input.originalWorkflowIDs {
		if input.startSteps[i] < 0 {
			return nil, fmt.Errorf("startStep must be >= 0, got %d", input.startSteps[i])
		}
		if len(input.forkedWorkflowIDs) > 0 && input.forkedWorkflowIDs[i] != "" {
			forkedWorkflowIDs[i] = input.forkedWorkflowIDs[i]
		} else {
			forkedWorkflowIDs[i] = uuid.New().String()
		}
	}

	tx := input.tx
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
	listInput := listWorkflowsDBInput{
		workflowIDs: input.originalWorkflowIDs,
		loadInput:   true,
		tx:          tx,
	}
	wfs, err := s.listWorkflows(ctx, listInput)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}
	statusByID := make(map[string]WorkflowStatus, len(wfs))
	for _, wf := range wfs {
		statusByID[wf.ID] = wf
	}
	for _, id := range input.originalWorkflowIDs {
		if _, ok := statusByID[id]; !ok {
			return nil, newNonExistentWorkflowError(id)
		}
	}

	// Determine the queue to place the forked workflows on
	queueName := input.queueName
	if queueName == "" {
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	var queuePartitionKey any
	if input.queuePartitionKey != "" {
		queuePartitionKey = input.queuePartitionKey
	}

	// Bulk insert all forked workflow status rows in one statement, each with
	// the same initial values as its original.
	insertColumns := []string{
		"workflow_uuid", "status", "name", "authenticated_user", "assumed_role",
		"authenticated_roles", "application_version", "application_id", "queue_name",
		"queue_partition_key", "inputs", "created_at", "updated_at", "recovery_attempts",
		"forked_from", "serialization", "class_name", "config_name", "attributes",
	}
	valueRows := make([]string, len(input.originalWorkflowIDs))
	insertArgs := make([]any, 0, len(input.originalWorkflowIDs)*len(insertColumns))
	nowMs := time.Now().UnixMilli()
	for i, originalWorkflowID := range input.originalWorkflowIDs {
		originalWorkflow := statusByID[originalWorkflowID]

		// Determine the application version to use
		appVersion := originalWorkflow.ApplicationVersion
		if input.applicationVersion != "" {
			appVersion = input.applicationVersion
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
			WorkflowStatusEnqueued,
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
	insertQuery := s.renderSQL(`INSERT INTO %sworkflow_status (`+strings.Join(insertColumns, ", ")+`)
		VALUES `+strings.Join(valueRows, ", "), s.dialect.SchemaPrefix(s.schema))
	if _, err = execCtx(ctx, insertQuery, insertArgs...); err != nil {
		return nil, fmt.Errorf("failed to insert forked workflow statuses: %w", err)
	}

	// For workflows forked from a step > 0, copy checkpoints, events, and streams.
	// A UNION ALL mapping of (orig_id, fork_id, start_step) makes each table copy
	// a single statement regardless of batch size.
	mappingBranches := make([]string, 0, len(input.originalWorkflowIDs))
	mappingArgs := make([]any, 0, len(input.originalWorkflowIDs)*3)
	for i, originalWorkflowID := range input.originalWorkflowIDs {
		if input.startSteps[i] <= 0 {
			continue
		}
		base := len(mappingArgs)
		mappingBranches = append(mappingBranches, fmt.Sprintf(
			"SELECT CAST($%d AS TEXT) AS orig_id, CAST($%d AS TEXT) AS fork_id, CAST($%d AS INTEGER) AS start_step",
			base+1, base+2, base+3))
		mappingArgs = append(mappingArgs, originalWorkflowID, forkedWorkflowIDs[i], input.startSteps[i])
	}

	if len(mappingBranches) > 0 {
		mapping := "(" + strings.Join(mappingBranches, " UNION ALL ") + ") AS m"

		copyOutputsQuery := s.renderSQL(`INSERT INTO %soperation_outputs
			(workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
			SELECT m.fork_id, oo.function_id, oo.output, oo.error, oo.function_name, oo.child_workflow_id, oo.started_at_epoch_ms, oo.completed_at_epoch_ms
			FROM `+mapping+`
			JOIN %soperation_outputs oo ON oo.workflow_uuid = m.orig_id AND oo.function_id < m.start_step`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyOutputsQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy operation outputs: %w", err)
		}

		copyEventsHistoryQuery := s.renderSQL(`INSERT INTO %sworkflow_events_history
			(workflow_uuid, function_id, key, value)
			SELECT m.fork_id, h.function_id, h.key, h.value
			FROM `+mapping+`
			JOIN %sworkflow_events_history h ON h.workflow_uuid = m.orig_id AND h.function_id < m.start_step`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyEventsHistoryQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy workflow events history: %w", err)
		}

		// Copy only the latest version of each event (highest function_id per key) into workflow_events.
		copyLatestEventsQuery := s.renderSQL(`INSERT INTO %sworkflow_events (workflow_uuid, key, value)
			SELECT workflow_uuid, key, value FROM (
				SELECT m.fork_id AS workflow_uuid, h.key AS key, h.value AS value,
					ROW_NUMBER() OVER (PARTITION BY m.fork_id, h.key ORDER BY h.function_id DESC) AS rn
				FROM `+mapping+`
				JOIN %sworkflow_events_history h ON h.workflow_uuid = m.orig_id AND h.function_id < m.start_step
			) ranked WHERE rn = 1`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyLatestEventsQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy latest workflow events: %w", err)
		}

		copyStreamsQuery := s.renderSQL(`INSERT INTO %sstreams
			(workflow_uuid, key, value, "offset", function_id)
			SELECT m.fork_id, st.key, st.value, st."offset", st.function_id
			FROM `+mapping+`
			JOIN %sstreams st ON st.workflow_uuid = m.orig_id AND st.function_id < m.start_step`,
			s.dialect.SchemaPrefix(s.schema), s.dialect.SchemaPrefix(s.schema))
		if _, err = execCtx(ctx, copyStreamsQuery, mappingArgs...); err != nil {
			return nil, fmt.Errorf("failed to copy streams: %w", err)
		}
	}

	// Mark the original workflows as having been forked from.
	markIDs, err := encodeArrayParam(s.dialect, input.originalWorkflowIDs)
	if err != nil {
		return nil, err
	}
	markForkedQuery := s.renderSQL(`UPDATE %sworkflow_status SET was_forked_from = TRUE WHERE `+dialectAnyClause(s.dialect, "workflow_uuid", 1), s.dialect.SchemaPrefix(s.schema))
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

type forkFromDBInput struct {
	workflowIDs        []string
	applicationVersion string
	queueName          string
	queuePartitionKey  string
	fromLastFailure    bool
	fromLastStep       bool
	fromStep           *int
	fromStepName       *string
}

// forkFrom forks a batch of workflows, computing each workflow's start step
// from its recorded checkpoints according to exactly one of four modes:
// fromLastFailure (last step that recorded an error, falling back to the last step),
// fromLastStep, fromStep (explicit step), or fromStepName (last occurrence of a named step).
func (s *sysDB) forkFrom(ctx context.Context, input forkFromDBInput) ([]string, error) {
	modes := 0
	for _, set := range []bool{input.fromLastFailure, input.fromLastStep, input.fromStep != nil, input.fromStepName != nil} {
		if set {
			modes++
		}
	}
	if modes != 1 {
		return nil, errors.New("exactly one of fromLastFailure, fromLastStep, fromStep, or fromStepName must be specified")
	}
	if len(input.workflowIDs) == 0 {
		return []string{}, nil
	}

	startSteps := make(map[string]int, len(input.workflowIDs))
	if input.fromStep != nil {
		for _, id := range input.workflowIDs {
			startSteps[id] = *input.fromStep
		}
	} else {
		idsParam, err := encodeArrayParam(s.dialect, input.workflowIDs)
		if err != nil {
			return nil, err
		}
		args := []any{idsParam}

		var stepExpr string
		switch {
		case input.fromLastFailure:
			stepExpr = "COALESCE(MAX(CASE WHEN error IS NOT NULL THEN function_id END), MAX(function_id))"
		default: // fromLastStep and fromStepName
			stepExpr = "MAX(function_id)"
		}
		nameFilter := ""
		if input.fromStepName != nil {
			nameFilter = " AND function_name = $2"
			args = append(args, *input.fromStepName)
		}

		query := s.renderSQL(`SELECT workflow_uuid, `+stepExpr+`
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

		for _, id := range input.workflowIDs {
			if _, ok := startSteps[id]; !ok {
				if input.fromStepName != nil {
					return nil, fmt.Errorf("workflow %s has no step named '%s'", id, *input.fromStepName)
				}
				return nil, fmt.Errorf("workflow %s has no steps", id)
			}
		}
	}

	orderedStartSteps := make([]int, len(input.workflowIDs))
	for i, id := range input.workflowIDs {
		orderedStartSteps[i] = startSteps[id]
	}
	return s.forkWorkflows(ctx, forkWorkflowsDBInput{
		originalWorkflowIDs: input.workflowIDs,
		startSteps:          orderedStartSteps,
		applicationVersion:  input.applicationVersion,
		queueName:           input.queueName,
		queuePartitionKey:   input.queuePartitionKey,
	})
}

type awaitWorkflowResultOutput struct {
	output        *string
	serialization string
	errStr        *string
}

func (s *sysDB) awaitWorkflowResult(ctx context.Context, workflowID string, pollInterval time.Duration) (*awaitWorkflowResultOutput, error) {
	query := s.renderSQL(`SELECT status, output, error, recovery_attempts, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))
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
		result := &awaitWorkflowResultOutput{output: outputString, serialization: storedSerialization}

		switch status {
		case WorkflowStatusSuccess, WorkflowStatusError:
			if errorStr != nil && len(*errorStr) > 0 {
				result.errStr = errorStr
			}
			return result, nil
		case WorkflowStatusCancelled:
			return result, newAwaitedWorkflowCancelledError(workflowID)
		case WorkflowStatusMaxRecoveryAttemptsExceeded:
			return result, newDeadLetterQueueError(workflowID, attempts-2)
		default:
			time.Sleep(pollInterval)
		}
	}
}

type recordOperationResultDBInput struct {
	workflowID      string
	childWorkflowID string
	stepID          int
	stepName        string
	output          *string
	errStr          *string
	tx              Tx
	startedAt       time.Time
	completedAt     time.Time
	serialization   string
}

func (s *sysDB) recordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	startedAtMs := input.startedAt.UnixMilli()
	completedAtMs := input.completedAt.UnixMilli()

	columns := []string{"workflow_uuid", "function_id", "output", "error", "function_name", "started_at_epoch_ms", "completed_at_epoch_ms", "serialization"}
	placeholders := []string{"$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8"}
	args := []any{input.workflowID, input.stepID, input.output, input.errStr, input.stepName, startedAtMs, completedAtMs, input.serialization}
	argCounter := 8

	if input.childWorkflowID != "" {
		columns = append(columns, "child_workflow_id")
		argCounter++
		placeholders = append(placeholders, fmt.Sprintf("$%d", argCounter))
		args = append(args, input.childWorkflowID)
	}

	query := s.renderSQL(`INSERT INTO %soperation_outputs (%s) VALUES (%s)`,
		s.dialect.SchemaPrefix(s.schema), strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, args...)
	} else {
		_, err = s.pool.Exec(ctx, query, args...)
	}

	if err != nil {
		if s.dialect.IsUniqueViolation(err) {
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
	tx               Tx
}

func (s *sysDB) recordChildWorkflow(ctx context.Context, input recordChildWorkflowDBInput) error {
	query := s.renderSQL(`INSERT INTO %soperation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)`, s.dialect.SchemaPrefix(s.schema))

	var result Result
	var err error
	if input.tx != nil {
		result, err = input.tx.Exec(ctx, query,
			input.parentWorkflowID, input.stepID, input.stepName, input.childWorkflowID)
	} else {
		result, err = s.pool.Exec(ctx, query,
			input.parentWorkflowID, input.stepID, input.stepName, input.childWorkflowID)
	}

	if err != nil {
		if s.dialect.IsUniqueViolation(err) {
			return fmt.Errorf(
				"child workflow %s already registered for parent workflow %s (operation ID: %d). Is your workflow deterministic?",
				input.childWorkflowID, input.parentWorkflowID, input.stepID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}

	n, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected after recording child workflow: %w", err)
	}
	if n == 0 {
		s.logger.Warn("RecordChildWorkflow No rows were affected by the insert")
	}

	return nil
}

func (s *sysDB) checkChildWorkflow(ctx context.Context, workflowID string, functionID int, functionName string) (*string, error) {
	query := s.renderSQL(`SELECT child_workflow_id, function_name
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
		return nil, newUnexpectedStepError(workflowID, functionID, functionName, recordedFunctionName)
	}

	return childWorkflowID, nil
}

// getDeduplicatedWorkflow returns the ID of the workflow currently holding the
// deduplication slot for (queueName, deduplicationID), or nil if the slot is free.
func (s *sysDB) getDeduplicatedWorkflow(ctx context.Context, queueName, deduplicationID string) (*string, error) {
	query := s.renderSQL(`SELECT workflow_uuid
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

type recordedResult struct {
	output        *string
	errStr        *string
	serialization string
}

type checkOperationExecutionDBInput struct {
	workflowID string
	stepID     int
	stepName   string
	tx         Tx
}

func (s *sysDB) checkOperationExecution(ctx context.Context, input checkOperationExecutionDBInput) (*recordedResult, error) {
	var tx Tx
	var err error

	// Use provided transaction or create a new one
	if input.tx != nil {
		tx = input.tx
	} else {
		tx, err = s.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx) // We don't need to commit this transaction -- it is just useful for having READ COMMITTED across the reads
	}

	// First query: Retrieve the workflow status
	workflowStatusQuery := s.renderSQL(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

	// Second query: Retrieve operation outputs if they exist
	stepOutputQuery := s.renderSQL(`SELECT output, error, function_name, serialization
							 FROM %soperation_outputs
							 WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))

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
	var serialization *string

	err = tx.QueryRow(ctx, stepOutputQuery, input.workflowID, input.stepID).Scan(&outputString, &errorStr, &recordedFunctionName, &serialization)

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

	var storedSerialization string
	if serialization != nil {
		storedSerialization = *serialization
	}
	var recordedErrStr *string
	if errorStr != nil && *errorStr != "" {
		recordedErrStr = errorStr
	}
	result := &recordedResult{
		output:        outputString,
		errStr:        recordedErrStr,
		serialization: storedSerialization,
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
	Serialization   string    // The serialization format used for this step
}

type getWorkflowStepsInput struct {
	workflowID string
	loadOutput bool
	limit      *int
	offset     *int
}

func (s *sysDB) getWorkflowSteps(ctx context.Context, input getWorkflowStepsInput) ([]stepInfo, error) {
	loadColumns := []string{"function_id", "function_name", "error", "child_workflow_id", "started_at_epoch_ms", "completed_at_epoch_ms", "serialization"}
	if input.loadOutput {
		loadColumns = append(loadColumns, "output")
	}
	query := s.renderSQL(`SELECT `+strings.Join(loadColumns, ", ")+`
			  FROM %soperation_outputs
			  WHERE workflow_uuid = $1
			  ORDER BY function_id ASC`, s.dialect.SchemaPrefix(s.schema))

	args := []any{input.workflowID}
	if input.limit != nil {
		args = append(args, *input.limit)
		query += fmt.Sprintf(" LIMIT $%d", len(args))
	} else if input.offset != nil {
		query += dialectNoLimitClause(s.dialect)
	}
	if input.offset != nil {
		args = append(args, *input.offset)
		query += fmt.Sprintf(" OFFSET $%d", len(args))
	}

	rows, err := s.pool.Query(ctx, query, args...)
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
		var serialization *string

		scanArgs := []any{&step.StepID, &step.StepName, &errorString, &childWorkflowID, &startedAtMs, &completedAtMs, &serialization}
		if input.loadOutput {
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
		if input.loadOutput {
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

// getWorkflowAggregatesDBInput represents the input parameters for getting workflow aggregates.
type getWorkflowAggregatesDBInput struct {
	groupByStatus             bool
	groupByName               bool
	groupByQueueName          bool
	groupByExecutorID         bool
	groupByApplicationVersion bool
	selectCount               bool
	selectMinCreatedAt        bool
	selectMaxQueueWaitMs      bool
	selectMaxTotalLatencyMs   bool
	timeBucketSizeMs          int64 // 0 disables time bucketing
	status                    []WorkflowStatusType
	startTime                 time.Time
	endTime                   time.Time
	completedAfter            time.Time
	completedBefore           time.Time
	dequeuedAfter             time.Time
	dequeuedBefore            time.Time
	workflowName              []string
	applicationVersion        []string
	executorID                []string
	queueName                 []string
	workflowIDPrefix          []string
	workflowIDs               []string
	authenticatedUser         []string
	forkedFrom                []string
	parentWorkflowID          []string
	wasForkedFrom             *bool
	hasParent                 *bool
	attributes                map[string]any
	limit                     int64 // 0 means use _DEFAULT_AGGREGATES_LIMIT
	tx                        Tx
}

func (s *sysDB) getWorkflowAggregates(ctx context.Context, input getWorkflowAggregatesDBInput) ([]WorkflowAggregateRow, error) {
	if input.timeBucketSizeMs < 0 {
		return nil, errors.New("timeBucketSizeMs must be > 0")
	}

	// Build group columns from boolean flags
	type groupCol struct {
		name string
		expr string
	}
	var groups []groupCol
	if input.groupByStatus {
		groups = append(groups, groupCol{name: "status", expr: "status"})
	}
	if input.groupByName {
		groups = append(groups, groupCol{name: "name", expr: "name"})
	}
	if input.groupByQueueName {
		groups = append(groups, groupCol{name: "queue_name", expr: "queue_name"})
	}
	if input.groupByExecutorID {
		groups = append(groups, groupCol{name: "executor_id", expr: "executor_id"})
	}
	if input.groupByApplicationVersion {
		groups = append(groups, groupCol{name: "application_version", expr: "application_version"})
	}

	qb := newQueryBuilder(s.dialect)

	if input.timeBucketSizeMs > 0 {
		// CockroachDB infers a placeholder's type from its first use and refuses
		// to reuse the same $n in two contexts with different types (here decimal
		// for the division, then int for the multiplication). Bind the bucket size
		// twice so each occurrence gets its own placeholder.
		qb.argCounter++
		divArg := qb.argCounter
		qb.args = append(qb.args, input.timeBucketSizeMs)
		qb.argCounter++
		mulArg := qb.argCounter
		qb.args = append(qb.args, input.timeBucketSizeMs)
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
	if len(input.status) > 0 {
		qb.addWhereAny("status", input.status)
	}
	if !input.startTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.startTime.UnixMilli())
	}
	if !input.endTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.endTime.UnixMilli())
	}
	if len(input.workflowName) > 0 {
		qb.addWhereAny("name", input.workflowName)
	}
	if len(input.applicationVersion) > 0 {
		qb.addWhereAny("application_version", input.applicationVersion)
	}
	if len(input.executorID) > 0 {
		qb.addWhereAny("executor_id", input.executorID)
	}
	if len(input.queueName) > 0 {
		qb.addWhereAny("queue_name", input.queueName)
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
	if len(input.forkedFrom) > 0 {
		qb.addWhereAny("forked_from", input.forkedFrom)
	}
	if len(input.parentWorkflowID) > 0 {
		qb.addWhereAny("parent_workflow_id", input.parentWorkflowID)
	}
	if input.wasForkedFrom != nil {
		qb.addWhere("was_forked_from", *input.wasForkedFrom)
	}
	if input.hasParent != nil {
		if *input.hasParent {
			qb.addWhereIsNotNull("parent_workflow_id")
		} else {
			qb.addWhereIsNull("parent_workflow_id")
		}
	}
	if len(input.attributes) > 0 {
		if !s.dialect.SupportsAttributesContainment() {
			return nil, fmt.Errorf("filtering workflows by attributes is not supported on %s; use a Postgres system database to filter by attributes", s.dialect.Name())
		}
		attributesJSON, err := json.Marshal(input.attributes)
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
	if !input.completedAfter.IsZero() {
		qb.addWhereGreaterEqual("completed_at", input.completedAfter.UnixMilli())
	}
	if !input.completedBefore.IsZero() {
		qb.addWhereLessEqual("completed_at", input.completedBefore.UnixMilli())
	}
	if !input.dequeuedAfter.IsZero() {
		qb.addWhereGreaterEqual("started_at_epoch_ms", input.dequeuedAfter.UnixMilli())
	}
	if !input.dequeuedBefore.IsZero() {
		qb.addWhereLessEqual("started_at_epoch_ms", input.dequeuedBefore.UnixMilli())
	}

	// Build select aggregates. MAX/MIN ignore NULLs, so workflows missing a
	// started_at_epoch_ms or completed_at drop out of the queue-wait / latency maxima.
	type selectCol struct {
		name string
		expr string
	}
	var selects []selectCol
	if input.selectCount {
		selects = append(selects, selectCol{name: "count", expr: "COUNT(*)"})
	}
	if input.selectMinCreatedAt {
		selects = append(selects, selectCol{name: "min_created_at", expr: "MIN(created_at)"})
	}
	if input.selectMaxQueueWaitMs {
		selects = append(selects, selectCol{name: "max_queue_wait_ms", expr: "MAX(started_at_epoch_ms - created_at)"})
	}
	if input.selectMaxTotalLatencyMs {
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
	limit := input.limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	query += fmt.Sprintf(" LIMIT %d", limit)

	var rows Rows
	var err error
	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, qb.args...)
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

// getStepAggregatesDBInput represents the input parameters for getting step aggregates.
type getStepAggregatesDBInput struct {
	groupByFunctionName bool
	groupByStatus       bool
	selectCount         bool
	selectMaxDurationMs bool
	timeBucketSizeMs    int64 // 0 disables time bucketing
	status              []string
	functionName        []string
	workflowIDPrefix    []string
	completedAfter      time.Time
	completedBefore     time.Time
	limit               int64 // 0 means use _DEFAULT_AGGREGATES_LIMIT
	tx                  Tx
}

// statusExpr derives a step's status from operation_outputs: rows with a NULL error are
// SUCCESS, otherwise ERROR. operation_outputs has no explicit status column.
const stepStatusExpr = "(CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END)"

func (s *sysDB) getStepAggregates(ctx context.Context, input getStepAggregatesDBInput) ([]StepAggregateRow, error) {
	if input.timeBucketSizeMs < 0 {
		return nil, errors.New("timeBucketSizeMs must be > 0")
	}

	type groupCol struct {
		name string
		expr string
	}
	var groups []groupCol
	if input.groupByFunctionName {
		groups = append(groups, groupCol{name: "function_name", expr: "function_name"})
	}
	if input.groupByStatus {
		groups = append(groups, groupCol{name: "status", expr: stepStatusExpr})
	}

	qb := newQueryBuilder(s.dialect)

	if input.timeBucketSizeMs > 0 {
		// Bucket on completed_at_epoch_ms, the indexed timestamp on this table.
		// Bind the bucket size twice: see getWorkflowAggregates for why CockroachDB
		// requires a distinct placeholder per type context.
		qb.argCounter++
		divArg := qb.argCounter
		qb.args = append(qb.args, input.timeBucketSizeMs)
		qb.argCounter++
		mulArg := qb.argCounter
		qb.args = append(qb.args, input.timeBucketSizeMs)
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
	if input.selectCount {
		selects = append(selects, selectCol{name: "count", expr: "COUNT(*)"})
	}
	if input.selectMaxDurationMs {
		selects = append(selects, selectCol{name: "max_duration_ms", expr: "MAX(completed_at_epoch_ms - started_at_epoch_ms)"})
	}
	if len(selects) == 0 {
		return nil, errors.New("at least one select_ flag must be set")
	}

	// Apply filters
	if len(input.status) > 0 {
		qb.addWhereAny(stepStatusExpr, input.status)
	}
	if len(input.functionName) > 0 {
		qb.addWhereAny("function_name", input.functionName)
	}
	if len(input.workflowIDPrefix) > 0 {
		qb.addWhereLikeAny("workflow_uuid", input.workflowIDPrefix, "%")
	}
	if !input.completedAfter.IsZero() {
		qb.addWhereGreaterEqual("completed_at_epoch_ms", input.completedAfter.UnixMilli())
	}
	if !input.completedBefore.IsZero() {
		qb.addWhereLessEqual("completed_at_epoch_ms", input.completedBefore.UnixMilli())
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
	limit := input.limit
	if limit <= 0 {
		limit = _DEFAULT_AGGREGATES_LIMIT
	}
	query += fmt.Sprintf(" LIMIT %d", limit)

	var rows Rows
	var err error
	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, qb.args...)
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

type sleepInput struct {
	duration   time.Duration // Duration to sleep
	skipSleep  bool          // If true, the function will not actually sleep and just return the remaining sleep duration
	workflowID string        // Workflow that owns this sleep (resolved by the caller from context)
	stepID     int           // Step ID for this sleep
}

// Sleep is a special type of step that sleeps for a specified duration
// A wakeup time is computed and recorded in the database
// If we sleep is re-executed, it will only sleep for the remaining duration until the wakeup time
// sleep can be called within other special steps (e.g., getEvent, recv) to provide durable sleep

func (s *sysDB) sleep(ctx context.Context, input sleepInput) (time.Duration, error) {
	functionName := "DBOS.sleep"

	stepID := input.stepID
	startTime := time.Now()

	// Check if operation was already executed
	checkInput := checkOperationExecutionDBInput{
		workflowID: input.workflowID,
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

		if recordedResult.errStr != nil { // This should never happen
			return 0, errors.New(*recordedResult.errStr)
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
			workflowID:    input.workflowID,
			stepID:        stepID,
			stepName:      functionName,
			output:        encodedEndTime,
			startedAt:     startTime,
			completedAt:   completedTime,
			serialization: "DBOS_JSON",
		}

		err = s.recordOperationResult(ctx, recordInput)
		if err != nil {
			// Check if this is a ConflictingIDError (operation already recorded by another process)
			if dbosErr, ok := err.(*DBOSError); ok && dbosErr.Code == ConflictingIDError {
			} else {
				return 0, fmt.Errorf("failed to record sleep operation result: %w", err)
			}
		}
	}

	// Calculate remaining duration until wake up time
	remainingDuration := max(0, time.Until(endTime))

	if !input.skipSleep {
		// Sleep for the remaining duration, but wake early if the context is cancelled.
		// If interrupted, return the duration actually slept.
		sleepStart := time.Now()
		timer := time.NewTimer(remainingDuration)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return time.Since(sleepStart), ctx.Err()
		}
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
	query := s.renderSQL(`SELECT function_name FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`, s.dialect.SchemaPrefix(s.schema))
	return functionName, s.pool.QueryRow(ctx, query, input.workflowID, input.stepID).Scan(&functionName)
}

func (s *sysDB) patch(ctx context.Context, input patchDBInput) (bool, error) {
	functionName, err := s.doesPatchExists(ctx, input)
	if err != nil {
		// No result means this is a new workflow, or an existing workflow that has not reached this step yet
		// Insert the patch marker and return true
		if err == pgx.ErrNoRows {
			insertQuery := s.renderSQL(`INSERT INTO %soperation_outputs (workflow_uuid, function_id, function_name) VALUES ($1, $2, $3)`, s.dialect.SchemaPrefix(s.schema))
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

	pgxPool := s.listenNotifyPool()
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
					time.Sleep(connectionRetryBackoff.delayFor(retryAttempt + 1))
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
			time.Sleep(connectionRetryBackoff.delayFor(retryAttempt + 1))
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
		case _DBOS_STREAMS_CHANNEL:
			if ch, ok := s.streamsMap.Load(n.Payload); ok {
				select {
				case ch.(chan struct{}) <- struct{}{}:
				default: // A wake-up hint is already pending
				}
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

		// Query database to check if an unconsumed notification exists
		query := s.renderSQL(`SELECT EXISTS (SELECT 1 FROM %snotifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)`, s.dialect.SchemaPrefix(s.schema))
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
		query := s.renderSQL(`SELECT EXISTS (SELECT 1 FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2)`, s.dialect.SchemaPrefix(s.schema))
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
	DestinationID  string
	Message        any
	Topic          string
	tx             Tx
	serialization  string
	idempotencyKey string
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

	// ON CONFLICT DO NOTHING makes Send idempotent: with an idempotency key the
	// message_uuid is deterministic, so a retried Send inserts at most once. Without
	// a key the random UUID never collides, so the clause is a no-op.
	insertQuery := s.renderSQL(`INSERT INTO %snotifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (message_uuid) DO NOTHING`, s.dialect.SchemaPrefix(s.schema))
	messageUUID := uuid.NewString()
	if input.idempotencyKey != "" {
		messageUUID = fmt.Sprintf("%s::%s", input.idempotencyKey, input.DestinationID)
	}
	createdAtMs := time.Now().UnixMilli()
	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, insertQuery, input.DestinationID, topic, input.Message, input.serialization, messageUUID, createdAtMs)
	} else {
		_, err = s.pool.Exec(ctx, insertQuery, input.DestinationID, topic, input.Message, input.serialization, messageUUID, createdAtMs)
	}
	if err != nil {
		s.logger.Error("failed to insert notification", "error", err, "query", insertQuery, "destination_id", input.DestinationID, "topic", topic, "message", input.Message)
		// Check for foreign key violation (destination workflow doesn't exist)
		if s.dialect.IsForeignKeyViolation(err) {
			return newNonExistentWorkflowError(input.DestinationID)
		}
		return fmt.Errorf("failed to insert notification: %w", err)
	}
	return nil
}

// Recv is a special type of step that receives a message destined for a given workflow
func (s *sysDB) recv(ctx context.Context, input recvInput) (*recvResult, error) {
	functionName := "DBOS.recv"

	stepID := input.stepID
	sleepStepID := input.sleepStepID
	destinationID := input.workflowID

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
		var recvErr error
		if recordedResult.errStr != nil {
			recvErr = errors.New(*recordedResult.errStr)
		}
		return &recvResult{message: recordedResult.output, serialization: recordedResult.serialization}, recvErr
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

	// Now check if there is already an unconsumed message available in the database.
	// If not, we'll wait for a notification and timeout
	var exists bool
	query := s.renderSQL(`SELECT EXISTS (SELECT 1 FROM %snotifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)`, s.dialect.SchemaPrefix(s.schema))
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
			duration:   input.Timeout,
			skipSleep:  true,
			workflowID: destinationID,
			stepID:     sleepStepID,
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

	// Capture start time before finding and consuming the message
	startTime := time.Now()

	// Find the oldest unconsumed message and atomically mark it consumed.
	// Notifications are retained (consumed=true) so they remain visible for observability;
	// rows are eventually cleaned up by FK cascade when the parent workflow is garbage-collected.
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	// Use message_uuid so we update exactly one row; created_at_epoch_ms can match multiple rows when inserts occur in the same millisecond.
	query = s.renderSQL(`
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
	err = tx.QueryRow(ctx, query, destinationID, topic).Scan(&messageString, &msgSerialization)
	if err != nil {
		if err != pgx.ErrNoRows {
			return nil, fmt.Errorf("failed to consume message: %w", err)
		}
	}

	// Use the sender's serialization from the notification; fall back to receiver's format for timeout/no-message case
	serialization := input.serialization
	if msgSerialization != nil && len(*msgSerialization) > 0 {
		serialization = *msgSerialization
	}

	// Record the operation result (with encoded message string)
	completedTime := time.Now()
	recordInput := recordOperationResultDBInput{
		workflowID:    destinationID,
		stepID:        stepID,
		stepName:      functionName,
		output:        messageString,
		tx:            tx,
		startedAt:     startTime,
		completedAt:   completedTime,
		serialization: serialization,
	}

	// Record an error if no message found and timeout occurred
	var timeoutErr error
	if timeoutOccurred && messageString == nil {
		timeoutErr = newTimeoutError(destinationID, functionName, fmt.Sprintf("no message received within %v", input.Timeout))
		s := timeoutErr.Error()
		recordInput.errStr = &s
	}

	err = s.recordOperationResult(ctx, recordInput)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return the message and its serialization format
	return &recvResult{message: messageString, serialization: serialization}, timeoutErr
}

type WorkflowSetEventInput struct {
	Key           string
	Message       any
	tx            Tx
	serialization string
	workflowID    string // Workflow that owns the event (resolved by the caller from context)
	stepID        int    // Step ID for this setEvent (the enclosing transaction step's ID)
}

func (s *sysDB) setEvent(ctx context.Context, input WorkflowSetEventInput) error {
	if _, ok := input.Message.(*string); !ok {
		return fmt.Errorf("message must be a pointer to a string")
	}

	// input.Message is already encoded *string from the typed layer
	// Insert or update the event using UPSERT
	insertQuery := s.renderSQL(`INSERT INTO %sworkflow_events (workflow_uuid, key, value, serialization)
					VALUES ($1, $2, $3, $4)
					ON CONFLICT (workflow_uuid, key)
					DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization`, s.dialect.SchemaPrefix(s.schema))

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, insertQuery, input.workflowID, input.Key, input.Message, input.serialization)
	} else {
		_, err = s.pool.Exec(ctx, insertQuery, input.workflowID, input.Key, input.Message, input.serialization)
	}
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Record event in workflow_events_history
	insertHistoryQuery := s.renderSQL(`INSERT INTO %sworkflow_events_history (workflow_uuid, function_id, key, value, serialization)
					VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (workflow_uuid, function_id, key)
					DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization`, s.dialect.SchemaPrefix(s.schema))

	if input.tx != nil {
		_, err = input.tx.Exec(ctx, insertHistoryQuery, input.workflowID, input.stepID, input.Key, input.Message, input.serialization)
	} else {
		_, err = s.pool.Exec(ctx, insertHistoryQuery, input.workflowID, input.stepID, input.Key, input.Message, input.serialization)
	}
	return err
}

func (s *sysDB) getEvent(ctx context.Context, input getEventInput) (*getEventResult, error) {
	functionName := "DBOS.getEvent"

	stepID := input.stepID
	sleepStepID := input.sleepStepID
	isInWorkflow := input.isInWorkflow

	startTime := time.Now()
	if isInWorkflow {
		// Check if operation was already executed (only if in workflow)
		checkInput := checkOperationExecutionDBInput{
			workflowID: input.workflowID,
			stepID:     stepID,
			stepName:   functionName,
		}
		recordedResult, err := s.checkOperationExecution(ctx, checkInput)
		if err != nil {
			return nil, err
		}
		if recordedResult != nil {
			var evtErr error
			if recordedResult.errStr != nil {
				evtErr = errors.New(*recordedResult.errStr)
			}
			return &getEventResult{value: recordedResult.output, serialization: recordedResult.serialization}, evtErr
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
	query := s.renderSQL(`SELECT value, serialization FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2`, s.dialect.SchemaPrefix(s.schema))
	var valueString *string
	var evtSerialization *string
	var row pgx.Row
	var err error

	// Helper function to query the event and handle errors
	queryEvent := func() error {
		row = s.pool.QueryRow(ctx, query, input.TargetWorkflowID, input.Key)
		err = row.Scan(&valueString, &evtSerialization)
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
					duration:   input.Timeout,
					skipSleep:  true,
					workflowID: input.workflowID,
					stepID:     sleepStepID,
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

	// Use the event's serialization from the DB; fall back to caller's format for timeout/no-event case
	serialization := input.serialization
	if evtSerialization != nil && len(*evtSerialization) > 0 {
		serialization = *evtSerialization
	}

	// Record the operation result if this is called within a workflow
	var timeoutErr error
	if isInWorkflow {
		completedTime := time.Now()
		recordInput := recordOperationResultDBInput{
			workflowID:    input.workflowID,
			stepID:        stepID,
			stepName:      functionName,
			output:        valueString,
			startedAt:     startTime,
			completedAt:   completedTime,
			serialization: serialization,
		}

		// Record an error if no event found and timeout occurred
		if timeoutOccurred && valueString == nil {
			timeoutErr = newTimeoutError(input.workflowID, functionName, fmt.Sprintf("no event found for key '%s' within %v", input.Key, input.Timeout))
			s := timeoutErr.Error()
			recordInput.errStr = &s
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

	// Return the event value and its serialization format
	return &getEventResult{value: valueString, serialization: serialization}, timeoutErr
}

/*******************************/
/******* STREAMS ********/
/*******************************/

type writeStreamDBInput struct {
	Key           string
	Value         *string // Already serialized
	tx            Tx
	serialization string
	workflowID    string // Workflow that owns the stream (resolved by the caller from context)
	stepID        int    // Step ID for this write (the enclosing transaction step's ID)
}

type readStreamDBInput struct {
	WorkflowID string
	Key        string
	FromOffset int
}

type streamEntry struct {
	Key           string
	Value         string
	Offset        int
	Serialization string
}

func (s *sysDB) writeStream(ctx context.Context, input writeStreamDBInput) error {
	// When no transaction is provided, run queries on the pool directly (no transaction).
	tx := input.tx
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

	checkClosedQuery := s.renderSQL(`SELECT 1 FROM %sstreams
		WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1`,
		schema)

	insertQuery := s.renderSQL(`INSERT INTO %sstreams (workflow_uuid, key, value, "offset", function_id, serialization)
		SELECT $1, $2, $3, COALESCE(
			(SELECT MAX("offset") FROM %sstreams WHERE workflow_uuid = $1 AND key = $2), -1
		) + 1, $4, $5`,
		schema, schema)

	var err error
	var exists int

	err = queryRow(ctx, checkClosedQuery, input.workflowID, input.Key, _DBOS_STREAM_CLOSED_SENTINEL).Scan(&exists)
	if err == nil && exists == 1 {
		return fmt.Errorf("stream '%s' is already closed", input.Key)
	} else if err != nil && err != pgx.ErrNoRows {
		return fmt.Errorf("failed to check stream status: %w", err)
	}

	_, err = exec(ctx, insertQuery, input.workflowID, input.Key, input.Value, input.stepID, input.serialization)
	if err != nil {
		return fmt.Errorf("failed to insert stream entry: %w", err)
	}

	return nil
}

// readStream reads stream entries starting from a given offset.
// Returns the entries, whether the stream is closed, and any error.
func (s *sysDB) readStream(ctx context.Context, input readStreamDBInput) ([]streamEntry, bool, error) {
	query := s.renderSQL(`SELECT value, "offset", serialization FROM %sstreams
		WHERE workflow_uuid = $1 AND key = $2 AND "offset" >= $3
		ORDER BY "offset" ASC`,
		s.dialect.SchemaPrefix(s.schema))

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
		var serialization *string
		if err := rows.Scan(&value, &offset, &serialization); err != nil {
			return nil, false, fmt.Errorf("failed to scan stream entry: %w", err)
		}

		if value == _DBOS_STREAM_CLOSED_SENTINEL {
			closed = true
			break
		}

		var ser string
		if serialization != nil {
			ser = *serialization
		}
		entries = append(entries, streamEntry{
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

// eventRecord is one row from the workflow_events table.
type eventRecord struct {
	Key           string
	Value         string
	Serialization string
}

// getAllEvents returns every event row currently set on the workflow.
func (s *sysDB) getAllEvents(ctx context.Context, workflowID string) ([]eventRecord, error) {
	query := s.renderSQL(`SELECT key, value, serialization FROM %sworkflow_events WHERE workflow_uuid = $1`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query workflow events: %w", err)
	}
	defer rows.Close()

	var events []eventRecord
	for rows.Next() {
		var rec eventRecord
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

// notificationRecord is one row from the notifications table.
// Topic is nil when the row stored the __null__topic__ sentinel.
type notificationRecord struct {
	Topic            *string
	Message          string
	Serialization    string
	CreatedAtEpochMs int64
	Consumed         bool
}

// getAllNotifications returns every notification sent to the workflow, ordered by arrival time.
// The __null__topic__ sentinel is normalized back to a nil Topic.
func (s *sysDB) getAllNotifications(ctx context.Context, workflowID string) ([]notificationRecord, error) {
	query := s.renderSQL(`SELECT topic, message, serialization, created_at_epoch_ms, consumed
		FROM %snotifications
		WHERE destination_uuid = $1
		ORDER BY created_at_epoch_ms`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query notifications: %w", err)
	}
	defer rows.Close()

	var results []notificationRecord
	for rows.Next() {
		var rec notificationRecord
		var serialization *string
		if err := rows.Scan(&rec.Topic, &rec.Message, &serialization, &rec.CreatedAtEpochMs, &rec.Consumed); err != nil {
			return nil, fmt.Errorf("failed to scan notification row: %w", err)
		}
		if rec.Topic != nil && *rec.Topic == _DBOS_NULL_TOPIC {
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

// getAllStreamEntries returns every stream entry for the workflow, ordered by (key, offset).
// Rows holding the stream-closed sentinel are filtered out; callers may group by Key.
func (s *sysDB) getAllStreamEntries(ctx context.Context, workflowID string) ([]streamEntry, error) {
	query := s.renderSQL(`SELECT key, value, "offset", serialization FROM %sstreams
		WHERE workflow_uuid = $1
		ORDER BY key, "offset"`,
		s.dialect.SchemaPrefix(s.schema))

	rows, err := s.pool.Query(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to query streams: %w", err)
	}
	defer rows.Close()

	var records []streamEntry
	for rows.Next() {
		var rec streamEntry
		var serialization *string
		if err := rows.Scan(&rec.Key, &rec.Value, &rec.Offset, &serialization); err != nil {
			return nil, fmt.Errorf("failed to scan stream row: %w", err)
		}
		if rec.Value == _DBOS_STREAM_CLOSED_SENTINEL {
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

type setWorkflowDelayDBInput struct {
	workflowID string
	delayUntil time.Time
	tx         Tx
}

// setWorkflowDelay updates the delay on a DELAYED workflow.
func (s *sysDB) setWorkflowDelay(ctx context.Context, input setWorkflowDelayDBInput) error {
	query := s.renderSQL(`UPDATE %sworkflow_status
		SET delay_until_epoch_ms = $1, updated_at = $2
		WHERE workflow_uuid = $3
		  AND status = $4`, s.dialect.SchemaPrefix(s.schema))

	nowMs := time.Now().UnixMilli()
	delayMs := input.delayUntil.UnixMilli()

	if input.tx != nil {
		_, err := input.tx.Exec(ctx, query, delayMs, nowMs, input.workflowID, WorkflowStatusDelayed)
		if err != nil {
			return fmt.Errorf("failed to set workflow delay: %w", err)
		}
	} else {
		_, err := s.pool.Exec(ctx, query, delayMs, nowMs, input.workflowID, WorkflowStatusDelayed)
		if err != nil {
			return fmt.Errorf("failed to set workflow delay: %w", err)
		}
	}
	return nil
}

// transitionDelayedWorkflows transitions DELAYED workflows whose delay has expired to ENQUEUED.
func (s *sysDB) transitionDelayedWorkflows(ctx context.Context) error {
	nowMs := time.Now().UnixMilli()
	query := s.renderSQL(`UPDATE %sworkflow_status
		SET status = $1
		WHERE status = $2
		  AND delay_until_epoch_ms <= $3`, s.dialect.SchemaPrefix(s.schema))

	_, err := s.pool.Exec(ctx, query, WorkflowStatusEnqueued, WorkflowStatusDelayed, nowMs)
	if err != nil {
		return fmt.Errorf("failed to transition delayed workflows: %w", err)
	}
	return nil
}

type dequeuedWorkflow struct {
	id            string
	name          string
	input         *string
	serialization string
	configName    *string
}

type dequeueWorkflowsInput struct {
	queue              WorkflowQueue
	executorID         string
	applicationVersion string
	queuePartitionKey  string
	localRunningCount  int
}

func (s *sysDB) dequeueWorkflows(ctx context.Context, input dequeueWorkflowsInput) ([]dequeuedWorkflow, error) {
	// Snapshot isolation is only required for global concurrency or rate limiting.
	// Otherwise read committed suffices: worker concurrency is enforced in-memory.
	snapshot := input.queue.GlobalConcurrency != nil || input.queue.RateLimit != nil
	tx, err := s.pool.BeginTx(ctx, TxOptions{IsoLevel: s.dialect.QueueDequeueIsolation(snapshot)})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	schemaPrefix := s.dialect.SchemaPrefix(s.schema)

	// Rate limiter: count workflows started within the limiter period.
	var numRecentQueries int
	if input.queue.RateLimit != nil {
		cutoffTimeMs := time.Now().Add(-input.queue.RateLimit.Period).UnixMilli()

		limiterQuery := s.renderSQL(`
		SELECT COUNT(*)
		FROM %sworkflow_status
		WHERE queue_name = $1
		  AND rate_limited = TRUE
		  AND status NOT IN ($2, $3)
		  AND started_at_epoch_ms > $4`, schemaPrefix)

		limiterArgs := []any{input.queue.Name, WorkflowStatusEnqueued, WorkflowStatusDelayed, cutoffTimeMs}
		if len(input.queuePartitionKey) > 0 {
			limiterQuery += ` AND queue_partition_key = $5`
			limiterArgs = append(limiterArgs, input.queuePartitionKey)
		}

		err := tx.QueryRow(ctx, s.dialect.RewriteQuery(limiterQuery), limiterArgs...).Scan(&numRecentQueries)
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}

		if numRecentQueries >= input.queue.RateLimit.Limit {
			return []dequeuedWorkflow{}, nil
		}
	}

	// Calculate max_tasks based on concurrency limits
	maxTasks := input.queue.MaxTasksPerIteration

	if input.queue.WorkerConcurrency != nil {
		workerConcurrency := *input.queue.WorkerConcurrency
		if input.localRunningCount > workerConcurrency {
			s.logger.Warn("Local running workflows on queue exceeds worker concurrency limit", "local_running", input.localRunningCount, "queue_name", input.queue.Name, "concurrency_limit", workerConcurrency)
		}
		maxTasks = max(workerConcurrency-input.localRunningCount, 0)
	}

	if input.queue.GlobalConcurrency != nil {
		pendingQuery := s.renderSQL(`
			SELECT COUNT(*)
			FROM %sworkflow_status
			WHERE queue_name = $1 AND status = $2`, schemaPrefix)

		pendingArgs := []any{input.queue.Name, WorkflowStatusPending}
		if len(input.queuePartitionKey) > 0 {
			pendingQuery += ` AND queue_partition_key = $3`
			pendingArgs = append(pendingArgs, input.queuePartitionKey)
		}

		var globalCount int
		if err := tx.QueryRow(ctx, s.dialect.RewriteQuery(pendingQuery), pendingArgs...).Scan(&globalCount); err != nil {
			return nil, fmt.Errorf("failed to query pending workflows: %w", err)
		}

		concurrency := *input.queue.GlobalConcurrency
		if globalCount > concurrency {
			s.logger.Warn("Total pending workflows on queue exceeds global concurrency limit", "total_pending", globalCount, "queue_name", input.queue.Name, "concurrency_limit", concurrency)
		}
		availableTasks := max(concurrency-globalCount, 0)
		if availableTasks < maxTasks {
			maxTasks = availableTasks
		}
	}

	if maxTasks <= 0 {
		return nil, nil
	}

	// Build the SELECT for candidate workflow IDs. Always order by
	// (priority, created_at) so the planner can satisfy the dequeue scan from
	// idx_workflow_status_in_flight (queue_name, status, priority, created_at).
	isLatestVersion := true
	switch latest, err := s.getLatestApplicationVersion(ctx, tx); {
	case err == nil:
		isLatestVersion = latest.Name == input.applicationVersion
	case errors.Is(err, &DBOSError{Code: NoApplicationVersions}):
		// No versions registered yet: treat this worker as the latest.
	default:
		return nil, fmt.Errorf("failed to query latest application version: %w", err)
	}

	versionClause := `application_version = $3`
	if isLatestVersion {
		versionClause = `(application_version = $3 OR application_version IS NULL)`
	}

	queryArgs := []any{input.queue.Name, WorkflowStatusEnqueued, input.applicationVersion}
	query := s.renderSQL(`
			SELECT workflow_uuid
			FROM %sworkflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND `+versionClause, schemaPrefix)

	if len(input.queuePartitionKey) > 0 {
		query += ` AND queue_partition_key = $4`
		queryArgs = append(queryArgs, input.queuePartitionKey)
	}

	query += ` ORDER BY priority ASC, created_at ASC`

	// Use SKIP LOCKED when no global concurrency is set to avoid blocking,
	// otherwise use NOWAIT to ensure consistent view across processes
	if input.queue.GlobalConcurrency == nil {
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
		s.logger.Debug("attempting to dequeue task(s)", "queue_name", input.queue.Name, "num_tasks", len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	updateQuery := s.renderSQL(`
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

	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIDs {
		if input.queue.RateLimit != nil {
			if len(retWorkflows)+numRecentQueries >= input.queue.RateLimit.Limit {
				break
			}
		}
		retWorkflow := dequeuedWorkflow{id: id}

		var serialization *string
		err := tx.QueryRow(ctx, updateQuery,
			WorkflowStatusPending,
			input.applicationVersion,
			input.executorID,
			time.Now().UnixMilli(),
			input.queue.RateLimit != nil,
			id,
			WorkflowStatusEnqueued).Scan(&retWorkflow.name, &retWorkflow.input, &serialization, &retWorkflow.configName)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			return nil, fmt.Errorf("failed to update workflow %s during dequeue: %w", id, err)
		}
		if serialization != nil {
			retWorkflow.serialization = *serialization
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

func (s *sysDB) clearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	query := s.renderSQL(`UPDATE %sworkflow_status
			  SET status = $1, started_at_epoch_ms = NULL
			  WHERE workflow_uuid = $2
			    AND queue_name IS NOT NULL
			    AND status = $3`, s.dialect.SchemaPrefix(s.schema))

	commandTag, err := s.pool.Exec(ctx, query,
		WorkflowStatusEnqueued,
		workflowID,
		WorkflowStatusPending)

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

// getQueuePartitions returns all unique partition keys for enqueued workflows in a queue.
func (s *sysDB) getQueuePartitions(ctx context.Context, queueName string) ([]string, error) {
	query := s.renderSQL(`
		SELECT DISTINCT queue_partition_key
		FROM %sworkflow_status
		WHERE queue_name = $1
		  AND status = $2
		  AND queue_partition_key IS NOT NULL`, s.dialect.SchemaPrefix(s.schema))

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
/******* QUEUE REGISTRY ********/
/*******************************/

const _QUEUE_SELECT_COLUMNS = "name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec, priority_enabled, partition_queue, polling_interval_sec"

type upsertQueueDBInput struct {
	queue          WorkflowQueue
	updateExisting bool
}

// scanQueueRow builds a database-backed WorkflowQueue from a row selecting
// _QUEUE_SELECT_COLUMNS, in order.
func scanQueueRow(row Row) (*WorkflowQueue, error) {
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
	q := &WorkflowQueue{
		Name:                 name,
		GlobalConcurrency:    concurrency,
		WorkerConcurrency:    workerConcurrency,
		PriorityEnabled:      priorityEnabled,
		PartitionQueue:       partitionQueue,
		MaxTasksPerIteration: _DEFAULT_MAX_TASKS_PER_ITERATION, // not persisted; queue table has no such column
		databaseBacked:       true,
	}
	if rateLimitMax != nil {
		var period time.Duration
		if rateLimitPeriodSec != nil {
			period = time.Duration(*rateLimitPeriodSec * float64(time.Second))
		}
		q.RateLimit = &RateLimiter{Limit: *rateLimitMax, Period: period}
	}
	base := time.Duration(pollingIntervalSec * float64(time.Second))
	if base <= 0 {
		base = _DEFAULT_BASE_POLLING_INTERVAL
	}
	q.basePollingInterval = base
	return q, nil
}

// getQueue returns the database-backed queue with the given name, or nil (with a
// nil error) when no such queue exists.
func (s *sysDB) getQueue(ctx context.Context, name string) (*WorkflowQueue, error) {
	return s.getQueueRow(ctx, s.pool, name)
}

func (s *sysDB) getQueueRow(ctx context.Context, db Querier, name string) (*WorkflowQueue, error) {
	query := s.renderSQL(`SELECT `+_QUEUE_SELECT_COLUMNS+` FROM %squeues WHERE name = $1`, s.dialect.SchemaPrefix(s.schema))
	q, err := scanQueueRow(db.QueryRow(ctx, s.dialect.RewriteQuery(query), name))
	if err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get queue %s: %w", name, err)
	}
	return q, nil
}

// listQueues returns all database-backed queues registered in the queues table.
func (s *sysDB) listQueues(ctx context.Context) ([]WorkflowQueue, error) {
	query := s.renderSQL(`SELECT `+_QUEUE_SELECT_COLUMNS+` FROM %squeues`, s.dialect.SchemaPrefix(s.schema))
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list queues: %w", err)
	}
	defer rows.Close()

	var queues []WorkflowQueue
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

// deleteQueue removes a database-backed queue's row, if it exists. Workflows
// still enqueued on it become unrecoverable.
func (s *sysDB) deleteQueue(ctx context.Context, name string) error {
	query := s.renderSQL(`DELETE FROM %squeues WHERE name = $1`, s.dialect.SchemaPrefix(s.schema))
	if _, err := s.pool.Exec(ctx, s.dialect.RewriteQuery(query), name); err != nil {
		return fmt.Errorf("failed to delete queue %s: %w", name, err)
	}
	return nil
}

// upsertQueue inserts a queue row or, when updateExisting is set, overwrites the
// existing configuration. It returns true iff a new row was inserted.
func (s *sysDB) upsertQueue(ctx context.Context, input upsertQueueDBInput) (bool, error) {
	q := input.queue
	var rateLimitMax *int
	var rateLimitPeriodSec *float64
	if q.RateLimit != nil {
		rateLimitMax = &q.RateLimit.Limit
		sec := q.RateLimit.Period.Seconds()
		rateLimitPeriodSec = &sec
	}
	pollingSec := q.basePollingInterval.Seconds()
	if pollingSec <= 0 {
		pollingSec = _DEFAULT_BASE_POLLING_INTERVAL.Seconds()
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
	insertQuery := s.renderSQL(`INSERT INTO %squeues
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

	if !inserted && input.updateExisting {
		if err := s.updateQueueRow(ctx, tx, q); err != nil {
			return false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}
	return inserted, nil
}

func (s *sysDB) updateQueueQuery(schemaPrefix string) string {
	return s.renderSQL(`UPDATE %squeues SET
		concurrency = $2, worker_concurrency = $3, rate_limit_max = $4, rate_limit_period_sec = $5,
		priority_enabled = $6, partition_queue = $7, polling_interval_sec = $8, updated_at = $9
		WHERE name = $1`, schemaPrefix)
}

// updateQueueRow overwrites the configuration columns of an existing queue row
// using the given Querier (a pool or a transaction). It returns an error if no
// row with the queue's name exists.
func (s *sysDB) updateQueueRow(ctx context.Context, db Querier, q WorkflowQueue) error {
	var rateLimitMax *int
	var rateLimitPeriodSec *float64
	if q.RateLimit != nil {
		rateLimitMax = &q.RateLimit.Limit
		sec := q.RateLimit.Period.Seconds()
		rateLimitPeriodSec = &sec
	}
	pollingSec := q.basePollingInterval.Seconds()
	if pollingSec <= 0 {
		pollingSec = _DEFAULT_BASE_POLLING_INTERVAL.Seconds()
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

// updateQueueConfig applies a single configuration change to a database-backed
// queue within one transaction: it reads the current row, passes it to mutate
// (which applies and validates the change against the freshly-read values),
// persists the row, and returns the updated queue. Run with snapshot isolation.
func (s *sysDB) updateQueueConfig(ctx context.Context, name string, mutate func(*WorkflowQueue) error) (*WorkflowQueue, error) {
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
	workflowQuery := s.renderSQL(`
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
	stepQuery := s.renderSQL(`
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
/******* SCHEDULES ********/
/*******************************/

type createScheduleDBInput struct {
	ScheduleID        string
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           string // JSON serialized
	Status            ScheduleStatus
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
	tx                Tx // optional: run inside an existing transaction
}

func (s *sysDB) createSchedule(ctx context.Context, input createScheduleDBInput) error {
	query := s.renderSQL(`
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
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, args...)
	} else {
		_, err = s.pool.Exec(ctx, query, args...)
	}
	if err != nil {
		return fmt.Errorf("failed to create schedule: %w", err)
	}
	return nil
}

type listSchedulesDBInput struct {
	Statuses             []ScheduleStatus
	WorkflowNames        []string
	ScheduleNamePrefixes []string
	tx                   Tx // optional: run inside an existing transaction
}

func (s *sysDB) listSchedules(ctx context.Context, input listSchedulesDBInput) ([]WorkflowSchedule, error) {
	query := s.renderSQL(`
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
	if input.tx != nil {
		rows, err = input.tx.Query(ctx, query, args...)
	} else {
		rows, err = s.pool.Query(ctx, query, args...)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to list schedules: %w", err)
	}
	defer rows.Close()

	var schedules []WorkflowSchedule
	for rows.Next() {
		var schedule WorkflowSchedule
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
			schedule.QueueName = _DBOS_INTERNAL_QUEUE_NAME
		}
		if workflowClassName != nil {
			schedule.WorkflowClassName = *workflowClassName
		}

		if lastFiredAtStr != nil {
			t, err := time.Parse(time.RFC3339Nano, *lastFiredAtStr)
			if err == nil {
				schedule.LastFiredAt = &t
			} else {
				t, err = time.Parse(time.RFC3339, *lastFiredAtStr)
				if err == nil {
					schedule.LastFiredAt = &t
				}
			}
		}
		if err := json.Unmarshal([]byte(contextJSON), &schedule.Context); err != nil {
			schedule.Context = contextJSON
		}

		schedules = append(schedules, schedule)
	}

	return schedules, nil
}

type updateScheduleDBInput struct {
	ScheduleName string
	Status       ScheduleStatus
	LastFiredAt  *time.Time
	tx           Tx // optional: run inside an existing transaction
}

func (s *sysDB) updateSchedule(ctx context.Context, input updateScheduleDBInput) error {
	query := s.renderSQL(`
		UPDATE %sworkflow_schedules
		SET status = $1, last_fired_at = $2
		WHERE schedule_name = $3
	`, s.dialect.SchemaPrefix(s.schema))

	var lastFiredAtVal any
	if input.LastFiredAt != nil {
		lastFiredAtVal = input.LastFiredAt.Format(time.RFC3339Nano)
	}

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.Status, lastFiredAtVal, input.ScheduleName)
	} else {
		_, err = s.pool.Exec(ctx, query, input.Status, lastFiredAtVal, input.ScheduleName)
	}
	if err != nil {
		return fmt.Errorf("failed to update schedule: %w", err)
	}
	return nil
}

func (s *sysDB) updateScheduleLastFiredAt(ctx context.Context, scheduleName string, lastFiredAt time.Time) error {
	query := s.renderSQL(`
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

type deleteScheduleDBInput struct {
	ScheduleName string
	tx           Tx // optional: run inside an existing transaction
}

func (s *sysDB) deleteSchedule(ctx context.Context, input deleteScheduleDBInput) error {
	query := s.renderSQL(`DELETE FROM %sworkflow_schedules WHERE schedule_name = $1`, s.dialect.SchemaPrefix(s.schema))

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.ScheduleName)
	} else {
		_, err = s.pool.Exec(ctx, query, input.ScheduleName)
	}
	if err != nil {
		return fmt.Errorf("failed to delete schedule: %w", err)
	}
	return nil
}

type backfillScheduleDBInput struct {
	ScheduleName string
	Schedule     string
	StartTime    time.Time
	EndTime      time.Time
}

func (s *sysDB) backfillSchedule(ctx context.Context, input backfillScheduleDBInput) ([]string, error) {
	schedules, err := s.listSchedules(ctx, listSchedulesDBInput{ScheduleNamePrefixes: []string{input.ScheduleName}})
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}
	var schedule *WorkflowSchedule
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

	scheduleEntry, err := newScheduleCronParser().Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cron schedule: %w", err)
	}

	queueName := _DBOS_INTERNAL_QUEUE_NAME
	if schedule.QueueName != "" {
		queueName = schedule.QueueName
	}

	ser := resolveEncoder(ctx)

	// Backfilled workflows always run against the latest registered application
	// version. If lookup fails (e.g. no versions registered yet) leave it unset.
	var backfillAppVersion string
	backfillLatest, err := retryWithResult(ctx, func() (*VersionInfo, error) {
		return s.getLatestApplicationVersion(ctx, nil)
	}, withRetrierLogger(s.logger))
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

	checkQuery := s.renderSQL(`SELECT 1 FROM %sworkflow_status WHERE workflow_uuid = $1 LIMIT 1`, s.dialect.SchemaPrefix(s.schema))

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

		encodedInput, encErr := ser.Encode(ScheduledWorkflowInput{
			ScheduledTime: nextTime,
			Context:       schedule.Context,
		})
		if encErr != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input for %s: %w", workflowID, encErr)
		}

		status := WorkflowStatus{
			ID:                 workflowID,
			Status:             WorkflowStatusEnqueued,
			Name:               schedule.WorkflowName,
			ClassName:          schedule.WorkflowClassName,
			QueueName:          queueName,
			CreatedAt:          now,
			Input:              encodedInput,
			Serialization:      ser.Name(),
			ApplicationVersion: backfillAppVersion,
		}
		if _, err := s.insertWorkflowStatus(ctx, insertWorkflowStatusDBInput{status: status, tx: tx}); err != nil {
			return nil, fmt.Errorf("failed to enqueue backfill workflow %s: %w", workflowID, err)
		}

		nextTime = scheduleEntry.Next(nextTime)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit backfill transaction: %w", err)
	}
	return workflowIDs, nil
}

// triggerSchedule immediately enqueues the named schedule's workflow at the
// current time, using the schedule's queue (or the internal queue by default)
// and preserving its workflow_class_name and context. Returns the workflow ID.
func (s *sysDB) triggerSchedule(ctx context.Context, scheduleName string) (string, error) {
	if scheduleName == "" {
		return "", errors.New("schedule_name is required")
	}

	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	schedules, err := s.listSchedules(ctx, listSchedulesDBInput{
		ScheduleNamePrefixes: []string{scheduleName},
		tx:                   tx,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get schedule: %w", err)
	}
	var schedule *WorkflowSchedule
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
		queueName = _DBOS_INTERNAL_QUEUE_NAME
	}

	now := time.Now()
	workflowID := fmt.Sprintf("sched-%s-trigger-%s", scheduleName, now.Format(time.RFC3339Nano))

	ser := resolveEncoder(ctx)
	encodedInput, err := ser.Encode(ScheduledWorkflowInput{
		ScheduledTime: now,
		Context:       schedule.Context,
	})
	if err != nil {
		return "", fmt.Errorf("failed to encode scheduled workflow input: %w", err)
	}

	// Triggered scheduled workflows run against the latest registered application
	// version. If lookup fails (e.g. no versions registered yet) leave it unset.
	var triggerAppVersion string
	triggerLatest, err := retryWithResult(ctx, func() (*VersionInfo, error) {
		return s.getLatestApplicationVersion(ctx, nil)
	}, withRetrierLogger(s.logger))
	if err != nil {
		s.logger.Error("failed to fetch latest application version for schedule trigger", "schedule", scheduleName, "error", err)
	} else if triggerLatest != nil {
		triggerAppVersion = triggerLatest.Name
	}

	status := WorkflowStatus{
		ID:                 workflowID,
		Status:             WorkflowStatusEnqueued,
		Name:               schedule.WorkflowName,
		ClassName:          schedule.WorkflowClassName,
		QueueName:          queueName,
		CreatedAt:          now,
		Input:              encodedInput,
		Serialization:      ser.Name(),
		ApplicationVersion: triggerAppVersion,
	}

	if _, err := s.insertWorkflowStatus(ctx, insertWorkflowStatusDBInput{status: status, tx: tx}); err != nil {
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

func (s *sysDB) createApplicationVersion(ctx context.Context, versionName string) error {
	query := s.renderSQL(`
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

func (s *sysDB) updateApplicationVersionTimestamp(ctx context.Context, versionName string, newTimestamp int64) error {
	query := s.renderSQL(`
		UPDATE %sapplication_versions
		SET version_timestamp = $1
		WHERE version_name = $2
	`, s.dialect.SchemaPrefix(s.schema))
	if _, err := s.pool.Exec(ctx, query, newTimestamp, versionName); err != nil {
		return fmt.Errorf("failed to update application version timestamp: %w", err)
	}
	return nil
}

func (s *sysDB) listApplicationVersions(ctx context.Context) ([]VersionInfo, error) {
	query := s.renderSQL(`
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

func (s *sysDB) getLatestApplicationVersion(ctx context.Context, tx Tx) (*VersionInfo, error) {
	query := s.renderSQL(`
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
			return nil, newNoApplicationVersionsError()
		}
		return nil, fmt.Errorf("failed to get latest application version: %w", err)
	}
	return &v, nil
}

/*******************************/
/******* UTILS ********/
/*******************************/

func isCockroachDB(conn *pgx.Conn) bool {
	return conn.PgConn().ParameterStatus("crdb_version") != ""
}

// dropDatabaseIfExists drops a database in a way that works with both PostgreSQL and CockroachDB.
// For CockroachDB, it terminates active connections first, then drops the database.
// For PostgreSQL, it uses the WITH (FORCE) syntax.
func dropDatabaseIfExists(ctx context.Context, conn *pgx.Conn, dbName string) error {
	crdb := isCockroachDB(conn)

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

// retry executes a function with retry logic using functional optionsr
func retry(ctx context.Context, fn func() error, options ...retryOption) error {
	config := &retryConfig{
		maxRetries:    -1,
		baseDelay:     100 * time.Millisecond,
		maxDelay:      30 * time.Second,
		backoffFactor: 2.0,
		jitterMin:     0.95,
		jitterMax:     1.05,
		retryConditionChain: []func(error, *slog.Logger) bool{
			postgresDialect{}.IsRetryable,
			sqliteDialect{}.IsRetryable,
		},
	}

	// Apply options
	for _, opt := range options {
		opt(config)
	}

	sched := backoffSchedule{
		base:      config.baseDelay,
		max:       config.maxDelay,
		factor:    config.backoffFactor,
		jitterMin: config.jitterMin,
		jitterMax: config.jitterMax,
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

	return retryLoop(ctx, sched, fn, decide, onRetry, onCancel)
}

// retryWithResult executes a function that returns a value with retry logic
// It uses the non-generic retry function under the hood
func retryWithResult[T any](ctx context.Context, fn func() (T, error), options ...retryOption) (T, error) {
	var result T

	wrappedFn := func() error {
		var err error
		result, err = fn()
		return err
	}

	// Return retry's error directly: it is the final fn() error, or ctx.Err()
	// when the context is cancelled during a backoff wait.
	return result, retry(ctx, wrappedFn, options...)
}

func (s *sysDB) exportWorkflow(ctx context.Context, workflowID string, exportChildren bool) ([]ExportedWorkflow, error) {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction for exportWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	workflowIDs := []string{workflowID}
	if exportChildren {
		children, err := s.getWorkflowChildren(ctx, getWorkflowChildrenDBInput{
			workflowID: workflowID,
			tx:         tx,
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
		statusQuery := s.renderSQL(`SELECT
				workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
				output, error, executor_id, created_at, updated_at, application_version, application_id,
				class_name, config_name, recovery_attempts, queue_name, workflow_timeout_ms,
				workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs, priority,
				queue_partition_key, forked_from, parent_workflow_id, delay_until_epoch_ms, serialization
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
		)
		err := row.Scan(
			&wfUUID, &status, &name, &authUser, &assumedRole, &authRoles,
			&output, &errStr, &executorID, &createdAt, &updatedAt, &appVersion, &appID,
			&className, &configName, &recoveryAttempts, &queueName, &workflowTimeoutMs,
			&workflowDeadlineEpochMs, &startedAtEpochMs, &dedupID, &inputs, &priority,
			&queuePartitionKey, &forkedFrom, &parentWorkflowID, &delayUntilEpochMs, &serialization,
		)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, newNonExistentWorkflowError(wfID)
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
		}

		// Export operation_outputs
		outputsQuery := s.renderSQL(`SELECT workflow_uuid, function_id, function_name, output, error,
				child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
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
			if err := outputRows.Scan(&opWfUUID, &opFuncID, &opFuncName, &opOutput, &opError, &opChildWfID, &opStartedAt, &opCompletedAt); err != nil {
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
			})
		}
		if cerr := outputRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close operation_outputs rows for %s: %w", wfID, cerr)
		}
		if err := outputRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating operation_outputs for %s: %w", wfID, err)
		}

		// Export workflow_events
		eventsQuery := s.renderSQL(`SELECT workflow_uuid, key, value
			FROM %sworkflow_events WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		eventRows, err := tx.Query(ctx, eventsQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events for %s: %w", wfID, err)
		}
		var workflowEvents []map[string]any
		for eventRows.Next() {
			var evWfUUID, evKey, evValue *string
			if err := eventRows.Scan(&evWfUUID, &evKey, &evValue); err != nil {
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
			})
		}
		if cerr := eventRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close workflow_events rows for %s: %w", wfID, cerr)
		}
		if err := eventRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating workflow_events for %s: %w", wfID, err)
		}

		// Export workflow_events_history
		historyQuery := s.renderSQL(`SELECT workflow_uuid, function_id, key, value
			FROM %sworkflow_events_history WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		historyRows, err := tx.Query(ctx, historyQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export workflow_events_history for %s: %w", wfID, err)
		}
		var workflowEventsHistory []map[string]any
		for historyRows.Next() {
			var hWfUUID, hKey, hValue *string
			var hFuncID *int
			if err := historyRows.Scan(&hWfUUID, &hFuncID, &hKey, &hValue); err != nil {
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
			})
		}
		if cerr := historyRows.Close(); cerr != nil {
			return nil, fmt.Errorf("failed to close workflow_events_history rows for %s: %w", wfID, cerr)
		}
		if err := historyRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating workflow_events_history for %s: %w", wfID, err)
		}

		// Export streams
		streamsQuery := s.renderSQL(`SELECT workflow_uuid, key, value, "offset", function_id
			FROM %sstreams WHERE workflow_uuid = $1`, s.dialect.SchemaPrefix(s.schema))

		streamRows, err := tx.Query(ctx, streamsQuery, wfID)
		if err != nil {
			return nil, fmt.Errorf("failed to export streams for %s: %w", wfID, err)
		}
		var streams []map[string]any
		for streamRows.Next() {
			var sWfUUID, sKey, sValue *string
			var sOffset, sFuncID *int
			if err := streamRows.Scan(&sWfUUID, &sKey, &sValue, &sOffset, &sFuncID); err != nil {
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

func (s *sysDB) importWorkflow(ctx context.Context, workflows []ExportedWorkflow) error {
	tx, err := s.pool.BeginTx(ctx, TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction for importWorkflow: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, wf := range workflows {
		status := wf.WorkflowStatus

		// Import workflow_status
		insertStatusQuery := s.renderSQL(`INSERT INTO %sworkflow_status (
				workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
				output, error, executor_id, created_at, updated_at, application_version, application_id,
				class_name, config_name, recovery_attempts, queue_name, workflow_timeout_ms,
				workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs, priority,
				queue_partition_key, forked_from, parent_workflow_id, delay_until_epoch_ms, serialization
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)`,
			s.dialect.SchemaPrefix(s.schema))

		_, err := tx.Exec(ctx, insertStatusQuery,
			status["workflow_uuid"], status["status"], status["name"],
			status["authenticated_user"], status["assumed_role"], status["authenticated_roles"],
			status["output"], status["error"], status["executor_id"],
			status["created_at"], status["updated_at"], status["application_version"], status["application_id"],
			status["class_name"], status["config_name"], status["recovery_attempts"], status["queue_name"],
			status["workflow_timeout_ms"], status["workflow_deadline_epoch_ms"], status["started_at_epoch_ms"],
			status["deduplication_id"], status["inputs"], status["priority"],
			status["queue_partition_key"], status["forked_from"], status["parent_workflow_id"],
			status["delay_until_epoch_ms"], status["serialization"],
		)
		if err != nil {
			return fmt.Errorf("failed to import workflow_status: %w", err)
		}

		// Import operation_outputs
		for _, op := range wf.OperationOutputs {
			insertOpQuery := s.renderSQL(`INSERT INTO %soperation_outputs (
					workflow_uuid, function_id, function_name, output, error,
					child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertOpQuery,
				op["workflow_uuid"], op["function_id"], op["function_name"],
				op["output"], op["error"], op["child_workflow_id"],
				op["started_at_epoch_ms"], op["completed_at_epoch_ms"],
			)
			if err != nil {
				return fmt.Errorf("failed to import operation_outputs: %w", err)
			}
		}

		// Import workflow_events
		for _, ev := range wf.WorkflowEvents {
			insertEvQuery := s.renderSQL(`INSERT INTO %sworkflow_events (
					workflow_uuid, key, value
				) VALUES ($1, $2, $3)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertEvQuery,
				ev["workflow_uuid"], ev["key"], ev["value"],
			)
			if err != nil {
				return fmt.Errorf("failed to import workflow_events: %w", err)
			}
		}

		// Import workflow_events_history
		for _, h := range wf.WorkflowEventsHistory {
			insertHistQuery := s.renderSQL(`INSERT INTO %sworkflow_events_history (
					workflow_uuid, function_id, key, value
				) VALUES ($1, $2, $3, $4)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertHistQuery,
				h["workflow_uuid"], h["function_id"], h["key"], h["value"],
			)
			if err != nil {
				return fmt.Errorf("failed to import workflow_events_history: %w", err)
			}
		}

		// Import streams
		for _, st := range wf.Streams {
			insertStreamQuery := s.renderSQL(`INSERT INTO %sstreams (
					workflow_uuid, key, value, "offset", function_id
				) VALUES ($1, $2, $3, $4, $5)`,
				s.dialect.SchemaPrefix(s.schema))

			_, err := tx.Exec(ctx, insertStreamQuery,
				st["workflow_uuid"], st["key"], st["value"], st["offset"], st["function_id"],
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

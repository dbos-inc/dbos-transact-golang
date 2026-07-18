package dbos

import (
	"database/sql"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// This file re-exports types defined in internal packages as part of the
// public dbos API. Aliases are identical types to the compiler; no conversion
// is ever needed.

type (
	// WorkflowStatus is a snapshot of a workflow execution: identity, current
	// state, timing, input/output, and queue placement. Returned by
	// ListWorkflows and by WorkflowHandle.GetStatus.
	WorkflowStatus = models.WorkflowStatus

	// WorkflowStatusType represents the current execution state of a workflow.
	WorkflowStatusType = models.WorkflowStatusType

	// WorkflowSchedule describes a database-backed schedule: its cron
	// expression, target workflow, activation status, and the user-defined
	// context attached to it. Returned by GetSchedule and ListSchedules.
	WorkflowSchedule = models.WorkflowSchedule

	// ScheduleStatus is the activation state of a schedule.
	ScheduleStatus = models.ScheduleStatus

	// ScheduledWorkflowInput is the input type that DB-backed scheduled
	// workflow functions must accept. ScheduledTime is the cron tick time;
	// Context carries the user-defined value attached to the schedule as raw
	// JSON (nil if none) — decode it with DecodeScheduleContext.
	ScheduledWorkflowInput = models.ScheduledWorkflowInput

	// StepInfo describes one recorded step of a workflow execution, as
	// returned by GetWorkflowSteps.
	StepInfo = models.StepInfo

	// RateLimiter configures rate limiting for workflow queue execution: at
	// most Limit workflows start within each Period.
	RateLimiter = models.RateLimiter
)

const (
	WorkflowStatusPending                     = models.WorkflowStatusPending                     // Workflow is running or ready to run
	WorkflowStatusEnqueued                    = models.WorkflowStatusEnqueued                    // Workflow is queued and waiting for execution
	WorkflowStatusDelayed                     = models.WorkflowStatusDelayed                     // Workflow is delayed and will transition to ENQUEUED after the delay expires
	WorkflowStatusSuccess                     = models.WorkflowStatusSuccess                     // Workflow completed successfully
	WorkflowStatusError                       = models.WorkflowStatusError                       // Workflow completed with an error
	WorkflowStatusCancelled                   = models.WorkflowStatusCancelled                   // Workflow was cancelled (manually or due to timeout)
	WorkflowStatusMaxRecoveryAttemptsExceeded = models.WorkflowStatusMaxRecoveryAttemptsExceeded // Workflow exceeded maximum recovery attempts

	ScheduleStatusActive = models.ScheduleStatusActive // Schedule is active and fires on its cron expression
	ScheduleStatusPaused = models.ScheduleStatusPaused // Schedule is paused and does not fire
)

type (
	// Error is the unified error type for all DBOS operations. It provides
	// structured error information with context-specific fields and error
	// codes for programmatic handling.
	Error = models.Error

	// ErrorCode classifies a DBOS Error for programmatic handling.
	ErrorCode = models.ErrorCode
)

const (
	ErrorCodeConflictingID            = models.ErrorCodeConflictingID            // Workflow ID conflicts or duplicate operations
	ErrorCodeInitialization           = models.ErrorCodeInitialization           // DBOS context initialization failures
	ErrorCodeNonExistentWorkflow      = models.ErrorCodeNonExistentWorkflow      // Referenced workflow does not exist
	ErrorCodeUnexpectedWorkflow       = models.ErrorCodeUnexpectedWorkflow       // Workflow ID previously used by a different workflow function or queue (non-deterministic reuse)
	ErrorCodeWorkflowCancelled        = models.ErrorCodeWorkflowCancelled        // Workflow was cancelled during execution
	ErrorCodeUnexpectedStep           = models.ErrorCodeUnexpectedStep           // Step function mismatch during recovery (non-deterministic workflow)
	ErrorCodeAwaitedWorkflowCancelled = models.ErrorCodeAwaitedWorkflowCancelled // A workflow being awaited was cancelled
	ErrorCodeConflictingRegistration  = models.ErrorCodeConflictingRegistration  // Attempting to register a workflow/queue that already exists
	ErrorCodeWorkflowUnexpectedType   = models.ErrorCodeWorkflowUnexpectedType   // Type mismatch in workflow input/output
	ErrorCodeWorkflowExecution        = models.ErrorCodeWorkflowExecution        // General workflow execution error
	ErrorCodeStepExecution            = models.ErrorCodeStepExecution            // General step execution error
	ErrorCodeDeadLetterQueue          = models.ErrorCodeDeadLetterQueue          // Workflow moved to dead letter queue after max retries
	ErrorCodeMaxStepRetriesExceeded   = models.ErrorCodeMaxStepRetriesExceeded   // Step exceeded maximum retry attempts
	ErrorCodeQueueDeduplicated        = models.ErrorCodeQueueDeduplicated        // Workflow was deduplicated in the queue
	ErrorCodePatchingNotEnabled       = models.ErrorCodePatchingNotEnabled       // Patching system is not enabled in the DBOS context configuration
	ErrorCodeTimeout                  = models.ErrorCodeTimeout                  // Operation timed out (e.g., recv timeout)
	ErrorCodeNoApplicationVersions    = models.ErrorCodeNoApplicationVersions    // No application versions are registered in the system database
	ErrorCodeQueueNotFound            = models.ErrorCodeQueueNotFound            // Referenced queue does not exist
	ErrorCodeScheduleNotFound         = models.ErrorCodeScheduleNotFound         // Referenced schedule does not exist
	ErrorCodeInvalidOption            = models.ErrorCodeInvalidOption            // Invalid or inconsistent options passed to a DBOS API
)

// Driver-facing SQL surface: custom database drivers implement Pool and
// Dialect; the remaining types are what those implementations return.
type (
	// Querier is the subset of SQL operations available on both a pool and a
	// transaction.
	Querier = sysdb.Querier

	// Tx is a transaction that can also execute queries.
	Tx = sysdb.Tx

	// Pool is a connection pool that can begin transactions.
	Pool = sysdb.Pool

	// TxOptions configures a new transaction.
	TxOptions = sysdb.TxOptions

	// IsoLevel is a portable isolation level. Dialects map it to their native
	// enum.
	IsoLevel = sysdb.IsoLevel

	// Result describes the effects of a write, mirroring database/sql.Result.
	Result = sysdb.Result

	// Rows is a cursor over a multi-row result set.
	Rows = sysdb.Rows

	// Row is a single-row result that has not yet been scanned. Scan returns
	// ErrNoRows if no row matched.
	Row = sysdb.Row

	// Dialect encapsulates per-backend SQL fragments and behaviours.
	Dialect = sysdb.Dialect

	// DialectName identifies the database backend. Stable string suitable for
	// logging.
	DialectName = sysdb.DialectName
)

const (
	IsoLevelDefault        = sysdb.IsoLevelDefault
	IsoLevelReadCommitted  = sysdb.IsoLevelReadCommitted
	IsoLevelRepeatableRead = sysdb.IsoLevelRepeatableRead
	IsoLevelSerializable   = sysdb.IsoLevelSerializable

	DialectPostgres  = sysdb.DialectPostgres
	DialectCockroach = sysdb.DialectCockroach
	DialectSQLite    = sysdb.DialectSQLite
)

// PgxPool unwraps a Pool backed by pgx, returning nil for other backends.
func PgxPool(p Pool) *pgxpool.Pool { return sysdb.PgxPool(p) }

// SQLDB unwraps a Pool backed by database/sql, returning nil for other backends.
func SQLDB(p Pool) *sql.DB { return sysdb.SQLDB(p) }

type (
	// ExportedWorkflow contains all data for a single workflow, in a portable
	// format suitable for exporting from one environment and importing into
	// another.
	ExportedWorkflow = sysdb.ExportedWorkflow

	// VersionInfo describes a registered application version.
	VersionInfo = sysdb.VersionInfo

	// WorkflowAggregateRow is a single row of GetWorkflowAggregates output.
	// Group maps each grouping column name (e.g. "status", "name",
	// "time_bucket") to its stringified value, with nil entries for grouping
	// columns that were NULL for that row. Unselected aggregates are nil
	// (serialized as null). MinCreatedAt is an epoch-ms timestamp; the latency
	// fields are in milliseconds.
	WorkflowAggregateRow = sysdb.WorkflowAggregateRow

	// StepAggregateRow is a single row of GetStepAggregates output. Group maps
	// each grouping column name (e.g. "function_name", "status",
	// "time_bucket") to its stringified value, with nil entries for grouping
	// columns that were NULL for that row. Unselected aggregates are nil
	// (serialized as null).
	StepAggregateRow = sysdb.StepAggregateRow
)

// ErrNoRows is returned by Row.Scan when no row matched.
var ErrNoRows = sysdb.ErrNoRows

type (
	// ListWorkflowsOption is a functional option for ListWorkflows.
	ListWorkflowsOption = models.ListWorkflowsOption

	// ListSchedulesOption is a functional option for ListSchedules.
	ListSchedulesOption = models.ListSchedulesOption

	// GetWorkflowStepsOption is a functional option for GetWorkflowSteps.
	GetWorkflowStepsOption = models.GetWorkflowStepsOption

	// ResumeWorkflowOption is a functional option for ResumeWorkflow.
	ResumeWorkflowOption = models.ResumeWorkflowOption

	// CancelWorkflowOption is a functional option for CancelWorkflow.
	CancelWorkflowOption = models.CancelWorkflowOption

	// ForkWorkflowInput is the input to ForkWorkflow. OriginalWorkflowID is
	// required; other fields are optional.
	ForkWorkflowInput = models.ForkWorkflowInput

	// GetWorkflowAggregatesInput is the input to GetWorkflowAggregates. At
	// least one GroupBy* flag must be true or TimeBucketSize must be > 0, and
	// at least one Select* flag must be true.
	GetWorkflowAggregatesInput = models.GetWorkflowAggregatesInput

	// GetStepAggregatesInput is the input to GetStepAggregates. At least one
	// GroupBy* flag must be true or TimeBucketSize must be > 0, and at least
	// one Select* flag must be true.
	GetStepAggregatesInput = models.GetStepAggregatesInput
)

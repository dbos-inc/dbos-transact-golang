package dbos

import (
	"database/sql"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// This file re-exports types that moved to internal packages so the public
// API of the dbos package is unchanged. Aliases are identical types to the
// compiler; no conversion is ever needed.

// Workflow status and schedule types (internal/models).
type (
	WorkflowStatus         = models.WorkflowStatus
	WorkflowStatusType     = models.WorkflowStatusType
	WorkflowSchedule       = models.WorkflowSchedule
	ScheduleStatus         = models.ScheduleStatus
	ScheduledWorkflowInput = models.ScheduledWorkflowInput
	StepInfo               = models.StepInfo
	RateLimiter            = models.RateLimiter
)

const (
	WorkflowStatusPending                     = models.WorkflowStatusPending
	WorkflowStatusEnqueued                    = models.WorkflowStatusEnqueued
	WorkflowStatusDelayed                     = models.WorkflowStatusDelayed
	WorkflowStatusSuccess                     = models.WorkflowStatusSuccess
	WorkflowStatusError                       = models.WorkflowStatusError
	WorkflowStatusCancelled                   = models.WorkflowStatusCancelled
	WorkflowStatusMaxRecoveryAttemptsExceeded = models.WorkflowStatusMaxRecoveryAttemptsExceeded

	ScheduleStatusActive = models.ScheduleStatusActive
	ScheduleStatusPaused = models.ScheduleStatusPaused
)

// Error types (internal/models).
type (
	DBOSError     = models.DBOSError
	DBOSErrorCode = models.DBOSErrorCode
)

const (
	ConflictingIDError           = models.ConflictingIDError
	InitializationError          = models.InitializationError
	NonExistentWorkflowError     = models.NonExistentWorkflowError
	ConflictingWorkflowError     = models.ConflictingWorkflowError
	WorkflowCancelled            = models.WorkflowCancelled
	UnexpectedStep               = models.UnexpectedStep
	AwaitedWorkflowCancelled     = models.AwaitedWorkflowCancelled
	ConflictingRegistrationError = models.ConflictingRegistrationError
	WorkflowUnexpectedTypeError  = models.WorkflowUnexpectedTypeError
	WorkflowExecutionError       = models.WorkflowExecutionError
	StepExecutionError           = models.StepExecutionError
	DeadLetterQueueError         = models.DeadLetterQueueError
	MaxStepRetriesExceeded       = models.MaxStepRetriesExceeded
	QueueDeduplicated            = models.QueueDeduplicated
	PatchingNotEnabled           = models.PatchingNotEnabled
	TimeoutError                 = models.TimeoutError
	NoApplicationVersions        = models.NoApplicationVersions
)

// Driver-facing SQL surface (internal/sysdb). Future database drivers
// implement Pool and Dialect; see dbq.go and dialect.go in internal/sysdb.
type (
	Querier   = sysdb.Querier
	Tx        = sysdb.Tx
	Pool      = sysdb.Pool
	TxOptions = sysdb.TxOptions
	IsoLevel  = sysdb.IsoLevel
	Result    = sysdb.Result
	Rows      = sysdb.Rows
	Row       = sysdb.Row

	Dialect     = sysdb.Dialect
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

// System-database row types (internal/sysdb).
type (
	ExportedWorkflow     = sysdb.ExportedWorkflow
	VersionInfo          = sysdb.VersionInfo
	WorkflowAggregateRow = sysdb.WorkflowAggregateRow
	StepAggregateRow     = sysdb.StepAggregateRow
)

// ErrNoRows is returned by Row.Scan when no row matched.
var ErrNoRows = sysdb.ErrNoRows

// Option and input types (internal/models).
type (
	ListWorkflowsOption        = models.ListWorkflowsOption
	ListSchedulesOption        = models.ListSchedulesOption
	GetWorkflowStepsOption     = models.GetWorkflowStepsOption
	ResumeWorkflowOption       = models.ResumeWorkflowOption
	CancelWorkflowOptions      = models.CancelWorkflowOptions
	ForkWorkflowInput          = models.ForkWorkflowInput
	GetWorkflowAggregatesInput = models.GetWorkflowAggregatesInput
	GetStepAggregatesInput     = models.GetStepAggregatesInput
)

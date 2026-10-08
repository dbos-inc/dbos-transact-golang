// Package internals is the contract between DBOS Transact and
// the commercial module. It exposes OSS internals to that module.
package internals

import (
	"context"
	"log/slog"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// Runtime is what the Context passed to a dbos.ConductorFactory also implements,
// beyond dbos.Context. OSS root context satisfies it.
type Runtime interface {
	dbos.Context

	SystemDB() SystemDatabase        // The system database, for queries beyond the public API
	Logger() *slog.Logger            // The context's logger
	AlertHandler() dbos.AlertHandler // The handler registered with SetAlertHandler, or nil
	DBOSVersion() string             // The dbos module version recorded in the binary's build info

	RecoverPendingWorkflows(executorIDs []string) ([]dbos.WorkflowHandle[any], error)
	DecodeStoredValue(value, serialization string) (any, error)
}

// RuntimeOf returns the Runtime "view" of a Context created by dbos.NewContext.
func RuntimeOf(ctx dbos.Context) (Runtime, error) {
	rt, ok := ctx.(Runtime)
	if !ok {
		return nil, &dbos.Error{
			Code:    dbos.ErrorCodeInitialization,
			Message: "the Context does not expose the DBOS runtime; it must come from dbos.NewContext",
		}
	}
	return rt, nil
}

// The system database.
type (
	SystemDatabase = sysdb.SystemDatabase // The interface
	Dialect        = sysdb.Dialect        // The SQL dialect of the system database

	ForkFromInput                = sysdb.ForkFromDBInput
	GarbageCollectWorkflowsInput = sysdb.GarbageCollectWorkflowsInput
	DeleteWorkflowsInput         = sysdb.DeleteWorkflowsDBInput
	BackfillScheduleInput        = sysdb.BackfillScheduleDBInput

	EventRecord        = sysdb.EventRecord
	NotificationRecord = sysdb.NotificationRecord
	StreamEntry        = sysdb.StreamEntry
)

// Retry are not part of the SystemDatabase interface today
type RetryOption = sysdb.RetryOption

func Retry(ctx context.Context, fn func() error, options ...RetryOption) error {
	return sysdb.Retry(ctx, fn, options...)
}

func RetryWithResult[T any](ctx context.Context, fn func() (T, error), options ...RetryOption) (T, error) {
	return sysdb.RetryWithResult(ctx, fn, options...)
}

func WithRetrierLogger(logger *slog.Logger) RetryOption {
	return sysdb.WithRetrierLogger(logger)
}

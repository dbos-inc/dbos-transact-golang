package dbos

// Sentinel errors for the most commonly handled DBOS error conditions.
// Match them with errors.Is:
//
//	if errors.Is(err, dbos.ErrWorkflowCancelled) { ... }
//
// Matching is by error code: (*Error).Is compares Code, so a sentinel matches
// any DBOS error carrying the same code regardless of its other fields.
var (
	// ErrWorkflowCancelled matches errors from workflows cancelled during execution.
	ErrWorkflowCancelled = &Error{Code: ErrorCodeWorkflowCancelled}
	// ErrAwaitedWorkflowCancelled matches errors returned when awaiting a workflow that was cancelled.
	ErrAwaitedWorkflowCancelled = &Error{Code: ErrorCodeAwaitedWorkflowCancelled}
	// ErrQueueDeduplicated matches errors from workflows deduplicated on enqueue.
	ErrQueueDeduplicated = &Error{Code: ErrorCodeQueueDeduplicated}
	// ErrNonExistentWorkflow matches errors referencing a workflow that does not exist.
	ErrNonExistentWorkflow = &Error{Code: ErrorCodeNonExistentWorkflow}
	// ErrConflictingWorkflowID matches errors from conflicting workflow IDs.
	ErrConflictingWorkflowID = &Error{Code: ErrorCodeConflictingID}
	// ErrUnexpectedWorkflow matches errors raised when a workflow ID is reused with a
	// different workflow function or a different queue, indicating non-determinism or
	// conflicting ID reuse. Match with errors.Is(err, dbos.ErrUnexpectedWorkflow).
	ErrUnexpectedWorkflow = &Error{Code: ErrorCodeUnexpectedWorkflow}
	// ErrMaxStepRetriesExceeded matches errors from steps that exhausted their retries.
	ErrMaxStepRetriesExceeded = &Error{Code: ErrorCodeMaxStepRetriesExceeded}
	// ErrDeadLetterQueue matches errors from workflows that exceeded their maximum
	// recovery attempts and were moved to the dead-letter queue.
	ErrDeadLetterQueue = &Error{Code: ErrorCodeDeadLetterQueue}
	// ErrTimeout matches DBOS timeout errors (e.g. Recv/GetEvent timeouts, GetResult
	// handle timeouts). A timeout error built from an expired context deadline also
	// wraps that cause, so errors.Is(err, context.DeadlineExceeded) matches it too,
	// including for errors read back from the database.
	ErrTimeout = &Error{Code: ErrorCodeTimeout}
	// ErrQueueNotFound matches errors referencing a queue that does not exist.
	ErrQueueNotFound = &Error{Code: ErrorCodeQueueNotFound}
	// ErrScheduleNotFound matches errors referencing a schedule that does not exist.
	ErrScheduleNotFound = &Error{Code: ErrorCodeScheduleNotFound}
	// ErrNoApplicationVersions matches errors from operations requiring a registered application version when none exists.
	ErrNoApplicationVersions = &Error{Code: ErrorCodeNoApplicationVersions}
	// ErrInvalidOption matches errors from invalid or inconsistent options passed to a DBOS API.
	ErrInvalidOption = &Error{Code: ErrorCodeInvalidOption}
)

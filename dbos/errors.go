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
	// ErrMaxStepRetriesExceeded matches errors from steps that exhausted their retries.
	ErrMaxStepRetriesExceeded = &Error{Code: ErrorCodeMaxStepRetriesExceeded}
	// ErrTimeout matches DBOS timeout errors (e.g. Recv/GetEvent timeouts, GetResult
	// handle timeouts). A timeout error built from an expired context deadline also
	// wraps that cause, so errors.Is(err, context.DeadlineExceeded) matches it too.
	ErrTimeout = &Error{Code: ErrorCodeTimeout}
)

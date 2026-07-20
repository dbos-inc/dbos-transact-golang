package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Docs for ErrorCode, Error, and the ErrorCode* constants live on their
// public aliases in dbos/aliases.go.

type ErrorCode int

const (
	ErrorCodeConflictingID ErrorCode = iota + 1
	ErrorCodeInitialization
	ErrorCodeNonExistentWorkflow
	ErrorCodeUnexpectedWorkflow
	ErrorCodeWorkflowCancelled
	ErrorCodeUnexpectedStep
	ErrorCodeAwaitedWorkflowCancelled
	ErrorCodeConflictingRegistration
	ErrorCodeWorkflowUnexpectedType
	ErrorCodeWorkflowExecution
	ErrorCodeStepExecution
	ErrorCodeDeadLetterQueue
	ErrorCodeMaxStepRetriesExceeded
	ErrorCodeQueueDeduplicated
	ErrorCodePatchingNotEnabled
	ErrorCodeTimeout
	ErrorCodeNoApplicationVersions
	ErrorCodeQueueNotFound
	ErrorCodeScheduleNotFound
	ErrorCodeInvalidOption

	// _errorCodeSentinel must remain the last value: it bounds parseErrorCode.
	_errorCodeSentinel
)

// String returns the name of the error code, e.g. "NonExistentWorkflow".
func (c ErrorCode) String() string {
	switch c {
	case ErrorCodeConflictingID:
		return "ConflictingID"
	case ErrorCodeInitialization:
		return "Initialization"
	case ErrorCodeNonExistentWorkflow:
		return "NonExistentWorkflow"
	case ErrorCodeUnexpectedWorkflow:
		return "UnexpectedWorkflow"
	case ErrorCodeWorkflowCancelled:
		return "WorkflowCancelled"
	case ErrorCodeUnexpectedStep:
		return "UnexpectedStep"
	case ErrorCodeAwaitedWorkflowCancelled:
		return "AwaitedWorkflowCancelled"
	case ErrorCodeConflictingRegistration:
		return "ConflictingRegistration"
	case ErrorCodeWorkflowUnexpectedType:
		return "WorkflowUnexpectedType"
	case ErrorCodeWorkflowExecution:
		return "WorkflowExecution"
	case ErrorCodeStepExecution:
		return "StepExecution"
	case ErrorCodeDeadLetterQueue:
		return "DeadLetterQueue"
	case ErrorCodeMaxStepRetriesExceeded:
		return "MaxStepRetriesExceeded"
	case ErrorCodeQueueDeduplicated:
		return "QueueDeduplicated"
	case ErrorCodePatchingNotEnabled:
		return "PatchingNotEnabled"
	case ErrorCodeTimeout:
		return "Timeout"
	case ErrorCodeNoApplicationVersions:
		return "NoApplicationVersions"
	case ErrorCodeQueueNotFound:
		return "QueueNotFound"
	case ErrorCodeScheduleNotFound:
		return "ScheduleNotFound"
	case ErrorCodeInvalidOption:
		return "InvalidOption"
	default:
		return fmt.Sprintf("ErrorCode(%d)", int(c))
	}
}

type Error struct {
	Message string    // Human-readable error message
	Code    ErrorCode // Error type code for programmatic handling

	// Optional context fields - only set when relevant to the error
	WorkflowID      string // Associated workflow identifier
	DestinationID   string // Target workflow identifier (for communication errors)
	StepName        string // Step function name (for step errors)
	QueueName       string // Queue name (for queue-related errors)
	DeduplicationID string // Deduplication identifier
	StepID          int    // Step sequence number
	ExpectedName    string // Expected function name (for determinism errors)
	RecordedName    string // Actually recorded function name (for determinism errors)
	MaxRetries      int    // Maximum retry limit (for retry-related errors)

	// CauseKind marks well-known stdlib wrapped causes ("canceled", "deadline")
	// so they survive serialization (wrappedErr is unexported and not encoded).
	// Set by constructors; not intended for direct use.
	CauseKind string

	wrappedErr error // Underlying error being wrapped (for error unwrapping)
}

const (
	causeKindCanceled = "canceled"
	causeKindDeadline = "deadline"
)

// withCause records cause for unwrapping and stamps CauseKind for the closed
// set of stdlib causes that must survive a DB round trip.
func (e *Error) withCause(cause error) *Error {
	e.wrappedErr = cause
	switch {
	case cause == nil:
	case errors.Is(cause, context.Canceled):
		e.CauseKind = causeKindCanceled
	case errors.Is(cause, context.DeadlineExceeded):
		e.CauseKind = causeKindDeadline
	}
	return e
}

// RestoreWrappedCause reconstructs the wrapped stdlib cause from CauseKind after
// decoding a persisted Error, so errors.Is(err, context.Canceled) and
// errors.Is(err, context.DeadlineExceeded) keep matching. No-op if a wrapped
// error is already present.
func (e *Error) RestoreWrappedCause() {
	if e.wrappedErr != nil {
		return
	}
	switch e.CauseKind {
	case causeKindCanceled:
		e.wrappedErr = context.Canceled
	case causeKindDeadline:
		e.wrappedErr = context.DeadlineExceeded
	}
}

// Error returns a formatted error message including the error code.
// This implements the standard Go error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("DBOS Error %s: %s", e.Code, e.Message)
}

// MarshalJSON renders the error in a stable public shape (message + symbolic code)
// for user-facing JSON such as WorkflowStatus, instead of dumping internal fields.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	}{Message: e.Message, Code: e.Code.String()})
}

// UnmarshalJSON decodes the shape produced by MarshalJSON. Reconstruction is
// lossy: only Message and Code survive the wire (context fields and wrapped
// causes do not). A non-string or unrecognized code leaves Code at 0.
func (e *Error) UnmarshalJSON(b []byte) error {
	var raw struct {
		Message string `json:"message"`
		Code    any    `json:"code"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	e.Message = raw.Message
	if s, ok := raw.Code.(string); ok {
		e.Code = parseErrorCode(s)
	}
	return nil
}

// parseErrorCode is the inverse of ErrorCode.String; unknown names map to 0.
func parseErrorCode(s string) ErrorCode {
	for c := ErrorCodeConflictingID; c < _errorCodeSentinel; c++ {
		if c.String() == s {
			return c
		}
	}
	return 0
}

// Unwrap returns the underlying error, if any.
// This enables Go's error unwrapping functionality with errors.Is and errors.As.
func (e *Error) Unwrap() error {
	return e.wrappedErr
}

// Implements https://pkg.go.dev/errors#Is
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	// Match if codes are equal (and target code is set)
	return t.Code != 0 && e.Code == t.Code
}

func NewUnexpectedWorkflowError(workflowID, message string) *Error {
	msg := fmt.Sprintf("Workflow ID %s was previously used by a different workflow. Check that your workflow is deterministic.", workflowID)
	if message != "" {
		msg += " " + message
	}
	return &Error{
		Message:    msg,
		Code:       ErrorCodeUnexpectedWorkflow,
		WorkflowID: workflowID,
	}
}

func NewInitializationError(message string) *Error {
	return &Error{
		Message: fmt.Sprintf("Error initializing DBOS Transact: %s", message),
		Code:    ErrorCodeInitialization,
	}
}

func NewNonExistentWorkflowError(workflowID string) *Error {
	return &Error{
		Message:    fmt.Sprintf("workflow %s does not exist", workflowID),
		Code:       ErrorCodeNonExistentWorkflow,
		WorkflowID: workflowID,
	}
}

func NewConflictingRegistrationError(name string) *Error {
	return &Error{
		Message: fmt.Sprintf("%s is already registered", name),
		Code:    ErrorCodeConflictingRegistration,
	}
}

func NewUnexpectedStepError(workflowID string, stepID int, expectedName, recordedName string) *Error {
	return &Error{
		Message:      fmt.Sprintf("During execution of workflow %s step %d, function %s was recorded when %s was expected. Check that your workflow is deterministic.", workflowID, stepID, recordedName, expectedName),
		Code:         ErrorCodeUnexpectedStep,
		WorkflowID:   workflowID,
		StepID:       stepID,
		ExpectedName: expectedName,
		RecordedName: recordedName,
	}
}

func NewAwaitedWorkflowCancelledError(workflowID string) *Error {
	return &Error{
		Message:    fmt.Sprintf("Awaited workflow %s was cancelled", workflowID),
		Code:       ErrorCodeAwaitedWorkflowCancelled,
		WorkflowID: workflowID,
	}
}

// NewWorkflowCancelledError wraps the cancellation cause (e.g. the context error that
// interrupted a step), so errors.Is still matches context.Canceled / context.DeadlineExceeded.
func NewWorkflowCancelledError(workflowID string, cause error) *Error {
	return (&Error{
		Message:    fmt.Sprintf("Workflow %s was cancelled", workflowID),
		Code:       ErrorCodeWorkflowCancelled,
		WorkflowID: workflowID,
	}).withCause(cause)
}

func NewWorkflowConflictIDError(workflowID string) *Error {
	return &Error{
		Message:    fmt.Sprintf("Conflicting workflow ID %s", workflowID),
		Code:       ErrorCodeConflictingID,
		WorkflowID: workflowID,
	}
}

func NewWorkflowUnexpectedResultType(workflowID, expectedType, actualType string) *Error {
	return &Error{
		Message:    fmt.Sprintf("Workflow %s returned unexpected result type: expected %s, got %s", workflowID, expectedType, actualType),
		Code:       ErrorCodeWorkflowUnexpectedType,
		WorkflowID: workflowID,
	}
}

func NewWorkflowUnexpectedInputType(workflowName, expectedType, actualType string) *Error {
	return &Error{
		Message: fmt.Sprintf("Workflow %s received unexpected input type: expected %s, got %s", workflowName, expectedType, actualType),
		Code:    ErrorCodeWorkflowUnexpectedType,
	}
}

func NewWorkflowExecutionError(workflowID string, err error) *Error {
	return (&Error{
		Message:    fmt.Sprintf("Workflow %s execution error: %s", workflowID, err.Error()),
		Code:       ErrorCodeWorkflowExecution,
		WorkflowID: workflowID,
	}).withCause(err)
}

func NewStepExecutionError(workflowID, stepName string, err error) *Error {
	return (&Error{
		Message:    fmt.Sprintf("Step %s in workflow %s execution error: %v", stepName, workflowID, err),
		Code:       ErrorCodeStepExecution,
		WorkflowID: workflowID,
		StepName:   stepName,
	}).withCause(err)
}

func NewDeadLetterQueueError(workflowID string, maxRetries int) *Error {
	return &Error{
		Message:    fmt.Sprintf("Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries", workflowID, maxRetries),
		Code:       ErrorCodeDeadLetterQueue,
		WorkflowID: workflowID,
		MaxRetries: maxRetries,
	}
}

func NewMaxStepRetriesExceededError(workflowID, stepName string, maxRetries int, err error) *Error {
	return (&Error{
		Message:    fmt.Sprintf("Step %s has exceeded its maximum of %d retries: %v", stepName, maxRetries, err),
		Code:       ErrorCodeMaxStepRetriesExceeded,
		WorkflowID: workflowID,
		StepName:   stepName,
		MaxRetries: maxRetries,
	}).withCause(err)
}

func NewQueueDeduplicatedError(workflowID, queueName, deduplicationID string) *Error {
	return &Error{
		Message:         fmt.Sprintf("Workflow %s was deduplicated due to an existing workflow in queue %s with deduplication ID %s", workflowID, queueName, deduplicationID),
		Code:            ErrorCodeQueueDeduplicated,
		WorkflowID:      workflowID,
		QueueName:       queueName,
		DeduplicationID: deduplicationID,
	}
}

func NewPatchingNotEnabledError() *Error {
	return &Error{
		Message: "Patching system is not enabled. Set EnablePatching to true in the DBOS context configuration to use Patch and DeprecatePatch",
		Code:    ErrorCodePatchingNotEnabled,
	}
}

func NewNoApplicationVersionsError() *Error {
	return &Error{
		Message: "No application versions are registered",
		Code:    ErrorCodeNoApplicationVersions,
	}
}

func NewQueueNotFoundError(queueName string) *Error {
	return &Error{
		Message:   fmt.Sprintf("queue %s does not exist", queueName),
		Code:      ErrorCodeQueueNotFound,
		QueueName: queueName,
	}
}

// NewInvalidOptionError reports invalid or inconsistent options passed to a DBOS API.
func NewInvalidOptionError(message string) *Error {
	return &Error{
		Message: message,
		Code:    ErrorCodeInvalidOption,
	}
}

func NewScheduleNotFoundError(scheduleName string) *Error {
	return &Error{
		Message: fmt.Sprintf("schedule %s does not exist", scheduleName),
		Code:    ErrorCodeScheduleNotFound,
	}
}

// NewTimeoutError builds a timeout error. When the timeout came from an expired
// context, pass context.Cause(ctx) as cause so errors.Is(err, context.DeadlineExceeded)
// matches via Unwrap; pass nil for timeouts with no context deadline behind them.
func NewTimeoutError(workflowID, stepName, message string, cause error) *Error {
	msg := "Operation timed out"
	if stepName != "" {
		msg = fmt.Sprintf("Step %s timed out", stepName)
	}
	if workflowID != "" {
		msg += fmt.Sprintf(" in workflow %s", workflowID)
	}
	if message != "" {
		msg += ": " + message
	}
	return (&Error{
		Message:    msg,
		Code:       ErrorCodeTimeout,
		WorkflowID: workflowID,
		StepName:   stepName,
	}).withCause(cause)
}

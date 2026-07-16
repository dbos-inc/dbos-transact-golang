package models

import "fmt"

// ErrorCode represents the different types of errors that can occur in DBOS operations.
type ErrorCode int

const (
	ErrorCodeConflictingID            ErrorCode = iota + 1 // Workflow ID conflicts or duplicate operations
	ErrorCodeInitialization                                // DBOS context initialization failures
	ErrorCodeNonExistentWorkflow                           // Referenced workflow does not exist
	ErrorCodeConflictingWorkflow                           // Workflow with same ID already exists with different parameters
	ErrorCodeWorkflowCancelled                             // Workflow was cancelled during execution
	ErrorCodeUnexpectedStep                                // Step function mismatch during recovery (non-deterministic workflow)
	ErrorCodeAwaitedWorkflowCancelled                      // A workflow being awaited was cancelled
	ErrorCodeConflictingRegistration                       // Attempting to register a workflow/queue that already exists
	ErrorCodeWorkflowUnexpectedType                        // Type mismatch in workflow input/output
	ErrorCodeWorkflowExecution                             // General workflow execution error
	ErrorCodeStepExecution                                 // General step execution error
	ErrorCodeDeadLetterQueue                               // Workflow moved to dead letter queue after max retries
	ErrorCodeMaxStepRetriesExceeded                        // Step exceeded maximum retry attempts
	ErrorCodeQueueDeduplicated                             // Workflow was deduplicated in the queue
	ErrorCodePatchingNotEnabled                            // Patching system is not enabled in the DBOS context configuration
	ErrorCodeTimeout                                       // Operation timed out (e.g., recv timeout)
	ErrorCodeNoApplicationVersions                         // No application versions are registered in the system database
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
	case ErrorCodeConflictingWorkflow:
		return "ConflictingWorkflow"
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
	default:
		return fmt.Sprintf("ErrorCode(%d)", int(c))
	}
}

// Error is the unified error type for all DBOS operations.
// It provides structured error information with context-specific fields
// and error codes for programmatic handling.
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

	wrappedErr error // Underlying error being wrapped (for error unwrapping)
}

// Error returns a formatted error message including the error code.
// This implements the standard Go error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("DBOS Error %s: %s", e.Code, e.Message)
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

func NewConflictingWorkflowError(workflowID, message string) *Error {
	msg := fmt.Sprintf("Conflicting workflow invocation with the same ID (%s)", workflowID)
	if message != "" {
		msg += ": " + message
	}
	return &Error{
		Message:    msg,
		Code:       ErrorCodeConflictingWorkflow,
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
	return &Error{
		Message:    fmt.Sprintf("Workflow %s was cancelled", workflowID),
		Code:       ErrorCodeWorkflowCancelled,
		WorkflowID: workflowID,
		wrappedErr: cause,
	}
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
	return &Error{
		Message:    fmt.Sprintf("Workflow %s execution error: %s", workflowID, err.Error()),
		Code:       ErrorCodeWorkflowExecution,
		WorkflowID: workflowID,
		wrappedErr: err,
	}
}

func NewStepExecutionError(workflowID, stepName string, err error) *Error {
	return &Error{
		Message:    fmt.Sprintf("Step %s in workflow %s execution error: %v", stepName, workflowID, err),
		Code:       ErrorCodeStepExecution,
		WorkflowID: workflowID,
		StepName:   stepName,
		wrappedErr: err,
	}
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
	return &Error{
		Message:    fmt.Sprintf("Step %s has exceeded its maximum of %d retries: %v", stepName, maxRetries, err),
		Code:       ErrorCodeMaxStepRetriesExceeded,
		WorkflowID: workflowID,
		StepName:   stepName,
		MaxRetries: maxRetries,
		wrappedErr: err,
	}
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
	return &Error{
		Message:    msg,
		Code:       ErrorCodeTimeout,
		WorkflowID: workflowID,
		StepName:   stepName,
		wrappedErr: cause,
	}
}

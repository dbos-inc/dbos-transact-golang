package dbos

import "fmt"

// DBOSErrorCode represents the different types of errors that can occur in DBOS operations.
type DBOSErrorCode int

const (
	ConflictingIDError           DBOSErrorCode = iota + 1 // Workflow ID conflicts or duplicate operations
	InitializationError                                   // DBOS context initialization failures
	NonExistentWorkflowError                              // Referenced workflow does not exist
	ConflictingWorkflowError                              // Workflow with same ID already exists with different parameters
	WorkflowCancelled                                     // Workflow was cancelled during execution
	UnexpectedStep                                        // Step function mismatch during recovery (non-deterministic workflow)
	AwaitedWorkflowCancelled                              // A workflow being awaited was cancelled
	ConflictingRegistrationError                          // Attempting to register a workflow/queue that already exists
	WorkflowUnexpectedTypeError                           // Type mismatch in workflow input/output
	WorkflowExecutionError                                // General workflow execution error
	StepExecutionError                                    // General step execution error
	DeadLetterQueueError                                  // Workflow moved to dead letter queue after max retries
	MaxStepRetriesExceeded                                // Step exceeded maximum retry attempts
	QueueDeduplicated                                     // Workflow was deduplicated in the queue
)

// DBOSError is the unified error type for all DBOS operations.
// It provides structured error information with context-specific fields
// and error codes for programmatic handling.
type DBOSError struct {
	Message string        // Human-readable error message
	Code    DBOSErrorCode // Error type code for programmatic handling

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
func (e *DBOSError) Error() string {
	return fmt.Sprintf("DBOS Error %d: %s", int(e.Code), e.Message)
}

// Unwrap returns the underlying error, if any.
// This enables Go's error unwrapping functionality with errors.Is and errors.As.
func (e *DBOSError) Unwrap() error {
	return e.wrappedErr
}

// Implements https://pkg.go.dev/errors#Is
func (e *DBOSError) Is(target error) bool {
	t, ok := target.(*DBOSError)
	if !ok {
		return false
	}
	// Match if codes are equal (and target code is set)
	return t.Code != 0 && e.Code == t.Code
}

func newConflictingWorkflowError(workflowID, message string) *DBOSError {
	msg := fmt.Sprintf("Conflicting workflow invocation with the same ID (%s)", workflowID)
	if message != "" {
		msg += ": " + message
	}
	return &DBOSError{
		Message:    msg,
		Code:       ConflictingWorkflowError,
		WorkflowID: workflowID,
	}
}

func newInitializationError(message string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("Error initializing DBOS Transact: %s", message),
		Code:    InitializationError,
	}
}

func newNonExistentWorkflowError(workflowID string) *DBOSError {
	return &DBOSError{
		Message:       fmt.Sprintf("workflow %s does not exist", workflowID),
		Code:          NonExistentWorkflowError,
		DestinationID: workflowID,
	}
}

func newConflictingRegistrationError(name string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("%s is already registered", name),
		Code:    ConflictingRegistrationError,
	}
}

func newUnexpectedStepError(workflowID string, stepID int, expectedName, recordedName string) *DBOSError {
	return &DBOSError{
		Message:      fmt.Sprintf("During execution of workflow %s step %d, function %s was recorded when %s was expected. Check that your workflow is deterministic.", workflowID, stepID, recordedName, expectedName),
		Code:         UnexpectedStep,
		WorkflowID:   workflowID,
		StepID:       stepID,
		ExpectedName: expectedName,
		RecordedName: recordedName,
	}
}

func newAwaitedWorkflowCancelledError(workflowID string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Awaited workflow %s was cancelled", workflowID),
		Code:       AwaitedWorkflowCancelled,
		WorkflowID: workflowID,
	}
}

func newAwaitedWorkflowMaxStepRetriesExceeded(workflowID string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Awaited workflow %s has exceeded the maximum number of step retries", workflowID),
		Code:       MaxStepRetriesExceeded,
		WorkflowID: workflowID,
	}
}

func newWorkflowCancelledError(workflowID string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("Workflow %s was cancelled", workflowID),
		Code:    WorkflowCancelled,
	}
}

func newWorkflowConflictIDError(workflowID string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Conflicting workflow ID %s", workflowID),
		Code:       ConflictingIDError,
		WorkflowID: workflowID,
	}
}

func newWorkflowUnexpectedResultType(workflowID, expectedType, actualType string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Workflow %s returned unexpected result type: expected %s, got %s", workflowID, expectedType, actualType),
		Code:       WorkflowUnexpectedTypeError,
		WorkflowID: workflowID,
	}
}

func newWorkflowUnexpectedInputType(workflowName, expectedType, actualType string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("Workflow %s received unexpected input type: expected %s, got %s", workflowName, expectedType, actualType),
		Code:    WorkflowUnexpectedTypeError,
	}
}

func newWorkflowExecutionError(workflowID string, err error) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Workflow %s execution error: %s", workflowID, err.Error()),
		Code:       WorkflowExecutionError,
		WorkflowID: workflowID,
		wrappedErr: err,
	}
}

func newStepExecutionError(workflowID, stepName string, err error) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Step %s in workflow %s execution error: %v", stepName, workflowID, err),
		Code:       StepExecutionError,
		WorkflowID: workflowID,
		StepName:   stepName,
		wrappedErr: err,
	}
}

func newDeadLetterQueueError(workflowID string, maxRetries int) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries", workflowID, maxRetries),
		Code:       DeadLetterQueueError,
		WorkflowID: workflowID,
		MaxRetries: maxRetries,
	}
}

func newMaxStepRetriesExceededError(workflowID, stepName string, maxRetries int, err error) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Step %s has exceeded its maximum of %d retries: %v", stepName, maxRetries, err),
		Code:       MaxStepRetriesExceeded,
		WorkflowID: workflowID,
		StepName:   stepName,
		MaxRetries: maxRetries,
		wrappedErr: err,
	}
}

func newQueueDeduplicatedError(workflowID, queueName, deduplicationID string) *DBOSError {
	return &DBOSError{
		Message:         fmt.Sprintf("Workflow %s was deduplicated due to an existing workflow in queue %s with deduplication ID %s", workflowID, queueName, deduplicationID),
		Code:            QueueDeduplicated,
		WorkflowID:      workflowID,
		QueueName:       queueName,
		DeduplicationID: deduplicationID,
	}
}

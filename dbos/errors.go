package dbos

import "fmt"

// DBOSErrorCode represents the different types of DBOS errors
type DBOSErrorCode int

const (
	ConflictingIDError DBOSErrorCode = iota + 1
	InitializationError
	WorkflowFunctionNotFound
	NonExistentWorkflowError
	ConflictingWorkflowError
	WorkflowCancelled
	UnexpectedStep
	AwaitedWorkflowCancelled
	ConflictingRegistrationError
	WorkflowUnexpectedTypeError
	WorkflowExecutionError
	StepExecutionError
	DeadLetterQueueError
)

// DBOSError is the unified error type for all DBOS errors
type DBOSError struct {
	Message    string
	Code       DBOSErrorCode
	StatusCode *int
	IsBase     bool // true for errors that shouldn't be caught by user code

	// Optional context fields - only set when relevant
	WorkflowID      string
	DestinationID   string
	StepName        string
	QueueName       string
	DeduplicationID string
	StepID          int
	ExpectedName    string
	RecordedName    string
	MaxRetries      int
	Errors          []error
}

func (e *DBOSError) Error() string {
	return fmt.Sprintf("DBOS Error %d: %s", int(e.Code), e.Message)
}

func NewConflictingWorkflowError(workflowID, message string) *DBOSError {
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

func NewInitializationError(message string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("Error initializing DBOS Transact: %s", message),
		Code:    InitializationError,
	}
}

func NewWorkflowFunctionNotFoundError(workflowID, message string) *DBOSError {
	msg := fmt.Sprintf("Workflow function not found for workflow ID %s", workflowID)
	if message != "" {
		msg += ": " + message
	}
	return &DBOSError{
		Message:    msg,
		Code:       WorkflowFunctionNotFound,
		WorkflowID: workflowID,
	}
}

func NewNonExistentWorkflowError(workflowID string) *DBOSError {
	return &DBOSError{
		Message:       fmt.Sprintf("workflow %s does not exist", workflowID),
		Code:          NonExistentWorkflowError,
		DestinationID: workflowID,
	}
}

func NewConflictingRegistrationError(name string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("%s is already registered", name),
		Code:    ConflictingRegistrationError,
	}
}

func NewUnexpectedStepError(workflowID string, stepID int, expectedName, recordedName string) *DBOSError {
	return &DBOSError{
		Message:      fmt.Sprintf("During execution of workflow %s step %d, function %s was recorded when %s was expected. Check that your workflow is deterministic.", workflowID, stepID, recordedName, expectedName),
		Code:         UnexpectedStep,
		WorkflowID:   workflowID,
		StepID:       stepID,
		ExpectedName: expectedName,
		RecordedName: recordedName,
	}
}

func NewAwaitedWorkflowCancelledError(workflowID string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Awaited workflow %s was cancelled", workflowID),
		Code:       AwaitedWorkflowCancelled,
		WorkflowID: workflowID,
	}
}

func NewWorkflowCancelledError(workflowID string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("Workflow %s was cancelled", workflowID),
		Code:    WorkflowCancelled,
		IsBase:  true,
	}
}

func NewWorkflowConflictIDError(workflowID string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Conflicting workflow ID %s", workflowID),
		Code:       ConflictingIDError,
		WorkflowID: workflowID,
		IsBase:     true,
	}
}

func NewWorkflowUnexpectedResultType(workflowID, expectedType, actualType string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Workflow %s returned unexpected result type: expected %s, got %s", workflowID, expectedType, actualType),
		Code:       WorkflowUnexpectedTypeError,
		WorkflowID: workflowID,
		IsBase:     true,
	}
}

func NewWorkflowUnexpectedInputType(workflowName, expectedType, actualType string) *DBOSError {
	return &DBOSError{
		Message: fmt.Sprintf("Workflow %s received unexpected input type: expected %s, got %s", workflowName, expectedType, actualType),
		Code:    WorkflowUnexpectedTypeError,
		IsBase:  true,
	}
}

func NewWorkflowExecutionError(workflowID, message string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Workflow %s execution error: %s", workflowID, message),
		Code:       WorkflowExecutionError,
		WorkflowID: workflowID,
		IsBase:     true,
	}
}

func NewStepExecutionError(workflowID, stepName, message string) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Step %s in workflow %s execution error: %s", stepName, workflowID, message),
		Code:       StepExecutionError,
		WorkflowID: workflowID,
		StepName:   stepName,
		IsBase:     true,
	}
}

func NewDeadLetterQueueError(workflowID string, maxRetries int) *DBOSError {
	return &DBOSError{
		Message:    fmt.Sprintf("Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries", workflowID, maxRetries),
		Code:       DeadLetterQueueError,
		WorkflowID: workflowID,
		MaxRetries: maxRetries,
		IsBase:     true,
	}
}

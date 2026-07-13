package models

import "time"

// WorkflowStatusType represents the current execution state of a workflow.
type WorkflowStatusType string

const (
	WorkflowStatusPending                     WorkflowStatusType = "PENDING"                        // Workflow is running or ready to run
	WorkflowStatusEnqueued                    WorkflowStatusType = "ENQUEUED"                       // Workflow is queued and waiting for execution
	WorkflowStatusDelayed                     WorkflowStatusType = "DELAYED"                        // Workflow is delayed and will transition to ENQUEUED after the delay expires
	WorkflowStatusSuccess                     WorkflowStatusType = "SUCCESS"                        // Workflow completed successfully
	WorkflowStatusError                       WorkflowStatusType = "ERROR"                          // Workflow completed with an error
	WorkflowStatusCancelled                   WorkflowStatusType = "CANCELLED"                      // Workflow was cancelled (manually or due to timeout)
	WorkflowStatusMaxRecoveryAttemptsExceeded WorkflowStatusType = "MAX_RECOVERY_ATTEMPTS_EXCEEDED" // Workflow exceeded maximum retry attempts
)

// WorkflowStatus contains comprehensive information about a workflow's current state and execution history.
type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`                 // Unique identifier for the workflow
	Status             WorkflowStatusType `json:"status"`                        // Current execution status
	Name               string             `json:"name"`                          // Function name of the workflow
	AuthenticatedUser  string             `json:"authenticated_user,omitempty"`  // User who initiated the workflow (if applicable)
	AssumedRole        string             `json:"assumed_role,omitempty"`        // Role assumed during execution (if applicable)
	AuthenticatedRoles []string           `json:"authenticated_roles,omitempty"` // Roles available to the user (if applicable)
	Output             any                `json:"output,omitempty"`              // Workflow output (available after completion)
	Error              error              `json:"error,omitempty"`               // Error information (if status is ERROR)
	ExecutorID         string             `json:"executor_id"`                   // ID of the executor running this workflow
	CreatedAt          time.Time          `json:"created_at"`                    // When the workflow was created
	UpdatedAt          time.Time          `json:"updated_at"`                    // When the workflow status was last updated
	ApplicationVersion string             `json:"application_version"`           // Version of the application that created this workflow
	ApplicationID      string             `json:"application_id,omitempty"`      // Application identifier
	Attempts           int                `json:"attempts"`                      // Number of execution attempts
	QueueName          string             `json:"queue_name,omitempty"`          // Queue name (if workflow was enqueued)
	Timeout            time.Duration      `json:"timeout,omitempty"`             // Workflow timeout duration
	Deadline           time.Time          `json:"deadline"`                      // Absolute deadline for workflow completion
	StartedAt          time.Time          `json:"started_at"`                    // When the workflow execution actually started
	DeduplicationID    string             `json:"deduplication_id,omitempty"`    // Queue deduplication identifier
	Input              any                `json:"input,omitempty"`               // Input parameters passed to the workflow
	Priority           int                `json:"priority,omitempty"`            // Queue execution priority (lower numbers have higher priority)
	QueuePartitionKey  string             `json:"queue_partition_key,omitempty"` // Queue partition key for partitioned queues
	ForkedFrom         string             `json:"forked_from,omitempty"`         // ID of the original workflow if this is a fork
	WasForkedFrom      bool               `json:"was_forked_from,omitempty"`     // Whether this workflow has been forked from
	ParentWorkflowID   string             `json:"parent_workflow_id,omitempty"`  // ID of the parent workflow if this is a child
	CompletedAt        time.Time          `json:"completed_at,omitempty"`        // When the workflow reached a terminal state (SUCCESS, ERROR, or CANCELLED)
	ClassName          string             `json:"class_name,omitempty"`          // Class/namespace name for cross-language dispatch
	ConfigName         *string            `json:"config_name,omitempty"`         // Instance/config name for cross-language dispatch (nil = unset, pointer to "" = explicit empty)
	Serialization      string             `json:"serialization,omitempty"`       // Serialization format used for inputs/outputs (e.g., "DBOS_JSON", "portable_json")
	DelayUntil         time.Time          `json:"delay_until,omitempty"`         // The time before which the workflow should not be dequeued
	Attributes         map[string]any     `json:"attributes,omitempty"`          // Custom key-value attributes attached to the workflow at creation
	ScheduleName       string             `json:"schedule_name,omitempty"`       // Name of the schedule that enqueued this workflow (if any)
}

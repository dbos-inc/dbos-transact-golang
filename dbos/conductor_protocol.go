package dbos

// MessageType represents the type of message exchanged with the conductor
type MessageType string

const (
	ExecutorInfo         MessageType = "executor_info"
	ListWorkflowsMessage MessageType = "list_workflows"
)

// BaseMessage represents the common structure of all conductor messages
type BaseMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"request_id"`
}

// BaseResponse extends BaseMessage with optional error handling
type BaseResponse struct {
	BaseMessage
	ErrorMessage *string `json:"error_message,omitempty"`
}

// ExecutorInfoRequest is sent by the conductor to request executor information
type ExecutorInfoRequest struct {
	BaseMessage
}

// ExecutorInfoResponse is sent in response to executor info requests
type ExecutorInfoResponse struct {
	BaseMessage
	ExecutorID         string  `json:"executor_id"`
	ApplicationVersion string  `json:"application_version"`
	Hostname           *string `json:"hostname,omitempty"`
	ErrorMessage       *string `json:"error_message,omitempty"`
}

// ListWorkflowsBody contains filter parameters for listing workflows
type ListWorkflowsBody struct {
	WorkflowUUIDs      []string             `json:"workflow_uuids,omitempty"`
	WorkflowName       *string              `json:"workflow_name,omitempty"`
	AuthenticatedUser  *string              `json:"authenticated_user,omitempty"`
	StartTime          *string              `json:"start_time,omitempty"`
	EndTime            *string              `json:"end_time,omitempty"`
	Status             *string              `json:"status,omitempty"`
	ApplicationVersion *string              `json:"application_version,omitempty"`
	Limit              *int                 `json:"limit,omitempty"`
	Offset             *int                 `json:"offset,omitempty"`
	SortDesc           bool                 `json:"sort_desc"`
	LoadInput          bool                 `json:"load_input"`
	LoadOutput         bool                 `json:"load_output"`
}

// ListWorkflowsRequest is sent by the conductor to list workflows
type ListWorkflowsRequest struct {
	BaseMessage
	Body ListWorkflowsBody `json:"body"`
}

// WorkflowsOutput represents a single workflow in the list response
type WorkflowsOutput struct {
	WorkflowUUID        string  `json:"WorkflowUUID"`
	Status              *string `json:"Status,omitempty"`
	WorkflowName        *string `json:"WorkflowName,omitempty"`
	WorkflowClassName   *string `json:"WorkflowClassName,omitempty"`
	WorkflowConfigName  *string `json:"WorkflowConfigName,omitempty"`
	AuthenticatedUser   *string `json:"AuthenticatedUser,omitempty"`
	AssumedRole         *string `json:"AssumedRole,omitempty"`
	AuthenticatedRoles  *string `json:"AuthenticatedRoles,omitempty"`
	Input               *string `json:"Input,omitempty"`
	Output              *string `json:"Output,omitempty"`
	Error               *string `json:"Error,omitempty"`
	CreatedAt           *string `json:"CreatedAt,omitempty"`
	UpdatedAt           *string `json:"UpdatedAt,omitempty"`
	QueueName           *string `json:"QueueName,omitempty"`
	ApplicationVersion  *string `json:"ApplicationVersion,omitempty"`
	ExecutorID          *string `json:"ExecutorID,omitempty"`
}

// ListWorkflowsResponse is sent in response to list workflows requests
type ListWorkflowsResponse struct {
	BaseMessage
	Output       []WorkflowsOutput `json:"output"`
	ErrorMessage *string           `json:"error_message,omitempty"`
}

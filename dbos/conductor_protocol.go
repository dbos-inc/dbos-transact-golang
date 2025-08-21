package dbos

import (
	"encoding/json"
	"time"
)

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

// listWorkflowsConductorRequestBody contains filter parameters for listing workflows
type listWorkflowsConductorRequestBody struct {
	WorkflowUUIDs      []string   `json:"workflow_uuids,omitempty"`
	WorkflowName       *string    `json:"workflow_name,omitempty"`
	AuthenticatedUser  *string    `json:"authenticated_user,omitempty"`
	StartTime          *time.Time `json:"start_time,omitempty"`
	EndTime            *time.Time `json:"end_time,omitempty"`
	Status             *string    `json:"status,omitempty"`
	ApplicationVersion *string    `json:"application_version,omitempty"`
	Limit              *int       `json:"limit,omitempty"`
	Offset             *int       `json:"offset,omitempty"`
	SortDesc           bool       `json:"sort_desc"`
	LoadInput          bool       `json:"load_input"`
	LoadOutput         bool       `json:"load_output"`
}

// listWorkflowsConductorRequest is sent by the conductor to list workflows
type listWorkflowsConductorRequest struct {
	BaseMessage
	Body listWorkflowsConductorRequestBody `json:"body"`
}

// listWorkflowsConductorResponseBody represents a single workflow in the list response
type listWorkflowsConductorResponseBody struct {
	WorkflowUUID       string  `json:"WorkflowUUID"`
	Status             *string `json:"Status,omitempty"`
	WorkflowName       *string `json:"WorkflowName,omitempty"`
	WorkflowClassName  *string `json:"WorkflowClassName,omitempty"`
	WorkflowConfigName *string `json:"WorkflowConfigName,omitempty"`
	AuthenticatedUser  *string `json:"AuthenticatedUser,omitempty"`
	AssumedRole        *string `json:"AssumedRole,omitempty"`
	AuthenticatedRoles *string `json:"AuthenticatedRoles,omitempty"`
	Input              *string `json:"Input,omitempty"`
	Output             *string `json:"Output,omitempty"`
	Error              *string `json:"Error,omitempty"`
	CreatedAt          *string `json:"CreatedAt,omitempty"`
	UpdatedAt          *string `json:"UpdatedAt,omitempty"`
	QueueName          *string `json:"QueueName,omitempty"`
	ApplicationVersion *string `json:"ApplicationVersion,omitempty"`
	ExecutorID         *string `json:"ExecutorID,omitempty"`
}

// listWorkflowsConductorResponse is sent in response to list workflows requests
type listWorkflowsConductorResponse struct {
	BaseMessage
	Output       []listWorkflowsConductorResponseBody `json:"output"`
	ErrorMessage *string                              `json:"error_message,omitempty"`
}

// formatListWorkflowsResponseBody converts WorkflowStatus to listWorkflowsConductorResponseBody for the conductor protocol
func formatListWorkflowsResponseBody(wf WorkflowStatus) listWorkflowsConductorResponseBody {
	output := listWorkflowsConductorResponseBody{
		WorkflowUUID: wf.ID,
	}

	// Convert status
	if wf.Status != "" {
		status := string(wf.Status)
		output.Status = &status
	}

	// Convert workflow name
	if wf.Name != "" {
		output.WorkflowName = &wf.Name
	}

	// Copy optional fields
	output.AuthenticatedUser = wf.AuthenticatedUser
	output.AssumedRole = wf.AssumedRole
	output.AuthenticatedRoles = wf.AuthenticatedRoles

	// Convert input/output to JSON strings if present
	if wf.Input != nil {
		inputJSON, err := json.Marshal(wf.Input)
		if err == nil {
			inputStr := string(inputJSON)
			output.Input = &inputStr
		}
	}
	if wf.Output != nil {
		outputJSON, err := json.Marshal(wf.Output)
		if err == nil {
			outputStr := string(outputJSON)
			output.Output = &outputStr
		}
	}

	// Convert error to string
	if wf.Error != nil {
		errorStr := wf.Error.Error()
		output.Error = &errorStr
	}

	// Convert timestamps to RFC3339 strings
	if !wf.CreatedAt.IsZero() {
		createdStr := wf.CreatedAt.Format(time.RFC3339)
		output.CreatedAt = &createdStr
	}
	if !wf.UpdatedAt.IsZero() {
		updatedStr := wf.UpdatedAt.Format(time.RFC3339)
		output.UpdatedAt = &updatedStr
	}

	// Copy queue name
	if wf.QueueName != "" {
		output.QueueName = &wf.QueueName
	}

	// Copy application version
	if wf.ApplicationVersion != "" {
		output.ApplicationVersion = &wf.ApplicationVersion
	}

	// Copy executor ID
	if wf.ExecutorID != "" {
		output.ExecutorID = &wf.ExecutorID
	}

	return output
}

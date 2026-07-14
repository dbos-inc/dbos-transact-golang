package conductor

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// StringOrList is a custom JSON type that accepts either a single string
// or an array of strings, matching the conductor's StringOrList for filter fields.
type StringOrList []string

func (s *StringOrList) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = StringOrList{single}
		return nil
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	*s = StringOrList(list)
	return nil
}

func (s StringOrList) toSlice() []string {
	return []string(s)
}

// MessageType represents the type of message exchanged with the conductor
type MessageType string

const (
	ExecutorInfo                 MessageType = "executor_info"
	RecoveryMessage              MessageType = "recovery"
	CancelWorkflowMessage        MessageType = "cancel"
	ResumeWorkflowMessage        MessageType = "resume"
	ListWorkflowsMessage         MessageType = "list_workflows"
	ListQueuedWorkflowsMessage   MessageType = "list_queued_workflows"
	ListStepsMessage             MessageType = "list_steps"
	GetWorkflowMessage           MessageType = "get_workflow"
	ForkWorkflowMessage          MessageType = "fork_workflow"
	ForkFromFailureMessage       MessageType = "fork_from_failure"
	ExistPendingWorkflowsMessage MessageType = "exist_pending_workflows"
	RetentionMessage             MessageType = "retention"
	GetMetricsMessage            MessageType = "get_metrics"
	ExportWorkflowMessage        MessageType = "export_workflow"
	ImportWorkflowMessage        MessageType = "import_workflow"
	DeleteWorkflowMessage        MessageType = "delete"
	AlertMessage                 MessageType = "alert"
	ListSchedulesMessage         MessageType = "list_schedules"
	GetScheduleMessage           MessageType = "get_schedule"
	PauseScheduleMessage         MessageType = "pause_schedule"
	ResumeScheduleMessage        MessageType = "resume_schedule"
	BackfillScheduleMessage      MessageType = "backfill_schedule"
	TriggerScheduleMessage       MessageType = "trigger_schedule"
	GetWorkflowEventsMessage     MessageType = "get_workflow_events"
	GetWorkflowNotificationsMsg  MessageType = "get_workflow_notifications"
	GetWorkflowStreamsMessage    MessageType = "get_workflow_streams"
	GetWorkflowAggregatesMessage MessageType = "get_workflow_aggregates"
	GetStepAggregatesMessage     MessageType = "get_step_aggregates"
	ListAppVersionsMessage       MessageType = "list_application_versions"
	SetLatestAppVersionMessage   MessageType = "set_latest_application_version"
	ListQueuesMessage            MessageType = "list_queues"
	GetQueueMessage              MessageType = "get_queue"
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
	BaseResponse
	ExecutorID         string         `json:"executor_id"`
	ApplicationVersion string         `json:"application_version"`
	Hostname           *string        `json:"hostname,omitempty"`
	DBOSVersion        string         `json:"dbos_version"`
	Language           string         `json:"language"`
	ExecutorMetadata   map[string]any `json:"executor_metadata,omitempty"`
}

// ListWorkflowsConductorRequestBody contains filter parameters for listing workflows.
type ListWorkflowsConductorRequestBody struct {
	WorkflowUUIDs      []string       `json:"workflow_uuids,omitempty"`
	WorkflowName       StringOrList   `json:"workflow_name,omitempty"`
	AuthenticatedUser  StringOrList   `json:"authenticated_user,omitempty"`
	StartTime          *time.Time     `json:"start_time,omitempty"`       // ISO 8601
	EndTime            *time.Time     `json:"end_time,omitempty"`         // ISO 8601
	CompletedAfter     *time.Time     `json:"completed_after,omitempty"`  // ISO 8601
	CompletedBefore    *time.Time     `json:"completed_before,omitempty"` // ISO 8601
	DequeuedAfter      *time.Time     `json:"dequeued_after,omitempty"`   // ISO 8601
	DequeuedBefore     *time.Time     `json:"dequeued_before,omitempty"`  // ISO 8601
	Status             StringOrList   `json:"status,omitempty"`
	ApplicationVersion StringOrList   `json:"application_version,omitempty"`
	ForkedFrom         StringOrList   `json:"forked_from,omitempty"`
	ParentWorkflowID   StringOrList   `json:"parent_workflow_id,omitempty"`
	WasForkedFrom      *bool          `json:"was_forked_from,omitempty"`
	HasParent          *bool          `json:"has_parent,omitempty"`
	QueueName          StringOrList   `json:"queue_name,omitempty"`
	Limit              *int           `json:"limit,omitempty"`
	Offset             *int           `json:"offset,omitempty"`
	SortDesc           bool           `json:"sort_desc"`
	WorkflowIDPrefix   StringOrList   `json:"workflow_id_prefix,omitempty"`
	LoadInput          bool           `json:"load_input"`
	LoadOutput         bool           `json:"load_output"`
	ExecutorID         StringOrList   `json:"executor_id,omitempty"`
	QueuesOnly         bool           `json:"queues_only"`
	Attributes         map[string]any `json:"attributes,omitempty"`
	ScheduleName       StringOrList   `json:"schedule_name,omitempty"`
}

// ListWorkflowsConductorRequest is sent by the conductor to list workflows
type ListWorkflowsConductorRequest struct {
	BaseMessage
	Body ListWorkflowsConductorRequestBody `json:"body"`
}

// ListWorkflowsConductorResponseBody represents a single workflow in the list response
type ListWorkflowsConductorResponseBody struct {
	WorkflowUUID            string  `json:"WorkflowUUID"`
	Status                  *string `json:"Status,omitempty"`
	WorkflowName            *string `json:"WorkflowName,omitempty"`
	WorkflowClassName       *string `json:"WorkflowClassName,omitempty"`
	WorkflowConfigName      *string `json:"WorkflowConfigName,omitempty"`
	AuthenticatedUser       *string `json:"AuthenticatedUser,omitempty"`
	AssumedRole             *string `json:"AssumedRole,omitempty"`
	AuthenticatedRoles      *string `json:"AuthenticatedRoles,omitempty"`
	Input                   *string `json:"Input,omitempty"`
	Output                  *string `json:"Output,omitempty"`
	Error                   *string `json:"Error,omitempty"`
	CreatedAt               *string `json:"CreatedAt,omitempty"`
	UpdatedAt               *string `json:"UpdatedAt,omitempty"`
	QueueName               *string `json:"QueueName,omitempty"`
	ApplicationVersion      *string `json:"ApplicationVersion,omitempty"`
	ExecutorID              *string `json:"ExecutorID,omitempty"`
	WorkflowTimeoutMS       *string `json:"WorkflowTimeoutMS,omitempty"`
	WorkflowDeadlineEpochMS *string `json:"WorkflowDeadlineEpochMS,omitempty"`
	DeduplicationID         *string `json:"DeduplicationID,omitempty"`
	Priority                *string `json:"Priority,omitempty"`
	QueuePartitionKey       *string `json:"QueuePartitionKey,omitempty"`
	ForkedFrom              *string `json:"ForkedFrom,omitempty"`
	WasForkedFrom           *bool   `json:"WasForkedFrom,omitempty"`
	ParentWorkflowID        *string `json:"ParentWorkflowID,omitempty"`
	DequeuedAt              *string `json:"DequeuedAt,omitempty"`
	DelayUntilEpochMS       *string `json:"DelayUntilEpochMS,omitempty"`
	CompletedAt             *string `json:"CompletedAt,omitempty"`
	Attributes              *string `json:"Attributes,omitempty"`
	ScheduleName            *string `json:"ScheduleName,omitempty"`
}

// ListWorkflowsConductorResponse is sent in response to list workflows requests
type ListWorkflowsConductorResponse struct {
	BaseResponse
	Output []ListWorkflowsConductorResponseBody `json:"output"`
}

// formatListWorkflowsResponseBody converts models.WorkflowStatus to ListWorkflowsConductorResponseBody for the conductor protocol
func formatListWorkflowsResponseBody(wf models.WorkflowStatus) ListWorkflowsConductorResponseBody {
	output := ListWorkflowsConductorResponseBody{
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

	// Convert identity fields
	if wf.AuthenticatedUser != "" {
		output.AuthenticatedUser = &wf.AuthenticatedUser
	}
	if wf.AssumedRole != "" {
		output.AssumedRole = &wf.AssumedRole
	}
	// Convert authenticated roles to JSON string if present
	if len(wf.AuthenticatedRoles) > 0 {
		rolesJSON, err := json.Marshal(wf.AuthenticatedRoles)
		if err == nil {
			rolesStr := string(rolesJSON)
			output.AuthenticatedRoles = &rolesStr
		}
	}

	// input/output are already JSON strings
	if wf.Input != nil {
		inputStr, ok := wf.Input.(string)
		if ok {
			output.Input = &inputStr
		}
	}
	if wf.Output != nil {
		outputStr, ok := wf.Output.(string)
		if ok {
			output.Output = &outputStr
		}
	}

	// Convert error to string
	if wf.Error != nil {
		errorStr := wf.Error.Error()
		output.Error = &errorStr
	}

	// Convert timestamps to unix epochs
	if !wf.CreatedAt.IsZero() {
		createdStr := strconv.FormatInt(wf.CreatedAt.UnixMilli(), 10)
		output.CreatedAt = &createdStr
	}
	if !wf.UpdatedAt.IsZero() {
		updatedStr := strconv.FormatInt(wf.UpdatedAt.UnixMilli(), 10)
		output.UpdatedAt = &updatedStr
	}

	// Copy queue name
	if wf.QueueName != "" {
		output.QueueName = &wf.QueueName
	}

	// Copy queue partition key
	if wf.QueuePartitionKey != "" {
		output.QueuePartitionKey = &wf.QueuePartitionKey
	}

	// Copy deduplication ID
	if wf.DeduplicationID != "" {
		output.DeduplicationID = &wf.DeduplicationID
	}

	// Copy priority (include "0" so conductor receives a string)
	priorityStr := strconv.Itoa(wf.Priority)
	output.Priority = &priorityStr

	// Copy application version
	if wf.ApplicationVersion != "" {
		output.ApplicationVersion = &wf.ApplicationVersion
	}

	// Copy executor ID
	if wf.ExecutorID != "" {
		output.ExecutorID = &wf.ExecutorID
	}

	// Convert timeout to milliseconds string
	if wf.Timeout > 0 {
		timeoutStr := strconv.FormatInt(wf.Timeout.Milliseconds(), 10)
		output.WorkflowTimeoutMS = &timeoutStr
	}

	// Convert deadline to epoch milliseconds string
	if !wf.Deadline.IsZero() {
		deadlineStr := strconv.FormatInt(wf.Deadline.UnixMilli(), 10)
		output.WorkflowDeadlineEpochMS = &deadlineStr
	}

	// Copy forked from
	if wf.ForkedFrom != "" {
		output.ForkedFrom = &wf.ForkedFrom
	}

	// Copy was_forked_from
	wasForkedFrom := wf.WasForkedFrom
	output.WasForkedFrom = &wasForkedFrom

	// Copy parent workflow ID
	if wf.ParentWorkflowID != "" {
		output.ParentWorkflowID = &wf.ParentWorkflowID
	}

	// DequeuedAt: when a workflow is dequeued and starts running, started_at is set.
	// Use StartedAt as DequeuedAt for workflows that have been dequeued (PENDING with started_at).
	if (wf.Status == models.WorkflowStatusPending) && !wf.StartedAt.IsZero() {
		dequeuedStr := strconv.FormatInt(wf.StartedAt.UnixMilli(), 10)
		output.DequeuedAt = &dequeuedStr
	}

	// Convert delay_until to epoch milliseconds string
	if !wf.DelayUntil.IsZero() {
		delayStr := strconv.FormatInt(wf.DelayUntil.UnixMilli(), 10)
		output.DelayUntilEpochMS = &delayStr
	}

	// Convert completed_at to epoch milliseconds string
	if !wf.CompletedAt.IsZero() {
		completedStr := strconv.FormatInt(wf.CompletedAt.UnixMilli(), 10)
		output.CompletedAt = &completedStr
	}

	// Marshal attributes to a JSON string so the wire format is parseable by Conductor
	if len(wf.Attributes) > 0 {
		attributesJSON, err := json.Marshal(wf.Attributes)
		if err == nil {
			attributesStr := string(attributesJSON)
			output.Attributes = &attributesStr
		}
	}

	// Copy schedule name
	if wf.ScheduleName != "" {
		output.ScheduleName = &wf.ScheduleName
	}

	return output
}

// ListStepsConductorRequest is sent by the conductor to list workflow steps
type ListStepsConductorRequest struct {
	BaseMessage
	WorkflowID string `json:"workflow_id"`
	LoadOutput bool   `json:"load_output"`
	Limit      *int   `json:"limit,omitempty"`
	Offset     *int   `json:"offset,omitempty"`
}

// WorkflowStepsConductorResponseBody represents a single workflow step in the list response
type WorkflowStepsConductorResponseBody struct {
	FunctionID         int     `json:"function_id"`
	FunctionName       string  `json:"function_name"`
	Output             *string `json:"output,omitempty"`
	Error              *string `json:"error,omitempty"`
	ChildWorkflowID    *string `json:"child_workflow_id,omitempty"`
	StartedAtEpochMs   *string `json:"started_at_epoch_ms,omitempty"`
	CompletedAtEpochMs *string `json:"completed_at_epoch_ms,omitempty"`
}

// ListStepsConductorResponse is sent in response to list steps requests
type ListStepsConductorResponse struct {
	BaseResponse
	Output *[]WorkflowStepsConductorResponseBody `json:"output,omitempty"`
}

// formatWorkflowStepsResponseBody converts models.StepInfo to WorkflowStepsConductorResponseBody for the conductor protocol
func formatWorkflowStepsResponseBody(step models.StepInfo) WorkflowStepsConductorResponseBody {
	output := WorkflowStepsConductorResponseBody{
		FunctionID:   step.StepID,
		FunctionName: step.StepName,
	}

	// output is already a JSON string
	if step.Output != nil {
		outputStr, ok := step.Output.(string)
		if ok {
			output.Output = &outputStr
		}
	}

	// Convert error to string if present
	if step.Error != nil {
		errorStr := step.Error.Error()
		output.Error = &errorStr
	}

	// Set child workflow ID if present
	if step.ChildWorkflowID != "" {
		output.ChildWorkflowID = &step.ChildWorkflowID
	}

	// Convert timestamps to epoch milliseconds strings
	if !step.StartedAt.IsZero() {
		startedAtStr := strconv.FormatInt(step.StartedAt.UnixMilli(), 10)
		output.StartedAtEpochMs = &startedAtStr
	}
	if !step.CompletedAt.IsZero() {
		completedAtStr := strconv.FormatInt(step.CompletedAt.UnixMilli(), 10)
		output.CompletedAtEpochMs = &completedAtStr
	}

	return output
}

// GetWorkflowConductorRequest is sent by the conductor to get a specific workflow
type GetWorkflowConductorRequest struct {
	BaseMessage
	WorkflowID string `json:"workflow_id"`
	LoadInput  bool   `json:"load_input"`
	LoadOutput bool   `json:"load_output"`
}

// GetWorkflowConductorResponse is sent in response to get workflow requests
type GetWorkflowConductorResponse struct {
	BaseResponse
	Output *ListWorkflowsConductorResponseBody `json:"output,omitempty"`
}

// ForkWorkflowConductorRequestBody contains the fork workflow parameters
type ForkWorkflowConductorRequestBody struct {
	WorkflowID         string  `json:"workflow_id"`
	StartStep          int     `json:"start_step"`
	ApplicationVersion *string `json:"application_version,omitempty"`
	NewWorkflowID      *string `json:"new_workflow_id,omitempty"`
	QueueName          *string `json:"queue_name,omitempty"`
	QueuePartitionKey  *string `json:"queue_partition_key,omitempty"`
}

// ForkWorkflowConductorRequest is sent by the conductor to fork a workflow
type ForkWorkflowConductorRequest struct {
	BaseMessage
	Body ForkWorkflowConductorRequestBody `json:"body"`
}

// ForkWorkflowConductorResponse is sent in response to fork workflow requests
type ForkWorkflowConductorResponse struct {
	BaseResponse
	NewWorkflowID *string `json:"new_workflow_id,omitempty"`
}

// ForkFromFailureConductorRequestBody contains the bulk fork-from-failure parameters
type ForkFromFailureConductorRequestBody struct {
	WorkflowIDs        []string `json:"workflow_ids"`
	ApplicationVersion *string  `json:"application_version,omitempty"`
	QueueName          *string  `json:"queue_name,omitempty"`
	QueuePartitionKey  *string  `json:"queue_partition_key,omitempty"`
	FromLastFailure    bool     `json:"from_last_failure,omitempty"`
	FromLastStep       bool     `json:"from_last_step,omitempty"`
	FromStep           *int     `json:"from_step,omitempty"`
	FromStepName       *string  `json:"from_step_name,omitempty"`
}

// ForkFromFailureConductorRequest is sent by the conductor to bulk fork workflows
type ForkFromFailureConductorRequest struct {
	BaseMessage
	Body ForkFromFailureConductorRequestBody `json:"body"`
}

// ForkFromFailureConductorResponse is sent in response to fork-from-failure requests
type ForkFromFailureConductorResponse struct {
	BaseResponse
	ForkedWorkflowIDs []string `json:"forked_workflow_ids,omitempty"`
}

// CancelWorkflowConductorRequest is sent by the conductor to cancel a workflow
type CancelWorkflowConductorRequest struct {
	BaseMessage
	CancelChildren bool     `json:"cancel_children"`
	WorkflowID     string   `json:"workflow_id"`
	WorkflowIDs    []string `json:"workflow_ids"`
}

// CancelWorkflowConductorResponse is sent in response to cancel workflow requests
type CancelWorkflowConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// RecoveryConductorRequest is sent by the conductor to request recovery of pending workflows
type RecoveryConductorRequest struct {
	BaseMessage
	ExecutorIDs []string `json:"executor_ids"`
}

// RecoveryConductorResponse is sent in response to recovery requests
type RecoveryConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// ExistPendingWorkflowsConductorRequest is sent by the conductor to check for pending workflows
type ExistPendingWorkflowsConductorRequest struct {
	BaseMessage
	ExecutorID         string `json:"executor_id"`
	ApplicationVersion string `json:"application_version"`
}

// ExistPendingWorkflowsConductorResponse is sent in response to exist pending workflows requests
type ExistPendingWorkflowsConductorResponse struct {
	BaseResponse
	Exist bool `json:"exist"`
}

// ResumeWorkflowConductorRequest is sent by the conductor to resume a workflow
type ResumeWorkflowConductorRequest struct {
	BaseMessage
	WorkflowID  string   `json:"workflow_id"`
	WorkflowIDs []string `json:"workflow_ids"`
	QueueName   *string  `json:"queue_name,omitempty"`
}

// ResumeWorkflowConductorResponse is sent in response to resume workflow requests
type ResumeWorkflowConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// RetentionConductorRequestBody contains retention policy parameters
type RetentionConductorRequestBody struct {
	GCCutoffEpochMs      *int `json:"gc_cutoff_epoch_ms,omitempty"`
	GCRowsThreshold      *int `json:"gc_rows_threshold,omitempty"`
	TimeoutCutoffEpochMs *int `json:"timeout_cutoff_epoch_ms,omitempty"`
}

// RetentionConductorRequest is sent by the conductor to enforce retention policies
type RetentionConductorRequest struct {
	BaseMessage
	Body RetentionConductorRequestBody `json:"body"`
}

// RetentionConductorResponse is sent in response to retention requests
type RetentionConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// GetMetricsConductorRequest is sent by the conductor to request metrics
type GetMetricsConductorRequest struct {
	BaseMessage
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	MetricClass string `json:"metric_class"`
}

// GetMetricsConductorResponse is sent in response to metrics requests
type GetMetricsConductorResponse struct {
	BaseResponse
	Metrics []sysdb.MetricData `json:"metrics"`
}

// ExportWorkflowConductorRequest is sent by the conductor to export a workflow
type ExportWorkflowConductorRequest struct {
	BaseMessage
	WorkflowID     string `json:"workflow_id"`
	ExportChildren bool   `json:"export_children"`
}

// ExportWorkflowConductorResponse is sent in response to export workflow requests
type ExportWorkflowConductorResponse struct {
	BaseResponse
	SerializedWorkflow *string `json:"serialized_workflow,omitempty"`
}

// ImportWorkflowConductorRequest is sent by the conductor to import a workflow
type ImportWorkflowConductorRequest struct {
	BaseMessage
	SerializedWorkflow string `json:"serialized_workflow"`
}

// ImportWorkflowConductorResponse is sent in response to import workflow requests
type ImportWorkflowConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// DeleteWorkflowConductorRequest is sent by the conductor to delete workflow(s)
type DeleteWorkflowConductorRequest struct {
	BaseMessage
	WorkflowID     string   `json:"workflow_id"`
	WorkflowIDs    []string `json:"workflow_ids"`
	DeleteChildren bool     `json:"delete_children"`
}

// DeleteWorkflowConductorResponse is sent in response to delete workflow requests
type DeleteWorkflowConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// AlertRequest is sent by the conductor to deliver an alert
type AlertRequest struct {
	BaseMessage
	Name     string            `json:"name"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata"`
}

// AlertConductorResponse is sent in response to alert requests
type AlertConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

// ScheduleConductorOutput is the wire shape of a schedule sent to the conductor.
// Context is rendered when load_context is true on the request, otherwise omitted.
type ScheduleConductorOutput struct {
	ScheduleID        string  `json:"schedule_id"`
	ScheduleName      string  `json:"schedule_name"`
	WorkflowName      string  `json:"workflow_name"`
	WorkflowClassName *string `json:"workflow_class_name"`
	Schedule          string  `json:"schedule"`
	Status            string  `json:"status"`
	Context           *string `json:"context"`
	LastFiredAt       *string `json:"last_fired_at"`
	AutomaticBackfill bool    `json:"automatic_backfill"`
	CronTimezone      *string `json:"cron_timezone"`
	QueueName         *string `json:"queue_name"`
}

// ListSchedulesConductorRequestBody contains filter parameters for listing schedules.
type ListSchedulesConductorRequestBody struct {
	Status             StringOrList `json:"status,omitempty"`
	WorkflowName       StringOrList `json:"workflow_name,omitempty"`
	ScheduleNamePrefix StringOrList `json:"schedule_name_prefix,omitempty"`
	LoadContext        *bool        `json:"load_context,omitempty"`
}

type ListSchedulesConductorRequest struct {
	BaseMessage
	Body ListSchedulesConductorRequestBody `json:"body"`
}

type ListSchedulesConductorResponse struct {
	BaseResponse
	Output []ScheduleConductorOutput `json:"output"`
}

type GetScheduleConductorRequest struct {
	BaseMessage
	ScheduleName string `json:"schedule_name"`
	LoadContext  *bool  `json:"load_context,omitempty"`
}

type GetScheduleConductorResponse struct {
	BaseResponse
	Output *ScheduleConductorOutput `json:"output"`
}

type PauseScheduleConductorRequest struct {
	BaseMessage
	ScheduleName string `json:"schedule_name"`
}

type PauseScheduleConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

type ResumeScheduleConductorRequest struct {
	BaseMessage
	ScheduleName string `json:"schedule_name"`
}

type ResumeScheduleConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

type BackfillScheduleConductorRequest struct {
	BaseMessage
	ScheduleName string `json:"schedule_name"`
	Start        string `json:"start"` // ISO 8601
	End          string `json:"end"`   // ISO 8601
}

type BackfillScheduleConductorResponse struct {
	BaseResponse
	WorkflowIDs []string `json:"workflow_ids"`
}

type TriggerScheduleConductorRequest struct {
	BaseMessage
	ScheduleName string `json:"schedule_name"`
}

type TriggerScheduleConductorResponse struct {
	BaseResponse
	WorkflowID *string `json:"workflow_id"`
}

// QueueConductorOutput is the wire shape of a database-backed queue sent to the conductor.
type QueueConductorOutput struct {
	Name               string   `json:"name"`
	Concurrency        *int     `json:"concurrency"`
	WorkerConcurrency  *int     `json:"worker_concurrency"`
	RateLimitMax       *int     `json:"rate_limit_max"`
	RateLimitPeriodSec *float64 `json:"rate_limit_period_sec"`
	PriorityEnabled    bool     `json:"priority_enabled"`
	PartitionQueue     bool     `json:"partition_queue"`
	PollingIntervalSec float64  `json:"polling_interval_sec"`
}

// toQueueConductorOutput renders a queue config into its conductor wire shape.
func toQueueConductorOutput(q models.QueueConfig) QueueConductorOutput {
	out := QueueConductorOutput{
		Name:              q.Name,
		Concurrency:       q.GlobalConcurrency,
		WorkerConcurrency: q.WorkerConcurrency,
		PriorityEnabled:   q.PriorityEnabled,
		PartitionQueue:    q.PartitionQueue,
	}
	out.PollingIntervalSec = q.BasePollingInterval.Seconds()
	if rl := q.RateLimit; rl != nil {
		limit := rl.Limit
		period := rl.Period.Seconds()
		out.RateLimitMax = &limit
		out.RateLimitPeriodSec = &period
	}
	return out
}

type ListQueuesConductorRequest struct {
	BaseMessage
}

type ListQueuesConductorResponse struct {
	BaseResponse
	Output []QueueConductorOutput `json:"output"`
}

type GetQueueConductorRequest struct {
	BaseMessage
	Name string `json:"name"`
}

type GetQueueConductorResponse struct {
	BaseResponse
	Output *QueueConductorOutput `json:"output"`
}

// EventOutput is one entry returned by a get_workflow_events response.
// Value is the workflow event's value decoded from its recorded serialization and re-marshaled as JSON.
type EventOutput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NotificationOutput is one entry returned by a get_workflow_notifications response.
// Topic is nil when the notification was sent without a topic.
// Message is decoded from its recorded serialization and re-marshaled as JSON.
type NotificationOutput struct {
	Topic            *string `json:"topic"`
	Message          string  `json:"message"`
	CreatedAtEpochMs int64   `json:"created_at_epoch_ms"`
	Consumed         bool    `json:"consumed"`
}

// StreamEntryOutput is one entry returned by a get_workflow_streams response.
// Values are grouped by stream key and ordered by write offset; each value is JSON-marshaled.
type StreamEntryOutput struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

type GetWorkflowEventsConductorRequest struct {
	BaseMessage
	WorkflowID string `json:"workflow_id"`
}

type GetWorkflowEventsConductorResponse struct {
	BaseResponse
	Events []EventOutput `json:"events"`
}

type GetWorkflowNotificationsConductorRequest struct {
	BaseMessage
	WorkflowID string `json:"workflow_id"`
}

type GetWorkflowNotificationsConductorResponse struct {
	BaseResponse
	Notifications []NotificationOutput `json:"notifications"`
}

type GetWorkflowStreamsConductorRequest struct {
	BaseMessage
	WorkflowID string `json:"workflow_id"`
}

type GetWorkflowStreamsConductorResponse struct {
	BaseResponse
	Streams []StreamEntryOutput `json:"streams"`
}

// GetWorkflowAggregatesConductorRequestBody contains the workflow aggregate query parameters.
type GetWorkflowAggregatesConductorRequestBody struct {
	GroupByStatus             bool           `json:"group_by_status"`
	GroupByName               bool           `json:"group_by_name"`
	GroupByQueueName          bool           `json:"group_by_queue_name"`
	GroupByExecutorID         bool           `json:"group_by_executor_id"`
	GroupByApplicationVersion bool           `json:"group_by_application_version"`
	SelectCount               bool           `json:"select_count"`
	SelectMinCreatedAt        bool           `json:"select_min_created_at"`
	SelectMaxQueueWaitMs      bool           `json:"select_max_queue_wait_ms"`
	SelectMaxTotalLatencyMs   bool           `json:"select_max_total_latency_ms"`
	TimeBucketSizeMs          *int64         `json:"time_bucket_size_ms,omitempty"`
	Status                    StringOrList   `json:"status,omitempty"`
	StartTime                 *time.Time     `json:"start_time,omitempty"`       // ISO 8601
	EndTime                   *time.Time     `json:"end_time,omitempty"`         // ISO 8601
	CompletedAfter            *time.Time     `json:"completed_after,omitempty"`  // ISO 8601
	CompletedBefore           *time.Time     `json:"completed_before,omitempty"` // ISO 8601
	DequeuedAfter             *time.Time     `json:"dequeued_after,omitempty"`   // ISO 8601
	DequeuedBefore            *time.Time     `json:"dequeued_before,omitempty"`  // ISO 8601
	Name                      StringOrList   `json:"name,omitempty"`
	AppVersion                StringOrList   `json:"app_version,omitempty"`
	ExecutorID                StringOrList   `json:"executor_id,omitempty"`
	QueueName                 StringOrList   `json:"queue_name,omitempty"`
	WorkflowIDPrefix          StringOrList   `json:"workflow_id_prefix,omitempty"`
	WorkflowIDs               StringOrList   `json:"workflow_ids,omitempty"`
	ForkedFrom                StringOrList   `json:"forked_from,omitempty"`
	ParentWorkflowID          StringOrList   `json:"parent_workflow_id,omitempty"`
	User                      StringOrList   `json:"user,omitempty"`
	WasForkedFrom             *bool          `json:"was_forked_from,omitempty"`
	HasParent                 *bool          `json:"has_parent,omitempty"`
	Attributes                map[string]any `json:"attributes,omitempty"`
}

// GetWorkflowAggregatesConductorRequest is sent by the conductor to fetch workflow aggregates.
type GetWorkflowAggregatesConductorRequest struct {
	BaseMessage
	Body GetWorkflowAggregatesConductorRequestBody `json:"body"`
}

// GetWorkflowAggregatesConductorResponse is sent in response to workflow aggregate requests.
// Output uses sysdb.WorkflowAggregateRow directly: it has the matching JSON tags and there is no
// conversion needed between the public Go shape and the wire shape.
type GetWorkflowAggregatesConductorResponse struct {
	BaseResponse
	Output []sysdb.WorkflowAggregateRow `json:"output"`
}

// GetStepAggregatesConductorRequestBody contains the step aggregate query parameters.
type GetStepAggregatesConductorRequestBody struct {
	GroupByFunctionName bool         `json:"group_by_function_name"`
	GroupByStatus       bool         `json:"group_by_status"`
	SelectCount         bool         `json:"select_count"`
	SelectMaxDurationMs bool         `json:"select_max_duration_ms"`
	TimeBucketSizeMs    *int64       `json:"time_bucket_size_ms,omitempty"`
	Status              StringOrList `json:"status,omitempty"`
	FunctionName        StringOrList `json:"function_name,omitempty"`
	WorkflowIDPrefix    StringOrList `json:"workflow_id_prefix,omitempty"`
	CompletedAfter      *time.Time   `json:"completed_after,omitempty"`  // ISO 8601
	CompletedBefore     *time.Time   `json:"completed_before,omitempty"` // ISO 8601
}

// GetStepAggregatesConductorRequest is sent by the conductor to fetch step aggregates.
type GetStepAggregatesConductorRequest struct {
	BaseMessage
	Body GetStepAggregatesConductorRequestBody `json:"body"`
}

// GetStepAggregatesConductorResponse is sent in response to step aggregate requests.
// Output uses sysdb.StepAggregateRow directly: it has the matching JSON tags and there is no
// conversion needed between the public Go shape and the wire shape.
type GetStepAggregatesConductorResponse struct {
	BaseResponse
	Output []sysdb.StepAggregateRow `json:"output"`
}

// ApplicationVersionOutput is the wire shape for a single application version
// returned to the conductor.
type ApplicationVersionOutput struct {
	ID        string `json:"version_id"`
	Name      string `json:"version_name"`
	Timestamp int64  `json:"version_timestamp"`
	CreatedAt int64  `json:"created_at"`
}

func formatApplicationVersionOutput(v sysdb.VersionInfo) ApplicationVersionOutput {
	return ApplicationVersionOutput{
		ID:        v.ID,
		Name:      v.Name,
		Timestamp: v.Timestamp,
		CreatedAt: v.CreatedAt,
	}
}

// ListApplicationVersionsConductorRequest is sent by the conductor to list registered application versions.
type ListApplicationVersionsConductorRequest struct {
	BaseMessage
}

// ListApplicationVersionsConductorResponse is sent in response to list application version requests.
type ListApplicationVersionsConductorResponse struct {
	BaseResponse
	Output []ApplicationVersionOutput `json:"output"`
}

// SetLatestApplicationVersionConductorRequest is sent by the conductor to mark a version as latest.
type SetLatestApplicationVersionConductorRequest struct {
	BaseMessage
	VersionName string `json:"version_name"`
}

// SetLatestApplicationVersionConductorResponse is sent in response to set-latest requests.
type SetLatestApplicationVersionConductorResponse struct {
	BaseResponse
	Success bool `json:"success"`
}

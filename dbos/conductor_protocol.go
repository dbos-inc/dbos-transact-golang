package dbos

import (
	"encoding/json"
	"strconv"
	"time"
)

// stringOrList is a custom JSON type that accepts either a single string
// or an array of strings, matching the conductor's StringOrList for filter fields.
type stringOrList []string

func (s *stringOrList) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = stringOrList{single}
		return nil
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	*s = stringOrList(list)
	return nil
}

func (s stringOrList) toSlice() []string {
	return []string(s)
}

// messageType represents the type of message exchanged with the conductor
type messageType string

const (
	executorInfo                 messageType = "executor_info"
	recoveryMessage              messageType = "recovery"
	cancelWorkflowMessage        messageType = "cancel"
	resumeWorkflowMessage        messageType = "resume"
	listWorkflowsMessage         messageType = "list_workflows"
	listQueuedWorkflowsMessage   messageType = "list_queued_workflows"
	listStepsMessage             messageType = "list_steps"
	getWorkflowMessage           messageType = "get_workflow"
	forkWorkflowMessage          messageType = "fork_workflow"
	existPendingWorkflowsMessage messageType = "exist_pending_workflows"
	retentionMessage             messageType = "retention"
	getMetricsMessage            messageType = "get_metrics"
	exportWorkflowMessage        messageType = "export_workflow"
	importWorkflowMessage        messageType = "import_workflow"
	deleteWorkflowMessage        messageType = "delete"
	alertMessage                 messageType = "alert"
	listSchedulesMessage         messageType = "list_schedules"
	getScheduleMessage           messageType = "get_schedule"
	pauseScheduleMessage         messageType = "pause_schedule"
	resumeScheduleMessage        messageType = "resume_schedule"
	backfillScheduleMessage      messageType = "backfill_schedule"
	triggerScheduleMessage       messageType = "trigger_schedule"
	getWorkflowEventsMessage     messageType = "get_workflow_events"
	getWorkflowNotificationsMsg  messageType = "get_workflow_notifications"
	getWorkflowStreamsMessage    messageType = "get_workflow_streams"
	getWorkflowAggregatesMessage messageType = "get_workflow_aggregates"
	getStepAggregatesMessage     messageType = "get_step_aggregates"
	listAppVersionsMessage       messageType = "list_application_versions"
	setLatestAppVersionMessage   messageType = "set_latest_application_version"
	listQueuesMessage            messageType = "list_queues"
	getQueueMessage              messageType = "get_queue"
)

// baseMessage represents the common structure of all conductor messages
type baseMessage struct {
	Type      messageType `json:"type"`
	RequestID string      `json:"request_id"`
}

// baseResponse extends baseMessage with optional error handling
type baseResponse struct {
	baseMessage
	ErrorMessage *string `json:"error_message,omitempty"`
}

// executorInfoRequest is sent by the conductor to request executor information
type executorInfoRequest struct {
	baseMessage
}

// executorInfoResponse is sent in response to executor info requests
type executorInfoResponse struct {
	baseResponse
	ExecutorID         string         `json:"executor_id"`
	ApplicationVersion string         `json:"application_version"`
	Hostname           *string        `json:"hostname,omitempty"`
	DBOSVersion        string         `json:"dbos_version"`
	Language           string         `json:"language"`
	ExecutorMetadata   map[string]any `json:"executor_metadata,omitempty"`
}

// listWorkflowsConductorRequestBody contains filter parameters for listing workflows.
type listWorkflowsConductorRequestBody struct {
	WorkflowUUIDs      []string       `json:"workflow_uuids,omitempty"`
	WorkflowName       stringOrList   `json:"workflow_name,omitempty"`
	AuthenticatedUser  stringOrList   `json:"authenticated_user,omitempty"`
	StartTime          *time.Time     `json:"start_time,omitempty"`       // ISO 8601
	EndTime            *time.Time     `json:"end_time,omitempty"`         // ISO 8601
	CompletedAfter     *time.Time     `json:"completed_after,omitempty"`  // ISO 8601
	CompletedBefore    *time.Time     `json:"completed_before,omitempty"` // ISO 8601
	DequeuedAfter      *time.Time     `json:"dequeued_after,omitempty"`   // ISO 8601
	DequeuedBefore     *time.Time     `json:"dequeued_before,omitempty"`  // ISO 8601
	Status             stringOrList   `json:"status,omitempty"`
	ApplicationVersion stringOrList   `json:"application_version,omitempty"`
	ForkedFrom         stringOrList   `json:"forked_from,omitempty"`
	ParentWorkflowID   stringOrList   `json:"parent_workflow_id,omitempty"`
	WasForkedFrom      *bool          `json:"was_forked_from,omitempty"`
	HasParent          *bool          `json:"has_parent,omitempty"`
	QueueName          stringOrList   `json:"queue_name,omitempty"`
	Limit              *int           `json:"limit,omitempty"`
	Offset             *int           `json:"offset,omitempty"`
	SortDesc           bool           `json:"sort_desc"`
	WorkflowIDPrefix   stringOrList   `json:"workflow_id_prefix,omitempty"`
	LoadInput          bool           `json:"load_input"`
	LoadOutput         bool           `json:"load_output"`
	ExecutorID         stringOrList   `json:"executor_id,omitempty"`
	QueuesOnly         bool           `json:"queues_only"`
	Attributes         map[string]any `json:"attributes,omitempty"`
}

// listWorkflowsConductorRequest is sent by the conductor to list workflows
type listWorkflowsConductorRequest struct {
	baseMessage
	Body listWorkflowsConductorRequestBody `json:"body"`
}

// listWorkflowsConductorResponseBody represents a single workflow in the list response
type listWorkflowsConductorResponseBody struct {
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
}

// listWorkflowsConductorResponse is sent in response to list workflows requests
type listWorkflowsConductorResponse struct {
	baseResponse
	Output []listWorkflowsConductorResponseBody `json:"output"`
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
	if (wf.Status == WorkflowStatusPending) && !wf.StartedAt.IsZero() {
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

	return output
}

// listStepsConductorRequest is sent by the conductor to list workflow steps
type listStepsConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
	LoadOutput bool   `json:"load_output"`
	Limit      *int   `json:"limit,omitempty"`
	Offset     *int   `json:"offset,omitempty"`
}

// workflowStepsConductorResponseBody represents a single workflow step in the list response
type workflowStepsConductorResponseBody struct {
	FunctionID         int     `json:"function_id"`
	FunctionName       string  `json:"function_name"`
	Output             *string `json:"output,omitempty"`
	Error              *string `json:"error,omitempty"`
	ChildWorkflowID    *string `json:"child_workflow_id,omitempty"`
	StartedAtEpochMs   *string `json:"started_at_epoch_ms,omitempty"`
	CompletedAtEpochMs *string `json:"completed_at_epoch_ms,omitempty"`
}

// listStepsConductorResponse is sent in response to list steps requests
type listStepsConductorResponse struct {
	baseResponse
	Output *[]workflowStepsConductorResponseBody `json:"output,omitempty"`
}

// formatWorkflowStepsResponseBody converts StepInfo to workflowStepsConductorResponseBody for the conductor protocol
func formatWorkflowStepsResponseBody(step StepInfo) workflowStepsConductorResponseBody {
	output := workflowStepsConductorResponseBody{
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

// getWorkflowConductorRequest is sent by the conductor to get a specific workflow
type getWorkflowConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
	LoadInput  bool   `json:"load_input"`
	LoadOutput bool   `json:"load_output"`
}

// getWorkflowConductorResponse is sent in response to get workflow requests
type getWorkflowConductorResponse struct {
	baseResponse
	Output *listWorkflowsConductorResponseBody `json:"output,omitempty"`
}

// forkWorkflowConductorRequestBody contains the fork workflow parameters
type forkWorkflowConductorRequestBody struct {
	WorkflowID         string  `json:"workflow_id"`
	StartStep          int     `json:"start_step"`
	ApplicationVersion *string `json:"application_version,omitempty"`
	NewWorkflowID      *string `json:"new_workflow_id,omitempty"`
	QueueName          *string `json:"queue_name,omitempty"`
	QueuePartitionKey  *string `json:"queue_partition_key,omitempty"`
}

// forkWorkflowConductorRequest is sent by the conductor to fork a workflow
type forkWorkflowConductorRequest struct {
	baseMessage
	Body forkWorkflowConductorRequestBody `json:"body"`
}

// forkWorkflowConductorResponse is sent in response to fork workflow requests
type forkWorkflowConductorResponse struct {
	baseResponse
	NewWorkflowID *string `json:"new_workflow_id,omitempty"`
}

// cancelWorkflowConductorRequest is sent by the conductor to cancel a workflow
type cancelWorkflowConductorRequest struct {
	baseMessage
	CancelChildren bool     `json:"cancel_children"`
	WorkflowID     string   `json:"workflow_id"`
	WorkflowIDs    []string `json:"workflow_ids"`
}

// cancelWorkflowConductorResponse is sent in response to cancel workflow requests
type cancelWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// recoveryConductorRequest is sent by the conductor to request recovery of pending workflows
type recoveryConductorRequest struct {
	baseMessage
	ExecutorIDs []string `json:"executor_ids"`
}

// recoveryConductorResponse is sent in response to recovery requests
type recoveryConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// existPendingWorkflowsConductorRequest is sent by the conductor to check for pending workflows
type existPendingWorkflowsConductorRequest struct {
	baseMessage
	ExecutorID         string `json:"executor_id"`
	ApplicationVersion string `json:"application_version"`
}

// existPendingWorkflowsConductorResponse is sent in response to exist pending workflows requests
type existPendingWorkflowsConductorResponse struct {
	baseResponse
	Exist bool `json:"exist"`
}

// resumeWorkflowConductorRequest is sent by the conductor to resume a workflow
type resumeWorkflowConductorRequest struct {
	baseMessage
	WorkflowID  string   `json:"workflow_id"`
	WorkflowIDs []string `json:"workflow_ids"`
	QueueName   *string  `json:"queue_name,omitempty"`
}

// resumeWorkflowConductorResponse is sent in response to resume workflow requests
type resumeWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// retentionConductorRequestBody contains retention policy parameters
type retentionConductorRequestBody struct {
	GCCutoffEpochMs      *int `json:"gc_cutoff_epoch_ms,omitempty"`
	GCRowsThreshold      *int `json:"gc_rows_threshold,omitempty"`
	TimeoutCutoffEpochMs *int `json:"timeout_cutoff_epoch_ms,omitempty"`
}

// retentionConductorRequest is sent by the conductor to enforce retention policies
type retentionConductorRequest struct {
	baseMessage
	Body retentionConductorRequestBody `json:"body"`
}

// retentionConductorResponse is sent in response to retention requests
type retentionConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// getMetricsConductorRequest is sent by the conductor to request metrics
type getMetricsConductorRequest struct {
	baseMessage
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	MetricClass string `json:"metric_class"`
}

// getMetricsConductorResponse is sent in response to metrics requests
type getMetricsConductorResponse struct {
	baseResponse
	Metrics []metricData `json:"metrics"`
}

// exportWorkflowConductorRequest is sent by the conductor to export a workflow
type exportWorkflowConductorRequest struct {
	baseMessage
	WorkflowID     string `json:"workflow_id"`
	ExportChildren bool   `json:"export_children"`
}

// exportWorkflowConductorResponse is sent in response to export workflow requests
type exportWorkflowConductorResponse struct {
	baseResponse
	SerializedWorkflow *string `json:"serialized_workflow,omitempty"`
}

// importWorkflowConductorRequest is sent by the conductor to import a workflow
type importWorkflowConductorRequest struct {
	baseMessage
	SerializedWorkflow string `json:"serialized_workflow"`
}

// importWorkflowConductorResponse is sent in response to import workflow requests
type importWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// deleteWorkflowConductorRequest is sent by the conductor to delete workflow(s)
type deleteWorkflowConductorRequest struct {
	baseMessage
	WorkflowID     string   `json:"workflow_id"`
	WorkflowIDs    []string `json:"workflow_ids"`
	DeleteChildren bool     `json:"delete_children"`
}

// deleteWorkflowConductorResponse is sent in response to delete workflow requests
type deleteWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// alertRequest is sent by the conductor to deliver an alert
type alertRequest struct {
	baseMessage
	Name     string            `json:"name"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata"`
}

// alertConductorResponse is sent in response to alert requests
type alertConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// scheduleConductorOutput is the wire shape of a schedule sent to the conductor.
// Context is rendered when load_context is true on the request, otherwise omitted.
type scheduleConductorOutput struct {
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

// listSchedulesConductorRequestBody contains filter parameters for listing schedules.
type listSchedulesConductorRequestBody struct {
	Status             stringOrList `json:"status,omitempty"`
	WorkflowName       stringOrList `json:"workflow_name,omitempty"`
	ScheduleNamePrefix stringOrList `json:"schedule_name_prefix,omitempty"`
	LoadContext        *bool        `json:"load_context,omitempty"`
}

type listSchedulesConductorRequest struct {
	baseMessage
	Body listSchedulesConductorRequestBody `json:"body"`
}

type listSchedulesConductorResponse struct {
	baseResponse
	Output []scheduleConductorOutput `json:"output"`
}

type getScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
	LoadContext  *bool  `json:"load_context,omitempty"`
}

type getScheduleConductorResponse struct {
	baseResponse
	Output *scheduleConductorOutput `json:"output"`
}

type pauseScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
}

type pauseScheduleConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type resumeScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
}

type resumeScheduleConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type backfillScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
	Start        string `json:"start"` // ISO 8601
	End          string `json:"end"`   // ISO 8601
}

type backfillScheduleConductorResponse struct {
	baseResponse
	WorkflowIDs []string `json:"workflow_ids"`
}

type triggerScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
}

type triggerScheduleConductorResponse struct {
	baseResponse
	WorkflowID *string `json:"workflow_id"`
}

// queueConductorOutput is the wire shape of a database-backed queue sent to the conductor.
type queueConductorOutput struct {
	Name               string   `json:"name"`
	Concurrency        *int     `json:"concurrency"`
	WorkerConcurrency  *int     `json:"worker_concurrency"`
	RateLimitMax       *int     `json:"rate_limit_max"`
	RateLimitPeriodSec *float64 `json:"rate_limit_period_sec"`
	PriorityEnabled    bool     `json:"priority_enabled"`
	PartitionQueue     bool     `json:"partition_queue"`
	PollingIntervalSec float64  `json:"polling_interval_sec"`
}

// toQueueConductorOutput renders a WorkflowQueue into its conductor wire shape.
func toQueueConductorOutput(q Queue) queueConductorOutput {
	out := queueConductorOutput{
		Name:              q.GetName(),
		Concurrency:       q.GetGlobalConcurrency(),
		WorkerConcurrency: q.GetWorkerConcurrency(),
		PriorityEnabled:   q.GetPriorityEnabled(),
		PartitionQueue:    q.GetPartitionQueue(),
	}
	if wq, ok := q.(*WorkflowQueue); ok {
		out.PollingIntervalSec = wq.basePollingInterval.Seconds()
	}
	if rl := q.GetRateLimit(); rl != nil {
		limit := rl.Limit
		period := rl.Period.Seconds()
		out.RateLimitMax = &limit
		out.RateLimitPeriodSec = &period
	}
	return out
}

type listQueuesConductorRequest struct {
	baseMessage
}

type listQueuesConductorResponse struct {
	baseResponse
	Output []queueConductorOutput `json:"output"`
}

type getQueueConductorRequest struct {
	baseMessage
	Name string `json:"name"`
}

type getQueueConductorResponse struct {
	baseResponse
	Output *queueConductorOutput `json:"output"`
}

// eventOutput is one entry returned by a get_workflow_events response.
// Value is the workflow event's value decoded from its recorded serialization and re-marshaled as JSON.
type eventOutput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// notificationOutput is one entry returned by a get_workflow_notifications response.
// Topic is nil when the notification was sent without a topic.
// Message is decoded from its recorded serialization and re-marshaled as JSON.
type notificationOutput struct {
	Topic            *string `json:"topic"`
	Message          string  `json:"message"`
	CreatedAtEpochMs int64   `json:"created_at_epoch_ms"`
	Consumed         bool    `json:"consumed"`
}

// streamEntryOutput is one entry returned by a get_workflow_streams response.
// Values are grouped by stream key and ordered by write offset; each value is JSON-marshaled.
type streamEntryOutput struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

type getWorkflowEventsConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
}

type getWorkflowEventsConductorResponse struct {
	baseResponse
	Events []eventOutput `json:"events"`
}

type getWorkflowNotificationsConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
}

type getWorkflowNotificationsConductorResponse struct {
	baseResponse
	Notifications []notificationOutput `json:"notifications"`
}

type getWorkflowStreamsConductorRequest struct {
	baseMessage
	WorkflowID string `json:"workflow_id"`
}

type getWorkflowStreamsConductorResponse struct {
	baseResponse
	Streams []streamEntryOutput `json:"streams"`
}

// getWorkflowAggregatesConductorRequestBody contains the workflow aggregate query parameters.
type getWorkflowAggregatesConductorRequestBody struct {
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
	Status                    stringOrList   `json:"status,omitempty"`
	StartTime                 *time.Time     `json:"start_time,omitempty"`       // ISO 8601
	EndTime                   *time.Time     `json:"end_time,omitempty"`         // ISO 8601
	CompletedAfter            *time.Time     `json:"completed_after,omitempty"`  // ISO 8601
	CompletedBefore           *time.Time     `json:"completed_before,omitempty"` // ISO 8601
	DequeuedAfter             *time.Time     `json:"dequeued_after,omitempty"`   // ISO 8601
	DequeuedBefore            *time.Time     `json:"dequeued_before,omitempty"`  // ISO 8601
	Name                      stringOrList   `json:"name,omitempty"`
	AppVersion                stringOrList   `json:"app_version,omitempty"`
	ExecutorID                stringOrList   `json:"executor_id,omitempty"`
	QueueName                 stringOrList   `json:"queue_name,omitempty"`
	WorkflowIDPrefix          stringOrList   `json:"workflow_id_prefix,omitempty"`
	WorkflowIDs               stringOrList   `json:"workflow_ids,omitempty"`
	ForkedFrom                stringOrList   `json:"forked_from,omitempty"`
	ParentWorkflowID          stringOrList   `json:"parent_workflow_id,omitempty"`
	User                      stringOrList   `json:"user,omitempty"`
	WasForkedFrom             *bool          `json:"was_forked_from,omitempty"`
	HasParent                 *bool          `json:"has_parent,omitempty"`
	Attributes                map[string]any `json:"attributes,omitempty"`
}

// getWorkflowAggregatesConductorRequest is sent by the conductor to fetch workflow aggregates.
type getWorkflowAggregatesConductorRequest struct {
	baseMessage
	Body getWorkflowAggregatesConductorRequestBody `json:"body"`
}

// getWorkflowAggregatesConductorResponse is sent in response to workflow aggregate requests.
// Output uses WorkflowAggregateRow directly: it has the matching JSON tags and there is no
// conversion needed between the public Go shape and the wire shape.
type getWorkflowAggregatesConductorResponse struct {
	baseResponse
	Output []WorkflowAggregateRow `json:"output"`
}

// getStepAggregatesConductorRequestBody contains the step aggregate query parameters.
type getStepAggregatesConductorRequestBody struct {
	GroupByFunctionName bool         `json:"group_by_function_name"`
	GroupByStatus       bool         `json:"group_by_status"`
	SelectCount         bool         `json:"select_count"`
	SelectMaxDurationMs bool         `json:"select_max_duration_ms"`
	TimeBucketSizeMs    *int64       `json:"time_bucket_size_ms,omitempty"`
	Status              stringOrList `json:"status,omitempty"`
	FunctionName        stringOrList `json:"function_name,omitempty"`
	WorkflowIDPrefix    stringOrList `json:"workflow_id_prefix,omitempty"`
	CompletedAfter      *time.Time   `json:"completed_after,omitempty"`  // ISO 8601
	CompletedBefore     *time.Time   `json:"completed_before,omitempty"` // ISO 8601
}

// getStepAggregatesConductorRequest is sent by the conductor to fetch step aggregates.
type getStepAggregatesConductorRequest struct {
	baseMessage
	Body getStepAggregatesConductorRequestBody `json:"body"`
}

// getStepAggregatesConductorResponse is sent in response to step aggregate requests.
// Output uses StepAggregateRow directly: it has the matching JSON tags and there is no
// conversion needed between the public Go shape and the wire shape.
type getStepAggregatesConductorResponse struct {
	baseResponse
	Output []StepAggregateRow `json:"output"`
}

// applicationVersionOutput is the wire shape for a single application version
// returned to the conductor.
type applicationVersionOutput struct {
	ID        string `json:"version_id"`
	Name      string `json:"version_name"`
	Timestamp int64  `json:"version_timestamp"`
	CreatedAt int64  `json:"created_at"`
}

func formatApplicationVersionOutput(v VersionInfo) applicationVersionOutput {
	return applicationVersionOutput{
		ID:        v.ID,
		Name:      v.Name,
		Timestamp: v.Timestamp,
		CreatedAt: v.CreatedAt,
	}
}

// listApplicationVersionsConductorRequest is sent by the conductor to list registered application versions.
type listApplicationVersionsConductorRequest struct {
	baseMessage
}

// listApplicationVersionsConductorResponse is sent in response to list application version requests.
type listApplicationVersionsConductorResponse struct {
	baseResponse
	Output []applicationVersionOutput `json:"output"`
}

// setLatestApplicationVersionConductorRequest is sent by the conductor to mark a version as latest.
type setLatestApplicationVersionConductorRequest struct {
	baseMessage
	VersionName string `json:"version_name"`
}

// setLatestApplicationVersionConductorResponse is sent in response to set-latest requests.
type setLatestApplicationVersionConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

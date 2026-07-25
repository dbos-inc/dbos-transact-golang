package models

import (
	"time"
)

type ListWorkflowsInput struct {
	WorkflowIDs      []string
	Status           []WorkflowStatusType
	StartTime        time.Time
	EndTime          time.Time
	Name             []string
	AppVersion       []string
	User             []string
	Limit            *int
	Offset           *int
	SortDesc         bool
	WorkflowIDPrefix []string
	LoadInput        bool
	LoadOutput       bool
	QueueName        []string
	QueuesOnly       bool
	ExecutorIDs      []string
	ForkedFrom       []string
	ParentWorkflowID []string
	DeduplicationID  []string
	CompletedAfter   time.Time
	CompletedBefore  time.Time
	DequeuedAfter    time.Time
	DequeuedBefore   time.Time
	WasForkedFrom    *bool
	HasParent        *bool
	Attributes       map[string]any
	ScheduleName     []string
	IsDebounced      *bool
}

// Docs for the exported option and input types below live on their public
// aliases in dbos/aliases.go.

type ListWorkflowsOption func(*ListWorkflowsInput)

type ListSchedulesInput struct {
	Statuses             []ScheduleStatus
	WorkflowNames        []string
	ScheduleNamePrefixes []string
}

type ListSchedulesOption func(*ListSchedulesInput)

type GetWorkflowStepsInput struct {
	LoadOutput *bool
	Limit      *int
	Offset     *int
}

type GetWorkflowStepsOption func(*GetWorkflowStepsInput)

type ResumeWorkflowInput struct {
	QueueName string
}

type ResumeWorkflowOption func(*ResumeWorkflowInput)

type CancelWorkflowInput struct {
	CancelChildren bool
}

type CancelWorkflowOption func(*CancelWorkflowInput)

type ForkWorkflowInput struct {
	OriginalWorkflowID string // Required: The UUID of the original workflow to fork from
	ForkedWorkflowID   string // Optional: Custom workflow ID for the forked workflow (auto-generated if empty)
	StartStep          uint   // Optional: Step to start the forked workflow from (default: 0)
	ApplicationVersion string // Optional: Application version for the forked workflow (inherits from original if empty)
	QueueName          string // Optional: Queue to enqueue the forked workflow on (defaults to the internal queue)
	QueuePartitionKey  string // Optional: Partition key when enqueueing the forked workflow onto a partitioned queue
}

type GetWorkflowAggregatesInput struct {
	GroupByStatus             bool
	GroupByName               bool
	GroupByQueueName          bool
	GroupByExecutorID         bool
	GroupByApplicationVersion bool

	// Select* flags choose which aggregates to compute. At least one must be true.
	// MinCreatedAt is an epoch-ms timestamp; the latency fields are in milliseconds.
	SelectCount             bool
	SelectMinCreatedAt      bool
	SelectMaxQueueWaitMs    bool
	SelectMaxTotalLatencyMs bool

	// When non-zero, groups results by created_at time bucket of this size.
	TimeBucketSize time.Duration

	// Filters
	Status             []WorkflowStatusType
	StartTime          time.Time
	EndTime            time.Time
	CompletedAfter     time.Time
	CompletedBefore    time.Time
	DequeuedAfter      time.Time
	DequeuedBefore     time.Time
	Name               []string
	ApplicationVersion []string
	ExecutorID         []string
	QueueName          []string
	WorkflowIDPrefix   []string
	WorkflowIDs        []string
	AuthenticatedUser  []string
	ForkedFrom         []string
	ParentWorkflowID   []string
	WasForkedFrom      *bool
	HasParent          *bool

	Attributes map[string]any
}

type GetStepAggregatesInput struct {
	GroupByFunctionName bool
	GroupByStatus       bool

	SelectCount         bool
	SelectMaxDurationMs bool

	// When non-zero, groups results by completed_at time bucket of this size.
	TimeBucketSize time.Duration

	// Filters
	Status           []string
	FunctionName     []string
	WorkflowIDPrefix []string
	CompletedAfter   time.Time
	CompletedBefore  time.Time
}

type StepInfo struct {
	StepID          int       // The sequential ID of the step within the workflow
	StepName        string    // The name of the step function
	Output          any       // The output returned by the step (if any)
	Error           error     // The error returned by the step (if any)
	ChildWorkflowID string    // The ID of a child workflow spawned by this step (if applicable)
	StartedAt       time.Time `json:",omitzero"` // When the step execution started
	CompletedAt     time.Time `json:",omitzero"` // When the step execution completed
}

// AlertHandler is a function that handles alerts received from DBOS Conductor.
type AlertHandler func(name string, message string, metadata map[string]string)

package models

import "time"

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
}

// ListWorkflowsOption is a functional option for configuring workflow listing parameters.
type ListWorkflowsOption func(*ListWorkflowsInput)

type ListSchedulesInput struct {
	Statuses             []ScheduleStatus
	WorkflowNames        []string
	ScheduleNamePrefixes []string
}

// ListSchedulesOption is a functional option for configuring schedule listing parameters.
type ListSchedulesOption func(*ListSchedulesInput)

type GetWorkflowStepsInput struct {
	LoadOutput *bool
	Limit      *int
	Offset     *int
}

// GetWorkflowStepsOption is a functional option for GetWorkflowSteps.
type GetWorkflowStepsOption func(*GetWorkflowStepsInput)

type ResumeWorkflowInput struct {
	QueueName string
}

// ResumeWorkflowOption is a functional option for configuring workflow resumption.
type ResumeWorkflowOption func(*ResumeWorkflowInput)

type CancelWorkflowInput struct {
	CancelChildren bool
}

type CancelWorkflowOptions func(*CancelWorkflowInput)

// ForkWorkflowInput holds configuration parameters for forking workflows.
// OriginalWorkflowID is required. Other fields are optional.
type ForkWorkflowInput struct {
	OriginalWorkflowID string // Required: The UUID of the original workflow to fork from
	ForkedWorkflowID   string // Optional: Custom workflow ID for the forked workflow (auto-generated if empty)
	StartStep          uint   // Optional: Step to start the forked workflow from (default: 0)
	ApplicationVersion string // Optional: Application version for the forked workflow (inherits from original if empty)
	QueueName          string // Optional: Queue to enqueue the forked workflow on (defaults to the internal queue)
	QueuePartitionKey  string // Optional: Partition key when enqueueing the forked workflow onto a partitioned queue
}

// GetWorkflowAggregatesInput is the input to GetWorkflowAggregates.
//
// At least one of the GroupBy* flags must be true, or TimeBucketSize must be > 0.
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

// GetStepAggregatesInput is the input to GetStepAggregates.
//
// At least one of the GroupBy* flags must be true, or TimeBucketSize must be > 0.
// At least one of the Select* flags must be true.
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

func WithResumeQueue(queueName string) ResumeWorkflowOption {
	return func(o *ResumeWorkflowInput) {
		o.QueueName = queueName
	}
}

func WithWorkflowIDs(workflowIDs []string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.WorkflowIDs = workflowIDs
	}
}

func WithStatus(status []WorkflowStatusType) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.Status = status
	}
}

func WithStartTime(startTime time.Time) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.StartTime = startTime
	}
}

func WithEndTime(endTime time.Time) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.EndTime = endTime
	}
}

func WithName(name ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.Name = name
	}
}

func WithAppVersion(appVersion ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.AppVersion = appVersion
	}
}

func WithUser(user ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.User = user
	}
}

func WithLimit(limit int) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.Limit = &limit
	}
}

func WithOffset(offset int) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.Offset = &offset
	}
}

func WithSortDesc() ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.SortDesc = true
	}
}

func WithWorkflowIDPrefix(prefix ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.WorkflowIDPrefix = prefix
	}
}

func WithLoadInput(loadInput bool) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.LoadInput = loadInput
	}
}

func WithLoadOutput(loadOutput bool) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.LoadOutput = loadOutput
	}
}

func WithQueueName(queueName ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.QueueName = queueName
	}
}

func WithQueuesOnly() ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.QueuesOnly = true
	}
}

func WithExecutorIDs(executorIDs []string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.ExecutorIDs = executorIDs
	}
}

func WithForkedFrom(forkedFrom ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.ForkedFrom = forkedFrom
	}
}

func WithParentWorkflowID(parentWorkflowID ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.ParentWorkflowID = parentWorkflowID
	}
}

func WithFilterDeduplicationID(deduplicationID ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.DeduplicationID = deduplicationID
	}
}

func WithCompletedAfter(completedAfter time.Time) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.CompletedAfter = completedAfter
	}
}

func WithCompletedBefore(completedBefore time.Time) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.CompletedBefore = completedBefore
	}
}

func WithDequeuedAfter(dequeuedAfter time.Time) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.DequeuedAfter = dequeuedAfter
	}
}

func WithDequeuedBefore(dequeuedBefore time.Time) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.DequeuedBefore = dequeuedBefore
	}
}

func WithWasForkedFrom(wasForkedFrom bool) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.WasForkedFrom = &wasForkedFrom
	}
}

func WithHasParent(hasParent bool) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.HasParent = &hasParent
	}
}

func WithFilterAttributes(attributes map[string]any) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.Attributes = attributes
	}
}

func WithFilterScheduleName(scheduleName ...string) ListWorkflowsOption {
	return func(p *ListWorkflowsInput) {
		p.ScheduleName = scheduleName
	}
}

func WithStepsLoadOutput(loadOutput bool) GetWorkflowStepsOption {
	return func(o *GetWorkflowStepsInput) {
		o.LoadOutput = &loadOutput
	}
}

func WithStepsLimit(limit int) GetWorkflowStepsOption {
	return func(o *GetWorkflowStepsInput) {
		o.Limit = &limit
	}
}

func WithStepsOffset(offset int) GetWorkflowStepsOption {
	return func(o *GetWorkflowStepsInput) {
		o.Offset = &offset
	}
}

func WithScheduleStatuses(statuses ...ScheduleStatus) ListSchedulesOption {
	return func(o *ListSchedulesInput) { o.Statuses = statuses }
}

func WithScheduleWorkflowNames(names ...string) ListSchedulesOption {
	return func(o *ListSchedulesInput) { o.WorkflowNames = names }
}

func WithScheduleNamePrefixes(prefixes ...string) ListSchedulesOption {
	return func(o *ListSchedulesInput) { o.ScheduleNamePrefixes = prefixes }
}

func WithCancelChildren() CancelWorkflowOptions {
	return func(cwo *CancelWorkflowInput) {
		cwo.CancelChildren = true
	}
}

type StepInfo struct {
	StepID          int       // The sequential ID of the step within the workflow
	StepName        string    // The name of the step function
	Output          any       // The output returned by the step (if any)
	Error           error     // The error returned by the step (if any)
	ChildWorkflowID string    // The ID of a child workflow spawned by this step (if applicable)
	StartedAt       time.Time // When the step execution started
	CompletedAt     time.Time // When the step execution completed
}

package models

import "time"

// InternalQueueName is the reserved queue used internally by DBOS.
const InternalQueueName = "_dbos_internal_queue"

// Default queue polling/dequeue settings.
const (
	DefaultMaxTasksPerIteration = 100
	DefaultBasePollingInterval  = 1 * time.Second
)

// RateLimiter configures rate limiting for workflow queue execution.
// Rate limits prevent overwhelming external services and provide backpressure.
type RateLimiter struct {
	Limit  int           // Maximum number of workflows to start within the period
	Period time.Duration // Time period for the rate limit
}

// QueueConfig is the persisted configuration of a workflow queue, as stored in
// the queues table. The public dbos.WorkflowQueue wraps it with runtime-only
// registration state.
type QueueConfig struct {
	Name                 string        `json:"name"`
	WorkerConcurrency    *int          `json:"workerConcurrency,omitempty"`
	GlobalConcurrency    *int          `json:"concurrency,omitempty"`
	PriorityEnabled      bool          `json:"priorityEnabled,omitempty"`
	RateLimit            *RateLimiter  `json:"rateLimit,omitempty"`
	MaxTasksPerIteration int           `json:"maxTasksPerIteration"`
	PartitionQueue       bool          `json:"partitionQueue,omitempty"`
	BasePollingInterval  time.Duration `json:"-"`
	MaxPollingInterval   time.Duration `json:"-"`
	DatabaseBacked       bool          `json:"-"`
}

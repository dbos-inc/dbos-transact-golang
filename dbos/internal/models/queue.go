package models

import "time"

// InternalQueueName is the reserved queue used internally by DBOS.
const InternalQueueName = "_dbos_internal_queue"

// DefaultBasePollingInterval is the default queue polling interval.
const DefaultBasePollingInterval = 1 * time.Second

// RateLimiter's docs live on its public alias in dbos/aliases.go.
type RateLimiter struct {
	Limit  int           // Maximum number of workflows to start within the period
	Period time.Duration // Time period for the rate limit
}

// QueueConfig is the persisted configuration of a workflow queue, as stored in
// the queues table.
type QueueConfig struct {
	Name                string        `json:"name"`
	WorkerConcurrency   *int          `json:"workerConcurrency,omitempty"`
	GlobalConcurrency   *int          `json:"concurrency,omitempty"`
	PriorityEnabled     bool          `json:"priorityEnabled,omitempty"`
	RateLimit           *RateLimiter  `json:"rateLimit,omitempty"`
	PartitionQueue      bool          `json:"partitionQueue,omitempty"`
	BasePollingInterval time.Duration `json:"-"`
	MaxPollingInterval  time.Duration `json:"-"`
	DatabaseBacked      bool          `json:"-"`
}

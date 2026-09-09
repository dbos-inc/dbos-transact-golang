package models

import (
	"encoding/json"
	"time"
)

// InternalQueueName is the reserved queue used internally by DBOS.
const InternalQueueName = "_dbos_internal_queue"

// DefaultBasePollingInterval is the default queue polling interval.
const DefaultBasePollingInterval = 1 * time.Second

// RateLimiter's docs live on its public alias in dbos/aliases.go.
type RateLimiter struct {
	Limit  int           `json:"limit"` // Maximum number of workflows to start within the period
	Period time.Duration `json:"-"`     // Time period for the rate limit; rendered as period in seconds in JSON (see MarshalJSON)
}

// MarshalJSON renders Period as seconds (period), matching the other DBOS SDKs
// and the queues table, instead of Go's nanosecond Duration.
func (r RateLimiter) MarshalJSON() ([]byte, error) {
	type alias RateLimiter
	return json.Marshal(struct {
		alias
		Period float64 `json:"period"`
	}{alias: alias(r), Period: r.Period.Seconds()})
}

// UnmarshalJSON decodes the shape produced by MarshalJSON: period in seconds
// back into a Duration.
func (r *RateLimiter) UnmarshalJSON(data []byte) error {
	type alias RateLimiter
	aux := struct {
		*alias
		Period float64 `json:"period"`
	}{alias: (*alias)(r)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	r.Period = time.Duration(aux.Period * float64(time.Second))
	return nil
}

// QueueConfig is the persisted configuration of a workflow queue, as stored in
// the queues table.
type QueueConfig struct {
	Name                       string        `json:"name"`
	WorkerConcurrency          *int          `json:"worker_concurrency,omitempty"`
	GlobalConcurrency          *int          `json:"concurrency,omitempty"`
	PriorityEnabled            bool          `json:"priority_enabled,omitempty"`
	RateLimit                  *RateLimiter  `json:"rate_limit,omitempty"`
	PartitionQueue             bool          `json:"partition_queue,omitempty"`
	PartitionConcurrency       *int          `json:"partition_concurrency,omitempty"`
	PartitionWorkerConcurrency *int          `json:"partition_worker_concurrency,omitempty"`
	PartitionRateLimit         *RateLimiter  `json:"partition_rate_limit,omitempty"`
	BasePollingInterval        time.Duration `json:"-"`
	MaxPollingInterval         time.Duration `json:"-"`
	DatabaseBacked             bool          `json:"-"`
	ApplicationName            string        `json:"application_name,omitempty"`
}

// ResolvedQueueLimits holds every limit on a queue at the scope it is enforced at.
type ResolvedQueueLimits struct {
	GlobalConcurrency          *int
	WorkerConcurrency          *int
	RateLimit                  *RateLimiter
	PartitionConcurrency       *int
	PartitionWorkerConcurrency *int
	PartitionRateLimit         *RateLimiter
}

// HasPartitionLimits reports whether any per-partition limit is set.
func (q QueueConfig) HasPartitionLimits() bool {
	return q.PartitionConcurrency != nil || q.PartitionWorkerConcurrency != nil || q.PartitionRateLimit != nil
}

// IsPartitioned reports whether the queue dequeues per partition key.
func (q QueueConfig) IsPartitioned() bool {
	return q.PartitionQueue || q.HasPartitionLimits()
}

// IsLegacyPartitioned reports the deprecated mode where the queue-wide limits apply per partition.
func (q QueueConfig) IsLegacyPartitioned() bool {
	return q.PartitionQueue && !q.HasPartitionLimits()
}

// ResolveLimits maps each limit to the scope it is enforced at.
func (q QueueConfig) ResolveLimits() ResolvedQueueLimits {
	if q.IsLegacyPartitioned() {
		return ResolvedQueueLimits{
			PartitionConcurrency:       q.GlobalConcurrency,
			PartitionWorkerConcurrency: q.WorkerConcurrency,
			PartitionRateLimit:         q.RateLimit,
		}
	}
	return ResolvedQueueLimits{
		GlobalConcurrency:          q.GlobalConcurrency,
		WorkerConcurrency:          q.WorkerConcurrency,
		RateLimit:                  q.RateLimit,
		PartitionConcurrency:       q.PartitionConcurrency,
		PartitionWorkerConcurrency: q.PartitionWorkerConcurrency,
		PartitionRateLimit:         q.PartitionRateLimit,
	}
}

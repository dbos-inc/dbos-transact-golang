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
	Name                string        `json:"name"`
	WorkerConcurrency   *int          `json:"worker_concurrency,omitempty"`
	GlobalConcurrency   *int          `json:"concurrency,omitempty"`
	PriorityEnabled     bool          `json:"priority_enabled,omitempty"`
	RateLimit           *RateLimiter  `json:"rate_limit,omitempty"`
	PartitionQueue      bool          `json:"partition_queue,omitempty"`
	BasePollingInterval time.Duration `json:"-"`
	MaxPollingInterval  time.Duration `json:"-"`
	DatabaseBacked      bool          `json:"-"`
	ApplicationName     string        `json:"application_name,omitempty"`
}

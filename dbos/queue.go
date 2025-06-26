package dbos

import (
	"context"
)

// WorkflowQueue interface defines queue operations and properties
type WorkflowQueue interface {
	Enqueue(ctx context.Context, workflow any, input any) error
}

// RateLimiter represents a rate limiting configuration
/*
type RateLimiter struct {
	Limit  int
	Period int
}
*/

type workflowQueue struct {
	name              string
	workerConcurrency *int
	globalConcurrency *int
	priorityEnabled   bool
	// limiter           *RateLimiter
}

// QueueOption is a functional option for configuring a workflow queue
type QueueOption func(*workflowQueue)

// WithWorkerConcurrency sets the worker concurrency for the queue
func WithWorkerConcurrency(concurrency int) QueueOption {
	return func(q *workflowQueue) {
		q.workerConcurrency = &concurrency
	}
}

// WithGlobalConcurrency sets the global concurrency for the queue
func WithGlobalConcurrency(concurrency int) QueueOption {
	return func(q *workflowQueue) {
		q.globalConcurrency = &concurrency
	}
}

// WithPriorityEnabled enables or disables priority handling for the queue
func WithPriorityEnabled(enabled bool) QueueOption {
	return func(q *workflowQueue) {
		q.priorityEnabled = enabled
	}
}

// WithRateLimiter sets the rate limiter for the queue
/*
func WithRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *workflowQueue) {
		q.limiter = limiter
	}
}
*/

// NewWorkflowQueue creates a new workflow queue with optional configuration
func NewWorkflowQueue(name string, options ...QueueOption) workflowQueue {
	// Create queue with default settings
	q := &workflowQueue{
		name:              name,
		workerConcurrency: nil,
		globalConcurrency: nil,
		priorityEnabled:   false,
		//limiter:           nil,
	}

	// Apply functional options
	for _, option := range options {
		option(q)
	}

	return *q
}

// This does not work because the wrapped function has a different type (P, R) than any, any
// For this to work the queue interface itself must be generic
func (w *workflowQueue) Enqueue(ctx context.Context, params WorkflowParams, workflow WorkflowWrapperFunc[any, any], input any) error {
	params.IsEnqueue = true
	workflow(ctx, params, input)
	return nil
}

func (w *workflowQueue) Dequeue(ctx context.Context) error {
	return nil
}

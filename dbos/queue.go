package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

var workflowQueueRegistry = make(map[string]WorkflowQueue)

// RateLimiter represents a rate limiting configuration
type RateLimiter struct {
	Limit  int
	Period float64
}

type WorkflowQueue struct {
	Name              string
	WorkerConcurrency *int
	GlobalConcurrency *int
	PriorityEnabled   bool
	Limiter           *RateLimiter
}

// QueueOption is a functional option for configuring a workflow queue
type QueueOption func(*WorkflowQueue)

// WithWorkerConcurrency sets the worker concurrency for the queue
func WithWorkerConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.WorkerConcurrency = &concurrency
	}
}

// WithGlobalConcurrency sets the global concurrency for the queue
func WithGlobalConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.GlobalConcurrency = &concurrency
	}
}

// WithPriorityEnabled enables or disables priority handling for the queue
func WithPriorityEnabled(enabled bool) QueueOption {
	return func(q *WorkflowQueue) {
		q.PriorityEnabled = enabled
	}
}

// WithRateLimiter sets the rate limiter for the queue
func WithRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *WorkflowQueue) {
		q.Limiter = limiter
	}
}

// NewWorkflowQueue creates a new workflow queue with optional configuration
func NewWorkflowQueue(name string, options ...QueueOption) WorkflowQueue {
	if getExecutor() != nil {
		fmt.Println("warning: NewWorkflowQueue called after DBOS initialization, dynamic registration is not supported")
		return WorkflowQueue{}
	}
	if _, exists := workflowQueueRegistry[name]; exists {
		panic(NewConflictingRegistrationError(name))
	}

	// Create queue with default settings
	q := WorkflowQueue{
		Name:              name,
		WorkerConcurrency: nil,
		GlobalConcurrency: nil,
		PriorityEnabled:   false,
		Limiter:           nil,
	}

	// Apply functional options
	for _, option := range options {
		option(&q)
	}

	// Register the queue in the global registry
	workflowQueueRegistry[name] = q

	return q
}

func queueRunner(ctx context.Context) {
	const (
		baseInterval    = 1.0   // Base interval in seconds
		minInterval     = 1.0   // Minimum polling interval in seconds
		maxInterval     = 120.0 // Maximum polling interval in seconds
		backoffFactor   = 2.0   // Exponential backoff multiplier
		scalebackFactor = 0.9   // Scale back factor for successful iterations
		jitterMin       = 0.95  // Minimum jitter multiplier
		jitterMax       = 1.05  // Maximum jitter multiplier
	)

	pollingInterval := baseInterval

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Queue runner stopping due to context cancellation")
			return
		default:
			hasBackoffError := false

			// Iterate through all queues in the registry
			for queueName, queue := range workflowQueueRegistry {
				// Call DequeueWorkflows for each queue
				dequeuedWorkflows, err := getExecutor().systemDB.DequeueWorkflows(ctx, queue)
				if err != nil {
					if pgErr, ok := err.(*pgconn.PgError); ok {
						switch pgErr.Code {
						case pgerrcode.SerializationFailure:
							hasBackoffError = true
						case pgerrcode.LockNotAvailable:
							hasBackoffError = true
						}
					} else {
						fmt.Printf("Error dequeuing workflows from queue '%s': %v\n", queueName, err)
					}
					continue
				}

				// Print what was dequeued
				if len(dequeuedWorkflows) > 0 {
					fmt.Printf("Dequeued %d workflows from queue '%s': %v\n", len(dequeuedWorkflows), queueName, dequeuedWorkflows)
				}
				for _, workflow := range dequeuedWorkflows {
					// Find the workflow in the registry
					registeredWorkflow, exists := registry[workflow.name]
					if !exists {
						fmt.Println("Error: workflow function not found in registry:", workflow.name)
						continue
					}

					// Deserialize input
					var input any
					if len(workflow.input) > 0 {
						inputBytes, err := base64.StdEncoding.DecodeString(workflow.input)
						if err != nil {
							fmt.Printf("failed to decode input for workflow %s: %v\n", workflow.id, err)
							continue
						}
						buf := bytes.NewBuffer(inputBytes)
						dec := gob.NewDecoder(buf)
						if err := dec.Decode(&input); err != nil {
							fmt.Printf("failed to decode input for workflow %s: %v\n", workflow.id, err)
							continue
						}
					}

					_, err := registeredWorkflow(ctx, input, WithWorkflowID(workflow.id))
					if err != nil {
						fmt.Println("Error recovering workflow:", err)
					}
				}
			}

			// Adjust polling interval based on errors
			if hasBackoffError {
				// Increase polling interval using exponential backoff
				pollingInterval = math.Min(pollingInterval*backoffFactor, maxInterval)
			} else {
				// Scale back polling interval on successful iteration
				pollingInterval = math.Max(minInterval, pollingInterval*scalebackFactor)
			}

			// Apply jitter to the polling interval
			jitter := jitterMin + rand.Float64()*(jitterMax-jitterMin)
			sleepDuration := time.Duration(pollingInterval * jitter * float64(time.Second))

			// Sleep with jittered interval
			time.Sleep(sleepDuration)
		}
	}
}

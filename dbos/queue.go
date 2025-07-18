package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	workflowQueueRegistry    = make(map[string]WorkflowQueue)
	DBOS_INTERNAL_QUEUE_NAME = "_dbos_internal_queue"
)

// RateLimiter represents a rate limiting configuration
type RateLimiter struct {
	Limit  int
	Period float64
}

type WorkflowQueue struct {
	Name                 string
	WorkerConcurrency    *int
	GlobalConcurrency    *int
	PriorityEnabled      bool
	Limiter              *RateLimiter
	MaxTasksPerIteration uint
}

// QueueOption is a functional option for configuring a workflow queue
type QueueOption func(*WorkflowQueue)

func WithWorkerConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.WorkerConcurrency = &concurrency
	}
}

func WithGlobalConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.GlobalConcurrency = &concurrency
	}
}

func WithPriorityEnabled(enabled bool) QueueOption {
	return func(q *WorkflowQueue) {
		q.PriorityEnabled = enabled
	}
}

func WithRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *WorkflowQueue) {
		q.Limiter = limiter
	}
}

func WithMaxTasksPerIteration(maxTasks uint) QueueOption {
	return func(q *WorkflowQueue) {
		q.MaxTasksPerIteration = maxTasks
	}
}

// NewWorkflowQueue creates a new workflow queue with optional configuration
func NewWorkflowQueue(name string, options ...QueueOption) WorkflowQueue {
	if getExecutor() != nil {
		getLogger().Warn("NewWorkflowQueue called after DBOS initialization, dynamic registration is not supported")
		return WorkflowQueue{}
	}
	if _, exists := workflowQueueRegistry[name]; exists {
		panic(NewConflictingRegistrationError(name))
	}

	// Create queue with default settings
	q := WorkflowQueue{
		Name:                 name,
		WorkerConcurrency:    nil,
		GlobalConcurrency:    nil,
		PriorityEnabled:      false,
		Limiter:              nil,
		MaxTasksPerIteration: 100, // Default max tasks per iteration
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

	// XXX doing this lets the dequeue and the task invokation survive the context cancellation
	// We might be OK with not doing this. During the tests it results in all sorts of error inside the two functions above due to context cancellation
	runnerContext := context.WithoutCancel(ctx)
	for {
		hasBackoffError := false

		// Iterate through all queues in the registry
		for queueName, queue := range workflowQueueRegistry {
			getLogger().Debug("Processing queue", "queue_name", queueName)
			// Call DequeueWorkflows for each queue
			dequeuedWorkflows, err := getExecutor().systemDB.DequeueWorkflows(runnerContext, queue)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					switch pgErr.Code {
					case pgerrcode.SerializationFailure:
						hasBackoffError = true
					case pgerrcode.LockNotAvailable:
						hasBackoffError = true
					}
				} else {
					getLogger().Error("Error dequeuing workflows from queue", "queue_name", queueName, "error", err)
				}
				continue
			}

			// Print what was dequeued
			if len(dequeuedWorkflows) > 0 {
				getLogger().Debug("Dequeued workflows from queue", "queue_name", queueName, "workflows", dequeuedWorkflows)
			}
			for _, workflow := range dequeuedWorkflows {
				// Find the workflow in the registry
				registeredWorkflow, exists := registry[workflow.name]
				if !exists {
					getLogger().Error("workflow function not found in registry", "workflow_name", workflow.name)
					continue
				}

				// Deserialize input
				var input any
				if len(workflow.input) > 0 {
					inputBytes, err := base64.StdEncoding.DecodeString(workflow.input)
					if err != nil {
						getLogger().Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
					buf := bytes.NewBuffer(inputBytes)
					dec := gob.NewDecoder(buf)
					if err := dec.Decode(&input); err != nil {
						getLogger().Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
				}

				_, err := registeredWorkflow.wrappedFunction(runnerContext, input, WithWorkflowID(workflow.id))
				if err != nil {
					getLogger().Error("Error recovering workflow", "error", err)
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

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.Done():
			getLogger().Info("Queue runner stopping due to context cancellation")
			return
		case <-time.After(sleepDuration):
			// Continue to next iteration
		}
	}
}

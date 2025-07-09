package dbos

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

var workflowQueueRegistry = make(map[string]workflowQueue)

// WorkflowQueue interface defines queue operations and properties
type WorkflowQueue interface {
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
	if getExecutor() != nil {
		fmt.Println("warning: NewWorkflowQueue called after DBOS initialization, dynamic registration is not supported")
		return workflowQueue{}
	}
	if _, exists := workflowQueueRegistry[name]; exists {
		panic(NewConflictingRegistrationError(name))
	}

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

	// Register the queue in the global registry
	workflowQueueRegistry[name] = *q

	return *q
}

func queueRunner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Queue runner stopping due to context cancellation")
			return
		default:
			// Iterate through all queues in the registry
			for queueName, queue := range workflowQueueRegistry {
				// Call DequeueWorkflows for each queue
				dequeuedWorkflows, err := getExecutor().systemDB.DequeueWorkflows(ctx, queue)
				if err != nil {
					if pgErr, ok := err.(*pgconn.PgError); ok {
						// TODO: change for polling backoff strategy
						switch pgErr.Code {
						case pgerrcode.SerializationFailure:
							// Handle serialization failure
							fmt.Printf("Serialization failure for workflow in queue '%s': %v\n", queueName, pgErr)
						case pgerrcode.LockNotAvailable:
							// Handle lock not available
							fmt.Printf("Lock not available for workflow in queue '%s': %v\n", queueName, pgErr)
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

			// Sleep for 1 second between dequeue cycles
			time.Sleep(1 * time.Second)
		}
	}
}

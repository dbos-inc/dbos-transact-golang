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

const (
	_DBOS_INTERNAL_QUEUE_NAME        = "_dbos_internal_queue"
	_DEFAULT_MAX_TASKS_PER_ITERATION = 100
)

// RateLimiter represents a rate limiting configuration
type RateLimiter struct {
	Limit  int
	Period float64
}

type WorkflowQueue struct {
	Name                 string       `json:"name"`
	WorkerConcurrency    *int         `json:"workerConcurrency,omitempty"`
	GlobalConcurrency    *int         `json:"concurrency,omitempty"` // Different key to match other transact APIs
	PriorityEnabled      bool         `json:"priorityEnabled,omitempty"`
	RateLimit            *RateLimiter `json:"rateLimit,omitempty"` // Named after other Transact APIs
	MaxTasksPerIteration int          `json:"maxTasksPerIteration"`
}

// queueOption is a functional option for configuring a workflow queue
type queueOption func(*WorkflowQueue)

func WithWorkerConcurrency(concurrency int) queueOption {
	return func(q *WorkflowQueue) {
		q.WorkerConcurrency = &concurrency
	}
}

func WithGlobalConcurrency(concurrency int) queueOption {
	return func(q *WorkflowQueue) {
		q.GlobalConcurrency = &concurrency
	}
}

func WithPriorityEnabled(enabled bool) queueOption {
	return func(q *WorkflowQueue) {
		q.PriorityEnabled = enabled
	}
}

func WithRateLimiter(limiter *RateLimiter) queueOption {
	return func(q *WorkflowQueue) {
		q.RateLimit = limiter
	}
}

func WithMaxTasksPerIteration(maxTasks int) queueOption {
	return func(q *WorkflowQueue) {
		q.MaxTasksPerIteration = maxTasks
	}
}

// NewWorkflowQueue creates a new workflow queue with optional configuration
func NewWorkflowQueue(dbosCtx DBOSContext, name string, options ...queueOption) WorkflowQueue {
	ctx, ok := dbosCtx.(*dbosContext)
	if !ok {
		return WorkflowQueue{} // Do nothing if the concrete type is not dbosContext
	}
	if ctx.launched.Load() {
		panic("Cannot register workflow queue after DBOS has launched")
	}
	ctx.logger.Debug("Creating new workflow queue", "queue_name", name)

	if _, exists := ctx.queueRunner.workflowQueueRegistry[name]; exists {
		panic(newConflictingRegistrationError(name))
	}

	// Create queue with default settings
	q := WorkflowQueue{
		Name:                 name,
		WorkerConcurrency:    nil,
		GlobalConcurrency:    nil,
		PriorityEnabled:      false,
		RateLimit:            nil,
		MaxTasksPerIteration: _DEFAULT_MAX_TASKS_PER_ITERATION,
	}

	// Apply functional options
	for _, option := range options {
		option(&q)
	}

	// Register the queue in the global registry
	ctx.queueRunner.workflowQueueRegistry[name] = q

	return q
}

type queueRunner struct {
	// Queue runner iteration parameters
	baseInterval    float64
	minInterval     float64
	maxInterval     float64
	backoffFactor   float64
	scalebackFactor float64
	jitterMin       float64
	jitterMax       float64

	// Queue registry
	workflowQueueRegistry map[string]WorkflowQueue

	// Channel to signal completion back to the DBOS context
	completionChan chan bool
}

func newQueueRunner() *queueRunner {
	return &queueRunner{
		baseInterval:          1.0,
		minInterval:           1.0,
		maxInterval:           120.0,
		backoffFactor:         2.0,
		scalebackFactor:       0.9,
		jitterMin:             0.95,
		jitterMax:             1.05,
		workflowQueueRegistry: make(map[string]WorkflowQueue),
		completionChan:        make(chan bool),
	}
}

func (qr *queueRunner) listQueues() []WorkflowQueue {
	queues := make([]WorkflowQueue, 0, len(qr.workflowQueueRegistry))
	for _, queue := range qr.workflowQueueRegistry {
		queues = append(queues, queue)
	}
	return queues
}

func (qr *queueRunner) run(ctx *dbosContext) {
	pollingInterval := qr.baseInterval

	for {
		hasBackoffError := false

		// Iterate through all queues in the registry
		for queueName, queue := range qr.workflowQueueRegistry {
			// Call DequeueWorkflows for each queue
			dequeuedWorkflows, err := ctx.systemDB.DequeueWorkflows(ctx, queue, ctx.executorID, ctx.applicationVersion)
			if err != nil {
				if pgErr, ok := err.(*pgconn.PgError); ok {
					switch pgErr.Code {
					case pgerrcode.SerializationFailure:
						hasBackoffError = true
					case pgerrcode.LockNotAvailable:
						hasBackoffError = true
					}
				} else {
					ctx.logger.Error("Error dequeuing workflows from queue", "queue_name", queueName, "error", err)
				}
				continue
			}

			if len(dequeuedWorkflows) > 0 {
				ctx.logger.Debug("Dequeued workflows from queue", "queue_name", queueName, "workflows", dequeuedWorkflows)
			}
			for _, workflow := range dequeuedWorkflows {
				// Find the workflow in the registry
				registeredWorkflow, exists := ctx.workflowRegistry[workflow.name]
				if !exists {
					ctx.logger.Error("workflow function not found in registry", "workflow_name", workflow.name)
					continue
				}

				// Deserialize input
				var input any
				if len(workflow.input) > 0 {
					inputBytes, err := base64.StdEncoding.DecodeString(workflow.input)
					if err != nil {
						ctx.logger.Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
					buf := bytes.NewBuffer(inputBytes)
					dec := gob.NewDecoder(buf)
					if err := dec.Decode(&input); err != nil {
						ctx.logger.Error("failed to decode input for workflow", "workflow_id", workflow.id, "error", err)
						continue
					}
				}

				_, err := registeredWorkflow.wrappedFunction(ctx, input, WithWorkflowID(workflow.id))
				if err != nil {
					ctx.logger.Error("Error running queued workflow", "error", err)
				}
			}
		}

		// Adjust polling interval based on errors
		if hasBackoffError {
			// Increase polling interval using exponential backoff
			pollingInterval = math.Min(pollingInterval*qr.backoffFactor, qr.maxInterval)
		} else {
			// Scale back polling interval on successful iteration
			pollingInterval = math.Max(qr.minInterval, pollingInterval*qr.scalebackFactor)
		}

		// Apply jitter to the polling interval
		jitter := qr.jitterMin + rand.Float64()*(qr.jitterMax-qr.jitterMin)
		sleepDuration := time.Duration(pollingInterval * jitter * float64(time.Second))

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.Done():
			ctx.logger.Info("Queue runner stopping due to context cancellation", "cause", context.Cause(ctx))
			qr.completionChan <- true
			return
		case <-time.After(sleepDuration):
			// Continue to next iteration
		}
	}
}

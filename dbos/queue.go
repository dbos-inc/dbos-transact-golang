package dbos

import (
	"context"
	"log/slog"
	"math"
	"math/rand"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	_DBOS_INTERNAL_QUEUE_NAME        = "_dbos_internal_queue"
	_DEFAULT_MAX_TASKS_PER_ITERATION = 100
	_DEFAULT_BASE_INTERVAL           = 1.0
	_DEFAULT_MAX_INTERVAL            = 120.0
	_DEFAULT_MIN_INTERVAL            = 1.0
)

// RateLimiter configures rate limiting for workflow queue execution.
// Rate limits prevent overwhelming external services and provide backpressure.
type RateLimiter struct {
	Limit  int           // Maximum number of workflows to start within the period
	Period time.Duration // Time period for the rate limit
}

// WorkflowQueue defines a named queue for workflow execution.
// Queues provide controlled workflow execution with concurrency limits, priority scheduling, and rate limiting.
type WorkflowQueue struct {
	Name                 string       `json:"name"`                        // Unique queue name
	WorkerConcurrency    *int         `json:"workerConcurrency,omitempty"` // Max concurrent workflows per executor
	GlobalConcurrency    *int         `json:"concurrency,omitempty"`       // Max concurrent workflows across all executors
	PriorityEnabled      bool         `json:"priorityEnabled,omitempty"`   // Enable priority-based scheduling
	RateLimit            *RateLimiter `json:"rateLimit,omitempty"`         // Rate limiting configuration
	MaxTasksPerIteration int          `json:"maxTasksPerIteration"`        // Max workflows to dequeue per iteration
	PartitionQueue       bool         `json:"partitionQueue,omitempty"`    // Enable partitioned queue mode
}

// QueueOption is a functional option for configuring a workflow queue
type QueueOption func(*WorkflowQueue)

// WithWorkerConcurrency limits the number of workflows this executor can run concurrently from the queue.
// This provides per-executor concurrency control.
func WithWorkerConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.WorkerConcurrency = &concurrency
	}
}

// WithGlobalConcurrency limits the total number of workflows that can run concurrently from the queue
// across all executors. This provides global concurrency control.
func WithGlobalConcurrency(concurrency int) QueueOption {
	return func(q *WorkflowQueue) {
		q.GlobalConcurrency = &concurrency
	}
}

// WithPriorityEnabled enables priority-based scheduling for the queue.
// When enabled, workflows with lower priority numbers are executed first.
func WithPriorityEnabled() QueueOption {
	return func(q *WorkflowQueue) {
		q.PriorityEnabled = true
	}
}

// WithRateLimiter configures rate limiting for the queue to prevent overwhelming external services.
// The rate limiter enforces a maximum number of workflow starts within a time period.
func WithRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *WorkflowQueue) {
		q.RateLimit = limiter
	}
}

// WithMaxTasksPerIteration sets the maximum number of workflows to dequeue in a single iteration.
// This controls batch sizes for queue processing.
func WithMaxTasksPerIteration(maxTasks int) QueueOption {
	return func(q *WorkflowQueue) {
		q.MaxTasksPerIteration = maxTasks
	}
}

// WithPartitionQueue enables partitioned queue mode.
// When enabled, workflows can be enqueued with a partition key, and each partition
// has its own concurrency limits. This allows distributing work across dynamically
// created queue partitions.
func WithPartitionQueue() QueueOption {
	return func(q *WorkflowQueue) {
		q.PartitionQueue = true
	}
}

// NewWorkflowQueue creates a new workflow queue with the specified name and configuration options.
// The queue must be created before workflows can be enqueued to it using the WithQueue option in RunWorkflow.
// Queues provide controlled execution with support for concurrency limits, priority scheduling, and rate limiting.
//
// Example:
//
//	queue := dbos.NewWorkflowQueue(ctx, "email-queue",
//	    dbos.WithWorkerConcurrency(5),
//	    dbos.WithRateLimiter(&dbos.RateLimiter{
//	        Limit:  100,
//	        Period: 60 * time.Second, // 100 workflows per minute
//	    }),
//	    dbos.WithPriorityEnabled(),
//	)
//
//	// Enqueue workflows to this queue:
//	handle, err := dbos.RunWorkflow(ctx, SendEmailWorkflow, emailData, dbos.WithQueue("email-queue"))
func NewWorkflowQueue(dbosCtx DBOSContext, name string, options ...QueueOption) WorkflowQueue {
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

// QueueRunnerConfig configures the queue runner polling behavior.
type QueueRunnerConfig struct {
	BaseInterval float64 // seconds
	MinInterval  float64 // seconds
	MaxInterval  float64 // seconds
}

type queueRunner struct {
	logger *slog.Logger

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
	completionChan chan struct{}
}

func newQueueRunner(logger *slog.Logger, config QueueRunnerConfig) *queueRunner {
	if config.BaseInterval == 0 {
		config.BaseInterval = _DEFAULT_BASE_INTERVAL
	}
	if config.MinInterval == 0 {
		config.MinInterval = _DEFAULT_MIN_INTERVAL
	}
	if config.MaxInterval == 0 {
		config.MaxInterval = _DEFAULT_MAX_INTERVAL
	}

	return &queueRunner{
		baseInterval:          config.BaseInterval,
		minInterval:           config.MinInterval,
		maxInterval:           config.MaxInterval,
		backoffFactor:         2.0,
		scalebackFactor:       0.9,
		jitterMin:             0.95,
		jitterMax:             1.05,
		workflowQueueRegistry: make(map[string]WorkflowQueue),
		completionChan:        make(chan struct{}, 1),
		logger:                logger.With("service", "queue_runner"),
	}
}

func (qr *queueRunner) listQueues() []WorkflowQueue {
	queues := make([]WorkflowQueue, 0, len(qr.workflowQueueRegistry))
	for _, queue := range qr.workflowQueueRegistry {
		queues = append(queues, queue)
	}
	return queues
}

// getQueue returns the queue with the given name from the registry.
// Returns a pointer to the queue if found, or nil if the queue does not exist.
func (qr *queueRunner) getQueue(queueName string) *WorkflowQueue {
	if queue, exists := qr.workflowQueueRegistry[queueName]; exists {
		return &queue
	}
	return nil
}

func (qr *queueRunner) run(ctx *dbosContext) {
	pollingInterval := qr.baseInterval

	for {
		hasBackoffError := false

		// Iterate through all queues in the registry
		for _, queue := range qr.workflowQueueRegistry {
			// Build list of partition keys to dequeue from
			// Default to empty string for non-partitioned queues
			partitionKeys := []string{""}
			if queue.PartitionQueue {
				partitions, err := retryWithResult(ctx, func() ([]string, error) {
					return ctx.systemDB.getQueuePartitions(ctx, queue.Name)
				}, withRetrierLogger(qr.logger))
				if err != nil {
					qr.logger.Error("Error getting queue partitions", "queue_name", queue.Name, "error", err)
					continue
				}
				partitionKeys = partitions
			}

			// Dequeue from each partition (or once for non-partitioned queues)
			var dequeuedWorkflows []dequeuedWorkflow
			for _, partitionKey := range partitionKeys {
				workflows, shouldContinue := qr.dequeueWorkflows(ctx, queue, partitionKey, &hasBackoffError)
				if shouldContinue {
					continue
				}
				dequeuedWorkflows = append(dequeuedWorkflows, workflows...)
			}

			if len(dequeuedWorkflows) > 0 {
				qr.logger.Debug("Dequeued workflows from queue", "queue_name", queue.Name, "workflows", len(dequeuedWorkflows))
			}
			for _, workflow := range dequeuedWorkflows {
				// Find the workflow in the registry

				wfName, ok := ctx.workflowCustomNametoFQN.Load(workflow.name)
				if !ok {
					qr.logger.Error("Workflow not found in registry", "workflow_name", workflow.name)
					continue
				}

				registeredWorkflowAny, exists := ctx.workflowRegistry.Load(wfName.(string))
				if !exists {
					qr.logger.Error("workflow function not found in registry", "workflow_name", workflow.name)
					continue
				}
				registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
				if !ok {
					qr.logger.Error("invalid workflow registry entry type", "workflow_name", workflow.name)
					continue
				}

				// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
				_, err := registeredWorkflow.wrappedFunction(ctx, workflow.input, WithWorkflowID(workflow.id))
				if err != nil {
					qr.logger.Error("Error running queued workflow", "error", err)
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
		jitter := qr.jitterMin + rand.Float64()*(qr.jitterMax-qr.jitterMin) // #nosec G404 -- non-crypto jitter; acceptable
		sleepDuration := time.Duration(pollingInterval * jitter * float64(time.Second))

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.Done():
			qr.logger.Debug("Queue runner stopping due to context cancellation", "cause", context.Cause(ctx))
			qr.completionChan <- struct{}{}
			return
		case <-time.After(sleepDuration):
			// Continue to next iteration
		}
	}
}

// dequeueWorkflows dequeues workflows from a specific partition and handles errors.
// Returns the dequeued workflows and a boolean indicating whether to continue to the next iteration.
func (qr *queueRunner) dequeueWorkflows(ctx *dbosContext, queue WorkflowQueue, partitionKey string, hasBackoffError *bool) ([]dequeuedWorkflow, bool) {
	dequeuedWorkflows, err := retryWithResult(ctx, func() ([]dequeuedWorkflow, error) {
		return ctx.systemDB.dequeueWorkflows(ctx, dequeueWorkflowsInput{
			queue:              queue,
			executorID:         ctx.executorID,
			applicationVersion: ctx.applicationVersion,
			queuePartitionKey:  partitionKey,
		})
	}, withRetrierLogger(qr.logger))

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch pgErr.Code {
			case pgerrcode.SerializationFailure:
				*hasBackoffError = true
			case pgerrcode.LockNotAvailable:
				*hasBackoffError = true
			}
		} else {
			qr.logger.Error("Error dequeuing workflows from queue", "queue_name", queue.Name, "partition_key", partitionKey, "error", err)
		}
		return nil, true // Indicate to continue to next iteration
	}

	return dequeuedWorkflows, false // Success, don't continue
}

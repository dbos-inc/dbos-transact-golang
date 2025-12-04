package dbos

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	_DBOS_INTERNAL_QUEUE_NAME        = "_dbos_internal_queue"
	_DEFAULT_MAX_TASKS_PER_ITERATION = 100
	_DEFAULT_BASE_POLLING_INTERVAL   = 1 * time.Second
	_DEFAULT_MAX_POLLING_INTERVAL    = 120 * time.Second
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
	Name                 string        `json:"name"`                        // Unique queue name
	WorkerConcurrency    *int          `json:"workerConcurrency,omitempty"` // Max concurrent workflows per executor
	GlobalConcurrency    *int          `json:"concurrency,omitempty"`       // Max concurrent workflows across all executors
	PriorityEnabled      bool          `json:"priorityEnabled,omitempty"`   // Enable priority-based scheduling
	RateLimit            *RateLimiter  `json:"rateLimit,omitempty"`         // Rate limiting configuration
	MaxTasksPerIteration int           `json:"maxTasksPerIteration"`        // Max workflows to dequeue per iteration
	PartitionQueue       bool          `json:"partitionQueue,omitempty"`    // Enable partitioned queue mode
	basePollingInterval  time.Duration // Base polling interval (minimum, never poll faster)
	maxPollingInterval   time.Duration // Maximum polling interval (never poll slower)
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

// WithQueueBasePollingInterval sets the initial polling interval for the queue.
// This is the starting interval and the minimum - the queue will never poll faster than this.
// If not set (0), the queue will use the default base polling interval during creation.
func WithQueueBasePollingInterval(interval time.Duration) QueueOption {
	return func(q *WorkflowQueue) {
		q.basePollingInterval = interval
	}
}

// WithQueueMaxPollingInterval sets the maximum polling interval for the queue.
// The queue will never poll slower than this value, even when backing off due to errors.
// If not set (0), the queue will use the default max polling interval during creation.
func WithQueueMaxPollingInterval(interval time.Duration) QueueOption {
	return func(q *WorkflowQueue) {
		q.maxPollingInterval = interval
	}
}

// NewWorkflowQueue creates a new workflow queue with the specified name and configuration options.
// The queue must be created before workflows can be enqueued to it using the WithQueue option in RunWorkflow.
// Queues provide controlled execution with support for concurrency limits, priority scheduling, rate limiting, and polling intervals.
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
//	    dbos.WithQueueBasePollingInterval(1 * time.Second),
//	    dbos.WithQueueMaxPollingInterval(60 * time.Second),
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
		basePollingInterval:  _DEFAULT_BASE_POLLING_INTERVAL,
		maxPollingInterval:   _DEFAULT_MAX_POLLING_INTERVAL,
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
	logger *slog.Logger

	// Queue runner iteration parameters
	backoffFactor   float64
	scalebackFactor float64
	jitterMin       float64
	jitterMax       float64

	// Queue registry
	workflowQueueRegistry map[string]WorkflowQueue

	// WaitGroup to track all queue goroutines
	queueGoroutinesWg sync.WaitGroup

	// Channel to signal completion back to the DBOS context
	completionChan chan struct{}
}

func newQueueRunner(logger *slog.Logger) *queueRunner {
	return &queueRunner{
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
// Returns a pointer to the queue if found, or nil if it does not exist.
func (qr *queueRunner) getQueue(queueName string) *WorkflowQueue {
	queue, exists := qr.workflowQueueRegistry[queueName]
	if !exists {
		return nil
	}
	return &queue
}

// run starts a goroutine for each registered queue to handle polling independently.
func (qr *queueRunner) run(ctx *dbosContext) {
	for _, queue := range qr.workflowQueueRegistry {
		qr.queueGoroutinesWg.Add(1)
		go qr.runQueue(ctx, queue)
	}

	// Wait for all queue goroutines to complete
	qr.queueGoroutinesWg.Wait()
	qr.logger.Debug("All queue goroutines completed")
	qr.completionChan <- struct{}{}
}

func (qr *queueRunner) runQueue(ctx *dbosContext, queue WorkflowQueue) {
	defer qr.queueGoroutinesWg.Done()

	queueLogger := qr.logger.With("queue_name", queue.Name)
	// Current polling interval starts at the base interval and adjusts based on errors
	currentPollingInterval := queue.basePollingInterval

	for {
		hasBackoffError := false
		skipDequeue := false

		// Build list of partition keys to dequeue from
		// Default to empty string for non-partitioned queues
		partitionKeys := []string{""}
		if queue.PartitionQueue {
			partitions, err := retryWithResult(ctx, func() ([]string, error) {
				return ctx.systemDB.getQueuePartitions(ctx, queue.Name)
			}, withRetrierLogger(queueLogger))
			if err != nil {
				skipDequeue = true
				if pgErr, ok := err.(*pgconn.PgError); ok {
					switch pgErr.Code {
					case pgerrcode.SerializationFailure, pgerrcode.LockNotAvailable:
						hasBackoffError = true
					}
				} else {
					queueLogger.Error("Error getting queue partitions", "error", err)
				}
			} else {
				partitionKeys = partitions
			}
		}

		// Dequeue from each partition (or once for non-partitioned queues)
		if !skipDequeue {
			var dequeuedWorkflows []dequeuedWorkflow
			for _, partitionKey := range partitionKeys {
				workflows, shouldContinue := qr.dequeueWorkflows(ctx, queue, partitionKey, &hasBackoffError)
				if shouldContinue {
					continue
				}
				dequeuedWorkflows = append(dequeuedWorkflows, workflows...)
			}

			if len(dequeuedWorkflows) > 0 {
				queueLogger.Debug("Dequeued workflows from queue", "workflows", len(dequeuedWorkflows))
			}
			for _, workflow := range dequeuedWorkflows {
				// Find the workflow in the registry
				wfName, ok := ctx.workflowCustomNametoFQN.Load(workflow.name)
				if !ok {
					queueLogger.Error("Workflow not found in registry", "workflow_name", workflow.name)
					continue
				}

				registeredWorkflowAny, exists := ctx.workflowRegistry.Load(wfName.(string))
				if !exists {
					queueLogger.Error("workflow function not found in registry", "workflow_name", workflow.name)
					continue
				}
				registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
				if !ok {
					queueLogger.Error("invalid workflow registry entry type", "workflow_name", workflow.name)
					continue
				}

				// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
				_, err := registeredWorkflow.wrappedFunction(ctx, workflow.input, WithWorkflowID(workflow.id))
				if err != nil {
					queueLogger.Error("Error running queued workflow", "error", err)
				}
			}
		}

		// Adjust polling interval for this queue based on errors
		if hasBackoffError {
			// Increase polling interval using exponential backoff, but never exceed maxPollingInterval
			newInterval := time.Duration(float64(currentPollingInterval) * qr.backoffFactor)
			currentPollingInterval = min(newInterval, queue.maxPollingInterval)
		} else {
			// Scale back polling interval on successful iteration, but never go below base interval
			newInterval := time.Duration(float64(currentPollingInterval) * qr.scalebackFactor)
			currentPollingInterval = max(newInterval, queue.basePollingInterval)
		}

		// Apply jitter to this queue's polling interval
		jitter := qr.jitterMin + rand.Float64()*(qr.jitterMax-qr.jitterMin) // #nosec G404 -- non-crypto jitter; acceptable
		sleepDuration := time.Duration(float64(currentPollingInterval) * jitter)

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.Done():
			queueLogger.Debug("Queue goroutine stopping due to context cancellation", "cause", context.Cause(ctx))
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
			case pgerrcode.SerializationFailure, pgerrcode.LockNotAvailable:
				*hasBackoffError = true
			}
		} else {
			qr.logger.Error("Error dequeuing workflows from queue", "queue_name", queue.Name, "partition_key", partitionKey, "error", err)
		}
		return nil, true // Indicate to continue to next iteration
	}

	return dequeuedWorkflows, false // Success, don't continue
}

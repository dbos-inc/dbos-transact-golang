package dbos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

const _DEFAULT_MAX_POLLING_INTERVAL = 120 * time.Second

// workflowQueue is the concrete implementation behind the Queue handle: a
// queue's configuration plus runtime-only registration state.
type workflowQueue struct {
	Name                string        `json:"name"`                        // Unique queue name
	WorkerConcurrency   *int          `json:"workerConcurrency,omitempty"` // Max concurrent workflows per executor
	GlobalConcurrency   *int          `json:"concurrency,omitempty"`       // Max concurrent workflows across all executors
	PriorityEnabled     bool          `json:"priorityEnabled,omitempty"`   // Enable priority-based scheduling
	RateLimit           *RateLimiter  `json:"rateLimit,omitempty"`         // Rate limiting configuration
	PartitionQueue      bool          `json:"partitionQueue,omitempty"`    // Enable partitioned queue mode
	basePollingInterval time.Duration // Base polling interval (minimum, never poll faster)
	maxPollingInterval  time.Duration // Maximum polling interval (never poll slower)

	databaseBacked bool                    // Whether this queue's config lives in the queues table
	onConflict     QueueConflictResolution // Registration conflict policy
}

// toConfig converts to the persisted representation used by internal/sysdb.
func (q workflowQueue) toConfig() models.QueueConfig {
	return models.QueueConfig{
		Name:                q.Name,
		WorkerConcurrency:   q.WorkerConcurrency,
		GlobalConcurrency:   q.GlobalConcurrency,
		PriorityEnabled:     q.PriorityEnabled,
		RateLimit:           q.RateLimit,
		PartitionQueue:      q.PartitionQueue,
		BasePollingInterval: q.basePollingInterval,
		MaxPollingInterval:  q.maxPollingInterval,
		DatabaseBacked:      q.databaseBacked,
	}
}

// queueFromConfig builds a workflowQueue from its persisted representation.
// Registration-only state (onConflict) is not persisted and stays zero.
func queueFromConfig(cfg models.QueueConfig) workflowQueue {
	return workflowQueue{
		Name:                cfg.Name,
		WorkerConcurrency:   cfg.WorkerConcurrency,
		GlobalConcurrency:   cfg.GlobalConcurrency,
		PriorityEnabled:     cfg.PriorityEnabled,
		RateLimit:           cfg.RateLimit,
		PartitionQueue:      cfg.PartitionQueue,
		basePollingInterval: cfg.BasePollingInterval,
		maxPollingInterval:  cfg.MaxPollingInterval,
		databaseBacked:      cfg.DatabaseBacked,
	}
}

func queuesFromConfigs(cfgs []models.QueueConfig) []workflowQueue {
	queues := make([]workflowQueue, 0, len(cfgs))
	for _, cfg := range cfgs {
		queues = append(queues, queueFromConfig(cfg))
	}
	return queues
}

// Queue is a handle to a registered workflow queue. It is returned by
// [RegisterQueue], [RetrieveQueue] and [ListQueues].
//
// Database-backed queues (registered via [RegisterQueue]) can have their
// configuration updated at runtime through the Set* methods, which persist the
// change to the queues table; live workers pick it up on their next reconcile
// without a restart. The Set* methods return an error for queues that are not
// database-backed.
type Queue interface {
	GetName() string
	GetGlobalConcurrency() *int
	GetWorkerConcurrency() *int
	GetRateLimit() *RateLimiter
	GetPriorityEnabled() bool
	GetPartitionQueue() bool
	GetPollingInterval() time.Duration

	SetGlobalConcurrency(ctx Client, value *int) error
	SetWorkerConcurrency(ctx Client, value *int) error
	SetRateLimit(ctx Client, value *RateLimiter) error
	SetPriorityEnabled(ctx Client, value bool) error
	SetPartitionQueue(ctx Client, value bool) error
	SetPollingInterval(ctx Client, value time.Duration) error
}

// Compile-time check that *workflowQueue satisfies the Queue interface.
var _ Queue = (*workflowQueue)(nil)

func (q *workflowQueue) GetName() string            { return q.Name }
func (q *workflowQueue) GetGlobalConcurrency() *int { return q.GlobalConcurrency }
func (q *workflowQueue) GetWorkerConcurrency() *int { return q.WorkerConcurrency }
func (q *workflowQueue) GetRateLimit() *RateLimiter { return q.RateLimit }
func (q *workflowQueue) GetPriorityEnabled() bool   { return q.PriorityEnabled }
func (q *workflowQueue) GetPartitionQueue() bool    { return q.PartitionQueue }

func (q *workflowQueue) GetPollingInterval() time.Duration { return q.basePollingInterval }

// SetGlobalConcurrency updates the queue's global concurrency limit. Pass nil to clear it.
func (q *workflowQueue) SetGlobalConcurrency(ctx Client, value *int) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) { c.GlobalConcurrency = value })
}

// SetWorkerConcurrency updates the queue's per-executor concurrency limit. Pass nil to clear it.
func (q *workflowQueue) SetWorkerConcurrency(ctx Client, value *int) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) { c.WorkerConcurrency = value })
}

// SetRateLimit updates the queue's rate limiter. Pass nil to clear it.
func (q *workflowQueue) SetRateLimit(ctx Client, value *RateLimiter) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) { c.RateLimit = value })
}

// SetPriorityEnabled toggles priority-based scheduling for the queue.
func (q *workflowQueue) SetPriorityEnabled(ctx Client, value bool) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) { c.PriorityEnabled = value })
}

// SetPartitionQueue toggles partitioned queue mode.
//
// Switching an existing queue from unpartitioned to partitioned abandons any
// workflows already enqueued on it: they were enqueued without a partition key,
// and a partitioned queue only dequeues from its partitions, so they will never
// be dequeued.
func (q *workflowQueue) SetPartitionQueue(ctx Client, value bool) error {
	wasUnpartitioned := !q.PartitionQueue
	if err := q.applyConfigChange(ctx, func(c *workflowQueue) { c.PartitionQueue = value }); err != nil {
		return err
	}
	if value && wasUnpartitioned {
		if c, ok := ctx.(*dbosContext); ok {
			c.logger.Warn("Switched queue to partitioned mode; workflows already enqueued without a partition key will be abandoned and never dequeued", "queue_name", q.Name)
		}
	}
	return nil
}

// SetPollingInterval updates the queue's base polling interval: the cadence at
// which workers poll for new work and the floor that backoff scales back to.
// This does not reset a worker currently backed off above the base; the change
// takes effect immediately only when it raises the floor above the current
// interval, otherwise as the worker scales back down on successful iterations.
func (q *workflowQueue) SetPollingInterval(ctx Client, value time.Duration) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) { c.basePollingInterval = value })
}

// applyConfigChange persists a single configuration change for a database-backed
// queue. The read-modify-write runs in one transaction (see
// systemDatabase.updateQueueConfig): the latest persisted row is read, mutate
// applies the change, cross-field validation runs against the fresh values, and
// the row is written. On success the change is reflected on the receiver so its
// getters return the updated value.
func (q *workflowQueue) applyConfigChange(ctx Client, mutate func(*workflowQueue)) error {
	if !q.databaseBacked {
		return fmt.Errorf("queue %s: configuration can only be updated on database-backed queues registered via RegisterQueue", q.Name)
	}
	c, ok := ctx.(*dbosContext)
	if !ok {
		return errors.New("invalid DBOS context")
	}
	_, err := sysdb.RetryWithResult(c, func() (*models.QueueConfig, error) {
		return c.systemDB.UpdateQueueConfig(c, q.Name, func(fresh *models.QueueConfig) error {
			w := queueFromConfig(*fresh)
			mutate(&w)
			if err := validateQueueConfig(&w); err != nil {
				return err
			}
			*fresh = w.toConfig()
			return nil
		})
	}, sysdb.WithRetrierLogger(c.logger), sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction))
	if err != nil {
		return err
	}
	mutate(q)
	return nil
}

// QueueConflictResolution controls how RegisterQueue behaves when a queue with
// the same name already exists in the system database.
type QueueConflictResolution string

const (
	// QueueConflictUpdateIfLatestVersion overwrites the existing row only when the
	// running application version is the latest registered version. This is the
	// default and is safe for rolling deployments.
	QueueConflictUpdateIfLatestVersion QueueConflictResolution = "update_if_latest_version"
	// QueueConflictAlwaysUpdate always overwrites the existing row.
	QueueConflictAlwaysUpdate QueueConflictResolution = "always_update"
	// QueueConflictNeverUpdate leaves an existing row unchanged.
	QueueConflictNeverUpdate QueueConflictResolution = "never_update"
)

// QueueOption is a functional option for configuring a workflow queue
type QueueOption func(*workflowQueue)

// WithWorkerConcurrency limits the number of workflows this executor can run concurrently from the queue.
// This provides per-executor concurrency control.
func WithWorkerConcurrency(concurrency int) QueueOption {
	return func(q *workflowQueue) {
		q.WorkerConcurrency = &concurrency
	}
}

// WithGlobalConcurrency limits the total number of workflows that can run concurrently from the queue
// across all executors. This provides global concurrency control.
func WithGlobalConcurrency(concurrency int) QueueOption {
	return func(q *workflowQueue) {
		q.GlobalConcurrency = &concurrency
	}
}

// WithPriorityEnabled enables priority-based scheduling for the queue.
// When enabled, workflows with lower priority numbers are executed first.
func WithPriorityEnabled() QueueOption {
	return func(q *workflowQueue) {
		q.PriorityEnabled = true
	}
}

// WithRateLimiter configures rate limiting for the queue to prevent overwhelming external services.
// The rate limiter enforces a maximum number of workflow starts within a time period.
func WithRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *workflowQueue) {
		q.RateLimit = limiter
	}
}

// WithPartitionQueue enables partitioned queue mode.
// When enabled, workflows can be enqueued with a partition key, and each partition
// has its own concurrency limits. This allows distributing work across dynamically
// created queue partitions.
func WithPartitionQueue() QueueOption {
	return func(q *workflowQueue) {
		q.PartitionQueue = true
	}
}

// WithQueueBasePollingInterval sets the initial polling interval for the queue.
// This is the starting interval and the minimum - the queue will never poll faster than this.
// If not set (0), the queue will use the default base polling interval during creation.
func WithQueueBasePollingInterval(interval time.Duration) QueueOption {
	return func(q *workflowQueue) {
		q.basePollingInterval = interval
	}
}

// WithQueueMaxPollingInterval sets the maximum polling interval for the queue.
// The queue will never poll slower than this value, even when backing off due to errors.
// If not set (0), the queue will use the default max polling interval during creation.
func WithQueueMaxPollingInterval(interval time.Duration) QueueOption {
	return func(q *workflowQueue) {
		q.maxPollingInterval = interval
	}
}

// WithQueueOnConflict sets the conflict resolution policy used by RegisterQueue
// when a queue with the same name already exists in the system database.
func WithQueueOnConflict(policy QueueConflictResolution) QueueOption {
	return func(q *workflowQueue) {
		q.onConflict = policy
	}
}

// validateQueueConfig validates a queue's configuration, returning an error on
// invalid input. Mirrors the cross-language validation rules.
func validateQueueConfig(q *workflowQueue) error {
	if q.WorkerConcurrency != nil && q.GlobalConcurrency != nil && *q.WorkerConcurrency > *q.GlobalConcurrency {
		return models.NewInvalidOptionError(fmt.Sprintf("queue %s: concurrency must be greater than or equal to worker_concurrency", q.Name))
	}
	if q.basePollingInterval <= 0 {
		return models.NewInvalidOptionError(fmt.Sprintf("queue %s: polling interval must be positive", q.Name))
	}
	if q.RateLimit != nil {
		if q.RateLimit.Limit <= 0 {
			return models.NewInvalidOptionError(fmt.Sprintf("queue %s: rate limiter limit must be positive", q.Name))
		}
		if q.RateLimit.Period <= 0 {
			return models.NewInvalidOptionError(fmt.Sprintf("queue %s: rate limiter period must be positive", q.Name))
		}
	}
	return nil
}

// RegisterQueue registers a queue and persists its configuration in the system
// database. Its configuration is periodically reloaded by the queue runner
// so changes take effect without a restart.
//
// The returned Queue reflects the persisted configuration. Use WithQueueOnConflict
// to control what happens when a queue with the same name already exists.
//
// Example:
//
//	q, err := dbos.RegisterQueue(ctx, "email-queue",
//	    dbos.WithWorkerConcurrency(5),
//	    dbos.WithPriorityEnabled(),
//	)
func RegisterQueue(ctx Client, name string, options ...QueueOption) (Queue, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.RegisterQueue(ctx, name, options...)
}

func (c *dbosContext) RegisterQueue(_ Client, name string, options ...QueueOption) (Queue, error) {
	if name == models.InternalQueueName {
		err := models.NewInvalidOptionError(fmt.Sprintf("cannot register queue %q: the name is reserved for the DBOS internal queue", name))
		c.logger.Error("queue name conflict", "queue_name", name, "error", err)
		return nil, err
	}

	q := workflowQueue{
		Name:                name,
		basePollingInterval: models.DefaultBasePollingInterval,
		maxPollingInterval:  _DEFAULT_MAX_POLLING_INTERVAL,
		onConflict:          QueueConflictUpdateIfLatestVersion,
		databaseBacked:      true,
	}
	for _, option := range options {
		option(&q)
	}
	// The maximum polling interval is a worker-local backoff ceiling that is not
	// persisted for database-backed queues (the worker derives it from the base
	// interval). Ignore an explicit WithQueueMaxPollingInterval override and warn.
	if q.maxPollingInterval != _DEFAULT_MAX_POLLING_INTERVAL {
		c.logger.Warn("WithQueueMaxPollingInterval is ignored for database-backed queues; the maximum polling interval is derived from the base polling interval", "queue_name", name)
		q.maxPollingInterval = _DEFAULT_MAX_POLLING_INTERVAL
	}
	if err := validateQueueConfig(&q); err != nil {
		return nil, err
	}

	// Resolve the conflict policy into whether an existing row should be overwritten.
	var updateExisting bool
	switch q.onConflict {
	case QueueConflictAlwaysUpdate:
		updateExisting = true
	case QueueConflictNeverUpdate:
		updateExisting = false
	default: // QueueConflictUpdateIfLatestVersion
		latest, err := sysdb.RetryWithResult(c, func() (*VersionInfo, error) {
			return c.systemDB.GetLatestApplicationVersion(c, nil)
		}, sysdb.WithRetrierLogger(c.logger))
		switch {
		case errors.Is(err, ErrNoApplicationVersions):
			// No registered versions yet: this process is the first, hence the latest.
			updateExisting = true
		case err != nil:
			// Don't silently overwrite on an unknown failure.
			c.logger.Error("failed to look up latest application version", "queue_name", name, "error", err)
			return nil, fmt.Errorf("failed to look up latest application version for queue %s: %w", name, err)
		default:
			updateExisting = latest.Name == c.applicationVersion
		}
	}

	inserted, err := sysdb.RetryWithResult(c, func() (bool, error) {
		return c.systemDB.UpsertQueue(c, sysdb.UpsertQueueDBInput{Queue: q.toConfig(), UpdateExisting: updateExisting})
	}, sysdb.WithRetrierLogger(c.logger), sysdb.WithRetryCondition(c.systemDB.Dialect().IsRetryableTransaction))
	if err != nil {
		return nil, err
	}
	persistedCfg, err := sysdb.RetryWithResult(c, func() (*models.QueueConfig, error) {
		return c.systemDB.GetQueue(c, name)
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}
	if persistedCfg == nil {
		return nil, fmt.Errorf("queue %s missing from database after upsert", name)
	}
	if inserted {
		c.logger.Info("Registered database-backed queue", "queue_name", name)
	}
	persisted := queueFromConfig(*persistedCfg)
	return &persisted, nil
}

// RetrieveQueue returns the queue with the given name. If no such queue
// has been registered, it returns an error matching ErrQueueNotFound.
func RetrieveQueue(ctx Client, name string) (Queue, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.RetrieveQueue(ctx, name)
}

func (c *dbosContext) RetrieveQueue(_ Client, name string) (Queue, error) {
	cfg, err := sysdb.RetryWithResult(c, func() (*models.QueueConfig, error) {
		return c.systemDB.GetQueue(c, name)
	}, sysdb.WithRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, models.NewQueueNotFoundError(name)
	}
	q := queueFromConfig(*cfg)
	return &q, nil
}

// ListQueues returns all queues registered in the system database.
func ListQueues(ctx Client) ([]Queue, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListQueues(ctx)
}

func (c *dbosContext) ListQueues(_ Client) ([]Queue, error) {
	cfgs, err := sysdb.RetryWithResult(c, func() ([]models.QueueConfig, error) {
		return c.systemDB.ListQueues(c)
	}, sysdb.WithRetrierLogger(c.logger))
	queues := queuesFromConfigs(cfgs)
	if err != nil {
		return nil, err
	}
	result := make([]Queue, len(queues))
	for i := range queues {
		q := queues[i]
		result[i] = &q
	}
	return result, nil
}

// DeleteQueue removes a database-backed queue by name from the system
// database. Processes serving the queue stop doing so at their next reconcile
// tick. Deleting a queue that does not exist is not an error.
func DeleteQueue(ctx Client, name string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.DeleteQueue(ctx, name)
}

func (c *dbosContext) DeleteQueue(_ Client, name string) error {
	return sysdb.Retry(c, func() error {
		return c.systemDB.DeleteQueue(c, name)
	}, sysdb.WithRetrierLogger(c.logger))
}

type queueRunner struct {
	logger *slog.Logger

	// Queue runner iteration parameters
	backoffFactor   float64
	scalebackFactor float64
	jitterMin       float64
	jitterMax       float64

	// The DBOS internal queue: the only queue that lives in-process rather than
	// in the queues table. Always available and always listened to.
	internalQueue workflowQueue

	// listenedQueues is the explicit set of queue names this process listens to.
	listenMu       sync.Mutex
	listenedQueues map[string]bool

	// currentQueues holds the latest reconciled set of queues this process runs
	// workers for (the in-memory registry plus database-backed queues, filtered by
	// the listen set). The supervisor rebuilds it once per reconcile tick by
	// replacing the reference, never mutating in place; workers read their
	// configuration from it.
	currentMu     sync.RWMutex
	currentQueues map[string]workflowQueue

	// WaitGroup to track all queue goroutines
	queueGoroutinesWg sync.WaitGroup

	// Channel to signal completion back to the DBOS context
	completionChan chan struct{}
}

func newQueueRunner(logger *slog.Logger) *queueRunner {
	return &queueRunner{
		backoffFactor:   2.0,
		scalebackFactor: 0.9,
		jitterMin:       0.95,
		jitterMax:       1.05,
		internalQueue: workflowQueue{
			Name:                models.InternalQueueName,
			basePollingInterval: models.DefaultBasePollingInterval,
			maxPollingInterval:  _DEFAULT_MAX_POLLING_INTERVAL,
		},
		listenedQueues: make(map[string]bool),
		currentQueues:  make(map[string]workflowQueue),
		completionChan: make(chan struct{}, 1),
		logger:         logger.With("service", "queue_runner"),
	}
}

// run supervises queue workers. On each reconcile tick it rebuilds the set of
// queues to listen to (the in-memory registry plus database-backed queues from
// the queues table), starts a worker goroutine for any queue that lacks a live
// one, and transitions delayed workflows centrally. This lets database-backed
// queues registered after launch be picked up without a restart. run blocks
// until the context is cancelled, then waits for all workers to stop.
func (qr *queueRunner) run(ctx *dbosContext) {
	defer func() {
		// Workers stop on context cancellation; wait for them before signalling.
		qr.queueGoroutinesWg.Wait()
		qr.logger.Debug("All queue goroutines completed")
		qr.completionChan <- struct{}{}
	}()

	// Track a done channel per running worker so we can tell whether a worker has
	// exited (e.g. because its database-backed queue was deleted) and respawn it
	// if the queue reappears.
	workerDone := make(map[string]chan struct{})

	const reconcileInterval = 1 * time.Second
	for ctx.Err() == nil { // While ctx is not cancelled
		// Transition any DELAYED workflows whose delay has expired to ENQUEUED.
		if err := sysdb.Retry(ctx, func() error {
			return ctx.systemDB.TransitionDelayedWorkflows(ctx)
		}, sysdb.WithRetrierLogger(qr.logger)); err != nil {
			qr.logger.Warn("Exception transitioning delayed workflows", "error", err)
		}

		// Rebuild the listen set each tick so changes made via ListenQueues
		// after launch take effect dynamically.
		for name, queue := range qr.queuesToListen(ctx) {
			if done, exists := workerDone[name]; exists {
				select {
				case <-done: // worker exited; go to respawn
				default:
					continue // still running
				}
			}
			done := make(chan struct{})
			workerDone[name] = done
			qr.queueGoroutinesWg.Add(1)
			go func(q workflowQueue, done chan struct{}) {
				defer qr.queueGoroutinesWg.Done()
				defer close(done)
				qr.runQueue(ctx, q)
			}(queue, done)
		}

		select {
		case <-ctx.Done():
		case <-time.After(reconcileInterval):
		}
	}
	qr.logger.Debug("Queue supervisor stopping due to context cancellation", "cause", context.Cause(ctx))
}

// queuesToListen rebuilds and publishes the set of queues (qr.currentQueues) this process should
// run workers for, combining the in-memory registry with database-backed queues
// (from a single listQueues call) and applying the listen filter set by
// ListenQueues. An empty listen set means listen to every queue. The internal
// queue is always included.
func (qr *queueRunner) queuesToListen(ctx *dbosContext) map[string]workflowQueue {
	// Snapshot the listen set; ListenQueues may mutate it concurrently after launch.
	qr.listenMu.Lock()
	listen := make(map[string]bool, len(qr.listenedQueues))
	for name := range qr.listenedQueues {
		listen[name] = true
	}
	qr.listenMu.Unlock()
	hasListenFilter := len(listen) > 0

	current := make(map[string]workflowQueue)

	// The internal queue is always listened to, regardless of the listen filter.
	current[models.InternalQueueName] = qr.internalQueue

	dbQueueCfgs, err := sysdb.RetryWithResult(ctx, func() ([]models.QueueConfig, error) {
		return ctx.systemDB.ListQueues(ctx)
	}, sysdb.WithRetrierLogger(qr.logger))
	dbQueues := queuesFromConfigs(dbQueueCfgs)
	if err != nil {
		// Return a snapshot of the current set in case of transient errors
		qr.logger.Warn("Exception listing database-backed queues", "error", err)
		for name, queue := range qr.snapshotCurrentQueues() {
			if !queue.databaseBacked || (hasListenFilter && !listen[name]) {
				continue
			}
			current[name] = queue
		}
	} else {
		for _, queue := range dbQueues {
			if hasListenFilter && !listen[queue.Name] {
				continue
			}
			current[queue.Name] = queue
		}
	}

	// Publish new set of queues
	qr.currentMu.Lock()
	qr.currentQueues = current
	qr.currentMu.Unlock()

	return current
}

// snapshotCurrentQueues returns the most recently published set of queues this
// process runs workers for. The returned map must not be mutated.
func (qr *queueRunner) snapshotCurrentQueues() map[string]workflowQueue {
	qr.currentMu.RLock()
	defer qr.currentMu.RUnlock()
	return qr.currentQueues
}

// currentQueueConfig returns the latest published configuration for a queue and
// whether it is still in the reconciled set (i.e. still exists and is listened).
func (qr *queueRunner) currentQueueConfig(name string) (workflowQueue, bool) {
	qr.currentMu.RLock()
	defer qr.currentMu.RUnlock()
	q, ok := qr.currentQueues[name]
	return q, ok
}

func (qr *queueRunner) runQueue(ctx *dbosContext, queue workflowQueue) {
	queueLogger := qr.logger.With("queue_name", queue.Name)
	// Current polling interval starts at the base interval and adjusts based on errors
	currentPollingInterval := queue.basePollingInterval

	for {
		// Reload database-backed queue configuration each iteration so runtime
		// changes (concurrency, rate limits, polling cadence) take effect.
		// If the queue is gone from the set (deleted or no longer listened), stop
		// the worker; the supervisor respawns it should it reappear.
		if queue.databaseBacked {
			fresh, ok := qr.currentQueueConfig(queue.Name)
			if !ok {
				queueLogger.Info("Queue no longer present in the reconciled set, stopping worker")
				return
			}
			// maxPollingInterval is a worker-local backoff ceiling that is not
			// persisted in the queues table, so the reloaded config leaves it unset.
			// Derive it from the (possibly updated) base interval here.
			fresh.maxPollingInterval = max(fresh.basePollingInterval, _DEFAULT_MAX_POLLING_INTERVAL)
			queue = fresh
			// Keep the current polling interval within the (possibly updated) bounds.
			currentPollingInterval = max(queue.basePollingInterval, min(currentPollingInterval, queue.maxPollingInterval))
		}

		hasBackoffError := false
		skipDequeue := false

		// Build list of partition keys to dequeue from
		// Default to empty string for non-partitioned queues
		partitionKeys := []string{""}
		if queue.PartitionQueue {
			partitions, err := sysdb.RetryWithResult(ctx, func() ([]string, error) {
				return ctx.systemDB.GetQueuePartitions(ctx, queue.Name)
			}, sysdb.WithRetrierLogger(queueLogger))
			if err != nil {
				skipDequeue = true
				if ctx.systemDB.IsContentionError(err) {
					hasBackoffError = true
				} else {
					queueLogger.Error("Error getting queue partitions", "error", err)
				}
			} else {
				partitionKeys = partitions
			}
		}

		// Dequeue from each partition (or once for non-partitioned queues)
		if !skipDequeue {
			var dequeuedWorkflows []sysdb.DequeuedWorkflow
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
				// Find the workflow in the registry. Configured instance workflows are
				// registered under a name qualified with their config name.
				lookupName := workflow.Name
				if workflow.ConfigName != nil && *workflow.ConfigName != "" {
					lookupName = instanceQualifiedName(workflow.Name, *workflow.ConfigName)
				}
				wfName, ok := ctx.workflowCustomNametoFQN.Load(lookupName)
				if !ok {
					queueLogger.Error("Workflow not found in registry", "workflow_name", workflow.Name)
					continue
				}

				registeredWorkflowAny, exists := ctx.workflowRegistry.Load(wfName.(string))
				if !exists {
					queueLogger.Error("workflow function not found in registry", "workflow_name", workflow.Name)
					continue
				}
				registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
				if !ok {
					queueLogger.Error("invalid workflow registry entry type", "workflow_name", workflow.Name)
					continue
				}

				// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
				_, err := registeredWorkflow.wrappedFunction(ctx, workflow.Input, workflow.Serialization, WithWorkflowID(workflow.Id), withIsDequeue())
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
func (qr *queueRunner) dequeueWorkflows(ctx *dbosContext, queue workflowQueue, partitionKey string, hasBackoffError *bool) ([]sysdb.DequeuedWorkflow, bool) {
	dequeuedWorkflows, err := sysdb.RetryWithResult(ctx, func() ([]sysdb.DequeuedWorkflow, error) {
		return ctx.systemDB.DequeueWorkflows(ctx, sysdb.DequeueWorkflowsInput{
			Queue:              queue.toConfig(),
			ExecutorID:         ctx.executorID,
			ApplicationVersion: ctx.applicationVersion,
			QueuePartitionKey:  partitionKey,
			LocalRunningCount:  ctx.countActiveWorkflowsForQueue(queue.Name, partitionKey),
		})
	}, sysdb.WithRetrierLogger(qr.logger))

	if err != nil {
		if ctx.systemDB.IsContentionError(err) {
			*hasBackoffError = true
		} else {
			qr.logger.Error("Error dequeuing workflows from queue", "queue_name", queue.Name, "partition_key", partitionKey, "error", err)
		}
		return nil, true // Indicate to continue to next iteration
	}

	return dequeuedWorkflows, false // Success, don't continue
}

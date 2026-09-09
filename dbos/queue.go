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
	Name                       string        `json:"name"`                                   // Unique queue name
	WorkerConcurrency          *int          `json:"worker_concurrency,omitempty"`           // Max concurrent workflows per executor
	GlobalConcurrency          *int          `json:"concurrency,omitempty"`                  // Max concurrent workflows across all executors
	PriorityEnabled            bool          `json:"priority_enabled,omitempty"`             // Enable priority-based scheduling
	RateLimit                  *RateLimiter  `json:"rate_limit,omitempty"`                   // Rate limiting configuration
	PartitionQueue             bool          `json:"partition_queue,omitempty"`              // Partitioned: dequeues per partition key
	ApplicationName            string        `json:"application_name,omitempty"`             // Owning application; empty if unclaimed
	PartitionConcurrency       *int          `json:"partition_concurrency,omitempty"`        // Max concurrent workflows per partition across all executors
	PartitionWorkerConcurrency *int          `json:"partition_worker_concurrency,omitempty"` // Max concurrent workflows per partition per executor
	PartitionRateLimit         *RateLimiter  `json:"partition_rate_limit,omitempty"`         // Rate limit applied to each partition separately
	basePollingInterval        time.Duration // Base polling interval (minimum, never poll faster)
	maxPollingInterval         time.Duration // Maximum polling interval (never poll slower)

	databaseBacked bool                    // Whether this queue's config lives in the queues table
	onConflict     QueueConflictResolution // Registration conflict policy
}

// toConfig converts to the persisted representation used by internal/sysdb.
func (q workflowQueue) toConfig() models.QueueConfig {
	return models.QueueConfig{
		Name:                       q.Name,
		WorkerConcurrency:          q.WorkerConcurrency,
		GlobalConcurrency:          q.GlobalConcurrency,
		PriorityEnabled:            q.PriorityEnabled,
		RateLimit:                  q.RateLimit,
		PartitionQueue:             q.PartitionQueue,
		ApplicationName:            q.ApplicationName,
		BasePollingInterval:        q.basePollingInterval,
		MaxPollingInterval:         q.maxPollingInterval,
		DatabaseBacked:             q.databaseBacked,
		PartitionConcurrency:       q.PartitionConcurrency,
		PartitionWorkerConcurrency: q.PartitionWorkerConcurrency,
		PartitionRateLimit:         q.PartitionRateLimit,
	}
}

func (q workflowQueue) hasPartitionLimits() bool  { return q.toConfig().HasPartitionLimits() }
func (q workflowQueue) isLegacyPartitioned() bool { return q.toConfig().IsLegacyPartitioned() }
func (q workflowQueue) resolveLimits() models.ResolvedQueueLimits {
	return q.toConfig().ResolveLimits()
}

// queueFromConfig builds a workflowQueue from its persisted representation.
// Registration-only state (onConflict) is not persisted and stays zero.
// PartitionQueue is set whenever a partition limit is, whatever the stored column says.
func queueFromConfig(cfg models.QueueConfig) workflowQueue {
	return workflowQueue{
		Name:                       cfg.Name,
		WorkerConcurrency:          cfg.WorkerConcurrency,
		GlobalConcurrency:          cfg.GlobalConcurrency,
		PriorityEnabled:            cfg.PriorityEnabled,
		RateLimit:                  cfg.RateLimit,
		PartitionQueue:             cfg.IsPartitioned(),
		ApplicationName:            cfg.ApplicationName,
		basePollingInterval:        cfg.BasePollingInterval,
		maxPollingInterval:         cfg.MaxPollingInterval,
		databaseBacked:             cfg.DatabaseBacked,
		PartitionConcurrency:       cfg.PartitionConcurrency,
		PartitionWorkerConcurrency: cfg.PartitionWorkerConcurrency,
		PartitionRateLimit:         cfg.PartitionRateLimit,
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
//
// Limit getters report each limit at its enforced scope: under the deprecated
// WithPartitionQueue option the queue-wide limits are reported as partition limits.
type Queue interface {
	GetName() string
	GetGlobalConcurrency() *int
	GetWorkerConcurrency() *int
	GetRateLimit() *RateLimiter
	GetPartitionConcurrency() *int
	GetPartitionWorkerConcurrency() *int
	GetPartitionRateLimit() *RateLimiter
	GetPriorityEnabled() bool
	GetPartitionQueue() bool
	GetPollingInterval() time.Duration
	GetApplicationName() string

	SetGlobalConcurrency(ctx Client, value *int) error
	SetWorkerConcurrency(ctx Client, value *int) error
	SetRateLimit(ctx Client, value *RateLimiter) error
	SetPartitionConcurrency(ctx Client, value *int) error
	SetPartitionWorkerConcurrency(ctx Client, value *int) error
	SetPartitionRateLimit(ctx Client, value *RateLimiter) error
	SetPriorityEnabled(ctx Client, value bool) error
	SetPartitionQueue(ctx Client, value bool) error
	SetPollingInterval(ctx Client, value time.Duration) error
}

// Compile-time check that *workflowQueue satisfies the Queue interface.
var _ Queue = (*workflowQueue)(nil)

func (q *workflowQueue) GetName() string               { return q.Name }
func (q *workflowQueue) GetGlobalConcurrency() *int    { return q.resolveLimits().GlobalConcurrency }
func (q *workflowQueue) GetWorkerConcurrency() *int    { return q.resolveLimits().WorkerConcurrency }
func (q *workflowQueue) GetRateLimit() *RateLimiter    { return q.resolveLimits().RateLimit }
func (q *workflowQueue) GetPartitionConcurrency() *int { return q.resolveLimits().PartitionConcurrency }
func (q *workflowQueue) GetPartitionWorkerConcurrency() *int {
	return q.resolveLimits().PartitionWorkerConcurrency
}
func (q *workflowQueue) GetPartitionRateLimit() *RateLimiter {
	return q.resolveLimits().PartitionRateLimit
}
func (q *workflowQueue) GetPriorityEnabled() bool { return q.PriorityEnabled }
func (q *workflowQueue) GetPartitionQueue() bool  { return q.PartitionQueue }

func (q *workflowQueue) GetPollingInterval() time.Duration { return q.basePollingInterval }

func (q *workflowQueue) GetApplicationName() string { return q.ApplicationName }

// SetGlobalConcurrency updates the queue's global concurrency limit. Pass nil to clear it.
func (q *workflowQueue) SetGlobalConcurrency(ctx Client, value *int) error {
	return q.applyLimitChange(ctx, "global concurrency", func(c *workflowQueue) { c.GlobalConcurrency = value })
}

// SetWorkerConcurrency updates the queue's per-executor concurrency limit. Pass nil to clear it.
func (q *workflowQueue) SetWorkerConcurrency(ctx Client, value *int) error {
	return q.applyLimitChange(ctx, "worker concurrency", func(c *workflowQueue) { c.WorkerConcurrency = value })
}

// SetRateLimit updates the queue's rate limiter. Pass nil to clear it.
func (q *workflowQueue) SetRateLimit(ctx Client, value *RateLimiter) error {
	return q.applyLimitChange(ctx, "rate limit", func(c *workflowQueue) { c.RateLimit = value })
}

// SetPartitionConcurrency updates the per-partition concurrency limit. Pass nil to clear it.
// Setting any partition limit partitions the queue; clearing the last one unpartitions it.
func (q *workflowQueue) SetPartitionConcurrency(ctx Client, value *int) error {
	return q.applyPartitionLimitChange(ctx, "partition concurrency", func(c *workflowQueue) { c.PartitionConcurrency = value })
}

// SetPartitionWorkerConcurrency updates the per-partition, per-executor concurrency limit. Pass nil to clear it.
func (q *workflowQueue) SetPartitionWorkerConcurrency(ctx Client, value *int) error {
	return q.applyPartitionLimitChange(ctx, "partition worker concurrency", func(c *workflowQueue) { c.PartitionWorkerConcurrency = value })
}

// SetPartitionRateLimit updates the per-partition rate limiter. Pass nil to clear it.
func (q *workflowQueue) SetPartitionRateLimit(ctx Client, value *RateLimiter) error {
	return q.applyPartitionLimitChange(ctx, "partition rate limit", func(c *workflowQueue) { c.PartitionRateLimit = value })
}

// SetPriorityEnabled toggles priority-based scheduling for the queue.
func (q *workflowQueue) SetPriorityEnabled(ctx Client, value bool) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) error { c.PriorityEnabled = value; return nil })
}

// SetPartitionQueue toggles the deprecated partitioned queue mode; a queue
// partitioned by its partition limits rejects it.
//
// Switching an existing queue from unpartitioned to partitioned abandons any
// workflows already enqueued on it: they were enqueued without a partition key,
// and a partitioned queue only dequeues from its partitions, so they will never
// be dequeued.
func (q *workflowQueue) SetPartitionQueue(ctx Client, value bool) error {
	wasPartitioned := q.PartitionQueue
	err := q.applyConfigChange(ctx, func(c *workflowQueue) error {
		if c.hasPartitionLimits() {
			return models.NewInvalidOptionError(fmt.Sprintf("cannot set partition queue mode on queue %s: it is partitioned by its partition limits; clear those instead", c.Name))
		}
		c.PartitionQueue = value
		return nil
	})
	if err != nil {
		return err
	}
	q.warnIfNewlyPartitioned(ctx, wasPartitioned)
	return nil
}

// applyLimitChange persists a queue-wide limit; the deprecated partition mode rejects it.
func (q *workflowQueue) applyLimitChange(ctx Client, field string, mutate func(*workflowQueue)) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) error {
		if err := c.requireNotLegacyPartitioned(field); err != nil {
			return err
		}
		mutate(c)
		return nil
	})
}

// applyPartitionLimitChange persists a partition limit and keeps the flag in step.
func (q *workflowQueue) applyPartitionLimitChange(ctx Client, field string, mutate func(*workflowQueue)) error {
	wasPartitioned := q.PartitionQueue
	err := q.applyConfigChange(ctx, func(c *workflowQueue) error {
		if err := c.requireNotLegacyPartitioned(field); err != nil {
			return err
		}
		mutate(c)
		c.PartitionQueue = c.hasPartitionLimits()
		return nil
	})
	if err != nil {
		return err
	}
	q.warnIfNewlyPartitioned(ctx, wasPartitioned)
	return nil
}

func (q *workflowQueue) requireNotLegacyPartitioned(field string) error {
	if !q.isLegacyPartitioned() {
		return nil
	}
	return models.NewInvalidOptionError(fmt.Sprintf("cannot set %s on queue %s: it is registered with the deprecated WithPartitionQueue option, under which the global concurrency, worker concurrency, and rate limit apply per partition; re-register the queue with the partition limits instead", field, q.Name))
}

func (q *workflowQueue) warnIfNewlyPartitioned(ctx Client, wasPartitioned bool) {
	if wasPartitioned || !q.PartitionQueue {
		return
	}
	if c, ok := ctx.(*dbosContext); ok {
		c.logger.Warn("Switched queue to partitioned mode; workflows already enqueued without a partition key will be abandoned and never dequeued", "queue_name", q.Name)
	}
}

// SetPollingInterval updates the queue's base polling interval: the cadence at
// which workers poll for new work and the floor that backoff scales back to.
// This does not reset a worker currently backed off above the base; the change
// takes effect immediately only when it raises the floor above the current
// interval, otherwise as the worker scales back down on successful iterations.
func (q *workflowQueue) SetPollingInterval(ctx Client, value time.Duration) error {
	return q.applyConfigChange(ctx, func(c *workflowQueue) error { c.basePollingInterval = value; return nil })
}

// applyConfigChange persists a single configuration change for a database-backed
// queue. The read-modify-write runs in one transaction (see
// systemDatabase.updateQueueConfig): the latest persisted row is read, mutate
// applies the change, cross-field validation runs against the fresh values, and
// the row is written. On success the receiver is refreshed from the persisted
// row so its getters return the updated values.
func (q *workflowQueue) applyConfigChange(ctx Client, mutate func(*workflowQueue) error) error {
	if !q.databaseBacked {
		return fmt.Errorf("queue %s: configuration can only be updated on database-backed queues registered via RegisterQueue", q.Name)
	}
	c, ok := ctx.(*dbosContext)
	if !ok {
		return errors.New("invalid DBOS context")
	}
	cfg, err := sysdb.RetryWithResult(c, func() (*models.QueueConfig, error) {
		return c.systemDB.UpdateQueueConfig(c, q.Name, func(fresh *models.QueueConfig) error {
			w := queueFromConfig(*fresh)
			if err := mutate(&w); err != nil {
				return err
			}
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
	// Keep the handle's value for non db-backed fields
	updated := queueFromConfig(*cfg)
	updated.maxPollingInterval = q.maxPollingInterval
	updated.onConflict = q.onConflict
	*q = updated
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

// WithPartitionQueue enables partitioned queue mode, under which the global
// concurrency, worker concurrency, and rate limiter each apply per partition.
//
// Deprecated: use a partition limit (WithPartitionConcurrency,
// WithPartitionWorkerConcurrency, WithPartitionRateLimiter) instead, which
// partitions the queue while the queue-wide limits keep their scope.
func WithPartitionQueue() QueueOption {
	return func(q *workflowQueue) {
		q.PartitionQueue = true
	}
}

// WithPartitionConcurrency limits the workflows from any one partition running
// concurrently across all executors. Any partition limit partitions the queue.
func WithPartitionConcurrency(concurrency int) QueueOption {
	return func(q *workflowQueue) {
		q.PartitionConcurrency = &concurrency
	}
}

// WithPartitionWorkerConcurrency limits the workflows from any one partition running concurrently on this executor.
func WithPartitionWorkerConcurrency(concurrency int) QueueOption {
	return func(q *workflowQueue) {
		q.PartitionWorkerConcurrency = &concurrency
	}
}

// WithPartitionRateLimiter rate limits workflow starts on each partition separately.
func WithPartitionRateLimiter(limiter *RateLimiter) QueueOption {
	return func(q *workflowQueue) {
		q.PartitionRateLimit = limiter
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

// WithQueueApplicationName sets the application that owns the queue and polls
// it. It defaults to the registering handle's application; registering a queue
// owned by a different application fails.
func WithQueueApplicationName(name string) QueueOption {
	return func(q *workflowQueue) {
		q.ApplicationName = name
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
	fail := func(msg string) error {
		return models.NewInvalidOptionError(fmt.Sprintf("queue %s: %s", q.Name, msg))
	}
	gc, wc, pc, pwc := q.GlobalConcurrency, q.WorkerConcurrency, q.PartitionConcurrency, q.PartitionWorkerConcurrency
	if pc != nil && *pc < 1 {
		return fail("partition concurrency must be at least 1")
	}
	if pwc != nil && *pwc < 1 {
		return fail("partition worker concurrency must be at least 1")
	}
	if pwc != nil && pc != nil && *pwc > *pc {
		return fail("partition concurrency must be greater than or equal to partition worker concurrency")
	}
	if pwc != nil && wc != nil && *pwc > *wc {
		return fail("worker concurrency must be greater than or equal to partition worker concurrency")
	}
	if wc != nil && gc != nil && *wc > *gc {
		return fail("concurrency must be greater than or equal to worker_concurrency")
	}
	if pc != nil && gc != nil && *pc > *gc {
		return fail("global concurrency must be greater than or equal to partition concurrency")
	}
	if pwc != nil && gc != nil && *pwc > *gc {
		return fail("concurrency must be greater than or equal to partition worker concurrency")
	}
	if q.basePollingInterval <= 0 {
		return fail("polling interval must be positive")
	}
	if err := validateRateLimiter("rate limiter", q.RateLimit); err != nil {
		return fail(err.Error())
	}
	if err := validateRateLimiter("partition rate limiter", q.PartitionRateLimit); err != nil {
		return fail(err.Error())
	}
	return nil
}

func validateRateLimiter(name string, rl *RateLimiter) error {
	if rl == nil {
		return nil
	}
	if rl.Limit <= 0 {
		return fmt.Errorf("%s limit must be positive", name)
	}
	if rl.Period <= 0 {
		return fmt.Errorf("%s period must be positive", name)
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
	if q.PartitionQueue && q.hasPartitionLimits() {
		return nil, models.NewInvalidOptionError(fmt.Sprintf("queue %s: WithPartitionQueue is deprecated in favor of the partition limits; set only one of them", name))
	}
	if err := validateQueueConfig(&q); err != nil {
		return nil, err
	}
	q.PartitionQueue = q.PartitionQueue || q.hasPartitionLimits()

	// Resolve the conflict policy into whether an existing row should be overwritten.
	var updateExisting bool
	switch q.onConflict {
	case QueueConflictAlwaysUpdate:
		updateExisting = true
	case QueueConflictNeverUpdate:
		updateExisting = false
	default: // QueueConflictUpdateIfLatestVersion
		latest, err := sysdb.RetryWithResult(c, func() (*VersionInfo, error) {
			return c.systemDB.GetLatestApplicationVersion(c, nil, "")
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
		return c.systemDB.UpsertQueue(c, sysdb.UpsertQueueDBInput{Queue: q.toConfig(), UpdateExisting: updateExisting, ApplicationName: c.requestedOwner(q.ApplicationName)})
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

// ListQueuesOption is a functional option for filtering ListQueues.
type ListQueuesOption func(*listQueuesOptions)

type listQueuesOptions struct {
	applicationNames []string
}

// WithListQueuesApplicationNames lists queues owned by these applications
// (plus unclaimed ones). By default only this handle's own application's
// queues are listed.
func WithListQueuesApplicationNames(names ...string) ListQueuesOption {
	return func(o *listQueuesOptions) {
		o.applicationNames = append(o.applicationNames, names...)
	}
}

// ListQueues returns database-backed queues owned by this application plus
// unclaimed ones. Use WithQueueApplicationNames to list other applications'
// queues.
func ListQueues(ctx Client, opts ...ListQueuesOption) ([]Queue, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.ListQueues(ctx, opts...)
}

func (c *dbosContext) ListQueues(_ Client, opts ...ListQueuesOption) ([]Queue, error) {
	var options listQueuesOptions
	for _, opt := range opts {
		opt(&options)
	}
	cfgs, err := sysdb.RetryWithResult(c, func() ([]models.QueueConfig, error) {
		return c.systemDB.ListQueues(c, options.applicationNames)
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
		return ctx.systemDB.ListQueues(ctx, nil)
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
			// Give an equal chance to each partition to be dequeued to avoid starvation.
			rand.Shuffle(len(partitionKeys), func(i, j int) { partitionKeys[i], partitionKeys[j] = partitionKeys[j], partitionKeys[i] }) // #nosec G404 -- non-crypto shuffle; acceptable
			limits := queue.resolveLimits()
			running := ctx.countActiveWorkflowsForQueue(queue.Name)
			var dequeuedIDs []string
			for _, partitionKey := range partitionKeys {
				if limits.WorkerConcurrency != nil && running+len(dequeuedIDs) >= *limits.WorkerConcurrency {
					break
				}
				ids, err := qr.dequeueWorkflows(ctx, queue, partitionKey, running+len(dequeuedIDs))
				if err != nil {
					switch {
					case !ctx.systemDB.IsContentionError(err):
						queueLogger.Error("Error dequeuing workflows from queue", "partition_key", partitionKey, "error", err)
					case partitionKey == "":
						hasBackoffError = true
					default:
						// Another worker holds this partition or won its claim: skip it, no queue-wide backoff.
						queueLogger.Debug("Partition is contended, skipping", "partition_key", partitionKey)
					}
					continue
				}
				dequeuedIDs = append(dequeuedIDs, ids...)
			}

			if len(dequeuedIDs) > 0 {
				queueLogger.Debug("Dequeued workflows from queue", "workflows", len(dequeuedIDs))
				qr.startDequeuedWorkflows(ctx, queueLogger, dequeuedIDs)
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

// startDequeuedWorkflows reads the claimed workflows' statuses in one round trip and
// starts each of them. The claim already wrote everything a status insert would
// (PENDING, executor, deadline, attempts), so the dispatch writes nothing.
func (qr *queueRunner) startDequeuedWorkflows(ctx *dbosContext, queueLogger *slog.Logger, workflowIDs []string) {
	statuses, err := sysdb.RetryWithResult(ctx, func() ([]models.WorkflowStatus, error) {
		return ctx.systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{WorkflowIDs: workflowIDs, LoadInput: true})
	}, sysdb.WithRetrierLogger(queueLogger))
	if err != nil {
		queueLogger.Error("Error listing dequeued workflows", "error", err)
		return
	}
	found := make(map[string]models.WorkflowStatus, len(statuses))
	for _, status := range statuses {
		found[status.ID] = status
	}

	// ListWorkflows sorts by creation time: start the workflows in dequeue order.
	for _, id := range workflowIDs {
		status, ok := found[id]
		if !ok {
			queueLogger.Error("Dequeued workflow not found", "workflow_id", id)
			continue
		}

		// Find the workflow in the registry. Configured instance workflows are
		// registered under a name qualified with their config name.
		lookupName := status.Name
		if status.ConfigName != nil && *status.ConfigName != "" {
			lookupName = instanceQualifiedName(status.Name, *status.ConfigName)
		}
		wfName, ok := ctx.workflowCustomNametoFQN.Load(lookupName)
		if !ok {
			queueLogger.Error("Workflow not found in registry", "workflow_name", status.Name)
			continue
		}
		registeredWorkflowAny, exists := ctx.workflowRegistry.Load(wfName.(string))
		if !exists {
			queueLogger.Error("workflow function not found in registry", "workflow_name", status.Name)
			continue
		}
		registeredWorkflow, ok := registeredWorkflowAny.(WorkflowRegistryEntry)
		if !ok {
			queueLogger.Error("invalid workflow registry entry type", "workflow_name", status.Name)
			continue
		}

		// The claim counted this dispatch: dead-letter the workflow if that exhausted its attempts.
		if registeredWorkflow.MaxRetries > 0 && status.Attempts > registeredWorkflow.MaxRetries+1 {
			err := sysdb.Retry(ctx, func() error {
				return ctx.systemDB.DeadLetterWorkflows(ctx, []string{id}, status.Attempts)
			}, sysdb.WithRetrierLogger(queueLogger))
			if err != nil {
				queueLogger.Error("Error dead lettering workflow", "workflow_id", id, "error", err)
				continue
			}
			queueLogger.Warn("Workflow exceeded its maximum recovery attempts and was dead lettered", "workflow_id", id, "max_retries", registeredWorkflow.MaxRetries)
			continue
		}

		// Only a PENDING row can own its outcome: a row that moved on since the claim
		// (cancelled, resumed) would run for nothing.
		if status.Status != WorkflowStatusPending {
			queueLogger.Warn("Dequeued workflow is no longer pending, skipping", "workflow_id", id, "status", status.Status)
			continue
		}

		// The input is passed encoded: it is decoded once the target type is known.
		// The auth identity is re-attached so child workflows spawned during the
		// dequeued execution inherit the same identity as the original run.
		serialization := status.Serialization
		fn := WorkflowFunc(func(ctx Context, input any) (any, error) {
			return registeredWorkflow.typeErasedFn(ctx, input, serialization)
		})
		ctx.executeWorkflow(fn, status.Input, workflowExecution{
			workflowID:         id,
			queueName:          status.QueueName,
			queuePartitionKey:  status.QueuePartitionKey,
			timeout:            status.Timeout,
			deadline:           status.Deadline,
			authenticatedUser:  status.AuthenticatedUser,
			assumedRole:        status.AssumedRole,
			authenticatedRoles: status.AuthenticatedRoles,
			isPortableWorkflow: serialization == PortableSerializerName,
		})
	}
}

// dequeueWorkflows claims workflows from one partition, or from the whole queue when partitionKey is empty.
func (qr *queueRunner) dequeueWorkflows(ctx *dbosContext, queue workflowQueue, partitionKey string, localRunning int) ([]string, error) {
	partitionRunning := 0
	if partitionKey != "" {
		partitionRunning = ctx.countActiveWorkflowsForPartition(queue.Name, partitionKey)
	}
	return sysdb.RetryWithResult(ctx, func() ([]string, error) {
		return ctx.systemDB.DequeueWorkflows(ctx, sysdb.DequeueWorkflowsInput{
			Queue:                      queue.toConfig(),
			ExecutorID:                 ctx.executorID,
			ApplicationVersion:         ctx.applicationVersion,
			QueuePartitionKey:          partitionKey,
			LocalRunningCount:          localRunning,
			PartitionLocalRunningCount: partitionRunning,
		})
	}, sysdb.WithRetrierLogger(qr.logger))
}

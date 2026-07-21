package dbos

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/google/uuid"
)

// Debouncer coalesces repeated invocations of a workflow. Each Debounce call
// with a given key pushes the workflow's start back by the given delay and
// replaces the input it will run with; when the delay elapses without another
// call, the workflow runs once with the latest input. If a timeout is
// configured, the start is never pushed past timeout from the first call
// (zero means no limit). The timeout is captured when the first call enqueues
// the workflow: changing it afterwards only affects the next debounce
// generation, once the pending workflow has started. Different keys debounce
// independently.
//
// A debounced workflow is enqueued in the DELAYED status holding the debounce
// key as its deduplication ID; each subsequent Debounce call atomically extends
// its delay and replaces its inputs. The deduplication ID is released when the
// delay expires, so the next Debounce call after that starts a new execution.
//
// Create with NewDebouncer; use DebouncerClient to debounce from outside the
// application.
type Debouncer[R any, P any] struct {
	WorkflowFQN string        // Fully qualified name of the target workflow (registry key)
	Timeout     time.Duration // Maximum time before starting the workflow (0 = no timeout)

	params debouncerParams
}

// debouncerParams identifies the debounced workflow by name. Debounce operates
// purely on names, so Debouncer and DebouncerClient share one implementation.
type debouncerParams struct {
	workflowName string        // Recorded workflow name, matched by the bounce and used for the enqueue
	dedupPrefix  string        // Debounce-key prefix; instance-qualified for configured instances
	className    string        // Receiver type name for configured instance workflows
	configName   string        // Config name for configured instance workflows
	queueName    string        // Queue the debounced workflow runs on ("" = the internal queue)
	timeout      time.Duration // Maximum time before starting the workflow (0 = no timeout)
}

// DebouncerOption is a functional option for configuring debouncer creation parameters.
type DebouncerOption func(*debouncerOptions)

type debouncerOptions struct {
	timeout    time.Duration
	instance   ConfiguredInstance
	configName string
	queueName  string
}

// WithDebouncerTimeout sets the maximum time before starting the workflow.
// If timeout is zero (the default), there is no maximum time limit.
// The deadline is fixed when the first Debounce call for a key enqueues the
// workflow; subsequent calls extend the delay up to that deadline but never
// move it, even if the debouncer's timeout has changed in the meantime.
func WithDebouncerTimeout(timeout time.Duration) DebouncerOption {
	return func(o *debouncerOptions) {
		o.timeout = timeout
	}
}

// WithDebouncerQueue runs the debounced workflow on the named queue instead of
// the DBOS internal queue. Debounce keys are scoped to the queue. The queue is
// fixed per debouncer; Debounce calls cannot override it.
func WithDebouncerQueue(queueName string) DebouncerOption {
	return func(o *debouncerOptions) {
		o.queueName = queueName
	}
}

// WithDebouncerInstance targets the workflow registration bound to the given configured
// instance (see WithInstance). Required when the debounced workflow is a method of a
// configured instance:
//
//	debouncer := dbos.NewDebouncer(ctx, slack.Send, dbos.WithDebouncerInstance(slack))
func WithDebouncerInstance(instance ConfiguredInstance) DebouncerOption {
	return func(o *debouncerOptions) {
		o.instance = instance
	}
}

// WithDebouncerConfigName targets the workflow registration bound to the configured
// instance with the given config name. Use with NewDebouncerClient, where the instance
// object itself is not available.
func WithDebouncerConfigName(configName string) DebouncerOption {
	return func(o *debouncerOptions) {
		o.configName = configName
	}
}

// resolveConfigName returns the config name from the instance, if set, else the raw config name.
func (o *debouncerOptions) resolveConfigName() string {
	if o.instance != nil {
		return o.instance.ConfigName()
	}
	return o.configName
}

// A debounce owns the workflow's deduplication ID (the debounce key) and its delay
// (the debounce period), so a caller must not also set them. The queue is fixed on
// the debouncer because the debounce key is scoped to it. Priority and partition
// keys are rejected because they cannot apply to a debounced enqueue, and a
// deduplication policy because a debounce owns the deduplication behavior.
func rejectConflictingDebounceOptions(options *workflowOptions) error {
	if options.QueueName != "" || options.queue != nil {
		return models.NewInvalidOptionError("cannot debounce a workflow with a queue set: the queue is configured on the debouncer (WithDebouncerQueue)")
	}
	if options.DeduplicationID != "" {
		return models.NewInvalidOptionError("cannot debounce a workflow with a deduplication ID set: the debounce key is used as the workflow's deduplication ID")
	}
	if options.DelayDuration != 0 {
		return models.NewInvalidOptionError("cannot debounce a workflow with a delay set: the debounce period controls the workflow's delay")
	}
	if options.Priority != 0 {
		return models.NewInvalidOptionError("cannot debounce a workflow with a priority set: priority is not supported for debounced workflows")
	}
	if options.QueuePartitionKey != "" {
		return models.NewInvalidOptionError("cannot debounce a workflow with a queue partition key set: partitioned queues do not support deduplication, which debouncing requires")
	}
	if options.DeduplicationPolicy != DeduplicationPolicyReject {
		return models.NewInvalidOptionError("cannot debounce a workflow with a deduplication policy set: a debounce owns the deduplication behavior")
	}
	return nil
}

// The action a debounce caller should take after a bounce attempt.
type bounceAction int

const (
	bounceReturn  bounceAction = iota // an existing debounced workflow was extended; return a handle to it
	bounceEnqueue                     // the key is unheld; enqueue a fresh debounced workflow
	bounceRaise                       // the key is held by a non-debounced workflow or by a different workflow whose debounce key collides; surface the conflict
	bounceRetry                       // a same-name debounced holder flipped out of DELAYED mid-bounce (a rare race); retry the bounce
)

func classifyBounce(result *sysdb.DebounceResult, workflowName string) bounceAction {
	switch {
	case result.BouncedWorkflowID != nil:
		return bounceReturn
	case result.HolderWorkflowID == nil:
		return bounceEnqueue
	case !result.HolderIsDebounced:
		return bounceRaise
	case result.HolderWorkflowName != workflowName:
		return bounceRaise
	default:
		return bounceRetry
	}
}

// debounce implements Debouncer.Debounce and DebouncerClient.Debounce.
func debounce[R any, P any](c *dbosContext, params debouncerParams, key string, delay time.Duration, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	if delay < 0 {
		return nil, models.NewInvalidOptionError("debounce delay cannot be negative")
	}

	options := workflowOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.err != nil {
		return nil, options.err
	}
	if err := rejectConflictingDebounceOptions(&options); err != nil {
		return nil, err
	}

	// Run on the debouncer's configured queue, else the internal queue.
	// Debounce keys are scoped to the queue.
	queueName := params.queueName
	if queueName == "" {
		queueName = models.InternalQueueName
	}
	deduplicationID := params.dedupPrefix + "-" + key

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && wfState != nil

	// Serialize new inputs with the same encoder Enqueue uses, so a bounce stays
	// consistent with the initial enqueue.
	var encodedInput *string
	var serialization string
	var err error
	if _, ok := any(input).(PortableWorkflowArgs); ok {
		encodedInput, err = newPortableSerializer[any]().Encode(input)
		serialization = PortableSerializerName
	} else {
		ser := resolveEncoder(c)
		encodedInput, err = ser.Encode(input)
		serialization = ser.Name()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to serialize input: %w", err)
	}

	// In a workflow, checkpoint the fresh-enqueue workflow ID so a recovery replay
	// re-enqueues idempotently instead of minting a duplicate workflow.
	workflowID := options.WorkflowID
	if workflowID == "" && isWithinWorkflow {
		workflowID, err = RunAsStep(c, func(ctx context.Context) (string, error) {
			return uuid.New().String(), nil
		}, WithStepName("DBOS.debounce.assignWorkflowID"))
		if err != nil {
			return nil, err
		}
	}

	for {
		// Try to extend an existing debounced workflow for this key first (the sole
		// coalescing mechanism). In a workflow, the bounce runs as a transactional step
		// so it commits atomically with its checkpoint: a crash can never commit one
		// without the other, which on recovery would re-bounce work that already ran.
		bounceInput := sysdb.DebounceDelayedWorkflowDBInput{
			WorkflowName:    params.workflowName,
			QueueName:       queueName,
			DeduplicationID: deduplicationID,
			DelayUntil:      time.Now().Add(delay),
			Input:           encodedInput,
			Serialization:   serialization,
		}
		var result *sysdb.DebounceResult
		if isWithinWorkflow {
			result, err = runAsTxn(c, func(ctx context.Context, tx Tx) (*sysdb.DebounceResult, error) {
				bounceInput.Tx = tx
				return ctx.(*dbosContext).systemDB.DebounceDelayedWorkflow(ctx, bounceInput)
			}, WithStepName("DBOS.debounceDelayedWorkflow"))
		} else {
			result, err = c.systemDB.DebounceDelayedWorkflow(WithoutCancel(c), bounceInput)
		}
		if err != nil {
			return nil, err
		}

		switch classifyBounce(result, params.workflowName) {
		case bounceReturn:
			return newWorkflowPollingHandle[R](c, *result.BouncedWorkflowID), nil
		case bounceRaise:
			return nil, models.NewQueueDeduplicatedError(*result.HolderWorkflowID, queueName, deduplicationID)
		case bounceRetry:
			continue
		}

		// The key is free: enqueue a fresh debounced workflow, delayed by the debounce
		// period and capped at the debounce timeout.
		delayDuration := delay
		var deadline time.Time
		if params.timeout > 0 {
			deadline = time.Now().Add(params.timeout)
			if delayDuration > params.timeout {
				delayDuration = params.timeout
			}
		}
		enqueueOpts := []EnqueueOption{
			WithEnqueueDeduplicationID(deduplicationID),
			WithEnqueueDelay(delayDuration),
			withEnqueueDebounce(deadline),
		}
		if workflowID != "" {
			enqueueOpts = append(enqueueOpts, WithEnqueueWorkflowID(workflowID))
		}
		if options.ApplicationVersion != "" {
			enqueueOpts = append(enqueueOpts, WithEnqueueApplicationVersion(options.ApplicationVersion))
		}
		if options.AuthenticatedUser != "" {
			enqueueOpts = append(enqueueOpts, WithEnqueueAuthenticatedUser(options.AuthenticatedUser))
		}
		if options.AssumedRole != "" {
			enqueueOpts = append(enqueueOpts, WithEnqueueAssumedRole(options.AssumedRole))
		}
		if len(options.AuthenticatedRoles) > 0 {
			enqueueOpts = append(enqueueOpts, WithEnqueueAuthenticatedRoles(options.AuthenticatedRoles...))
		}
		if len(options.WorkflowAttributes) > 0 {
			enqueueOpts = append(enqueueOpts, WithEnqueueAttributes(options.WorkflowAttributes))
		}
		if params.className != "" {
			enqueueOpts = append(enqueueOpts, WithEnqueueClassName(params.className))
		}
		if params.configName != "" {
			enqueueOpts = append(enqueueOpts, WithEnqueueConfigName(params.configName))
		}

		handle, err := Enqueue[R](c, queueName, params.workflowName, input, enqueueOpts...)
		if err != nil {
			// A concurrent debounce grabbed the key between bounce and enqueue; loop to
			// bounce that workflow instead.
			if errors.Is(err, ErrQueueDeduplicated) {
				continue
			}
			return nil, err
		}
		return handle, nil
	}
}

// NewDebouncer creates a new debouncer for the specified workflow.
//
// Parameters:
//   - ctx: DBOS context for the debouncer
//   - workflow: The workflow function to debounce (must be registered)
//   - opts: Optional functional options for configuring the debouncer:
//   - WithDebouncerTimeout: Maximum time before starting the workflow (0 = no timeout) [optional]
//   - WithDebouncerQueue: Queue the debounced workflow runs on (default: the DBOS internal queue) [optional]
//   - WithDebouncerInstance: The configured instance the workflow method is bound to [required for instance methods]
//
// Returns a pointer to a Debouncer instance that can be used to call Debounce.
//
// Example:
//
//	// Create a debouncer with maximum timeout of 10 seconds
//	debouncer := dbos.NewDebouncer(ctx, MyWorkflowFunction, WithDebouncerTimeout(10*time.Second))
//
//	// Later, use the debouncer with different keys and delays
//	handle1, err := debouncer.Debounce(ctx, "user-123", 2*time.Second, inputData1)
//	handle2, err := debouncer.Debounce(ctx, "user-456", 3*time.Second, inputData2)
func NewDebouncer[R any, P any](
	ctx Context,
	workflow Workflow[P, R],
	opts ...DebouncerOption,
) *Debouncer[R, P] {
	options := debouncerOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	dbosCtx, ok := ctx.(*dbosContext)
	if !ok {
		return &Debouncer[R, P]{} // Do nothing if the concrete type is not dbosContext
	}

	// Configured instance workflows are registered under a name qualified with their config name.
	fqn := resolveWorkflowFunctionName(workflow)
	configName := options.resolveConfigName()
	if configName != "" {
		fqn = instanceQualifiedName(fqn, configName)
	}

	dbosCtx.logger.Debug("Creating new debouncer", "workflow_fqn", fqn)

	// Validate that the workflow is registered in the registry
	// Assertively panic if the workflow is not registered, as a sign of highly unexpected behavior
	entryAny, exists := dbosCtx.workflowRegistry.Load(fqn)
	if !exists {
		panic(models.NewNonExistentWorkflowError(fqn))
	}
	entry, ok := entryAny.(WorkflowRegistryEntry)
	if !ok {
		panic(fmt.Sprintf("invalid workflow registry entry type for workflow %s", fqn))
	}

	// The recorded workflow name (custom name if one was registered; unqualified for
	// configured instances) guards the bounce and names the enqueue, while the
	// debounce-key prefix stays instance-qualified so instances of the same method
	// debounce independently.
	workflowName := entry.Name
	if workflowName == "" {
		workflowName = fqn
	}
	dedupPrefix := workflowName
	if configName != "" {
		dedupPrefix = instanceQualifiedName(workflowName, configName)
	}

	return &Debouncer[R, P]{
		WorkflowFQN: fqn,
		Timeout:     options.timeout,
		params: debouncerParams{
			workflowName: workflowName,
			dedupPrefix:  dedupPrefix,
			className:    entry.ClassName,
			configName:   configName,
			queueName:    options.queueName,
		},
	}
}

// Debounce schedules the debounced workflow to start after delay, or, if an
// execution is already pending for key, pushes its start back by delay and
// replaces the input it will run with. Returns a handle to the pending
// workflow execution.
func (d *Debouncer[R, P]) Debounce(ctx Context, key string, delay time.Duration, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	c, ok := ctx.(*dbosContext)
	if !ok {
		return nil, errors.New("ctx must be a DBOS context")
	}
	params := d.params
	params.timeout = d.Timeout
	return debounce[R](c, params, key, delay, input, opts...)
}

// DebouncerClient provides workflow debouncing functionality using a Client.
// It is similar to Debouncer but uses a Client interface instead of a Context
// and takes a workflow name string instead of a workflow function.
type DebouncerClient[R any, P any] struct {
	WorkflowName string        // Name of the target workflow
	Client       Client        // DBOS client for operations
	Timeout      time.Duration // Maximum time before starting the workflow (0 = no timeout)

	params debouncerParams
}

// NewDebouncerClient creates a new debouncer client for the specified workflow.
//
// Parameters:
//   - workflowName: The name of the workflow to debounce
//   - client: The DBOS client to use for operations
//   - opts: Optional functional options for configuring the debouncer:
//   - WithDebouncerTimeout: Maximum time before starting the workflow (0 = no timeout) [optional]
//   - WithDebouncerQueue: Queue the debounced workflow runs on (default: the DBOS internal queue) [optional]
//   - WithDebouncerConfigName: Config name of the configured instance the workflow is bound to [required for instance methods]
//
// Returns a pointer to a DebouncerClient instance that can be used to call Debounce.
func NewDebouncerClient[R any, P any](
	workflowName string,
	client Client,
	opts ...DebouncerOption,
) *DebouncerClient[R, P] {
	options := debouncerOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	// The recorded workflow name stays unqualified for configured instances (the config
	// name is recorded alongside it), but the debounce-key prefix is instance-qualified
	// so instances of the same method debounce independently.
	configName := options.resolveConfigName()
	dedupPrefix := workflowName
	if configName != "" {
		dedupPrefix = instanceQualifiedName(workflowName, configName)
	}

	return &DebouncerClient[R, P]{
		WorkflowName: workflowName,
		Client:       client,
		Timeout:      options.timeout,
		params: debouncerParams{
			workflowName: workflowName,
			dedupPrefix:  dedupPrefix,
			configName:   configName,
			queueName:    options.queueName,
		},
	}
}

// Debounce delays workflow execution by a configurable delay amount, with each
// subsequent call pushing back the start time by the delay (up to an optional maximum timeout).
//
// Parameters:
//   - key: A unique key to group debounce calls (calls with the same key are debounced together)
//   - delay: Time by which to delay workflow execution
//   - input: Input parameters to pass to the workflow
//   - opts: Optional workflow options (e.g., WithWorkflowID, WithWorkflowAttributes, etc.)
//
// Returns a WorkflowHandle that can be used to check status and retrieve results.
func (dc *DebouncerClient[R, P]) Debounce(key string, delay time.Duration, input P, opts ...WorkflowOption) (WorkflowHandle[R], error) {
	c, ok := dc.Client.(*dbosContext)
	if !ok || c == nil {
		return nil, errors.New("client is nil or an unsupported implementation")
	}
	params := dc.params
	params.timeout = dc.Timeout
	return debounce[R](c, params, key, delay, input, opts...)
}

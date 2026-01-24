package dbos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ClientConfig struct {
	DatabaseURL    string        // DatabaseURL is a PostgreSQL connection string. Either this or SystemDBPool is required.
	SystemDBPool   *pgxpool.Pool // SystemDBPool is a custom System Database Pool. It's optional and takes precedence over DatabaseURL if both are provided.
	DatabaseSchema string        // Database schema name (defaults to "dbos")
	Logger         *slog.Logger  // Optional custom logger
}

// Client provides a programmatic way to interact with your DBOS application from external code.
// It manages the underlying DBOSContext and provides methods for workflow operations
// without requiring direct management of the context lifecycle.
type Client interface {
	Enqueue(queueName, workflowName string, input any, opts ...EnqueueOption) (WorkflowHandle[any], error)
	ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error)
	Send(destinationID string, message any, topic string) error
	GetEvent(targetWorkflowID, key string, timeout time.Duration) (any, error)
	RetrieveWorkflow(workflowID string) (WorkflowHandle[any], error)
	CancelWorkflow(workflowID string) error
	ResumeWorkflow(workflowID string) (WorkflowHandle[any], error)
	ForkWorkflow(input ForkWorkflowInput) (WorkflowHandle[any], error)
	GetWorkflowSteps(workflowID string) ([]StepInfo, error)
	ClientReadStream(workflowID string, key string) ([]any, bool, error)
	ClientReadStreamAsync(workflowID string, key string) (<-chan StreamValue[any], error)
	Shutdown(timeout time.Duration) // Simply close the system DB connection pool
}

type client struct {
	dbosCtx DBOSContext
}

// NewClient creates a new DBOS client with the provided configuration.
// The client manages its own DBOSContext internally.
//
// Example:
//
//	config := dbos.ClientConfig{
//	    DatabaseURL: "postgres://user:pass@localhost:5432/dbname",
//	}
//	client, err := dbos.NewClient(context.Background(), config)
//	if err != nil {
//	    log.Fatal(err)
//	}
func NewClient(ctx context.Context, config ClientConfig) (Client, error) {
	dbosCtx, err := NewDBOSContext(ctx, Config{
		DatabaseURL:    config.DatabaseURL,
		DatabaseSchema: config.DatabaseSchema,
		AppName:        "dbos-client",
		Logger:         config.Logger,
		SystemDBPool:   config.SystemDBPool,
	})
	if err != nil {
		return nil, err
	}

	asDBOSCtx, ok := dbosCtx.(*dbosContext)
	if ok {
		asDBOSCtx.systemDB.launch(asDBOSCtx)
	}

	return &client{
		dbosCtx: dbosCtx,
	}, nil
}

// EnqueueOption is a functional option for configuring workflow enqueue parameters.
type EnqueueOption func(*enqueueOptions)

// WithEnqueueWorkflowID sets a custom workflow ID instead of generating one automatically.
func WithEnqueueWorkflowID(id string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.workflowID = id
	}
}

// WithEnqueueApplicationVersion overrides the application version for the enqueued workflow.
func WithEnqueueApplicationVersion(version string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.applicationVersion = version
	}
}

// WithEnqueueDeduplicationID sets a deduplication ID for the enqueued workflow.
func WithEnqueueDeduplicationID(id string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.deduplicationID = id
	}
}

// WithEnqueuePriority sets the execution priority for the enqueued workflow.
func WithEnqueuePriority(priority uint) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.priority = priority
	}
}

// WithEnqueueTimeout sets the maximum execution time for the enqueued workflow.
func WithEnqueueTimeout(timeout time.Duration) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.workflowTimeout = timeout
	}
}

// WithEnqueueQueuePartitionKey sets the queue partition key for partitioned queues.
// When a queue is partitioned, workflows with the same partition key are processed
// with separate concurrency limits per partition.
func WithEnqueueQueuePartitionKey(partitionKey string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.queuePartitionKey = partitionKey
	}
}

type enqueueOptions struct {
	workflowName       string
	workflowID         string
	applicationVersion string
	deduplicationID    string
	priority           uint
	workflowTimeout    time.Duration
	workflowInput      any
	queuePartitionKey  string
}

// EnqueueWorkflow enqueues a workflow to a named queue for deferred execution.
func (c *client) Enqueue(queueName, workflowName string, input any, opts ...EnqueueOption) (WorkflowHandle[any], error) {
	// Get the concrete dbosContext to access internal fields
	dbosCtx, ok := c.dbosCtx.(*dbosContext)
	if !ok {
		return nil, fmt.Errorf("invalid DBOSContext type")
	}

	// Process options
	params := &enqueueOptions{
		workflowName:       workflowName,
		applicationVersion: dbosCtx.GetApplicationVersion(),
		workflowInput:      input,
	}
	for _, opt := range opts {
		opt(params)
	}

	if len(queueName) == 0 {
		return nil, fmt.Errorf("queue name is required")
	}

	if len(workflowName) == 0 {
		return nil, fmt.Errorf("workflow name is required")
	}

	// Validate partition key and deduplication ID are not both provided (they are incompatible)
	if len(params.queuePartitionKey) > 0 && len(params.deduplicationID) > 0 {
		return nil, fmt.Errorf("partition key and deduplication ID cannot be used together")
	}

	workflowID := params.workflowID
	if workflowID == "" {
		workflowID = uuid.New().String()
	}

	var deadline time.Time
	if params.workflowTimeout > 0 {
		deadline = time.Now().Add(params.workflowTimeout)
	}

	if params.priority > uint(math.MaxInt) {
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.priority, math.MaxInt)
	}

	status := WorkflowStatus{
		Name:               params.workflowName,
		ApplicationVersion: params.applicationVersion,
		Status:             WorkflowStatusEnqueued,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            params.workflowTimeout,
		Input:              input,
		QueueName:          queueName,
		DeduplicationID:    params.deduplicationID,
		Priority:           int(params.priority),
		QueuePartitionKey:  params.queuePartitionKey,
	}

	uncancellableCtx := WithoutCancel(dbosCtx)

	tx, err := dbosCtx.systemDB.(*sysDB).pool.Begin(uncancellableCtx)
	if err != nil {
		return nil, newWorkflowExecutionError(workflowID, fmt.Errorf("failed to begin transaction: %v", err))
	}
	defer tx.Rollback(uncancellableCtx) // Rollback if not committed

	// Insert workflow status with transaction
	insertInput := insertWorkflowStatusDBInput{
		status: status,
		tx:     tx,
	}
	_, err = dbosCtx.systemDB.insertWorkflowStatus(uncancellableCtx, insertInput)
	if err != nil {
		dbosCtx.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
		return nil, err
	}

	if err := tx.Commit(uncancellableCtx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return newWorkflowPollingHandle[any](uncancellableCtx, workflowID), nil
}

// Enqueue adds a workflow to a named queue for later execution with type safety.
// The workflow will be persisted with ENQUEUED status until picked up by a DBOS process.
// This provides asynchronous workflow execution with durability guarantees.
//
// Parameters:
//   - c: Client instance for the operation
//   - queueName: Name of the queue to enqueue the workflow to
//   - workflowName: Name of the registered workflow function to execute
//   - input: Input parameters to pass to the workflow (type P)
//   - opts: Optional configuration options
//
// Available options:
//   - WithEnqueueWorkflowID: Custom workflow ID (auto-generated if not provided)
//   - WithEnqueueApplicationVersion: Application version override
//   - WithEnqueueDeduplicationID: Deduplication identifier for idempotent enqueuing
//   - WithEnqueuePriority: Execution priority
//   - WithEnqueueTimeout: Maximum execution time for the workflow
//   - WithEnqueueQueuePartitionKey: Queue partition key for partitioned queues
//
// Returns a typed workflow handle that can be used to check status and retrieve results.
// The handle uses polling to check workflow completion since the execution is asynchronous.
//
// Example usage:
//
//	// Enqueue a workflow with string input and int output
//	handle, err := dbos.Enqueue[string, int](client, "data-processing", "ProcessDataWorkflow", "input data",
//	    dbos.WithEnqueueTimeout(30 * time.Minute))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Check status
//	status, err := handle.GetStatus()
//	if err != nil {
//	    log.Printf("Failed to get status: %v", err)
//	}
//
//	// Wait for completion and get result
//	result, err := handle.GetResult()
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %d", result)
//	}
//
//	// Enqueue with deduplication and custom workflow ID
//	handle, err := dbos.Enqueue[MyInputType, MyOutputType](client, "my-queue", "MyWorkflow", MyInputType{Field: "value"},
//	    dbos.WithEnqueueWorkflowID("custom-workflow-id"),
//	    dbos.WithEnqueueDeduplicationID("unique-operation-id"))
func Enqueue[P any, R any](c Client, queueName, workflowName string, input P, opts ...EnqueueOption) (WorkflowHandle[R], error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}

	// Serialize input
	serializer := newJSONSerializer[P]()
	encodedInput, err := serializer.Encode(input)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize workflow input: %w", err)
	}

	// Call the interface method with the same signature
	handle, err := c.Enqueue(queueName, workflowName, encodedInput, opts...)
	if err != nil {
		return nil, err
	}

	return newWorkflowPollingHandle[R](c.(*client).dbosCtx, handle.GetWorkflowID()), nil
}

// ListWorkflows retrieves a list of workflows based on the provided filters.
func (c *client) ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	return c.dbosCtx.ListWorkflows(c.dbosCtx, opts...)
}

// Send sends a message to another workflow.
func (c *client) Send(destinationID string, message any, topic string) error {
	return c.dbosCtx.Send(c.dbosCtx, destinationID, message, topic)
}

// GetEvent retrieves a key-value event from a target workflow.
func (c *client) GetEvent(targetWorkflowID, key string, timeout time.Duration) (any, error) {
	return c.dbosCtx.GetEvent(c.dbosCtx, targetWorkflowID, key, timeout)
}

// RetrieveWorkflow returns a handle to an existing workflow.
func (c *client) RetrieveWorkflow(workflowID string) (WorkflowHandle[any], error) {
	return c.dbosCtx.RetrieveWorkflow(c.dbosCtx, workflowID)
}

// CancelWorkflow cancels a running or enqueued workflow.
func (c *client) CancelWorkflow(workflowID string) error {
	return c.dbosCtx.CancelWorkflow(c.dbosCtx, workflowID)
}

// ResumeWorkflow resumes a workflow from its last completed step.
func (c *client) ResumeWorkflow(workflowID string) (WorkflowHandle[any], error) {
	return c.dbosCtx.ResumeWorkflow(c.dbosCtx, workflowID)
}

// ForkWorkflow creates a new workflow instance by copying an existing workflow from a specific step.
func (c *client) ForkWorkflow(input ForkWorkflowInput) (WorkflowHandle[any], error) {
	return c.dbosCtx.ForkWorkflow(c.dbosCtx, input)
}

// GetWorkflowSteps retrieves the execution steps of a workflow.
func (c *client) GetWorkflowSteps(workflowID string) ([]StepInfo, error) {
	return c.dbosCtx.GetWorkflowSteps(c.dbosCtx, workflowID)
}

// ReadStream reads values from a durable stream.
// This method blocks until one of the following conditions is met:
//   - The workflow becomes inactive (status is not PENDING or ENQUEUED)
//   - The stream is closed (sentinel value is found)
//
// Returns the values, whether the stream is closed, and any error.
func (c *client) ClientReadStream(workflowID string, key string) ([]any, bool, error) {
	return c.dbosCtx.ReadStream(c.dbosCtx, workflowID, key)
}

// ClientReadStream reads values from a durable stream with type safety.
// This method blocks until one of the following conditions is met:
//   - The workflow becomes inactive (status is not PENDING or ENQUEUED)
//   - The stream is closed (sentinel value is found)
//
// Returns the typed values, whether the stream is closed, and any error.
//
// Example:
//
//	values, closed, err := dbos.ClientReadStream[string](client, "workflow-id", "my-stream")
//	if err != nil {
//	    return err
//	}
//	for _, value := range values {
//	    log.Printf("Stream value: %s", value)
//	}
func ClientReadStream[R any](c Client, workflowID string, key string) ([]R, bool, error) {
	if c == nil {
		return nil, false, errors.New("client cannot be nil")
	}
	values, closed, err := c.ClientReadStream(workflowID, key)
	if err != nil {
		return nil, false, err
	}

	// Decode each value to type R
	serializer := newJSONSerializer[R]()
	typedValues := make([]R, len(values))
	for i, val := range values {
		encodedStr, ok := val.(string)
		if !ok {
			return nil, false, fmt.Errorf("stream value is not a string, got %T", val)
		}
		decodedValue, decodeErr := serializer.Decode(&encodedStr)
		if decodeErr != nil {
			return nil, false, fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)
		}
		typedValues[i] = decodedValue
	}

	return typedValues, closed, nil
}

// ClientReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
func (c *client) ClientReadStreamAsync(workflowID string, key string) (<-chan StreamValue[any], error) {
	return c.dbosCtx.ReadStreamAsync(c.dbosCtx, workflowID, key)
}

// ClientReadStreamAsync reads values from a durable stream asynchronously with type safety.
// Returns a channel that will receive StreamValue items as they're read.
//
// This method returns immediately with a channel. Values will be sent to the channel
// as they're read from the stream. The channel will be closed when:
//   - The stream is closed (sentinel value is found)
//   - The workflow becomes inactive (status is not PENDING or ENQUEUED)
//   - An error occurs
//
// Example:
//
//	ch, err := dbos.ClientReadStreamAsync[string](client, "workflow-id", "my-stream")
//	if err != nil {
//	    return err
//	}
//	for streamValue := range ch {
//	    if streamValue.Err != nil {
//	        log.Printf("Error: %v", streamValue.Err)
//	        break
//	    }
//	    if streamValue.Closed {
//	        log.Println("Stream closed")
//	        break
//	    }
//	    log.Printf("Received value: %s", streamValue.Value)
//	}
func ClientReadStreamAsync[R any](c Client, workflowID string, key string) (<-chan StreamValue[R], error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}

	anyCh, err := c.ClientReadStreamAsync(workflowID, key)
	if err != nil {
		return nil, err
	}

	typedCh := make(chan StreamValue[R], 1)

	go func() {
		defer close(typedCh)

		serializer := newJSONSerializer[R]()

		for streamValue := range anyCh {
			if streamValue.Err != nil {
				typedCh <- StreamValue[R]{Err: streamValue.Err}
				return
			}

			if streamValue.Closed {
				typedCh <- StreamValue[R]{Closed: true}
				return
			}

			encodedStr, ok := streamValue.Value.(string)
			if !ok {
				typedCh <- StreamValue[R]{Err: fmt.Errorf("stream value is not a string, got %T", streamValue.Value)}
				return
			}

			decodedValue, decodeErr := serializer.Decode(&encodedStr)
			if decodeErr != nil {
				typedCh <- StreamValue[R]{Err: fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)}
				return
			}

			typedCh <- StreamValue[R]{Value: decodedValue}
		}
	}()

	return typedCh, nil
}

// Shutdown gracefully shuts down the client and closes the system database connection.
func (c *client) Shutdown(timeout time.Duration) {
	// Get the concrete dbosContext to access internal fields
	dbosCtx, ok := c.dbosCtx.(*dbosContext)
	if !ok {
		return
	}

	// Close the system database
	if dbosCtx.systemDB != nil {
		// Cancel the context to signal all resources to stop
		dbosCtx.ctxCancelFunc(errors.New("client shutdown initiated"))

		dbosCtx.logger.Debug("Shutting down system database")
		dbosCtx.systemDB.shutdown(dbosCtx, timeout)
	}
}

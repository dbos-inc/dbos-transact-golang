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

type enqueueOptions struct {
	workflowName       string
	workflowID         string
	applicationVersion string
	deduplicationID    string
	priority           uint
	workflowTimeout    time.Duration
	workflowInput      any
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
	serializer := newGobSerializer[P]()
	encodedInput, err := serializer.Encode(input)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize workflow input: %w", err)
	}

	// Call the interface method with the same signature
	handle, err := c.Enqueue(queueName, workflowName, &encodedInput, opts...)
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

// Shutdown gracefully shuts down the client and closes the system database connection.
func (c *client) Shutdown(timeout time.Duration) {
	// Get the concrete dbosContext to access internal fields
	dbosCtx, ok := c.dbosCtx.(*dbosContext)
	if !ok {
		return
	}

	// Close the system database
	if dbosCtx.systemDB != nil {
		dbosCtx.logger.Debug("Shutting down system database")
		dbosCtx.systemDB.shutdown(dbosCtx, timeout)
	}
}

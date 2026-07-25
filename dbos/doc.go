// Package dbos provides lightweight durable workflow orchestration backed by SQLite or Postgres.
//
// DBOS Transact enables developers to write resilient distributed applications using workflows
// and steps backed by a system database: SQLite for zero-setup local development, PostgreSQL
// or CockroachDB for production. All application state is automatically persisted, providing
// exactly-once execution guarantees and automatic recovery from failures.
//
// # Getting Started
//
// Create a DBOS context to start building durable applications:
//
//	dbosContext, err := dbos.NewContext(context.Background(), dbos.Config{
//	    AppName:     "my-app",
//	    DatabaseURL: os.Getenv("DBOS_SYSTEM_DATABASE_URL"),
//	})
//	defer dbos.Shutdown(dbosContext, 5 * time.Second)
//
//	// Register workflows before launching
//	dbos.RegisterWorkflow(dbosContext, myWorkflow)
//
//	// Launch the context to start processing
//	err = dbos.Launch(dbosContext)
//
// # Workflows
//
// Workflows provide durable execution, automatically resuming from the last completed step
// after any failure. Write workflows as normal Go functions that take a Context and
// return serializable values:
//
//	func myWorkflow(ctx dbos.Context, input string) (string, error) {
//	    // Workflow logic here
//	    result, err := dbos.RunAsStep(ctx, someOperation)
//	    if err != nil {
//	        return "", err
//	    }
//	    return result, nil
//	}
//
// Key workflow features:
//   - Automatic recovery: Workflows resume from the last completed step after crashes
//   - Idempotency: Assign workflow IDs to ensure operations run exactly once
//   - Determinism: Workflow functions must be deterministic; use steps for non-deterministic operations
//   - Timeouts: Set durable timeouts that persist across restarts
//   - Events & messaging: Workflows can emit events and receive messages for coordination
//
// # Steps
//
// Steps wrap non-deterministic operations (API calls, random numbers, current time) within workflows.
// If a workflow is interrupted, it resumes from the last completed step:
//
//	func fetchData(ctx context.Context) (string, error) {
//	    resp, err := http.Get("https://api.example.com/data")
//	    // Handle response...
//	    return data, nil
//	}
//
//	func workflow(ctx dbos.Context, input string) (string, error) {
//	    data, err := dbos.RunAsStep(ctx, fetchData,
//	        dbos.WithStepName("fetchData"),
//	        dbos.WithStepMaxRetries(3))
//	    if err != nil {
//	        return "", err
//	    }
//	    return data, nil
//	}
//
// Steps support configurable retries with exponential backoff for handling transient failures.
//
// # Queues
//
// Queues manage workflow concurrency and rate limiting. Register a queue to
// persist its configuration in the system database:
//
//	queue, err := dbos.RegisterQueue(dbosContext, "task_queue",
//	    dbos.WithWorkerConcurrency(5),    // Max 5 concurrent workflows per process
//	    dbos.WithRateLimiter(&dbos.RateLimiter{
//	        Limit:  100,
//	        Period: 60 * time.Second,  // 100 workflows per 60 seconds
//	    }))
//
//	// Enqueue workflows with optional deduplication and priority
//	handle, err := dbos.RunWorkflow(ctx, taskWorkflow, input,
//	    dbos.WithQueue(queue),
//	    dbos.WithDeduplicationID("unique-id"),
//	    dbos.WithPriority(10))
//
// # Workflow Management
//
// DBOS provides comprehensive workflow management capabilities:
//
//	// List workflows
//	workflows, err := dbos.ListWorkflows(ctx)
//
//	// Cancel a running workflow
//	err = dbos.CancelWorkflow(ctx, workflowID)
//
//	// Resume a cancelled workflow
//	err = dbos.ResumeWorkflow(ctx, workflowID)
//
//	// Fork a workflow from a specific step
//	handle, err := dbos.ForkWorkflow[string](ctx, dbos.ForkWorkflowInput{OriginalWorkflowID: originalID, StartStep: stepNumber})
//
// Workflows can also be visualized and managed through the DBOS Console web UI.
//
// # Error Handling
//
// DBOS APIs return *Error values carrying an error code. Match conditions against the
// package sentinels with errors.Is:
//
//	Situation                                            Sentinel
//	Workflow cancelled via CancelWorkflow                ErrWorkflowCancelled (cause matches context.Canceled)
//	Workflow deadline/timeout expired                    ErrWorkflowCancelled (cause matches context.DeadlineExceeded)
//	Awaiting a workflow that was cancelled               ErrAwaitedWorkflowCancelled
//	Recv/GetEvent/GetResult wait timeout                 ErrTimeout
//	Enqueue rejected by deduplication ID                 ErrQueueDeduplicated
//	Workflow ID conflict or duplicate operation          ErrConflictingWorkflowID
//	Workflow ID reused with different function or queue  ErrUnexpectedWorkflow
//	Step exhausted its retries                           ErrMaxStepRetriesExceeded
//	Workflow not found                                   ErrNonExistentWorkflow
//	Queue not found                                      ErrQueueNotFound
//	Schedule not found                                   ErrScheduleNotFound
//
// For example, a parent workflow can detect a cancelled child:
//
//	_, err := handle.GetResult()
//	if errors.Is(err, dbos.ErrAwaitedWorkflowCancelled) { ... }
//
// # Testing
//
// Context is an interface, so workflows can be unit-tested against a mock
// generated with your mocking framework of choice (e.g. mockery):
//
//	func TestWorkflow(t *testing.T) {
//	    mockCtx := NewMockContext(t) // generated from dbos.Context
//	    mockCtx.On("RunAsStep", mockCtx, mock.Anything, mock.Anything).Return("result", nil)
//
//	    result, err := myWorkflow(mockCtx, "input")
//	    assert.NoError(t, err)
//	    assert.Equal(t, "expected", result)
//	}
//
// For detailed documentation and examples, see https://docs.dbos.dev/golang/programming-guide
package dbos

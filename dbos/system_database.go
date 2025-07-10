package dbos

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*******************************/
/******* INTERFACE ********/
/*******************************/

type SystemDatabase interface {
	Destroy()
	ResetSystemDB(ctx context.Context) error
	InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error)
	RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	RecordChildWorkflow(ctx context.Context, input RecordChildWorkflowDBInput) error
	CheckChildWorkflow(ctx context.Context, workflowUUID string, functionID int) (*string, error)
	ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error)
	UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error
	AwaitWorkflowResult(ctx context.Context, workflowID string) (any, error)
	DequeueWorkflows(ctx context.Context, queue WorkflowQueue) ([]dequeuedWorkflow, error)
	ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error)
	CheckOperationExecution(ctx context.Context, input CheckOperationExecutionDBInput) (*RecordedResult, error)
	RecordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error
}

type systemDatabase struct {
	pool *pgxpool.Pool
}

/*******************************/
/******* INITIALIZATION ********/
/*******************************/

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(databaseURL string) error {
	// Connect to the postgres database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to parse database URL: %v", err))
	}

	dbName := parsedURL.Database
	if dbName == "" {
		return NewInitializationError("database name not found in URL")
	}

	serverURL := parsedURL.Copy()
	serverURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), serverURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to connect to PostgreSQL server: %v", err))
	}
	defer conn.Close(context.Background())

	// Create the system database if it doesn't exist
	var exists bool
	err = conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to check if database exists: %v", err))
	}
	if !exists {
		// TODO: validate db name
		createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(context.Background(), createSQL)
		if err != nil {
			return NewInitializationError(fmt.Sprintf("failed to create database %s: %v", dbName, err))
		}
	}

	return nil
}

//go:embed migrations/*.sql
var migrationFiles embed.FS

// TODO: must use the systemdb name
func runMigrations(databaseURL string) error {
	// Change the driver to pgx5
	databaseURL = "pgx5://" + strings.TrimPrefix(databaseURL, "postgres://")

	// Create migration source from embedded files
	d, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to create migration source: %v", err))
	}

	// Create migrator
	m, err := migrate.NewWithSourceInstance("iofs", d, databaseURL)
	if err != nil {
		return NewInitializationError(fmt.Sprintf("failed to create migrator: %v", err))
	}
	defer m.Close()

	// Run migrations
	// FIXME: tolerate errors when the migration is bcz we run an older version of transact
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return NewInitializationError(fmt.Sprintf("failed to run migrations: %v", err))
	}

	return nil
}

// New creates a new SystemDatabase instance and runs migrations
func NewSystemDatabase() (SystemDatabase, error) {
	// TODO: pass proper config
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" {
		return nil, NewInitializationError("DBOS_DATABASE_URL environment variable is required")
	}

	// Create the database if it doesn't exist
	if err := createDatabaseIfNotExists(databaseURL); err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to create database: %v", err))
	}

	// Run migrations first
	if err := runMigrations(databaseURL); err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to run migrations: %v", err))
	}

	// Create pgx pool
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, NewInitializationError(fmt.Sprintf("failed to create connection pool: %v", err))
	}

	// Test the connection
	// FIXME: remove this
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, NewInitializationError(fmt.Sprintf("failed to ping database: %v", err))
	}

	return &systemDatabase{
		pool: pool,
	}, nil
}

func (s *systemDatabase) Destroy() {
	fmt.Println("Closing system database connection pool")
	s.pool.Close()
}

/*******************************/
/******* WORKFLOWS ********/
/*******************************/

type InsertWorkflowResult struct {
	Attempts                int                `json:"attempts"`
	Status                  WorkflowStatusType `json:"status"`
	Name                    string             `json:"name"`
	QueueName               *string            `json:"queue_name"`
	WorkflowDeadlineEpochMs *int64             `json:"workflow_deadline_epoch_ms"`
}

type InsertWorkflowStatusDBInput struct {
	Status WorkflowStatus
	Tx     pgx.Tx
}

func (s *systemDatabase) InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error) {
	if input.Tx == nil {
		return nil, errors.New("transaction is required for InsertWorkflowStatus")
	}

	// Set default values
	attempts := 1
	if input.Status.Status == WorkflowStatusEnqueued {
		attempts = 0
	}

	updatedAt := time.Now()
	if !input.Status.UpdatedAt.IsZero() {
		updatedAt = input.Status.UpdatedAt
	}

	var deadline *int64 = nil
	if !input.Status.Deadline.IsZero() {
		millis := input.Status.Deadline.UnixMilli()
		deadline = &millis
	}

	var timeoutMs int64 = 0
	if input.Status.Timeout > 0 {
		timeoutMs = input.Status.Timeout.Milliseconds()
	}

	inputString, err := serialize(input.Status.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize input: %w", err)
	}

	query := `INSERT INTO dbos.workflow_status (
        workflow_uuid,
        status,
        name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        executor_id,
        application_version,
        application_id,
        created_at,
        recovery_attempts,
        updated_at,
        workflow_timeout_ms,
        workflow_deadline_epoch_ms,
        inputs,
        deduplication_id,
        priority
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
			recovery_attempts = CASE
                WHEN EXCLUDED.status != $19 THEN workflow_status.recovery_attempts + 1
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status = $20 THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, workflow_deadline_epoch_ms`

	var result InsertWorkflowResult
	err = input.Tx.QueryRow(ctx, query,
		input.Status.ID,
		input.Status.Status,
		input.Status.Name,
		input.Status.QueueName,
		input.Status.AuthenticatedUser,
		input.Status.AssumedRole,
		input.Status.AuthenticatedRoles,
		input.Status.ExecutorID,
		input.Status.ApplicationVersion,
		input.Status.ApplicationID,
		input.Status.CreatedAt.UnixMilli(),
		attempts,
		updatedAt.UnixMilli(),
		timeoutMs,
		deadline,
		inputString,
		input.Status.DeduplicationID,
		input.Status.Priority,
		WorkflowStatusEnqueued,
		WorkflowStatusEnqueued,
	).Scan(
		&result.Attempts,
		&result.Status,
		&result.Name,
		&result.QueueName,
		&result.WorkflowDeadlineEpochMs,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	if len(input.Status.Name) > 0 && result.Name != input.Status.Name {
		return nil, NewConflictingWorkflowError(input.Status.ID, fmt.Sprintf("Workflow already exists with a different name: %s, but the provided name is: %s", result.Name, input.Status.Name))
	}
	if len(input.Status.QueueName) > 0 && result.QueueName != nil && input.Status.QueueName != *result.QueueName {
		fmt.Printf("WARNING: Queue name conflict for workflow %s: %s vs %s\n", input.Status.ID, *result.QueueName, input.Status.QueueName)
	}

	// TODO implement max retries

	return &result, nil
}

// ListWorkflowsInput represents the input parameters for listing workflows
type ListWorkflowsDBInput struct {
	WorkflowName       string
	QueueName          string
	WorkflowIDPrefix   string
	WorkflowIDs        []string
	AuthenticatedUser  string
	StartTime          time.Time
	EndTime            time.Time
	Status             []WorkflowStatusType
	ApplicationVersion string
	ExecutorIDs        []string
	Limit              *int
	Offset             *int
	SortDesc           bool
	Tx                 pgx.Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *systemDatabase) ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error) {
	qb := newQueryBuilder()

	// Build the base query
	baseQuery := `SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
	                 output, error, executor_id, created_at, updated_at, application_version, application_id,
	                 recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
					 deduplication_id, inputs, priority
	          FROM dbos.workflow_status`

	// Add filters using query builder
	if input.WorkflowName != "" {
		qb.addWhere("name", input.WorkflowName)
	}
	if input.QueueName != "" {
		qb.addWhere("queue_name", input.QueueName)
	}
	if input.WorkflowIDPrefix != "" {
		qb.addWhereLike("workflow_uuid", input.WorkflowIDPrefix+"%")
	}
	if len(input.WorkflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.WorkflowIDs)
	}
	if input.AuthenticatedUser != "" {
		qb.addWhere("authenticated_user", input.AuthenticatedUser)
	}
	if !input.StartTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.StartTime.UnixMilli())
	}
	if !input.EndTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.EndTime.UnixMilli())
	}
	if len(input.Status) > 0 {
		qb.addWhereAny("status", input.Status)
	}
	if input.ApplicationVersion != "" {
		qb.addWhere("application_version", input.ApplicationVersion)
	}
	if len(input.ExecutorIDs) > 0 {
		qb.addWhereAny("executor_id", input.ExecutorIDs)
	}

	// Build complete query
	var query string
	if len(qb.whereClauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", baseQuery, strings.Join(qb.whereClauses, " AND "))
	} else {
		query = baseQuery
	}

	// Add sorting
	if input.SortDesc {
		query += " ORDER BY created_at DESC"
	} else {
		query += " ORDER BY created_at ASC"
	}

	// Add limit and offset
	if input.Limit != nil {
		qb.argCounter++
		query += fmt.Sprintf(" LIMIT $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Limit)
	}

	if input.Offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Offset)
	}

	// Execute the query
	var rows pgx.Rows
	var err error

	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute ListWorkflows query: %w", err)
	}
	defer rows.Close()

	var workflows []WorkflowStatus
	for rows.Next() {
		var wf WorkflowStatus
		var createdAtMs, updatedAtMs int64
		var timeoutMs *int64
		var deadlineMs, startedAtMs *int64
		var outputString, inputString *string
		var errorStr *string

		err := rows.Scan(
			&wf.ID, &wf.Status, &wf.Name, &wf.AuthenticatedUser, &wf.AssumedRole,
			&wf.AuthenticatedRoles, &outputString, &errorStr, &wf.ExecutorID, &createdAtMs,
			&updatedAtMs, &wf.ApplicationVersion, &wf.ApplicationID,
			&wf.Attempts, &wf.QueueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &wf.DeduplicationID,
			&inputString, &wf.Priority,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow row: %w", err)
		}

		// Convert milliseconds to time.Time
		wf.CreatedAt = time.Unix(0, createdAtMs*int64(time.Millisecond))
		wf.UpdatedAt = time.Unix(0, updatedAtMs*int64(time.Millisecond))

		// Convert timeout milliseconds to time.Duration
		if timeoutMs != nil && *timeoutMs > 0 {
			wf.Timeout = time.Duration(*timeoutMs) * time.Millisecond
		}

		// Convert deadline milliseconds to time.Time
		if deadlineMs != nil {
			wf.Deadline = time.Unix(0, *deadlineMs*int64(time.Millisecond))
		}

		// Convert started at milliseconds to time.Time
		if startedAtMs != nil {
			wf.StartedAt = time.Unix(0, *startedAtMs*int64(time.Millisecond))
		}

		// Convert error string to error type if present
		if errorStr != nil && *errorStr != "" {
			wf.Error = errors.New(*errorStr)
		}

		// XXX maybe set wf.Output to outputString and run deserialize out of system DB
		wf.Output, err = deserialize(outputString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize output: %w", err)
		}

		wf.Input, err = deserialize(inputString)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize input: %w", err)
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type UpdateWorkflowOutcomeDBInput struct {
	workflowID string
	status     WorkflowStatusType
	output     any
	err        error
	tx         pgx.Tx
}

// Will evolve as we serialize all output and error types
func (s *systemDatabase) UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error {
	query := `UPDATE dbos.workflow_status
			  SET status = $1, output = $2, error = $3, updated_at = $4, deduplication_id = NULL
			  WHERE workflow_uuid = $5`

	outputString, err := serialize(input.output)
	if err != nil {
		return fmt.Errorf("failed to serialize output: %w", err)
	}

	var errorStr string
	if input.err != nil {
		errorStr = input.err.Error()
	}

	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.status, outputString, errorStr, time.Now().UnixMilli(), input.workflowID)
	} else {
		_, err = s.pool.Exec(ctx, query, input.status, outputString, errorStr, time.Now().UnixMilli(), input.workflowID)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

func (s *systemDatabase) CancelWorkflow(ctx context.Context, workflowID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Rollback if not committed

	// Check if workflow exists
	listInput := ListWorkflowsDBInput{
		WorkflowIDs: []string{workflowID},
		Tx:          tx,
	}
	wfs, err := s.ListWorkflows(ctx, listInput)
	if err != nil {
		return err
	}
	if len(wfs) == 0 {
		return NewNonExistentWorkflowError(workflowID)
	}

	wf := wfs[0]
	switch wf.Status {
	case "SUCCESS", "ERROR", "CANCELLED":
		// Workflow is already in a terminal state, rollback and return
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}

	//Set the workflow's status to CANCELLED
	updateStatusQuery := `UPDATE dbos.workflow_status
						  SET status = 'CANCELLED', updated_at = $1
						  WHERE workflow_uuid = $2`

	_, err = tx.Exec(ctx, updateStatusQuery, time.Now().UnixMilli(), workflowID)
	if err != nil {
		return fmt.Errorf("failed to update workflow status to CANCELLED: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *systemDatabase) AwaitWorkflowResult(ctx context.Context, workflowID string) (any, error) {
	query := `SELECT status, output, error FROM dbos.workflow_status WHERE workflow_uuid = $1`
	var status WorkflowStatusType
	for {
		row := s.pool.QueryRow(ctx, query, workflowID)
		var outputString *string
		var errorStr *string
		err := row.Scan(&status, &outputString, &errorStr)
		if err != nil {
			if err == pgx.ErrNoRows {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		switch status {
		case WorkflowStatusSuccess:
			// Deserialize output from TEXT to bytes then from bytes to R using gob
			output, err := deserialize(outputString)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize output: %w", err)
			}
			return output, nil
		case WorkflowStatusError:
			return nil, errors.New(*errorStr) // Assuming errorStr can be converted to an error type
		case WorkflowStatusCancelled:
			return nil, NewAwaitedWorkflowCancelledError(workflowID)
		default:
			time.Sleep(1 * time.Second) // Wait before checking again
		}
	}
}

type recordOperationResultDBInput struct {
	workflowID    string
	operationID   int
	operationName string
	output        any
	err           error
}

func (s *systemDatabase) RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, output, error, function_name)
            VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	outputString, err := serialize(input.output)
	if err != nil {
		return fmt.Errorf("failed to serialize output: %w", err)
	}

	commandTag, err := s.pool.Exec(ctx, query,
		input.workflowID,
		input.operationID,
		outputString,
		errorString,
		input.operationName,
	)

	/*
		fmt.Printf("RecordOperationResult - CommandTag: %v\n", commandTag)
		fmt.Printf("RecordOperationResult - Rows affected: %d\n", commandTag.RowsAffected())
		fmt.Printf("RecordOperationResult - SQL: %s\n", commandTag.String())
	*/

	// TODO handle serialization errors
	if err != nil {
		fmt.Printf("RecordOperationResult - Error occurred: %v\n", err)
		return fmt.Errorf("failed to record operation result: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		fmt.Printf("RecordOperationResult - WARNING: No rows were affected by the insert\n")
	}

	return nil
}

/*******************************/
/******* CHILD WORKFLOWS ********/
/*******************************/

type RecordChildWorkflowDBInput struct {
	ParentWorkflowID string
	ChildWorkflowID  string
	FunctionID       int
	FunctionName     string
	Tx               pgx.Tx
}

func (s *systemDatabase) RecordChildWorkflow(ctx context.Context, input RecordChildWorkflowDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)`

	var commandTag pgconn.CommandTag
	var err error

	if input.Tx != nil {
		commandTag, err = input.Tx.Exec(ctx, query,
			input.ParentWorkflowID,
			input.FunctionID,
			input.FunctionName,
			input.ChildWorkflowID,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.ParentWorkflowID,
			input.FunctionID,
			input.FunctionName,
			input.ChildWorkflowID,
		)
	}

	if err != nil {
		// Check for unique constraint violation (conflict ID error)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			return fmt.Errorf(
				"child workflow %s already registered for parent workflow %s (operation ID: %d)",
				input.ChildWorkflowID, input.ParentWorkflowID, input.FunctionID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		fmt.Printf("RecordChildWorkflow - WARNING: No rows were affected by the insert\n")
	}

	return nil
}

func (s *systemDatabase) CheckChildWorkflow(ctx context.Context, workflowID string, functionID int) (*string, error) {
	query := `SELECT child_workflow_id
              FROM dbos.operation_outputs
              WHERE workflow_uuid = $1 AND function_id = $2`

	var childWorkflowID *string
	err := s.pool.QueryRow(ctx, query, workflowID, functionID).Scan(&childWorkflowID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to check child workflow: %w", err)
	}

	return childWorkflowID, nil
}

type recordChildGetResultDBInput struct {
	parentWorkflowID string
	childWorkflowID  string
	operationID      int
	output           string
	err              error
}

func (s *systemDatabase) RecordChildGetResult(ctx context.Context, input recordChildGetResultDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id)
            VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT DO NOTHING`

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	_, err := s.pool.Exec(ctx, query,
		input.parentWorkflowID,
		input.operationID,
		"DBOS.getResult",
		input.output,
		errorString,
		input.childWorkflowID,
	)
	if err != nil {
		return fmt.Errorf("failed to record get result: %w", err)
	}
	return nil
}

/*******************************/
/******* STEPS ********/
/*******************************/

type RecordedResult struct {
	output any
	err    error
}

type CheckOperationExecutionDBInput struct {
	workflowID   string
	operationID  int
	functionName string
}

func (s *systemDatabase) CheckOperationExecution(ctx context.Context, input CheckOperationExecutionDBInput) (*RecordedResult, error) {
	// Create transaction for this operation
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // We never really need to commit this transaction

	// First query: Retrieve the workflow status
	workflowStatusQuery := `SELECT status FROM dbos.workflow_status WHERE workflow_uuid = $1`

	// Second query: Retrieve operation outputs if they exist
	operationOutputQuery := `SELECT output, error, function_name
							 FROM dbos.operation_outputs
							 WHERE workflow_uuid = $1 AND function_id = $2`

	var workflowStatus WorkflowStatusType

	// Execute first query to get workflow status
	err = tx.QueryRow(ctx, workflowStatusQuery, input.workflowID).Scan(&workflowStatus)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, NewNonExistentWorkflowError(input.workflowID)
		}
		return nil, fmt.Errorf("failed to get workflow status: %w", err)
	}

	// If the workflow is cancelled, raise the exception
	if workflowStatus == WorkflowStatusCancelled {
		return nil, NewWorkflowCancelledError(input.workflowID)
	}

	// Execute second query to get operation outputs
	var outputString *string
	var errorStr *string
	var recordedFunctionName string

	err = tx.QueryRow(ctx, operationOutputQuery, input.workflowID, input.operationID).Scan(&outputString, &errorStr, &recordedFunctionName)

	// If there are no operation outputs, return nil
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get operation outputs: %w", err)
	}

	// If the provided and recorded function name are different, throw an exception
	if input.functionName != recordedFunctionName {
		return nil, NewUnexpectedStepError(input.workflowID, input.operationID, input.functionName, recordedFunctionName)
	}

	output, err := deserialize(outputString)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize output: %w", err)
	}

	var recordedError error
	if errorStr != nil && *errorStr != "" {
		recordedError = errors.New(*errorStr)
	}
	result := &RecordedResult{
		output: output,
		err:    recordedError,
	}
	return result, nil
}

/*******************************/
/******* QUEUES ********/
/*******************************/

type dequeuedWorkflow struct {
	id    string
	name  string
	input string
}

func (s *systemDatabase) DequeueWorkflows(ctx context.Context, queue WorkflowQueue) ([]dequeuedWorkflow, error) {
	// Begin transaction with snapshot isolation
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Set transaction isolation level to repeatable read (similar to snapshot isolation)
	_, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		return nil, fmt.Errorf("failed to set transaction isolation level: %w", err)
	}

	// First check the rate limiter
	startTimeMs := time.Now().UnixMilli()
	var numRecentQueries int
	if queue.Limiter != nil {
		limiterPeriod := time.Duration(queue.Limiter.Period * float64(time.Second))

		// Calculate the cutoff time: current time minus limiter period
		cutoffTimeMs := time.Now().Add(-limiterPeriod).UnixMilli()

		// Count workflows that have started in the limiter period
		limiterQuery := `
		SELECT COUNT(*)
		FROM dbos.workflow_status
		WHERE queue_name = $1
		  AND status != $2
		  AND started_at_epoch_ms > $3`

		err := tx.QueryRow(ctx, limiterQuery,
			queue.Name,
			WorkflowStatusEnqueued,
			cutoffTimeMs).Scan(&numRecentQueries)
		if err != nil {
			return nil, fmt.Errorf("failed to query rate limiter: %w", err)
		}

		if numRecentQueries >= queue.Limiter.Limit {
			return []dequeuedWorkflow{}, nil
		}
	}

	// Calculate max_tasks based on concurrency limits
	maxTasks := queue.MaxTasksPerIteration

	if queue.WorkerConcurrency != nil || queue.GlobalConcurrency != nil {
		// Count pending workflows by executor
		pendingQuery := `
			SELECT executor_id, COUNT(*) as task_count
			FROM dbos.workflow_status
			WHERE queue_name = $1 AND status = $2
			GROUP BY executor_id`

		rows, err := tx.Query(ctx, pendingQuery, queue.Name, WorkflowStatusPending)
		if err != nil {
			return nil, fmt.Errorf("failed to query pending workflows: %w", err)
		}
		defer rows.Close()

		pendingWorkflowsDict := make(map[string]int)
		for rows.Next() {
			var executorIDRow string
			var taskCount int
			if err := rows.Scan(&executorIDRow, &taskCount); err != nil {
				return nil, fmt.Errorf("failed to scan pending workflow row: %w", err)
			}
			pendingWorkflowsDict[executorIDRow] = taskCount
		}

		localPendingWorkflows := pendingWorkflowsDict[EXECUTOR_ID]

		// Check worker concurrency limit
		if queue.WorkerConcurrency != nil {
			workerConcurrency := *queue.WorkerConcurrency
			if localPendingWorkflows > workerConcurrency {
				fmt.Printf("WARNING: Local pending workflows (%d) on queue %s exceeds worker concurrency limit (%d)\n",
					localPendingWorkflows, queue.Name, workerConcurrency)
			}
			availableWorkerTasks := max(workerConcurrency-localPendingWorkflows, 0)
			maxTasks = uint(availableWorkerTasks)
		}

		// Check global concurrency limit
		if queue.GlobalConcurrency != nil {
			globalPendingWorkflows := 0
			for _, count := range pendingWorkflowsDict {
				globalPendingWorkflows += count
			}

			concurrency := *queue.GlobalConcurrency
			if globalPendingWorkflows > concurrency {
				fmt.Printf("WARNING: Total pending workflows (%d) on queue %s exceeds global concurrency limit (%d)\n",
					globalPendingWorkflows, queue.Name, concurrency)
			}
			availableTasks := max(concurrency-globalPendingWorkflows, 0)
			if uint(availableTasks) < maxTasks {
				maxTasks = uint(availableTasks)
			}
		}
	}

	// Build the query to select workflows for dequeueing
	// Use SKIP LOCKED when no global concurrency is set to avoid blocking,
	// otherwise use NOWAIT to ensure consistent view across processes
	skipLocks := queue.GlobalConcurrency == nil
	var lockClause string
	if skipLocks {
		lockClause = "FOR UPDATE SKIP LOCKED"
	} else {
		lockClause = "FOR UPDATE NOWAIT"
	}

	var query string
	if queue.PriorityEnabled {
		query = fmt.Sprintf(`
			SELECT workflow_uuid
			FROM dbos.workflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND (application_version = $3 OR application_version IS NULL)
			ORDER BY priority ASC, created_at ASC
			%s`, lockClause)
	} else {
		query = fmt.Sprintf(`
			SELECT workflow_uuid
			FROM dbos.workflow_status
			WHERE queue_name = $1
			  AND status = $2
			  AND (application_version = $3 OR application_version IS NULL)
			ORDER BY created_at ASC
			%s`, lockClause)
	}

	// Add limit if maxTasks is finite
	if maxTasks != MaxInt {
		query += fmt.Sprintf(" LIMIT %d", int(maxTasks))
	}

	// Execute the query to get workflow IDs
	rows, err := tx.Query(ctx, query, queue.Name, WorkflowStatusEnqueued, APP_VERSION)
	if err != nil {
		return nil, fmt.Errorf("failed to query enqueued workflows: %w", err)
	}
	defer rows.Close()

	var dequeuedIDs []string
	for rows.Next() {
		var workflowID string
		if err := rows.Scan(&workflowID); err != nil {
			return nil, fmt.Errorf("failed to scan workflow ID: %w", err)
		}
		dequeuedIDs = append(dequeuedIDs, workflowID)
	}

	if len(dequeuedIDs) > 0 {
		fmt.Printf("[%s] attempting to dequeue %d task(s)\n", queue.Name, len(dequeuedIDs))
	}

	// Update workflows to PENDING status and get their details
	var retWorkflows []dequeuedWorkflow
	for _, id := range dequeuedIDs {
		// If we have a limiter, stop dequeueing workflows when the number of workflows started this period exceeds the limit.
		if queue.Limiter != nil {
			if len(retWorkflows)+numRecentQueries >= queue.Limiter.Limit {
				break
			}
		}
		retWorkflow := dequeuedWorkflow{
			id: id,
		}

		// Update workflow status to PENDING and return name and inputs
		updateQuery := `
			UPDATE dbos.workflow_status
			SET status = $1,
			    application_version = $2,
			    executor_id = $3,
			    started_at_epoch_ms = $4,
			    workflow_deadline_epoch_ms = CASE
			        WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
			        THEN EXTRACT(epoch FROM NOW()) * 1000 + workflow_timeout_ms
			        ELSE workflow_deadline_epoch_ms
			    END
			WHERE workflow_uuid = $5
			RETURNING name, inputs`

		var inputString *string
		err := tx.QueryRow(ctx, updateQuery,
			WorkflowStatusPending,
			APP_VERSION,
			EXECUTOR_ID,
			startTimeMs,
			id).Scan(&retWorkflow.name, &inputString)

		if inputString != nil && len(*inputString) > 0 {
			retWorkflow.input = *inputString
		}

		if err != nil {
			return nil, fmt.Errorf("failed to update workflow %s during dequeue: %w", id, err)
		}
		retWorkflows = append(retWorkflows, retWorkflow)
	}

	// Commit only if workflows were dequeued. Avoids WAL bloat and XID advancement.
	if len(retWorkflows) > 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}
	}

	return retWorkflows, nil
}

func (s *systemDatabase) ClearQueueAssignment(ctx context.Context, workflowID string) (bool, error) {
	query := `UPDATE dbos.workflow_status
			  SET status = $1, started_at_epoch_ms = NULL
			  WHERE workflow_uuid = $2
			    AND queue_name IS NOT NULL
			    AND status = $3`

	commandTag, err := s.pool.Exec(ctx, query,
		WorkflowStatusEnqueued,
		workflowID,
		WorkflowStatusPending)

	if err != nil {
		return false, fmt.Errorf("failed to clear queue assignment for workflow %s: %w", workflowID, err)
	}

	// If no rows were affected, the workflow is not anymore in the queue or was already completed
	return commandTag.RowsAffected() > 0, nil
}

/*******************************/
/******* UTILS ********/
/*******************************/

func (s *systemDatabase) ResetSystemDB(ctx context.Context) error {
	// Get the current database configuration from the pool
	config := s.pool.Config()
	if config == nil || config.ConnConfig == nil {
		return fmt.Errorf("failed to get pool configuration")
	}

	// Extract the database name before closing the pool
	dbName := config.ConnConfig.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in pool configuration")
	}

	// Close the current pool before dropping the database
	s.pool.Close()

	// Create a new connection configuration pointing to the postgres database
	postgresConfig := config.ConnConfig.Copy()
	postgresConfig.Database = "postgres"

	// Connect to the postgres database
	conn, err := pgx.ConnectConfig(ctx, postgresConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}
	defer conn.Close(ctx)

	// Drop the database
	dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", pgx.Identifier{dbName}.Sanitize())
	_, err = conn.Exec(ctx, dropSQL)
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %w", dbName, err)
	}

	return nil
}

type queryBuilder struct {
	setClauses   []string
	whereClauses []string
	args         []any
	argCounter   int
}

func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		setClauses:   make([]string, 0),
		whereClauses: make([]string, 0),
		args:         make([]any, 0),
		argCounter:   0,
	}
}

func (qb *queryBuilder) addSet(column string, value any) {
	qb.argCounter++
	qb.setClauses = append(qb.setClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addSetRaw(clause string) {
	qb.setClauses = append(qb.setClauses, clause)
}

func (qb *queryBuilder) addWhere(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLike(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s LIKE $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereAny(column string, values any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s = ANY($%d)", column, qb.argCounter))
	qb.args = append(qb.args, values)
}

func (qb *queryBuilder) addWhereGreaterEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s >= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLessEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s <= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

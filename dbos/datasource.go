package dbos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// datasource.go: user-provided data sources (transactions).
//
// A DataSource wraps a database engine the user already owns (a *pgxpool.Pool
// or a *sql.DB) so a single transaction can write BOTH application tables AND a
// DBOS durability record. That record lives in a `transaction_completion` table
// in the USER's database.

const transactionCompletionTable = "transaction_completion"

// DataSource is a handle to a user-provided database engine registered with
// DBOS. It is returned by RegisterDataSource and passed to RunAsTransaction.
type DataSource struct {
	name    string
	pool    Pool
	dialect Dialect
	schema  string
	logger  *slog.Logger

	// sameAsSystemDB is set at Launch when this data source's pool is the very
	// same engine handle as the DBOS system database.
	sameAsSystemDB bool
}

// Name returns the data source's name, as given to RegisterDataSource.
func (ds *DataSource) Name() string { return ds.name }

type dataSourceOptions struct {
	schema string
}

// DataSourceOption configures a data source at registration time.
type DataSourceOption func(*dataSourceOptions)

// WithDataSourceSchema overrides the schema that holds the transaction_completion
// table (default "dbos"). Ignored by SQLite, which has no schemas.
func WithDataSourceSchema(schema string) DataSourceOption {
	return func(o *dataSourceOptions) { o.schema = schema }
}

// Engine is the set of database engine types that can back a DataSource:
// *pgxpool.Pool (Postgres/CockroachDB) or *sql.DB (SQLite). It is a generic
// constraint, not an ordinary type, so RegisterDataSource rejects any other
// engine type at compile time rather than at runtime.
type Engine interface {
	*pgxpool.Pool | *sql.DB
}

// RegisterDataSource registers a user-provided database engine under a unique
// name so its transactions can be made durable via RunAsTransaction. The engine
// type is constrained to *pgxpool.Pool (Postgres/CockroachDB) or *sql.DB
// (SQLite). It must be called before Launch; the transaction_completion table
// is created during Launch.
//
// All misuse — a nil engine, an empty or already-registered name, or
// registering after Launch — panics, consistent with RegisterWorkflow.
//
// Example:
//
//	pool, _ := pgxpool.New(ctx, appDatabaseURL)
//	ds := dbos.RegisterDataSource(ctx, "app", pool)
func RegisterDataSource[E Engine](ctx DBOSContext, name string, engine E, opts ...DataSourceOption) *DataSource {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	c, ok := ctx.(*dbosContext)
	if !ok {
		panic("RegisterDataSource requires a concrete DBOS context")
	}
	if c.launched.Load() {
		panic("cannot register a data source after DBOS has launched")
	}
	if name == "" {
		panic("data source name cannot be empty")
	}

	options := dataSourceOptions{schema: _DEFAULT_SYSTEM_DB_SCHEMA}
	for _, opt := range opts {
		opt(&options)
	}

	// The constraint makes the switch exhaustive. A typed nil pointer still
	// satisfies the constraint, so each case re-checks the concrete value.
	// pgx pools default to the Postgres dialect; CockroachDB is detected at
	// Launch (postgresDialect is wire-compatible with CRDB for everything a
	// data source uses, so this default is already correct).
	var (
		pool    Pool
		dialect Dialect
	)
	switch e := any(engine).(type) {
	case *pgxpool.Pool:
		if e == nil {
			panic("data source engine (*pgxpool.Pool) is nil")
		}
		pool = newPgxPool(e)
		dialect = postgresDialect{} // resolve CRDB at launch time
	case *sql.DB:
		if e == nil {
			panic("data source engine (*sql.DB) is nil")
		}
		pool = newSQLPool(e)
		dialect = sqliteDialect{}
	}

	ds := &DataSource{
		name:    name,
		pool:    pool,
		dialect: dialect,
		schema:  options.schema,
		logger:  c.logger.With("service", "datasource", "data_source", name),
	}

	c.dataSourcesMu.Lock()
	defer c.dataSourcesMu.Unlock()
	for _, existing := range c.dataSources {
		if existing.name == name {
			panic(fmt.Sprintf("a data source named %q is already registered", name))
		}
	}
	c.dataSources = append(c.dataSources, ds)

	c.logger.Debug("Registered data source", "data_source", name, "dialect", dialect.Name(), "schema", ds.schema)
	return ds
}

// qualifiedCompletionTable returns the schema-qualified transaction_completion
// table name for this data source's dialect (e.g. `"dbos".transaction_completion`
// for Postgres, `transaction_completion` for SQLite).
func (ds *DataSource) qualifiedCompletionTable() string {
	return ds.dialect.SchemaPrefix(ds.schema) + transactionCompletionTable
}

// completionTableStatements returns the DDL that creates the durability table
// (and, for Postgres, its schema).
func (ds *DataSource) completionTableStatements() []string {
	table := ds.qualifiedCompletionTable()
	if ds.dialect.Name() == DialectSQLite {
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	workflow_id TEXT NOT NULL,
	step_id INTEGER NOT NULL,
	output TEXT,
	error TEXT,
	serialization TEXT,
	created_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workflow_id, step_id)
)`, table)}
	}
	return []string{
		fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, pgx.Identifier{ds.schema}.Sanitize()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	workflow_id TEXT NOT NULL,
	step_id INT NOT NULL,
	output TEXT,
	error TEXT,
	serialization TEXT,
	created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
	PRIMARY KEY (workflow_id, step_id)
)`, table),
	}
}

// resolveDialect refines a Postgres data source to the CockroachDB dialect when
// the underlying pool is actually CRDB. The two are wire-compatible and share
// postgresDialect for everything a data source needs, so this only keeps the
// dialect Name accurate (for logs) and future-proofs any later divergence.
// No-op for *sql.DB (SQLite), whose dialect is already final.
func (ds *DataSource) resolveDialect(ctx context.Context) error {
	pgxPool := PgxPool(ds.pool)
	if pgxPool == nil {
		return nil
	}
	crdb, err := retryWithResult(ctx, func() (bool, error) {
		conn, err := pgxPool.Acquire(ctx)
		if err != nil {
			return false, err
		}
		defer conn.Release()
		return isCockroachDB(ctx, conn.Conn()), nil
	}, withRetrierLogger(ds.logger))
	if err != nil {
		return err
	}
	if crdb {
		ds.dialect = cockroachDialect{}
		ds.logger.Debug("Detected CockroachDB data source")
	}
	return nil
}

// createCompletionTable creates the transaction_completion table in the user's
// database.
func (ds *DataSource) createCompletionTable(ctx context.Context) error {
	for _, stmt := range ds.completionTableStatements() {
		query := stmt
		if err := retry(ctx, func() error {
			_, execErr := ds.pool.Exec(ctx, query)
			return execErr
		}, withRetrierLogger(ds.logger)); err != nil {
			return fmt.Errorf("creating %s table: %w", transactionCompletionTable, err)
		}
	}
	return nil
}

// launchDataSources prepares every registered data source. A data source that
// shares the system database's pool needs no durability table. Every
// other data source gets its transaction_completion table created here.
func (c *dbosContext) launchDataSources(ctx context.Context) error {
	c.dataSourcesMu.Lock()
	defer c.dataSourcesMu.Unlock()
	sysPool := c.systemDB.(*sysDB).pool
	for _, ds := range c.dataSources {
		if sameEngine(ds.pool, sysPool) {
			ds.sameAsSystemDB = true
			c.logger.Debug("Data source shares the system database; using single-transaction durability", "data_source", ds.name)
			continue
		}
		if err := ds.resolveDialect(ctx); err != nil {
			return fmt.Errorf("data source %q: %w", ds.name, err)
		}
		if err := ds.createCompletionTable(ctx); err != nil {
			return fmt.Errorf("data source %q: %w", ds.name, err)
		}
		c.logger.Debug("Initialized data source", "data_source", ds.name, "dialect", ds.dialect.Name())
	}
	return nil
}

// completionRecord is a row from the transaction_completion table. Only the
// success case stores output (error stays nil); the error column exists for
// cross-SDK schema parity but the Go implementation records failures in the
// system database's operation_outputs instead.
type completionRecord struct {
	output        *string
	errStr        *string
	serialization string
}

// checkCompletion reads the durability row for (workflowID, stepID), or returns
// (nil, nil) when none exists.
func (ds *DataSource) checkCompletion(ctx context.Context, q Querier, workflowID string, stepID int) (*completionRecord, error) {
	query := ds.dialect.RewriteQuery(fmt.Sprintf(
		`SELECT output, error, serialization FROM %s WHERE workflow_id = $1 AND step_id = $2`, ds.qualifiedCompletionTable()))
	var (
		rec           completionRecord
		serialization *string
	)
	if err := q.QueryRow(ctx, query, workflowID, stepID).Scan(&rec.output, &rec.errStr, &serialization); err != nil {
		if errors.Is(err, ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if serialization != nil {
		rec.serialization = *serialization
	}
	return &rec, nil
}

// recordCompletion writes the durability row for (workflowID, stepID).
// A duplicate row surfaces as a workflow-conflict error.
func (ds *DataSource) recordCompletion(ctx context.Context, q Querier, workflowID string, stepID int, output, errStr *string, serialization string) error {
	query := ds.dialect.RewriteQuery(fmt.Sprintf(
		`INSERT INTO %s (workflow_id, step_id, output, error, serialization, created_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		ds.qualifiedCompletionTable()))
	if _, err := q.Exec(ctx, query, workflowID, stepID, output, errStr, serialization, time.Now().UnixMilli()); err != nil {
		if ds.dialect.IsUniqueViolation(err) {
			return newWorkflowConflictIDError(workflowID)
		}
		return err
	}
	return nil
}

// RunAsTransaction durably executes fn as a transaction against the data source
// ds. fn receives a portable Tx; within it the application can write its own
// tables and DBOS atomically records a durability row, so the step runs
// exactly once even across crashes and recovery.
//
// It must be called from within a workflow. Standard StepOptions apply
// (WithStepName, WithStepMaxRetries, retry predicate, WithStepTxIsolation).
//
// A mock DBOSContext is supported for testing: the call dispatches through the
// context's RunAsTransaction method, so a mock can intercept it, just like
// RunAsStep. convertStepResult handles the mock-returned result.
//
// Example:
//
//	n, err := dbos.RunAsTransaction(ctx, ds, func(ctx context.Context, tx dbos.Tx) (int64, error) {
//	    res, err := tx.Exec(ctx, "INSERT INTO orders(item) VALUES ($1)", item)
//	    if err != nil {
//	        return 0, err
//	    }
//	    return res.RowsAffected()
//	})
func RunAsTransaction[R any](ctx DBOSContext, ds *DataSource, fn txn[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}
	if ds == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("data source cannot be nil"))
	}
	if fn == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("transaction function cannot be nil"))
	}

	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	typeErasedFn := TxnFunc(func(ctx context.Context, tx Tx) (any, error) { return fn(ctx, tx) })

	result, err := ctx.RunAsTransaction(ctx, ds, typeErasedFn, opts...)
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

// RunAsTransaction splits durability across two databases: txn1 (the user pool) atomically
// commits the application writes plus a transaction_completion row; txn2 (the
// system pool) checkpoints operation_outputs. Recovery checks operation_outputs
// first (layer 1) then transaction_completion (layer 2), replaying the stored
// output without re-running fn when the user transaction already committed.
//
// This is the concrete DBOSContext.RunAsTransaction; the generic RunAsTransaction
// wrapper type-erases fn and dispatches here (or to a mock implementation).
//
// When the data source shares the system database's pool, the call collapses onto runAsTxn.
func (c *dbosContext) RunAsTransaction(dbosCtx DBOSContext, ds *DataSource, fn TxnFunc, opts ...StepOption) (any, error) {
	// Reject a transaction nested inside another transaction.
	// (A transaction nested inside a plain RunAsStep is fine)
	if ws, ok := c.Value(workflowStateKey).(*workflowState); ok && ws != nil && ws.isWithinTransaction {
		stepOpts := &stepOptions{}
		for _, opt := range opts {
			opt(stepOpts)
		}
		return nil, newStepExecutionError(ws.workflowID, stepOpts.stepName, fmt.Errorf("cannot call RunAsTransaction within a transaction"))
	}

	if ds.sameAsSystemDB {
		// runAsTxn manages a transaction for the user function.
		// reuse our internal path used for all DBOS "special" steps (e.g., setEvent)
		return c.runAsTxn(dbosCtx, fn, opts...)
	}

	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("transaction function cannot be nil"))
	}

	if prep.IsWithinStep {
		// Invoked inside an enclosing step: open a real transaction on the user
		// pool and manage its commit/rollback, but record no durability row.
		txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}
		if prep.StepOpts.txIsoLevel != nil {
			txOpts.IsoLevel = *prep.StepOpts.txIsoLevel
		}
		uncancellableCtx := WithoutCancel(c)
		tx, err := ds.pool.BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)
		output, err := fn(withinTransactionContext(c), tx)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", err))
		}
		return output, nil
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepState.isWithinTransaction = true
	stepOpts := prep.StepOpts
	stepCtx := WithValue(c, workflowStateKey, stepState)
	ser := resolveEncoder(c)

	// checkpoint writes the step outcome into the system database (txn2). The
	// user transaction has already committed durably, so this is retried hard.
	checkpoint := func(encodedOutput, serializedErr *string, serialization string, startedAt time.Time) error {
		dbInput := recordOperationResultDBInput{
			workflowID:    stepState.workflowID,
			stepName:      stepOpts.stepName,
			stepID:        stepState.stepID,
			output:        encodedOutput,
			errStr:        serializedErr,
			startedAt:     startedAt,
			completedAt:   time.Now(),
			serialization: serialization,
		}
		return retry(c, func() error {
			return c.systemDB.recordOperationResult(uncancellableCtx, dbInput)
		}, withRetrierLogger(c.logger))
	}

	// Layer 1: already checkpointed in the system database? Replay it.
	recordedOutput, err := retryWithResult(c, func() (*recordedResult, error) {
		return c.systemDB.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowID: stepState.workflowID,
			stepID:     stepState.stepID,
			stepName:   stepOpts.stepName,
		})
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
	}
	if recordedOutput != nil {
		return stepCheckpointedOutcome{value: recordedOutput.output, serialization: recordedOutput.serialization},
			deserializeWorkflowError(recordedOutput.errStr, recordedOutput.serialization)
	}

	// Layer 2: did the user transaction commit on a previous run (crash window
	// between txn1 and txn2)? Replay the stored output and apply txn2 without
	// re-running fn.
	completion, err := retryWithResult(c, func() (*completionRecord, error) {
		return ds.checkCompletion(uncancellableCtx, ds.pool, stepState.workflowID, stepState.stepID)
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking transaction completion: %w", err))
	}
	if completion != nil {
		// Replay with the codec the row was written with, not the current one.
		replaySer := completion.serialization
		if replaySer == "" {
			replaySer = ser.Name()
		}
		if cerr := checkpoint(completion.output, completion.errStr, replaySer, time.Now()); cerr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, cerr)
		}
		return stepCheckpointedOutcome{value: completion.output, serialization: replaySer},
			deserializeWorkflowError(completion.errStr, replaySer)
	}

	// Fresh execution.
	txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}
	if stepOpts.txIsoLevel != nil {
		txOpts.IsoLevel = *stepOpts.txIsoLevel
	}
	stepStartTime := time.Now()

	// runTxnOnce is one fresh-transaction attempt against the user database: run
	// fn, record the completion row, and commit — all atomic. A fresh tx is
	// begun on every (retried) call so a closed/aborted tx never leaks.
	runTxnOnce := func() (any, error) {
		tx, err := ds.pool.BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)

		output, txErr := fn(stepCtx, tx)
		if txErr != nil {
			return nil, txErr
		}

		encoded, serErr := ser.Encode(output)
		if serErr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize transaction output: %w", serErr))
		}
		if recErr := ds.recordCompletion(uncancellableCtx, tx, stepState.workflowID, stepState.stepID, encoded, nil, ser.Name()); recErr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("recording transaction completion: %w", recErr))
		}
		if cmErr := tx.Commit(uncancellableCtx); cmErr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", cmErr))
		}
		return output, nil
	}

	// INNER: absorb infrastructure errors (dropped connections, closed tx) and
	// transaction conflicts (serialization/deadlock), retrying with a fresh tx
	// indefinitely. Application errors are non-retryable here and pass straight
	// through to the user retry policy below — so connection retries never burn
	// the user's maxRetries budget (no compounding).
	runTxnResilient := func() (any, error) {
		return retryWithResult(c, runTxnOnce, withRetrierLogger(c.logger), withRetryCondition(ds.dialect.IsRetryableTransaction))
	}

	// OUTER: the user-facing step retry policy (maxRetries + predicate).
	stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, runTxnResilient)

	// txn2: checkpoint the outcome into the system database.
	encodedStepOutput, serErr := ser.Encode(stepOutput)
	if serErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize transaction output: %w", serErr))
	}
	var serializedErr *string
	if stepError != nil {
		s := serializeWorkflowError(stepError, ser.Name())
		serializedErr = &s
	}
	if cerr := checkpoint(encodedStepOutput, serializedErr, ser.Name(), stepStartTime); cerr != nil {
		if stepError != nil {
			cerr = errors.Join(cerr, stepError)
		}
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, cerr)
	}

	return stepOutput, stepError
}

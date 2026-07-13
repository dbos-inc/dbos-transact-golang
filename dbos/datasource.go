package dbos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

const defaultDataSourceName = "datasource"

// DataSource is a handle to a user-provided database engine. It is returned by
// NewDataSource and passed to RunAsTransaction.
type DataSource struct {
	name    string
	pool    Pool
	dialect Dialect
	schema  string

	// sameAsSystemDB is set when this data source's pool is the very same engine
	// handle as the DBOS system database.
	sameAsSystemDB bool
}

// Name returns the data source's name (WithDataSourceName, default "datasource").
func (ds *DataSource) Name() string { return ds.name }

type dataSourceOptions struct {
	name   string
	schema string
}

// DataSourceOption configures a data source.
type DataSourceOption func(*dataSourceOptions)

// WithDataSourceName sets the data source's name (default "datasource"). It is
// used only in logs and error messages; names need not be unique.
func WithDataSourceName(name string) DataSourceOption {
	return func(o *dataSourceOptions) { o.name = name }
}

// WithDataSourceSchema overrides the schema that holds the transaction_completion
// table (default "dbos"). Ignored by SQLite, which has no schemas.
func WithDataSourceSchema(schema string) DataSourceOption {
	return func(o *dataSourceOptions) { o.schema = schema }
}

// Engine is the set of database engine types that can back a DataSource:
// *pgxpool.Pool (Postgres/CockroachDB) or *sql.DB (SQLite). It is a generic
// constraint, not an ordinary type, so NewDataSource rejects any other
// engine type at compile time rather than at runtime.
type Engine interface {
	*pgxpool.Pool | *sql.DB
}

// NewDataSource builds a durable data source over a user-provided database engine
// so its transactions can be made durable via RunAsTransaction. The engine type
// is constrained to *pgxpool.Pool (Postgres/CockroachDB) or *sql.DB (SQLite).
//
// The returned handle is ready to use immediately: NewDataSource detects whether
// the engine is the DBOS system database, resolves the dialect (CockroachDB), and
// creates the transaction_completion table if it does not already exist. It may
// be called at any time, before or after Launch.
//
// Example:
//
//	pool, _ := pgxpool.New(ctx, appDatabaseURL)
//	ds, err := dbos.NewDataSource(ctx, pool, dbos.WithDataSourceName("app"))
func NewDataSource[E Engine](ctx DBOSContext, engine E, opts ...DataSourceOption) (*DataSource, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	c, ok := ctx.(*dbosContext)
	if !ok {
		return nil, errors.New("NewDataSource requires a concrete DBOS context")
	}

	options := dataSourceOptions{name: defaultDataSourceName, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
	for _, opt := range opts {
		opt(&options)
	}
	if options.name == "" {
		options.name = defaultDataSourceName
	}

	var (
		pool    Pool
		dialect Dialect
	)
	switch e := any(engine).(type) {
	case *pgxpool.Pool:
		if e == nil {
			return nil, errors.New("data source engine (*pgxpool.Pool) is nil")
		}
		pool = newPgxPool(e)
		dialect = postgresDialect{} // resolve CRDB below
	case *sql.DB:
		if e == nil {
			return nil, errors.New("data source engine (*sql.DB) is nil")
		}
		pool = newSQLPool(e)
		dialect = sqliteDialect{}
	}

	ds := &DataSource{
		name:    options.name,
		pool:    pool,
		dialect: dialect,
		schema:  options.schema,
	}

	// A data source whose pool is the very same engine as the system database
	// needs no durability table: RunAsTransaction collapses onto the single
	// system transaction (runAsTxn), so skip dialect resolution and table
	// creation entirely.
	if sameEngine(ds.pool, c.systemDB.(*sysDB).pool) {
		ds.sameAsSystemDB = true
		c.logger.Debug("Data source shares the system database; using single-transaction durability", "datasource", ds.name)
		return ds, nil
	}

	if err := ds.resolveDialect(c); err != nil {
		return nil, fmt.Errorf("data source %q: %w", ds.name, err)
	}
	if err := ds.ensureCompletionTable(c); err != nil {
		return nil, fmt.Errorf("data source %q: %w", ds.name, err)
	}

	c.logger.Debug("Created data source", "datasource", ds.name, "dialect", ds.dialect.Name(), "schema", ds.schema)
	return ds, nil
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
func (ds *DataSource) resolveDialect(c *dbosContext) error {
	pgxPool := PgxPool(ds.pool)
	if pgxPool == nil {
		return nil
	}
	crdb, err := retryWithResult(c, func() (bool, error) {
		conn, err := pgxPool.Acquire(c)
		if err != nil {
			return false, err
		}
		defer conn.Release()
		return isCockroachDB(conn.Conn()), nil
	}, withRetrierLogger(c.logger))
	if err != nil {
		return err
	}
	if crdb {
		ds.dialect = cockroachDialect{}
		c.logger.Debug("Detected CockroachDB data source", "datasource", ds.name)
	}
	return nil
}

// completionTableInstalled reports whether the transaction_completion table
// already exists in the user's database. When it does, ensureCompletionTable
// skips all DDL — so a least-privilege role with only DML rights works against a
// table that was pre-created (e.g. in the application's own migrations).
func (ds *DataSource) completionTableInstalled(c *dbosContext) (bool, error) {
	return retryWithResult(c, func() (bool, error) {
		if ds.dialect.Name() == DialectSQLite {
			var name string
			err := ds.pool.QueryRow(c,
				`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`,
				transactionCompletionTable).Scan(&name)
			if errors.Is(err, ErrNoRows) {
				return false, nil
			}
			return err == nil, err
		}
		var exists bool
		err := ds.pool.QueryRow(c,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
			ds.schema, transactionCompletionTable).Scan(&exists)
		return exists, err
	}, withRetrierLogger(c.logger))
}

// ensureCompletionTable creates the transaction_completion table in the user's
// database when it is not already present. It pre-checks for the table first, so
// a role with only DML privileges works against a pre-provisioned table; only a
// missing table triggers the CREATE SCHEMA / CREATE TABLE DDL.
// A failed create returns an actionable error pointing the user at their own database migrations.
func (ds *DataSource) ensureCompletionTable(c *dbosContext) error {
	installed, err := ds.completionTableInstalled(c)
	if err != nil {
		return fmt.Errorf("checking for the %s table: %w", ds.qualifiedCompletionTable(), err)
	}
	if installed {
		c.logger.Debug("transaction_completion table already present; skipping creation", "datasource", ds.name)
		return nil
	}
	for _, stmt := range ds.completionTableStatements() {
		query := stmt
		if err := retry(c, func() error {
			_, execErr := ds.pool.Exec(c, query)
			return execErr
		}, withRetrierLogger(c.logger)); err != nil {
			return fmt.Errorf("the %s table does not exist and could not be created: %w; "+
				"create it ahead of time in your application's database migrations "+
				"or ensure the connecting role has CREATE privileges on the database and schema",
				ds.qualifiedCompletionTable(), err)
		}
	}
	return nil
}

// completionRecord is a row from the transaction_completion table. A success row
// stores output (error nil); a permanently failed transaction stores a serialized
// error (output nil) written in a standalone insert after fn's transaction rolls
// back. Failures are also checkpointed (best effort) in the system database's operation_outputs.
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
// (WithStepName, WithStepMaxRetries, retry predicate).
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
func RunAsTransaction[R any](ctx DBOSContext, ds *DataSource, fn Txn[R], opts ...StepOption) (R, error) {
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

	stepStartTime := time.Now()

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
			deserializeWorkflowError(recordedOutput.errStr)
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
		if cerr := checkpoint(completion.output, completion.errStr, replaySer, stepStartTime); cerr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, cerr)
		}
		return stepCheckpointedOutcome{value: completion.output, serialization: replaySer},
			deserializeWorkflowError(completion.errStr)
	}

	// Fresh execution.
	txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}
	if stepOpts.txIsoLevel != nil {
		txOpts.IsoLevel = *stepOpts.txIsoLevel
	}
	stepStartTime = time.Now()

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

	if stepInterruptedByCancellation(stepState, stepError) {
		return stepOutput, newWorkflowCancelledError(stepState.workflowID, stepError)
	}

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

	// Mirror the failure into the user database so the user's own database is self-describing.
	// fn's transaction rolled back, so this is a standalone insert on the pool,
	// written before the system-DB checkpoint to keep the layer-1-then-layer-2 recovery order.
	// Best-effort.
	if serializedErr != nil {
		if recErr := retry(c, func() error {
			return ds.recordCompletion(uncancellableCtx, ds.pool, stepState.workflowID, stepState.stepID, nil, serializedErr, ser.Name())
		}, withRetrierLogger(c.logger)); recErr != nil {
			c.logger.Warn("Failed to record transaction failure in the user database; the system database remains the source of truth",
				"datasource", ds.name, "workflow_id", stepState.workflowID, "step_id", stepState.stepID, "error", recErr)
		}
	}

	if cerr := checkpoint(encodedStepOutput, serializedErr, ser.Name(), stepStartTime); cerr != nil {
		if stepError != nil {
			cerr = errors.Join(cerr, stepError)
		}
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, cerr)
	}

	return stepOutput, stepError
}

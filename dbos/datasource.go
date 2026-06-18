package dbos

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

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

// launchDataSources creates the durability table for every registered data source.
func (c *dbosContext) launchDataSources(ctx context.Context) error {
	c.dataSourcesMu.Lock()
	defer c.dataSourcesMu.Unlock()
	for _, ds := range c.dataSources {
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

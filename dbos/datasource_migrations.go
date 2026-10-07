package dbos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Records the data source schema version. Separate from the system database's
// dbos_migrations, since a data source may share its schema.
const dataSourceMigrationsTable = "dbos_transaction_completion_migrations"

// Longest a migrating transaction may sit idle while holding the migration lock.
var dataSourceMigrationIdleTimeout = "30s"

// dataSourceMigrations returns the data source schema migrations, in order;
// migration N moves the schema to version N.
func dataSourceMigrations(dialect Dialect, schema string) []string {
	table := dialect.SchemaPrefix(schema) + transactionCompletionTable
	// Migration 1 is IF NOT EXISTS so tables created before versioning adopt it cleanly.
	if dialect.Name() == DialectSQLite {
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	workflow_id TEXT NOT NULL,
	step_id INTEGER NOT NULL,
	output TEXT,
	error TEXT,
	serialization TEXT,
	created_at INTEGER NOT NULL DEFAULT (CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)),
	PRIMARY KEY (workflow_id, step_id)
)`, table)}
	}
	return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	workflow_id TEXT NOT NULL,
	step_id INT4 NOT NULL,
	output TEXT,
	error TEXT,
	serialization TEXT,
	created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
	PRIMARY KEY (workflow_id, step_id)
)`, table)}
}

func dataSourceMigrationLockKey(schema string) int64 {
	return sysdb.AdvisoryLockKey("dbos.datasource_migrations." + schema)
}

func dataSourceTableExists(ctx context.Context, q Querier, dialect Dialect, schema, table string) (bool, error) {
	var one int
	var err error
	if dialect.Name() == DialectSQLite {
		err = q.QueryRow(ctx, `SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&one)
	} else {
		// pg_catalog, not information_schema, which hides tables the role holds no grant on.
		err = q.QueryRow(ctx, `SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = $2`, schema, table).Scan(&one)
	}
	if errors.Is(err, ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// readDataSourceVersion returns the recorded data source schema version; a
// missing version table reads as 0.
func readDataSourceVersion(ctx context.Context, q Querier, dialect Dialect, schema string) (int64, error) {
	exists, err := dataSourceTableExists(ctx, q, dialect, schema, dataSourceMigrationsTable)
	if err != nil || !exists {
		return 0, err
	}
	var version int64
	err = q.QueryRow(ctx, fmt.Sprintf(`SELECT version FROM %s`, dialect.SchemaPrefix(schema)+dataSourceMigrationsTable)).Scan(&version)
	if errors.Is(err, ErrNoRows) {
		return 0, nil
	}
	return version, err
}

// migrateDataSource brings the data source schema to the latest version in one
// transaction. It issues no DDL when the schema is already at or ahead of it.
// Connection errors and transaction conflicts (a SQLite migrator whose deferred
// transaction loses the write lock to a peer) are retried with a fresh transaction.
func migrateDataSource(ctx context.Context, pool Pool, dialect Dialect, schema string, logger *slog.Logger) error {
	return sysdb.Retry(ctx, func() error {
		tx, err := pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(ctx)
		if err := migrateDataSourceTx(ctx, tx, dialect, schema); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}, sysdb.WithRetrierLogger(logger), sysdb.WithRetryCondition(dialect.IsRetryableTransaction, isDataSourceMigrationRace))
}

// Matches the errors an unserialized migrator (CockroachDB has no advisory locks)
// gets when a peer creates the same schema objects first.
func isDataSourceMigrationRace(err error, _ *slog.Logger) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case pgerrcode.DuplicateSchema, pgerrcode.DuplicateTable, pgerrcode.UniqueViolation:
		return true
	}
	return false
}

// migrateDataSourceTx runs the migration inside tx, whose commit releases the lock.
func migrateDataSourceTx(ctx context.Context, tx Querier, dialect Dialect, schema string) error {
	migrations := dataSourceMigrations(dialect, schema)
	latest := int64(len(migrations))

	current, err := readDataSourceVersion(ctx, tx, dialect, schema)
	if err != nil {
		return fmt.Errorf("failed to read the data source schema version: %w", err)
	}
	if current >= latest {
		return nil
	}

	versionTable := dialect.SchemaPrefix(schema) + dataSourceMigrationsTable
	if dialect.Name() == DialectSQLite {
		// SQLite gets a single db-wide write lock when tx is started.
		if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (version INTEGER NOT NULL PRIMARY KEY)`, versionTable)); err != nil {
			return fmt.Errorf("failed to create the %s table: %w", versionTable, err)
		}
	} else {
		// CockroachDB has no advisory locks, so it migrates unserialized.
		if dialect.Name() != DialectCockroach {
			// Set a timeout so we don't create a deadlock by holding the advisory lock indefinitely.
			if _, err := tx.Exec(ctx, fmt.Sprintf(`SET LOCAL idle_in_transaction_session_timeout = '%s'`, dataSourceMigrationIdleTimeout)); err != nil {
				return fmt.Errorf("failed to set the migration idle timeout: %w", err)
			}
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, dataSourceMigrationLockKey(schema)); err != nil {
				return fmt.Errorf("failed to take the migration lock: %w", err)
			}
		}
		// Check if the schema exist (don't do CREATE IF NOT EXISTS because that requires the CREATE privilege).
		var one int
		err := tx.QueryRow(ctx, `SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1`, schema).Scan(&one)
		if errors.Is(err, ErrNoRows) {
			if _, err := tx.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
				return fmt.Errorf("failed to create schema %s: %w", schema, err)
			}
		} else if err != nil {
			return fmt.Errorf("failed to check for schema %s: %w", schema, err)
		}
		exists, err := dataSourceTableExists(ctx, tx, dialect, schema, dataSourceMigrationsTable)
		if err != nil {
			return fmt.Errorf("failed to check for the %s table: %w", versionTable, err)
		}
		if !exists {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`CREATE TABLE %s (version BIGINT NOT NULL PRIMARY KEY)`, versionTable)); err != nil {
				return fmt.Errorf("failed to create the %s table: %w", versionTable, err)
			}
		}
	}
	// A concurrent migratorh might have executed in between us checking the current version and obtaining the lock.
	current, err = readDataSourceVersion(ctx, tx, dialect, schema)
	if err != nil {
		return fmt.Errorf("failed to read the data source schema version: %w", err)
	}
	if current >= latest {
		return nil
	}

	for i := current; i < latest; i++ {
		if _, err := tx.Exec(ctx, migrations[i]); err != nil {
			return fmt.Errorf("failed to apply data source migration %d: %w", i+1, err)
		}
	}
	record := fmt.Sprintf(`UPDATE %s SET version = $1`, versionTable)
	if current == 0 {
		record = fmt.Sprintf(`INSERT INTO %s (version) VALUES ($1)`, versionTable)
	}
	if _, err := tx.Exec(ctx, dialect.RewriteQuery(record), latest); err != nil {
		return fmt.Errorf("failed to record data source schema version %d: %w", latest, err)
	}
	return nil
}

// verifyDataSource errors unless the data source schema is migrated, creating
// and changing nothing. A schema ahead of this build belongs to a newer peer,
// which migration also tolerates.
func verifyDataSource(ctx context.Context, pool Pool, dialect Dialect, schema, name string) error {
	current, err := sysdb.RetryWithResult(ctx, func() (int64, error) {
		return readDataSourceVersion(ctx, pool, dialect, schema)
	})
	if err != nil {
		return fmt.Errorf("failed to read the data source schema version: %w", err)
	}
	latest := int64(len(dataSourceMigrations(dialect, schema)))
	if current < latest {
		return models.NewInitializationError(fmt.Sprintf(
			"data source %q schema %q is at transaction schema version %d, but this version of DBOS requires %d. "+
				"This data source is configured with SkipMigrations, so it will not migrate it: either migrate it "+
				"out of band (dbos.MigrateDataSource) or create it with migrations enabled",
			name, schema, current, latest))
	}
	return nil
}

// DataSourcePermissionStatements returns the SQL granting roleName the minimal
// permissions a data source needs at runtime on schemaName: read the schema
// version, and read, write and delete checkpoints. MigrateDataSource executes
// it for WithMigrateApplicationRole.
func DataSourcePermissionStatements(schemaName, roleName string) []string {
	if schemaName == "" {
		schemaName = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	schemaSQL := pgx.Identifier{schemaName}.Sanitize()
	roleSQL := pgx.Identifier{roleName}.Sanitize()
	return []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT SELECT, INSERT, DELETE ON %s.%s TO %s`, schemaSQL, transactionCompletionTable, roleSQL),
		fmt.Sprintf(`GRANT SELECT ON %s.%s TO %s`, schemaSQL, dataSourceMigrationsTable, roleSQL),
	}
}

// MigrateDataSource creates or migrates the DBOS schema of a data source's
// database, typically with a privileged role, without a DBOS context. Pair it
// with WithDataSourceSkipMigrations for application processes whose role cannot
// run DDL. It accepts the same engine types as NewDataSource and the schema,
// application role and logger options of Migrate.
//
// Example:
//
//	err := dbos.MigrateDataSource(ctx, adminPool, dbos.WithMigrateApplicationRole("app_user"))
func MigrateDataSource[E Engine](ctx context.Context, engine E, opts ...MigrateOption) error {
	options := migrateOptions{schema: _DEFAULT_SYSTEM_DB_SCHEMA, logger: slog.Default()}
	for _, opt := range opts {
		opt(&options)
	}
	if options.schema == "" {
		options.schema = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	if options.logger == nil {
		options.logger = slog.Default()
	}

	var (
		pool    Pool
		dialect Dialect
	)
	switch e := any(engine).(type) {
	case *pgxpool.Pool:
		if e == nil {
			return errors.New("data source engine (*pgxpool.Pool) is nil")
		}
		pool = sysdb.NewPgxPool(e)
		dialect = sysdb.PostgresDialect{}
	case *sql.DB:
		if e == nil {
			return errors.New("data source engine (*sql.DB) is nil")
		}
		if options.applicationRole != "" {
			return errors.New("an application role is not supported for SQLite")
		}
		pool = sysdb.NewSQLPool(e)
		dialect = sysdb.SqliteDialect{}
	}
	dialect, err := resolvePoolDialect(ctx, pool, dialect, options.logger)
	if err != nil {
		return err
	}
	if err := migrateDataSource(ctx, pool, dialect, options.schema, options.logger); err != nil {
		return err
	}
	if options.applicationRole == "" {
		return nil
	}
	options.logger.Info("Granting data source permissions", "schema", options.schema, "role", options.applicationRole)
	for _, stmt := range DataSourcePermissionStatements(options.schema, options.applicationRole) {
		if err := sysdb.Retry(ctx, func() error {
			_, err := pool.Exec(ctx, stmt)
			return err
		}, sysdb.WithRetrierLogger(options.logger)); err != nil {
			return fmt.Errorf("failed to grant permissions to role %s: %w", options.applicationRole, err)
		}
	}
	return nil
}

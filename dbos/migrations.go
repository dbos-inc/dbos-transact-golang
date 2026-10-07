package dbos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

type migrateOptions struct {
	schema          string
	applicationRole string
	logger          *slog.Logger
}

// MigrateOption configures Migrate.
type MigrateOption func(*migrateOptions)

// WithMigrateSchema sets the schema holding the DBOS system tables (default "dbos").
func WithMigrateSchema(schema string) MigrateOption {
	return func(o *migrateOptions) { o.schema = schema }
}

// WithMigrateApplicationRole grants role access to the migrated schema, so an
// application whose role cannot run DDL can launch with Config.SkipMigrations
// (or create its data source with WithDataSourceSkipMigrations). Postgres only.
func WithMigrateApplicationRole(role string) MigrateOption {
	return func(o *migrateOptions) { o.applicationRole = role }
}

// WithMigrateLogger sets the logger Migrate reports progress to (default slog.Default()).
func WithMigrateLogger(logger *slog.Logger) MigrateOption {
	return func(o *migrateOptions) { o.logger = logger }
}

// Migrate creates or migrates the DBOS system database at databaseURL without
// launching DBOS, typically with a privileged role. Pair it with
// Config.SkipMigrations for application processes whose role cannot run DDL.
//
// Example:
//
//	err := dbos.Migrate(ctx, adminURL, dbos.WithMigrateApplicationRole("app_user"))
func Migrate(ctx context.Context, databaseURL string, opts ...MigrateOption) error {
	if databaseURL == "" {
		return errors.New("database URL cannot be empty")
	}
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
	dialect, err := sysdb.DetectDialect(databaseURL)
	if err != nil {
		return err
	}
	if options.applicationRole != "" && dialect == DialectSQLite {
		return errors.New("an application role is not supported for SQLite")
	}

	// A system database handle creates the database and runs the migrations;
	// it is never launched, so no listener or notifier starts.
	systemDB, err := sysdb.NewSystemDatabase(ctx, sysdb.NewSystemDatabaseInput{
		DatabaseURL:            databaseURL,
		DatabaseSchema:         options.schema,
		Logger:                 options.logger,
		IdleTransactionTimeout: sysdb.DefaultIdleTransactionTimeout,
	})
	if err != nil {
		return err
	}
	systemDB.Shutdown(ctx, 30*time.Second)

	if options.applicationRole == "" {
		return nil
	}
	return grantSystemSchemaPermissions(ctx, databaseURL, options.schema, options.applicationRole, options.logger)
}

// PermissionStatements returns the SQL granting roleName access to every
// current and future object in the system schema, as executed by Migrate for
// WithMigrateApplicationRole.
func PermissionStatements(schemaName, roleName string) []string {
	if schemaName == "" {
		schemaName = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	schemaSQL := pgx.Identifier{schemaName}.Sanitize()
	roleSQL := pgx.Identifier{roleName}.Sanitize()
	return []string{
		fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON TABLES TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL ON SEQUENCES TO %s`, schemaSQL, roleSQL),
		fmt.Sprintf(`ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s`, schemaSQL, roleSQL),
	}
}

func grantSystemSchemaPermissions(ctx context.Context, databaseURL, schemaName, roleName string, logger *slog.Logger) error {
	logger.Info("Granting permissions on the system schema", "schema", schemaName, "role", roleName)
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to the system database: %w", err)
	}
	defer conn.Close(ctx)
	for _, stmt := range PermissionStatements(schemaName, roleName) {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("failed to grant permissions to role %s: %w", roleName, err)
		}
	}
	return nil
}

// MigrationStatements returns the SQL the system database migrations execute
// against a PostgreSQL database for the given schema, as an ordered list of
// semicolon-terminated statements and "--" comment lines suitable for
// execution with psql. It never connects to a database.
//
// from is a migration version number: pass 1 for the full fresh-database
// script (including schema creation, the dbos_migrations table, and the
// initial version row), or a later number for migrations from through the
// latest only. Version numbers are not contiguous: versions between the end
// of the Go-specific history and the cross-SDK shared base (100) are unused
// and emit nothing. Each migration is followed by its version bookkeeping,
// mirroring the runner. The SQL contains CREATE/DROP INDEX CONCURRENTLY, so it
// must run outside a transaction block. An empty schemaName uses the default
// ("dbos").
func MigrationStatements(schemaName string, from int) ([]string, error) {
	if schemaName == "" {
		schemaName = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	migrations := sysdb.BuildMigrations(schemaName, false)
	latest := migrations[len(migrations)-1].Version
	if from < 1 || int64(from) > latest {
		// Printed verbatim by the CLI, so worded for the end user.
		return nil, fmt.Errorf("Migration %d does not exist: valid migrations are 1 through %d", from, latest)
	}
	sanitizedSchema := pgx.Identifier{schemaName}.Sanitize()

	var statements []string
	if from == 1 {
		statements = append(statements,
			fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", sanitizedSchema),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (version BIGINT NOT NULL PRIMARY KEY);", sanitizedSchema, sysdb.MigrationTable),
		)
	}

	versionRowExists := from > 1
	for _, migration := range migrations {
		if migration.Version < int64(from) {
			continue
		}
		if migration.Version == 10 {
			// Migration 10 backfills the notifications primary key, which
			// migration 1 already creates on a fresh database.
			statements = append(statements, "-- Migration 10 skipped: not applicable on fresh databases")
		} else if sql := strings.TrimSpace(migration.SQL); sql != "" {
			if !strings.HasSuffix(sql, ";") {
				sql += ";"
			}
			statements = append(statements, fmt.Sprintf("-- Migration %d", migration.Version), sql)
		}
		// Mirror the runner's per-migration version bookkeeping.
		if versionRowExists {
			statements = append(statements, fmt.Sprintf("UPDATE %s.%s SET version = %d;", sanitizedSchema, sysdb.MigrationTable, migration.Version))
		} else {
			statements = append(statements, fmt.Sprintf("INSERT INTO %s.%s (version) VALUES (%d);", sanitizedSchema, sysdb.MigrationTable, migration.Version))
			versionRowExists = true
		}
	}
	return statements, nil
}

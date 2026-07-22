package dbos

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// NumMigrations returns the number of DBOS system database migrations.
func NumMigrations() int {
	return len(sysdb.BuildMigrations(_DEFAULT_SYSTEM_DB_SCHEMA, false))
}

// MigrationStatements returns the SQL the system database migrations execute
// against a PostgreSQL database for the given schema, as an ordered list of
// semicolon-terminated statements and "--" comment lines suitable for
// execution with psql. It never connects to a database.
//
// from is a 1-based migration number: pass 1 for the full fresh-database
// script (including schema creation, the dbos_migrations table, and the
// initial version row), or a later number for migrations from through
// NumMigrations() only. Each migration is followed by its version
// bookkeeping, mirroring the runner. The SQL contains CREATE/DROP INDEX
// CONCURRENTLY, so it must run outside a transaction block. An empty
// schemaName uses the default ("dbos").
func MigrationStatements(schemaName string, from int) ([]string, error) {
	if schemaName == "" {
		schemaName = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	migrations := sysdb.BuildMigrations(schemaName, false)
	if from < 1 || from > len(migrations) {
		return nil, fmt.Errorf("migration %d does not exist: valid migrations are 1 through %d", from, len(migrations))
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
	for _, migration := range migrations[from-1:] {
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

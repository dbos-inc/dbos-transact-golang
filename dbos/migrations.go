package dbos

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
)

// MigrationStatements returns the ordered SQL statements a system database
// migration executes against a fresh PostgreSQL database for the given
// schema, including schema creation and migration-version bookkeeping.
// Each entry is a semicolon-terminated statement (or batch of statements)
// suitable for execution with psql. An empty schemaName uses the default
// ("dbos").
func MigrationStatements(schemaName string) []string {
	if schemaName == "" {
		schemaName = _DEFAULT_SYSTEM_DB_SCHEMA
	}
	sanitizedSchema := pgx.Identifier{schemaName}.Sanitize()

	statements := []string{
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", sanitizedSchema),
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (version BIGINT NOT NULL PRIMARY KEY);", sanitizedSchema, sysdb.MigrationTable),
	}

	for i, migration := range sysdb.BuildMigrations(schemaName, false) {
		if sql := strings.TrimSpace(migration.SQL); sql != "" {
			statements = append(statements, sql)
		}
		// Mirror the runner's per-migration version bookkeeping.
		if i == 0 {
			statements = append(statements, fmt.Sprintf("INSERT INTO %s.%s (version) VALUES (%d);", sanitizedSchema, sysdb.MigrationTable, migration.Version))
		} else {
			statements = append(statements, fmt.Sprintf("UPDATE %s.%s SET version = %d;", sanitizedSchema, sysdb.MigrationTable, migration.Version))
		}
	}
	return statements
}

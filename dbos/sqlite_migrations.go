package dbos

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// SQLite migration numbering mirrors pg numbering (matching Python's
// sqlite_migrations list). pg migrations 10, 14, 20, 38, and 39 have no SQLite
// counterpart, so those version numbers are skipped rather than renumbered.

//go:embed migrations/sqlite/1_initial_dbos_schema.sql
var sqliteMigration1SQL string

//go:embed migrations/sqlite/2_add_queue_partition_key.sql
var sqliteMigration2SQL string

//go:embed migrations/sqlite/3_add_workflow_status_index.sql
var sqliteMigration3SQL string

//go:embed migrations/sqlite/4_add_forked_from.sql
var sqliteMigration4SQL string

//go:embed migrations/sqlite/5_add_step_timestamps.sql
var sqliteMigration5SQL string

//go:embed migrations/sqlite/6_add_workflow_events_history.sql
var sqliteMigration6SQL string

//go:embed migrations/sqlite/7_add_owner_xid.sql
var sqliteMigration7SQL string

//go:embed migrations/sqlite/8_add_parent_workflow_id.sql
var sqliteMigration8SQL string

//go:embed migrations/sqlite/9_add_workflow_schedules.sql
var sqliteMigration9SQL string

//go:embed migrations/sqlite/11_add_serialization_columns.sql
var sqliteMigration11SQL string

//go:embed migrations/sqlite/12_add_notifications_consumed.sql
var sqliteMigration12SQL string

//go:embed migrations/sqlite/13_add_application_versions.sql
var sqliteMigration13SQL string

//go:embed migrations/sqlite/15_add_workflow_schedule_columns.sql
var sqliteMigration15SQL string

//go:embed migrations/sqlite/16_add_delay_until.sql
var sqliteMigration16SQL string

//go:embed migrations/sqlite/17_add_workflow_schedule_queue_name.sql
var sqliteMigration17SQL string

//go:embed migrations/sqlite/18_add_was_forked_from.sql
var sqliteMigration18SQL string

//go:embed migrations/sqlite/19_add_operation_outputs_completed_at_index.sql
var sqliteMigration19SQL string

//go:embed migrations/sqlite/21_create_queues_table.sql
var sqliteMigration21SQL string

//go:embed migrations/sqlite/22_drop_forked_from_index.sql
var sqliteMigration22SQL string

//go:embed migrations/sqlite/23_create_partial_forked_from_index.sql
var sqliteMigration23SQL string

//go:embed migrations/sqlite/24_drop_parent_workflow_id_index.sql
var sqliteMigration24SQL string

//go:embed migrations/sqlite/25_create_partial_parent_workflow_id_index.sql
var sqliteMigration25SQL string

//go:embed migrations/sqlite/26_drop_executor_id_index.sql
var sqliteMigration26SQL string

//go:embed migrations/sqlite/27_create_partial_dedup_id_index.sql
var sqliteMigration27SQL string

//go:embed migrations/sqlite/28_drop_dedup_id_constraint.sql
var sqliteMigration28SQL string

//go:embed migrations/sqlite/29_create_pending_index.sql
var sqliteMigration29SQL string

//go:embed migrations/sqlite/30_create_failed_index.sql
var sqliteMigration30SQL string

//go:embed migrations/sqlite/31_drop_status_index.sql
var sqliteMigration31SQL string

//go:embed migrations/sqlite/32_create_in_flight_index.sql
var sqliteMigration32SQL string

//go:embed migrations/sqlite/33_add_rate_limited.sql
var sqliteMigration33SQL string

//go:embed migrations/sqlite/34_create_rate_limited_index.sql
var sqliteMigration34SQL string

//go:embed migrations/sqlite/35_drop_queue_status_started_index.sql
var sqliteMigration35SQL string

//go:embed migrations/sqlite/36_add_completed_at.sql
var sqliteMigration36SQL string

//go:embed migrations/sqlite/37_create_started_at_index.sql
var sqliteMigration37SQL string

// pg migrations 38 and 39 (plpgsql functions/triggers) have no SQLite
// counterpart and are omitted.

//go:embed migrations/sqlite/40_add_attributes.sql
var sqliteMigration40SQL string

//go:embed migrations/sqlite/41_add_schedule_name.sql
var sqliteMigration41SQL string

// buildSqliteMigrations returns the SQLite migration list. Versions mirror pg
// numbering (matching Python's sqlite_migrations); pg migrations 10, 14, 20,
// 38, and 39 have no SQLite counterpart and are omitted.
func buildSqliteMigrations() []migrationFile {
	return []migrationFile{
		{version: 1, sql: sqliteMigration1SQL},
		{version: 2, sql: sqliteMigration2SQL},
		{version: 3, sql: sqliteMigration3SQL},
		{version: 4, sql: sqliteMigration4SQL},
		{version: 5, sql: sqliteMigration5SQL},
		{version: 6, sql: sqliteMigration6SQL},
		{version: 7, sql: sqliteMigration7SQL},
		{version: 8, sql: sqliteMigration8SQL},
		{version: 9, sql: sqliteMigration9SQL},
		{version: 11, sql: sqliteMigration11SQL},
		{version: 12, sql: sqliteMigration12SQL},
		{version: 13, sql: sqliteMigration13SQL},
		{version: 15, sql: sqliteMigration15SQL},
		{version: 16, sql: sqliteMigration16SQL},
		{version: 17, sql: sqliteMigration17SQL},
		{version: 18, sql: sqliteMigration18SQL},
		{version: 19, sql: sqliteMigration19SQL},
		{version: 21, sql: sqliteMigration21SQL},
		{version: 22, sql: sqliteMigration22SQL},
		{version: 23, sql: sqliteMigration23SQL},
		{version: 24, sql: sqliteMigration24SQL},
		{version: 25, sql: sqliteMigration25SQL},
		{version: 26, sql: sqliteMigration26SQL},
		{version: 27, sql: sqliteMigration27SQL},
		{version: 28, sql: sqliteMigration28SQL},
		{version: 29, sql: sqliteMigration29SQL},
		{version: 30, sql: sqliteMigration30SQL},
		{version: 31, sql: sqliteMigration31SQL},
		{version: 32, sql: sqliteMigration32SQL},
		{version: 33, sql: sqliteMigration33SQL},
		{version: 34, sql: sqliteMigration34SQL},
		{version: 35, sql: sqliteMigration35SQL},
		{version: 36, sql: sqliteMigration36SQL},
		{version: 37, sql: sqliteMigration37SQL},
		{version: 40, sql: sqliteMigration40SQL},
		{version: 41, sql: sqliteMigration41SQL},
	}
}

func runSqliteMigrations(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	migrations := buildSqliteMigrations()

	// Ensure the dbos_migrations table exists.
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`,
		_DBOS_MIGRATION_TABLE).Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to probe sqlite_master: %v", err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`CREATE TABLE %s (version INTEGER NOT NULL PRIMARY KEY)`, _DBOS_MIGRATION_TABLE)); err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}

	// Read current version (single-row table).
	var currentVersion int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT version FROM %s LIMIT 1`, _DBOS_MIGRATION_TABLE)).Scan(&currentVersion); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read current migration version: %v", err)
	}

	for _, m := range migrations {
		if m.version <= currentVersion {
			continue
		}
		if err := applySqliteMigration(ctx, db, m, currentVersion, logger); err != nil {
			return err
		}
		currentVersion = m.version
	}
	return nil
}

func applySqliteMigration(
	ctx context.Context,
	db *sql.DB,
	m migrationFile,
	lastApplied int64,
	logger *slog.Logger,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for migration %d: %v", m.version, err)
	}
	defer func() { _ = tx.Rollback() }()

	body := strings.TrimSpace(m.sql)
	for _, stmt := range splitSqliteStatements(body) {
		if logger != nil {
			logger.Debug("applying sqlite migration statement", "version", m.version, "stmt_prefix", firstNonEmptyLine(stmt))
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute migration %d: %v", m.version, err)
		}
	}

	if lastApplied == 0 {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (version) VALUES (?)`, _DBOS_MIGRATION_TABLE), m.version); err != nil {
			return fmt.Errorf("failed to insert migration version %d: %v", m.version, err)
		}
	} else {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %s SET version = ?`, _DBOS_MIGRATION_TABLE), m.version); err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", m.version, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration %d: %v", m.version, err)
	}
	return nil
}

func splitSqliteStatements(body string) []string {
	var clean strings.Builder
	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		clean.WriteString(line)
		clean.WriteByte('\n')
	}
	parts := strings.Split(clean.String(), ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func firstNonEmptyLine(s string) string {
	for line := range strings.SplitSeq(s, "\n") {
		t := strings.TrimSpace(line)
		if t != "" {
			if len(t) > 80 {
				return t[:80] + "..."
			}
			return t
		}
	}
	return ""
}

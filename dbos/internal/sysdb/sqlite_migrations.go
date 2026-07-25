package sysdb

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

//go:embed migrations/sqlite/42_add_debounce_columns.sql
var sqliteMigration42SQL string

// BuildSqliteMigrations returns the SQLite migration list. Versions mirror pg
// numbering (matching Python's sqlite_migrations); pg migrations 10, 14, 20,
// 38, and 39 have no SQLite counterpart and are omitted.
func BuildSqliteMigrations() []MigrationFile {
	return []MigrationFile{
		{Version: 1, SQL: sqliteMigration1SQL},
		{Version: 2, SQL: sqliteMigration2SQL},
		{Version: 3, SQL: sqliteMigration3SQL},
		{Version: 4, SQL: sqliteMigration4SQL},
		{Version: 5, SQL: sqliteMigration5SQL},
		{Version: 6, SQL: sqliteMigration6SQL},
		{Version: 7, SQL: sqliteMigration7SQL},
		{Version: 8, SQL: sqliteMigration8SQL},
		{Version: 9, SQL: sqliteMigration9SQL},
		{Version: 11, SQL: sqliteMigration11SQL},
		{Version: 12, SQL: sqliteMigration12SQL},
		{Version: 13, SQL: sqliteMigration13SQL},
		{Version: 15, SQL: sqliteMigration15SQL},
		{Version: 16, SQL: sqliteMigration16SQL},
		{Version: 17, SQL: sqliteMigration17SQL},
		{Version: 18, SQL: sqliteMigration18SQL},
		{Version: 19, SQL: sqliteMigration19SQL},
		{Version: 21, SQL: sqliteMigration21SQL},
		{Version: 22, SQL: sqliteMigration22SQL},
		{Version: 23, SQL: sqliteMigration23SQL},
		{Version: 24, SQL: sqliteMigration24SQL},
		{Version: 25, SQL: sqliteMigration25SQL},
		{Version: 26, SQL: sqliteMigration26SQL},
		{Version: 27, SQL: sqliteMigration27SQL},
		{Version: 28, SQL: sqliteMigration28SQL},
		{Version: 29, SQL: sqliteMigration29SQL},
		{Version: 30, SQL: sqliteMigration30SQL},
		{Version: 31, SQL: sqliteMigration31SQL},
		{Version: 32, SQL: sqliteMigration32SQL},
		{Version: 33, SQL: sqliteMigration33SQL},
		{Version: 34, SQL: sqliteMigration34SQL},
		{Version: 35, SQL: sqliteMigration35SQL},
		{Version: 36, SQL: sqliteMigration36SQL},
		{Version: 37, SQL: sqliteMigration37SQL},
		{Version: 40, SQL: sqliteMigration40SQL},
		{Version: 41, SQL: sqliteMigration41SQL},
		{Version: 42, SQL: sqliteMigration42SQL},
	}
}

func RunSqliteMigrations(ctx context.Context, db *sql.DB, logger *slog.Logger) error {
	migrations := BuildSqliteMigrations()

	// Ensure the dbos_migrations table exists.
	var exists int
	err := db.QueryRowContext(ctx,
		`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`,
		MigrationTable).Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to probe sqlite_master: %v", err)
	}
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := db.ExecContext(ctx,
			fmt.Sprintf(`CREATE TABLE %s (version INTEGER NOT NULL PRIMARY KEY)`, MigrationTable)); err != nil {
			return fmt.Errorf("failed to create migrations table: %v", err)
		}
	}

	// Read current version (single-row table).
	var currentVersion int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT version FROM %s LIMIT 1`, MigrationTable)).Scan(&currentVersion); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to read current migration version: %v", err)
	}

	for _, m := range migrations {
		if m.Version <= currentVersion {
			continue
		}
		if err := applySqliteMigration(ctx, db, m, currentVersion, logger); err != nil {
			return err
		}
		currentVersion = m.Version
	}
	return nil
}

func applySqliteMigration(
	ctx context.Context,
	db *sql.DB,
	m MigrationFile,
	lastApplied int64,
	logger *slog.Logger,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for migration %d: %v", m.Version, err)
	}
	defer func() { _ = tx.Rollback() }()

	body := strings.TrimSpace(m.SQL)
	for _, stmt := range splitSqliteStatements(body) {
		if logger != nil {
			logger.Debug("applying sqlite migration statement", "version", m.Version, "stmt_prefix", firstNonEmptyLine(stmt))
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to execute migration %d: %v", m.Version, err)
		}
	}

	if lastApplied == 0 {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO %s (version) VALUES (?)`, MigrationTable), m.Version); err != nil {
			return fmt.Errorf("failed to insert migration version %d: %v", m.Version, err)
		}
	} else {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf(`UPDATE %s SET version = ?`, MigrationTable), m.Version); err != nil {
			return fmt.Errorf("failed to update migration version to %d: %v", m.Version, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit migration %d: %v", m.Version, err)
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

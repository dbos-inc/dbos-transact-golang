package sysdb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// A migration that another process applied after the caller read the version is skipped.
func TestApplySqliteMigrationSkipsAppliedVersion(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "applied.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TABLE `+MigrationTable+` (version INTEGER NOT NULL PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	m := BuildSqliteMigrations()[0]
	for range 2 {
		if err := applySqliteMigration(ctx, db, m, nil); err != nil {
			t.Fatalf("migration %d: %v", m.Version, err)
		}
	}

	var version int64
	if err := db.QueryRowContext(ctx, `SELECT version FROM `+MigrationTable).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != m.Version {
		t.Fatalf("version = %d, want %d", version, m.Version)
	}
}

package sysdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "modernc.org/sqlite"
)

type planNode struct {
	NodeType          string     `json:"Node Type"`
	ActualRows        float64    `json:"Actual Rows"`
	ActualLoops       float64    `json:"Actual Loops"`
	RowsRemovedFilter float64    `json:"Rows Removed by Filter"`
	SharedHitBlocks   float64    `json:"Shared Hit Blocks"`
	SharedReadBlocks  float64    `json:"Shared Read Blocks"`
	Plans             []planNode `json:"Plans"`
}

func (n planNode) nodes() []planNode {
	out := []planNode{n}
	for _, child := range n.Plans {
		out = append(out, child.nodes()...)
	}
	return out
}

// TestLatestApplicationVersionUsesIndexes mirrors the TypeScript test: the
// newest third of the history belongs to a peer, so a timestamp scan filtered
// by owner would walk 10000 rows; the two-probe query must touch one row per
// probe through the 124/125 indexes.
func TestLatestApplicationVersionUsesIndexes(t *testing.T) {
	if os.Getenv("DBOS_TEST_BACKEND") == "sqlite" {
		t.Skip("Postgres plan test")
	}
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	const schema = "dbos_versions_index_test"
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema)); err != nil {
		t.Fatal(err)
	}
	defer pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema)) //nolint:errcheck
	if err := RunMigrations(ctx, pool, schema, false, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %q.application_versions (version_id, version_name, version_timestamp, created_at, application_name)
		SELECT i::text, 'version-' || i, i, i,
		       CASE WHEN i <= 10000 THEN 'ours' WHEN i <= 20000 THEN NULL ELSE 'peer' END
		FROM generate_series(1, 30000) AS i`, schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(`ANALYZE %q.application_versions`, schema)); err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct{ owner, want string }{
		{"ours", "version-20000"},
		{"absent", "version-20000"},
		{"peer", "version-30000"},
		{"", "version-30000"},
	} {
		s := &SysDB{pool: NewPgxPool(pool), dialect: PostgresDialect{}, schema: schema, appName: tt.owner}
		latest, err := s.GetLatestApplicationVersion(ctx, nil, "")
		if err != nil {
			t.Fatalf("owner %q: %v", tt.owner, err)
		}
		if latest.Name != tt.want {
			t.Fatalf("owner %q: got %s, want %s", tt.owner, latest.Name, tt.want)
		}

		query, args := s.latestApplicationVersionQuery(tt.owner)
		var plans []struct{ Plan planNode }
		if err := pool.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&plans); err != nil {
			t.Fatalf("owner %q: %v", tt.owner, err)
		}
		plan := plans[0].Plan
		if blocks := plan.SharedHitBlocks + plan.SharedReadBlocks; blocks >= 32 {
			t.Fatalf("owner %q: read %v blocks", tt.owner, blocks)
		}
		scans := 0
		for _, node := range plan.nodes() {
			if !strings.Contains(node.NodeType, "Scan") {
				continue
			}
			scans++
			if !strings.Contains(node.NodeType, "Index") {
				t.Fatalf("owner %q: %s is not an index scan", tt.owner, node.NodeType)
			}
			if rows := node.ActualRows * node.ActualLoops; rows > 1 {
				t.Fatalf("owner %q: %s returned %v rows", tt.owner, node.NodeType, rows)
			}
			if node.RowsRemovedFilter != 0 {
				t.Fatalf("owner %q: %s filtered %v rows", tt.owner, node.NodeType, node.RowsRemovedFilter)
			}
		}
		if scans == 0 {
			t.Fatalf("owner %q: no scan nodes in plan", tt.owner)
		}
	}

	// An empty scope keeps the public missing-version error.
	if _, err := pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %q.application_versions WHERE application_name IS NULL`, schema)); err != nil {
		t.Fatal(err)
	}
	s := &SysDB{pool: NewPgxPool(pool), dialect: PostgresDialect{}, schema: schema, appName: "absent"}
	if _, err := s.GetLatestApplicationVersion(ctx, nil, ""); !errors.Is(err, &models.Error{Code: models.ErrorCodeNoApplicationVersions}) {
		t.Fatalf("expected missing-version error, got %v", err)
	}
}

// TestLatestApplicationVersionSqlite checks the two-probe query is valid SQLite
// and scopes the same way as on Postgres.
func TestLatestApplicationVersionSqlite(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE application_versions (version_id TEXT, version_name TEXT, version_timestamp INTEGER, created_at INTEGER, application_name TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, m := range BuildSqliteMigrations() {
		if m.Version == 124 || m.Version == 125 {
			if _, err := db.Exec(m.SQL); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, owner := range []any{"ours", nil, "peer"} {
		ts := (i + 1) * 100
		if _, err := db.Exec(`INSERT INTO application_versions VALUES (?, ?, ?, ?, ?)`, fmt.Sprint(ts), fmt.Sprintf("version-%d", ts), ts, ts, owner); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	for _, tt := range []struct{ owner, want string }{
		{"ours", "version-200"},
		{"absent", "version-200"},
		{"peer", "version-300"},
		{"", "version-300"},
	} {
		s := &SysDB{pool: NewSQLPool(db), dialect: SqliteDialect{}, appName: tt.owner}
		latest, err := s.GetLatestApplicationVersion(ctx, nil, "")
		if err != nil {
			t.Fatalf("owner %q: %v", tt.owner, err)
		}
		if latest.Name != tt.want {
			t.Fatalf("owner %q: got %s, want %s", tt.owner, latest.Name, tt.want)
		}
	}
}

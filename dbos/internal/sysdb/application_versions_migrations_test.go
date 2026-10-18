package sysdb

import (
	"database/sql"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestApplicationVersionMigrations(t *testing.T) {
	for _, cockroach := range []bool{false, true} {
		migrations := BuildMigrations(`version"schema`, cockroach)
		for _, m := range migrations {
			if m.Version != 124 && m.Version != 125 {
				continue
			}
			if m.Online != !cockroach || strings.Contains(m.SQL, "CONCURRENTLY") != !cockroach {
				t.Fatalf("migration %d has the wrong online protocol: %+v", m.Version, m)
			}
			if !strings.Contains(m.SQL, `"version""schema".`) {
				t.Fatalf("migration %d did not quote the schema: %s", m.Version, m.SQL)
			}
		}
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE application_versions (version_name TEXT, version_timestamp INTEGER, application_name TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, m := range BuildSqliteMigrations() {
		if m.Version != 124 && m.Version != 125 {
			continue
		}
		for range 2 {
			if _, err := db.Exec(m.SQL); err != nil {
				t.Fatalf("migration %d must be idempotent: %v", m.Version, err)
			}
		}
	}
	for _, tt := range []struct {
		name string
		keys []string
	}{
		{"idx_application_versions_timestamp", []string{"version_timestamp:1"}},
		{"idx_application_versions_owner_timestamp", []string{"application_name:0", "version_timestamp:1"}},
	} {
		rows, err := db.Query(`SELECT name || ':' || desc FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno`, tt.name)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				t.Fatal(err)
			}
			got = append(got, key)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if strings.Join(got, ",") != strings.Join(tt.keys, ",") {
			t.Fatalf("%s keys: got %v, want %v", tt.name, got, tt.keys)
		}
	}
}

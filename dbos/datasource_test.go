package dbos

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// openUserSQLiteDB opens a separate, user-owned sqlite database (distinct file
// from the DBOS system database) to stand in for an application's own engine.
func openUserSQLiteDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "userapp.db")
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestRegisterDataSource(t *testing.T) {
	if !useSqliteBackend() {
		t.Skip("data source registration smoke test runs on the sqlite backend")
	}

	t.Run("CreatesCompletionTableAtLaunch", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		userDB := openUserSQLiteDB(t)

		ds := RegisterDataSource(ctx, "app", userDB)
		require.NotNil(t, ds)
		require.Equal(t, "app", ds.Name())

		// Table must not exist until Launch creates it.
		require.False(t, sqliteTableExists(t, userDB, "transaction_completion"))

		require.NoError(t, Launch(ctx))

		require.True(t, sqliteTableExists(t, userDB, "transaction_completion"))
	})

	t.Run("PanicsOnEmptyName", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		userDB := openUserSQLiteDB(t)

		require.Panics(t, func() { _ = RegisterDataSource(ctx, "", userDB) })
	})

	t.Run("PanicsOnDuplicateName", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})

		require.NotNil(t, RegisterDataSource(ctx, "app", openUserSQLiteDB(t)))
		require.Panics(t, func() { _ = RegisterDataSource(ctx, "app", openUserSQLiteDB(t)) })
	})

	// A typed nil pointer satisfies the Engine constraint, so it reaches the
	// per-case nil check. (An untyped nil won't compile — type inference fails.)
	t.Run("PanicsOnTypedNilEngine", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		var nilDB *sql.DB
		require.Panics(t, func() { _ = RegisterDataSource(ctx, "app", nilDB) })
	})

	t.Run("PanicsOnRegistrationAfterLaunch", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, Launch(ctx))

		userDB := openUserSQLiteDB(t)
		require.Panics(t, func() { _ = RegisterDataSource(ctx, "app", userDB) })
	})
}

func sqliteTableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var name string
	err := db.QueryRowContext(context.Background(),
		`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name)
	if err == sql.ErrNoRows {
		return false
	}
	require.NoError(t, err)
	return name == table
}

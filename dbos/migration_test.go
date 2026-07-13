package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poolFromContext extracts the underlying pgxpool from a DBOSContext that was
// set up via setupDBOS.
func poolFromContext(t *testing.T, ctx DBOSContext) *pgxpool.Pool {
	t.Helper()
	c, ok := ctx.(*dbosContext)
	require.True(t, ok)
	s, ok := c.systemDB.(*sysdb.SysDB)
	require.True(t, ok)
	return PgxPool(s.Pool())
}

// detectCockroach reports whether the pool is connected to CockroachDB, so
// migration tests build and run the same SQL variant the production runner
// selects (some migrations, e.g. 38, emit Postgres-only DDL otherwise).
func detectCockroach(t *testing.T, pool *pgxpool.Pool) bool {
	t.Helper()
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err)
	defer conn.Release()
	return sysdb.IsCockroachDB(conn.Conn())
}

// TestShouldMigrate verifies the early-exit predicate used to skip the full
// migration pipeline when the schema is already at the latest version.
func TestShouldMigrate(t *testing.T) {
	skipIfSqlite(t, "pg migration pipeline; sqlite uses runSqliteMigrations")
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()
	migs := sysdb.BuildMigrations("dbos", false)
	latest := migs[len(migs)-1].Version

	// Freshly-migrated schema should report no migration needed.
	need, err := sysdb.ShouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.False(t, need, "fully migrated schema should not need migration")

	// Rewinding the version makes a migration pending again.
	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", latest-1)
	require.NoError(t, err)
	need, err = sysdb.ShouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.True(t, need, "rewound schema should need migration")

	// Restore, then drop the dbos_migrations table to simulate a partially
	// initialised schema. shouldMigrate must report True.
	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", latest)
	require.NoError(t, err)
	need, err = sysdb.ShouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.False(t, need)

	_, err = pool.Exec(bg, "DROP TABLE dbos.dbos_migrations")
	require.NoError(t, err)
	need, err = sysdb.ShouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.True(t, need, "missing migration table should need migration")

	// A schema that does not exist should also need migration.
	need, err = sysdb.ShouldMigrate(bg, pool, "nonexistent_schema_xyz", false)
	require.NoError(t, err)
	assert.True(t, need, "nonexistent schema should need migration")
}

// TestOnlineMigrationsAreIdempotent rewinds the migration version to just
// before the first online migration and re-runs the runner. Every online
// migration must include IF [NOT] EXISTS guards so that re-running them
// against an already-migrated schema succeeds.
func TestOnlineMigrationsAreIdempotent(t *testing.T) {
	skipIfSqlite(t, "pg online-migration semantics; sqlite migrations are all inline")
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()
	isCockroach := detectCockroach(t, pool)

	// First online migration is version 22 (drop forked_from index).
	const rewindTo = int64(21)
	migs := sysdb.BuildMigrations("dbos", isCockroach)
	latest := migs[len(migs)-1].Version

	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	logger := slog.Default()
	require.NoError(t, sysdb.RunMigrations(bg, pool, "dbos", isCockroach, logger))

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

// TestVersionNotBumpedOnMigrationFailure ensures that when a single migration
// fails mid-run, the dbos_migrations version counter stays at the prior value
// so the runner re-attempts it on next start.
func TestVersionNotBumpedOnMigrationFailure(t *testing.T) {
	skipIfSqlite(t, "pg-only migration failure semantics")
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()
	isCockroach := detectCockroach(t, pool)
	migs := sysdb.BuildMigrations("dbos", isCockroach)
	latest := migs[len(migs)-1].Version

	const rewindTo = int64(20)
	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	err = sysdb.RunMigrations(bg, pool, "dbos", isCockroach, slog.Default())
	require.Error(t, err, "migration 21 should fail because dbos.queues already exists")
	assert.Contains(t, err.Error(), "already exists")

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, rewindTo, version, "version should still be 20 (migration 21 failed inside its tx)")

	// Clear the conflict and re-run: the catalog tx now commits and the
	// later online migrations idempotently re-apply.
	_, err = pool.Exec(bg, "DROP TABLE dbos.queues")
	require.NoError(t, err)
	require.NoError(t, sysdb.RunMigrations(bg, pool, "dbos", isCockroach, slog.Default()))
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

// TestRunnerResumesAfterInvalidIndex simulates a CREATE INDEX CONCURRENTLY
// that crashed mid-build (leaving an INVALID index) and verifies the runner
// cleans it up and re-runs the migration on the next start.
func TestRunnerResumesAfterInvalidIndex(t *testing.T) {
	skipIfSqlite(t, "pg invalid-index recovery is pg-only")
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	// Postgres-only: CRDB blocks direct pg_index mutation, and its migrations
	// are not online so cleanupInvalidIndexes is never invoked on CRDB.
	conn, err := pool.Acquire(bg)
	require.NoError(t, err)
	if sysdb.IsCockroachDB(conn.Conn()) {
		conn.Release()
		t.Skip("invalid-index recovery is Postgres-only")
	}
	conn.Release()

	const targetIndex = "idx_workflow_status_in_flight"
	const rewindTo = int64(31) // migration 32 builds the target index
	migs := sysdb.BuildMigrations("dbos", false)
	latest := migs[len(migs)-1].Version

	// Drop the valid index, then plant an invalid one of the same name.
	// Flipping pg_index.indisvalid mimics what Postgres leaves behind when
	// CREATE INDEX CONCURRENTLY aborts mid-build.
	_, err = pool.Exec(bg, fmt.Sprintf(`DROP INDEX IF EXISTS dbos.%q`, targetIndex))
	require.NoError(t, err)
	_, err = pool.Exec(bg, fmt.Sprintf(
		`CREATE INDEX %q ON dbos.workflow_status (queue_name, status, priority, created_at) WHERE status IN ('ENQUEUED', 'PENDING')`,
		targetIndex))
	require.NoError(t, err)
	_, err = pool.Exec(bg, fmt.Sprintf(
		`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'dbos.%s'::regclass`,
		targetIndex))
	require.NoError(t, err)

	// Confirm the planted index is INVALID.
	var valid bool
	require.NoError(t, pool.QueryRow(bg,
		fmt.Sprintf(`SELECT indisvalid FROM pg_index WHERE indexrelid = 'dbos.%s'::regclass`, targetIndex)).Scan(&valid))
	assert.False(t, valid)

	// Rewind so the runner re-applies migration 32.
	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	// Re-run migrations. cleanupInvalidIndexes should drop the invalid index,
	// then migration 32+ rebuild it.
	require.NoError(t, sysdb.RunMigrations(bg, pool, "dbos", false, slog.Default()))

	require.NoError(t, pool.QueryRow(bg,
		fmt.Sprintf(`SELECT indisvalid FROM pg_index WHERE indexrelid = 'dbos.%s'::regclass`, targetIndex)).Scan(&valid))
	assert.True(t, valid, "index should be valid after cleanup + rebuild")

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

// TestNewSystemDatabaseErrorPathNoDeadlock forces shouldMigrate to fail inside
// newSystemDatabase and verifies the error is returned instead of deadlocking.
// The error paths after the CockroachDB-detection Acquire call pool.Close()
// while that connection is still checked out; puddle's Close blocks until all
// resources are destroyed, and the deferred Release can only run after
// newSystemDatabase returns — a single-goroutine deadlock.
func TestNewSystemDatabaseErrorPathNoDeadlock(t *testing.T) {
	skipIfSqlite(t, "pg pool lifecycle; sqlite has no pgx pool to close")
	databaseURL := getDatabaseURL()
	bg := context.Background()

	pool, err := pgxpool.New(bg, databaseURL)
	require.NoError(t, err)
	defer pool.Close()

	// A migration table without a version column makes shouldMigrate fail
	// with a non-retryable error (undefined column).
	const schema = "deadlock_test_schema"
	_, err = pool.Exec(bg, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	require.NoError(t, err)
	_, err = pool.Exec(bg, fmt.Sprintf("CREATE SCHEMA %s", schema))
	require.NoError(t, err)
	_, err = pool.Exec(bg, fmt.Sprintf("CREATE TABLE %s.%s (bogus INT)", schema, sysdb.MigrationTable))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(bg, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
	})

	done := make(chan error, 1)
	go func() {
		_, sdErr := sysdb.NewSystemDatabase(bg, sysdb.NewSystemDatabaseInput{
			DatabaseURL:    databaseURL,
			DatabaseSchema: schema,
			Logger:         slog.Default(),
		})
		done <- sdErr
	}()

	select {
	case sdErr := <-done:
		require.Error(t, sdErr)
		assert.Contains(t, sdErr.Error(), "failed to determine migration status")
	case <-time.After(30 * time.Second):
		t.Fatal("newSystemDatabase deadlocked on its error path: pool.Close() waits on the still-acquired CockroachDB-detection connection")
	}
}

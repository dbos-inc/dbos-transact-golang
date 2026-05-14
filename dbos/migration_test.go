package dbos

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

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
	s, ok := c.systemDB.(*sysDB)
	require.True(t, ok)
	return s.pool
}

// TestShouldMigrate verifies the early-exit predicate used to skip the full
// migration pipeline when the schema is already at the latest version.
func TestShouldMigrate(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()
	migs := buildMigrations("dbos", false)
	latest := migs[len(migs)-1].version

	// Freshly-migrated schema should report no migration needed.
	need, err := shouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.False(t, need, "fully migrated schema should not need migration")

	// Rewinding the version makes a migration pending again.
	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", latest-1)
	require.NoError(t, err)
	need, err = shouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.True(t, need, "rewound schema should need migration")

	// Restore, then drop the dbos_migrations table to simulate a partially
	// initialised schema. shouldMigrate must report True.
	_, err = pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", latest)
	require.NoError(t, err)
	need, err = shouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.False(t, need)

	_, err = pool.Exec(bg, "DROP TABLE dbos.dbos_migrations")
	require.NoError(t, err)
	need, err = shouldMigrate(bg, pool, "dbos", false)
	require.NoError(t, err)
	assert.True(t, need, "missing migration table should need migration")

	// A schema that does not exist should also need migration.
	need, err = shouldMigrate(bg, pool, "nonexistent_schema_xyz", false)
	require.NoError(t, err)
	assert.True(t, need, "nonexistent schema should need migration")
}

// TestOnlineMigrationsAreIdempotent rewinds the migration version to just
// before the first online migration and re-runs the runner. Every online
// migration must include IF [NOT] EXISTS guards so that re-running them
// against an already-migrated schema succeeds.
func TestOnlineMigrationsAreIdempotent(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	// First online migration is version 22 (drop forked_from index).
	const rewindTo = int64(21)
	migs := buildMigrations("dbos", false)
	latest := migs[len(migs)-1].version

	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	logger := slog.Default()
	require.NoError(t, runMigrations(bg, pool, "dbos", false, logger))

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

// TestVersionNotBumpedOnMigrationFailure ensures that when a single migration
// fails mid-run, the dbos_migrations version counter stays at the prior value
// so the runner re-attempts it on next start.
func TestVersionNotBumpedOnMigrationFailure(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	// Rewind so migration 32 (create in_flight index) is pending again.
	const rewindTo = int64(31)
	_, err := pool.Exec(bg, "UPDATE dbos.dbos_migrations SET version = $1", rewindTo)
	require.NoError(t, err)

	// Swap migration 32's body with invalid SQL and re-run. The runner must
	// raise and leave the version unchanged.
	migrations := buildMigrations("dbos", false)
	require.Equal(t, int64(32), migrations[31].version)
	migrations[31].sql = "THIS IS NOT VALID SQL"
	latest := migrations[len(migrations)-1].version

	err = applyMigrationsForTest(bg, pool, "dbos", migrations, rewindTo, slog.Default())
	require.Error(t, err)

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, rewindTo, version, "version should not have advanced past 31")

	// Re-run with real migrations: IF NOT EXISTS guards make 32+ idempotent.
	require.NoError(t, runMigrations(bg, pool, "dbos", false, slog.Default()))
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

// TestRunnerResumesAfterInvalidIndex simulates a CREATE INDEX CONCURRENTLY
// that crashed mid-build (leaving an INVALID index) and verifies the runner
// cleans it up and re-runs the migration on the next start.
func TestRunnerResumesAfterInvalidIndex(t *testing.T) {
	ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	pool := poolFromContext(t, ctx)
	bg := context.Background()

	const targetIndex = "idx_workflow_status_in_flight"
	const rewindTo = int64(31) // migration 32 builds the target index
	migs := buildMigrations("dbos", false)
	latest := migs[len(migs)-1].version

	// Drop the valid index, then plant an invalid one of the same name.
	// Flipping pg_index.indisvalid mimics what Postgres leaves behind when
	// CREATE INDEX CONCURRENTLY aborts mid-build.
	_, err := pool.Exec(bg, fmt.Sprintf(`DROP INDEX IF EXISTS dbos.%q`, targetIndex))
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
	require.NoError(t, runMigrations(bg, pool, "dbos", false, slog.Default()))

	require.NoError(t, pool.QueryRow(bg,
		fmt.Sprintf(`SELECT indisvalid FROM pg_index WHERE indexrelid = 'dbos.%s'::regclass`, targetIndex)).Scan(&valid))
	assert.True(t, valid, "index should be valid after cleanup + rebuild")

	var version int64
	require.NoError(t, pool.QueryRow(bg, "SELECT version FROM dbos.dbos_migrations").Scan(&version))
	assert.Equal(t, latest, version)
}

// applyMigrationsForTest is a minimal wrapper that runs only the
// pending-migration loop of runMigrations against a pre-rendered migration
// list. Used by tests that need to inject a failing migration body without
// rebuilding the whole runner.
func applyMigrationsForTest(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema string,
	migrations []migrationFile,
	currentVersion int64,
	logger *slog.Logger,
) error {
	invalidCleaned := false
	for _, migration := range migrations {
		if migration.version <= currentVersion {
			continue
		}
		if migration.online {
			if !invalidCleaned {
				if err := cleanupInvalidIndexes(ctx, pool, schema, logger); err != nil {
					return err
				}
				invalidCleaned = true
			}
			if _, err := pool.Exec(ctx, migration.sql); err != nil {
				return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
			}
			if err := writeMigrationVersion(ctx, pool, schema, migration.version, currentVersion); err != nil {
				return err
			}
		} else {
			mtx, err := pool.Begin(ctx)
			if err != nil {
				return err
			}
			if _, err := mtx.Exec(ctx, migration.sql); err != nil {
				mtx.Rollback(ctx)
				return fmt.Errorf("failed to execute migration %d: %v", migration.version, err)
			}
			if err := writeMigrationVersion(ctx, mtx, schema, migration.version, currentVersion); err != nil {
				mtx.Rollback(ctx)
				return err
			}
			if err := mtx.Commit(ctx); err != nil {
				return err
			}
		}
		currentVersion = migration.version
	}
	return nil
}


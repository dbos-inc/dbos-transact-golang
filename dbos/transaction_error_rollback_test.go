package dbos

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestTransactionErrorRollbackIsInconsistent runs the SAME failing transaction
// step against two data sources and shows they disagree on whether the body's
// write is rolled back.
//
// A transaction body writes a row and then returns an application error:
//   - on a separate data source, the write is rolled back (row count 0)
//   - on a data source that shares the system-DB pool (the runAsTxn path), the
//     write is committed along with the error record (row count 1)
//
// The second is the bug this documents. The test asserts both roll back, so it
// FAILS on the shared-pool path — intentionally. See the PR description.
func TestTransactionErrorRollbackIsInconsistent(t *testing.T) {
	// A shared data source (pool == system DB) and a separate one, one context.
	ctx, sharedDS, sharedDB := setupSharedDBOS(t)
	separateDB := openUserBackend(t)
	separateDS := separateDB.register(t, ctx, "separate")
	require.True(t, sharedDS.sameAsSystemDB, "sharedDS must use the system-DB pool")
	require.False(t, separateDS.sameAsSystemDB, "separateDS must be a separate pool")

	// Distinct tables: on Postgres both pools point at the same database, so a
	// shared table name would let one path's row mask the other's.
	bg := context.Background()
	for _, tbl := range []struct {
		db   *userBackend
		name string
	}{{separateDB, "kv_sep"}, {sharedDB, "kv_shared"}} {
		_, err := tbl.db.pool.Exec(bg, "DROP TABLE IF EXISTS "+tbl.name)
		require.NoError(t, err)
		_, err = tbl.db.pool.Exec(bg, "CREATE TABLE "+tbl.name+" (k TEXT PRIMARY KEY, v TEXT)")
		require.NoError(t, err)
	}

	// A transaction that writes a row and then returns an application error. The
	// error is ignored so the workflow runs both back to back.
	writeThenFail := func(dctx DBOSContext, ds *DataSource, db *userBackend, tbl string) {
		_, _ = RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (string, error) {
			_, _ = tx.Exec(c, db.rw("INSERT INTO "+tbl+" (k, v) VALUES ($1, $2)"), "k", "v")
			return "", errors.New("business rule violated")
		})
	}
	wf := func(dctx DBOSContext, _ string) (string, error) {
		writeThenFail(dctx, separateDS, separateDB, "kv_sep")
		writeThenFail(dctx, sharedDS, sharedDB, "kv_shared")
		return "done", nil
	}
	RegisterWorkflow(ctx, wf)
	require.NoError(t, Launch(ctx))

	h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(uuid.NewString()))
	require.NoError(t, err)
	_, err = h.GetResult()
	require.NoError(t, err)

	separate := separateDB.countRows(t, "SELECT count(*) FROM kv_sep")
	shared := sharedDB.countRows(t, "SELECT count(*) FROM kv_shared")
	t.Logf("rows left by the failed transaction — separate data source: %d, shared system-DB pool: %d",
		separate, shared)

	require.Equal(t, 0, separate, "separate data source rolls the failed write back")
	require.Equal(t, 0, shared, "shared system-DB pool should roll back too, but the write persisted")
}

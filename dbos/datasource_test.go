package dbos

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// createUserAppTable creates a simple application table in the user's database.
func createUserAppTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
}

// countRows returns the number of rows matching the single-column query.
func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(query, args...).Scan(&n))
	return n
}

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

func TestRunAsTransaction(t *testing.T) {
	if !useSqliteBackend() {
		t.Skip("RunAsTransaction tests run on the sqlite backend")
	}

	t.Run("HappyPath", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		userDB := openUserSQLiteDB(t)
		ds := RegisterDataSource(ctx, "app", userDB)

		var runs atomic.Int32
		wf := func(dctx DBOSContext, item string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				if _, err := tx.Exec(c, `INSERT INTO kv (k, v) VALUES (?, ?)`, "k1", item); err != nil {
					return 0, err
				}
				return 42, nil
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		createUserAppTable(t, userDB)

		wfID := uuid.NewString()
		handle, err := RunWorkflow(ctx, wf, "hello", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, int64(42), res)
		require.Equal(t, int32(1), runs.Load())

		// Application write committed.
		var v string
		require.NoError(t, userDB.QueryRow(`SELECT v FROM kv WHERE k = 'k1'`).Scan(&v))
		require.Equal(t, "hello", v)

		// Durability row in the user DB (txn1). The first step is step_id 0.
		require.Equal(t, 1, countRows(t, userDB,
			`SELECT count(*) FROM transaction_completion WHERE workflow_id = ? AND step_id = 0`, wfID))

		// Checkpoint in the system DB (txn2).
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, 0, steps[0].StepID)
	})

	t.Run("UserRetryThenSucceed", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		userDB := openUserSQLiteDB(t)
		ds := RegisterDataSource(ctx, "app", userDB)

		var runs atomic.Int32
		wf := func(dctx DBOSContext, _ string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				n := runs.Add(1)
				// Insert on every attempt; failing attempts must roll back.
				if _, err := tx.Exec(c, `INSERT INTO kv (k, v) VALUES (?, ?)`, "k1", "v"); err != nil {
					return 0, err
				}
				if n < 3 {
					return 0, errors.New("transient app failure")
				}
				return int64(n), nil
			}, WithStepMaxRetries(3))
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		createUserAppTable(t, userDB)

		handle, err := RunWorkflow(ctx, wf, "", WithWorkflowID(uuid.NewString()))
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, int64(3), res)
		require.Equal(t, int32(3), runs.Load()) // 2 rollbacks + 1 commit

		// Only the committed attempt persisted; rollbacks discarded.
		require.Equal(t, 1, countRows(t, userDB, `SELECT count(*) FROM kv`))
		require.Equal(t, 1, countRows(t, userDB, `SELECT count(*) FROM transaction_completion`))
	})

	t.Run("Layer1ReplayOnRecovery", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		userDB := openUserSQLiteDB(t)
		ds := RegisterDataSource(ctx, "app", userDB)

		var runs atomic.Int32
		wf := func(dctx DBOSContext, _ string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				_, err := tx.Exec(c, `INSERT INTO kv (k, v) VALUES (?, ?)`, "k1", "v")
				return 7, err
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		createUserAppTable(t, userDB)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
		require.Equal(t, int32(1), runs.Load())

		// Force recovery: operation_outputs is intact, so layer-1 replays.
		setWorkflowStatusPending(t, ctx, wfID)
		handles, err := recoverPendingWorkflows(ctx.(*dbosContext), []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		res, err := handles[0].GetResult()
		require.NoError(t, err)
		require.EqualValues(t, 7, res)          // recovered handle is WorkflowHandle[any] → float64
		require.Equal(t, int32(1), runs.Load()) // fn NOT re-run
		require.Equal(t, 1, countRows(t, userDB, `SELECT count(*) FROM kv`))
	})

	t.Run("Layer2CrashWindowReplay", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		userDB := openUserSQLiteDB(t)
		ds := RegisterDataSource(ctx, "app", userDB)

		var runs atomic.Int32
		wf := func(dctx DBOSContext, _ string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				_, err := tx.Exec(c, `INSERT INTO kv (k, v) VALUES (?, ?)`, "k1", "v")
				return 9, err
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		createUserAppTable(t, userDB)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
		require.Equal(t, int32(1), runs.Load())

		// Simulate a crash between txn1 (user commit) and txn2 (system
		// checkpoint): drop the operation_outputs row but keep the
		// transaction_completion row.
		sysPool := ctx.(*dbosContext).systemDB.(*sysDB).pool
		_, err = sysPool.Exec(context.Background(),
			`DELETE FROM operation_outputs WHERE workflow_uuid = ? AND function_id = ?`, wfID, 0)
		require.NoError(t, err)
		require.Equal(t, 1, countRows(t, userDB,
			`SELECT count(*) FROM transaction_completion WHERE workflow_id = ? AND step_id = 0`, wfID))

		// Recover: layer-1 misses, layer-2 replays the stored output without
		// re-running fn.
		setWorkflowStatusPending(t, ctx, wfID)
		handles, err := recoverPendingWorkflows(ctx.(*dbosContext), []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		res, err := handles[0].GetResult()
		require.NoError(t, err)
		require.EqualValues(t, 9, res)                                       // recovered handle is WorkflowHandle[any] → float64
		require.Equal(t, int32(1), runs.Load())                              // fn NOT re-run
		require.Equal(t, 1, countRows(t, userDB, `SELECT count(*) FROM kv`)) // no duplicate insert

		// txn2 re-applied: operation_outputs restored.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
	})
}

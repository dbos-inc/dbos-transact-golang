package dbos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// userBackend abstracts the user-owned database for data source tests so the
// suite runs on every backend: a standalone sqlite file, or a pgx pool against
// the same server as the system database (Postgres/CockroachDB). The completion
// table lives in the user database — under the "dbos" schema on Postgres, schema-
// less on sqlite — so test SQL is written in canonical $N form and rewritten per
// dialect via rw()/completionTable().
type userBackend struct {
	pool    Pool
	dialect Dialect
	schema  string
}

// openUserBackend opens a user-owned database distinct from the DBOS system
// database (a separate sqlite file, or a separate pgx pool on the same server).
func openUserBackend(t *testing.T) *userBackend {
	t.Helper()
	if useSqliteBackend() {
		path := filepath.Join(t.TempDir(), "userapp.db")
		db, err := sql.Open("sqlite", path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return &userBackend{pool: newSQLPool(db), dialect: sqliteDialect{}, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
	}
	cfg, err := pgxpool.ParseConfig(backendDatabaseURL(t))
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &userBackend{pool: newPgxPool(pool), dialect: postgresDialect{}, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
}

// register registers this backend's engine as a data source. The two concrete
// branches instantiate the generic RegisterDataSource with the real engine type.
func (u *userBackend) register(t *testing.T, ctx DBOSContext, name string, opts ...DataSourceOption) *DataSource {
	t.Helper()
	if db := SQLDB(u.pool); db != nil {
		return RegisterDataSource(ctx, name, db, opts...)
	}
	return RegisterDataSource(ctx, name, PgxPool(u.pool), opts...)
}

// dropCompletionTable removes the transaction_completion table for a clean
// slate before Launch. setupDBOS's dropDB resets the DBOS system tables but not
// this one — it lives in the user database — so a re-run would otherwise see a
// stale table. Drops only the table (via the sanitized schema-qualified name),
// never the schema, so the system tables sharing "dbos" on Postgres survive.
// DROP TABLE IF EXISTS tolerates both a missing table and a missing schema.
func (u *userBackend) dropCompletionTable(t *testing.T) {
	t.Helper()
	_, err := u.pool.Exec(context.Background(),
		fmt.Sprintf(`DROP TABLE IF EXISTS %s`, u.completionTable()))
	require.NoError(t, err)
}

// rw rewrites a canonical $N query into the backend's native placeholder form.
func (u *userBackend) rw(q string) string { return u.dialect.RewriteQuery(q) }

// completionTable returns the schema-qualified transaction_completion table name.
func (u *userBackend) completionTable() string {
	return u.dialect.SchemaPrefix(u.schema) + transactionCompletionTable
}

// createAppTable (re)creates the application's kv table, freshly per test.
func (u *userBackend) createAppTable(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	_, err := u.pool.Exec(ctx, `DROP TABLE IF EXISTS kv`)
	require.NoError(t, err)
	_, err = u.pool.Exec(ctx, `CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
}

// countRows runs a single-column count query (canonical $N) against the user DB.
func (u *userBackend) countRows(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, u.pool.QueryRow(context.Background(), u.rw(query), args...).Scan(&n))
	return n
}

// queryString scans a single text column from the user DB.
func (u *userBackend) queryString(t *testing.T, query string, args ...any) string {
	t.Helper()
	var s string
	require.NoError(t, u.pool.QueryRow(context.Background(), u.rw(query), args...).Scan(&s))
	return s
}

// completionTableExists reports whether the transaction_completion table exists.
func (u *userBackend) completionTableExists(t *testing.T) bool {
	t.Helper()
	ctx := context.Background()
	if u.dialect.Name() == DialectSQLite {
		var name string
		err := u.pool.QueryRow(ctx,
			`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, transactionCompletionTable).Scan(&name)
		if errors.Is(err, ErrNoRows) {
			return false
		}
		require.NoError(t, err)
		return true
	}
	var exists bool
	require.NoError(t, u.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)`,
		u.schema, transactionCompletionTable).Scan(&exists))
	return exists
}

func TestRegisterDataSource(t *testing.T) {
	t.Run("CreatesCompletionTableAtLaunch", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ub.dropCompletionTable(t)

		ds := ub.register(t, ctx, "app")
		require.NotNil(t, ds)
		require.Equal(t, "app", ds.Name())

		// Table must not exist until Launch creates it.
		require.False(t, ub.completionTableExists(t))

		require.NoError(t, Launch(ctx))

		require.True(t, ub.completionTableExists(t))
	})

	// A schema name full of characters that must be quoted to be a valid SQL
	// identifier (uppercase, digits, '@', '-') must survive registration and the
	// CREATE SCHEMA / CREATE TABLE at Launch — exercising pgx.Identifier escaping
	// end to end. No-op schema on SQLite, so this just confirms the table still
	// lands there too.
	t.Run("CreatesCompletionTableInFunkySchema", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ub.schema = "F8nny_sCHem@-n@m3"
		ub.dropCompletionTable(t)

		ds := ub.register(t, ctx, "app", WithDataSourceSchema(ub.schema))
		require.NotNil(t, ds)

		require.False(t, ub.completionTableExists(t))

		require.NoError(t, Launch(ctx))

		require.True(t, ub.completionTableExists(t))
	})

	t.Run("PanicsOnEmptyName", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)

		require.Panics(t, func() { _ = ub.register(t, ctx, "") })
	})

	t.Run("PanicsOnDuplicateName", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)

		require.NotNil(t, ub.register(t, ctx, "app"))
		require.Panics(t, func() { _ = ub.register(t, ctx, "app") })
	})

	// A typed nil pointer satisfies the Engine constraint, so it reaches the
	// per-case nil check. (An untyped nil won't compile — type inference fails.)
	t.Run("PanicsOnTypedNilEngine", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		if useSqliteBackend() {
			var nilDB *sql.DB
			require.Panics(t, func() { _ = RegisterDataSource(ctx, "app", nilDB) })
		} else {
			var nilPool *pgxpool.Pool
			require.Panics(t, func() { _ = RegisterDataSource(ctx, "app", nilPool) })
		}
	})

	t.Run("PanicsOnRegistrationAfterLaunch", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, Launch(ctx))

		ub := openUserBackend(t)
		require.Panics(t, func() { _ = ub.register(t, ctx, "app") })
	})
}

func TestRunAsTransaction(t *testing.T) {
	t.Run("HappyPath", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		var runs atomic.Int32
		wf := func(dctx DBOSContext, item string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", item); err != nil {
					return 0, err
				}
				return 42, nil
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

		wfID := uuid.NewString()
		handle, err := RunWorkflow(ctx, wf, "hello", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, int64(42), res)
		require.Equal(t, int32(1), runs.Load())

		// Application write committed.
		require.Equal(t, "hello", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k1'`))

		// Durability row in the user DB (txn1). The first step is step_id 0.
		require.Equal(t, 1, ub.countRows(t,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE workflow_id = $1 AND step_id = 0`, ub.completionTable()), wfID))

		// Checkpoint in the system DB (txn2).
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, 0, steps[0].StepID)
	})

	t.Run("UserRetryThenSucceed", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		var runs atomic.Int32
		wf := func(dctx DBOSContext, _ string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				n := runs.Add(1)
				// Insert on every attempt; failing attempts must roll back.
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "v"); err != nil {
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
		ub.createAppTable(t)

		handle, err := RunWorkflow(ctx, wf, "", WithWorkflowID(uuid.NewString()))
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, int64(3), res)
		require.Equal(t, int32(3), runs.Load()) // 2 rollbacks + 1 commit

		// Only the committed attempt persisted; rollbacks discarded.
		require.Equal(t, 1, ub.countRows(t, `SELECT count(*) FROM kv`))
		require.Equal(t, 1, ub.countRows(t, fmt.Sprintf(`SELECT count(*) FROM %s`, ub.completionTable())))
	})

	t.Run("Layer1ReplayOnRecovery", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		var runs atomic.Int32
		wf := func(dctx DBOSContext, _ string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				_, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "v")
				return 7, err
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

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
		require.Equal(t, 1, ub.countRows(t, `SELECT count(*) FROM kv`))
	})

	t.Run("Layer2CrashWindowReplay", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		var runs atomic.Int32
		wf := func(dctx DBOSContext, _ string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				_, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "v")
				return 9, err
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.NoError(t, err)
		require.Equal(t, int32(1), runs.Load())

		// Simulate a crash between txn1 (user commit) and txn2 (system
		// checkpoint): drop the operation_outputs row but keep the
		// transaction_completion row.
		sys := ctx.(*dbosContext).systemDB.(*sysDB)
		delQ := sys.dialect.RewriteQuery(fmt.Sprintf(
			`DELETE FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
			sys.dialect.SchemaPrefix(sys.schema)))
		_, err = sys.pool.Exec(context.Background(), delQ, wfID, 0)
		require.NoError(t, err)
		require.Equal(t, 1, ub.countRows(t,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE workflow_id = $1 AND step_id = 0`, ub.completionTable()), wfID))

		// Recover: layer-1 misses, layer-2 replays the stored output without
		// re-running fn.
		setWorkflowStatusPending(t, ctx, wfID)
		handles, err := recoverPendingWorkflows(ctx.(*dbosContext), []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		res, err := handles[0].GetResult()
		require.NoError(t, err)
		require.EqualValues(t, 9, res)                                  // recovered handle is WorkflowHandle[any] → float64
		require.Equal(t, int32(1), runs.Load())                         // fn NOT re-run
		require.Equal(t, 1, ub.countRows(t, `SELECT count(*) FROM kv`)) // no duplicate insert

		// txn2 re-applied: operation_outputs restored.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
	})
}

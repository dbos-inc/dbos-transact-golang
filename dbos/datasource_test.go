package dbos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

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
		return &userBackend{pool: sysdb.NewSQLPool(db), dialect: sysdb.SqliteDialect{}, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
	}
	cfg, err := pgxpool.ParseConfig(backendDatabaseURL(t))
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &userBackend{pool: sysdb.NewPgxPool(pool), dialect: sysdb.PostgresDialect{}, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
}

// register creates a data source over this backend's engine, naming it via
// WithDataSourceName. The two concrete branches instantiate the generic
// NewDataSource with the real engine type. NewDataSource creates the completion
// table eagerly, so any failure is surfaced here.
func (u *userBackend) register(t *testing.T, ctx DBOSContext, name string, opts ...DataSourceOption) *DataSource {
	t.Helper()
	opts = append(opts, WithDataSourceName(name))
	var (
		ds  *DataSource
		err error
	)
	if db := SQLDB(u.pool); db != nil {
		ds, err = NewDataSource(ctx, db, opts...)
	} else {
		ds, err = NewDataSource(ctx, PgxPool(u.pool), opts...)
	}
	require.NoError(t, err)
	return ds
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

// completionCells returns the output and error columns of the durability row for
// (workflowID, stepID). Both are nullable: a success row has output set and error
// nil; a failure row has the reverse.
func (u *userBackend) completionCells(t *testing.T, wfID string, stepID int) (output, errStr *string) {
	t.Helper()
	require.NoError(t, u.pool.QueryRow(context.Background(),
		u.rw(fmt.Sprintf(`SELECT output, error FROM %s WHERE workflow_id = $1 AND step_id = $2`, u.completionTable())),
		wfID, stepID).Scan(&output, &errStr))
	return output, errStr
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

func TestNewDataSource(t *testing.T) {
	t.Run("CreatesCompletionTableEagerly", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ub.dropCompletionTable(t)

		// Table must not exist until NewDataSource creates it.
		require.False(t, ub.completionTableExists(t))

		ds := ub.register(t, ctx, "app")
		require.NotNil(t, ds)
		require.Equal(t, "app", ds.Name())

		// Created eagerly at NewDataSource time, before Launch.
		require.True(t, ub.completionTableExists(t))
	})

	// A schema name full of characters that must be quoted to be a valid SQL
	// identifier (uppercase, digits, '@', '-') must survive the CREATE SCHEMA /
	// CREATE TABLE that NewDataSource runs — exercising pgx.Identifier escaping end
	// to end. No-op schema on SQLite, so this just confirms the table still lands.
	t.Run("CreatesCompletionTableInFunkySchema", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ub.schema = "F8nny_sCHem@-n@m3"
		ub.dropCompletionTable(t)

		require.False(t, ub.completionTableExists(t))

		ds := ub.register(t, ctx, "app", WithDataSourceSchema(ub.schema))
		require.NotNil(t, ds)

		require.True(t, ub.completionTableExists(t))
	})

	// Dynamic creation: a data source may be created after Launch, and its
	// completion table is still created on the spot.
	t.Run("CreatesAfterLaunch", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		require.NoError(t, Launch(ctx))

		ub := openUserBackend(t)
		ub.dropCompletionTable(t)
		require.False(t, ub.completionTableExists(t))

		ds := ub.register(t, ctx, "app")
		require.NotNil(t, ds)
		require.True(t, ub.completionTableExists(t))
	})

	t.Run("DefaultsName", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ub.dropCompletionTable(t)

		var (
			ds  *DataSource
			err error
		)
		if db := SQLDB(ub.pool); db != nil {
			ds, err = NewDataSource(ctx, db)
		} else {
			ds, err = NewDataSource(ctx, PgxPool(ub.pool))
		}
		require.NoError(t, err)
		require.Equal(t, defaultDataSourceName, ds.Name())
	})

	// A typed nil pointer satisfies the Engine constraint, so it reaches the
	// per-case nil check. (An untyped nil won't compile — type inference fails.)
	t.Run("ErrorsOnTypedNilEngine", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		if useSqliteBackend() {
			var nilDB *sql.DB
			_, err := NewDataSource(ctx, nilDB)
			require.Error(t, err)
		} else {
			var nilPool *pgxpool.Pool
			_, err := NewDataSource(ctx, nilPool)
			require.Error(t, err)
		}
	})

	// The existence pre-check drives the skip-DDL-when-installed path: a role with
	// only DML rights works against a pre-provisioned table. completionTableInstalled
	// must track the table's presence on both backends.
	t.Run("DetectsInstalledTable", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ub.dropCompletionTable(t)

		ds := ub.register(t, ctx, "app") // creates the table
		dctx := ctx.(*dbosContext)
		installed, err := ds.completionTableInstalled(dctx)
		require.NoError(t, err)
		require.True(t, installed)

		ub.dropCompletionTable(t)
		installed, err = ds.completionTableInstalled(dctx)
		require.NoError(t, err)
		require.False(t, installed)
	})
}

// TestNewDataSourceNoDDLPrivileges exercises the user-database permission model
// with a real role that lacks DDL rights (Postgres only — SQLite has no roles).
// It proves both halves of the TS-parity behavior: a missing table the role
// cannot create yields an actionable error, while a pre-provisioned table reached
// with only DML grants works end to end (the pre-check skips all DDL).
func TestNewDataSourceNoDDLPrivileges(t *testing.T) {
	skipIfSqlite(t, "Postgres role privileges; SQLite has no roles")
	skipIfCockroach(t, "insecure-mode CRDB rejects password login roles; no SUPERUSER attribute")

	const role = "dbos_noddl_role"
	const rolePw = "noddl_pw"

	// newAdmin connects as the (superuser) test role and (re)creates a fresh
	// login role that holds no DDL privileges: no CREATE on the database (so it
	// cannot CREATE SCHEMA) and no CREATE on any schema (so it cannot CREATE TABLE).
	newAdmin := func(t *testing.T) *pgxpool.Pool {
		t.Helper()
		admin, err := pgxpool.New(context.Background(), getDatabaseURL())
		require.NoError(t, err)
		t.Cleanup(admin.Close)
		bg := context.Background()
		// Fresh role: revoke anything previously granted to it, then recreate.
		_, _ = admin.Exec(bg, "DROP OWNED BY "+role)
		_, _ = admin.Exec(bg, "DROP ROLE IF EXISTS "+role)
		_, err = admin.Exec(bg, fmt.Sprintf(
			"CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOCREATEDB NOCREATEROLE", role, rolePw))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = admin.Exec(context.Background(), "DROP OWNED BY "+role)
			_, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role)
		})
		return admin
	}

	// openLimited builds a pgx pool that authenticates as the no-DDL role.
	openLimited := func(t *testing.T) *pgxpool.Pool {
		t.Helper()
		cfg, err := pgxpool.ParseConfig(getDatabaseURL())
		require.NoError(t, err)
		cfg.ConnConfig.User = role
		cfg.ConnConfig.Password = rolePw
		limited, err := pgxpool.NewWithConfig(context.Background(), cfg)
		require.NoError(t, err)
		t.Cleanup(limited.Close)
		return limited
	}

	// A role without CREATE on the database/schema cannot create the missing
	// completion table; NewDataSource must fail fast with the actionable error
	// (the permission error is non-retryable, so this does not hang).
	t.Run("ActionableErrorWhenCannotCreate", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		admin := newAdmin(t)
		const schema = "dbos_noddl_absent"
		_, err := admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
		require.NoError(t, err)
		limited := openLimited(t)

		_, err = NewDataSource(ctx, limited, WithDataSourceSchema(schema))
		require.Error(t, err)
		msg := err.Error()
		// The failure is reported against the right table...
		require.Contains(t, msg, fmt.Sprintf(`"%s".transaction_completion`, schema))
		require.Contains(t, msg, "could not be created")
		// ...is genuinely a privilege denial (not some unrelated DDL error)...
		require.Contains(t, msg, "permission denied")
		// ...and points the user at provisioning it via their migrations.
		require.Contains(t, msg, "migrations")
	})

	// A pre-provisioned table plus DML-only grants is enough: NewDataSource's
	// pre-check finds the table and attempts no DDL, and the role runs a
	// transaction end to end (app write + durability row, both via the user pool).
	t.Run("WorksWhenPreProvisionedWithDMLOnly", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		admin := newAdmin(t)
		limited := openLimited(t)
		bg := context.Background()
		const schema = "dbos_noddl_present"

		mustExec := func(q string) { _, e := admin.Exec(bg, q); require.NoError(t, e) }
		mustExec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
		mustExec(fmt.Sprintf(`CREATE SCHEMA %q`, schema))
		mustExec(fmt.Sprintf(`CREATE TABLE %q.transaction_completion (
			workflow_id TEXT NOT NULL, step_id INT NOT NULL, output TEXT, error TEXT,
			serialization TEXT, created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
			PRIMARY KEY (workflow_id, step_id))`, schema))
		mustExec(fmt.Sprintf(`CREATE TABLE %q.kv (k TEXT PRIMARY KEY, v TEXT)`, schema))
		mustExec(fmt.Sprintf(`GRANT USAGE ON SCHEMA %q TO %s`, schema, role))
		mustExec(fmt.Sprintf(`GRANT SELECT, INSERT ON %q.transaction_completion TO %s`, schema, role))
		mustExec(fmt.Sprintf(`GRANT SELECT, INSERT, UPDATE ON %q.kv TO %s`, schema, role))
		t.Cleanup(func() {
			_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
		})

		// The pre-check finds the pre-provisioned table, so no DDL is attempted.
		ds, err := NewDataSource(ctx, limited, WithDataSourceSchema(schema))
		require.NoError(t, err)
		require.NotNil(t, ds)

		wf := func(dctx DBOSContext, item string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				_, e := tx.Exec(c, fmt.Sprintf(`INSERT INTO %q.kv (k, v) VALUES ($1, $2)`, schema), "k1", item)
				return 7, e
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))

		h, err := RunWorkflow(ctx, wf, "hello", WithWorkflowID(uuid.NewString()))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, int64(7), res)

		// The DML-only role wrote both the application row and the durability row.
		var v string
		require.NoError(t, limited.QueryRow(bg,
			fmt.Sprintf(`SELECT v FROM %q.kv WHERE k = 'k1'`, schema)).Scan(&v))
		require.Equal(t, "hello", v)
		var n int
		require.NoError(t, limited.QueryRow(bg,
			fmt.Sprintf(`SELECT count(*) FROM %q.transaction_completion`, schema)).Scan(&n))
		require.Equal(t, 1, n)
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

	t.Run("CancelledTransactionNotCheckpointed", func(t *testing.T) {
		// A transaction interrupted by workflow cancellation must not checkpoint
		// its cancellation error — in the user DB or the system DB — so resume
		// re-executes it instead of replaying the error.
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		var attempts atomic.Int32
		started := NewEvent()
		wf := func(dctx DBOSContext, _ string) (string, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (string, error) {
				if attempts.Add(1) == 1 {
					started.Set()
					<-c.Done()
					return "", c.Err()
				}
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "v1"); err != nil {
					return "", err
				}
				return "completed", nil
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

		cancelCtx, cancelFunc := WithCancel(ctx)
		defer cancelFunc()
		wfID := uuid.NewString()
		handle, err := RunWorkflow(cancelCtx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)

		started.Wait()
		cancelFunc()

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		require.True(t, errors.Is(err, &DBOSError{Code: WorkflowCancelled}), "expected WorkflowCancelled error, got: %v", err)

		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err)
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "workflow did not reach cancelled status in time")

		// Neither database recorded the interrupted transaction.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 0, "transaction interrupted by cancellation must not be checkpointed in the system DB")
		require.Equal(t, 0, ub.countRows(t,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE workflow_id = $1`, ub.completionTable()), wfID),
			"transaction interrupted by cancellation must not be recorded in the user DB")

		resumedHandle, err := ResumeWorkflow[string](ctx, wfID)
		require.NoError(t, err)
		res, err := resumedHandle.GetResult()
		require.NoError(t, err, "resumed workflow should complete successfully")
		require.Equal(t, "completed", res)
		require.EqualValues(t, 2, attempts.Load(), "expected the transaction to re-execute on resume")

		require.Equal(t, "v1", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k1'`))
		require.Equal(t, 1, ub.countRows(t,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE workflow_id = $1`, ub.completionTable()), wfID))
		steps, err = GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1, "expected the re-executed transaction to be checkpointed")
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
		sys := ctx.(*dbosContext).systemDB.(*sysdb.SysDB)
		delQ := sys.Dialect().RewriteQuery(fmt.Sprintf(
			`DELETE FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
			sys.Dialect().SchemaPrefix(sys.Schema())))
		_, err = sys.Pool().Exec(context.Background(), delQ, wfID, 0)
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

	// Transactions and plain steps draw from the same per-workflow step counter.
	// Interleaving them (txn, step, txn, step) must assign sequential IDs 0..3 in
	// call order, with the transactions landing at 0 and 2.
	t.Run("InterleavedStepIDs", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		insert := func(k string) Txn[int64] {
			return func(c context.Context, tx Tx) (int64, error) {
				_, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), k, "v")
				return 0, err
			}
		}
		wf := func(dctx DBOSContext, _ string) (string, error) {
			if _, err := RunAsTransaction(dctx, ds, insert("a")); err != nil { // step 0
				return "", err
			}
			if _, err := RunAsStep(dctx, func(context.Context) (string, error) { return "s1", nil }); err != nil { // step 1
				return "", err
			}
			if _, err := RunAsTransaction(dctx, ds, insert("b")); err != nil { // step 2
				return "", err
			}
			if _, err := RunAsStep(dctx, func(context.Context) (string, error) { return "s2", nil }); err != nil { // step 3
				return "", err
			}
			return "done", nil
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

		wfID := uuid.NewString()
		handle, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, "done", res)

		// All four operations share one counter: sequential IDs 0..3 in order.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		ids := make([]int, len(steps))
		for i, s := range steps {
			ids[i] = s.StepID
		}
		require.Equal(t, []int{0, 1, 2, 3}, ids)

		// Only the two transactions wrote durability rows, at positions 0 and 2.
		require.Equal(t, 2, ub.countRows(t, fmt.Sprintf(`SELECT count(*) FROM %s`, ub.completionTable())))
		require.Equal(t, 1, ub.countRows(t,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE step_id = 0`, ub.completionTable())))
		require.Equal(t, 1, ub.countRows(t,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE step_id = 2`, ub.completionTable())))
	})

	// One workflow exercises both ways a RunAsTransaction can be nested:
	//   - inside a RunAsStep (allowed): the enclosing step holds no transaction,
	//     so the within-step transaction is the only writer; it commits its
	//     application write but records no durability row or step of its own.
	//   - inside another RunAsTransaction (rejected): the inner would open a
	//     second connection on the same database — deadlocking SQLite's single
	//     writer, and committing independently of the outer elsewhere — so
	//     RunAsTransaction returns an error instead. The outer reports it and
	//     still commits its own write.
	// The workflow therefore has two durable steps (the RunAsStep and the
	// top-level transaction) and the user database holds a single completion row
	// (the top-level transaction's).
	t.Run("Nesting", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")

		insertTxn := func(dctx DBOSContext, key string) (string, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (string, error) {
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), key, "inner"); err != nil {
					return "", err
				}
				return "inner-ok", nil
			})
		}
		wf := func(dctx DBOSContext, _ string) (string, error) {
			// Durable step 0: a RunAsStep wrapping a within-step transaction (allowed).
			if _, err := RunAsStep(dctx, func(c context.Context) (string, error) {
				return insertTxn(c.(DBOSContext), "k_step")
			}); err != nil {
				return "", err
			}
			// Durable step 1: a top-level transaction whose fn nests another
			// transaction. The nested call is rejected; the outer reports the
			// error but still commits its own write.
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (string, error) {
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k_outer", "outer"); err != nil {
					return "", err
				}
				if _, nerr := insertTxn(c.(DBOSContext), "k_inner"); nerr != nil {
					return nerr.Error(), nil
				}
				return "unexpected", nil
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Contains(t, res, "cannot call RunAsTransaction within a transaction")

		// The RunAsStep-nested transaction and the top-level transaction committed;
		// the transaction-nested one was rejected before it could write.
		require.Equal(t, "inner", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k_step'`))
		require.Equal(t, "outer", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k_outer'`))
		require.Equal(t, 0, ub.countRows(t, `SELECT count(*) FROM kv WHERE k = 'k_inner'`))

		// Two durable steps (RunAsStep + top-level transaction); the within-step
		// transaction recorded none, and the rejected nested call consumed no step ID.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 2)

		// Only the top-level transaction wrote a completion row in the user DB.
		require.Equal(t, 1, ub.countRows(t, fmt.Sprintf(`SELECT count(*) FROM %s`, ub.completionTable())))
	})

	// A transaction whose fn errors rolls back its application write in every
	// nesting position and on both data source flavors, but durability differs:
	//   - within a RunAsStep: the enclosing step owns durability, so the
	//     transaction records no row of its own in either table.
	//   - top-level on a data source sharing the system-DB pool: the savepoint
	//     discards the write and only the error checkpoint commits to
	//     operation_outputs (shared sources have no completion table).
	//   - top-level on a separate data source: the failure is recorded in BOTH
	//     durability tables — the user DB transaction_completion (error set,
	//     output null) and the system DB operation_outputs.
	// Uses setupSharedDBOS (unlike the sibling subtests) because a shared-pool
	// data source can only exist when the system pool is user-provided.
	t.Run("RollsBackOnError", func(t *testing.T) {
		ctx, sharedDS, sharedUB := setupSharedDBOS(t)
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")
		require.False(t, ds.sameAsSystemDB)
		require.True(t, sharedDS.sameAsSystemDB)

		const wantErr = "permanent app failure"
		wf := func(dctx DBOSContext, _ string) (string, error) {
			// Step 0: a within-step transaction that errors. It rolls back and,
			// because the enclosing step owns durability, records nothing itself.
			if _, err := RunAsStep(dctx, func(c context.Context) (string, error) {
				_, terr := RunAsTransaction(c.(DBOSContext), ds, func(c context.Context, tx Tx) (string, error) {
					if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k_step", "rollback"); err != nil {
						return "", err
					}
					return "", errors.New("within-step boom")
				})
				if terr == nil {
					return "", errors.New("expected within-step transaction to fail")
				}
				return "handled", nil
			}); err != nil {
				return "", err
			}
			// Step 1: a top-level transaction on the shared-pool data source that
			// errors. The savepoint discards its write; only the error checkpoint
			// commits.
			if _, err := RunAsTransaction(dctx, sharedDS, func(c context.Context, tx Tx) (string, error) {
				if _, err := tx.Exec(c, sharedUB.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k_shared", "rollback"); err != nil {
					return "", err
				}
				return "", errors.New("shared boom")
			}); err == nil {
				return "", errors.New("expected shared transaction to fail")
			}
			// Step 2: a top-level transaction on the separate data source that
			// errors. It rolls back too, but the failure is recorded in both
			// durability tables.
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (string, error) {
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k_top", "rollback"); err != nil {
					return "", err
				}
				return "", errors.New(wantErr)
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		// Create the kv table on both backends: one physical table on Postgres
		// (both pools share the database), one per file on SQLite. Distinct keys
		// keep the writes tellable apart either way.
		ub.createAppTable(t)
		sharedUB.createAppTable(t)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = h.GetResult()
		require.ErrorContains(t, err, wantErr)

		// All three transactions rolled back their application writes.
		require.Equal(t, 0, ub.countRows(t, `SELECT count(*) FROM kv`))
		require.Equal(t, 0, sharedUB.countRows(t, `SELECT count(*) FROM kv`))

		// User DB: only the separate top-level transaction wrote a row — a failure
		// row at step 2 (the others recorded nothing): error set, output null.
		require.Equal(t, 1, ub.countRows(t, fmt.Sprintf(`SELECT count(*) FROM %s`, ub.completionTable())))
		output, errStr := ub.completionCells(t, wfID, 2)
		require.Nil(t, output)
		require.NotNil(t, errStr)
		require.Contains(t, *errStr, wantErr)

		// System DB: the RunAsStep (step 0) succeeded; both top-level transactions
		// are checkpointed with their errors.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 3)
		require.Nil(t, steps[0].Error)
		require.NotNil(t, steps[1].Error)
		require.Contains(t, steps[1].Error.Error(), "shared boom")
		require.NotNil(t, steps[2].Error)
		require.Contains(t, steps[2].Error.Error(), wantErr)
	})

	// RunAsTransaction must be called from within a workflow: invoked at top level
	// (no workflow state in the context) the separate-DB path reaches
	// prepareStepExecution and returns an error instead of opening a transaction.
	t.Run("OutsideWorkflowReturnsError", func(t *testing.T) {
		ctx := setupDBOS(t, setupDBOSOptions{dropDB: true})
		ub := openUserBackend(t)
		ds := ub.register(t, ctx, "app")
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)
		require.False(t, ds.sameAsSystemDB)

		_, err := RunAsTransaction(ctx, ds, func(c context.Context, tx Tx) (string, error) {
			_, e := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "v1")
			return "unexpected", e
		})
		require.ErrorContains(t, err, "workflow state not found in context")

		// fn never ran, so nothing was written.
		require.Equal(t, 0, ub.countRows(t, `SELECT count(*) FROM kv`))
	})
}

// setupSharedDBOS builds a DBOS context whose system-database pool is ALSO the
// engine handed to RegisterDataSource, so the data source and the system DB
// share one pool. This is the single-transaction path: no transaction_completion
// table, application writes and the operation_outputs checkpoint commit together.
// Returns the context, the registered data source, and a userBackend over the
// shared pool for assertions. The context is unlaunched (caller registers
// workflows then calls Launch).
func setupSharedDBOS(t *testing.T) (DBOSContext, *DataSource, *userBackend) {
	t.Helper()
	var (
		config Config
		ub     *userBackend
	)
	if useSqliteBackend() {
		// Open the shared handle with DBOS's recommended pragmas (busy_timeout,
		// WAL, immediate txlock) so the data source's DDL/writes coexist with the
		// system DB's background loops on one *sql.DB without SQLITE_BUSY.
		path := filepath.Join(t.TempDir(), "shared.db")
		db, err := sysdb.OpenSQLitePool(context.Background(), "sqlite:"+path)
		require.NoError(t, err)
		config = Config{AppName: "test-app", SqliteSystemDB: db}
		ub = &userBackend{pool: sysdb.NewSQLPool(db), dialect: sysdb.SqliteDialect{}, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
	} else {
		url := getDatabaseURL()
		resetTestDatabase(t, url)
		cfg, err := pgxpool.ParseConfig(url)
		require.NoError(t, err)
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		require.NoError(t, err)
		config = Config{AppName: "test-app", SystemDBPool: pool}
		ub = &userBackend{pool: sysdb.NewPgxPool(pool), dialect: sysdb.PostgresDialect{}, schema: _DEFAULT_SYSTEM_DB_SCHEMA}
	}

	ctx, err := NewDBOSContext(context.Background(), config)
	require.NoError(t, err)
	// Shutdown owns the shared pool (sysDB.shutdown closes it); don't close it
	// separately here.
	t.Cleanup(func() { Shutdown(ctx, 30*time.Second) })

	// A leftover completion table from a prior two-table test (reset clears rows,
	// not tables) would mask the optimization. Start clean.
	ub.dropCompletionTable(t)

	ds := ub.register(t, ctx, "app")
	return ctx, ds, ub
}

// RunAsTransaction on a data source that shares the system database's pool: the
// same-database optimization collapses onto the single-transaction path. Each
// subtest gets its own shared-pool DBOS context via setupSharedDBOS.
func TestRunAsTransactionSharedSystemDB(t *testing.T) {
	// The base case: RunAsTransaction collapses onto the single-transaction path —
	// no transaction_completion table is created, and the application write commits
	// atomically with the operation_outputs checkpoint (recovery then replays from
	// layer 1 alone).
	t.Run("HappyPath", func(t *testing.T) {
		ctx, ds, ub := setupSharedDBOS(t)

		var runs atomic.Int32
		wf := func(dctx DBOSContext, item string) (int64, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (int64, error) {
				runs.Add(1)
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", item); err != nil {
					return 0, err
				}
				return 99, nil
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)

		// The optimization is detected at NewDataSource and skips the durability table.
		require.True(t, ds.sameAsSystemDB)
		require.False(t, ub.completionTableExists(t))

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "hello", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, int64(99), res)
		require.Equal(t, int32(1), runs.Load())

		// Application write committed, and the step checkpointed to operation_outputs
		// in the same transaction.
		require.Equal(t, "hello", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k1'`))
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		require.Equal(t, 0, steps[0].StepID)

		// Recovery replays from operation_outputs (layer 1) without re-running fn.
		setWorkflowStatusPending(t, ctx, wfID)
		handles, err := recoverPendingWorkflows(ctx.(*dbosContext), []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		rres, err := handles[0].GetResult()
		require.NoError(t, err)
		require.EqualValues(t, 99, rres)
		require.Equal(t, int32(1), runs.Load())
		require.Equal(t, 1, ub.countRows(t, `SELECT count(*) FROM kv`))
	})

	// When a shared-pool data source's RunAsTransaction is invoked inside an
	// enclosing step, the call routes through runAsTxn's within-step path: it must
	// still give the user fn a real transaction (on the shared pool) and manage
	// commit/rollback, while recording no durability row of its own. Runs on every
	// backend — the enclosing step holds no transaction, so the within-step
	// transaction is the only writer (no SQLite single-writer contention).
	t.Run("WithinStep", func(t *testing.T) {
		ctx, ds, ub := setupSharedDBOS(t)

		var txRuns atomic.Int32
		wf := func(dctx DBOSContext, _ string) (string, error) {
			return RunAsStep(dctx, func(c context.Context) (string, error) {
				return RunAsTransaction(c.(DBOSContext), ds, func(c context.Context, tx Tx) (string, error) {
					txRuns.Add(1)
					if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "inner"); err != nil {
						return "", err
					}
					return "ok", nil
				})
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)
		require.True(t, ds.sameAsSystemDB)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Equal(t, "ok", res)
		require.Equal(t, int32(1), txRuns.Load())

		// The within-step transaction committed the application write on the shared pool.
		require.Equal(t, "inner", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k1'`))

		// Only the enclosing RunAsStep is durable; the within-step transaction recorded none.
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
	})

	// A transaction nested inside another transaction is rejected on the shared-pool
	// path too. The guard runs before the same-database optimization, so the inner
	// never reaches runAsTxn to open a second connection on the shared pool (which
	// would deadlock SQLite's single writer). Runs on every backend.
	t.Run("RejectsNested", func(t *testing.T) {
		ctx, ds, ub := setupSharedDBOS(t)

		wf := func(dctx DBOSContext, _ string) (string, error) {
			return RunAsTransaction(dctx, ds, func(c context.Context, tx Tx) (string, error) {
				if _, err := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k_outer", "outer"); err != nil {
					return "", err
				}
				_, nerr := RunAsTransaction(c.(DBOSContext), ds, func(c context.Context, tx Tx) (string, error) {
					_, e := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k_inner", "inner")
					return "", e
				})
				if nerr != nil {
					return nerr.Error(), nil
				}
				return "unexpected", nil
			})
		}
		RegisterWorkflow(ctx, wf)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)
		require.True(t, ds.sameAsSystemDB)

		wfID := uuid.NewString()
		h, err := RunWorkflow(ctx, wf, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		res, err := h.GetResult()
		require.NoError(t, err)
		require.Contains(t, res, "cannot call RunAsTransaction within a transaction")

		// The outer transaction committed; the rejected inner never wrote.
		require.Equal(t, "outer", ub.queryString(t, `SELECT v FROM kv WHERE k = 'k_outer'`))
		require.Equal(t, 0, ub.countRows(t, `SELECT count(*) FROM kv WHERE k = 'k_inner'`))

		// Just the one durable step (the outer transaction).
		steps, err := GetWorkflowSteps(ctx, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
	})

	// On the shared-pool path too, RunAsTransaction must be called from within a
	// workflow. The same-database optimization routes to runAsTxn, which reaches
	// prepareStepExecution and returns the same error when no workflow state is in
	// the context. Runs on every backend.
	t.Run("OutsideWorkflow", func(t *testing.T) {
		ctx, ds, ub := setupSharedDBOS(t)
		require.NoError(t, Launch(ctx))
		ub.createAppTable(t)
		require.True(t, ds.sameAsSystemDB)

		_, err := RunAsTransaction(ctx, ds, func(c context.Context, tx Tx) (string, error) {
			_, e := tx.Exec(c, ub.rw(`INSERT INTO kv (k, v) VALUES ($1, $2)`), "k1", "v1")
			return "unexpected", e
		})
		require.ErrorContains(t, err, "workflow state not found in context")

		// fn never ran, so nothing was written.
		require.Equal(t, 0, ub.countRows(t, `SELECT count(*) FROM kv`))
	})
}

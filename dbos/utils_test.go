package dbos

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func getDatabaseURL() string {
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	return databaseURL
}

func useSqliteBackend() bool {
	return os.Getenv("DBOS_TEST_BACKEND") == "sqlite"
}

func skipIfSqlite(t *testing.T, reason string) {
	t.Helper()
	if useSqliteBackend() {
		t.Skipf("skipping on sqlite backend: %s", reason)
	}
}

func skipIfCockroach(t *testing.T, reason string) {
	t.Helper()
	if useSqliteBackend() {
		return // sqlite is never CRDB
	}
	conn, err := pgx.Connect(context.Background(), getDatabaseURL())
	require.NoError(t, err)
	defer conn.Close(context.Background())
	if sysdb.IsCockroachDB(conn) {
		t.Skipf("skipping on CockroachDB: %s", reason)
	}
}

var testDBURLs sync.Map // *testing.T -> string; ensures setupDBOS and follow-up callers (e.g. NewClient) share the same sqlite file.

func backendDatabaseURL(t *testing.T) string {
	t.Helper()
	if v, ok := testDBURLs.Load(t); ok {
		return v.(string)
	}
	var url string
	if useSqliteBackend() {
		url = "sqlite:" + filepath.Join(t.TempDir(), "dbos.db")
	} else {
		url = getDatabaseURL()
	}
	testDBURLs.Store(t, url)
	t.Cleanup(func() { testDBURLs.Delete(t) })
	return url
}

/*
	Test database reset.

Deletes rows from dbos-managed tables instead of dropping the database, which
is much cheaper — especially on CockroachDB where every DDL is an async
schema-change job. Falls back to a real drop when any dbos schema is not at
the latest migration version (e.g. after a schema-mutating migration test or
a branch switch).
*/
func resetTestDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	if useSqliteBackend() {
		return
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		// Database likely does not exist yet; NewDBOSContext will create it.
		return
	}
	defer conn.Close(ctx)

	if !cleanDatabaseRows(ctx, conn) {
		conn.Close(ctx)
		dropTestDatabase(t, databaseURL)
	}
}

// cleanDatabaseRows empties every dbos-managed schema in the connected
// database. Returns false when the database cannot be safely reused and must
// be dropped instead.
func cleanDatabaseRows(ctx context.Context, conn *pgx.Conn) bool {
	migrations := sysdb.BuildMigrations(_DEFAULT_SYSTEM_DB_SCHEMA, sysdb.IsCockroachDB(conn))
	latestVersion := migrations[len(migrations)-1].Version

	rows, err := conn.Query(ctx,
		`SELECT DISTINCT table_schema FROM information_schema.tables WHERE table_name IN ('workflow_status', $1)`,
		sysdb.MigrationTable)
	if err != nil {
		return false
	}
	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			rows.Close()
			return false
		}
		schemas = append(schemas, schema)
	}
	rows.Close()
	if rows.Err() != nil {
		return false
	}

	for _, schema := range schemas {
		var version int64
		q := fmt.Sprintf("SELECT version FROM %s.%s LIMIT 1", pgx.Identifier{schema}.Sanitize(), sysdb.MigrationTable)
		if err := conn.QueryRow(ctx, q).Scan(&version); err != nil || version != latestVersion {
			return false
		}
	}

	for _, schema := range schemas {
		sanitizedSchema := pgx.Identifier{schema}.Sanitize()
		// workflow_status first: its ON DELETE CASCADE empties the FK children.
		if _, err := conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s.workflow_status", sanitizedSchema)); err != nil {
			return false
		}
		tableRows, err := conn.Query(ctx,
			`SELECT table_name FROM information_schema.tables
			 WHERE table_schema = $1 AND table_type = 'BASE TABLE' AND table_name NOT IN ('workflow_status', $2)`,
			schema, sysdb.MigrationTable)
		if err != nil {
			return false
		}
		var tables []string
		for tableRows.Next() {
			var table string
			if err := tableRows.Scan(&table); err != nil {
				tableRows.Close()
				return false
			}
			tables = append(tables, table)
		}
		tableRows.Close()
		if tableRows.Err() != nil {
			return false
		}
		for _, table := range tables {
			q := fmt.Sprintf("DELETE FROM %s.%s", sanitizedSchema, pgx.Identifier{table}.Sanitize())
			if _, err := conn.Exec(ctx, q); err != nil {
				return false
			}
		}
	}
	return true
}

// dropTestDatabase drops the test database entirely.
func dropTestDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	parsedURL, err := pgx.ParseConfig(databaseURL)
	require.NoError(t, err)

	dbName := parsedURL.Database
	if dbName == "" {
		t.Skip("DBOS_SYSTEM_DATABASE_URL does not specify a database name, skipping integration test")
	}

	postgresURL := parsedURL.Copy()
	postgresURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), postgresURL)
	require.NoError(t, err)
	defer conn.Close(context.Background())

	err = sysdb.DropDatabaseIfExists(context.Background(), conn, dbName)
	require.NoError(t, err)
}

type setupDBOSOptions struct {
	dropDB                   bool
	checkLeaks               bool
	serializer               Serializer[any]
	schedulerPollingInterval time.Duration
	databaseURL              string // share another test's database (sqlite URLs are per-*testing.T otherwise)
}

/* Test database setup */
func setupDBOS(t *testing.T, opts setupDBOSOptions) DBOSContext {
	t.Helper()

	databaseURL := opts.databaseURL
	if databaseURL == "" {
		if opts.dropDB && useSqliteBackend() {
			testDBURLs.Delete(t)
		}
		databaseURL = backendDatabaseURL(t)
		if opts.dropDB && !useSqliteBackend() {
			resetTestDatabase(t, databaseURL)
		}
	}

	config := Config{
		DatabaseURL:              databaseURL,
		AppName:                  "test-app",
		Serializer:               opts.serializer,
		SchedulerPollingInterval: opts.schedulerPollingInterval,
	}

	dbosCtx, err := NewDBOSContext(context.Background(), config)
	require.NoError(t, err)
	require.NotNil(t, dbosCtx)

	// Register cleanup to run after test completes
	t.Cleanup(func() {
		dbosCtx.(*dbosContext).logger.Info("Cleaning up DBOS instance...")
		if dbosCtx != nil {
			Shutdown(dbosCtx, 30*time.Second) // Wait for workflows to finish and shutdown admin server and system database
		}
		dbosCtx = nil
		if opts.checkLeaks {
			goleak.VerifyNone(t,
				// Ignore pgx health checks
				// https://github.com/jackc/pgx/blob/15bca4a4e14e0049777c1245dba4c16300fe4fd0/pgxpool/pool.go#L417
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck"),
				goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),
				// database/sql's connectionOpener/connectionCleaner exit after
				// Close but slightly after goleak's check fires. Ignored under
				// the sqlite backend.
				goleak.IgnoreAnyFunction("database/sql.(*DB).connectionOpener"),
				goleak.IgnoreAnyFunction("database/sql.(*DB).connectionCleaner"),
			)
		}
	})

	return dbosCtx
}

/* Event struct provides a simple synchronization primitive that can be used to signal between goroutines. */
type Event struct {
	mu    sync.Mutex
	cond  *sync.Cond
	IsSet bool
}

func NewEvent() *Event {
	e := &Event{}
	e.cond = sync.NewCond(&e.mu)
	return e
}

func (e *Event) Wait() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for !e.IsSet {
		e.cond.Wait()
	}
}

func (e *Event) Set() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = true
	e.cond.Broadcast()
}

func (e *Event) Clear() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.IsSet = false
}

// setWorkflowStatusPending sets the workflow's status to PENDING in the DB (clearing output, error, started_at_epoch_ms).
func setWorkflowStatusPending(t *testing.T, dbosCtx DBOSContext, workflowID string) {
	t.Helper()
	c, ok := dbosCtx.(*dbosContext)
	require.True(t, ok, "expected DBOSContext to be *dbosContext")
	sysDB, ok := c.systemDB.(*sysdb.SysDB)
	require.True(t, ok, "expected systemDB to be *sysDB")
	updateQuery := sysDB.Dialect().RewriteQuery(fmt.Sprintf(`UPDATE %sworkflow_status
		SET status = $1, output = NULL, error = NULL, started_at_epoch_ms = NULL, updated_at = $2
		WHERE workflow_uuid = $3`, sysDB.Dialect().SchemaPrefix(sysDB.Schema())))
	_, err := sysDB.Pool().Exec(context.Background(), updateQuery,
		WorkflowStatusPending, time.Now().UnixMilli(), workflowID)
	require.NoError(t, err, "failed to set workflow status to PENDING")
}

func queueEntriesAreCleanedUp(ctx DBOSContext) bool {
	maxTries := 10
	success := false
	exec, ok := ctx.(*dbosContext)
	if !ok {
		fmt.Println("Expected ctx to be of type *dbosContext in queueEntriesAreCleanedUp")
		return false
	}
	sdb := exec.systemDB.(*sysdb.SysDB)
	for range maxTries {
		tx, err := sdb.Pool().BeginTx(ctx, TxOptions{})
		if err != nil {
			return false
		}

		query := sdb.Dialect().RewriteQuery(fmt.Sprintf(`SELECT COUNT(*)
				  FROM %sworkflow_status
				  WHERE queue_name IS NOT NULL
					AND queue_name != $1
					AND status IN ('ENQUEUED', 'PENDING')`, sdb.Dialect().SchemaPrefix(sdb.Schema())))

		var count int
		err = tx.QueryRow(ctx, query, models.InternalQueueName).Scan(&count)
		tx.Rollback(ctx)

		if err != nil {
			return false
		}

		if count == 0 {
			success = true
			break
		}

		time.Sleep(1 * time.Second)
	}
	return success
}

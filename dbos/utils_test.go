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

/* Test database reset */
func resetTestDatabase(t *testing.T, databaseURL string) {
	t.Helper()

	if useSqliteBackend() {
		return
	}

	// Clean up the test database
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

	err = dropDatabaseIfExists(context.Background(), conn, dbName)
	require.NoError(t, err)
}

type setupDBOSOptions struct {
	dropDB                   bool
	checkLeaks               bool
	serializer               Serializer[any]
	schedulerPollingInterval time.Duration
}

/* Test database setup */
func setupDBOS(t *testing.T, opts setupDBOSOptions) DBOSContext {
	t.Helper()

	if opts.dropDB && useSqliteBackend() {
		testDBURLs.Delete(t)
	}
	databaseURL := backendDatabaseURL(t)
	if opts.dropDB && !useSqliteBackend() {
		resetTestDatabase(t, databaseURL)
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
	sysDB, ok := c.systemDB.(*sysDB)
	require.True(t, ok, "expected systemDB to be *sysDB")
	updateQuery := sysDB.dialect.RewriteQuery(fmt.Sprintf(`UPDATE %sworkflow_status
		SET status = $1, output = NULL, error = NULL, started_at_epoch_ms = NULL, updated_at = $2
		WHERE workflow_uuid = $3`, sysDB.dialect.SchemaPrefix(sysDB.schema)))
	_, err := sysDB.pool.Exec(context.Background(), updateQuery,
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
	sdb := exec.systemDB.(*sysDB)
	for range maxTries {
		tx, err := sdb.pool.BeginTx(ctx, TxOptions{})
		if err != nil {
			return false
		}

		query := sdb.dialect.RewriteQuery(fmt.Sprintf(`SELECT COUNT(*)
				  FROM %sworkflow_status
				  WHERE queue_name IS NOT NULL
					AND queue_name != $1
					AND status IN ('ENQUEUED', 'PENDING')`, sdb.dialect.SchemaPrefix(sdb.schema)))

		var count int
		err = tx.QueryRow(ctx, query, _DBOS_INTERNAL_QUEUE_NAME).Scan(&count)
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

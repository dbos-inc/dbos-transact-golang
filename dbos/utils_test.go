package dbos

import (
	"context"
	"fmt"
	"net/url"
	"os"
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

/* Test database reset */
func resetTestDatabase(t *testing.T, databaseURL string) {
	t.Helper()

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
	dropDB          bool
	checkLeaks      bool
	useListenNotify *bool
}

/* Test database setup */
func setupDBOS(t *testing.T, opts setupDBOSOptions) DBOSContext {
	t.Helper()

	databaseURL := getDatabaseURL()
	if opts.dropDB {
		resetTestDatabase(t, databaseURL)
	}

	config := Config{
		DatabaseURL: databaseURL,
		AppName:     "test-app",
	}
	if opts.useListenNotify != nil {
		config.UseListenNotify = opts.useListenNotify
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

/* Helpers */

func queueEntriesAreCleanedUp(ctx DBOSContext) bool {
	maxTries := 10
	success := false
	for range maxTries {
		// Begin transaction
		exec, ok := ctx.(*dbosContext)
		if !ok {
			fmt.Println("Expected ctx to be of type *dbosContext in queueEntriesAreCleanedUp")
			return false
		}
		tx, err := exec.systemDB.(*sysDB).pool.Begin(ctx)
		if err != nil {
			return false
		}

		query := fmt.Sprintf(`SELECT COUNT(*)
				  FROM %s.workflow_status
				  WHERE queue_name IS NOT NULL
					AND queue_name != $1
					AND status IN ('ENQUEUED', 'PENDING')`, pgx.Identifier{exec.systemDB.(*sysDB).schema}.Sanitize())

		var count int
		err = tx.QueryRow(ctx, query, _DBOS_INTERNAL_QUEUE_NAME).Scan(&count)
		tx.Rollback(ctx) // Clean up transaction

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

func checkWfStatus(ctx DBOSContext, expectedStatus WorkflowStatusType) (bool, error) {
	wfid, err := GetWorkflowID(ctx)
	if err != nil {
		return false, err
	}
	me, err := RetrieveWorkflow[string](ctx, wfid)
	if err != nil {
		return false, err
	}
	meStatus, err := me.GetStatus()
	if err != nil {
		return false, err
	}
	if meStatus.Status == expectedStatus {
		return true, nil
	}
	return false, nil
}

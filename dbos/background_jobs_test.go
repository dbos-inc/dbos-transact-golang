package dbos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/sysdb"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipUnlessAdvisoryLocks skips a test that needs the background-jobs lock to exclude:
// SQLite and CockroachDB grant every try.
func skipUnlessAdvisoryLocks(t *testing.T) {
	t.Helper()
	skipIfSqlite(t, "every process holds the background-jobs lock")
	skipIfCockroach(t, "every process holds the background-jobs lock")
}

type lockSession struct {
	backendXmin *uint32
	state       string
}

// advisoryLockSessions returns the sessions holding the bigint advisory lock key.
func advisoryLockSessions(t *testing.T, db *sysdb.SysDB, key int64) []lockSession {
	t.Helper()
	rows, err := db.Pool().Query(context.Background(), `SELECT a.backend_xmin::text::bigint, a.state FROM pg_stat_activity a
		JOIN pg_locks l ON l.pid = a.pid WHERE l.locktype = 'advisory' AND l.granted
		AND l.classid::int8 = $1 AND l.objid::int8 = $2 AND l.objsubid = 1`,
		(key>>32)&0xFFFFFFFF, key&0xFFFFFFFF)
	require.NoError(t, err)
	defer rows.Close()
	var sessions []lockSession
	for rows.Next() {
		var xmin *int64
		var state string
		require.NoError(t, rows.Scan(&xmin, &state))
		session := lockSession{state: state}
		if xmin != nil {
			v := uint32(*xmin) // #nosec G115 -- an xid fits 32 bits
			session.backendXmin = &v
		}
		sessions = append(sessions, session)
	}
	require.NoError(t, rows.Err())
	return sessions
}

// simpleProtocolPool fails the test on a transaction's statement in the extended protocol.
type simpleProtocolPool struct {
	sysdb.Pool
	t *testing.T
}

func (p *simpleProtocolPool) BeginTx(ctx context.Context, opts sysdb.TxOptions) (sysdb.Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &simpleProtocolTx{Tx: tx, t: p.t}, nil
}

type simpleProtocolTx struct {
	sysdb.Tx
	t *testing.T
}

func (s *simpleProtocolTx) QueryRow(ctx context.Context, query string, args ...any) sysdb.Row {
	assert.Equal(s.t, []any{pgx.QueryExecModeSimpleProtocol}, args, query)
	return s.Tx.QueryRow(ctx, query, args...)
}

func (s *simpleProtocolTx) Exec(ctx context.Context, query string, args ...any) (sysdb.Result, error) {
	assert.Equal(s.t, []any{pgx.QueryExecModeSimpleProtocol}, args, query)
	return s.Tx.Exec(ctx, query, args...)
}

// A held lock's idle session holds no xmin, even after many renewals, so it never holds
// back vacuum.
func TestBackgroundJobLockSessionHoldsNoSnapshot(t *testing.T) {
	skipUnlessAdvisoryLocks(t)
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	db := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)
	db.SetPool(&simpleProtocolPool{Pool: db.Pool(), t: t})
	key := sysdb.AdvisoryLockKey(fmt.Sprintf("dbos.background_job.%s.test-app.xmin_job", db.Schema()))

	lock, err := db.BackgroundJobLock(context.Background(), "xmin_job")
	require.NoError(t, err)
	require.NotNil(t, lock)
	defer lock.Release()
	for range 8 {
		require.NoError(t, lock.Renew(context.Background()))
	}
	sessions := advisoryLockSessions(t, db, key)
	require.Len(t, sessions, 1)
	assert.Nil(t, sessions[0].backendXmin)
	assert.Equal(t, "idle in transaction", sessions[0].state)
}

// An online migration's CREATE INDEX CONCURRENTLY waits for older snapshots' transactions
// to end; the lock's idle transaction holds none, so a build never waits out a hold.
func TestConcurrentIndexBuildsDoNotWaitOnTheBackgroundJobLock(t *testing.T) {
	skipUnlessAdvisoryLocks(t)
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	db := dbosCtx.(*dbosContext).systemDB.(*sysdb.SysDB)

	conn, err := pgx.Connect(context.Background(), getDatabaseURL())
	require.NoError(t, err)
	defer conn.Close(context.Background())
	for _, statement := range []string{"SET statement_timeout = '10s'", "DROP TABLE IF EXISTS cic_probe", "CREATE TABLE cic_probe (id int)"} {
		_, err := conn.Exec(context.Background(), statement)
		require.NoError(t, err)
	}
	defer conn.Exec(context.Background(), "DROP TABLE IF EXISTS cic_probe")

	lock, err := db.BackgroundJobLock(context.Background(), "cic_job")
	require.NoError(t, err)
	require.NotNil(t, lock)
	defer lock.Release()
	for range 8 {
		require.NoError(t, lock.Renew(context.Background()))
	}
	started := time.Now()
	_, err = conn.Exec(context.Background(), "CREATE INDEX CONCURRENTLY cic_probe_idx ON cic_probe (id)")
	require.NoError(t, err)
	assert.Less(t, time.Since(started), 3*time.Second)
}

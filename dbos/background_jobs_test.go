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

// The background-jobs loop stops on shutdown.
func TestBackgroundJobsStopOnShutdown(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})
	require.NoError(t, Launch(dbosCtx))
	c := dbosCtx.(*dbosContext)
	if db := c.systemDB.(*sysdb.SysDB); db.Dialect().Name() == sysdb.DialectPostgres {
		key := sysdb.AdvisoryLockKey(fmt.Sprintf("dbos.background_job.%s.test-app.background_jobs.%s", db.Schema(), c.applicationVersion))
		require.Eventually(t, func() bool { return len(advisoryLockSessions(t, db, key)) == 1 }, 5*time.Second, 50*time.Millisecond)
	}

	require.NoError(t, Shutdown(dbosCtx, 10*time.Second))
	exited := make(chan struct{})
	go func() {
		c.backgroundJobsWg.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("the background-jobs loop did not exit on shutdown")
	}
}

// Two processes' loops: whichever holds the lock runs the jobs, so their runs start at
// least an interval apart, a failing job skips no other, and when the holder stops the
// other takes over.
func TestBackgroundJobsRunOncePerIntervalAcrossProcesses(t *testing.T) {
	skipUnlessAdvisoryLocks(t)
	local := setupDBOS(t, setupDBOSOptions{dropDB: true})
	peer := setupDBOS(t, setupDBOSOptions{dropDB: false})
	const interval = 200 * time.Millisecond

	type run struct {
		name string
		at   time.Time
	}
	var mu sync.Mutex
	var runs []run
	stops := map[string]context.CancelFunc{}
	done := map[string]chan struct{}{}
	for name, ctx := range map[string]Context{"local": local, "peer": peer} {
		c := ctx.(*dbosContext)
		stop, cancel := context.WithCancel(context.Background())
		stops[name] = cancel
		exited := make(chan struct{})
		done[name] = exited
		jobs := []backgroundJob{
			{name: "failing", run: func() error { return errors.New("injected job failure") }},
			{name: "job", run: func() error {
				mu.Lock()
				defer mu.Unlock()
				runs = append(runs, run{name: name, at: time.Now()})
				return nil
			}},
		}
		go func() {
			defer close(exited)
			runBackgroundJobLoop(stop, c.systemDB, c.logger, "test_jobs", interval, _BACKGROUND_JOBS_HOLD, jobs)
		}()
	}
	t.Cleanup(func() {
		for name, cancel := range stops {
			cancel()
			<-done[name]
		}
	})
	snapshot := func() []run {
		mu.Lock()
		defer mu.Unlock()
		return append([]run(nil), runs...)
	}

	require.Eventually(t, func() bool { return len(snapshot()) >= 8 }, 10*time.Second, 50*time.Millisecond)
	got := snapshot()
	// A holder keeps the lock an interval past its start; the bounds allow for timer slack.
	span := got[len(got)-1].at.Sub(got[0].at)
	assert.GreaterOrEqual(t, span, time.Duration(float64(len(got)-1)*float64(interval)*0.9), "%v", got)
	for i := 1; i < len(got); i++ {
		assert.GreaterOrEqual(t, got[i].at.Sub(got[i-1].at), interval/2, "%v", got)
	}

	holder := got[len(got)-1].name
	other := "peer"
	if holder == "peer" {
		other = "local"
	}
	stops[holder]()
	<-done[holder]
	require.Eventually(t, func() bool {
		latest := snapshot()
		return latest[len(latest)-1].name == other
	}, 10*time.Second, 50*time.Millisecond, "the other process should take the jobs over")
}

// shortLockTimeoutPool shortens the lock statements' idle timeout to 500 ms.
type shortLockTimeoutPool struct{ sysdb.Pool }

func (p *shortLockTimeoutPool) BeginTx(ctx context.Context, opts sysdb.TxOptions) (sysdb.Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &shortLockTimeoutTx{Tx: tx}, nil
}

type shortLockTimeoutTx struct{ sysdb.Tx }

func (*shortLockTimeoutTx) shorten(query string) string {
	if strings.Contains(query, "idle_in_transaction_session_timeout") && !strings.Contains(query, "'5000'") {
		panic("the lock statement's idle timeout is no longer '5000': " + query)
	}
	return strings.ReplaceAll(query, "'5000'", "'500'")
}

func (t *shortLockTimeoutTx) QueryRow(ctx context.Context, query string, args ...any) sysdb.Row {
	return t.Tx.QueryRow(ctx, t.shorten(query), args...)
}

func (t *shortLockTimeoutTx) Exec(ctx context.Context, query string, args ...any) (sysdb.Result, error) {
	return t.Tx.Exec(ctx, t.shorten(query), args...)
}

// A holder keeps its lock session across intervals, renewing it in place past the idle
// timeout, so a peer retrying as fast as it can never finds the lock free mid-hold.
func TestBackgroundJobsHoldOneLockSessionAcrossIntervals(t *testing.T) {
	skipUnlessAdvisoryLocks(t)
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true})
	c := dbosCtx.(*dbosContext)
	// Short, so a hold that went idle instead of renewing would lose its session mid-test.
	db := c.systemDB.(*sysdb.SysDB)
	db.SetPool(&shortLockTimeoutPool{Pool: db.Pool()})

	var mu sync.Mutex
	runs := 0
	stop, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runBackgroundJobLoop(stop, c.systemDB, c.logger, "hold_jobs", 100*time.Millisecond, 3*time.Second,
			[]backgroundJob{{name: "job", run: func() error { mu.Lock(); runs++; mu.Unlock(); return nil }}})
	}()
	t.Cleanup(func() { cancel(); <-done })

	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return runs > 0 }, 5*time.Second, 20*time.Millisecond)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		lock, err := c.systemDB.BackgroundJobLock(context.Background(), "hold_jobs")
		require.NoError(t, err)
		if lock != nil {
			lock.Release()
			t.Fatal("a peer took the lock mid-hold")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, runs, 10)
}

// Each application version takes its own background-jobs lock, so every live version runs
// the jobs, the latest code included.
func TestBackgroundJobsLockIsPerApplicationVersion(t *testing.T) {
	databaseURL := backendDatabaseURL(t)
	if !useSqliteBackend() {
		resetTestDatabase(t, databaseURL)
	}
	var contexts []*dbosContext
	for _, version := range []string{"v1", "v2"} {
		ctx, err := NewContext(context.Background(), Config{DatabaseURL: databaseURL, AppName: "test-app", ApplicationVersion: version})
		require.NoError(t, err)
		t.Cleanup(func() { Shutdown(ctx, 10*time.Second) })
		require.NoError(t, Launch(ctx))
		contexts = append(contexts, ctx.(*dbosContext))
	}
	db := contexts[0].systemDB.(*sysdb.SysDB)
	if db.Dialect().Name() != sysdb.DialectPostgres {
		return // No lock to look for.
	}
	// Each holds the lock named after its version.
	for _, version := range []string{"v1", "v2"} {
		key := sysdb.AdvisoryLockKey(fmt.Sprintf("dbos.background_job.%s.test-app.background_jobs.%s", db.Schema(), version))
		assert.Eventually(t, func() bool { return len(advisoryLockSessions(t, db, key)) == 1 }, 5*time.Second, 50*time.Millisecond, "version %s", version)
	}
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

type scriptedLock struct{ renewErr error }

func (l scriptedLock) Renew(context.Context) error { return l.renewErr }
func (scriptedLock) Release()                      {}

// scriptedLockDB answers each try of the lock with the next entry of tries, then refuses.
type scriptedLockDB struct {
	sysdb.SystemDatabase
	tries []func() (sysdb.HeldLock, error)
}

func (s *scriptedLockDB) BackgroundJobLock(context.Context, string) (sysdb.HeldLock, error) {
	if len(s.tries) == 0 {
		return nil, nil
	}
	try := s.tries[0]
	s.tries = s.tries[1:]
	return try()
}

// The loop keeps trying the lock when a try is refused, and when taking or renewing it fails,
// and runs the jobs only while it holds the lock.
func TestBackgroundJobsKeepTryingTheLockAfterLosingIt(t *testing.T) {
	granted := func() (sysdb.HeldLock, error) { return scriptedLock{}, nil }
	const interval = 20 * time.Millisecond
	for name, tc := range map[string]struct {
		hold  time.Duration
		lossy func() (sysdb.HeldLock, error)
	}{
		"Refused":   {hold: 0, lossy: func() (sysdb.HeldLock, error) { return nil, nil }},
		"TakeFails": {hold: 0, lossy: func() (sysdb.HeldLock, error) { return nil, errors.New("injected take failure") }},
		"RenewFails": {hold: time.Minute, lossy: func() (sysdb.HeldLock, error) {
			return scriptedLock{renewErr: errors.New("injected renew failure")}, nil
		}},
	} {
		t.Run(name, func(t *testing.T) {
			runs := 0
			// The job runs before each try, read by the next one on the loop's goroutine.
			var seen []int
			observe := func(try func() (sysdb.HeldLock, error)) func() (sysdb.HeldLock, error) {
				return func() (sysdb.HeldLock, error) {
					seen = append(seen, runs)
					return try()
				}
			}
			last := make(chan struct{})
			db := &scriptedLockDB{}
			if tc.hold == 0 {
				db.tries = append(db.tries, observe(granted))
			}
			db.tries = append(db.tries, observe(tc.lossy), observe(func() (sysdb.HeldLock, error) {
				close(last)
				return nil, nil
			}))
			jobs := []backgroundJob{{name: "job", run: func() error { runs++; return nil }}}
			stop, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				runBackgroundJobLoop(stop, db, slog.New(slog.DiscardHandler), "scripted_jobs", interval, tc.hold, jobs)
			}()
			select {
			case <-last:
			case <-time.After(5 * time.Second):
				t.Error("the loop stopped trying the lock")
			}
			cancel()
			<-done
			if tc.hold == 0 {
				// One pass for the grant, none for the refused or failed try.
				assert.Equal(t, []int{0, 1, 1}, seen)
			} else {
				// One pass before the renewal fails, none after.
				assert.Equal(t, []int{0, 1}, seen)
			}
		})
	}
}

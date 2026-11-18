package sysdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNotificationLoopCompletionDoesNotRequireShutdownWaiter(t *testing.T) {
	s := &SysDB{
		dialect: SqliteDialect{},
		logger:  slog.New(slog.DiscardHandler),
	}
	var previousDone chan struct{}
	for launch := 0; launch < 2; launch++ {
		ctx, cancel := context.WithCancel(context.Background())
		s.Launch(ctx)
		s.notificationLoopMu.Lock()
		done := s.notificationLoopDone
		s.notificationLoopMu.Unlock()
		if done == previousDone {
			t.Fatal("notification completion channel was reused across launches")
		}
		previousDone = done
		cancel()

		select {
		case _, ok := <-done:
			if ok {
				t.Fatal("notification loop completion channel was sent to instead of closed")
			}
		case <-time.After(time.Second):
			t.Fatal("notification loop did not exit")
		}
	}
}

func TestStreamWakeChannelCleanupPreservesConcurrentReaders(t *testing.T) {
	s := &SysDB{streamNotifier: newNotifyRegistry(_DBOS_STREAMS_CHANNEL, true)}
	const readers = 32

	type subscription struct {
		ch      chan struct{}
		cleanup func()
	}
	subs := make([]subscription, readers)
	for i := range subs {
		subs[i].ch, subs[i].cleanup = s.StreamWakeChannel(context.Background(), "workflow", "key")
	}

	var cleanupWG sync.WaitGroup
	for i := 0; i < readers; i += 2 {
		cleanupWG.Add(1)
		go func(cleanup func()) {
			defer cleanupWG.Done()
			cleanup()
		}(subs[i].cleanup)
	}
	cleanupWG.Wait()

	s.streamNotifier.notify("workflow::key")
	for i := 1; i < readers; i += 2 {
		select {
		case <-subs[i].ch:
		case <-time.After(time.Second):
			t.Fatalf("reader %d was unregistered by another reader's cleanup", i)
		}
		subs[i].cleanup()
	}
}

// fakeRows simulates a result set that is truncated mid-stream: it yields its
// rows, then Next() returns false with the error parked on Err() — exactly how
// pgx/database/sql surface a connection dropped during iteration.
type fakeRows struct {
	rows [][]any
	idx  int
	err  error
}

func (r *fakeRows) Next() bool {
	if r.idx < len(r.rows) {
		r.idx++
		return true
	}
	return false
}

func (r *fakeRows) Scan(dest ...any) error {
	for i, v := range r.rows[r.idx-1] {
		if v == nil {
			continue // leave dest at its zero value (NULL column)
		}
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

func (r *fakeRows) Err() error   { return r.err }
func (r *fakeRows) Close() error { return nil }

type fakeQueryPool struct {
	rows Rows
}

func (p *fakeQueryPool) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	return p.rows, nil
}

func (p *fakeQueryPool) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	return nil, errors.New("not implemented")
}

func (p *fakeQueryPool) QueryRow(ctx context.Context, q string, args ...any) Row {
	panic("not implemented")
}

func (p *fakeQueryPool) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	return nil, errors.New("not implemented")
}

func (p *fakeQueryPool) Ping(ctx context.Context) error { return nil }
func (p *fakeQueryPool) Close()                         {}

func newFakeSysDB(rows Rows) *SysDB {
	return &SysDB{
		pool:    &fakeQueryPool{rows: rows},
		dialect: PostgresDialect{},
		schema:  "dbos",
		logger:  slog.New(slog.DiscardHandler),
	}
}

// A truncated schedule list returned as success makes the scheduler reconciler
// remove every schedule missing from it, so mid-iteration errors must surface.
func TestListSchedulesSurfacesRowsErr(t *testing.T) {
	connErr := errors.New("simulated connection loss")
	rows := &fakeRows{
		rows: [][]any{{
			"schedule-id-1",             // schedule_id
			"sched-1",                   // schedule_name
			"wf",                        // workflow_name
			nil,                         // workflow_class_name
			"* * * * *",                 // schedule
			models.ScheduleStatusActive, // status
			"null",                      // context
			nil,                         // last_fired_at
			false,                       // automatic_backfill
			"UTC",                       // cron_timezone
			nil,                         // queue_name
		}},
		err: connErr,
	}

	schedules, err := newFakeSysDB(rows).ListSchedules(context.Background(), ListSchedulesDBInput{})
	if err == nil {
		t.Fatalf("ListSchedules returned truncated list of %d schedule(s) as success; want error", len(schedules))
	}
	if !errors.Is(err, connErr) {
		t.Fatalf("ListSchedules error = %v; want wrapped %v", err, connErr)
	}
}

func TestGetQueuePartitionsSurfacesRowsErr(t *testing.T) {
	connErr := errors.New("simulated connection loss")
	rows := &fakeRows{
		rows: [][]any{{"partition-1"}},
		err:  connErr,
	}

	partitions, err := newFakeSysDB(rows).GetQueuePartitions(context.Background(), "test-queue")
	if err == nil {
		t.Fatalf("GetQueuePartitions returned truncated list of %d partition(s) as success; want error", len(partitions))
	}
	if !errors.Is(err, connErr) {
		t.Fatalf("GetQueuePartitions error = %v; want wrapped %v", err, connErr)
	}
}

// context.DeadlineExceeded satisfies net.Error, so IsRetryable's trailing
// net.Error check used to classify it -- and anything wrapping it -- as a
// transient driver failure. DBOS builds its own timeout errors on top of that
// cause, so a Recv/GetEvent timeout would be retried forever by the infinite
// system-database retrier while the workflow context was still live.
func TestIsRetryableRejectsContextErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"deadline", context.DeadlineExceeded},
		{"canceled", context.Canceled},
		{"wrapped deadline", models.NewTimeoutError("wf", "DBOS.recv", "no message received", context.DeadlineExceeded)},
		{"wrapped canceled", models.NewTimeoutError("wf", "", "interrupted", context.Canceled)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if (PostgresDialect{}).IsRetryable(tc.err, nil) {
				t.Fatalf("IsRetryable(%v) = true; want false", tc.err)
			}
		})
	}
}

func TestIsRetryableAcceptsIdleTransactionKill(t *testing.T) {
	err := &pgconn.PgError{Code: pgerrcode.IdleInTransactionSessionTimeout, Message: "terminating connection due to idle-in-transaction timeout"}
	if !(PostgresDialect{}).IsRetryable(err, nil) {
		t.Fatal("the server's idle-in-transaction kill must be retried")
	}
}

// pgx returns io.ErrUnexpectedEOF when the peer closes the connection while a response is
// awaited (pgproto3 translates io.EOF), e.g. a proxy or pooler dropping the client mid-query.
func TestIsRetryableAcceptsUnexpectedEOF(t *testing.T) {
	err := fmt.Errorf("failed to get workflow status: %w", io.ErrUnexpectedEOF)
	if !(PostgresDialect{}).IsRetryable(err, nil) {
		t.Fatal("a connection closed mid-response must be retried")
	}
}

// cutProxy is a TCP proxy in front of the test database. Once armed, it drops the next client
// message containing marker and half-closes its side of that connection (FIN, no ErrorResponse),
// as a pooler or load balancer dropping a client mid-query does.
type cutProxy struct {
	ln     net.Listener
	target string
	marker []byte
	armed  atomic.Bool
	cuts   atomic.Int32
	wg     sync.WaitGroup
}

func startCutProxy(t *testing.T, target, marker string) *cutProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &cutProxy{ln: ln, target: target, marker: []byte(marker)}
	p.wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.wg.Go(func() { p.serve(c) })
		}
	})
	return p
}

// stop waits for the proxied connections to end, so the clients must be closed first.
func (p *cutProxy) stop() {
	p.ln.Close()
	p.wg.Wait()
}

func (p *cutProxy) serve(c net.Conn) {
	defer c.Close()
	s, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer s.Close()
	p.wg.Go(func() { // server -> client
		_, _ = io.Copy(c, s)
		c.Close()
	})
	buf := make([]byte, 64<<10)
	for { // client -> server
		n, err := c.Read(buf)
		if err != nil {
			return
		}
		if bytes.Contains(buf[:n], p.marker) && p.armed.CompareAndSwap(true, false) {
			p.cuts.Add(1)
			_ = c.(*net.TCPConn).CloseWrite() // FIN
			_, _ = io.Copy(io.Discard, c)     // keep reading until the client closes, so the kernel sends no RST
			return
		}
		if _, err := s.Write(buf[:n]); err != nil {
			return
		}
	}
}

// A pgx pool whose connection is closed by the peer mid-query: pgx reports io.ErrUnexpectedEOF,
// and Retry must run the query again.
func TestRetryRecoversFromConnectionClosedMidQuery(t *testing.T) {
	if os.Getenv("DBOS_TEST_BACKEND") == "sqlite" {
		t.Skip("needs a network connection to the database")
	}
	databaseURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if databaseURL == "" {
		password := os.Getenv("PGPASSWORD")
		if password == "" {
			password = "dbos"
		}
		databaseURL = fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", url.QueryEscape(password))
	}
	u, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	target := u.Host
	if u.Port() == "" {
		target = net.JoinHostPort(u.Hostname(), "5432")
	}

	const query = "SELECT 'cut me'"
	proxy := startCutProxy(t, target, query)
	defer proxy.stop()
	u.Host = proxy.ln.Addr().String()
	params := u.Query()
	params.Set("default_query_exec_mode", "exec") // one message per query, carrying the SQL text
	u.RawQuery = params.Encode()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	proxy.armed.Store(true)
	var attempts []error
	got, err := RetryWithResult(ctx, func() (string, error) {
		var v string
		err := pool.QueryRow(ctx, query).Scan(&v)
		attempts = append(attempts, err)
		return v, err
	})
	if err != nil {
		t.Fatalf("the query was not retried: %v", err)
	}
	if got != "cut me" {
		t.Fatalf("got %q; want %q", got, "cut me")
	}
	if len(attempts) != 2 || !errors.Is(attempts[0], io.ErrUnexpectedEOF) {
		t.Fatalf("attempts = %v; want io.ErrUnexpectedEOF, then success", attempts)
	}
	if cuts := proxy.cuts.Load(); cuts != 1 {
		t.Fatalf("the proxy cut %d connections; want 1", cuts)
	}
}

func TestConnConfigSetsIdleTransactionTimeout(t *testing.T) {
	cases := []struct {
		connString string
		want       bool
	}{
		{"postgres://user:pass@localhost:5432/dbos?options=-c%20idle_in_transaction_session_timeout%3D7000", true},
		{"postgres://user:pass@localhost:5432/dbos?idle_in_transaction_session_timeout=7000", true},
		{"postgres://user:pass@localhost:5432/dbos?options=-c%20statement_timeout%3D7000", false},
		{"postgres://user:pass@localhost:5432/dbos?sslmode=disable", false},
	}
	for _, c := range cases {
		config, err := pgx.ParseConfig(c.connString)
		if err != nil {
			t.Fatalf("ParseConfig(%q): %v", c.connString, err)
		}
		if got := connConfigSetsIdleTransactionTimeout(config); got != c.want {
			t.Errorf("connConfigSetsIdleTransactionTimeout(%q) = %v, want %v", c.connString, got, c.want)
		}
	}
}

func TestConnStringSetsPoolMaxConns(t *testing.T) {
	cases := []struct {
		connString string
		want       bool
	}{
		{"postgres://user:pass@localhost:5432/dbos?sslmode=disable&pool_max_conns=7", true},
		{"postgres://user:pass@localhost:5432/dbos?pool_max_conns=7", true},
		{"postgres://user:pass@localhost:5432/dbos?sslmode=disable", false},
		{"host=localhost port=5432 dbname=dbos pool_max_conns=7", true},
		{"host=localhost port=5432 dbname=dbos", false},
	}
	for _, c := range cases {
		if got := connStringSetsPoolMaxConns(c.connString); got != c.want {
			t.Errorf("connStringSetsPoolMaxConns(%q) = %v, want %v", c.connString, got, c.want)
		}
	}
}

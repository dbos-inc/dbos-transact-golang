package sysdb

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
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
		subs[i].ch, subs[i].cleanup = s.StreamWakeChannel("workflow", "key")
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

// gcFakePool scripts the two statements one garbage collection batch issues: the
// bound query picking the batch's upper watermark, and the delete removing it.
type gcFakePool struct {
	steps      []int64 // bounds returned by successive bound queries; exhausted means the final batch
	failDelete int     // 1-based delete to fail, 0 for none
	bounds     [][]any // args of each bound query, in order
	deletes    [][]any // args of each delete, in order
	commits    int
	rollbacks  int
}

type gcFakeTx struct {
	pool *gcFakePool
	done bool
}

type gcFakeRow struct {
	step *int64
}

func (r gcFakeRow) Scan(dest ...any) error {
	if r.step == nil {
		return ErrNoRows
	}
	*(dest[0].(*int64)) = *r.step
	return nil
}

type gcFakeResult struct{}

func (gcFakeResult) RowsAffected() (int64, error) { return 0, nil }

func (t *gcFakeTx) QueryRow(_ context.Context, _ string, args ...any) Row {
	t.pool.bounds = append(t.pool.bounds, args)
	if len(t.pool.steps) == 0 {
		return gcFakeRow{}
	}
	step := t.pool.steps[0]
	t.pool.steps = t.pool.steps[1:]
	return gcFakeRow{step: &step}
}

func (t *gcFakeTx) Exec(_ context.Context, _ string, args ...any) (Result, error) {
	t.pool.deletes = append(t.pool.deletes, args)
	if t.pool.failDelete == len(t.pool.deletes) {
		return nil, errors.New("injected garbage collection failure")
	}
	return gcFakeResult{}, nil
}

func (t *gcFakeTx) Query(context.Context, string, ...any) (Rows, error) {
	return nil, errors.New("unexpected Query")
}

func (t *gcFakeTx) Commit(context.Context) error {
	t.done = true
	t.pool.commits++
	return nil
}

func (t *gcFakeTx) Rollback(context.Context) error {
	if !t.done {
		t.pool.rollbacks++
	}
	return nil
}

func (p *gcFakePool) BeginTx(context.Context, TxOptions) (Tx, error) { return &gcFakeTx{pool: p}, nil }
func (p *gcFakePool) Exec(context.Context, string, ...any) (Result, error) {
	return nil, errors.New("unexpected Exec outside a batch transaction")
}
func (p *gcFakePool) Query(context.Context, string, ...any) (Rows, error) {
	return nil, errors.New("unexpected Query")
}
func (p *gcFakePool) QueryRow(context.Context, string, ...any) Row {
	return gcFakeRow{}
}
func (p *gcFakePool) Ping(context.Context) error { return nil }
func (p *gcFakePool) Close()                     {}

func TestGarbageCollectBatches(t *testing.T) {
	cutoff, batchSize := int64(100), 3
	newSysDB := func(pool Pool) *SysDB {
		return &SysDB{pool: pool, dialect: SqliteDialect{}, logger: slog.New(slog.DiscardHandler)}
	}
	input := GarbageCollectWorkflowsInput{CutoffEpochTimestampMs: &cutoff, BatchSize: &batchSize}

	t.Run("advances a watermark, one committed transaction per batch", func(t *testing.T) {
		pool := &gcFakePool{steps: []int64{3, 6, 9}}
		if err := newSysDB(pool).GarbageCollectWorkflows(context.Background(), input); err != nil {
			t.Fatalf("GarbageCollectWorkflows: %v", err)
		}
		wantBounds := [][]any{
			{cutoff, int64(0), batchSize - 1},
			{cutoff, int64(3), batchSize - 1},
			{cutoff, int64(6), batchSize - 1},
			{cutoff, int64(9), batchSize - 1},
		}
		if !reflect.DeepEqual(pool.bounds, wantBounds) {
			t.Errorf("bound queries = %v, want %v", pool.bounds, wantBounds)
		}
		// The final delete drops the upper bound, taking the whole tail above the watermark
		wantDeletes := [][]any{
			{cutoff, int64(0), int64(3)},
			{cutoff, int64(3), int64(6)},
			{cutoff, int64(6), int64(9)},
			{cutoff, int64(9)},
		}
		if !reflect.DeepEqual(pool.deletes, wantDeletes) {
			t.Errorf("deletes = %v, want %v", pool.deletes, wantDeletes)
		}
		if pool.commits != len(wantDeletes) {
			t.Errorf("commits = %d, want %d", pool.commits, len(wantDeletes))
		}
	})

	t.Run("leaves batches committed before a failure", func(t *testing.T) {
		pool := &gcFakePool{steps: []int64{3, 6, 9}, failDelete: 2}
		if err := newSysDB(pool).GarbageCollectWorkflows(context.Background(), input); err == nil {
			t.Fatal("GarbageCollectWorkflows: expected the injected failure to surface")
		}
		if pool.commits != 1 {
			t.Errorf("commits = %d, want 1", pool.commits)
		}
		if pool.rollbacks != 1 {
			t.Errorf("rollbacks = %d, want 1", pool.rollbacks)
		}
		if len(pool.deletes) != 2 {
			t.Errorf("deletes = %d, want 2", len(pool.deletes))
		}
	})
}

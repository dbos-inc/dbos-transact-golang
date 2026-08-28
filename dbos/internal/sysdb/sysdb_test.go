package sysdb

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos/internal/models"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
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

// gcFakePool models the rows a garbage collection pass walks: each batch runs a
// bound query for its upper watermark and a delete, and only a committed batch
// removes rows, so a replayed batch sees exactly what it saw before.
type gcFakePool struct {
	rows           []int64 // remaining completed_at values, ascending
	cutoff         int64
	failDelete     int   // 1-based delete attempt to fail, 0 for none
	failCommit     int   // 1-based commit attempt to fail, 0 for none
	failErr        error // error the failed attempt returns; a retryable one is replayed
	bounds         [][]any
	deletes        [][]any
	commits        int
	commitAttempts int
	rollbacks      int
}

// eligible returns the rows a batch starting at watermark would collect.
func (p *gcFakePool) eligible(watermark int64) []int64 {
	var out []int64
	for _, v := range p.rows {
		if v > watermark && v < p.cutoff {
			out = append(out, v)
		}
	}
	return out
}

func (p *gcFakePool) remove(deleted []int64) {
	gone := make(map[int64]bool, len(deleted))
	for _, v := range deleted {
		gone[v] = true
	}
	kept := make([]int64, 0, len(p.rows))
	for _, v := range p.rows {
		if !gone[v] {
			kept = append(kept, v)
		}
	}
	p.rows = kept
}

type gcFakeTx struct {
	pool   *gcFakePool
	staged []int64 // rows this batch deleted, dropped on rollback
	done   bool
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

type gcFakeResult struct{ rows int64 }

func (r gcFakeResult) RowsAffected() (int64, error) { return r.rows, nil }

func (t *gcFakeTx) QueryRow(_ context.Context, _ string, args ...any) Row {
	t.pool.bounds = append(t.pool.bounds, args)
	eligible := t.pool.eligible(args[1].(int64))
	offset := args[2].(int)
	if offset >= len(eligible) {
		return gcFakeRow{}
	}
	return gcFakeRow{step: &eligible[offset]}
}

func (t *gcFakeTx) Exec(_ context.Context, _ string, args ...any) (Result, error) {
	t.pool.deletes = append(t.pool.deletes, args)
	if len(t.pool.deletes) == t.pool.failDelete {
		if t.pool.failErr != nil {
			return nil, t.pool.failErr
		}
		return nil, errors.New("injected garbage collection failure")
	}
	// The final batch drops the upper bound and takes the whole tail
	upper := int64(math.MaxInt64)
	if len(args) > 2 {
		upper = args[2].(int64)
	}
	for _, v := range t.pool.eligible(args[1].(int64)) {
		if v <= upper {
			t.staged = append(t.staged, v)
		}
	}
	return gcFakeResult{rows: int64(len(t.staged))}, nil
}

func (t *gcFakeTx) Query(context.Context, string, ...any) (Rows, error) {
	return nil, errors.New("unexpected Query")
}

func (t *gcFakeTx) Commit(context.Context) error {
	t.pool.commitAttempts++
	if t.pool.commitAttempts == t.pool.failCommit {
		return t.pool.failErr
	}
	t.pool.remove(t.staged)
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

// The unbatched path deletes everything eligible in one statement off the pool.
func (p *gcFakePool) Exec(_ context.Context, _ string, args ...any) (Result, error) {
	p.deletes = append(p.deletes, args)
	deleted := p.eligible(0)
	p.remove(deleted)
	return gcFakeResult{rows: int64(len(deleted))}, nil
}
func (p *gcFakePool) Query(context.Context, string, ...any) (Rows, error) {
	return nil, errors.New("unexpected Query")
}
func (p *gcFakePool) QueryRow(context.Context, string, ...any) Row {
	return gcFakeRow{}
}
func (p *gcFakePool) Ping(context.Context) error { return nil }
func (p *gcFakePool) Close()                     {}

// gcLogSink captures the deleted_count the collector logs, the only place the
// total surfaces.
type gcLogSink struct {
	slog.Handler
	deleted *int64
}

func (gcLogSink) Enabled(context.Context, slog.Level) bool { return true }

func (h gcLogSink) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "deleted_count" {
			*h.deleted = a.Value.Int64()
		}
		return true
	})
	return nil
}

func TestGarbageCollectBatches(t *testing.T) {
	cutoff, batchSize := int64(100), 3
	newPool := func() *gcFakePool {
		return &gcFakePool{rows: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, cutoff: cutoff}
	}
	// deleted holds the count the pass logged, 0 if it never got there
	newSysDB := func(pool Pool) (*SysDB, *int64) {
		var deleted int64
		return &SysDB{pool: pool, dialect: PostgresDialect{}, logger: slog.New(gcLogSink{slog.DiscardHandler, &deleted})}, &deleted
	}
	input := GarbageCollectWorkflowsInput{CutoffEpochTimestampMs: &cutoff, BatchSize: &batchSize}

	t.Run("advances a watermark, one committed transaction per batch", func(t *testing.T) {
		pool := newPool()
		sysDB, deleted := newSysDB(pool)
		if err := sysDB.GarbageCollectWorkflows(context.Background(), input); err != nil {
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
		if len(pool.rows) != 0 {
			t.Errorf("rows left = %v, want none", pool.rows)
		}
		if *deleted != 9 {
			t.Errorf("logged deleted_count = %d, want 9", *deleted)
		}
	})

	t.Run("leaves batches committed before a failure", func(t *testing.T) {
		pool := newPool()
		pool.failDelete = 2
		sysDB, _ := newSysDB(pool)
		if err := sysDB.GarbageCollectWorkflows(context.Background(), input); err == nil {
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
		// Only the first batch's rows are gone
		if !reflect.DeepEqual(pool.rows, []int64{4, 5, 6, 7, 8, 9}) {
			t.Errorf("rows left = %v, want 4..9", pool.rows)
		}
	})

	// A batch whose commit loses a serialization race is replayed whole, so its
	// rows must be counted on commit and not a statement earlier.
	t.Run("replays a conflicted batch without double counting", func(t *testing.T) {
		pool := newPool()
		pool.failCommit = 2
		pool.failErr = &pgconn.PgError{Code: pgerrcode.SerializationFailure}
		sysDB, deleted := newSysDB(pool)
		if err := sysDB.GarbageCollectWorkflows(context.Background(), input); err != nil {
			t.Fatalf("GarbageCollectWorkflows: %v", err)
		}
		// The replayed batch re-runs its delete, but only a commit counts rows
		if len(pool.deletes) != 5 {
			t.Errorf("deletes = %d, want 5", len(pool.deletes))
		}
		if pool.commits != 4 || pool.rollbacks != 1 {
			t.Errorf("commits/rollbacks = %d/%d, want 4/1", pool.commits, pool.rollbacks)
		}
		if len(pool.rows) != 0 {
			t.Errorf("rows left = %v, want none", pool.rows)
		}
		if *deleted != 9 {
			t.Errorf("logged deleted_count = %d, want 9", *deleted)
		}
	})

	t.Run("deletes in one statement when unbatched", func(t *testing.T) {
		pool := newPool()
		sysDB, deleted := newSysDB(pool)
		unbatched := GarbageCollectWorkflowsInput{CutoffEpochTimestampMs: &cutoff}
		if err := sysDB.GarbageCollectWorkflows(context.Background(), unbatched); err != nil {
			t.Fatalf("GarbageCollectWorkflows: %v", err)
		}
		if len(pool.deletes) != 1 || pool.commits != 0 {
			t.Errorf("deletes/commits = %d/%d, want 1/0", len(pool.deletes), pool.commits)
		}
		if len(pool.rows) != 0 {
			t.Errorf("rows left = %v, want none", pool.rows)
		}
		if *deleted != 9 {
			t.Errorf("logged deleted_count = %d, want 9", *deleted)
		}
	})
}

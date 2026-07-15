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
	s := &SysDB{streamNotifier: newNotifyRegistry()}
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

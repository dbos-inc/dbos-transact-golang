package sysdb

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// flushRecorderPool counts the pg_notify statements the notifier issues, and
// can fail the first few of them.
type flushRecorderPool struct {
	mu     sync.Mutex
	n      int
	errs   []error // returned to the first len(errs) calls, one each
	sentTo []string
}

func (p *flushRecorderPool) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

func (p *flushRecorderPool) Exec(ctx context.Context, q string, args ...any) (Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	if p.n <= len(p.errs) {
		return nil, p.errs[p.n-1]
	}
	if len(args) > 0 {
		if channel, ok := args[0].(string); ok {
			p.sentTo = append(p.sentTo, channel)
		}
	}
	return nil, nil
}

func (p *flushRecorderPool) delivered() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sentTo...)
}

func (p *flushRecorderPool) Query(ctx context.Context, q string, args ...any) (Rows, error) {
	return nil, errors.New("not implemented")
}
func (p *flushRecorderPool) QueryRow(ctx context.Context, q string, args ...any) Row {
	panic("not implemented")
}
func (p *flushRecorderPool) BeginTx(ctx context.Context, opts TxOptions) (Tx, error) {
	return nil, errors.New("not implemented")
}
func (p *flushRecorderPool) Ping(ctx context.Context) error { return nil }
func (p *flushRecorderPool) Close()                         {}

// pushingSysDB is a SysDB whose registries push, wired to a pool that only
// records flushes.
func pushingSysDB(interval time.Duration) *SysDB {
	return &SysDB{
		dialect:                      SqliteDialect{},
		logger:                       slog.New(slog.DiscardHandler),
		pool:                         &flushRecorderPool{},
		RecvNotifier:                 newNotifyRegistry(_DBOS_NOTIFICATIONS_CHANNEL, false),
		EventNotifier:                newNotifyRegistry(_DBOS_WORKFLOW_EVENTS_CHANNEL, true),
		streamNotifier:               newNotifyRegistry(_DBOS_STREAMS_CHANNEL, true),
		notificationCoalesceInterval: interval,
	}
}

func TestSignalWakesLocalWaiterAndQueuesOnePush(t *testing.T) {
	s := pushingSysDB(time.Hour) // never ticks: the queue is inspected by hand
	ch, release := s.StreamWakeChannel("wf", "key")
	defer release()

	// Repeated writes to the same stream wake the reader now and collapse into a
	// single wakeup for other processes: that collapsing is the point of
	// coalescing.
	for range 100 {
		s.SignalStreamWrite("wf", "key")
	}
	select {
	case <-ch:
	default:
		t.Fatal("local stream reader was not woken")
	}
	s.SignalStreamWrite("wf", "other")
	s.SignalEventSet("wf", "event")

	if got := len(s.streamNotifier.drainPending()); got != 2 {
		t.Errorf("expected 2 deduplicated stream payloads, got %d", got)
	}
	if got := len(s.EventNotifier.drainPending()); got != 1 {
		t.Errorf("expected 1 event payload, got %d", got)
	}
	// A drain empties the queue, so an idle notifier issues no statements.
	if got := s.streamNotifier.drainPending(); got != nil {
		t.Errorf("expected nothing pending after drain, got %v", got)
	}
}

func TestRegistriesThatDoNotPushOnlyWakeLocalWaiters(t *testing.T) {
	// Messages keep their database trigger, and no channel is pushed at all on a
	// backend without listen/notify: signaling must still wake local waiters
	// without queueing anything.
	s := pushingSysDB(time.Hour)
	s.RecvNotifier = newNotifyRegistry(_DBOS_NOTIFICATIONS_CHANNEL, false)
	s.EventNotifier = newNotifyRegistry(_DBOS_WORKFLOW_EVENTS_CHANNEL, false)
	s.streamNotifier = newNotifyRegistry(_DBOS_STREAMS_CHANNEL, false)

	if s.pushesNotifications() {
		t.Fatal("a backend that pushes nothing should not need a notifier loop")
	}
	ch, release := s.StreamWakeChannel("wf", "key")
	defer release()

	s.SignalStreamWrite("wf", "key")
	select {
	case <-ch:
	default:
		t.Fatal("local stream reader was not woken")
	}
	if got := s.streamNotifier.drainPending(); got != nil {
		t.Errorf("expected nothing queued for push, got %v", got)
	}
}

func TestFlushRetriesTransientErrorsAndDropsTheRest(t *testing.T) {
	// A database blip must not cost waiters in other processes their wakeup: the
	// push is retried until it lands.
	s := pushingSysDB(time.Hour)
	pool := s.pool.(*flushRecorderPool)
	pool.errs = []error{&pgconn.PgError{Code: pgerrcode.ConnectionFailure}}
	s.SignalStreamWrite("wf", "key")

	s.flushNotifications(context.Background())
	if got := pool.calls(); got != 2 {
		t.Fatalf("expected the transient failure to be retried (2 calls), got %d", got)
	}
	if got := pool.delivered(); len(got) != 1 || got[0] != _DBOS_STREAMS_CHANNEL {
		t.Fatalf("expected the retry to deliver the stream batch, got %v", got)
	}

	// An error retrying cannot fix ends the flush instead of looping forever. The
	// batch is dropped, not requeued: waiters fall back to polling.
	s = pushingSysDB(time.Hour)
	pool = s.pool.(*flushRecorderPool)
	pool.errs = []error{errors.New("malformed payload")}
	s.SignalStreamWrite("wf", "key")

	s.flushNotifications(context.Background())
	if got := pool.calls(); got != 1 {
		t.Fatalf("expected no retry of a non-retryable error, got %d calls", got)
	}
	if got := s.streamNotifier.drainPending(); got != nil {
		t.Errorf("expected the dropped batch not to be requeued, got %v", got)
	}
}

func TestNotifierLoopFlushesAfterCancel(t *testing.T) {
	// The final push lets values written just before shutdown wake waiters in
	// other processes instead of waiting out their fallback poll.
	s := pushingSysDB(time.Hour) // never ticks; only the final flush can fire
	s.SignalStreamWrite("wf", "key")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.notifierLoop(ctx)
		close(done)
	}()
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("notifier loop did not exit after cancel")
	}
	if got := s.pool.(*flushRecorderPool).calls(); got != 1 {
		t.Fatalf("expected one final push, got %d", got)
	}
	if got := s.streamNotifier.drainPending(); got != nil {
		t.Errorf("expected the final push to drain the queue, got %v", got)
	}
}

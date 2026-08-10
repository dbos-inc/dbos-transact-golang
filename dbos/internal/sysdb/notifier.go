package sysdb

import (
	"context"
	"sync"
	"time"
)

// notifier.go: how a payload written here reaches every waiter for it.
//
// Waiters (recv, getEvent, stream readers) subscribe to a notifyRegistry, one
// per LISTEN/NOTIFY channel. A payload reaches a registry either from the wire
// -- the listener loop or the polling fallback in system_database.go calls
// notify() -- or from this process writing it, which calls signal(): that wakes
// the local waiters immediately and queues the payload for the notifier loop to
// push to the other processes listening on the channel.

// DefaultNotificationCoalesceInterval is how often payloads queued by
// notifyRegistry.signal are pushed to the other processes listening on their
// channel. It bounds the wakeup latency this adds and caps the rate of
// notifying commits regardless of write throughput.
const DefaultNotificationCoalesceInterval = 10 * time.Millisecond

// MinNotificationCoalesceInterval is the smallest configurable flush interval.
const MinNotificationCoalesceInterval = time.Millisecond

// _NOTIFIER_FINAL_FLUSH_TIMEOUT bounds the flush issued after the notifier's
// context is canceled, so a hung database cannot delay shutdown.
const _NOTIFIER_FINAL_FLUSH_TIMEOUT = 5 * time.Second

// notifyRegistry is one notification channel's local half: it delivers
// per-payload wake-ups to the waiters in this process (recv, getEvent, stream
// readers). Payloads reach it from two directions -- notify() for one observed
// on the wire, signal() for one this process just wrote -- and signal() also
// queues the payload for the notifier loop to push to the other processes
// listening on the channel.
type notifyRegistry struct {
	channel string // the LISTEN/NOTIFY channel this registry mirrors
	mu      sync.Mutex
	subs    map[string]map[chan struct{}]struct{} // payload -> set of waiter channels
	pending map[string]struct{}                   // payloads awaiting a push; nil when this process does not push on this channel
}

// newNotifyRegistry builds the registry for a channel. push enables the
// outbound half: it is off where notifications are not pushed from the
// application, either because the backend has no listen/notify or because a
// database trigger already emits them (the notifications channel).
func newNotifyRegistry(channel string, push bool) *notifyRegistry {
	n := &notifyRegistry{
		channel: channel,
		subs:    make(map[string]map[chan struct{}]struct{}),
	}
	if push {
		n.pending = make(map[string]struct{})
	}
	return n
}

func (n *notifyRegistry) addLocked(payload string, ch chan struct{}) {
	set := n.subs[payload]
	if set == nil {
		set = make(map[chan struct{}]struct{})
		n.subs[payload] = set
	}
	set[ch] = struct{}{}
}

// subscribe registers a new waiter for payload and returns its wake channel.
func (n *notifyRegistry) subscribe(payload string) chan struct{} {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.addLocked(payload, ch)
	n.mu.Unlock()
	return ch
}

// subscribeExclusive registers the sole waiter for payload, returning false if one
// already exists.
func (n *notifyRegistry) subscribeExclusive(payload string) (chan struct{}, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.subs[payload]) > 0 {
		return nil, false
	}
	ch := make(chan struct{}, 1)
	n.addLocked(payload, ch)
	return ch, true
}

// unsubscribe removes a waiter; the payload entry is dropped once its last waiter leaves.
func (n *notifyRegistry) unsubscribe(payload string, ch chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	set := n.subs[payload]
	delete(set, ch)
	if len(set) == 0 {
		delete(n.subs, payload)
	}
}

// notify wakes every waiter for payload.
func (n *notifyRegistry) notify(payload string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notifyLocked(payload)
}

func (n *notifyRegistry) notifyLocked(payload string) {
	for ch := range n.subs[payload] {
		select {
		case ch <- struct{}{}:
		default: // Do not block (coalesce multiple notifications into one)
		}
	}
}

// signal reports a payload this process just wrote: waiters here are woken
// immediately, with no round trip, and the payload is queued for the notifier
// loop to push to waiters in other processes.
//
// Call it only after the write has committed, or a woken waiter may re-read
// before the row is visible and go back to sleep until its fallback poll.
func (n *notifyRegistry) signal(payload string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.notifyLocked(payload)
	if n.pending != nil {
		n.pending[payload] = struct{}{}
	}
}

// pushes reports whether payloads signaled here are pushed to other processes.
func (n *notifyRegistry) pushes() bool {
	if n == nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.pending != nil
}

// drainPending removes and returns the payloads queued for the next push.
// Duplicates have already collapsed: one wakeup per payload per flush.
func (n *notifyRegistry) drainPending() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.pending) == 0 {
		return nil
	}
	payloads := make([]string, 0, len(n.pending))
	for payload := range n.pending {
		payloads = append(payloads, payload)
	}
	clear(n.pending)
	return payloads
}

// notifyAll wakes every waiter regardless of payload; used after a listener
// reconnect so waiters re-poll for a value whose notification may have been missed.
func (n *notifyRegistry) notifyAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, set := range n.subs {
		for ch := range set {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

// payloads returns a snapshot of the currently registered payloads (used by the
// polling fallback to know which rows to check).
func (n *notifyRegistry) payloads() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]string, 0, len(n.subs))
	for payload := range n.subs {
		out = append(out, payload)
	}
	return out
}

// has reports whether any waiter is registered for payload.
func (n *notifyRegistry) Has(payload string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.subs[payload]) > 0
}

// waiterCount reports the number of waiters registered for payload.
func (n *notifyRegistry) WaiterCount(payload string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.subs[payload])
}

// clear drops all registrations (used on shutdown).
func (n *notifyRegistry) clear() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.subs = make(map[string]map[chan struct{}]struct{})
}

// pushRegistries returns the notification registries where notification pushes are enabled, among eligible candidates.
func (s *SysDB) pushRegistries() []*notifyRegistry {
	registries := make([]*notifyRegistry, 0, 2)
	for _, registry := range []*notifyRegistry{s.EventNotifier, s.streamNotifier} {
		if registry.pushes() {
			registries = append(registries, registry)
		}
	}
	return registries
}

// pushesNotifications reports whether this process pushes any notification at
// all, and so needs a notifier loop.
func (s *SysDB) pushesNotifications() bool {
	return len(s.pushRegistries()) > 0
}

// SignalStreamWrite wakes readers of the given stream, here and elsewhere. Call
// it after the write commits.
func (s *SysDB) SignalStreamWrite(workflowID, key string) {
	s.streamNotifier.signal(workflowID + "::" + key)
}

// SignalEventSet wakes GetEvent waiters on the given key, here and elsewhere.
// Call it after the write commits.
func (s *SysDB) SignalEventSet(workflowID, key string) {
	s.EventNotifier.signal(workflowID + "::" + key)
}

// notifierLoop pushes queued notifications until the context is canceled, then
// pushes once more so writes made just before shutdown still wake waiters in
// other processes promptly.
func (s *SysDB) notifierLoop(ctx context.Context) {
	defer s.logger.Debug("Notifier loop exiting")
	s.logger.Debug("DBOS: Starting notifier loop")

	interval := s.notificationCoalesceInterval
	if interval <= 0 {
		interval = DefaultNotificationCoalesceInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// The loop's context is already canceled by the time shutdown runs,
			// so the final flush needs its own bounded context.
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), _NOTIFIER_FINAL_FLUSH_TIMEOUT)
			s.flushNotifications(flushCtx)
			cancel()
			return
		case <-ticker.C:
			s.flushNotifications(ctx)
		}
	}
}

// flushNotifications emits one notifying transaction per channel for everything
// queued since the last flush.
func (s *SysDB) flushNotifications(ctx context.Context) {
	for _, registry := range s.pushRegistries() { // streams, events, etc
		payloads := registry.drainPending()
		if len(payloads) == 0 {
			continue
		}
		// Batch pg_notify calls for all payloads.
		if err := Retry(ctx, func() error {
			_, err := s.pool.Exec(ctx, `SELECT pg_notify($1, p) FROM unnest($2::text[]) AS p`, registry.channel, payloads)
			return err
		}, WithRetrierLogger(s.logger)); err != nil {
			// For non transient DB connection errors, drop the batch rather than requeue it.
			// Waiters make progress on their fallback poll.
			if ctx.Err() == nil {
				s.logger.Warn("Failed to push notifications", "channel", registry.channel, "count", len(payloads), "error", err)
			}
		}
	}
}

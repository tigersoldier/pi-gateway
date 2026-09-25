package session

import (
	"sync"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// terminalTypes are records that can flow through the hub and that a client
// must never miss: dropping them would leave it unable to reconstruct state
// (docs/design.md §7). Handshake and replay frames never pass through the hub.
var terminalTypes = map[string]bool{
	"message_end":           true,
	"tool_execution_end":    true,
	"turn_end":              true,
	"agent_end":             true,
	"agent_settled":         true,
	"response":              true,
	"extension_ui_request":  true,
	"gw_session_state":      true,
	"gw_error":              true,
	"gw_state_changed":      true,
	"gw_turn":               true,
	"gw_queue":              true,
	"gw_presence":           true,
	"compaction_start":      true,
	"compaction_end":        true,
	"auto_retry_start":      true,
	"auto_retry_end":        true,
	"queue_update":          true,
	"bash_execution_update": true,
	"extension_error":       true,
}

// IsTerminal reports whether a record type must never be dropped for a lossy
// client. It is used by both delivery hops: the hub and the connection.
func IsTerminal(typ string) bool { return terminalTypes[typ] }

// SubOptions configures a subscriber's delivery behavior.
type SubOptions struct {
	// Buffer is the delivery buffer size in records.
	Buffer int
	// AllowLossy lets the hub drop non-terminal records instead of dropping
	// the subscriber; the client is told via a gw_lag marker so it can resync.
	AllowLossy bool
	// FilterUnowned delivers only records addressed to this subscriber: used
	// for clients that lack the `observe` capability, which must not read the
	// session's event stream or transcript (docs/design.md §10). The zero
	// value delivers every record, which is what internal subscribers want.
	FilterUnowned bool
}

// Subscriber is one client's view of a session's event stream.
type Subscriber struct {
	// id is the owning client; with FilterUnowned, records addressed to it are
	// still delivered.
	id   string
	ch   chan protocol.Record
	opts SubOptions
	// subscriberMetrics is copied from the hub so delivery can count drops and
	// lags without reaching back into the hub.
	subscriberMetrics *metrics.Registry

	mu     sync.Mutex
	closed bool
	dead   bool // closed because the consumer could not keep up
	// lagFrom/lagTo track records dropped for a lossy subscriber but not yet
	// reported with a gw_lag marker.
	lagFrom uint64
	lagTo   uint64
}

// C returns the receive side. The channel is closed when the subscriber is
// unsubscribed or dropped for being too slow.
func (s *Subscriber) C() <-chan protocol.Record { return s.ch }

// Dead reports whether the subscriber was dropped because it stopped reading.
// The owning connection should be closed with slow_consumer.
func (s *Subscriber) Dead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

func (s *Subscriber) deliver(rec protocol.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.opts.FilterUnowned && rec.Owner != s.id {
		// Not this client's business: it lacks `observe`. The records it may
		// see are addressed to it (its responses, errors, and dialogs).
		return
	}
	if s.lagFrom != 0 {
		select {
		case s.ch <- s.lagRecord():
			s.subscriberMetrics.Inc(metrics.ClientLags)
			s.lagFrom, s.lagTo = 0, 0
		default:
			s.overflowLocked(rec)
			return
		}
	}
	select {
	case s.ch <- rec:
	default:
		s.overflowLocked(rec)
	}
}

// overflowLocked handles a full subscriber buffer: lossy subscribers drop
// non-terminal records and are told about the gap later; everyone else is
// dropped so the connection can reconnect and resync.
func (s *Subscriber) overflowLocked(rec protocol.Record) {
	if s.opts.AllowLossy && !terminalTypes[rec.Type] {
		s.subscriberMetrics.Inc(metrics.FramesDropped)
		s.recordLagLocked(rec.Seq)
		return
	}
	s.subscriberMetrics.Inc(metrics.SubscriberDrops)
	s.dead = true
	s.closed = true
	close(s.ch)
}

// recordLagLocked remembers a dropped record so the next successful delivery
// can be preceded by a gw_lag marker.
func (s *Subscriber) recordLagLocked(seq uint64) {
	if seq == 0 {
		return
	}
	if s.lagFrom == 0 || seq < s.lagFrom {
		s.lagFrom = seq
	}
	if seq > s.lagTo {
		s.lagTo = seq
	}
}

// lagRecord builds the marker telling the client which records it missed.
func (s *Subscriber) lagRecord() protocol.Record {
	raw, err := protocol.LagFrame(s.lagFrom, s.lagTo)
	if err != nil {
		return protocol.Record{}
	}
	return protocol.Record{Raw: raw, Type: "gw_lag"}
}

// Close unsubscribes without marking the subscriber dead.
func (s *Subscriber) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}

// Hub is the per-session, totally ordered event log and fan-out point.
//
// Ordering invariant: Seq is assigned exactly once, in publish order, and
// every subscriber observes records in non-decreasing Seq order. Publish is
// called from the SessionActor loop only.
type Hub struct {
	// metrics is optional instrumentation; a nil registry records nothing.
	metrics *metrics.Registry

	mu      sync.RWMutex
	seq     uint64
	buf     []protocol.Record
	head    int // next write index
	n       int // number of valid records
	subs    map[string]*Subscriber
	session string // value stamped into gw_session
}

// NewHub creates a hub retaining up to capacity records for replay.
func NewHub(capacity int, session string) *Hub {
	if capacity < 1 {
		capacity = 1
	}
	return &Hub{
		buf:     make([]protocol.Record, capacity),
		subs:    make(map[string]*Subscriber),
		session: session,
	}
}

// SetMetrics attaches the metrics registry. It must be called before the hub
// is shared with other goroutines.
func (h *Hub) SetMetrics(r *metrics.Registry) { h.metrics = r }

// Count reports how many subscribers are attached.
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}

// HeadSeq is the most recently assigned sequence.
func (h *Hub) HeadSeq() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.seq
}

// OldestSeq is the oldest retained sequence.
func (h *Hub) OldestSeq() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.oldestLocked()
}

func (h *Hub) oldestLocked() uint64 {
	if h.n == 0 {
		return h.seq + 1
	}
	idx := (h.head - h.n + len(h.buf)) % len(h.buf)
	return h.buf[idx].Seq
}

// Publish assigns the next sequence, stamps gateway fields, retains the record
// for replay, and fans it out. The returned record carries the assigned Seq.
func (h *Hub) Publish(rec protocol.Record) protocol.Record {
	h.mu.Lock()
	h.seq++
	rec.Seq = h.seq
	if stamped, err := protocol.Stamp(rec.Raw, map[string]any{
		"gw_seq":     h.seq,
		"gw_session": h.session,
		"gw_ts":      time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
	}); err == nil {
		rec.Raw = stamped
	}
	h.buf[h.head] = rec
	h.head = (h.head + 1) % len(h.buf)
	if h.n < len(h.buf) {
		h.n++
	}
	subs := make([]*Subscriber, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.Unlock()

	for _, s := range subs {
		s.deliver(rec)
	}
	return rec
}

// Subscribe registers a subscriber with a bounded delivery buffer.
func (h *Hub) Subscribe(id string, opts SubOptions) *Subscriber {
	if opts.Buffer < 1 {
		opts.Buffer = 1
	}
	s := &Subscriber{
		id:                id,
		ch:                make(chan protocol.Record, opts.Buffer),
		opts:              opts,
		subscriberMetrics: h.metrics,
	}
	h.mu.Lock()
	h.subs[id] = s
	h.mu.Unlock()
	return s
}

// Unsubscribe removes and closes a subscriber.
func (h *Hub) Unsubscribe(id string) {
	h.mu.Lock()
	s := h.subs[id]
	delete(h.subs, id)
	h.mu.Unlock()
	if s != nil {
		s.Close()
	}
}

// Replay returns retained records with Seq > afterSeq. ok is false when the
// cursor was evicted or lies beyond the head (which means it belongs to a
// previous log instance, for example after a daemon restart); the caller must
// resync.
func (h *Hub) Replay(afterSeq uint64) ([]protocol.Record, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if afterSeq > h.seq || afterSeq+1 < h.oldestLocked() {
		return nil, false
	}
	out := make([]protocol.Record, 0, h.n)
	for i := 0; i < h.n; i++ {
		idx := (h.head - h.n + i + len(h.buf)) % len(h.buf)
		if rec := h.buf[idx]; rec.Seq > afterSeq {
			out = append(out, rec)
		}
	}
	return out, true
}

// SetSession updates the session identifier stamped into new records. It is
// used when the daemon learns the file path of an implicitly created session.
func (h *Hub) SetSession(session string) {
	h.mu.Lock()
	h.session = session
	h.mu.Unlock()
}

// CloseAll closes every subscriber. Used when the session goes away; pumps
// see the closed channel and close their connections.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	subs := make([]*Subscriber, 0, len(h.subs))
	for id, s := range h.subs {
		delete(h.subs, id)
		subs = append(subs, s)
	}
	h.mu.Unlock()
	for _, s := range subs {
		s.Close()
	}
}

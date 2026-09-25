package session

import (
	"sync"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// Subscriber is one client's view of a session's event stream.
type Subscriber struct {
	ch chan protocol.Record

	mu     sync.Mutex
	closed bool
	dead   bool // closed because the consumer could not keep up
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
	select {
	case s.ch <- rec:
	default:
		// A subscriber must never stall pi or the actor. Drop it; the client
		// reconnects and resyncs.
		s.dead = true
		s.closed = true
		close(s.ch)
	}
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
func (h *Hub) Subscribe(id string, buffer int) *Subscriber {
	if buffer < 1 {
		buffer = 1
	}
	s := &Subscriber{ch: make(chan protocol.Record, buffer)}
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

// Replay returns retained records with Seq > afterSeq. ok is false when
// afterSeq is older than the retained window; the caller must resync.
func (h *Hub) Replay(afterSeq uint64) ([]protocol.Record, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if afterSeq+1 < h.oldestLocked() {
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

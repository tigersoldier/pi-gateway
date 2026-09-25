package gateway

import (
	"sync"

	"github.com/example/pi-gateway/internal/protocol"
)

// Hub is the per-session, totally ordered event log and fan-out point.
//
// Ordering invariant: Seq is assigned exactly once, in the order records are
// published, and every subscriber observes records in non-decreasing Seq
// order. Publish must therefore only be called from the SessionActor loop
// (and from the single pi reader goroutine via that loop).
type Hub struct {
	mu        sync.RWMutex
	seq       uint64
	sessionID string
	buf       []protocol.Record
	head      int // next write index
	n         int // number of valid records
	subs      map[string]*Subscriber
}

// Subscriber is one client's view of the event stream.
type Subscriber struct {
	ID    string
	lossy bool
	ch    chan protocol.Record

	mu      sync.Mutex
	dropped uint64
	closed  bool
}

// C returns the receive side of the subscriber channel. The channel is closed
// when the subscriber is unsubscribed or disconnected for being slow.
func (s *Subscriber) C() <-chan protocol.Record { return s.ch }

// Dropped reports how many records were dropped for a lossy subscriber.
func (s *Subscriber) Dropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
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
		if s.lossy {
			s.dropped++
			return
		}
		// A non-lossy subscriber must never stall pi. Disconnect it so the
		// client can reconnect and resync from a cursor.
		s.closed = true
		close(s.ch)
	}
}

func (s *Subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}

func NewHub(capacity int, sessionID string) *Hub {
	if capacity < 1 {
		capacity = 1
	}
	return &Hub{
		sessionID: sessionID,
		buf:       make([]protocol.Record, capacity),
		subs:      make(map[string]*Subscriber),
	}
}

func (h *Hub) HeadSeq() uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.seq
}

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

// Publish assigns the next Seq, stamps gw_seq into the record once, stores it
// for replay, and fans it out. Stamping once means every subscriber shares the
// same bytes.
func (h *Hub) Publish(rec protocol.Record) protocol.Record {
	h.mu.Lock()
	h.seq++
	rec.Seq = h.seq
	if stamped, err := protocol.Stamp(rec.Raw, map[string]any{"gw_seq": h.seq}); err == nil {
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

func (h *Hub) Subscribe(id string, buffer int, lossy bool) *Subscriber {
	if buffer < 1 {
		buffer = 1
	}
	s := &Subscriber{
		ID:    id,
		lossy: lossy,
		ch:    make(chan protocol.Record, buffer),
	}
	h.mu.Lock()
	h.subs[id] = s
	h.mu.Unlock()
	return s
}

func (h *Hub) Unsubscribe(id string) {
	h.mu.Lock()
	s := h.subs[id]
	delete(h.subs, id)
	h.mu.Unlock()
	if s != nil {
		s.close()
	}
}

// Replay returns all records with Seq > afterSeq, or ok=false if afterSeq is
// older than the retained window (the caller must resync via snapshot).
func (h *Hub) Replay(afterSeq uint64) ([]protocol.Record, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if afterSeq+1 < h.oldestLocked() {
		return nil, false
	}
	out := make([]protocol.Record, 0, h.n)
	for i := 0; i < h.n; i++ {
		idx := (h.head - h.n + i + len(h.buf)) % len(h.buf)
		rec := h.buf[idx]
		if rec.Seq > afterSeq {
			out = append(out, rec)
		}
	}
	return out, true
}

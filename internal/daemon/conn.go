package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/catalog"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/piargs"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/session"
)

var errUnauthorized = errors.New("daemon: unauthorized")

// outFrame is one queued outgoing frame. gen is 0 for connection-scoped
// frames and the binding generation for session frames, so records from a
// previous binding are dropped after a rebind.
type outFrame struct {
	gen uint64
	rec protocol.Record
}

const (
	// outQueueDepth bounds a connection's pending outgoing frames; a client
	// that stops reading is disconnected (docs/design.md §7).
	outQueueDepth = 1024
	// closeAckWait and closeFlushWait bound the best-effort final flush so an
	// explanatory gw_error is not lost when the socket is dropped.
	closeAckWait   = 250 * time.Millisecond
	closeFlushWait = 50 * time.Millisecond
)

// lossyTail records non-terminal records dropped for an allowLossy client so
// the next delivered record can be preceded by a gw_lag marker. It is only
// touched by send, which runs on both the connection and pump goroutines.
type lossyTail struct {
	mu       sync.Mutex
	from, to uint64
	// metrics is copied from the connection so a drop is counted here.
	metrics *metrics.Registry
}

func (l *lossyTail) note(seq uint64) {
	if seq == 0 {
		return
	}
	l.metrics.Inc(metrics.FramesDropped)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.from == 0 || seq < l.from {
		l.from = seq
	}
	if seq > l.to {
		l.to = seq
	}
}

// frame builds the marker for the pending gap without clearing it.
func (l *lossyTail) frame() (protocol.Record, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.from == 0 {
		return protocol.Record{}, false
	}
	raw, err := protocol.LagFrame(l.from, l.to)
	if err != nil {
		l.from, l.to = 0, 0
		return protocol.Record{}, false
	}
	return protocol.Record{Raw: raw, Type: "gw_lag"}, true
}

func (l *lossyTail) reset() {
	l.mu.Lock()
	l.from, l.to = 0, 0
	l.mu.Unlock()
}

// conn is one client connection. It implements session.Client.
type conn struct {
	d  *Daemon
	nc net.Conn
	id string

	in   *protocol.Codec
	out  chan outFrame
	gen  atomic.Uint64
	done chan struct{}

	// flushReq asks the writer to drain queued frames and acknowledge, so a
	// final gw_error is delivered before the socket is dropped.
	flushReq chan chan struct{}

	closeOnce sync.Once
	wg        sync.WaitGroup

	info    protocol.ClientInfo
	caps    map[string]bool
	granted []string
	piSpec  *piargs.Spec
	lossy   lossyTail
	// allowLossy lets the hub drop non-terminal deltas for this client and
	// report the gap with gw_lag instead of dropping the connection.
	allowLossy bool

	helloDone bool

	mu      sync.Mutex
	session *session.Actor
	sub     *session.Subscriber
}

func newConn(d *Daemon, nc net.Conn) *conn {
	return &conn{
		d:        d,
		nc:       nc,
		id:       d.clientID(),
		in:       protocol.NewCodec(nc, nc),
		out:      make(chan outFrame, outQueueDepth),
		done:     make(chan struct{}),
		flushReq: make(chan chan struct{}, 1),
		lossy:    lossyTail{metrics: d.metrics},
	}
}

// session.Client implementation.

func (c *conn) ID() string                 { return c.id }
func (c *conn) Kind() string               { return c.info.Kind }
func (c *conn) Name() string               { return c.info.Name }
func (c *conn) Has(capability string) bool { return c.caps[capability] }
func (c *conn) Ref() protocol.ClientRef {
	return protocol.ClientRef{ClientID: c.id, Kind: c.info.Kind, Name: c.info.Name}
}
func (c *conn) Summary() protocol.ClientSummary {
	return protocol.ClientSummary{
		ClientID:     c.id,
		Name:         c.info.Name,
		Kind:         c.info.Kind,
		Capabilities: c.granted,
		Tags:         c.info.Tags,
	}
}

// run reads frames until the peer disconnects or the daemon closes the
// connection, then waits for the writer to drain.
func (c *conn) run() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.writeLoop()
	}()
	c.readLoop()
	c.close()
	c.wg.Wait()
}

func (c *conn) readLoop() {
	for {
		raw, err := c.in.Read()
		if err != nil {
			return
		}
		c.d.metrics.Inc(metrics.FramesIn)
		c.d.metrics.Add(metrics.BytesIn, int64(len(raw)))
		if !c.helloDone {
			if err := c.handleHello(raw); err != nil {
				c.closeAfterFlush()
				return
			}
			c.helloDone = true
			continue
		}
		c.handleFrame(raw)
	}
}

func (c *conn) writeLoop() {
	for {
		select {
		case f := <-c.out:
			if !c.writeOne(f) {
				return
			}
		case ack := <-c.flushReq:
			for len(c.out) > 0 {
				select {
				case f := <-c.out:
					if !c.writeOne(f) {
						return
					}
				default:
				}
			}
			close(ack)
		case <-c.done:
			return
		}
	}
}

// writeOne transforms and writes a single frame. It returns false when the
// writer must stop (connection closed or write error).
func (c *conn) writeOne(f outFrame) bool {
	if f.gen != 0 && f.gen != c.gen.Load() {
		return true // record from a previous binding
	}
	raw, ok := c.transform(f.rec)
	if !ok {
		return true
	}
	if err := c.in.WriteRaw(raw); err != nil {
		c.close()
		return false
	}
	c.d.metrics.Inc(metrics.FramesOut)
	c.d.metrics.Add(metrics.BytesOut, int64(len(raw)))
	return true
}

// transform applies per-recipient routing: responses go only to their
// originator with the local id restored; other owner-tagged events are
// broadcast with gw_owner.
func (c *conn) transform(rec protocol.Record) ([]byte, bool) {
	switch {
	case rec.Type == "response":
		if rec.Owner != "" && rec.Owner != c.id {
			return nil, false
		}
		if rec.Owner == c.id {
			return protocol.RestoreID(rec.Raw, c.id), true
		}
		return rec.Raw, true
	case rec.Type == "gw_error":
		if rec.Owner != "" && rec.Owner != c.id {
			return nil, false
		}
		return rec.Raw, true
	case rec.Owner != "" && rec.Type == "extension_ui_request":
		// Dialogs are routed to one client (the turn author or a fallback);
		// others must neither see nor answer them (docs/design.md §9).
		if rec.Owner == c.id {
			return rec.Raw, true
		}
		return nil, false
	case rec.Owner != "" && rec.Type == "bash_execution_update":
		if rec.Owner == c.id {
			return protocol.RestoreID(rec.Raw, c.id), true
		}
		out, err := protocol.Stamp(rec.Raw, map[string]any{"gw_owner": rec.Owner})
		if err != nil {
			return nil, false
		}
		return out, true
	}
	return rec.Raw, true
}

// send queues one frame. For an allowLossy client that has fallen behind,
// non-terminal records are dropped and reported later with a gw_lag marker
// instead of closing the connection; terminal records still fail the send so
// the pump can disconnect the client with slow_consumer.
func (c *conn) send(f outFrame) bool {
	if marker, ok := c.lossy.frame(); ok {
		select {
		case c.out <- outFrame{gen: f.gen, rec: marker}:
			c.d.metrics.Inc(metrics.ClientLags)
			c.lossy.reset()
		default:
			if c.droppable(f) {
				c.lossy.note(f.rec.Seq)
				return true
			}
			return false
		}
	}
	select {
	case c.out <- f:
		return true
	case <-c.done:
		return false
	default:
		if c.droppable(f) {
			c.lossy.note(f.rec.Seq)
			return true
		}
		return false
	}
}

// droppable reports whether this frame may be dropped for a lossy client: only
// session-log records that are not terminal.
func (c *conn) droppable(f outFrame) bool {
	return c.allowLossy && f.gen != 0 && f.rec.Seq != 0 && !session.IsTerminal(f.rec.Type)
}

func (c *conn) sendRaw(raw []byte, typ string) {
	_ = c.send(outFrame{rec: protocol.Record{Raw: raw, Type: typ}})
}

func (c *conn) sendJSON(v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		return
	}
	c.sendRaw(raw, protocol.Field(raw, "type"))
}

func (c *conn) sendError(code, msg string) {
	c.sendJSON(protocol.NewError(code, msg))
}

func (c *conn) sendResponse(id, command string, success bool, code, msg string, data json.RawMessage) {
	c.sendRaw(protocol.Response(id, command, success, code, msg, data), "response")
}

func (c *conn) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.nc.Close()
		c.mu.Lock()
		sub, a := c.sub, c.session
		c.sub, c.session = nil, nil
		c.mu.Unlock()
		if a != nil {
			a.Unsubscribe(c.id)
			a.Detach(c.id)
		} else if sub != nil {
			sub.Close()
		}
	})
}

// closeAfterFlush closes the connection after the writer drains frames that
// are already queued, so an explanatory gw_error is not lost in the race
// between queueing it and dropping the socket.
func (c *conn) closeAfterFlush() {
	ack := make(chan struct{})
	select {
	case c.flushReq <- ack:
		select {
		case <-ack:
		case <-time.After(closeAckWait):
		}
	case <-time.After(closeFlushWait):
	case <-c.done:
	}
	c.close()
}

// closeSlow is used when the client stops reading: make one best-effort
// attempt to tell it why, then drop the connection.
func (c *conn) closeSlow() {
	if raw, err := json.Marshal(protocol.NewError(protocol.CodeSlowConsumer,
		"client is not reading; reconnect and resync")); err == nil {
		select {
		case c.out <- outFrame{rec: protocol.Record{Raw: raw, Type: "gw_error"}}:
		default:
		}
	}
	c.close()
}

// ---------------------------------------------------------------------------
// Handshake

func (c *conn) handleHello(raw []byte) error {
	h, err := protocol.ParseHello(raw)
	if err != nil {
		code := protocol.CodeBadFrame
		if errors.Is(err, protocol.ErrUnsupportedVersion) {
			code = protocol.CodeNotSupported
		}
		c.sendError(code, err.Error())
		return err
	}
	grant, ok := c.d.lookUpToken(h.Token)
	if !ok {
		c.d.metrics.Inc(metrics.Unauthorized)
		c.sendError(protocol.CodeUnauthorized, "missing or wrong token")
		return errUnauthorized
	}
	c.allowLossy = h.AllowLossy
	c.info = h.Client
	if c.info.Name == "" {
		c.info.Name = "client"
	}
	if c.info.Kind == "" {
		c.info.Kind = "integration"
	}
	c.granted, c.caps = grantCapabilities(h.Client.Capabilities, grant.Capabilities)

	spec, err := piargs.Parse(h.PiArgs)
	if err != nil {
		c.sendError(protocol.CodeBadFrame, err.Error())
		return err
	}
	c.piSpec = spec

	var actor *session.Actor
	var attachErr error
	if h.Session != "" {
		actor, attachErr = c.d.attach(h.Session, spec)
	}
	// Subscribe before reading the replay window: records published between the
	// two must land in the subscriber buffer (the pump's watermark drops the
	// ones the replay window already covered).
	if actor != nil && attachErr == nil {
		if err := c.bind(actor, false); err != nil {
			c.sendError(protocol.CodeSessionCrashed, err.Error())
			return nil
		}
	}

	// Decide replay before sending the welcome so resyncRequired is accurate.
	// Without observe there is no transcript: no replay and no snapshot.
	replayRequested := h.Resume != nil && !h.LiveOnly && c.Has(protocol.CapObserve)
	var replay []protocol.Record
	resync := false
	if actor != nil && replayRequested {
		var ok bool
		replay, ok = actor.Replay(h.Resume.SinceSeq)
		resync = !ok
	}
	if resync && actor != nil && c.Has(protocol.CapObserve) && h.Resume.LeafEntryID != "" {
		// The cursor is not replayable, but the client's durable transcript may
		// already match the session file: a daemon restart empties the ring
		// while the file keeps the leaf (docs/protocol.md §6).
		if leaf, err := catalog.LeafID(actor.Path()); err == nil && leaf != "" && leaf == h.Resume.LeafEntryID {
			resync = false
			replayRequested = false // no replay and no snapshot: attach live
		}
	}

	w := protocol.Welcome{
		Type:           "gw_welcome",
		Protocol:       protocol.Version,
		ClientID:       c.id,
		Kind:           c.info.Kind,
		Granted:        c.granted,
		PiVersion:      c.d.piVersion,
		Concurrency:    "queue",
		ResyncRequired: resync,
		Turn:           protocol.TurnState{State: "idle"},
	}
	if actor != nil {
		// Ask pi for its state first: the response teaches the actor the
		// session name and id, so the welcome can report them.
		if rec, err := actor.Call([]byte(`{"type":"get_state"}`), 5*time.Second); err == nil {
			w.PiState = protocol.DataField(rec.Raw)
		}
		info := actor.Info()
		sessionID := actor.SessionID()
		if sessionID == "" {
			sessionID = info.Path
		}
		w.Session = &protocol.SessionRef{Path: info.Path, Name: actor.SessionName(), ID: sessionID}
		w.HeadSeq, w.OldestSeq = info.HeadSeq, info.OldestSeq
		w.Turn = info.Turn
		w.Clients = info.Clients
	}
	c.sendJSON(&w)

	if attachErr != nil {
		c.d.metrics.Inc(metrics.AttachFailures)
		c.sendError(errorCode(attachErr), attachErr.Error())
		return nil
	}
	if actor == nil {
		return nil
	}

	// The replay window (or snapshot) must reach the client before any live
	// record; the pump starts only after those frames are queued, and drops
	// records the window already covered.
	var watermark uint64
	switch {
	case resync && !c.Has(protocol.CapObserve):
		// No observe: attach without replaying or snapshotting the transcript.
	case resync:
		if snap, err := actor.Snapshot(); err != nil {
			c.sendError(protocol.CodeResyncRequired, err.Error())
		} else {
			c.sendJSON(&snap)
		}
		// Live records resume from the head captured after the snapshot reads.
		watermark = actor.Info().HeadSeq
	case replayRequested:
		watermark = h.Resume.SinceSeq
		gen := c.gen.Load()
		for _, rec := range replay {
			if !c.send(outFrame{gen: gen, rec: rec}) {
				c.closeSlow()
				return nil
			}
			watermark = rec.Seq
		}
		c.sendJSON(protocol.ReplayDone{Type: "gw_replay_done", HeadSeq: watermark})
	}
	c.startPump(watermark)
	return nil
}

// ---------------------------------------------------------------------------
// Binding and rebinding

func (c *conn) bound() *session.Actor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

// bind subscribes to a session and registers this connection with it. The
// subscription is created before the actor learns about the client, so no
// event is lost in between; records published before the pump starts are held
// in the subscriber buffer.
//
// startLive starts the live pump immediately (rebinds and lazy creation).
// The hello path passes false because it must queue the replay window or
// snapshot before any live record; it calls startPump itself afterwards.
func (c *conn) bind(a *session.Actor, startLive bool) error {
	c.unbind()
	sub := a.Subscribe(c.id, session.SubOptions{
		Buffer:     c.d.cfg.SubscriberBuffer,
		AllowLossy: c.allowLossy,
		// A client without observe receives only records addressed to it (its
		// own responses and dialogs), never the session's event stream.
		FilterUnowned: !c.Has(protocol.CapObserve),
	})
	c.gen.Add(1) // invalidate frames queued for the previous binding
	if !c.d.registerClient(a, c) {
		sub.Close()
		return errors.New("session is shutting down")
	}
	c.mu.Lock()
	c.session, c.sub = a, sub
	c.mu.Unlock()
	if startLive {
		c.startPump(0)
	}
	return nil
}

// startPump begins delivering live records, dropping any whose sequence the
// replay window or snapshot already covered.
func (c *conn) startPump(watermark uint64) {
	c.mu.Lock()
	sub, a := c.sub, c.session
	c.mu.Unlock()
	if sub == nil || a == nil {
		return
	}
	go c.pump(sub, c.gen.Load(), watermark)
}

func (c *conn) unbind() {
	c.mu.Lock()
	sub, a := c.sub, c.session
	c.sub, c.session = nil, nil
	c.mu.Unlock()
	if a != nil {
		a.Unsubscribe(c.id)
		a.Detach(c.id)
	} else if sub != nil {
		sub.Close()
	}
}

func (c *conn) pump(s *session.Subscriber, gen, watermark uint64) {
	co := &coalescer{}
	ticker := time.NewTicker(c.d.cfg.DeltaFlush)
	defer ticker.Stop()
	send := func(rec protocol.Record) bool { return c.send(outFrame{gen: gen, rec: rec}) }
	flush := func() bool {
		for _, rec := range co.flush() {
			if !send(rec) {
				return false
			}
		}
		return true
	}
	for {
		select {
		case rec, ok := <-s.C():
			if !ok {
				if !c.isCurrent(s) {
					return // rebound or disconnected
				}
				_ = flush()
				if s.Dead() {
					c.closeSlow()
				} else {
					c.sendError(protocol.CodeSessionCrashed, "session stopped")
					c.closeAfterFlush()
				}
				return
			}
			if rec.Seq != 0 && rec.Seq <= watermark {
				continue // already covered by the replay window
			}
			for _, out := range co.offer(rec) {
				if !send(out) {
					c.closeSlow()
					return
				}
			}
		case <-ticker.C:
			if !co.pending() {
				continue
			}
			if !flush() {
				c.closeSlow()
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *conn) isCurrent(s *session.Subscriber) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sub == s
}

// ---------------------------------------------------------------------------
// Frames

func (c *conn) handleFrame(raw []byte) {
	typ := protocol.Field(raw, "type")
	if typ == "" {
		c.sendError(protocol.CodeBadFrame, "frame has no type")
		return
	}
	if typ != strings.ToLower(typ) {
		// Command types are canonical; a case variant would dodge the
		// capability table and could lazily create a session.
		c.sendError(protocol.CodeBadFrame, fmt.Sprintf("unknown command %q", typ))
		return
	}
	id := protocol.Field(raw, "id")
	// Capability enforcement happens here, once, before any command is
	// dispatched or can lazily create a session (docs/design.md §10). The
	// actor only enforces ownership (for example which client owns a dialog).
	if capability := protocol.CommandCapability(typ); capability != "" {
		if !c.require(id, typ, capability) {
			return
		}
	}
	switch typ {
	case "gw_ping":
		c.sendJSON(protocol.Pong{Type: "gw_pong"})
		return
	case "gw_bye":
		c.close()
		return
	case "gw_hello":
		c.sendError(protocol.CodeBadFrame, "duplicate gw_hello")
		return
	case "gw_list_sessions":
		c.handleListSessions(raw)
		return
	case "gw_new_session":
		c.handleGWNewSession(raw)
		return
	case "gw_reload_session":
		c.handleReload(raw)
		return
	}
	if protocol.IsGatewayType(typ) {
		c.sendError(protocol.CodeNotSupported, fmt.Sprintf("%s is not supported by this daemon revision", typ))
		return
	}

	switch typ {
	case "switch_session":
		c.handleSwitch(raw, id)
		return
	case "new_session":
		c.handleNewSession(id)
		return
	case "fork", "clone":
		if c.bound() == nil {
			c.sendResponse(id, typ, false, protocol.CodeUnknownSession, "not attached to a session", nil)
			return
		}
		// The actor owns the shared-session policy, since only it knows how
		// many clients are attached.
	}

	a := c.bound()
	if a == nil {
		// Lazy binding: the first session-scoped command creates a session
		// (pilish's fresh-session flow).
		actor, ok := c.bindNew(id, typ, c.piSpec)
		if !ok {
			return
		}
		a = actor
	}
	if err := a.Submit(session.ClientCommand{Client: c, LocalID: id, Type: typ, Raw: raw}); err != nil {
		c.sendResponse(id, typ, false, protocol.CodeQueueFull, err.Error(), nil)
	}
}

func (c *conn) handleSwitch(raw []byte, id string) {
	path := protocol.Field(raw, "sessionPath")
	if path == "" {
		path = protocol.Field(raw, "session")
	}
	if path == "" {
		c.sendResponse(id, "switch_session", false, protocol.CodeUnknownSession, "missing sessionPath", nil)
		return
	}
	actor, err := c.d.attach(path, c.piSpec)
	if err != nil {
		c.d.metrics.Inc(metrics.AttachFailures)
		c.sendResponse(id, "switch_session", false, errorCode(err), err.Error(), nil)
		return
	}
	if err := c.bind(actor, true); err != nil {
		c.sendResponse(id, "switch_session", false, protocol.CodeSessionCrashed, err.Error(), nil)
		return
	}
	c.sendResponse(id, "switch_session", true, "", "", []byte(`{"cancelled":false}`))
}

// handleNewSession creates a session and rebinds only this connection.
func (c *conn) handleNewSession(id string) {
	if _, ok := c.bindNew(id, "new_session", c.piSpec); !ok {
		return
	}
	c.sendResponse(id, "new_session", true, "", "", []byte(`{"cancelled":false}`))
}

// bindNew creates a session with spec and binds this connection to it. On
// failure it answers the triggering command with session_crashed and reports
// false.
func (c *conn) bindNew(id, command string, spec *piargs.Spec) (*session.Actor, bool) {
	actor, err := c.d.create(spec)
	if err != nil {
		c.sendResponse(id, command, false, protocol.CodeSessionCrashed, err.Error(), nil)
		return nil, false
	}
	if err := c.bind(actor, true); err != nil {
		c.sendResponse(id, command, false, protocol.CodeSessionCrashed, err.Error(), nil)
		return nil, false
	}
	return actor, true
}

// require checks a capability and replies with `forbidden` when missing.
func (c *conn) require(id, command, capability string) bool {
	if c.Has(capability) {
		return true
	}
	c.sendResponse(id, command, false, protocol.CodeForbidden,
		fmt.Sprintf("%s requires the %s capability", command, capability), nil)
	return false
}

// handleListSessions answers the session catalog query (docs/protocol.md §3.2).
func (c *conn) handleListSessions(raw []byte) {
	id := protocol.Field(raw, "id")
	var req struct {
		Filter struct {
			Cwd   string `json:"cwd"`
			Live  bool   `json:"live"`
			Limit int    `json:"limit"`
		} `json:"filter"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		c.sendResponse(id, "gw_list_sessions", false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	rows := c.d.listSessions(req.Filter.Cwd, req.Filter.Live, req.Filter.Limit)
	body, err := json.Marshal(map[string]any{"sessions": rows})
	if err != nil {
		c.sendResponse(id, "gw_list_sessions", false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	c.sendResponse(id, "gw_list_sessions", true, "", "", body)
}

// handleGWNewSession creates a session explicitly and rebinds the requester
// (docs/protocol.md §3.3).
func (c *conn) handleGWNewSession(raw []byte) {
	id := protocol.Field(raw, "id")
	var req struct {
		Name   string            `json:"name"`
		PiArgs []string          `json:"piArgs"`
		Tags   map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		c.sendResponse(id, "gw_new_session", false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	spec, err := piargs.Parse(req.PiArgs)
	if err != nil {
		c.sendResponse(id, "gw_new_session", false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	if req.Name != "" {
		spec.SetName(req.Name)
	}
	actor, ok := c.bindNew(id, "gw_new_session", spec)
	if !ok {
		return
	}
	c.d.setCreated(actor, c.Ref(), req.Tags)
	// The session file is only known once pi has reported it.
	rec, err := actor.Call([]byte(`{"type":"get_state"}`), 10*time.Second)
	if err != nil {
		c.sendResponse(id, "gw_new_session", false, protocol.CodeSessionCrashed, err.Error(), nil)
		return
	}
	st := protocol.ParsePiState(rec.Raw)
	if st.SessionFile == "" {
		c.sendResponse(id, "gw_new_session", false, protocol.CodeSessionCrashed,
			"pi reported no session file (is --no-session set?)", nil)
		return
	}
	body, err := json.Marshal(map[string]any{
		"path": st.SessionFile, "name": st.SessionName, "sessionId": st.SessionID,
	})
	if err != nil {
		c.sendResponse(id, "gw_new_session", false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	c.sendResponse(id, "gw_new_session", true, "", "", body)
}

// handleReload restarts pi for a session (docs/protocol.md §3.6).
func (c *conn) handleReload(raw []byte) {
	id := protocol.Field(raw, "id")
	var req struct {
		Session string `json:"session"`
		Force   bool   `json:"force"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		c.sendResponse(id, "gw_reload_session", false, protocol.CodeBadFrame, err.Error(), nil)
		return
	}
	var actor *session.Actor
	if req.Session == "" {
		actor = c.bound()
		if actor == nil {
			c.sendResponse(id, "gw_reload_session", false, protocol.CodeUnknownSession,
				"not attached to a session", nil)
			return
		}
	} else {
		var err error
		actor, err = c.d.liveActor(req.Session)
		if err != nil {
			c.sendResponse(id, "gw_reload_session", false, errorCode(err), err.Error(), nil)
			return
		}
	}
	ref := c.Ref()
	// The response goes straight to this connection: the target actor is not
	// necessarily the one this client is subscribed to, so publishing it on
	// that session's hub would drop it.
	err := actor.Restart(req.Force, c.id, &ref, 30*time.Second)
	switch {
	case err == nil:
		c.sendResponse(id, "gw_reload_session", true, "", "", nil)
	case errors.Is(err, session.ErrReloadBusy):
		c.sendResponse(id, "gw_reload_session", false, protocol.CodeReloadBusy,
			"a turn is running or another client is attached", nil)
	default:
		c.sendResponse(id, "gw_reload_session", false, protocol.CodeSessionCrashed, err.Error(), nil)
	}
}

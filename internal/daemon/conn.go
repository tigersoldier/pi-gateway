package daemon

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

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
		out:      make(chan outFrame, 1024),
		done:     make(chan struct{}),
		flushReq: make(chan chan struct{}, 1),
	}
}

// session.Client implementation.

func (c *conn) ID() string                 { return c.id }
func (c *conn) Info() protocol.ClientInfo  { return c.info }
func (c *conn) Kind() string               { return c.info.Kind }
func (c *conn) Name() string               { return c.info.Name }
func (c *conn) Has(capability string) bool { return c.caps[capability] }
func (c *conn) Ref() protocol.ClientRef {
	return protocol.ClientRef{ClientID: c.id, Kind: c.info.Kind, Name: c.info.Name}
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

func (c *conn) send(f outFrame) bool {
	select {
	case c.out <- f:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
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
		case <-time.After(250 * time.Millisecond):
		}
	case <-time.After(50 * time.Millisecond):
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
		c.sendError(protocol.CodeBadFrame, err.Error())
		return err
	}
	if subtle.ConstantTimeCompare([]byte(h.Token), []byte(c.d.cfg.Token)) != 1 {
		c.sendError(protocol.CodeUnauthorized, "missing or wrong token")
		return errUnauthorized
	}
	c.info = h.Client
	if c.info.Name == "" {
		c.info.Name = "client"
	}
	if c.info.Kind == "" {
		c.info.Kind = "integration"
	}
	c.granted, c.caps = grantCapabilities(h.Client.Capabilities)

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

	// Decide replay before sending the welcome so resyncRequired is accurate.
	replayRequested := h.Resume != nil && !h.LiveOnly
	var replay []protocol.Record
	resync := false
	if actor != nil && replayRequested {
		var ok bool
		replay, ok = actor.Replay(h.Resume.SinceSeq)
		resync = !ok
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
		info := actor.Info()
		name := actor.SessionName()
		w.Session = &protocol.SessionRef{Path: info.Path, Name: name, ID: info.Path}
		w.HeadSeq, w.OldestSeq = info.HeadSeq, info.OldestSeq
		w.Turn = info.Turn
		w.Clients = info.Clients
		if rec, err := actor.Call([]byte(`{"type":"get_state"}`), 5*time.Second); err == nil {
			w.PiState = protocol.DataField(rec.Raw)
		}
	}
	c.sendJSON(&w)

	if attachErr != nil {
		c.sendError(errorCode(attachErr), attachErr.Error())
		return nil
	}
	if actor == nil {
		return nil
	}
	if err := c.bind(actor, false); err != nil {
		c.sendError(protocol.CodeSessionCrashed, err.Error())
		return nil
	}

	// The replay window (or snapshot) must reach the client before any live
	// record; the pump starts only after those frames are queued, and drops
	// records the window already covered.
	var watermark uint64
	switch {
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
	sub := a.Subscribe(c.id, 1024)
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
	for {
		select {
		case rec, ok := <-s.C():
			if !ok {
				if !c.isCurrent(s) {
					return // rebound or disconnected
				}
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
			if !c.send(outFrame{gen: gen, rec: rec}) {
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
	}
	if protocol.IsGatewayType(typ) {
		c.sendError(protocol.CodeNotSupported, fmt.Sprintf("%s is not supported by this daemon revision", typ))
		return
	}

	id := protocol.Field(raw, "id")
	switch typ {
	case "switch_session":
		c.handleSwitch(raw, id)
		return
	case "new_session":
		c.handleNewSession(id)
		return
	case "fork", "clone":
		// Forwarding these would move the live pi process to a new file, which
		// is unsafe while other clients are attached; adoption lands in M2.
		c.sendResponse(id, typ, false, protocol.CodeNotSupported,
			"fork/clone are not supported in this revision", nil)
		return
	}

	a := c.bound()
	if a == nil {
		// Lazy binding: the first session-scoped command creates a session
		// (pilish's fresh-session flow).
		actor, err := c.d.create(c.piSpec)
		if err != nil {
			c.sendError(protocol.CodeSessionCrashed, err.Error())
			return
		}
		if err := c.bind(actor, true); err != nil {
			c.sendError(protocol.CodeSessionCrashed, err.Error())
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
	actor, err := c.d.create(c.piSpec)
	if err != nil {
		c.sendResponse(id, "new_session", false, protocol.CodeSessionCrashed, err.Error(), nil)
		return
	}
	if err := c.bind(actor, true); err != nil {
		c.sendResponse(id, "new_session", false, protocol.CodeSessionCrashed, err.Error(), nil)
		return
	}
	c.sendResponse(id, "new_session", true, "", "", []byte(`{"cancelled":false}`))
}

// grantCapabilities intersects requested capabilities with the token's role.
// The daemon-generated token grants the full set, so a request for nothing
// means everything (see docs/protocol.md §2).
func grantCapabilities(requested []string) ([]string, map[string]bool) {
	known := make(map[string]bool, len(protocol.AllCapabilities))
	for _, cap := range protocol.AllCapabilities {
		known[cap] = true
	}
	set := make(map[string]bool)
	if len(requested) == 0 {
		for _, cap := range protocol.AllCapabilities {
			set[cap] = true
		}
	} else {
		for _, cap := range requested {
			if known[cap] {
				set[cap] = true
			}
		}
	}
	granted := make([]string, 0, len(set))
	for _, cap := range protocol.AllCapabilities {
		if set[cap] {
			granted = append(granted, cap)
		}
	}
	return granted, set
}

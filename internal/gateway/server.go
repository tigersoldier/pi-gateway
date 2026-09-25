package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

// helloMessage is the first frame a gateway-protocol client must send. The
// `connect` bridge uses LiveOnly to attach without replaying history.
type helloMessage struct {
	Type      string `json:"type"`
	Protocol  int    `json:"protocol"`
	SessionID string `json:"sessionId"`
	Client    struct {
		Name         string   `json:"name"`
		InstanceID   string   `json:"instanceId"`
		Capabilities []string `json:"capabilities"`
	} `json:"client"`
	Resume struct {
		SinceSeq    uint64 `json:"sinceSeq"`
		LeafEntryID string `json:"leafEntryID"`
	} `json:"resume"`
	AllowLossy bool `json:"allowLossy"`
	// LiveOnly attaches at the current head without replay. A raw pi front-end
	// (the `connect` bridge) uses this so its UI sees only live traffic and
	// rebuilds state via get_state/get_messages/get_entries.
	LiveOnly bool `json:"liveOnly"`
}

// allCapabilities is the capability vocabulary.
var allCapabilities = map[string]bool{
	"observe": true, "interject": true, "prompt": true,
	"ui": true, "control": true, "admin": true,
}

// Server owns the listener, the connection table, and the session registry.
// It speaks only the gateway protocol (gw_hello + gw_*); raw pi clients are
// served by the separate `connect` bridge process.
type Server struct {
	mu     sync.RWMutex
	conns  map[string]*Conn
	actors map[SessionID]*Actor
	nextID uint64

	piCfg   PiConfig
	piArgs  func(SessionID) []string
	hubCap  int
	mode    Mode
	rootCtx context.Context

	// singleSession, when set, rejects gw_hello for any other session id. This
	// prevents two pi processes sharing one session file when piArgs do not
	// map session ids to distinct files.
	singleSession SessionID
}

func NewServer(ctx context.Context, cfg PiConfig, piArgs func(SessionID) []string, hubCapacity int, mode Mode) *Server {
	if hubCapacity < 1 {
		hubCapacity = 8192
	}
	return &Server{
		conns:   make(map[string]*Conn),
		actors:  make(map[SessionID]*Actor),
		piCfg:   cfg,
		piArgs:  piArgs,
		hubCap:  hubCapacity,
		mode:    mode,
		rootCtx: ctx,
	}
}

// SetSingleSession pins the server to one session id. Requires a session id on
// every gw_hello; mismatches are rejected.
func (s *Server) SetSingleSession(id SessionID) { s.singleSession = id }

// EnsureSession returns the actor for id, creating and starting it if needed.
func (s *Server) EnsureSession(id SessionID) (*Actor, error) {
	s.mu.RLock()
	a := s.actors[id]
	s.mu.RUnlock()
	if a != nil {
		return a, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if a = s.actors[id]; a != nil {
		return a, nil
	}
	cfg := s.piCfg
	if s.piArgs != nil {
		cfg.Args = s.piArgs(id)
	}
	a, err := NewActor(s.rootCtx, id, cfg, s.hubCap, s.mode)
	if err != nil {
		return nil, err
	}
	a.SetClients(s)
	s.actors[id] = a
	go a.Run()
	return a, nil
}

// Actor returns an existing session actor (used by tests and tooling).
func (s *Server) Actor(id SessionID) (*Actor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.actors[id]
	return a, ok
}

// SendTo implements the Clients interface used by Actor for targeted sends.
func (s *Server) SendTo(clientID string, raw []byte) bool {
	s.mu.RLock()
	c := s.conns[clientID]
	s.mu.RUnlock()
	if c == nil {
		return false
	}
	c.SendRaw(raw)
	return true
}

func (s *Server) removeConn(c *Conn) {
	s.mu.Lock()
	delete(s.conns, c.id)
	s.mu.Unlock()
}

func (s *Server) addConn(c *Conn) {
	s.mu.Lock()
	s.conns[c.id] = c
	s.mu.Unlock()
}

func (s *Server) nextClientID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	return fmt.Sprintf("c_%d", s.nextID)
}

// Serve accepts connections until ln is closed or ctx is cancelled.
func (s *Server) Serve(ln net.Listener) error {
	go func() {
		<-s.rootCtx.Done()
		_ = ln.Close()
	}()
	for {
		nc, err := ln.Accept()
		if err != nil {
			select {
			case <-s.rootCtx.Done():
				return nil
			default:
				return err
			}
		}
		go s.handleConn(nc)
	}
}

func (s *Server) handleConn(st stream) {
	codec := protocol.NewCodec(st, st)

	setReadDeadline(st, 15*time.Second)
	first, err := codec.Read()
	if err != nil {
		_ = st.Close()
		return
	}
	clearReadDeadline(st)

	var hello helloMessage
	if json.Unmarshal(first, &hello) != nil || hello.Type != "gw_hello" {
		_ = codec.WriteJSON(errorResponse("", "gw_hello", "bad_frame", "expected gw_hello", ""))
		_ = st.Close()
		return
	}
	s.serveHello(st, codec, hello)
}

func (s *Server) serveHello(st stream, codec *protocol.Codec, hello helloMessage) {
	if hello.Protocol != protocol.Version {
		_ = codec.WriteJSON(errorResponse("", "gw_hello", "bad_frame",
			fmt.Sprintf("unsupported protocol version %d", hello.Protocol), ""))
		_ = st.Close()
		return
	}
	if hello.SessionID == "" {
		_ = codec.WriteJSON(errorResponse("", "gw_hello", "bad_frame", "sessionId required", ""))
		_ = st.Close()
		return
	}
	if s.singleSession != "" && SessionID(hello.SessionID) != s.singleSession {
		_ = codec.WriteJSON(errorResponse("", "gw_hello", "bad_frame",
			fmt.Sprintf("unknown session %q (this server serves %q)", hello.SessionID, s.singleSession), ""))
		_ = st.Close()
		return
	}

	actor, err := s.EnsureSession(SessionID(hello.SessionID))
	if err != nil {
		_ = codec.WriteJSON(errorResponse("", "gw_hello", "session_crashed", err.Error(), ""))
		_ = st.Close()
		return
	}

	caps := map[string]bool{"observe": true}
	for _, cap := range hello.Client.Capabilities {
		if allCapabilities[cap] {
			caps[cap] = true
		}
	}

	c := &Conn{
		id:      s.nextClientID(),
		name:    hello.Client.Name,
		caps:    caps,
		session: SessionID(hello.SessionID),
		actor:   actor,
		server:  s,
		st:      st,
		codec:   codec,
		sendCh:  make(chan []byte, 1024),
		closed:  make(chan struct{}),
		lossy:   hello.AllowLossy,
	}
	s.addConn(c)

	head, resync := c.writeHandshake(hello)
	s.publishPresence(c, "join")

	go c.writer(head)
	if resync {
		// Cursor too old or unknown: ask the actor to build a snapshot.
		if !actor.Submit(command{client: c, localID: "", raw: []byte(`{"type":"gw_request_snapshot"}`), typ: "gw_request_snapshot"}) {
			c.SendJSON(errorResponse("", "gw_request_snapshot", "queue_full", "try again", ""))
		}
	}

	c.readLoop()
}

func (s *Server) publishPresence(c *Conn, event string) {
	a, ok := s.Actor(c.session)
	if !ok {
		return
	}
	a.publish(map[string]any{
		"type":  "gw_presence",
		"event": event,
		"client": map[string]any{
			"clientId":     c.id,
			"name":         c.name,
			"capabilities": capabilityList(c.caps),
		},
	})
}

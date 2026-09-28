// Package gwclient is the exported client library for pi-gatewayd.
//
// It speaks the gateway protocol (docs/protocol.md) to a running daemon, so a
// program — a chat bot, a dashboard, a test harness — can attach to pi
// sessions without spawning a UI or a bridge:
//
//	c, err := gwclient.Dial(ctx, gwclient.Config{Name: "slack", Kind: "bot"})
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	sess, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: "/work/repo"})
//	if err != nil {
//		return err
//	}
//	if _, err := c.Prompt(ctx, "summarise the open issues"); err != nil {
//		return err
//	}
//	for ev := range c.Events() {
//		if ev.Type == "message_end" {
//			// render the assistant message to the chat
//		}
//	}
//
// Discovery mirrors the bridge (docs/protocol.md §1): an explicit Addr or
// Port wins, then the daemon's port file, then 127.0.0.1:7331. The token comes
// from Config.Token, Config.TokenFile, or <state dir>/token.
//
// # Concurrency model
//
// One goroutine reads the connection. Responses are matched to the Do call
// that issued them by id; every other frame is delivered on Events(). The
// event queue is unbounded up to Config.MaxPendingEvents, after which the
// client fails with ErrEventBufferFull rather than dropping frames silently,
// so a long-lived consumer must drain Events() (or finish a turn with
// RecvTurn-like loops) instead of only calling Do.
//
// A Client is safe for concurrent use. Notifications (Send) never wait; Do
// waits for the matching response, the context deadline, or the connection
// ending.
//
// # Reconnects
//
// A long-lived consumer can keep the cursor it has consumed (Cursor, LastSeq,
// LeafID) and dial again with Config.Resume set; Reconnect does exactly that
// from the client whose connection failed. An unreplayable cursor is answered
// with a snapshot instead of a gap (Welcome.ResyncRequired, Event.Snapshot).
package gwclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/config"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// Defaults applied to a Config field left zero.
const (
	DefaultName             = "pi-gateway-client"
	DefaultKind             = "integration"
	DefaultDialTimeout      = 10 * time.Second
	DefaultRequestTimeout   = 30 * time.Second
	DefaultMaxPendingEvents = 65536
)

// Errors reported through Err() and returned by Do.
var (
	// ErrClosed means Close was called (or the connection ended and Close
	// already claimed it).
	ErrClosed = errors.New("gwclient: client closed")
	// ErrEventBufferFull means the caller did not drain Events() and
	// MaxPendingEvents frames backed up; the client is closed so the loss is
	// explicit instead of silent.
	ErrEventBufferFull = errors.New("gwclient: event buffer full; drain Events()")
)

// Config configures a Dial.
type Config struct {
	// Connection. Addr wins over Port, which wins over discovery.
	Addr        string // explicit daemon "host:port"
	Port        int    // explicit daemon port on 127.0.0.1
	StateDir    string // overrides config.Dir()
	Token       string // explicit token value; wins over TokenFile
	TokenFile   string // default: <state dir>/token
	DialTimeout time.Duration

	// Identity sent in gw_hello (docs/protocol.md §2).
	Name         string
	Kind         string
	Capabilities []string // default: the full set
	Tags         map[string]string

	// Attach. Session may be a session file path or a session name; empty
	// leaves the connection unbound until its first session command.
	Session string
	// Cwd is the directory a session this connection creates is spawned in
	// (gw_hello.cwd). Empty means the daemon's own directory; NewSession takes
	// its own Cwd and does not use this one.
	Cwd    string
	PiArgs []string
	// Resume asks for replay and enables durable resume; LiveOnly attaches at
	// the head and ignores Resume.
	Resume     *protocol.Resume
	LiveOnly   bool
	AllowLossy bool

	// OnEvent, when set, receives every non-response frame from the read
	// goroutine instead of Events(). Frames arrive in order, one at a time;
	// the handler must return promptly, and must not call a method that waits
	// for a response (Do, GetState, Prompt, ...) because the response can only
	// be read after the handler returns. Events() delivers nothing while
	// OnEvent is set, and MaxPendingEvents does not apply.
	OnEvent func(Event)

	// RequestTimeout bounds one Do call; default DefaultRequestTimeout.
	RequestTimeout time.Duration
	// MaxPendingEvents bounds the undelivered event queue; default
	// DefaultMaxPendingEvents.
	MaxPendingEvents int
}

// SessionFilter selects catalog rows for ListSessions (docs/protocol.md §3.2).
type SessionFilter struct {
	Cwd   string // only sessions in this directory
	Live  bool   // only sessions with a running pi
	Limit int    // 0 means the daemon's default
	// Tags keeps only sessions whose creator tags contain every entry. The
	// daemon filters cwd/live/limit itself but has no creator filter, so a tag
	// filter fetches the full page and filters in the client. Tags are
	// reported only for sessions created explicitly with gw_new_session
	// (NewSession) and the daemon holds them in memory, so they describe
	// sessions the running daemon saw created; a daemon restart loses them.
	Tags map[string]string
	// CreatedBy keeps only the sessions one creator created (the clientId the
	// creator reported). It has the same lifetime caveat as Tags.
	CreatedBy string
}

// SessionRow is one gw_list_sessions entry. It aliases the shared protocol
// wire type so the daemon and the client cannot drift.
type SessionRow = protocol.SessionRow

// Event is one frame the daemon sent that was not a response to an
// outstanding Do. Raw is the complete frame, gateway fields included, so a
// consumer can decode pi's own payloads or the typed gateway messages in
// protocol.
type Event struct {
	Seq  uint64 // gw_seq, 0 when the frame carries none
	Type string
	Raw  json.RawMessage
}

// Unmarshal decodes Raw into v.
func (e Event) Unmarshal(v any) error { return json.Unmarshal(e.Raw, v) }

// Field returns a top-level string field of the frame.
func (e Event) Field(name string) string { return protocol.Field(e.Raw, name) }

// Response is a decoded pi-shaped response frame.
type Response struct {
	ID      string
	Command string
	Success bool
	Code    string // protocol error code when Success is false
	Error   string // human-readable failure when Success is false
	Data    json.RawMessage
}

// Decode unmarshals the response's data object into v.
func (r *Response) Decode(v any) error {
	if len(r.Data) == 0 {
		return errors.New("gwclient: response has no data")
	}
	return json.Unmarshal(r.Data, v)
}

// ResponseError reports a command the daemon answered with success:false.
type ResponseError struct {
	Command string
	Code    string
	Message string
}

func (e *ResponseError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("gwclient: %s failed: %s (%s)", e.Command, e.Message, e.Code)
	}
	return fmt.Sprintf("gwclient: %s failed: %s", e.Command, e.Message)
}

// Client is a connection to pi-gatewayd.
type Client struct {
	cfg     Config
	conn    net.Conn
	codec   *protocol.Codec
	welcome protocol.Welcome

	events chan Event
	closed chan struct{}
	pong   chan struct{} // gw_pong signal, capacity 1

	mu          sync.Mutex
	cond        *sync.Cond
	pending     map[string]chan *Response
	queue       []Event
	session     *protocol.SessionRef
	leafID      string
	lastSeq     uint64
	turnRunning bool
	turnWait    chan struct{}
	err         error

	seq        atomic.Uint64
	userClosed atomic.Bool
	closeOnce  sync.Once

	bashMu       sync.Mutex
	bashHandlers map[string]func(BashUpdate)
}

// Dial connects to the daemon, performs gw_hello/gw_welcome, and starts
// delivering frames. The returned client is bound according to cfg.Session.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	applyDefaults(&cfg)

	token, err := cfg.token()
	if err != nil {
		return nil, err
	}

	addrs := config.Addresses(cfg.Addr, cfg.Port, cfg.stateDir())
	dialer := net.Dialer{Timeout: cfg.DialTimeout}
	var (
		conn    net.Conn
		dialErr error
	)
	for _, addr := range addrs {
		conn, dialErr = dialer.DialContext(ctx, "tcp", addr)
		if dialErr == nil {
			break
		}
	}
	if dialErr != nil {
		return nil, fmt.Errorf("gwclient: cannot reach pi-gatewayd at %v: %w", addrs, dialErr)
	}

	c := &Client{
		cfg:          cfg,
		conn:         conn,
		codec:        protocol.NewCodec(conn, conn),
		events:       make(chan Event, 64),
		closed:       make(chan struct{}),
		pong:         make(chan struct{}, 1),
		pending:      make(map[string]chan *Response),
		turnWait:     make(chan struct{}),
		bashHandlers: make(map[string]func(BashUpdate)),
	}
	c.cond = sync.NewCond(&c.mu)
	if err := c.handshake(ctx, token); err != nil {
		_ = conn.Close()
		return nil, err
	}
	// HeadSeq is the newest record that existed when this client attached;
	// nothing before it belongs to this connection's cursor. An explicit
	// Resume cursor wins, so a caller restarting from a stored cursor keeps
	// it even when the daemon's ring was reset.
	if cfg.Resume != nil {
		c.lastSeq, c.leafID = cfg.Resume.SinceSeq, cfg.Resume.LeafEntryID
	} else {
		c.lastSeq = c.welcome.HeadSeq
	}
	c.turnRunning = c.welcome.Turn.State == "running"
	go c.serve()
	go c.pump()
	return c, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Name == "" {
		cfg.Name = DefaultName
	}
	if cfg.Kind == "" {
		cfg.Kind = DefaultKind
	}
	if len(cfg.Capabilities) == 0 {
		cfg.Capabilities = append([]string(nil), protocol.AllCapabilities...)
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.MaxPendingEvents <= 0 {
		cfg.MaxPendingEvents = DefaultMaxPendingEvents
	}
}

func (cfg Config) stateDir() string {
	if cfg.StateDir != "" {
		return cfg.StateDir
	}
	return config.Dir()
}

func (cfg Config) token() (string, error) {
	if cfg.Token != "" {
		return cfg.Token, nil
	}
	path := cfg.TokenFile
	if path == "" {
		path = config.TokenPath(cfg.stateDir())
	}
	tok, err := config.ReadToken(path)
	if err != nil {
		return "", fmt.Errorf("gwclient: read token %s: %w", path, err)
	}
	return tok, nil
}

func (c *Client) handshake(ctx context.Context, token string) error {
	hello := protocol.Hello{
		Type:     "gw_hello",
		Protocol: protocol.Version,
		Token:    token,
		Client: protocol.ClientInfo{
			Name:         c.cfg.Name,
			Kind:         c.cfg.Kind,
			Capabilities: c.cfg.Capabilities,
			Tags:         c.cfg.Tags,
		},
		Session:    c.cfg.Session,
		Cwd:        c.cfg.Cwd,
		PiArgs:     c.cfg.PiArgs,
		Resume:     c.cfg.Resume,
		LiveOnly:   c.cfg.LiveOnly,
		AllowLossy: c.cfg.AllowLossy,
	}
	if err := c.codec.WriteJSON(&hello); err != nil {
		return fmt.Errorf("gwclient: send gw_hello: %w", err)
	}
	for {
		raw, err := c.codec.Read()
		if err != nil {
			return fmt.Errorf("gwclient: no gw_welcome from daemon: %w", err)
		}
		switch protocol.Field(raw, "type") {
		case "gw_welcome":
			if err := json.Unmarshal(raw, &c.welcome); err != nil {
				return fmt.Errorf("gwclient: malformed gw_welcome: %w", err)
			}
			if c.welcome.Protocol != protocol.Version {
				return fmt.Errorf("gwclient: daemon speaks protocol %d, want %d",
					c.welcome.Protocol, protocol.Version)
			}
			c.session = c.welcome.Session
			if c.cfg.Session != "" && c.welcome.Session == nil {
				return c.attachError(ctx)
			}
			return nil
		case "gw_error":
			return fmt.Errorf("gwclient: handshake rejected: %s (%s)",
				protocol.Field(raw, "message"), protocol.Field(raw, "code"))
		}
	}
}

// attachError reads the gw_error the daemon sends immediately after a
// gw_welcome whose attach failed (docs/protocol.md §2). Without this, Dial
// would report success on an unbound connection and the next command would
// silently create a new session instead of the requested one.
func (c *Client) attachError(ctx context.Context) error {
	budget := c.cfg.DialTimeout
	if budget <= 0 {
		budget = DefaultDialTimeout
	}
	deadline := time.Now().Add(budget)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.conn.SetReadDeadline(deadline)
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	for {
		raw, err := c.codec.Read()
		if err != nil {
			return fmt.Errorf("gwclient: attach %s: no error frame from daemon: %w", c.cfg.Session, err)
		}
		if protocol.Field(raw, "type") == "gw_error" {
			return &ResponseError{
				Command: "gw_hello",
				Code:    protocol.Field(raw, "code"),
				Message: protocol.Field(raw, "message"),
			}
		}
	}
}

// Welcome returns the handshake result.
func (c *Client) Welcome() protocol.Welcome {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.welcome
	w.Session = c.session
	return w
}

// ClientID is the id the daemon assigned this connection.
func (c *Client) ClientID() string { return c.welcome.ClientID }

// Granted is the capability set the daemon granted (the intersection of the
// requested set and the token's role).
func (c *Client) Granted() []string { return append([]string(nil), c.welcome.Granted...) }

// Features is the gateway feature set reported in gw_welcome (docs/protocol.md
// §2). It names the command surface the daemon revision supports; an empty
// list means an older daemon.
func (c *Client) Features() []string { return append([]string(nil), c.welcome.Features...) }

// HasFeature reports whether the daemon advertises a feature.
func (c *Client) HasFeature(name string) bool {
	for _, f := range c.welcome.Features {
		if f == name {
			return true
		}
	}
	return false
}

// Can reports whether the daemon granted a capability on this connection.
func (c *Client) Can(capability string) bool {
	for _, granted := range c.welcome.Granted {
		if granted == capability {
			return true
		}
	}
	return false
}

// Session returns the session this connection is bound to, or nil when it is
// still unbound.
func (c *Client) Session() *protocol.SessionRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.session
}

// Events returns the stream of frames that were not responses to a Do call.
// It is closed when the connection ends; see Err for why. A consumer must
// drain it.
func (c *Client) Events() <-chan Event { return c.events }

// Done is closed when the connection ends, gracefully or not.
func (c *Client) Done() <-chan struct{} { return c.closed }

// Err returns the error that ended the connection, or nil while it lives.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Close ends the connection. The daemon-owned session survives; Close never
// stops pi.
func (c *Client) Close() error {
	if c.userClosed.CompareAndSwap(false, true) {
		c.finish(ErrClosed)
	}
	return nil
}

// IsClosed reports whether an error means the connection ended.
func IsClosed(err error) bool {
	return err != nil && (errors.Is(err, ErrClosed) || errors.Is(err, net.ErrClosed))
}

func (c *Client) serve() {
	for {
		raw, err := c.codec.Read()
		if err != nil {
			if c.userClosed.Load() {
				c.finish(ErrClosed)
			} else {
				c.finish(fmt.Errorf("gwclient: connection lost: %w", err))
			}
			return
		}
		c.dispatch(raw)
	}
}

func (c *Client) dispatch(raw []byte) {
	rec, err := protocol.DecodeRecord(raw)
	if err != nil {
		return
	}
	if rec.ID != "" && (rec.Type == "response" || rec.Type == "gw_error") {
		if ch := c.takePending(rec.ID); ch != nil {
			ch <- decodeResponse(raw, rec)
			return
		}
	}
	seq := uint64(protocol.NumField(raw, "gw_seq"))
	if seq != 0 {
		c.advanceSeq(seq)
	}
	switch rec.Type {
	case "gw_pong":
		select {
		case c.pong <- struct{}{}:
		default:
		}
	case "gw_turn":
		c.noteTurn(protocol.Field(raw, "state") == "running")
	case "gw_session_state":
		var st protocol.SessionStateEvent
		if json.Unmarshal(raw, &st) == nil && sessionDetached(st) {
			// The daemon unbound this connection: the session was deleted, or
			// it was stopped on request. Drop the binding and the local turn
			// state, so Session() reports nil and a later command cannot
			// silently reuse a session this client is no longer attached to.
			c.setSession(nil)
			c.noteTurn(false)
		}
	case "gw_snapshot":
		var snap protocol.Snapshot
		if json.Unmarshal(raw, &snap) == nil {
			if snap.HeadSeq > 0 {
				// The snapshot replaces everything through HeadSeq, even
				// records from a previous daemon generation whose seq
				// numbering was higher.
				c.resetSeq(snap.HeadSeq)
			}
			c.noteLeaf(snap.LeafID)
		}
	case "gw_replay_done":
		var done protocol.ReplayDone
		if json.Unmarshal(raw, &done) == nil {
			c.advanceSeq(done.HeadSeq)
		}
	case "bash_execution_update":
		if h := c.bashHandler(rec.ID); h != nil {
			h(BashUpdate{ID: rec.ID, Delta: protocol.Field(raw, "delta")})
			return
		}
	}
	c.pushEvent(Event{
		Seq:  seq,
		Type: rec.Type,
		Raw:  append(json.RawMessage(nil), raw...),
	})
}

func decodeResponse(raw []byte, rec protocol.Record) *Response {
	if rec.Type == "gw_error" {
		return &Response{
			ID:      rec.ID,
			Success: false,
			Code:    protocol.Field(raw, "code"),
			Error:   protocol.Field(raw, "message"),
		}
	}
	var obj struct {
		ID      string          `json:"id"`
		Command string          `json:"command"`
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Error   string          `json:"error"`
		Data    json.RawMessage `json:"data"`
	}
	_ = json.Unmarshal(raw, &obj)
	return &Response{
		ID:      obj.ID,
		Command: obj.Command,
		Success: obj.Success,
		Code:    obj.Code,
		Error:   obj.Error,
		Data:    obj.Data,
	}
}

// sessionDetached reports whether a gw_session_state record means the daemon
// has unbound this connection: the session was deleted, or it was stopped on
// request (which keeps the file but does not keep the binding). A crash and a
// hibernation leave the binding alone, so a crashed session can still be
// reloaded in place.
func sessionDetached(st protocol.SessionStateEvent) bool {
	if st.State == protocol.SessionStateDeleted {
		return true
	}
	return st.State == "stopped" &&
		(st.Reason == protocol.StopReasonRequested || st.Reason == protocol.StopReasonForced)
}

func (c *Client) pushEvent(e Event) {
	if c.cfg.OnEvent != nil {
		c.cfg.OnEvent(e)
		return
	}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	if len(c.queue) >= c.cfg.MaxPendingEvents {
		c.mu.Unlock()
		c.finish(ErrEventBufferFull)
		return
	}
	c.queue = append(c.queue, e)
	c.cond.Signal()
	c.mu.Unlock()
}

// pump moves queued frames onto the Events channel. It is a separate
// goroutine so a consumer that is slow to drain Dials never blocks response
// correlation in serve.
func (c *Client) pump() {
	for {
		c.mu.Lock()
		for len(c.queue) == 0 && c.err == nil {
			c.cond.Wait()
		}
		if c.err != nil {
			c.mu.Unlock()
			close(c.events)
			return
		}
		e := c.queue[0]
		c.queue = c.queue[1:]
		c.mu.Unlock()
		select {
		case c.events <- e:
		case <-c.closed:
		}
	}
}

func (c *Client) finish(err error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		if c.err == nil {
			if err == nil {
				err = ErrClosed
			}
			c.err = err
		}
		c.mu.Unlock()
		close(c.closed)
		c.cond.Broadcast()
		_ = c.conn.Close()
	})
}

// Send writes one command without an id and without waiting for a response.
// Use Do when the caller needs the response; use Send for fire-and-forget
// traffic (notifications, gw_ping, extension_ui_response).
func (c *Client) Send(command string, fields map[string]any) error {
	raw, err := commandFrame("", command, fields)
	if err != nil {
		return err
	}
	return c.write(raw)
}

// Do sends one command with a fresh id and waits for its response. It returns
// a *ResponseError (with the Response still populated) when the daemon
// answers success:false, and a transport/context error otherwise.
func (c *Client) Do(ctx context.Context, command string, fields map[string]any) (*Response, error) {
	return c.do(ctx, c.newID(), command, fields)
}

// DoID is Do with a caller-supplied id. The id is echoed on the response and
// on any frame pi ties to the command (for example a bash command's
// bash_execution_update events), which lets the caller correlate them by
// subscribing to Events() before the response arrives.
func (c *Client) DoID(ctx context.Context, id, command string, fields map[string]any) (*Response, error) {
	if id == "" {
		return nil, errors.New("gwclient: DoID: empty id")
	}
	return c.do(ctx, id, command, fields)
}

func (c *Client) do(ctx context.Context, id, command string, fields map[string]any) (*Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	raw, err := commandFrame(id, command, fields)
	if err != nil {
		return nil, err
	}
	ch := make(chan *Response, 1)
	if err := c.addPending(id, ch); err != nil {
		return nil, err
	}
	defer c.takePending(id)
	if err := c.write(raw); err != nil {
		return nil, err
	}

	timer := time.NewTimer(c.cfg.RequestTimeout)
	defer timer.Stop()
	select {
	case resp := <-ch:
		if !resp.Success {
			return resp, &ResponseError{Command: command, Code: resp.Code, Message: resp.Error}
		}
		return resp, nil
	case <-c.closed:
		return nil, c.Err()
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("gwclient: %s: no response after %s", command, c.cfg.RequestTimeout)
	}
}

func (c *Client) write(raw []byte) error {
	if err := c.Err(); err != nil {
		return err
	}
	if err := c.codec.WriteRaw(raw); err != nil {
		c.finish(fmt.Errorf("gwclient: write: %w", err))
		return c.Err()
	}
	return nil
}

func (c *Client) newID() string {
	return "gwclient-" + strconv.FormatUint(c.seq.Add(1), 10)
}

func (c *Client) addPending(id string, ch chan *Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.err
	}
	c.pending[id] = ch
	return nil
}

func (c *Client) takePending(id string) chan *Response {
	if id == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := c.pending[id]
	delete(c.pending, id)
	return ch
}

// setSession records a session the connection was (re)bound to.
func (c *Client) setSession(session *protocol.SessionRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.session = session
}

// commandFrame builds one command frame. A non-empty id wins over an "id"
// field in fields.
func commandFrame(id, command string, fields map[string]any) ([]byte, error) {
	obj := make(map[string]any, len(fields)+2)
	for k, v := range fields {
		obj[k] = v
	}
	obj["type"] = command
	if id != "" {
		obj["id"] = id
	}
	return json.Marshal(obj)
}

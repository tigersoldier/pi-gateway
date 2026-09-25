package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/gwlog"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// Client is the actor's view of an attached connection. It is implemented by
// the daemon connection.
type Client interface {
	ID() string
	Kind() string
	Name() string
	Has(capability string) bool
	Ref() protocol.ClientRef
	Summary() protocol.ClientSummary
}

// ClientCommand is one pi command forwarded by a client.
type ClientCommand struct {
	Client  Client
	LocalID string
	Type    string
	Raw     []byte
}

// RuntimeCommand is a runtime-applicable parameter change (model, thinking
// level, session name) that must be applied to the shared session via RPC.
type RuntimeCommand struct {
	Command string
	Raw     []byte
}

// Params configures a session actor.
type Params struct {
	PiBin       string
	PiArgs      []string // accepted pi arguments, excluding --mode rpc/--session
	Cwd         string   // pi's working directory; empty inherits the daemon's
	SessionPath string   // known path; empty for implicit creation
	IdleTimeout time.Duration
	ShortGrace  time.Duration
	HubCapacity int
	SessionID   string // gw_session value before a path is known
	Log         gwlog.Logger
	Metrics     *metrics.Registry
}

// snapInfo is the race-free view of fields read from outside the actor loop.
type snapInfo struct {
	state       string
	path        string
	sessionName string
	sessionID   string
}

// Info is a snapshot of actor state used to build gw_welcome.
type Info struct {
	Path        string
	State       string
	HeadSeq     uint64
	OldestSeq   uint64
	Turn        protocol.TurnState
	Clients     []protocol.ClientSummary
	Subscribers int
}

// Actor errors surfaced to the daemon.
var (
	ErrStopped     = errors.New("session: actor stopped")
	ErrQueueFull   = errors.New("session: command queue full")
	ErrCallTimeout = errors.New("session: pi call timed out")
	ErrNoPi        = errors.New("session: pi is not running")
	// ErrReloadBusy is returned when gw_reload_session is refused because a
	// turn is running or another client is attached.
	ErrReloadBusy = errors.New("session: reload refused while busy")
)

const (
	stateStarting   = "starting"
	stateReady      = "ready"
	stateRestarting = "restarting"
	stateStopping   = "stopping"
	stateHibernated = "hibernated"
	stateStopped    = "stopped"
	stateCrashed    = "crashed"

	turnIdle    = "idle"
	turnRunning = "running"

	defaultCloseGrace = 2 * time.Second
)

// globalMutations are client commands that change shared session state; their
// successful responses are announced to every client (docs/protocol.md §4.4).
var globalMutations = map[string]bool{
	"set_model":           true,
	"cycle_model":         true,
	"set_thinking_level":  true,
	"set_steering_mode":   true,
	"set_follow_up_mode":  true,
	"compact":             true,
	"set_auto_compaction": true,
	"set_auto_retry":      true,
	"set_session_name":    true,
	"gw_reload_session":   true,
}

// dialogMethods are the extension UI methods that block pi until a client
// answers; they are routed to one client instead of broadcast
// (docs/design.md §9).
var dialogMethods = map[string]bool{
	"select":  true,
	"confirm": true,
	"input":   true,
	"editor":  true,
}

// uiPending is one unanswered extension UI dialog.
type uiPending struct {
	winner string
	raw    []byte // original extension_ui_request, republished on reassignment
}

type callReq struct {
	id   string
	raw  []byte
	resp chan protocol.Record
}

type applyReq struct {
	cmds []RuntimeCommand
	by   *protocol.ClientRef
}

// restartReq asks the actor to stop and respawn pi for the same session. The
// daemon connection that requested it sends the pi-shaped response itself, so
// the answer also reaches a requester attached to a different session.
type restartReq struct {
	force     bool
	requester string // client that asked; it does not count as "another client"
	by        *protocol.ClientRef
	done      chan error
}

type actorMsg struct {
	cmd       *ClientCommand
	attach    Client
	attachAck chan struct{}
	detach    *string
	call      *callReq
	apply     *applyReq
	restart   *restartReq
	info      chan Info
}

// Actor owns one pi process, its ordered event log, and the daemon queue.
// All state transitions happen on its single loop goroutine.
type Actor struct {
	params  Params
	log     gwlog.Logger
	metrics *metrics.Registry
	hub     *Hub
	pi      *PiProcess
	queue   Queue

	in       chan actorMsg
	stopCh   chan struct{}
	stopOnce sync.Once
	finished chan struct{}
	dead     bool

	snap     atomic.Pointer[snapInfo]
	clients  map[string]Client
	attached atomic.Int64

	state       string
	stopReason  string
	path        string
	sessionName string
	sessionID   string
	hasMessages bool

	turnState  string
	turnID     string
	turnAuthor *protocol.ClientRef
	turnSeq    uint64

	pendingCalls      map[string]chan protocol.Record
	internalCallbacks map[string]func(protocol.Record)
	queueRuns         map[string]*protocol.ClientRef
	forkPending       map[string]bool
	runtimeCalls      map[string]protocol.StateChanged
	clearPending      map[string][]QueuedItem
	owners            map[string]string
	pendingUI         map[string]*uiPending
	lastActive        map[string]time.Time
	restarting        bool

	timer      *time.Timer
	timerArmed bool

	nextID atomic.Uint64

	// Hooks, set by the daemon before Start.
	OnPath    func(a *Actor, path string)
	OnStopped func(a *Actor, reason string)
	Retire    func(a *Actor) bool
}

// NewActor builds an actor. Call Start to spawn pi and run the loop.
func NewActor(p Params) *Actor {
	if p.HubCapacity <= 0 {
		p.HubCapacity = 4096
	}
	sessionID := p.SessionID
	if sessionID == "" {
		if p.SessionPath != "" {
			sessionID = p.SessionPath
		} else {
			sessionID = fmt.Sprintf("s_%d", time.Now().UnixNano())
		}
	}
	log := p.Log
	if log == nil {
		log = gwlog.Nop()
	}
	a := &Actor{
		params:            p,
		log:               log,
		metrics:           p.Metrics,
		hub:               NewHub(p.HubCapacity, sessionID),
		in:                make(chan actorMsg, 512),
		stopCh:            make(chan struct{}),
		finished:          make(chan struct{}),
		clients:           make(map[string]Client),
		state:             stateStarting,
		path:              p.SessionPath,
		sessionID:         sessionID,
		turnState:         turnIdle,
		pendingCalls:      make(map[string]chan protocol.Record),
		internalCallbacks: make(map[string]func(protocol.Record)),
		queueRuns:         make(map[string]*protocol.ClientRef),
		forkPending:       make(map[string]bool),
		runtimeCalls:      make(map[string]protocol.StateChanged),
		clearPending:      make(map[string][]QueuedItem),
		owners:            make(map[string]string),
		pendingUI:         make(map[string]*uiPending),
		lastActive:        make(map[string]time.Time),
	}
	a.hub.SetMetrics(p.Metrics)
	a.timer = time.NewTimer(time.Hour)
	if !a.timer.Stop() {
		<-a.timer.C
	}
	a.publishSnapshot()
	return a
}

// Start spawns pi and starts the actor loop.
func (a *Actor) Start() error {
	pi, err := a.spawnPi()
	if err != nil {
		a.setState(stateCrashed)
		return err
	}
	a.pi = pi
	a.setState(stateReady)
	go a.loop()
	a.publishState(stateReady, "", nil)
	// Arm the idle timer immediately: an actor that is never bound by a client
	// (for example because the bind lost a race) must not leak its pi process.
	a.reevaluateIdle()
	return nil
}

// spawnPi starts a pi process for this actor's session.
func (a *Actor) spawnPi() (*PiProcess, error) {
	args := []string{"--mode", "rpc"}
	if a.path != "" {
		args = append(args, "--session", a.path)
	}
	args = append(args, a.params.PiArgs...)
	return StartPi(PiConfig{Bin: a.params.PiBin, Args: args, Dir: a.params.Cwd, Log: a.log})
}

// stopPi asks the process to abort and closes it, bounded by a grace period
// (docs/design.md §4.7).
func (a *Actor) stopPi() {
	if a.pi != nil {
		_ = a.pi.Send([]byte(`{"type":"abort"}`))
		a.pi.Close(defaultCloseGrace)
		a.pi = nil
	}
}

// Stop requests a graceful shutdown and waits for the loop to finish.
func (a *Actor) Stop() {
	a.stopOnce.Do(func() { close(a.stopCh) })
	<-a.finished
}

// Finished is closed when the actor loop has exited.
func (a *Actor) Finished() <-chan struct{} { return a.finished }

// Path is the session file path, once known.
func (a *Actor) Path() string {
	if s := a.snap.Load(); s != nil {
		return s.path
	}
	return ""
}

// State is the process/session state.
func (a *Actor) State() string {
	if s := a.snap.Load(); s != nil {
		return s.state
	}
	return stateStarting
}

// SessionName is pi's session name, once known.
func (a *Actor) SessionName() string {
	if s := a.snap.Load(); s != nil {
		return s.sessionName
	}
	return ""
}

// SessionID is pi's session identifier, once known.
func (a *Actor) SessionID() string {
	if s := a.snap.Load(); s != nil {
		return s.sessionID
	}
	return ""
}

// Attached is the number of attached clients.
func (a *Actor) Attached() int { return int(a.attached.Load()) }

// Live reports whether the session has a running pi process. The daemon uses
// it to decide whether a registered actor is still usable.
func (a *Actor) Live() bool {
	switch a.State() {
	case stateReady, stateStarting:
		return true
	}
	return false
}

// Subscribe registers a client's event subscription.
func (a *Actor) Subscribe(clientID string, opts SubOptions) *Subscriber {
	return a.hub.Subscribe(clientID, opts)
}

// Unsubscribe removes a client's event subscription.
func (a *Actor) Unsubscribe(clientID string) { a.hub.Unsubscribe(clientID) }

// Replay returns retained records after afterSeq.
func (a *Actor) Replay(afterSeq uint64) ([]protocol.Record, bool) { return a.hub.Replay(afterSeq) }

// Attach registers a client and announces its presence.
func (a *Actor) Attach(c Client) bool {
	return a.send(actorMsg{attach: c})
}

// AttachSync registers a client and waits until the actor loop has done so.
// The daemon uses it so an idle-retire decision cannot race the attach.
func (a *Actor) AttachSync(c Client, timeout time.Duration) bool {
	ack := make(chan struct{})
	if !a.send(actorMsg{attach: c, attachAck: ack}) {
		return false
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ack:
		return true
	case <-t.C:
		return false
	case <-a.finished:
		return false
	}
}

// Detach removes a client and announces its departure.
func (a *Actor) Detach(clientID string) bool {
	return a.send(actorMsg{detach: &clientID})
}

// Submit forwards a client command for processing.
func (a *Actor) Submit(cmd ClientCommand) error {
	if !a.send(actorMsg{cmd: &cmd}) {
		return ErrStopped
	}
	return nil
}

// ApplyRuntime applies runtime parameter changes to the shared session.
func (a *Actor) ApplyRuntime(cmds []RuntimeCommand, by *protocol.ClientRef) error {
	if len(cmds) == 0 {
		return nil
	}
	if !a.send(actorMsg{apply: &applyReq{cmds: cmds, by: by}}) {
		return ErrStopped
	}
	return nil
}

// Call runs one daemon-internal pi command and waits for its response.
func (a *Actor) Call(raw []byte, timeout time.Duration) (protocol.Record, error) {
	id := a.internalID("c")
	raw2, err := protocol.RewriteID(raw, id)
	if err != nil {
		return protocol.Record{}, err
	}
	req := &callReq{id: id, raw: raw2, resp: make(chan protocol.Record, 1)}
	if !a.send(actorMsg{call: req}) {
		return protocol.Record{}, ErrStopped
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case rec := <-req.resp:
		if rec.Type == "" {
			return protocol.Record{}, ErrNoPi
		}
		return rec, nil
	case <-t.C:
		return protocol.Record{}, ErrCallTimeout
	case <-a.finished:
		return protocol.Record{}, ErrStopped
	}
}

// Restart stops pi and respawns it for the same session file (the daemon
// operation behind gw_reload_session). It is refused with ErrReloadBusy while
// another client is attached or a turn is running, unless force is set.
func (a *Actor) Restart(force bool, requesterID string, by *protocol.ClientRef, timeout time.Duration) error {
	req := &restartReq{force: force, requester: requesterID, by: by, done: make(chan error, 1)}
	if !a.send(actorMsg{restart: req}) {
		return ErrStopped
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case err := <-req.done:
		return err
	case <-t.C:
		return ErrCallTimeout
	case <-a.finished:
		return ErrStopped
	}
}

// Info returns a snapshot for welcome messages.
func (a *Actor) Info() Info {
	reply := make(chan Info, 1)
	if !a.send(actorMsg{info: reply}) {
		return a.fallbackInfo()
	}
	select {
	case i := <-reply:
		return i
	case <-a.finished:
		return a.fallbackInfo()
	case <-time.After(2 * time.Second):
		return a.fallbackInfo()
	}
}

func (a *Actor) send(m actorMsg) bool {
	select {
	case <-a.finished:
		return false
	default:
	}
	select {
	case a.in <- m:
		return true
	case <-a.finished:
		return false
	default:
		// The channel is full: never block the connection goroutine.
		a.log.Warn("actor input queue full, dropping message", "session", a.Path())
		return false
	}
}

func (a *Actor) loop() {
	defer close(a.finished)
	var piEvents <-chan protocol.Record
	var piDone <-chan struct{}
	if a.pi != nil {
		piEvents, piDone = a.pi.Events(), a.pi.Done()
	}
	for !a.dead {
		select {
		case m := <-a.in:
			piEvents, piDone = a.handleMsg(m, piEvents, piDone)
		case rec, ok := <-piEvents:
			if !ok {
				piEvents = nil
				continue
			}
			a.handlePiEvent(rec)
		case <-piDone:
			piDone = nil
			a.handlePiExit()
		case <-a.timer.C:
			a.timerArmed = false
			a.handleIdle()
		case <-a.stopCh:
			a.dead = true
		}
	}
	a.shutdown()
}

func (a *Actor) shutdown() {
	if a.stopReason == "" {
		a.stopReason = stateStopped
	}
	a.state = stateStopping
	a.stopPi()
	a.failQueued(protocol.CodeSessionCrashed, "session "+a.stopReason)
	a.failPendingCalls()
	a.failInternalCallbacks()
	// Publish before closing the subscribers, or nobody sees the final state.
	a.setState(stateStopped)
	a.publishState(stateStopped, a.stopReason, nil)
	a.hub.CloseAll()
	if a.OnStopped != nil {
		a.OnStopped(a, a.stopReason)
	}
}

func (a *Actor) handleMsg(m actorMsg, events <-chan protocol.Record, done <-chan struct{}) (<-chan protocol.Record, <-chan struct{}) {
	switch {
	case m.cmd != nil:
		a.handleCommand(*m.cmd)
	case m.attach != nil:
		a.attachClient(m.attach)
		if m.attachAck != nil {
			close(m.attachAck)
		}
	case m.detach != nil:
		a.detachClient(*m.detach)
	case m.call != nil:
		a.startCall(m.call)
	case m.apply != nil:
		a.applyRuntime(m.apply)
	case m.restart != nil:
		return a.handleRestart(m.restart, events, done)
	case m.info != nil:
		m.info <- a.info()
	}
	return events, done
}

// ---------------------------------------------------------------------------
// Clients

func (a *Actor) attachClient(c Client) {
	if _, ok := a.clients[c.ID()]; ok {
		return
	}
	a.clients[c.ID()] = c
	a.lastActive[c.ID()] = time.Now()
	a.attached.Store(int64(len(a.clients)))
	a.publishPresence("join", c)
	a.reevaluateIdle()
}

func (a *Actor) detachClient(clientID string) {
	c, ok := a.clients[clientID]
	if !ok {
		return
	}
	delete(a.clients, clientID)
	delete(a.lastActive, clientID)
	a.attached.Store(int64(len(a.clients)))
	a.publishPresence("leave", c)
	a.reassignUI(clientID)
	if a.attached.Load() == 0 && (a.state == stateCrashed || a.state == stateStopped) {
		a.dead = true
		return
	}
	a.reevaluateIdle()
}

// reassignUI hands dialogs the departing client owned to the next candidate,
// so a disconnected UI cannot strand pi until its timeout (design §9).
func (a *Actor) reassignUI(clientID string) {
	for id, p := range a.pendingUI {
		if p.winner != clientID {
			continue
		}
		if next := a.pickUIWinner(); next != "" {
			p.winner = next
			a.hub.Publish(protocol.Record{Raw: p.raw, Type: "extension_ui_request", Owner: next})
			continue
		}
		delete(a.pendingUI, id) // nobody can answer; pi's timeout applies
	}
}

func (a *Actor) info() Info {
	out := Info{
		Path:        a.path,
		State:       a.state,
		HeadSeq:     a.hub.HeadSeq(),
		OldestSeq:   a.hub.OldestSeq(),
		Subscribers: a.hub.Count(),
		Turn: protocol.TurnState{
			State:  a.turnState,
			TurnID: a.turnID,
			Author: a.turnAuthor,
			Queued: a.queue.Len(),
		},
	}
	for _, c := range a.clients {
		out.Clients = append(out.Clients, c.Summary())
	}
	return out
}

// ---------------------------------------------------------------------------
// Command handling

func (a *Actor) handleCommand(c ClientCommand) {
	a.lastActive[c.Client.ID()] = time.Now()
	switch c.Type {
	case "prompt", "follow_up":
		a.handlePrompt(c)
	case "steer":
		a.forward(c)
	case "abort":
		// Session-wide; requires `interject`, like steer (docs/protocol.md §10).
		a.forward(c)
	case "clear_queue":
		a.handleClearQueue(c)
	case "switch_session":
		// The daemon intercepts switch_session; reaching the actor means the
		// connection was already bound when it was sent.
		a.errorResponse(c, protocol.CodeNotSupported, "switch_session is handled by the daemon")
	case "new_session":
		a.errorResponse(c, protocol.CodeNotSupported, "new_session is handled by the daemon")
	case "fork", "clone":
		a.handleFork(c)
	case "extension_ui_response":
		a.handleUIResponse(c)
	default:
		if protocol.IsGatewayType(c.Type) {
			a.errorResponse(c, protocol.CodeBadFrame, fmt.Sprintf("unknown gateway message %q", c.Type))
			return
		}
		a.forward(c)
	}
}

func (a *Actor) handlePrompt(c ClientCommand) {
	// A follow-up submitted to an idle session runs as a normal prompt.
	raw, err := protocol.RewriteCommand(c.Raw, c.LocalID, "prompt")
	if err != nil {
		a.errorResponse(c, protocol.CodeBadFrame, err.Error())
		return
	}
	item := QueuedItem{
		ClientID: c.Client.ID(),
		Kind:     c.Client.Kind(),
		Name:     c.Client.Name(),
		LocalID:  c.LocalID,
		Type:     "prompt",
		Raw:      raw,
		Message:  protocol.Field(c.Raw, "message"),
	}
	if a.state != stateReady {
		a.metrics.Inc(metrics.PromptsRejected)
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	if a.turnState == turnIdle && a.queue.Len() == 0 {
		a.forwardCommand(item, c.Client, c.LocalID)
		return
	}
	a.queue.Push(item)
	a.metrics.Inc(metrics.PromptsQueued)
	a.respond(c, true, []byte(`{"queued":true}`))
	a.publishQueue()
}

// forwardCommand sends an item to pi immediately and marks the turn running.
func (a *Actor) forwardCommand(it QueuedItem, client Client, localID string) {
	ns := protocol.NamespaceID(client.ID(), localID)
	raw, err := protocol.RewriteID(it.Raw, ns)
	if err != nil {
		raw = it.Raw
	}
	if err := a.pi.Send(raw); err != nil {
		a.errorResponse(ClientCommand{Client: client, LocalID: localID, Type: "prompt"}, protocol.CodeSessionCrashed, err.Error())
		return
	}
	a.owners[ns] = client.ID()
	a.startTurn(it.Ref())
}

func (a *Actor) handleClearQueue(c ClientCommand) {
	cleared := a.queue.Clear()
	if len(cleared) > 0 {
		a.publishQueue()
	}
	if a.state != stateReady {
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	ns := protocol.NamespaceID(c.Client.ID(), c.LocalID)
	if len(cleared) > 0 {
		a.clearPending[ns] = cleared
	}
	a.owners[ns] = c.Client.ID()
	a.forwardRaw(c, ns)
}

// forward forwards a command with a namespaced id without touching the queue.
func (a *Actor) forward(c ClientCommand) {
	if a.state != stateReady {
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	ns := protocol.NamespaceID(c.Client.ID(), c.LocalID)
	a.owners[ns] = c.Client.ID()
	a.forwardRaw(c, ns)
}

func (a *Actor) forwardRaw(c ClientCommand, ns string) {
	if a.pi == nil {
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	raw, err := protocol.RewriteID(c.Raw, ns)
	if err != nil {
		a.errorResponse(c, protocol.CodeBadFrame, err.Error())
		return
	}
	if err := a.pi.Send(raw); err != nil {
		a.errorResponse(c, protocol.CodeSessionCrashed, err.Error())
	}
}

// handleFork forwards pi's fork/clone, which moves the live process to a new
// session file. It is refused while other clients are attached, because they
// would silently end up on the new session (docs/protocol.md §3.8).
func (a *Actor) handleFork(c ClientCommand) {
	if len(a.clients) > 1 {
		a.errorResponse(c, protocol.CodeSharedSession, c.Type+" is unavailable while other clients are attached")
		return
	}
	if a.state != stateReady || a.pi == nil {
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	ns := protocol.NamespaceID(c.Client.ID(), c.LocalID)
	raw, err := protocol.RewriteID(c.Raw, ns)
	if err != nil {
		a.errorResponse(c, protocol.CodeBadFrame, err.Error())
		return
	}
	a.owners[ns] = c.Client.ID()
	if err := a.pi.Send(raw); err != nil {
		delete(a.owners, ns)
		a.errorResponse(c, protocol.CodeSessionCrashed, err.Error())
		return
	}
	a.forkPending[ns] = true
}

func (a *Actor) handleUIResponse(c ClientCommand) {
	if a.state != stateReady || a.pi == nil {
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	p, ok := a.pendingUI[c.LocalID]
	if !ok {
		a.metrics.Inc(metrics.UIStale)
		a.errorResponse(c, protocol.CodeUIStale, "extension UI request is no longer pending")
		return
	}
	if p.winner != c.Client.ID() {
		a.metrics.Inc(metrics.UIStale)
		a.errorResponse(c, protocol.CodeUIStale, "extension UI request was routed to another client")
		return
	}
	a.metrics.Inc(metrics.UIAnswered)
	delete(a.pendingUI, c.LocalID)
	// The answer keeps pi's original request id; route any response with that
	// id back to this client only.
	a.owners[c.LocalID] = c.Client.ID()
	a.forwardRaw(c, c.LocalID)
}

// ---------------------------------------------------------------------------
// Turn and queue

func (a *Actor) startTurn(author *protocol.ClientRef) {
	a.metrics.Inc(metrics.TurnsStarted)
	a.turnState = turnRunning
	a.turnSeq++
	a.turnID = fmt.Sprintf("t_%d", a.turnSeq)
	a.turnAuthor = author
	a.hasMessages = true
	a.publishTurn(turnRunning)
	a.reevaluateIdle()
}

func (a *Actor) onSettled() {
	if a.turnState == turnRunning {
		a.metrics.Inc(metrics.TurnsSettled)
		a.turnState = turnIdle
		a.publishTurn("settled")
	}
	if a.queue.Len() > 0 && a.state == stateReady {
		a.drainQueue()
		return
	}
	a.turnAuthor = nil
	a.reevaluateIdle()
}

// drainQueue forwards the next queued item as a normal prompt. The forwarded
// command uses an internal id: the author already received the queued
// response, so only failures are reported back as gw_error.
func (a *Actor) drainQueue() {
	it, ok := a.queue.Pop()
	if !ok {
		return
	}
	id := a.internalID("q")
	raw, err := protocol.RewriteCommand(it.Raw, id, "prompt")
	if err != nil {
		a.log.Warn("dropping malformed queued command", "session", a.path, "err", err)
		a.publishQueue()
		return
	}
	a.queueRuns[id] = it.Ref()
	if err := a.pi.Send(raw); err != nil {
		delete(a.queueRuns, id)
		a.publishError(it.Ref(), protocol.CodeSessionCrashed, err.Error())
		a.publishQueue()
		return
	}
	a.startTurn(it.Ref())
	a.publishQueue()
}

func (a *Actor) applyRuntime(r *applyReq) {
	if a.pi == nil {
		return
	}
	for _, rc := range r.cmds {
		id := a.internalID("r")
		raw, err := protocol.RewriteID(rc.Raw, id)
		if err != nil {
			continue
		}
		a.runtimeCalls[id] = protocol.StateChanged{
			Type:    "gw_state_changed",
			Command: rc.Command,
			By:      r.by,
		}
		if err := a.pi.Send(raw); err != nil {
			delete(a.runtimeCalls, id)
			a.log.Warn("runtime parameter apply failed", "session", a.path, "command", rc.Command, "err", err)
		}
	}
}

func (a *Actor) startCall(req *callReq) {
	if a.pi == nil {
		req.resp <- protocol.Record{}
		return
	}
	a.pendingCalls[req.id] = req.resp
	if err := a.pi.Send(req.raw); err != nil {
		delete(a.pendingCalls, req.id)
		req.resp <- protocol.Record{}
	}
}

// ---------------------------------------------------------------------------
// pi events

func (a *Actor) handlePiEvent(rec protocol.Record) {
	// Adopt the reported session state before publishing or handing the
	// response to a caller, so records carry the canonical gw_session and
	// fork/clone moves are picked up even when nobody asks for get_state.
	if rec.Type == "response" && protocol.Field(rec.Raw, "command") == "get_state" {
		a.adoptState(rec.Raw)
	}
	// Internal responses never reach clients.
	if rec.Type == "response" && protocol.IsInternalID(rec.ID) {
		a.handleInternalResponse(rec)
		return
	}
	if rec.ID != "" {
		if owner, ok := a.owners[rec.ID]; ok {
			rec.Owner = owner
			if rec.Type == "response" {
				delete(a.owners, rec.ID)
			}
		}
	}
	if rec.Type == "response" {
		if cleared, ok := a.clearPending[rec.ID]; ok {
			delete(a.clearPending, rec.ID)
			if raw, ok := mergeClearedQueue(rec.Raw, cleared); ok {
				rec.Raw = raw
			}
		}
	}
	if rec.Type == "extension_ui_request" && !a.routeUI(&rec) {
		return // no ui-capable client: pi's own dialog timeout applies
	}
	a.hub.Publish(rec)
	a.postProcess(rec)
}

func (a *Actor) handleInternalResponse(rec protocol.Record) {
	if cb, ok := a.internalCallbacks[rec.ID]; ok {
		delete(a.internalCallbacks, rec.ID)
		cb(rec)
		return
	}
	if ch, ok := a.pendingCalls[rec.ID]; ok {
		delete(a.pendingCalls, rec.ID)
		ch <- rec
		return
	}
	if ref, ok := a.queueRuns[rec.ID]; ok {
		delete(a.queueRuns, rec.ID)
		if !protocol.BoolField(rec.Raw, "success", true) {
			msg := protocol.Field(rec.Raw, "error")
			if msg == "" {
				msg = "queued prompt failed"
			}
			a.publishError(ref, protocol.CodeSessionCrashed, msg)
			a.failTurn()
		}
		return
	}
	if sc, ok := a.runtimeCalls[rec.ID]; ok {
		delete(a.runtimeCalls, rec.ID)
		if !protocol.BoolField(rec.Raw, "success", false) {
			return
		}
		sc.Data = protocol.DataField(rec.Raw)
		if raw, err := json.Marshal(sc); err == nil {
			a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_state_changed"})
		}
	}
}

func (a *Actor) postProcess(rec protocol.Record) {
	switch rec.Type {
	case "agent_start":
		if a.turnState != turnRunning {
			a.startTurn(nil)
		}
	case "agent_settled":
		a.onSettled()
	case "message_start", "message_update", "message_end",
		"tool_execution_start", "tool_execution_update", "tool_execution_end":
		a.hasMessages = true
	case "response":
		cmd := protocol.Field(rec.Raw, "command")
		success := protocol.BoolField(rec.Raw, "success", false)
		if !success && (cmd == "prompt" || cmd == "follow_up") {
			// pi rejected the prompt (for example because it was still
			// streaming): release the turn so the queue is not wedged.
			a.failTurn()
		}
		if cmd == "fork" || cmd == "clone" {
			if a.forkPending[rec.ID] {
				delete(a.forkPending, rec.ID)
				if success {
					// pi moved the live process to a new session file; learn it
					// and let the daemon re-key the session.
					a.startInternal("get_state", func(rec protocol.Record) { a.adoptState(rec.Raw) })
				}
			}
		}
		if success && globalMutations[cmd] {
			a.publishStateChanged(cmd, rec.Raw, rec.Owner)
		}
	}
}

// failTurn releases a turn pi never started so queued prompts still run.
func (a *Actor) failTurn() {
	if a.turnState != turnRunning {
		return
	}
	a.turnState = turnIdle
	a.turnAuthor = nil
	a.publishTurn("settled")
	if a.queue.Len() > 0 && a.state == stateReady {
		a.drainQueue()
		return
	}
	a.reevaluateIdle()
}

// adoptState learns the session file, name, and id from a get_state response
// so an implicitly created session can be registered in the catalog and a
// fork/clone move is followed.
func (a *Actor) adoptState(raw []byte) {
	data := protocol.DataField(raw)
	if len(data) == 0 {
		return
	}

	st := protocol.ParsePiState(data)
	if st.SessionName != "" {
		a.sessionName = st.SessionName
	}
	if st.SessionID != "" {
		a.sessionID = st.SessionID
	}
	if st.SessionFile != "" && st.SessionFile != a.path {
		a.setPath(st.SessionFile)
		if a.OnPath != nil {
			a.OnPath(a, st.SessionFile)
		}
	}
	a.publishSnapshot()
}

func (a *Actor) handlePiExit() {
	pi := a.pi
	a.pi = nil
	code := 0
	reason := "pi exited"
	if pi != nil {
		code = pi.ExitCode()
		if err := pi.WaitErr(); err != nil {
			reason = err.Error()
		}
	}
	if a.dead {
		return
	}
	a.state = stateCrashed
	a.stopReason = stateCrashed
	a.metrics.Inc(metrics.PiExits)
	a.publishState(stateCrashed, reason, &code)
	a.failQueued(protocol.CodeSessionCrashed, "pi exited")
	a.failPendingCalls()
	a.failInternalCallbacks()
	a.pendingUI = make(map[string]*uiPending)
	a.forkPending = make(map[string]bool)
	if a.attached.Load() == 0 {
		a.dead = true
	}
}

// ---------------------------------------------------------------------------
// Idle handling

func (a *Actor) handleIdle() {
	if a.state != stateReady || a.turnState == turnRunning || a.attached.Load() > 0 {
		a.reevaluateIdle()
		return
	}
	if a.Retire != nil && !a.Retire(a) {
		a.reevaluateIdle()
		return
	}
	a.stopReason = stateHibernated
	a.publishState(stateHibernated, "idle timeout", nil)
	a.dead = true
}

func (a *Actor) reevaluateIdle() {
	if a.dead || a.state != stateReady {
		return
	}
	idle := a.attached.Load() == 0 && a.turnState == turnIdle && a.queue.Len() == 0
	if !idle {
		a.stopTimer()
		return
	}
	if a.timerArmed {
		return
	}
	d := a.params.IdleTimeout
	if !a.hasMessages {
		d = a.params.ShortGrace
	}
	if d <= 0 {
		d = time.Minute
	}
	a.timer.Reset(d)
	a.timerArmed = true
}

func (a *Actor) stopTimer() {
	if !a.timerArmed {
		return
	}
	if !a.timer.Stop() {
		select {
		case <-a.timer.C:
		default:
		}
	}
	a.timerArmed = false
}

// ---------------------------------------------------------------------------
// Failure handling

func (a *Actor) failQueued(code, msg string) {
	for _, it := range a.queue.Clear() {
		a.publishError(it.Ref(), code, msg)
	}
	a.failQueueRuns(code, msg)
	a.publishQueue()
}

// internalID allocates a daemon-generated command id, tagged with a one-letter
// kind so logs can tell callbacks, queued prompts, runtime changes, and
// generic internal calls apart.
func (a *Actor) internalID(kind string) string {
	return fmt.Sprintf("%s%s%d", protocol.InternalIDPrefix, kind, a.nextID.Add(1))
}

// failQueued reports prompts that were already forwarded to pi and cannot
// be answered anymore; the not-yet-forwarded queue is left alone.
func (a *Actor) failQueueRuns(code, msg string) {
	for id, ref := range a.queueRuns {
		delete(a.queueRuns, id)
		a.publishError(ref, code, msg)
	}
}

func (a *Actor) failPendingCalls() {
	for id, ch := range a.pendingCalls {
		delete(a.pendingCalls, id)
		ch <- protocol.Record{}
	}
}

// failInternalCallbacks completes daemon-internal pi commands whose process
// died, so no caller waits for a response that will never arrive.
func (a *Actor) failInternalCallbacks() {
	for id, cb := range a.internalCallbacks {
		delete(a.internalCallbacks, id)
		cb(protocol.Record{})
	}
}

// startInternal sends a daemon-generated pi command and runs cb with the
// response (an empty Record when pi is gone).
func (a *Actor) startInternal(command string, cb func(protocol.Record)) {
	if a.pi == nil {
		cb(protocol.Record{})
		return
	}
	id := a.internalID("x")
	raw, err := protocol.RewriteID([]byte(`{"type":"`+command+`"}`), id)
	if err != nil {
		cb(protocol.Record{})
		return
	}
	a.internalCallbacks[id] = cb
	if err := a.pi.Send(raw); err != nil {
		delete(a.internalCallbacks, id)
		cb(protocol.Record{})
	}
}

// handleRestart stops and respawns pi for the same session file. It runs on
// the actor loop and returns the new process channels for the loop to use; nil
// channels mean there is no process anymore.
func (a *Actor) handleRestart(req *restartReq, events <-chan protocol.Record, done <-chan struct{}) (<-chan protocol.Record, <-chan struct{}) {
	fail := func(err error) (<-chan protocol.Record, <-chan struct{}) {
		req.done <- err
		return events, done
	}
	if a.restarting {
		return fail(ErrReloadBusy)
	}
	if a.state != stateReady && a.state != stateCrashed {
		return fail(ErrStopped)
	}
	others := 0
	for id := range a.clients {
		if id != req.requester {
			others++
		}
	}
	if !req.force && (others > 0 || a.turnState == turnRunning) {
		a.metrics.Inc(metrics.ReloadsRefused)
		return fail(ErrReloadBusy)
	}
	a.metrics.Inc(metrics.Reloads)
	a.setState(stateRestarting)
	a.publishState(stateRestarting, "reload", nil)
	a.stopPi()
	// The old process takes the running turn with it: forwarded prompts fail,
	// while the not-yet-forwarded daemon queue is kept (docs/design.md §4.4).
	a.failQueueRuns(protocol.CodeSessionCrashed, "session reloaded")
	a.failPendingCalls()
	a.failTurn()
	a.pendingUI = make(map[string]*uiPending)
	a.forkPending = make(map[string]bool)

	pi, err := a.spawnPi()
	if err != nil {
		a.setState(stateCrashed)
		a.publishState(stateCrashed, err.Error(), nil)
		req.done <- err
		return nil, nil
	}
	a.pi = pi
	a.restarting = true
	a.startInternal("get_state", func(rec protocol.Record) {
		a.restarting = false
		if rec.Type == "" || !protocol.BoolField(rec.Raw, "success", false) {
			a.setState(stateCrashed)
			a.publishState(stateCrashed, "reload failed", nil)
			req.done <- ErrNoPi
			return
		}
		a.setState(stateReady)
		a.publishState(stateReady, "reload", nil)
		a.publishStateChanged("gw_reload_session", rec.Raw, refClientID(req.by))
		// Prompts queued while the old process died must run again; without
		// this the queue would be stranded (no turn is running to settle).
		if a.queue.Len() > 0 {
			a.drainQueue()
		} else {
			a.reevaluateIdle()
		}
		req.done <- nil
	})
	return pi.Events(), pi.Done()
}

// routeUI sends a dialog to one client and remembers the winner; other UI
// methods are fire-and-forget broadcasts (docs/design.md §9). It reports
// whether the record should be published: a dialog nobody can answer is
// dropped so pi's own timeout resolves it.
func (a *Actor) routeUI(rec *protocol.Record) bool {
	if !dialogMethods[protocol.Field(rec.Raw, "method")] {
		return true
	}
	a.metrics.Inc(metrics.UIRequests)
	winner := a.pickUIWinner()
	if winner == "" {
		a.metrics.Inc(metrics.UIUnroutable)
		return false
	}
	rec.Owner = winner
	if rec.ID != "" {
		a.pendingUI[rec.ID] = &uiPending{winner: winner, raw: rec.Raw}
	}
	return true
}

// pickUIWinner prefers the author of the running turn, then the most recently
// active ui-capable client.
func (a *Actor) pickUIWinner() string {
	if a.turnAuthor != nil {
		if c, ok := a.clients[a.turnAuthor.ClientID]; ok && c.Has(protocol.CapUI) {
			return c.ID()
		}
	}
	best, bestAt := "", time.Time{}
	for id, c := range a.clients {
		if !c.Has(protocol.CapUI) {
			continue
		}
		at := a.lastActive[id]
		if best == "" || at.After(bestAt) {
			best, bestAt = id, at
		}
	}
	return best
}

func refClientID(ref *protocol.ClientRef) string {
	if ref == nil {
		return ""
	}
	return ref.ClientID
}

// ---------------------------------------------------------------------------
// Publishing

func (a *Actor) respond(c ClientCommand, success bool, data []byte) {
	a.respondFrame(c, success, "", "", data)
}

func (a *Actor) errorResponse(c ClientCommand, code, msg string) {
	a.respondFrame(c, false, code, msg, nil)
}

// respondFrame publishes a pi-shaped response addressed to the requester.
func (a *Actor) respondFrame(c ClientCommand, success bool, code, msg string, data []byte) {
	raw := protocol.Response(c.LocalID, c.Type, success, code, msg, data)
	a.hub.Publish(protocol.Record{Raw: raw, Type: "response", ID: c.LocalID, Owner: c.Client.ID()})
}

// publishStateChanged tells every client about a shared-state mutation.
func (a *Actor) publishStateChanged(command string, responseRaw []byte, owner string) {
	ev := protocol.StateChanged{
		Type:    "gw_state_changed",
		Command: command,
		Data:    protocol.DataField(responseRaw),
	}
	if c, ok := a.clients[owner]; ok {
		ref := c.Ref()
		ev.By = &ref
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_state_changed"})
}

// publishError sends a gw_error to one client.
func (a *Actor) publishError(ref *protocol.ClientRef, code, msg string) {
	if ref == nil {
		return
	}
	if raw, err := json.Marshal(protocol.NewError(code, msg)); err == nil {
		a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_error", Owner: ref.ClientID})
	}
}

func (a *Actor) publishTurn(state string) {
	ev := protocol.TurnEvent{Type: "gw_turn", State: state, TurnID: a.turnID, Author: a.turnAuthor}
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_turn"})
}

func (a *Actor) publishQueue() {
	ev := protocol.QueueEvent{Type: "gw_queue", Pending: a.queue.Pending()}
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_queue"})
}

func (a *Actor) publishPresence(event string, c Client) {
	ev := protocol.PresenceEvent{Type: "gw_presence", Event: event, Client: c.Ref()}
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_presence"})
}

func (a *Actor) publishState(state, reason string, exitCode *int) {
	ev := protocol.SessionStateEvent{Type: "gw_session_state", State: state, Reason: reason, ExitCode: exitCode}
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.hub.Publish(protocol.Record{Raw: raw, Type: "gw_session_state"})
}

// mergeClearedQueue adds the daemon-cleared prompts to pi's clear_queue data.
func mergeClearedQueue(raw []byte, cleared []QueuedItem) ([]byte, bool) {
	if !protocol.BoolField(raw, "success", false) {
		return nil, false
	}
	data := protocol.DataField(raw)
	obj := map[string]any{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &obj); err != nil {
			return nil, false
		}
	}
	followUp, _ := obj["followUp"].([]any)
	for _, it := range cleared {
		followUp = append(followUp, it.Message)
	}
	obj["followUp"] = followUp
	if _, ok := obj["steering"]; !ok {
		obj["steering"] = []any{}
	}
	dataOut, err := json.Marshal(obj)
	if err != nil {
		return nil, false
	}
	return protocol.Response(protocol.Field(raw, "id"), "clear_queue", true, "", "", dataOut), true
}

// Snapshot builds a gw_snapshot payload from pi's durable history.
func (a *Actor) Snapshot() (protocol.Snapshot, error) {
	state, err := a.Call([]byte(`{"type":"get_state"}`), 10*time.Second)
	if err != nil {
		return protocol.Snapshot{}, err
	}
	entries, err := a.Call([]byte(`{"type":"get_entries"}`), 10*time.Second)
	if err != nil {
		return protocol.Snapshot{}, err
	}
	snap := protocol.Snapshot{Type: "gw_snapshot", PiState: protocol.DataField(state.Raw)}
	var data struct {
		Entries []json.RawMessage `json:"entries"`
		LeafID  string            `json:"leafId"`
	}
	if d := protocol.DataField(entries.Raw); len(d) > 0 {
		_ = json.Unmarshal(d, &data)
	}
	snap.Entries = data.Entries
	snap.LeafID = data.LeafID
	return snap, nil
}

func (a *Actor) publishSnapshot() {
	a.snap.Store(&snapInfo{
		state:       a.state,
		path:        a.path,
		sessionName: a.sessionName,
		sessionID:   a.sessionID,
	})
}

func (a *Actor) setState(state string) {
	a.state = state
	a.publishSnapshot()
}

func (a *Actor) setPath(path string) {
	a.path = path
	a.hub.SetSession(path)
	a.publishSnapshot()
}

func (a *Actor) fallbackInfo() Info {
	return Info{
		Path:      a.Path(),
		State:     a.State(),
		HeadSeq:   a.hub.HeadSeq(),
		OldestSeq: a.hub.OldestSeq(),
		Turn:      protocol.TurnState{State: turnIdle},
	}
}

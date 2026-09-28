package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/gwlog"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/protocol"
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
	// StopGrace bounds a forced stop's wait for the running turn to settle;
	// 0 uses defaultStopGrace.
	StopGrace   time.Duration
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
	// ErrSessionBusy is returned when a stop/delete is refused because a turn
	// is running and force was not set.
	ErrSessionBusy = errors.New("session: a turn is running")
	// ErrSessionAttached is returned when gw_stop_session is refused because
	// other clients are attached and force was not set.
	ErrSessionAttached = errors.New("session: other clients are attached")
)

const (
	stateStarting   = "starting"
	stateReady      = "ready"
	stateRestarting = "restarting"
	stateStopping   = "stopping"
	stateHibernated = "hibernated"
	stateStopped    = "stopped"
	stateCrashed    = "crashed"
	// stateDeleted is the terminal state of gw_delete_session: the session
	// file is gone and the session is unusable (docs/protocol.md §5.2).
	stateDeleted = protocol.SessionStateDeleted

	turnIdle    = "idle"
	turnRunning = "running"

	defaultCloseGrace = 2 * time.Second
	// defaultStopGrace bounds how long a forced stop waits for a running turn
	// to settle after `abort` before pi is stopped anyway.
	defaultStopGrace = 5 * time.Second
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
	// piArgs replaces the spawn arguments for the respawn when non-nil
	// (gw_reload_session{piArgs}); nil keeps the actor's recorded parameters.
	piArgs []string
	done   chan error
}

// StopRequest asks the actor to stop pi and terminate the session (the daemon
// operation behind gw_stop_session and gw_delete_session).
type StopRequest struct {
	// Force lets a running turn be aborted and attached clients be detached.
	Force bool
	// Requester is the client that asked; it does not count as an attached
	// client, mirroring gw_reload_session.
	Requester string
	// Delete selects the terminal `deleted` state: the daemon removes the
	// session file after the actor has finished.
	Delete bool
}

// StopResult reports what a stop or delete actually did.
type StopResult struct {
	Path            string
	Name            string
	SessionID       string
	PiStopped       bool
	DetachedClients int
}

// stopReq is one stop request traveling through the actor mailbox.
type stopReq struct {
	force     bool
	requester string
	deleted   bool
	done      chan stopOutcome
}

type stopOutcome struct {
	result StopResult
	err    error
}

type actorMsg struct {
	cmd       *ClientCommand
	attach    Client
	attachAck chan struct{}
	detach    *string
	call      *callReq
	apply     *applyReq
	restart   *restartReq
	stop      *stopReq
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
	// injects are the dedupe keys of context injections pi has accepted, so a
	// reconnect that retries an inject does not duplicate the instruction.
	// The record lives as long as the actor, matching pi's own in-memory queue
	// (docs/protocol.md §3.11).
	injects map[string]bool
	// pendingInject maps a forwarded send_message id to its dedupe key, so the
	// key is only remembered once pi accepted the command.
	pendingInject map[string]string

	// terminalState is what shutdown publishes (stateStopped, or stateDeleted
	// for gw_delete_session). pendingStop is the forced stop waiting for the
	// aborted turn to settle, bounded by stopGrace.
	terminalState string
	pendingStop   *stopReq
	stopResult    StopResult
	stopGrace     *time.Timer
	stopGraceC    <-chan time.Time

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
		injects:           make(map[string]bool),
		pendingInject:     make(map[string]string),
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
	return a.spawnPiArgs(a.params.PiArgs)
}

// spawnPiArgs starts pi with an explicit accepted-argument list; a nil list
// falls back to the actor's configured parameters.
func (a *Actor) spawnPiArgs(piArgs []string) (*PiProcess, error) {
	if piArgs == nil {
		piArgs = a.params.PiArgs
	}
	args := []string{"--mode", "rpc"}
	if a.path != "" {
		args = append(args, "--session", a.path)
	}
	args = append(args, piArgs...)
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
// it to decide whether a registered actor must be replaced, so a reloading
// actor counts as live: it has no *usable* pi for a moment, but it owns the
// session and will come back. Replacing it instead would run two pi processes
// on one session file (docs/design.md §3.2, §4.4).
func (a *Actor) Live() bool {
	switch a.State() {
	case stateReady, stateStarting, stateRestarting:
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
// another client is attached or a turn is running, unless force is set. A
// non-nil piArgs replaces the spawn arguments for the new process, which is
// how gw_reload_session{piArgs} installs a new spawn configuration.
func (a *Actor) Restart(force bool, requesterID string, by *protocol.ClientRef, piArgs []string, timeout time.Duration) error {
	req := &restartReq{force: force, requester: requesterID, by: by, piArgs: piArgs, done: make(chan error, 1)}
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

// StopSession stops pi and terminates the session (the daemon operation behind
// gw_stop_session and gw_delete_session). It returns once the actor has begun
// its terminal shutdown; the daemon waits for Finished before touching the
// session file. It is refused with ErrSessionBusy while a turn is running and
// with ErrSessionAttached while other clients are attached, unless Force is
// set.
func (a *Actor) StopSession(req StopRequest, timeout time.Duration) (StopResult, error) {
	r := &stopReq{force: req.Force, requester: req.Requester, deleted: req.Delete, done: make(chan stopOutcome, 1)}
	if !a.send(actorMsg{stop: r}) {
		return StopResult{}, ErrStopped
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case out := <-r.done:
		return out.result, out.err
	case <-t.C:
		return StopResult{}, ErrCallTimeout
	case <-a.finished:
		// Stopping finishes the actor, so both channels can be ready at once:
		// prefer the outcome the actor queued before it exited.
		select {
		case out := <-r.done:
			return out.result, out.err
		default:
			return StopResult{}, ErrStopped
		}
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
		case <-a.stopGraceC:
			if a.pendingStop != nil {
				a.finishStop(a.pendingStop, a.stopResult)
			}
		case <-a.stopCh:
			a.dead = true
		}
	}
	a.shutdown()
}

func (a *Actor) shutdown() {
	terminal := a.terminalState
	if terminal == "" {
		terminal = stateStopped
	}
	if a.stopReason == "" {
		a.stopReason = stateStopped
	}
	a.state = stateStopping
	a.stopPi()
	a.failQueued(protocol.CodeSessionCrashed, "session "+a.stopReason)
	a.failPendingCalls()
	a.failInternalCallbacks()
	// Publish before closing the subscribers, or nobody sees the final state.
	a.setState(terminal)
	a.publishState(terminal, a.stopReason, nil)
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
	case m.stop != nil:
		a.handleStop(m.stop)
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
	case "inject":
		a.handleInject(c)
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

// Gateway constants for the inject translation. Keeping pi's command name in
// one place makes a future upstream rename a one-line change.
const (
	piSendMessageCommand      = "send_message"
	injectPayloadQueued       = `{"queued":true}`
	injectPayloadDeduplicated = `{"queued":true,"deduplicated":true}`
	// maxInjectDedupeKeys bounds the per-session dedupe memory. Dedupe is
	// best-effort: at this many distinct keys the oldest are dropped wholesale
	// rather than growing the map for the actor's lifetime.
	maxInjectDedupeKeys = 4096
)

// handleInject forwards a context injection to pi as its send_message
// primitive. It never starts a turn: pi decides when the message joins the
// context from `deliverAs`, and the message is a custom (non-user) entry
// (docs/protocol.md §3.11). A dedupe key makes a retry after a reconnect a
// no-op instead of a duplicated instruction.
func (a *Actor) handleInject(c ClientCommand) {
	if a.state != stateReady || a.pi == nil {
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	ns := protocol.NamespaceID(c.Client.ID(), c.LocalID)
	raw, err := buildSendMessage(c.Raw, ns)
	if err != nil {
		a.errorResponse(c, protocol.CodeBadFrame, err.Error())
		return
	}
	key := protocol.Field(c.Raw, "dedupeKey")
	if key != "" && a.injects[key] {
		a.respond(c, true, []byte(injectPayloadDeduplicated))
		return
	}
	if err := a.pi.Send(raw); err != nil {
		a.errorResponse(c, protocol.CodeSessionCrashed, err.Error())
		return
	}
	a.owners[ns] = c.Client.ID()
	// Track every forwarded injection, not just keyed ones: the response
	// translation keys on this id so it does not depend on pi's echoed command
	// name (a rename must not reshape the client-visible response).
	a.pendingInject[ns] = key
}

// buildSendMessage validates the gateway's inject frame and maps it onto pi's
// flat send_message command: the nested custom message is hoisted, dedupeKey
// stays at the gateway, and triggerTurn is forced false because an injection
// never starts a turn (docs/protocol.md §3.11).
func buildSendMessage(raw []byte, ns string) ([]byte, error) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("inject: %w", err)
	}
	msg, ok := obj["message"].(map[string]any)
	if !ok {
		return nil, errors.New("inject requires a message object")
	}
	content, _ := msg["content"].(string)
	if content == "" {
		return nil, errors.New("inject requires a non-empty message.content")
	}
	customType, _ := msg["customType"].(string)
	if customType == "" {
		customType = "pi-gateway/inject"
	}
	out := map[string]any{
		"type":        piSendMessageCommand,
		"id":          ns,
		"triggerTurn": false,
		"customType":  customType,
		"content":     content,
	}
	if v, ok := msg["display"]; ok {
		out["display"] = v
	}
	if v, ok := obj["deliverAs"].(string); ok && v != "" {
		switch v {
		case "nextTurn", "steer", "followUp":
			out["deliverAs"] = v
		default:
			return nil, fmt.Errorf("inject: unknown deliverAs %q", v)
		}
	}
	return json.Marshal(out)
}

// translateInject rewrites pi's send_message response into the gateway's
// inject shape: the command name the client used, a `{queued:true}` success
// payload, and `not_supported` when the managed pi does not know the command
// (docs/protocol.md §3.11).
func (a *Actor) translateInject(rec protocol.Record) ([]byte, bool) {
	key, pending := a.pendingInject[rec.ID]
	if pending {
		delete(a.pendingInject, rec.ID)
	}
	success := protocol.BoolField(rec.Raw, "success", false)
	if success {
		if pending && key != "" {
			if len(a.injects) >= maxInjectDedupeKeys {
				a.injects = make(map[string]bool)
			}
			a.injects[key] = true
		}
		return protocol.Response(rec.ID, "inject", true, "", "", []byte(injectPayloadQueued)), true
	}
	msg := protocol.Field(rec.Raw, "error")
	code := ""
	if strings.HasPrefix(msg, "Unknown command") {
		code = protocol.CodeNotSupported
	}
	return protocol.Response(rec.ID, "inject", false, code, msg, nil), true
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
	if a.pendingStop != nil {
		// A forced stop was waiting for this turn to unwind.
		a.finishStop(a.pendingStop, a.stopResult)
		return
	}
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
		// Context injection is the gateway's `inject`; pi answers the
		// underlying `send_message`, so the response is translated back to the
		// client-facing command (docs/protocol.md §3.11). Keying on the pending
		// request id — not on pi's echoed command name — keeps the client-facing
		// shape stable if pi renames its primitive and avoids rewriting a
		// hand-sent `send_message` the daemon never originated.
		if _, pending := a.pendingInject[rec.ID]; pending {
			if raw, ok := a.translateInject(rec); ok {
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
	if a.pendingStop != nil {
		// pi exited (or was already gone) while a forced stop was waiting for
		// the aborted turn: the stop is complete, and the terminal state is
		// the stop's, not a crash.
		a.metrics.Inc(metrics.PiExits)
		a.finishStop(a.pendingStop, a.stopResult)
		return
	}
	a.setState(stateCrashed)
	a.stopReason = stateCrashed
	a.metrics.Inc(metrics.PiExits)
	a.publishState(stateCrashed, reason, &code)
	a.failQueued(protocol.CodeSessionCrashed, "pi exited")
	a.failPendingCalls()
	a.failInternalCallbacks()
	a.pendingUI = make(map[string]*uiPending)
	a.forkPending = make(map[string]bool)
	a.pendingInject = make(map[string]string)
	a.injects = make(map[string]bool)
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

// handleStop validates a stop request and either refuses it or begins the
// terminal sequence: the daemon queue is discarded, attached clients are
// detached (their connections stay open, they are only unbound), and a
// running turn is aborted and given a bounded grace to settle before pi is
// stopped anyway.
func (a *Actor) handleStop(req *stopReq) {
	if a.dead || a.pendingStop != nil {
		req.done <- stopOutcome{err: ErrStopped}
		return
	}
	switch a.state {
	case stateReady, stateCrashed, stateRestarting:
	default:
		req.done <- stopOutcome{err: ErrStopped}
		return
	}
	others := 0
	for id := range a.clients {
		if id != req.requester {
			others++
		}
	}
	if a.turnState == turnRunning && !req.force {
		req.done <- stopOutcome{err: ErrSessionBusy}
		return
	}
	// Deleting a session never blocks on attached clients: they are notified
	// and unbound (docs/protocol.md §3.10). Stopping pi under an active user
	// is a resource action, so it refuses unless forced.
	if !req.deleted && !req.force && others > 0 {
		req.done <- stopOutcome{err: ErrSessionAttached}
		return
	}
	res := StopResult{
		Path:            a.path,
		Name:            a.sessionName,
		SessionID:       a.sessionID,
		PiStopped:       a.pi != nil,
		DetachedClients: others,
	}
	a.terminalState = stateStopped
	if req.deleted {
		a.terminalState = stateDeleted
	}
	a.stopReason = protocol.StopReasonRequested
	if req.force {
		a.stopReason = protocol.StopReasonForced
	}
	// Discard the daemon queue and the prompts already forwarded from it, and
	// republish the empty queue before anything else: no queued prompt may
	// start a turn in a session that is going away.
	a.discardQueue()
	for id := range a.clients {
		a.detachClient(id)
	}
	if a.turnState == turnRunning && a.pi != nil {
		// Forced stop of a running turn: ask pi to abort, then wait for it to
		// settle, bounded by the grace.
		a.pendingStop = req
		a.stopResult = res
		a.startInternal("abort", func(protocol.Record) {})
		a.stopGrace = time.NewTimer(a.stopGraceDuration())
		a.stopGraceC = a.stopGrace.C
		return
	}
	a.finishStop(req, res)
}

// finishStop delivers the stop outcome and marks the actor for shutdown; the
// loop exits after the current handler returns, publishing the terminal state.
func (a *Actor) finishStop(req *stopReq, res StopResult) {
	if a.stopGrace != nil {
		a.stopGrace.Stop()
		a.stopGrace, a.stopGraceC = nil, nil
	}
	a.pendingStop = nil
	req.done <- stopOutcome{result: res}
	a.dead = true
}

func (a *Actor) stopGraceDuration() time.Duration {
	if a.params.StopGrace > 0 {
		return a.params.StopGrace
	}
	return defaultStopGrace
}

// discardQueue drops the daemon-owned queue and the prompts already forwarded
// from it, and republishes the empty queue.
func (a *Actor) discardQueue() {
	a.queue.Clear()
	for id := range a.queueRuns {
		delete(a.queueRuns, id)
	}
	a.publishQueue()
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
	// A new pi process means an empty in-memory queue: forget the dedupe keys
	// pi accepted, or a re-injection after a reload would be answered
	// "deduplicated" while the instruction is gone (docs/protocol.md §3.11).
	a.injects = make(map[string]bool)
	a.pendingInject = make(map[string]string)

	pi, err := a.spawnPiArgs(req.piArgs)
	if err != nil {
		a.setState(stateCrashed)
		a.publishState(stateCrashed, err.Error(), nil)
		req.done <- err
		return nil, nil
	}
	// A replacement configuration becomes the actor's parameters, so a later
	// plain reload and a cold respawn by another client both agree with the
	// record (docs/protocol.md §4.3).
	if req.piArgs != nil {
		a.params.PiArgs = append([]string(nil), req.piArgs...)
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

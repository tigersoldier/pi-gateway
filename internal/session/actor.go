package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// Client is the actor's view of an attached connection. It is implemented by
// the daemon connection.
type Client interface {
	ID() string
	Info() protocol.ClientInfo
	Kind() string
	Name() string
	Has(capability string) bool
	Ref() protocol.ClientRef
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
	SessionPath string   // known path; empty for implicit creation
	Cwd         string
	IdleTimeout time.Duration
	ShortGrace  time.Duration
	HubCapacity int
	SessionID   string // gw_session value before a path is known
	Logf        func(format string, args ...any)
}

// snapInfo is the race-free view of fields read from outside the actor loop.
type snapInfo struct {
	state       string
	path        string
	sessionName string
}

// Info is a snapshot of actor state used to build gw_welcome.
type Info struct {
	Path      string
	State     string
	HeadSeq   uint64
	OldestSeq uint64
	Turn      protocol.TurnState
	Clients   []protocol.ClientInfo
}

// Actor errors surfaced to the daemon.
var (
	ErrStopped     = errors.New("session: actor stopped")
	ErrQueueFull   = errors.New("session: command queue full")
	ErrCallTimeout = errors.New("session: pi call timed out")
	ErrNoPi        = errors.New("session: pi is not running")
)

const (
	stateStarting   = "starting"
	stateReady      = "ready"
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

type actorMsg struct {
	cmd       *ClientCommand
	attach    Client
	attachAck chan struct{}
	detach    *string
	call      *callReq
	apply     *applyReq
	info      chan Info
}

// Actor owns one pi process, its ordered event log, and the daemon queue.
// All state transitions happen on its single loop goroutine.
type Actor struct {
	params Params
	logf   func(format string, args ...any)
	hub    *Hub
	pi     *PiProcess
	queue  Queue

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
	hasMessages bool

	turnState  string
	turnID     string
	turnAuthor *protocol.ClientRef
	turnSeq    uint64

	pendingCalls map[string]chan protocol.Record
	queueRuns    map[string]*protocol.ClientRef
	runtimeCalls map[string]protocol.StateChanged
	clearPending map[string][]QueuedItem
	owners       map[string]string
	pendingUI    map[string]bool

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
	logf := p.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	a := &Actor{
		params:       p,
		logf:         logf,
		hub:          NewHub(p.HubCapacity, sessionID),
		in:           make(chan actorMsg, 512),
		stopCh:       make(chan struct{}),
		finished:     make(chan struct{}),
		clients:      make(map[string]Client),
		state:        stateStarting,
		path:         p.SessionPath,
		turnState:    turnIdle,
		pendingCalls: make(map[string]chan protocol.Record),
		queueRuns:    make(map[string]*protocol.ClientRef),
		runtimeCalls: make(map[string]protocol.StateChanged),
		clearPending: make(map[string][]QueuedItem),
		owners:       make(map[string]string),
		pendingUI:    make(map[string]bool),
	}
	a.timer = time.NewTimer(time.Hour)
	if !a.timer.Stop() {
		<-a.timer.C
	}
	a.publishSnapshot()
	return a
}

// Start spawns pi and starts the actor loop.
func (a *Actor) Start() error {
	args := []string{"--mode", "rpc"}
	if a.path != "" {
		args = append(args, "--session", a.path)
	}
	args = append(args, a.params.PiArgs...)
	pi, err := StartPi(PiConfig{Bin: a.params.PiBin, Args: args, Dir: a.params.Cwd, Logf: a.logf})
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
func (a *Actor) Subscribe(clientID string, buffer int) *Subscriber {
	return a.hub.Subscribe(clientID, buffer)
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
	id := fmt.Sprintf("%sc%d", protocol.InternalIDPrefix, a.nextID.Add(1))
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
		a.logf("session %s: actor input queue full, dropping message", a.Path())
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
			a.handleMsg(m)
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
	if a.pi != nil {
		// Give pi a chance to stop the in-flight turn before stdin closes
		// (docs/design.md §4.7).
		_ = a.pi.Send([]byte(`{"type":"abort"}`))
		a.pi.Close(defaultCloseGrace)
		a.pi = nil
	}
	a.failQueued(protocol.CodeSessionCrashed, "session "+a.stopReason)
	a.failPendingCalls()
	a.hub.CloseAll()
	a.setState(stateStopped)
	if a.OnStopped != nil {
		a.OnStopped(a, a.stopReason)
	}
}

func (a *Actor) handleMsg(m actorMsg) {
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
	case m.info != nil:
		m.info <- a.info()
	}
}

// ---------------------------------------------------------------------------
// Clients

func (a *Actor) attachClient(c Client) {
	if _, ok := a.clients[c.ID()]; ok {
		return
	}
	a.clients[c.ID()] = c
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
	a.attached.Store(int64(len(a.clients)))
	a.publishPresence("leave", c)
	if a.attached.Load() == 0 && (a.state == stateCrashed || a.state == stateStopped) {
		a.dead = true
		return
	}
	a.reevaluateIdle()
}

func (a *Actor) info() Info {
	out := Info{
		Path:      a.path,
		State:     a.state,
		HeadSeq:   a.hub.HeadSeq(),
		OldestSeq: a.hub.OldestSeq(),
		Turn: protocol.TurnState{
			State:  a.turnState,
			TurnID: a.turnID,
			Author: a.turnAuthor,
			Queued: a.queue.Len(),
		},
	}
	for _, c := range a.clients {
		out.Clients = append(out.Clients, c.Info())
	}
	return out
}

// ---------------------------------------------------------------------------
// Command handling

func (a *Actor) handleCommand(c ClientCommand) {
	switch c.Type {
	case "prompt", "follow_up":
		a.handlePrompt(c)
	case "steer":
		if !c.Client.Has("interject") {
			a.errorResponse(c, protocol.CodeForbidden, "steer requires the interject capability")
			return
		}
		a.forward(c)
	case "abort":
		// Session-wide and open to any client (docs/protocol.md §3.9).
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
		a.errorResponse(c, protocol.CodeNotSupported, "fork/clone are not supported in this revision")
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
	if !c.Client.Has("prompt") {
		a.errorResponse(c, protocol.CodeForbidden, "prompt requires the prompt capability")
		return
	}
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
		a.errorResponse(c, protocol.CodeSessionCrashed, "session is not running")
		return
	}
	if a.turnState == turnIdle && a.queue.Len() == 0 {
		a.forwardCommand(item, c.Client, c.LocalID)
		return
	}
	a.queue.Push(item)
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

func (a *Actor) handleUIResponse(c ClientCommand) {
	if !c.Client.Has("ui") {
		a.errorResponse(c, protocol.CodeForbidden, "extension_ui_response requires the ui capability")
		return
	}
	if a.state != stateReady || a.pi == nil {
		a.errorResponse(c, protocol.CodeUIStale, "session is not running")
		return
	}
	if !a.pendingUI[c.LocalID] {
		a.errorResponse(c, protocol.CodeUIStale, "extension UI request is no longer pending")
		return
	}
	delete(a.pendingUI, c.LocalID)
	a.forwardRaw(c, c.LocalID)
}

// ---------------------------------------------------------------------------
// Turn and queue

func (a *Actor) startTurn(author *protocol.ClientRef) {
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
	id := fmt.Sprintf("%sq%d", protocol.InternalIDPrefix, a.nextID.Add(1))
	raw, err := protocol.RewriteCommand(it.Raw, id, "prompt")
	if err != nil {
		a.logf("session %s: dropping malformed queued command: %v", a.path, err)
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
		id := fmt.Sprintf("%sr%d", protocol.InternalIDPrefix, a.nextID.Add(1))
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
			a.logf("session %s: apply %s failed: %v", a.path, rc.Command, err)
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
		// Adopt the reported session path before publishing so the response
		// itself already carries the canonical gw_session (not s_<nanos>).
		if protocol.Field(rec.Raw, "command") == "get_state" {
			a.adoptState(rec.Raw)
		}
	}
	a.hub.Publish(rec)
	a.postProcess(rec)
}

func (a *Actor) handleInternalResponse(rec protocol.Record) {
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
	case "extension_ui_request":
		if rec.ID != "" {
			a.pendingUI[rec.ID] = true
		}
	case "response":
		cmd := protocol.Field(rec.Raw, "command")
		success := protocol.BoolField(rec.Raw, "success", false)
		if !success && (cmd == "prompt" || cmd == "follow_up") {
			// pi rejected the prompt (for example because it was still
			// streaming): release the turn so the queue is not wedged.
			a.failTurn()
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

// adoptState learns the session file path and name from a get_state response
// so an implicitly created session can be registered in the catalog.
func (a *Actor) adoptState(raw []byte) {
	data := protocol.DataField(raw)
	if len(data) == 0 {
		return
	}
	var st struct {
		SessionFile string `json:"sessionFile"`
		SessionName string `json:"sessionName"`
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return
	}
	if st.SessionName != "" {
		a.sessionName = st.SessionName
		a.publishSnapshot()
	}
	if a.path == "" && st.SessionFile != "" {
		a.setPath(st.SessionFile)
		if a.OnPath != nil {
			a.OnPath(a, st.SessionFile)
		}
	}
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
	a.publishState(stateCrashed, reason, &code)
	a.failQueued(protocol.CodeSessionCrashed, "pi exited")
	a.failPendingCalls()
	a.pendingUI = make(map[string]bool)
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
	for id, ref := range a.queueRuns {
		delete(a.queueRuns, id)
		a.publishError(ref, code, msg)
	}
	a.publishQueue()
}

func (a *Actor) failPendingCalls() {
	for id, ch := range a.pendingCalls {
		delete(a.pendingCalls, id)
		ch <- protocol.Record{}
	}
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
	a.snap.Store(&snapInfo{state: a.state, path: a.path, sessionName: a.sessionName})
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

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

// SessionID identifies one pi session (one pi process, one session file).
type SessionID string

// Client is the actor's minimal view of a connected client.
type Client interface {
	ID() string
	Name() string
	Capabilities() map[string]bool
	SendRaw(raw []byte)
	SendJSON(v any)
}

// Clients is the directory the actor uses for targeted sends.
type Clients interface {
	SendTo(clientID string, raw []byte) bool
}

// command is one inbound client command, already validated at the transport.
type command struct {
	client  Client
	localID string
	raw     []byte
	typ     string
}

// Actor owns one pi process and linearizes every command and event for that
// session. It is the only writer to pi stdin.
type Actor struct {
	id      SessionID
	hub     *Hub
	arb     *Arbiter
	pi      *PiProcess
	ui      *UIBroker
	clients Clients
	cmds    chan command

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex // guards the snapshot-visible bits below
	idle    bool
	partial partialState

	// Actor-loop-only state (no lock needed).
	pending map[string]Client
	queued  []command
	snap    *snapshotReq
	stopped bool
}

type partialState struct {
	active  bool
	content []map[string]any
}

type snapshotReq struct {
	client  Client
	state   json.RawMessage
	entries json.RawMessage
	leafID  string
	need    int // bit 1 = state, bit 2 = entries
}

const snapIDPrefix = "__gw_snap__:"

func NewActor(ctx context.Context, id SessionID, piCfg PiConfig, hubCapacity int, mode Mode) (*Actor, error) {
	ctx, cancel := context.WithCancel(ctx)
	pi, err := StartPi(ctx, piCfg)
	if err != nil {
		cancel()
		return nil, err
	}
	a := &Actor{
		id:      id,
		hub:     NewHub(hubCapacity, string(id)),
		arb:     NewArbiter(mode, 120*time.Second),
		pi:      pi,
		ui:      NewUIBroker(),
		cmds:    make(chan command, 256),
		ctx:     ctx,
		cancel:  cancel,
		idle:    true,
		pending: make(map[string]Client),
	}
	return a, nil
}

func (a *Actor) ID() SessionID            { return a.id }
func (a *Actor) Hub() *Hub                { return a.hub }
func (a *Actor) Context() context.Context { return a.ctx }

func (a *Actor) SetClients(c Clients) { a.clients = c }

// Submit enqueues a command. It never blocks: a full queue is reported so the
// caller can return queue_full to the client.
func (a *Actor) Submit(cmd command) bool {
	select {
	case a.cmds <- cmd:
		return true
	case <-a.ctx.Done():
		return false
	default:
		return false
	}
}

// Snapshot returns the cached idle flag and in-flight assistant prefix.
func (a *Actor) Snapshot() (idle bool, partial []map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]map[string]any, len(a.partial.content))
	copy(out, a.partial.content)
	return a.idle, out
}

// Run is the actor loop. It prioritizes pi events over client commands so the
// event stream is never reordered or delayed by command handling.
func (a *Actor) Run() {
	defer a.cancel()
	for {
		// Drain pi stdout first.
		select {
		case rec, ok := <-a.pi.Events():
			if !ok {
				a.onPiExit()
				return
			}
			a.onPiRecord(rec)
			continue
		default:
		}

		select {
		case <-a.ctx.Done():
			a.shutdown()
			return
		case rec, ok := <-a.pi.Events():
			if !ok {
				a.onPiExit()
				return
			}
			a.onPiRecord(rec)
		case cmd := <-a.cmds:
			a.onCommand(cmd)
		}
	}
}

func (a *Actor) onPiExit() {
	a.mu.Lock()
	a.stopped = true
	a.idle = true
	a.mu.Unlock()
	a.arb.AbortAll()
	a.failQueued("session_crashed", "pi process exited")
	a.publish(map[string]any{"type": "gw_session_state", "state": "stopped", "reason": errString(a.pi.Err())})
}

func (a *Actor) shutdown() {
	a.arb.AbortAll()
	a.failQueued("session_crashed", "gateway shutting down")
	_ = a.pi.Close()
}

func (a *Actor) failQueued(code, msg string) {
	for _, cmd := range a.queued {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, code, msg, ""))
	}
	a.queued = nil
}

// ---------------------------------------------------------------------------
// pi event handling
// ---------------------------------------------------------------------------

func (a *Actor) onPiRecord(rec protocol.Record) {
	switch rec.Type {
	case "agent_start":
		a.setIdle(false)
		a.publishTurn("running")
	case "agent_settled":
		a.setIdle(true)
		next := a.arb.Settle()
		a.publishTurn("settled")
		a.publishFloor("settled")
		a.flushQueue(next)
	case "message_start":
		a.startPartial(rec)
	case "message_update":
		a.updatePartial(rec)
	case "message_end":
		a.clearPartial()
	case "extension_ui_request":
		a.handleUIRequest(rec)
	case "response":
		a.routeResponse(rec)
		return // responses are targeted, not broadcast
	case "bash_execution_update":
		a.routeBashUpdate(rec)
		return
	}

	// Broadcast everything else, with gateway metadata.
	fields := map[string]any{
		"gw_session": string(a.id),
		"gw_ts":      time.Now().UTC().Format(time.RFC3339Nano),
	}
	stamped, err := protocol.Stamp(rec.Raw, fields)
	if err != nil {
		log.Printf("gateway: stamp event: %v", err)
		stamped = rec.Raw
	}
	a.hub.Publish(protocol.Record{Raw: stamped, Type: rec.Type, ID: rec.ID})
}

func (a *Actor) publish(v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		log.Printf("gateway: marshal gateway event: %v", err)
		return
	}
	stamped, err := protocol.Stamp(raw, map[string]any{
		"gw_session": string(a.id),
		"gw_ts":      time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		stamped = raw
	}
	a.hub.Publish(protocol.Record{Raw: stamped, Type: protocol.Field(raw, "type")})
}

func (a *Actor) publishTurn(state string) {
	owner, _ := a.arb.Owner(time.Now())
	a.publish(map[string]any{
		"type":  "gw_turn",
		"state": state,
		"owner": owner,
	})
}

func (a *Actor) publishFloor(reason string) {
	owner, _ := a.arb.Owner(time.Now())
	fields := map[string]any{
		"type":   "gw_floor",
		"owner":  owner,
		"mode":   string(a.arb.Mode()),
		"reason": reason,
	}
	if owner != "" {
		fields["expiresAt"] = time.Now().Add(120 * time.Second).UTC().Format(time.RFC3339)
	}
	a.publish(fields)
}

func (a *Actor) setIdle(v bool) {
	a.mu.Lock()
	a.idle = v
	a.mu.Unlock()
}

func (a *Actor) startPartial(rec protocol.Record) {
	a.mu.Lock()
	a.partial = partialState{active: true}
	a.mu.Unlock()
}

func (a *Actor) updatePartial(rec protocol.Record) {
	var ev struct {
		AssistantMessageEvent struct {
			Type         string `json:"type"`
			ContentIndex int    `json:"contentIndex"`
			Delta        string `json:"delta"`
		} `json:"assistantMessageEvent"`
	}
	if err := json.Unmarshal(rec.Raw, &ev); err != nil {
		return
	}
	d := ev.AssistantMessageEvent
	switch d.Type {
	case "text_delta", "thinking_delta":
		a.mu.Lock()
		defer a.mu.Unlock()
		for len(a.partial.content) <= d.ContentIndex {
			a.partial.content = append(a.partial.content, nil)
		}
		block := a.partial.content[d.ContentIndex]
		if block == nil {
			kind := "text"
			if d.Type == "thinking_delta" {
				kind = "thinking"
			}
			block = map[string]any{"type": kind, "text": ""}
			if kind == "thinking" {
				block = map[string]any{"type": kind, "thinking": ""}
			}
			a.partial.content[d.ContentIndex] = block
		}
		key := "text"
		if d.Type == "thinking_delta" {
			key = "thinking"
		}
		block[key] = block[key].(string) + d.Delta
	}
}

func (a *Actor) clearPartial() {
	a.mu.Lock()
	a.partial = partialState{}
	a.mu.Unlock()
}

// ---------------------------------------------------------------------------
// command handling
// ---------------------------------------------------------------------------

func (a *Actor) onCommand(cmd command) {
	a.mu.Lock()
	stopped := a.stopped
	a.mu.Unlock()
	if stopped && cmd.typ != "get_state" && !strings.HasPrefix(cmd.typ, "gw_") {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "session_crashed", "pi process is not running", ""))
		return
	}

	switch cmd.typ {
	case "gw_ping":
		cmd.client.SendJSON(map[string]any{"type": "gw_pong"})
	case "gw_take_turn":
		a.handleTakeTurn(cmd)
	case "gw_release_turn":
		d := a.arb.ReleaseTurn(cmd.client.ID())
		if d.Verdict == Reject {
			cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, d.Code, d.Message, d.Owner))
			return
		}
		a.publishFloor("released")
		cmd.client.SendJSON(map[string]any{"type": "response", "id": cmd.localID, "command": cmd.typ, "success": true})
	case "gw_set_mode":
		a.handleSetMode(cmd)
	case "gw_request_snapshot":
		a.handleSnapshotRequest(cmd)
	case "extension_ui_response":
		a.handleUIResponse(cmd)
	case "prompt":
		a.handlePrompt(cmd)
	case "steer", "follow_up":
		if d := a.arb.AdmitInterjection(); d.Verdict == Reject {
			cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, d.Code, d.Message, d.Owner))
			return
		}
		a.forward(cmd)
	case "abort":
		if !a.requireControl(cmd) {
			return
		}
		a.arb.AbortAll()
		a.forward(cmd)
	case "new_session", "switch_session", "fork", "clone", "compact",
		"set_model", "cycle_model", "set_thinking_level", "cycle_thinking_level",
		"set_steering_mode", "set_follow_up_mode", "set_auto_compaction",
		"set_auto_retry", "abort_retry", "set_session_name", "abort_bash":
		if !a.requireControl(cmd) {
			return
		}
		a.forward(cmd)
		a.publish(map[string]any{"type": "gw_state_changed", "command": cmd.typ, "by": cmd.client.ID()})
	default:
		// Read-only queries (get_state, get_messages, get_entries, ...) and
		// anything else pi understands pass straight through.
		a.forward(cmd)
	}
}

func (a *Actor) requireControl(cmd command) bool {
	if cmd.client.Capabilities()["control"] {
		return true
	}
	cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "forbidden", "control capability required", ""))
	return false
}

func (a *Actor) handlePrompt(cmd command) {
	a.mu.Lock()
	idle := a.idle
	a.mu.Unlock()

	d := a.arb.AdmitPrompt(cmd.client.ID(), idle, time.Now())
	switch d.Verdict {
	case Reject:
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, d.Code, d.Message, d.Owner))
		return
	case Queue:
		a.queued = append(a.queued, cmd)
		a.publishFloor("queued")
		cmd.client.SendJSON(map[string]any{
			"type": "response", "id": cmd.localID, "command": cmd.typ,
			"success": true, "queued": true, "queuePosition": len(a.queued),
		})
		return
	}
	a.publishFloor("acquired")
	a.forward(cmd)
}

func (a *Actor) flushQueue(next string) {
	if len(a.queued) == 0 {
		return
	}
	cmd := a.queued[0]
	a.queued = a.queued[1:]
	a.publishFloor("dequeued")
	a.forward(cmd)
}

func (a *Actor) handleTakeTurn(cmd command) {
	var body struct {
		TTLMs int  `json:"ttlMs"`
		Steal bool `json:"steal"`
	}
	_ = json.Unmarshal(cmd.raw, &body)
	if body.Steal && !a.requireControl(cmd) {
		return
	}
	d := a.arb.TakeTurn(cmd.client.ID(), time.Duration(body.TTLMs)*time.Millisecond, time.Now())
	if d.Verdict == Reject {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, d.Code, d.Message, d.Owner))
		return
	}
	a.publishFloor("taken")
	cmd.client.SendJSON(map[string]any{"type": "response", "id": cmd.localID, "command": cmd.typ, "success": true})
}

func (a *Actor) handleSetMode(cmd command) {
	if !a.requireControl(cmd) {
		return
	}
	var body struct {
		Mode Mode `json:"mode"`
	}
	if err := json.Unmarshal(cmd.raw, &body); err != nil {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "bad_frame", err.Error(), ""))
		return
	}
	switch body.Mode {
	case ModeExclusive, ModeQueue, ModeOwnerOnly, ModeReadOnly:
	default:
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "bad_frame", "unknown mode", ""))
		return
	}
	a.arb.SetMode(body.Mode)
	a.publishFloor("mode_changed")
	cmd.client.SendJSON(map[string]any{"type": "response", "id": cmd.localID, "command": cmd.typ, "success": true})
}

func (a *Actor) forward(cmd command) {
	if a.clients == nil {
		// still allow pi forwarding; direct sends will be dropped
	}
	nsID := protocol.NamespaceID(cmd.client.ID(), cmd.localID)
	raw, err := protocol.RewriteID(cmd.raw, nsID)
	if err != nil {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "bad_frame", err.Error(), ""))
		return
	}
	if err := a.pi.Send(raw); err != nil {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "session_crashed", err.Error(), ""))
		return
	}
	a.pending[nsID] = cmd.client
}

func (a *Actor) routeResponse(rec protocol.Record) {
	if strings.HasPrefix(rec.ID, snapIDPrefix) {
		a.onSnapshotResponse(rec)
		return
	}
	origin, ok := a.pending[rec.ID]
	if ok {
		delete(a.pending, rec.ID)
	}
	if origin == nil {
		// Response to an internal command whose client is gone, or an
		// unnamespaced id. Drop it rather than leak it to other clients.
		return
	}
	_, localID := protocol.SplitNamespaceID(rec.ID)
	out, err := protocol.RewriteID(rec.Raw, localID)
	if err != nil {
		out = rec.Raw
	}
	origin.SendRaw(out)
}

func (a *Actor) routeBashUpdate(rec protocol.Record) {
	origin, ok := a.pending[rec.ID]
	_, localID := protocol.SplitNamespaceID(rec.ID)
	if ok {
		out, err := protocol.RewriteID(rec.Raw, localID)
		if err != nil {
			out = rec.Raw
		}
		origin.SendRaw(out)
	}
	// Other clients get a copy tagged with the owner so they can render
	// remote activity. The owner's connection filters this out.
	ownerID, _ := protocol.SplitNamespaceID(rec.ID)
	fields := map[string]any{
		"gw_session": string(a.id),
		"gw_ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"gw_owner":   ownerID,
	}
	stamped, err := protocol.Stamp(rec.Raw, fields)
	if err != nil {
		return
	}
	stamped, _ = protocol.RewriteID(stamped, "")
	a.hub.Publish(protocol.Record{Raw: stamped, Type: rec.Type})
}

// ---------------------------------------------------------------------------
// snapshot / resync
// ---------------------------------------------------------------------------

func (a *Actor) handleSnapshotRequest(cmd command) {
	stateID := snapIDPrefix + cmd.client.ID() + ":state"
	entriesID := snapIDPrefix + cmd.client.ID() + ":entries"
	a.snap = &snapshotReq{client: cmd.client, need: 3}
	_ = a.pi.Send(mustJSON(map[string]any{"type": "get_state", "id": stateID}))
	_ = a.pi.Send(mustJSON(map[string]any{"type": "get_entries", "id": entriesID}))
}

func (a *Actor) onSnapshotResponse(rec protocol.Record) {
	if a.snap == nil {
		return
	}
	suffix := strings.TrimPrefix(rec.ID, snapIDPrefix)
	switch {
	case strings.HasSuffix(suffix, ":state"):
		a.snap.state = dataField(rec.Raw)
		a.snap.need &^= 1
	case strings.HasSuffix(suffix, ":entries"):
		a.snap.entries = dataField(rec.Raw)
		a.snap.leafID = protocol.Field(rec.Raw, "leafId")
		a.snap.need &^= 2
	}
	if a.snap.need != 0 {
		return
	}
	_, partial := a.Snapshot()
	msg := map[string]any{
		"type":    "gw_snapshot",
		"piState": json.RawMessage(a.snap.state),
		"entries": json.RawMessage(a.snap.entries),
		"leafId":  a.snap.leafID,
	}
	if len(partial) > 0 {
		msg["partial"] = partial
	}
	a.snap.client.SendJSON(msg)
	a.snap = nil
}

// ---------------------------------------------------------------------------
// extension UI
// ---------------------------------------------------------------------------

func (a *Actor) handleUIRequest(rec protocol.Record) {
	owner, _ := a.arb.Owner(time.Now())
	faf, preferred := a.ui.OnRequest(rec, owner)
	fields := map[string]any{
		"gw_session": string(a.id),
		"gw_ts":      time.Now().UTC().Format(time.RFC3339Nano),
	}
	if preferred != "" {
		fields["gw_target"] = preferred
	}
	if !faf {
		fields["gw_dialog"] = true
	}
	stamped, err := protocol.Stamp(rec.Raw, fields)
	if err != nil {
		stamped = rec.Raw
	}
	a.hub.Publish(protocol.Record{Raw: stamped, Type: rec.Type, ID: rec.ID})
}

func (a *Actor) handleUIResponse(cmd command) {
	forward, code, msg := a.ui.OnResponse(cmd.client.ID(), protocol.Record{Raw: cmd.raw, ID: cmd.localID})
	if !forward {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, code, msg, ""))
		return
	}
	// Forward the response to pi with its original (pi-generated) id.
	raw, err := protocol.RewriteID(cmd.raw, cmd.localID)
	if err != nil {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "bad_frame", err.Error(), ""))
		return
	}
	if err := a.pi.Send(raw); err != nil {
		cmd.client.SendJSON(errorResponse(cmd.localID, cmd.typ, "session_crashed", err.Error(), ""))
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func errorResponse(id, cmd, code, message, owner string) map[string]any {
	m := map[string]any{
		"type":    "response",
		"command": cmd,
		"success": false,
		"error":   message,
		"code":    code,
	}
	if id != "" {
		m["id"] = id
	}
	if owner != "" {
		m["owner"] = owner
	}
	return m
}

func dataField(raw []byte) json.RawMessage {
	var obj struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Data
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("gateway: marshal: %v", err))
	}
	return b
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

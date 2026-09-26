package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Gateway control messages (gw_*) plus the hello handshake.
//
// Only fields the daemon understands are typed here; unknown JSON fields are
// ignored by encoding/json, so clients may send newer payloads.

// Hello is the first frame a client must send.
type Hello struct {
	Type     string     `json:"type"`
	Protocol int        `json:"protocol"`
	Token    string     `json:"token"`
	Client   ClientInfo `json:"client"`
	// Cwd is the client's working directory; new sessions spawn pi there.
	// Empty means "the daemon's own directory" (docs/protocol.md §2).
	Cwd        string   `json:"cwd,omitempty"`
	Session    string   `json:"session,omitempty"`
	PiArgs     []string `json:"piArgs,omitempty"`
	Resume     *Resume  `json:"resume,omitempty"`
	LiveOnly   bool     `json:"liveOnly,omitempty"`
	AllowLossy bool     `json:"allowLossy,omitempty"`
}

// ClientInfo identifies a client connection.
type ClientInfo struct {
	Name         string            `json:"name,omitempty"`
	Kind         string            `json:"kind,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// Resume is a replay cursor.
type Resume struct {
	SinceSeq    uint64 `json:"sinceSeq,omitempty"`
	LeafEntryID string `json:"leafEntryId,omitempty"`
}

// SessionRef names a session in welcome messages.
type SessionRef struct {
	Path string `json:"path"`
	Name string `json:"name,omitempty"`
	ID   string `json:"id,omitempty"`
}

// ClientSummary is the daemon's view of an attached client, used for the
// gw_welcome roster and catalog rows. Unlike ClientInfo (what a client sends)
// it includes the assigned clientId.
type ClientSummary struct {
	ClientID     string            `json:"clientId"`
	Name         string            `json:"name,omitempty"`
	Kind         string            `json:"kind,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Tags         map[string]string `json:"tags,omitempty"`
}

// ClientRef is the compact client identity carried on gateway events. Tags is
// set where a creator's integration tags are reported (catalog rows).
type ClientRef struct {
	ClientID string            `json:"clientId"`
	Kind     string            `json:"kind,omitempty"`
	Name     string            `json:"name,omitempty"`
	Tags     map[string]string `json:"tags,omitempty"`
}

// TurnState reports the current turn in gw_welcome.
type TurnState struct {
	State  string     `json:"state"`
	TurnID string     `json:"turnId,omitempty"`
	Author *ClientRef `json:"author,omitempty"`
	Queued int        `json:"queued"`
}

// Welcome is the handshake result.
type Welcome struct {
	Type           string          `json:"type"`
	Protocol       int             `json:"protocol"`
	ClientID       string          `json:"clientId"`
	Kind           string          `json:"kind,omitempty"`
	Granted        []string        `json:"granted"`
	PiVersion      string          `json:"piVersion,omitempty"`
	Concurrency    string          `json:"concurrency"`
	Session        *SessionRef     `json:"session"`
	HeadSeq        uint64          `json:"headSeq"`
	OldestSeq      uint64          `json:"oldestSeq"`
	ResyncRequired bool            `json:"resyncRequired"`
	PiState        json.RawMessage `json:"piState"`
	Turn           TurnState       `json:"turn"`
	Clients        []ClientSummary `json:"clients"`
}

// TurnEvent is a gw_turn lifecycle notification.
type TurnEvent struct {
	Type   string     `json:"type"`
	State  string     `json:"state"`
	TurnID string     `json:"turnId,omitempty"`
	Author *ClientRef `json:"author,omitempty"`
}

// QueuePending is one daemon-queued prompt.
type QueuePending struct {
	ID      string     `json:"id"`
	Mode    string     `json:"mode"`
	Author  *ClientRef `json:"author,omitempty"`
	Preview string     `json:"preview"`
}

// QueueEvent is a gw_queue notification.
type QueueEvent struct {
	Type    string         `json:"type"`
	Pending []QueuePending `json:"pending"`
}

// PresenceEvent is a gw_presence roster notification.
type PresenceEvent struct {
	Type   string    `json:"type"`
	Event  string    `json:"event"`
	Client ClientRef `json:"client"`
}

// SessionStateEvent is a gw_session_state lifecycle notification.
type SessionStateEvent struct {
	Type     string `json:"type"`
	State    string `json:"state"`
	Reason   string `json:"reason,omitempty"`
	ExitCode *int   `json:"exitCode,omitempty"`
}

// Session states carried by gw_session_state (docs/protocol.md §5.2). The
// terminal SessionStateDeleted is the only one that changes client behavior:
// the session file is gone and the session is unusable.
const (
	SessionStateDeleted = "deleted"
)

// Stop reasons reported on the terminal gw_session_state of
// gw_stop_session/gw_delete_session: requested (nothing was overridden) or
// forced (a running turn or attached clients were overridden).
const (
	StopReasonRequested = "requested"
	StopReasonForced    = "forced"
)

// ErrorEvent is a gw_error protocol error.
type ErrorEvent struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
	ID      string `json:"id,omitempty"`
}

// Pong answers gw_ping.
type Pong struct {
	Type string `json:"type"`
}

// ReplayDone ends a replay window.
type ReplayDone struct {
	Type    string `json:"type"`
	HeadSeq uint64 `json:"headSeq"`
}

// Snapshot is a full resync payload. HeadSeq is the hub watermark the
// snapshot covers: live records continue after it, so a reconnecting client
// can resume from HeadSeq instead of re-reading a ring that was reset.
type Snapshot struct {
	Type    string            `json:"type"`
	PiState json.RawMessage   `json:"piState"`
	Entries []json.RawMessage `json:"entries"`
	LeafID  string            `json:"leafId,omitempty"`
	HeadSeq uint64            `json:"headSeq,omitempty"`
}

// StateChanged announces a shared-state mutation.
type StateChanged struct {
	Type    string          `json:"type"`
	Command string          `json:"command"`
	Data    json.RawMessage `json:"data,omitempty"`
	By      *ClientRef      `json:"by,omitempty"`
}

// Capabilities are the authority model (docs/design.md §10). The default
// token grants the full set, so an empty request means everything.
const (
	CapObserve   = "observe"
	CapInterject = "interject"
	CapPrompt    = "prompt"
	CapUI        = "ui"
	CapControl   = "control"
	CapAdmin     = "admin"
)

// AllCapabilities is the full capability set granted by the default token; it
// is also what the bridge requests on a UI's behalf.
var AllCapabilities = []string{CapObserve, CapInterject, CapPrompt, CapUI, CapControl, CapAdmin}

// NewError builds a gw_error frame.
func NewError(code, msg string) ErrorEvent {
	return ErrorEvent{Type: "gw_error", Code: code, Message: msg}
}

// Error codes from docs/protocol.md §8.
const (
	CodeUnauthorized       = "unauthorized"
	CodeForbidden          = "forbidden"
	CodeBadFrame           = "bad_frame"
	CodeUnknownSession     = "unknown_session"
	CodeAmbiguousSession   = "ambiguous_session"
	CodeSpawnParamConflict = "spawn_param_conflict"
	CodeSharedSession      = "shared_session"
	CodeReloadBusy         = "reload_busy"
	CodeSessionBusy        = "session_busy"
	CodeSessionAttached    = "session_attached"
	CodeQueueFull          = "queue_full"
	CodeSlowConsumer       = "slow_consumer"
	CodeUIStale            = "ui_stale"
	CodeSessionCrashed     = "session_crashed"
	CodeResyncRequired     = "resync_required"
	CodeNotSupported       = "not_supported"
)

// Response builds a pi-shaped response frame with a local id. data may be nil.
func Response(id, command string, success bool, code, errMsg string, data json.RawMessage) []byte {
	obj := map[string]any{
		"type":    "response",
		"command": command,
		"success": success,
	}
	if id != "" {
		obj["id"] = id
	}
	if !success {
		obj["error"] = errMsg
		if code != "" {
			obj["code"] = code
		}
	}
	if len(data) > 0 {
		obj["data"] = json.RawMessage(data)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return fmt.Appendf(nil, `{"type":"response","id":%q,"command":%q,"success":false,"error":"internal marshal error"}`, id, command)
	}
	return out
}

// ErrUnsupportedVersion marks a gw_hello whose protocol version the daemon
// does not speak; the daemon answers such a frame with `not_supported`.
var ErrUnsupportedVersion = errors.New("protocol: unsupported protocol version")

// ParseHello decodes a gw_hello frame.
func ParseHello(raw []byte) (*Hello, error) {
	var h Hello
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil, fmt.Errorf("protocol: gw_hello: %w", err)
	}
	if h.Type != "gw_hello" {
		return nil, fmt.Errorf("protocol: expected gw_hello, got %q", h.Type)
	}
	if h.Protocol != Version {
		return nil, fmt.Errorf("%w: %d (daemon speaks %d)", ErrUnsupportedVersion, h.Protocol, Version)
	}
	return &h, nil
}

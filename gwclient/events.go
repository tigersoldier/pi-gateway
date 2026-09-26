package gwclient

import (
	"encoding/json"

	"github.com/tigersoldier/pi-gateway/protocol"
)

// Typed decoders for the gateway's own events (docs/protocol.md §5.2), so a
// consumer does not hand-roll json.Unmarshal for each one. Each reports the
// decode error; callers usually switch on Event.Type first.

// Snapshot decodes a gw_snapshot (full resync payload).
func (e Event) Snapshot() (protocol.Snapshot, error) {
	var v protocol.Snapshot
	err := e.Unmarshal(&v)
	return v, err
}

// Turn decodes a gw_turn (turn lifecycle).
func (e Event) Turn() (protocol.TurnEvent, error) {
	var v protocol.TurnEvent
	err := e.Unmarshal(&v)
	return v, err
}

// Queue decodes a gw_queue (daemon queue contents).
func (e Event) Queue() (protocol.QueueEvent, error) {
	var v protocol.QueueEvent
	err := e.Unmarshal(&v)
	return v, err
}

// Presence decodes a gw_presence (client roster change).
func (e Event) Presence() (protocol.PresenceEvent, error) {
	var v protocol.PresenceEvent
	err := e.Unmarshal(&v)
	return v, err
}

// SessionState decodes a gw_session_state (pi process lifecycle).
func (e Event) SessionState() (protocol.SessionStateEvent, error) {
	var v protocol.SessionStateEvent
	err := e.Unmarshal(&v)
	return v, err
}

// StateChanged decodes a gw_state_changed (shared-state mutation).
func (e Event) StateChanged() (protocol.StateChanged, error) {
	var v protocol.StateChanged
	err := e.Unmarshal(&v)
	return v, err
}

// ReplayDone decodes a gw_replay_done (end of a replay window).
func (e Event) ReplayDone() (protocol.ReplayDone, error) {
	var v protocol.ReplayDone
	err := e.Unmarshal(&v)
	return v, err
}

// ErrorEvent decodes a gw_error (protocol error, usually owner-scoped).
func (e Event) ErrorEvent() (protocol.ErrorEvent, error) {
	var v protocol.ErrorEvent
	err := e.Unmarshal(&v)
	return v, err
}

// Lag decodes a gw_lag (a lossy client missed non-terminal records). oldSeq
// and headSeq say which range was lost; recover by reconnecting with a cursor
// at or before oldSeq and applying the snapshot the daemon answers with.
func (e Event) Lag() (oldSeq, headSeq uint64) {
	var v struct {
		OldestSeq uint64 `json:"oldestSeq"`
		HeadSeq   uint64 `json:"headSeq"`
	}
	_ = e.Unmarshal(&v)
	return v.OldestSeq, v.HeadSeq
}

// Compile-time reminder that Event.Raw is JSON: helpers here all decode it.
var _ = json.RawMessage(nil)

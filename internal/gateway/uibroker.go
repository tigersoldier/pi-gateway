package gateway

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

// fireAndForgetUI are extension UI methods that expect no response.
var fireAndForgetUI = map[string]bool{
	"notify":          true,
	"setStatus":       true,
	"setWidget":       true,
	"setTitle":        true,
	"set_editor_text": true,
}

type uiPending struct {
	method   string
	winner   string
	deadline time.Time
}

// UIBroker routes pi's extension_ui_request dialogs to exactly one client and
// enforces first-response-wins for broadcast dialogs.
type UIBroker struct {
	mu      sync.Mutex
	pending map[string]*uiPending
}

func NewUIBroker() *UIBroker {
	return &UIBroker{pending: make(map[string]*uiPending)}
}

// OnRequest records a dialog (if it needs a response) and reports whether the
// request is fire-and-forget and which client should be preferred.
//
// Preference: the current floor owner when it has UI capability, otherwise the
// caller broadcasts with first-response-wins.
func (b *UIBroker) OnRequest(rec protocol.Record, floorOwner string) (fireAndForget bool, preferred string) {
	method := protocol.Field(rec.Raw, "method")
	if fireAndForgetUI[method] {
		return true, ""
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	timeout := time.Duration(0)
	if ms := intField(rec.Raw, "timeout"); ms > 0 {
		timeout = time.Duration(ms) * time.Millisecond
	}
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	b.pending[rec.ID] = &uiPending{method: method, deadline: deadline}
	return false, floorOwner
}

// OnResponse validates a client's extension_ui_response.
//
// It returns forward=true exactly once per request, for the winning client.
func (b *UIBroker) OnResponse(clientID string, rec protocol.Record) (forward bool, code, message string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.pending[rec.ID]
	if p == nil {
		return false, "ui_stale", "no pending extension UI request"
	}
	if !p.deadline.IsZero() && time.Now().After(p.deadline) {
		delete(b.pending, rec.ID)
		return false, "ui_stale", "extension UI request timed out"
	}
	if p.winner == "" {
		p.winner = clientID
		delete(b.pending, rec.ID)
		return true, "", ""
	}
	if p.winner == clientID {
		return true, "", ""
	}
	return false, "ui_stale", "another client already answered"
}

// Forget drops a request, e.g. when its pi timeout fired or the owner left.
func (b *UIBroker) Forget(id string) {
	b.mu.Lock()
	delete(b.pending, id)
	b.mu.Unlock()
}

func intField(raw []byte, name string) int {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return 0
	}
	switch v := obj[name].(type) {
	case float64:
		return int(v)
	case int:
		return v
	}
	return 0
}

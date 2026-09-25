package protocol

import "strings"

// Capability roles bundle the capabilities a provisioned token confers
// (docs/design.md §10). A token in ~/.config/pi-gateway/tokens.json names
// either one of these presets ("role") or an explicit capability list.
const (
	RoleAdmin    = "admin"
	RoleOperator = "operator"
	RoleObserver = "observer"
)

// TokenGrant is one accepted token and the capabilities it confers.
type TokenGrant struct {
	Name         string
	Token        string
	Capabilities []string
}

// RoleNames lists the preset roles, most powerful first.
func RoleNames() []string { return []string{RoleAdmin, RoleOperator, RoleObserver} }

// RoleCapabilities resolves a preset role name to its capabilities.
func RoleCapabilities(role string) ([]string, bool) {
	switch role {
	case RoleAdmin:
		return append([]string(nil), AllCapabilities...), true
	case RoleOperator:
		return []string{CapObserve, CapInterject, CapPrompt, CapUI}, true
	case RoleObserver:
		return []string{CapObserve}, true
	}
	return nil, false
}

// CommandCapability returns the capability a client command requires, or ""
// when any authenticated client may send it. This is the single
// command→capability table from docs/design.md §10; the daemon enforces it
// once, before dispatching the frame (docs/protocol.md §10).
//
// Command types are canonical: the daemon rejects a frame whose type is not
// lowercase, so the table never has to consider case variants.
func CommandCapability(command string) string {
	if strings.HasPrefix(command, "get_") {
		// pi's queries only observe a session.
		return CapObserve
	}
	switch command {
	// Reading a session or its transcript.
	case "gw_list_sessions", "export_html":
		return CapObserve
	// Causing work in the session. `bash` runs a shell command as the daemon
	// user, which a prompt-capable client could reach through the agent
	// anyway; `switch_session` may start pi for a hibernated session.
	case "prompt", "follow_up", "new_session", "fork", "clone", "bash", "switch_session":
		return CapPrompt
	// Interjecting into (or cancelling) a running turn.
	case "steer", "abort", "abort_bash", "abort_retry", "clear_queue":
		return CapInterject
	// Driving the user's UI.
	case "extension_ui_response", "notify":
		return CapUI
	// Session-global state and administration.
	case "gw_reload_session",
		"set_model", "cycle_model",
		"set_thinking_level", "cycle_thinking_level",
		"set_steering_mode", "set_follow_up_mode",
		"compact", "set_auto_compaction", "set_auto_retry",
		"set_session_name", "set_editor_text":
		return CapControl
	// Provisioning.
	case "gw_new_session":
		return CapAdmin
	}
	// Deliberately open to any authenticated client: only the connection
	// keep-alives. Everything that changes or cancels session work is gated,
	// including the cancellation primitives (they share `interject` with
	// `steer`, because `clear_queue` can withdraw another client's queued or
	// forwarded prompt).
	return ""
}

// NormalizeCapabilities keeps the known capabilities from caps in canonical
// order and reports the unknown ones separately.
func NormalizeCapabilities(caps []string) (known, unknown []string) {
	want := make(map[string]bool, len(caps))
	for _, c := range caps {
		want[c] = true
	}
	for _, c := range AllCapabilities {
		if want[c] {
			known = append(known, c)
		}
	}
	for _, c := range caps {
		if !slicesContains(AllCapabilities, c) && !slicesContains(unknown, c) {
			unknown = append(unknown, c)
		}
	}
	return known, unknown
}

func slicesContains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

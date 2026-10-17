package protocol

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

// commandCapabilities is the explicit client-command allowlist and the single
// command→capability table from docs/design.md §10. Empty values are supported
// commands requiring no capability; absence means unsupported, not allowed.
// Keep this synchronized with the complete command surface in docs/protocol.md
// §4. The daemon checks IsSupportedCommand before dispatch and checks the
// returned capability once before any command can create a session.
var commandCapabilities = map[string]string{
	// Reading a session or its transcript.
	"get_state":                     CapObserve,
	"get_messages":                  CapObserve,
	"get_available_models":          CapObserve,
	"get_available_thinking_levels": CapObserve,
	"get_session_stats":             CapObserve,
	"get_entries":                   CapObserve,
	"get_tree":                      CapObserve,
	"get_fork_messages":             CapObserve,
	"get_last_assistant_text":       CapObserve,
	"get_commands":                  CapObserve,
	"export_html":                   CapObserve,
	"gw_list_sessions":              CapObserve,
	// Causing work in the session. `bash` runs a shell command as the daemon
	// user, which a prompt-capable client could reach through the agent
	// anyway; `switch_session` may start pi for a hibernated session.
	"prompt":         CapPrompt,
	"follow_up":      CapPrompt,
	"new_session":    CapPrompt,
	"fork":           CapPrompt,
	"clone":          CapPrompt,
	"bash":           CapPrompt,
	"switch_session": CapPrompt,
	// Interjecting into (or cancelling) a running turn.
	"steer":       CapInterject,
	"abort":       CapInterject,
	"abort_bash":  CapInterject,
	"abort_retry": CapInterject,
	"clear_queue": CapInterject,
	// Driving the user's UI.
	"extension_ui_response": CapUI,
	// Session-global state and administration.
	"gw_reload_session":    CapControl,
	"set_model":            CapControl,
	"cycle_model":          CapControl,
	"set_thinking_level":   CapControl,
	"cycle_thinking_level": CapControl,
	"set_steering_mode":    CapControl,
	"set_follow_up_mode":   CapControl,
	"compact":              CapControl,
	"set_auto_compaction":  CapControl,
	"set_auto_retry":       CapControl,
	"set_session_name":     CapControl,
	// Provisioning: creating a session and destroying one are the operator's
	// authority, not something any attached client may do (docs/design.md §10).
	"gw_new_session":    CapAdmin,
	"gw_stop_session":   CapAdmin,
	"gw_delete_session": CapAdmin,
	// Connection keep-alives are the only supported commands with no capability.
	"gw_ping": "",
	"gw_bye":  "",
}

// IsSupportedCommand reports whether command is part of the gateway's client
// command surface. Unknown types are not made valid by an empty capability.
func IsSupportedCommand(command string) bool {
	_, ok := commandCapabilities[command]
	return ok
}

// CommandCapability returns the capability a supported command requires, or
// "" if it requires no capability or is unsupported. Callers that dispatch a
// command must first check IsSupportedCommand. Only commands a client may send
// appear in the table; pi's `extension_ui_request` methods (`notify`,
// `setStatus`, `setWidget`, `setTitle`, `set_editor_text`) are outbound
// notifications, not commands. Command types are canonical: the daemon rejects
// frames whose type is not lowercase, so case variants are not in the table.
func CommandCapability(command string) string {
	return commandCapabilities[command]
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

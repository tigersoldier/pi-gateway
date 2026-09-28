package protocol

import (
	"reflect"
	"testing"
)

func TestRoleCapabilities(t *testing.T) {
	admin, ok := RoleCapabilities(RoleAdmin)
	if !ok || !reflect.DeepEqual(admin, AllCapabilities) {
		t.Fatalf("admin = %v, ok=%v", admin, ok)
	}
	operator, ok := RoleCapabilities(RoleOperator)
	if !ok || !reflect.DeepEqual(operator, []string{CapObserve, CapInterject, CapPrompt, CapUI}) {
		t.Fatalf("operator = %v, ok=%v", operator, ok)
	}
	observer, ok := RoleCapabilities(RoleObserver)
	if !ok || !reflect.DeepEqual(observer, []string{CapObserve}) {
		t.Fatalf("observer = %v, ok=%v", observer, ok)
	}
	if _, ok := RoleCapabilities("root"); ok {
		t.Fatal("unknown role should not resolve")
	}
	// The returned slice must not alias the package-level list.
	admin[0] = "mutated"
	if AllCapabilities[0] == "mutated" {
		t.Fatal("RoleCapabilities returned an aliased slice")
	}
}

func TestNormalizeCapabilities(t *testing.T) {
	known, unknown := NormalizeCapabilities([]string{CapUI, CapObserve, "sudo", "sudo"})
	if !reflect.DeepEqual(known, []string{CapObserve, CapUI}) {
		t.Fatalf("known = %v", known)
	}
	if !reflect.DeepEqual(unknown, []string{"sudo"}) {
		t.Fatalf("unknown = %v", unknown)
	}
	if known, unknown := NormalizeCapabilities(nil); len(known) != 0 || len(unknown) != 0 {
		t.Fatalf("empty input produced %v / %v", known, unknown)
	}
}

func TestCommandCapability(t *testing.T) {
	cases := map[string]string{
		"get_state":             CapObserve,
		"get_messages":          CapObserve,
		"export_html":           CapObserve,
		"gw_list_sessions":      CapObserve,
		"prompt":                CapPrompt,
		"follow_up":             CapPrompt,
		"new_session":           CapPrompt,
		"fork":                  CapPrompt,
		"clone":                 CapPrompt,
		"bash":                  CapPrompt,
		"switch_session":        CapPrompt,
		"steer":                 CapInterject,
		"abort":                 CapInterject,
		"abort_bash":            CapInterject,
		"abort_retry":           CapInterject,
		"clear_queue":           CapInterject,
		"extension_ui_response": CapUI,
		"set_model":             CapControl,
		"cycle_thinking_level":  CapControl,
		"compact":               CapControl,
		"gw_reload_session":     CapControl,
		"gw_new_session":        CapAdmin,
		"gw_stop_session":       CapAdmin,
		"gw_delete_session":     CapAdmin,
		"gw_ping":               "",
		"gw_bye":                "",
	}
	for command, want := range cases {
		if got := CommandCapability(command); got != want {
			t.Errorf("CommandCapability(%q) = %q, want %q", command, got, want)
		}
	}
	// pi's `extension_ui_request` methods are notifications to the client, not
	// commands, so they must not be in the table (docs/design.md §9).
	for _, notACommand := range []string{"notify", "setStatus", "setWidget", "setTitle", "set_editor_text"} {
		if got := CommandCapability(notACommand); got != "" {
			t.Errorf("CommandCapability(%q) = %q; it is a pi UI notification, not a command", notACommand, got)
		}
	}
}

// piCommands is every command in pi 0.85.1's RpcCommand union
// (dist/modes/rpc/rpc-types.d.ts), plus extension_ui_response, which pi handles
// out of band. Each one is a command a client may send, so each must be gated
// by a capability; a command added to pi that the gateway forwards must be
// added here and to the table deliberately. The list is the guard against the
// gateway advertising a command pi cannot run (and vice versa).
var piCommands = []string{
	"prompt", "steer", "follow_up", "abort", "clear_queue",
	"new_session", "get_state", "set_model", "cycle_model",
	"get_available_models", "set_thinking_level", "cycle_thinking_level",
	"get_available_thinking_levels", "set_steering_mode", "set_follow_up_mode",
	"compact", "set_auto_compaction", "set_auto_retry", "abort_retry",
	"bash", "abort_bash", "get_session_stats", "export_html",
	"switch_session", "fork", "clone", "get_fork_messages", "get_entries",
	"get_tree", "get_last_assistant_text", "set_session_name", "get_messages",
	"get_commands", "extension_ui_response",
}

func TestEveryPiCommandIsGated(t *testing.T) {
	for _, command := range piCommands {
		if got := CommandCapability(command); got == "" {
			t.Errorf("CommandCapability(%q) = \"\"; every pi command needs a capability", command)
		}
	}
}

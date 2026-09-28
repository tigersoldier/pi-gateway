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
	if !ok || !reflect.DeepEqual(operator, []string{CapObserve, CapInterject, CapPrompt, CapContext, CapUI}) {
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
		"inject":                CapContext,
		"steer":                 CapInterject,
		"abort":                 CapInterject,
		"abort_bash":            CapInterject,
		"abort_retry":           CapInterject,
		"clear_queue":           CapInterject,
		"extension_ui_response": CapUI,
		"notify":                CapUI,
		"set_model":             CapControl,
		"cycle_thinking_level":  CapControl,
		"set_editor_text":       CapControl,
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
}

package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tigersoldier/pi-gateway/gwclient"
)

// event builds a frame the way the daemon would send it.
func event(t *testing.T, typ string, fields map[string]any) gwclient.Event {
	t.Helper()
	obj := map[string]any{"type": typ}
	for key, value := range fields {
		obj[key] = value
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return gwclient.Event{Type: typ, Raw: raw}
}

// delta builds one message_update carrying an assistant delta event.
func delta(t *testing.T, typ, text string) gwclient.Event {
	t.Helper()
	return event(t, "message_update", map[string]any{
		"assistantMessageEvent": map[string]any{"type": typ, "delta": text},
	})
}

func newTestRenderer() (*renderer, *bytes.Buffer, *bytes.Buffer) {
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	return &renderer{out: out, errOut: errOut, tools: true}, out, errOut
}

func TestRenderStreamsTextDeltas(t *testing.T) {
	r, out, errOut := newTestRenderer()
	r.Handle(delta(t, "text_start", ""))
	r.Handle(delta(t, "text_delta", "Hel"))
	r.Handle(delta(t, "text_delta", "lo"))
	r.Handle(delta(t, "text_end", ""))
	if got := out.String(); got != "Hello\n" {
		t.Fatalf("stdout = %q, want %q", got, "Hello\n")
	}
	if errOut.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", errOut.String())
	}
}

func TestRenderClosesTextWithoutEndEvent(t *testing.T) {
	r, out, _ := newTestRenderer()
	r.Handle(delta(t, "text_delta", "no end event"))
	r.Handle(event(t, "message_end", map[string]any{"message": map[string]any{"stopReason": "stop"}}))
	if got := out.String(); got != "no end event\n" {
		t.Fatalf("stdout = %q", got)
	}
}

func TestRenderThinkingOnlyWhenEnabled(t *testing.T) {
	r, out, errOut := newTestRenderer()
	r.Handle(delta(t, "thinking_start", ""))
	r.Handle(delta(t, "thinking_delta", "secret reasoning"))
	r.Handle(delta(t, "thinking_end", ""))
	if strings.Contains(out.String()+errOut.String(), "secret") {
		t.Fatalf("thinking rendered without --thinking: %q %q", out.String(), errOut.String())
	}

	errOut.Reset()
	r.thinking = true
	r.Handle(delta(t, "thinking_start", ""))
	r.Handle(delta(t, "thinking_delta", "secret reasoning"))
	r.Handle(delta(t, "thinking_end", ""))
	if !strings.Contains(errOut.String(), "secret reasoning") {
		t.Fatalf("thinking missing from %q", errOut.String())
	}
	if out.Len() != 0 {
		t.Fatalf("thinking leaked to stdout: %q", out.String())
	}
}

func TestRenderToolCallAndResult(t *testing.T) {
	r, _, errOut := newTestRenderer()
	r.Handle(event(t, "tool_execution_start", map[string]any{
		"toolName": "bash",
		"args":     map[string]any{"command": "ls -la"},
	}))
	r.Handle(event(t, "tool_execution_end", map[string]any{
		"toolName": "bash",
		"isError":  false,
		"result": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "total 0\n"}},
		},
	}))
	got := errOut.String()
	for _, want := range []string{"bash", `{"command":"ls -la"}`, "total 0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("tool output %q missing %q", got, want)
		}
	}
}

func TestRenderToolErrorAndSuppression(t *testing.T) {
	r, _, errOut := newTestRenderer()
	r.Handle(event(t, "tool_execution_end", map[string]any{
		"toolName": "bash",
		"isError":  true,
		"result": map[string]any{
			"content": []any{map[string]any{"type": "text", "text": "boom"}},
		},
	}))
	if !strings.Contains(errOut.String(), "[tool bash failed] boom") {
		t.Fatalf("tool error = %q", errOut.String())
	}

	errOut.Reset()
	r.tools = false
	r.Handle(event(t, "tool_execution_start", map[string]any{"toolName": "bash"}))
	r.Handle(event(t, "tool_execution_end", map[string]any{"toolName": "bash"}))
	if errOut.Len() != 0 {
		t.Fatalf("--no-tools still rendered %q", errOut.String())
	}
}

func TestRenderBusyFollowsGwTurn(t *testing.T) {
	r, _, _ := newTestRenderer()
	if r.Busy() {
		t.Fatal("renderer starts busy")
	}
	r.Handle(event(t, "gw_turn", map[string]any{"state": "running"}))
	if !r.Busy() {
		t.Fatal("gw_turn running did not mark the renderer busy")
	}
	r.Handle(event(t, "gw_turn", map[string]any{"state": "settled"}))
	if r.Busy() {
		t.Fatal("gw_turn settled left the renderer busy")
	}
}

func TestRenderPendingTracksWork(t *testing.T) {
	r, _, _ := newTestRenderer()
	if r.Pending() {
		t.Fatal("idle renderer reports pending work")
	}
	r.MarkPending()
	if !r.Pending() {
		t.Fatal("submitted work is not pending")
	}
	r.Handle(event(t, "gw_turn", map[string]any{"state": "running"}))
	if !r.Pending() {
		t.Fatal("running turn is not pending")
	}
	r.Handle(event(t, "gw_turn", map[string]any{"state": "settled"}))
	if r.Pending() {
		t.Fatal("settled turn still reports pending")
	}

	r.Handle(event(t, "gw_queue", map[string]any{"pending": []any{map[string]any{"id": "q1"}}}))
	if !r.Pending() {
		t.Fatal("daemon-queued prompt is not pending")
	}
	r.Handle(event(t, "gw_queue", map[string]any{"pending": []any{}}))
	if r.Pending() {
		t.Fatal("drained queue still reports pending")
	}

	r.MarkPending()
	r.Handle(event(t, "gw_session_state", map[string]any{"state": "crashed", "reason": "boom"}))
	if r.Pending() {
		t.Fatal("crashed session still reports pending")
	}
}

func TestRenderAssistantErrorAndUsage(t *testing.T) {
	r, _, errOut := newTestRenderer()
	r.usage = true
	r.Handle(event(t, "message_update", map[string]any{
		"usage":                 map[string]any{"input": 10, "output": 5, "totalTokens": 15},
		"assistantMessageEvent": map[string]any{"type": "text_delta", "delta": "hi"},
	}))
	r.Handle(event(t, "message_end", map[string]any{"message": map[string]any{
		"stopReason":   "error",
		"errorMessage": "model exploded",
	}}))
	got := errOut.String()
	for _, want := range []string{"[assistant error: model exploded]", "[usage] input=10 output=5 total=15"} {
		if !strings.Contains(got, want) {
			t.Fatalf("stderr %q missing %q", got, want)
		}
	}
}

func TestRenderGatewayErrors(t *testing.T) {
	r, _, errOut := newTestRenderer()
	r.Handle(event(t, "gw_error", map[string]any{"code": "forbidden", "message": "no prompt capability"}))
	if !strings.Contains(errOut.String(), "forbidden") || !strings.Contains(errOut.String(), "no prompt capability") {
		t.Fatalf("gw_error = %q", errOut.String())
	}

	errOut.Reset()
	r.Handle(event(t, "extension_error", map[string]any{"error": "extension blew up"}))
	if !strings.Contains(errOut.String(), "extension blew up") {
		t.Fatalf("extension_error = %q", errOut.String())
	}
}

func TestRenderIgnoresOtherClientsOutput(t *testing.T) {
	r, out, errOut := newTestRenderer()
	r.verbose = true
	r.Handle(event(t, "bash_execution_update", map[string]any{"gw_owner": "c_9", "delta": "other client\n"}))
	r.Handle(event(t, "message_update", map[string]any{
		"gw_owner":              "c_9",
		"assistantMessageEvent": map[string]any{"type": "text_delta", "delta": "not ours"},
	}))
	if out.Len() != 0 || errOut.Len() != 0 {
		t.Fatalf("another client's output leaked: stdout=%q stderr=%q", out.String(), errOut.String())
	}
}

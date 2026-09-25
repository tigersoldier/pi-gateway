package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

// fakePiScript emulates the subset of `pi --mode rpc` the e2e test needs.
const fakePiScript = `#!/usr/bin/env bash
set -euo pipefail
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"type":"prompt"'*)
      echo '{"type":"agent_start"}'
      echo '{"type":"message_start","message":{"role":"assistant","content":[]}}'
      echo '{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"hello"}}'
      echo '{"type":"message_end","message":{"role":"assistant","content":[{"type":"text","text":"hello"}]}}'
      echo "{\"type\":\"response\",\"id\":\"$id\",\"command\":\"prompt\",\"success\":true}"
      echo '{"type":"agent_settled"}'
      ;;
    *'"type":"get_state"'*)
      echo "{\"type\":\"response\",\"id\":\"$id\",\"command\":\"get_state\",\"success\":true,\"data\":{\"isStreaming\":false,\"sessionId\":\"fake\"}}"
      ;;
    *'"type":"get_entries"'*)
      echo "{\"type\":\"response\",\"id\":\"$id\",\"command\":\"get_entries\",\"success\":true,\"data\":{\"entries\":[],\"leafId\":null}}"
      ;;
    *)
      echo "{\"type\":\"response\",\"id\":\"$id\",\"command\":\"unknown\",\"success\":false,\"error\":\"unknown command\"}"
      ;;
  esac
done
`

type testClient struct {
	t     *testing.T
	conn  net.Conn
	codec *protocol.Codec
}

func (c *testClient) send(v any) {
	c.t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		c.t.Fatalf("marshal: %v", err)
	}
	if err := c.codec.WriteRaw(raw); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *testClient) read() json.RawMessage {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	raw, err := c.codec.Read()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return raw
}

// tryRead reads one record, returning ok=false on timeout.
func (c *testClient) tryRead(timeout time.Duration) (json.RawMessage, bool) {
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	raw, err := c.codec.Read()
	if err != nil {
		return nil, false
	}
	return raw, true
}

// readUntil collects records until pred returns true (inclusive).
func (c *testClient) readUntil(pred func(typ string, raw json.RawMessage) bool) []json.RawMessage {
	c.t.Helper()
	var out []json.RawMessage
	for {
		raw := c.read()
		out = append(out, raw)
		if pred(protocol.Field(raw, "type"), raw) {
			return out
		}
	}
}

func dialTestClient(t *testing.T, addr, name string, caps []string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := &testClient{t: t, conn: conn, codec: protocol.NewCodec(conn, conn)}
	c.send(map[string]any{
		"type":      "gw_hello",
		"protocol":  protocol.Version,
		"sessionId": "s1",
		"client":    map[string]any{"name": name, "capabilities": caps},
	})
	// Consume welcome and replay window.
	c.readUntil(func(typ string, _ json.RawMessage) bool { return typ == "gw_replay_done" })
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

func TestE2EMultiClientFanOutAndTargetedResponses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake pi uses a POSIX shell script")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "fakepi.sh")
	if err := os.WriteFile(script, []byte(fakePiScript), 0o755); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(ctx, PiConfig{Bin: script}, func(SessionID) []string { return nil }, 256, ModeExclusive)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()

	addr := ln.Addr().String()
	a := dialTestClient(t, addr, "A", []string{"observe", "prompt", "control"})
	b := dialTestClient(t, addr, "B", []string{"observe", "prompt"})

	a.send(map[string]any{"type": "prompt", "id": "req-1", "message": "hi"})

	// A: collect until the turn settles and A receives its targeted response.
	var aRecs []json.RawMessage
	seenResp, seenSettled := false, false
	for !(seenResp && seenSettled) {
		raw := a.read()
		aRecs = append(aRecs, raw)
		switch protocol.Field(raw, "type") {
		case "response":
			if protocol.Field(raw, "command") == "prompt" {
				if protocol.Field(raw, "id") != "req-1" {
					t.Fatalf("id not restored: %s", raw)
				}
				seenResp = true
			}
		case "agent_settled":
			seenSettled = true
		}
	}
	assertHasTurnEvents(t, "A", aRecs)

	// B: collect the same turn, then confirm it never receives A's response.
	var bRecs []json.RawMessage
	for {
		raw := b.read()
		bRecs = append(bRecs, raw)
		if protocol.Field(raw, "type") == "agent_settled" {
			break
		}
	}
	assertHasTurnEvents(t, "B", bRecs)
	for {
		raw, ok := b.tryRead(300 * time.Millisecond)
		if !ok {
			break
		}
		if protocol.Field(raw, "type") == "response" {
			t.Fatalf("client B received another client's response: %s", raw)
		}
	}
}

func assertHasTurnEvents(t *testing.T, who string, recs []json.RawMessage) {
	t.Helper()
	seen := map[string]bool{}
	for _, r := range recs {
		seen[protocol.Field(r, "type")] = true
	}
	for _, want := range []string{"agent_start", "message_start", "message_update", "message_end", "agent_settled"} {
		if !seen[want] {
			t.Fatalf("client %s missing event %q; saw %v", who, want, seen)
		}
	}
}

func TestE2EExclusiveModeRejectsSecondPrompt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake pi uses a POSIX shell script")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "fakepi.sh")
	if err := os.WriteFile(script, []byte(fakePiScript), 0o755); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(ctx, PiConfig{Bin: script}, func(SessionID) []string { return nil }, 256, ModeExclusive)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()

	addr := ln.Addr().String()
	a := dialTestClient(t, addr, "A", []string{"observe", "prompt"})
	b := dialTestClient(t, addr, "B", []string{"observe", "prompt"})

	// A acquires the floor but does not settle: the fake pi only emits
	// agent_settled immediately, so instead test with explicit take_turn.
	a.send(map[string]any{"type": "gw_take_turn", "id": "t1", "ttlMs": 60000})
	a.readUntil(func(typ string, raw json.RawMessage) bool {
		return typ == "response" && protocol.Field(raw, "command") == "gw_take_turn"
	})

	b.send(map[string]any{"type": "prompt", "id": "req-b", "message": "hi"})
	recs := b.readUntil(func(typ string, raw json.RawMessage) bool {
		return typ == "response" && protocol.Field(raw, "command") == "prompt"
	})
	last := recs[len(recs)-1]
	if protocol.Field(last, "success") != "false" && !bytesContains(last, []byte(`"success":false`)) {
		t.Fatalf("expected rejection, got %s", last)
	}
	if !bytesContains(last, []byte(`"code":"turn_held"`)) {
		t.Fatalf("expected turn_held, got %s", last)
	}
}

func bytesContains(b []byte, sub []byte) bool {
	for i := 0; i+len(sub) <= len(b); i++ {
		if string(b[i:i+len(sub)]) == string(sub) {
			return true
		}
	}
	return false
}

// TestStdioBridgeClient exercises the separate `connect` bridge: a raw pi peer
// (simulated with a net.Pipe instead of stdio) talks to the TCP server through
// the bridge. It must see only pristine pi messages, with ids restored, and no
// gw_* traffic.
func TestStdioBridgeClient(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fake pi uses a POSIX shell script")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "fakepi.sh")
	if err := os.WriteFile(script, []byte(fakePiScript), 0o755); err != nil {
		t.Fatalf("write fake pi: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(ctx, PiConfig{Bin: script}, func(SessionID) []string { return nil }, 256, ModeExclusive)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()

	bridge, err := Connect(ctx, ClientOptions{
		ServerAddr: ln.Addr().String(),
		SessionID:  "s1",
		Name:       "stdio-bridge",
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	rawSide, pipeEnd := net.Pipe()
	defer rawSide.Close()
	go func() { _ = bridge.Bridge(ctx, pipeEnd) }()

	codec := protocol.NewCodec(rawSide, rawSide)
	if err := codec.WriteRaw([]byte(`{"type":"prompt","id":"req-1","message":"hi"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}

	seen := map[string]bool{}
	for !(seen["response"] && seen["agent_settled"]) {
		_ = rawSide.SetReadDeadline(time.Now().Add(3 * time.Second))
		raw, err := codec.Read()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		// The bridge must strip every gateway artifact.
		if bytes.Contains(raw, []byte(`"gw_`)) {
			t.Fatalf("bridge forwarded a gateway-stamped frame: %s", raw)
		}
		typ := protocol.Field(raw, "type")
		if protocol.IsGatewayType(typ) {
			t.Fatalf("bridge forwarded gateway-only message: %s", raw)
		}
		if typ == "response" && protocol.Field(raw, "command") == "prompt" {
			if protocol.Field(raw, "id") != "req-1" {
				t.Fatalf("id not restored for raw client: %s", raw)
			}
			seen["response"] = true
		}
		if typ == "agent_settled" {
			seen["agent_settled"] = true
		}
	}
}

// TestSingleSessionGuard proves the server refuses a foreign session id before
// it would spawn a second pi process against the same session file.
func TestSingleSessionGuard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := NewServer(ctx, PiConfig{Bin: "definitely-not-a-real-pi"}, nil, 16, ModeExclusive)
	srv.SetSingleSession("demo")

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	codec := protocol.NewCodec(conn, conn)
	if err := codec.WriteRaw([]byte(`{"type":"gw_hello","protocol":1,"sessionId":"other","client":{"capabilities":["observe"]}}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	raw, err := codec.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Contains(raw, []byte("unknown session")) {
		t.Fatalf("expected single-session rejection, got %s", raw)
	}
}

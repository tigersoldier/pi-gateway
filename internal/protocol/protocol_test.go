package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCodecFraming(t *testing.T) {
	in := strings.NewReader("{\"a\":1}\r\n{\"b\":\"x\u2028y\"}\n{\"c\":3}")
	c := NewCodec(in, nil)

	first, err := c.Read()
	if err != nil {
		t.Fatalf("read 1: %v", err)
	}
	if string(first) != `{"a":1}` {
		t.Fatalf("trailing CR not stripped: %q", first)
	}

	second, err := c.Read()
	if err != nil {
		t.Fatalf("read 2: %v", err)
	}
	if !strings.Contains(string(second), "\u2028") {
		t.Fatalf("U+2028 must not split a frame: %q", second)
	}

	third, err := c.Read()
	if err != nil {
		t.Fatalf("read 3 (unterminated): %v", err)
	}
	if string(third) != `{"c":3}` {
		t.Fatalf("unterminated final frame: %q", third)
	}

	if _, err := c.Read(); err == nil {
		t.Fatalf("expected EOF")
	}
}

func TestCodecRejectsOversizedFrame(t *testing.T) {
	c := NewCodec(strings.NewReader(strings.Repeat("x", MaxFrameBytes+1)+"\n"), nil)
	if _, err := c.Read(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("got %v, want ErrFrameTooLarge", err)
	}
}

func TestCodecWriteRawAppendsLF(t *testing.T) {
	var buf bytes.Buffer
	c := NewCodec(nil, &buf)
	if err := c.WriteRaw([]byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteJSON(map[string]any{"b": 2}); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "{\"a\":1}\n{\"b\":2}\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestStampAndStripGatewayFields(t *testing.T) {
	raw := []byte(`{"type":"message_update","delta":"x"}`)
	stamped, err := Stamp(raw, map[string]any{"gw_seq": 7, "gw_session": "p"})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(stamped, &m); err != nil {
		t.Fatal(err)
	}
	if m["gw_seq"].(float64) != 7 || m["type"] != "message_update" {
		t.Fatalf("bad stamp: %s", stamped)
	}
	pristine := StripGatewayFields(stamped)
	if bytes.Contains(pristine, []byte("gw_")) {
		t.Fatalf("gateway fields survived: %s", pristine)
	}
	if !bytes.Contains(pristine, []byte(`"type":"message_update"`)) {
		t.Fatalf("pi fields lost: %s", pristine)
	}
}

func TestRestoreID(t *testing.T) {
	raw := []byte(`{"type":"response","id":"c_3:req-1","success":true}`)
	got := RestoreID(raw, "c_3")
	if !bytes.Contains(got, []byte(`"id":"req-1"`)) {
		t.Fatalf("prefix not stripped: %s", got)
	}
	// Not ours: unchanged.
	other := RestoreID(raw, "c_9")
	if !bytes.Equal(other, raw) {
		t.Fatalf("foreign id changed: %s", other)
	}
	// Namespaced with an empty local id: the field is dropped.
	bare := []byte(`{"type":"response","id":"c_3"}`)
	if out := RestoreID(bare, "c_3"); bytes.Contains(out, []byte(`"id"`)) {
		t.Fatalf("empty local id should remove the field: %s", out)
	}
}

func TestNamespaceRoundTrip(t *testing.T) {
	if got := NamespaceID("c_1", "req-1"); got != "c_1:req-1" {
		t.Fatalf("NamespaceID = %q", got)
	}
	client, local := SplitNamespaceID("c_1:req-1")
	if client != "c_1" || local != "req-1" {
		t.Fatalf("SplitNamespaceID = %q, %q", client, local)
	}
	if client, local := SplitNamespaceID("plain"); client != "" || local != "plain" {
		t.Fatalf("SplitNamespaceID(plain) = %q, %q", client, local)
	}
}

func TestRewriteCommand(t *testing.T) {
	raw := []byte(`{"type":"follow_up","id":"local","message":"hi"}`)
	out, err := RewriteCommand(raw, "c_1:local", "prompt")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["id"] != "c_1:local" || m["type"] != "prompt" {
		t.Fatalf("bad rewrite: %s", out)
	}
}

func TestIsInternalIDAndGatewayType(t *testing.T) {
	if !IsInternalID(InternalIDPrefix + "1") {
		t.Fatal("internal id not detected")
	}
	if IsInternalID("c_1:req") {
		t.Fatal("client id misdetected as internal")
	}
	if !IsGatewayType("gw_turn") || IsGatewayType("prompt") {
		t.Fatal("gateway type detection is wrong")
	}
}

func TestResponseShape(t *testing.T) {
	ok := Response("req-1", "prompt", true, "", "", []byte(`{"queued":true}`))
	var m map[string]any
	if err := json.Unmarshal(ok, &m); err != nil {
		t.Fatal(err)
	}
	if m["success"] != true || m["command"] != "prompt" || m["id"] != "req-1" {
		t.Fatalf("bad success response: %s", ok)
	}
	if _, hasErr := m["error"]; hasErr {
		t.Fatalf("success response must not carry error: %s", ok)
	}

	bad := Response("req-2", "switch_session", false, CodeUnknownSession, "nope", nil)
	if err := json.Unmarshal(bad, &m); err != nil {
		t.Fatal(err)
	}
	if m["success"] != false || m["code"] != CodeUnknownSession || m["error"] != "nope" {
		t.Fatalf("bad error response: %s", bad)
	}
}

func TestParseHello(t *testing.T) {
	raw := []byte(`{"type":"gw_hello","protocol":1,"token":"t","client":{"name":"n","kind":"pilish"},"piArgs":["--approve"],"liveOnly":true}`)
	h, err := ParseHello(raw)
	if err != nil {
		t.Fatal(err)
	}
	if h.Client.Kind != "pilish" || !h.LiveOnly || len(h.PiArgs) != 1 {
		t.Fatalf("bad hello: %+v", h)
	}
	if _, err := ParseHello([]byte(`{"type":"gw_hello","protocol":99}`)); err == nil {
		t.Fatal("unsupported protocol must fail")
	}
	if _, err := ParseHello([]byte(`{"type":"prompt"}`)); err == nil {
		t.Fatal("non-hello must fail")
	}
}

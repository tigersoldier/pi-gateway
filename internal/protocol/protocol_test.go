package protocol

import (
	"bytes"
	"strings"
	"testing"
)

// pi's RPC framing is LF-only: U+2028 and U+2029 are valid inside JSON strings
// and must not split records. This is the single most common client bug.
func TestCodecLFOnlyFraming(t *testing.T) {
	input := "{\"type\":\"prompt\",\"message\":\"a\u2028b\u2029c\"}\n{\"type\":\"abort\"}\n"
	c := NewCodec(strings.NewReader(input), nil)

	first, err := c.Read()
	if err != nil {
		t.Fatalf("first read: %v", err)
	}
	if !bytes.Contains(first, []byte("\u2028")) || !bytes.Contains(first, []byte("\u2029")) {
		t.Fatalf("record was split on Unicode separators: %q", first)
	}
	if got := Field(first, "type"); got != "prompt" {
		t.Fatalf("type = %q, want prompt", got)
	}

	second, err := c.Read()
	if err != nil {
		t.Fatalf("second read: %v", err)
	}
	if got := Field(second, "type"); got != "abort" {
		t.Fatalf("type = %q, want abort", got)
	}

	if _, err := c.Read(); err == nil {
		t.Fatalf("expected EOF after last record")
	}
}

func TestCodecStripsTrailingCR(t *testing.T) {
	c := NewCodec(strings.NewReader("{\"type\":\"abort\"}\r\n"), nil)
	raw, err := c.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if bytes.HasSuffix(raw, []byte("\r")) {
		t.Fatalf("trailing CR not stripped: %q", raw)
	}
}

func TestCodecHandlesFramesLargerThanBuffer(t *testing.T) {
	big := strings.Repeat("x", 200<<10)
	input := "{\"type\":\"prompt\",\"message\":\"" + big + "\"}\n"
	c := NewCodec(strings.NewReader(input), nil)
	raw, err := c.Read()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Contains(raw, []byte(big)) {
		t.Fatalf("large frame was truncated")
	}
}

func TestNamespaceRoundTrip(t *testing.T) {
	ns := NamespaceID("c_1", "req-1")
	client, local := SplitNamespaceID(ns)
	if client != "c_1" || local != "req-1" {
		t.Fatalf("round trip = (%q,%q)", client, local)
	}
}

func TestRewriteID(t *testing.T) {
	raw, err := RewriteID([]byte(`{"type":"abort","id":"old"}`), "c_1:new")
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if got := Field(raw, "id"); got != "c_1:new" {
		t.Fatalf("id = %q", got)
	}
	cleared, err := RewriteID(raw, "")
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	if got := Field(cleared, "id"); got != "" {
		t.Fatalf("id not cleared: %q", got)
	}
}

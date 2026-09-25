package gwlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestNewValidation(t *testing.T) {
	var buf bytes.Buffer
	if _, err := New(&buf, "loud", "text"); err == nil {
		t.Fatal("expected an error for an unknown level")
	}
	if _, err := New(&buf, "debug", "xml"); err == nil {
		t.Fatal("expected an error for an unknown format")
	}
	for _, level := range []string{"", "debug", "info", "warn", "warning", "error", "DEBUG"} {
		if _, err := New(&buf, level, "text"); err != nil {
			t.Fatalf("level %q: %v", level, err)
		}
	}
}

func TestTextFormatAndLevel(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "info", "text")
	if err != nil {
		t.Fatal(err)
	}
	log.Debug("hidden detail")
	log.Info("session started", "session", "/tmp/a.jsonl", "pid", 42)
	log.Warn("careful")
	out := buf.String()
	if strings.Contains(out, "hidden detail") {
		t.Fatalf("debug record was not filtered: %q", out)
	}
	for _, want := range []string{"msg=\"session started\"", "session=/tmp/a.jsonl", "pid=42", "level=WARN"} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output %q missing %q", out, want)
		}
	}
}

func TestJSONFormatAndWith(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "debug", "json")
	if err != nil {
		t.Fatal(err)
	}
	log.With("session", "/tmp/a.jsonl").Error("boom", "err", "broken")
	var rec map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
		t.Fatalf("json output %q: %v", buf.String(), err)
	}
	if rec["msg"] != "boom" || rec["session"] != "/tmp/a.jsonl" || rec["err"] != "broken" {
		t.Fatalf("unexpected record: %v", rec)
	}
}

func TestNopAndFromLogf(t *testing.T) {
	Nop().Info("discarded")
	var lines []string
	log := FromLogf(func(format string, args ...any) {
		lines = append(lines, fmt.Sprintf(format, args...))
	})
	log.Warn("hello", "k", 1)
	log.With("component", "actor").Info("nested")
	if len(lines) != 2 || !strings.Contains(lines[0], "hello k=1") || !strings.Contains(lines[1], "component=actor") {
		t.Fatalf("FromLogf lines = %q", lines)
	}
}

package daemon

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

func deltaRecord(t *testing.T, seq int, kind, index, delta string) protocol.Record {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"type":   "message_update",
		"gw_seq": seq,
		"usage":  map[string]any{"output": seq},
		"assistantMessageEvent": map[string]any{
			"type": kind, "contentIndex": index, "delta": delta,
		},
	})
	if err != nil {
		t.Fatalf("marshal delta record: %v", err)
	}
	return protocol.Record{Raw: raw, Type: "message_update", Seq: uint64(seq)}
}

func deltaOf(t *testing.T, rec protocol.Record) string {
	t.Helper()
	var frame struct {
		Event struct {
			Delta string `json:"delta"`
		} `json:"assistantMessageEvent"`
	}
	if err := json.Unmarshal(rec.Raw, &frame); err != nil {
		t.Fatalf("bad record: %v", err)
	}
	return frame.Event.Delta
}

func TestCoalescerMergesConsecutiveDeltas(t *testing.T) {
	co := &coalescer{}
	if out := co.offer(deltaRecord(t, 1, "text_delta", "0", "Hello")); len(out) != 0 {
		t.Fatalf("first delta must be held, got %v", out)
	}
	if out := co.offer(deltaRecord(t, 2, "text_delta", "0", " world")); len(out) != 0 {
		t.Fatalf("second delta must be held, got %v", out)
	}
	if !co.pending() {
		t.Fatal("coalescer should have a pending run")
	}
	out := co.flush()
	if len(out) != 1 {
		t.Fatalf("flush returned %d records, want 1", len(out))
	}
	if out[0].Seq != 2 {
		t.Fatalf("merged record kept seq %d, want the newest (2)", out[0].Seq)
	}
	if got := deltaOf(t, out[0]); got != "Hello world" {
		t.Fatalf("merged delta = %q", got)
	}
	if co.pending() {
		t.Fatal("flush must clear the run")
	}
	// The gateway metadata of the newest record survives the merge.
	if !strings.Contains(string(out[0].Raw), `"gw_seq":2`) {
		t.Fatalf("merged record lost gw_seq: %s", out[0].Raw)
	}
}

func TestCoalescerFlushesOnNonDeltaAndOtherBlocks(t *testing.T) {
	co := &coalescer{}
	co.offer(deltaRecord(t, 1, "text_delta", "0", "a"))
	// A different content index cannot be merged into the same run, so the old
	// run flushes and a new one starts.
	out := co.offer(deltaRecord(t, 2, "text_delta", "1", "b"))
	if len(out) != 1 {
		t.Fatalf("content change: got %d records, want the flushed run", len(out))
	}
	if got := deltaOf(t, out[0]); got != "a" {
		t.Fatalf("flushed delta = %q", got)
	}
	if out := co.flush(); len(out) != 1 || deltaOf(t, out[0]) != "b" {
		t.Fatalf("second run = %v", out)
	}

	// A non-delta record flushes a pending run and passes through unchanged.
	co.offer(deltaRecord(t, 3, "text_delta", "0", "x"))
	out = co.offer(protocol.Record{Raw: []byte(`{"type":"tool_execution_start"}`), Type: "tool_execution_start"})
	if len(out) != 2 || out[1].Type != "tool_execution_start" {
		t.Fatalf("non-delta handling = %v", out)
	}
	if got := deltaOf(t, out[0]); got != "x" {
		t.Fatalf("flushed delta = %q", got)
	}

	// Thinking and text deltas are separate runs.
	co.offer(deltaRecord(t, 4, "thinking_delta", "0", "think"))
	out = co.offer(deltaRecord(t, 5, "text_delta", "0", "text"))
	if len(out) != 1 || deltaOf(t, out[0]) != "think" {
		t.Fatalf("kind change = %v", out)
	}
	if out := co.flush(); len(out) != 1 || deltaOf(t, out[0]) != "text" {
		t.Fatalf("text run = %v", out)
	}
}

func TestCoalescerFlushesOnSizeBudget(t *testing.T) {
	co := &coalescer{}
	chunk := strings.Repeat("x", 1024)
	var flushed []protocol.Record
	for i := 0; i < 16; i++ {
		flushed = append(flushed, co.offer(deltaRecord(t, i+1, "text_delta", "0", chunk))...)
	}
	if len(flushed) == 0 {
		t.Fatal("the size budget must force a flush")
	}
	if len(co.merged.String()) >= deltaFlushBytes {
		t.Fatalf("pending run still holds %d bytes", len(co.merged.String()))
	}
	total := strings.Builder{}
	for _, rec := range flushed {
		total.WriteString(deltaOf(t, rec))
	}
	total.WriteString(co.merged.String())
	if total.Len() != 16*len(chunk) {
		t.Fatalf("merged %d bytes, want %d", total.Len(), 16*len(chunk))
	}
}

func TestCoalescerPassesThroughNonStreamingRecords(t *testing.T) {
	co := &coalescer{}
	rec := protocol.Record{Raw: []byte(`{"type":"agent_settled"}`), Type: "agent_settled"}
	out := co.offer(rec)
	if len(out) != 1 || out[0].Type != "agent_settled" {
		t.Fatalf("passthrough = %v", out)
	}
	// A malformed frame is never treated as a delta.
	bad := protocol.Record{Raw: []byte(`{"type":"message_update"`), Type: "message_update"}
	if out := co.offer(bad); len(out) != 1 {
		t.Fatalf("malformed frame must pass through: %v", out)
	}
}

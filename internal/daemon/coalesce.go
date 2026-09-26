package daemon

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/tigersoldier/pi-gateway/protocol"
)

// Delta coalescing keeps a slow client from being flooded by per-token
// updates: consecutive streaming deltas for the same content block are merged
// into one frame per flush interval or size budget (docs/design.md §7).
const (
	defaultDeltaFlush = 50 * time.Millisecond
	deltaFlushBytes   = 8 << 10
)

// coalescer merges runs of streaming deltas. It is not safe for concurrent
// use; the connection pump owns it.
type coalescer struct {
	last   *protocol.Record // newest record of the current run
	key    string           // delta kind + content index
	merged strings.Builder
	size   int
	count  int
}

// offer accepts a record and returns the frames that are ready to be sent.
// A returned slice has at most one entry: either a flushed run or the record
// itself when it is not a streaming delta. A delta that starts a new run is
// held until it is flushed.
func (co *coalescer) offer(rec protocol.Record) []protocol.Record {
	key, delta, ok := deltaKind(rec.Raw)
	if !ok {
		return append(co.flush(), rec)
	}
	switch {
	case co.last == nil:
		co.start(rec, key, delta)
		return nil
	case co.key == key:
		co.merged.WriteString(delta)
		co.size += len(delta)
		co.count++
		co.last = &rec // keep the newest metadata (usage, gw_seq)
		if co.size >= deltaFlushBytes {
			return co.flush()
		}
		return nil
	}
	out := co.flush()
	co.start(rec, key, delta)
	return out
}

// pending reports whether a merged run is waiting to be flushed.
func (co *coalescer) pending() bool { return co.last != nil }

// flush returns the current run as a single record, if any.
func (co *coalescer) flush() []protocol.Record {
	if co.last == nil {
		return nil
	}
	rec := *co.last
	text := co.merged.String()
	count := co.count
	co.last, co.count, co.size = nil, 0, 0
	co.merged.Reset()
	if count <= 1 {
		return []protocol.Record{rec}
	}
	patched, ok := patchDelta(rec.Raw, text)
	if !ok {
		return []protocol.Record{rec}
	}
	rec.Raw = patched
	return []protocol.Record{rec}
}

func (co *coalescer) start(rec protocol.Record, key, delta string) {
	co.last = &rec
	co.key = key
	co.merged.Reset()
	co.merged.WriteString(delta)
	co.size = len(delta)
	co.count = 1
}

// deltaKind reports whether a record is a mergeable streaming delta and
// returns its merge key and text.
func deltaKind(raw []byte) (key, delta string, ok bool) {
	var frame struct {
		Type  string `json:"type"`
		Event struct {
			Type         string          `json:"type"`
			ContentIndex json.RawMessage `json:"contentIndex"`
			Delta        string          `json:"delta"`
		} `json:"assistantMessageEvent"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		return "", "", false
	}
	if frame.Type != "message_update" || frame.Event.Delta == "" {
		return "", "", false
	}
	switch frame.Event.Type {
	case "text_delta", "thinking_delta", "toolcall_delta":
	default:
		return "", "", false
	}
	return frame.Event.Type + ":" + string(frame.Event.ContentIndex), frame.Event.Delta, true
}

// patchDelta replaces the delta of a message_update frame with the merged
// text, keeping the frame's gateway metadata (gw_seq and friends).
func patchDelta(raw []byte, text string) ([]byte, bool) {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, false
	}
	event, ok := obj["assistantMessageEvent"].(map[string]any)
	if !ok {
		return nil, false
	}
	event["delta"] = text
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, false
	}
	return out, true
}

package gateway

import (
	"testing"
	"time"

	"github.com/example/pi-gateway/internal/protocol"
)

func rec(typ string) protocol.Record {
	return protocol.Record{Raw: []byte(`{"type":"` + typ + `"}`), Type: typ}
}

func TestHubAssignsMonotonicSeq(t *testing.T) {
	h := NewHub(16, "s1")
	for i := 1; i <= 10; i++ {
		got := h.Publish(rec("message_update"))
		if got.Seq != uint64(i) {
			t.Fatalf("publish %d: seq = %d", i, got.Seq)
		}
	}
	if h.HeadSeq() != 10 {
		t.Fatalf("head = %d", h.HeadSeq())
	}
	if h.OldestSeq() != 1 {
		t.Fatalf("oldest = %d", h.OldestSeq())
	}
}

func TestHubReplayWindow(t *testing.T) {
	h := NewHub(16, "s1")
	for i := 0; i < 5; i++ {
		h.Publish(rec("message_update"))
	}
	replay, ok := h.Replay(2)
	if !ok {
		t.Fatalf("replay unexpectedly required resync")
	}
	if len(replay) != 3 || replay[0].Seq != 3 || replay[2].Seq != 5 {
		t.Fatalf("unexpected replay window: %+v", replay)
	}
}

func TestHubReplayEvictionForcesResync(t *testing.T) {
	h := NewHub(4, "s1")
	for i := 0; i < 10; i++ {
		h.Publish(rec("message_update"))
	}
	if h.OldestSeq() != 7 {
		t.Fatalf("oldest = %d, want 7", h.OldestSeq())
	}
	if _, ok := h.Replay(2); ok {
		t.Fatalf("expected resync requirement for evicted cursor")
	}
	if replay, ok := h.Replay(7); !ok || len(replay) != 3 {
		t.Fatalf("cursor at oldest should replay: ok=%v len=%d", ok, len(replay))
	}
}

func TestHubSubscriberObservesOrder(t *testing.T) {
	h := NewHub(64, "s1")
	sub := h.Subscribe("c_1", 64, false)
	defer h.Unsubscribe("c_1")

	const n = 50
	for i := 0; i < n; i++ {
		h.Publish(rec("message_update"))
	}
	var last uint64
	for i := 0; i < n; i++ {
		select {
		case got := <-sub.C():
			if got.Seq <= last {
				t.Fatalf("out of order: %d after %d", got.Seq, last)
			}
			last = got.Seq
		case <-time.After(time.Second):
			t.Fatalf("timed out after %d records", i)
		}
	}
}

func TestArbiterExclusive(t *testing.T) {
	a := NewArbiter(ModeExclusive, time.Minute)
	now := time.Now()

	if d := a.AdmitPrompt("c_1", true, now); d.Verdict != Allow {
		t.Fatalf("first prompt: %+v", d)
	}
	if d := a.AdmitPrompt("c_2", false, now); d.Verdict != Reject || d.Code != "turn_held" {
		t.Fatalf("second prompt should be rejected: %+v", d)
	}
	if d := a.AdmitInterjection(); d.Verdict != Allow {
		t.Fatalf("interjection should be allowed: %+v", d)
	}
	a.Settle()
	if d := a.AdmitPrompt("c_2", true, now); d.Verdict != Allow {
		t.Fatalf("prompt after settle: %+v", d)
	}
}

func TestArbiterQueue(t *testing.T) {
	a := NewArbiter(ModeQueue, time.Minute)
	now := time.Now()

	if d := a.AdmitPrompt("c_1", true, now); d.Verdict != Allow {
		t.Fatalf("first: %+v", d)
	}
	if d := a.AdmitPrompt("c_2", false, now); d.Verdict != Queue {
		t.Fatalf("busy prompt should queue: %+v", d)
	}
	if a.QueueDepth() != 1 {
		t.Fatalf("queue depth = %d", a.QueueDepth())
	}
	if next := a.Settle(); next != "c_2" {
		t.Fatalf("next = %q", next)
	}
}

func TestArbiterReadOnly(t *testing.T) {
	a := NewArbiter(ModeReadOnly, time.Minute)
	if d := a.AdmitPrompt("c_1", true, time.Now()); d.Verdict != Reject || d.Code != "read_only" {
		t.Fatalf("read-only prompt: %+v", d)
	}
	if d := a.AdmitInterjection(); d.Verdict != Reject {
		t.Fatalf("read-only interjection: %+v", d)
	}
}

func TestUIBrokerFirstResponseWins(t *testing.T) {
	b := NewUIBroker()
	req := protocol.Record{Type: "extension_ui_request", ID: "u1",
		Raw: []byte(`{"type":"extension_ui_request","id":"u1","method":"confirm","timeout":10000}`)}
	faf, _ := b.OnRequest(req, "c_1")
	if faf {
		t.Fatalf("confirm must not be fire-and-forget")
	}
	resp := protocol.Record{Type: "extension_ui_response", ID: "u1", Raw: []byte(`{"type":"extension_ui_response","id":"u1","confirmed":true}`)}
	if forward, _, _ := b.OnResponse("c_1", resp); !forward {
		t.Fatalf("first response must forward")
	}
	if forward, code, _ := b.OnResponse("c_2", resp); forward || code != "ui_stale" {
		t.Fatalf("second response must be stale, got forward=%v code=%q", forward, code)
	}
}

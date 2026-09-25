package session

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.Cleanup()
	os.Exit(code)
}

func TestHubSequencesFansOutAndReplays(t *testing.T) {
	h := NewHub(3, "s_test")
	sub := h.Subscribe("c_1", 8)
	defer sub.Close()

	for i := 0; i < 3; i++ {
		h.Publish(protocol.Record{Raw: []byte(`{"type":"message_update"}`), Type: "message_update"})
	}
	for i := 1; i <= 3; i++ {
		select {
		case rec := <-sub.C():
			if rec.Seq != uint64(i) {
				t.Fatalf("seq = %d, want %d", rec.Seq, i)
			}
			if !strings.Contains(string(rec.Raw), `"gw_seq"`) {
				t.Fatalf("gw_seq not stamped: %s", rec.Raw)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for record %d", i)
		}
	}

	recs, ok := h.Replay(1)
	if !ok || len(recs) != 2 || recs[0].Seq != 2 {
		t.Fatalf("Replay(1) = %d records, ok=%v", len(recs), ok)
	}
	// Capacity is 3: publishing four more evicts everything up to seq 4.
	for i := 0; i < 4; i++ {
		h.Publish(protocol.Record{Raw: []byte(`{"type":"message_update"}`), Type: "message_update"})
	}
	if _, ok := h.Replay(0); ok {
		t.Fatal("evicted cursor must not be replayable")
	}
	recs, ok = h.Replay(5)
	if !ok || len(recs) != 2 || recs[0].Seq != 6 {
		t.Fatalf("Replay(5) = %d records, ok=%v", len(recs), ok)
	}
}

func TestHubDropsSlowSubscriber(t *testing.T) {
	h := NewHub(8, "s")
	sub := h.Subscribe("slow", 1)
	h.Publish(protocol.Record{Raw: []byte(`{"type":"a"}`), Type: "a"})
	h.Publish(protocol.Record{Raw: []byte(`{"type":"b"}`), Type: "b"})
	if !sub.Dead() {
		t.Fatal("overflowing subscriber must be marked dead")
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-sub.C():
			if !ok {
				return // closed as expected
			}
		case <-deadline:
			t.Fatal("subscriber channel was not closed")
		}
	}
}

func TestQueueFIFOAndTags(t *testing.T) {
	var q Queue
	q.Push(QueuedItem{ClientID: "c_1", Kind: "pilish", Name: "laptop", LocalID: "req-1",
		Type: "prompt", Message: "  hello \n world  "})
	q.Push(QueuedItem{ClientID: "c_2", Kind: "bot", Name: "slack", LocalID: "req-2",
		Type: "follow_up", Message: strings.Repeat("x", 200)})

	pending := q.Pending()
	if len(pending) != 2 {
		t.Fatalf("pending = %d", len(pending))
	}
	if pending[0].Mode != "followUp" || pending[0].Author.ClientID != "c_1" {
		t.Fatalf("bad first entry: %+v", pending[0])
	}
	if pending[0].Preview != "hello world" {
		t.Fatalf("preview = %q", pending[0].Preview)
	}
	if !strings.HasSuffix(pending[1].Preview, "…") || len(pending[1].Preview) >= 200 {
		t.Fatalf("long preview not truncated: %q", pending[1].Preview)
	}

	first, ok := q.Pop()
	if !ok || first.ClientID != "c_1" {
		t.Fatalf("FIFO violated: %+v", first)
	}
	if cleared := q.Clear(); len(cleared) != 1 || cleared[0].ClientID != "c_2" {
		t.Fatalf("Clear = %+v", cleared)
	}
	if q.Len() != 0 {
		t.Fatalf("queue not empty after clear")
	}
}

type fakeClient struct {
	id   string
	kind string
	name string
	caps map[string]bool
}

func (c *fakeClient) ID() string                 { return c.id }
func (c *fakeClient) Kind() string               { return c.kind }
func (c *fakeClient) Name() string               { return c.name }
func (c *fakeClient) Has(capability string) bool { return c.caps[capability] }
func (c *fakeClient) Info() protocol.ClientInfo {
	return protocol.ClientInfo{Name: c.name, Kind: c.kind}
}
func (c *fakeClient) Ref() protocol.ClientRef {
	return protocol.ClientRef{ClientID: c.id, Kind: c.kind, Name: c.name}
}

func startTestActor(t *testing.T, params Params) *Actor {
	t.Helper()
	bin, err := testutil.FakePi()
	if err != nil {
		t.Skipf("cannot build fake pi: %v", err)
	}
	params.PiBin = bin
	if params.IdleTimeout == 0 {
		params.IdleTimeout = time.Minute
	}
	if params.ShortGrace == 0 {
		params.ShortGrace = time.Minute
	}
	a := NewActor(params)
	if err := a.Start(); err != nil {
		t.Fatalf("start actor: %v", err)
	}
	t.Cleanup(a.Stop)
	return a
}

func collectUntil(t *testing.T, sub *Subscriber, pred func(protocol.Record) bool, timeout time.Duration) []protocol.Record {
	t.Helper()
	deadline := time.After(timeout)
	var out []protocol.Record
	for {
		select {
		case rec, ok := <-sub.C():
			if !ok {
				t.Fatalf("subscriber closed before predicate matched (%d records)", len(out))
			}
			out = append(out, rec)
			if pred(rec) {
				return out
			}
		case <-deadline:
			t.Fatalf("timed out after %d records:\n%s", len(out), dump(out))
		}
	}
}

func promptCmd(c Client, id, message string) ClientCommand {
	raw := []byte(`{"type":"prompt","id":"` + id + `","message":"` + message + `"}`)
	return ClientCommand{Client: c, LocalID: id, Type: "prompt", Raw: raw}
}

func TestActorPromptThenQueue(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "25")
	a := startTestActor(t, Params{SessionPath: filepath.Join(t.TempDir(), "s.jsonl"), HubCapacity: 256})
	client := &fakeClient{id: "c_1", kind: "test", name: "one",
		caps: map[string]bool{"prompt": true, "observe": true}}
	a.Attach(client)
	sub := a.Subscribe(client.id, 256)

	if err := a.Submit(promptCmd(client, "r1", "first")); err != nil {
		t.Fatalf("submit 1: %v", err)
	}
	if err := a.Submit(promptCmd(client, "r2", "second")); err != nil {
		t.Fatalf("submit 2: %v", err)
	}

	first := collectUntil(t, sub, func(rec protocol.Record) bool {
		return rec.Type == "gw_queue" && strings.Contains(string(rec.Raw), `"second"`)
	}, 5*time.Second)
	if !hasFrame(first, "gw_turn", `"running"`) {
		t.Fatalf("no gw_turn running in %s", dump(first))
	}

	// The queued prompt must be forwarded only after the first turn settles.
	second := collectUntil(t, sub, func(rec protocol.Record) bool {
		return rec.Type == "message_end" && strings.Contains(string(rec.Raw), "echo: second")
	}, 5*time.Second)
	recs := append(append([]protocol.Record{}, first...), second...)
	order := make([]string, 0, 4)
	for _, rec := range recs {
		if rec.Type == "message_end" {
			order = append(order, string(rec.Raw))
		}
	}
	if len(order) != 2 || !strings.Contains(order[0], "echo: first") || !strings.Contains(order[1], "echo: second") {
		t.Fatalf("turns ran out of order: %v", order)
	}

	// The first prompt's response comes from pi and is addressed to its owner.
	if !hasFrame(recs, "response", `"id":"c_1:r1"`) {
		t.Fatalf("missing owned response in %s", dump(recs))
	}
}

func TestActorClearQueueReturnsDaemonPrompts(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "30")
	a := startTestActor(t, Params{SessionPath: filepath.Join(t.TempDir(), "s.jsonl"), HubCapacity: 256})
	client := &fakeClient{id: "c_1", kind: "test", name: "one",
		caps: map[string]bool{"prompt": true, "observe": true}}
	a.Attach(client)
	sub := a.Subscribe(client.id, 256)

	if err := a.Submit(promptCmd(client, "r1", "running")); err != nil {
		t.Fatal(err)
	}
	if err := a.Submit(promptCmd(client, "r2", "queued")); err != nil {
		t.Fatal(err)
	}
	if err := a.Submit(ClientCommand{Client: client, LocalID: "r3", Type: "clear_queue",
		Raw: []byte(`{"type":"clear_queue","id":"r3"}`)}); err != nil {
		t.Fatal(err)
	}
	recs := collectUntil(t, sub, func(rec protocol.Record) bool {
		return rec.Type == "response" && strings.Contains(string(rec.Raw), `"id":"c_1:r3"`)
	}, 5*time.Second)
	raw := dump(recs)
	if !strings.Contains(raw, "queued") || !strings.Contains(raw, `"followUp"`) {
		t.Fatalf("clear_queue response must include the daemon-queued prompt: %s", raw)
	}
}

func TestActorRetiresWhenIdle(t *testing.T) {
	bin, err := testutil.FakePi()
	if err != nil {
		t.Skipf("cannot build fake pi: %v", err)
	}
	a := NewActor(Params{
		PiBin:       bin,
		SessionPath: filepath.Join(t.TempDir(), "s.jsonl"),
		ShortGrace:  50 * time.Millisecond,
		IdleTimeout: time.Minute,
	})
	retired := make(chan struct{}, 1)
	stopped := make(chan string, 1)
	a.Retire = func(*Actor) bool { retired <- struct{}{}; return true }
	a.OnStopped = func(_ *Actor, reason string) { stopped <- reason }
	if err := a.Start(); err != nil {
		t.Fatalf("start actor: %v", err)
	}
	t.Cleanup(a.Stop)

	client := &fakeClient{id: "c_1", kind: "test", name: "one", caps: map[string]bool{}}
	a.Attach(client)
	a.Detach(client.ID())

	select {
	case <-retired:
	case <-time.After(3 * time.Second):
		t.Fatal("actor was not retired after the short grace")
	}
	select {
	case reason := <-stopped:
		if reason != stateHibernated {
			t.Fatalf("stop reason = %q", reason)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("actor stop was not reported")
	}
	select {
	case <-a.Finished():
	case <-time.After(3 * time.Second):
		t.Fatal("actor loop did not exit")
	}
}

func hasFrame(recs []protocol.Record, typ, substr string) bool {
	for _, rec := range recs {
		if rec.Type == typ && strings.Contains(string(rec.Raw), substr) {
			return true
		}
	}
	return false
}

func dump(recs []protocol.Record) string {
	var b strings.Builder
	for _, rec := range recs {
		b.WriteString(rec.Type)
		b.WriteString(": ")
		b.Write(rec.Raw)
		b.WriteString("\n")
	}
	return b.String()
}

func TestHubUnsubscribeRemovesSubscriber(t *testing.T) {
	h := NewHub(4, "s")
	sub := h.Subscribe("c_1", 2)
	h.Unsubscribe("c_1")
	if got := len(h.subs); got != 0 {
		t.Fatalf("subscriber map still has %d entries", got)
	}
	if _, ok := <-sub.C(); ok {
		t.Fatal("unsubscribed channel must be closed")
	}
	// Publishing after unsubscribe must not panic or block.
	h.Publish(protocol.Record{Raw: []byte(`{"type":"x"}`), Type: "x"})
}

func TestActorRejectedPromptReleasesTurn(t *testing.T) {
	t.Setenv("FAKEPI_REJECT_PROMPT", "1")
	a := startTestActor(t, Params{SessionPath: filepath.Join(t.TempDir(), "s.jsonl"), HubCapacity: 256})
	client := &fakeClient{id: "c_1", kind: "test", name: "one", caps: map[string]bool{"prompt": true}}
	a.Attach(client)
	sub := a.Subscribe(client.id, 256)

	if err := a.Submit(promptCmd(client, "r1", "rejected")); err != nil {
		t.Fatal(err)
	}
	recs := collectUntil(t, sub, func(rec protocol.Record) bool {
		return rec.Type == "gw_turn" && strings.Contains(string(rec.Raw), `"settled"`)
	}, 5*time.Second)
	if !hasFrame(recs, "response", `"success":false`) {
		t.Fatalf("expected a rejected prompt response: %s", dump(recs))
	}

	// The next prompt must be forwarded immediately, not stuck in the queue.
	if err := a.Submit(promptCmd(client, "r2", "next")); err != nil {
		t.Fatal(err)
	}
	recs = collectUntil(t, sub, func(rec protocol.Record) bool {
		return rec.Type == "response" && strings.Contains(string(rec.Raw), `"id":"c_1:r2"`)
	}, 5*time.Second)
	if strings.Contains(dump(recs), `"queued":true`) {
		t.Fatalf("prompt was queued after a rejected turn: %s", dump(recs))
	}
	if !hasFrame(recs, "gw_turn", `"running"`) {
		t.Fatalf("second prompt did not start a turn: %s", dump(recs))
	}
}

func TestActorStaleUIResponseAfterCrash(t *testing.T) {
	t.Setenv("FAKEPI_UI_REQUEST", "1")
	t.Setenv("FAKEPI_EXIT_AFTER_FIRST_TURN", "1")
	a := startTestActor(t, Params{SessionPath: filepath.Join(t.TempDir(), "s.jsonl"), HubCapacity: 256})
	client := &fakeClient{id: "c_1", kind: "test", name: "one",
		caps: map[string]bool{"prompt": true, "ui": true}}
	a.Attach(client)
	sub := a.Subscribe(client.id, 256)

	if err := a.Submit(promptCmd(client, "r1", "dialog")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	var sawUI, sawCrash bool
	for !sawCrash {
		select {
		case rec, ok := <-sub.C():
			if !ok {
				t.Fatal("subscriber closed before the crash notification")
			}
			switch {
			case rec.Type == "extension_ui_request":
				sawUI = true
			case rec.Type == "gw_session_state" && strings.Contains(string(rec.Raw), `"crashed"`):
				sawCrash = true
			}
		case <-deadline:
			t.Fatalf("timed out waiting for the crash (ui=%v)", sawUI)
		}
	}
	if !sawUI {
		t.Fatal("fake pi never asked for a dialog")
	}

	// Answering the stale dialog must not panic the actor; it gets ui_stale.
	if err := a.Submit(ClientCommand{Client: client, LocalID: "ui-1", Type: "extension_ui_response",
		Raw: []byte(`{"type":"extension_ui_response","id":"ui-1","confirmed":true}`)}); err != nil {
		t.Fatal(err)
	}
	recs := collectUntil(t, sub, func(rec protocol.Record) bool {
		return rec.Type == "response" && strings.Contains(string(rec.Raw), `"id":"ui-1"`)
	}, 5*time.Second)
	if !strings.Contains(dump(recs), protocol.CodeUIStale) {
		t.Fatalf("expected ui_stale, got %s", dump(recs))
	}
}

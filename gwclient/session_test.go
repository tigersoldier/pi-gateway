package gwclient_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/gwclient"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// TestAttachConflictReturnsError pins the gw_hello attach contract: when the
// requested session exists but conflicts on a spawn parameter, Dial must
// report the gw_error instead of returning an unbound client whose next
// command would create a second session.
func TestAttachConflictReturnsError(t *testing.T) {
	addr, _ := gwtest.StartDaemon(t, testToken, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	creator := dialTo(t, addr, nil)
	sess, err := creator.NewSession(ctx, gwclient.NewSessionRequest{
		Cwd:    t.TempDir(),
		PiArgs: []string{"--approve"},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	bad, err := gwclient.Dial(ctx, gwclient.Config{
		Addr: addr, Token: testToken, LiveOnly: true,
		Session: sess.Path, PiArgs: []string{"--no-approve"},
	})
	var rerr *gwclient.ResponseError
	if !errors.As(err, &rerr) {
		t.Fatalf("Dial err = %v (%T), want *ResponseError", err, err)
	}
	if rerr.Code != protocol.CodeSpawnParamConflict {
		t.Fatalf("error code = %q, want %q", rerr.Code, protocol.CodeSpawnParamConflict)
	}
	if bad != nil {
		t.Fatalf("Dial returned a client (%+v) despite the failed attach", bad)
	}

	rows, err := creator.ListSessions(ctx, gwclient.SessionFilter{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(rows) != 1 || rows[0].Path != sess.Path {
		t.Fatalf("catalog = %+v, want only the requested session %s", rows, sess.Path)
	}
}

// TestLazySessionBecomesVisible covers the implicit session a first command
// creates: GetState must teach the client the resolved path and turn state.
func TestLazySessionBecomesVisible(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if c.Session() != nil {
		t.Fatalf("unbound client reports session %+v", c.Session())
	}
	if _, err := c.Prompt(ctx, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	text, err := waitForAssistantText(c, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if text != "echo: hello" {
		t.Fatalf("assistant text = %q", text)
	}

	state, err := c.GetState(ctx)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	got := c.Session()
	if got == nil || got.Path == "" {
		t.Fatalf("Session() = %+v after GetState, want the lazily created session", got)
	}
	if got.Path != state.SessionFile {
		t.Fatalf("Session().Path = %q, get_state sessionFile = %q", got.Path, state.SessionFile)
	}
}

func TestPromptCarriesImages(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	img := protocol.NewImage([]byte("not-really-a-png"), "image/png")
	if _, err := c.Prompt(ctx, "look", img); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	text, err := waitForAssistantText(c, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if text != "echo: look [images: 1]" {
		t.Fatalf("assistant text = %q, want the image to reach pi", text)
	}
}

// TestReconnectResumesFromCursor loses a connection, lets another client
// advance the session, and checks the reconnect replays exactly the missed
// turn before going live again.
func TestReconnectResumesFromCursor(t *testing.T) {
	addr, _ := gwtest.StartDaemon(t, testToken, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	c := dialTo(t, addr, nil)
	sess, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := c.Prompt(ctx, "one"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if text, err := waitForAssistantText(c, 20*time.Second); err != nil || text != "echo: one" {
		t.Fatalf("first turn: text=%q err=%v", text, err)
	}
	cursor := c.Cursor()
	if cursor.SinceSeq == 0 {
		t.Fatalf("cursor has no sequence: %+v", cursor)
	}

	// The connection is lost...
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// ...and the session moves on without us.
	other := dialTo(t, addr, nil)
	if _, err := other.SwitchSession(ctx, sess.Path); err != nil {
		t.Fatalf("SwitchSession: %v", err)
	}
	if _, err := other.Prompt(ctx, "two"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if text, err := waitForAssistantText(other, 20*time.Second); err != nil || text != "echo: two" {
		t.Fatalf("second turn: text=%q err=%v", text, err)
	}

	next, err := c.Reconnect(ctx)
	if err != nil {
		t.Fatalf("Reconnect: %v", err)
	}
	t.Cleanup(func() { _ = next.Close() })
	if got := next.Session(); got == nil || got.Path != sess.Path {
		t.Fatalf("reconnected session = %+v, want %s", got, sess.Path)
	}
	if next.Welcome().ResyncRequired {
		t.Fatalf("reconnect required a resync: %+v", next.Welcome())
	}

	var replayed []string
	sawDone := false
	deadline := time.After(20 * time.Second)
	for !sawDone {
		select {
		case ev, ok := <-next.Events():
			if !ok {
				t.Fatalf("events closed before gw_replay_done: %v", next.Err())
			}
			if text, ok := assistantText(ev); ok {
				replayed = append(replayed, text)
			}
			if ev.Type == "gw_replay_done" {
				sawDone = true
			}
		case <-deadline:
			t.Fatal("no gw_replay_done after reconnect")
		}
	}
	if len(replayed) != 1 || replayed[0] != "echo: two" {
		t.Fatalf("replayed messages = %v, want [echo: two]", replayed)
	}
	if next.LastSeq() <= cursor.SinceSeq {
		t.Fatalf("cursor did not advance past the gap: %d -> %d", cursor.SinceSeq, next.LastSeq())
	}

	if _, err := next.Prompt(ctx, "three"); err != nil {
		t.Fatalf("Prompt after reconnect: %v", err)
	}
	if text, err := waitForAssistantText(next, 20*time.Second); err != nil || text != "echo: three" {
		t.Fatalf("turn after reconnect: text=%q err=%v", text, err)
	}
}

// TestResyncSnapshotCarriesWatermark forces the unreplayable-cursor path and
// checks the snapshot reports the boundary the pump used, so the client's
// cursor does not stay stuck at the old daemon generation's sequence.
func TestResyncSnapshotCarriesWatermark(t *testing.T) {
	addr, _ := gwtest.StartDaemon(t, testToken, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := dialTo(t, addr, nil)
	sess, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := c.Prompt(ctx, "one"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if _, err := waitForAssistantText(c, 20*time.Second); err != nil {
		t.Fatal(err)
	}

	b, err := gwclient.Dial(ctx, gwclient.Config{
		Addr: addr, Token: testToken,
		Session: sess.Path,
		Resume:  &protocol.Resume{SinceSeq: 1 << 40}, // beyond any live hub
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if !b.Welcome().ResyncRequired {
		t.Fatalf("welcome = %+v, want resyncRequired", b.Welcome())
	}

	var snap protocol.Snapshot
	deadline := time.After(20 * time.Second)
	for snap.Type == "" {
		select {
		case ev, ok := <-b.Events():
			if !ok {
				t.Fatalf("events closed before gw_snapshot: %v", b.Err())
			}
			if ev.Type != "gw_snapshot" {
				continue
			}
			s, err := ev.Snapshot()
			if err != nil {
				t.Fatalf("Snapshot decode: %v", err)
			}
			snap = s
		case <-deadline:
			t.Fatal("no gw_snapshot arrived")
		}
	}
	if snap.HeadSeq == 0 {
		t.Fatalf("snapshot has no watermark: %+v", snap)
	}
	if b.LastSeq() != snap.HeadSeq {
		t.Fatalf("client cursor = %d, want the snapshot watermark %d", b.LastSeq(), snap.HeadSeq)
	}
	if len(snap.Entries) == 0 {
		t.Fatalf("snapshot carried no entries: %+v", snap)
	}

	if _, err := b.Prompt(ctx, "two"); err != nil {
		t.Fatalf("Prompt after resync: %v", err)
	}
	if text, err := waitForAssistantText(b, 20*time.Second); err != nil || text != "echo: two" {
		t.Fatalf("turn after resync: text=%q err=%v", text, err)
	}
	if b.LastSeq() <= snap.HeadSeq {
		t.Fatalf("cursor did not advance past the snapshot: %d -> %d", snap.HeadSeq, b.LastSeq())
	}
}

func TestListSessionsCreatorFilter(t *testing.T) {
	c := dial(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	alpha, err := c.NewSession(ctx, gwclient.NewSessionRequest{
		Name: "alpha", Cwd: t.TempDir(), Tags: map[string]string{"bot": "alpha"},
	})
	if err != nil {
		t.Fatalf("NewSession alpha: %v", err)
	}
	beta, err := c.NewSession(ctx, gwclient.NewSessionRequest{
		Name: "beta", Cwd: t.TempDir(), Tags: map[string]string{"bot": "beta"},
	})
	if err != nil {
		t.Fatalf("NewSession beta: %v", err)
	}

	rows, err := c.ListSessions(ctx, gwclient.SessionFilter{Tags: map[string]string{"bot": "alpha"}})
	if err != nil {
		t.Fatalf("ListSessions tags: %v", err)
	}
	if len(rows) != 1 || rows[0].Path != alpha.Path {
		t.Fatalf("tag filter = %+v, want only %s", rows, alpha.Path)
	}

	rows, err = c.ListSessions(ctx, gwclient.SessionFilter{CreatedBy: c.ClientID()})
	if err != nil {
		t.Fatalf("ListSessions creator: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("creator filter = %+v, want the two sessions this client created", rows)
	}

	rows, err = c.ListSessions(ctx, gwclient.SessionFilter{Tags: map[string]string{"bot": "gamma"}})
	if err != nil {
		t.Fatalf("ListSessions unknown tag: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("unknown tag = %+v, want none", rows)
	}

	rows, err = c.ListSessions(ctx, gwclient.SessionFilter{
		Tags: map[string]string{"bot": "beta"}, Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListSessions limit: %v", err)
	}
	if len(rows) != 1 || rows[0].Path != beta.Path {
		t.Fatalf("tag filter with limit = %+v, want only %s", rows, beta.Path)
	}
}

// TestOnEventDeliversInsteadOfEvents checks the callback delivery mode: no
// consumer goroutine is needed, frames arrive on the read goroutine, and the
// channel stays empty.
func TestOnEventDeliversInsteadOfEvents(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	c := dial(t, func(cfg *gwclient.Config) {
		cfg.OnEvent = func(ev gwclient.Event) {
			mu.Lock()
			seen = append(seen, ev.Type)
			mu.Unlock()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := c.NewSession(ctx, gwclient.NewSessionRequest{Cwd: t.TempDir()}); err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if _, err := c.Prompt(ctx, "hello"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if err := c.AwaitSettled(ctx); err != nil {
		t.Fatalf("AwaitSettled: %v", err)
	}

	mu.Lock()
	sawEnd := false
	for _, typ := range seen {
		if typ == "message_end" {
			sawEnd = true
		}
	}
	mu.Unlock()
	if !sawEnd {
		t.Fatalf("OnEvent saw %v, want a message_end", seen)
	}
	select {
	case ev := <-c.Events():
		t.Fatalf("Events() delivered %q while OnEvent was set", ev.Type)
	default:
	}
}

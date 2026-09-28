package daemon_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/catalog"
	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// writeCatalogSession writes a session file the catalog scanner can parse, and
// returns its path.
func writeCatalogSession(t *testing.T, root, fileID, name, cwd string, messageCount int) string {
	t.Helper()
	dir := filepath.Join(root, "--cwd--")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fileID+".jsonl")
	lines := []any{testutil.SessionHeader("id-"+fileID, cwd, "2026-01-01T00:00:00.000Z")}
	if name != "" {
		lines = append(lines, map[string]any{"type": "session_info", "id": "n1", "name": name})
	}
	for i := 0; i < messageCount; i++ {
		lines = append(lines, map[string]any{
			"type": "message", "id": fmt.Sprintf("e%d", i),
			"timestamp": fmt.Sprintf("2026-01-01T00:00:%02d.000Z", i),
			"message":   map[string]any{"role": "user", "content": fmt.Sprintf("question %d", i)},
		})
	}
	testutil.WriteSession(t, path, lines...)
	return path
}

func listSessions(t *testing.T, c *testutil.Conn, id string) []map[string]any {
	t.Helper()
	c.Send(map[string]any{"type": "gw_list_sessions", "id": id})
	resp := c.WaitResponse(id, testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("gw_list_sessions failed: %v", resp)
	}
	var out []map[string]any
	for _, raw := range testutil.Arr(testutil.Obj(resp, "data"), "sessions") {
		if row, ok := raw.(map[string]any); ok {
			out = append(out, row)
		}
	}
	return out
}

func findRow(t *testing.T, rows []map[string]any, path string) map[string]any {
	t.Helper()
	for _, row := range rows {
		if testutil.Str(row, "path") == path {
			return row
		}
	}
	t.Fatalf("no catalog row for %s in %v", path, rows)
	return nil
}

func TestListSessionsCatalog(t *testing.T) {
	root := t.TempDir()
	path := writeCatalogSession(t, root, "auth", "auth-refactor", "/home/u/proj", 3)
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		c.CatalogRoots = []string{root}
	})
	c := dial(t, addr, nil)
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	clientID := testutil.Str(w, "clientId")

	rows := listSessions(t, c, "l1")
	row := findRow(t, rows, path)
	if got := testutil.Str(row, "name"); got != "auth-refactor" {
		t.Fatalf("name = %q", got)
	}
	if got := testutil.Str(row, "title"); got != "question 0" {
		t.Fatalf("title = %q", got)
	}
	if got := testutil.Str(row, "id"); got != "id-auth" {
		t.Fatalf("id = %q", got)
	}
	if got := testutil.Num(row, "messageCount"); got != 3 {
		t.Fatalf("messageCount = %v", got)
	}
	if row["live"] == true {
		t.Fatalf("session must not be live before attach: %v", row)
	}

	// Attach by name: the daemon resolves it and spawns pi for the file.
	c.Send(map[string]any{"type": "switch_session", "id": "s1", "sessionPath": "auth-refactor"})
	if resp := c.WaitResponse("s1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("attach by name failed: %v", resp)
	}
	row = findRow(t, listSessions(t, c, "l2"), path)
	if row["live"] != true {
		t.Fatalf("attached session must be live: %v", row)
	}
	clients := testutil.Arr(row, "clients")
	if len(clients) != 1 {
		t.Fatalf("live session clients = %v", clients)
	}
	if got := testutil.Str(clients[0].(map[string]any), "clientId"); got != clientID {
		t.Fatalf("client = %q, want %q", got, clientID)
	}

	// The cwd filter keeps the matching session and drops others.
	c.Send(map[string]any{"type": "gw_list_sessions", "id": "l3",
		"filter": map[string]any{"cwd": "/home/u/proj"}})
	resp := c.WaitResponse("l3", testutil.DefaultTimeout)
	sessions := testutil.Arr(testutil.Obj(resp, "data"), "sessions")
	if len(sessions) != 1 {
		t.Fatalf("cwd filter = %v, want only the project session", sessions)
	}
	c.Send(map[string]any{"type": "gw_list_sessions", "id": "l4",
		"filter": map[string]any{"cwd": "/somewhere/else"}})
	resp = c.WaitResponse("l4", testutil.DefaultTimeout)
	if sessions := testutil.Arr(testutil.Obj(resp, "data"), "sessions"); len(sessions) != 0 {
		t.Fatalf("cwd filter should drop every session: %v", sessions)
	}
}

func TestSessionNameResolution(t *testing.T) {
	root := t.TempDir()
	unique := writeCatalogSession(t, root, "one", "unique-name", "/home/u/proj", 1)
	writeCatalogSession(t, root, "dup-a", "dup-name", "/home/u/proj", 1)
	writeCatalogSession(t, root, "dup-b", "dup-name", "/home/u/proj", 1)
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		c.CatalogRoots = []string{root}
	})
	c := dial(t, addr, nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	c.Send(map[string]any{"type": "switch_session", "id": "a1", "sessionPath": "unique-name"})
	if resp := c.WaitResponse("a1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("unique name must attach: %v", resp)
	}
	if got := sessionPath(t, map[string]any{"data": map[string]any{"sessionFile": unique}}); got != unique {
		t.Fatalf("unexpected path %q", got)
	}

	c.Send(map[string]any{"type": "switch_session", "id": "a2", "sessionPath": "dup-name"})
	resp := c.WaitResponse("a2", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeAmbiguousSession {
		t.Fatalf("ambiguous name must fail with ambiguous_session: %v", resp)
	}

	c.Send(map[string]any{"type": "switch_session", "id": "a3", "sessionPath": "no-such-session"})
	resp = c.WaitResponse("a3", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUnknownSession {
		t.Fatalf("unknown name must fail with unknown_session: %v", resp)
	}
}

func TestGWNewSessionCreatesAndBinds(t *testing.T) {
	root := t.TempDir()
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		c.CatalogRoots = []string{root}
	})
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Client = protocol.ClientInfo{Name: "slack-bot", Kind: "integration"}
	})
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	clientID := testutil.Str(w, "clientId")

	c.Send(map[string]any{
		"type": "gw_new_session", "id": "n1", "name": "slack-auth-thread",
		"piArgs": []string{"--session-dir", root},
		"tags":   map[string]string{"channel": "#auth"},
	})
	resp := c.WaitResponse("n1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("gw_new_session failed: %v", resp)
	}
	data := testutil.Obj(resp, "data")
	path := testutil.Str(data, "path")
	if path == "" || !strings.HasPrefix(path, root) {
		t.Fatalf("path = %q, want a file under %s", path, root)
	}
	if got := testutil.Str(data, "name"); got != "slack-auth-thread" {
		t.Fatalf("name = %q", got)
	}
	if testutil.Str(data, "sessionId") == "" {
		t.Fatalf("sessionId missing: %v", data)
	}

	// The requester is rebound to the new session.
	c.Send(map[string]any{"type": "get_state", "id": "g1"})
	if got := sessionPath(t, c.WaitResponse("g1", testutil.DefaultTimeout)); got != path {
		t.Fatalf("bound session = %q, want %q", got, path)
	}

	// The catalog reports the creator and its tags.
	row := findRow(t, listSessions(t, c, "l1"), path)
	if row["live"] != true {
		t.Fatalf("new session must be live: %v", row)
	}
	createdBy := testutil.Obj(row, "createdBy")
	if got := testutil.Str(createdBy, "clientId"); got != clientID {
		t.Fatalf("createdBy.clientId = %q, want %q", got, clientID)
	}
	if got := testutil.Str(createdBy, "kind"); got != "integration" {
		t.Fatalf("createdBy.kind = %q", got)
	}
	if got := testutil.Str(testutil.Obj(createdBy, "tags"), "channel"); got != "#auth" {
		t.Fatalf("createdBy.tags = %v", createdBy["tags"])
	}
}

func TestReloadSessionOtherClientAndForced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reload.jsonl")
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		c.CatalogRoots = []string{dir}
	})
	a, _, aID := dialSession(t, addr, path)
	a.Send(prompt("p1", "hello"))
	if resp := a.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	a.WaitType("agent_settled", testutil.DefaultTimeout)
	b, _, _ := dialSession(t, addr, path)

	a.Send(map[string]any{"type": "gw_reload_session", "id": "r1"})
	resp := a.WaitResponse("r1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeReloadBusy {
		t.Fatalf("reload with another client attached must be reload_busy: %v", resp)
	}

	// The lifecycle events are published before the response, so collect the
	// requester's stream until it arrives.
	a.Send(map[string]any{"type": "gw_reload_session", "id": "r2", "force": true})
	sawResponse, sawReady, sawChanged := false, false, false
	framesA := a.CollectUntil(func(f map[string]any) bool {
		if f["type"] == "response" && testutil.Str(f, "id") == "r2" {
			if f["success"] != true {
				t.Fatalf("forced reload failed: %v", f)
			}
			sawResponse = true
		}
		if f["type"] == "gw_session_state" && testutil.Str(f, "state") == "ready" {
			sawReady = true
		}
		if f["type"] == "gw_state_changed" && testutil.Str(f, "command") == "gw_reload_session" {
			sawChanged = true
		}
		return sawResponse && sawReady && sawChanged
	}, testutil.DefaultTimeout)
	// The bystander sees the same lifecycle without asking for it.
	framesB := b.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "gw_state_changed" && testutil.Str(f, "command") == "gw_reload_session"
	}, testutil.DefaultTimeout)
	assertReloadFrames(t, "requester", framesA, aID)
	assertReloadFrames(t, "bystander", framesB, aID)

	// The session file survived the restart with its history.
	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	data := testutil.Obj(a.WaitResponse("g1", testutil.DefaultTimeout), "data")
	if got := testutil.Num(data, "messageCount"); got < 2 {
		t.Fatalf("messageCount after reload = %v, want the file's history", got)
	}
}

func TestReloadSessionBusyWhileTurnRunning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "busy.jsonl")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "300")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	c, _, _ := dialSession(t, addr, path)
	c.Send(prompt("p1", "slow turn"))
	if resp := c.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	c.Send(map[string]any{"type": "gw_reload_session", "id": "r1"})
	resp := c.WaitResponse("r1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeReloadBusy {
		t.Fatalf("reload during a turn must be reload_busy: %v", resp)
	}
	c.Send(map[string]any{"type": "abort", "id": "a1"})
	c.WaitResponse("a1", testutil.DefaultTimeout)
	c.WaitFor(func(f map[string]any) bool { return f["type"] == "agent_settled" }, testutil.DefaultTimeout)
}

func TestForkClonePolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "forkable.jsonl")
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		c.CatalogRoots = []string{dir}
	})
	a, _, _ := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)

	a.Send(map[string]any{"type": "clone", "id": "c1"})
	resp := a.WaitResponse("c1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeSharedSession {
		t.Fatalf("clone with another client attached must be shared_session: %v", resp)
	}

	// Wait for the detach to reach the actor: the shared-session check counts
	// the clients the actor knows about.
	b.Close()
	a.WaitFor(func(f map[string]any) bool {
		return f["type"] == "gw_presence" && testutil.Str(f, "event") == "leave"
	}, testutil.DefaultTimeout)
	a.Send(map[string]any{"type": "clone", "id": "c2"})
	resp = a.WaitResponse("c2", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("clone as sole client failed: %v", resp)
	}
	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	newPath := sessionPath(t, a.WaitResponse("g1", testutil.DefaultTimeout))
	if newPath == path {
		t.Fatalf("clone did not move the session (still %q)", path)
	}

	// The daemon re-keyed the live actor: the new path is live, the old one is
	// not, and a fresh client can attach to the new file.
	rows := listSessions(t, a, "l1")
	if row := findRow(t, rows, newPath); row["live"] != true {
		t.Fatalf("cloned session must be live under its new path: %v", row)
	}
	if row := findRow(t, rows, path); row["live"] == true {
		t.Fatalf("old path must not stay live after clone: %v", row)
	}
	d, w, _ := dialSession(t, addr, newPath)
	ref := testutil.Obj(w, "session")
	if got := testutil.Str(ref, "path"); got != newPath {
		t.Fatalf("second client attached to %q, want %q", got, newPath)
	}
	if id := testutil.Str(ref, "id"); id == "" || id == newPath {
		t.Fatalf("welcome session.id = %q, want pi's session id", id)
	}
	d.Close()
}

func TestUIRequestRoutedToTurnAuthor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ui.jsonl")
	t.Setenv("FAKEPI_UI_REQUEST", "1")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "20")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	// Both clients are ui-capable; the turn author must win the dialog.
	a, _, _ := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)
	a.Send(prompt("p1", "ask me"))
	if resp := a.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	req := a.WaitFor(func(f map[string]any) bool { return f["type"] == "extension_ui_request" }, testutil.DefaultTimeout)
	if got := testutil.Str(req, "method"); got != "confirm" {
		t.Fatalf("dialog = %v", req)
	}
	if frames := b.Drain(150 * time.Millisecond); len(frames) > 0 {
		for _, f := range frames {
			if f["type"] == "extension_ui_request" {
				t.Fatalf("non-author received a dialog: %v", f)
			}
		}
	}

	// Only the routed client may answer.
	b.Send(map[string]any{"type": "extension_ui_response", "id": "ui-1", "confirmed": true})
	resp := b.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "command") == "extension_ui_response"
	}, testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUIStale {
		t.Fatalf("non-owner response must be ui_stale: %v", resp)
	}
	// The owner's answer is accepted (and consumed); pi writes no response, so a
	// repeat from the same client is ui_stale — the observable the gateway owns.
	a.Send(map[string]any{"type": "extension_ui_response", "id": "ui-1", "confirmed": true})
	a.Send(map[string]any{"type": "extension_ui_response", "id": "ui-1", "confirmed": true})
	if resp := a.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "command") == "extension_ui_response"
	}, testutil.DefaultTimeout); resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUIStale {
		t.Fatalf("repeat owner response must be ui_stale: %v", resp)
	}
	a.WaitType("agent_settled", testutil.DefaultTimeout)

	// A disconnected author hands the pending dialog to the other ui client.
	a.Send(prompt("p2", "ask again"))
	a.WaitResponse("p2", testutil.DefaultTimeout)
	a.WaitFor(func(f map[string]any) bool { return f["type"] == "extension_ui_request" }, testutil.DefaultTimeout)
	a.Close()
	reassigned := b.WaitFor(func(f map[string]any) bool { return f["type"] == "extension_ui_request" }, testutil.DefaultTimeout)
	if got := testutil.Str(reassigned, "id"); got != "ui-1" {
		t.Fatalf("reassigned dialog id = %q", got)
	}
	b.Send(map[string]any{"type": "extension_ui_response", "id": "ui-1", "confirmed": true})
	b.Send(map[string]any{"type": "extension_ui_response", "id": "ui-1", "confirmed": true})
	if resp := b.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "command") == "extension_ui_response"
	}, testutil.DefaultTimeout); resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUIStale {
		t.Fatalf("repeat reassigned response must be ui_stale: %v", resp)
	}
}

func TestDurableResumeWithMatchingLeafSkipsSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "durable.jsonl")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	a, _, _ := dialSession(t, addr, path)
	a.Send(prompt("p1", "persist me"))
	if resp := a.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	a.WaitType("agent_settled", testutil.DefaultTimeout)

	leaf := leafID(t, path)

	// A cursor far beyond the ring is unreplayable, but the leaf proves the
	// client's transcript is already complete.
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.LiveOnly = false
		h.Resume = &protocol.Resume{SinceSeq: 1 << 30, LeafEntryID: leaf}
	})
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["resyncRequired"] == true {
		t.Fatalf("matching leaf must not force a resync: %v", w)
	}
	for _, f := range c.Drain(200 * time.Millisecond) {
		if f["type"] == "gw_snapshot" || f["type"] == "gw_replay" {
			t.Fatalf("unexpected resync frame: %v", f)
		}
	}

	// A stale leaf still requires a snapshot.
	c2 := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.LiveOnly = false
		h.Resume = &protocol.Resume{SinceSeq: 1 << 30, LeafEntryID: "not-the-leaf"}
	})
	w2 := c2.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w2["resyncRequired"] != true {
		t.Fatalf("stale leaf must require a resync: %v", w2)
	}
	c2.WaitType("gw_snapshot", testutil.DefaultTimeout)
}

// leafID reads the last entry id pi persisted for a session file.
func leafID(t *testing.T, path string) string {
	t.Helper()
	leaf, err := catalog.LeafID(path)
	if err != nil || leaf == "" {
		t.Fatalf("catalog.LeafID(%s) = %q, %v", path, leaf, err)
	}
	return leaf
}

func TestStreamingDeltasAreCoalesced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "coalesce.jsonl")
	t.Setenv("FAKEPI_TURN_EVENTS", "40")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "5")
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		// A long flush window makes the merge deterministic: only a
		// non-delta frame (text_end) flushes the run.
		c.DeltaFlush = time.Second
	})
	c, _, _ := dialSession(t, addr, path)
	c.Send(prompt("p1", "stream"))

	var deltas []string
	deadline := time.After(testutil.DefaultTimeout)
	for {
		var frame map[string]any
		select {
		case f, ok := <-c.Frames():
			if !ok {
				t.Fatal("connection closed while streaming")
			}
			frame = f
		case <-deadline:
			t.Fatal("timed out waiting for the turn to settle")
		}
		if frame["type"] == "message_update" {
			event := testutil.Obj(frame, "assistantMessageEvent")
			if testutil.Str(event, "type") == "text_delta" {
				deltas = append(deltas, testutil.Str(event, "delta"))
			}
		}
		if frame["type"] == "agent_settled" {
			break
		}
	}
	if len(deltas) == 0 {
		t.Fatal("no text deltas arrived")
	}
	if len(deltas) >= 40 {
		t.Fatalf("deltas were not coalesced: %d frames", len(deltas))
	}
	var want strings.Builder
	for i := 0; i < 40; i++ {
		fmt.Fprintf(&want, "stream[%d]", i)
	}
	if got := strings.Join(deltas, ""); got != want.String() {
		t.Fatalf("coalesced text = %q, want %q", got, want.String())
	}
}

// assertReloadFrames checks that a reload stream carried restarting, ready, and
// the shared-state change, in that order, authored by the requester.
func assertReloadFrames(t *testing.T, name string, frames []map[string]any, author string) {
	t.Helper()
	restarting, ready, changed := -1, -1, -1
	for i, f := range frames {
		switch {
		case f["type"] == "gw_session_state" && testutil.Str(f, "state") == "restarting" && restarting < 0:
			restarting = i
		case f["type"] == "gw_session_state" && testutil.Str(f, "state") == "ready" && ready < 0:
			ready = i
		case f["type"] == "gw_state_changed" && testutil.Str(f, "command") == "gw_reload_session" && changed < 0:
			changed = i
			if got := testutil.Str(testutil.Obj(f, "by"), "clientId"); got != author {
				t.Fatalf("%s: reload state change author = %q, want %q", name, got, author)
			}
		}
	}
	if restarting < 0 || ready < 0 || changed < 0 || !(restarting < ready && ready < changed) {
		t.Fatalf("%s: reload frames out of order (restarting=%d ready=%d changed=%d): %v",
			name, restarting, ready, changed, frames)
	}
}

// TestResumeDeliversEveryDeltaAcrossReplayBoundary guards the ordering fix in
// handleHello: the connection subscribes before reading the replay window, so
// records published in between are held in the subscriber buffer and the
// watermark drops only the duplicates. Content completeness is checked rather
// than gw_seq contiguity, because coalescing intentionally skips sequences.
func TestResumeDeliversEveryDeltaAcrossReplayBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resume.jsonl")
	t.Setenv("FAKEPI_TURN_EVENTS", "200")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "5")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	a, _, _ := dialSession(t, addr, path)
	a.Send(prompt("p1", "stream"))
	if resp := a.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	// Attach with a resume cursor while the turn is streaming.
	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.LiveOnly = false
		h.Resume = &protocol.Resume{SinceSeq: 0}
	})
	b.WaitType("gw_welcome", testutil.DefaultTimeout)

	var text strings.Builder
	sawReplayDone := false
	deadline := time.After(20 * time.Second)
	for {
		var frame map[string]any
		select {
		case f, ok := <-b.Frames():
			if !ok {
				t.Fatal("connection closed while collecting deltas")
			}
			frame = f
		case <-deadline:
			t.Fatal("timed out waiting for the turn to settle")
		}
		switch frame["type"] {
		case "gw_replay_done":
			sawReplayDone = true
		case "message_update":
			event := testutil.Obj(frame, "assistantMessageEvent")
			if testutil.Str(event, "type") == "text_delta" {
				text.WriteString(testutil.Str(event, "delta"))
			}
		case "agent_settled":
			if !sawReplayDone {
				t.Fatal("resume attach produced no replay window")
			}
			var want strings.Builder
			for i := 0; i < 200; i++ {
				fmt.Fprintf(&want, "stream[%d]", i)
			}
			if got := text.String(); got != want.String() {
				t.Fatalf("resumed stream is incomplete:\n got %d bytes\nwant %d bytes",
					len(got), want.Len())
			}
			return
		}
	}
}

// TestForcedReloadDrainsQueuedPrompts guards the reload path: prompts queued
// while the old pi process died must run after the new one is ready.
func TestForcedReloadDrainsQueuedPrompts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reload-queue.jsonl")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "250")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	c, _, _ := dialSession(t, addr, path)
	c.Send(prompt("p1", "first"))
	if resp := c.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("first prompt rejected: %v", resp)
	}
	c.Send(prompt("p2", "second"))
	if resp := c.WaitResponse("p2", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("second prompt rejected: %v", resp)
	}
	c.Send(map[string]any{"type": "gw_reload_session", "id": "r1", "force": true})
	if resp := c.WaitResponse("r1", 30*time.Second); resp["success"] != true {
		t.Fatalf("forced reload failed: %v", resp)
	}
	if f := c.WaitFor(func(f map[string]any) bool {
		return f["type"] == "message_end" && testutil.MessageEndText(f) == "echo: second"
	}, 10*time.Second); f == nil {
		t.Fatal("the queued prompt never ran after the reload")
	}
}

// TestCrossSessionReloadGetsResponse covers gw_reload_session{session:...} for
// a session the requester is not attached to: the response must reach the
// requesting connection, not the target session's hub.
func TestCrossSessionReloadGetsResponse(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.jsonl")
	other := filepath.Join(dir, "other.jsonl")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	a, _, _ := dialSession(t, addr, other)
	b, _, _ := dialSession(t, addr, target)

	// The target has another client attached, so a forced reload is required.
	a.Send(map[string]any{"type": "gw_reload_session", "id": "r1", "session": target, "force": true})
	if resp := a.WaitResponse("r1", 30*time.Second); resp["success"] != true {
		t.Fatalf("cross-session reload failed: %v", resp)
	}
	// The target's client still sees the lifecycle events.
	b.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "gw_session_state" && testutil.Str(f, "state") == "ready"
	}, testutil.DefaultTimeout)

	// A reload_busy refusal for a foreign session is answered too.
	b2, _, _ := dialSession(t, addr, target)
	_ = b2
	a.Send(map[string]any{"type": "gw_reload_session", "id": "r2", "session": target})
	resp := a.WaitResponse("r2", 30*time.Second)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeReloadBusy {
		t.Fatalf("busy cross-session reload = %v, want reload_busy", resp)
	}
}

// TestListSessionsWhileCreatingIsRaceFree exercises the catalog builder and
// the session table concurrently; it fails under -race if the live snapshot
// reads entry fields without the daemon lock. Listing and creating use
// separate connections because a single connection is served sequentially.
func TestListSessionsWhileCreatingIsRaceFree(t *testing.T) {
	dir := t.TempDir()
	addr, _ := startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		c.CatalogRoots = []string{dir}
	})
	lister := dial(t, addr, nil)
	lister.WaitType("gw_welcome", testutil.DefaultTimeout)
	creator := dial(t, addr, nil)
	creator.WaitType("gw_welcome", testutil.DefaultTimeout)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			lister.Send(map[string]any{"type": "gw_list_sessions", "id": fmt.Sprintf("l%d", i)})
			time.Sleep(time.Millisecond)
		}
	}()
	for i := 0; i < 20; i++ {
		creator.Send(map[string]any{
			"type": "gw_new_session", "id": fmt.Sprintf("n%d", i),
			"name":   fmt.Sprintf("race-%d", i),
			"piArgs": []string{"--session-dir", dir},
			"tags":   map[string]string{"i": fmt.Sprint(i)},
		})
		if resp := creator.WaitResponse(fmt.Sprintf("n%d", i), 20*time.Second); resp["success"] != true {
			t.Fatalf("create %d failed: %v", i, resp)
		}
	}
	close(stop)
	<-done
	// The lister ran throughout and got answers for at least some queries.
	lister.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "command") == "gw_list_sessions"
	}, testutil.DefaultTimeout)
	lister.Drain(50 * time.Millisecond)
}

// TestDialogNotSentToNonUIClient checks that a dialog nobody can answer is not
// broadcast: a client without the ui capability must never see it.
func TestDialogNotSentToNonUIClient(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no-ui.jsonl")
	t.Setenv("FAKEPI_UI_REQUEST", "1")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.Client.Capabilities = []string{protocol.CapObserve, protocol.CapPrompt}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	c.Send(prompt("p1", "ask me"))
	if resp := c.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	for _, f := range c.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "agent_settled"
	}, testutil.DefaultTimeout) {
		if f["type"] == "extension_ui_request" {
			t.Fatalf("a client without the ui capability received a dialog: %v", f)
		}
	}
}

// TestDialogAnswerKeepsPiRequestID proves the answer reaches pi with the id pi
// is waiting for. Real pi writes no response for extension_ui_response, so the
// fake records the frame it received in FAKEPI_UI_RESPONSE_FILE rather than
// inventing a response frame.
func TestDialogAnswerKeepsPiRequestID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ui-id.jsonl")
	received := filepath.Join(dir, "ui-responses.jsonl")
	t.Setenv("FAKEPI_UI_REQUEST", "1")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "20")
	t.Setenv("FAKEPI_UI_RESPONSE_FILE", received)
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	c, _, _ := dialSession(t, addr, path)
	c.Send(prompt("p1", "ask me"))
	if resp := c.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	req := c.WaitFor(func(f map[string]any) bool { return f["type"] == "extension_ui_request" }, testutil.DefaultTimeout)
	reqID := testutil.Str(req, "id")
	if reqID == "" {
		t.Fatalf("dialog has no id: %v", req)
	}
	c.Send(map[string]any{"type": "extension_ui_response", "id": reqID, "confirmed": true})
	deadline := time.Now().Add(testutil.DefaultTimeout)
	for {
		data, _ := os.ReadFile(received)
		if strings.Contains(string(data), `"id":"`+reqID+`"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pi never received the dialog answer (recorded %q)", data)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The pending dialog is consumed: a repeat answer from the owner is ui_stale.
	c.Send(map[string]any{"type": "extension_ui_response", "id": reqID, "confirmed": true})
	if resp := c.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "command") == "extension_ui_response"
	}, testutil.DefaultTimeout); resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUIStale {
		t.Fatalf("repeat dialog answer must be ui_stale: %v", resp)
	}
	c.WaitType("agent_settled", testutil.DefaultTimeout)
}

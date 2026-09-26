package daemon_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
	"github.com/tigersoldier/pi-gateway/protocol"
)

const testToken = "test-token-0123456789"

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.Cleanup()
	os.Exit(code)
}

// startDaemon runs a daemon with fake pi and returns its address and the fake
// pi binary path (used to count live pi processes).
func startDaemon(t *testing.T, idle, shortGrace time.Duration) (string, string) {
	t.Helper()
	return startDaemonOpts(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = idle, shortGrace
	})
}

// startDaemonOpts starts a daemon and lets the test adjust its configuration.
func startDaemonOpts(t *testing.T, mutate func(*daemon.Config)) (string, string) {
	t.Helper()
	return gwtest.StartDaemon(t, testToken, mutate)
}

func dial(t *testing.T, addr string, mutate func(*protocol.Hello)) *testutil.Conn {
	t.Helper()
	return testutil.Dial(t, addr, testToken, mutate)
}

func dialSession(t *testing.T, addr, path string) (*testutil.Conn, map[string]any, string) {
	t.Helper()
	c := dial(t, addr, func(h *protocol.Hello) { h.Session = path })
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if testutil.Str(w, "clientId") == "" {
		t.Fatalf("welcome has no clientId: %v", w)
	}
	return c, w, testutil.Str(w, "clientId")
}

func sessionPath(t *testing.T, resp map[string]any) string {
	t.Helper()
	path := testutil.Str(testutil.Obj(resp, "data"), "sessionFile")
	if path == "" {
		t.Fatalf("response has no sessionFile: %v", resp)
	}
	return path
}

func prompt(id, message string) map[string]any {
	return map[string]any{"type": "prompt", "id": id, "message": message}
}

func TestHandshakeAndAuth(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)

	bad := testutil.Dial(t, addr, "wrong-token", nil)
	rejected := bad.WaitType("gw_error", testutil.DefaultTimeout)
	if testutil.Str(rejected, "code") != protocol.CodeUnauthorized {
		t.Fatalf("wrong token: %v", rejected)
	}

	c := dial(t, addr, nil)
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["session"] != nil {
		t.Fatalf("unbound session must be null: %v", w["session"])
	}
	if got := testutil.Str(w, "concurrency"); got != "queue" {
		t.Fatalf("concurrency = %q", got)
	}
	if got := testutil.Str(w, "piVersion"); got != "fakepi 0.1.0" {
		t.Fatalf("piVersion = %q", got)
	}
	if len(testutil.Arr(w, "granted")) == 0 {
		t.Fatalf("no capabilities granted: %v", w)
	}
	if got := testutil.Str(testutil.Obj(w, "turn"), "state"); got != "idle" {
		t.Fatalf("turn state = %q", got)
	}
}

func TestFreshSessionCreatesOnFirstCommand(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	c := dial(t, addr, nil)
	if w := c.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] != nil {
		t.Fatalf("expected unbound welcome: %v", w)
	}

	c.Send(map[string]any{"type": "get_state", "id": "req-1"})
	file := sessionPath(t, c.WaitResponse("req-1", testutil.DefaultTimeout))
	t.Cleanup(func() { _ = os.Remove(file) })

	c.Send(prompt("req-2", "hello"))
	if resp := c.WaitResponse("req-2", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}

	frames := c.CollectUntil(func(f map[string]any) bool { return f["type"] == "agent_settled" }, 5*time.Second)
	var lastSeq float64
	for _, f := range frames {
		seq := testutil.Num(f, "gw_seq")
		if seq <= lastSeq {
			t.Fatalf("gw_seq not increasing: %v then %v", lastSeq, seq)
		}
		lastSeq = seq
		if got := testutil.Str(f, "gw_session"); got != file {
			t.Fatalf("gw_session = %q, want %q", got, file)
		}
	}
	if !testutil.HasMessageEnd(frames, "echo: hello") {
		t.Fatalf("turn did not complete: %v", frames)
	}
}

func TestTwoClientsQueueFIFOAndFanOut(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "shared.jsonl")

	a, _, aID := dialSession(t, addr, path)
	b, _, bID := dialSession(t, addr, path)

	a.Send(prompt("a1", "first"))
	if resp := a.WaitResponse("a1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("first prompt rejected: %v", resp)
	}
	b.Send(prompt("b1", "second"))

	// b has consumed nothing yet, so its frame window covers the whole
	// sequence: turn 1 running, the queued response, the queue event, and the
	// drained second turn.
	windowB := b.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "message_end" && testutil.MessageEndText(f) == "echo: second"
	}, 5*time.Second)

	var texts []string
	var secondSeq float64
	var queuedResponse bool
	var queueAuthor string
	for _, f := range windowB {
		switch f["type"] {
		case "message_end":
			texts = append(texts, testutil.MessageEndText(f))
			if testutil.MessageEndText(f) == "echo: second" {
				secondSeq = testutil.Num(f, "gw_seq")
			}
		case "response":
			if f["id"] == "b1" && testutil.Obj(f, "data")["queued"] == true {
				queuedResponse = true
			}
		case "gw_queue":
			if pending := testutil.Arr(f, "pending"); len(pending) > 0 {
				entry := pending[0].(map[string]any)
				if testutil.Str(entry, "preview") == "second" {
					queueAuthor = testutil.Str(testutil.Obj(entry, "author"), "clientId")
				}
			}
		}
	}
	if len(texts) != 2 || texts[0] != "echo: first" || texts[1] != "echo: second" {
		t.Fatalf("turn order: %v", texts)
	}
	if !queuedResponse {
		t.Fatal("the second prompt must be queued while the first turn runs")
	}
	if queueAuthor != bID {
		t.Fatalf("queue author = %q, want %q", queueAuthor, bID)
	}
	if !hasTurn(windowB, "running", aID) || !hasTurn(windowB, "running", bID) {
		t.Fatalf("missing gw_turn running for both authors:\n%s", dumpFrames(windowB))
	}

	// The other client sees the same ordered stream, including the queued turn.
	windowA := a.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "message_end" && testutil.MessageEndText(f) == "echo: second"
	}, 5*time.Second)
	var textsA []string
	for _, f := range windowA {
		if f["type"] == "message_end" {
			textsA = append(textsA, testutil.MessageEndText(f))
			if testutil.MessageEndText(f) == "echo: second" && testutil.Num(f, "gw_seq") != secondSeq {
				t.Fatalf("subscribers disagree on the settling seq: %v vs %v", testutil.Num(f, "gw_seq"), secondSeq)
			}
		}
	}
	if len(textsA) != 2 || textsA[0] != "echo: first" || textsA[1] != "echo: second" {
		t.Fatalf("fan-out order on the first client: %v", textsA)
	}
}

func TestSessionSurvivesClientDeath(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "survives.jsonl")

	a, _, _ := dialSession(t, addr, path)
	a.Send(prompt("a1", "keep running"))
	if resp := a.WaitResponse("a1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}
	a.Close() // ssh drop mid-turn

	b, w, _ := dialSession(t, addr, path)
	if got := testutil.Str(testutil.Obj(w, "session"), "path"); got != path {
		t.Fatalf("session.path = %q, want %q", got, path)
	}
	// The turn continues on the daemon after the client died; poll until pi has
	// recorded it instead of sleeping a fixed duration.
	waitFor(t, 5*time.Second, "the turn to finish server-side", func() bool {
		b.Send(map[string]any{"type": "get_state", "id": "poll"})
		resp := b.WaitResponse("poll", testutil.DefaultTimeout)
		return testutil.Num(testutil.Obj(resp, "data"), "messageCount") >= 1
	})
}

func TestShortGraceReapsUnusedSession(t *testing.T) {
	addr, piBin := startDaemon(t, time.Minute, 300*time.Millisecond)

	c := dial(t, addr, nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	c.Send(map[string]any{"type": "get_state", "id": "r1"})
	unused := sessionPath(t, c.WaitResponse("r1", testutil.DefaultTimeout))
	t.Cleanup(func() { _ = os.Remove(unused) })

	// Switching away leaves the never-messaged session with no clients.
	other := filepath.Join(t.TempDir(), "other.jsonl")
	c.Send(map[string]any{"type": "switch_session", "id": "r2", "sessionPath": other})
	if resp := c.WaitResponse("r2", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("switch failed: %v", resp)
	}

	waitFor(t, 5*time.Second, "unused session to be reaped", func() bool {
		return testutil.CountProcesses(piBin) == 1
	})

	// Re-attaching respawns pi from the file.
	d, w, _ := dialSession(t, addr, unused)
	if got := testutil.Str(testutil.Obj(w, "session"), "path"); got != unused {
		t.Fatalf("re-attach path = %q", got)
	}
	d.Send(map[string]any{"type": "get_state", "id": "r3"})
	if resp := d.WaitResponse("r3", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("re-attach get_state failed: %v", resp)
	}
	waitFor(t, 5*time.Second, "respawned pi", func() bool {
		return testutil.CountProcesses(piBin) == 2
	})
}

func TestHibernationAfterIdleTimeout(t *testing.T) {
	addr, piBin := startDaemon(t, 400*time.Millisecond, time.Minute)
	path := filepath.Join(t.TempDir(), "idle.jsonl")

	a, _, _ := dialSession(t, addr, path)
	a.Send(prompt("a1", "message"))
	a.CollectUntil(func(f map[string]any) bool { return f["type"] == "agent_settled" }, 5*time.Second)
	a.Close()

	waitFor(t, 5*time.Second, "session to hibernate", func() bool {
		return testutil.CountProcesses(piBin) == 0
	})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("session file must remain after hibernation: %v", err)
	}

	// A later attach reloads the file with a fresh pi process.
	b, _, _ := dialSession(t, addr, path)
	b.Send(map[string]any{"type": "get_state", "id": "r1"})
	if resp := b.WaitResponse("r1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("wake-up get_state failed: %v", resp)
	}
}

func TestReplayOnResume(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "replay.jsonl")

	a, _, _ := dialSession(t, addr, path)
	a.Send(prompt("a1", "replay me"))
	a.CollectUntil(func(f map[string]any) bool { return f["type"] == "agent_settled" }, 5*time.Second)
	a.Close()
	time.Sleep(100 * time.Millisecond)

	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.LiveOnly = false
		h.Resume = &protocol.Resume{SinceSeq: 0}
	})
	b.WaitType("gw_welcome", testutil.DefaultTimeout)

	var seqs []float64
	for {
		f := b.WaitFor(func(f map[string]any) bool {
			return testutil.Num(f, "gw_seq") > 0 || f["type"] == "gw_replay_done"
		}, testutil.DefaultTimeout)
		if f["type"] == "gw_replay_done" {
			break
		}
		seqs = append(seqs, testutil.Num(f, "gw_seq"))
	}
	if len(seqs) == 0 {
		t.Fatal("no records replayed")
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("replay out of order: %v", seqs)
		}
	}
}

func TestSpawnConflictAndRuntimeParams(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "params.jsonl")

	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--approve", "--model", "m1"}
	})
	a.WaitType("gw_welcome", testutil.DefaultTimeout)

	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--approve", "--model", "m2"}
	})
	b.WaitType("gw_welcome", testutil.DefaultTimeout)

	changed := a.WaitFor(func(f map[string]any) bool {
		return f["type"] == "gw_state_changed" && testutil.Str(f, "command") == "set_model"
	}, testutil.DefaultTimeout)
	if testutil.Str(testutil.Obj(changed, "data"), "id") != "m2" {
		t.Fatalf("state change did not carry the new model: %v", changed)
	}

	b.Send(map[string]any{"type": "get_state", "id": "r1"})
	data := testutil.Obj(b.WaitResponse("r1", testutil.DefaultTimeout), "data")
	if got := testutil.Str(testutil.Obj(data, "model"), "id"); got != "m2" {
		t.Fatalf("runtime model not applied: %q", got)
	}

	// A conflicting spawn-only parameter fails the attach.
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--no-approve"}
	})
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["session"] != nil {
		t.Fatalf("conflicting attach must not bind: %v", w["session"])
	}
	errFrame := c.WaitType("gw_error", testutil.DefaultTimeout)
	if testutil.Str(errFrame, "code") != protocol.CodeSpawnParamConflict {
		t.Fatalf("error = %v", errFrame)
	}

	// The same conflict is reported for an explicit switch_session.
	c.Send(map[string]any{"type": "switch_session", "id": "r2", "sessionPath": path})
	resp := c.WaitResponse("r2", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeSpawnParamConflict {
		t.Fatalf("switch_session conflict: %v", resp)
	}
}

func TestNewSessionRebindsOnlyRequester(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "keep.jsonl")

	a, _, _ := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)

	a.Send(map[string]any{"type": "new_session", "id": "n1"})
	if resp := a.WaitResponse("n1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("new_session failed: %v", resp)
	}
	a.Send(map[string]any{"type": "get_state", "id": "r1"})
	aFile := sessionPath(t, a.WaitResponse("r1", testutil.DefaultTimeout))
	if aFile == path {
		t.Fatalf("requester was not rebound: %q", aFile)
	}

	b.Send(map[string]any{"type": "get_state", "id": "r2"})
	if bFile := sessionPath(t, b.WaitResponse("r2", testutil.DefaultTimeout)); bFile != path {
		t.Fatalf("bystander moved to %q, want %q", bFile, path)
	}
	for _, f := range b.Drain(200 * time.Millisecond) {
		if f["type"] == "gw_state_changed" {
			t.Fatalf("bystander saw a state change: %v", f)
		}
	}
}

func TestSwitchSessionUnknownNameAndUnsupportedCommands(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	c := dial(t, addr, nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	c.Send(map[string]any{"type": "switch_session", "id": "r1", "sessionPath": "auth-refactor"})
	resp := c.WaitResponse("r1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUnknownSession {
		t.Fatalf("unknown name must fail with unknown_session: %v", resp)
	}

	// fork/clone need a bound session before the actor can judge sharing.
	c.Send(map[string]any{"type": "clone", "id": "r2"})
	resp = c.WaitResponse("r2", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUnknownSession {
		t.Fatalf("clone without a session must fail: %v", resp)
	}

	// A gateway message from the removed draft is still rejected politely.
	c.Send(map[string]any{"type": "gw_take_turn", "id": "r3"})
	if f := c.WaitType("gw_error", testutil.DefaultTimeout); testutil.Str(f, "code") != protocol.CodeNotSupported {
		t.Fatalf("gw_take_turn must be not_supported: %v", f)
	}
}

func dumpFrames(frames []map[string]any) string {
	var b strings.Builder
	for _, f := range frames {
		b.WriteString(f["type"].(string))
		b.WriteString(" ")
		for k, v := range f {
			if k == "type" {
				continue
			}
			fmt.Fprintf(&b, "%s=%v ", k, v)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func waitFor(t *testing.T, timeout time.Duration, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func hasTurn(frames []map[string]any, state, author string) bool {
	for _, f := range frames {
		if f["type"] != "gw_turn" || testutil.Str(f, "state") != state {
			continue
		}
		if author == "" || testutil.Str(testutil.Obj(f, "author"), "clientId") == author {
			return true
		}
	}
	return false
}

// TestReplayDuringRunningTurnIsOrdered attaches mid-turn with a resume cursor
// and asserts that the replay window precedes live records with no duplicates
// and strictly increasing gw_seq (design §5.2's replay watermark).
func TestReplayDuringRunningTurnIsOrdered(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "live-replay.jsonl")

	a, _, _ := dialSession(t, addr, path)
	a.Send(prompt("a1", "live"))
	if resp := a.WaitResponse("a1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt rejected: %v", resp)
	}

	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.LiveOnly = false
		h.Resume = &protocol.Resume{SinceSeq: 0}
	})
	w := b.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["resyncRequired"] == true {
		t.Fatalf("ring cursor 0 should be replayable for this test: %v", w)
	}

	var (
		seqs    []float64
		seen    = map[float64]bool{}
		sawDone bool
	)
	deadline := time.After(10 * time.Second)
	for {
		var frame map[string]any
		select {
		case f, ok := <-b.Frames():
			if !ok {
				t.Fatal("connection closed while collecting replay/live frames")
			}
			frame = f
		case <-deadline:
			t.Fatalf("timed out waiting for the turn to settle (seqs=%v)", seqs)
		}
		if frame["type"] == "gw_replay_done" {
			sawDone = true
			continue
		}
		if seq := testutil.Num(frame, "gw_seq"); seq > 0 {
			if seen[seq] {
				t.Fatalf("duplicate gw_seq %v (replay and live overlapped)", seq)
			}
			seen[seq] = true
			if len(seqs) > 0 && seq <= seqs[len(seqs)-1] {
				t.Fatalf("gw_seq not increasing: %v then %v", seqs[len(seqs)-1], seq)
			}
			seqs = append(seqs, seq)
		}
		if frame["type"] == "agent_settled" {
			break
		}
	}
	if !sawDone {
		t.Fatal("no gw_replay_done frame")
	}
	if len(seqs) < 3 {
		t.Fatalf("expected replayed records before the live turn end, got %v", seqs)
	}
}

// TestClientStateChangeNotifiesAllClients covers protocol §4.4 for
// client-initiated shared-state mutations.
func TestClientStateChangeNotifiesAllClients(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "state.jsonl")

	a, _, aID := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)

	a.Send(map[string]any{"type": "set_model", "id": "m1", "modelId": "m9", "provider": "p9"})
	if resp := a.WaitResponse("m1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("set_model failed: %v", resp)
	}
	for name, c := range map[string]*testutil.Conn{"author": a, "bystander": b} {
		f := c.WaitFor(func(f map[string]any) bool {
			return f["type"] == "gw_state_changed" && testutil.Str(f, "command") == "set_model"
		}, testutil.DefaultTimeout)
		if got := testutil.Str(testutil.Obj(f, "data"), "id"); got != "m9" {
			t.Fatalf("%s: state change data = %v", name, f)
		}
		if got := testutil.Str(testutil.Obj(f, "by"), "clientId"); got != aID {
			t.Fatalf("%s: state change author = %q, want %q", name, got, aID)
		}
	}
}

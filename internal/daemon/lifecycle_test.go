package daemon_test

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// startLifecycle starts a daemon with a long idle timeout, so nothing is
// reaped underneath a test, and returns it with its address and fake pi path.
func startLifecycle(t *testing.T, mutate func(*daemon.Config)) (*daemon.Daemon, string, string) {
	t.Helper()
	d, piBin := gwtest.StartDaemonHandle(t, testToken, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 60*time.Second, 30*time.Second
		if mutate != nil {
			mutate(c)
		}
	})
	return d, d.Addr().String(), piBin
}

// beginTurn sends a prompt and returns once the turn is streaming.
func beginTurn(t *testing.T, c *testutil.Conn, id, message string) {
	t.Helper()
	c.Send(prompt(id, message))
	if resp := c.WaitResponse(id, testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt %s rejected: %v", id, resp)
	}
	c.WaitFor(typeIs("message_update"), testutil.DefaultTimeout)
}

// settleTurn waits until the actor's turn latch is idle, so a following stop
// cannot be refused as busy.
func settleTurn(t *testing.T, c *testutil.Conn) {
	t.Helper()
	c.WaitFor(func(f map[string]any) bool {
		return f["type"] == "gw_turn" && testutil.Str(f, "state") == "settled"
	}, testutil.DefaultTimeout)
}

// stopGate sends one stop/delete command and returns the response together
// with every frame that preceded it, so a test can assert what the daemon
// published before answering.
func stopGate(t *testing.T, c *testutil.Conn, command, id string, force bool, target string) (map[string]any, []map[string]any) {
	t.Helper()
	frame := map[string]any{"type": command, "id": id, "force": force}
	if target != "" {
		frame["session"] = target
	}
	c.Send(frame)
	frames := c.CollectUntil(response(id), testutil.DefaultTimeout)
	return frames[len(frames)-1], frames
}

// responseData decodes a response's data object.
func responseData(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	data := testutil.Obj(resp, "data")
	if data == nil {
		t.Fatalf("response has no data object: %v", resp)
	}
	return data
}

// assertFrame requires one frame of typ whose fields match want.
func assertFrame(t *testing.T, frames []map[string]any, typ string, want map[string]any) {
	t.Helper()
	for _, f := range frames {
		if f["type"] != typ {
			continue
		}
		match := true
		for k, v := range want {
			if f[k] != v {
				match = false
				break
			}
		}
		if match {
			return
		}
	}
	t.Fatalf("no %s frame matching %v in %d frames", typ, want, len(frames))
}

// waitPiCount waits until exactly want fake-pi processes are running.
func waitPiCount(t *testing.T, piBin string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.CountProcesses(piBin) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("fake pi process count = %d, want %d", testutil.CountProcesses(piBin), want)
}

// countState counts gw_session_state frames in one state.
func countState(frames []map[string]any, state string) int {
	n := 0
	for _, f := range frames {
		if f["type"] == "gw_session_state" && testutil.Str(f, "state") == state {
			n++
		}
	}
	return n
}

// TestStopDeleteRequireAdmin is the capability check: an operator token must
// be refused before dispatch, so nothing is stopped, nothing is deleted, and
// no session is lazily created.
func TestStopDeleteRequireAdmin(t *testing.T) {
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) {
		c.CatalogRoots = []string{dir}
		c.Tokens = []protocol.TokenGrant{{
			Name: "operator", Token: "op-token",
			Capabilities: []string{protocol.CapObserve, protocol.CapInterject, protocol.CapPrompt, protocol.CapUI},
		}}
	})
	path := filepath.Join(dir, "guarded.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)
	before := testutil.CountProcesses(piBin)

	op := dialWithToken(t, addr, "op-token")
	for _, command := range []string{"gw_stop_session", "gw_delete_session"} {
		resp, _ := stopGate(t, op, command, "req-"+command, false, path)
		if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeForbidden {
			t.Fatalf("%s as operator = %v, want forbidden", command, resp)
		}
	}
	if got := testutil.CountProcesses(piBin); got != before {
		t.Fatalf("pi processes = %d, want %d (nothing may be stopped)", got, before)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("session file was touched: %v", err)
	}
	rows := listSessions(t, op, "l1")
	if len(rows) != 1 || testutil.Str(rows[0], "path") != path {
		t.Fatalf("sessions = %v, want exactly the guarded session (no lazy create)", rows)
	}
}

// TestStopSessionKeepsFileAndRespawns covers gw_stop_session: pi exits, the
// file stays, the catalog reports a non-live session, and re-attaching
// respawns pi from that file.
func TestStopSessionKeepsFileAndRespawns(t *testing.T) {
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "stop.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)

	resp, frames := stopGate(t, a, "gw_stop_session", "s1", false, "")
	if resp["success"] != true {
		t.Fatalf("stop failed: %v", resp)
	}
	data := responseData(t, resp)
	if data["piStopped"] != true || testutil.Num(data, "detachedClients") != 0 {
		t.Fatalf("stop data = %v", data)
	}
	if _, ok := data["fileDeleted"]; ok {
		t.Fatalf("stop must not report fileDeleted: %v", data)
	}
	assertFrame(t, frames, "gw_session_state", map[string]any{"state": "stopped", "reason": "requested"})
	waitPiCount(t, piBin, 0)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stop must keep the session file: %v", err)
	}

	// The requester's own connection survived and is usable while unbound.
	row := findRow(t, listSessions(t, a, "l1"), path)
	if row["live"] != false {
		t.Fatalf("stopped session still reports live: %v", row)
	}

	// Re-attaching respawns pi for the same file.
	b, w, _ := dialSession(t, addr, path)
	if got := testutil.Str(testutil.Obj(w, "session"), "path"); got != path {
		t.Fatalf("re-attach bound %q, want %q", got, path)
	}
	b.Send(map[string]any{"type": "get_state", "id": "g1"})
	if resp := b.WaitResponse("g1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("get_state after re-attach: %v", resp)
	}
	waitPiCount(t, piBin, 1)
}

// TestDeleteSessionRemovesFileAndTargets covers gw_delete_session on a live
// session: the file is gone, the catalog drops it, and neither a path nor a
// name can reach it again.
func TestDeleteSessionRemovesFileAndTargets(t *testing.T) {
	dir := t.TempDir()
	d, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "gone.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)
	a.Send(map[string]any{"type": "set_session_name", "id": "n1", "name": "gone-name"})
	if resp := a.WaitResponse("n1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("set_session_name: %v", resp)
	}

	resp, frames := stopGate(t, a, "gw_delete_session", "d1", false, "")
	if resp["success"] != true {
		t.Fatalf("delete failed: %v", resp)
	}
	data := responseData(t, resp)
	if data["piStopped"] != true || data["fileDeleted"] != true || testutil.Num(data, "detachedClients") != 0 {
		t.Fatalf("delete data = %v", data)
	}
	assertFrame(t, frames, "gw_session_state", map[string]any{"state": "deleted", "reason": "requested"})
	waitPiCount(t, piBin, 0)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("session file still exists (err=%v)", err)
	}
	if got := d.Status().Registered; got != 0 {
		t.Fatalf("registered = %d after delete, want 0", got)
	}
	if rows := listSessions(t, a, "l1"); len(rows) != 0 {
		t.Fatalf("catalog still lists a deleted session: %v", rows)
	}

	// A fresh connection cannot resurrect it by path or by name.
	fresh := dial(t, addr, nil)
	fresh.WaitType("gw_welcome", testutil.DefaultTimeout)
	for i, target := range []string{path, "gone-name"} {
		id := fmt.Sprintf("sw%d", i)
		fresh.Send(map[string]any{"type": "switch_session", "id": id, "sessionPath": target})
		resp := fresh.WaitResponse(id, testutil.DefaultTimeout)
		if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUnknownSession {
			t.Fatalf("switch to %q after delete = %v, want unknown_session", target, resp)
		}
	}
	// Repeating the delete is unknown_session, not a harmless second success.
	again, _ := stopGate(t, fresh, "gw_delete_session", "d2", false, path)
	if again["success"] != false || testutil.Str(again, "code") != protocol.CodeUnknownSession {
		t.Fatalf("repeat delete = %v, want unknown_session", again)
	}
}

// TestDeleteHibernatedSessionByName covers a delete of a session with no live
// pi: the name resolves from the file scan, the file goes, and the emptied
// encoded-cwd directory goes with it.
func TestDeleteHibernatedSessionByName(t *testing.T) {
	root := t.TempDir()
	path := writeCatalogSession(t, root, "hiber", "hiber-name", root, 2)
	_, addr, _ := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{root} })
	c := dial(t, addr, nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	resp, _ := stopGate(t, c, "gw_delete_session", "d1", false, "hiber-name")
	if resp["success"] != true {
		t.Fatalf("delete of a hibernated session failed: %v", resp)
	}
	data := responseData(t, resp)
	if data["piStopped"] != false || data["fileDeleted"] != true {
		t.Fatalf("hibernated delete data = %v", data)
	}
	if got := testutil.Str(data, "path"); got != path {
		t.Fatalf("path = %q, want %q", got, path)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file not deleted: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("empty encoded-cwd directory not removed: %v", err)
	}
	again, _ := stopGate(t, c, "gw_delete_session", "d2", false, "hiber-name")
	if again["success"] != false || testutil.Str(again, "code") != protocol.CodeUnknownSession {
		t.Fatalf("second delete by name = %v, want unknown_session", again)
	}
}

// TestDeleteSessionWithoutFile covers a session pi never flushed
// (--no-session): deleting it succeeds and reports fileDeleted:false.
func TestDeleteSessionWithoutFile(t *testing.T) {
	_, addr, _ := startLifecycle(t, nil)
	c := dial(t, addr, func(h *protocol.Hello) { h.PiArgs = []string{"--no-session"} })
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	c.Send(map[string]any{"type": "get_state", "id": "g1"})
	resp := c.WaitResponse("g1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("get_state: %v", resp)
	}
	if file := testutil.Str(testutil.Obj(resp, "data"), "sessionFile"); file != "" {
		t.Fatalf("--no-session produced a file: %q", file)
	}
	del, _ := stopGate(t, c, "gw_delete_session", "d1", false, "")
	if del["success"] != true {
		t.Fatalf("delete without a file failed: %v", del)
	}
	data := responseData(t, del)
	if data["fileDeleted"] != false || data["piStopped"] != true {
		t.Fatalf("delete data = %v", data)
	}
}

// TestStopDeleteRefuseBusyAndAttached covers the two refusals: a running turn
// without force, and a stop with another client attached without force.
func TestStopDeleteRefuseBusyAndAttached(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "80")
	t.Setenv("FAKEPI_TURN_EVENTS", "12")
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "busy.jsonl")
	a, _, _ := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "slow turn")

	for _, command := range []string{"gw_stop_session", "gw_delete_session"} {
		resp, _ := stopGate(t, a, command, "r-"+command, false, "")
		if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeSessionBusy {
			t.Fatalf("%s during a turn = %v, want session_busy", command, resp)
		}
	}
	// The refusals changed nothing: the turn continues and settles.
	settleTurn(t, a)
	if got := testutil.CountProcesses(piBin); got != 1 {
		t.Fatalf("pi processes = %d, want 1 after a refused stop", got)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("refused delete touched the file: %v", err)
	}

	// Stopping pi under an attached client needs force.
	resp, _ := stopGate(t, a, "gw_stop_session", "s1", false, "")
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeSessionAttached {
		t.Fatalf("stop with another client = %v, want session_attached", resp)
	}
	if got := testutil.CountProcesses(piBin); got != 1 {
		t.Fatalf("pi processes = %d, want 1 after a refused stop", got)
	}

	// Deleting is never blocked by attached clients: they are notified and
	// unbound instead, and the bystander's connection stays usable.
	del, _ := stopGate(t, a, "gw_delete_session", "d1", true, "")
	if del["success"] != true {
		t.Fatalf("forced delete = %v", del)
	}
	if got := testutil.Num(responseData(t, del), "detachedClients"); got != 1 {
		t.Fatalf("detachedClients = %v, want 1 (the bystander)", got)
	}
	b.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "gw_session_state" && testutil.Str(f, "state") == "deleted"
	}, testutil.DefaultTimeout)
	b.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	if resp := b.WaitResponse("l1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("bystander connection unusable after delete: %v", resp)
	}
	waitPiCount(t, piBin, 0)
}

// TestForcedDeleteAbortsRunningTurn covers the force path where pi honors the
// abort: the turn unwinds, the abort is observable, and the delete does not
// wait for the full turn.
func TestForcedDeleteAbortsRunningTurn(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "abort.marker")
	t.Setenv("FAKEPI_ABORT_MARKER", marker)
	t.Setenv("FAKEPI_TURN_DELAY_MS", "100")
	t.Setenv("FAKEPI_TURN_EVENTS", "40")
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "abort.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "long turn")

	start := time.Now()
	resp, frames := stopGate(t, a, "gw_delete_session", "d1", true, "")
	elapsed := time.Since(start)
	if resp["success"] != true {
		t.Fatalf("forced delete failed: %v", resp)
	}
	if elapsed > 4*time.Second {
		t.Fatalf("forced delete took %v; the abort should have settled the turn", elapsed)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("abort was not observed by pi: %v", err)
	}
	assertFrame(t, frames, "gw_session_state", map[string]any{"state": "deleted", "reason": "forced"})
	waitPiCount(t, piBin, 0)
}

// TestForcedStopGraceBoundsAnUnresponsiveTurn covers the force path where pi
// ignores the abort: the stop happens at the grace, not never.
func TestForcedStopGraceBoundsAnUnresponsiveTurn(t *testing.T) {
	t.Setenv("FAKEPI_ABORT_IGNORE", "1")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "250")
	t.Setenv("FAKEPI_TURN_EVENTS", "40") // ~10s if never stopped
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) {
		c.CatalogRoots = []string{dir}
		c.StopGrace = 250 * time.Millisecond
	})
	path := filepath.Join(dir, "sticky.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "unresponsive")

	start := time.Now()
	resp, frames := stopGate(t, a, "gw_stop_session", "s1", true, "")
	elapsed := time.Since(start)
	if resp["success"] != true {
		t.Fatalf("forced stop failed: %v", resp)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("forced stop took %v; the grace did not bound it", elapsed)
	}
	assertFrame(t, frames, "gw_session_state", map[string]any{"state": "stopped", "reason": "forced"})
	waitPiCount(t, piBin, 0)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stop must keep the file: %v", err)
	}
}

// TestDeleteWaitsForPiReap is the resurrection guard: pi appends one more
// entry while shutting down, so a file removed before the reap would come
// back. The delete must run after pi is gone.
func TestDeleteWaitsForPiReap(t *testing.T) {
	t.Setenv("FAKEPI_FLUSH_ON_EXIT", "1")
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "reap.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)

	resp, _ := stopGate(t, a, "gw_delete_session", "d1", false, "")
	if resp["success"] != true || responseData(t, resp)["fileDeleted"] != true {
		t.Fatalf("delete = %v", resp)
	}
	waitPiCount(t, piBin, 0)
	time.Sleep(150 * time.Millisecond)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("pi's shutdown flush resurrected the deleted session: %v", err)
	}
}

// TestDeleteUnbindsEveryClient covers notification: every attached client
// receives the terminal event exactly once, ends up unbound, and keeps a
// usable connection.
func TestDeleteUnbindsEveryClient(t *testing.T) {
	dir := t.TempDir()
	_, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "two.jsonl")
	a, _, _ := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)

	a.Send(map[string]any{"type": "gw_delete_session", "id": "d1"})
	framesA := a.CollectUntil(response("d1"), testutil.DefaultTimeout)
	if respA := framesA[len(framesA)-1]; respA["success"] != true {
		t.Fatalf("delete failed: %v", respA)
	}
	if got := countState(framesA, "deleted"); got != 1 {
		t.Fatalf("requester saw %d deleted events, want 1", got)
	}
	framesB := b.CollectUntil(func(f map[string]any) bool {
		return f["type"] == "gw_session_state" && testutil.Str(f, "state") == "deleted"
	}, testutil.DefaultTimeout)
	if got := countState(framesB, "deleted"); got != 1 {
		t.Fatalf("bystander saw %d deleted events, want 1", got)
	}
	if extra := a.Drain(250 * time.Millisecond); countState(extra, "deleted") != 0 {
		t.Fatalf("requester got a duplicate deleted event: %v", extra)
	}
	if extra := b.Drain(250 * time.Millisecond); countState(extra, "deleted") != 0 {
		t.Fatalf("bystander got a duplicate deleted event: %v", extra)
	}
	for i, c := range []*testutil.Conn{a, b} {
		id := fmt.Sprintf("l%d", i)
		c.Send(map[string]any{"type": "gw_list_sessions", "id": id})
		if resp := c.WaitResponse(id, testutil.DefaultTimeout); resp["success"] != true {
			t.Fatalf("connection %d unusable after delete: %v", i, resp)
		}
	}
	waitPiCount(t, piBin, 0)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file still present: %v", err)
	}
}

// TestDeleteDiscardsQueuedPrompts covers the queue: a queued prompt from
// another client is discarded, the empty queue is announced before the
// terminal event, and no turn starts afterwards.
func TestDeleteDiscardsQueuedPrompts(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "100")
	t.Setenv("FAKEPI_TURN_EVENTS", "20")
	dir := t.TempDir()
	d, addr, _ := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "queue.jsonl")
	a, _, _ := dialSession(t, addr, path)
	b, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "slow turn")

	b.Send(map[string]any{"type": "follow_up", "id": "f1", "message": "must be discarded"})
	resp := b.WaitResponse("f1", testutil.DefaultTimeout)
	if resp["success"] != true || testutil.Obj(resp, "data")["queued"] != true {
		t.Fatalf("follow_up was not queued: %v", resp)
	}
	turnsBefore := d.Metrics().Value(metrics.TurnsStarted)

	a.Send(map[string]any{"type": "gw_delete_session", "id": "d1", "force": true})
	frames := a.CollectUntil(response("d1"), testutil.DefaultTimeout)
	if last := frames[len(frames)-1]; last["success"] != true {
		t.Fatalf("forced delete failed: %v", last)
	}
	queueIdx, deletedIdx := -1, -1
	for i, f := range frames {
		if f["type"] == "gw_queue" && len(testutil.Arr(f, "pending")) == 0 && queueIdx < 0 {
			queueIdx = i
		}
		if f["type"] == "gw_session_state" && testutil.Str(f, "state") == "deleted" && deletedIdx < 0 {
			deletedIdx = i
		}
	}
	if queueIdx < 0 {
		t.Fatalf("no empty gw_queue before the delete response")
	}
	if deletedIdx < 0 || queueIdx > deletedIdx {
		t.Fatalf("the empty gw_queue must precede the deleted event (queue=%d deleted=%d)", queueIdx, deletedIdx)
	}
	time.Sleep(400 * time.Millisecond)
	if got := d.Metrics().Value(metrics.TurnsStarted); got != turnsBefore {
		t.Fatalf("turns started = %d, want %d: a discarded prompt ran", got, turnsBefore)
	}
	if got := d.Status().Registered; got != 0 {
		t.Fatalf("registered = %d, want 0", got)
	}
}

// attachWatch is a goroutine-safe hello whose outcome is either the attach
// refusal code or the deleted event of a session it bound to.
type attachWatch struct {
	ready chan struct{}
	done  chan string
	err   chan error
	once  sync.Once
}

func startAttachWatch(addr, session string) *attachWatch {
	w := &attachWatch{ready: make(chan struct{}), done: make(chan string, 1), err: make(chan error, 1)}
	go func() {
		nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			w.err <- err
			return
		}
		defer nc.Close()
		codec := protocol.NewCodec(nc, nc)
		hello := protocol.Hello{
			Type: "gw_hello", Protocol: protocol.Version, Token: testToken,
			Client:   protocol.ClientInfo{Name: "race", Kind: "test", Capabilities: protocol.AllCapabilities},
			LiveOnly: true,
			Session:  session,
		}
		if err := codec.WriteJSON(&hello); err != nil {
			w.err <- err
			return
		}
		_ = nc.SetDeadline(time.Now().Add(10 * time.Second))
		for {
			raw, err := codec.Read()
			if err != nil {
				w.err <- err
				return
			}
			switch protocol.Field(raw, "type") {
			case "gw_welcome":
				var welcome protocol.Welcome
				if err := json.Unmarshal(raw, &welcome); err != nil {
					w.err <- err
					return
				}
				if welcome.Session != nil {
					w.once.Do(func() { close(w.ready) })
				}
			case "gw_error":
				w.done <- protocol.Field(raw, "code")
				return
			case "gw_session_state":
				if protocol.Field(raw, "state") == protocol.SessionStateDeleted {
					w.done <- "deleted"
					return
				}
			}
		}
	}()
	return w
}

func (w *attachWatch) result(t *testing.T, timeout time.Duration) string {
	t.Helper()
	select {
	case got := <-w.done:
		return got
	case err := <-w.err:
		t.Fatalf("attach watch failed: %v", err)
	case <-time.After(timeout):
		t.Fatal("attach watch timed out")
	}
	return ""
}

// TestConcurrentAttachAndDelete is the §3 race: an attach either completes
// first (and is then notified and unbound) or resolves after the delete
// (unknown_session). No third outcome is allowed.
func TestConcurrentAttachAndDelete(t *testing.T) {
	dir := t.TempDir()
	_, addr, _ := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "race.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)

	pre := startAttachWatch(addr, path)
	select {
	case <-pre.ready:
	case err := <-pre.err:
		t.Fatalf("pre-attach failed: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("pre-attach did not bind")
	}
	racers := make([]*attachWatch, 0, 4)
	for i := 0; i < 4; i++ {
		racers = append(racers, startAttachWatch(addr, path))
	}
	a.Send(map[string]any{"type": "gw_delete_session", "id": "d1"})
	frames := a.CollectUntil(response("d1"), testutil.DefaultTimeout)
	if last := frames[len(frames)-1]; last["success"] != true {
		t.Fatalf("delete failed: %v", last)
	}
	if got := pre.result(t, testutil.DefaultTimeout); got != "deleted" {
		t.Fatalf("a client bound before the delete ended in %q, want deleted", got)
	}
	for i, w := range racers {
		switch got := w.result(t, testutil.DefaultTimeout); got {
		case "deleted", protocol.CodeUnknownSession:
		default:
			t.Fatalf("racing attach %d ended in %q, want deleted or unknown_session", i, got)
		}
	}
}

// TestCommandsAfterDeleteDoNotCreateASession covers the deleted-unbound
// contract: a session-scoped command on a connection whose session was
// deleted is refused, not answered by lazily creating a new session.
// Attaching or creating a session explicitly still works.
func TestCommandsAfterDeleteDoNotCreateASession(t *testing.T) {
	dir := t.TempDir()
	d, addr, _ := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "norevive.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)
	resp, _ := stopGate(t, a, "gw_delete_session", "d1", false, "")
	if resp["success"] != true {
		t.Fatalf("delete failed: %v", resp)
	}

	a.Send(map[string]any{"type": "prompt", "id": "p2", "message": "hello again"})
	lazy := a.WaitResponse("p2", testutil.DefaultTimeout)
	if lazy["success"] != false || testutil.Str(lazy, "code") != protocol.CodeUnknownSession {
		t.Fatalf("prompt after delete = %v, want unknown_session", lazy)
	}
	if got := d.Status().Registered; got != 0 {
		t.Fatalf("a command after the delete created a session: registered = %d", got)
	}

	// Explicit attachment is still available on the same connection.
	other := filepath.Join(dir, "other.jsonl")
	a.Send(map[string]any{"type": "switch_session", "id": "sw1", "sessionPath": other})
	if resp := a.WaitResponse("sw1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("explicit attach after delete = %v", resp)
	}
	if got := d.Status().Registered; got != 1 {
		t.Fatalf("registered = %d after an explicit attach, want 1", got)
	}
}

// TestDeleteUpdatesStatusAndMetrics covers the operational surface: the
// registered count and the delete counter move, and the idle reaper does not
// fire for a deleted session.
func TestDeleteUpdatesStatusAndMetrics(t *testing.T) {
	dir := t.TempDir()
	d, addr, piBin := startLifecycle(t, func(c *daemon.Config) { c.CatalogRoots = []string{dir} })
	path := filepath.Join(dir, "counted.jsonl")
	a, _, _ := dialSession(t, addr, path)
	beginTurn(t, a, "p1", "hello")
	settleTurn(t, a)
	if got := d.Status().Registered; got != 1 {
		t.Fatalf("registered = %d before delete, want 1", got)
	}
	resp, _ := stopGate(t, a, "gw_delete_session", "d1", false, "")
	if resp["success"] != true {
		t.Fatalf("delete failed: %v", resp)
	}
	if got := d.Status().Registered; got != 0 {
		t.Fatalf("registered = %d after delete, want 0", got)
	}
	if got := d.Metrics().Value(metrics.SessionsDeleted); got != 1 {
		t.Fatalf("sessions_deleted_total = %d, want 1", got)
	}
	if got := d.Metrics().Value(metrics.SessionsReaped); got != 0 {
		t.Fatalf("the reaper fired for a deleted session: %d", got)
	}
	waitPiCount(t, piBin, 0)
}

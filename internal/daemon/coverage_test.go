package daemon_test

import (
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// listSessionsFiltered sends gw_list_sessions with a filter and returns its rows.
func listSessionsFiltered(t *testing.T, c *testutil.Conn, id string, filter map[string]any) []map[string]any {
	t.Helper()
	c.Send(map[string]any{"type": "gw_list_sessions", "id": id, "filter": filter})
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

// frameLog collects every frame a connection receives. testutil.Conn's waits
// consume and drop frames that do not match, so tests that need to observe
// several interleaved events must not interleave WaitFor calls with the sends
// they are measuring.
type frameLog struct {
	mu     sync.Mutex
	frames []map[string]any
}

func newFrameLog(c *testutil.Conn) *frameLog {
	l := &frameLog{}
	go func() {
		for f := range c.Frames() {
			l.mu.Lock()
			l.frames = append(l.frames, f)
			l.mu.Unlock()
		}
	}()
	return l
}

func (l *frameLog) all() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]map[string]any(nil), l.frames...)
}

func (l *frameLog) types() []string {
	var out []string
	for _, f := range l.all() {
		if typ := testutil.Str(f, "type"); typ != "" {
			out = append(out, typ)
		}
	}
	return out
}

func (l *frameLog) find(pred func(map[string]any) bool) map[string]any {
	for _, f := range l.all() {
		if pred(f) {
			return f
		}
	}
	return nil
}

func (l *frameLog) wait(t *testing.T, timeout time.Duration, what string, pred func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f := l.find(pred); f != nil {
			return f
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s; frames seen: %v", what, l.types())
	return nil
}

// response answers a request by id.
func response(id string) func(map[string]any) bool {
	return func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "id") == id
	}
}

// typeIs matches one frame type.
func typeIs(typ string) func(map[string]any) bool {
	return func(f map[string]any) bool { return f["type"] == typ }
}

// countType counts frames of one type.
func countType(frames []map[string]any, typ string) int {
	n := 0
	for _, f := range frames {
		if f["type"] == typ {
			n++
		}
	}
	return n
}

// TestPingPongAndBye covers the two lifecycle commands that need no capability:
// gw_ping answers gw_pong, and gw_bye drops the connection (without killing the
// session or any other client).
func TestPingPongAndBye(t *testing.T) {
	d, addr := startDaemonHandle(t, nil)

	keep := dial(t, addr, nil)
	defer keep.Close()
	keep.Send(map[string]any{"type": "get_state", "id": "s1"})
	keep.WaitResponse("s1", testutil.DefaultTimeout)

	bye := dial(t, addr, nil)
	bye.Send(map[string]any{"type": "gw_ping", "id": "p1"})
	if got := testutil.Str(bye.WaitType("gw_pong", testutil.DefaultTimeout), "type"); got != "gw_pong" {
		t.Fatalf("gw_ping answered with %q", got)
	}
	bye.Send(map[string]any{"type": "gw_bye", "id": "b1"})
	waitFor(t, 5*time.Second, "the connection to close after gw_bye", func() bool {
		return d.Status().Connections == 1 // only the keeper remains
	})
	if got := d.Status().Registered; got != 1 {
		t.Fatalf("registered sessions after gw_bye = %d, want 1 (bye must not kill the session)", got)
	}
}

// TestSessionControlResponseShapes pins the synthesized replies for the two
// intercepted session commands: pi-shaped success with data.cancelled false.
func TestSessionControlResponseShapes(t *testing.T) {
	_, addr := startDaemonHandle(t, nil)
	a := dial(t, addr, nil)
	defer a.Close()

	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	first := sessionPath(t, a.WaitResponse("g1", testutil.DefaultTimeout))

	a.Send(map[string]any{"type": "new_session", "id": "n1"})
	assertCancelledFalse(t, "new_session", a.WaitResponse("n1", testutil.DefaultTimeout))
	a.Send(map[string]any{"type": "get_state", "id": "g2"})
	if second := sessionPath(t, a.WaitResponse("g2", testutil.DefaultTimeout)); second == first {
		t.Fatalf("new_session did not rebind: still on %q", second)
	}

	a.Send(map[string]any{"type": "switch_session", "id": "sw1", "sessionPath": first})
	assertCancelledFalse(t, "switch_session", a.WaitResponse("sw1", testutil.DefaultTimeout))
	a.Send(map[string]any{"type": "get_state", "id": "g3"})
	if got := sessionPath(t, a.WaitResponse("g3", testutil.DefaultTimeout)); got != first {
		t.Fatalf("switch_session rebound to %q, want %q", got, first)
	}

	// The `session` alias documented for switch_session.
	a.Send(map[string]any{"type": "switch_session", "id": "sw2", "session": first})
	assertCancelledFalse(t, "switch_session", a.WaitResponse("sw2", testutil.DefaultTimeout))
}

func assertCancelledFalse(t *testing.T, command string, resp map[string]any) {
	t.Helper()
	if resp["success"] != true {
		t.Fatalf("%s failed: %v", command, resp)
	}
	if resp["command"] != command {
		t.Fatalf("%s response echoes command %v", command, resp["command"])
	}
	if got, ok := testutil.Obj(resp, "data")["cancelled"]; !ok || got != false {
		t.Fatalf("%s data = %v, want cancelled:false", command, resp["data"])
	}
}

// TestListSessionsLiveFilter covers filter.live: a file that was never live is
// listed without the filter and hidden with it.
func TestListSessionsLiveFilter(t *testing.T) {
	root := t.TempDir()
	fileOnly := writeCatalogSession(t, root, "file-only", "file-only", "/home/u/proj", 2)
	_, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.CatalogRoots = []string{root}
	})

	a := dial(t, addr, nil)
	defer a.Close()
	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	live := sessionPath(t, a.WaitResponse("g1", testutil.DefaultTimeout))

	all := listSessionsFiltered(t, a, "l1", nil)
	findRow(t, all, fileOnly)
	findRow(t, all, live)

	liveOnly := listSessionsFiltered(t, a, "l2", map[string]any{"live": true})
	findRow(t, liveOnly, live)
	for _, row := range liveOnly {
		if testutil.Str(row, "path") == fileOnly {
			t.Fatalf("filter.live returned a file-only session: %v", row)
		}
	}

	// filter.cwd is exact-match on the recorded directory.
	byCwd := listSessionsFiltered(t, a, "l3", map[string]any{"cwd": "/home/u/proj"})
	findRow(t, byCwd, fileOnly)
}

// TestSteerIsImmediateWhileFollowUpIsQueued covers the queue split: steer is
// forwarded to pi at once and never listed; follow_up waits in the daemon
// queue and is reported by gw_queue with its author.
func TestSteerIsImmediateWhileFollowUpIsQueued(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "120")
	t.Setenv("FAKEPI_TURN_EVENTS", "12")
	_, addr := startDaemonHandle(t, nil)

	a := dial(t, addr, nil)
	defer a.Close()
	log := newFrameLog(a)

	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	log.wait(t, testutil.DefaultTimeout, "get_state response", response("g1"))
	a.Send(map[string]any{"type": "prompt", "id": "p1", "message": "slow turn"})
	log.wait(t, testutil.DefaultTimeout, "prompt response", response("p1"))
	log.wait(t, testutil.DefaultTimeout, "the turn to start streaming", typeIs("message_update"))

	a.Send(map[string]any{"type": "steer", "id": "st1", "message": "urgent steer"})
	log.wait(t, testutil.DefaultTimeout, "steer response", response("st1"))
	a.Send(map[string]any{"type": "follow_up", "id": "f1", "message": "queued follow-up"})
	log.wait(t, testutil.DefaultTimeout, "follow_up response", response("f1"))

	// The queued follow-up is announced; the steer is never in the queue.
	log.wait(t, testutil.DefaultTimeout, "gw_queue to list the follow-up", func(f map[string]any) bool {
		if f["type"] != "gw_queue" {
			return false
		}
		for _, raw := range testutil.Arr(f, "pending") {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if strings.Contains(testutil.Str(item, "preview"), "queued follow-up") {
				return true
			}
		}
		return false
	})
	for _, f := range log.all() {
		if f["type"] != "gw_queue" {
			continue
		}
		for _, raw := range testutil.Arr(f, "pending") {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if strings.Contains(testutil.Str(item, "preview"), "urgent steer") {
				t.Fatalf("steer was queued instead of forwarded: %v", item)
			}
			if mode := testutil.Str(item, "mode"); mode != "followUp" {
				t.Fatalf("queued item mode = %q, want followUp", mode)
			}
			if testutil.Str(testutil.Obj(item, "author"), "clientId") == "" {
				t.Fatalf("queued item has no author: %v", item)
			}
		}
	}
}

// TestAbortKeepsQueuedFollowUp covers abort semantics: it stops the shared turn
// but leaves the daemon-owned queue intact, so the queued follow-up still runs.
func TestAbortKeepsQueuedFollowUp(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "120")
	t.Setenv("FAKEPI_TURN_EVENTS", "12")
	_, addr := startDaemonHandle(t, nil)

	a := dial(t, addr, nil)
	defer a.Close()
	log := newFrameLog(a)

	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	log.wait(t, testutil.DefaultTimeout, "get_state response", response("g1"))
	a.Send(map[string]any{"type": "prompt", "id": "p1", "message": "long turn"})
	log.wait(t, testutil.DefaultTimeout, "prompt response", response("p1"))
	log.wait(t, testutil.DefaultTimeout, "the turn to start streaming", typeIs("message_update"))

	a.Send(map[string]any{"type": "follow_up", "id": "f1", "message": "must still run"})
	log.wait(t, testutil.DefaultTimeout, "follow_up response", response("f1"))
	a.Send(map[string]any{"type": "abort", "id": "ab1"})
	log.wait(t, testutil.DefaultTimeout, "abort response", response("ab1"))

	// Two turns settle: the aborted one and the queued follow-up that survived.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if countType(log.all(), "agent_settled") >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the queued follow-up did not run after abort; frames: %v", log.types())
}

// TestDialogRoutedToUIClientOnly covers dialog ownership: the request reaches
// the turn author (which may answer it) and is never sent to a client without
// the `ui` capability, rather than being broadcast.
func TestDialogRoutedToUIClientOnly(t *testing.T) {
	t.Setenv("FAKEPI_UI_REQUEST", "1")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "150")
	t.Setenv("FAKEPI_TURN_EVENTS", "10")
	_, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.Tokens = []protocol.TokenGrant{
			{Name: "ui", Token: "ui-token", Capabilities: protocol.AllCapabilities},
			{Name: "bot", Token: "bot-token",
				Capabilities: []string{protocol.CapObserve, protocol.CapPrompt}},
		}
	})

	author := dialWithToken(t, addr, "ui-token")
	defer author.Close()
	authorLog := newFrameLog(author)
	author.Send(map[string]any{"type": "get_state", "id": "g1"})
	authorLog.wait(t, testutil.DefaultTimeout, "get_state response", response("g1"))
	author.Send(map[string]any{"type": "get_state", "id": "g1b"})
	path := testutil.Str(testutil.Obj(authorLog.wait(t, testutil.DefaultTimeout,
		"get_state response", response("g1b")), "data"), "sessionFile")
	if path == "" {
		t.Fatal("author has no session file")
	}

	bot := dialWithToken(t, addr, "bot-token")
	defer bot.Close()
	botLog := newFrameLog(bot)
	bot.Send(map[string]any{"type": "switch_session", "id": "sw", "sessionPath": path})
	botLog.wait(t, testutil.DefaultTimeout, "switch_session response", response("sw"))

	author.Send(map[string]any{"type": "prompt", "id": "p1", "message": "with a dialog"})
	req := authorLog.wait(t, testutil.DefaultTimeout, "the dialog for the turn author",
		typeIs("extension_ui_request"))
	if testutil.Str(req, "method") == "" {
		t.Fatalf("ui request has no method: %v", req)
	}
	// The turn author owns the dialog and may answer it while the turn runs.
	// Real pi writes no response for extension_ui_response, so the observable
	// the gateway owns is that the pending dialog is consumed: a second answer
	// from the owner is dropped as ui_stale.
	dialogID := testutil.Str(req, "id")
	author.Send(map[string]any{"type": "extension_ui_response", "id": dialogID, "confirmed": true})
	author.Send(map[string]any{"type": "extension_ui_response", "id": dialogID, "confirmed": true})
	resp := authorLog.wait(t, testutil.DefaultTimeout, "the second dialog answer", func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "command") == "extension_ui_response"
	})
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUIStale {
		t.Fatalf("second dialog answer = %v, want ui_stale", resp)
	}
	// The non-ui client never sees it.
	authorLog.wait(t, 30*time.Second, "the turn to settle", typeIs("agent_settled"))
	if n := countType(botLog.all(), "extension_ui_request"); n != 0 {
		t.Fatalf("dialog was sent to a non-ui client %d time(s)", n)
	}
}

// TestSessionNotStoppedMidTurn covers the hibernation rule: detaching during a
// turn does not stop pi, and the session is reaped only after the turn settles
// and the idle timeout passes.
func TestSessionNotStoppedMidTurn(t *testing.T) {
	t.Setenv("FAKEPI_TURN_DELAY_MS", "200")
	t.Setenv("FAKEPI_TURN_EVENTS", "10")
	realPi, err := testutil.FakePi()
	if err != nil {
		t.Fatalf("fake pi: %v", err)
	}
	_, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 150*time.Millisecond, 150*time.Millisecond
	})

	a := dial(t, addr, nil)
	log := newFrameLog(a)
	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	log.wait(t, testutil.DefaultTimeout, "get_state response", response("g1"))
	a.Send(map[string]any{"type": "prompt", "id": "p1", "message": "slow"})
	log.wait(t, testutil.DefaultTimeout, "prompt response", response("p1"))
	log.wait(t, testutil.DefaultTimeout, "the turn to start streaming", typeIs("message_update"))

	// Detach while the turn is still running.
	a.Close()
	time.Sleep(300 * time.Millisecond)
	if n := testutil.CountProcesses(realPi); n != 1 {
		t.Fatalf("pi processes after mid-turn detach = %d, want 1 (never stopped mid-turn)", n)
	}
	// It does go away once the turn has settled and the idle timeout elapsed.
	waitFor(t, 20*time.Second, "the idle session to be reaped", func() bool {
		return testutil.CountProcesses(realPi) == 0
	})
}

// TestCrashedSessionReportsAndReAdopts covers pi crash handling: the crash is
// announced, api is not restarted on its own, and the next attach re-adopts the
// file and respawns pi with its history (lazy re-adoption, design.md §4.6).
func TestCrashedSessionReportsAndReAdopts(t *testing.T) {
	t.Setenv("FAKEPI_EXIT_AFTER_FIRST_TURN", "1")
	realPi, err := testutil.FakePi()
	if err != nil {
		t.Fatalf("fake pi: %v", err)
	}
	d, addr := startDaemonHandle(t, nil)

	a := dial(t, addr, nil)
	defer a.Close()
	log := newFrameLog(a)
	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	path := sessionPath(t, map[string]any{"data": testutil.Obj(
		log.wait(t, testutil.DefaultTimeout, "get_state response", response("g1")), "data")})
	a.Send(map[string]any{"type": "prompt", "id": "p1", "message": "then crash"})
	log.wait(t, testutil.DefaultTimeout, "prompt response", response("p1"))
	log.wait(t, testutil.DefaultTimeout, "the turn to settle", typeIs("agent_settled"))

	crash := log.wait(t, 15*time.Second, "the crash report", func(f map[string]any) bool {
		return f["type"] == "gw_session_state" && testutil.Str(f, "state") == "crashed"
	})
	if testutil.Str(crash, "reason") == "" {
		t.Fatalf("crash report has no reason: %v", crash)
	}

	// The daemon does not restart pi on its own, and the session is no longer
	// reported as live.
	waitFor(t, 10*time.Second, "pi to be gone after the crash", func() bool {
		return testutil.CountProcesses(realPi) == 0
	})
	if live := d.Status().Live; live != 0 {
		t.Fatalf("live sessions after a crash = %d, want 0 (no auto-respawn)", live)
	}

	// A new client re-adopts the file; pi is respawned and history survives.
	b := dial(t, addr, nil)
	defer b.Close()
	blog := newFrameLog(b)
	b.Send(map[string]any{"type": "switch_session", "id": "sw", "sessionPath": path})
	blog.wait(t, testutil.DefaultTimeout, "switch_session response", response("sw"))
	b.Send(map[string]any{"type": "get_state", "id": "g2"})
	state := testutil.Obj(blog.wait(t, testutil.DefaultTimeout, "get_state response",
		response("g2")), "data")
	if got := testutil.Str(state, "sessionFile"); got != path {
		t.Fatalf("re-adopted session = %q, want %q", got, path)
	}
	if got := testutil.Num(state, "messageCount"); got < 2 {
		t.Fatalf("re-adopted session messageCount = %v, want the crashed turn's history", got)
	}
	if n := testutil.CountProcesses(realPi); n != 1 {
		t.Fatalf("pi processes after re-adoption = %d, want 1", n)
	}
}

// TestDaemonRestartReAdoptsSessionFile covers design.md §4.5 at the daemon
// level: a restarted daemon starts with no pi processes, and the next attach
// resumes the session from its file.
func TestDaemonRestartReAdoptsSessionFile(t *testing.T) {
	realPi, err := testutil.FakePi()
	if err != nil {
		t.Fatalf("fake pi: %v", err)
	}
	first, addr := startDaemonHandle(t, nil)

	a := dial(t, addr, nil)
	a.Send(map[string]any{"type": "get_state", "id": "g1"})
	path := sessionPath(t, a.WaitResponse("g1", testutil.DefaultTimeout))
	a.Send(map[string]any{"type": "prompt", "id": "p1", "message": "before restart"})
	a.WaitResponse("p1", testutil.DefaultTimeout)
	a.WaitType("agent_settled", testutil.DefaultTimeout)
	a.Send(map[string]any{"type": "get_state", "id": "g2"})
	before := testutil.Num(testutil.Obj(a.WaitResponse("g2", testutil.DefaultTimeout), "data"), "messageCount")
	a.Close()

	first.Shutdown()
	waitFor(t, 10*time.Second, "the first daemon's pi to stop", func() bool {
		return testutil.CountProcesses(realPi) == 0
	})

	_, addr2 := startDaemonHandle(t, nil)
	b := dial(t, addr2, nil)
	defer b.Close()
	b.Send(map[string]any{"type": "switch_session", "id": "sw", "sessionPath": path})
	b.WaitResponse("sw", testutil.DefaultTimeout)
	b.Send(map[string]any{"type": "get_state", "id": "g3"})
	state := testutil.Obj(b.WaitResponse("g3", testutil.DefaultTimeout), "data")
	if got := testutil.Num(state, "messageCount"); got < before {
		t.Fatalf("messageCount after restart = %v, want at least %v", got, before)
	}
	// The resumed session still works.
	b.Send(map[string]any{"type": "prompt", "id": "p2", "message": "after restart"})
	if resp := b.WaitResponse("p2", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt after restart failed: %v", resp)
	}
	b.WaitType("agent_settled", 30*time.Second)
}

// metricsValue reads one counter for diagnostics.
func metricsValue(reg *metrics.Registry, name string) int64 {
	return reg.Value(name)
}

// dialRawNoRead connects a raw gateway client that never reads, with a small
// receive buffer so the daemon's writer blocks quickly. The codec is returned
// unused until the test decides to read.
func dialRawNoRead(t *testing.T, addr string, hello protocol.Hello, readBuf int) (net.Conn, *protocol.Codec) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if readBuf > 0 {
		if tcp, ok := nc.(*net.TCPConn); ok {
			if err := tcp.SetReadBuffer(readBuf); err != nil {
				t.Fatalf("set read buffer: %v", err)
			}
		}
	}
	codec := protocol.NewCodec(nc, nc)
	if err := codec.WriteJSON(&hello); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	return nc, codec
}

// driverWithSession opens a reading client and returns it with the session
// file its first get_state created.
func driverWithSession(t *testing.T, addr string) (*testutil.Conn, *frameLog, string) {
	t.Helper()
	driver := dial(t, addr, nil)
	log := newFrameLog(driver)
	driver.Send(map[string]any{"type": "get_state", "id": "g0"})
	resp := log.wait(t, testutil.DefaultTimeout, "get_state response", response("g0"))
	path := testutil.Str(testutil.Obj(resp, "data"), "sessionFile")
	if path == "" {
		t.Fatalf("no session file: %v", resp)
	}
	return driver, log, path
}

// floodEnv must be called before the session's pi is spawned.
//
// The pump coalesces streaming deltas at 8 KB per frame, so overflowing a
// 1024-frame out queue needs roughly 10 MB of delta content.
func floodEnv(t *testing.T, events int) {
	t.Helper()
	t.Setenv("FAKEPI_TURN_EVENTS", strconv.Itoa(events))
	t.Setenv("FAKEPI_TURN_DELAY_MS", "0")
}

// floodTurn runs one turn whose records overflow a subscriber that is not
// keeping up.
func floodTurn(t *testing.T, driver *testutil.Conn, log *frameLog) {
	t.Helper()
	driver.Send(map[string]any{"type": "prompt", "id": "p1",
		"message": strings.Repeat("x", 200)})
	log.wait(t, testutil.DefaultTimeout, "prompt response", response("p1"))
	log.wait(t, 120*time.Second, "the flood turn to settle", typeIs("agent_settled"))
}

// TestSilentClientIsClosedAsSlowConsumer covers the slow-consumer guard: a
// client that stops reading while events flow is dropped, while the session and
// its other clients keep working.
func TestSilentClientIsClosedAsSlowConsumer(t *testing.T) {
	floodEnv(t, 50000)
	d, addr := startDaemonHandle(t, func(c *daemon.Config) {
		// A 1 ms flush turns the flood into smaller frames, so the out queue
		// (1024 frames) is reached while the client is not reading.
		c.DeltaFlush = time.Millisecond
	})
	driver, log, path := driverWithSession(t, addr)
	defer driver.Close()

	hello := protocol.Hello{Type: "gw_hello", Protocol: protocol.Version, Token: testToken,
		Client: protocol.ClientInfo{Kind: "test"}, Session: path}
	nc, _ := dialRawNoRead(t, addr, hello, 4096)
	// The kernel keeps flushing the daemon's send queue after a close, so the
	// client's own EOF is slow here by design; the observable disconnect is the
	// daemon's state.
	defer nc.Close()

	floodTurn(t, driver, log)

	waitFor(t, 60*time.Second, "the daemon to drop the silent client", func() bool {
		st := d.Status()
		return st.Connections == 1 && st.Subscribers == 1
	})
	if st := d.Status(); st.Live != 1 || st.Clients != 1 {
		t.Fatalf("the session did not survive the dropped client: %+v", st)
	}
	// The remaining client is still fully usable.
	driver.Send(map[string]any{"type": "gw_ping", "id": "ping1"})
	log.wait(t, testutil.DefaultTimeout, "gw_pong after the drop", typeIs("gw_pong"))
}

// TestLossyClientReceivesLagMarker covers the allowLossy path end to end: a
// client that falls behind during a long streaming turn is told about the
// dropped range with gw_lag once it catches up — before the next record — and
// the connection survives. (Terminal records are never dropped, so a client
// that is still behind when a turn ends is closed instead; that is the
// slow-consumer test above.)
func TestLossyClientReceivesLagMarker(t *testing.T) {
	// A turn that streams long enough for the client to fall behind and then
	// catch up while records keep flowing.
	t.Setenv("FAKEPI_TURN_EVENTS", "20000")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "1")
	d, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.DeltaFlush = time.Millisecond
	})
	reg := d.Metrics()
	driver, log, path := driverWithSession(t, addr)
	defer driver.Close()

	hello := protocol.Hello{Type: "gw_hello", Protocol: protocol.Version, Token: testToken,
		Client: protocol.ClientInfo{Kind: "test"}, Session: path, AllowLossy: true}
	// A small-but-usable receive buffer: the reader must be able to catch up on
	// loopback once it starts reading.
	nc, codec := dialRawNoRead(t, addr, hello, 256<<10)
	defer nc.Close()

	driver.Send(map[string]any{"type": "prompt", "id": "p1", "message": "stream"})
	log.wait(t, testutil.DefaultTimeout, "prompt response", response("p1"))

	// The client is not reading: the daemon must start dropping non-terminal
	// records for it instead of blocking the session.
	waitFor(t, 60*time.Second, "drops for the falling-behind client", func() bool {
		return metricsValue(reg, metrics.FramesDropped) > 0
	})

	// Now it reads. The next record the daemon sends must carry the gap marker.
	var mu sync.Mutex
	seen := map[string]int{}
	go func() {
		for {
			_ = nc.SetReadDeadline(time.Now().Add(120 * time.Second))
			rec, err := codec.Read()
			if err != nil {
				return
			}
			mu.Lock()
			seen[protocol.Field(rec, "type")]++
			mu.Unlock()
		}
	}()
	countSeen := func(typ string) int {
		mu.Lock()
		defer mu.Unlock()
		return seen[typ]
	}
	if !waitSeen(t, countSeen, "gw_lag") {
		return
	}
	// The stream still ends properly for this client.
	if !waitSeen(t, countSeen, "agent_settled") {
		return
	}
	// The connection is still usable.
	if err := codec.WriteJSON(map[string]any{"type": "gw_ping", "id": "ping2"}); err != nil {
		t.Fatalf("ping after the flood: %v", err)
	}
	if !waitSeen(t, countSeen, "gw_pong") {
		return
	}
	if metricsValue(reg, metrics.ClientLags) == 0 {
		t.Fatal("client_lags_total did not count the delivered gap marker")
	}
}

// waitSeen polls for a frame type and reports whether it appeared.
func waitSeen(t *testing.T, count func(string) int, typ string) bool {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if count(typ) > 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s on the lossy client", typ)
	return false
}

// TestUnsupportedCommandSurfacesPiError pins the passthrough contract: a
// command the gateway does not intercept is forwarded to pi, and when pi does
// not know it the client sees pi's own error rather than a fake success. It is
// the regression test for fakepi answering success to every unknown type,
// which hid exactly this class of mismatch (docs/development.md, "Remove two
// non-commands from the capability table").
func TestUnsupportedCommandSurfacesPiError(t *testing.T) {
	dir := t.TempDir()
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	c, _, _ := dialSession(t, addr, filepath.Join(dir, "unsupported.jsonl"))

	// `notify` is a pi extension_ui_request method, not a command. It must not
	// be gated by a capability (the table no longer lists it) and must come back
	// as pi's unknown-command error.
	c.Send(map[string]any{"type": "notify", "id": "n1", "message": "hi"})
	resp := c.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && testutil.Str(f, "id") == "n1"
	}, testutil.DefaultTimeout)
	if resp["success"] != false {
		t.Fatalf("notify answered success=%v; pi has no notify command", resp["success"])
	}
	if got := testutil.Str(resp, "error"); !strings.Contains(got, "Unknown command: notify") {
		t.Fatalf("notify error = %q, want pi's unknown-command error", got)
	}
	if code := testutil.Str(resp, "code"); code == protocol.CodeForbidden {
		t.Fatalf("notify was capability-gated: %v", resp)
	}
}

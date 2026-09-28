package daemon_test

import (
	"context"
	"encoding/json"
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

// startDaemonWithState runs a daemon whose durable spawn state and catalog
// live at the caller-provided directories, so a test can stop it and start a
// second daemon over the same state (the daemon-restart case). startDaemonOpts
// cannot do this because gwtest gives every daemon a fresh StateDir.
func startDaemonWithState(t *testing.T, stateDir, sessionsDir string, mutate func(*daemon.Config)) (addr string, stop func()) {
	t.Helper()
	piBin, err := testutil.FakePi()
	if err != nil {
		t.Skipf("cannot build fake pi: %v", err)
	}
	t.Setenv("PI_CODING_AGENT_SESSION_DIR", sessionsDir)
	cfg := daemon.Config{
		Addr:     "127.0.0.1:0",
		Token:    testToken,
		PiBin:    piBin,
		StateDir: stateDir,
		Log:      gwtest.Logger(t),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	d := daemon.New(cfg)
	if err := d.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.Serve(ctx)
	}()
	stop = func() {
		cancel()
		<-done
		d.Shutdown()
	}
	t.Cleanup(stop)
	return d.Addr().String(), stop
}

// waitForArgs polls the fake pi argv marker written by the first pi process
// whose command line mentions want, and returns the full argv.
func waitForArgsFile(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(testutil.DefaultTimeout)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil && len(b) > 0 {
			return string(b)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no fake pi argv at %s", path)
	return ""
}

// TestSpawnConfigSurvivesDaemonRestart is the P2 regression: a session created
// with --append-system-prompt must be respawned with that parameter by a fresh
// daemon that only knows the session file, even when the attaching client
// passes no parameters (the G3 defect).
func TestSpawnConfigSurvivesDaemonRestart(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(sessionsDir, "spawn-restart.jsonl")

	addr, stop := startDaemonWithState(t, stateDir, sessionsDir, nil)
	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "one"}
	})
	if w := a.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] == nil {
		t.Fatalf("attach failed: %v", w)
	}
	argv := waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "one") {
		t.Fatalf("first spawn argv does not carry the prompt: %q", argv)
	}
	a.Close()
	stop()

	// A new daemon, same durable state, no parameters from the client.
	_ = os.Remove(argsFile)
	addr2, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)
	b := dial(t, addr2, func(h *protocol.Hello) { h.Session = path })
	w := b.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["session"] == nil {
		t.Fatalf("re-attach failed: %v", w)
	}
	argv = waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "one") {
		t.Fatalf("respawn lost the recorded spawn parameter: %q", argv)
	}
}

// TestColdAttachMergesUnrecordedSpawnKeys pins the merge rule: a recorded key
// wins, and a key the record never set is filled in from the requester when
// the session is cold.
func TestColdAttachMergesUnrecordedSpawnKeys(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(sessionsDir, "spawn-merge.jsonl")

	addr, stop := startDaemonWithState(t, stateDir, sessionsDir, nil)
	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "one"}
	})
	a.WaitType("gw_welcome", testutil.DefaultTimeout)
	waitForArgsFile(t, argsFile)
	a.Close()
	stop()

	// The requester asks for a different value for the recorded key (recorded
	// wins) and a key that was never recorded (filled in).
	_ = os.Remove(argsFile)
	addr2, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)
	b := dial(t, addr2, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "two", "-e", "/x.ts"}
	})
	if w := b.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] == nil {
		t.Fatalf("attach failed: %v", w)
	}
	argv := waitForArgsFile(t, argsFile)
	if strings.Contains(argv, "two") {
		t.Fatalf("requester overrode a recorded spawn value: %q", argv)
	}
	if !strings.Contains(argv, "one") || !strings.Contains(argv, "/x.ts") {
		t.Fatalf("merged argv = %q, want recorded prompt and new extension", argv)
	}
}

// TestLiveAttachIgnoresUnrecordedSpawnKey is R5: a client that passes a spawn
// parameter the live session never recorded must still attach.
func TestLiveAttachIgnoresUnrecordedSpawnKey(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "spawn-live.jsonl")

	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--approve"}
	})
	a.WaitType("gw_welcome", testutil.DefaultTimeout)

	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"-e", "/x.ts"}
	})
	w := b.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["session"] == nil {
		t.Fatalf("unrecorded spawn key must not refuse attachment: %v", w)
	}
}

// TestCatalogExposesSpawnConfig is P3: a client can see the recorded spawn
// configuration before attaching.
func TestCatalogExposesSpawnConfig(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	path := filepath.Join(sessionsDir, "spawn-catalog.jsonl")
	addr, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)

	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "hello"}
	})
	a.WaitType("gw_welcome", testutil.DefaultTimeout)

	a.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	resp := a.WaitResponse("l1", testutil.DefaultTimeout)
	sessions := testutil.Arr(testutil.Obj(resp, "data"), "sessions")
	found := false
	for _, raw := range sessions {
		row, _ := raw.(map[string]any)
		if testutil.Str(row, "path") != path {
			continue
		}
		found = true
		spawn := testutil.Obj(row, "spawn")
		if got := testutil.StrSlice(spawn, "append-system-prompt"); len(got) != 1 || got[0] != "hello" {
			t.Fatalf("row spawn = %v, want append-system-prompt hello", spawn)
		}
	}
	if !found {
		t.Fatalf("session %s not in catalog: %v", path, sessions)
	}
}

// TestWelcomeAdvertisesFeatures is P3: feature detection without probing.
func TestWelcomeAdvertisesFeatures(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	c := dial(t, addr, nil)
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	features := testutil.StrSlice(w, "features")
	if !contains(features, protocol.FeatureSpawnConfig) {
		t.Fatalf("features = %v, want %q", features, protocol.FeatureSpawnConfig)
	}
	if contains(features, "inject") {
		t.Fatalf("features still advertise the removed inject command: %v", features)
	}
}

// TestReloadReplacesSpawnConfig is P4: gw_reload_session{piArgs} restarts pi
// with the new configuration and makes it the durable record.
func TestReloadReplacesSpawnConfig(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(sessionsDir, "spawn-reload.jsonl")
	addr, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)

	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "one"}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	waitForArgsFile(t, argsFile)

	_ = os.Remove(argsFile)
	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "r1",
		"piArgs": []string{"--append-system-prompt", "two"},
	})
	resp := c.WaitResponse("r1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("reload failed: %v", resp)
	}
	argv := waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "two") {
		t.Fatalf("reload did not spawn with the replacement args: %q", argv)
	}

	c.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	listResp := c.WaitResponse("l1", testutil.DefaultTimeout)
	for _, raw := range testutil.Arr(testutil.Obj(listResp, "data"), "sessions") {
		row, _ := raw.(map[string]any)
		if testutil.Str(row, "path") != path {
			continue
		}
		got := testutil.StrSlice(testutil.Obj(row, "spawn"), "append-system-prompt")
		if len(got) != 1 || got[0] != "two" {
			t.Fatalf("recorded spawn after reload = %v, want two", got)
		}
	}
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// TestReloadWithoutPiArgsKeepsReplacement pins that a replacement configuration
// becomes the actor's parameters, so a later plain reload does not silently
// respawn pi with the pre-replacement arguments (docs/protocol.md §4.3).
func TestReloadWithoutPiArgsKeepsReplacement(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(sessionsDir, "reload-chain.jsonl")
	addr, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)

	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "one"}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if argv := waitForArgsFile(t, argsFile); !strings.Contains(argv, "one") {
		t.Fatalf("initial argv = %q", argv)
	}

	_ = os.Remove(argsFile)
	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "r1",
		"piArgs": []string{"--append-system-prompt", "two"},
	})
	if resp := c.WaitResponse("r1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("replacement reload failed: %v", resp)
	}
	if argv := waitForArgsFile(t, argsFile); !strings.Contains(argv, "two") {
		t.Fatalf("replacement argv = %q", argv)
	}

	_ = os.Remove(argsFile)
	c.Send(map[string]any{"type": "gw_reload_session", "id": "r2"})
	if resp := c.WaitResponse("r2", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("plain reload failed: %v", resp)
	}
	argv := waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "two") || strings.Contains(argv, "one") {
		t.Fatalf("plain reload lost the replacement: %q", argv)
	}
}

// TestSpawnCatalogRedactsCredentialsAndIsAbsentFromDebug covers the P3 leak:
// an api-key is never exposed on gw_list_sessions, and the unauthenticated
// debug catalog carries no spawn view at all.
func TestSpawnCatalogRedactsCredentialsAndIsAbsentFromDebug(t *testing.T) {
	d, addr := startDaemonHandle(t, nil)
	const secret = "sk-secret-value"
	const prompt = "PROMPT_MARKER_XYZ"
	path := filepath.Join(t.TempDir(), "redact.jsonl")

	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--api-key", secret, "--append-system-prompt", prompt}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	c.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	resp := c.WaitResponse("l1", testutil.DefaultTimeout)
	raw, _ := json.Marshal(resp)
	if strings.Contains(string(raw), secret) || strings.Contains(string(raw), `"api-key"`) {
		t.Fatalf("gw_list_sessions leaked the api-key: %s", raw)
	}
	found := false
	for _, item := range testutil.Arr(testutil.Obj(resp, "data"), "sessions") {
		row, _ := item.(map[string]any)
		if testutil.Str(row, "path") != path {
			continue
		}
		found = true
		spawn := testutil.Obj(row, "spawn")
		if got := testutil.StrSlice(spawn, "append-system-prompt"); len(got) != 1 || got[0] != prompt {
			t.Fatalf("redacted spawn = %v, want the prompt", spawn)
		}
	}
	if !found {
		t.Fatalf("session %s missing from catalog", path)
	}

	body, err := d.CatalogJSON("", 0)
	if err != nil {
		t.Fatalf("CatalogJSON: %v", err)
	}
	if strings.Contains(string(body), secret) || strings.Contains(string(body), prompt) || strings.Contains(string(body), `"spawn"`) {
		t.Fatalf("debug catalog leaked spawn data: %s", body)
	}
}

// TestCatalogColdRowReadsSidecar covers the catalog path a client takes before
// attaching to a hibernated session: the row's spawn view comes from the
// durable record, not from a live actor (docs/protocol.md §3.2).
func TestCatalogColdRowReadsSidecar(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(sessionsDir, "cold-row.jsonl")

	addr, stop := startDaemonWithState(t, stateDir, sessionsDir, nil)
	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--append-system-prompt", "cold"}
	})
	a.WaitType("gw_welcome", testutil.DefaultTimeout)
	waitForArgsFile(t, argsFile)
	a.Close()
	stop()

	// A fresh daemon has no actor for the file: the row must come from the
	// sidecar.
	addr2, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)
	b := dial(t, addr2, nil)
	b.WaitType("gw_welcome", testutil.DefaultTimeout)
	b.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	resp := b.WaitResponse("l1", testutil.DefaultTimeout)
	for _, item := range testutil.Arr(testutil.Obj(resp, "data"), "sessions") {
		row, _ := item.(map[string]any)
		if testutil.Str(row, "path") != path {
			continue
		}
		if row["live"] == true {
			t.Fatalf("expected a cold row, got live: %v", row)
		}
		got := testutil.StrSlice(testutil.Obj(row, "spawn"), "append-system-prompt")
		if len(got) != 1 || got[0] != "cold" {
			t.Fatalf("cold row spawn = %v, want append-system-prompt cold", row["spawn"])
		}
		return
	}
	t.Fatalf("session %s not in catalog: %v", path, resp)
}

// TestStopReportsUnbound pins the `unbound` field a client uses to clear its
// binding without waiting for the terminal event: true when the target was the
// requester's own session, false when it was another.
func TestStopReportsUnbound(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.jsonl")
	pathB := filepath.Join(dir, "b.jsonl")

	a := dial(t, addr, func(h *protocol.Hello) { h.Session = pathA })
	a.WaitType("gw_welcome", testutil.DefaultTimeout)
	b := dial(t, addr, func(h *protocol.Hello) { h.Session = pathB })
	b.WaitType("gw_welcome", testutil.DefaultTimeout)

	// Stopping another client's busy/attached session is forced and does not
	// unbind the requester.
	a.Send(map[string]any{"type": "gw_stop_session", "id": "s1", "session": pathB, "force": true})
	resp := a.WaitResponse("s1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("stop of another session failed: %v", resp)
	}
	if testutil.Obj(resp, "data")["unbound"] == true {
		t.Fatalf("stopping another session reported unbound: %v", resp)
	}

	// The requester's own session does unbind it.
	a.Send(map[string]any{"type": "gw_stop_session", "id": "s2"})
	resp = a.WaitResponse("s2", testutil.DefaultTimeout)
	if resp["success"] != true || testutil.Obj(resp, "data")["unbound"] != true {
		t.Fatalf("self stop = %v, want unbound:true", resp)
	}
}

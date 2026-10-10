package daemon_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/config"
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
		h.PiArgs = []string{"--append-system-prompt", "one", "--extension", "/opt/pi-chat/chat-tools.ts"}
	})
	if w := a.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] == nil {
		t.Fatalf("attach failed: %v", w)
	}
	argv := waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "one") || !strings.Contains(argv, "/opt/pi-chat/chat-tools.ts") {
		t.Fatalf("first spawn argv does not carry the prompt and extension: %q", argv)
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
	if !strings.Contains(argv, "one") || !strings.Contains(argv, "/opt/pi-chat/chat-tools.ts") {
		t.Fatalf("respawn lost the recorded prompt or extension: %q", argv)
	}
}

// TestNewSessionWritesOneSpawnRecord is GH-1: gw_new_session used to persist a
// spawn sidecar before pi reported the session path, and canonicalPath("") resolved
// to the daemon's working directory, so every created session also wrote a record
// under <sha256(cwd)>. That record could never be deleted (gw_delete_session keys
// on the session path) and was overwritten by the next created session. Creating
// one session must leave exactly one sidecar, keyed by the path pi reported.
func TestNewSessionWritesOneSpawnRecord(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	addr, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)

	c := dial(t, addr, nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	c.Send(map[string]any{
		"type":   "gw_new_session",
		"id":     "n1",
		"name":   "probe",
		"piArgs": []string{"--append-system-prompt", "MARKER", "--extension", "/opt/pi-chat/chat-tools.ts"},
	})
	resp := c.WaitResponse("n1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("gw_new_session failed: %v", resp)
	}
	path := testutil.Str(testutil.Obj(resp, "data"), "path")
	if path == "" {
		t.Fatalf("gw_new_session returned no path: %v", resp)
	}
	canon := resolvedPath(t, path)
	if argv := waitForArgsFile(t, argsFile); !strings.Contains(argv, "/opt/pi-chat/chat-tools.ts") {
		t.Fatalf("gw_new_session did not spawn Pi with the extension: %q", argv)
	}
	c.Send(map[string]any{"type": "get_state", "id": "state1"})
	stateResp := c.WaitResponse("state1", testutil.DefaultTimeout)
	if got := testutil.Str(testutil.Obj(stateResp, "data"), "sessionFile"); got != path {
		t.Fatalf("get_state.sessionFile = %q, want gw_new_session path %q", got, path)
	}

	spawnDir := config.SpawnDir(stateDir)
	entries, err := os.ReadDir(spawnDir)
	if err != nil {
		t.Fatalf("read spawn dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(entries) != 1 {
		t.Fatalf("spawn dir holds %d records %v, want exactly one keyed by %s", len(entries), names, canon)
	}
	sum := sha256.Sum256([]byte(canon))
	if want := hex.EncodeToString(sum[:]) + ".json"; names[0] != want {
		t.Fatalf("sidecar name = %q, want %q (keyed by %s)", names[0], want, canon)
	}
	raw, err := os.ReadFile(filepath.Join(spawnDir, names[0]))
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	var rec struct {
		Path  string              `json:"path"`
		Spawn map[string][]string `json:"spawn"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("parse sidecar: %v", err)
	}
	if rec.Path != canon {
		t.Fatalf("sidecar path = %q, want %q", rec.Path, canon)
	}
	if got := rec.Spawn["append-system-prompt"]; len(got) != 1 || got[0] != "MARKER" {
		t.Fatalf("sidecar spawn = %v, want append-system-prompt MARKER", rec.Spawn)
	}
	if got := rec.Spawn["extension"]; len(got) != 1 || got[0] != "/opt/pi-chat/chat-tools.ts" {
		t.Fatalf("sidecar extension = %v, want installed chat-tools path", got)
	}
}

// resolvedPath mirrors how the daemon canonicalizes a session path: the file
// exists, so EvalSymlinks resolves it.
func resolvedPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatalf("resolve %s: %v", path, err)
	}
	return resolved
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
	if !contains(features, protocol.FeatureSpawnConfigAdditive) {
		t.Fatalf("features = %v, want %q", features, protocol.FeatureSpawnConfigAdditive)
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

// TestAddPiArgsMergesAndPersistsSpawnConfig pins the generic additive reload:
// extensions are merged idempotently, runtime args survive the live restart,
// and the new spawn record is used after a daemon restart.
func TestAddPiArgsMergesAndPersistsSpawnConfig(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(sessionsDir, "add-extension.jsonl")

	addr, stop := startDaemonWithState(t, stateDir, sessionsDir, nil)
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--provider", "openai", "--extension", "/opt/pi-chat/first.ts"}
	})
	if w := c.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] == nil {
		t.Fatalf("attach failed: %v", w)
	}
	waitForArgsFile(t, argsFile)

	// A live attach can change runtime state without changing the actor's
	// startup argv. Additive restart must snapshot Pi's current state instead.
	runtimeClient := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--provider", "live-provider", "--model", "live-model", "--thinking", "high", "--name", "live-name"}
	})
	runtimeWelcome := runtimeClient.WaitType("gw_welcome", testutil.DefaultTimeout)
	runtimeID := testutil.Str(runtimeWelcome, "clientId")
	runtimeClient.Send(map[string]any{"type": "get_state", "id": "runtime-state"})
	state := runtimeClient.WaitResponse("runtime-state", testutil.DefaultTimeout)
	stateData := testutil.Obj(state, "data")
	model := testutil.Obj(stateData, "model")
	if testutil.Str(model, "id") != "live-model" || testutil.Str(model, "provider") != "live-provider" ||
		testutil.Str(stateData, "thinkingLevel") != "high" || testutil.Str(stateData, "sessionName") != "live-name" {
		t.Fatalf("live runtime changes did not apply before additive reload: %v", stateData)
	}
	runtimeClient.Close()
	c.WaitFor(func(frame map[string]any) bool {
		client, _ := frame["client"].(map[string]any)
		return frame["type"] == "gw_presence" && frame["event"] == "leave" && testutil.Str(client, "clientId") == runtimeID
	}, testutil.DefaultTimeout)

	_ = os.Remove(argsFile)
	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "add1", "force": true,
		"addPiArgs": []string{"-e", "/opt/pi-chat/first.ts", "--extension", "/opt/pi-chat/second.ts"},
	})
	resp := c.WaitResponse("add1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("additive reload failed: %v", resp)
	}
	argv := waitForArgsFile(t, argsFile)
	if strings.Count(argv, "/opt/pi-chat/first.ts") != 1 || strings.Count(argv, "/opt/pi-chat/second.ts") != 1 {
		t.Fatalf("extension additions were not deduplicated/merged: %q", argv)
	}
	for _, pair := range [][2]string{
		{"--provider", "live-provider"}, {"--model", "live-model"},
		{"--thinking", "high"}, {"--name", "live-name"},
	} {
		if !strings.Contains(argv, pair[0]+"\n"+pair[1]+"\n") {
			t.Fatalf("additive reload dropped live runtime setting %s=%s: %q", pair[0], pair[1], argv)
		}
	}

	c.Send(map[string]any{"type": "gw_list_sessions", "id": "list1"})
	listed := c.WaitResponse("list1", testutil.DefaultTimeout)
	found := false
	for _, item := range testutil.Arr(testutil.Obj(listed, "data"), "sessions") {
		row, _ := item.(map[string]any)
		if testutil.Str(row, "path") != path {
			continue
		}
		found = true
		got := testutil.StrSlice(testutil.Obj(row, "spawn"), "extension")
		if len(got) != 2 || got[0] != "/opt/pi-chat/first.ts" || got[1] != "/opt/pi-chat/second.ts" {
			t.Fatalf("catalog extensions = %v, want both ordered paths", got)
		}
	}
	if !found {
		t.Fatalf("session %s missing from catalog: %v", path, listed)
	}

	// A later plain reload reuses the merged config, not the pre-addition argv.
	_ = os.Remove(argsFile)
	c.Send(map[string]any{"type": "gw_reload_session", "id": "reload1"})
	if resp := c.WaitResponse("reload1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("plain reload failed: %v", resp)
	}
	argv = waitForArgsFile(t, argsFile)
	if strings.Count(argv, "/opt/pi-chat/first.ts") != 1 || strings.Count(argv, "/opt/pi-chat/second.ts") != 1 {
		t.Fatalf("plain reload lost the additive config: %q", argv)
	}

	// An ordinary live attach with no piArgs must not replace the record.
	live := dial(t, addr, func(h *protocol.Hello) { h.Session = path })
	if w := live.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] == nil {
		t.Fatalf("live attach without piArgs failed: %v", w)
	}
	c.Send(map[string]any{"type": "gw_list_sessions", "id": "list2"})
	listed = c.WaitResponse("list2", testutil.DefaultTimeout)
	foundAfterAttach := false
	for _, item := range testutil.Arr(testutil.Obj(listed, "data"), "sessions") {
		row, _ := item.(map[string]any)
		if testutil.Str(row, "path") == path {
			foundAfterAttach = true
			got := testutil.StrSlice(testutil.Obj(row, "spawn"), "extension")
			if len(got) != 2 || got[0] != "/opt/pi-chat/first.ts" || got[1] != "/opt/pi-chat/second.ts" {
				t.Fatalf("live attach changed recorded extensions: %v", got)
			}
		}
	}
	if !foundAfterAttach {
		t.Fatalf("session %s missing after live attach: %v", path, listed)
	}

	live.Close()
	c.Close()
	stop()
	_ = os.Remove(argsFile)
	addr2, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)
	cold := dial(t, addr2, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--provider", "openai"}
	})
	if w := cold.WaitType("gw_welcome", testutil.DefaultTimeout); w["session"] == nil {
		t.Fatalf("cold attach failed: %v", w)
	}
	argv = waitForArgsFile(t, argsFile)
	if strings.Count(argv, "/opt/pi-chat/first.ts") != 1 || strings.Count(argv, "/opt/pi-chat/second.ts") != 1 {
		t.Fatalf("daemon restart lost the additive config: %q", argv)
	}
}

// TestAddPiArgsAttachWaitsForSpawnCommit ensures a concurrent live attach
// validates against the new record, not the record from before a reload.
func TestAddPiArgsAttachWaitsForSpawnCommit(t *testing.T) {
	t.Setenv("FAKEPI_GET_STATE_DELAY_ON_ARG", "/new-dir")
	t.Setenv("FAKEPI_GET_STATE_DELAY_MS", "400")
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	path := filepath.Join(t.TempDir(), "attach-during-reload.jsonl")
	_, addr := startDaemonHandle(t, func(cfg *daemon.Config) {
		cfg.ReloadTimeout = 2 * time.Second
	})
	c := dial(t, addr, func(h *protocol.Hello) { h.Session = path })
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if err := os.Remove(argsFile); err != nil {
		t.Fatalf("remove initial argv: %v", err)
	}

	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "add1",
		"addPiArgs": []string{"--session-dir", "/new-dir"},
	})
	argv := waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "--session-dir\n/new-dir\n") {
		t.Fatalf("replacement argv did not contain new setting: %q", argv)
	}

	attaching := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--session-dir", "/other-dir"}
	})
	welcome := attaching.WaitType("gw_welcome", testutil.DefaultTimeout)
	if welcome["session"] != nil {
		t.Fatalf("concurrent attach used stale spawn record: %v", welcome)
	}
	attachErr := attaching.WaitType("gw_error", testutil.DefaultTimeout)
	if testutil.Str(attachErr, "code") != protocol.CodeSpawnParamConflict {
		t.Fatalf("concurrent attach error = %v, want spawn_param_conflict", attachErr)
	}
	if resp := c.WaitResponse("add1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("additive reload failed: %v", resp)
	}
}

// TestAddPiArgsTimeoutCancelsLateReady ensures an unresponsive replacement is
// stopped at the timeout and cannot later commit actor state without its record.
func TestAddPiArgsTimeoutCancelsLateReady(t *testing.T) {
	t.Setenv("FAKEPI_GET_STATE_DELAY_ON_ARG", "/late-extension.ts")
	t.Setenv("FAKEPI_GET_STATE_DELAY_MS", "1500")
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	stateDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "timeout-reload.jsonl")
	_, addr := startDaemonHandle(t, func(cfg *daemon.Config) {
		cfg.StateDir = stateDir
		cfg.ReloadTimeout = time.Second
	})
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--extension", "/old-extension.ts"}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	waitForArgsFile(t, argsFile)
	if err := os.Remove(argsFile); err != nil {
		t.Fatalf("remove initial argv: %v", err)
	}
	canon := resolvedPath(t, path)
	sum := sha256.Sum256([]byte(canon))
	recordPath := filepath.Join(config.SpawnDir(stateDir), hex.EncodeToString(sum[:])+".json")
	oldRecord, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read original spawn record: %v", err)
	}

	frames := newFrameLog(c)
	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "timeout1",
		"addPiArgs": []string{"--extension", "/late-extension.ts"},
	})
	argv := waitForArgsFile(t, argsFile)
	if !strings.Contains(argv, "/late-extension.ts") {
		t.Fatalf("replacement process did not start before timeout: %q", argv)
	}
	state := frames.wait(t, testutil.DefaultTimeout, "reload completion state", func(frame map[string]any) bool {
		if frame["type"] != "gw_session_state" {
			return false
		}
		return (testutil.Str(frame, "state") == "crashed" && testutil.Str(frame, "reason") == "reload timed out") ||
			(testutil.Str(frame, "state") == "ready" && testutil.Str(frame, "reason") == "reload")
	})
	if testutil.Str(state, "state") != "crashed" {
		t.Fatalf("late replacement became ready after timeout: %v", state)
	}
	resp := frames.wait(t, testutil.DefaultTimeout, "reload response", func(frame map[string]any) bool {
		return frame["type"] == "response" && testutil.Str(frame, "id") == "timeout1"
	})
	if resp["success"] != false || resp["code"] != protocol.CodeReloadTimeout {
		t.Fatalf("timed-out reload = %v, want reload_timeout", resp)
	}
	got, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read spawn record after timeout: %v", err)
	}
	if string(got) != string(oldRecord) {
		t.Fatalf("timed-out reload changed durable record:\nold %s\nnew %s", oldRecord, got)
	}
}

// TestAddPiArgsPreflightTimeoutKeepsOldProcess ensures a timed-out state
// snapshot does not stop the process whose configuration is still authoritative.
func TestAddPiArgsPreflightTimeoutKeepsOldProcess(t *testing.T) {
	t.Setenv("FAKEPI_GET_STATE_DELAY_ON_ARG", "/old-extension.ts")
	t.Setenv("FAKEPI_GET_STATE_DELAY_MS", "250")
	argsFile := filepath.Join(t.TempDir(), "argv")
	t.Setenv("FAKEPI_ARGS_FILE", argsFile)
	stateDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "preflight-timeout.jsonl")
	_, addr := startDaemonHandle(t, func(cfg *daemon.Config) {
		cfg.StateDir = stateDir
		cfg.ReloadTimeout = 40 * time.Millisecond
	})
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--extension", "/old-extension.ts"}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	oldArgv := waitForArgsFile(t, argsFile)
	canon := resolvedPath(t, path)
	sum := sha256.Sum256([]byte(canon))
	recordPath := filepath.Join(config.SpawnDir(stateDir), hex.EncodeToString(sum[:])+".json")
	oldRecord, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read original spawn record: %v", err)
	}

	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "preflight-timeout1",
		"addPiArgs": []string{"--extension", "/new-extension.ts"},
	})
	resp := c.WaitResponse("preflight-timeout1", testutil.DefaultTimeout)
	if resp["success"] != false || resp["code"] != protocol.CodeReloadTimeout {
		t.Fatalf("preflight timeout response = %v, want reload_timeout", resp)
	}
	c.Send(map[string]any{"type": "get_state", "id": "old-process-state"})
	if got := c.WaitResponse("old-process-state", testutil.DefaultTimeout); got["success"] != true {
		t.Fatalf("old process did not remain usable after preflight timeout: %v", got)
	}
	if argv := waitForArgsFile(t, argsFile); argv != oldArgv || strings.Contains(argv, "/new-extension.ts") {
		t.Fatalf("preflight timeout replaced process argv: %q", argv)
	}
	got, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read spawn record after preflight timeout: %v", err)
	}
	if string(got) != string(oldRecord) {
		t.Fatalf("preflight timeout changed durable record:\nold %s\nnew %s", oldRecord, got)
	}
}

// TestAddPiArgsReportsPersistenceFailure ensures a process restart is not
// reported durable when the sidecar save fails; the live catalog still matches
// the running process so retrying can persist the same configuration.
func TestAddPiArgsReportsPersistenceFailure(t *testing.T) {
	stateDir := t.TempDir()
	path := filepath.Join(t.TempDir(), "persist-failure.jsonl")
	_, addr := startDaemonHandle(t, func(cfg *daemon.Config) { cfg.StateDir = stateDir })
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--extension", "/old-extension.ts"}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	canon := resolvedPath(t, path)
	sum := sha256.Sum256([]byte(canon))
	spawnDir := config.SpawnDir(stateDir)
	recordPath := filepath.Join(spawnDir, hex.EncodeToString(sum[:])+".json")
	oldRecord, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read original spawn record: %v", err)
	}
	backupDir := spawnDir + ".backup"
	if err := os.Rename(spawnDir, backupDir); err != nil {
		t.Fatalf("move spawn directory: %v", err)
	}
	if err := os.WriteFile(spawnDir, []byte("block writes"), 0o600); err != nil {
		t.Fatalf("block spawn directory: %v", err)
	}
	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "persist1",
		"addPiArgs": []string{"--extension", "/new-extension.ts"},
	})
	resp := c.WaitResponse("persist1", testutil.DefaultTimeout)
	if resp["success"] != false || resp["code"] != protocol.CodeSpawnConfigPersistFailed {
		t.Fatalf("sidecar write failure response = %v, want spawn_config_persist_failed", resp)
	}
	if err := os.Remove(spawnDir); err != nil {
		t.Fatalf("remove blocked spawn path: %v", err)
	}
	if err := os.Rename(backupDir, spawnDir); err != nil {
		t.Fatalf("restore spawn directory: %v", err)
	}
	got, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read restored spawn record: %v", err)
	}
	if string(got) != string(oldRecord) {
		t.Fatalf("failed persistence changed durable record:\nold %s\nnew %s", oldRecord, got)
	}

	c.Send(map[string]any{"type": "gw_list_sessions", "id": "list-persist1"})
	listed := c.WaitResponse("list-persist1", testutil.DefaultTimeout)
	for _, item := range testutil.Arr(testutil.Obj(listed, "data"), "sessions") {
		row, _ := item.(map[string]any)
		if testutil.Str(row, "path") != path {
			continue
		}
		got := testutil.StrSlice(testutil.Obj(row, "spawn"), "extension")
		if len(got) != 2 || got[0] != "/old-extension.ts" || got[1] != "/new-extension.ts" {
			t.Fatalf("in-memory spawn after persistence failure = %v", got)
		}
		return
	}
	t.Fatalf("session %s missing from catalog after persistence error: %v", path, listed)
}

// TestAddPiArgsRejectsConflictsAndBusySessions ensures additions neither
// replace singleton settings nor force a restart under a second attachment.
func TestAddPiArgsRejectsConflictsAndBusySessions(t *testing.T) {
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	path := filepath.Join(t.TempDir(), "add-busy.jsonl")
	a := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--system-prompt", "original"}
	})
	a.WaitType("gw_welcome", testutil.DefaultTimeout)
	b := dial(t, addr, func(h *protocol.Hello) { h.Session = path })
	b.WaitType("gw_welcome", testutil.DefaultTimeout)

	invalid := []map[string]any{
		{"type": "gw_reload_session", "id": "invalid1", "addPiArgs": []string{"--unknown-flag"}},
		{"type": "gw_reload_session", "id": "invalid2", "addPiArgs": []string{"--model", "new-model"}},
		{"type": "gw_reload_session", "id": "invalid3", "piArgs": []string{"--extension", "/replacement.ts"}, "addPiArgs": []string{"--extension", "/extra.ts"}},
		{"type": "gw_reload_session", "id": "invalid4", "addPiArgs": []string{}},
	}
	for _, frame := range invalid {
		id := testutil.Str(frame, "id")
		a.Send(frame)
		resp := a.WaitResponse(id, testutil.DefaultTimeout)
		if resp["success"] != false || resp["code"] != protocol.CodeBadFrame {
			t.Fatalf("invalid additive request %s = %v, want bad_frame", id, resp)
		}
	}

	a.Send(map[string]any{
		"type": "gw_reload_session", "id": "conflict1",
		"addPiArgs": []string{"--system-prompt", "replacement"},
	})
	conflict := a.WaitResponse("conflict1", testutil.DefaultTimeout)
	if conflict["success"] != false || conflict["code"] != protocol.CodeSpawnParamConflict {
		t.Fatalf("conflicting addition = %v, want spawn_param_conflict", conflict)
	}

	a.Send(map[string]any{
		"type": "gw_reload_session", "id": "busy1", "force": true,
		"addPiArgs": []string{"--extension", "/opt/pi-chat/chat-tools.ts"},
	})
	busy := a.WaitResponse("busy1", testutil.DefaultTimeout)
	if busy["success"] != false || busy["code"] != protocol.CodeReloadBusy {
		t.Fatalf("shared additive restart = %v, want reload_busy despite force", busy)
	}
}

// TestAddPiArgsCannotForceRestartDuringTurn ensures additive adoption also
// refuses an active turn even when the request includes force:true.
func TestAddPiArgsCannotForceRestartDuringTurn(t *testing.T) {
	t.Setenv("FAKEPI_TURN_EVENTS", "100")
	t.Setenv("FAKEPI_TURN_DELAY_MS", "20")
	addr, _ := startDaemon(t, 30*time.Second, 5*time.Second)
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = filepath.Join(t.TempDir(), "add-turn.jsonl")
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	c.Send(map[string]any{"type": "prompt", "id": "prompt1", "message": "hold turn"})
	if resp := c.WaitResponse("prompt1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt failed: %v", resp)
	}
	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "busy-turn", "force": true,
		"addPiArgs": []string{"--extension", "/opt/pi-chat/chat-tools.ts"},
	})
	resp := c.WaitResponse("busy-turn", testutil.DefaultTimeout)
	if resp["success"] != false || resp["code"] != protocol.CodeReloadBusy {
		t.Fatalf("active-turn additive restart = %v, want reload_busy despite force", resp)
	}
}

// TestAddPiArgsFailedRestartDoesNotCommit checks that a failed Pi startup does
// not replace the authoritative record with arguments the process never loaded.
func TestAddPiArgsFailedRestartDoesNotCommit(t *testing.T) {
	stateDir := t.TempDir()
	sessionsDir := t.TempDir()
	t.Setenv("FAKEPI_EXIT_ON_ARG", "/fail-extension.ts")
	path := filepath.Join(sessionsDir, "add-failed.jsonl")
	addr, _ := startDaemonWithState(t, stateDir, sessionsDir, nil)
	c := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--extension", "/old-extension.ts"}
	})
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	c.Send(map[string]any{
		"type": "gw_reload_session", "id": "fail1",
		"addPiArgs": []string{"--extension", "/fail-extension.ts"},
	})
	resp := c.WaitResponse("fail1", testutil.DefaultTimeout)
	if resp["success"] != false {
		t.Fatalf("failed Pi startup reported success: %v", resp)
	}

	canon := resolvedPath(t, path)
	sum := sha256.Sum256([]byte(canon))
	recordPath := filepath.Join(config.SpawnDir(stateDir), hex.EncodeToString(sum[:])+".json")
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("read spawn record: %v", err)
	}
	var record struct {
		Spawn map[string][]string `json:"spawn"`
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("parse spawn record: %v", err)
	}
	got := record.Spawn["extension"]
	if len(got) != 1 || got[0] != "/old-extension.ts" {
		t.Fatalf("failed restart committed extensions %v, want only old extension", got)
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

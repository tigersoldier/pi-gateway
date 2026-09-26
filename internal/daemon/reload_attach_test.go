package daemon_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// slowRespawnPi wraps fake pi so that every start after the first sleeps,
// which makes a reload's restart window deterministic. The daemon's startup
// `--version` probe is answered without counting. The returned function
// reports how many times pi was actually started for a session.
func slowRespawnPi(t *testing.T, delay time.Duration) (bin string, starts func() int) {
	t.Helper()
	real, err := testutil.FakePi()
	if err != nil {
		t.Fatalf("fake pi: %v", err)
	}
	dir := t.TempDir()
	counter := filepath.Join(dir, "starts")
	bin = filepath.Join(dir, "pi-slow-respawn")
	script := fmt.Sprintf(`#!/bin/sh
if [ "$1" = "--version" ]; then exec %q "$@"; fi
n=$(cat %q 2>/dev/null || echo 0)
n=$((n+1))
echo "$n" > %q
if [ "$n" -ge 2 ]; then sleep %g; fi
exec %q "$@"
`, real, counter, counter, delay.Seconds(), real)
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write pi wrapper: %v", err)
	}
	return bin, func() int {
		raw, err := os.ReadFile(counter)
		if err != nil {
			return 0
		}
		n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			return 0
		}
		return n
	}
}

// TestAttachDuringReloadJoinsTheRestartingActor covers a client that attaches
// while a forced reload is mid-restart. The reloading actor still owns the
// session (it has a pi process starting up), so the new client must join it:
// replacing it would spawn a second pi on the same session file and orphan the
// first (docs/design.md §3.2 single-writer invariant, §4.4 reload).
func TestAttachDuringReloadJoinsTheRestartingActor(t *testing.T) {
	probe, starts := slowRespawnPi(t, 1500*time.Millisecond)
	real, err := testutil.FakePi()
	if err != nil {
		t.Fatalf("fake pi: %v", err)
	}
	_, addr := startDaemonHandle(t, func(c *daemon.Config) { c.PiBin = probe })

	a := dial(t, addr, nil)
	defer a.Close()
	a.Send(map[string]any{"type": "get_state", "id": "s1"})
	path := sessionPath(t, a.WaitResponse("s1", testutil.DefaultTimeout))

	// Force a reload: the wrapper makes the respawn take ~1.5s, so the
	// restarting window is wide enough to attach into deterministically.
	a.Send(map[string]any{"type": "gw_reload_session", "id": "r1", "force": true})
	waitFor(t, 10*time.Second, "the reload to respawn pi", func() bool { return starts() >= 2 })

	b := dial(t, addr, func(h *protocol.Hello) { h.Session = path })
	defer b.Close()
	b.Send(map[string]any{"type": "get_state", "id": "s2"})
	if got := sessionPath(t, b.WaitResponse("s2", testutil.DefaultTimeout)); got != path {
		t.Fatalf("client that attached mid-reload landed on %q, want %q", got, path)
	}

	if resp := a.WaitResponse("r1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("forced reload failed: %v", resp)
	}
	waitFor(t, 5*time.Second, "the reload to settle", func() bool {
		return testutil.CountProcesses(real) == 1
	})
	if n := testutil.CountProcesses(real); n != 1 {
		t.Fatalf("running pi processes = %d, want exactly 1 (one actor per session file)", n)
	}
	if n := starts(); n != 2 {
		t.Fatalf("pi was started %d times, want 2 (initial spawn + one reload)", n)
	}

	// Both clients are on the same live session.
	b.Send(map[string]any{"type": "prompt", "id": "p1", "message": "after reload"})
	if resp := b.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt after reload failed: %v", resp)
	}
	b.WaitType("agent_settled", 30*time.Second)
}

// TestLargeReplayResyncsInsteadOfClosing covers a resume whose replay window is
// legal (still in the hub ring) but larger than the connection's out queue.
// Sending it would fill the queue and close the client as a slow consumer, so
// the daemon must treat it as unreplayable and answer with a snapshot
// (docs/protocol.md §6).
func TestLargeReplayResyncsInsteadOfClosing(t *testing.T) {
	t.Setenv("FAKEPI_TURN_EVENTS", "1200")
	_, addr := startDaemonHandle(t, nil)

	a := dial(t, addr, nil)
	defer a.Close()
	a.Send(map[string]any{"type": "get_state", "id": "s0"})
	path := sessionPath(t, a.WaitResponse("s0", testutil.DefaultTimeout))
	a.Send(map[string]any{"type": "prompt", "id": "p1", "message": "emit a lot"})
	if resp := a.WaitResponse("p1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt failed: %v", resp)
	}
	a.WaitType("agent_settled", 60*time.Second)

	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.Resume = &protocol.Resume{SinceSeq: 0}
		h.LiveOnly = false
	})
	defer b.Close()
	w := b.WaitType("gw_welcome", testutil.DefaultTimeout)
	if w["resyncRequired"] != true {
		t.Fatalf("a replay larger than the out queue must require resync, got %v", w)
	}
	if snap := b.WaitType("gw_snapshot", testutil.DefaultTimeout); snap == nil {
		t.Fatal("no gw_snapshot after an unreplayable window")
	}
	// The connection survives: a live command still works.
	b.Send(map[string]any{"type": "get_state", "id": "s1"})
	if resp := b.WaitResponse("s1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("client was not usable after resync: %v", resp)
	}
}

// TestAttachRuntimeChangeIsAttributedToTheAttachingClient checks gw_state_changed.by
// for runtime parameters applied on attach: the client that asked for the
// change authored it, not the daemon (docs/protocol.md §6.4).
func TestAttachRuntimeChangeIsAttributedToTheAttachingClient(t *testing.T) {
	_, addr := startDaemonHandle(t, nil)
	path := filepath.Join(t.TempDir(), "attributed.jsonl")

	a, _, _ := dialSession(t, addr, path)
	defer a.Close()

	b := dial(t, addr, func(h *protocol.Hello) {
		h.Session = path
		h.PiArgs = []string{"--name", "renamed-by-b"}
	})
	defer b.Close()
	bID := testutil.Str(b.WaitType("gw_welcome", testutil.DefaultTimeout), "clientId")

	f := a.WaitFor(func(f map[string]any) bool {
		return f["type"] == "gw_state_changed" && testutil.Str(f, "command") == "set_session_name"
	}, testutil.DefaultTimeout)
	if got := testutil.Str(testutil.Obj(f, "by"), "clientId"); got != bID {
		t.Fatalf("gw_state_changed.by = %q, want the attaching client %q (%v)", got, bID, f)
	}
}

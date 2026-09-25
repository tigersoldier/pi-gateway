package daemon_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

// cwdProbePi writes the working directory of the spawned pi process to a file
// and then runs the real fake pi. Pointing the daemon's --pi at it makes the
// child's directory directly observable, instead of inferring it from state pi
// itself records.
func cwdProbePi(t *testing.T) (bin, record string) {
	t.Helper()
	record = filepath.Join(t.TempDir(), "pi-cwd")
	real, err := testutil.FakePi()
	if err != nil {
		t.Fatalf("fake pi: %v", err)
	}
	bin = filepath.Join(t.TempDir(), "pi-cwd-probe")
	script := "#!/bin/sh\npwd > \"" + record + "\"\nexec " + real + " \"$@\"\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write probe: %v", err)
	}
	return bin, record
}

func recordedCwd(t *testing.T, record string) string {
	t.Helper()
	raw, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("read recorded cwd: %v", err)
	}
	return strings.TrimSpace(string(raw))
}

// resolved returns a path with symlinks resolved, as pi's os.Getwd reports it.
func resolved(t *testing.T, dir string) string {
	t.Helper()
	out, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolve %s: %v", dir, err)
	}
	return out
}

// headerCwd reads the working directory recorded in a session file's header,
// independently of the catalog helper the daemon uses.
func headerCwd(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read session file: %v", err)
	}
	first, _, _ := strings.Cut(string(raw), "\n")
	var header struct {
		Type string `json:"type"`
		Cwd  string `json:"cwd"`
	}
	if err := json.Unmarshal([]byte(first), &header); err != nil {
		t.Fatalf("parse session header: %v", err)
	}
	if header.Type != "session" {
		t.Fatalf("first line is not a session header: %s", first)
	}
	return header.Cwd
}

func TestSessionSpawnsInClientWorkingDirectory(t *testing.T) {
	probe, record := cwdProbePi(t)
	dir := t.TempDir()
	_, addr := startDaemonHandle(t, func(c *daemon.Config) { c.PiBin = probe })

	c := dial(t, addr, func(h *protocol.Hello) { h.Cwd = dir })
	defer c.Close()
	c.Send(map[string]any{"type": "get_state", "id": "s1"})
	path := sessionPath(t, c.WaitResponse("s1", testutil.DefaultTimeout))

	if got, want := recordedCwd(t, record), resolved(t, dir); got != want {
		t.Fatalf("pi ran in %q, want the client's directory %q", got, want)
	}
	if got, want := headerCwd(t, path), resolved(t, dir); got != want {
		t.Fatalf("session header cwd = %q, want %q", got, want)
	}
}

func TestHibernatedSessionRespawnsInItsOwnDirectory(t *testing.T) {
	probe, record := cwdProbePi(t)
	dir := t.TempDir()
	other := t.TempDir()
	d, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.PiBin = probe
		c.IdleTimeout, c.ShortGrace = 200*time.Millisecond, 200*time.Millisecond
	})

	c := dial(t, addr, func(h *protocol.Hello) { h.Cwd = dir })
	c.Send(map[string]any{"type": "get_state", "id": "s1"})
	path := sessionPath(t, c.WaitResponse("s1", testutil.DefaultTimeout))
	c.Close()

	// Wait for the idle timeout to stop pi: the session file is now the only
	// record of the session's directory.
	deadline := time.Now().Add(15 * time.Second)
	for d.Status().Live != 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if live := d.Status().Live; live != 0 {
		t.Fatalf("session did not hibernate: %d live", live)
	}

	// Attach from a different directory: the session's own directory wins.
	b := dial(t, addr, func(h *protocol.Hello) {
		h.Cwd = other
		h.Session = path
	})
	defer b.Close()
	b.Send(map[string]any{"type": "get_state", "id": "s2"})
	b.WaitResponse("s2", testutil.DefaultTimeout)

	if got, want := recordedCwd(t, record), resolved(t, dir); got != want {
		t.Fatalf("respawned pi ran in %q, want the session's directory %q", got, want)
	}
}

func TestBadClientCwdIsRejected(t *testing.T) {
	_, addr := startDaemonHandle(t, nil)
	for _, bad := range []string{"relative/dir", "/definitely/not/a/real/directory"} {
		c := dial(t, addr, func(h *protocol.Hello) { h.Cwd = bad })
		err := c.WaitType("gw_error", testutil.DefaultTimeout)
		if testutil.Str(err, "code") != protocol.CodeBadFrame {
			t.Fatalf("cwd %q: code = %q, want %q (%v)",
				bad, testutil.Str(err, "code"), protocol.CodeBadFrame, err)
		}
		c.Close()
	}
}

func TestNewSessionCwdOverride(t *testing.T) {
	connectionDir := t.TempDir()
	overrideDir := t.TempDir()
	_, addr := startDaemonHandle(t, nil)

	c := dial(t, addr, func(h *protocol.Hello) { h.Cwd = connectionDir })
	defer c.Close()

	// Explicit gw_new_session without cwd uses the connection's directory.
	c.Send(map[string]any{"type": "gw_new_session", "id": "n1"})
	resp := c.WaitResponse("n1", testutil.DefaultTimeout)
	path := testutil.Str(testutil.Obj(resp, "data"), "path")
	if path == "" {
		t.Fatalf("gw_new_session returned no path: %v", resp)
	}
	if got, want := headerCwd(t, path), resolved(t, connectionDir); got != want {
		t.Fatalf("default cwd = %q, want %q", got, want)
	}

	// An explicit cwd overrides it.
	c.Send(map[string]any{"type": "gw_new_session", "id": "n2", "cwd": overrideDir})
	resp = c.WaitResponse("n2", testutil.DefaultTimeout)
	if path := testutil.Str(testutil.Obj(resp, "data"), "path"); path == "" {
		t.Fatalf("gw_new_session(cwd) returned no path: %v", resp)
	} else if got, want := headerCwd(t, path), resolved(t, overrideDir); got != want {
		t.Fatalf("override cwd = %q, want %q", got, want)
	}

	// A bad override is refused.
	c.Send(map[string]any{"type": "gw_new_session", "id": "n3", "cwd": "nope"})
	resp = c.WaitResponse("n3", testutil.DefaultTimeout)
	if testutil.Str(resp, "code") != protocol.CodeBadFrame {
		t.Fatalf("bad cwd override: %v", resp)
	}
}

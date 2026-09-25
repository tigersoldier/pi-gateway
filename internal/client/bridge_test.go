package client_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/client"
	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

const bridgeToken = "bridge-test-token-0123456789"

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.Cleanup()
	os.Exit(code)
}

func startDaemon(t *testing.T) (addr, tokenFile, piBin string) {
	t.Helper()
	piBin, err := testutil.FakePi()
	if err != nil {
		t.Skipf("cannot build fake pi: %v", err)
	}
	dir := t.TempDir()
	tokenFile = filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte(bridgeToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := daemon.New(daemon.Config{
		Addr:        "127.0.0.1:0",
		Token:       bridgeToken,
		PiBin:       piBin,
		IdleTimeout: 30 * time.Second,
		ShortGrace:  5 * time.Second,
		Logf:        func(format string, args ...any) { t.Logf(format, args...) },
	})
	if err := d.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		d.Shutdown()
	})
	return d.Addr().String(), tokenFile, piBin
}

// ui is a test-side UI attached to a bridge process over pipes.
type ui struct {
	t      *testing.T
	in     *os.File
	out    *os.File
	code   chan int
	frames chan map[string]any
}

func startBridge(t *testing.T, addr, tokenFile string, args ...string) *ui {
	t.Helper()
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	runArgs := append([]string{"--server", addr, "--token-file", tokenFile}, args...)
	code := make(chan int, 1)
	go func() {
		defer func() { _ = outW.Close() }()
		code <- client.Run(context.Background(), client.Options{
			Args:   runArgs,
			Stdin:  inR,
			Stdout: outW,
			Stderr: os.Stderr,
		})
	}()
	u := &ui{t: t, in: inW, out: outR, code: code, frames: make(chan map[string]any, 1024)}
	go func() {
		defer close(u.frames)
		reader := bufio.NewReader(outR)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				var m map[string]any
				if json.Unmarshal(line, &m) == nil {
					u.frames <- m
				}
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = inW.Close()
		_ = outR.Close()
	})
	return u
}

func (u *ui) send(v any) {
	u.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		u.t.Fatal(err)
	}
	if _, err := u.in.Write(append(b, '\n')); err != nil {
		u.t.Fatalf("write to bridge: %v", err)
	}
}

func (u *ui) waitFor(pred func(map[string]any) bool, timeout time.Duration) map[string]any {
	u.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-u.frames:
			if !ok {
				u.t.Fatalf("bridge stdout closed while waiting")
			}
			if pred(f) {
				return f
			}
		case <-deadline:
			u.t.Fatalf("timed out waiting for a frame")
		}
	}
}

func (u *ui) closeStdin() {
	u.t.Helper()
	_ = u.in.Close()
}

func (u *ui) waitExit(timeout time.Duration) int {
	u.t.Helper()
	select {
	case code := <-u.code:
		return code
	case <-time.After(timeout):
		u.t.Fatalf("bridge did not exit")
		return -1
	}
}

func TestBridgeRelaysPristinePiRPC(t *testing.T) {
	addr, tokenFile, piBin := startDaemon(t)
	u := startBridge(t, addr, tokenFile, "--mode", "rpc", "--approve")

	u.send(map[string]any{"type": "get_state", "id": "req-1"})
	resp := u.waitFor(func(f map[string]any) bool {
		return f["type"] == "response" && f["id"] == "req-1"
	}, testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("get_state through bridge failed: %v", resp)
	}
	data, _ := resp["data"].(map[string]any)
	file, _ := data["sessionFile"].(string)
	if file == "" {
		t.Fatalf("no sessionFile: %v", resp)
	}
	t.Cleanup(func() { _ = os.Remove(file) })
	assertNoGatewayFields(t, resp)

	u.send(map[string]any{"type": "prompt", "id": "req-2", "message": "through the bridge"})
	var frames []map[string]any
	for {
		f := u.waitFor(func(map[string]any) bool { return true }, testutil.DefaultTimeout)
		frames = append(frames, f)
		assertNoGatewayFields(t, f)
		if f["type"] == "agent_settled" {
			break
		}
	}
	if !testutil.HasMessageEnd(frames, "echo: through the bridge") {
		t.Fatalf("turn did not complete through the bridge")
	}
	if !hasResponse(frames, "req-2") {
		t.Fatalf("prompt response missing")
	}

	if n := testutil.CountProcesses(piBin); n != 1 {
		t.Fatalf("expected 1 pi process, got %d", n)
	}
	u.closeStdin()
	if code := u.waitExit(testutil.DefaultTimeout); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
}

func TestBridgeVersionHelpAndErrors(t *testing.T) {
	addr, tokenFile, _ := startDaemon(t)

	// --version needs no interactive relay: capture stdout directly.
	var stdout, stderr bytes.Buffer
	if code := client.Run(context.Background(), client.Options{
		Args:   []string{"--server", addr, "--token-file", tokenFile, "--version"},
		Stdin:  strings.NewReader(""),
		Stdout: &stdout,
		Stderr: &stderr,
	}); code != 0 {
		t.Fatalf("--version exit = %d (stderr: %s)", code, stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "fakepi 0.1.0" {
		t.Fatalf("--version printed %q", got)
	}

	stdout.Reset()
	stderr.Reset()
	if code := client.Run(context.Background(), client.Options{
		Args: []string{"--help"}, Stdout: &stdout, Stderr: &stderr,
	}); code != 0 {
		t.Fatalf("--help exit = %d", code)
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Fatalf("--help output: %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := client.Run(context.Background(), client.Options{
		Args: []string{"--mode", "tui"}, Stdout: &stdout, Stderr: &stderr,
	}); code != 2 {
		t.Fatalf("TUI mode exit = %d, want 2", code)
	}

	// A bare invocation is a TUI attempt and must be rejected too.
	stderr.Reset()
	if code := client.Run(context.Background(), client.Options{
		Args: []string{"--approve"}, Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr,
	}); code != 2 {
		t.Fatalf("bare invocation exit = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "--mode rpc") {
		t.Fatalf("bare invocation error should mention --mode rpc: %q", stderr.String())
	}

	stderr.Reset()
	if code := client.Run(context.Background(), client.Options{
		Args:   []string{"--server", addr, "--token-file", tokenFile, "--nope"},
		Stdout: &stdout, Stderr: &stderr,
	}); code != 2 {
		t.Fatalf("unknown pi option exit = %d, want 2", code)
	}
}

func TestBridgeRejectsWrongToken(t *testing.T) {
	addr, _, _ := startDaemon(t)
	badToken := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(badToken, []byte("not-the-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	code := client.Run(context.Background(), client.Options{
		Args:   []string{"--server", addr, "--token-file", badToken, "--mode", "rpc"},
		Stdin:  strings.NewReader(""),
		Stdout: &bytes.Buffer{},
		Stderr: &stderr,
	})
	if code != 3 {
		t.Fatalf("wrong token exit = %d, want 3", code)
	}
	if !strings.Contains(stderr.String(), "token") {
		t.Fatalf("stderr should explain the token failure: %q", stderr.String())
	}
}

// TestBridgeReattachAfterDisconnect is the ssh-drop scenario: the first bridge
// dies, and a second bridge re-attaches to the same session without restarting
// pi.
func TestBridgeReattachAfterDisconnect(t *testing.T) {
	addr, tokenFile, piBin := startDaemon(t)

	first := startBridge(t, addr, tokenFile, "--mode", "rpc")
	first.send(map[string]any{"type": "get_state", "id": "r1"})
	resp := first.waitFor(func(f map[string]any) bool {
		return f["type"] == "response" && f["id"] == "r1"
	}, testutil.DefaultTimeout)
	file, _ := resp["data"].(map[string]any)["sessionFile"].(string)
	t.Cleanup(func() { _ = os.Remove(file) })

	first.send(map[string]any{"type": "prompt", "id": "r2", "message": "before the drop"})
	first.waitFor(func(f map[string]any) bool { return f["type"] == "agent_settled" }, testutil.DefaultTimeout)
	first.closeStdin()
	first.waitExit(testutil.DefaultTimeout)

	// The session is still alive with one pi process.
	if n := testutil.CountProcesses(piBin); n != 1 {
		t.Fatalf("session did not survive the bridge: %d pi processes", n)
	}

	second := startBridge(t, addr, tokenFile, "--mode", "rpc")
	second.send(map[string]any{"type": "switch_session", "id": "s1", "sessionPath": file})
	if resp := second.waitFor(func(f map[string]any) bool {
		return f["type"] == "response" && f["id"] == "s1"
	}, testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("re-attach failed: %v", resp)
	}
	second.send(map[string]any{"type": "get_state", "id": "r3"})
	resp = second.waitFor(func(f map[string]any) bool {
		return f["type"] == "response" && f["id"] == "r3"
	}, testutil.DefaultTimeout)
	if got := resp["data"].(map[string]any)["sessionFile"].(string); got != file {
		t.Fatalf("re-attached to %q, want %q", got, file)
	}
	if n := testutil.CountProcesses(piBin); n != 1 {
		t.Fatalf("re-attach must not restart pi: %d processes", n)
	}

	// A prompt through the second bridge still runs.
	second.send(map[string]any{"type": "prompt", "id": "r4", "message": "after the drop"})
	second.waitFor(func(f map[string]any) bool {
		return testutil.MessageEndText(f) == "echo: after the drop"
	}, testutil.DefaultTimeout)
	second.closeStdin()
	if code := second.waitExit(testutil.DefaultTimeout); code != 0 {
		t.Fatalf("second bridge exit = %d", code)
	}
}

func assertNoGatewayFields(t *testing.T, frame map[string]any) {
	t.Helper()
	for key := range frame {
		if strings.HasPrefix(key, "gw_") {
			t.Fatalf("raw UI frame contains gateway field %q: %v", key, frame)
		}
	}
}

func hasResponse(frames []map[string]any, id string) bool {
	for _, f := range frames {
		if f["type"] == "response" && f["id"] == id {
			return true
		}
	}
	return false
}

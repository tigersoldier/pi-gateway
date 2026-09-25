// Package testutil builds the fake pi binary and provides a minimal raw
// gateway-protocol client for tests.
package testutil

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// DefaultTimeout is used when a test does not specify one.
const DefaultTimeout = 5 * time.Second

var (
	fakePiOnce sync.Once
	fakePiPath string
	fakePiDir  string
	fakePiErr  error
)

// FakePi returns the path to the compiled fake pi binary, building it once per
// test process.
func FakePi() (string, error) {
	fakePiOnce.Do(func() {
		dir, err := os.MkdirTemp("", "pi-gateway-fakepi-")
		if err != nil {
			fakePiErr = err
			return
		}
		fakePiDir = dir
		out := filepath.Join(dir, "fakepi")
		cmd := exec.Command("go", "build", "-o", out, "github.com/tigersoldier/pi-gateway/internal/fakepi")
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fakePiErr = err
			return
		}
		fakePiPath = out
	})
	return fakePiPath, fakePiErr
}

// Cleanup removes the fake pi build directory. Call from TestMain.
func Cleanup() {
	if fakePiDir != "" {
		_ = os.RemoveAll(fakePiDir)
	}
}

// CountProcesses returns the number of running processes whose command line
// contains substr. Linux-only, which is what the daemon targets.
func CountProcesses(substr string) int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return -1
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
		if err != nil {
			continue
		}
		if strings.Contains(strings.ReplaceAll(string(b), "\x00", " "), substr) {
			count++
		}
	}
	return count
}

// Conn is a raw gateway-protocol client: every frame becomes a map.
type Conn struct {
	t      *testing.T
	NC     net.Conn
	codec  *protocol.Codec
	frames chan map[string]any
	closed chan struct{}
}

// Dial connects, sends gw_hello, and returns without waiting for gw_welcome.
// mutate may adjust the default hello (kind, session, resume, …).
func Dial(t *testing.T, addr, token string, mutate func(*protocol.Hello)) *Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, DefaultTimeout)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	h := protocol.Hello{
		Type:     "gw_hello",
		Protocol: protocol.Version,
		Token:    token,
		Client: protocol.ClientInfo{
			Name:         t.Name(),
			Kind:         "test",
			Capabilities: protocol.AllCapabilities,
		},
		LiveOnly: true,
	}
	if mutate != nil {
		mutate(&h)
	}
	c := &Conn{
		t:      t,
		NC:     nc,
		codec:  protocol.NewCodec(nc, nc),
		frames: make(chan map[string]any, 4096),
		closed: make(chan struct{}),
	}
	if err := c.codec.WriteJSON(&h); err != nil {
		t.Fatalf("send gw_hello: %v", err)
	}
	go c.readLoop()
	t.Cleanup(c.Close)
	return c
}

func (c *Conn) readLoop() {
	defer close(c.closed)
	for {
		raw, err := c.codec.Read()
		if err != nil {
			close(c.frames)
			return
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		select {
		case c.frames <- m:
		case <-c.closed:
			return
		}
	}
}

// Send writes one frame.
func (c *Conn) Send(v any) {
	c.t.Helper()
	if err := c.codec.WriteJSON(v); err != nil {
		c.t.Fatalf("send: %v", err)
	}
}

// SendRaw writes a pre-encoded frame.
func (c *Conn) SendRaw(raw []byte) {
	c.t.Helper()
	if err := c.codec.WriteRaw(raw); err != nil {
		c.t.Fatalf("send raw: %v", err)
	}
}

// WaitFor returns the first frame matching pred, failing the test on timeout.
func (c *Conn) WaitFor(pred func(map[string]any) bool, timeout time.Duration) map[string]any {
	c.t.Helper()
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	deadline := time.After(timeout)
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				c.t.Fatalf("connection closed while waiting for a frame")
			}
			if pred(f) {
				return f
			}
		case <-deadline:
			c.t.Fatalf("timed out waiting for a matching frame")
		}
	}
}

// WaitType returns the next frame of the given type.
func (c *Conn) WaitType(typ string, timeout time.Duration) map[string]any {
	c.t.Helper()
	return c.WaitFor(func(f map[string]any) bool { return f["type"] == typ }, timeout)
}

// WaitResponse returns the response for a local id.
func (c *Conn) WaitResponse(id string, timeout time.Duration) map[string]any {
	c.t.Helper()
	return c.WaitFor(func(f map[string]any) bool {
		return f["type"] == "response" && f["id"] == id
	}, timeout)
}

// CollectUntil gathers frames until pred matches (inclusive) or the timeout.
func (c *Conn) CollectUntil(pred func(map[string]any) bool, timeout time.Duration) []map[string]any {
	c.t.Helper()
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	deadline := time.After(timeout)
	var out []map[string]any
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				c.t.Fatalf("connection closed while collecting frames")
			}
			out = append(out, f)
			if pred(f) {
				return out
			}
		case <-deadline:
			c.t.Fatalf("timed out collecting frames (got %d)", len(out))
		}
	}
}

// Drain returns every frame received within d.
func (c *Conn) Drain(d time.Duration) []map[string]any {
	var out []map[string]any
	deadline := time.After(d)
	for {
		select {
		case f, ok := <-c.frames:
			if !ok {
				return out
			}
			out = append(out, f)
		case <-deadline:
			return out
		}
	}
}

// Frames returns the decoded frame stream for tests that need to inspect
// every record rather than wait for a predicate.
func (c *Conn) Frames() <-chan map[string]any { return c.frames }

// Close shuts the connection down.
func (c *Conn) Close() { _ = c.NC.Close() }

// Str reads a string field from a decoded frame.
func Str(f map[string]any, key string) string {
	s, _ := f[key].(string)
	return s
}

// Obj reads a nested object from a decoded frame.
func Obj(f map[string]any, key string) map[string]any {
	m, _ := f[key].(map[string]any)
	return m
}

// Arr reads a nested array from a decoded frame.
func Arr(f map[string]any, key string) []any {
	a, _ := f[key].([]any)
	return a
}

// Num reads a numeric field as float64 (JSON numbers).
func Num(f map[string]any, key string) float64 {
	n, _ := f[key].(float64)
	return n
}

// MessageEndText extracts the assistant text from a message_end frame.
func MessageEndText(f map[string]any) string {
	msg := Obj(f, "message")
	for _, raw := range Arr(msg, "content") {
		if part, ok := raw.(map[string]any); ok && part["type"] == "text" {
			return Str(part, "text")
		}
	}
	return ""
}

// HasMessageEnd reports whether frames contain a message_end with text.
func HasMessageEnd(frames []map[string]any, text string) bool {
	for _, f := range frames {
		if f["type"] == "message_end" && MessageEndText(f) == text {
			return true
		}
	}
	return false
}

// WriteSession writes a session file (one JSON entry per line), creating
// parent directories. It is the shared writer for catalog and end-to-end
// session fixtures.
func WriteSession(t *testing.T, path string, lines ...any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	for _, line := range lines {
		raw, err := json.Marshal(line)
		if err != nil {
			t.Fatal(err)
		}
		buf.Write(append(raw, '\n'))
	}
	if err := os.WriteFile(path, []byte(buf.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// SessionHeader builds the mandatory first entry of a session file.
func SessionHeader(id, cwd, timestamp string) map[string]any {
	return map[string]any{
		"type": "session", "version": 3, "id": id, "cwd": cwd, "timestamp": timestamp,
	}
}

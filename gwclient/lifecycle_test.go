package gwclient_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/gwclient"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/protocol"
)

// TestDeleteSessionClearsBinding covers the client contract for the terminal
// session state: DeleteSession reports success and the file outcome, the
// deleted event clears the binding and the turn latch, and the connection
// stays usable.
func TestDeleteSessionClearsBinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bot.jsonl")
	addr, _ := gwtest.StartDaemon(t, testToken, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var deleted atomic.Int32
	c := dialTo(t, addr, func(cfg *gwclient.Config) {
		cfg.Session = path
		cfg.OnEvent = func(ev gwclient.Event) {
			if st, err := ev.SessionState(); err == nil && st.State == protocol.SessionStateDeleted {
				deleted.Add(1)
			}
		}
	})
	if c.Session() == nil || c.Session().Path != path {
		t.Fatalf("client is not attached to %s: %+v", path, c.Session())
	}
	if _, err := c.Prompt(ctx, "hello"); err != nil {
		t.Fatalf("prompt: %v", err)
	}
	if err := c.AwaitSettled(ctx); err != nil {
		t.Fatalf("await settled: %v", err)
	}

	resp, err := c.DeleteSession(ctx, "", false)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !resp.Success {
		t.Fatalf("delete failed: %+v", resp)
	}
	var data struct {
		Path        string `json:"path"`
		PiStopped   bool   `json:"piStopped"`
		FileDeleted bool   `json:"fileDeleted"`
	}
	if err := resp.Decode(&data); err != nil {
		t.Fatalf("decode delete data: %v", err)
	}
	if data.Path != path || !data.PiStopped || !data.FileDeleted {
		t.Fatalf("delete data = %+v, want %s stopped and deleted", data, path)
	}
	// The terminal event and the response travel on the same connection but are
	// enqueued by different goroutines (the request handler writes the response;
	// the session pump forwards the event), so wait for the event rather than
	// assuming the daemon ordered them.
	deadline := time.Now().Add(5 * time.Second)
	for deleted.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := deleted.Load(); got != 1 {
		t.Fatalf("deleted events = %d, want 1", got)
	}
	if c.Session() != nil {
		t.Fatalf("Session() = %+v after delete, want nil", c.Session())
	}
	if c.TurnRunning() {
		t.Fatal("turn latch still running after delete")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("session file still exists: %v", err)
	}

	// The connection survives the delete, unbound and usable.
	if _, err := c.ListSessions(ctx, gwclient.SessionFilter{}); err != nil {
		t.Fatalf("connection unusable after delete: %v", err)
	}
}

// TestStopSessionClearsBinding covers the stop half: stopping keeps the file,
// unbinds the requester, and leaves the client able to attach to the same
// session again.
func TestStopSessionClearsBinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stop.jsonl")
	addr, _ := gwtest.StartDaemon(t, testToken, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := dialTo(t, addr, func(cfg *gwclient.Config) { cfg.Session = path })
	resp, err := c.StopSession(ctx, "", false)
	if err != nil {
		t.Fatalf("stop: %v", err)
	}
	if !resp.Success {
		t.Fatalf("stop failed: %+v", resp)
	}
	if c.Session() != nil {
		t.Fatalf("Session() = %+v after stop, want nil", c.Session())
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stop must keep the session file: %v", err)
	}
	// The session still exists: re-attaching resolves it by path and works.
	if _, err := c.SwitchSession(ctx, path); err != nil {
		t.Fatalf("re-attach after stop: %v", err)
	}
	if c.Session() == nil || c.Session().Path != path {
		t.Fatalf("re-attach bound %+v, want %s", c.Session(), path)
	}
	if _, err := c.GetState(ctx); err != nil {
		t.Fatalf("get_state after re-attach: %v", err)
	}
}

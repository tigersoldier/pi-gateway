package daemon_test

import (
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

// startDaemonWithToken starts a daemon that authenticates with token.
func startDaemonWithToken(t *testing.T, token string) (*daemon.Daemon, string) {
	t.Helper()
	d, _ := gwtest.StartDaemonHandle(t, token, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
	})
	return d, d.Addr().String()
}

// observerDaemon starts a daemon whose restricted token has exactly cap.
func observerDaemon(t *testing.T, cap string) (*daemon.Daemon, string) {
	t.Helper()
	return startDaemonHandle(t, func(c *daemon.Config) {
		c.Tokens = []protocol.TokenGrant{{
			Name:         "restricted",
			Token:        "restricted-token",
			Capabilities: []string{cap},
		}}
	})
}

func dialWithToken(t *testing.T, addr, token string) *testutil.Conn {
	t.Helper()
	c := testutil.Dial(t, addr, token, nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)
	return c
}

// TestObserverCannotRunPrivilegedCommands is the M3 review regression: before
// the capability table, any authenticated token could run `bash` (arbitrary
// shell as the daemon user) and dump the transcript with `export_html`.
func TestObserverCannotRunPrivilegedCommands(t *testing.T) {
	d, addr := observerDaemon(t, protocol.CapObserve)
	c := dialWithToken(t, addr, "restricted-token")

	for _, command := range []string{"bash", "prompt", "steer", "set_model", "gw_new_session", "gw_reload_session"} {
		id := "cmd-" + command
		raw := map[string]any{"type": command, "id": id}
		switch command {
		case "bash":
			raw["command"] = "id"
		case "prompt", "steer":
			raw["message"] = "hi"
		}
		c.Send(raw)
		resp := c.WaitResponse(id, testutil.DefaultTimeout)
		if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeForbidden {
			t.Fatalf("%s as observer = %v, want forbidden", command, resp)
		}
	}

	// A refused command must not have created a session either.
	if snap := d.Status(); snap.Registered != 0 || snap.Live != 0 {
		t.Fatalf("refused commands created a session: %+v", snap)
	}
	// Reading is still allowed: listing, and exporting the transcript.
	c.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	if resp := c.WaitResponse("l1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("observer cannot list sessions: %v", resp)
	}
	c.Send(map[string]any{"type": "export_html", "id": "e1", "outputPath": "/dev/null"})
	if resp := c.WaitResponse("e1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("observer cannot export html: %v", resp)
	}
}

// TestPromptAndInterjectScopes verifies the gates are neither wider nor
// narrower than the table: prompt allows bash but not cancellation or shared
// state; interject allows cancellation but not prompt or shared state.
func TestPromptAndInterjectScopes(t *testing.T) {
	_, addr := observerDaemon(t, protocol.CapPrompt)
	promptOnly := dialWithToken(t, addr, "restricted-token")
	promptOnly.Send(map[string]any{"type": "bash", "id": "b1", "command": "true"})
	if resp := promptOnly.WaitResponse("b1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("prompt-capable client could not run bash: %v", resp)
	}
	for _, command := range []string{"set_model", "abort", "clear_queue"} {
		id := "forbidden-" + command
		promptOnly.Send(map[string]any{"type": command, "id": id})
		resp := promptOnly.WaitResponse(id, testutil.DefaultTimeout)
		if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeForbidden {
			t.Fatalf("prompt-only client sent %s: %v, want forbidden", command, resp)
		}
	}

	// interject covers steer and the cancellation primitives.
	_, addr2 := observerDaemon(t, protocol.CapInterject)
	interjector := dialWithToken(t, addr2, "restricted-token")
	for _, command := range []string{"steer", "abort", "clear_queue"} {
		raw := map[string]any{"type": command, "id": command}
		if command == "steer" {
			raw["message"] = "look left"
		}
		interjector.Send(raw)
		if resp := interjector.WaitResponse(command, testutil.DefaultTimeout); resp["success"] != true {
			t.Fatalf("interject-capable client could not %s: %v", command, resp)
		}
	}
	interjector.Send(map[string]any{"type": "bash", "id": "b2", "command": "true"})
	resp := interjector.WaitResponse("b2", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeForbidden {
		t.Fatalf("interject-only client ran bash: %v", resp)
	}
}

// TestNonObserveTokenReceivesNoTranscript is the second M3 review regression:
// a token without observe used to receive the whole event stream.
func TestNonObserveTokenReceivesNoTranscript(t *testing.T) {
	_, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.Tokens = []protocol.TokenGrant{{
			Name: "writer", Token: "writer-token", Capabilities: []string{protocol.CapPrompt},
		}}
	})
	full := dial(t, addr, nil)
	full.Send(map[string]any{"type": "get_state", "id": "g1"})
	path := sessionPath(t, full.WaitResponse("g1", testutil.DefaultTimeout))

	watcher := dialWithToken(t, addr, "writer-token")
	watcher.Send(map[string]any{"type": "switch_session", "id": "s1", "sessionPath": path})
	if resp := watcher.WaitResponse("s1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("writer could not attach: %v", resp)
	}
	watcher.Drain(100 * time.Millisecond)

	full.Send(prompt("p1", "secret transcript"))
	full.WaitResponse("p1", testutil.DefaultTimeout)
	full.WaitType("agent_settled", testutil.DefaultTimeout)

	frames := watcher.Drain(300 * time.Millisecond)
	for _, f := range frames {
		switch typ := testutil.Str(f, "type"); typ {
		case "message_start", "message_update", "message_end", "turn_start", "turn_end",
			"agent_start", "agent_end", "agent_settled", "gw_turn", "gw_queue", "gw_presence":
			t.Fatalf("non-observe client received %s: %v", typ, f)
		}
	}
	// Its own turn still works, and it sees the answer to its own command.
	watcher.Send(prompt("p2", "my turn"))
	if resp := watcher.WaitResponse("p2", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("writer prompt failed: %v", resp)
	}
}

func TestNonCanonicalCommandTypeIsRejected(t *testing.T) {
	d, addr := startDaemonHandle(t, nil)
	c := dial(t, addr, nil)
	c.Send(map[string]any{"type": "Get_State", "id": "g1"})
	err := c.WaitType("gw_error", testutil.DefaultTimeout)
	if testutil.Str(err, "code") != protocol.CodeBadFrame {
		t.Fatalf("case variant answer = %v", err)
	}
	if snap := d.Status(); snap.Registered != 0 {
		t.Fatalf("case variant created a session: %+v", snap)
	}
}

func TestEmptyDaemonTokenRejectsEverything(t *testing.T) {
	d, addr := startDaemonWithToken(t, "")
	bad := testutil.Dial(t, addr, "", nil)
	err := bad.WaitType("gw_error", testutil.DefaultTimeout)
	if testutil.Str(err, "code") != protocol.CodeUnauthorized {
		t.Fatalf("empty token accepted: %v", err)
	}
	if got := d.Metrics().Value(metrics.Unauthorized); got != 1 {
		t.Fatalf("unauthorized = %d, want 1", got)
	}
}

func TestAttachFailuresAreCounted(t *testing.T) {
	d, addr := startDaemonHandle(t, nil)
	c := dial(t, addr, nil)
	c.Send(map[string]any{"type": "switch_session", "id": "s1", "session": "definitely-not-a-session"})
	resp := c.WaitResponse("s1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeUnknownSession {
		t.Fatalf("attach to an unknown name = %v", resp)
	}
	if got := d.Metrics().Value(metrics.AttachFailures); got != 1 {
		t.Fatalf("attach_failures = %d, want 1", got)
	}
}

// TestNonLoopbackListenerRefused: the unauthenticated debug listener must not
// be bindable to a routable address.
func TestNonLoopbackListenerRefused(t *testing.T) {
	for _, addr := range []string{"0.0.0.0:7331", ":7331", "10.1.2.3:7331"} {
		d := daemon.New(daemon.Config{Addr: addr, Token: "t"})
		err := d.Listen()
		if err == nil {
			d.Shutdown()
			t.Fatalf("Listen(%q) was accepted", addr)
		}
		if !strings.Contains(err.Error(), "loopback") {
			t.Fatalf("Listen(%q) error = %v", addr, err)
		}
	}
}

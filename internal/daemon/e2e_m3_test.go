package daemon_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/debughttp"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

// startDaemonHandle starts a daemon and returns it together with its address,
// for tests that exercise the operational surface.
func startDaemonHandle(t *testing.T, mutate func(*daemon.Config)) (*daemon.Daemon, string) {
	t.Helper()
	d, _ := gwtest.StartDaemonHandle(t, testToken, func(c *daemon.Config) {
		c.IdleTimeout, c.ShortGrace = 30*time.Second, 5*time.Second
		if mutate != nil {
			mutate(c)
		}
	})
	return d, d.Addr().String()
}

func TestRestrictedTokenGrantsOnlyItsRole(t *testing.T) {
	_, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.Tokens = []protocol.TokenGrant{{
			Name:         "dashboard",
			Token:        "observer-token",
			Capabilities: []string{protocol.CapObserve},
		}}
	})

	// The provisioned token authenticates but is limited to its role.
	c := testutil.Dial(t, addr, "observer-token", nil)
	w := c.WaitType("gw_welcome", testutil.DefaultTimeout)
	if got := strings.Join(testutil.StrSlice(w, "granted"), ","); got != protocol.CapObserve {
		t.Fatalf("granted = %q, want %q", got, protocol.CapObserve)
	}
	c.Send(map[string]any{"type": "gw_list_sessions", "id": "l1"})
	if resp := c.WaitResponse("l1", testutil.DefaultTimeout); resp["success"] != true {
		t.Fatalf("observer could not list sessions: %v", resp)
	}

	// A prompt exceeds the role.
	c.Send(prompt("p1", "hello"))
	resp := c.WaitResponse("p1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeForbidden {
		t.Fatalf("prompt as observer = %v", resp)
	}
	c.Close()

	// The daemon-generated token still has full authority.
	full := dial(t, addr, nil)
	fullW := full.WaitType("gw_welcome", testutil.DefaultTimeout)
	if len(testutil.StrSlice(fullW, "granted")) != len(protocol.AllCapabilities) {
		t.Fatalf("default token granted = %v", testutil.Arr(fullW, "granted"))
	}
	full.Close()
}

func TestGetQueriesRequireObserve(t *testing.T) {
	d, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.Tokens = []protocol.TokenGrant{{
			Name:         "writer",
			Token:        "prompt-only",
			Capabilities: []string{protocol.CapPrompt},
		}}
	})
	c := testutil.Dial(t, addr, "prompt-only", nil)
	c.WaitType("gw_welcome", testutil.DefaultTimeout)

	// get_* is an observation: refused, and it must not create a session.
	c.Send(map[string]any{"type": "get_state", "id": "g1"})
	resp := c.WaitResponse("g1", testutil.DefaultTimeout)
	if resp["success"] != false || testutil.Str(resp, "code") != protocol.CodeForbidden {
		t.Fatalf("get_state without observe = %v", resp)
	}
	if snap := d.Status(); snap.Registered != 0 {
		t.Fatalf("a refused get_state created a session: %+v", snap)
	}

	// The prompt capability still works, and lazily creates a session.
	c.Send(prompt("p1", "hello"))
	resp = c.WaitResponse("p1", testutil.DefaultTimeout)
	if resp["success"] != true {
		t.Fatalf("prompt with prompt-only token = %v", resp)
	}
}

func TestDebugEndpointsReportLiveState(t *testing.T) {
	// Keep the fake pi's session file inside the catalog root the daemon scans,
	// so /catalog exercises the file scan and the live decoration together.
	dir := t.TempDir()
	d, addr := startDaemonHandle(t, func(c *daemon.Config) {
		c.CatalogRoots = append(c.CatalogRoots, dir)
	})
	c := testutil.Dial(t, addr, testToken, func(h *protocol.Hello) {
		h.PiArgs = []string{"--session-dir", dir}
	})
	// pilish's flow: the first get_state creates the session and registers its
	// file path with the daemon.
	c.Send(map[string]any{"type": "get_state", "id": "g1"})
	if path := sessionPath(t, c.WaitResponse("g1", testutil.DefaultTimeout)); path == "" {
		t.Fatal("no session file reported")
	}
	c.Send(prompt("p1", "debug me"))
	c.WaitResponse("p1", testutil.DefaultTimeout)
	// agent_settled is the last event of a fake turn: the file writes and the
	// idle transition have happened by then.
	c.WaitType("agent_settled", testutil.DefaultTimeout)

	srv := httptest.NewServer(debughttp.Handler(d, debughttp.Options{
		Version: "0.2.0", Addr: addr, Started: d.Started(),
	}))
	defer srv.Close()

	// No token: the debug listener is deliberately unauthenticated.
	body := httpGet(t, srv.URL+"/status")
	for _, want := range []string{
		`"version":"0.2.0"`, `"registered":1`, `"live":1`, `"tokens":1`,
		`"state":"ready"`, `"subscribers":1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/status missing %s:\n%s", want, body)
		}
	}
	catalog := httpGet(t, srv.URL+"/catalog")
	for _, want := range []string{`"messageCount":2`, `"live":true`, `"isStreaming":false`, `"title":"debug me"`} {
		if !strings.Contains(catalog, want) {
			t.Fatalf("/catalog missing %s:\n%s", want, catalog)
		}
	}
	if !strings.Contains(catalog, dir) {
		t.Fatalf("/catalog did not list the session file in %s:\n%s", dir, catalog)
	}
	m := httpGet(t, srv.URL+"/metrics")
	for _, want := range []string{
		"pi_gateway_sessions_started_total 1",
		"pi_gateway_turns_started_total 1",
		"pi_gateway_frames_in_total",
		"# TYPE pi_gateway_sessions gauge",
	} {
		if !strings.Contains(m, want) {
			t.Fatalf("/metrics missing %q:\n%s", want, m)
		}
	}

	// Live gauges follow the daemon, not just the scrape.
	snap := d.Status()
	if snap.Live != 1 || snap.Registered != 1 || snap.Clients != 1 || snap.Tokens != 1 {
		t.Fatalf("Status = %+v", snap)
	}
	if got := d.Metrics().Value(metrics.Attaches); got != 1 {
		t.Fatalf("attaches = %d, want 1", got)
	}
	if got := d.Metrics().Value(metrics.PromptsQueued); got != 0 {
		t.Fatalf("queued prompts = %d, want 0 (ran immediately)", got)
	}
	if got := d.Metrics().Value(metrics.TurnsSettled); got != 1 {
		t.Fatalf("settled turns = %d, want 1", got)
	}
}

func TestSetTokensReplacesTheTable(t *testing.T) {
	d, addr := startDaemonHandle(t, nil)

	if err := d.SetTokens([]protocol.TokenGrant{{
		Name: "bot", Token: "bot-token", Capabilities: []string{protocol.CapObserve},
	}}); err != nil {
		t.Fatal(err)
	}
	bot := testutil.Dial(t, addr, "bot-token", nil)
	w := bot.WaitType("gw_welcome", testutil.DefaultTimeout)
	if got := strings.Join(testutil.StrSlice(w, "granted"), ","); got != protocol.CapObserve {
		t.Fatalf("bot granted = %q", got)
	}
	bot.Close()

	// Duplicates are rejected, including a token equal to the default one.
	if err := d.SetTokens([]protocol.TokenGrant{{Name: "dup", Token: testToken}}); err == nil {
		t.Fatal("expected an error for a duplicate token value")
	}
	if err := d.SetTokens([]protocol.TokenGrant{{Name: "empty"}}); err == nil {
		t.Fatal("expected an error for an empty token value")
	}

	// A reload drops the old table.
	if err := d.SetTokens(nil); err != nil {
		t.Fatal(err)
	}
	gone := testutil.Dial(t, addr, "bot-token", nil)
	rejected := gone.WaitType("gw_error", testutil.DefaultTimeout)
	if testutil.Str(rejected, "code") != protocol.CodeUnauthorized {
		t.Fatalf("removed token was still accepted: %v", rejected)
	}
	if names := d.TokenNames(); len(names) != 1 || names[0] != "default" {
		t.Fatalf("TokenNames = %v", names)
	}
}

func TestUnauthorizedConnectionsAreCounted(t *testing.T) {
	d, addr := startDaemonHandle(t, nil)
	bad := testutil.Dial(t, addr, "nope", nil)
	bad.WaitType("gw_error", testutil.DefaultTimeout)
	if got := d.Metrics().Value(metrics.Unauthorized); got != 1 {
		t.Fatalf("unauthorized = %d, want 1", got)
	}
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

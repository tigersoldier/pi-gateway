package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/gwtest"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

// tokenAccepted reports whether the daemon welcomes a handshake with token.
func tokenAccepted(t *testing.T, addr, token string) bool {
	t.Helper()
	c := testutil.Dial(t, addr, token, nil)
	defer c.Close()
	f := c.WaitFor(func(f map[string]any) bool {
		return f["type"] == "gw_welcome" || f["type"] == "gw_error"
	}, testutil.DefaultTimeout)
	return f["type"] == "gw_welcome"
}

// waitTokenAccepted polls until the handshake outcome matches want.
func waitTokenAccepted(t *testing.T, addr, token string, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tokenAccepted(t, addr, token) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("token %q never became accepted=%v", token, want)
}

// TestSIGHUPReloadsProvisionedTokens covers the daemon-side reload wiring:
// a provisioned token is only accepted after the signal, and a broken file
// leaves the running table untouched.
func TestSIGHUPReloadsProvisionedTokens(t *testing.T) {
	tokensPath := filepath.Join(t.TempDir(), "tokens.json")
	d, _ := gwtest.StartDaemonHandle(t, "default-token", nil)
	addr := d.Addr().String()
	log := gwtest.Logger(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hup := make(chan os.Signal, 1)
	go watchTokens(ctx, hup, tokensPath, d, log)

	if tokenAccepted(t, addr, "provisioned-token") {
		t.Fatal("an unknown token was accepted before provisioning")
	}

	grant, err := config.AddToken(tokensPath, "bot", "operator", nil)
	if err != nil {
		t.Fatalf("AddToken: %v", err)
	}
	if grant.Token == "" {
		t.Fatal("AddToken returned no token value")
	}
	if tokenAccepted(t, addr, grant.Token) {
		t.Fatal("a provisioned token was accepted without a reload")
	}

	hup <- syscall.SIGHUP
	waitTokenAccepted(t, addr, grant.Token, true, 10*time.Second)
	if names := strings.Join(d.TokenNames(), ","); !strings.Contains(names, "bot") {
		t.Fatalf("reloaded table does not list the new token: %q", names)
	}

	// A file the daemon cannot parse must not drop the live table.
	if err := os.WriteFile(tokensPath, []byte("{ this is not json"), 0o600); err != nil {
		t.Fatalf("write broken file: %v", err)
	}
	hup <- syscall.SIGHUP
	time.Sleep(300 * time.Millisecond)
	if !tokenAccepted(t, addr, grant.Token) {
		t.Fatal("a broken tokens file removed a working token")
	}
	if !tokenAccepted(t, addr, "default-token") {
		t.Fatal("a broken tokens file removed the default token")
	}
}

// TestPackagedUnitMatchesTheDaemon guards the shipped systemd unit against
// drift from the daemon's real behaviour.
func TestPackagedUnitMatchesTheDaemon(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "packaging", "pi-gatewayd.service"))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	unit := string(raw)
	for _, want := range []string{
		"ExecStart=%h/.local/bin/pi-gatewayd",
		"Type=simple",
		"Restart=on-failure",
		"ExecReload=/bin/kill -HUP $MAINPID", // SIGHUP reloads the token table
		"KillSignal=SIGTERM",
		"TimeoutStopSec=30",
	} {
		if !strings.Contains(unit, want) {
			t.Errorf("unit is missing %q", want)
		}
	}
	// Every Environment= line must stay commented: the daemon reads its config
	// from flags and the state directory, and a stray env line would silently
	// change behaviour once installed.
	for _, line := range strings.Split(unit, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Environment=") {
			t.Errorf("active Environment line in the shipped unit: %q", trimmed)
		}
	}
}

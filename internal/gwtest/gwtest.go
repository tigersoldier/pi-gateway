// Package gwtest starts a pi-gateway daemon for end-to-end tests. It lives
// outside internal/testutil so packages whose tests also use testutil (for
// example internal/session) do not pull in the daemon and create a cycle.
package gwtest

import (
	"context"
	"testing"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/gwlog"
	"github.com/tigersoldier/pi-gateway/internal/testutil"
)

// Logger routes a daemon's log records through the test log.
func Logger(t *testing.T) gwlog.Logger {
	t.Helper()
	return gwlog.FromLogf(func(format string, args ...any) { t.Logf(format, args...) })
}

// StartDaemon runs a daemon with the fake pi binary for the test's duration
// and returns its address and the fake pi binary path (used to count live pi
// processes). mutate may adjust the configuration before it starts.
func StartDaemon(t *testing.T, token string, mutate func(*daemon.Config)) (addr, piBin string) {
	t.Helper()
	d, piBin := StartDaemonHandle(t, token, mutate)
	return d.Addr().String(), piBin
}

// StartDaemonHandle is StartDaemon but also returns the daemon, for tests that
// exercise the operational surface (status, catalog, metrics).
func StartDaemonHandle(t *testing.T, token string, mutate func(*daemon.Config)) (*daemon.Daemon, string) {
	t.Helper()
	piBin, err := testutil.FakePi()
	if err != nil {
		t.Skipf("cannot build fake pi: %v", err)
	}
	// Keep the catalog scan hermetic: without this the daemon would scan the
	// developer's real ~/.pi/agent/sessions on every gw_list_sessions.
	t.Setenv("PI_CODING_AGENT_SESSION_DIR", t.TempDir())
	cfg := daemon.Config{
		Addr:     "127.0.0.1:0",
		Token:    token,
		PiBin:    piBin,
		StateDir: t.TempDir(),
		Log:      Logger(t),
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
	t.Cleanup(func() {
		cancel()
		<-done
		d.Shutdown()
	})
	return d, piBin
}

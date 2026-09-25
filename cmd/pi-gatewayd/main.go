// Command pi-gatewayd is the pi-gateway session daemon: it owns pi processes,
// keeps sessions alive across client disconnects, and speaks the gateway
// protocol on loopback TCP.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/debughttp"
	"github.com/tigersoldier/pi-gateway/internal/gwlog"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// version is the daemon's own version (not pi's).
const version = "0.2.0"

// defaultDebugAddr is the loopback address of the read-only debug listener.
const defaultDebugAddr = "127.0.0.1:7332"

func main() {
	fs := flag.NewFlagSet("pi-gatewayd", flag.ExitOnError)
	var (
		listen      = fs.String("listen", config.DefaultAddr, "loopback address to listen on")
		port        = fs.Int("port", 0, "loopback port (overrides --listen)")
		tokenFile   = fs.String("token-file", "", "token file (default: <state-dir>/token)")
		tokensFile  = fs.String("tokens-file", "", "optional JSON file of restricted tokens (default: <state-dir>/tokens.json)")
		stateDir    = fs.String("state-dir", "", "state directory (default: ~/.config/pi-gateway)")
		piBin       = fs.String("pi", "pi", "pi binary to manage")
		idleTimeout = fs.Duration("idle-timeout", 15*time.Minute,
			"keep a session warm this long after the last client detaches")
		shortGrace = fs.Duration("short-grace", 10*time.Second,
			"grace for sessions that never received a message and have no clients")
		sessionDirs = &stringList{}
		deltaFlush  = fs.Duration("delta-flush", 50*time.Millisecond,
			"coalescing window for streaming deltas (per client)")
		logLevel    = fs.String("log-level", "info", "log level: debug, info, warn, or error")
		logFormat   = fs.String("log-format", "text", "log format: text or json")
		debugAddr   = fs.String("debug-addr", defaultDebugAddr, "loopback address of the read-only debug listener")
		debugPort   = fs.Int("debug-port", 0, "debug listener port (overrides --debug-addr)")
		noDebug     = fs.Bool("no-debug", false, "disable the debug listener")
		provision   = fs.Bool("provision-token", false, "create a restricted token in the tokens file and print it")
		tokenName   = fs.String("token-name", "", "token name for --provision-token")
		tokenRole   = fs.String("token-role", "", "role for --provision-token: "+strings.Join(protocol.RoleNames(), ", "))
		tokenCaps   = fs.String("token-caps", "", "comma-separated capabilities for --provision-token (alternative to --token-role)")
		showVersion = fs.Bool("version", false, "print pi-gatewayd's own version and exit")
	)
	fs.Var(sessionDirs, "session-dir",
		"extra session directory to scan for the catalog (repeatable)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: pi-gatewayd [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Exit: SIGINT/SIGTERM stop all sessions; SIGHUP reloads the tokens file.
Debug listener (read-only, unauthenticated, loopback): /status /catalog /metrics
`)
	}
	_ = fs.Parse(os.Args[1:])

	if *showVersion {
		fmt.Printf("pi-gatewayd %s (gateway protocol %d)\n", version, protocol.Version)
		return
	}

	dir := config.Dir()
	if *stateDir != "" {
		dir = *stateDir
	}
	logPath := *tokenFile
	if logPath == "" {
		logPath = config.TokenPath(dir)
	}
	grantsPath := *tokensFile
	if grantsPath == "" {
		grantsPath = config.TokensPath(dir)
	}

	if *provision {
		provisionToken(grantsPath, *tokenName, *tokenRole, splitList(*tokenCaps))
		return
	}

	log, err := gwlog.New(os.Stderr, *logLevel, *logFormat)
	if err != nil {
		fatal(err)
	}

	token, err := config.LoadOrCreateToken(logPath)
	if err != nil {
		fatal(err)
	}
	grants, err := config.LoadTokens(grantsPath)
	if err != nil {
		fatal(err)
	}

	addr := *listen
	if *port > 0 {
		addr = net.JoinHostPort(config.DefaultHost, strconv.Itoa(*port))
	}
	d := daemon.New(daemon.Config{
		Addr:         addr,
		Token:        token,
		PortFile:     config.PortPath(dir),
		PiBin:        *piBin,
		IdleTimeout:  *idleTimeout,
		ShortGrace:   *shortGrace,
		CatalogRoots: sessionDirs.Values,
		DeltaFlush:   *deltaFlush,
		Log:          log,
		Metrics:      metrics.New(),
		Tokens:       grants,
	})
	if err := d.Listen(); err != nil {
		fatal(err)
	}
	piVersion := d.PiVersion()
	if piVersion == "" {
		piVersion = "unknown"
	}
	log.Info("daemon started",
		"version", version,
		"protocol", protocol.Version,
		"addr", d.Addr().String(),
		"tokenFile", logPath,
		"tokensFile", grantsPath,
		"tokens", strings.Join(d.TokenNames(), ","),
		"pi", piVersion,
		"catalogRoots", strings.Join(d.CatalogRoots(), ","))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	debugDone := startDebug(ctx, *noDebug, *debugAddr, *debugPort, dir, d, log)

	// SIGHUP reloads the token table, so a freshly provisioned integration is
	// accepted without a restart.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				updated, err := config.LoadTokens(grantsPath)
				if err != nil {
					log.Error("token reload failed", "err", err, "tokensFile", grantsPath)
					continue
				}
				if err := d.SetTokens(updated); err != nil {
					log.Error("token reload rejected", "err", err)
					continue
				}
				log.Info("tokens reloaded", "tokens", strings.Join(d.TokenNames(), ","))
			}
		}
	}()

	if err := d.Serve(ctx); err != nil {
		fatal(err)
	}
	if debugDone != nil {
		<-debugDone
	}
	log.Info("daemon stopped")
}

// startDebug binds the read-only debug listener and returns a channel closed
// when it has shut down (nil when disabled). The server follows ctx.
func startDebug(ctx context.Context, disabled bool, addr string, port int, dir string,
	d *daemon.Daemon, log gwlog.Logger) <-chan struct{} {
	portFile := config.DebugPortPath(dir)
	if disabled {
		config.RemovePortFile(portFile)
		return nil
	}
	if port > 0 {
		addr = net.JoinHostPort(config.DefaultHost, strconv.Itoa(port))
	}
	opts := debughttp.Options{Version: version, Addr: d.Addr().String(), Started: d.Started()}
	srv, ln, err := debughttp.Listen(addr, d, opts, log)
	if err != nil {
		log.Error("debug listener disabled", "err", err)
		config.RemovePortFile(portFile)
		return nil
	}

	boundPort, err := config.PortOf(ln.Addr().String())
	if err == nil {
		if err := config.WritePort(portFile, boundPort); err != nil {
			log.Warn("cannot write debug port file", "err", err, "path", portFile)
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("debug listener stopped", "err", err)
		}
		config.RemovePortIfMatches(portFile, boundPort)
	}()
	go func() {
		// The debug listener follows the daemon: closing it on shutdown lets
		// the process exit without waiting for open keep-alive connections.
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	return done
}

// provisionToken mints a restricted token, appends it to the tokens file, and
// prints only the token on stdout so scripts can capture it.
func provisionToken(path, name, role string, caps []string) {
	if name == "" {
		fatal(fmt.Errorf("--provision-token requires --token-name"))
	}
	if role != "" && len(caps) > 0 {
		fatal(fmt.Errorf("--token-role and --token-caps are mutually exclusive"))
	}
	if role == "" && len(caps) == 0 {
		fatal(fmt.Errorf("--provision-token requires --token-role or --token-caps"))
	}
	grant, err := config.AddToken(path, name, role, caps)
	if err != nil {
		fatal(err)
	}
	spec := config.TokenSpec{Name: grant.Name, Value: grant.Token, Role: role, Capabilities: caps}
	fmt.Fprintln(os.Stderr, "pi-gatewayd: provisioned token "+grant.Name+" ("+spec.Describe()+") in "+path)
	fmt.Fprintln(os.Stderr, "pi-gatewayd: run `systemctl --user reload pi-gatewayd` (SIGHUP) to accept it")
	fmt.Println(grant.Token)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "pi-gatewayd: %v\n", err)
	os.Exit(1)
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// stringList collects a repeatable string flag.
type stringList struct {
	Values []string
}

func (s *stringList) String() string { return strings.Join(s.Values, ", ") }

func (s *stringList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("empty value")
	}
	s.Values = append(s.Values, value)
	return nil
}

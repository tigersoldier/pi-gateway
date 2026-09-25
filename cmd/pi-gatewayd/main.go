// Command pi-gatewayd is the pi-gateway session daemon: it owns pi processes,
// keeps sessions alive across client disconnects, and speaks the gateway
// protocol on loopback TCP.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// version is the daemon's own version (not pi's).
const version = "0.1.0"

func main() {
	fs := flag.NewFlagSet("pi-gatewayd", flag.ExitOnError)
	var (
		listen      = fs.String("listen", config.DefaultAddr, "loopback address to listen on")
		port        = fs.Int("port", 0, "loopback port (overrides --listen)")
		tokenFile   = fs.String("token-file", "", "token file (default: <state-dir>/token)")
		stateDir    = fs.String("state-dir", "", "state directory (default: ~/.config/pi-gateway)")
		piBin       = fs.String("pi", "pi", "pi binary to manage")
		idleTimeout = fs.Duration("idle-timeout", 15*time.Minute,
			"keep a session warm this long after the last client detaches")
		shortGrace = fs.Duration("short-grace", 10*time.Second,
			"grace for sessions that never received a message and have no clients")
		verbose     = fs.Bool("verbose", false, "log session lifecycle and errors")
		showVersion = fs.Bool("version", false, "print pi-gatewayd's own version and exit")
	)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: pi-gatewayd [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExit: SIGINT/SIGTERM shut down all sessions.\n")
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
	tokenPath := *tokenFile
	if tokenPath == "" {
		tokenPath = config.TokenPath(dir)
	}
	token, err := config.LoadOrCreateToken(tokenPath)
	if err != nil {
		fatal(err)
	}

	addr := *listen
	if *port > 0 {
		addr = net.JoinHostPort(config.DefaultHost, strconv.Itoa(*port))
	}
	logf := func(string, ...any) {}
	if *verbose {
		log.SetFlags(0)
		log.SetOutput(os.Stderr)
		logf = log.Printf
	}

	d := daemon.New(daemon.Config{
		Addr:        addr,
		Token:       token,
		PortFile:    config.PortPath(dir),
		PiBin:       *piBin,
		IdleTimeout: *idleTimeout,
		ShortGrace:  *shortGrace,
		Logf:        logf,
	})
	if err := d.Listen(); err != nil {
		fatal(err)
	}
	piVersion := d.PiVersion()
	if piVersion == "" {
		piVersion = "unknown"
	}
	fmt.Fprintf(os.Stderr, "pi-gatewayd %s listening on %s (token %s, pi %s)\n",
		version, d.Addr(), tokenPath, piVersion)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := d.Serve(ctx); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "pi-gatewayd: %v\n", err)
	os.Exit(1)
}

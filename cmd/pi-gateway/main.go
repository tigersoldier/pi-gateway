// Command pi-gateway brokers one pi RPC session across many clients.
//
// Two roles:
//
//	pi-gateway serve    owns pi and speaks the gateway protocol (gw_hello/gw_*)
//	pi-gateway connect  a bridge client: raw pi RPC on stdio/TCP <-> gateway
//
// A third-party UI that spawns `pi --mode rpc` instead spawns the bridge:
//
//	pi-gateway connect --stdio --server 127.0.0.1:7331 -- --provider openai
//
// Flags after `--` are passed through to pi by `serve`.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/example/pi-gateway/internal/gateway"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "connect":
		runConnect(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	log.SetOutput(os.Stderr)
	log.SetPrefix("")
	log.Printf(`usage:
  pi-gateway serve   [flags] [-- pi flags...]
  pi-gateway connect [flags]

serve flags:
  --listen addr        TCP+JSONL address (default 127.0.0.1:7331)
  --pi path            pi binary (default "pi")
  --session file       session JSONL file passed to pi as --session
  --session-id id      gateway session id clients must use
  --mode mode          exclusive|queue|owner-only|read-only
  --hub-capacity n     replay ring capacity

connect flags:
  --server addr        gateway server address (default 127.0.0.1:7331)
  --session-id id      session id on the server (default "default")
  --stdio              bridge raw pi RPC on stdin/stdout (default true)
  --listen addr        additionally accept raw pi RPC over TCP
  --capabilities csv   observe,interject,prompt,ui,control
  --replay             forward replayed history to the raw peer`)
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:7331", "TCP+JSONL listen address")
	piBin := fs.String("pi", "pi", "pi binary to spawn")
	sessionFile := fs.String("session", "", "session JSONL file passed to pi as --session")
	sessionID := fs.String("session-id", "", "gateway session id clients must use")
	mode := fs.String("mode", string(gateway.ModeExclusive), "concurrency mode: exclusive|queue|owner-only|read-only")
	hubCap := fs.Int("hub-capacity", 8192, "replay ring capacity in records")
	_ = fs.Parse(args)
	passthrough := fs.Args()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.SetOutput(os.Stderr)
	log.SetPrefix("pi-gateway: ")

	cfg := gateway.PiConfig{
		Bin:   *piBin,
		OnLog: func(line []byte) { log.Printf("pi: %s", line) },
	}
	piArgs := func(id gateway.SessionID) []string {
		out := []string{"--mode", "rpc"}
		if *sessionFile != "" && !hasSessionArg(passthrough) {
			out = append(out, "--session", *sessionFile)
		}
		return append(out, passthrough...)
	}

	sid := gateway.SessionID(resolveSessionID(*sessionID, *sessionFile))
	srv := gateway.NewServer(ctx, cfg, piArgs, *hubCap, gateway.Mode(*mode))
	srv.SetSingleSession(sid)

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("serving on %s (session=%s mode=%s)", ln.Addr(), sid, *mode)
	if err := srv.Serve(ln); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func runConnect(args []string) {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	serverAddr := fs.String("server", "127.0.0.1:7331", "gateway server address")
	sessionID := fs.String("session-id", "default", "session id on the server")
	name := fs.String("name", "pi-bridge", "client name reported to the server")
	capsCSV := fs.String("capabilities", "observe,interject,prompt,ui,control", "comma-separated capabilities")
	replay := fs.Bool("replay", false, "forward replayed history to the raw peer")
	stdio := fs.Bool("stdio", true, "bridge raw pi RPC on stdin/stdout")
	listen := fs.String("listen", "", "additionally accept raw pi RPC over TCP on this address")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.SetOutput(os.Stderr)
	log.SetPrefix("pi-gateway: ")

	opts := gateway.ClientOptions{
		ServerAddr:   *serverAddr,
		SessionID:    *sessionID,
		Name:         *name,
		Capabilities: splitCSV(*capsCSV),
		Replay:       *replay,
	}

	if *listen != "" {
		ln, err := net.Listen("tcp", *listen)
		if err != nil {
			log.Fatalf("listen: %v", err)
		}
		log.Printf("bridging raw pi RPC on %s -> %s (session=%s)", ln.Addr(), *serverAddr, *sessionID)
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				go func() {
					client, err := gateway.Connect(ctx, opts)
					if err != nil {
						log.Printf("connect: %v", err)
						_ = conn.Close()
						return
					}
					_ = client.Bridge(ctx, conn)
				}()
			}
		}()
	}

	if *stdio {
		client, err := gateway.Connect(ctx, opts)
		if err != nil {
			log.Fatalf("connect: %v", err)
		}
		log.Printf("bridging raw pi RPC on stdio -> %s (session=%s)", *serverAddr, *sessionID)
		err = client.Bridge(ctx, gateway.StdioStream())
		if err != nil && ctx.Err() == nil {
			log.Printf("stdio bridge ended: %v", err)
		}
		stop()
		return
	}

	if *listen == "" {
		log.Fatalf("nothing to do: pass --stdio and/or --listen")
	}
	<-ctx.Done()
}

func hasSessionArg(args []string) bool {
	for _, a := range args {
		if a == "--session" || strings.HasPrefix(a, "--session=") {
			return true
		}
	}
	return false
}

func resolveSessionID(explicit, sessionFile string) string {
	if explicit != "" {
		return explicit
	}
	if sessionFile == "" {
		return "default"
	}
	base := sessionFile
	if i := strings.LastIndexAny(base, "/\\"); i >= 0 {
		base = base[i+1:]
	}
	base = strings.TrimSuffix(base, ".jsonl")
	if base == "" {
		return "default"
	}
	return base
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

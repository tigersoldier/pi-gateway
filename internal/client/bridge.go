// Package client implements pi-gateway: the bridge that third-party UIs spawn
// in place of `pi`. It speaks the gateway protocol to pi-gatewayd and presents
// pristine pi RPC on stdio, so the UI needs no gateway awareness.
package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/piargs"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

const (
	binaryName = "pi-gateway"

	// EnvName and EnvKind let a UI label its bridge; the defaults describe the
	// common case (pilish).
	EnvName = "PI_GATEWAY_CLIENT_NAME"
	EnvKind = "PI_GATEWAY_CLIENT_KIND"

	defaultName = "pi-gateway"
	defaultKind = "pilish"
)

// bridgeCapabilities is the full set the bridge requests on the UI's behalf.
const usage = `pi-gateway — pi RPC bridge to a pi-gatewayd session daemon

Usage:
  pi-gateway [client options] [pi options]

A UI spawns this binary instead of ` + "`pi`" + `; it connects to pi-gatewayd,
performs the gateway handshake, and relays pristine pi RPC on stdin/stdout.
The session survives this process: reconnecting re-attaches to it.

Client options:
  --server <host:port>   daemon address (default: port file, then 127.0.0.1:7331)
  --port <n>             daemon port on 127.0.0.1
  --token-file <path>    token file (default: ~/.config/pi-gateway/token)
  --mode rpc             accepted and consumed; any other mode is an error
  --version              print the managed pi version (daemon-answered)
  --help                 print this help

Accepted pi options: trust (--approve/-a, --no-approve/-na), extensions (-e,
--extension, --no-extensions/-ne), resource/tool toggles (--skill,
--no-skills/-ns, --prompt-template, --no-prompt-templates/-np, --theme,
--no-themes, --no-context-files/-nc, --tools/-t, --exclude-tools/-xt,
--no-builtin-tools/-nbt, --no-tools/-nt, --system-prompt,
--append-system-prompt), and --provider, --model, --models, --thinking,
--name/-n, --session-dir, --no-session, --api-key, --offline, --verbose.
Other pi options are rejected.

Environment:
  PI_GATEWAY_CLIENT_NAME  client name advertised to the daemon (default %q)
  PI_GATEWAY_CLIENT_KIND  client kind (default %q)
  PI_GATEWAY_CONFIG_DIR   state directory override
`

// Options configures a bridge run.
type Options struct {
	Args   []string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// DialTimeout defaults to 10s.
	DialTimeout time.Duration
}

type parsed struct {
	server    string
	port      int
	tokenFile string
	piArgs    []string
	mode      string
	help      bool
	version   bool
}

// Run executes the bridge and returns the process exit code.
//
// Exit codes: 0 success, 2 usage/pi-option error, 3 daemon/connection error.
func Run(ctx context.Context, opts Options) int {
	parsedArgs, err := parseArgs(opts.Args)
	if err != nil {
		fmt.Fprintf(opts.Stderr, "%s: %v\n", binaryName, err)
		return 2
	}
	if parsedArgs.help {
		fmt.Fprintf(opts.Stdout, usage, defaultName, defaultKind)
		return 0
	}
	if _, err := piargs.Parse(parsedArgs.piArgs); err != nil {
		fmt.Fprintf(opts.Stderr, "%s: %v\n", binaryName, err)
		return 2
	}

	addrs := candidateAddrs(parsedArgs)
	token, err := resolveToken(parsedArgs)
	if err != nil {
		fmt.Fprintf(opts.Stderr, "%s: %v\n", binaryName, err)
		return 3
	}

	timeout := opts.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialer := net.Dialer{Timeout: timeout}
	var (
		nc      net.Conn
		dialErr error
	)
	for _, addr := range addrs {
		nc, dialErr = dialer.DialContext(ctx, "tcp", addr)
		if dialErr == nil {
			break
		}
	}
	if dialErr != nil {
		fmt.Fprintf(opts.Stderr, "%s: cannot reach pi-gatewayd at %s: %v\n(is pi-gatewayd running?)\n",
			binaryName, strings.Join(addrs, ", "), dialErr)
		return 3
	}
	defer nc.Close()

	b := &bridge{
		conn:   nc,
		up:     protocol.NewCodec(nc, nc),
		down:   protocol.NewCodec(opts.Stdin, nil),
		ui:     protocol.NewCodec(nil, opts.Stdout),
		stdout: opts.Stdout,
		stderr: opts.Stderr,
		parsed: parsedArgs,
		token:  token,
	}
	return b.run(ctx)
}

func parseArgs(args []string) (*parsed, error) {
	p := &parsed{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		name, inline, hasInline := piargs.SplitFlag(arg)
		value := func() (string, error) {
			if hasInline {
				return inline, nil
			}
			if i+1 >= len(args) {
				return "", fmt.Errorf("option %s requires a value", name)
			}
			i++
			return args[i], nil
		}
		switch name {
		case "--help", "-h":
			p.help = true
		case "--version":
			p.version = true
		case "--server":
			v, err := value()
			if err != nil {
				return nil, err
			}
			p.server = v
		case "--port":
			v, err := value()
			if err != nil {
				return nil, err
			}
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 || n > 65535 {
				return nil, fmt.Errorf("invalid --port %q", v)
			}
			p.port = n
		case "--token-file":
			v, err := value()
			if err != nil {
				return nil, err
			}
			p.tokenFile = v
		case "--mode":
			v, err := value()
			if err != nil {
				return nil, err
			}
			if v != "rpc" {
				return nil, fmt.Errorf("only --mode rpc is supported (the pi TUI is not served by the gateway)")
			}
			p.mode = v
		default:
			// Everything else must be an accepted pi option.
			p.piArgs = append(p.piArgs, arg)
		}
	}
	if !p.help && !p.version && p.mode != "rpc" {
		return nil, errors.New("pi-gateway is a bridge for pi-gatewayd: run it as `pi-gateway --mode rpc` (or --help/--version)")
	}
	return p, nil
}

// candidateAddrs returns the daemon addresses to try, in order. A port-file
// address comes first, with the fixed default as a fallback in case the file
// is stale (for example after an unclean daemon exit).
func candidateAddrs(p *parsed) []string {
	if p.server != "" {
		return []string{p.server}
	}
	if p.port > 0 {
		return []string{net.JoinHostPort(config.DefaultHost, strconv.Itoa(p.port))}
	}
	var addrs []string
	if port, err := config.ReadPort(config.PortPath(config.Dir())); err == nil {
		addrs = append(addrs, net.JoinHostPort(config.DefaultHost, strconv.Itoa(port)))
	}
	if len(addrs) == 0 || addrs[0] != config.DefaultAddr {
		addrs = append(addrs, config.DefaultAddr)
	}
	return addrs
}

func resolveToken(p *parsed) (string, error) {
	path := p.tokenFile
	if path == "" {
		path = config.TokenPath(config.Dir())
	}
	tok, err := config.ReadToken(path)
	if err != nil {
		return "", fmt.Errorf("cannot read token %s: %w", path, err)
	}
	return tok, nil
}

type bridge struct {
	conn   net.Conn
	up     *protocol.Codec // raw connection codec
	down   *protocol.Codec // UI stdin
	ui     *protocol.Codec // UI stdout
	stdout io.Writer
	stderr io.Writer

	parsed *parsed
	token  string
}

type relayResult struct {
	side string
	err  error
}

func (b *bridge) run(ctx context.Context) int {
	hello := protocol.Hello{
		Type:     "gw_hello",
		Protocol: protocol.Version,
		Token:    b.token,
		Client: protocol.ClientInfo{
			Name:         envOr(EnvName, defaultName),
			Kind:         envOr(EnvKind, defaultKind),
			Capabilities: protocol.AllCapabilities,
		},
		PiArgs:   b.parsed.piArgs,
		Cwd:      workingDir(),
		LiveOnly: true,
	}
	if err := b.up.WriteJSON(&hello); err != nil {
		fmt.Fprintf(b.stderr, "%s: handshake failed: %v\n", binaryName, err)
		return 3
	}
	welcome, err := b.awaitWelcome()
	if err != nil {
		fmt.Fprintf(b.stderr, "%s: %v\n", binaryName, err)
		return 3
	}
	if b.parsed.version {
		return b.printVersion(welcome)
	}

	results := make(chan relayResult, 2)
	go b.relayUI(&results)
	go b.relayDaemon(&results)

	select {
	case r := <-results:
		if r.err == nil {
			return 0
		}
		if r.side == "daemon" {
			fmt.Fprintf(b.stderr, "%s: daemon connection lost: %v\n", binaryName, r.err)
			return 3
		}
		fmt.Fprintf(b.stderr, "%s: %v\n", binaryName, r.err)
		return 2
	case <-ctx.Done():
		return 0
	}
}

func (b *bridge) awaitWelcome() (protocol.Welcome, error) {
	for {
		raw, err := b.up.Read()
		if err != nil {
			return protocol.Welcome{}, fmt.Errorf("no gw_welcome from daemon: %w", err)
		}
		switch protocol.Field(raw, "type") {
		case "gw_welcome":
			var w protocol.Welcome
			if err := json.Unmarshal(raw, &w); err != nil {
				return protocol.Welcome{}, fmt.Errorf("malformed gw_welcome: %w", err)
			}
			return w, nil
		case "gw_error":
			code := protocol.Field(raw, "code")
			msg := protocol.Field(raw, "message")
			if code == protocol.CodeUnauthorized {
				return protocol.Welcome{}, fmt.Errorf("daemon rejected the token: %s", msg)
			}
			return protocol.Welcome{}, fmt.Errorf("daemon rejected the handshake: %s", msg)
		}
	}
}

func (b *bridge) printVersion(w protocol.Welcome) int {
	if strings.TrimSpace(w.PiVersion) == "" {
		fmt.Fprintf(b.stderr, "%s: daemon could not determine the managed pi version\n", binaryName)
		return 3
	}
	fmt.Fprintln(b.stdout, w.PiVersion)
	return 0
}

// relayUI forwards raw pi commands from the UI to the daemon verbatim.
func (b *bridge) relayUI(results *chan relayResult) {
	for {
		raw, err := b.down.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				*results <- relayResult{side: "ui", err: nil}
			} else {
				*results <- relayResult{side: "ui", err: err}
			}
			return
		}
		if err := b.up.WriteRaw(raw); err != nil {
			*results <- relayResult{side: "daemon", err: err}
			return
		}
	}
}

// relayDaemon strips gateway traffic and forwards pristine pi frames.
func (b *bridge) relayDaemon(results *chan relayResult) {
	for {
		raw, err := b.up.Read()
		if err != nil {
			*results <- relayResult{side: "daemon", err: err}
			return
		}
		typ := protocol.Field(raw, "type")
		if protocol.IsGatewayType(typ) {
			continue
		}
		// Updates produced by another client's command are not ours to show.
		if protocol.Field(raw, "gw_owner") != "" {
			continue
		}
		if err := b.ui.WriteRaw(protocol.StripGatewayFields(raw)); err != nil {
			*results <- relayResult{side: "ui", err: err}
			return
		}
	}
}

// workingDir reports the directory the UI spawned us in. The daemon creates
// the session there, so pi sees the project the UI is working on.
func workingDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	return dir
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

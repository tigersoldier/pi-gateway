// Command chat is an example interactive CLI built on the gwclient package.
// It is not a shipped binary; it exists to show an integration (a chat bot, a
// remote shell) how to drive one session:
//
//   - send input as a prompt, queued by the daemon while a turn runs;
//   - render the streamed assistant turn as it arrives;
//   - queue a follow-up, steer the running turn, or abort it;
//   - discover and run pi commands, prompt templates and skills with /name.
//
// Assistant output goes to stdout so it can be piped; prompts, tool summaries
// and status lines go to stderr. Dialogs (extension_ui_request) are answered
// from the same stdin, which is the part a real integration replaces with its
// own UI.
//
// Usage:
//
//	go run ./examples/chat                     # new session in $PWD
//	go run ./examples/chat --session auth      # attach by name or path
//	go run ./examples/chat --pi-arg=--approve  # pi parameters for a new session
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tigersoldier/pi-gateway/gwclient"
)

func main() {
	opts, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		os.Exit(2)
	}
	if err := run(context.Background(), opts); err != nil {
		fmt.Fprintf(os.Stderr, "chat: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	server     string
	port       int
	stateDir   string
	tokenFile  string
	session    string
	name       string
	cwd        string
	clientName string
	kind       string
	piArgs     multiFlag
	thinking   bool
	usage      bool
	verbose    bool
	noTools    bool
	timeout    time.Duration
}

// multiFlag collects a repeatable flag.
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, " ") }

func (m *multiFlag) Set(value string) error {
	*m = append(*m, value)
	return nil
}

func parseFlags(args []string) (options, error) {
	opts := options{clientName: "chat-example", kind: "bot", timeout: 60 * time.Second}
	fs := flag.NewFlagSet("chat", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&opts.server, "server", "", "daemon address host:port (default: the port file, then 127.0.0.1:7331)")
	fs.IntVar(&opts.port, "port", 0, "daemon port on 127.0.0.1")
	fs.StringVar(&opts.stateDir, "state-dir", "", "gateway state directory (default: ~/.config/pi-gateway)")
	fs.StringVar(&opts.tokenFile, "token-file", "", "token file (default <state-dir>/token)")
	fs.StringVar(&opts.session, "session", "", "attach to an existing session by path or name")
	fs.StringVar(&opts.name, "name", "", "name for the new session (default: none)")
	fs.StringVar(&opts.cwd, "cwd", "", "working directory for the new session (default: current directory)")
	fs.StringVar(&opts.clientName, "client-name", opts.clientName, "client name advertised to the daemon")
	fs.StringVar(&opts.kind, "kind", opts.kind, "client kind advertised to the daemon")
	fs.Var(&opts.piArgs, "pi-arg", "pi parameter for a new session, e.g. --pi-arg=--approve (repeatable)")
	fs.BoolVar(&opts.thinking, "thinking", false, "render thinking deltas")
	fs.BoolVar(&opts.usage, "usage", false, "print token usage after each assistant message")
	fs.BoolVar(&opts.verbose, "verbose", false, "print turn, queue and tool status lines")
	fs.BoolVar(&opts.noTools, "no-tools", false, "do not print tool calls and results")
	fs.DurationVar(&opts.timeout, "timeout", opts.timeout, "per-command response timeout")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: chat [flags]\n\n")
		fs.PrintDefaults()
		fmt.Fprint(fs.Output(), helpText)
	}
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	return opts, nil
}

func run(ctx context.Context, opts options) error {
	client, err := gwclient.Dial(ctx, gwclient.Config{
		Addr:           opts.server,
		Port:           opts.port,
		StateDir:       opts.stateDir,
		TokenFile:      opts.tokenFile,
		Name:           opts.clientName,
		Kind:           opts.kind,
		RequestTimeout: opts.timeout,
	})
	if err != nil {
		return err
	}
	defer client.Close()

	// Attach or create before starting the loops, so a bad --session fails
	// with the daemon's error instead of a half-usable prompt.
	if opts.session != "" {
		if _, err := client.SwitchSession(ctx, opts.session); err != nil {
			return fmt.Errorf("attach %q: %w", opts.session, err)
		}
	} else {
		cwd := opts.cwd
		if cwd == "" {
			cwd, _ = os.Getwd()
		}
		if _, err := client.NewSession(ctx, gwclient.NewSessionRequest{
			Name:   opts.name,
			Cwd:    cwd,
			PiArgs: opts.piArgs,
		}); err != nil {
			return fmt.Errorf("create session: %w", err)
		}
	}
	banner(os.Stderr, client)

	render := &renderer{
		out:      os.Stdout,
		errOut:   os.Stderr,
		thinking: opts.thinking,
		usage:    opts.usage,
		verbose:  opts.verbose,
		tools:    !opts.noTools,
	}
	// Dialogs are answered from the main loop, which also reads stdin, so
	// they are handed over instead of being rendered.
	dialogs := make(chan gwclient.UIRequest, 8)
	go func() {
		for ev := range client.Events() {
			if req, ok := ev.UIRequest(); ok {
				select {
				case dialogs <- req:
				case <-ctx.Done():
					return
				}
				continue
			}
			render.Handle(ev)
		}
	}()

	lines := readLines(ctx, os.Stdin, os.Stderr)
	interrupt := make(chan os.Signal, 2)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(interrupt)

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return finish(ctx, client, render, interrupt)
			}
			if handleLine(ctx, client, render, line) {
				return finish(ctx, client, render, interrupt)
			}
		case dialog := <-dialogs:
			handleDialog(ctx, client, dialog, lines)
		case <-interrupt:
			if render.Busy() {
				fmt.Fprintln(os.Stderr, "\n^C aborting the turn (again to quit)")
				if _, err := client.Abort(ctx); err != nil {
					reportError("abort", err)
				}
				continue
			}
			return finish(ctx, client, render, interrupt)
		case <-client.Done():
			return client.Err()
		case <-ctx.Done():
			return finish(ctx, client, render, interrupt)
		}
	}
}

// handleLine runs one input line. It reports true when the user asked to quit.
func handleLine(ctx context.Context, client *gwclient.Client, render *renderer, line string) bool {
	kind, arg := parseInput(line)
	switch kind {
	case inputPrompt:
		if arg == "" {
			return false
		}
		resp, err := client.Prompt(ctx, arg)
		switch {
		case err != nil:
			reportError("prompt", err)
		default:
			render.MarkPending()
			if queued(resp) {
				fmt.Fprintln(os.Stderr, "[queued; runs after the current turn]")
			}
		}
	case inputSteer:
		if arg == "" {
			fmt.Fprintln(os.Stderr, "usage: !steer <text>")
			return false
		}
		if _, err := client.Steer(ctx, arg); err != nil {
			reportError("steer", err)
		}
	case inputQueue:
		if arg == "" {
			fmt.Fprintln(os.Stderr, "usage: !queue <text>")
			return false
		}
		if _, err := client.FollowUp(ctx, arg); err != nil {
			reportError("follow_up", err)
		} else {
			render.MarkPending()
			fmt.Fprintln(os.Stderr, "[queued]")
		}
	case inputAbort:
		if _, err := client.Abort(ctx); err != nil {
			reportError("abort", err)
		}
	case inputCommands:
		printCommands(ctx, client)
	case inputSession:
		printSession(os.Stderr, client)
	case inputHelp:
		fmt.Fprint(os.Stderr, helpText)
	case inputQuit:
		return true
	case inputUnknown:
		fmt.Fprintf(os.Stderr, "unknown command %q — !help lists the local ones\n", strings.TrimSpace(line))
	}
	return false
}

// handleDialog answers an extension dialog. notify/set* are fire-and-forget;
// blocking dialogs read their answer from the input stream.
func handleDialog(ctx context.Context, client *gwclient.Client, req gwclient.UIRequest, lines <-chan string) {
	if !req.Blocking {
		if req.Method == gwclient.UIMethodNotify {
			fmt.Fprintf(os.Stderr, "\n[%s] %s\n", req.NotifyTypeOrInfo(), req.Message)
		}
		return
	}
	answer, ok := askDialog(req, lines)
	if !ok {
		answer = req.Cancelled()
	}
	if err := client.RespondUI(ctx, req.ID, answer); err != nil {
		reportError("extension_ui_response", err)
	}
}

// askDialog reads one answer from the input stream.
func askDialog(req gwclient.UIRequest, lines <-chan string) (map[string]any, bool) {
	switch req.Method {
	case gwclient.UIMethodConfirm:
		fmt.Fprintf(os.Stderr, "\n[confirm] %s: %s [y/N] ", req.Title, req.Message)
		line, ok := nextLine(lines)
		if !ok {
			return nil, false
		}
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "y", "yes":
			return req.Confirmed(true), true
		default:
			return req.Confirmed(false), true
		}
	case gwclient.UIMethodSelect:
		fmt.Fprintf(os.Stderr, "\n[select] %s\n", req.Title)
		for i, option := range req.Options {
			fmt.Fprintf(os.Stderr, "  %d) %s\n", i+1, option)
		}
		fmt.Fprint(os.Stderr, "choice (number or text, empty cancels): ")
		line, ok := nextLine(lines)
		if !ok || strings.TrimSpace(line) == "" {
			return nil, false
		}
		value := strings.TrimSpace(line)
		if n, err := strconv.Atoi(value); err == nil && n >= 1 && n <= len(req.Options) {
			value = req.Options[n-1]
		}
		return req.Value(value), true
	case gwclient.UIMethodInput, gwclient.UIMethodEditor:
		hint := req.Title
		if hint == "" {
			hint = req.Placeholder
		}
		fmt.Fprintf(os.Stderr, "\n[input] %s (empty cancels): ", hint)
		line, ok := nextLine(lines)
		if !ok || strings.TrimSpace(line) == "" {
			return nil, false
		}
		return req.Value(line), true
	default:
		fmt.Fprintf(os.Stderr, "\n[unsupported dialog %q; cancelling]\n", req.Method)
		return nil, false
	}
}

func nextLine(lines <-chan string) (string, bool) {
	line, ok := <-lines
	return line, ok
}

// readLines feeds stdin lines to the main loop and prints the prompt each
// time. A single reader keeps the prompt and dialog reads in order.
func readLines(ctx context.Context, in io.Reader, prompt io.Writer) <-chan string {
	lines := make(chan string)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(in)
		scanner.Buffer(make([]byte, 0, 64<<10), 4<<20)
		for {
			fmt.Fprint(prompt, "\n> ")
			if !scanner.Scan() {
				return
			}
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	return lines
}

func banner(w io.Writer, client *gwclient.Client) {
	fmt.Fprintf(w, "pi-gateway chat example — connected as %s\n", client.ClientID())
	if session := client.Session(); session != nil {
		fmt.Fprintf(w, "session: %s\n", session.Path)
		if session.Name != "" {
			fmt.Fprintf(w, "name:    %s\n", session.Name)
		}
	}
	fmt.Fprint(w, helpText)
}

func printSession(w io.Writer, client *gwclient.Client) {
	session := client.Session()
	if session == nil {
		fmt.Fprintln(w, "not attached to a session")
		return
	}
	fmt.Fprintf(w, "path: %s\n", session.Path)
	fmt.Fprintf(w, "name: %s\n", session.Name)
	fmt.Fprintf(w, "id:   %s\n", session.ID)
}

func printCommands(ctx context.Context, client *gwclient.Client) {
	commands, err := client.GetCommands(ctx)
	if err != nil {
		reportError("get_commands", err)
		return
	}
	if len(commands) == 0 {
		fmt.Fprintln(os.Stderr, "no commands, prompt templates or skills in this session")
		return
	}
	fmt.Fprintf(os.Stderr, "%d available; run one with /<name> [args]\n", len(commands))
	for _, command := range commands {
		description := command.Description
		if description == "" {
			description = "—"
		}
		fmt.Fprintf(os.Stderr, "  /%-28s %-9s %s\n", command.Name, command.Source, description)
	}
}

// queued reports whether the daemon accepted a prompt into its queue instead
// of starting a turn immediately (data {"queued":true}).
func queued(resp *gwclient.Response) bool {
	if resp == nil {
		return false
	}
	var data struct {
		Queued bool `json:"queued"`
	}
	return json.Unmarshal(resp.Data, &data) == nil && data.Queued
}

func reportError(command string, err error) {
	var respErr *gwclient.ResponseError
	if errors.As(err, &respErr) {
		fmt.Fprintf(os.Stderr, "[%s failed: %s (%s)]\n", command, respErr.Message, respErr.Code)
		return
	}
	fmt.Fprintf(os.Stderr, "[%s failed: %v]\n", command, err)
}

// finish waits for outstanding work (so piped input still renders its turn),
// then closes the connection politely. A second interrupt skips the wait.
func finish(ctx context.Context, client *gwclient.Client, render *renderer, interrupt <-chan os.Signal) error {
	drain(ctx, client, render, interrupt)
	return bye(client)
}

// drain polls until the submitted turn and the daemon queue are empty. It is
// bounded by a two-minute cap, and Ctrl-C breaks out immediately.
func drain(ctx context.Context, client *gwclient.Client, render *renderer, interrupt <-chan os.Signal) {
	if !render.Pending() {
		return
	}
	fmt.Fprintln(os.Stderr, "[waiting for the turn to finish — Ctrl-C to stop]")
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	limit := time.NewTimer(2 * time.Minute)
	defer limit.Stop()
	for render.Pending() {
		select {
		case <-client.Done():
			return
		case <-ctx.Done():
			return
		case <-interrupt:
			fmt.Fprintln(os.Stderr, "[interrupted; exiting]")
			return
		case <-limit.C:
			fmt.Fprintln(os.Stderr, "[still running after 2m; exiting]")
			return
		case <-ticker.C:
		}
	}
}

// bye closes the connection politely. The session survives.
func bye(client *gwclient.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = client.Bye(ctx)
	return nil
}

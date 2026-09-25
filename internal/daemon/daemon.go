// Package daemon implements pi-gatewayd: the loopback server that owns pi
// sessions, keeps them alive across client disconnects, and speaks the gateway
// protocol (docs/protocol.md).
package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/piargs"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/session"
)

// Config configures the daemon.
type Config struct {
	Addr        string
	Token       string
	PortFile    string // "" disables port-file discovery
	PiBin       string
	IdleTimeout time.Duration
	ShortGrace  time.Duration
	Logf        func(format string, args ...any)
}

// entry is a registered session: the actor plus the spawn configuration it was
// started with, used to detect conflicting re-attaches.
type entry struct {
	actor *session.Actor
	spawn map[string][]string
	// attaching counts in-flight binds; retire must not reap a session while a
	// client is being registered to it.
	attaching int
}

// Daemon owns the listener and the session table.
type Daemon struct {
	cfg  Config
	logf func(format string, args ...any)
	ln   net.Listener

	mu       sync.Mutex
	sessions map[string]*entry // canonical path -> entry
	pending  map[*session.Actor]*entry
	conns    map[*conn]struct{}

	nextClient atomic.Uint64
	piVersion  string
	port       int

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// New creates a daemon. Call Listen before Serve.
func New(cfg Config) *Daemon {
	if cfg.Addr == "" {
		cfg.Addr = config.DefaultAddr
	}
	if cfg.PiBin == "" {
		cfg.PiBin = "pi"
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	return &Daemon{
		cfg:      cfg,
		logf:     logf,
		sessions: make(map[string]*entry),
		pending:  make(map[*session.Actor]*entry),
		conns:    make(map[*conn]struct{}),
		done:     make(chan struct{}),
	}
}

// Listen binds the loopback listener, writes the port file, and discovers the
// managed pi version.
func (d *Daemon) Listen() error {
	ln, err := net.Listen("tcp", d.cfg.Addr)
	if err != nil {
		return fmt.Errorf("daemon: listen %s: %w", d.cfg.Addr, err)
	}
	d.ln = ln
	if d.cfg.PortFile != "" {
		port, err := config.PortOf(ln.Addr().String())
		if err == nil {
			d.port = port
			if err := config.WritePort(d.cfg.PortFile, port); err != nil {
				d.logf("daemon: write port file: %v", err)
			}
		}
	}
	d.piVersion = discoverPiVersion(d.cfg.PiBin)
	return nil
}

// Addr is the bound listener address.
func (d *Daemon) Addr() net.Addr {
	if d.ln == nil {
		return nil
	}
	return d.ln.Addr()
}

// PiVersion is the version reported by the managed pi binary ("" if unknown).
func (d *Daemon) PiVersion() string { return d.piVersion }

// Serve accepts connections until the context is canceled or Shutdown is
// called.
func (d *Daemon) Serve(ctx context.Context) error {
	go func() {
		select {
		case <-ctx.Done():
			d.Shutdown()
		case <-d.done:
		}
	}()
	for {
		nc, err := d.ln.Accept()
		if err != nil {
			select {
			case <-d.done:
				return nil
			default:
				return err
			}
		}
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.serveConn(nc)
		}()
	}
}

// Shutdown closes the listener and all connections, then stops every session.
func (d *Daemon) Shutdown() {
	d.closeOnce.Do(func() {
		close(d.done)
		if d.ln != nil {
			_ = d.ln.Close()
		}
		d.mu.Lock()
		conns := make([]*conn, 0, len(d.conns))
		for c := range d.conns {
			conns = append(conns, c)
		}
		actors := d.allActorsLocked()
		d.mu.Unlock()
		for _, c := range conns {
			c.close()
		}
		for _, a := range actors {
			a.Stop()
		}
		if d.cfg.PortFile != "" {
			removePortFile(d.cfg.PortFile, d.port)
		}
	})
	d.wg.Wait()
}

func (d *Daemon) serveConn(nc net.Conn) {
	c := newConn(d, nc)
	d.mu.Lock()
	d.conns[c] = struct{}{}
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		delete(d.conns, c)
		d.mu.Unlock()
	}()
	c.run()
}

// clientID allocates a connection identifier.
func (d *Daemon) clientID() string {
	return fmt.Sprintf("c_%d", d.nextClient.Add(1))
}

// ---------------------------------------------------------------------------
// Session table

// attach resolves a target path, joining a live session or spawning pi for a
// hibernated/absent one. The returned actor is live but not yet bound to the
// client.
func (d *Daemon) attach(target string, spec *piargs.Spec) (*session.Actor, error) {
	if target == "" {
		return nil, &attachError{protocol.CodeUnknownSession, "no session path given"}
	}
	if !looksLikePath(target) {
		// Name resolution lands in M2; fail clearly instead of guessing.
		return nil, &attachError{protocol.CodeUnknownSession,
			fmt.Sprintf("session name %q cannot be resolved yet; pass a session file path", target)}
	}
	canon := canonicalPath(target)

	d.mu.Lock()
	e := d.sessions[canon]
	if e != nil && !actorLive(e) {
		delete(d.sessions, canon)
		e = nil
	}
	if e == nil {
		a, err := d.newActor(canon, spec)
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}
		e = &entry{actor: a, spawn: spec.SpawnValues()}
		d.sessions[canon] = e
		d.mu.Unlock()
		d.logf("daemon: session %s spawned (pid managed by actor)", canon)
		return e.actor, nil
	}
	d.mu.Unlock()

	if key, conflict := piargs.SpawnConflict(e.spawn, spec.SpawnValues()); conflict {
		return nil, &attachError{protocol.CodeSpawnParamConflict,
			fmt.Sprintf("session is live with a different value for %s", key)}
	}
	d.applyRuntime(e, spec)
	return e.actor, nil
}

// create starts a session whose file path is not known yet; the actor reports
// it through OnPath once pi answers get_state.
func (d *Daemon) create(spec *piargs.Spec) (*session.Actor, error) {
	a, err := d.newActor("", spec)
	if err != nil {
		return nil, err
	}
	e := &entry{actor: a, spawn: spec.SpawnValues()}
	d.mu.Lock()
	d.pending[a] = e
	d.mu.Unlock()
	return a, nil
}

func (d *Daemon) newActor(path string, spec *piargs.Spec) (*session.Actor, error) {
	a := session.NewActor(session.Params{
		PiBin:       d.cfg.PiBin,
		PiArgs:      spec.Args,
		SessionPath: path,
		IdleTimeout: d.cfg.IdleTimeout,
		ShortGrace:  d.cfg.ShortGrace,
		Logf:        d.logf,
	})
	a.OnPath = d.onPath
	a.OnStopped = d.onStopped
	a.Retire = d.retire
	if err := a.Start(); err != nil {
		return nil, err
	}
	return a, nil
}

// onPath registers a session once pi reports its file path.
func (d *Daemon) onPath(a *session.Actor, path string) {
	canon := canonicalPath(path)
	d.mu.Lock()
	e := d.pending[a]
	delete(d.pending, a)
	if e == nil {
		e = &entry{actor: a}
	}
	if existing, ok := d.sessions[canon]; ok && existing.actor != a {
		d.mu.Unlock()
		d.logf("daemon: session path %s is already registered; retiring duplicate actor", canon)
		// Stop asynchronously: onPath runs on the actor loop, and Stop waits
		// for that loop to exit.
		go a.Stop()
		return
	}
	d.sessions[canon] = e
	d.mu.Unlock()
	d.logf("daemon: session %s registered", canon)
}

// onStopped drops a finished actor from the table.
func (d *Daemon) onStopped(a *session.Actor, reason string) {
	d.mu.Lock()
	for path, e := range d.sessions {
		if e.actor == a {
			delete(d.sessions, path)
		}
	}
	delete(d.pending, a)
	d.mu.Unlock()
	d.logf("daemon: session %s stopped (%s)", a.Path(), reason)
}

// retire removes an idle actor from the table so a later attach respawns pi.
// It returns false when a client attached in the meantime.
func (d *Daemon) retire(a *session.Actor) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.entryLocked(a); e != nil && e.attaching > 0 {
		return false
	}
	if a.Attached() > 0 {
		return false
	}
	for path, e := range d.sessions {
		if e.actor == a {
			delete(d.sessions, path)
		}
	}
	delete(d.pending, a)
	return true
}

// registerClient attaches c to a while a is still registered. It is the race
// barrier between binding and idle retirement: the entry counts in-flight
// attaches so retire refuses to reap a session another goroutine is binding.
func (d *Daemon) registerClient(a *session.Actor, c *conn) bool {
	d.mu.Lock()
	if !d.registeredLocked(a) {
		d.mu.Unlock()
		return false
	}
	e := d.entryLocked(a)
	if e == nil {
		d.mu.Unlock()
		return false
	}
	e.attaching++
	d.mu.Unlock()

	ok := a.AttachSync(c, 2*time.Second)

	d.mu.Lock()
	e.attaching--
	d.mu.Unlock()
	return ok
}

// entryLocked finds the table entry for an actor. Caller holds d.mu.
func (d *Daemon) entryLocked(a *session.Actor) *entry {
	for _, e := range d.sessions {
		if e.actor == a {
			return e
		}
	}
	return d.pending[a]
}

func (d *Daemon) registeredLocked(a *session.Actor) bool {
	for _, e := range d.sessions {
		if e.actor == a {
			return true
		}
	}
	_, ok := d.pending[a]
	return ok
}

func (d *Daemon) allActorsLocked() []*session.Actor {
	out := make([]*session.Actor, 0, len(d.sessions)+len(d.pending))
	for _, e := range d.sessions {
		out = append(out, e.actor)
	}
	for a := range d.pending {
		out = append(out, a)
	}
	return out
}

// applyRuntime applies runtime-applicable parameters to a live session, if the
// requested values differ from the session's current state.
func (d *Daemon) applyRuntime(e *entry, spec *piargs.Spec) {
	rt := spec.RuntimeValues()
	if len(rt) == 0 {
		return
	}
	rec, err := e.actor.Call([]byte(`{"type":"get_state"}`), 5*time.Second)
	if err != nil {
		d.logf("daemon: cannot read state to apply runtime parameters: %v", err)
		return
	}
	var st struct {
		Model struct {
			ID       string `json:"id"`
			Provider string `json:"provider"`
		} `json:"model"`
		ThinkingLevel string `json:"thinkingLevel"`
		SessionName   string `json:"sessionName"`
	}
	_ = json.Unmarshal(protocol.DataField(rec.Raw), &st)

	var cmds []session.RuntimeCommand
	wantModel, wantProvider := rt[piargs.KeyModel], rt[piargs.KeyProvider]
	if wantModel != "" || wantProvider != "" {
		modelID, provider := wantModel, wantProvider
		if modelID == "" {
			modelID = st.Model.ID
		}
		if provider == "" {
			provider = st.Model.Provider
		}
		if modelID != st.Model.ID || (wantProvider != "" && provider != st.Model.Provider) {
			cmds = appendChange(cmds, "set_model", map[string]any{
				"type": "set_model", "modelId": modelID, "provider": provider,
			})
		}
	}
	if lvl := rt[piargs.KeyThinking]; lvl != "" && lvl != st.ThinkingLevel {
		cmds = appendChange(cmds, "set_thinking_level", map[string]any{
			"type": "set_thinking_level", "level": lvl,
		})
	}
	if name := rt[piargs.KeyName]; name != "" && name != st.SessionName {
		cmds = appendChange(cmds, "set_session_name", map[string]any{
			"type": "set_session_name", "name": name,
		})
	}
	if len(cmds) == 0 {
		return
	}
	ref := protocol.ClientRef{ClientID: "daemon", Kind: "daemon", Name: "pi-gatewayd"}
	_ = e.actor.ApplyRuntime(cmds, &ref)
}

// appendChange marshals one runtime RPC and appends it to cmds.
func appendChange(cmds []session.RuntimeCommand, command string, body map[string]any) []session.RuntimeCommand {
	raw, err := json.Marshal(body)
	if err != nil {
		return cmds
	}
	return append(cmds, session.RuntimeCommand{Command: command, Raw: raw})
}

// ---------------------------------------------------------------------------
// Helpers

// attachError carries a protocol error code.
type attachError struct {
	code string
	msg  string
}

func (e *attachError) Error() string { return e.msg }

func errorCode(err error) string {
	var ae *attachError
	if errors.As(err, &ae) {
		return ae.code
	}
	return protocol.CodeBadFrame
}

func actorLive(e *entry) bool { return e.actor.Live() }

func looksLikePath(target string) bool {
	return strings.ContainsRune(target, os.PathSeparator) || strings.HasSuffix(target, ".jsonl") || strings.HasPrefix(target, "~")
}

func canonicalPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, path[2:])
		}
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	abs = filepath.Clean(abs)
	// Resolve the file itself when it exists, so the same session reached
	// through a symlink maps to a single actor (and one pi writer).
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved
	}
	if resolved, err := filepath.EvalSymlinks(filepath.Dir(abs)); err == nil {
		abs = filepath.Join(resolved, filepath.Base(abs))
	}
	return abs
}

// discoverPiVersion resolves the operator-configured pi binary (--pi) and
// runs it with --version, bounded by a timeout.
func discoverPiVersion(bin string) string {
	resolved, err := exec.LookPath(bin)
	if err != nil {
		return ""
	}
	out, err := runVersionCommand(resolved)
	if err != nil {
		return ""
	}
	return out
}

// runVersionCommand executes `<binary> --version`. The shell itself is a
// static, auditable path; binary is operator configuration (the --pi flag,
// already resolved through PATH) and is passed as a positional parameter, so
// it is never interpolated into the script text.
func runVersionCommand(binary string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "/bin/sh", "-c", `exec "$1" --version`, "sh", binary).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func removePortFile(path string, port int) {
	if p, err := config.ReadPort(path); err == nil && p == port {
		_ = os.Remove(path)
	}
}

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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/catalog"
	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/gwlog"
	"github.com/tigersoldier/pi-gateway/internal/metrics"
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
	// CatalogRoots are extra session directories to scan, on top of the ones
	// pi itself uses (catalog.DefaultRoots).
	CatalogRoots []string
	// SubscriberBuffer is the per-connection delivery buffer in records.
	SubscriberBuffer int
	// DeltaFlush is the streaming-delta coalescing interval.
	DeltaFlush time.Duration
	// Log receives structured operational records.
	Log gwlog.Logger
	// Metrics collects counters and gauges; nil creates a private registry.
	Metrics *metrics.Registry
	// Tokens are additional token grants (tokens.json). The primary Token
	// always grants the full capability set.
	Tokens []protocol.TokenGrant
}

// entry is a registered session: the actor plus the spawn configuration it was
// started with, used to detect conflicting re-attaches.
type entry struct {
	actor *session.Actor
	spawn map[string][]string
	// createdBy records the client that explicitly created the session and the
	// integration tags it supplied, for gw_list_sessions. It lives only as long
	// as the session is registered (no persisted store, decision 6).
	createdBy *protocol.ClientRef
	// attaching counts in-flight binds; retire must not reap a session while a
	// client is being registered to it.
	attaching int
}

// Daemon owns the listener and the session table.
type Daemon struct {
	cfg     Config
	log     gwlog.Logger
	metrics *metrics.Registry
	ln      net.Listener
	scanner *catalog.Scanner
	// tokens is the accepted token table: the default (full authority) grant
	// plus the provisioned ones. Replaced atomically on SIGHUP.
	tokens  atomic.Pointer[[]protocol.TokenGrant]
	started time.Time
	// gauges caches the actor-derived gauge values for one scrape interval
	// (see ops.go), so the debug listener cannot amplify actor round-trips.
	gaugeMu     sync.Mutex
	gaugeAt     time.Time
	gaugeCounts gaugeCounts

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
	log := cfg.Log
	if log == nil {
		log = gwlog.Nop()
	}
	m := cfg.Metrics
	if m == nil {
		m = metrics.New()
	}
	if cfg.SubscriberBuffer <= 0 {
		cfg.SubscriberBuffer = 1024
	}
	if cfg.DeltaFlush <= 0 {
		cfg.DeltaFlush = defaultDeltaFlush
	}
	d := &Daemon{
		cfg:      cfg,
		log:      log,
		metrics:  m,
		scanner:  &catalog.Scanner{Roots: catalog.DefaultRoots(cfg.CatalogRoots...)},
		sessions: make(map[string]*entry),
		pending:  make(map[*session.Actor]*entry),
		conns:    make(map[*conn]struct{}),
		done:     make(chan struct{}),
		started:  time.Now(),
	}
	m.Declare()
	if err := d.SetTokens(cfg.Tokens); err != nil {
		log.Error("invalid token configuration", "err", err)
		_ = d.SetTokens(nil)
	}
	d.registerGauges()
	return d
}

// Listen binds the loopback listener, writes the port file, and discovers the
// managed pi version.
func (d *Daemon) Listen() error {
	if err := config.RequireLoopback(d.cfg.Addr); err != nil {
		return err
	}
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
				d.log.Warn("cannot write port file", "err", err)
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
			config.RemovePortIfMatches(d.cfg.PortFile, d.port)
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

// resolveTarget maps a switch_session/hello target to a canonical session
// path. A path (or something that looks like one) is used as-is; anything else
// is resolved as a session name across session files and live actors
// (docs/protocol.md §3.1).
func (d *Daemon) resolveTarget(target string) (string, error) {
	if target == "" {
		return "", &attachError{protocol.CodeUnknownSession, "no session path given"}
	}
	if looksLikePath(target) {
		return canonicalPath(target), nil
	}
	files, err := d.scanner.FindName(target)
	truncated := errors.Is(err, catalog.ErrTooManyCandidates)
	if err != nil && !truncated {
		return "", &attachError{protocol.CodeUnknownSession, err.Error()}
	}
	live := d.liveInfos()
	info, found, resolveErr := catalog.Resolve(target, files, live)
	if resolveErr != nil {
		return "", &attachError{protocol.CodeAmbiguousSession, resolveErr.Error()}
	}
	canon := ""
	if found {
		canon = canonicalPath(info.Path)
	}
	if truncated && found && !isLivePath(live, canon) {
		// The scan may have skipped a duplicate outside the bound, so the name
		// cannot be resolved reliably from files alone.
		return "", &attachError{protocol.CodeAmbiguousSession,
			fmt.Sprintf("session name %q could not be resolved uniquely", target)}
	}
	if !found {
		return "", &attachError{protocol.CodeUnknownSession,
			fmt.Sprintf("no session named %q", target)}
	}
	return canon, nil
}

// isLivePath reports whether one of the live sessions owns path.
func isLivePath(live []catalog.Info, path string) bool {
	for _, info := range live {
		if canonicalPath(info.Path) == path {
			return true
		}
	}
	return false
}

// liveInfos describes the sessions that currently have a pi process, using
// pi's reported name and the actor's path. These cover sessions a file scan
// cannot see (--no-session, custom --session-dir).
func (d *Daemon) liveInfos() []catalog.Info {
	d.mu.Lock()
	entries := make([]*entry, 0, len(d.sessions))
	for _, e := range d.sessions {
		entries = append(entries, e)
	}
	d.mu.Unlock()
	out := make([]catalog.Info, 0, len(entries))
	for _, e := range entries {
		if !actorLive(e) {
			continue
		}
		path := e.actor.Path()
		if path == "" {
			continue
		}
		out = append(out, catalog.Info{Path: path, Name: e.actor.SessionName(), ID: e.actor.SessionID()})
	}
	return out
}

// liveActor resolves a target and returns its live actor.
func (d *Daemon) liveActor(target string) (*session.Actor, error) {
	canon, err := d.resolveTarget(target)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	e := d.sessions[canon]
	if e == nil || !actorLive(e) {
		return nil, &attachError{protocol.CodeUnknownSession, "session is not live"}
	}
	return e.actor, nil
}

// setCreated records the client that explicitly created a session.
func (d *Daemon) setCreated(a *session.Actor, ref protocol.ClientRef, tags map[string]string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.entryLocked(a); e != nil {
		ref.Tags = tags
		e.createdBy = &ref
	}
}

// Row is one gw_list_sessions entry (docs/protocol.md §3.2).
type Row struct {
	Path         string                   `json:"path"`
	Name         string                   `json:"name,omitempty"`
	Title        string                   `json:"title,omitempty"`
	ID           string                   `json:"id,omitempty"`
	Cwd          string                   `json:"cwd,omitempty"`
	Live         bool                     `json:"live"`
	IsStreaming  bool                     `json:"isStreaming"`
	MessageCount int                      `json:"messageCount"`
	LastActivity string                   `json:"lastActivity,omitempty"`
	CreatedBy    *protocol.ClientRef      `json:"createdBy,omitempty"`
	Clients      []protocol.ClientSummary `json:"clients,omitempty"`
}

// listSessions builds the session catalog: files newest first, enriched with
// the live actors' state.
func (d *Daemon) listSessions(cwd string, liveOnly bool, limit int) []Row {
	live := d.liveSnapshot()

	rows := make([]Row, 0, len(live)+8)
	seen := make(map[string]bool)
	for _, c := range d.scanner.List(cwd, limit) {
		canon := canonicalPath(c.Path)
		if seen[canon] {
			continue
		}
		row := Row{
			Path:         canon,
			Name:         c.Name,
			Title:        c.Title,
			ID:           c.ID,
			Cwd:          c.Cwd,
			MessageCount: c.MessageCount,
		}
		if !c.LastActivity.IsZero() {
			row.LastActivity = c.LastActivity.UTC().Format(time.RFC3339)
		}
		d.decorate(&row, canon, live)
		if liveOnly && !row.Live {
			continue
		}
		seen[canon] = true
		rows = append(rows, row)
	}
	// Live sessions the scan did not return (name-only, --no-session with a
	// path pi never wrote, or a file beyond the scan bound).
	for canon, ls := range live {
		if seen[canon] {
			continue
		}
		if cwd != "" {
			// A live-only session has no file header to check against.
			continue
		}
		row := Row{
			Path: canon,
			Name: ls.actor.SessionName(),
			ID:   ls.actor.SessionID(),
		}
		d.decorate(&row, canon, live)
		rows = append(rows, row)
		seen[canon] = true
	}
	if len(rows) > 1 {
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].Live != rows[j].Live {
				return rows[i].Live
			}
			return rows[i].LastActivity > rows[j].LastActivity
		})
	}
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

// liveSession is an immutable snapshot of a registered session, taken under
// d.mu so catalog building never reads entry fields concurrently.
type liveSession struct {
	actor     *session.Actor
	createdBy *protocol.ClientRef
}

// liveSnapshot copies what the catalog needs from the session table.
func (d *Daemon) liveSnapshot() map[string]liveSession {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]liveSession, len(d.sessions))
	for path, e := range d.sessions {
		if !actorLive(e) {
			continue
		}
		ls := liveSession{actor: e.actor}
		if e.createdBy != nil {
			ref := *e.createdBy
			ls.createdBy = &ref
		}
		out[path] = ls
	}
	return out
}

// decorate fills in live-session fields for one catalog row.
func (d *Daemon) decorate(row *Row, canon string, live map[string]liveSession) {
	ls, ok := live[canon]
	if !ok {
		return
	}
	state := stateOf(ls.actor)
	row.Live = true
	row.IsStreaming = state.Streaming
	row.Clients = state.Clients
	if ls.createdBy != nil {
		ref := *ls.createdBy
		row.CreatedBy = &ref
	}
	if row.Name == "" {
		row.Name = ls.actor.SessionName()
	}
	if row.ID == "" {
		row.ID = ls.actor.SessionID()
	}
}

// resolveCwd validates the working directory a client asked its session to use
// (docs/protocol.md §2). An empty value means "the daemon's own directory",
// which keeps clients that do not send one working.
func resolveCwd(cwd string) (string, error) {
	if cwd == "" {
		return "", nil
	}
	if !filepath.IsAbs(cwd) {
		return "", fmt.Errorf("cwd must be an absolute path: %q", cwd)
	}
	info, err := os.Stat(cwd)
	if err != nil {
		return "", fmt.Errorf("cwd %s: %w", cwd, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("cwd %s is not a directory", cwd)
	}
	return filepath.Clean(cwd), nil
}

// spawnCwd picks the directory a session about to be spawned should run in: a
// session file records its own directory, so a respawn (hibernation, daemon
// restart) uses it instead of whichever client happened to attach. Falls back
// to the client's directory, then to the daemon's.
func spawnCwd(path, clientCwd string) string {
	if path != "" {
		if dir := catalog.HeaderCwd(path); dir != "" {
			if info, err := os.Stat(dir); err == nil && info.IsDir() {
				return dir
			}
		}
	}
	return clientCwd
}

// attach resolves a target path or name, joining a live session or spawning
// pi for a hibernated/absent one. The returned actor is live but not yet bound
// to the client.
func (d *Daemon) attach(target string, spec *piargs.Spec, cwd string, by *protocol.ClientRef) (*session.Actor, error) {
	canon, err := d.resolveTarget(target)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	e := d.sessions[canon]
	if e != nil && !actorLive(e) {
		delete(d.sessions, canon)
		e = nil
	}
	if e == nil {
		a, err := d.newActor(canon, spec, spawnCwd(canon, cwd))
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}
		e = &entry{actor: a, spawn: spec.SpawnValues()}
		d.sessions[canon] = e
		d.mu.Unlock()
		d.log.Info("session started", "session", canon)
		return e.actor, nil
	}
	d.mu.Unlock()

	if key, conflict := piargs.SpawnConflict(e.spawn, spec.SpawnValues()); conflict {
		return nil, &attachError{protocol.CodeSpawnParamConflict,
			fmt.Sprintf("session is live with a different value for %s", key)}
	}
	d.applyRuntime(e, spec, by)
	return e.actor, nil
}

// create starts a session whose file path is not known yet; the actor reports
// it through OnPath once pi answers get_state.
func (d *Daemon) create(spec *piargs.Spec, cwd string) (*session.Actor, error) {
	a, err := d.newActor("", spec, cwd)
	if err != nil {
		return nil, err
	}
	e := &entry{actor: a, spawn: spec.SpawnValues()}
	d.mu.Lock()
	d.pending[a] = e
	d.mu.Unlock()
	return a, nil
}

func (d *Daemon) newActor(path string, spec *piargs.Spec, cwd string) (*session.Actor, error) {
	a := session.NewActor(session.Params{
		PiBin:       d.cfg.PiBin,
		PiArgs:      spec.Args,
		Cwd:         cwd,
		SessionPath: path,
		IdleTimeout: d.cfg.IdleTimeout,
		ShortGrace:  d.cfg.ShortGrace,
		Log:         d.log.With("session", path),
		Metrics:     d.metrics,
	})
	a.OnPath = d.onPath
	a.OnStopped = d.onStopped
	a.Retire = d.retire
	if err := a.Start(); err != nil {
		return nil, err
	}
	// Count here so both entry points are covered: attach (a session file) and
	// create (a client's implicit session before pi reports its path).
	d.metrics.Inc(metrics.SessionsStarted)
	return a, nil
}

// onPath registers a session once pi reports its file path, and re-keys it
// when fork/clone moved a live process to a new file.
func (d *Daemon) onPath(a *session.Actor, path string) {
	canon := canonicalPath(path)
	d.mu.Lock()
	e := d.pending[a]
	delete(d.pending, a)
	if e == nil {
		e = d.entryLocked(a)
	}
	if e == nil {
		e = &entry{actor: a}
	}
	if existing, ok := d.sessions[canon]; ok && existing.actor != a {
		d.mu.Unlock()
		d.log.Warn("session path already registered; retiring duplicate actor", "session", canon)
		// Stop asynchronously: onPath runs on the actor loop, and Stop waits
		// for that loop to exit.
		go a.Stop()
		return
	}
	// A fork/clone leaves the old path behind: the actor serves only the new
	// one now, so drop stale keys without touching the entry itself.
	for old, other := range d.sessions {
		if other == e && old != canon {
			delete(d.sessions, old)
		}
	}
	d.sessions[canon] = e
	d.mu.Unlock()
	d.log.Debug("session registered", "session", canon)
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
	d.metrics.Inc(metrics.SessionsEnded)
	d.log.Info("session stopped", "session", a.Path(), "reason", reason)
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
	d.metrics.Inc(metrics.SessionsReaped)
	d.log.Info("session retired", "session", a.Path())
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
	if ok {
		d.metrics.Inc(metrics.Attaches)
	} else {
		d.metrics.Inc(metrics.AttachFailures)
	}

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
func (d *Daemon) applyRuntime(e *entry, spec *piargs.Spec, by *protocol.ClientRef) {
	rt := spec.RuntimeValues()
	if len(rt) == 0 {
		return
	}
	rec, err := e.actor.Call([]byte(`{"type":"get_state"}`), 5*time.Second)
	if err != nil {
		d.log.Warn("cannot read state to apply runtime parameters", "session", e.actor.Path(), "err", err)
		return
	}
	st := protocol.ParsePiState(rec.Raw)

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
	if by == nil {
		// No requesting client (internal/administrative attach).
		by = &protocol.ClientRef{ClientID: "daemon", Kind: "daemon", Name: "pi-gatewayd"}
	}
	_ = e.actor.ApplyRuntime(cmds, by)
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

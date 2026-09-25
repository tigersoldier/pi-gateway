package daemon

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/metrics"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
	"github.com/tigersoldier/pi-gateway/internal/session"
)

// ---------------------------------------------------------------------------
// Tokens

// SetTokens replaces the provisioned tokens (normally read from
// ~/.config/pi-gateway/tokens.json). The daemon-generated token is always
// accepted with full authority; extra grants are additive and apply to new
// handshakes.
func (d *Daemon) SetTokens(extra []protocol.TokenGrant) error {
	if d.cfg.Token == "" {
		// Never install an empty token: it would authenticate an empty client
		// token with full authority.
		return errors.New("daemon: the default token is empty")
	}
	grants := []protocol.TokenGrant{{Name: "default", Token: d.cfg.Token, Capabilities: protocol.AllCapabilities}}
	seen := map[string]bool{d.cfg.Token: true}
	for _, g := range extra {
		if g.Token == "" {
			return fmt.Errorf("daemon: token %q has an empty value", g.Name)
		}
		if seen[g.Token] {
			return fmt.Errorf("daemon: token %q duplicates another token value", g.Name)
		}
		seen[g.Token] = true
		grants = append(grants, g)
	}
	d.tokens.Store(&grants)
	return nil
}

// tokenGrants returns the current token table.
func (d *Daemon) tokenGrants() []protocol.TokenGrant {
	if p := d.tokens.Load(); p != nil {
		return *p
	}
	return nil
}

// TokenNames lists the configured token names, never their values.
func (d *Daemon) TokenNames() []string {
	grants := d.tokenGrants()
	out := make([]string, 0, len(grants))
	for _, g := range grants {
		out = append(out, g.Name)
	}
	return out
}

// lookUpToken returns the grant for a token. Every entry is compared in
// constant time so a wrong token does not reveal how close it was.
func (d *Daemon) lookUpToken(token string) (protocol.TokenGrant, bool) {
	grants := d.tokenGrants()
	found := -1
	for i := range grants {
		if subtle.ConstantTimeCompare([]byte(token), []byte(grants[i].Token)) == 1 {
			found = i
		}
	}
	if found < 0 {
		return protocol.TokenGrant{}, false
	}
	return grants[found], true
}

// grantCapabilities intersects the requested capabilities with the token's
// role. An empty request means "everything the token allows" (docs/design.md
// §10).
func grantCapabilities(requested, allowed []string) ([]string, map[string]bool) {
	allowed, _ = protocol.NormalizeCapabilities(allowed) // canonical order
	permitted := make(map[string]bool, len(allowed))
	for _, c := range allowed {
		permitted[c] = true
	}
	set := make(map[string]bool, len(allowed))
	if len(requested) == 0 {
		for _, c := range allowed {
			set[c] = true
		}
	} else {
		for _, c := range requested {
			if permitted[c] {
				set[c] = true
			}
		}
	}
	granted := make([]string, 0, len(set))
	for _, c := range allowed {
		if set[c] {
			granted = append(granted, c)
		}
	}
	return granted, set
}

// ---------------------------------------------------------------------------
// Operational view

// LiveSession describes one registered session for /status.
type LiveSession struct {
	Path       string `json:"path"`
	Name       string `json:"name,omitempty"`
	State      string `json:"state"`
	Clients    int    `json:"clients"`
	QueueDepth int    `json:"queueDepth"`
	Streaming  bool   `json:"streaming"`
}

// Snapshot is the daemon's operational state, served by the debug listener.
// Registered counts every tracked session, including one a client created that
// has not reported a session file yet; live counts those with a running pi.
type Snapshot struct {
	PiVersion    string        `json:"piVersion,omitempty"`
	Registered   int           `json:"registered"`
	Live         int           `json:"live"`
	Streaming    int           `json:"streaming"`
	Clients      int           `json:"clients"`
	Connections  int           `json:"connections"`
	QueueDepth   int           `json:"queueDepth"`
	Subscribers  int           `json:"subscribers"`
	Tokens       int           `json:"tokens"`
	LiveSessions []LiveSession `json:"liveSessions,omitempty"`
}

// actorState is the derived live-session view of one actor. Both the catalog
// decoration and the status document use it, so "is streaming", the client
// count, and the queue depth are defined once.
type actorState struct {
	Live        bool
	Path        string
	Name        string
	State       string
	Streaming   bool
	Clients     []protocol.ClientSummary
	QueueDepth  int
	Subscribers int
}

// stateOf asks the actor for its state and derives the shared view.
func stateOf(a *session.Actor) actorState {
	info := a.Info()
	return actorState{
		Live:        a.Live(),
		Path:        info.Path,
		Name:        a.SessionName(),
		State:       info.State,
		Streaming:   info.Turn.State == "running",
		Clients:     info.Clients,
		QueueDepth:  info.Turn.Queued,
		Subscribers: info.Subscribers,
	}
}

// Status snapshots the daemon for the debug endpoints.
func (d *Daemon) Status() Snapshot {
	actors := d.actors()
	snap := Snapshot{PiVersion: d.piVersion, Registered: len(actors), Tokens: len(d.tokenGrants())}
	for _, a := range actors {
		state := stateOf(a)
		if state.Streaming {
			snap.Streaming++
		}
		if state.Live {
			snap.Live++
		}
		snap.Clients += len(state.Clients)
		snap.Subscribers += state.Subscribers
		snap.QueueDepth += state.QueueDepth
		snap.LiveSessions = append(snap.LiveSessions, LiveSession{
			Path:       state.Path,
			Name:       state.Name,
			State:      state.State,
			Clients:    len(state.Clients),
			QueueDepth: state.QueueDepth,
			Streaming:  state.Streaming,
		})
	}
	snap.Connections = d.connCount()
	sort.Slice(snap.LiveSessions, func(i, j int) bool {
		return snap.LiveSessions[i].Path < snap.LiveSessions[j].Path
	})
	return snap
}

// CatalogJSON marshals the session catalog response body. Both the
// gw_list_sessions control and the debug listener's /catalog serve it, so the
// two can never drift.
func (d *Daemon) CatalogJSON(cwd string, limit int) ([]byte, error) {
	rows := d.listSessions(cwd, false, limit)
	if rows == nil {
		rows = []Row{}
	}
	return json.Marshal(map[string]any{"sessions": rows})
}

// CatalogRoots lists the session directories the catalog scans.
func (d *Daemon) CatalogRoots() []string {
	if d.scanner == nil {
		return nil
	}
	return append([]string(nil), d.scanner.Roots...)
}

// Metrics returns the daemon's metrics registry.
func (d *Daemon) Metrics() *metrics.Registry { return d.metrics }

// WriteMetrics writes the Prometheus text exposition.
func (d *Daemon) WriteMetrics(w io.Writer) error { return d.metrics.WriteText(w) }

// Started reports when the daemon was created.
func (d *Daemon) Started() time.Time { return d.started }

// actors snapshots every tracked session, including one that a client created
// and pi has not yet reported a file path for.
func (d *Daemon) actors() []*session.Actor {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.allActorsLocked()
}

// connCount reports open client connections.
func (d *Daemon) connCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

// gaugeCounts is the actor-derived subset of the operational snapshot that the
// gauges report.
type gaugeCounts struct {
	registered, live, streaming, clients, queueDepth, subscribers int
}

// gaugeTTL bounds how stale a gauge reading may be. One /metrics scrape
// evaluates every gauge, and each actor walk is a synchronous round-trip
// through an actor loop (Actor.Info has a 2s timeout), so the walk is shared
// and cached briefly instead of repeating up to six times per scrape on the
// unauthenticated debug listener.
const gaugeTTL = time.Second

// gauges returns the shared gauge computation, refreshing it at most once per
// gaugeTTL.
func (d *Daemon) gauges() gaugeCounts {
	d.gaugeMu.Lock()
	defer d.gaugeMu.Unlock()
	if time.Since(d.gaugeAt) < gaugeTTL {
		return d.gaugeCounts
	}
	counts := gaugeCounts{}
	for _, a := range d.actors() {
		state := stateOf(a)
		counts.registered++
		if state.Live {
			counts.live++
		}
		if state.Streaming {
			counts.streaming++
		}
		counts.clients += len(state.Clients)
		counts.queueDepth += state.QueueDepth
		counts.subscribers += state.Subscribers
	}
	d.gaugeCounts, d.gaugeAt = counts, time.Now()
	return counts
}

// registerGauges wires the gauge metrics to live daemon state.
func (d *Daemon) registerGauges() {
	m := d.metrics
	gauge := func(name string, value func() int64) {
		m.Gauge(name, metrics.Help(name), value)
	}
	gauge(metrics.GaugeSessions, func() int64 { return int64(d.gauges().registered) })
	gauge(metrics.GaugeSessionsLive, func() int64 { return int64(d.gauges().live) })
	gauge(metrics.GaugeSessionsStreaming, func() int64 { return int64(d.gauges().streaming) })
	gauge(metrics.GaugeQueueDepth, func() int64 { return int64(d.gauges().queueDepth) })
	gauge(metrics.GaugeSubscribers, func() int64 { return int64(d.gauges().subscribers) })
	gauge(metrics.GaugeClients, func() int64 { return int64(d.gauges().clients) })
	gauge(metrics.GaugeConnections, func() int64 { return int64(d.connCount()) })
	gauge(metrics.GaugeTokens, func() int64 { return int64(len(d.tokenGrants())) })
	gauge(metrics.GaugeUptime, func() int64 { return int64(time.Since(d.started).Seconds()) })
}

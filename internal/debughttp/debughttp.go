// Package debughttp serves the daemon's read-only operational endpoints on a
// separate loopback port: /status, /catalog, and /metrics (docs/design.md
// §14 M3).
//
// The listener is loopback-only and, by decision, unauthenticated. It must
// therefore stay read-only and expose only operational metadata: no commands,
// no prompts, and no message content beyond catalog titles that the session
// files themselves already hold.
package debughttp

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/config"
	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/gwlog"
	"github.com/tigersoldier/pi-gateway/internal/protocol"
)

// Source is the read-only daemon view the endpoints serve.
type Source interface {
	// Status reports aggregate state and the live session list.
	Status() daemon.Snapshot
	// CatalogJSON returns the catalog response body, newest first: exactly the
	// bytes gw_list_sessions serves, so the two cannot drift.
	CatalogJSON(cwd string, limit int) ([]byte, error)
	// WriteMetrics writes the Prometheus text exposition.
	WriteMetrics(w io.Writer) error
}

// Options are the static facts the status document reports.
type Options struct {
	Version   string
	Addr      string
	DebugAddr string
	Started   time.Time
}

// Handler returns the debug endpoints. Every route is read-only.
func Handler(src Source, opts Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if !allowRead(w, r) {
			return
		}
		writeJSON(w, buildStatus(src, opts))
	})
	mux.HandleFunc("/catalog", func(w http.ResponseWriter, r *http.Request) {
		if !allowRead(w, r) {
			return
		}
		cwd := r.URL.Query().Get("cwd")
		limit := atoiDefault(r.URL.Query().Get("limit"), 0)
		body, err := src.CatalogJSON(cwd, limit)
		if err != nil {
			writeJSONStatus(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(append(body, '\n'))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if !allowRead(w, r) {
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := src.WriteMetrics(w); err != nil {
			http.Error(w, "metrics unavailable: "+err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSONStatus(w, http.StatusNotFound, map[string]any{
			"error": "unknown debug endpoint",
			"paths": []string{"/status", "/catalog", "/metrics"},
		})
	})
	return mux
}

// Listen binds the debug listener on addr (loopback by default) and returns
// the server and the bound address.
func Listen(addr string, src Source, opts Options, log gwlog.Logger) (*http.Server, net.Listener, error) {
	// The debug listener takes no token, so it must never leave loopback.
	if err := config.RequireLoopback(addr); err != nil {
		return nil, nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	// The handler reports its own address, which is only known after binding
	// (a port of 0 asks the kernel for a free one).
	opts.DebugAddr = ln.Addr().String()
	srv := &http.Server{
		Handler:           Handler(src, opts),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Info("debug listener started", "addr", opts.DebugAddr, "endpoints", "/status,/catalog,/metrics")
	return srv, ln, nil
}

// statusDoc is the /status response body.
type statusDoc struct {
	Version       string          `json:"version"`
	Protocol      int             `json:"protocol"`
	PiVersion     string          `json:"piVersion,omitempty"`
	Addr          string          `json:"addr,omitempty"`
	DebugAddr     string          `json:"debugAddr,omitempty"`
	StartedAt     string          `json:"startedAt,omitempty"`
	UptimeSeconds float64         `json:"uptimeSeconds"`
	Sessions      daemon.Snapshot `json:"sessions"`
}

func buildStatus(src Source, opts Options) statusDoc {
	doc := statusDoc{
		Version:   opts.Version,
		Protocol:  protocol.Version,
		Addr:      opts.Addr,
		DebugAddr: opts.DebugAddr,
		Sessions:  src.Status(),
	}
	doc.PiVersion = doc.Sessions.PiVersion
	if !opts.Started.IsZero() {
		doc.StartedAt = opts.Started.UTC().Format(time.RFC3339)
		doc.UptimeSeconds = time.Since(opts.Started).Seconds()
	}
	return doc
}

// allowRead rejects anything but GET/HEAD.
func allowRead(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	writeJSONStatus(w, http.StatusMethodNotAllowed, map[string]any{"error": "read-only endpoint"})
	return false
}

func writeJSON(w http.ResponseWriter, v any) { writeJSONStatus(w, http.StatusOK, v) }

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "encode response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}

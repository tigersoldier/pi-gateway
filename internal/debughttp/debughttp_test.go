package debughttp

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tigersoldier/pi-gateway/internal/daemon"
	"github.com/tigersoldier/pi-gateway/internal/gwlog"
)

// stubSource is a minimal Source for handler tests.
type stubSource struct {
	snap     daemon.Snapshot
	catalog  string
	lastCwd  string
	lastLim  int
	metricsz string
}

func (s *stubSource) Status() daemon.Snapshot { return s.snap }
func (s *stubSource) CatalogJSON(cwd string, limit int) ([]byte, error) {
	s.lastCwd, s.lastLim = cwd, limit
	body := s.catalog
	if body == "" {
		body = `{"sessions":[]}`
	}
	return []byte(body), nil
}
func (s *stubSource) WriteMetrics(w io.Writer) error {
	_, err := io.WriteString(w, s.metricsz)
	return err
}

func newTestServer(src Source, opts Options) *httptest.Server {
	return httptest.NewServer(Handler(src, opts))
}

func TestStatusDocument(t *testing.T) {
	src := &stubSource{snap: daemon.Snapshot{
		PiVersion:  "9.9.9",
		Registered: 3,
		Live:       2,
		Streaming:  1,
		Clients:    2,
		Tokens:     2,
		LiveSessions: []daemon.LiveSession{
			{Path: "/tmp/a.jsonl", Name: "auth", State: "ready", Clients: 1, QueueDepth: 1, Streaming: true},
		},
	}}
	opts := Options{Version: "0.2.0", Addr: "127.0.0.1:7331", DebugAddr: "127.0.0.1:7332", Started: time.Now().Add(-time.Minute)}
	srv := newTestServer(src, opts)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("status %d content-type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	var doc struct {
		Version       string          `json:"version"`
		Protocol      int             `json:"protocol"`
		PiVersion     string          `json:"piVersion"`
		Addr          string          `json:"addr"`
		UptimeSeconds float64         `json:"uptimeSeconds"`
		Sessions      daemon.Snapshot `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if doc.Version != "0.2.0" || doc.Protocol == 0 || doc.PiVersion != "9.9.9" || doc.Addr != "127.0.0.1:7331" {
		t.Fatalf("doc = %+v", doc)
	}
	if doc.UptimeSeconds < 59 || doc.Sessions.Live != 2 || len(doc.Sessions.LiveSessions) != 1 {
		t.Fatalf("sessions = %+v", doc.Sessions)
	}
}

func TestCatalogParamsAndShape(t *testing.T) {
	src := &stubSource{catalog: `{"sessions":[{"path":"/tmp/a.jsonl","live":true,"isStreaming":false,"messageCount":2}]}`}
	srv := newTestServer(src, Options{})
	defer srv.Close()

	body := getBody(t, srv.URL+"/catalog?cwd=/tmp&limit=5")
	if src.lastCwd != "/tmp" || src.lastLim != 5 {
		t.Fatalf("params not passed: cwd=%q limit=%d", src.lastCwd, src.lastLim)
	}
	var doc struct {
		Sessions []daemon.Row `json:"sessions"`
	}
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Sessions) != 1 || doc.Sessions[0].Path != "/tmp/a.jsonl" || !doc.Sessions[0].Live {
		t.Fatalf("catalog = %+v", doc.Sessions)
	}
	// A limit that does not parse is ignored, not fatal.
	getBody(t, srv.URL+"/catalog?limit=abc")
	if src.lastLim != 0 {
		t.Fatalf("bad limit produced %d", src.lastLim)
	}

	empty := &stubSource{}
	emptySrv := newTestServer(empty, Options{})
	defer emptySrv.Close()
	if body := getBody(t, emptySrv.URL+"/catalog"); !strings.Contains(body, `"sessions":[]`) {
		t.Fatalf("empty catalog body = %q", body)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	src := &stubSource{metricsz: "# TYPE pi_gateway_sessions gauge\npi_gateway_sessions 2\n"}
	srv := newTestServer(src, Options{})
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type = %q", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "pi_gateway_sessions 2") {
		t.Fatalf("metrics body = %q", body)
	}
}

func TestErrorsAndMethods(t *testing.T) {
	srv := newTestServer(&stubSource{}, Options{})
	defer srv.Close()

	if resp, err := http.Post(srv.URL+"/status", "text/plain", nil); err != nil {
		t.Fatal(err)
	} else {
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "GET, HEAD" {
			t.Fatalf("POST /status = %d, allow %q", resp.StatusCode, resp.Header.Get("Allow"))
		}
	}
	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /nope = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "unknown debug endpoint") || !strings.Contains(string(body), "/metrics") {
		t.Fatalf("404 body = %q", body)
	}
}

func TestListenBindsLoopback(t *testing.T) {
	srv, ln, err := Listen("127.0.0.1:0", &stubSource{}, Options{}, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if !strings.HasPrefix(ln.Addr().String(), "127.0.0.1:") {
		t.Fatalf("bound %s, want loopback", ln.Addr())
	}
}

// testLogger routes the listener's log records through the test log.
func testLogger(t *testing.T) gwlog.Logger {
	t.Helper()
	return gwlog.FromLogf(func(format string, args ...any) { t.Logf(format, args...) })
}

func getBody(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

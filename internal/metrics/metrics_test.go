package metrics

import (
	"bytes"
	"sort"
	"strings"
	"testing"
)

func TestCountersAndGauges(t *testing.T) {
	r := New()
	r.Inc("pi_gateway_test_total")
	r.Add("pi_gateway_test_total", 4)
	if got := r.Value("pi_gateway_test_total"); got != 5 {
		t.Fatalf("Value = %d, want 5", got)
	}
	if r.Value("pi_gateway_missing_total") != 0 {
		t.Fatal("unknown counter should read 0")
	}
	scrapes := 0
	r.Gauge("pi_gateway_test_gauge", "a gauge", func() int64 {
		scrapes++
		return 7
	})
	var buf bytes.Buffer
	if err := r.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{
		"# TYPE pi_gateway_test_total counter",
		"pi_gateway_test_total 5",
		"# HELP pi_gateway_test_gauge a gauge",
		"# TYPE pi_gateway_test_gauge gauge",
		"pi_gateway_test_gauge 7",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("exposition missing %q:\n%s", want, out)
		}
	}
	if scrapes != 1 {
		t.Fatalf("gauge read %d times, want 1", scrapes)
	}
	// Sample lines are emitted in sorted order.
	var names []string
	for _, line := range strings.Split(out, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		names = append(names, strings.Fields(line)[0])
	}
	if !sort.StringsAreSorted(names) {
		t.Fatalf("sample lines are not sorted: %v", names)
	}
}

func TestDeclareDocumentsEveryCounter(t *testing.T) {
	r := New()
	r.Declare()
	var buf bytes.Buffer
	if err := r.WriteText(&buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, name := range []string{SessionsStarted, SessionsDeleted, TurnsStarted, SubscriberDrops, FramesOut} {
		if !strings.Contains(out, "# HELP "+name+" ") {
			t.Errorf("no HELP line for %s", name)
		}
	}
}

func TestNilRegistryIsSafe(t *testing.T) {
	var r *Registry
	r.Inc(SessionsStarted)
	r.Add(SessionsStarted, 3)
	r.Counter(SessionsStarted, "help")
	r.Gauge(GaugeSessions, "help", func() int64 { return 1 })
	if got := r.Value(SessionsStarted); got != 0 {
		t.Fatalf("nil registry produced %d", got)
	}
	if err := r.WriteText(&bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
}

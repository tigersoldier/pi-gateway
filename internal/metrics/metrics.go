// Package metrics is a minimal Prometheus-text registry for the daemon: named
// counters and gauges, no labels, no dependencies (docs/design.md §14 M3).
//
// Every method is nil-safe, so instrumentation can be optional: a nil
// *Registry simply records nothing.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// Registry holds counters and gauges. Create it with New.
type Registry struct {
	mu       sync.Mutex
	counters map[string]*counter
	gauges   map[string]gauge
}

type counter struct {
	help  string
	value atomic.Int64
}

type gauge struct {
	help string
	fn   func() int64
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{counters: map[string]*counter{}, gauges: map[string]gauge{}}
}

// Counter declares a counter and its help text. Add and Inc create counters on
// first use, so declaring is optional; it only adds the HELP line.
func (r *Registry) Counter(name, help string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.counterLocked(name)
	if help != "" {
		c.help = help
	}
}

// Gauge registers a gauge whose value is read at scrape time.
func (r *Registry) Gauge(name, help string, fn func() int64) {
	if r == nil || fn == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gauges[name] = gauge{help: help, fn: fn}
}

// Inc adds one to a counter.
func (r *Registry) Inc(name string) { r.Add(name, 1) }

// Add adds delta to a counter.
func (r *Registry) Add(name string, delta int64) {
	if r == nil {
		return
	}
	r.mu.Lock()
	c := r.counterLocked(name)
	r.mu.Unlock()
	c.value.Add(delta)
}

// Value reads a counter's current value.
func (r *Registry) Value(name string) int64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	c := r.counters[name]
	r.mu.Unlock()
	if c == nil {
		return 0
	}
	return c.value.Load()
}

func (r *Registry) counterLocked(name string) *counter {
	c := r.counters[name]
	if c == nil {
		c = &counter{}
		r.counters[name] = c
	}
	return c
}

// WriteText writes the Prometheus text exposition format (version 0.0.4).
// Gauge functions run without the registry lock held, so they may call back
// into the registry.
func (r *Registry) WriteText(w io.Writer) error {
	if r == nil {
		return nil
	}
	type sample struct {
		kind  string
		help  string
		value int64
	}
	r.mu.Lock()
	samples := make(map[string]sample, len(r.counters)+len(r.gauges))
	for name, c := range r.counters {
		samples[name] = sample{kind: "counter", help: c.help, value: c.value.Load()}
	}
	for name, g := range r.gauges {
		if _, ok := samples[name]; !ok {
			samples[name] = sample{kind: "gauge", help: g.help}
		}
	}
	gauges := make(map[string]func() int64, len(r.gauges))
	for name, g := range r.gauges {
		gauges[name] = g.fn
	}
	r.mu.Unlock()

	names := make([]string, 0, len(samples))
	for name := range samples {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		s := samples[name]
		if fn, ok := gauges[name]; ok {
			s.value = fn()
		}
		if s.help != "" {
			if _, err := fmt.Fprintf(w, "# HELP %s %s\n", name, s.help); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", name, s.kind); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(w, "%s %d\n", name, s.value); err != nil {
			return err
		}
	}
	return nil
}

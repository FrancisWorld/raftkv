// Package metrics is a small Prometheus text-format (v0.0.4) exposition
// library: counters (optionally labelled), function-backed gauges and
// histograms. It covers what raftkv exports without pulling in a dependency.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families and renders them.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	name() string
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.families = append(r.families, f)
}

// ServeHTTP renders every family, sorted by name, in the text exposition format.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	r.mu.Lock()
	fams := append([]family(nil), r.families...)
	r.mu.Unlock()
	sort.Slice(fams, func(i, j int) bool { return fams[i].name() < fams[j].name() })
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, f := range fams {
		f.write(w)
	}
}

func header(w io.Writer, name, help, typ string) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`)

func renderLabels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = fmt.Sprintf(`%s="%s"`, n, labelEscaper.Replace(values[i]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// Counter is a monotonically increasing value.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// CounterVec is a counter family partitioned by label values.
type CounterVec struct {
	fname, help string
	labels      []string
	mu          sync.Mutex
	children    map[string]*Counter
	values      map[string][]string
}

// NewCounterVec registers a labelled counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	v := &CounterVec{fname: name, help: help, labels: labels, children: map[string]*Counter{}, values: map[string][]string{}}
	r.add(v)
	return v
}

// With returns the counter for the given label values.
func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d label values, got %d", v.fname, len(v.labels), len(values)))
	}
	key := strings.Join(values, "\xff")
	v.mu.Lock()
	defer v.mu.Unlock()
	c, ok := v.children[key]
	if !ok {
		c = &Counter{}
		v.children[key] = c
		v.values[key] = append([]string(nil), values...)
	}
	return c
}

func (v *CounterVec) name() string { return v.fname }

func (v *CounterVec) write(w io.Writer) {
	header(w, v.fname, v.help, "counter")
	v.mu.Lock()
	defer v.mu.Unlock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(w, "%s%s %d\n", v.fname, renderLabels(v.labels, v.values[k]), v.children[k].v.Load())
	}
}

type funcMetric struct {
	fname, help, typ string
	fn               func() float64
}

func (f *funcMetric) name() string { return f.fname }
func (f *funcMetric) write(w io.Writer) {
	header(w, f.fname, f.help, f.typ)
	_, _ = fmt.Fprintf(w, "%s %g\n", f.fname, f.fn())
}

// NewGaugeFunc registers a gauge whose value is read at scrape time.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.add(&funcMetric{fname: name, help: help, typ: "gauge", fn: fn})
}

// NewCounterFunc registers a counter whose value is read at scrape time. fn
// must be monotonic.
func (r *Registry) NewCounterFunc(name, help string, fn func() float64) {
	r.add(&funcMetric{fname: name, help: help, typ: "counter", fn: fn})
}

// Histogram observes values into cumulative buckets.
type Histogram struct {
	fname, help string
	bounds      []float64
	mu          sync.Mutex
	counts      []uint64 // per bucket, non-cumulative; last slot is +Inf
	sum         float64
	total       uint64
}

// NewHistogram registers a histogram with the given ascending upper bounds.
func (r *Registry) NewHistogram(name, help string, bounds []float64) *Histogram {
	h := &Histogram{fname: name, help: help, bounds: bounds, counts: make([]uint64, len(bounds)+1)}
	r.add(h)
	return h
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	i := sort.SearchFloat64s(h.bounds, v)
	h.counts[i]++
	h.sum += v
	h.total++
}

func (h *Histogram) name() string { return h.fname }

func (h *Histogram) write(w io.Writer) {
	header(w, h.fname, h.help, "histogram")
	h.mu.Lock()
	defer h.mu.Unlock()
	var cum uint64
	for i, b := range h.bounds {
		cum += h.counts[i]
		_, _ = fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", h.fname, b, cum)
	}
	_, _ = fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", h.fname, h.total, h.fname, h.sum, h.fname, h.total)
}

// Package metrics expõe contadores e histogramas no formato texto do
// Prometheus, sem dependências externas.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Registry reúne as métricas e as serve em /metrics.
type Registry struct {
	mu       sync.Mutex
	counters []*CounterVec
	hists    []*HistogramVec
}

// New cria um registro vazio.
func New() *Registry { return &Registry{} }

// CounterVec é um contador com rótulos.
type CounterVec struct {
	name, help string
	labels     []string
	mu         sync.Mutex
	values     map[string]float64
}

// Counter registra um contador.
func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, labels: labels, values: map[string]float64{}}
	r.mu.Lock()
	r.counters = append(r.counters, c)
	r.mu.Unlock()
	return c
}

// Inc soma 1 à série identificada pelos valores dos rótulos.
func (c *CounterVec) Inc(values ...string) { c.Add(1, values...) }

// Add soma v à série.
func (c *CounterVec) Add(v float64, values ...string) {
	k := key(c.labels, values)
	c.mu.Lock()
	c.values[k] += v
	c.mu.Unlock()
}

// HistogramVec é um histograma com rótulos.
type HistogramVec struct {
	name, help string
	labels     []string
	buckets    []float64
	mu         sync.Mutex
	series     map[string]*histSeries
}

type histSeries struct {
	counts []uint64 // por bucket (não acumulado)
	sum    float64
	count  uint64
}

// Histogram registra um histograma.
func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	h := &HistogramVec{name: name, help: help, labels: labels, buckets: buckets, series: map[string]*histSeries{}}
	r.mu.Lock()
	r.hists = append(r.hists, h)
	r.mu.Unlock()
	return h
}

// Observe registra uma observação.
func (h *HistogramVec) Observe(v float64, values ...string) {
	k := key(h.labels, values)
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.series[k]
	if s == nil {
		s = &histSeries{counts: make([]uint64, len(h.buckets))}
		h.series[k] = s
	}
	for i, b := range h.buckets {
		if v <= b {
			s.counts[i]++
			break
		}
	}
	s.sum += v
	s.count++
}

func escape(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	return strings.ReplaceAll(v, "\n", `\n`)
}

// key monta {a="x",b="y"} (vazio se não há rótulos).
func key(labels, values []string) string {
	if len(labels) == 0 {
		return ""
	}
	parts := make([]string, len(labels))
	for i, l := range labels {
		v := ""
		if i < len(values) {
			v = values[i]
		}
		parts[i] = fmt.Sprintf(`%s="%s"`, l, escape(v))
	}
	return strings.Join(parts, ",")
}

func series(name, labels, extra string) string {
	all := labels
	if extra != "" {
		if all != "" {
			all += ","
		}
		all += extra
	}
	if all == "" {
		return name
	}
	return name + "{" + all + "}"
}

// Write grava todas as métricas no formato de exposição do Prometheus.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	counters := append([]*CounterVec(nil), r.counters...)
	hists := append([]*HistogramVec(nil), r.hists...)
	r.mu.Unlock()

	for _, c := range counters {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.name, c.help, c.name)
		c.mu.Lock()
		keys := sortedKeys(c.values)
		for _, k := range keys {
			fmt.Fprintf(w, "%s %g\n", series(c.name, k, ""), c.values[k])
		}
		c.mu.Unlock()
	}
	for _, h := range hists {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
		h.mu.Lock()
		keys := make([]string, 0, len(h.series))
		for k := range h.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := h.series[k]
			var cum uint64
			for i, b := range h.buckets {
				cum += s.counts[i]
				fmt.Fprintf(w, "%s %d\n", series(h.name+"_bucket", k, fmt.Sprintf(`le="%g"`, b)), cum)
			}
			fmt.Fprintf(w, "%s %d\n", series(h.name+"_bucket", k, `le="+Inf"`), s.count)
			fmt.Fprintf(w, "%s %g\n", series(h.name+"_sum", k, ""), s.sum)
			fmt.Fprintf(w, "%s %d\n", series(h.name+"_count", k, ""), s.count)
		}
		h.mu.Unlock()
	}
}

func sortedKeys(m map[string]float64) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Handler serve as métricas.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

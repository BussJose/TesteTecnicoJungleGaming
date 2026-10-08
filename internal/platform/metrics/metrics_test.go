package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestExposition(t *testing.T) {
	r := New()
	c := r.Counter("req_total", "Requests.", "method", "status")
	c.Inc("GET", "200")
	c.Inc("GET", "200")
	c.Add(3, "POST", `4"0`)
	h := r.Histogram("dur_seconds", "Duration.", []float64{0.1, 1}, "route")
	h.Observe(0.05, "/a")
	h.Observe(0.5, "/a")
	h.Observe(5, "/a")

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		"# TYPE req_total counter",
		`req_total{method="GET",status="200"} 2`,
		`req_total{method="POST",status="4\"0"} 3`,
		`dur_seconds_bucket{route="/a",le="0.1"} 1`,
		`dur_seconds_bucket{route="/a",le="1"} 2`,
		`dur_seconds_bucket{route="/a",le="+Inf"} 3`,
		`dur_seconds_count{route="/a"} 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	c := r.Counter("n_total", "n")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Inc(); r.Write(&strings.Builder{}) }()
	}
	wg.Wait()
	var b strings.Builder
	r.Write(&b)
	if !strings.Contains(b.String(), "n_total 50") {
		t.Fatal(b.String())
	}
}

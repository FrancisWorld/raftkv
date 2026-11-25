package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func scrape(r *Registry) string {
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

func TestExposition(t *testing.T) {
	r := NewRegistry()
	reqs := r.NewCounterVec("app_requests_total", "Requests.", "method", "code")
	reqs.With("GET", "200").Inc()
	reqs.With("GET", "200").Inc()
	reqs.With("PUT", `5"00`).Inc()
	r.NewGaugeFunc("app_term", "Current term.", func() float64 { return 7 })
	h := r.NewHistogram("app_latency_seconds", "Latency.", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(5)

	out := scrape(r)
	for _, want := range []string{
		"# TYPE app_requests_total counter",
		`app_requests_total{method="GET",code="200"} 2`,
		`app_requests_total{method="PUT",code="5\"00"} 1`,
		"app_term 7",
		`app_latency_seconds_bucket{le="0.1"} 1`,
		`app_latency_seconds_bucket{le="1"} 2`,
		`app_latency_seconds_bucket{le="+Inf"} 3`,
		"app_latency_seconds_count 3",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Index(out, "app_latency_seconds") > strings.Index(out, "app_term") {
		t.Error("families are not sorted by name")
	}
}

func TestLabelArityIsEnforced(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic for wrong label count")
		}
	}()
	NewRegistry().NewCounterVec("x_total", "x", "a").With("1", "2")
}

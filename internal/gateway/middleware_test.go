package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/harjeetschahal/flexstore/internal/observability"
)

func TestMiddlewareMintsAndEchoesARequestID(t *testing.T) {
	m := observability.NewMetrics("test")

	var seen string
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The handler must be able to read the ID from the context, because
		// that is how it reaches internal gRPC calls and error bodies.
		seen = observability.RequestIDFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	}), testLogger(), m)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/objects/b/k", nil))

	if seen == "" {
		t.Fatal("no request ID reached the handler")
	}
	if got := rec.Header().Get(requestIDHeader); got != seen {
		t.Fatalf("echoed %q, handler saw %q", got, seen)
	}
}

func TestMiddlewareHonoursACallerSuppliedRequestID(t *testing.T) {
	m := observability.NewMetrics("test")
	var seen string
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = observability.RequestIDFrom(r.Context())
	}), testLogger(), m)

	req := httptest.NewRequest(http.MethodGet, "/objects/b/k", nil)
	req.Header.Set(requestIDHeader, "caller-supplied-id")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "caller-supplied-id" {
		t.Fatalf("request ID = %q, want the caller's value", seen)
	}
}

func TestMiddlewareRecordsMetricsWithBoundedCardinality(t *testing.T) {
	m := observability.NewMetrics("test")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /objects/{bucket}/{key...}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := Middleware(mux, testLogger(), m)

	// Many distinct object keys must collapse onto a single metric series;
	// labelling by raw path would blow up Prometheus cardinality.
	for _, key := range []string{"a.bin", "b.bin", "nested/c.bin", "very/deeply/nested/d.bin"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/objects/bucket/"+key, nil))
	}

	count := countSeries(t, m.Registry, "flexstore_http_requests_total")
	if count != 1 {
		t.Fatalf("%d metric series for 4 different keys; route label is not being used", count)
	}
}

func TestMiddlewareRecordsStatusClass(t *testing.T) {
	m := observability.NewMetrics("test")
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}), testLogger(), m)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/objects/b/k", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d", rec.Code)
	}

	got := gatherLabels(t, m.Registry, "flexstore_http_requests_total")
	if got["status"] != "4xx" {
		t.Fatalf("status label = %q, want 4xx", got["status"])
	}
}

func TestResponseRecorderReportsCommitState(t *testing.T) {
	// isWritten drives whether the gateway may still render a JSON error, so
	// the recorder must report commit state accurately.
	rec := &responseRecorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	if rec.wroteHeader {
		t.Fatal("a fresh recorder must not report itself as committed")
	}
	if _, err := rec.Write([]byte("body without an explicit WriteHeader")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !rec.wroteHeader {
		t.Fatal("an implicit 200 must still mark the response committed")
	}
	if rec.status != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.status)
	}
	if rec.bytes != int64(len("body without an explicit WriteHeader")) {
		t.Fatalf("byte count = %d", rec.bytes)
	}

	// A second WriteHeader must not overwrite the recorded status.
	rec.WriteHeader(http.StatusInternalServerError)
	if rec.status != http.StatusOK {
		t.Fatalf("status changed to %d after the response was committed", rec.status)
	}
}

func TestResponseRecorderSupportsResponseController(t *testing.T) {
	// The download path calls http.NewResponseController on the wrapped
	// writer; without Unwrap that silently fails.
	inner := httptest.NewRecorder()
	rec := &responseRecorder{ResponseWriter: inner, status: http.StatusOK}
	if rec.Unwrap() != http.ResponseWriter(inner) {
		t.Fatal("Unwrap does not expose the underlying ResponseWriter")
	}
}

func TestStatusLabel(t *testing.T) {
	for code, want := range map[int]string{
		100: "1xx", 200: "2xx", 204: "2xx", 301: "3xx",
		404: "4xx", 499: "4xx", 500: "5xx", 503: "5xx",
	} {
		if got := statusLabel(code); got != want {
			t.Errorf("statusLabel(%d) = %q, want %q", code, got, want)
		}
	}
}

func countSeries(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return len(f.GetMetric())
		}
	}
	return 0
}

func gatherLabels(t *testing.T, reg *prometheus.Registry, name string) map[string]string {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		if len(f.GetMetric()) == 0 {
			t.Fatalf("metric %s has no series", name)
		}
		out := map[string]string{}
		for _, lp := range f.GetMetric()[0].GetLabel() {
			out[lp.GetName()] = lp.GetValue()
		}
		return out
	}
	t.Fatalf("metric %s was never registered", name)
	return nil
}

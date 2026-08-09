package gateway

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func TestLimitRequestBodyRejectsDeclaredOversize(t *testing.T) {
	h := LimitRequestBody(okHandler(), 1024, discardLogger())
	req := httptest.NewRequest(http.MethodPut, "/objects/b/k", strings.NewReader("x"))
	req.ContentLength = 1 << 20
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 before reading any body", rec.Code)
	}
}

// TestLimitRequestBodyCapsUndeclaredSize covers the chunked-transfer case,
// where Content-Length is -1 and the only defence is the read cap.
func TestLimitRequestBodyCapsUndeclaredSize(t *testing.T) {
	var read int64
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		read = n
		if err == nil {
			t.Error("reading past the limit returned no error")
		}
		w.WriteHeader(http.StatusOK)
	})
	h := LimitRequestBody(inner, 1024, discardLogger())
	req := httptest.NewRequest(http.MethodPut, "/objects/b/k", strings.NewReader(strings.Repeat("x", 8192)))
	req.ContentLength = -1
	h.ServeHTTP(httptest.NewRecorder(), req)
	if read > 1024 {
		t.Fatalf("read %d bytes, limit was 1024", read)
	}
}

func TestLimitRequestBodyIgnoresReads(t *testing.T) {
	h := LimitRequestBody(okHandler(), 1024, discardLogger())
	req := httptest.NewRequest(http.MethodGet, "/objects/b/k", nil)
	req.ContentLength = 1 << 30
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET = %d, want 200: the cap applies to uploads, not reads", rec.Code)
	}
}

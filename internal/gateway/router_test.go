package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// newTestRouter builds the real route table. The handler's dependencies are
// nil because these tests only assert routing, never handler behaviour.
func newTestRouter(t *testing.T) *http.ServeMux {
	t.Helper()
	h := NewHandler(config.Gateway{}, nil, nil, nil, testLogger(), observability.NewMetrics("test"))
	return newMux(h)
}

// TestRouterPatternsDoNotConflict is the reason this file exists: net/http
// panics at registration time if two patterns are ambiguous, and the
// multipart URL scheme deliberately overlaps ("/multipart/{id}/complete" vs
// "/multipart/{bucket}/{key...}"). A regression here breaks the binary at
// startup, not at request time, so it must be caught in CI.
func TestRouterPatternsDoNotConflict(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("route registration panicked: %v", r)
		}
	}()
	newTestRouter(t)
}

func TestRouting(t *testing.T) {
	mux := newTestRouter(t)

	cases := []struct {
		method, path string
		wantPattern  string
		wantVars     map[string]string
	}{
		{"PUT", "/objects/photos/cat.jpg", "PUT /objects/{bucket}/{key...}",
			map[string]string{"bucket": "photos", "key": "cat.jpg"}},
		// Multi-segment keys are the reason {key...} is used rather than {key}.
		{"GET", "/objects/photos/2026/03/cat.jpg", "GET /objects/{bucket}/{key...}",
			map[string]string{"bucket": "photos", "key": "2026/03/cat.jpg"}},
		{"HEAD", "/objects/photos/cat.jpg", "HEAD /objects/{bucket}/{key...}", nil},
		{"DELETE", "/objects/photos/cat.jpg", "DELETE /objects/{bucket}/{key...}", nil},
		{"GET", "/objects/photos", "GET /objects/{bucket}", map[string]string{"bucket": "photos"}},

		{"POST", "/multipart/videos/big.mp4", "POST /multipart/{bucket}/{key...}",
			map[string]string{"bucket": "videos", "key": "big.mp4"}},
		{"PUT", "/multipart/abc-123/7", "PUT /multipart/{uploadId}/{partNumber}",
			map[string]string{"uploadId": "abc-123", "partNumber": "7"}},
		{"DELETE", "/multipart/abc-123", "DELETE /multipart/{uploadId}",
			map[string]string{"uploadId": "abc-123"}},

		{"GET", "/admin/nodes", "GET /admin/nodes", nil},
		{"GET", "/admin/objects/photos/a/b.jpg", "GET /admin/objects/{bucket}/{key...}",
			map[string]string{"bucket": "photos", "key": "a/b.jpg"}},
	}

	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.path, nil)
			_, pattern := mux.Handler(req)
			if pattern != c.wantPattern {
				t.Fatalf("matched %q, want %q", pattern, c.wantPattern)
			}
			if c.wantVars == nil {
				return
			}
			// Re-run through the mux so PathValue is populated.
			rec := httptest.NewRecorder()
			var got map[string]string
			probe := http.NewServeMux()
			probe.HandleFunc(c.wantPattern, func(w http.ResponseWriter, r *http.Request) {
				got = map[string]string{}
				for k := range c.wantVars {
					got[k] = r.PathValue(k)
				}
			})
			probe.ServeHTTP(rec, req)
			for k, want := range c.wantVars {
				if got[k] != want {
					t.Errorf("PathValue(%q) = %q, want %q", k, got[k], want)
				}
			}
		})
	}
}

// TestMultipartCompleteWinsOverKeyWildcard pins the precedence decision that
// makes the specified URL scheme work at all.
func TestMultipartCompleteWinsOverKeyWildcard(t *testing.T) {
	mux := newTestRouter(t)
	req := httptest.NewRequest(http.MethodPost, "/multipart/some-upload-id/complete", nil)
	_, pattern := mux.Handler(req)
	if pattern != "POST /multipart/{uploadId}/complete" {
		t.Fatalf("matched %q; the literal 'complete' segment must win over {key...}", pattern)
	}
}

// TestMultipartKeyNamedCompleteIsShadowed documents the cost of the above:
// a multipart object key of exactly "complete" is unreachable. This is a
// property of the specified URL scheme, and the README calls it out. The test
// exists so the limitation is provable rather than folklore.
func TestMultipartKeyNamedCompleteIsShadowed(t *testing.T) {
	mux := newTestRouter(t)
	req := httptest.NewRequest(http.MethodPost, "/multipart/mybucket/complete", nil)
	_, pattern := mux.Handler(req)
	if pattern == "POST /multipart/{bucket}/{key...}" {
		t.Fatal("routing changed: a key named 'complete' now reaches CreateMultipartUpload; " +
			"update the README's documented limitation")
	}
}

func TestUnknownRoutesAreNotSilentlySwallowed(t *testing.T) {
	mux := newTestRouter(t)
	// A path under /objects with the wrong method must not fall through to the
	// catch-all "GET /" handler and return 200.
	req := httptest.NewRequest(http.MethodPatch, "/objects/bucket/key", nil)
	_, pattern := mux.Handler(req)
	if strings.HasPrefix(pattern, "PATCH") {
		t.Fatalf("PATCH should not be routed, matched %q", pattern)
	}
}

// TestFallbacksSpeakTheErrorEnvelope pins the fix for the one place the API
// broke its own contract: the mux's built-in 404 and 405 responses were plain
// text, while every other error in the system carries the JSON envelope and
// the X-Flexstore-Error-Code header.
func TestFallbacksSpeakTheErrorEnvelope(t *testing.T) {
	h := NewHandler(config.Gateway{}, nil, nil, nil, testLogger(), observability.NewMetrics("test"))
	router := NewRouter(h)

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantCode   string
		wantAllow  bool
	}{
		{"wrong method on a real route", "PATCH", "/objects/bucket/key",
			http.StatusMethodNotAllowed, "MethodNotAllowed", true},
		{"write method on a read-only admin route", "PUT", "/admin/nodes",
			http.StatusMethodNotAllowed, "MethodNotAllowed", true},
		// GET /multipart/{id} is a 404, not a 405: the "GET /" catch-all is a
		// legitimate GET match for that path, and its handler answers unknown
		// paths with the JSON 404.
		{"read method on the multipart route", "GET", "/multipart/some-id",
			http.StatusNotFound, "NoSuchKey", false},
		{"unknown deep path", "GET", "/nope/deeper",
			http.StatusNotFound, "NoSuchKey", false},
		// PATCH /nope is 405, not 404: the "GET /" catch-all matches every
		// path, so the mux correctly reports that GET is the allowed method
		// for this URL -- and a GET there would get the JSON 404.
		{"unknown method on an unknown path", "PATCH", "/nope",
			http.StatusMethodNotAllowed, "MethodNotAllowed", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get("X-Flexstore-Error-Code"); got != tc.wantCode {
				t.Errorf("X-Flexstore-Error-Code = %q, want %q", got, tc.wantCode)
			}
			if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
				t.Errorf("Content-Type = %q, want JSON: the envelope is the contract", ct)
			}
			if tc.wantAllow && rec.Header().Get("Allow") == "" {
				t.Error("405 without an Allow header; RFC 9110 requires one")
			}
		})
	}
}

func TestRootReturnsServiceInfo(t *testing.T) {
	mux := newTestRouter(t)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET / returned %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "flexstore-gateway") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
}

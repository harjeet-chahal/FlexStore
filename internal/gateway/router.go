package gateway

import (
	"net/http"

	"github.com/harjeetschahal/flexstore/internal/apierr"
)

// NewRouter builds the client-facing route table.
//
// Uses net/http's 1.22+ pattern matching rather than a third-party router:
// the routing needs here (method + path wildcards + a trailing multi-segment
// wildcard for keys) are exactly what the standard library now covers, and
// avoiding a dependency keeps the request path free of unknown middleware.
func NewRouter(h *Handler) http.Handler {
	return jsonFallbacks(newMux(h), h)
}

// newMux builds the raw route table. Separate from NewRouter so the routing
// tests can interrogate pattern matching directly on the *http.ServeMux.
func newMux(h *Handler) *http.ServeMux {
	mux := http.NewServeMux()

	// --- S3-style object API ---
	// {key...} is a multi-segment wildcard, so keys may contain slashes
	// ("photos/2024/cat.jpg") exactly as in S3.
	mux.HandleFunc("PUT /objects/{bucket}/{key...}", h.PutObject)
	mux.HandleFunc("GET /objects/{bucket}/{key...}", h.GetObject)
	mux.HandleFunc("HEAD /objects/{bucket}/{key...}", h.HeadObject)
	mux.HandleFunc("DELETE /objects/{bucket}/{key...}", h.DeleteObject)

	// Bucket listing. Only the slash-free form is registered: adding
	// "GET /objects/{bucket}/" would be an ambiguous duplicate of the
	// {key...} pattern above and net/http panics at registration time.
	// "/objects/{bucket}/" therefore arrives at GetObject with an empty key,
	// which that handler forwards to ListObjects.
	mux.HandleFunc("GET /objects/{bucket}", h.ListObjects)

	// --- Multipart API ---
	// The literal-segment patterns are strictly more specific than the
	// {key...} pattern below them, so Go's precedence rules route
	// "/multipart/<id>/complete" to CompleteMultipartUpload. The consequence is
	// that a multipart *object key* of exactly "complete" is unreachable; that
	// is a documented limitation of the specified URL scheme.
	mux.HandleFunc("POST /multipart/{uploadId}/complete", h.CompleteMultipartUpload)
	mux.HandleFunc("PUT /multipart/{uploadId}/{partNumber}", h.UploadPart)
	mux.HandleFunc("DELETE /multipart/{uploadId}", h.AbortMultipartUpload)
	mux.HandleFunc("POST /multipart/{bucket}/{key...}", h.CreateMultipartUpload)

	// --- Live dashboard ---
	// One self-contained page, embedded in the binary, that polls the admin
	// endpoints below. It shows nothing the API would not also say.
	mux.HandleFunc("GET /dashboard", h.Dashboard)

	// --- Introspection ---
	// Read-only and logical: node health, capacity, replica placement, repair
	// progress. Deliberately no filesystem paths or storage internals.
	mux.HandleFunc("GET /admin/health", h.AdminHealth)
	mux.HandleFunc("GET /admin/stats", h.AdminStats)
	mux.HandleFunc("GET /admin/nodes", h.AdminNodes)
	mux.HandleFunc("GET /admin/replication", h.AdminReplication)
	mux.HandleFunc("GET /admin/objects/{bucket}/{key...}", h.AdminObjectChunks)
	// The only mutating admin route; it can only make the cluster's view more
	// conservative, never destroy data.
	mux.HandleFunc("POST /admin/reconcile", h.AdminReconcile)

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			writeNotFound(w, r, h)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"service": "flexstore-gateway",
			"api":     "/objects/{bucket}/{key}",
		}, h.log)
	})

	return mux
}

// jsonFallbacks makes the mux's own 404 and 405 responses speak the same JSON
// error envelope as every handler.
//
// Go 1.22's ServeMux answers an unmatched path with plain-text "404 page not
// found" and a matched path with the wrong method with plain-text "405 Method
// Not Allowed" -- neither carries the {"error":{code,message,request_id}} body
// or the X-Flexstore-Error-Code header that clients (and this repo's own
// integration tests) treat as the contract for every error. Those were the only
// two responses in the API that broke it.
//
// Detection uses mux.Handler, which reports the matched pattern: an empty
// pattern means the mux would serve one of its built-in fallbacks. The built-in
// 405 is distinguished from the 404 by replaying it against a header-only
// recorder to recover the status and the Allow header -- the recorder never
// receives a body because those fallbacks are header-plus-status responses.
func jsonFallbacks(mux *http.ServeMux, h *Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler, pattern := mux.Handler(r)
		if pattern != "" {
			mux.ServeHTTP(w, r)
			return
		}

		probe := &statusProbe{header: make(http.Header)}
		handler.ServeHTTP(probe, r)

		if probe.status == http.StatusMethodNotAllowed {
			if allow := probe.header.Get("Allow"); allow != "" {
				w.Header().Set("Allow", allow)
			}
			h.fail(w, r, apierr.New(http.StatusMethodNotAllowed, apierr.CodeMethodNotAllowed,
				r.Method+" is not supported on this route"))
			return
		}
		writeNotFound(w, r, h)
	})
}

// statusProbe captures the status and headers of a response and discards the
// body. Only used to inspect the mux's built-in fallback responses.
type statusProbe struct {
	header http.Header
	status int
}

func (p *statusProbe) Header() http.Header { return p.header }
func (p *statusProbe) WriteHeader(code int) {
	if p.status == 0 {
		p.status = code
	}
}
func (p *statusProbe) Write(b []byte) (int, error) {
	if p.status == 0 {
		p.status = http.StatusOK
	}
	return len(b), nil
}

func writeNotFound(w http.ResponseWriter, r *http.Request, h *Handler) {
	h.fail(w, r, notFoundError())
}

package gateway

import (
	"log/slog"
	"net/http"

	"github.com/harjeetschahal/flexstore/internal/apierr"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// LimitRequestBody caps how many bytes a request body may deliver.
//
// This is the wire-level backstop for the streaming size check. The splitter
// already aborts an oversized upload, but only after it has read past the
// limit -- and by then the chunks it produced are already durably replicated on
// storage nodes, to be reclaimed later by the GC. MaxBytesReader stops the read
// at the boundary, so an oversized upload costs bandwidth up to the limit and
// nothing beyond it.
func LimitRequestBody(next http.Handler, maxBytes int64, log *slog.Logger) http.Handler {
	if maxBytes <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
			// A declared Content-Length over the limit is rejected before a
			// single byte is read, which is the difference between refusing a
			// 10 GiB upload and transferring 5 TiB of it first.
			if r.ContentLength > maxBytes {
				apierr.Write(w, r, observability.RequestIDFrom(r.Context()),
					apierr.New(http.StatusRequestEntityTooLarge, apierr.CodeEntityTooLarge,
						"request body exceeds the configured maximum object size"), log)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}
		next.ServeHTTP(w, r)
	})
}

package gateway

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/harjeetschahal/flexstore/internal/apierr"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// requestIDHeader lets a caller supply its own correlation ID; otherwise the
// gateway mints one. Either way it is echoed back and propagated into every
// internal gRPC call.
const requestIDHeader = "X-Request-Id"

// responseRecorder captures the status code and byte count for metrics and
// access logs, and tells handlers whether the response is already committed.
type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(code int) {
	if r.wroteHeader {
		return
	}
	r.status = code
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Flush forwards to the underlying writer so streaming downloads are not
// buffered by the middleware chain.
func (r *responseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.NewResponseController reach the real ResponseWriter.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Middleware wires request IDs, structured access logs and HTTP metrics.
func Middleware(next http.Handler, log *slog.Logger, m *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get(requestIDHeader)
		if requestID == "" {
			requestID = uuid.NewString()
		}
		w.Header().Set(requestIDHeader, requestID)

		reqLog := log.With(
			slog.String("request_id", requestID),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
		)
		ctx := observability.WithRequestID(r.Context(), requestID)
		ctx = observability.WithLogger(ctx, reqLog)

		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		m.HTTPInFlight.Inc()
		defer m.HTTPInFlight.Dec()

		next.ServeHTTP(rec, r.WithContext(ctx))

		// route (the matched pattern) rather than the raw path keeps metric
		// cardinality bounded -- one series per endpoint, not per object key.
		route := routeLabel(r)
		status := statusLabel(rec.status)
		elapsed := time.Since(start)

		m.HTTPRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
		m.HTTPRequestDuration.WithLabelValues(r.Method, route, status).Observe(elapsed.Seconds())

		level := slog.LevelInfo
		if rec.status >= 500 {
			level = slog.LevelError
		}
		reqLog.Log(ctx, level, "http request",
			slog.Int("status", rec.status),
			slog.Int64("response_bytes", rec.bytes),
			slog.Duration("duration", elapsed))
	})
}

// routeLabel returns the matched mux pattern, falling back to a coarse prefix
// when there is none (404s).
func routeLabel(r *http.Request) string {
	if p := r.Pattern; p != "" {
		return p
	}
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "/"
	}
	return "/" + parts[0]
}

func statusLabel(code int) string {
	switch {
	case code < 200:
		return "1xx"
	case code < 300:
		return "2xx"
	case code < 400:
		return "3xx"
	case code < 500:
		return "4xx"
	default:
		return "5xx"
	}
}

func notFoundError() error {
	return apierr.New(http.StatusNotFound, apierr.CodeNoSuchKey, "no such route")
}

// Package observability wires structured logging, Prometheus metrics and the
// small always-on HTTP surface (/metrics, /healthz, /readyz) that every
// FlexStore binary exposes.
package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// contextKey is unexported so no other package can collide with our keys.
type contextKey int

const (
	requestIDKey contextKey = iota
	loggerKey
)

// NewLogger builds the process logger. JSON is the default because these logs
// are meant to be scraped, not read by a human tailing a terminal.
func NewLogger(w io.Writer, service, level, format string) *slog.Logger {
	if w == nil {
		w = os.Stdout
	}
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h).With(slog.String("service", service))
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// WithRequestID returns a context carrying the request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFrom returns the request ID stored in ctx, or "".
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// WithLogger returns a context carrying a request-scoped logger.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// LoggerFrom returns the request-scoped logger, falling back to fallback (or
// slog.Default when fallback is nil). It never returns nil, so callers do not
// need nil checks on a hot path.
func LoggerFrom(ctx context.Context, fallback *slog.Logger) *slog.Logger {
	if l, ok := ctx.Value(loggerKey).(*slog.Logger); ok && l != nil {
		return l
	}
	if fallback != nil {
		return fallback
	}
	return slog.Default()
}

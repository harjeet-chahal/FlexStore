package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// AdminServer is the sidecar HTTP listener every FlexStore binary runs. It
// carries /metrics, /healthz (process is alive) and /readyz (dependencies are
// usable), on a port separate from the client API so an overloaded data path
// never starves liveness probes.
type AdminServer struct {
	srv   *http.Server
	log   *slog.Logger
	ready atomic.Bool
}

// ReadinessFunc reports whether the service can currently serve traffic.
type ReadinessFunc func(ctx context.Context) error

// NewAdminServer builds the admin listener. readiness may be nil, in which case
// readiness is driven solely by SetReady.
func NewAdminServer(addr string, m *Metrics, log *slog.Logger, readiness ReadinessFunc) *AdminServer {
	a := &AdminServer{log: log}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writePlain(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !a.ready.Load() {
			writePlain(w, http.StatusServiceUnavailable, "starting")
			return
		}
		if readiness != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			if err := readiness(ctx); err != nil {
				a.log.Warn("readiness check failed", slog.String("error", err.Error()))
				writePlain(w, http.StatusServiceUnavailable, "not ready: "+err.Error())
				return
			}
		}
		writePlain(w, http.StatusOK, "ready")
	})

	a.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	return a
}

// SetReady flips the readiness flag once startup has finished.
func (a *AdminServer) SetReady(v bool) { a.ready.Store(v) }

// Start runs the listener until Shutdown is called. It returns only on a real
// failure; a clean shutdown yields nil.
func (a *AdminServer) Start() error {
	a.log.Info("admin server listening", slog.String("addr", a.srv.Addr))
	if err := a.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully stops the listener.
func (a *AdminServer) Shutdown(ctx context.Context) error {
	return a.srv.Shutdown(ctx)
}

func writePlain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	// A failed write to a client that already hung up is not actionable.
	_, _ = w.Write([]byte(body + "\n"))
}

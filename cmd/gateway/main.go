// Command gateway runs the client-facing S3-style HTTP API.
//
// It is stateless: all metadata lives in the coordinator and all bytes live on
// storage nodes, so gateways can be scaled horizontally behind a load balancer
// without any coordination between them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/gateway"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/runtime"
)

const maxMessageBytes = 16 << 20

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadGateway()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log := observability.NewLogger(os.Stdout, "gateway", cfg.LogLevel, cfg.LogFormat)
	metrics := observability.NewMetrics("gateway")

	coord, err := gateway.NewCoordinatorClient(cfg.CoordinatorAddr, cfg.CoordinatorTimeout)
	if err != nil {
		return fmt.Errorf("connect to coordinator: %w", err)
	}
	defer func() {
		if err := coord.Close(); err != nil {
			log.Warn("closing coordinator client", slog.String("error", err.Error()))
		}
	}()

	pool := nodeclient.NewPool(maxMessageBytes)
	defer func() {
		if err := pool.Close(); err != nil {
			log.Warn("closing node client pool", slog.String("error", err.Error()))
		}
	}()

	reader := gateway.NewChunkReader(pool, cfg.ChunkReadTimeout, log, metrics)
	// Detection alone does not heal anything: the reader must be able to tell
	// the control plane which replica served bad bytes so it gets replaced.
	reader.SetCorruptionReporter(coord)

	handler := gateway.NewHandler(
		cfg, coord,
		gateway.NewChunkWriter(pool, cfg.ChunkWriteTimeout, log, metrics),
		reader,
		log, metrics,
	)

	// Middleware order, outermost first:
	//
	//   Middleware       request ID + metrics + logging, so everything below is
	//                    observable including the rejections
	//   LimitRequestBody caps what a client may send before a byte is read
	//   Router           the handlers
	var api http.Handler = gateway.NewRouter(handler)
	api = gateway.LimitRequestBody(api, cfg.MaxObjectSize, log)
	api = gateway.Middleware(api, log, metrics)

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: api,
		// ReadTimeout/WriteTimeout are intentionally left at zero: a wall-clock
		// cap would kill legitimate multi-gigabyte transfers. Liveness comes
		// from the per-chunk gRPC deadlines instead. ReadHeaderTimeout still
		// protects against slowloris-style header attacks.
		ReadHeaderTimeout: 15 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}

	admin := observability.NewAdminServer(cfg.MetricsAddr, metrics, log, coord.Healthy)

	signalCtx, stopSignals := runtime.SignalContext()
	defer stopSignals()
	group, ctx := runtime.NewGroup(signalCtx, log)

	group.Go(ctx, "http", func(context.Context) error {
		log.Info("gateway listening",
			slog.String("addr", cfg.HTTPAddr),
			slog.String("coordinator", cfg.CoordinatorAddr),
			slog.Int64("chunk_size", cfg.ChunkSize))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	group.Go(ctx, "admin", func(context.Context) error { return admin.Start() })

	admin.SetReady(true)

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := runtime.ShutdownContext(cfg.ShutdownWait)
	defer cancel()

	// Stop accepting new requests but let in-flight uploads/downloads finish,
	// up to the grace period.
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warn("http shutdown", slog.String("error", err.Error()))
		if cerr := srv.Close(); cerr != nil {
			log.Warn("http force close", slog.String("error", cerr.Error()))
		}
	}
	if err := admin.Shutdown(shutdownCtx); err != nil {
		log.Warn("admin server shutdown", slog.String("error", err.Error()))
	}

	if err := group.Wait(); err != nil {
		return err
	}
	log.Info("stopped")
	return nil
}

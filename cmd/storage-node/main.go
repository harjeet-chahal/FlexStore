// Command storage-node runs one FlexStore data-plane node: it serves chunk
// reads/writes over gRPC and heartbeats to the coordinator.
//
// Every node in the cluster runs this same binary; only FLEXSTORE_NODE_ID,
// FLEXSTORE_ADVERTISE_ADDR and FLEXSTORE_DATA_DIR differ.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/runtime"
	"github.com/harjeetschahal/flexstore/internal/storage"
)

// maxMessageBytes must exceed the largest gRPC frame the gateway sends plus
// protobuf overhead. Frames are 256 KiB; 16 MiB leaves ample headroom and
// still bounds a hostile peer.
const maxMessageBytes = 16 << 20

func main() {
	if err := run(); err != nil {
		// slog may not be configured yet if config parsing failed.
		fmt.Fprintf(os.Stderr, "storage-node: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadStorageNode()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log := observability.NewLogger(os.Stdout, "storage-node", cfg.LogLevel, cfg.LogFormat).
		With(slog.String("node_id", cfg.NodeID))
	metrics := observability.NewMetrics("storage-node")

	store, err := storage.NewChunkStore(cfg.DataDir, cfg.FsyncData, cfg.CapacityBytes)
	if err != nil {
		return fmt.Errorf("open chunk store at %s: %w", cfg.DataDir, err)
	}

	svc := storage.NewService(cfg.NodeID, store, log, metrics)

	// Publish capacity immediately so a node that never manages to register
	// still shows up in Prometheus with its true numbers.
	if _, err := svc.Stats(); err != nil {
		log.Warn("initial node stats unavailable", slog.String("error", err.Error()))
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			// The gateway pings on idle connections between uploads; without
			// this the server would terminate them as abusive.
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(metrics, log)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(metrics, log)),
	)
	flexstorev1.RegisterStorageNodeServiceServer(grpcServer, svc)

	registrar, err := storage.NewRegistrar(
		cfg.NodeID, cfg.AdvertiseAddr, cfg.CoordinatorAddr,
		cfg.HeartbeatInterval, cfg.RegisterTimeout, svc.Stats, log)
	if err != nil {
		return fmt.Errorf("create registrar: %w", err)
	}
	defer registrar.Close()

	admin := observability.NewAdminServer(cfg.MetricsAddr, metrics, log, func(ctx context.Context) error {
		if _, err := svc.Stats(); err != nil {
			return err
		}
		return nil
	})

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	signalCtx, stopSignals := runtime.SignalContext()
	defer stopSignals()
	group, ctx := runtime.NewGroup(signalCtx, log)

	group.Go(ctx, "grpc", func(context.Context) error {
		log.Info("storage node listening",
			slog.String("addr", cfg.GRPCAddr),
			slog.String("advertise", cfg.AdvertiseAddr),
			slog.String("data_dir", cfg.DataDir))
		return grpcServer.Serve(lis)
	})
	group.Go(ctx, "admin", func(context.Context) error { return admin.Start() })

	group.Go(ctx, "registrar", func(ctx context.Context) error {
		// Registration must succeed before heartbeats mean anything, but a
		// failure here should not kill the node -- the gRPC service is already
		// serving and the coordinator may simply be slow to start.
		if err := registrar.Register(ctx); err != nil {
			// A cancelled context here means the process is shutting down, not
			// that registration failed; reporting it would turn a clean Ctrl-C
			// into a non-zero exit.
			if ctx.Err() != nil {
				return nil //nolint:nilerr // shutdown, not a registration failure
			}
			log.Error("initial registration failed; continuing with heartbeats",
				slog.String("error", err.Error()))
		}
		admin.SetReady(true)
		return registrar.Run(ctx)
	})

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := runtime.ShutdownContext(cfg.ShutdownWait)
	defer cancel()

	// GracefulStop drains in-flight chunk transfers; the timer is the backstop
	// for a client that never finishes.
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		log.Warn("graceful stop timed out; forcing")
		grpcServer.Stop()
	}

	if err := admin.Shutdown(shutdownCtx); err != nil {
		log.Warn("admin server shutdown", slog.String("error", err.Error()))
	}

	err = group.Wait()
	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	log.Info("stopped")
	return nil
}

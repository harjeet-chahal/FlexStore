// Command coordinator runs the FlexStore control plane: metadata ownership,
// placement, node health tracking and background reclamation.
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
	"github.com/harjeetschahal/flexstore/internal/cache"
	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/coordinator"
	"github.com/harjeetschahal/flexstore/internal/health"
	"github.com/harjeetschahal/flexstore/internal/metadata"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/placement"
	"github.com/harjeetschahal/flexstore/internal/runtime"
)

const maxMessageBytes = 16 << 20

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "coordinator: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadCoordinator()
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}

	log := observability.NewLogger(os.Stdout, "coordinator", cfg.LogLevel, cfg.LogFormat)
	metrics := observability.NewMetrics("coordinator")

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancelStartup()

	store, err := metadata.Open(startupCtx, cfg.PostgresDSN, log)
	if err != nil {
		return err
	}
	defer store.Close()

	if cfg.MigrateOnStart {
		if err := metadata.Migrate(startupCtx, store.Pool(), log); err != nil {
			return fmt.Errorf("migrate schema: %w", err)
		}
		log.Info("schema up to date")
	}

	redis, err := cache.New(startupCtx, cache.Options{
		Addr: cfg.RedisAddr, DB: cfg.RedisDB, Prefix: "fs",
	}, log)
	if err != nil {
		return err
	}
	defer func() {
		if err := redis.Close(); err != nil {
			log.Warn("closing cache", slog.String("error", err.Error()))
		}
	}()

	monitor := health.NewMonitor(health.Thresholds{
		SuspectAfter: cfg.SuspectTimeout,
		DeadAfter:    cfg.DeadTimeout,
	}, time.Now)

	// randSeed 0 => non-deterministic placement in production (weighted only;
	// rendezvous is deterministic by construction).
	placer, err := placement.New(cfg.PlacementStrategy, placement.DefaultConfig(), 0)
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	log.Info("placement strategy selected", slog.String("strategy", placer.Name()))

	svc := coordinator.NewService(cfg, store, redis, monitor, placer, log, metrics)

	pool := nodeclient.NewPool(maxMessageBytes)
	defer func() {
		if err := pool.Close(); err != nil {
			log.Warn("closing node client pool", slog.String("error", err.Error()))
		}
	}()

	healthWorker := coordinator.NewHealthWorker(svc, cfg.HealthScanInterval)
	if err := healthWorker.Rehydrate(startupCtx); err != nil {
		return fmt.Errorf("rehydrate health monitor: %w", err)
	}
	gcWorker := coordinator.NewGCWorker(svc, pool, cfg.GCInterval, cfg.GCBatchSize)
	statsWorker := coordinator.NewStatsWorker(svc, cfg.HealthScanInterval*3)

	// Every repair lease and reconciliation records which process owns it, so
	// an operator can tell whose work stalled. Hostname alone is ambiguous when
	// two coordinators share a host, so the PID is included.
	instanceID := coordinatorInstanceID()

	repairManager := coordinator.NewRepairManager(svc, pool, coordinator.RepairConfig{
		Enabled:           cfg.RepairEnabled,
		Workers:           cfg.RepairWorkers,
		ScanInterval:      cfg.RepairScanInterval,
		ScanBatch:         cfg.RepairScanBatch,
		MaxPerNode:        cfg.RepairMaxPerNode,
		JobTimeout:        cfg.RepairJobTimeout,
		LeaseDuration:     cfg.RepairLease,
		MaxAttempts:       cfg.RepairMaxAttempts,
		BaseBackoff:       cfg.RepairBaseBackoff,
		MaxBackoff:        cfg.RepairMaxBackoff,
		ReplicationFactor: cfg.ReplicationFactor,
		HistoryRetention:  cfg.RepairHistoryTTL,
	}, instanceID, log)
	// Wired after construction because the manager needs the service and the
	// service needs to reach the manager when a node recovers.
	svc.SetRepairManager(repairManager)

	reconciler := coordinator.NewReconciler(svc, pool, coordinator.ReconcileConfig{
		Enabled:         cfg.ReconcileEnabled,
		Interval:        cfg.ReconcileInterval,
		PageSize:        cfg.ReconcilePageSize,
		JobTimeout:      cfg.ReconcileTimeout,
		LeaseDuration:   cfg.ReconcileLease,
		VerifyChecksums: cfg.ReconcileVerify,
	}, instanceID, log)

	auditor := coordinator.NewDurabilityAuditor(svc, cfg.DurabilityAuditInterval)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 10 * time.Minute,
			Time:              30 * time.Second,
			Timeout:           10 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(observability.UnaryServerInterceptor(metrics, log)),
		grpc.ChainStreamInterceptor(observability.StreamServerInterceptor(metrics, log)),
	)
	flexstorev1.RegisterCoordinatorServiceServer(grpcServer, svc)

	admin := observability.NewAdminServer(cfg.MetricsAddr, metrics, log, func(ctx context.Context) error {
		return store.Ping(ctx)
	})

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}

	signalCtx, stopSignals := runtime.SignalContext()
	defer stopSignals()
	group, ctx := runtime.NewGroup(signalCtx, log)

	group.Go(ctx, "grpc", func(context.Context) error {
		log.Info("coordinator listening",
			slog.String("addr", cfg.GRPCAddr),
			slog.Int("replication_factor", cfg.ReplicationFactor),
			slog.Int("min_write_replicas", cfg.MinWriteReplicas),
			slog.Int64("chunk_size", cfg.ChunkSize),
			slog.String("placement", placer.Name()),
			slog.Bool("repair_enabled", cfg.RepairEnabled))
		return grpcServer.Serve(lis)
	})
	group.Go(ctx, "admin", func(context.Context) error { return admin.Start() })
	group.Go(ctx, "health-monitor", healthWorker.Run)
	group.Go(ctx, "gc", gcWorker.Run)
	group.Go(ctx, "stats", statsWorker.Run)
	group.Go(ctx, "repair", repairManager.Run)
	group.Go(ctx, "reconcile", reconciler.Run)
	group.Go(ctx, "durability-audit", auditor.Run)

	admin.SetReady(true)

	<-ctx.Done()
	log.Info("shutting down")

	shutdownCtx, cancel := runtime.ShutdownContext(cfg.ShutdownWait)
	defer cancel()

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

// coordinatorInstanceID identifies this process in job leases.
func coordinatorInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}

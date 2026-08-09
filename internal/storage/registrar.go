package storage

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/retry"
)

// Registrar keeps this node registered with the coordinator and heartbeats on
// an interval the coordinator dictates.
//
// It never gives up permanently: a coordinator restart must not require a
// storage-node restart. But every individual attempt is bounded by a deadline,
// so a hung coordinator cannot wedge the loop.
type Registrar struct {
	nodeID          string
	advertiseAddr   string
	coordinatorAddr string
	interval        time.Duration
	callTimeout     time.Duration

	stats func() (*flexstorev1.NodeStats, error)
	log   *slog.Logger

	conn   *grpc.ClientConn
	client flexstorev1.CoordinatorServiceClient
}

// NewRegistrar dials the coordinator. The dial itself is lazy (grpc.NewClient
// does not block), so this succeeds even when the coordinator is not up yet;
// the first heartbeat is what actually establishes the connection.
func NewRegistrar(
	nodeID, advertiseAddr, coordinatorAddr string,
	interval, callTimeout time.Duration,
	stats func() (*flexstorev1.NodeStats, error),
	log *slog.Logger,
) (*Registrar, error) {
	conn, err := grpc.NewClient(coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor()),
	)
	if err != nil {
		return nil, err
	}
	return &Registrar{
		nodeID:          nodeID,
		advertiseAddr:   advertiseAddr,
		coordinatorAddr: coordinatorAddr,
		interval:        interval,
		callTimeout:     callTimeout,
		stats:           stats,
		log:             log,
		conn:            conn,
		client:          flexstorev1.NewCoordinatorServiceClient(conn),
	}, nil
}

// Close releases the coordinator connection.
func (r *Registrar) Close() error { return r.conn.Close() }

// Register performs the initial registration, retrying with bounded backoff
// while the coordinator comes up.
func (r *Registrar) Register(ctx context.Context) error {
	return retry.Do(ctx, retry.StartupPolicy(), func(ctx context.Context, attempt int) error {
		stats, err := r.stats()
		if err != nil {
			return retry.Permanent(err)
		}
		callCtx, cancel := context.WithTimeout(ctx, r.callTimeout)
		defer cancel()

		resp, err := r.client.RegisterNode(callCtx, &flexstorev1.RegisterNodeRequest{
			NodeId:      r.nodeID,
			GrpcAddress: r.advertiseAddr,
			Stats:       stats,
		})
		if err != nil {
			r.log.Debug("waiting for coordinator",
				slog.Int("attempt", attempt), slog.String("error", err.Error()))
			return err
		}
		if ms := resp.HeartbeatIntervalMs; ms > 0 {
			r.interval = time.Duration(ms) * time.Millisecond
		}
		r.log.Info("registered with coordinator",
			slog.String("coordinator", r.coordinatorAddr),
			slog.Duration("heartbeat_interval", r.interval))
		return nil
	})
}

// Run heartbeats until ctx is cancelled. It returns nil on clean shutdown --
// a cancelled context is the expected way this loop ends.
func (r *Registrar) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	// consecutiveFailures only drives log verbosity: we do not want a
	// coordinator outage to produce one error line every 5 seconds forever.
	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		newInterval, err := r.heartbeatOnce(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			consecutiveFailures++
			// First failure is an error (something changed); subsequent ones
			// drop to warn so the log stays readable during a long outage.
			if consecutiveFailures == 1 {
				r.log.Error("heartbeat failed", slog.String("error", err.Error()))
			} else if consecutiveFailures%12 == 0 {
				r.log.Warn("heartbeat still failing",
					slog.Int("consecutive_failures", consecutiveFailures),
					slog.String("error", err.Error()))
			}
			continue
		}
		if consecutiveFailures > 0 {
			r.log.Info("heartbeat recovered",
				slog.Int("after_failures", consecutiveFailures))
			consecutiveFailures = 0
		}
		if newInterval > 0 && newInterval != r.interval {
			r.interval = newInterval
			ticker.Reset(newInterval)
			r.log.Info("heartbeat interval updated", slog.Duration("interval", newInterval))
		}
	}
}

func (r *Registrar) heartbeatOnce(ctx context.Context) (time.Duration, error) {
	stats, err := r.stats()
	if err != nil {
		return 0, err
	}
	callCtx, cancel := context.WithTimeout(ctx, r.callTimeout)
	defer cancel()

	resp, err := r.client.Heartbeat(callCtx, &flexstorev1.HeartbeatRequest{
		NodeId:      r.nodeID,
		GrpcAddress: r.advertiseAddr,
		Stats:       stats,
	})
	if err != nil {
		return 0, err
	}
	if resp.MustReregister {
		// The coordinator lost our registration (fresh database, for example).
		// Re-register inline so the node rejoins without an operator restart.
		r.log.Warn("coordinator requested re-registration")
		if _, err := r.client.RegisterNode(callCtx, &flexstorev1.RegisterNodeRequest{
			NodeId:      r.nodeID,
			GrpcAddress: r.advertiseAddr,
			Stats:       stats,
		}); err != nil {
			return 0, err
		}
	}
	return time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond, nil
}

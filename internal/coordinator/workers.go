package coordinator

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/health"
	"github.com/harjeetschahal/flexstore/internal/metadata"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
)

// deletionMaxAttempts bounds retries per deletion job. Past this the job is
// dropped so an unreachable node cannot pin metadata forever; see
// Store.FailDeletion for the trade-off.
const deletionMaxAttempts = 8

// HealthWorker drives the node health state machine on a ticker.
//
// Heartbeats can only ever move a node *towards* healthy; only this sweep can
// move it towards SUSPECT/DEAD. Concentrating the demotion logic in one
// periodic task means there is exactly one place where "how do we decide a
// node is gone" lives.
type HealthWorker struct {
	svc      *Service
	interval time.Duration
}

// NewHealthWorker builds the health sweeper.
func NewHealthWorker(svc *Service, interval time.Duration) *HealthWorker {
	return &HealthWorker{svc: svc, interval: interval}
}

// Rehydrate seeds the in-memory monitor from PostgreSQL. Without this, a
// coordinator restart would briefly consider every node UNKNOWN and could
// reject uploads for a full heartbeat interval.
func (w *HealthWorker) Rehydrate(ctx context.Context) error {
	nodes, err := w.svc.store.ListNodes(ctx, false)
	if err != nil {
		return err
	}
	for _, n := range nodes {
		w.svc.monitor.Seed(n.ID, n.Address, n.LastHeartbeatAt)
		st := w.svc.monitor.State(n.ID)
		w.svc.m.NodeHealth.WithLabelValues(n.ID).Set(float64(st))
		w.svc.m.StorageCapacityBytes.WithLabelValues(n.ID).Set(float64(n.TotalBytes))
		w.svc.m.StorageUsedBytes.WithLabelValues(n.ID).Set(float64(n.UsedBytes))
		// Persist the recomputed state so the database agrees with memory even
		// if the coordinator was down long enough for nodes to age out.
		if st.String() != n.Health {
			if _, err := w.svc.store.SetNodeHealth(ctx, n.ID, st.String()); err != nil {
				return err
			}
		}
	}
	w.svc.log.Info("health monitor rehydrated", slog.Int("nodes", len(nodes)))
	return nil
}

// Run sweeps until ctx is cancelled.
func (w *HealthWorker) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		for _, tr := range w.svc.monitor.Sweep() {
			w.svc.onTransition(ctx, tr)
		}
	}
}

// GCWorker reclaims storage: it pushes queued chunk deletions to nodes, purges
// fully-reclaimed metadata, and fails uploads that have gone stale.
type GCWorker struct {
	svc      *Service
	pool     *nodeclient.Pool
	interval time.Duration
	batch    int
}

// NewGCWorker builds the reclamation worker.
func NewGCWorker(svc *Service, pool *nodeclient.Pool, interval time.Duration, batch int) *GCWorker {
	return &GCWorker{svc: svc, pool: pool, interval: interval, batch: batch}
}

// Run reclaims until ctx is cancelled.
func (w *GCWorker) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := w.cycle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.svc.m.GCErrors.Inc()
			w.svc.log.Error("gc cycle failed", slog.String("error", err.Error()))
		}
	}
}

func (w *GCWorker) cycle(ctx context.Context) error {
	if n, err := w.svc.store.ReapStaleUploads(ctx, w.svc.cfg.UploadStaleAfter, w.batch); err != nil {
		return err
	} else if n > 0 {
		w.svc.log.Warn("reaped stale uploads", slog.Int("count", n))
	}
	if n, err := w.svc.store.ReapStaleMultipartUploads(ctx, w.svc.cfg.UploadStaleAfter, w.batch); err != nil {
		return err
	} else if n > 0 {
		w.svc.log.Warn("reaped stale multipart uploads", slog.Int("count", n))
	}

	if err := w.processDeletions(ctx); err != nil {
		return err
	}

	chunks, objects, err := w.svc.store.PurgeReclaimedChunks(ctx, w.batch)
	if err != nil {
		return err
	}
	if chunks > 0 || objects > 0 {
		w.svc.log.Info("purged reclaimed metadata",
			slog.Int64("chunks", chunks), slog.Int64("objects", objects))
	}
	if _, err := w.svc.store.PurgeCompletedMultipartUploads(ctx, time.Hour, w.batch); err != nil {
		return err
	}
	return nil
}

// processDeletions leases a batch of deletion jobs and executes them
// sequentially. Sequential rather than parallel on purpose: reclamation is
// background work and must never compete with the data path for a node's
// request budget.
func (w *GCWorker) processDeletions(ctx context.Context) error {
	jobs, err := w.svc.store.ClaimDeletions(ctx, w.batch, 2*w.interval)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		// Stop mid-batch on shutdown. The remaining jobs keep their lease and
		// simply become due again, so abandoning them is not an error.
		if ctx.Err() != nil {
			return nil //nolint:nilerr // shutdown, not a deletion failure
		}
		if err := w.deleteOne(ctx, job); err != nil {
			w.svc.m.GCErrors.Inc()
			backoff := time.Duration(job.Attempts) * w.interval
			if ferr := w.svc.store.FailDeletion(ctx, job.ID, err.Error(), backoff, deletionMaxAttempts); ferr != nil {
				return ferr
			}
			continue
		}
		if err := w.svc.store.FinishDeletion(ctx, job.ID, job.ChunkID, job.NodeID); err != nil {
			return err
		}
		w.svc.m.GCDeletedChunks.Inc()
	}
	return nil
}

func (w *GCWorker) deleteOne(ctx context.Context, job metadata.PendingDeletion) error {
	if job.Address == "" {
		// The node row is gone, so there is nothing left to call. Treat the
		// replica as reclaimed: the node's whole dataset went with it.
		return nil
	}
	client, err := w.pool.Client(job.Address)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, w.svc.cfg.NodeCallTimeout)
	defer cancel()

	_, err = client.DeleteChunk(callCtx, &flexstorev1.DeleteChunkRequest{ChunkId: job.ChunkID.String()})
	if err != nil {
		// NotFound means the node does not have it, which is the desired end
		// state. Anything else is a genuine failure worth retrying.
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	return nil
}

// StatsWorker refreshes the durability gauges that Prometheus scrapes.
// Computing them on a schedule (rather than inside the /metrics handler) keeps
// a scrape from triggering an expensive aggregate query on every poll.
type StatsWorker struct {
	svc      *Service
	interval time.Duration
}

// NewStatsWorker builds the metrics refresher.
func NewStatsWorker(svc *Service, interval time.Duration) *StatsWorker {
	return &StatsWorker{svc: svc, interval: interval}
}

// Run refreshes until ctx is cancelled.
func (w *StatsWorker) Run(ctx context.Context) error {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	// Publish once immediately so a freshly started coordinator does not export
	// zeros for a whole interval.
	w.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		w.refresh(ctx)
	}
}

func (w *StatsWorker) refresh(ctx context.Context) {
	// Damage counters come from the index and are cheap, so they refresh on
	// every tick. Everything else rides along on the same cadence.
	under, unavailable, err := w.svc.store.DurabilityCounts(ctx, w.svc.cfg.ReplicationFactor)
	if err != nil {
		if ctx.Err() == nil {
			w.svc.log.Error("failed to refresh durability counters", slog.String("error", err.Error()))
		}
		return
	}
	w.svc.m.UnderReplicatedChunks.Set(float64(under))
	w.svc.m.UnavailableChunks.Set(float64(unavailable))

	if counts, err := w.svc.store.RepairCounts(ctx); err == nil {
		for _, st := range []metadata.RepairJobState{
			metadata.RepairPending, metadata.RepairRunning,
			metadata.RepairSucceeded, metadata.RepairFailed,
		} {
			w.svc.m.RepairJobs.WithLabelValues(string(st)).Set(float64(counts[st]))
		}
	} else if ctx.Err() == nil {
		w.svc.log.Warn("failed to refresh repair job counts", slog.String("error", err.Error()))
	}

	stats, err := w.svc.store.ClusterStats(ctx, w.svc.cfg.ReplicationFactor)
	if err != nil {
		if ctx.Err() == nil {
			w.svc.log.Error("failed to refresh cluster stats", slog.String("error", err.Error()))
		}
		return
	}
	w.svc.m.ChunksTotal.Set(float64(stats.TotalChunks))
	for _, st := range []metadata.ObjectState{
		metadata.ObjectUploading, metadata.ObjectComplete,
		metadata.ObjectDeleting, metadata.ObjectFailed,
	} {
		w.svc.m.ObjectsTotal.WithLabelValues(string(st)).Set(float64(stats.ObjectsByState[st]))
	}

	nodes, err := w.svc.store.ListNodes(ctx, false)
	if err != nil {
		return
	}
	for _, n := range nodes {
		st, _ := health.ParseState(n.Health)
		w.svc.m.NodeHealth.WithLabelValues(n.ID).Set(float64(st))
		w.svc.m.StorageUsedBytes.WithLabelValues(n.ID).Set(float64(n.UsedBytes))
		w.svc.m.StorageCapacityBytes.WithLabelValues(n.ID).Set(float64(n.TotalBytes))
		w.svc.m.NodeActiveRequests.WithLabelValues(n.ID).Set(float64(n.ActiveRequests))
	}
}

// DurabilityAuditor periodically recomputes the denormalised
// chunks.available_replicas counters from the underlying replica rows.
//
// The trigger keeps them exact during normal operation, so this should always
// find zero drift. It exists because the counter is what the repair scanner
// trusts: if anything ever bypassed the intended write path, durability would
// silently look fine while chunks rotted. A non-zero result is exported as
// flexstore_durability_counter_drift_total and logged at ERROR.
type DurabilityAuditor struct {
	svc      *Service
	interval time.Duration
}

// NewDurabilityAuditor builds the auditor.
func NewDurabilityAuditor(svc *Service, interval time.Duration) *DurabilityAuditor {
	return &DurabilityAuditor{svc: svc, interval: interval}
}

// Run audits until ctx is cancelled.
func (a *DurabilityAuditor) Run(ctx context.Context) error {
	t := time.NewTicker(a.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		drift, err := a.svc.store.VerifyDurabilityCounters(ctx)
		if err != nil {
			if ctx.Err() == nil {
				a.svc.log.Error("durability audit failed", slog.String("error", err.Error()))
			}
			continue
		}
		if drift > 0 {
			a.svc.m.DurabilityDrift.Add(float64(drift))
			a.svc.log.Error("durability counters had drifted and were corrected; "+
				"something wrote replica rows outside the normal path",
				slog.Int64("chunks_corrected", drift))
		}
	}
}

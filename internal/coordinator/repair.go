package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/health"
	"github.com/harjeetschahal/flexstore/internal/metadata"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
	"github.com/harjeetschahal/flexstore/internal/placement"
)

// RepairManager restores durability: it finds under-replicated chunks and
// copies them onto healthy nodes until the replication factor is met again.
//
// Structure:
//
//	scanner   one goroutine, enqueues jobs for damaged chunks on a timer
//	workers   N goroutines, each claims one job at a time and executes it
//	reaper    reclaims leases abandoned by a crashed coordinator
//
// Every piece of durable state lives in PostgreSQL (repair_jobs), so the
// manager holds no recovery-critical state in memory. Killing the coordinator
// mid-repair loses at most the in-flight copies, and those become claimable
// again as soon as their leases expire.
type RepairManager struct {
	svc  *Service
	pool *nodeclient.Pool
	cfg  RepairConfig
	log  *slog.Logger

	// owner identifies this coordinator instance in job leases, so an operator
	// can see which process is working on what.
	owner string

	// inFlightPerNode bounds how many concurrent repairs may target the same
	// node. Without it, losing one node in a five-node cluster would point
	// every repair at whichever node currently looks emptiest and bury it --
	// the replication storm this milestone explicitly has to avoid.
	mu              sync.Mutex
	inFlightPerNode map[string]int
}

// RepairConfig tunes the manager.
type RepairConfig struct {
	Enabled bool
	// Workers is the total number of concurrent repairs.
	Workers int
	// ScanInterval is how often to look for newly damaged chunks.
	ScanInterval time.Duration
	// ScanBatch bounds how many jobs one scan may enqueue, so a mass failure
	// produces a steady stream of work rather than one enormous transaction.
	ScanBatch int
	// MaxPerNode bounds concurrent repairs targeting a single node.
	MaxPerNode int
	// JobTimeout bounds one copy operation.
	JobTimeout time.Duration
	// LeaseDuration is how long a claimed job stays claimed. Must exceed
	// JobTimeout, or a worker's own job could be stolen while it still runs.
	LeaseDuration time.Duration
	// MaxAttempts before a job is marked FAILED.
	MaxAttempts int
	// BaseBackoff is the first retry delay; it doubles per attempt.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// ReplicationFactor is the durability target.
	ReplicationFactor int
	// HistoryRetention is how long finished jobs are kept.
	HistoryRetention time.Duration
}

// NewRepairManager builds the manager.
func NewRepairManager(svc *Service, pool *nodeclient.Pool, cfg RepairConfig, owner string, log *slog.Logger) *RepairManager {
	return &RepairManager{
		svc:             svc,
		pool:            pool,
		cfg:             cfg,
		log:             log.With(slog.String("component", "repair")),
		owner:           owner,
		inFlightPerNode: make(map[string]int),
	}
}

// Run starts the scanner, the workers and the lease reaper, and blocks until
// ctx is cancelled.
func (m *RepairManager) Run(ctx context.Context) error {
	if !m.cfg.Enabled {
		m.log.Warn("repair manager disabled by configuration; " +
			"under-replicated chunks will be reported but not fixed")
		<-ctx.Done()
		return nil
	}

	// Any job left RUNNING by a previous incarnation of this process is
	// unowned now. Reclaim immediately rather than waiting for the lease to
	// lapse, so a restart resumes repair in seconds instead of minutes.
	if n, err := m.svc.store.ReclaimExpiredRepairLeases(ctx); err != nil {
		m.log.Error("failed to reclaim repair leases at startup", slog.String("error", err.Error()))
	} else if n > 0 {
		m.log.Info("reclaimed repair jobs abandoned by a previous run", slog.Int64("jobs", n))
	}

	m.log.Info("repair manager started",
		slog.Int("workers", m.cfg.Workers),
		slog.Int("replication_factor", m.cfg.ReplicationFactor),
		slog.Duration("scan_interval", m.cfg.ScanInterval),
		slog.Int("max_per_node", m.cfg.MaxPerNode))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); m.runScanner(ctx) }()

	for i := 0; i < m.cfg.Workers; i++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); m.runWorker(ctx, id) }(i)
	}

	wg.Wait()
	m.log.Info("repair manager stopped")
	return nil
}

// runScanner periodically enqueues repair jobs for damaged chunks.
func (m *RepairManager) runScanner(ctx context.Context) {
	t := time.NewTicker(m.cfg.ScanInterval)
	defer t.Stop()

	// Scan once immediately: after a restart there may already be damage, and
	// waiting a full interval to notice is wasted recovery time.
	m.scanOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		m.scanOnce(ctx)
	}
}

func (m *RepairManager) scanOnce(ctx context.Context) {
	if n, err := m.svc.store.ReclaimExpiredRepairLeases(ctx); err != nil {
		if ctx.Err() == nil {
			m.log.Error("failed to reclaim expired leases", slog.String("error", err.Error()))
		}
	} else if n > 0 {
		m.log.Warn("reclaimed repair jobs with expired leases", slog.Int64("jobs", n))
	}

	enqueued, err := m.svc.store.EnqueueRepairs(ctx, m.cfg.ReplicationFactor, m.cfg.ScanBatch)
	if err != nil {
		if ctx.Err() == nil {
			m.log.Error("repair scan failed", slog.String("error", err.Error()))
		}
		return
	}
	if enqueued > 0 {
		m.log.Info("scheduled repairs for under-replicated chunks", slog.Int64("jobs", enqueued))
	}

	// Recovery is bidirectional. A node that dies gets its chunks re-replicated
	// elsewhere; when it returns and its copies are verified, those chunks sit
	// at RF+1. Trimming here -- in the component that owns "replication equals
	// RF" -- keeps a node failure from permanently inflating storage.
	if trimmed, err := m.svc.store.TrimOverReplicatedChunks(ctx, m.cfg.ReplicationFactor, m.cfg.ScanBatch); err != nil {
		if ctx.Err() == nil {
			m.log.Error("failed to trim over-replicated chunks", slog.String("error", err.Error()))
		}
	} else if trimmed > 0 {
		m.svc.m.ReplicasTrimmed.Add(float64(trimmed))
		m.log.Info("trimmed excess replicas left by recovery", slog.Int64("replicas", trimmed))
	}

	if _, err := m.svc.store.PurgeFinishedRepairJobs(ctx, m.cfg.HistoryRetention, m.cfg.ReplicationFactor, 500); err != nil && ctx.Err() == nil {
		m.log.Warn("failed to trim repair history", slog.String("error", err.Error()))
	}
}

// runWorker claims and executes repair jobs until ctx is cancelled.
func (m *RepairManager) runWorker(ctx context.Context, id int) {
	log := m.log.With(slog.Int("worker", id))

	// Idle backoff: with no work, polling the queue tightly would be pure
	// database load. The scanner's interval is the natural upper bound.
	idle := 250 * time.Millisecond
	maxIdle := m.cfg.ScanInterval
	if maxIdle < time.Second {
		maxIdle = time.Second
	}

	for {
		if ctx.Err() != nil {
			return
		}

		worked, err := m.claimAndRepair(ctx, log)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Error("repair worker error", slog.String("error", err.Error()))
		case worked:
			idle = 250 * time.Millisecond
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(idle):
		}
		if idle < maxIdle {
			idle *= 2
		}
	}
}

// claimAndRepair executes at most one job. It returns true when work was done,
// so the worker knows whether to poll again immediately.
func (m *RepairManager) claimAndRepair(ctx context.Context, log *slog.Logger) (bool, error) {
	target, err := m.svc.store.ClaimRepairJob(ctx, m.owner, m.cfg.LeaseDuration, m.cfg.ReplicationFactor)
	if errors.Is(err, metadata.ErrNoRepairWork) {
		return false, nil
	}
	if err != nil {
		if errors.Is(err, metadata.ErrNotFound) {
			// The chunk vanished while the job waited (its object was deleted).
			// Nothing to repair; that is a success, not a failure.
			return true, nil
		}
		return false, err
	}

	start := time.Now()
	log = log.With(
		slog.Int64("job_id", target.Job.ID),
		slog.String("chunk_id", target.Job.ChunkID.String()),
		slog.Int("attempt", int(target.Job.Attempts)))

	err = m.executeRepair(ctx, target, log)
	elapsed := time.Since(start)

	if err == nil {
		m.svc.m.RepairDuration.WithLabelValues("succeeded").Observe(elapsed.Seconds())
		return true, nil
	}

	// Cancellation during shutdown is not a repair failure. The job stays
	// RUNNING; its lease lapses and another worker picks it up. Recording it as
	// a failure would burn a retry attempt for a shutdown nobody caused.
	if ctx.Err() != nil {
		return true, nil //nolint:nilerr // shutdown, not a repair failure
	}

	m.svc.m.RepairDuration.WithLabelValues("failed").Observe(elapsed.Seconds())
	m.svc.m.RepairsTotal.WithLabelValues("failed").Inc()
	m.svc.m.RepairsFailedTotal.Inc()

	backoff := m.backoffFor(int(target.Job.Attempts))
	terminal, ferr := m.svc.store.FailRepair(ctx, target.Job.ID, err.Error(), backoff, m.cfg.MaxAttempts)
	if ferr != nil {
		return true, ferr
	}
	if terminal {
		log.Error("repair permanently failed; chunk remains under-replicated and needs an operator",
			slog.String("error", err.Error()))
	} else {
		log.Warn("repair attempt failed; will retry",
			slog.String("error", err.Error()), slog.Duration("backoff", backoff))
	}
	return true, nil
}

// executeRepair performs one copy: pick a source, pick a destination, pull the
// chunk, verify it, record it.
func (m *RepairManager) executeRepair(ctx context.Context, target metadata.RepairTarget, log *slog.Logger) error {
	job := target.Job

	// Nothing to do: the chunk recovered on its own (its node came back and
	// reconciled) between enqueue and execution.
	if target.DesiredReplicas <= 0 {
		log.Debug("chunk already at full replication; closing job")
		return m.svc.store.FinishRepairAsNoop(ctx, job.ID, "already at full replication")
	}

	if len(target.Sources) == 0 {
		// Every replica is unusable. This is reported loudly and the job is
		// retried later -- a node may still come back. Critically, no metadata
		// is destroyed: the chunk keeps its replica rows so a returning node
		// can be reconciled and restore the data.
		m.svc.m.UnavailableChunkEvents.Inc()
		log.Error("chunk has no readable replica; data is currently unavailable",
			slog.String("chunk_id", job.ChunkID.String()))
		return fmt.Errorf("%w for chunk %s", metadata.ErrNoSource, job.ChunkID)
	}
	if target.Checksum == "" {
		return fmt.Errorf("chunk %s has no recorded checksum; refusing to copy unverifiable data", job.ChunkID)
	}

	source := target.Sources[0]

	dest, release, err := m.pickDestination(ctx, target)
	if err != nil {
		// Every node already holds a replica row for this chunk, and at least
		// one of those rows is not an AVAILABLE copy: it is corrupt bytes
		// awaiting collection, an unverified copy awaiting the reconciler, or a
		// copy on a node that is currently down. Copying is not the remedy for
		// any of those, and retrying would burn the job's attempts on a
		// situation no number of copies can resolve -- ending in FAILED, where
		// it would sit until something external woke it up.
		//
		// Close the job instead. The blocking rows are cleared by the GC, the
		// reconciler or the health monitor within seconds, and the durability
		// scanner re-enqueues the chunk if it is still short afterwards. This
		// is a deferral, not a dismissal: nothing is forgotten, because the
		// chunk's under-replication is derived state, not a queue entry.
		if errors.Is(err, placement.ErrInsufficientNodes) && target.TransientOccupancy > 0 {
			log.Info("deferring repair; every candidate node holds a replica row that is not yet clear",
				slog.Int("transient_occupancy", target.TransientOccupancy),
				slog.Int("stale_replicas", target.StaleReplicas))
			return m.svc.store.FinishRepairAsNoop(ctx, job.ID,
				fmt.Sprintf("deferred: all %d candidate nodes hold a non-available replica row",
					target.TransientOccupancy))
		}
		return err
	}
	defer release()

	if err := m.svc.store.RecordRepairPlan(ctx, job.ID, source.NodeID, dest.ID); err != nil {
		return err
	}

	log = log.With(
		slog.String("source_node", source.NodeID),
		slog.String("target_node", dest.ID),
		slog.Int64("size_bytes", target.Size))
	log.Info("starting repair")

	client, err := m.pool.Client(dest.Address)
	if err != nil {
		return fmt.Errorf("dial destination %s: %w", dest.ID, err)
	}

	callCtx, cancel := context.WithTimeout(ctx, m.cfg.JobTimeout)
	defer cancel()

	// Pull semantics: the destination fetches from the source. The copy work
	// therefore lands on the node that gains the replica, not on the
	// coordinator, which never touches object bytes.
	resp, err := client.ReplicateChunk(callCtx, &flexstorev1.ReplicateChunkRequest{
		ChunkId:        job.ChunkID.String(),
		SourceAddress:  source.Address,
		ChecksumSha256: target.Checksum,
		SizeBytes:      target.Size,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			// The source served bytes that did not match the recorded digest.
			// That source is corrupt; demote it so the next attempt picks a
			// different one instead of retrying the same bad copy forever.
			m.svc.m.ChecksumFailures.WithLabelValues("repair").Inc()
			log.Error("source replica failed verification during repair; demoting it",
				slog.String("error", err.Error()))
			if _, derr := m.svc.store.MarkReplicaCorrupt(ctx, job.ChunkID, source.NodeID,
				"failed verification during repair: "+err.Error()); derr != nil {
				log.Error("failed to demote corrupt source", slog.String("error", derr.Error()))
			}
			return fmt.Errorf("replicate %s from %s to %s: %w", job.ChunkID, source.NodeID, dest.ID, err)
		}

		// Any other pull failure gets one direct question to the source: do you
		// actually hold this chunk? The read path already reports phantoms it
		// trips over, but repair was the one consumer of replica metadata with
		// no way to shed one -- a phantom AVAILABLE row made every attempt fail
		// with NotFound and the job retried the same dead source until its
		// budget ran out (observed on a CI runner, where the widened timing
		// windows made it reproducible). The probe is one cheap RPC on an
		// already-failed path.
		//
		// The row is dropped only on a POSITIVE answer: the source is up,
		// responding, and says the chunk is not there. A transport error keeps
		// the row -- an unreachable node's replicas are the health monitor's
		// call, not repair's.
		m.reportPhantomSource(ctx, log, job.ChunkID, source)
		return fmt.Errorf("replicate %s from %s to %s: %w", job.ChunkID, source.NodeID, dest.ID, err)
	}

	// Independent post-copy verification. ReplicateChunk already verifies while
	// writing, but re-reading from disk also proves the file was actually
	// persisted and named correctly, which a write-path digest does not.
	check, err := client.CheckChunk(callCtx, &flexstorev1.CheckChunkRequest{
		ChunkId: job.ChunkID.String(), VerifyChecksum: true,
	})
	if err != nil {
		return fmt.Errorf("verify repaired chunk on %s: %w", dest.ID, err)
	}
	if !check.Exists {
		return fmt.Errorf("chunk %s missing on %s immediately after repair", job.ChunkID, dest.ID)
	}
	if check.ChecksumSha256 != target.Checksum {
		m.svc.m.ChecksumFailures.WithLabelValues("repair-verify").Inc()
		return fmt.Errorf("repaired chunk %s on %s has checksum %s, expected %s",
			job.ChunkID, dest.ID, check.ChecksumSha256, target.Checksum)
	}

	available, err := m.svc.store.CompleteRepair(ctx, job.ID, job.ChunkID, dest.ID)
	if err != nil {
		return err
	}

	// The chunk now lives somewhere it did not before. A cached layout still
	// names the old replica set, and while the gateway's read failover would
	// survive that, it would pay a failed dial per read until the TTL lapsed --
	// exactly when the cluster is already degraded and least able to spare it.
	m.svc.invalidateChunkOwnerCache(ctx, job.ChunkID)

	m.svc.m.RepairsTotal.WithLabelValues("succeeded").Inc()
	m.svc.m.RepairBytesTotal.Add(float64(resp.BytesWritten))
	m.svc.m.ReplicaCount.WithLabelValues("repaired").Observe(float64(available))

	log.Info("repair complete",
		slog.Int64("bytes", resp.BytesWritten),
		slog.Int("available_replicas", available),
		slog.Bool("already_existed", resp.AlreadyExisted))

	// Still short? The scanner will re-enqueue on its next pass, one replica at
	// a time. Repairing incrementally keeps each job small and independently
	// retryable rather than making one job responsible for N copies.
	return nil
}

// pickDestination chooses a node to receive the new replica and reserves a
// per-node concurrency slot. The returned func releases the slot.
func (m *RepairManager) pickDestination(ctx context.Context, target metadata.RepairTarget) (placement.NodeInfo, func(), error) {
	nodes, err := m.svc.healthyNodes(ctx)
	if err != nil {
		return placement.NodeInfo{}, func() {}, fmt.Errorf("load nodes for repair: %w", err)
	}

	// Exclude every node already associated with this chunk in any replica
	// state, so a repair can never produce two copies on one machine.
	req := placement.Request{
		Key:       target.Job.ChunkID.String(),
		Replicas:  1,
		ChunkSize: target.Size,
		Exclude:   append([]string(nil), target.Occupied...),
	}

	// Also exclude nodes already saturated with repair traffic. This is the
	// storm guard: it spreads recovery load instead of hammering one node.
	m.mu.Lock()
	for id, n := range m.inFlightPerNode {
		if n >= m.cfg.MaxPerNode {
			req.Exclude = append(req.Exclude, id)
		}
	}
	m.mu.Unlock()

	selected, err := m.svc.placer.SelectNodes(nodes, req)
	if err != nil {
		m.svc.m.PlacementFailures.WithLabelValues("repair").Inc()
		return placement.NodeInfo{}, func() {}, fmt.Errorf("no destination for chunk %s: %w",
			target.Job.ChunkID, err)
	}
	dest := selected[0]

	m.mu.Lock()
	m.inFlightPerNode[dest.ID]++
	m.svc.m.RepairsInFlight.Inc()
	m.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			m.mu.Lock()
			m.inFlightPerNode[dest.ID]--
			if m.inFlightPerNode[dest.ID] <= 0 {
				delete(m.inFlightPerNode, dest.ID)
			}
			m.mu.Unlock()
			m.svc.m.RepairsInFlight.Dec()
		})
	}
	return dest, release, nil
}

// backoffFor returns the retry delay after the given attempt number, doubling
// per attempt and capped so a permanently broken chunk does not drift into
// retrying once a day.
func (m *RepairManager) backoffFor(attempt int) time.Duration {
	d := m.cfg.BaseBackoff
	for i := 1; i < attempt && d < m.cfg.MaxBackoff; i++ {
		d *= 2
	}
	if d > m.cfg.MaxBackoff {
		d = m.cfg.MaxBackoff
	}
	return d
}

// OnNodeRecovered gives previously-failed repairs another chance.
//
// "There was nowhere to put it" is the most common terminal failure, and a node
// joining or recovering is exactly the event that fixes it. Without this, a
// cluster that briefly ran out of placement targets would stay under-replicated
// until an operator intervened.
func (m *RepairManager) OnNodeRecovered(ctx context.Context, nodeID string) {
	if !m.cfg.Enabled {
		return
	}
	n, err := m.svc.store.RequeueFailedRepairs(ctx, m.cfg.ScanBatch)
	if err != nil {
		m.log.Error("failed to requeue repairs after node recovery",
			slog.String("node_id", nodeID), slog.String("error", err.Error()))
		return
	}
	if n > 0 {
		m.log.Info("requeued previously failed repairs after node recovery",
			slog.String("node_id", nodeID), slog.Int64("jobs", n))
	}
}

// healthStateName is a small helper shared with the reconciler for logging.
func healthStateName(s health.State) string { return s.String() }

// reportPhantomSource checks whether a repair source that just failed actually
// holds the chunk, and drops its replica row if the node itself says no.
// Mirrors what the gateway does when a read meets a phantom, for the same
// reason: metadata claiming a replica that does not exist is a durability lie,
// and every subsystem that discovers one must be able to correct it.
func (m *RepairManager) reportPhantomSource(ctx context.Context, log *slog.Logger, chunkID uuid.UUID, source metadata.Replica) {
	client, err := m.pool.Client(source.Address)
	if err != nil {
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	check, err := client.CheckChunk(probeCtx, &flexstorev1.CheckChunkRequest{
		ChunkId: chunkID.String(),
	})
	if err != nil || check.Exists {
		return
	}
	log.Error("repair source is a phantom: metadata says AVAILABLE, the node says the chunk is not there; dropping the replica row",
		slog.String("source_node", source.NodeID))
	m.svc.m.ReconcileActions.WithLabelValues("phantom").Inc()
	if derr := m.svc.store.DropReplica(ctx, chunkID, source.NodeID); derr != nil {
		log.Error("failed to drop phantom source replica", slog.String("error", derr.Error()))
		return
	}
	m.svc.invalidateChunkOwnerCache(ctx, chunkID)
}

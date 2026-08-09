package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/metadata"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
)

// Reconciler makes a returning node's disk agree with the coordinator's
// metadata, in that direction.
//
// The rule this enforces: **metadata is the source of truth, a returning node's
// files are only evidence.** When a node comes back, its replicas are demoted
// to STALE (they stop counting towards durability) and a reconciliation is
// queued. Only after the node proves, chunk by chunk, that it still holds the
// right bytes does a replica become AVAILABLE again.
//
// Three classes of disagreement, each with a different remedy:
//
//	verified  metadata says the node has it, and it does, with a matching
//	          SHA-256           -> promote STALE to AVAILABLE
//	phantom   metadata says the node has it, but it does not (or the bytes are
//	          wrong)            -> drop the replica row, which makes the chunk
//	                               under-replicated and schedules a repair
//	orphan    the node has a file metadata knows nothing about, e.g. the object
//	          was deleted while the node was down
//	                            -> queue the file for deletion
type Reconciler struct {
	svc  *Service
	pool *nodeclient.Pool
	cfg  ReconcileConfig
	log  *slog.Logger

	owner string
}

// ReconcileConfig tunes the reconciler.
type ReconcileConfig struct {
	Enabled  bool
	Interval time.Duration
	// PageSize bounds how many stale replicas are verified per round trip.
	PageSize int
	// JobTimeout bounds one whole node reconciliation.
	JobTimeout    time.Duration
	LeaseDuration time.Duration
	// VerifyChecksums re-hashes each chunk on the node. Correct but expensive;
	// when off, presence and size are checked instead. Reads still verify
	// checksums either way, so this trades scrub thoroughness, not safety.
	VerifyChecksums bool
}

// NewReconciler builds the reconciler.
func NewReconciler(svc *Service, pool *nodeclient.Pool, cfg ReconcileConfig, owner string, log *slog.Logger) *Reconciler {
	return &Reconciler{
		svc:   svc,
		pool:  pool,
		cfg:   cfg,
		log:   log.With(slog.String("component", "reconcile")),
		owner: owner,
	}
}

// Run processes reconciliation jobs until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	if !r.cfg.Enabled {
		r.log.Warn("reconciler disabled; returning nodes will keep STALE replicas " +
			"and their chunks will be re-replicated elsewhere instead of being verified")
		<-ctx.Done()
		return nil
	}

	if n, err := r.svc.store.ReclaimExpiredReconcileLeases(ctx); err != nil {
		r.log.Error("failed to reclaim reconcile leases at startup", slog.String("error", err.Error()))
	} else if n > 0 {
		r.log.Info("reclaimed reconciliations abandoned by a previous run", slog.Int64("jobs", n))
	}

	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	r.log.Info("reconciler started",
		slog.Duration("interval", r.cfg.Interval),
		slog.Bool("verify_checksums", r.cfg.VerifyChecksums))

	for {
		// Drain the queue, then wait. Draining rather than doing one job per
		// tick means a mass restart is reconciled promptly instead of one node
		// per interval.
		for {
			done, err := r.runOne(ctx)
			if err != nil && ctx.Err() == nil {
				r.log.Error("reconciliation failed", slog.String("error", err.Error()))
			}
			if !done || ctx.Err() != nil {
				break
			}
		}

		select {
		case <-ctx.Done():
			r.log.Info("reconciler stopped")
			return nil
		case <-t.C:
		}

		if _, err := r.svc.store.ReclaimExpiredReconcileLeases(ctx); err != nil && ctx.Err() == nil {
			r.log.Warn("failed to reclaim reconcile leases", slog.String("error", err.Error()))
		}
		r.sweepStaleNodes(ctx)
	}
}

// sweepStaleNodes queues reconciliation for any healthy node still holding
// unverified replicas.
//
// Rejoin already enqueues a reconciliation, but that enqueue can be lost: the
// job may FAIL because the node was still starting when its inventory was
// requested, or be dropped by the idempotence check if the node flaps while one
// is already RUNNING. Nothing would ever retry it.
//
// The consequence is worse than it sounds. STALE replicas are excluded from
// durability *and* they occupy their node, so the repair manager cannot use it
// as a destination either. A chunk whose every remaining node holds a STALE
// copy therefore deadlocks: permanently under-replicated, with no legal
// placement. Sweeping on the actual condition rather than on the event that
// caused it makes STALE resolve regardless of how it arose.
func (r *Reconciler) sweepStaleNodes(ctx context.Context) {
	nodes, err := r.svc.store.NodesWithStaleReplicas(ctx, 32)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Error("failed to scan for unverified replicas", slog.String("error", err.Error()))
		}
		return
	}
	for _, nodeID := range nodes {
		queued, err := r.svc.store.EnqueueReconcile(ctx, nodeID)
		if err != nil {
			r.log.Error("failed to queue reconciliation for a node with unverified replicas",
				slog.String("node_id", nodeID), slog.String("error", err.Error()))
			continue
		}
		if queued {
			r.log.Info("queued reconciliation for a node still holding unverified replicas",
				slog.String("node_id", nodeID))
		}
	}
}

// runOne claims and executes one reconciliation. Returns false when the queue
// is empty.
func (r *Reconciler) runOne(ctx context.Context) (bool, error) {
	job, err := r.svc.store.ClaimReconcileJob(ctx, r.owner, r.cfg.LeaseDuration)
	if errors.Is(err, metadata.ErrNoReconcileWork) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	log := r.log.With(slog.String("node_id", job.NodeID), slog.Int64("job_id", job.ID))
	start := time.Now()
	log.Info("reconciling returning node")

	jobCtx, cancel := context.WithTimeout(ctx, r.cfg.JobTimeout)
	defer cancel()

	res, err := r.reconcileNode(jobCtx, job, log)
	if err != nil {
		if ctx.Err() != nil {
			return true, nil // shutting down; lease will lapse
		}
		r.svc.m.ReconcileTotal.WithLabelValues("failed").Inc()
		log.Error("reconciliation failed", slog.String("error", err.Error()))
		if ferr := r.svc.store.FailReconcile(context.WithoutCancel(ctx), job.ID, err.Error()); ferr != nil {
			return true, ferr
		}
		return true, nil
	}

	if err := r.svc.store.CompleteReconcile(ctx, job.ID, res); err != nil {
		return true, err
	}
	r.svc.m.ReconcileTotal.WithLabelValues("succeeded").Inc()
	r.svc.m.ReconcileDuration.Observe(time.Since(start).Seconds())

	log.Info("reconciliation complete",
		slog.Int64("chunks_on_node", res.ChunksSeen),
		slog.Int64("verified", res.Verified),
		slog.Int64("phantoms", res.PhantomsFound),
		slog.Int64("orphans", res.OrphansFound),
		slog.Int64("corrupt", res.CorruptFound),
		slog.Duration("took", time.Since(start)))

	// A node that just proved it still holds good data is a placement target
	// again, so retry repairs that previously had nowhere to go.
	if rm := r.svc.repairManager(); rm != nil {
		rm.OnNodeRecovered(ctx, job.NodeID)
	}
	return true, nil
}

func (r *Reconciler) reconcileNode(ctx context.Context, job metadata.ReconcileJob, log *slog.Logger) (metadata.ReconcileResult, error) {
	var res metadata.ReconcileResult

	if job.Address == "" {
		return res, fmt.Errorf("node %s has no address", job.NodeID)
	}
	client, err := r.pool.Client(job.Address)
	if err != nil {
		return res, fmt.Errorf("dial %s: %w", job.NodeID, err)
	}

	// Phase 1: what does metadata think this node holds?
	known, err := r.svc.store.ChunkIDsKnownOnNode(ctx, job.NodeID)
	if err != nil {
		return res, err
	}

	// Phase 2: what does the node actually hold? Streamed, so a node with
	// millions of chunks does not have to be materialised in one message.
	stream, err := client.ListChunks(ctx, &flexstorev1.ListChunksRequest{BatchSize: int32(r.cfg.PageSize)})
	if err != nil {
		return res, fmt.Errorf("list chunks on %s: %w", job.NodeID, err)
	}

	onDisk := make(map[uuid.UUID]struct{})
	for {
		msg, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			return res, fmt.Errorf("stream chunk inventory from %s: %w", job.NodeID, rerr)
		}
		for _, entry := range msg.Chunks {
			id, perr := uuid.Parse(entry.ChunkId)
			if perr != nil {
				// A file whose name is not a chunk ID cannot belong to us.
				// Leave it alone rather than deleting something unrecognised.
				log.Warn("skipping unrecognised file on node", slog.String("name", entry.ChunkId))
				continue
			}
			onDisk[id] = struct{}{}
			res.ChunksSeen++
		}
	}

	// Phase 3: orphans -- on disk, but metadata has no replica row for them
	// here. Their object was almost certainly deleted while the node was down.
	for id := range onDisk {
		if _, ok := known[id]; ok {
			continue
		}
		// QueueOrphanDeletion re-checks under SQL whether this really is an
		// orphan: `known` is a snapshot taken before the inventory stream, and
		// an upload that landed since would look identical to an orphan here.
		queued, err := r.svc.store.QueueOrphanDeletion(ctx, id, job.NodeID)
		if err != nil {
			return res, err
		}
		if !queued {
			// A write landed while we were streaming. Not an orphan.
			continue
		}
		res.OrphansFound++
		r.svc.m.ReconcileActions.WithLabelValues("orphan").Inc()
	}
	if res.OrphansFound > 0 {
		log.Warn("queued orphaned chunks for deletion",
			slog.Int64("count", res.OrphansFound))
	}

	// Phase 4: verify the STALE replicas, page by page.
	after := uuid.Nil
	for {
		stale, err := r.svc.store.StaleReplicasOnNode(ctx, job.NodeID, after, r.cfg.PageSize)
		if err != nil {
			return res, err
		}
		if len(stale) == 0 {
			break
		}
		for _, rep := range stale {
			after = rep.ChunkID

			if _, present := onDisk[rep.ChunkID]; !present {
				// Phantom: metadata claimed it, the disk does not have it.
				// Dropping the row is what makes the chunk under-replicated and
				// therefore repairable. Keeping it would be a durability lie.
				//
				// Only STALE replicas reach this loop, and STALE is never
				// applied to an in-flight write (see MarkReplicasStaleOnNode),
				// so this cannot fire for a chunk that is merely still
				// uploading.
				if err := r.svc.store.DropReplica(ctx, rep.ChunkID, job.NodeID); err != nil {
					return res, err
				}
				r.svc.invalidateChunkOwnerCache(ctx, rep.ChunkID)
				res.PhantomsFound++
				r.svc.m.ReconcileActions.WithLabelValues("phantom").Inc()
				continue
			}

			ok, err := r.verifyReplica(ctx, client, job.NodeID, rep)
			if err != nil {
				return res, err
			}
			if ok {
				promoted, err := r.svc.store.PromoteVerifiedReplica(ctx, rep.ChunkID, job.NodeID)
				if err != nil {
					return res, err
				}
				if promoted {
					r.svc.invalidateChunkOwnerCache(ctx, rep.ChunkID)
					res.Verified++
					r.svc.m.ReconcileActions.WithLabelValues("verified").Inc()
				}
				continue
			}

			// Present but wrong. Demote and queue removal; repair replaces it.
			if _, err := r.svc.store.MarkReplicaCorrupt(ctx, rep.ChunkID, job.NodeID,
				"failed verification during reconciliation"); err != nil {
				return res, err
			}
			r.svc.invalidateChunkOwnerCache(ctx, rep.ChunkID)
			res.CorruptFound++
			r.svc.m.ChecksumFailures.WithLabelValues("reconcile").Inc()
			r.svc.m.ReconcileActions.WithLabelValues("corrupt").Inc()
		}
		if len(stale) < r.cfg.PageSize {
			break
		}
	}

	return res, nil
}

// verifyReplica asks the node whether it really holds a chunk with the recorded
// checksum.
func (r *Reconciler) verifyReplica(ctx context.Context, client flexstorev1.StorageNodeServiceClient,
	nodeID string, rep metadata.StaleReplicaOnNode) (bool, error) {
	resp, err := client.CheckChunk(ctx, &flexstorev1.CheckChunkRequest{
		ChunkId:        rep.ChunkID.String(),
		VerifyChecksum: r.cfg.VerifyChecksums,
	})
	if err != nil {
		return false, fmt.Errorf("check chunk %s on %s: %w", rep.ChunkID, nodeID, err)
	}
	if !resp.Exists {
		return false, nil
	}
	if resp.SizeBytes != rep.Size {
		return false, nil
	}
	if r.cfg.VerifyChecksums {
		// An empty recorded checksum should be impossible for a committed
		// chunk; treating it as unverifiable is safer than treating it as OK.
		return rep.Checksum != "" && resp.ChecksumSha256 == rep.Checksum, nil
	}
	return true, nil
}

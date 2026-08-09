package coordinator

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/metadata"
)

// ReportCorruptReplica records that a replica failed SHA-256 verification.
//
// This is the link between detection and repair. The gateway already fails over
// to another replica when it finds bad bytes, but without telling the control
// plane the bad copy would sit there being re-tried forever. Demoting it makes
// the chunk under-replicated, which the repair scanner turns into a job.
func (s *Service) ReportCorruptReplica(ctx context.Context, req *flexstorev1.ReportCorruptReplicaRequest) (*flexstorev1.ReportCorruptReplicaResponse, error) {
	chunkID, err := uuid.Parse(req.ChunkId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_id: %v", err)
	}
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	remaining, err := s.store.MarkReplicaCorrupt(ctx, chunkID, req.NodeId, req.Detail)
	if err != nil {
		return nil, toStatus(err, "report corrupt replica")
	}
	s.m.ChecksumFailures.WithLabelValues("reported").Inc()

	log := s.log.With(
		slog.String("chunk_id", req.ChunkId),
		slog.String("node_id", req.NodeId),
		slog.Int("remaining_replicas", remaining))

	if remaining == 0 {
		// Worth its own log line at ERROR: this chunk cannot currently be read
		// from anywhere, which is categorically worse than under-replication.
		log.Error("corruption reported and no usable replica remains; chunk is unreadable",
			slog.String("detail", req.Detail))
	} else {
		log.Warn("replica reported corrupt; scheduling replacement",
			slog.String("detail", req.Detail))
	}

	// Enqueue eagerly rather than waiting for the next scan, so a corrupt
	// replica found during a read is being replaced within milliseconds.
	scheduled := false
	if rm := s.repairManager(); rm != nil && rm.cfg.Enabled && remaining > 0 {
		if n, err := s.store.EnqueueRepairs(ctx, s.cfg.ReplicationFactor, 1); err != nil {
			log.Error("failed to schedule repair after corruption report",
				slog.String("error", err.Error()))
		} else {
			scheduled = n > 0
		}
	}

	// The cached object layout still lists the bad replica as a read target.
	s.invalidateChunkOwnerCache(ctx, chunkID)

	return &flexstorev1.ReportCorruptReplicaResponse{
		RemainingAvailableReplicas: int32(remaining),
		RepairScheduled:            scheduled,
	}, nil
}

// GetReplicationStatus reports durability and repair progress.
func (s *Service) GetReplicationStatus(ctx context.Context, req *flexstorev1.GetReplicationStatusRequest) (*flexstorev1.GetReplicationStatusResponse, error) {
	under, unavailable, err := s.store.DurabilityCounts(ctx, s.cfg.ReplicationFactor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "durability counts: %v", err)
	}
	total, err := s.store.CommittedChunkCount(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "chunk count: %v", err)
	}
	counts, err := s.store.RepairCounts(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "repair counts: %v", err)
	}

	limit := int(req.RecentJobLimit)
	jobs, err := s.store.RecentRepairJobs(ctx, limit)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "recent repair jobs: %v", err)
	}

	resp := &flexstorev1.GetReplicationStatusResponse{
		ReplicationFactor:     int32(s.cfg.ReplicationFactor),
		TotalChunks:           total,
		UnderReplicatedChunks: under,
		UnavailableChunks:     unavailable,
		JobsPending:           counts[metadata.RepairPending],
		JobsRunning:           counts[metadata.RepairRunning],
		JobsFailed:            counts[metadata.RepairFailed],
		JobsSucceededTotal:    counts[metadata.RepairSucceeded],
		RecentJobs:            make([]*flexstorev1.RepairJobInfo, 0, len(jobs)),
		RepairEnabled:         s.repairManager() != nil && s.repairManager().cfg.Enabled,
	}
	for _, j := range jobs {
		info := &flexstorev1.RepairJobInfo{
			Id:           j.ID,
			ChunkId:      j.ChunkID.String(),
			State:        toProtoRepairState(j.State),
			SourceNodeId: j.SourceNodeID,
			TargetNodeId: j.TargetNodeID,
			Attempts:     j.Attempts,
			LastError:    j.LastError,
			CreatedAt:    timestamppb.New(j.CreatedAt),
			UpdatedAt:    timestamppb.New(j.UpdatedAt),
		}
		resp.RecentJobs = append(resp.RecentJobs, info)
	}
	return resp, nil
}

// TriggerReconcile schedules inventory comparison for one node or all of them.
func (s *Service) TriggerReconcile(ctx context.Context, req *flexstorev1.TriggerReconcileRequest) (*flexstorev1.TriggerReconcileResponse, error) {
	var targets []string
	if req.NodeId != "" {
		if _, err := s.store.GetNode(ctx, req.NodeId); err != nil {
			return nil, toStatus(err, "trigger reconcile")
		}
		targets = []string{req.NodeId}
	} else {
		nodes, err := s.store.ListNodes(ctx, true)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
		}
		for _, n := range nodes {
			targets = append(targets, n.ID)
		}
	}

	scheduled := make([]string, 0, len(targets))
	for _, id := range targets {
		queued, err := s.store.EnqueueReconcile(ctx, id)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "enqueue reconcile for %s: %v", id, err)
		}
		if queued {
			scheduled = append(scheduled, id)
		}
	}
	if len(scheduled) > 0 {
		s.log.Info("reconciliation scheduled on request", slog.Any("nodes", scheduled))
	}
	return &flexstorev1.TriggerReconcileResponse{ScheduledNodeIds: scheduled}, nil
}

// invalidateChunkOwnerCache drops the cached layout of whatever object owns a
// chunk, so the next read does not hand a client a replica we just demoted.
func (s *Service) invalidateChunkOwnerCache(ctx context.Context, chunkID uuid.UUID) {
	bucket, key, err := s.store.ObjectKeyForChunk(ctx, chunkID)
	if err != nil {
		if !errors.Is(err, metadata.ErrNotFound) {
			s.log.Warn("could not resolve chunk owner for cache invalidation",
				slog.String("chunk_id", chunkID.String()), slog.String("error", err.Error()))
		}
		return
	}
	s.invalidateObject(ctx, bucket, key)
}

func toProtoRepairState(s metadata.RepairJobState) flexstorev1.RepairJobState {
	switch s {
	case metadata.RepairPending:
		return flexstorev1.RepairJobState_REPAIR_JOB_STATE_PENDING
	case metadata.RepairRunning:
		return flexstorev1.RepairJobState_REPAIR_JOB_STATE_RUNNING
	case metadata.RepairSucceeded:
		return flexstorev1.RepairJobState_REPAIR_JOB_STATE_SUCCEEDED
	case metadata.RepairFailed:
		return flexstorev1.RepairJobState_REPAIR_JOB_STATE_FAILED
	default:
		return flexstorev1.RepairJobState_REPAIR_JOB_STATE_UNSPECIFIED
	}
}

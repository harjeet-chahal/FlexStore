// Package coordinator implements the control plane: metadata ownership,
// placement, node health and background reclamation.
package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/cache"
	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/health"
	"github.com/harjeetschahal/flexstore/internal/metadata"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/placement"
)

// Service implements CoordinatorService.
type Service struct {
	flexstorev1.UnimplementedCoordinatorServiceServer

	cfg     config.Coordinator
	store   *metadata.Store
	cache   cache.Cache
	monitor *health.Monitor
	placer  placement.Strategy
	log     *slog.Logger
	m       *observability.Metrics

	// repair is set after construction (the manager needs the service), so it
	// is read on the RPC path and written once during startup. Guarded by
	// setRepair/repairManager rather than left as a data race.
	repairMu sync.RWMutex
	repair   *RepairManager
}

// SetRepairManager wires the repair manager in after both are constructed.
// Called once during startup, before the gRPC server accepts traffic.
func (s *Service) SetRepairManager(m *RepairManager) {
	s.repairMu.Lock()
	defer s.repairMu.Unlock()
	s.repair = m
}

func (s *Service) repairManager() *RepairManager {
	s.repairMu.RLock()
	defer s.repairMu.RUnlock()
	return s.repair
}

// NewService wires the control plane together.
func NewService(
	cfg config.Coordinator,
	store *metadata.Store,
	c cache.Cache,
	monitor *health.Monitor,
	placer placement.Strategy,
	log *slog.Logger,
	m *observability.Metrics,
) *Service {
	return &Service{cfg: cfg, store: store, cache: c, monitor: monitor, placer: placer, log: log, m: m}
}

// ---------------------------------------------------------------- node ----

// RegisterNode records (or refreshes) a storage node.
func (s *Service) RegisterNode(ctx context.Context, req *flexstorev1.RegisterNodeRequest) (*flexstorev1.RegisterNodeResponse, error) {
	if req.NodeId == "" || req.GrpcAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and grpc_address are required")
	}
	now := time.Now()
	if err := s.store.UpsertNode(ctx, req.NodeId, req.GrpcAddress,
		health.StateHealthy.String(), toStoreStats(req.Stats), now); err != nil {
		return nil, status.Errorf(codes.Internal, "register node: %v", err)
	}

	prev := s.monitor.State(req.NodeId)
	s.monitor.Observe(req.NodeId, req.GrpcAddress, now)
	s.m.NodeHealth.WithLabelValues(req.NodeId).Set(float64(health.StateHealthy))
	s.invalidateNodeCache(ctx)

	// A returning node's files are evidence, not truth. Its replicas become
	// STALE -- excluded from durability accounting and from reads -- until the
	// reconciler has verified each one against the recorded checksum. Trusting
	// them immediately would let a rolled-back snapshot, a truncated file, or a
	// copy of an object that was deleted meanwhile silently become
	// authoritative again.
	if prev == health.StateDead || prev == health.StateSuspect || prev == health.StateUnknown {
		s.onNodeRejoined(ctx, req.NodeId, prev)
	}

	s.log.Info("storage node registered",
		slog.String("node_id", req.NodeId),
		slog.String("address", req.GrpcAddress),
		slog.Int64("capacity_bytes", req.GetStats().GetTotalBytes()))

	return &flexstorev1.RegisterNodeResponse{
		HeartbeatIntervalMs: s.cfg.HeartbeatInterval.Milliseconds(),
	}, nil
}

// Heartbeat refreshes liveness and capacity.
func (s *Service) Heartbeat(ctx context.Context, req *flexstorev1.HeartbeatRequest) (*flexstorev1.HeartbeatResponse, error) {
	if req.NodeId == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	now := time.Now()
	prev := s.monitor.State(req.NodeId)

	err := s.store.RecordHeartbeat(ctx, req.NodeId, req.GrpcAddress,
		toStoreStats(req.Stats), health.StateHealthy.String(), now)
	if errors.Is(err, metadata.ErrNotFound) {
		// Metadata was reset (or this node never registered). Tell it to
		// re-register rather than silently dropping its heartbeats.
		return &flexstorev1.HeartbeatResponse{
			HeartbeatIntervalMs: s.cfg.HeartbeatInterval.Milliseconds(),
			MustReregister:      true,
		}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}

	if t := s.monitor.Observe(req.NodeId, req.GrpcAddress, now); t != nil {
		s.onTransition(ctx, *t)
	}
	s.m.NodeHealth.WithLabelValues(req.NodeId).Set(float64(health.StateHealthy))
	s.m.StorageUsedBytes.WithLabelValues(req.NodeId).Set(float64(req.GetStats().GetUsedBytes()))
	s.m.StorageCapacityBytes.WithLabelValues(req.NodeId).Set(float64(req.GetStats().GetTotalBytes()))
	s.m.NodeActiveRequests.WithLabelValues(req.NodeId).Set(float64(req.GetStats().GetActiveRequests()))

	// Capacity changes on every heartbeat and placement reads it, so the
	// roster cache must not outlive the data it summarises.
	if prev != health.StateHealthy {
		s.invalidateNodeCache(ctx)
	}

	return &flexstorev1.HeartbeatResponse{
		HeartbeatIntervalMs: s.cfg.HeartbeatInterval.Milliseconds(),
	}, nil
}

// ListNodes returns the cluster roster.
func (s *Service) ListNodes(ctx context.Context, req *flexstorev1.ListNodesRequest) (*flexstorev1.ListNodesResponse, error) {
	nodes, err := s.store.ListNodes(ctx, req.HealthyOnly)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
	}
	out := make([]*flexstorev1.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, toProtoNode(n))
	}
	return &flexstorev1.ListNodesResponse{Nodes: out}, nil
}

// -------------------------------------------------------------- upload ----

// BeginUpload opens a new object version.
func (s *Service) BeginUpload(ctx context.Context, req *flexstorev1.BeginUploadRequest) (*flexstorev1.BeginUploadResponse, error) {
	if err := validateBucketKey(req.Bucket, req.Key); err != nil {
		return nil, err
	}
	chunkSize := req.ChunkSize
	if chunkSize <= 0 {
		chunkSize = s.cfg.ChunkSize
	}
	contentType := req.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	obj, err := s.store.BeginUpload(ctx, req.Bucket, req.Key, contentType, chunkSize)
	if err != nil {
		return nil, toStatus(err, "begin upload")
	}
	return &flexstorev1.BeginUploadResponse{
		ObjectId:          obj.ID.String(),
		ChunkSize:         obj.ChunkSize,
		ReplicationFactor: int32(s.cfg.ReplicationFactor),
		MinWriteReplicas:  int32(s.cfg.MinWriteReplicas),
	}, nil
}

// AllocateChunk runs placement and mints a chunk ID.
func (s *Service) AllocateChunk(ctx context.Context, req *flexstorev1.AllocateChunkRequest) (*flexstorev1.AllocateChunkResponse, error) {
	owner, err := parseOwner(req.ObjectId, req.PartId)
	if err != nil {
		return nil, err
	}
	if req.ChunkIndex < 0 {
		return nil, status.Error(codes.InvalidArgument, "chunk_index must be >= 0")
	}

	nodes, err := s.healthyNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load nodes: %v", err)
	}

	// Reserve the chunk ID *before* placement: deterministic strategies hash
	// it, and reusing the same ID on a retry keeps a retried chunk on the same
	// nodes instead of scattering half-written copies.
	chunkID, err := s.store.ReserveChunk(ctx, owner, req.ChunkIndex, req.SizeBytes)
	if err != nil {
		return nil, toStatus(err, "reserve chunk")
	}

	targets, err := s.placer.SelectNodes(nodes, placement.Request{
		Key:       chunkID.String(),
		Replicas:  s.cfg.ReplicationFactor,
		ChunkSize: req.SizeBytes,
	})
	if err != nil {
		switch {
		case errors.Is(err, placement.ErrInsufficientNodes):
			s.m.PlacementFailures.WithLabelValues("insufficient_nodes").Inc()
			return nil, status.Errorf(codes.ResourceExhausted,
				"cannot place %d replicas: %v", s.cfg.ReplicationFactor, err)
		default:
			s.m.PlacementFailures.WithLabelValues("invalid").Inc()
			return nil, status.Errorf(codes.InvalidArgument, "placement: %v", err)
		}
	}

	ids := make([]string, len(targets))
	for i, t := range targets {
		ids[i] = t.ID
	}
	if err := s.store.RecordChunkTargets(ctx, chunkID, ids); err != nil {
		return nil, toStatus(err, "record chunk targets")
	}

	protoTargets := make([]*flexstorev1.NodeInfo, len(targets))
	for i, t := range targets {
		protoTargets[i] = &flexstorev1.NodeInfo{
			NodeId:      t.ID,
			GrpcAddress: t.Address,
			Health:      flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY,
			Stats: &flexstorev1.NodeStats{
				TotalBytes:     t.TotalBytes,
				UsedBytes:      t.UsedBytes,
				AvailableBytes: t.AvailableBytes,
				ChunkCount:     t.ChunkCount,
				ActiveRequests: t.ActiveRequests,
			},
		}
	}

	return &flexstorev1.AllocateChunkResponse{
		ChunkId:          chunkID.String(),
		Targets:          protoTargets,
		MinWriteReplicas: int32(s.cfg.MinWriteReplicas),
	}, nil
}

// CommitChunk records replica outcomes and enforces the durability policy.
func (s *Service) CommitChunk(ctx context.Context, req *flexstorev1.CommitChunkRequest) (*flexstorev1.CommitChunkResponse, error) {
	chunkID, err := uuid.Parse(req.ChunkId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "chunk_id: %v", err)
	}
	if !checksum.Valid(req.ChecksumSha256) {
		return nil, status.Errorf(codes.InvalidArgument, "checksum_sha256 %q is not a hex SHA-256", req.ChecksumSha256)
	}

	available, err := s.store.CommitChunk(ctx, chunkID, req.ChecksumSha256, req.SizeBytes,
		req.SucceededNodeIds, req.FailedNodeIds, s.cfg.MinWriteReplicas)
	if err != nil {
		if errors.Is(err, metadata.ErrDurabilityNotMet) {
			s.m.ReplicaCount.WithLabelValues("rejected").Observe(float64(available))
			return nil, status.Errorf(codes.ResourceExhausted, "%v", err)
		}
		return nil, toStatus(err, "commit chunk")
	}
	s.m.ReplicaCount.WithLabelValues("committed").Observe(float64(available))

	return &flexstorev1.CommitChunkResponse{AvailableReplicas: int32(available)}, nil
}

// CompleteUpload makes the new version visible.
func (s *Service) CompleteUpload(ctx context.Context, req *flexstorev1.CompleteUploadRequest) (*flexstorev1.CompleteUploadResponse, error) {
	objectID, err := uuid.Parse(req.ObjectId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "object_id: %v", err)
	}

	obj, err := s.store.CompleteUpload(ctx, objectID, req.SizeBytes, req.ChunkCount, req.Etag)
	if err != nil {
		if errors.Is(err, metadata.ErrDurabilityNotMet) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, toStatus(err, "complete upload")
	}
	s.invalidateObject(ctx, obj.Bucket, obj.Key)

	s.log.Info("object committed",
		slog.String("bucket", obj.Bucket),
		slog.String("key", obj.Key),
		slog.Int64("version", obj.Version),
		slog.Int64("size_bytes", obj.SizeBytes),
		slog.Int("chunks", int(obj.ChunkCount)))

	return &flexstorev1.CompleteUploadResponse{
		ObjectId:    obj.ID.String(),
		Etag:        obj.ETag,
		CompletedAt: timestamppb.New(obj.CompletedAt),
	}, nil
}

// AbortUpload fails an in-flight upload and queues its chunks for reclamation.
func (s *Service) AbortUpload(ctx context.Context, req *flexstorev1.AbortUploadRequest) (*flexstorev1.AbortUploadResponse, error) {
	objectID, err := uuid.Parse(req.ObjectId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "object_id: %v", err)
	}
	if err := s.store.AbortUpload(ctx, objectID, req.Reason); err != nil {
		return nil, toStatus(err, "abort upload")
	}
	s.log.Info("upload aborted",
		slog.String("object_id", req.ObjectId), slog.String("reason", req.Reason))
	return &flexstorev1.AbortUploadResponse{}, nil
}

// ---------------------------------------------------------------- read ----

// GetObject returns metadata plus the full chunk layout, served from Redis
// when warm. Cache entries are invalidated on every write to the same key and
// additionally expire on a TTL.
func (s *Service) GetObject(ctx context.Context, req *flexstorev1.GetObjectRequest) (*flexstorev1.GetObjectResponse, error) {
	if err := validateBucketKey(req.Bucket, req.Key); err != nil {
		return nil, err
	}

	key := cache.ObjectKey(req.Bucket, req.Key)
	var cached cachedObject
	switch err := s.cache.Get(ctx, key, &cached); {
	case err == nil:
		s.m.MetadataCacheHits.WithLabelValues("object").Inc()
		return cached.toProto(), nil
	case errors.Is(err, cache.ErrMiss):
		s.m.MetadataCacheMisses.WithLabelValues("object").Inc()
	default:
		// A broken cache must degrade to a slower path, never to an error. It is
		// counted as an error rather than a miss: a cold cache and a broken one
		// look identical in a hit-rate graph, and only one of them needs an
		// operator.
		s.m.MetadataCacheErrors.WithLabelValues("object", "get").Inc()
		s.log.Warn("object cache read failed",
			slog.String("key", key), slog.String("error", err.Error()))
	}

	obj, chunks, err := s.store.GetObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return nil, toStatus(err, "get object")
	}

	resp := &flexstorev1.GetObjectResponse{
		Object: toProtoObject(obj),
		Chunks: make([]*flexstorev1.ChunkPlacement, 0, len(chunks)),
	}
	for _, c := range chunks {
		placementMsg := &flexstorev1.ChunkPlacement{
			ChunkId:        c.ID.String(),
			ChunkIndex:     c.Index,
			SizeBytes:      c.SizeBytes,
			ChecksumSha256: c.Checksum,
			Nodes:          make([]*flexstorev1.NodeInfo, 0, len(c.Replicas)),
		}
		for _, r := range c.Replicas {
			hs, _ := health.ParseState(r.NodeHealth)
			placementMsg.Nodes = append(placementMsg.Nodes, &flexstorev1.NodeInfo{
				NodeId:      r.NodeID,
				GrpcAddress: r.Address,
				Health:      toProtoHealth(hs),
			})
		}
		resp.Chunks = append(resp.Chunks, placementMsg)
	}

	if err := s.cache.Set(ctx, key, newCachedObject(resp), s.cfg.ObjectCacheTTL); err != nil {
		s.m.MetadataCacheErrors.WithLabelValues("object", "set").Inc()
		s.log.Warn("object cache write failed",
			slog.String("key", key), slog.String("error", err.Error()))
	}
	return resp, nil
}

// HeadObject returns metadata only.
func (s *Service) HeadObject(ctx context.Context, req *flexstorev1.HeadObjectRequest) (*flexstorev1.HeadObjectResponse, error) {
	if err := validateBucketKey(req.Bucket, req.Key); err != nil {
		return nil, err
	}
	obj, err := s.store.HeadObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return nil, toStatus(err, "head object")
	}
	return &flexstorev1.HeadObjectResponse{Object: toProtoObject(obj)}, nil
}

// DeleteObject retires the current version.
func (s *Service) DeleteObject(ctx context.Context, req *flexstorev1.DeleteObjectRequest) (*flexstorev1.DeleteObjectResponse, error) {
	if err := validateBucketKey(req.Bucket, req.Key); err != nil {
		return nil, err
	}
	id, existed, err := s.store.DeleteObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return nil, toStatus(err, "delete object")
	}
	s.invalidateObject(ctx, req.Bucket, req.Key)

	resp := &flexstorev1.DeleteObjectResponse{Existed: existed}
	if existed {
		resp.ObjectId = id.String()
		s.log.Info("object deleted",
			slog.String("bucket", req.Bucket), slog.String("key", req.Key),
			slog.String("object_id", id.String()))
	}
	return resp, nil
}

// ListObjects paginates a bucket.
func (s *Service) ListObjects(ctx context.Context, req *flexstorev1.ListObjectsRequest) (*flexstorev1.ListObjectsResponse, error) {
	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}
	objs, truncated, err := s.store.ListObjects(ctx, req.Bucket, req.Prefix, req.StartAfter, int(req.Limit))
	if err != nil {
		return nil, toStatus(err, "list objects")
	}
	out := make([]*flexstorev1.ObjectInfo, 0, len(objs))
	for _, o := range objs {
		out = append(out, toProtoObject(o))
	}
	return &flexstorev1.ListObjectsResponse{Objects: out, Truncated: truncated}, nil
}

// ------------------------------------------------------------- cluster ----

// GetClusterStatus reports nodes and durability counters.
func (s *Service) GetClusterStatus(ctx context.Context, _ *flexstorev1.GetClusterStatusRequest) (*flexstorev1.GetClusterStatusResponse, error) {
	nodes, err := s.store.ListNodes(ctx, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
	}
	stats, err := s.store.ClusterStats(ctx, s.cfg.ReplicationFactor)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cluster stats: %v", err)
	}

	protoNodes := make([]*flexstorev1.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		protoNodes = append(protoNodes, toProtoNode(n))
	}
	var totalObjects int64
	for _, n := range stats.ObjectsByState {
		totalObjects += n
	}
	return &flexstorev1.GetClusterStatusResponse{
		Nodes:                 protoNodes,
		TotalObjects:          totalObjects,
		TotalChunks:           stats.TotalChunks,
		UnderReplicatedChunks: stats.UnderReplicatedChunks,
		UnavailableChunks:     stats.UnavailableChunks,
		ReplicationFactor:     int32(s.cfg.ReplicationFactor),
		RepairJobsPending:     stats.RepairJobsByState[metadata.RepairPending],
		RepairJobsRunning:     stats.RepairJobsByState[metadata.RepairRunning],
	}, nil
}

// GetObjectLayout exposes the full replica map for an object. This is the
// introspection endpoint integration tests use to assert that replicas really
// landed on distinct nodes.
func (s *Service) GetObjectLayout(ctx context.Context, req *flexstorev1.GetObjectLayoutRequest) (*flexstorev1.GetObjectLayoutResponse, error) {
	if err := validateBucketKey(req.Bucket, req.Key); err != nil {
		return nil, err
	}
	obj, err := s.store.HeadObject(ctx, req.Bucket, req.Key)
	if err != nil {
		return nil, toStatus(err, "head object")
	}
	// availableOnly = false: the layout view deliberately shows every replica,
	// including UNAVAILABLE ones, because that is what makes a durability
	// problem diagnosable.
	chunks, err := s.store.ChunksForObject(ctx, obj.ID, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load chunks: %v", err)
	}

	out := make([]*flexstorev1.ChunkLayout, 0, len(chunks))
	for _, c := range chunks {
		layout := &flexstorev1.ChunkLayout{
			ChunkId:        c.ID.String(),
			ChunkIndex:     c.Index,
			SizeBytes:      c.SizeBytes,
			ChecksumSha256: c.Checksum,
		}
		for _, r := range c.Replicas {
			hs, _ := health.ParseState(r.NodeHealth)
			layout.Replicas = append(layout.Replicas, &flexstorev1.ReplicaLocation{
				NodeId:      r.NodeID,
				GrpcAddress: r.Address,
				State:       toProtoReplicaState(r.State),
				NodeHealth:  toProtoHealth(hs),
			})
		}
		out = append(out, layout)
	}
	return &flexstorev1.GetObjectLayoutResponse{Object: toProtoObject(obj), Chunks: out}, nil
}

// ------------------------------------------------------------ internal ----

// healthyNodes loads the placement roster, using a very short Redis TTL. The
// TTL is intentionally shorter than the heartbeat interval so placement never
// acts on capacity data that predates the last heartbeat round.
func (s *Service) healthyNodes(ctx context.Context) ([]placement.NodeInfo, error) {
	var cached []placement.NodeInfo
	if err := s.cache.Get(ctx, cache.NodesKey, &cached); err == nil {
		return cached, nil
	} else if !errors.Is(err, cache.ErrMiss) {
		s.log.Warn("node roster cache read failed", slog.String("error", err.Error()))
	}

	nodes, err := s.store.ListNodes(ctx, true)
	if err != nil {
		return nil, err
	}
	out := make([]placement.NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		hs, _ := health.ParseState(n.Health)
		out = append(out, placement.NodeInfo{
			ID:             n.ID,
			Address:        n.Address,
			Health:         toPlacementHealth(hs),
			TotalBytes:     n.TotalBytes,
			UsedBytes:      n.UsedBytes,
			AvailableBytes: n.AvailableBytes,
			ActiveRequests: n.ActiveRequests,
			ChunkCount:     n.ChunkCount,
		})
	}
	if err := s.cache.Set(ctx, cache.NodesKey, out, s.cfg.NodeCacheTTL); err != nil {
		s.log.Warn("node roster cache write failed", slog.String("error", err.Error()))
	}
	return out, nil
}

func (s *Service) invalidateNodeCache(ctx context.Context) {
	if err := s.cache.Delete(ctx, cache.NodesKey); err != nil {
		s.log.Warn("failed to invalidate node roster cache", slog.String("error", err.Error()))
	}
}

func (s *Service) invalidateObject(ctx context.Context, bucket, key string) {
	if err := s.cache.Delete(ctx, cache.ObjectKey(bucket, key)); err != nil {
		s.log.Warn("failed to invalidate object cache",
			slog.String("bucket", bucket), slog.String("key", key),
			slog.String("error", err.Error()))
	}
}

// onTransition persists and exports a health state change.
func (s *Service) onTransition(ctx context.Context, t health.Transition) {
	s.log.Info("node health changed",
		slog.String("node_id", t.NodeID),
		slog.String("from", t.From.String()),
		slog.String("to", t.To.String()),
		slog.Duration("heartbeat_age", t.Age))

	// SetNodeHealth demotes the node's replicas in the same transaction when
	// the new state is DEAD, so durability accounting can never lag the health
	// change.
	demoted, err := s.store.SetNodeHealth(ctx, t.NodeID, t.To.String())
	if err != nil {
		s.log.Error("failed to persist health transition",
			slog.String("node_id", t.NodeID), slog.String("error", err.Error()))
	}
	s.m.NodeHealth.WithLabelValues(t.NodeID).Set(float64(t.To))
	s.invalidateNodeCache(ctx)

	switch t.To {
	case health.StateDead:
		// Writing off the replicas (done inside SetNodeHealth above) is what
		// drives everything else: the trigger-maintained available_replicas
		// counter drops, the chunks fall below the replication factor, and the
		// repair scanner picks them up on its next pass. Nothing here talks to
		// the repair manager directly.
		if demoted > 0 {
			s.log.Warn("node declared dead; its replicas no longer count towards durability",
				slog.String("node_id", t.NodeID), slog.Int64("replicas", demoted))
		}
	case health.StateHealthy:
		if t.From == health.StateDead || t.From == health.StateSuspect {
			s.onNodeRejoined(ctx, t.NodeID, t.From)
		}
	}
}

// onNodeRejoined demotes a returning node's replicas to STALE and queues a
// reconciliation to verify them.
func (s *Service) onNodeRejoined(ctx context.Context, nodeID string, from health.State) {
	n, err := s.store.MarkReplicasStaleOnNode(ctx, nodeID)
	if err != nil {
		s.log.Error("failed to mark returning node's replicas stale",
			slog.String("node_id", nodeID), slog.String("error", err.Error()))
		return
	}

	queued, err := s.store.EnqueueReconcile(ctx, nodeID)
	if err != nil {
		s.log.Error("failed to schedule reconciliation",
			slog.String("node_id", nodeID), slog.String("error", err.Error()))
		return
	}

	if n > 0 || queued {
		s.log.Info("node rejoined; replicas held pending verification",
			slog.String("node_id", nodeID),
			slog.String("previous_state", healthStateName(from)),
			slog.Int64("stale_replicas", n),
			slog.Bool("reconciliation_queued", queued))
	}

	// A recovered node is a placement target again, so repairs that previously
	// failed for lack of anywhere to put a copy deserve another attempt.
	if rm := s.repairManager(); rm != nil {
		rm.OnNodeRecovered(ctx, nodeID)
	}
}

func parseOwner(objectID, partID string) (metadata.ChunkOwner, error) {
	switch {
	case objectID != "" && partID != "":
		return metadata.ChunkOwner{}, status.Error(codes.InvalidArgument,
			"exactly one of object_id or part_id must be set")
	case objectID != "":
		id, err := uuid.Parse(objectID)
		if err != nil {
			return metadata.ChunkOwner{}, status.Errorf(codes.InvalidArgument, "object_id: %v", err)
		}
		return metadata.ChunkOwner{ObjectID: &id}, nil
	case partID != "":
		id, err := uuid.Parse(partID)
		if err != nil {
			return metadata.ChunkOwner{}, status.Errorf(codes.InvalidArgument, "part_id: %v", err)
		}
		return metadata.ChunkOwner{PartID: &id}, nil
	default:
		return metadata.ChunkOwner{}, status.Error(codes.InvalidArgument,
			"one of object_id or part_id is required")
	}
}

// toStatus maps metadata sentinels onto gRPC codes so the gateway can turn
// them into meaningful HTTP responses without string matching.
func toStatus(err error, op string) error {
	switch {
	case errors.Is(err, metadata.ErrNotFound):
		return status.Errorf(codes.NotFound, "%s: %v", op, err)
	case errors.Is(err, metadata.ErrConflict):
		return status.Errorf(codes.FailedPrecondition, "%s: %v", op, err)
	case errors.Is(err, metadata.ErrDurabilityNotMet):
		return status.Errorf(codes.ResourceExhausted, "%s: %v", op, err)
	case errors.Is(err, context.Canceled):
		return status.Errorf(codes.Canceled, "%s: %v", op, err)
	case errors.Is(err, context.DeadlineExceeded):
		return status.Errorf(codes.DeadlineExceeded, "%s: %v", op, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", op, err)
	}
}

func validateBucketKey(bucket, key string) error {
	if err := ValidateBucket(bucket); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := ValidateKey(key); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return nil
}

func toStoreStats(s *flexstorev1.NodeStats) metadata.NodeStats {
	if s == nil {
		return metadata.NodeStats{}
	}
	return metadata.NodeStats{
		TotalBytes:     s.TotalBytes,
		UsedBytes:      s.UsedBytes,
		AvailableBytes: s.AvailableBytes,
		ChunkCount:     s.ChunkCount,
		ActiveRequests: s.ActiveRequests,
	}
}

func toProtoNode(n metadata.Node) *flexstorev1.NodeInfo {
	hs, _ := health.ParseState(n.Health)
	info := &flexstorev1.NodeInfo{
		NodeId:      n.ID,
		GrpcAddress: n.Address,
		Health:      toProtoHealth(hs),
		Stats: &flexstorev1.NodeStats{
			TotalBytes:     n.TotalBytes,
			UsedBytes:      n.UsedBytes,
			AvailableBytes: n.AvailableBytes,
			ChunkCount:     n.ChunkCount,
			ActiveRequests: n.ActiveRequests,
		},
		RegisteredAt: timestamppb.New(n.RegisteredAt),
	}
	if !n.LastHeartbeatAt.IsZero() {
		info.LastHeartbeatAt = timestamppb.New(n.LastHeartbeatAt)
	}
	return info
}

func toProtoObject(o metadata.Object) *flexstorev1.ObjectInfo {
	info := &flexstorev1.ObjectInfo{
		ObjectId:    o.ID.String(),
		Bucket:      o.Bucket,
		Key:         o.Key,
		Version:     o.Version,
		State:       toProtoObjectState(o.State),
		SizeBytes:   o.SizeBytes,
		ChunkSize:   o.ChunkSize,
		ChunkCount:  o.ChunkCount,
		Etag:        o.ETag,
		ContentType: o.ContentType,
		CreatedAt:   timestamppb.New(o.CreatedAt),
	}
	if !o.CompletedAt.IsZero() {
		info.CompletedAt = timestamppb.New(o.CompletedAt)
	}
	return info
}

func toProtoHealth(s health.State) flexstorev1.NodeHealth {
	switch s {
	case health.StateHealthy:
		return flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY
	case health.StateSuspect:
		return flexstorev1.NodeHealth_NODE_HEALTH_SUSPECT
	case health.StateDead:
		return flexstorev1.NodeHealth_NODE_HEALTH_DEAD
	default:
		return flexstorev1.NodeHealth_NODE_HEALTH_UNSPECIFIED
	}
}

func toProtoObjectState(s metadata.ObjectState) flexstorev1.ObjectState {
	switch s {
	case metadata.ObjectUploading:
		return flexstorev1.ObjectState_OBJECT_STATE_UPLOADING
	case metadata.ObjectComplete:
		return flexstorev1.ObjectState_OBJECT_STATE_COMPLETE
	case metadata.ObjectDeleting:
		return flexstorev1.ObjectState_OBJECT_STATE_DELETING
	case metadata.ObjectFailed:
		return flexstorev1.ObjectState_OBJECT_STATE_FAILED
	default:
		return flexstorev1.ObjectState_OBJECT_STATE_UNSPECIFIED
	}
}

func toProtoReplicaState(s metadata.ReplicaState) flexstorev1.ReplicaState {
	switch s {
	case metadata.ReplicaWriting:
		return flexstorev1.ReplicaState_REPLICA_STATE_WRITING
	case metadata.ReplicaAvailable:
		return flexstorev1.ReplicaState_REPLICA_STATE_AVAILABLE
	case metadata.ReplicaUnavailable:
		return flexstorev1.ReplicaState_REPLICA_STATE_UNAVAILABLE
	case metadata.ReplicaDeleting:
		return flexstorev1.ReplicaState_REPLICA_STATE_DELETING
	default:
		return flexstorev1.ReplicaState_REPLICA_STATE_UNSPECIFIED
	}
}

// toPlacementHealth converts explicitly rather than relying on the two enums
// happening to share numeric values -- they are deliberately independent types
// so the placement package stays free of coordinator dependencies.
func toPlacementHealth(s health.State) placement.Health {
	switch s {
	case health.StateHealthy:
		return placement.HealthHealthy
	case health.StateSuspect:
		return placement.HealthSuspect
	case health.StateDead:
		return placement.HealthDead
	default:
		return placement.HealthUnknown
	}
}

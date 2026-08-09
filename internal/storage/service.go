package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// streamFrameSize is the payload size of one gRPC data frame. 256 KiB keeps
// per-message overhead negligible while staying well under gRPC's default
// 4 MiB message limit, so an 8 MiB chunk needs no limit tuning.
const streamFrameSize = 256 << 10

// Service implements the StorageNodeService gRPC API over a ChunkStore.
type Service struct {
	flexstorev1.UnimplementedStorageNodeServiceServer

	nodeID string
	store  *ChunkStore
	log    *slog.Logger
	m      *observability.Metrics
}

// NewService builds the storage node's gRPC service.
func NewService(nodeID string, store *ChunkStore, log *slog.Logger, m *observability.Metrics) *Service {
	return &Service{nodeID: nodeID, store: store, log: log, m: m}
}

// StoreChunk implements the write path. The first stream message must be a
// header; everything after it is payload.
func (s *Service) StoreChunk(stream flexstorev1.StorageNodeService_StoreChunkServer) error {
	s.store.BeginRequest()
	defer s.store.EndRequest()

	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "empty StoreChunk stream")
		}
		return status.Errorf(codes.Internal, "receive header: %v", err)
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first StoreChunk message must be a header")
	}
	if err := ValidateChunkID(header.ChunkId); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if header.ChecksumSha256 != "" && !checksum.Valid(header.ChecksumSha256) {
		return status.Errorf(codes.InvalidArgument, "malformed checksum %q", header.ChecksumSha256)
	}

	start := time.Now()
	// streamReader adapts the gRPC stream to io.Reader so ChunkStore.Write does
	// not need to know it is being fed by gRPC.
	res, err := s.store.Write(header.ChunkId, &streamReader{stream: stream},
		header.SizeBytes, header.ChecksumSha256)
	if err != nil {
		s.m.ChunkWriteDuration.WithLabelValues("error").Observe(time.Since(start).Seconds())
		s.m.ChunksStoredTotal.WithLabelValues(s.nodeID, "error").Inc()

		log := observability.LoggerFrom(stream.Context(), s.log)
		if errors.Is(err, checksum.ErrMismatch) {
			s.m.ChecksumFailures.WithLabelValues("write").Inc()
			s.m.ChunkWriteFailures.WithLabelValues("checksum").Inc()
			log.Error("rejecting corrupt chunk write",
				slog.String("chunk_id", header.ChunkId), slog.String("error", err.Error()))
			return status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		s.m.ChunkWriteFailures.WithLabelValues("io").Inc()
		log.Error("chunk write failed",
			slog.String("chunk_id", header.ChunkId), slog.String("error", err.Error()))
		return status.Errorf(codes.Internal, "store chunk: %v", err)
	}

	s.m.ChunkWriteDuration.WithLabelValues("ok").Observe(time.Since(start).Seconds())
	result := "stored"
	if res.AlreadyExisted {
		result = "deduplicated"
	}
	s.m.ChunksStoredTotal.WithLabelValues(s.nodeID, result).Inc()

	return stream.SendAndClose(&flexstorev1.StoreChunkResponse{
		ChunkId:        header.ChunkId,
		BytesWritten:   res.BytesWritten,
		ChecksumSha256: res.Checksum,
		AlreadyExisted: res.AlreadyExisted,
	})
}

// ReadChunk streams a chunk while re-hashing it. If the digest does not match
// what the caller expected, the stream ends with FailedPrecondition -- the
// caller has received bytes but is explicitly told not to trust them.
func (s *Service) ReadChunk(req *flexstorev1.ReadChunkRequest, stream flexstorev1.StorageNodeService_ReadChunkServer) error {
	s.store.BeginRequest()
	defer s.store.EndRequest()

	if err := ValidateChunkID(req.ChunkId); err != nil {
		return status.Errorf(codes.InvalidArgument, "%v", err)
	}

	start := time.Now()
	f, size, err := s.store.Open(req.ChunkId)
	if err != nil {
		s.m.ChunkReadDuration.WithLabelValues("error").Observe(time.Since(start).Seconds())
		if errors.Is(err, ErrNotFound) {
			return status.Errorf(codes.NotFound, "chunk %s not on node %s", req.ChunkId, s.nodeID)
		}
		return status.Errorf(codes.Internal, "open chunk: %v", err)
	}
	defer func() { _ = f.Close() }()

	h := checksum.New()
	buf := make([]byte, streamFrameSize)
	var sent int64
	for {
		// Honour cancellation between frames so an abandoned download stops
		// reading from disk promptly instead of streaming into the void.
		if err := stream.Context().Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		n, rerr := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
			if err := stream.Send(&flexstorev1.ReadChunkResponse{Data: buf[:n]}); err != nil {
				s.m.ChunkReadDuration.WithLabelValues("error").Observe(time.Since(start).Seconds())
				return status.Errorf(codes.Unavailable, "send chunk data: %v", err)
			}
			sent += int64(n)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			s.m.ChunkReadDuration.WithLabelValues("error").Observe(time.Since(start).Seconds())
			return status.Errorf(codes.Internal, "read chunk: %v", rerr)
		}
	}

	actual := checksum.Hex(h)
	if req.ExpectedChecksumSha256 != "" {
		if err := checksum.Verify(req.ChunkId, req.ExpectedChecksumSha256, actual); err != nil {
			s.m.ChecksumFailures.WithLabelValues("read").Inc()
			s.m.ChunkReadDuration.WithLabelValues("corrupt").Observe(time.Since(start).Seconds())
			observability.LoggerFrom(stream.Context(), s.log).Error("chunk failed verification on read",
				slog.String("chunk_id", req.ChunkId),
				slog.String("node_id", s.nodeID),
				slog.String("error", err.Error()))
			return status.Errorf(codes.FailedPrecondition, "%v", err)
		}
	}
	if sent != size {
		return status.Errorf(codes.Internal, "chunk %s: sent %d of %d bytes", req.ChunkId, sent, size)
	}

	s.m.ChunkReadDuration.WithLabelValues("ok").Observe(time.Since(start).Seconds())
	return nil
}

// DeleteChunk removes a chunk. Idempotent by design.
func (s *Service) DeleteChunk(ctx context.Context, req *flexstorev1.DeleteChunkRequest) (*flexstorev1.DeleteChunkResponse, error) {
	if err := ValidateChunkID(req.ChunkId); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	existed, err := s.store.Delete(req.ChunkId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete chunk: %v", err)
	}
	return &flexstorev1.DeleteChunkResponse{Existed: existed}, nil
}

// ReplicateChunk pulls a chunk from a peer node into this node's store. Used
// by the (future) repair loop; implemented now because the pull direction is
// the part that constrains the API design.
func (s *Service) ReplicateChunk(ctx context.Context, req *flexstorev1.ReplicateChunkRequest) (*flexstorev1.ReplicateChunkResponse, error) {
	if err := ValidateChunkID(req.ChunkId); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if req.SourceAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "source_address is required")
	}

	s.store.BeginRequest()
	defer s.store.EndRequest()

	conn, err := grpc.NewClient(req.SourceAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial source %s: %v", req.SourceAddress, err)
	}
	defer func() { _ = conn.Close() }()

	src, err := flexstorev1.NewStorageNodeServiceClient(conn).ReadChunk(ctx, &flexstorev1.ReadChunkRequest{
		ChunkId:                req.ChunkId,
		ExpectedChecksumSha256: req.ChecksumSha256,
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read from source: %v", err)
	}

	res, err := s.store.Write(req.ChunkId, &readChunkReader{stream: src}, req.SizeBytes, req.ChecksumSha256)
	if err != nil {
		if errors.Is(err, checksum.ErrMismatch) {
			s.m.ChecksumFailures.WithLabelValues("replicate").Inc()
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "replicate chunk: %v", err)
	}
	s.m.ChunksStoredTotal.WithLabelValues(s.nodeID, "replicated").Inc()

	return &flexstorev1.ReplicateChunkResponse{
		BytesWritten:   res.BytesWritten,
		AlreadyExisted: res.AlreadyExisted,
	}, nil
}

// CheckChunk reports presence and optionally re-verifies the payload.
func (s *Service) CheckChunk(ctx context.Context, req *flexstorev1.CheckChunkRequest) (*flexstorev1.CheckChunkResponse, error) {
	if err := ValidateChunkID(req.ChunkId); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	res, err := s.store.Check(req.ChunkId, req.VerifyChecksum)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "check chunk: %v", err)
	}
	return &flexstorev1.CheckChunkResponse{
		Exists:         res.Exists,
		SizeBytes:      res.SizeBytes,
		ChecksumSha256: res.Checksum,
		ChecksumValid:  res.Valid,
	}, nil
}

// GetNodeStats reports capacity and load.
func (s *Service) GetNodeStats(ctx context.Context, _ *flexstorev1.GetNodeStatsRequest) (*flexstorev1.GetNodeStatsResponse, error) {
	stats, err := s.Stats()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "node stats: %v", err)
	}
	return &flexstorev1.GetNodeStatsResponse{NodeId: s.nodeID, Stats: stats}, nil
}

// ListChunks streams this node's chunk inventory.
//
// The reconciler uses it to compare what a returning node actually holds
// against what the coordinator believes it holds. Only well-formed chunk IDs
// are reported, so a stray file in the data directory is never presented to the
// coordinator as something it might decide to delete.
func (s *Service) ListChunks(req *flexstorev1.ListChunksRequest, stream flexstorev1.StorageNodeService_ListChunksServer) error {
	s.store.BeginRequest()
	defer s.store.EndRequest()

	batchSize := int(req.BatchSize)
	if batchSize <= 0 || batchSize > 10000 {
		batchSize = 1000
	}

	err := s.store.WalkInventory(batchSize, func(entries []InventoryEntry) error {
		// Honour cancellation between batches so an abandoned reconciliation
		// stops walking the disk promptly.
		if err := stream.Context().Err(); err != nil {
			return err
		}
		msg := &flexstorev1.ListChunksResponse{
			Chunks: make([]*flexstorev1.ChunkInventoryEntry, 0, len(entries)),
		}
		for _, e := range entries {
			msg.Chunks = append(msg.Chunks, &flexstorev1.ChunkInventoryEntry{
				ChunkId:          e.ChunkID,
				SizeBytes:        e.Size,
				ModifiedUnixNano: e.Modified.UnixNano(),
			})
		}
		return stream.Send(msg)
	})
	if err != nil {
		if ctxErr := stream.Context().Err(); ctxErr != nil {
			return status.FromContextError(ctxErr).Err()
		}
		return status.Errorf(codes.Internal, "list chunks: %v", err)
	}
	return nil
}

// Stats builds the protobuf stats message and updates the capacity gauges.
// Shared by the gRPC handler and the heartbeat loop.
func (s *Service) Stats() (*flexstorev1.NodeStats, error) {
	total, used, available, chunks, active, err := s.store.Stats()
	if err != nil {
		return nil, fmt.Errorf("read node stats: %w", err)
	}
	s.m.StorageUsedBytes.WithLabelValues(s.nodeID).Set(float64(used))
	s.m.StorageCapacityBytes.WithLabelValues(s.nodeID).Set(float64(total))
	s.m.NodeActiveRequests.WithLabelValues(s.nodeID).Set(float64(active))
	return &flexstorev1.NodeStats{
		TotalBytes:     total,
		UsedBytes:      used,
		AvailableBytes: available,
		ChunkCount:     chunks,
		ActiveRequests: active,
	}, nil
}

// streamReader adapts a StoreChunk server stream to io.Reader.
type streamReader struct {
	stream flexstorev1.StorageNodeService_StoreChunkServer
	buf    []byte
	done   bool
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		msg, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.done = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		if h := msg.GetHeader(); h != nil {
			return 0, fmt.Errorf("unexpected second header in StoreChunk stream")
		}
		r.buf = msg.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// readChunkReader adapts a ReadChunk client stream to io.Reader.
type readChunkReader struct {
	stream flexstorev1.StorageNodeService_ReadChunkClient
	buf    []byte
	done   bool
}

func (r *readChunkReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		msg, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			r.done = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		r.buf = msg.GetData()
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

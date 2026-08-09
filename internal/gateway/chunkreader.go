package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// ErrNoReplica means every known replica of a chunk was unreachable or corrupt.
var ErrNoReplica = errors.New("no readable replica")

// CorruptionReporter tells the control plane that a replica served bad bytes.
//
// An interface rather than a direct coordinator client so the reader stays
// testable without a control plane, and so a nil reporter degrades to
// "fail over but do not report" instead of panicking.
type CorruptionReporter interface {
	ReportCorruptReplica(ctx context.Context, chunkID, nodeID, detail string) error
}

// ChunkReader fetches chunks, failing over between replicas.
type ChunkReader struct {
	pool     *nodeclient.Pool
	timeout  time.Duration
	log      *slog.Logger
	m        *observability.Metrics
	reporter CorruptionReporter
}

// NewChunkReader builds the read-path fetcher.
func NewChunkReader(pool *nodeclient.Pool, timeout time.Duration, log *slog.Logger, m *observability.Metrics) *ChunkReader {
	return &ChunkReader{pool: pool, timeout: timeout, log: log, m: m}
}

// SetCorruptionReporter wires in the control-plane reporter. Called once during
// startup, before the HTTP listener accepts traffic.
func (r *ChunkReader) SetCorruptionReporter(rep CorruptionReporter) { r.reporter = rep }

// reportCorruption notifies the coordinator that a replica is bad.
//
// Synchronous, with its own short deadline. Corruption is rare, the RPC is
// tiny, and doing it inline keeps the reader free of background goroutines
// whose lifetime would have to be managed across shutdown. The cost is a small
// latency addition on an already-degraded read, which is the right trade: the
// alternative is a corrupt replica nobody ever replaces.
func (r *ChunkReader) reportCorruption(ctx context.Context, chunkID, nodeID, detail string) {
	if r.reporter == nil {
		return
	}
	// Detached from the request context: the client may well have hung up by
	// now, and the report is more important than the request that found it.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := r.reporter.ReportCorruptReplica(reportCtx, chunkID, nodeID, detail); err != nil {
		r.log.Error("failed to report corrupt replica to the coordinator; "+
			"the bad copy will not be replaced until the next scrub",
			slog.String("chunk_id", chunkID),
			slog.String("node_id", nodeID),
			slog.String("error", err.Error()))
		return
	}
	r.log.Info("reported corrupt replica for repair",
		slog.String("chunk_id", chunkID), slog.String("node_id", nodeID))
}

// Fetch reads one chunk into buf and returns the populated slice.
//
// The whole chunk is buffered and verified *before* any of it is handed to the
// caller. That is the difference between "we detect corruption" and "we never
// serve corruption": once bytes are written to an HTTP response they cannot be
// recalled. Buffering one chunk (8 MiB by default) is the price, and it is a
// fixed cost per request regardless of object size.
func (r *ChunkReader) Fetch(ctx context.Context, chunk *flexstorev1.ChunkPlacement, buf []byte) ([]byte, error) {
	if len(chunk.Nodes) == 0 {
		return nil, fmt.Errorf("%w: chunk %s has no replica locations", ErrNoReplica, chunk.ChunkId)
	}
	if int64(cap(buf)) < chunk.SizeBytes {
		buf = make([]byte, 0, chunk.SizeBytes)
	}

	var attemptErrs []error
	for _, node := range chunk.Nodes {
		start := time.Now()
		data, err := r.fetchFrom(ctx, node, chunk, buf[:0])
		if err == nil {
			r.m.ChunkReadDuration.WithLabelValues("ok").Observe(time.Since(start).Seconds())
			return data, nil
		}

		// A cancelled request is the client's doing, not a replica problem;
		// trying the next node would waste work on a response nobody wants.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		outcome := "error"
		if errors.Is(err, checksum.ErrMismatch) || status.Code(err) == codes.FailedPrecondition {
			outcome = "corrupt"
			r.m.ChecksumFailures.WithLabelValues("read").Inc()
			r.log.Error("replica failed checksum verification; failing over",
				slog.String("chunk_id", chunk.ChunkId),
				slog.String("node_id", node.NodeId),
				slog.String("error", err.Error()))
			// Detection is only half the job: without telling the control
			// plane, the bad copy stays listed as a valid replica and every
			// future read pays the same failover cost.
			r.reportCorruption(ctx, chunk.ChunkId, node.NodeId, err.Error())
		} else {
			r.log.Warn("replica read failed; failing over",
				slog.String("chunk_id", chunk.ChunkId),
				slog.String("node_id", node.NodeId),
				slog.String("error", err.Error()))
		}
		r.m.ChunkReadDuration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
		attemptErrs = append(attemptErrs, fmt.Errorf("node %s: %w", node.NodeId, err))
	}

	return nil, fmt.Errorf("%w for chunk %s after %d attempts: %w",
		ErrNoReplica, chunk.ChunkId, len(chunk.Nodes), errors.Join(attemptErrs...))
}

func (r *ChunkReader) fetchFrom(ctx context.Context, node *flexstorev1.NodeInfo, chunk *flexstorev1.ChunkPlacement, buf []byte) ([]byte, error) {
	client, err := r.pool.Client(node.GrpcAddress)
	if err != nil {
		return nil, err
	}

	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	// The stream must be torn down before we return, or the gRPC transport
	// keeps reading from the node into a buffer nobody drains.
	defer cancel()

	stream, err := client.ReadChunk(callCtx, &flexstorev1.ReadChunkRequest{
		ChunkId:                chunk.ChunkId,
		ExpectedChecksumSha256: chunk.ChecksumSha256,
	})
	if err != nil {
		return nil, fmt.Errorf("open ReadChunk: %w", err)
	}

	h := checksum.New()
	for {
		msg, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
		data := msg.GetData()
		if int64(len(buf))+int64(len(data)) > chunk.SizeBytes {
			// A node sending more than the recorded size is either buggy or
			// serving different data. Classified as corruption, not as a
			// transport error: the length is as much a property of the stored
			// chunk as its digest, so a replica that gets it wrong must be
			// reported and replaced, not merely skipped. Wrapping
			// checksum.ErrMismatch keeps one classification path for
			// "this replica cannot be trusted".
			return nil, fmt.Errorf("%w: node returned more than the recorded %d bytes",
				checksum.ErrMismatch, chunk.SizeBytes)
		}
		buf = append(buf, data...)
		h.Write(data)
	}

	if int64(len(buf)) != chunk.SizeBytes {
		return nil, fmt.Errorf("%w: read %d bytes, metadata says %d",
			checksum.ErrMismatch, len(buf), chunk.SizeBytes)
	}
	// Independent verification at the gateway: the node checks its disk, we
	// check the wire. Only both passing means the client gets the bytes.
	if err := checksum.Verify(chunk.ChunkId, chunk.ChecksumSha256, checksum.Hex(h)); err != nil {
		return nil, err
	}
	return buf, nil
}

package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// frameSize is the gRPC payload size used when streaming a chunk to a node.
// Must stay well under the default 4 MiB max message size.
const frameSize = 256 << 10

// ErrDurability means too few replicas acknowledged the write.
var ErrDurability = errors.New("durability policy not satisfied")

// ChunkWriter fans one chunk out to its selected replicas.
type ChunkWriter struct {
	pool    *nodeclient.Pool
	timeout time.Duration
	log     *slog.Logger
	m       *observability.Metrics
}

// NewChunkWriter builds the replica fan-out writer.
func NewChunkWriter(pool *nodeclient.Pool, timeout time.Duration, log *slog.Logger, m *observability.Metrics) *ChunkWriter {
	return &ChunkWriter{pool: pool, timeout: timeout, log: log, m: m}
}

// WriteResult reports which nodes accepted a chunk.
type WriteResult struct {
	Succeeded []string
	Failed    []string
}

// Write sends data to every target concurrently and returns once all have
// finished.
//
// It deliberately waits for *all* replicas rather than returning as soon as
// the quorum is met: the extra replicas are what the replication factor
// promises, and abandoning them mid-write would leave partial copies that the
// (not yet built) repair loop would have to clean up. The cost is that a slow
// node adds latency; the per-node deadline bounds that.
func (w *ChunkWriter) Write(
	ctx context.Context,
	chunkID string,
	data []byte,
	sha256Hex string,
	targets []*flexstorev1.NodeInfo,
	minReplicas int,
) (WriteResult, error) {
	if len(targets) == 0 {
		return WriteResult{}, fmt.Errorf("%w: no placement targets for chunk %s", ErrDurability, chunkID)
	}

	var (
		mu   sync.Mutex
		res  WriteResult
		wg   sync.WaitGroup
		errs []error
	)

	for _, t := range targets {
		wg.Add(1)
		go func(node *flexstorev1.NodeInfo) {
			defer wg.Done()

			// Each replica gets its own deadline derived from the request
			// context, so a hung node cannot block the others past the bound.
			nodeCtx, cancel := context.WithTimeout(ctx, w.timeout)
			defer cancel()

			err := w.writeOne(nodeCtx, node, chunkID, data, sha256Hex)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				res.Failed = append(res.Failed, node.NodeId)
				errs = append(errs, fmt.Errorf("node %s: %w", node.NodeId, err))
				w.m.ReplicaWriteResults.WithLabelValues(node.NodeId, "error").Inc()
				return
			}
			res.Succeeded = append(res.Succeeded, node.NodeId)
			w.m.ReplicaWriteResults.WithLabelValues(node.NodeId, "ok").Inc()
		}(t)
	}
	wg.Wait()

	if len(res.Succeeded) < minReplicas {
		w.m.ChunkWriteFailures.WithLabelValues("durability").Inc()
		return res, fmt.Errorf("%w: chunk %s stored on %d of %d nodes, %d required: %w",
			ErrDurability, chunkID, len(res.Succeeded), len(targets), minReplicas, errors.Join(errs...))
	}
	if len(errs) > 0 {
		// Quorum met but some replicas failed: log it, because this is exactly
		// the condition that produces under-replicated chunks.
		w.log.Warn("chunk written with fewer replicas than requested",
			slog.String("chunk_id", chunkID),
			slog.Int("succeeded", len(res.Succeeded)),
			slog.Int("targets", len(targets)),
			slog.String("errors", errors.Join(errs...).Error()))
	}
	return res, nil
}

func (w *ChunkWriter) writeOne(ctx context.Context, node *flexstorev1.NodeInfo, chunkID string, data []byte, sha256Hex string) error {
	client, err := w.pool.Client(node.GrpcAddress)
	if err != nil {
		return err
	}
	stream, err := client.StoreChunk(ctx)
	if err != nil {
		return fmt.Errorf("open StoreChunk: %w", err)
	}

	if err := stream.Send(&flexstorev1.StoreChunkRequest{
		Payload: &flexstorev1.StoreChunkRequest_Header{
			Header: &flexstorev1.ChunkHeader{
				ChunkId:        chunkID,
				SizeBytes:      int64(len(data)),
				ChecksumSha256: sha256Hex,
			},
		},
	}); err != nil {
		return fmt.Errorf("send header: %w", sendErr(stream, err))
	}

	for off := 0; off < len(data); off += frameSize {
		end := off + frameSize
		if end > len(data) {
			end = len(data)
		}
		if err := stream.Send(&flexstorev1.StoreChunkRequest{
			Payload: &flexstorev1.StoreChunkRequest_Data{Data: data[off:end]},
		}); err != nil {
			return fmt.Errorf("send frame at %d: %w", off, sendErr(stream, err))
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("close StoreChunk: %w", err)
	}
	if resp.BytesWritten != int64(len(data)) {
		return fmt.Errorf("node acknowledged %d of %d bytes", resp.BytesWritten, len(data))
	}
	if resp.ChecksumSha256 != sha256Hex {
		// Belt and braces: the node already verifies this, but trusting a
		// single verifier defeats the purpose of end-to-end checksums.
		w.m.ChecksumFailures.WithLabelValues("write-ack").Inc()
		return fmt.Errorf("node reported checksum %s, expected %s", resp.ChecksumSha256, sha256Hex)
	}
	return nil
}

// sendErr recovers the real error behind a stream Send failure. gRPC reports
// io.EOF from Send when the server has already terminated the stream; the
// actual cause only surfaces from CloseAndRecv.
func sendErr(stream flexstorev1.StorageNodeService_StoreChunkClient, err error) error {
	if status.Code(err) != codes.Unknown && status.Code(err) != codes.OK {
		return err
	}
	if _, rerr := stream.CloseAndRecv(); rerr != nil {
		return rerr
	}
	return err
}

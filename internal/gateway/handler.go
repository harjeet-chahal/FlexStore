package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/apierr"
	"github.com/harjeetschahal/flexstore/internal/bufpool"
	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/chunking"
	"github.com/harjeetschahal/flexstore/internal/config"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// Handler holds the gateway's dependencies. One instance serves all routes.
type Handler struct {
	cfg    config.Gateway
	coord  *CoordinatorClient
	writer *ChunkWriter
	reader *ChunkReader
	log    *slog.Logger
	m      *observability.Metrics
	// bufs recycles chunk-sized working buffers across requests. Both data
	// paths already reused one buffer per request; this removes the remaining
	// per-request allocation, which at 32 concurrent 8 MiB transfers was
	// 256 MiB of garbage per request cycle. See internal/bufpool.
	bufs *bufpool.Pool
}

// NewHandler builds the HTTP handler set.
func NewHandler(
	cfg config.Gateway,
	coord *CoordinatorClient,
	writer *ChunkWriter,
	reader *ChunkReader,
	log *slog.Logger,
	m *observability.Metrics,
) *Handler {
	return &Handler{
		cfg: cfg, coord: coord, writer: writer, reader: reader, log: log, m: m,
		bufs: bufpool.New(int(cfg.ChunkSize)),
	}
}

// chunkOwner identifies what an uploaded stream's chunks belong to: either a
// whole object (single-shot PUT) or one part of a multipart upload.
type chunkOwner struct {
	objectID string
	partID   string
}

// uploadResult summarises a completed stream.
type uploadResult struct {
	SizeBytes  int64
	ChunkCount int32
	// ETag is the SHA-256 of the entire stream. Unlike S3's MD5-based ETag it
	// is a genuine content hash for both single and multi-chunk objects.
	ETag string
}

// streamUpload is the heart of the write path.
//
// Memory profile: exactly one chunk-sized buffer is live at a time (allocated
// once by the splitter and reused), plus per-replica gRPC frame buffers. A
// 1 GiB upload therefore costs ~8 MiB of gateway memory, not 1 GiB.
//
// Chunks are processed sequentially. That is a deliberate simplification: it
// makes ordering trivial and bounds memory to one chunk, at the cost of not
// overlapping chunk N+1's read with chunk N's replication. Pipelining is a
// tracked follow-up, not an accident.
func (h *Handler) streamUpload(ctx context.Context, src io.Reader, owner chunkOwner, chunkSize int64) (uploadResult, error) {
	buf := h.bufs.Get(int(chunkSize))
	defer h.bufs.Put(buf)
	splitter, err := chunking.NewSplitterBuffer(src, buf, h.cfg.MaxObjectSize)
	if err != nil {
		return uploadResult{}, apierr.Wrap(http.StatusInternalServerError,
			apierr.CodeInternalError, "failed to initialise chunker", err)
	}

	objectHash := checksum.New()
	var total int64
	var count int32

	for {
		chunk, err := splitter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if errors.Is(err, chunking.ErrChunkTooLarge) {
				return uploadResult{}, apierr.Wrap(http.StatusRequestEntityTooLarge,
					apierr.CodeEntityTooLarge, "object exceeds the configured maximum size", err)
			}
			// A read failure here is almost always the client disconnecting.
			return uploadResult{}, apierr.Wrap(499, apierr.CodeClientClosedRequest,
				"failed reading request body", err)
		}

		sum := checksum.Sum(chunk.Data)
		objectHash.Write(chunk.Data)

		alloc, err := h.coord.AllocateChunk(ctx, &flexstorev1.AllocateChunkRequest{
			ObjectId:   owner.objectID,
			PartId:     owner.partID,
			ChunkIndex: int32(chunk.Index),
			SizeBytes:  int64(len(chunk.Data)),
		})
		if err != nil {
			if status.Code(err) == codes.ResourceExhausted {
				return uploadResult{}, apierr.Wrap(http.StatusServiceUnavailable,
					apierr.CodeInsufficientCapacity,
					"cluster cannot currently place the required number of replicas", err)
			}
			return uploadResult{}, apierr.Wrap(http.StatusBadGateway,
				apierr.CodeServiceUnavailable, "failed to allocate chunk", err)
		}

		minReplicas := int(alloc.MinWriteReplicas)
		if minReplicas < 1 {
			minReplicas = 1
		}
		res, err := h.writer.Write(ctx, alloc.ChunkId, chunk.Data, sum, alloc.Targets, minReplicas)
		if err != nil {
			// Record the failed replicas anyway so the coordinator knows which
			// nodes may hold partial state for this chunk.
			h.recordFailedCommit(ctx, alloc.ChunkId, sum, int64(len(chunk.Data)), res)
			return uploadResult{}, apierr.Wrap(http.StatusServiceUnavailable,
				apierr.CodeInsufficientReplicas,
				"could not durably store chunk on enough storage nodes", err)
		}

		commit, err := h.coord.CommitChunk(ctx, &flexstorev1.CommitChunkRequest{
			ChunkId:          alloc.ChunkId,
			ChecksumSha256:   sum,
			SizeBytes:        int64(len(chunk.Data)),
			SucceededNodeIds: res.Succeeded,
			FailedNodeIds:    res.Failed,
		})
		if err != nil {
			if status.Code(err) == codes.ResourceExhausted {
				return uploadResult{}, apierr.Wrap(http.StatusServiceUnavailable,
					apierr.CodeInsufficientReplicas,
					"chunk did not meet the durability policy", err)
			}
			return uploadResult{}, apierr.Wrap(http.StatusBadGateway,
				apierr.CodeServiceUnavailable, "failed to commit chunk metadata", err)
		}
		h.m.ReplicaCount.WithLabelValues("gateway").Observe(float64(commit.AvailableReplicas))

		total += int64(len(chunk.Data))
		count++
		h.m.UploadBytesTotal.Add(float64(len(chunk.Data)))
	}

	return uploadResult{
		SizeBytes:  total,
		ChunkCount: count,
		ETag:       checksum.Hex(objectHash),
	}, nil
}

// recordFailedCommit best-effort reports partial replica state after a failed
// write. It uses a detached context because the request context is usually
// already dead by the time we get here.
func (h *Handler) recordFailedCommit(ctx context.Context, chunkID, sum string, size int64, res WriteResult) {
	if len(res.Succeeded) == 0 && len(res.Failed) == 0 {
		return
	}
	detached := context.WithoutCancel(ctx)
	_, err := h.coord.Raw().CommitChunk(detached, &flexstorev1.CommitChunkRequest{
		ChunkId:          chunkID,
		ChecksumSha256:   sum,
		SizeBytes:        size,
		SucceededNodeIds: res.Succeeded,
		FailedNodeIds:    res.Failed,
	})
	// ResourceExhausted is the expected answer (that is why we are here); it
	// still had the side effect of recording replica states, which is the point.
	if err != nil && status.Code(err) != codes.ResourceExhausted {
		h.log.Warn("failed to record partial chunk state",
			slog.String("chunk_id", chunkID), slog.String("error", err.Error()))
	}
}

// streamDownload writes the given chunk spans to w in order, verifying each
// chunk before any of it is forwarded.
//
// Ranged and whole-object reads share this one path: a whole-object read is
// simply every chunk with a full-width span. There is deliberately no separate
// "fast path" for the unranged case, because the invariant that matters --
// nothing reaches the client until its chunk has been verified -- would then
// have to be maintained in two places.
func (h *Handler) streamDownload(ctx context.Context, w io.Writer, obj *flexstorev1.GetObjectResponse, spans []chunkSpan) (int64, error) {
	// One reusable buffer for the whole response, sized to the object's chunk
	// size. Reused across chunks so a 100-chunk object still allocates once.
	bufSize := obj.Object.ChunkSize
	if bufSize <= 0 {
		bufSize = h.cfg.ChunkSize
	}
	buf := h.bufs.Get(int(bufSize))
	defer h.bufs.Put(buf)
	buf = buf[:0]

	var written int64
	for _, span := range spans {
		data, err := h.reader.Fetch(ctx, span.chunk, buf)
		if err != nil {
			return written, err
		}
		// Trim only after verification: the checksum covers the whole chunk, so
		// a partial read could not be verified at all.
		if span.from > 0 || span.to < int64(len(data)) {
			data = data[span.from:span.to]
		}
		n, werr := w.Write(data)
		written += int64(n)
		if werr != nil {
			// The client went away mid-download. Not our failure.
			return written, fmt.Errorf("write to client: %w", werr)
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		buf = data[:0]
	}
	h.m.DownloadBytesTotal.Add(float64(written))
	return written, nil
}

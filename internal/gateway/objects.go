package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/apierr"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// PutObject implements PUT /objects/{bucket}/{key...}.
func (h *Handler) PutObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	log := observability.LoggerFrom(ctx, h.log)
	bucket, key := r.PathValue("bucket"), r.PathValue("key")

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	begin, err := h.coord.BeginUpload(ctx, &flexstorev1.BeginUploadRequest{
		Bucket:      bucket,
		Key:         key,
		ContentType: contentType,
		ChunkSize:   h.cfg.ChunkSize,
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	// Any exit path that is not an explicit success must abort the upload, or
	// its chunks leak until the coordinator's reaper notices.
	committed := false
	defer func() {
		if committed {
			return
		}
		// Detached context: the request's is usually already cancelled here.
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if aerr := h.coord.AbortUpload(abortCtx, begin.ObjectId, "gateway aborted upload"); aerr != nil {
			log.Error("failed to abort upload",
				slog.String("object_id", begin.ObjectId), slog.String("error", aerr.Error()))
		}
	}()

	res, err := h.streamUpload(ctx, r.Body, chunkOwner{objectID: begin.ObjectId}, begin.ChunkSize)
	if err != nil {
		h.fail(w, r, err)
		return
	}

	complete, err := h.coord.CompleteUpload(ctx, &flexstorev1.CompleteUploadRequest{
		ObjectId:   begin.ObjectId,
		SizeBytes:  res.SizeBytes,
		ChunkCount: res.ChunkCount,
		Etag:       res.ETag,
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	committed = true

	log.Info("object stored",
		slog.String("bucket", bucket), slog.String("key", key),
		slog.Int64("size_bytes", res.SizeBytes), slog.Int("chunks", int(res.ChunkCount)))

	w.Header().Set("ETag", quoteETag(complete.Etag))
	w.Header().Set("X-Flexstore-Object-Id", complete.ObjectId)
	w.Header().Set("X-Flexstore-Chunk-Count", strconv.Itoa(int(res.ChunkCount)))
	w.WriteHeader(http.StatusCreated)
}

// GetObject implements GET /objects/{bucket}/{key...}.
func (h *Handler) GetObject(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket, key := r.PathValue("bucket"), r.PathValue("key")

	// "GET /objects/{bucket}/" cannot be registered as its own pattern without
	// colliding with the {key...} wildcard, so it lands here with an empty key
	// and is treated as a bucket listing -- the same thing S3 does.
	if key == "" {
		h.ListObjects(w, r)
		return
	}

	obj, err := h.coord.GetObject(ctx, bucket, key)
	if err != nil {
		h.fail(w, r, err)
		return
	}

	writeObjectHeaders(w, obj.Object)
	w.Header().Set("Accept-Ranges", "bytes")

	// Range handling. RFC 7233 is specific about the three outcomes: a usable
	// range gets 206 with Content-Range, a syntactically valid range that does
	// not overlap the object gets 416, and anything unparseable is ignored and
	// answered with the whole object.
	status := http.StatusOK
	contentLength := obj.Object.SizeBytes
	spans := allChunks(obj.Chunks)

	if raw := r.Header.Get("Range"); raw != "" {
		// A range is an offset into a byte sequence, so it is only meaningful
		// if the object's recorded size agrees with the chunks that make it up.
		// If they disagree the metadata is inconsistent, and serving a window
		// computed from the wrong total would return plausible-looking wrong
		// bytes -- each chunk verifying perfectly on the way out. Fall back to
		// the whole object, which cannot be silently wrong.
		summed := objectSizeFromChunks(obj.Chunks)
		switch br, rerr := parseRange(raw, obj.Object.SizeBytes); {
		case rerr == nil && summed != obj.Object.SizeBytes:
			observability.LoggerFrom(ctx, h.log).Warn(
				"ignoring Range: object size disagrees with the sum of its chunks",
				slog.String("bucket", bucket), slog.String("key", key),
				slog.Int64("object_size", obj.Object.SizeBytes),
				slog.Int64("summed_chunks", summed))
		case rerr == nil:
			spans = selectChunks(obj.Chunks, br)
			contentLength = br.length()
			status = http.StatusPartialContent
			w.Header().Set("Content-Range", br.contentRange(obj.Object.SizeBytes))
		case errors.Is(rerr, errUnsatisfiableRange):
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Object.SizeBytes))
			h.fail(w, r, apierr.New(http.StatusRequestedRangeNotSatisfiable,
				apierr.CodeInvalidRange, "the requested range does not overlap the object"))
			return
		default:
			// Unparseable: RFC 7233 section 3.1 says ignore it.
		}
	}

	w.Header().Set("Content-Length", strconv.FormatInt(contentLength, 10))
	// Status is written before the body: an error after this point can only be
	// signalled by aborting the connection, which is why every chunk is
	// verified before its bytes reach this writer.
	w.WriteHeader(status)

	written, err := h.streamDownload(ctx, w, obj, spans)
	if err != nil {
		log := observability.LoggerFrom(ctx, h.log)
		log.Error("download failed mid-stream",
			slog.String("bucket", bucket), slog.String("key", key),
			slog.Int64("bytes_written", written),
			slog.Int64("expected_bytes", contentLength),
			slog.String("error", err.Error()))

		// Truncating the response is the only in-protocol way to tell the
		// client not to trust what it received. http.NewResponseController
		// lets us kill the connection so the client sees a transport error
		// rather than a short but "successful" body.
		if rc := http.NewResponseController(w); rc != nil {
			_ = rc.Flush()
		}
		panic(http.ErrAbortHandler)
	}
}

// HeadObject implements HEAD /objects/{bucket}/{key...}.
func (h *Handler) HeadObject(w http.ResponseWriter, r *http.Request) {
	obj, err := h.coord.HeadObject(r.Context(), r.PathValue("bucket"), r.PathValue("key"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeObjectHeaders(w, obj.Object)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Object.SizeBytes, 10))
	w.WriteHeader(http.StatusOK)
}

// DeleteObject implements DELETE /objects/{bucket}/{key...}.
//
// Deleting a key that does not exist returns 204, matching S3: DELETE is
// idempotent and clients should not have to distinguish "deleted it" from
// "it was already gone".
func (h *Handler) DeleteObject(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coord.DeleteObject(r.Context(), r.PathValue("bucket"), r.PathValue("key"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if resp.Existed {
		w.Header().Set("X-Flexstore-Object-Id", resp.ObjectId)
	}
	w.WriteHeader(http.StatusNoContent)
}

// ListObjects implements GET /objects/{bucket}.
func (h *Handler) ListObjects(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	bucket := r.PathValue("bucket")
	q := r.URL.Query()

	limit := 1000
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			h.fail(w, r, apierr.New(http.StatusBadRequest, apierr.CodeInvalidRequest,
				"limit must be a positive integer"))
			return
		}
		limit = n
	}

	resp, err := h.coord.Raw().ListObjects(ctx, &flexstorev1.ListObjectsRequest{
		Bucket:     bucket,
		Prefix:     q.Get("prefix"),
		StartAfter: q.Get("start_after"),
		Limit:      int32(limit),
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	type item struct {
		Key         string `json:"key"`
		Size        int64  `json:"size_bytes"`
		ETag        string `json:"etag"`
		ContentType string `json:"content_type"`
		Version     int64  `json:"version"`
		ModifiedAt  string `json:"modified_at,omitempty"`
	}
	out := struct {
		Bucket    string `json:"bucket"`
		Objects   []item `json:"objects"`
		Truncated bool   `json:"truncated"`
	}{Bucket: bucket, Objects: make([]item, 0, len(resp.Objects)), Truncated: resp.Truncated}

	for _, o := range resp.Objects {
		it := item{
			Key: o.Key, Size: o.SizeBytes, ETag: quoteETag(o.Etag),
			ContentType: o.ContentType, Version: o.Version,
		}
		if o.CompletedAt != nil {
			it.ModifiedAt = o.CompletedAt.AsTime().UTC().Format(time.RFC3339Nano)
		}
		out.Objects = append(out.Objects, it)
	}
	writeJSON(w, http.StatusOK, out, h.log)
}

// fail renders an error response using the request-scoped logger.
func (h *Handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	ctx := r.Context()
	// A cancelled request means the client hung up; there is nobody to tell.
	if errors.Is(ctx.Err(), context.Canceled) && !isWritten(w) {
		observability.LoggerFrom(ctx, h.log).Info("client cancelled request")
		return
	}
	apierr.Write(w, r, observability.RequestIDFrom(ctx), err, observability.LoggerFrom(ctx, h.log))
}

// isWritten reports whether the response has already been committed. The
// metrics middleware wraps every ResponseWriter, so this is reliable on all
// routes.
func isWritten(w http.ResponseWriter) bool {
	if rw, ok := w.(*responseRecorder); ok {
		return rw.wroteHeader
	}
	return false
}

func writeObjectHeaders(w http.ResponseWriter, o *flexstorev1.ObjectInfo) {
	w.Header().Set("Content-Type", o.ContentType)
	w.Header().Set("ETag", quoteETag(o.Etag))
	w.Header().Set("Accept-Ranges", "none")
	w.Header().Set("X-Flexstore-Object-Id", o.ObjectId)
	w.Header().Set("X-Flexstore-Version", strconv.FormatInt(o.Version, 10))
	w.Header().Set("X-Flexstore-Chunk-Count", strconv.Itoa(int(o.ChunkCount)))
	w.Header().Set("X-Flexstore-Chunk-Size", strconv.FormatInt(o.ChunkSize, 10))
	if o.CompletedAt != nil {
		w.Header().Set("Last-Modified", o.CompletedAt.AsTime().UTC().Format(http.TimeFormat))
	}
}

func quoteETag(e string) string {
	if e == "" {
		return ""
	}
	return `"` + e + `"`
}

func shortHealth(h flexstorev1.NodeHealth) string {
	switch h {
	case flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY:
		return "HEALTHY"
	case flexstorev1.NodeHealth_NODE_HEALTH_SUSPECT:
		return "SUSPECT"
	case flexstorev1.NodeHealth_NODE_HEALTH_DEAD:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

func shortReplicaState(s flexstorev1.ReplicaState) string {
	switch s {
	case flexstorev1.ReplicaState_REPLICA_STATE_WRITING:
		return "WRITING"
	case flexstorev1.ReplicaState_REPLICA_STATE_AVAILABLE:
		return "AVAILABLE"
	case flexstorev1.ReplicaState_REPLICA_STATE_UNAVAILABLE:
		return "UNAVAILABLE"
	case flexstorev1.ReplicaState_REPLICA_STATE_DELETING:
		return "DELETING"
	default:
		return "UNKNOWN"
	}
}

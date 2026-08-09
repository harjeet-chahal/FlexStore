package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/apierr"
)

// maxCompleteBodyBytes bounds the JSON manifest a client may POST when
// completing a multipart upload. 10 000 parts of ~100 bytes each is the
// worst legitimate case; anything larger is malformed or hostile.
const maxCompleteBodyBytes = 2 << 20

// CreateMultipartUpload implements POST /multipart/{bucket}/{key...}.
func (h *Handler) CreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coord.Raw().CreateMultipartUpload(r.Context(), &flexstorev1.CreateMultipartUploadRequest{
		Bucket:      r.PathValue("bucket"),
		Key:         r.PathValue("key"),
		ContentType: r.Header.Get("Content-Type"),
		ChunkSize:   h.cfg.ChunkSize,
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, struct {
		UploadID  string `json:"upload_id"`
		Bucket    string `json:"bucket"`
		Key       string `json:"key"`
		ChunkSize int64  `json:"chunk_size"`
	}{resp.UploadId, r.PathValue("bucket"), r.PathValue("key"), resp.ChunkSize}, h.log)
}

// UploadPart implements PUT /multipart/{uploadId}/{partNumber}.
//
// A part is chunked exactly like a whole object; the only difference is that
// its chunks are owned by the part until the upload is completed, at which
// point they are re-parented in a single metadata transaction.
func (h *Handler) UploadPart(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	uploadID := r.PathValue("uploadId")

	partNumber, err := strconv.Atoi(r.PathValue("partNumber"))
	if err != nil || partNumber < 1 || partNumber > 10000 {
		h.fail(w, r, apierr.New(http.StatusBadRequest, apierr.CodeInvalidPartNumber,
			"partNumber must be an integer in [1, 10000]"))
		return
	}

	begin, err := h.coord.Raw().BeginPart(ctx, &flexstorev1.BeginPartRequest{
		UploadId:   uploadID,
		PartNumber: int32(partNumber), // #nosec G109 -- rejected above unless within [1, 10000]
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	res, err := h.streamUpload(ctx, r.Body, chunkOwner{partID: begin.PartId}, begin.ChunkSize)
	if err != nil {
		h.fail(w, r, err)
		return
	}

	if _, err := h.coord.Raw().CompletePart(ctx, &flexstorev1.CompletePartRequest{
		PartId:     begin.PartId,
		SizeBytes:  res.SizeBytes,
		ChunkCount: res.ChunkCount,
		Etag:       res.ETag,
	}); err != nil {
		h.fail(w, r, err)
		return
	}

	w.Header().Set("ETag", quoteETag(res.ETag))
	w.Header().Set("X-Flexstore-Part-Id", begin.PartId)
	writeJSON(w, http.StatusOK, struct {
		UploadID   string `json:"upload_id"`
		PartNumber int    `json:"part_number"`
		ETag       string `json:"etag"`
		SizeBytes  int64  `json:"size_bytes"`
	}{uploadID, partNumber, res.ETag, res.SizeBytes}, h.log)
}

// CompleteMultipartUpload implements POST /multipart/{uploadId}/complete.
func (h *Handler) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	uploadID := r.PathValue("uploadId")

	var body struct {
		Parts []struct {
			PartNumber int32  `json:"part_number"`
			ETag       string `json:"etag"`
		} `json:"parts"`
	}
	// The manifest is optional: with no body the coordinator uses whatever
	// parts it recorded, which is the convenient path for simple clients.
	limited := io.LimitReader(r.Body, maxCompleteBodyBytes)
	raw, err := io.ReadAll(limited)
	if err != nil {
		h.fail(w, r, apierr.Wrap(http.StatusBadRequest, apierr.CodeInvalidRequest,
			"failed to read request body", err))
		return
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			h.fail(w, r, apierr.Wrap(http.StatusBadRequest, apierr.CodeInvalidRequest,
				"request body must be JSON of the form {\"parts\":[{\"part_number\":1,\"etag\":\"...\"}]}", err))
			return
		}
	}

	parts := make([]*flexstorev1.CompletedPart, 0, len(body.Parts))
	for _, p := range body.Parts {
		parts = append(parts, &flexstorev1.CompletedPart{PartNumber: p.PartNumber, Etag: p.ETag})
	}

	resp, err := h.coord.Raw().CompleteMultipartUpload(ctx, &flexstorev1.CompleteMultipartUploadRequest{
		UploadId: uploadID,
		Parts:    parts,
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	w.Header().Set("ETag", quoteETag(resp.Etag))
	writeJSON(w, http.StatusOK, struct {
		UploadID  string `json:"upload_id"`
		ObjectID  string `json:"object_id"`
		ETag      string `json:"etag"`
		SizeBytes int64  `json:"size_bytes"`
	}{uploadID, resp.ObjectId, resp.Etag, resp.SizeBytes}, h.log)
}

// AbortMultipartUpload implements DELETE /multipart/{uploadId}.
func (h *Handler) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	_, err := h.coord.Raw().AbortMultipartUpload(r.Context(), &flexstorev1.AbortMultipartUploadRequest{
		UploadId: r.PathValue("uploadId"),
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, status int, body any, log logger) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent, so this can only be reported.
		if !errors.Is(err, http.ErrHandlerTimeout) {
			log.Warn("failed to encode response body", "error", err.Error())
		}
	}
}

// logger is the minimal logging surface writeJSON needs, kept tiny so the
// helper does not drag slog into every call site's signature.
type logger interface {
	Warn(msg string, args ...any)
}

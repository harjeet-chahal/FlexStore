package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/cache"
	"github.com/harjeetschahal/flexstore/internal/metadata"
)

// CreateMultipartUpload opens a session.
func (s *Service) CreateMultipartUpload(ctx context.Context, req *flexstorev1.CreateMultipartUploadRequest) (*flexstorev1.CreateMultipartUploadResponse, error) {
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

	mu, err := s.store.CreateMultipartUpload(ctx, req.Bucket, req.Key, contentType, chunkSize)
	if err != nil {
		return nil, toStatus(err, "create multipart upload")
	}
	s.cacheUpload(ctx, mu)
	s.log.Info("multipart upload created",
		slog.String("upload_id", mu.ID.String()),
		slog.String("bucket", mu.Bucket), slog.String("key", mu.Key))

	return &flexstorev1.CreateMultipartUploadResponse{
		UploadId:  mu.ID.String(),
		ChunkSize: mu.ChunkSize,
	}, nil
}

// BeginPart opens (or reopens) a part.
func (s *Service) BeginPart(ctx context.Context, req *flexstorev1.BeginPartRequest) (*flexstorev1.BeginPartResponse, error) {
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "upload_id: %v", err)
	}
	// Every part upload needs the session's chunk size; caching it turns an
	// N-part upload from N+1 session lookups into one.
	mu, err := s.loadUpload(ctx, uploadID)
	if err != nil {
		return nil, toStatus(err, "begin part")
	}
	part, err := s.store.BeginPart(ctx, uploadID, req.PartNumber)
	if err != nil {
		return nil, toStatus(err, "begin part")
	}
	return &flexstorev1.BeginPartResponse{
		PartId:    part.ID.String(),
		ChunkSize: mu.ChunkSize,
	}, nil
}

// CompletePart finalises a part.
func (s *Service) CompletePart(ctx context.Context, req *flexstorev1.CompletePartRequest) (*flexstorev1.CompletePartResponse, error) {
	partID, err := uuid.Parse(req.PartId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "part_id: %v", err)
	}
	if err := s.store.CompletePart(ctx, partID, req.SizeBytes, req.ChunkCount, req.Etag); err != nil {
		if errors.Is(err, metadata.ErrDurabilityNotMet) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, toStatus(err, "complete part")
	}
	return &flexstorev1.CompletePartResponse{Etag: req.Etag}, nil
}

// CompleteMultipartUpload assembles the parts into an object.
//
// A short distributed lock keyed on bucket/key serialises concurrent completes
// for the same key. It is an optimisation, not the correctness mechanism --
// the database transaction inside CompleteMultipartUpload is -- but it turns a
// racing pair of completes into "one waits" instead of "one does the work then
// fails".
func (s *Service) CompleteMultipartUpload(ctx context.Context, req *flexstorev1.CompleteMultipartUploadRequest) (*flexstorev1.CompleteMultipartUploadResponse, error) {
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "upload_id: %v", err)
	}
	mu, err := s.loadUpload(ctx, uploadID)
	if err != nil {
		return nil, toStatus(err, "complete multipart upload")
	}

	lockKey := "object:" + mu.Bucket + "/" + mu.Key
	ok, release, err := s.cache.AcquireLock(ctx, lockKey, s.cfg.NodeCallTimeout)
	if err != nil {
		s.log.Warn("lock acquisition failed; proceeding without it",
			slog.String("key", lockKey), slog.String("error", err.Error()))
	} else if !ok {
		return nil, status.Errorf(codes.Aborted,
			"another completion is in progress for %s/%s", mu.Bucket, mu.Key)
	} else {
		defer release(context.WithoutCancel(ctx))
	}

	expected := make([]metadata.CompletedPartRef, 0, len(req.Parts))
	for _, p := range req.Parts {
		expected = append(expected, metadata.CompletedPartRef{PartNumber: p.PartNumber, ETag: p.Etag})
	}

	obj, err := s.store.CompleteMultipartUpload(ctx, uploadID, expected)
	if err != nil {
		return nil, toStatus(err, "complete multipart upload")
	}
	s.invalidateObject(ctx, obj.Bucket, obj.Key)
	s.invalidateUpload(ctx, uploadID.String())

	s.log.Info("multipart upload completed",
		slog.String("upload_id", uploadID.String()),
		slog.String("bucket", obj.Bucket), slog.String("key", obj.Key),
		slog.Int64("size_bytes", obj.SizeBytes),
		slog.Int("chunks", int(obj.ChunkCount)))

	return &flexstorev1.CompleteMultipartUploadResponse{
		ObjectId:  obj.ID.String(),
		Etag:      obj.ETag,
		SizeBytes: obj.SizeBytes,
	}, nil
}

// AbortMultipartUpload cancels a session.
func (s *Service) AbortMultipartUpload(ctx context.Context, req *flexstorev1.AbortMultipartUploadRequest) (*flexstorev1.AbortMultipartUploadResponse, error) {
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "upload_id: %v", err)
	}
	if err := s.store.AbortMultipartUpload(ctx, uploadID); err != nil {
		return nil, toStatus(err, "abort multipart upload")
	}
	s.invalidateUpload(ctx, uploadID.String())
	s.log.Info("multipart upload aborted", slog.String("upload_id", uploadID.String()))
	return &flexstorev1.AbortMultipartUploadResponse{}, nil
}

// cachedUpload is the small, immutable slice of a multipart session that the
// upload path reads repeatedly. Mutable fields (state, object_id) are
// deliberately excluded: anything that changes during the session must come
// from PostgreSQL so a cached value can never authorise a write to a session
// that has since been aborted.
type cachedUpload struct {
	Bucket      string `json:"b"`
	Key         string `json:"k"`
	ChunkSize   int64  `json:"cs"`
	ContentType string `json:"ct"`
}

// loadUpload reads a session header, preferring the cache.
func (s *Service) loadUpload(ctx context.Context, id uuid.UUID) (metadata.MultipartUpload, error) {
	key := cache.MultipartKey(id.String())

	var c cachedUpload
	if err := s.cache.Get(ctx, key, &c); err == nil {
		return metadata.MultipartUpload{
			ID: id, Bucket: c.Bucket, Key: c.Key,
			ChunkSize: c.ChunkSize, ContentType: c.ContentType,
		}, nil
	} else if !errors.Is(err, cache.ErrMiss) {
		s.log.Warn("multipart cache read failed",
			slog.String("upload_id", id.String()), slog.String("error", err.Error()))
	}

	mu, err := s.store.GetMultipartUpload(ctx, id)
	if err != nil {
		return metadata.MultipartUpload{}, err
	}
	s.cacheUpload(ctx, mu)
	return mu, nil
}

func (s *Service) cacheUpload(ctx context.Context, mu metadata.MultipartUpload) {
	// Sessions are long-lived, so a generous TTL is fine; correctness never
	// depends on it because the header fields cached here never change.
	if err := s.cache.Set(ctx, cache.MultipartKey(mu.ID.String()), cachedUpload{
		Bucket: mu.Bucket, Key: mu.Key, ChunkSize: mu.ChunkSize, ContentType: mu.ContentType,
	}, time.Hour); err != nil {
		s.log.Warn("multipart cache write failed",
			slog.String("upload_id", mu.ID.String()), slog.String("error", err.Error()))
	}
}

func (s *Service) invalidateUpload(ctx context.Context, uploadID string) {
	if err := s.cache.Delete(ctx, cache.MultipartKey(uploadID)); err != nil {
		s.log.Warn("failed to invalidate multipart cache",
			slog.String("upload_id", uploadID), slog.String("error", err.Error()))
	}
}

package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const objectColumns = `
	id, bucket, key, version, state, size_bytes, chunk_size, chunk_count,
	etag, content_type, failure_reason, created_at, updated_at, completed_at`

// BeginUpload creates a new UPLOADING object version.
//
// It does not touch any existing COMPLETE version: readers keep seeing the old
// object for the entire duration of the upload, and the swap happens in a
// single transaction inside CompleteUpload. That is what makes overwrite
// atomic from a client's point of view.
func (s *Store) BeginUpload(ctx context.Context, bucket, key, contentType string, chunkSize int64) (Object, error) {
	id := uuid.New()
	var obj Object
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var next int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(version), 0) + 1 FROM objects WHERE bucket = $1 AND key = $2`,
			bucket, key).Scan(&next); err != nil {
			return fmt.Errorf("compute next version: %w", err)
		}

		rows, err := tx.Query(ctx, `
			INSERT INTO objects (id, bucket, key, version, state, chunk_size, content_type)
			VALUES ($1, $2, $3, $4, 'UPLOADING', $5, $6)
			RETURNING `+objectColumns,
			id, bucket, key, next, chunkSize, contentType)
		if err != nil {
			return fmt.Errorf("insert object: %w", err)
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("insert object returned no row")
		}
		obj, err = scanObject(rows)
		return err
	})
	if err != nil {
		return Object{}, err
	}
	return obj, nil
}

// TouchUpload refreshes updated_at so the stale-upload reaper does not reclaim
// a slow but healthy upload.
func (s *Store) TouchUpload(ctx context.Context, objectID uuid.UUID) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE objects SET updated_at = now() WHERE id = $1 AND state = 'UPLOADING'`, objectID)
	if err != nil {
		return fmt.Errorf("touch upload %s: %w", objectID, err)
	}
	return nil
}

// CompleteUpload validates durability, then atomically promotes this version
// to COMPLETE and demotes any previously current version to DELETING.
func (s *Store) CompleteUpload(ctx context.Context, objectID uuid.UUID, sizeBytes int64, chunkCount int32, etag string) (Object, error) {
	var obj Object
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Lock the row so two concurrent CompleteUpload calls for the same
		// object cannot both pass validation.
		var bucket, key, state string
		if err := tx.QueryRow(ctx,
			`SELECT bucket, key, state FROM objects WHERE id = $1 FOR UPDATE`, objectID).
			Scan(&bucket, &key, &state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("object %s: %w", objectID, ErrNotFound)
			}
			return fmt.Errorf("lock object %s: %w", objectID, err)
		}
		if ObjectState(state) != ObjectUploading {
			return fmt.Errorf("object %s is %s, expected UPLOADING: %w", objectID, state, ErrConflict)
		}

		// Every chunk the gateway claims to have written must actually be
		// COMMITTED. Without this an aborted-then-resumed upload could produce
		// an object with holes.
		var total, committed int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE state = 'COMMITTED')
			FROM chunks WHERE object_id = $1`, objectID).Scan(&total, &committed); err != nil {
			return fmt.Errorf("count chunks: %w", err)
		}
		if total != int64(chunkCount) || committed != int64(chunkCount) {
			return fmt.Errorf(
				"object %s: expected %d committed chunks, found %d committed of %d: %w",
				objectID, chunkCount, committed, total, ErrDurabilityNotMet)
		}

		// Supersede the previous current version. Doing this before the UPDATE
		// below keeps the partial unique index satisfied at all times.
		if err := supersedeCurrentVersion(ctx, tx, bucket, key, objectID); err != nil {
			return err
		}

		rows, err := tx.Query(ctx, `
			UPDATE objects SET
				state = 'COMPLETE', size_bytes = $2, chunk_count = $3,
				etag = $4, completed_at = now(), updated_at = now()
			WHERE id = $1
			RETURNING `+objectColumns,
			objectID, sizeBytes, chunkCount, etag)
		if err != nil {
			return fmt.Errorf("complete object: %w", err)
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("complete object %s returned no row", objectID)
		}
		obj, err = scanObject(rows)
		return err
	})
	if err != nil {
		return Object{}, err
	}
	return obj, nil
}

// AbortUpload marks an in-flight upload FAILED and queues its chunks for
// reclamation. Safe to call repeatedly (aborting an already-failed upload is a
// no-op), because the gateway calls it from a defer on every error path.
func (s *Store) AbortUpload(ctx context.Context, objectID uuid.UUID, reason string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE objects SET state = 'FAILED', failure_reason = $2, updated_at = now()
			WHERE id = $1 AND state = 'UPLOADING'`, objectID, truncate(reason, 500))
		if err != nil {
			return fmt.Errorf("abort upload %s: %w", objectID, err)
		}
		if tag.RowsAffected() == 0 {
			return nil // already aborted, completed, or gone
		}
		return orphanObjectChunks(ctx, tx, objectID)
	})
}

// DeleteObject moves the current version to DELETING and queues replica
// removal. The object disappears from reads immediately; the bytes are
// reclaimed asynchronously by the GC worker.
func (s *Store) DeleteObject(ctx context.Context, bucket, key string) (uuid.UUID, bool, error) {
	var id uuid.UUID
	var existed bool
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE objects SET state = 'DELETING', deleted_at = now(), updated_at = now()
			WHERE bucket = $1 AND key = $2 AND state = 'COMPLETE'
			RETURNING id`, bucket, key).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("delete object %s/%s: %w", bucket, key, err)
		}
		existed = true
		return enqueueObjectChunkDeletions(ctx, tx, id)
	})
	return id, existed, err
}

// GetObject returns the current COMPLETE version with its full chunk layout,
// ordered by chunk index and including every AVAILABLE replica location.
func (s *Store) GetObject(ctx context.Context, bucket, key string) (Object, []ChunkWithReplicas, error) {
	obj, err := s.HeadObject(ctx, bucket, key)
	if err != nil {
		return Object{}, nil, err
	}
	chunks, err := s.ChunksForObject(ctx, obj.ID, true)
	if err != nil {
		return Object{}, nil, err
	}
	if len(chunks) != int(obj.ChunkCount) {
		// Metadata is internally inconsistent; refusing to serve is far better
		// than returning a silently truncated object.
		return Object{}, nil, fmt.Errorf(
			"object %s/%s: metadata lists %d chunks but %d are readable: %w",
			bucket, key, obj.ChunkCount, len(chunks), ErrConflict)
	}
	return obj, chunks, nil
}

// HeadObject returns metadata for the current COMPLETE version.
func (s *Store) HeadObject(ctx context.Context, bucket, key string) (Object, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = $1 AND key = $2 AND state = 'COMPLETE'`,
		bucket, key)
	if err != nil {
		return Object{}, fmt.Errorf("head object %s/%s: %w", bucket, key, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Object{}, err
		}
		return Object{}, fmt.Errorf("object %s/%s: %w", bucket, key, ErrNotFound)
	}
	return scanObject(rows)
}

// GetObjectByID returns any object version regardless of state.
func (s *Store) GetObjectByID(ctx context.Context, id uuid.UUID) (Object, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+objectColumns+` FROM objects WHERE id = $1`, id)
	if err != nil {
		return Object{}, fmt.Errorf("get object %s: %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Object{}, err
		}
		return Object{}, fmt.Errorf("object %s: %w", id, ErrNotFound)
	}
	return scanObject(rows)
}

// ListObjects returns COMPLETE objects in a bucket, lexicographically ordered.
// limit+1 rows are fetched so truncation can be reported without a count query.
func (s *Store) ListObjects(ctx context.Context, bucket, prefix, startAfter string, limit int) ([]Object, bool, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+objectColumns+` FROM objects
		WHERE bucket = $1 AND state = 'COMPLETE' AND key > $2 AND key LIKE $3 || '%'
		ORDER BY key
		LIMIT $4`, bucket, startAfter, prefix, limit+1)
	if err != nil {
		return nil, false, fmt.Errorf("list objects in %s: %w", bucket, err)
	}
	defer rows.Close()

	var out []Object
	for rows.Next() {
		o, err := scanObject(rows)
		if err != nil {
			return nil, false, err
		}
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	truncated := len(out) > limit
	if truncated {
		out = out[:limit]
	}
	return out, truncated, nil
}

// ReapStaleUploads fails uploads that have not progressed within the timeout.
// Without this, a gateway crash mid-upload would leak chunks forever.
func (s *Store) ReapStaleUploads(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	var reaped int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT id FROM objects
			WHERE state = 'UPLOADING' AND updated_at < now() - $1::interval
			ORDER BY updated_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED`, olderThan.String(), limit)
		if err != nil {
			return fmt.Errorf("scan stale uploads: %w", err)
		}
		var ids []uuid.UUID
		for rows.Next() {
			var id uuid.UUID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		for _, id := range ids {
			if _, err := tx.Exec(ctx, `
				UPDATE objects SET state = 'FAILED', failure_reason = 'upload timed out', updated_at = now()
				WHERE id = $1`, id); err != nil {
				return fmt.Errorf("fail stale upload %s: %w", id, err)
			}
			if err := orphanObjectChunks(ctx, tx, id); err != nil {
				return err
			}
			reaped++
		}
		return nil
	})
	return reaped, err
}

// supersedeCurrentVersion retires whatever COMPLETE version of (bucket, key)
// exists, excluding newID, and queues its replicas for reclamation.
//
// Enqueuing the deletions here rather than leaving it to a scanner is what
// stops an overwrite-heavy workload from leaking a full copy of the object on
// every PUT.
func supersedeCurrentVersion(ctx context.Context, tx pgx.Tx, bucket, key string, newID uuid.UUID) error {
	rows, err := tx.Query(ctx, `
		UPDATE objects SET state = 'DELETING', deleted_at = now(), updated_at = now()
		WHERE bucket = $1 AND key = $2 AND state = 'COMPLETE' AND id <> $3
		RETURNING id`, bucket, key, newID)
	if err != nil {
		return fmt.Errorf("supersede previous version: %w", err)
	}
	var superseded []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan superseded version: %w", err)
		}
		superseded = append(superseded, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate superseded versions: %w", err)
	}

	for _, id := range superseded {
		if err := enqueueObjectChunkDeletions(ctx, tx, id); err != nil {
			return err
		}
	}
	return nil
}

func scanObject(rows pgx.Rows) (Object, error) {
	var (
		o           Object
		etag        *string
		failure     *string
		completedAt *time.Time
		state       string
	)
	if err := rows.Scan(&o.ID, &o.Bucket, &o.Key, &o.Version, &state, &o.SizeBytes,
		&o.ChunkSize, &o.ChunkCount, &etag, &o.ContentType, &failure,
		&o.CreatedAt, &o.UpdatedAt, &completedAt); err != nil {
		return Object{}, fmt.Errorf("scan object: %w", err)
	}
	o.State = ObjectState(state)
	o.ETag = nullString(etag)
	o.FailureReason = nullString(failure)
	o.CompletedAt = nullTime(completedAt)
	return o, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// CreateMultipartUpload opens a new multipart session.
func (s *Store) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string, chunkSize int64) (MultipartUpload, error) {
	id := uuid.New()
	var mu MultipartUpload
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO multipart_uploads (id, bucket, key, state, chunk_size, content_type)
			VALUES ($1, $2, $3, 'IN_PROGRESS', $4, $5)
			RETURNING id, bucket, key, state, chunk_size, content_type, created_at, updated_at`,
			id, bucket, key, chunkSize, contentType)
		var state string
		if err := row.Scan(&mu.ID, &mu.Bucket, &mu.Key, &state, &mu.ChunkSize,
			&mu.ContentType, &mu.CreatedAt, &mu.UpdatedAt); err != nil {
			return fmt.Errorf("create multipart upload: %w", err)
		}
		mu.State = MultipartState(state)
		return nil
	})
	return mu, err
}

// GetMultipartUpload loads a session by ID.
func (s *Store) GetMultipartUpload(ctx context.Context, id uuid.UUID) (MultipartUpload, error) {
	var mu MultipartUpload
	var state string
	var objectID *uuid.UUID
	err := s.pool.QueryRow(ctx, `
		SELECT id, bucket, key, state, chunk_size, content_type, object_id, created_at, updated_at
		FROM multipart_uploads WHERE id = $1`, id).
		Scan(&mu.ID, &mu.Bucket, &mu.Key, &state, &mu.ChunkSize, &mu.ContentType,
			&objectID, &mu.CreatedAt, &mu.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return MultipartUpload{}, fmt.Errorf("upload %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return MultipartUpload{}, fmt.Errorf("get multipart upload %s: %w", id, err)
	}
	mu.State = MultipartState(state)
	mu.ObjectID = objectID
	return mu, nil
}

// BeginPart starts (or restarts) a part upload.
//
// Re-uploading a part is legal in S3 and replaces the previous copy. We
// implement that by orphaning the old part's chunks -- the GC reclaims them --
// rather than mutating them in place, so a concurrent CompleteMultipartUpload
// can never see a half-replaced part.
func (s *Store) BeginPart(ctx context.Context, uploadID uuid.UUID, partNumber int32) (Part, error) {
	if partNumber < 1 || partNumber > 10000 {
		return Part{}, fmt.Errorf("part number %d out of range [1,10000]: %w", partNumber, ErrConflict)
	}

	id := uuid.New()
	var part Part
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// FOR SHARE, not FOR UPDATE. An exclusive lock here would serialise
		// every concurrent part upload in the session behind a single row --
		// the opposite of what multipart exists for. A shared lock still blocks
		// CompleteMultipartUpload and AbortMultipartUpload (both take FOR
		// UPDATE), which is the only mutual exclusion actually required: parts
		// may be added concurrently, but not while the session is being closed.
		var state string
		if err := tx.QueryRow(ctx,
			`SELECT state FROM multipart_uploads WHERE id = $1 FOR SHARE`, uploadID).Scan(&state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("upload %s: %w", uploadID, ErrNotFound)
			}
			return fmt.Errorf("lock upload %s: %w", uploadID, err)
		}
		if MultipartState(state) != MultipartInProgress {
			return fmt.Errorf("upload %s is %s: %w", uploadID, state, ErrConflict)
		}

		// Supersede any prior attempt at this part number.
		rows, err := tx.Query(ctx, `
			UPDATE multipart_parts SET state = 'FAILED', updated_at = now()
			WHERE upload_id = $1 AND part_number = $2 AND state <> 'FAILED'
			RETURNING id`, uploadID, partNumber)
		if err != nil {
			return fmt.Errorf("supersede prior part: %w", err)
		}
		var priorIDs []uuid.UUID
		for rows.Next() {
			var pid uuid.UUID
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				return err
			}
			priorIDs = append(priorIDs, pid)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, pid := range priorIDs {
			if err := orphanPartChunks(ctx, tx, pid); err != nil {
				return err
			}
		}

		var pstate string
		if err := tx.QueryRow(ctx, `
			INSERT INTO multipart_parts (id, upload_id, part_number, state)
			VALUES ($1, $2, $3, 'UPLOADING')
			RETURNING id, upload_id, part_number, state, size_bytes, chunk_count, created_at`,
			id, uploadID, partNumber).
			Scan(&part.ID, &part.UploadID, &part.PartNumber, &pstate,
				&part.SizeBytes, &part.ChunkCount, &part.CreatedAt); err != nil {
			return fmt.Errorf("insert part: %w", err)
		}
		part.State = PartState(pstate)

		_, err = tx.Exec(ctx, `UPDATE multipart_uploads SET updated_at = now() WHERE id = $1`, uploadID)
		return err
	})
	return part, err
}

// CompletePart finalises a part after all of its chunks are committed.
func (s *Store) CompletePart(ctx context.Context, partID uuid.UUID, sizeBytes int64, chunkCount int32, etag string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var state string
		if err := tx.QueryRow(ctx,
			`SELECT state FROM multipart_parts WHERE id = $1 FOR UPDATE`, partID).Scan(&state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("part %s: %w", partID, ErrNotFound)
			}
			return fmt.Errorf("lock part %s: %w", partID, err)
		}
		if PartState(state) != PartUploading {
			return fmt.Errorf("part %s is %s, expected UPLOADING: %w", partID, state, ErrConflict)
		}

		var total, committed int64
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*), COUNT(*) FILTER (WHERE state = 'COMMITTED')
			FROM chunks WHERE part_id = $1`, partID).Scan(&total, &committed); err != nil {
			return fmt.Errorf("count part chunks: %w", err)
		}
		if total != int64(chunkCount) || committed != int64(chunkCount) {
			return fmt.Errorf("part %s: expected %d committed chunks, found %d of %d: %w",
				partID, chunkCount, committed, total, ErrDurabilityNotMet)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE multipart_parts SET state = 'COMPLETE', size_bytes = $2, chunk_count = $3,
				etag = $4, completed_at = now(), updated_at = now()
			WHERE id = $1`, partID, sizeBytes, chunkCount, etag); err != nil {
			return fmt.Errorf("complete part %s: %w", partID, err)
		}
		return nil
	})
}

// ListParts returns the COMPLETE parts of an upload in part-number order.
func (s *Store) ListParts(ctx context.Context, uploadID uuid.UUID) ([]Part, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, upload_id, part_number, state, size_bytes, chunk_count, COALESCE(etag, ''), created_at
		FROM multipart_parts
		WHERE upload_id = $1 AND state = 'COMPLETE'
		ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, fmt.Errorf("list parts for %s: %w", uploadID, err)
	}
	defer rows.Close()

	var out []Part
	for rows.Next() {
		var p Part
		var state string
		if err := rows.Scan(&p.ID, &p.UploadID, &p.PartNumber, &state,
			&p.SizeBytes, &p.ChunkCount, &p.ETag, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan part: %w", err)
		}
		p.State = PartState(state)
		out = append(out, p)
	}
	return out, rows.Err()
}

// CompleteMultipartUpload assembles the parts into a single object.
//
// Assembly is pure metadata: the already-written chunks are re-parented from
// their parts to the new object and renumbered into one contiguous sequence.
// No bytes move, so completing a 100 GB multipart upload is a single
// transaction, not a 100 GB copy.
func (s *Store) CompleteMultipartUpload(ctx context.Context, uploadID uuid.UUID, expected []CompletedPartRef) (Object, error) {
	var obj Object
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var bucket, key, state, contentType string
		var chunkSize int64
		if err := tx.QueryRow(ctx, `
			SELECT bucket, key, state, content_type, chunk_size
			FROM multipart_uploads WHERE id = $1 FOR UPDATE`, uploadID).
			Scan(&bucket, &key, &state, &contentType, &chunkSize); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("upload %s: %w", uploadID, ErrNotFound)
			}
			return fmt.Errorf("lock upload %s: %w", uploadID, err)
		}
		if MultipartState(state) != MultipartInProgress {
			return fmt.Errorf("upload %s is %s: %w", uploadID, state, ErrConflict)
		}

		rows, err := tx.Query(ctx, `
			SELECT id, part_number, size_bytes, chunk_count, COALESCE(etag, '')
			FROM multipart_parts
			WHERE upload_id = $1 AND state = 'COMPLETE'
			ORDER BY part_number`, uploadID)
		if err != nil {
			return fmt.Errorf("load parts: %w", err)
		}
		type partRow struct {
			id     uuid.UUID
			number int32
			size   int64
			chunks int32
			etag   string
		}
		var parts []partRow
		for rows.Next() {
			var p partRow
			if err := rows.Scan(&p.id, &p.number, &p.size, &p.chunks, &p.etag); err != nil {
				rows.Close()
				return err
			}
			parts = append(parts, p)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		// Without a manifest, "assemble what is here" is only a coherent
		// instruction when nothing is still arriving. A part in UPLOADING state
		// is one the client believes it is still sending; assembling around it
		// produces an object silently short by exactly that part, returns 200,
		// and gives the client no way to notice. Refuse instead.
		//
		// This check is deliberately scoped to the manifest-less form. When the
		// client does name its parts, that list is the contract and it is
		// validated below -- an unrelated part still uploading is then the
		// client's business, not a reason to fail a well-specified request.
		if len(expected) == 0 {
			var inFlight int
			if err := tx.QueryRow(ctx, `
				SELECT COUNT(*) FROM multipart_parts
				WHERE upload_id = $1 AND state = 'UPLOADING'`, uploadID).Scan(&inFlight); err != nil {
				return fmt.Errorf("count in-flight parts: %w", err)
			}
			if inFlight > 0 {
				return fmt.Errorf(
					"upload %s still has %d part(s) uploading; wait for them or name the parts you want: %w",
					uploadID, inFlight, ErrConflict)
			}
		}
		if len(parts) == 0 {
			return fmt.Errorf("upload %s has no completed parts: %w", uploadID, ErrConflict)
		}

		// If the client sent a manifest, it must match exactly. This is what
		// catches "I thought part 3 uploaded but it didn't".
		if len(expected) > 0 {
			if len(expected) != len(parts) {
				return fmt.Errorf("upload %s: client listed %d parts, %d are complete: %w",
					uploadID, len(expected), len(parts), ErrConflict)
			}
			for i, want := range expected {
				got := parts[i]
				if want.PartNumber != got.number {
					return fmt.Errorf("upload %s: expected part %d at position %d, found %d: %w",
						uploadID, want.PartNumber, i, got.number, ErrConflict)
				}
				if want.ETag != "" && strings.Trim(want.ETag, `"`) != got.etag {
					return fmt.Errorf("upload %s: part %d etag mismatch: %w",
						uploadID, want.PartNumber, ErrConflict)
				}
			}
		}

		var totalSize int64
		var totalChunks int32
		etagInput := make([]string, 0, len(parts))
		for _, p := range parts {
			totalSize += p.size
			totalChunks += p.chunks
			etagInput = append(etagInput, p.etag)
		}

		objectID := uuid.New()
		var version int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MAX(version), 0) + 1 FROM objects WHERE bucket = $1 AND key = $2`,
			bucket, key).Scan(&version); err != nil {
			return fmt.Errorf("compute version: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO objects (id, bucket, key, version, state, chunk_size, content_type)
			VALUES ($1, $2, $3, $4, 'UPLOADING', $5, $6)`,
			objectID, bucket, key, version, chunkSize, contentType); err != nil {
			return fmt.Errorf("create assembled object: %w", err)
		}

		// Re-parent and renumber in one statement. ROW_NUMBER over
		// (part_number, chunk_index) produces the object's global ordering.
		tag, err := tx.Exec(ctx, `
			UPDATE chunks c
			SET object_id = $1, part_id = NULL, chunk_index = renum.new_index
			FROM (
				SELECT c2.id,
				       (ROW_NUMBER() OVER (ORDER BY p.part_number, c2.chunk_index) - 1)::int AS new_index
				FROM chunks c2
				JOIN multipart_parts p ON p.id = c2.part_id
				WHERE p.upload_id = $2 AND p.state = 'COMPLETE' AND c2.state = 'COMMITTED'
			) AS renum
			WHERE c.id = renum.id`, objectID, uploadID)
		if err != nil {
			return fmt.Errorf("reparent part chunks: %w", err)
		}
		if tag.RowsAffected() != int64(totalChunks) {
			return fmt.Errorf("upload %s: reparented %d chunks, parts declare %d: %w",
				uploadID, tag.RowsAffected(), totalChunks, ErrConflict)
		}

		etag := MultipartETag(etagInput)

		if err := supersedeCurrentVersion(ctx, tx, bucket, key, objectID); err != nil {
			return err
		}

		orows, err := tx.Query(ctx, `
			UPDATE objects SET state = 'COMPLETE', size_bytes = $2, chunk_count = $3,
				etag = $4, completed_at = now(), updated_at = now()
			WHERE id = $1
			RETURNING `+objectColumns, objectID, totalSize, totalChunks, etag)
		if err != nil {
			return fmt.Errorf("finalise assembled object: %w", err)
		}
		defer orows.Close()
		if !orows.Next() {
			if err := orows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("finalise assembled object returned no row")
		}
		obj, err = scanObject(orows)
		if err != nil {
			return err
		}
		orows.Close()

		// Any chunks still attached to superseded/failed parts are garbage.
		if err := enqueuePartChunkDeletions(ctx, tx, uploadID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE chunks SET state = 'ORPHANED'
			WHERE part_id IN (SELECT id FROM multipart_parts WHERE upload_id = $1)`, uploadID); err != nil {
			return fmt.Errorf("orphan leftover part chunks: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE multipart_uploads SET state = 'COMPLETED', object_id = $2,
				completed_at = now(), updated_at = now()
			WHERE id = $1`, uploadID, objectID); err != nil {
			return fmt.Errorf("mark upload completed: %w", err)
		}
		return nil
	})
	return obj, err
}

// AbortMultipartUpload cancels a session and queues every uploaded part's
// chunks for reclamation.
func (s *Store) AbortMultipartUpload(ctx context.Context, uploadID uuid.UUID) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE multipart_uploads SET state = 'ABORTED', updated_at = now()
			WHERE id = $1 AND state IN ('IN_PROGRESS', 'COMPLETING')`, uploadID)
		if err != nil {
			return fmt.Errorf("abort upload %s: %w", uploadID, err)
		}
		if tag.RowsAffected() == 0 {
			var exists bool
			if err := tx.QueryRow(ctx,
				`SELECT EXISTS(SELECT 1 FROM multipart_uploads WHERE id = $1)`, uploadID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("upload %s: %w", uploadID, ErrNotFound)
			}
			return nil // already aborted or completed: idempotent
		}
		if _, err := tx.Exec(ctx, `
			UPDATE multipart_parts SET state = 'FAILED', updated_at = now()
			WHERE upload_id = $1 AND state <> 'FAILED'`, uploadID); err != nil {
			return fmt.Errorf("fail parts: %w", err)
		}
		if err := enqueuePartChunkDeletions(ctx, tx, uploadID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE chunks SET state = 'ORPHANED'
			WHERE part_id IN (SELECT id FROM multipart_parts WHERE upload_id = $1)`, uploadID); err != nil {
			return fmt.Errorf("orphan part chunks: %w", err)
		}
		return nil
	})
}

// ReapStaleMultipartUploads aborts sessions that have gone quiet.
func (s *Store) ReapStaleMultipartUploads(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM multipart_uploads
		WHERE state = 'IN_PROGRESS' AND updated_at < now() - $1::interval
		ORDER BY updated_at LIMIT $2`, olderThan.String(), limit)
	if err != nil {
		return 0, fmt.Errorf("scan stale multipart uploads: %w", err)
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	n := 0
	for _, id := range ids {
		if err := s.AbortMultipartUpload(ctx, id); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func orphanPartChunks(ctx context.Context, tx pgx.Tx, partID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO chunk_deletions (chunk_id, node_id)
		SELECT r.chunk_id, r.node_id FROM chunk_replicas r
		JOIN chunks c ON c.id = r.chunk_id
		WHERE c.part_id = $1 AND r.state <> 'DELETING'
		ON CONFLICT (chunk_id, node_id) DO NOTHING`, partID); err != nil {
		return fmt.Errorf("enqueue part deletions: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE chunk_replicas SET state = 'DELETING', updated_at = now()
		WHERE chunk_id IN (SELECT id FROM chunks WHERE part_id = $1)`, partID); err != nil {
		return fmt.Errorf("mark part replicas deleting: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE chunks SET state = 'ORPHANED' WHERE part_id = $1`, partID); err != nil {
		return fmt.Errorf("orphan part chunks: %w", err)
	}
	return nil
}

// CompletedPartRef is a client-supplied part assertion.
type CompletedPartRef struct {
	PartNumber int32
	ETag       string
}

// MultipartETag derives a multipart object's ETag from its part ETags.
//
// It follows S3's *shape* -- "<digest>-<partCount>" -- so clients can tell a
// multipart object from a single-shot one, but the digest is SHA-256 rather
// than MD5. FlexStore does not claim ETag compatibility with S3.
func MultipartETag(partETags []string) string {
	h := sha256.New()
	for _, e := range partETags {
		h.Write([]byte(strings.Trim(e, `"`)))
	}
	return fmt.Sprintf("%s-%d", hex.EncodeToString(h.Sum(nil)), len(partETags))
}

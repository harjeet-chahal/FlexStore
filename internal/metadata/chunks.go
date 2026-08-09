package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ChunkOwner identifies what a chunk belongs to. Exactly one field is set.
type ChunkOwner struct {
	ObjectID *uuid.UUID
	PartID   *uuid.UUID
}

// AllocateChunk mints a stable chunk ID and pre-records one WRITING replica
// row per target node.
//
// Recording replicas *before* the write (rather than only on success) is what
// lets the GC reclaim bytes after a gateway crash: even if nobody ever calls
// CommitChunk, the coordinator still knows which nodes may be holding data for
// this chunk.
//
// The two ON CONFLICT variants exist because PostgreSQL can only target one
// partial unique index per statement, and object-owned vs part-owned chunks
// are uniquely indexed differently.
func (s *Store) AllocateChunk(ctx context.Context, owner ChunkOwner, index int32, sizeBytes int64, targetNodeIDs []string) (uuid.UUID, error) {
	if len(targetNodeIDs) == 0 {
		return uuid.Nil, fmt.Errorf("no placement targets supplied: %w", ErrConflict)
	}
	id, err := s.ReserveChunk(ctx, owner, index, sizeBytes)
	if err != nil {
		return uuid.Nil, err
	}
	if err := s.RecordChunkTargets(ctx, id, targetNodeIDs); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// ReserveChunk mints (or recovers) the stable chunk ID for one position in an
// owner's chunk sequence, without choosing where it goes.
//
// Splitting this out from placement is what lets the coordinator hash a chunk's
// real ID when the rendezvous strategy is configured: the ID has to exist
// before placement can be deterministic. It also makes retries stable -- a
// re-allocated index returns the *same* ID, so a retried chunk is placed on the
// same nodes rather than scattering half-written copies around the cluster.
func (s *Store) ReserveChunk(ctx context.Context, owner ChunkOwner, index int32, sizeBytes int64) (uuid.UUID, error) {
	if (owner.ObjectID == nil) == (owner.PartID == nil) {
		return uuid.Nil, fmt.Errorf("chunk must have exactly one owner: %w", ErrConflict)
	}

	// Re-allocating the same index is a supported retry path: the previous
	// PENDING row is reused rather than colliding with the unique index.
	const insertObjectChunk = `
		INSERT INTO chunks (id, object_id, chunk_index, size_bytes, state)
		VALUES ($1, $2, $3, $4, 'PENDING')
		ON CONFLICT (object_id, chunk_index) WHERE object_id IS NOT NULL
		DO UPDATE SET size_bytes = EXCLUDED.size_bytes, state = 'PENDING', committed_at = NULL
		RETURNING id`
	const insertPartChunk = `
		INSERT INTO chunks (id, part_id, chunk_index, size_bytes, state)
		VALUES ($1, $2, $3, $4, 'PENDING')
		ON CONFLICT (part_id, chunk_index) WHERE part_id IS NOT NULL
		DO UPDATE SET size_bytes = EXCLUDED.size_bytes, state = 'PENDING', committed_at = NULL
		RETURNING id`

	stmt, ownerID := insertObjectChunk, owner.ObjectID
	if owner.PartID != nil {
		stmt, ownerID = insertPartChunk, owner.PartID
	}

	id := uuid.New()
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := assertOwnerWritable(ctx, tx, owner); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, stmt, id, *ownerID, index, sizeBytes).Scan(&id); err != nil {
			return fmt.Errorf("reserve chunk %d: %w", index, err)
		}
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

// RecordChunkTargets pre-records one WRITING replica row per placement target.
//
// Recording targets before the data is written is what lets the GC reclaim
// bytes after a gateway crash: even if CommitChunk is never called, the
// coordinator still knows which nodes may be holding data for this chunk.
func (s *Store) RecordChunkTargets(ctx context.Context, chunkID uuid.UUID, targetNodeIDs []string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		for _, nodeID := range targetNodeIDs {
			if _, err := tx.Exec(ctx, `
				INSERT INTO chunk_replicas (chunk_id, node_id, state)
				VALUES ($1, $2, 'WRITING')
				ON CONFLICT (chunk_id, node_id) DO UPDATE SET state = 'WRITING', updated_at = now()`,
				chunkID, nodeID); err != nil {
				return fmt.Errorf("record target replica %s: %w", nodeID, err)
			}
		}
		return nil
	})
}

// assertOwnerWritable refuses to allocate chunks against an owner that is no
// longer accepting writes, so a late gateway cannot resurrect an aborted
// upload.
func assertOwnerWritable(ctx context.Context, tx pgx.Tx, owner ChunkOwner) error {
	if owner.ObjectID != nil {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM objects WHERE id = $1`, *owner.ObjectID).Scan(&state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("object %s: %w", *owner.ObjectID, ErrNotFound)
			}
			return fmt.Errorf("check object state: %w", err)
		}
		if ObjectState(state) != ObjectUploading {
			return fmt.Errorf("object %s is %s, not accepting chunks: %w", *owner.ObjectID, state, ErrConflict)
		}
		return nil
	}

	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM multipart_parts WHERE id = $1`, *owner.PartID).Scan(&state); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("part %s: %w", *owner.PartID, ErrNotFound)
		}
		return fmt.Errorf("check part state: %w", err)
	}
	if PartState(state) != PartUploading {
		return fmt.Errorf("part %s is %s, not accepting chunks: %w", *owner.PartID, state, ErrConflict)
	}
	return nil
}

// CommitChunk records the outcome of a chunk write. It enforces the durability
// policy: if fewer than minReplicas nodes acknowledged, the chunk stays
// PENDING and the caller gets ErrDurabilityNotMet.
func (s *Store) CommitChunk(
	ctx context.Context,
	chunkID uuid.UUID,
	checksum string,
	sizeBytes int64,
	succeeded, failed []string,
	minReplicas int,
) (int, error) {
	var available int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM chunks WHERE id = $1 FOR UPDATE`, chunkID).Scan(&state); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("chunk %s: %w", chunkID, ErrNotFound)
			}
			return fmt.Errorf("lock chunk %s: %w", chunkID, err)
		}

		for _, nodeID := range failed {
			if _, err := tx.Exec(ctx, `
				UPDATE chunk_replicas SET state = 'UNAVAILABLE', updated_at = now()
				WHERE chunk_id = $1 AND node_id = $2`, chunkID, nodeID); err != nil {
				return fmt.Errorf("mark failed replica %s: %w", nodeID, err)
			}
		}
		for _, nodeID := range succeeded {
			if _, err := tx.Exec(ctx, `
				INSERT INTO chunk_replicas (chunk_id, node_id, state, verified_at)
				VALUES ($1, $2, 'AVAILABLE', now())
				ON CONFLICT (chunk_id, node_id)
				DO UPDATE SET state = 'AVAILABLE', updated_at = now(), verified_at = now()`,
				chunkID, nodeID); err != nil {
				return fmt.Errorf("mark available replica %s: %w", nodeID, err)
			}
		}

		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM chunk_replicas WHERE chunk_id = $1 AND state = 'AVAILABLE'`,
			chunkID).Scan(&available); err != nil {
			return fmt.Errorf("count available replicas: %w", err)
		}

		if available < minReplicas {
			return fmt.Errorf(
				"chunk %s has %d available replicas, policy requires %d: %w",
				chunkID, available, minReplicas, ErrDurabilityNotMet)
		}

		if _, err := tx.Exec(ctx, `
			UPDATE chunks SET state = 'COMMITTED', checksum_sha256 = $2, size_bytes = $3, committed_at = now()
			WHERE id = $1`, chunkID, checksum, sizeBytes); err != nil {
			return fmt.Errorf("commit chunk %s: %w", chunkID, err)
		}

		// A committed chunk is proof the upload is making progress, so it also
		// counts as a heartbeat for the owning session.
		//
		// Without this, the stale-upload reaper reclaims by objects.updated_at,
		// which is only ever set at BeginUpload. Any single-shot upload that
		// takes longer than FLEXSTORE_UPLOAD_STALE_AFTER (30 minutes) is
		// therefore failed *while it is still streaming successfully*, and the
		// client sees a 502 from the next CommitChunk with no indication that
		// the coordinator killed its own upload. A 1 GiB object is nowhere near
		// that, but a slow client on a thin link is exactly the case that
		// breaks -- and it is the case least able to afford restarting.
		//
		// Doing it here rather than in a separate keepalive RPC means progress
		// and liveness are the same fact recorded once, in the transaction that
		// already proves it, so they cannot disagree.
		if _, err := tx.Exec(ctx, `
			UPDATE objects SET updated_at = now()
			WHERE id = (SELECT object_id FROM chunks WHERE id = $1)
			  AND state = 'UPLOADING'`, chunkID); err != nil {
			return fmt.Errorf("refresh upload liveness for chunk %s: %w", chunkID, err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE multipart_uploads u SET updated_at = now()
			FROM multipart_parts p
			WHERE p.id = (SELECT part_id FROM chunks WHERE id = $1)
			  AND u.id = p.upload_id
			  AND u.state = 'IN_PROGRESS'`, chunkID); err != nil {
			return fmt.Errorf("refresh multipart liveness for chunk %s: %w", chunkID, err)
		}
		return nil
	})
	return available, err
}

// ObjectKeyForChunk resolves which object owns a chunk. Used to invalidate the
// right cache entry when a replica is demoted out from under a cached layout.
func (s *Store) ObjectKeyForChunk(ctx context.Context, chunkID uuid.UUID) (bucket, key string, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT o.bucket, o.key
		FROM chunks c
		JOIN objects o ON o.id = c.object_id
		WHERE c.id = $1`, chunkID).Scan(&bucket, &key)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("chunk %s has no object owner: %w", chunkID, ErrNotFound)
	}
	if err != nil {
		return "", "", fmt.Errorf("resolve chunk owner: %w", err)
	}
	return bucket, key, nil
}

// ChunksForObject returns an object's chunks in index order. When
// availableOnly is set, only AVAILABLE replicas on non-DEAD nodes are
// attached, which is exactly the set the gateway may try to read from.
func (s *Store) ChunksForObject(ctx context.Context, objectID uuid.UUID, availableOnly bool) ([]ChunkWithReplicas, error) {
	return s.chunksWhere(ctx, `c.object_id = $1 AND c.state = 'COMMITTED'`, availableOnly, objectID)
}

// ChunksForPart returns a multipart part's chunks in index order.
func (s *Store) ChunksForPart(ctx context.Context, partID uuid.UUID, availableOnly bool) ([]ChunkWithReplicas, error) {
	return s.chunksWhere(ctx, `c.part_id = $1 AND c.state = 'COMMITTED'`, availableOnly, partID)
}

func (s *Store) chunksWhere(ctx context.Context, where string, availableOnly bool, arg any) ([]ChunkWithReplicas, error) {
	replicaFilter := ""
	if availableOnly {
		replicaFilter = ` AND r.state = 'AVAILABLE' AND n.health <> 'DEAD'`
	}

	rows, err := s.pool.Query(ctx, `
		SELECT c.id, c.chunk_index, c.size_bytes, c.checksum_sha256, c.state, c.committed_at,
		       r.node_id, n.grpc_address, r.state, n.health, r.updated_at
		FROM chunks c
		LEFT JOIN chunk_replicas r ON r.chunk_id = c.id
		LEFT JOIN storage_nodes n ON n.id = r.node_id
		WHERE `+where+replicaFilter+`
		ORDER BY c.chunk_index, r.node_id`, arg)
	if err != nil {
		return nil, fmt.Errorf("load chunks: %w", err)
	}
	defer rows.Close()

	var (
		out   []ChunkWithReplicas
		curID uuid.UUID
		first = true
	)
	for rows.Next() {
		var (
			c            Chunk
			chunkState   string
			committedAt  *time.Time
			checksum     *string
			nodeID       *string
			address      *string
			replicaState *string
			nodeHealth   *string
			updatedAt    *time.Time
		)
		if err := rows.Scan(&c.ID, &c.Index, &c.SizeBytes, &checksum, &chunkState, &committedAt,
			&nodeID, &address, &replicaState, &nodeHealth, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan chunk row: %w", err)
		}
		c.Checksum = nullString(checksum)
		c.State = ChunkState(chunkState)
		c.CommittedAt = nullTime(committedAt)

		if first || c.ID != curID {
			out = append(out, ChunkWithReplicas{Chunk: c})
			curID = c.ID
			first = false
		}
		if nodeID != nil {
			cur := &out[len(out)-1]
			cur.Replicas = append(cur.Replicas, Replica{
				ChunkID:    c.ID,
				NodeID:     *nodeID,
				Address:    nullString(address),
				NodeHealth: nullString(nodeHealth),
				State:      ReplicaState(nullString(replicaState)),
				UpdatedAt:  nullTime(updatedAt),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// orphanObjectChunks marks an object's chunks ORPHANED and queues every known
// replica for deletion. Called when an upload fails.
func orphanObjectChunks(ctx context.Context, tx pgx.Tx, objectID uuid.UUID) error {
	if _, err := tx.Exec(ctx,
		`UPDATE chunks SET state = 'ORPHANED' WHERE object_id = $1`, objectID); err != nil {
		return fmt.Errorf("orphan chunks for %s: %w", objectID, err)
	}
	return enqueueObjectChunkDeletions(ctx, tx, objectID)
}

// enqueueObjectChunkDeletions pushes one deletion job per (chunk, node).
//
// The queue is durable rather than in-memory precisely so a node that is down
// at delete time still gets its bytes reclaimed when it comes back.
func enqueueObjectChunkDeletions(ctx context.Context, tx pgx.Tx, objectID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO chunk_deletions (chunk_id, node_id)
		SELECT r.chunk_id, r.node_id
		FROM chunk_replicas r
		JOIN chunks c ON c.id = r.chunk_id
		WHERE c.object_id = $1 AND r.state <> 'DELETING'
		ON CONFLICT (chunk_id, node_id) DO NOTHING`, objectID); err != nil {
		return fmt.Errorf("enqueue deletions for %s: %w", objectID, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE chunk_replicas SET state = 'DELETING', updated_at = now()
		WHERE chunk_id IN (SELECT id FROM chunks WHERE object_id = $1)`, objectID); err != nil {
		return fmt.Errorf("mark replicas deleting for %s: %w", objectID, err)
	}
	return nil
}

// enqueuePartChunkDeletions is the multipart-part equivalent.
func enqueuePartChunkDeletions(ctx context.Context, tx pgx.Tx, uploadID uuid.UUID) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO chunk_deletions (chunk_id, node_id)
		SELECT r.chunk_id, r.node_id
		FROM chunk_replicas r
		JOIN chunks c ON c.id = r.chunk_id
		JOIN multipart_parts p ON p.id = c.part_id
		WHERE p.upload_id = $1 AND r.state <> 'DELETING'
		ON CONFLICT (chunk_id, node_id) DO NOTHING`, uploadID); err != nil {
		return fmt.Errorf("enqueue part deletions for %s: %w", uploadID, err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE chunk_replicas SET state = 'DELETING', updated_at = now()
		WHERE chunk_id IN (
			SELECT c.id FROM chunks c
			JOIN multipart_parts p ON p.id = c.part_id
			WHERE p.upload_id = $1)`, uploadID); err != nil {
		return fmt.Errorf("mark part replicas deleting for %s: %w", uploadID, err)
	}
	return nil
}

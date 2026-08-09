package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ClaimDeletions leases a batch of pending deletion jobs, pushing their
// next_attempt_at forward so a concurrent coordinator (or the next tick) does
// not pick up the same work. This is a lease, not a lock: if the process dies
// mid-batch the jobs simply become due again.
func (s *Store) ClaimDeletions(ctx context.Context, limit int, lease time.Duration) ([]PendingDeletion, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE chunk_deletions d
		SET attempts = d.attempts + 1, next_attempt_at = now() + $2::interval
		WHERE d.id IN (
			SELECT id FROM chunk_deletions
			WHERE next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING d.id, d.chunk_id, d.node_id, d.attempts,
		          COALESCE((SELECT grpc_address FROM storage_nodes WHERE id = d.node_id), '')`,
		limit, lease.String())
	if err != nil {
		return nil, fmt.Errorf("claim deletions: %w", err)
	}
	defer rows.Close()

	var out []PendingDeletion
	for rows.Next() {
		var p PendingDeletion
		if err := rows.Scan(&p.ID, &p.ChunkID, &p.NodeID, &p.Attempts, &p.Address); err != nil {
			return nil, fmt.Errorf("scan deletion: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// FinishDeletion removes a completed deletion job and its replica row.
func (s *Store) FinishDeletion(ctx context.Context, id int64, chunkID uuid.UUID, nodeID string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`DELETE FROM chunk_replicas WHERE chunk_id = $1 AND node_id = $2`, chunkID, nodeID); err != nil {
			return fmt.Errorf("drop replica row: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM chunk_deletions WHERE id = $1`, id); err != nil {
			return fmt.Errorf("drop deletion job: %w", err)
		}
		return nil
	})
}

// FailDeletion records a failed attempt and backs the job off.
//
// After maxAttempts the job is given up on: at that point the node has been
// unreachable or erroring for a long time, and retrying forever would mean the
// owning object can never be reclaimed. The consequence -- a potentially
// orphaned file on that node -- is bounded, and reconciliation finds it when
// the node comes back.
//
// Giving up has to drop the replica row as well as the queue row. Dropping
// only the queue row looks like it releases the object, but
// PurgeReclaimedChunks requires that no chunk_replicas row survives before it
// will remove a chunk, and no chunk may survive before it will remove the
// object. A DELETING replica row left behind therefore pins the chunk row and
// the object row in PostgreSQL forever -- the exact outcome this branch was
// written to avoid. Both rows go in one transaction.
func (s *Store) FailDeletion(ctx context.Context, id int64, cause string, backoff time.Duration, maxAttempts int) error {
	var expired bool
	if err := s.inTx(ctx, func(tx pgx.Tx) error {
		var chunkID uuid.UUID
		var nodeID string
		err := tx.QueryRow(ctx, `
			DELETE FROM chunk_deletions WHERE id = $1 AND attempts >= $2
			RETURNING chunk_id, node_id`, id, maxAttempts).Scan(&chunkID, &nodeID)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("expire deletion job: %w", err)
		}
		expired = true
		if _, err := tx.Exec(ctx, `
			DELETE FROM chunk_replicas
			WHERE chunk_id = $1 AND node_id = $2 AND state = 'DELETING'`, chunkID, nodeID); err != nil {
			return fmt.Errorf("drop abandoned replica row: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if expired {
		return nil
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE chunk_deletions SET last_error = $2, next_attempt_at = now() + $3::interval
		WHERE id = $1`, id, truncate(cause, 500), backoff.String()); err != nil {
		return fmt.Errorf("record deletion failure: %w", err)
	}
	return nil
}

// PurgeReclaimedChunks removes chunk rows whose replicas are all gone and
// whose deletion queue is empty, then removes objects that have no chunks
// left. Running it after the deletion pass is what actually shrinks the
// metadata, and doing it in that order guarantees we never drop the row that
// told us where the bytes were.
func (s *Store) PurgeReclaimedChunks(ctx context.Context, limit int) (chunksPurged, objectsPurged int64, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			DELETE FROM chunks c
			WHERE c.id IN (
				SELECT id FROM chunks
				WHERE state = 'ORPHANED'
				  AND NOT EXISTS (SELECT 1 FROM chunk_replicas r WHERE r.chunk_id = chunks.id)
				  AND NOT EXISTS (SELECT 1 FROM chunk_deletions d WHERE d.chunk_id = chunks.id)
				LIMIT $1)`, limit)
		if err != nil {
			return fmt.Errorf("purge orphaned chunks: %w", err)
		}
		chunksPurged = tag.RowsAffected()

		// Objects in DELETING/FAILED whose chunks are fully reclaimed. The
		// cascade takes any remaining chunk rows with them.
		tag, err = tx.Exec(ctx, `
			DELETE FROM objects o
			WHERE o.id IN (
				SELECT id FROM objects
				WHERE state IN ('DELETING', 'FAILED')
				  AND NOT EXISTS (
					SELECT 1 FROM chunks c
					JOIN chunk_replicas r ON r.chunk_id = c.id
					WHERE c.object_id = objects.id)
				  AND NOT EXISTS (
					SELECT 1 FROM chunks c
					JOIN chunk_deletions d ON d.chunk_id = c.id
					WHERE c.object_id = objects.id)
				LIMIT $1)`, limit)
		if err != nil {
			return fmt.Errorf("purge deleted objects: %w", err)
		}
		objectsPurged = tag.RowsAffected()
		return nil
	})
	return chunksPurged, objectsPurged, err
}

// PurgeCompletedMultipartUploads removes finished sessions once their parts
// carry no chunks. Kept separate from chunk GC so a stuck deletion cannot
// block session cleanup indefinitely.
func (s *Store) PurgeCompletedMultipartUploads(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM multipart_uploads u
		WHERE u.id IN (
			SELECT id FROM multipart_uploads
			WHERE state IN ('COMPLETED', 'ABORTED')
			  AND updated_at < now() - $1::interval
			  AND NOT EXISTS (
				SELECT 1 FROM multipart_parts p
				JOIN chunks c ON c.part_id = p.id
				WHERE p.upload_id = multipart_uploads.id)
			LIMIT $2)`, olderThan.String(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge multipart uploads: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ClusterStats computes durability and inventory counters for /metrics and the
// GetClusterStatus RPC.
func (s *Store) ClusterStats(ctx context.Context, replicationFactor int) (ClusterStats, error) {
	var st ClusterStats
	st.ObjectsByState = map[ObjectState]int64{}

	// Damage counts come from the partial index on (available_replicas) WHERE
	// state = 'COMMITTED', so they cost time proportional to the number of
	// damaged chunks, not to the size of the dataset. The total is a separate,
	// slower query -- see DurabilityCounts and CommittedChunkCount.
	under, unavailable, err := s.DurabilityCounts(ctx, replicationFactor)
	if err != nil {
		return st, err
	}
	st.UnderReplicatedChunks = under
	st.UnavailableChunks = unavailable

	total, err := s.CommittedChunkCount(ctx)
	if err != nil {
		return st, err
	}
	st.TotalChunks = total

	jobs, err := s.RepairCounts(ctx)
	if err != nil {
		return st, err
	}
	st.RepairJobsByState = jobs

	rows, err := s.pool.Query(ctx, `SELECT state, COUNT(*) FROM objects GROUP BY state`)
	if err != nil {
		return st, fmt.Errorf("count objects by state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return st, err
		}
		st.ObjectsByState[ObjectState(state)] = n
	}
	return st, rows.Err()
}

// ReplicaCountsForObject returns per-chunk available replica counts. Used by
// the admin layout endpoint and by integration tests asserting that replicas
// really do land on distinct nodes.
func (s *Store) ReplicaCountsForObject(ctx context.Context, objectID uuid.UUID) (map[uuid.UUID]int, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, COUNT(r.node_id) FILTER (WHERE r.state = 'AVAILABLE')
		FROM chunks c
		LEFT JOIN chunk_replicas r ON r.chunk_id = c.id
		WHERE c.object_id = $1
		GROUP BY c.id`, objectID)
	if err != nil {
		return nil, fmt.Errorf("count replicas for object %s: %w", objectID, err)
	}
	defer rows.Close()

	out := map[uuid.UUID]int{}
	for rows.Next() {
		var id uuid.UUID
		var n int
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

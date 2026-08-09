package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// ErrNoReconcileWork is returned when the reconciliation queue is empty.
var ErrNoReconcileWork = errors.New("no reconciliation work available")

// MarkReplicasStaleOnNode demotes a returning node's replicas to STALE.
//
// This is the "a returning node's files are not authoritative" rule. Previously
// a node that came back had its replicas restored straight to AVAILABLE, which
// trusts a disk we stopped observing: the files could have been rolled back by
// a snapshot restore, truncated by a bad shutdown, or belong to objects that
// were deleted meanwhile. STALE replicas do not count towards durability, so
// the cluster keeps repairing as if they were gone until the reconciler has
// actually verified each one against the recorded checksum.
// Only UNAVAILABLE replicas are demoted. A WRITING replica belongs to an upload
// that is in flight *right now*; it is not a stale copy from a previous life,
// and CommitChunk will resolve it in moments. Demoting it would hand an
// in-flight write to the reconciler, which would then see a chunk it has no
// record of on the node and delete the bytes the client is still uploading.
func (s *Store) MarkReplicasStaleOnNode(ctx context.Context, nodeID string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE chunk_replicas SET state = 'STALE', updated_at = now()
		WHERE node_id = $1 AND state = 'UNAVAILABLE'`, nodeID)
	if err != nil {
		return 0, fmt.Errorf("mark replicas stale on %s: %w", nodeID, err)
	}
	return tag.RowsAffected(), nil
}

// EnqueueReconcile schedules an inventory comparison for a node. Idempotent via
// the partial unique index on (node_id) WHERE state IN (PENDING, RUNNING).
func (s *Store) EnqueueReconcile(ctx context.Context, nodeID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO node_reconciliations (node_id, state) VALUES ($1, 'PENDING')
		ON CONFLICT (node_id) WHERE state IN ('PENDING', 'RUNNING') DO NOTHING`, nodeID)
	if err != nil {
		return false, fmt.Errorf("enqueue reconcile for %s: %w", nodeID, err)
	}
	return tag.RowsAffected() > 0, nil
}

// NodesWithStaleReplicas returns HEALTHY nodes that still hold unverified
// replicas.
//
// This is the self-correcting path for STALE. Enqueue-on-rejoin alone is not
// enough: a reconciliation can FAIL (the node was still starting when its
// inventory was requested), or be dropped by the idempotence check (a node that
// flaps while one is already RUNNING), and nothing would ever retry it. The
// replicas would then stay STALE forever -- not counted for durability, and
// worse, occupying the node so repair cannot use it as a destination either.
//
// Sweeping on the actual condition rather than on the event that caused it
// means STALE resolves regardless of how it arose.
func (s *Store) NodesWithStaleReplicas(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT r.node_id
		FROM chunk_replicas r
		JOIN storage_nodes n ON n.id = r.node_id
		WHERE r.state = 'STALE' AND n.health = 'HEALTHY'
		ORDER BY r.node_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("find nodes with stale replicas: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ReclaimExpiredReconcileLeases returns abandoned reconciliations to PENDING.
func (s *Store) ReclaimExpiredReconcileLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE node_reconciliations
		SET state = 'PENDING', owner = NULL, lease_expires_at = NULL, updated_at = now()
		WHERE state = 'RUNNING' AND lease_expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("reclaim reconcile leases: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ClaimReconcileJob leases one pending reconciliation.
//
// Only nodes the coordinator currently considers HEALTHY are claimed: there is
// no point streaming an inventory from a node that is not answering, and doing
// so would tie up a worker for the full RPC deadline.
func (s *Store) ClaimReconcileJob(ctx context.Context, owner string, lease time.Duration) (ReconcileJob, error) {
	var job ReconcileJob
	err := s.pool.QueryRow(ctx, `
		UPDATE node_reconciliations r
		SET state = 'RUNNING', owner = $1, lease_expires_at = now() + $2::interval, updated_at = now()
		WHERE r.id = (
			SELECT rr.id FROM node_reconciliations rr
			JOIN storage_nodes n ON n.id = rr.node_id
			WHERE rr.state = 'PENDING' AND n.health = 'HEALTHY'
			ORDER BY rr.created_at, rr.id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING r.id, r.node_id, r.state, r.created_at,
		          (SELECT grpc_address FROM storage_nodes WHERE id = r.node_id)`,
		owner, lease.String()).
		Scan(&job.ID, &job.NodeID, &job.State, &job.CreatedAt, &job.Address)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconcileJob{}, ErrNoReconcileWork
	}
	if err != nil {
		return ReconcileJob{}, fmt.Errorf("claim reconcile job: %w", err)
	}
	return job, nil
}

// CompleteReconcile closes a reconciliation with its findings.
func (s *Store) CompleteReconcile(ctx context.Context, jobID int64, res ReconcileResult) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE node_reconciliations
		SET state = 'SUCCEEDED', owner = NULL, lease_expires_at = NULL,
		    chunks_seen = $2, orphans_found = $3, phantoms_found = $4,
		    verified = $5, corrupt_found = $6,
		    last_error = NULL, finished_at = now(), updated_at = now()
		WHERE id = $1`,
		jobID, res.ChunksSeen, res.OrphansFound, res.PhantomsFound, res.Verified, res.CorruptFound)
	if err != nil {
		return fmt.Errorf("complete reconcile job: %w", err)
	}
	return nil
}

// FailReconcile records a failed reconciliation.
func (s *Store) FailReconcile(ctx context.Context, jobID int64, cause string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE node_reconciliations
		SET state = 'FAILED', owner = NULL, lease_expires_at = NULL,
		    last_error = $2, finished_at = now(), updated_at = now()
		WHERE id = $1`, jobID, truncate(cause, 500))
	if err != nil {
		return fmt.Errorf("fail reconcile job: %w", err)
	}
	return nil
}

// StaleReplicaOnNode is one replica awaiting verification on a returning node.
type StaleReplicaOnNode struct {
	ChunkID  uuid.UUID
	Checksum string
	Size     int64
}

// StaleReplicasOnNode lists replicas the reconciler must verify. Paged by chunk
// ID so a node holding millions of chunks is processed incrementally rather
// than materialising the whole set in memory.
func (s *Store) StaleReplicasOnNode(ctx context.Context, nodeID string, after uuid.UUID, limit int) ([]StaleReplicaOnNode, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id, COALESCE(c.checksum_sha256, ''), c.size_bytes
		FROM chunk_replicas r
		JOIN chunks c ON c.id = r.chunk_id
		WHERE r.node_id = $1 AND r.state = 'STALE' AND c.id > $2
		ORDER BY c.id
		LIMIT $3`, nodeID, after, limit)
	if err != nil {
		return nil, fmt.Errorf("list stale replicas on %s: %w", nodeID, err)
	}
	defer rows.Close()

	var out []StaleReplicaOnNode
	for rows.Next() {
		var r StaleReplicaOnNode
		if err := rows.Scan(&r.ChunkID, &r.Checksum, &r.Size); err != nil {
			return nil, fmt.Errorf("scan stale replica: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PromoteVerifiedReplica marks a STALE replica AVAILABLE after the reconciler
// confirmed the node holds it with a matching checksum. Only STALE rows are
// promoted, so this can never resurrect a replica that was deliberately
// demoted for another reason.
func (s *Store) PromoteVerifiedReplica(ctx context.Context, chunkID uuid.UUID, nodeID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE chunk_replicas
		SET state = 'AVAILABLE', updated_at = now(), verified_at = now(), last_error = NULL
		WHERE chunk_id = $1 AND node_id = $2 AND state = 'STALE'`, chunkID, nodeID)
	if err != nil {
		return false, fmt.Errorf("promote verified replica: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ChunkIDsKnownOnNode returns the chunk IDs metadata associates with a node, in
// any replica state. Used to classify a node's actual inventory: anything the
// node holds that is not in this set is an orphan.
func (s *Store) ChunkIDsKnownOnNode(ctx context.Context, nodeID string) (map[uuid.UUID]ReplicaState, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT chunk_id, state FROM chunk_replicas WHERE node_id = $1`, nodeID)
	if err != nil {
		return nil, fmt.Errorf("list replicas on %s: %w", nodeID, err)
	}
	defer rows.Close()

	out := map[uuid.UUID]ReplicaState{}
	for rows.Next() {
		var id uuid.UUID
		var state string
		if err := rows.Scan(&id, &state); err != nil {
			return nil, err
		}
		out[id] = ReplicaState(state)
	}
	return out, rows.Err()
}

// QueueOrphanDeletion schedules removal of a file the node holds that metadata
// does not account for. It reports whether the deletion was actually queued.
//
// chunk_deletions has no foreign key onto chunks precisely so this works: the
// chunk row is usually already gone (that is what makes the file an orphan).
//
// The two NOT EXISTS guards re-check the decision *at the moment of deletion*
// rather than trusting the inventory snapshot the reconciler took earlier. That
// snapshot can be seconds old, and an upload that landed in between would
// otherwise look exactly like an orphan -- the file is on disk, and the
// coordinator's stale snapshot has no replica row for it. Deleting a chunk the
// client is still uploading is the worst thing reconciliation could do, so the
// guards live in SQL where the race cannot be reintroduced by a caller:
//
//	guard 1  a replica row for (chunk, node) exists now -> not an orphan
//	guard 2  the chunk row exists and is PENDING        -> write in flight
func (s *Store) QueueOrphanDeletion(ctx context.Context, chunkID uuid.UUID, nodeID string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO chunk_deletions (chunk_id, node_id)
		SELECT $1, $2
		WHERE NOT EXISTS (
			SELECT 1 FROM chunk_replicas WHERE chunk_id = $1 AND node_id = $2
		)
		AND NOT EXISTS (
			SELECT 1 FROM chunks WHERE id = $1 AND state = 'PENDING'
		)
		ON CONFLICT (chunk_id, node_id) DO NOTHING`, chunkID, nodeID)
	if err != nil {
		return false, fmt.Errorf("queue orphan deletion: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// RecentReconciliations returns reconciliation history for the admin API.
func (s *Store) RecentReconciliations(ctx context.Context, limit int) ([]ReconcileSummary, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT node_id, state, chunks_seen, orphans_found, phantoms_found, verified,
		       corrupt_found, COALESCE(last_error, ''), created_at, finished_at
		FROM node_reconciliations
		ORDER BY updated_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list reconciliations: %w", err)
	}
	defer rows.Close()

	var out []ReconcileSummary
	for rows.Next() {
		var r ReconcileSummary
		var finished *time.Time
		if err := rows.Scan(&r.NodeID, &r.State, &r.Result.ChunksSeen, &r.Result.OrphansFound,
			&r.Result.PhantomsFound, &r.Result.Verified, &r.Result.CorruptFound,
			&r.LastError, &r.CreatedAt, &finished); err != nil {
			return nil, fmt.Errorf("scan reconciliation: %w", err)
		}
		r.FinishedAt = nullTime(finished)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ReconcileSummary is one row of reconciliation history.
type ReconcileSummary struct {
	NodeID     string
	State      ReconcileState
	Result     ReconcileResult
	LastError  string
	CreatedAt  time.Time
	FinishedAt time.Time
}

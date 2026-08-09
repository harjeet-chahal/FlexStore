package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const nodeColumns = `
	id, grpc_address, health, total_bytes, used_bytes, available_bytes,
	chunk_count, active_requests, last_heartbeat_at, registered_at`

// UpsertNode registers a node or refreshes an existing registration. A node
// restarting with the same ID keeps its identity (and therefore its recorded
// replicas), which is what makes a node restart a non-event for durability.
func (s *Store) UpsertNode(ctx context.Context, id, address, health string, stats NodeStats, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO storage_nodes (
			id, grpc_address, health, total_bytes, used_bytes, available_bytes,
			chunk_count, active_requests, last_heartbeat_at, registered_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now(), now())
		ON CONFLICT (id) DO UPDATE SET
			grpc_address      = EXCLUDED.grpc_address,
			health            = EXCLUDED.health,
			total_bytes       = EXCLUDED.total_bytes,
			used_bytes        = EXCLUDED.used_bytes,
			available_bytes   = EXCLUDED.available_bytes,
			chunk_count       = EXCLUDED.chunk_count,
			active_requests   = EXCLUDED.active_requests,
			last_heartbeat_at = EXCLUDED.last_heartbeat_at,
			updated_at        = now()`,
		id, address, health, stats.TotalBytes, stats.UsedBytes, stats.AvailableBytes,
		stats.ChunkCount, stats.ActiveRequests, at)
	if err != nil {
		return fmt.Errorf("upsert node %s: %w", id, err)
	}
	return nil
}

// RecordHeartbeat updates liveness and capacity for an already-registered
// node. It reports ErrNotFound when the node is unknown so the node can
// re-register instead of silently heartbeating into the void (which happens
// after a metadata reset).
func (s *Store) RecordHeartbeat(ctx context.Context, id, address string, stats NodeStats, health string, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE storage_nodes SET
			grpc_address      = $2,
			health            = $3,
			total_bytes       = $4,
			used_bytes        = $5,
			available_bytes   = $6,
			chunk_count       = $7,
			active_requests   = $8,
			last_heartbeat_at = $9,
			updated_at        = now()
		WHERE id = $1`,
		id, address, health, stats.TotalBytes, stats.UsedBytes, stats.AvailableBytes,
		stats.ChunkCount, stats.ActiveRequests, at)
	if err != nil {
		return fmt.Errorf("heartbeat node %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("node %s: %w", id, ErrNotFound)
	}
	return nil
}

// SetNodeHealth persists a health transition and keeps replica states
// consistent with it, in one transaction.
//
// Marking a node DEAD *always* demotes its AVAILABLE replicas here rather than
// in the caller. Durability is now derived from the trigger-maintained
// chunks.available_replicas counter, which does not join storage_nodes -- so if
// a node could be DEAD while its replicas still read AVAILABLE, the cluster
// would under-report damage and never schedule the repairs. Enforcing it at the
// store boundary makes that state unreachable through this API rather than
// merely unlikely.
//
// The reverse is deliberately NOT done: returning to HEALTHY does not restore
// replicas. A returning node's files are verified by the reconciler first (see
// MarkReplicasStaleOnNode).
func (s *Store) SetNodeHealth(ctx context.Context, id, health string) (replicasDemoted int64, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE storage_nodes SET health = $2, updated_at = now() WHERE id = $1`, id, health); err != nil {
			return fmt.Errorf("set health for %s: %w", id, err)
		}
		if health != "DEAD" {
			return nil
		}
		tag, err := tx.Exec(ctx, `
			UPDATE chunk_replicas SET state = 'UNAVAILABLE', updated_at = now()
			WHERE node_id = $1 AND state IN ('AVAILABLE', 'WRITING')`, id)
		if err != nil {
			return fmt.Errorf("demote replicas on dead node %s: %w", id, err)
		}
		replicasDemoted = tag.RowsAffected()
		return nil
	})
	return replicasDemoted, err
}

// MarkReplicasUnavailableOnNode flips a node's replicas to UNAVAILABLE so
// durability accounting reflects reality. It does not delete anything: the data
// may well still be on disk, and reconciliation will either re-verify it or the
// repair manager will re-replicate elsewhere.
//
// SetNodeHealth("DEAD") already does this transactionally; this remains for
// callers that need the demotion without a health change.
func (s *Store) MarkReplicasUnavailableOnNode(ctx context.Context, nodeID string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE chunk_replicas SET state = 'UNAVAILABLE', updated_at = now()
		WHERE node_id = $1 AND state IN ('AVAILABLE', 'WRITING')`, nodeID)
	if err != nil {
		return 0, fmt.Errorf("mark replicas unavailable on %s: %w", nodeID, err)
	}
	return tag.RowsAffected(), nil
}

// ListNodes returns every registered node, optionally filtered to HEALTHY.
func (s *Store) ListNodes(ctx context.Context, healthyOnly bool) ([]Node, error) {
	q := `SELECT ` + nodeColumns + ` FROM storage_nodes`
	if healthyOnly {
		q += ` WHERE health = 'HEALTHY'`
	}
	q += ` ORDER BY id`

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	defer rows.Close()

	var out []Node
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// GetNode returns one node by ID.
func (s *Store) GetNode(ctx context.Context, id string) (Node, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+nodeColumns+` FROM storage_nodes WHERE id = $1`, id)
	if err != nil {
		return Node{}, fmt.Errorf("get node %s: %w", id, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Node{}, err
		}
		return Node{}, fmt.Errorf("node %s: %w", id, ErrNotFound)
	}
	return scanNode(rows)
}

func scanNode(rows pgx.Rows) (Node, error) {
	var (
		n       Node
		lastHB  *time.Time
		regAt   time.Time
		address string
	)
	if err := rows.Scan(&n.ID, &address, &n.Health, &n.TotalBytes, &n.UsedBytes,
		&n.AvailableBytes, &n.ChunkCount, &n.ActiveRequests, &lastHB, &regAt); err != nil {
		return Node{}, fmt.Errorf("scan node: %w", err)
	}
	n.Address = address
	n.LastHeartbeatAt = nullTime(lastHB)
	n.RegisteredAt = regAt
	return n, nil
}

// ErrNoNodes is returned by placement helpers when the cluster is empty.
var ErrNoNodes = errors.New("no storage nodes registered")

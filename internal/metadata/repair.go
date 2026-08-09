package metadata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// liveOwnerPredicate restricts durability accounting and repair to chunks whose
// owner is still live. Chunks belonging to a DELETING/FAILED object, or to a
// superseded multipart part, are garbage awaiting the GC -- repairing them
// would burn bandwidth restoring data that is about to be deleted.
const liveOwnerPredicate = `
	(
	  EXISTS (
	    SELECT 1 FROM objects o
	    WHERE o.id = c.object_id AND o.state IN ('COMPLETE', 'UPLOADING')
	  )
	  OR EXISTS (
	    SELECT 1 FROM multipart_parts p
	    JOIN multipart_uploads u ON u.id = p.upload_id
	    WHERE p.id = c.part_id
	      AND p.state IN ('UPLOADING', 'COMPLETE')
	      AND u.state = 'IN_PROGRESS'
	  )
	)`

// ErrNoRepairWork is returned by ClaimRepairJob when the queue is empty.
var ErrNoRepairWork = errors.New("no repair work available")

// ErrNoSource means every replica of a chunk is unusable. The chunk's data is
// currently unreadable; this is reported, never papered over.
var ErrNoSource = errors.New("no healthy source replica")

// DurabilityCounts reports how many committed chunks are damaged.
//
// Both queries are driven by the chunks_durability_idx partial index on
// (available_replicas) WHERE state = 'COMMITTED', so their cost scales with the
// number of *damaged* chunks rather than with the size of the whole dataset.
// The liveOwnerPredicate is evaluated only for rows the index already selected.
func (s *Store) DurabilityCounts(ctx context.Context, replicationFactor int) (under, unavailable int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE c.available_replicas = 0)
		FROM chunks c
		WHERE c.state = 'COMMITTED'
		  AND c.available_replicas < $1
		  AND `+liveOwnerPredicate, replicationFactor).Scan(&under, &unavailable)
	if err != nil {
		return 0, 0, fmt.Errorf("count damaged chunks: %w", err)
	}
	return under, unavailable, nil
}

// CommittedChunkCount returns how many committed chunks have a live owner.
// Separate from DurabilityCounts because it is a full count that cannot use the
// damage index, so it runs on a slower cadence.
func (s *Store) CommittedChunkCount(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM chunks c
		WHERE c.state = 'COMMITTED' AND `+liveOwnerPredicate).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count committed chunks: %w", err)
	}
	return n, nil
}

// EnqueueRepairs scans for under-replicated chunks and creates PENDING jobs.
//
// Idempotent by construction: repair_jobs_active_idx is a unique index on
// chunk_id filtered to PENDING/RUNNING, so ON CONFLICT DO NOTHING makes
// re-enqueueing a chunk that is already being repaired a no-op. That is what
// lets the scanner run on a short timer without producing duplicate work.
func (s *Store) EnqueueRepairs(ctx context.Context, replicationFactor, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO repair_jobs (chunk_id, state)
		SELECT c.id, 'PENDING'
		FROM chunks c
		WHERE c.state = 'COMMITTED'
		  AND c.available_replicas < $1
		  AND c.available_replicas > 0
		  AND `+liveOwnerPredicate+`
		ORDER BY c.available_replicas ASC, c.id
		LIMIT $2
		ON CONFLICT (chunk_id) WHERE state IN ('PENDING', 'RUNNING') DO NOTHING`,
		replicationFactor, limit)
	if err != nil {
		return 0, fmt.Errorf("enqueue repairs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// TrimOverReplicatedChunks removes replicas above the replication factor.
//
// Over-replication is the natural consequence of recovery: a node dies, its
// chunks are re-replicated elsewhere, then it comes back and its verified
// copies are promoted -- leaving RF+1. Without trimming, every node failure
// permanently inflates storage for the affected chunks (measured at 33% in a
// 3-replica cluster), and the excess is never reclaimed.
//
// Safety is structural rather than procedural: victims are chosen by rank, not
// by count. The AVAILABLE replicas are numbered emptiest-node-first and only
// ranks beyond RF are demoted, so a chunk with RF or fewer replicas has no
// eligible rank and nothing happens. The excess therefore comes off the fullest
// nodes first -- trimming doubles as gentle rebalancing -- and the statement
// cannot leave fewer than RF replicas even if it races with a repair or if the
// denormalised durability counter has drifted. See the comment on the statement
// itself for why the arithmetic formulation was unsafe.
func (s *Store) TrimOverReplicatedChunks(ctx context.Context, replicationFactor, limit int) (int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT c.id FROM chunks c
		WHERE c.state = 'COMMITTED'
		  AND c.available_replicas > $1
		  AND `+liveOwnerPredicate+`
		ORDER BY c.available_replicas DESC, c.id
		LIMIT $2`, replicationFactor, limit)
	if err != nil {
		return 0, fmt.Errorf("find over-replicated chunks: %w", err)
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

	var trimmed int64
	for _, id := range ids {
		err := s.inTx(ctx, func(tx pgx.Tx) error {
			// Re-read under lock: another worker may have changed durability
			// between the scan above and this transaction.
			var available int
			if err := tx.QueryRow(ctx,
				`SELECT available_replicas FROM chunks WHERE id = $1 FOR UPDATE`, id).Scan(&available); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return nil
				}
				return err
			}
			if available-replicationFactor <= 0 {
				return nil
			}

			// The victims are chosen by RANK, not by count.
			//
			// The obvious formulation -- excess := available - RF, then
			// "LIMIT excess" over the AVAILABLE rows -- takes its count from
			// the denormalised chunks.available_replicas and its rows from
			// chunk_replicas. Those are two different sources of truth, and
			// they can disagree: that is exactly what
			// VerifyDurabilityCounters exists to detect. If the counter ever
			// reads high, LIMIT excess demotes every AVAILABLE replica the
			// chunk has, and a trim intended to remove one surplus copy
			// destroys the data instead.
			//
			// Ranking removes the possibility rather than guarding against it.
			// The rows are numbered cheapest-node-first and only ranks beyond
			// the replication factor are demoted, so the statement cannot
			// leave fewer than RF replicas no matter what any counter claims --
			// when there are RF or fewer rows, no rank exceeds RF and nothing
			// is demoted at all. Selection and mutation are one statement, so
			// there is also no window between choosing a victim and demoting
			// it.
			tag, err := tx.Exec(ctx, `
				WITH ranked AS (
					SELECT r.node_id,
					       ROW_NUMBER() OVER (ORDER BY n.used_bytes ASC, n.id) AS rn
					FROM chunk_replicas r
					JOIN storage_nodes n ON n.id = r.node_id
					WHERE r.chunk_id = $1 AND r.state = 'AVAILABLE'
				)
				UPDATE chunk_replicas r
				SET state = 'DELETING', updated_at = now()
				WHERE r.chunk_id = $1
				  AND r.node_id IN (SELECT node_id FROM ranked WHERE rn > $2)`,
				id, replicationFactor)
			if err != nil {
				return fmt.Errorf("demote excess replicas: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO chunk_deletions (chunk_id, node_id)
				SELECT chunk_id, node_id FROM chunk_replicas
				WHERE chunk_id = $1 AND state = 'DELETING'
				ON CONFLICT (chunk_id, node_id) DO NOTHING`, id); err != nil {
				return fmt.Errorf("queue excess replica removal: %w", err)
			}
			trimmed += tag.RowsAffected()
			return nil
		})
		if err != nil {
			return trimmed, err
		}
	}
	return trimmed, nil
}

// ReclaimExpiredRepairLeases returns RUNNING jobs whose lease has expired to
// PENDING. This is what makes a coordinator crash mid-repair recoverable: the
// job is not lost, it simply becomes claimable again once the lease lapses.
func (s *Store) ReclaimExpiredRepairLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE repair_jobs
		SET state = 'PENDING', owner = NULL, lease_expires_at = NULL,
		    next_attempt_at = now(), updated_at = now(),
		    last_error = COALESCE(last_error, 'lease expired; reclaimed after coordinator restart')
		WHERE state = 'RUNNING' AND lease_expires_at < now()`)
	if err != nil {
		return 0, fmt.Errorf("reclaim repair leases: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ClaimRepairJob leases one due job and loads everything needed to run it.
//
// Concurrency model: FOR UPDATE SKIP LOCKED means N workers (in one process or
// across several coordinators) can claim concurrently without blocking each
// other and without ever handing the same job to two workers. The row is
// flipped to RUNNING with a lease inside the same transaction, so a worker that
// dies holds the job only until its lease expires.
func (s *Store) ClaimRepairJob(ctx context.Context, owner string, lease time.Duration, replicationFactor int) (RepairTarget, error) {
	var out RepairTarget

	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var job RepairJob
		var lastErr *string
		err := tx.QueryRow(ctx, `
			UPDATE repair_jobs j
			SET state = 'RUNNING', owner = $1, lease_expires_at = now() + $2::interval,
			    attempts = j.attempts + 1, updated_at = now()
			WHERE j.id = (
				SELECT id FROM repair_jobs
				WHERE state = 'PENDING' AND next_attempt_at <= now()
				ORDER BY next_attempt_at, id
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING j.id, j.chunk_id, j.state, j.attempts, j.last_error, j.created_at`,
			owner, lease.String()).
			Scan(&job.ID, &job.ChunkID, &job.State, &job.Attempts, &lastErr, &job.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNoRepairWork
		}
		if err != nil {
			return fmt.Errorf("claim repair job: %w", err)
		}
		job.LastError = nullString(lastErr)
		out.Job = job

		// Load the chunk. A job whose chunk vanished (object deleted while the
		// job waited) is simply finished as succeeded -- there is nothing left
		// to repair, and failing it would create noise.
		var checksum *string
		var chunkState string
		var available int
		err = tx.QueryRow(ctx, `
			SELECT COALESCE(size_bytes, 0), checksum_sha256, state, available_replicas
			FROM chunks WHERE id = $1`, job.ChunkID).
			Scan(&out.Size, &checksum, &chunkState, &available)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("chunk %s: %w", job.ChunkID, ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("load chunk for repair: %w", err)
		}
		out.Checksum = nullString(checksum)
		out.DesiredReplicas = replicationFactor - available
		if out.DesiredReplicas < 0 {
			out.DesiredReplicas = 0
		}

		// Candidate sources: AVAILABLE replicas on nodes that are not DEAD.
		// Ordered by node load so repair traffic spreads rather than hammering
		// whichever node happens to sort first.
		rows, err := tx.Query(ctx, `
			SELECT r.node_id, n.grpc_address, n.health, r.state
			FROM chunk_replicas r
			JOIN storage_nodes n ON n.id = r.node_id
			WHERE r.chunk_id = $1 AND r.state = 'AVAILABLE' AND n.health <> 'DEAD'
			ORDER BY n.active_requests, n.id`, job.ChunkID)
		if err != nil {
			return fmt.Errorf("load repair sources: %w", err)
		}
		for rows.Next() {
			var rep Replica
			if err := rows.Scan(&rep.NodeID, &rep.Address, &rep.NodeHealth, &rep.State); err != nil {
				rows.Close()
				return err
			}
			rep.ChunkID = job.ChunkID
			out.Sources = append(out.Sources, rep)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		// Every node already associated with the chunk, in any state. The
		// destination must avoid all of them so a "replica" is never a second
		// copy on a machine that already has one.
		orows, err := tx.Query(ctx,
			`SELECT node_id, state FROM chunk_replicas WHERE chunk_id = $1`, job.ChunkID)
		if err != nil {
			return fmt.Errorf("load occupied nodes: %w", err)
		}
		for orows.Next() {
			var id, state string
			if err := orows.Scan(&id, &state); err != nil {
				orows.Close()
				return err
			}
			out.Occupied = append(out.Occupied, id)
			switch ReplicaState(state) {
			case ReplicaStale:
				out.StaleReplicas++
				out.TransientOccupancy++
			case ReplicaAvailable:
			default:
				// CORRUPT, DELETING, UNAVAILABLE, WRITING: the row blocks the
				// node today but will not survive the next GC / reconciler /
				// heartbeat cycle.
				out.TransientOccupancy++
			}
		}
		orows.Close()
		return orows.Err()
	})
	if err != nil {
		return RepairTarget{}, err
	}
	return out, nil
}

// RecordRepairPlan stores the chosen source and destination before the copy
// starts, so an operator inspecting a stuck job can see what it was attempting.
func (s *Store) RecordRepairPlan(ctx context.Context, jobID int64, source, target string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE repair_jobs SET source_node_id = $2, target_node_id = $3, updated_at = now()
		WHERE id = $1`, jobID, source, target)
	if err != nil {
		return fmt.Errorf("record repair plan: %w", err)
	}
	return nil
}

// CompleteRepair records a successful copy: the new replica becomes AVAILABLE
// and the job is closed, in one transaction.
//
// The replica insert is idempotent (ON CONFLICT), so a worker that succeeded
// but crashed before closing the job does not double-count when the job is
// retried -- the second run simply re-asserts the same row.
func (s *Store) CompleteRepair(ctx context.Context, jobID int64, chunkID uuid.UUID, targetNode string) (int, error) {
	var available int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO chunk_replicas (chunk_id, node_id, state, verified_at)
			VALUES ($1, $2, 'AVAILABLE', now())
			ON CONFLICT (chunk_id, node_id)
			DO UPDATE SET state = 'AVAILABLE', updated_at = now(), verified_at = now(),
			              last_error = NULL`,
			chunkID, targetNode); err != nil {
			return fmt.Errorf("record repaired replica: %w", err)
		}
		// Read the trigger-maintained counter rather than recounting.
		if err := tx.QueryRow(ctx,
			`SELECT available_replicas FROM chunks WHERE id = $1`, chunkID).Scan(&available); err != nil {
			return fmt.Errorf("read durability after repair: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE repair_jobs SET state = 'SUCCEEDED', owner = NULL, lease_expires_at = NULL,
			    last_error = NULL, finished_at = now(), updated_at = now()
			WHERE id = $1`, jobID); err != nil {
			return fmt.Errorf("close repair job: %w", err)
		}
		return nil
	})
	return available, err
}

// FinishRepairAsNoop closes a job that had nothing to do (chunk deleted, or
// already at full replication by the time the worker got to it).
func (s *Store) FinishRepairAsNoop(ctx context.Context, jobID int64, reason string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE repair_jobs SET state = 'SUCCEEDED', owner = NULL, lease_expires_at = NULL,
		    last_error = $2, finished_at = now(), updated_at = now()
		WHERE id = $1`, jobID, truncate(reason, 500))
	if err != nil {
		return fmt.Errorf("finish repair as noop: %w", err)
	}
	return nil
}

// FailRepair records a failed attempt. Below maxAttempts the job is re-queued
// with an exponential backoff; at the cap it becomes FAILED and stops
// consuming worker slots.
//
// Terminal FAILED is deliberate rather than infinite retry: a chunk that cannot
// be repaired after N attempts needs an operator, and a job spinning forever
// would both hide that and crowd out repairs that can succeed. The chunk stays
// visible in flexstore_under_replicated_chunks, so nothing is silently dropped.
func (s *Store) FailRepair(ctx context.Context, jobID int64, cause string, backoff time.Duration, maxAttempts int) (terminal bool, err error) {
	err = s.pool.QueryRow(ctx, `
		UPDATE repair_jobs
		SET state = CASE WHEN attempts >= $4 THEN 'FAILED' ELSE 'PENDING' END,
		    owner = NULL, lease_expires_at = NULL,
		    last_error = $2,
		    next_attempt_at = now() + $3::interval,
		    finished_at = CASE WHEN attempts >= $4 THEN now() ELSE NULL END,
		    updated_at = now()
		WHERE id = $1
		RETURNING state = 'FAILED'`,
		jobID, truncate(cause, 500), backoff.String(), maxAttempts).Scan(&terminal)
	if err != nil {
		return false, fmt.Errorf("fail repair job: %w", err)
	}
	return terminal, nil
}

// RequeueFailedRepairs gives terminally-failed jobs another chance. Called when
// the cluster topology changes (a node joins or recovers), because the reason a
// repair failed is very often "there was nowhere to put it".
func (s *Store) RequeueFailedRepairs(ctx context.Context, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE repair_jobs
		SET state = 'PENDING', attempts = 0, next_attempt_at = now(),
		    finished_at = NULL, updated_at = now()
		WHERE id IN (
			SELECT j.id FROM repair_jobs j
			JOIN chunks c ON c.id = j.chunk_id
			WHERE j.state = 'FAILED' AND c.state = 'COMMITTED'
			ORDER BY j.updated_at
			LIMIT $1
		)
		AND NOT EXISTS (
			SELECT 1 FROM repair_jobs a
			WHERE a.chunk_id = repair_jobs.chunk_id AND a.state IN ('PENDING', 'RUNNING')
		)`, limit)
	if err != nil {
		return 0, fmt.Errorf("requeue failed repairs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// MarkReplicaCorrupt demotes a replica that failed verification and returns how
// many usable replicas remain.
//
// It never deletes the chunk row or the other replicas: a corruption report is
// evidence about one copy, not about the object.
func (s *Store) MarkReplicaCorrupt(ctx context.Context, chunkID uuid.UUID, nodeID, detail string) (remaining int, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		// No rows affected is fine: the replica may already be DELETING, or the
		// reporter may be racing another detector. The durability read below
		// still gives the caller an accurate answer.
		if _, err := tx.Exec(ctx, `
			UPDATE chunk_replicas
			SET state = 'CORRUPT', updated_at = now(), last_error = $3
			WHERE chunk_id = $1 AND node_id = $2 AND state <> 'DELETING'`,
			chunkID, nodeID, truncate(detail, 500)); err != nil {
			return fmt.Errorf("mark replica corrupt: %w", err)
		}
		if err := tx.QueryRow(ctx,
			`SELECT available_replicas FROM chunks WHERE id = $1`, chunkID).Scan(&remaining); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("chunk %s: %w", chunkID, ErrNotFound)
			}
			return fmt.Errorf("read durability after corruption report: %w", err)
		}
		// The corrupt file must go, or it will keep being found by scrubs and
		// keep occupying the node in placement decisions.
		if _, err := tx.Exec(ctx, `
			INSERT INTO chunk_deletions (chunk_id, node_id) VALUES ($1, $2)
			ON CONFLICT (chunk_id, node_id) DO NOTHING`, chunkID, nodeID); err != nil {
			return fmt.Errorf("queue corrupt replica removal: %w", err)
		}
		return nil
	})
	return remaining, err
}

// DropReplica removes a replica row entirely. Used by reconciliation when a
// node turns out not to hold a chunk the metadata claimed.
func (s *Store) DropReplica(ctx context.Context, chunkID uuid.UUID, nodeID string) error {
	_, err := s.pool.Exec(ctx,
		`DELETE FROM chunk_replicas WHERE chunk_id = $1 AND node_id = $2`, chunkID, nodeID)
	if err != nil {
		return fmt.Errorf("drop replica %s/%s: %w", chunkID, nodeID, err)
	}
	return nil
}

// RepairCounts returns job counts by state, for metrics and the admin API.
func (s *Store) RepairCounts(ctx context.Context) (map[RepairJobState]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT state, COUNT(*) FROM repair_jobs GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count repair jobs: %w", err)
	}
	defer rows.Close()

	out := map[RepairJobState]int64{}
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return nil, err
		}
		out[RepairJobState(state)] = n
	}
	return out, rows.Err()
}

// RecentRepairJobs returns the most recently touched jobs for the admin API.
func (s *Store) RecentRepairJobs(ctx context.Context, limit int) ([]RepairJob, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, chunk_id, state, COALESCE(source_node_id, ''), COALESCE(target_node_id, ''),
		       attempts, COALESCE(last_error, ''), created_at, updated_at, finished_at
		FROM repair_jobs
		ORDER BY updated_at DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list repair jobs: %w", err)
	}
	defer rows.Close()

	var out []RepairJob
	for rows.Next() {
		var j RepairJob
		var finished *time.Time
		if err := rows.Scan(&j.ID, &j.ChunkID, &j.State, &j.SourceNodeID, &j.TargetNodeID,
			&j.Attempts, &j.LastError, &j.CreatedAt, &j.UpdatedAt, &finished); err != nil {
			return nil, fmt.Errorf("scan repair job: %w", err)
		}
		j.FinishedAt = nullTime(finished)
		out = append(out, j)
	}
	return out, rows.Err()
}

// PurgeFinishedRepairJobs trims history so the table does not grow without
// bound in a long-running cluster.
//
// It also clears FAILED jobs whose chunk is no longer damaged. A job typically
// fails because there was nowhere to put a copy; once the chunk reaches the
// replication factor by another route -- a different job, a node returning and
// being reconciled -- the failed row is stale history, not an outstanding
// problem. Leaving it would keep `repair_jobs_failed` permanently non-zero and
// make the "repairs are stuck" alert fire forever after any transient shortage,
// which is exactly how an alert stops meaning anything.
func (s *Store) PurgeFinishedRepairJobs(ctx context.Context, olderThan time.Duration, replicationFactor, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM repair_jobs
		WHERE id IN (
			SELECT id FROM repair_jobs
			WHERE state = 'SUCCEEDED' AND finished_at < now() - $1::interval
			ORDER BY finished_at
			LIMIT $2)`, olderThan.String(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge repair jobs: %w", err)
	}
	purged := tag.RowsAffected()

	resolved, err := s.pool.Exec(ctx, `
		DELETE FROM repair_jobs j
		WHERE j.id IN (
			SELECT rj.id FROM repair_jobs rj
			LEFT JOIN chunks c ON c.id = rj.chunk_id
			WHERE rj.state = 'FAILED'
			  AND (c.id IS NULL OR c.available_replicas >= $1)
			LIMIT $2)`, replicationFactor, limit)
	if err != nil {
		return purged, fmt.Errorf("purge resolved failed repair jobs: %w", err)
	}
	return purged + resolved.RowsAffected(), nil
}

// VerifyDurabilityCounters recomputes available_replicas from the underlying
// rows and repairs any drift, returning how many chunks were wrong.
//
// The trigger makes drift impossible within normal operation; this is the
// backstop for out-of-band changes (a manual UPDATE, a restore from backup, a
// future bulk import that disables triggers). It runs rarely, so the full scan
// is acceptable -- and a non-zero result is a loud signal that something
// bypassed the intended write path.
func (s *Store) VerifyDurabilityCounters(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE chunks c
		SET available_replicas = actual.n
		FROM (
			SELECT c2.id, COALESCE(COUNT(r.node_id) FILTER (WHERE r.state = 'AVAILABLE'), 0) AS n
			FROM chunks c2
			LEFT JOIN chunk_replicas r ON r.chunk_id = c2.id
			GROUP BY c2.id
		) AS actual
		WHERE c.id = actual.id AND c.available_replicas <> actual.n`)
	if err != nil {
		return 0, fmt.Errorf("verify durability counters: %w", err)
	}
	return tag.RowsAffected(), nil
}

// CompleteRepairForTest records an extra AVAILABLE replica without a job.
// Exported for tests that need to construct an over-replicated chunk directly;
// production code always goes through CompleteRepair so a job is closed too.
func (s *Store) CompleteRepairForTest(ctx context.Context, chunkID uuid.UUID, nodeID string) (int, error) {
	var available int
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO chunk_replicas (chunk_id, node_id, state, verified_at)
			VALUES ($1, $2, 'AVAILABLE', now())
			ON CONFLICT (chunk_id, node_id)
			DO UPDATE SET state = 'AVAILABLE', updated_at = now()`, chunkID, nodeID); err != nil {
			return err
		}
		return tx.QueryRow(ctx,
			`SELECT available_replicas FROM chunks WHERE id = $1`, chunkID).Scan(&available)
	})
	return available, err
}

-- Self-healing milestone: indexed under-replication detection, a durable
-- repair queue, and the replica states needed to distinguish "node is gone"
-- from "bytes are wrong" from "not verified since the node came back".

-- ---------------------------------------------------------------- states ---

-- CORRUPT: bytes failed SHA-256 verification. The remedy is to delete the file
--          and place a fresh replica, not to wait for the node.
-- STALE:   the node went away and came back. Probably fine, but not trusted
--          until the reconciler has verified it, so it does not count towards
--          durability. This is what stops a returning node's files from
--          silently becoming authoritative again.
ALTER TABLE chunk_replicas DROP CONSTRAINT chunk_replicas_state_check;
ALTER TABLE chunk_replicas ADD CONSTRAINT chunk_replicas_state_check
    CHECK (state IN ('WRITING', 'AVAILABLE', 'UNAVAILABLE', 'DELETING', 'CORRUPT', 'STALE'));

ALTER TABLE chunk_replicas ADD COLUMN last_error TEXT;

-- ------------------------------------------------- durability accounting ---

-- Denormalised count of AVAILABLE replicas, maintained by trigger.
--
-- Why denormalise: the previous implementation recomputed durability with a
-- GROUP BY over every chunk joined to every replica, on a timer. That is a
-- full-table scan several times a minute, and it is also the query the repair
-- scanner needs -- so its cost would grow with the whole dataset rather than
-- with the amount of damage. A trigger-maintained counter plus a partial index
-- turns "which chunks need repair" into an index range scan over exactly the
-- damaged rows.
--
-- Correctness: the trigger fires inside the same transaction as every replica
-- state change, so the counter can never diverge from the rows it summarises
-- within a transaction boundary. A periodic verifier (see
-- Store.VerifyDurabilityCounters) recounts and repairs any drift caused by
-- out-of-band changes, and logs loudly if it finds some.
ALTER TABLE chunks ADD COLUMN available_replicas INTEGER NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION flexstore_refresh_available_replicas()
RETURNS TRIGGER AS $$
DECLARE
    target UUID;
BEGIN
    target := COALESCE(NEW.chunk_id, OLD.chunk_id);
    UPDATE chunks
       SET available_replicas = (
           SELECT COUNT(*) FROM chunk_replicas r
           WHERE r.chunk_id = target AND r.state = 'AVAILABLE')
     WHERE id = target;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- AFTER, statement-level per row: fires on every path that can change the
-- count, including cascades from a deleted storage node.
CREATE TRIGGER chunk_replicas_durability
AFTER INSERT OR UPDATE OF state OR DELETE ON chunk_replicas
FOR EACH ROW EXECUTE FUNCTION flexstore_refresh_available_replicas();

-- Backfill for any rows that already exist.
UPDATE chunks c
   SET available_replicas = COALESCE((
       SELECT COUNT(*) FROM chunk_replicas r
       WHERE r.chunk_id = c.id AND r.state = 'AVAILABLE'), 0);

-- The repair scanner's index. Partial on COMMITTED because only committed
-- chunks have a durability guarantee to violate, and ordered by
-- available_replicas so the most endangered chunks are found first.
CREATE INDEX chunks_durability_idx
    ON chunks (available_replicas, id)
    WHERE state = 'COMMITTED';

-- ------------------------------------------------------------ repair queue --

CREATE TABLE repair_jobs (
    id              BIGSERIAL PRIMARY KEY,
    chunk_id        UUID        NOT NULL REFERENCES chunks (id) ON DELETE CASCADE,
    state           TEXT        NOT NULL
                                CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    -- Chosen at execution time, not enqueue time: the cluster may look
    -- different by the time a job runs.
    source_node_id  TEXT,
    target_node_id  TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    last_error      TEXT,
    -- Lease fields. A RUNNING job whose lease has expired is reclaimable,
    -- which is what makes a coordinator crash mid-repair recoverable.
    owner           TEXT,
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- At most one live job per chunk. This is the idempotence guarantee: the
-- scanner can run as often as it likes and re-enqueueing is a no-op while a
-- job is already in flight, so a slow repair never accumulates duplicates.
CREATE UNIQUE INDEX repair_jobs_active_idx
    ON repair_jobs (chunk_id)
    WHERE state IN ('PENDING', 'RUNNING');

-- The claim query's index: due PENDING jobs, oldest first.
CREATE INDEX repair_jobs_due_idx
    ON repair_jobs (next_attempt_at)
    WHERE state = 'PENDING';

-- Reclaiming expired leases after a coordinator crash.
CREATE INDEX repair_jobs_lease_idx
    ON repair_jobs (lease_expires_at)
    WHERE state = 'RUNNING';

CREATE INDEX repair_jobs_history_idx ON repair_jobs (finished_at DESC NULLS LAST);

-- ------------------------------------------------------ node reconciliation --

-- When a node rejoins we compare its actual inventory against metadata. This
-- table records the outcome so operators can see what reconciliation did, and
-- so a restart does not repeat work that already finished.
CREATE TABLE node_reconciliations (
    id              BIGSERIAL PRIMARY KEY,
    node_id         TEXT        NOT NULL REFERENCES storage_nodes (id) ON DELETE CASCADE,
    state           TEXT        NOT NULL
                                CHECK (state IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    chunks_seen     BIGINT      NOT NULL DEFAULT 0,
    -- Files on the node that metadata knows nothing about (object deleted
    -- while the node was down). Queued for deletion.
    orphans_found   BIGINT      NOT NULL DEFAULT 0,
    -- Replicas metadata claims but the node does not actually have. Dropped,
    -- which makes the chunk under-replicated and schedules a repair.
    phantoms_found  BIGINT      NOT NULL DEFAULT 0,
    -- STALE replicas confirmed present with a matching checksum.
    verified        BIGINT      NOT NULL DEFAULT 0,
    corrupt_found   BIGINT      NOT NULL DEFAULT 0,
    last_error      TEXT,
    owner           TEXT,
    lease_expires_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX node_reconciliations_active_idx
    ON node_reconciliations (node_id)
    WHERE state IN ('PENDING', 'RUNNING');

CREATE INDEX node_reconciliations_due_idx
    ON node_reconciliations (state, created_at);

-- Indexes justified by measured query plans, not by intuition.
--
-- Every index below was added because scripts/explain-hot-queries.sh showed a
-- sequential scan on a query that runs per request, per chunk deletion or per
-- admin poll. The script is reproducible and its output for this schema is
-- committed at docs/explain-hot-queries.txt, so the reasoning can be re-checked
-- rather than taken on trust.
--
-- Indexes deliberately NOT added, and why:
--
--   storage_nodes (health)         already exists and the planner correctly
--                                  ignores it -- a five-row table is cheaper to
--                                  scan than to look up. Kept because the table
--                                  grows with the cluster.
--   chunks (object_id)             already covered by chunks_object_index_idx,
--                                  whose leading column it is.
--   chunk_deletions (chunk_id)     already covered by the UNIQUE (chunk_id,
--                                  node_id) constraint.

-- ------------------------------------------------------- foreign key scans ---

-- repair_jobs.chunk_id REFERENCES chunks ON DELETE CASCADE.
--
-- PostgreSQL does not index the referencing side of a foreign key
-- automatically, so every chunk deletion had to scan repair_jobs to find rows
-- to cascade. repair_jobs_active_idx is unique on chunk_id but PARTIAL (state
-- IN ('PENDING','RUNNING')), and a partial index cannot serve a lookup that
-- must consider every row -- including the SUCCEEDED and FAILED history that
-- makes up almost all of the table.
--
-- Measured on 100k chunks with a 10k-row repair queue: "Seq Scan on
-- repair_jobs" per chunk deleted. Deleting one 1 GiB object is 128 chunks,
-- hence 128 sequential scans of the entire repair history.
CREATE INDEX IF NOT EXISTS repair_jobs_chunk_idx ON repair_jobs (chunk_id);

-- multipart_uploads.object_id REFERENCES objects ON DELETE SET NULL.
-- Same problem, on the object deletion path: measured "Seq Scan on
-- multipart_uploads" for every object removed.
CREATE INDEX IF NOT EXISTS multipart_uploads_object_idx
    ON multipart_uploads (object_id) WHERE object_id IS NOT NULL;

-- node_reconciliations.node_id REFERENCES storage_nodes ON DELETE CASCADE.
-- The existing index on this column is partial (active jobs only), so
-- decommissioning a node scanned the full reconciliation history.
CREATE INDEX IF NOT EXISTS node_reconciliations_node_idx
    ON node_reconciliations (node_id);

-- --------------------------------------------------------- ordered reads ---

-- RecentRepairJobs (admin API, and the repair panel of the Grafana dashboard)
-- orders by updated_at DESC. Measured: "Seq Scan on repair_jobs" plus a sort of
-- the whole table to return twenty rows.
CREATE INDEX IF NOT EXISTS repair_jobs_updated_idx ON repair_jobs (updated_at DESC);

-- PurgeFinishedRepairJobs scans SUCCEEDED jobs older than a cutoff, in
-- finished_at order. The pre-existing repair_jobs_history_idx covered
-- finished_at but not the state filter, so the scan had to read and discard
-- every PENDING, RUNNING and FAILED row it encountered. A partial index on the
-- state the query actually selects is both smaller and complete for it.
DROP INDEX IF EXISTS repair_jobs_history_idx;
CREATE INDEX IF NOT EXISTS repair_jobs_purge_idx
    ON repair_jobs (finished_at) WHERE state = 'SUCCEEDED';

-- ------------------------------------------------- reconciliation paging ---

-- StaleReplicasOnNode walks one node's STALE replicas in chunk_id order,
-- keyset-paginated so a node holding millions of chunks is processed
-- incrementally. The existing (node_id, state) index locates the rows but
-- cannot supply the ordering, so each page cost a sort of the whole matching
-- set -- making a full reconciliation quadratic in the number of stale
-- replicas, which is exactly the case that only arises on a large node
-- rejoining.
--
-- Extending the index with chunk_id makes each page an index range scan. It is
-- a strict superset of the old index's prefix, so the old one is redundant and
-- is dropped rather than left to consume writes.
DROP INDEX IF EXISTS chunk_replicas_node_idx;
CREATE INDEX IF NOT EXISTS chunk_replicas_node_state_chunk_idx
    ON chunk_replicas (node_id, state, chunk_id);

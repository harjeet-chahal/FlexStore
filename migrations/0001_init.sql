-- FlexStore control-plane schema.
--
-- Design notes:
--   * No object payload ever lives here. PostgreSQL is the durable metadata
--     source of truth; bytes live on storage nodes.
--   * State machines are enforced with CHECK constraints rather than only in
--     application code, so a buggy service cannot persist an impossible state.
--   * Chunk ownership is exclusive: a chunk belongs either to an object or to
--     an in-flight multipart part, never both.

CREATE TABLE storage_nodes (
    id                TEXT PRIMARY KEY,
    grpc_address      TEXT        NOT NULL,
    health            TEXT        NOT NULL DEFAULT 'UNKNOWN'
                                  CHECK (health IN ('UNKNOWN', 'HEALTHY', 'SUSPECT', 'DEAD')),
    total_bytes       BIGINT      NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
    used_bytes        BIGINT      NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    available_bytes   BIGINT      NOT NULL DEFAULT 0 CHECK (available_bytes >= 0),
    chunk_count       BIGINT      NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    active_requests   INTEGER     NOT NULL DEFAULT 0 CHECK (active_requests >= 0),
    last_heartbeat_at TIMESTAMPTZ,
    registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX storage_nodes_health_idx ON storage_nodes (health);

CREATE TABLE objects (
    id            UUID PRIMARY KEY,
    bucket        TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    version       BIGINT      NOT NULL DEFAULT 1,
    state         TEXT        NOT NULL
                              CHECK (state IN ('UPLOADING', 'COMPLETE', 'DELETING', 'FAILED')),
    size_bytes    BIGINT      NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    chunk_size    BIGINT      NOT NULL CHECK (chunk_size > 0),
    chunk_count   INTEGER     NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    etag          TEXT,
    content_type  TEXT        NOT NULL DEFAULT 'application/octet-stream',
    failure_reason TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at  TIMESTAMPTZ,
    deleted_at    TIMESTAMPTZ
);

-- At most one COMPLETE version of a (bucket, key) is visible at a time.
-- Superseded versions move to DELETING and are reclaimed by the GC worker,
-- which is what makes overwrite atomic from a reader's point of view.
CREATE UNIQUE INDEX objects_current_version_idx
    ON objects (bucket, key)
    WHERE state = 'COMPLETE';

CREATE INDEX objects_bucket_key_idx ON objects (bucket, key);
CREATE INDEX objects_state_idx ON objects (state);
-- Supports the reaper's "stale UPLOADING" scan.
CREATE INDEX objects_state_updated_idx ON objects (state, updated_at);

CREATE TABLE multipart_uploads (
    id           UUID PRIMARY KEY,
    bucket       TEXT        NOT NULL,
    key          TEXT        NOT NULL,
    state        TEXT        NOT NULL
                             CHECK (state IN ('IN_PROGRESS', 'COMPLETING', 'COMPLETED', 'ABORTED')),
    chunk_size   BIGINT      NOT NULL CHECK (chunk_size > 0),
    content_type TEXT        NOT NULL DEFAULT 'application/octet-stream',
    object_id    UUID        REFERENCES objects (id) ON DELETE SET NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX multipart_uploads_state_idx ON multipart_uploads (state, updated_at);
CREATE INDEX multipart_uploads_bucket_key_idx ON multipart_uploads (bucket, key);

CREATE TABLE multipart_parts (
    id           UUID PRIMARY KEY,
    upload_id    UUID        NOT NULL REFERENCES multipart_uploads (id) ON DELETE CASCADE,
    part_number  INTEGER     NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    state        TEXT        NOT NULL
                             CHECK (state IN ('UPLOADING', 'COMPLETE', 'FAILED')),
    size_bytes   BIGINT      NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    chunk_count  INTEGER     NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    etag         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

-- Re-uploading a part replaces it, so only one COMPLETE row per part number.
CREATE UNIQUE INDEX multipart_parts_complete_idx
    ON multipart_parts (upload_id, part_number)
    WHERE state = 'COMPLETE';

CREATE INDEX multipart_parts_upload_idx ON multipart_parts (upload_id, part_number);

CREATE TABLE chunks (
    id              UUID PRIMARY KEY,
    object_id       UUID REFERENCES objects (id) ON DELETE CASCADE,
    part_id         UUID REFERENCES multipart_parts (id) ON DELETE CASCADE,
    chunk_index     INTEGER     NOT NULL CHECK (chunk_index >= 0),
    size_bytes      BIGINT      NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    checksum_sha256 TEXT        CHECK (checksum_sha256 IS NULL OR checksum_sha256 ~ '^[0-9a-f]{64}$'),
    state           TEXT        NOT NULL
                                CHECK (state IN ('PENDING', 'COMMITTED', 'ORPHANED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    committed_at    TIMESTAMPTZ,
    -- Exactly one owner. Enforced here so re-parenting during multipart
    -- completion cannot leave a chunk owned by both or neither.
    CONSTRAINT chunks_single_owner CHECK ((object_id IS NOT NULL) <> (part_id IS NOT NULL))
);

CREATE UNIQUE INDEX chunks_object_index_idx
    ON chunks (object_id, chunk_index) WHERE object_id IS NOT NULL;
CREATE UNIQUE INDEX chunks_part_index_idx
    ON chunks (part_id, chunk_index) WHERE part_id IS NOT NULL;
CREATE INDEX chunks_state_idx ON chunks (state);

CREATE TABLE chunk_replicas (
    chunk_id    UUID        NOT NULL REFERENCES chunks (id) ON DELETE CASCADE,
    node_id     TEXT        NOT NULL REFERENCES storage_nodes (id) ON DELETE CASCADE,
    state       TEXT        NOT NULL
                            CHECK (state IN ('WRITING', 'AVAILABLE', 'UNAVAILABLE', 'DELETING')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_at TIMESTAMPTZ,
    PRIMARY KEY (chunk_id, node_id)
);

CREATE INDEX chunk_replicas_node_idx ON chunk_replicas (node_id, state);
CREATE INDEX chunk_replicas_state_idx ON chunk_replicas (state);

-- Pending deletions the GC worker must push to storage nodes. A separate queue
-- (rather than deleting rows immediately) means a node that is down when an
-- object is deleted still gets its chunk reclaimed once it returns.
CREATE TABLE chunk_deletions (
    id          BIGSERIAL PRIMARY KEY,
    chunk_id    UUID        NOT NULL,
    node_id     TEXT        NOT NULL,
    attempts    INTEGER     NOT NULL DEFAULT 0,
    last_error  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (chunk_id, node_id)
);

CREATE INDEX chunk_deletions_due_idx ON chunk_deletions (next_attempt_at);

package metadata

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests run against a REAL PostgreSQL instance. The metadata layer is
// almost entirely SQL -- transactions, partial unique indexes, CHECK
// constraints, window functions -- so a mocked database would test nothing
// that matters. They skip when no DSN is configured so `go test ./...` still
// works on a bare checkout.
//
//	make test-db      # start a throwaway PostgreSQL
//	make test-metadata
const dsnEnv = "FLEXSTORE_TEST_POSTGRES_DSN"

var uniq atomic.Int64

func testStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set; run `make test-db` first", dsnEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := Open(ctx, dsn, log)
	if err != nil {
		t.Fatalf("connecting to test postgres: %v", err)
	}
	t.Cleanup(store.Close)

	if err := Migrate(ctx, store.Pool(), log); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Several store operations are deliberately *global*: a repair worker takes
	// the oldest due job in the cluster, and the reapers scan every stale
	// upload regardless of bucket. That is correct in production and hostile to
	// test isolation -- a row left behind by an earlier run (or an earlier run
	// from an hour ago, which is long enough to look "stale") would show up in
	// a later test's counts.
	//
	// Each test therefore starts from an empty slate. Tests in this package are
	// not parallel, so this is safe, and it is far more robust than trying to
	// scope every assertion to one bucket.
	for _, table := range []string{
		"repair_jobs", "node_reconciliations", "chunk_deletions",
		"multipart_uploads", "objects",
	} {
		if _, err := store.Pool().Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("clearing %s: %v", table, err)
		}
	}
	return store, ctx
}

// bucket returns a name unique to this test run, so tests are isolated without
// truncating shared tables (which would break parallel runs).
func bucket(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test-%d-%d", time.Now().UnixNano()%1e9, uniq.Add(1))
}

// registerNodes creates n storage nodes with plenty of capacity and returns
// their IDs. chunk_replicas has a foreign key onto storage_nodes, so replicas
// cannot be recorded for nodes that were never registered.
func registerNodes(t *testing.T, ctx context.Context, s *Store, n int) []string {
	t.Helper()
	prefix := fmt.Sprintf("node-%d-%d", time.Now().UnixNano()%1e9, uniq.Add(1))
	ids := make([]string, n)
	for i := range ids {
		id := fmt.Sprintf("%s-%d", prefix, i)
		err := s.UpsertNode(ctx, id, id+":9100", "HEALTHY", NodeStats{
			TotalBytes: 1 << 40, AvailableBytes: 1 << 40,
		}, time.Now())
		if err != nil {
			t.Fatalf("registering node %s: %v", id, err)
		}
		ids[i] = id
		//nolint:contextcheck // cleanup must outlive the test's context
		t.Cleanup(func() {
			// Deleting the node cascades to chunk_replicas, but chunk_deletions
			// deliberately has no foreign key (a queued deletion must outlive a
			// node that is temporarily gone). Without this the queue would grow
			// across runs and eventually push a later test's jobs past
			// ClaimDeletions' batch limit.
			bg := context.Background()
			_, _ = s.Pool().Exec(bg, `DELETE FROM chunk_deletions WHERE node_id = $1`, id)
			_, _ = s.Pool().Exec(bg, `DELETE FROM storage_nodes WHERE id = $1`, id)
		})
	}
	return ids
}

// putObject writes a complete object with the given per-chunk sizes and
// returns its ID. This is the happy path expressed once so the individual
// tests stay about the property under test.
func putObject(t *testing.T, ctx context.Context, s *Store, bkt, key string, nodes []string, chunkSizes ...int64) uuid.UUID {
	t.Helper()

	obj, err := s.BeginUpload(ctx, bkt, key, "application/octet-stream", 8<<20)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}

	var total int64
	for i, size := range chunkSizes {
		chunkID, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, int32(i), size, nodes)
		if err != nil {
			t.Fatalf("AllocateChunk %d: %v", i, err)
		}
		if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(i), size, nodes, nil, 1); err != nil {
			t.Fatalf("CommitChunk %d: %v", i, err)
		}
		total += size
	}

	if _, err := s.CompleteUpload(ctx, obj.ID, total, int32(len(chunkSizes)), "etag-"+key); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	return obj.ID
}

// fakeChecksum produces a syntactically valid but distinct SHA-256 per index.
// The schema enforces the hex format, so tests cannot use placeholder strings.
func fakeChecksum(i int) string {
	return fmt.Sprintf("%064x", i+1)
}

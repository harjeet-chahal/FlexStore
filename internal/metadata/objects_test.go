package metadata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUploadLifecycle(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "lifecycle/object.bin"

	obj, err := s.BeginUpload(ctx, bkt, key, "text/plain", 8<<20)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	if obj.State != ObjectUploading {
		t.Fatalf("state = %s, want UPLOADING", obj.State)
	}
	if obj.Version != 1 {
		t.Fatalf("version = %d, want 1", obj.Version)
	}

	// An UPLOADING object must not be visible to readers.
	if _, err := s.HeadObject(ctx, bkt, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("an in-flight upload is readable: %v", err)
	}

	chunkID, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 1024, nodes)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	// Replicas are recorded as WRITING at allocation time so the GC can find
	// them even if the gateway dies before committing.
	replicas := replicaStates(t, ctx, s, chunkID.String())
	if len(replicas) != 3 {
		t.Fatalf("%d replica rows after allocation, want 3", len(replicas))
	}
	for node, state := range replicas {
		if state != string(ReplicaWriting) {
			t.Errorf("replica on %s is %s, want WRITING", node, state)
		}
	}

	available, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 1024, nodes, nil, 2)
	if err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}
	if available != 3 {
		t.Fatalf("available replicas = %d, want 3", available)
	}

	completed, err := s.CompleteUpload(ctx, obj.ID, 1024, 1, "etag-abc")
	if err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}
	if completed.State != ObjectComplete {
		t.Fatalf("state = %s, want COMPLETE", completed.State)
	}
	if completed.CompletedAt.IsZero() {
		t.Error("CompletedAt was not set")
	}

	got, chunks, err := s.GetObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if got.SizeBytes != 1024 || got.ChunkCount != 1 || got.ETag != "etag-abc" {
		t.Fatalf("unexpected object: %+v", got)
	}
	if got.ContentType != "text/plain" {
		t.Errorf("ContentType = %q", got.ContentType)
	}
	if len(chunks) != 1 || len(chunks[0].Replicas) != 3 {
		t.Fatalf("expected 1 chunk with 3 replicas, got %d chunks", len(chunks))
	}
	for _, r := range chunks[0].Replicas {
		if r.State != ReplicaAvailable {
			t.Errorf("replica on %s is %s, want AVAILABLE", r.NodeID, r.State)
		}
		if r.Address == "" {
			t.Errorf("replica on %s has no address; the gateway could not reach it", r.NodeID)
		}
	}
}

func TestCommitChunkEnforcesDurabilityPolicy(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)

	obj, err := s.BeginUpload(ctx, bkt, "durability.bin", "", 8<<20)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	chunkID, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 512, nodes)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	// Only one node acknowledged, but the policy demands two.
	_, err = s.CommitChunk(ctx, chunkID, fakeChecksum(0), 512, nodes[:1], nodes[1:], 2)
	if !errors.Is(err, ErrDurabilityNotMet) {
		t.Fatalf("expected ErrDurabilityNotMet, got %v", err)
	}

	// The chunk must NOT be committed: a rolled-back transaction is what makes
	// "COMPLETE means durable" true.
	if got := chunkState(t, ctx, s, chunkID.String()); got != string(ChunkPending) {
		t.Fatalf("chunk state = %s after a failed commit, want PENDING", got)
	}

	// And the object cannot be completed either.
	if _, err := s.CompleteUpload(ctx, obj.ID, 512, 1, "etag"); !errors.Is(err, ErrDurabilityNotMet) {
		t.Fatalf("expected CompleteUpload to reject an uncommitted chunk, got %v", err)
	}
	if _, err := s.HeadObject(ctx, bkt, "durability.bin"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an object with an uncommitted chunk became visible")
	}
}

func TestCommitChunkSucceedsAtExactlyQuorum(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "quorum.bin", "", 8<<20)
	chunkID, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 512, nodes)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	available, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 512, nodes[:2], nodes[2:], 2)
	if err != nil {
		t.Fatalf("2-of-3 should satisfy a quorum of 2: %v", err)
	}
	if available != 2 {
		t.Fatalf("available = %d, want 2", available)
	}

	// The node that failed must be recorded as UNAVAILABLE, not dropped --
	// that record is what a repair pass will act on.
	states := replicaStates(t, ctx, s, chunkID.String())
	if states[nodes[2]] != string(ReplicaUnavailable) {
		t.Fatalf("failed node recorded as %q, want UNAVAILABLE", states[nodes[2]])
	}
}

func TestCompleteUploadRejectsAChunkCountMismatch(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "mismatch.bin", "", 8<<20)
	chunkID, _ := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 100, nodes, nil, 1); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}

	// The gateway claims 5 chunks but only 1 exists: a truncated upload.
	if _, err := s.CompleteUpload(ctx, obj.ID, 500, 5, "etag"); !errors.Is(err, ErrDurabilityNotMet) {
		t.Fatalf("expected the count mismatch to be rejected, got %v", err)
	}
}

func TestChunkOrderingIsByIndexNotInsertionOrLexicographic(t *testing.T) {
	// A 12-chunk object catches lexicographic ordering bugs ("10" < "2").
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "ordering/large.bin"

	sizes := make([]int64, 12)
	for i := range sizes {
		sizes[i] = int64(100 + i)
	}
	putObject(t, ctx, s, bkt, key, nodes, sizes...)

	_, chunks, err := s.GetObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if len(chunks) != 12 {
		t.Fatalf("got %d chunks, want 12", len(chunks))
	}
	for i, c := range chunks {
		if c.Index != int32(i) {
			t.Fatalf("position %d holds chunk index %d; object bytes would be reordered", i, c.Index)
		}
		if c.SizeBytes != sizes[i] {
			t.Errorf("chunk %d size = %d, want %d", i, c.SizeBytes, sizes[i])
		}
		if c.Checksum != fakeChecksum(i) {
			t.Errorf("chunk %d checksum = %s, want %s", i, c.Checksum, fakeChecksum(i))
		}
	}
}

func TestOverwriteSupersedesThePreviousVersionAtomically(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "overwrite.bin"

	firstID := putObject(t, ctx, s, bkt, key, nodes, 100)
	secondID := putObject(t, ctx, s, bkt, key, nodes, 200, 300)

	got, err := s.HeadObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got.ID != secondID {
		t.Fatalf("current version is %s, want the newer %s", got.ID, secondID)
	}
	if got.Version != 2 {
		t.Fatalf("version = %d, want 2", got.Version)
	}
	if got.SizeBytes != 500 {
		t.Fatalf("size = %d, want 500", got.SizeBytes)
	}

	// The old version must be retired, not left as a second COMPLETE row --
	// the partial unique index would reject that, so this also proves the swap
	// happened inside one transaction.
	old, err := s.GetObjectByID(ctx, firstID)
	if err != nil {
		t.Fatalf("GetObjectByID: %v", err)
	}
	if old.State != ObjectDeleting {
		t.Fatalf("previous version is %s, want DELETING", old.State)
	}
	// Its chunks must be queued for reclamation, or overwriting would leak.
	if n := pendingDeletions(t, ctx, s, firstID.String()); n == 0 {
		t.Fatal("superseded version left no deletion jobs; its bytes would leak")
	}
}

func TestDeleteObject(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "delete-me.bin"

	objID := putObject(t, ctx, s, bkt, key, nodes, 100, 200)

	gotID, existed, err := s.DeleteObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}
	if !existed || gotID != objID {
		t.Fatalf("DeleteObject returned (%s, %v), want (%s, true)", gotID, existed, objID)
	}

	if _, err := s.HeadObject(ctx, bkt, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("object is still readable after delete: %v", err)
	}

	// 2 chunks x 3 replicas = 6 deletion jobs.
	if n := pendingDeletions(t, ctx, s, objID.String()); n != 6 {
		t.Fatalf("%d deletion jobs queued, want 6", n)
	}

	// Deleting again is a no-op rather than an error: DELETE is idempotent.
	_, existed, err = s.DeleteObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("second DeleteObject: %v", err)
	}
	if existed {
		t.Fatal("second DeleteObject reported the object as present")
	}
}

func TestAbortUploadOrphansChunks(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "aborted.bin", "", 8<<20)
	chunkID, _ := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 100, nodes, nil, 1); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}

	if err := s.AbortUpload(ctx, obj.ID, "client disconnected"); err != nil {
		t.Fatalf("AbortUpload: %v", err)
	}

	after, err := s.GetObjectByID(ctx, obj.ID)
	if err != nil {
		t.Fatalf("GetObjectByID: %v", err)
	}
	if after.State != ObjectFailed {
		t.Fatalf("state = %s, want FAILED", after.State)
	}
	if after.FailureReason != "client disconnected" {
		t.Errorf("FailureReason = %q", after.FailureReason)
	}
	if got := chunkState(t, ctx, s, chunkID.String()); got != string(ChunkOrphaned) {
		t.Fatalf("chunk state = %s, want ORPHANED", got)
	}
	if n := pendingDeletions(t, ctx, s, obj.ID.String()); n != 3 {
		t.Fatalf("%d deletion jobs, want 3", n)
	}

	// Aborting twice must not error; the gateway calls it from a defer.
	if err := s.AbortUpload(ctx, obj.ID, "again"); err != nil {
		t.Fatalf("second AbortUpload: %v", err)
	}
}

func TestAllocateChunkRefusesANonUploadingObject(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "closed.bin", "", 8<<20)
	if err := s.AbortUpload(ctx, obj.ID, "done"); err != nil {
		t.Fatalf("AbortUpload: %v", err)
	}

	// A late gateway must not be able to resurrect an aborted upload.
	_, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict, got %v", err)
	}
}

func TestAllocateChunkIsRetryableForTheSameIndex(t *testing.T) {
	// The gateway retries a chunk after a transient node failure; reallocating
	// the same index must reuse the row rather than violate the unique index.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "retry.bin", "", 8<<20)
	first, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if err != nil {
		t.Fatalf("first AllocateChunk: %v", err)
	}
	second, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 150, nodes)
	if err != nil {
		t.Fatalf("retried AllocateChunk: %v", err)
	}
	if first != second {
		t.Fatalf("retry minted a new chunk id (%s vs %s); the old one would leak", first, second)
	}
}

func TestAllocateChunkRequiresExactlyOneOwner(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 1)

	if _, err := s.AllocateChunk(ctx, ChunkOwner{}, 0, 100, nodes); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict with no owner, got %v", err)
	}
	obj, _ := s.BeginUpload(ctx, bucket(t), "owner.bin", "", 8<<20)
	partID := obj.ID // any UUID; the point is that both fields are set
	if _, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID, PartID: &partID}, 0, 100, nodes); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict with two owners, got %v", err)
	}
	if _, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nil); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict with no placement targets, got %v", err)
	}
}

func TestEmptyObjectIsValid(t *testing.T) {
	s, ctx := testStore(t)
	bkt, key := bucket(t), "empty.bin"

	obj, err := s.BeginUpload(ctx, bkt, key, "", 8<<20)
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	if _, err := s.CompleteUpload(ctx, obj.ID, 0, 0, "etag-empty"); err != nil {
		t.Fatalf("a zero-byte object should be storable: %v", err)
	}
	got, chunks, err := s.GetObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if got.SizeBytes != 0 || len(chunks) != 0 {
		t.Fatalf("size = %d, chunks = %d, want 0/0", got.SizeBytes, len(chunks))
	}
}

func TestListObjects(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)

	for _, key := range []string{"a.bin", "b/1.bin", "b/2.bin", "c.bin"} {
		putObject(t, ctx, s, bkt, key, nodes, 10)
	}

	all, truncated, err := s.ListObjects(ctx, bkt, "", "", 100)
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	if len(all) != 4 || truncated {
		t.Fatalf("got %d objects (truncated=%v), want 4/false", len(all), truncated)
	}
	// Lexicographic order is part of the contract for pagination to work.
	for i := 1; i < len(all); i++ {
		if all[i-1].Key >= all[i].Key {
			t.Fatalf("results are not sorted: %q then %q", all[i-1].Key, all[i].Key)
		}
	}

	prefixed, _, err := s.ListObjects(ctx, bkt, "b/", "", 100)
	if err != nil {
		t.Fatalf("ListObjects with prefix: %v", err)
	}
	if len(prefixed) != 2 {
		t.Fatalf("prefix query returned %d objects, want 2", len(prefixed))
	}

	page, truncated, err := s.ListObjects(ctx, bkt, "", "", 2)
	if err != nil {
		t.Fatalf("paginated ListObjects: %v", err)
	}
	if len(page) != 2 || !truncated {
		t.Fatalf("page = %d objects (truncated=%v), want 2/true", len(page), truncated)
	}

	next, truncated, err := s.ListObjects(ctx, bkt, "", page[1].Key, 2)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(next) != 2 || truncated {
		t.Fatalf("second page = %d objects (truncated=%v), want 2/false", len(next), truncated)
	}
	if next[0].Key <= page[1].Key {
		t.Fatalf("pagination overlapped: %q follows %q", next[0].Key, page[1].Key)
	}
}

func TestReapStaleUploads(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "stalled.bin", "", 8<<20)
	if _, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes); err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	// Nothing is stale yet.
	if n, err := s.ReapStaleUploads(ctx, time.Hour, 10); err != nil || n != 0 {
		t.Fatalf("ReapStaleUploads reaped %d (err=%v), want 0", n, err)
	}

	// Age the row rather than sleeping.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE objects SET updated_at = now() - interval '2 hours' WHERE id = $1`, obj.ID); err != nil {
		t.Fatalf("ageing the upload: %v", err)
	}

	n, err := s.ReapStaleUploads(ctx, time.Hour, 10)
	if err != nil {
		t.Fatalf("ReapStaleUploads: %v", err)
	}
	if n != 1 {
		t.Fatalf("reaped %d uploads, want 1", n)
	}
	after, _ := s.GetObjectByID(ctx, obj.ID)
	if after.State != ObjectFailed {
		t.Fatalf("state = %s, want FAILED", after.State)
	}
	if pendingDeletions(t, ctx, s, obj.ID.String()) == 0 {
		t.Fatal("reaped upload left no deletion jobs; its chunks would leak")
	}
}

func TestTouchUploadKeepsASlowUploadAlive(t *testing.T) {
	s, ctx := testStore(t)
	obj, _ := s.BeginUpload(ctx, bucket(t), "slow.bin", "", 8<<20)

	if _, err := s.Pool().Exec(ctx,
		`UPDATE objects SET updated_at = now() - interval '2 hours' WHERE id = $1`, obj.ID); err != nil {
		t.Fatalf("ageing: %v", err)
	}
	if err := s.TouchUpload(ctx, obj.ID); err != nil {
		t.Fatalf("TouchUpload: %v", err)
	}
	if n, _ := s.ReapStaleUploads(ctx, time.Hour, 10); n != 0 {
		t.Fatalf("a touched upload was reaped anyway (%d)", n)
	}
}

// ---- helpers -------------------------------------------------------------

func replicaStates(t *testing.T, ctx context.Context, s *Store, chunkID string) map[string]string {
	t.Helper()
	rows, err := s.Pool().Query(ctx,
		`SELECT node_id, state FROM chunk_replicas WHERE chunk_id = $1`, chunkID)
	if err != nil {
		t.Fatalf("querying replicas: %v", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var node, state string
		if err := rows.Scan(&node, &state); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[node] = state
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating replicas: %v", err)
	}
	return out
}

func chunkState(t *testing.T, ctx context.Context, s *Store, chunkID string) string {
	t.Helper()
	var state string
	if err := s.Pool().QueryRow(ctx, `SELECT state FROM chunks WHERE id = $1`, chunkID).Scan(&state); err != nil {
		t.Fatalf("querying chunk state: %v", err)
	}
	return state
}

func pendingDeletions(t *testing.T, ctx context.Context, s *Store, objectID string) int {
	t.Helper()
	var n int
	err := s.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM chunk_deletions d
		JOIN chunks c ON c.id = d.chunk_id
		WHERE c.object_id = $1`, objectID).Scan(&n)
	if err != nil {
		t.Fatalf("counting deletions: %v", err)
	}
	return n
}

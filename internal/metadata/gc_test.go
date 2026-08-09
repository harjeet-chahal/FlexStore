package metadata

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDeletionQueueLifecycle(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "gc/target.bin"

	objID := putObject(t, ctx, s, bkt, key, nodes, 100, 200)
	if _, _, err := s.DeleteObject(ctx, bkt, key); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	jobs, err := s.ClaimDeletions(ctx, 100, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDeletions: %v", err)
	}
	mine := jobsForObject(t, ctx, s, jobs, objID.String())
	if len(mine) != 6 {
		t.Fatalf("claimed %d jobs for this object, want 6", len(mine))
	}
	for _, j := range mine {
		if j.Address == "" {
			t.Errorf("job for node %s has no address; the GC could not reach it", j.NodeID)
		}
		if j.Attempts != 1 {
			t.Errorf("attempts = %d after the first claim, want 1", j.Attempts)
		}
	}

	// The lease must stop a second claim from picking up the same work.
	again, err := s.ClaimDeletions(ctx, 100, time.Minute)
	if err != nil {
		t.Fatalf("second ClaimDeletions: %v", err)
	}
	if len(jobsForObject(t, ctx, s, again, objID.String())) != 0 {
		t.Fatal("leased jobs were handed out twice; two coordinators would double-delete")
	}

	for _, j := range mine {
		if err := s.FinishDeletion(ctx, j.ID, j.ChunkID, j.NodeID); err != nil {
			t.Fatalf("FinishDeletion: %v", err)
		}
	}

	// Metadata only shrinks once every replica is confirmed gone.
	chunks, objects, err := s.PurgeReclaimedChunks(ctx, 100)
	if err != nil {
		t.Fatalf("PurgeReclaimedChunks: %v", err)
	}
	if objects < 1 {
		t.Fatalf("purged %d objects (%d chunks); the deleted object should be gone", objects, chunks)
	}
	if _, err := s.GetObjectByID(ctx, objID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("object row survived reclamation: %v", err)
	}
}

func TestPurgeWaitsForOutstandingDeletions(t *testing.T) {
	// Dropping the object row before its replicas are reclaimed would lose the
	// only record of where the bytes are, orphaning them forever.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "gc/pending.bin"

	objID := putObject(t, ctx, s, bkt, key, nodes, 100)
	if _, _, err := s.DeleteObject(ctx, bkt, key); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	if _, _, err := s.PurgeReclaimedChunks(ctx, 100); err != nil {
		t.Fatalf("PurgeReclaimedChunks: %v", err)
	}
	if _, err := s.GetObjectByID(ctx, objID); err != nil {
		t.Fatalf("object was purged while deletions were still pending: %v", err)
	}
}

func TestFailedDeletionsBackOffAndEventuallyExpire(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 1)
	bkt, key := bucket(t), "gc/failing.bin"

	putObject(t, ctx, s, bkt, key, nodes, 100)
	if _, _, err := s.DeleteObject(ctx, bkt, key); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	jobs, err := s.ClaimDeletions(ctx, 10, time.Millisecond)
	if err != nil {
		t.Fatalf("ClaimDeletions: %v", err)
	}
	if len(jobs) == 0 {
		t.Fatal("no deletion jobs to fail")
	}
	job := jobs[0]

	// A failure with attempts below the cap re-queues with a backoff.
	if err := s.FailDeletion(ctx, job.ID, "node unreachable", 0, 8); err != nil {
		t.Fatalf("FailDeletion: %v", err)
	}
	if !deletionExists(t, ctx, s, job.ID) {
		t.Fatal("the job was dropped on its first failure")
	}

	// Push it past the attempt cap: the job must be abandoned rather than
	// pinning metadata forever behind an unreachable node.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE chunk_deletions SET attempts = 99 WHERE id = $1`, job.ID); err != nil {
		t.Fatalf("bumping attempts: %v", err)
	}
	if err := s.FailDeletion(ctx, job.ID, "still unreachable", 0, 8); err != nil {
		t.Fatalf("FailDeletion at the cap: %v", err)
	}
	if deletionExists(t, ctx, s, job.ID) {
		t.Fatal("the job survived past the attempt cap")
	}
}

func TestClusterStatsCountsUnderReplicatedChunks(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)

	// Two fully-replicated chunks.
	putObject(t, ctx, s, bkt, "healthy.bin", nodes, 100, 200)

	before, err := s.ClusterStats(ctx, 3)
	if err != nil {
		t.Fatalf("ClusterStats: %v", err)
	}

	// One chunk written to only two of three nodes.
	obj, _ := s.BeginUpload(ctx, bkt, "thin.bin", "", 8<<20)
	chunkID, _ := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 50, nodes)
	if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 50, nodes[:2], nodes[2:], 2); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}
	if _, err := s.CompleteUpload(ctx, obj.ID, 50, 1, "etag"); err != nil {
		t.Fatalf("CompleteUpload: %v", err)
	}

	after, err := s.ClusterStats(ctx, 3)
	if err != nil {
		t.Fatalf("ClusterStats: %v", err)
	}
	if after.UnderReplicatedChunks != before.UnderReplicatedChunks+1 {
		t.Fatalf("under-replicated went from %d to %d, want +1",
			before.UnderReplicatedChunks, after.UnderReplicatedChunks)
	}
	if after.TotalChunks != before.TotalChunks+1 {
		t.Fatalf("total chunks went from %d to %d, want +1", before.TotalChunks, after.TotalChunks)
	}
	if after.ObjectsByState[ObjectComplete] < 2 {
		t.Fatalf("COMPLETE objects = %d, want at least 2", after.ObjectsByState[ObjectComplete])
	}
}

func TestClusterStatsIgnoresChunksAwaitingReclamation(t *testing.T) {
	// Deleting an object leaves its chunks COMMITTED with DELETING replicas
	// until the GC catches up. Counting those as under-replicated would make
	// the durability alert fire on every normal delete.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "reclaim-noise.bin"

	putObject(t, ctx, s, bkt, key, nodes, 100, 200, 300)
	before, err := s.ClusterStats(ctx, 3)
	if err != nil {
		t.Fatalf("ClusterStats: %v", err)
	}

	if _, _, err := s.DeleteObject(ctx, bkt, key); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	after, err := s.ClusterStats(ctx, 3)
	if err != nil {
		t.Fatalf("ClusterStats: %v", err)
	}
	if after.UnderReplicatedChunks != before.UnderReplicatedChunks {
		t.Fatalf("deleting an object changed under-replicated from %d to %d; "+
			"garbage awaiting GC is being counted as a durability problem",
			before.UnderReplicatedChunks, after.UnderReplicatedChunks)
	}
	if after.TotalChunks != before.TotalChunks-3 {
		t.Fatalf("total chunks went from %d to %d, want -3 (the deleted object's chunks)",
			before.TotalChunks, after.TotalChunks)
	}
}

func TestClusterStatsTreatsDeadNodeReplicasAsLost(t *testing.T) {
	// Durability accounting must reflect node health, not just row counts:
	// three replicas on three dead nodes is zero durability.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	putObject(t, ctx, s, bucket(t), "on-dead-nodes.bin", nodes, 100)

	before, _ := s.ClusterStats(ctx, 3)
	for _, id := range nodes {
		if _, err := s.SetNodeHealth(ctx, id, "DEAD"); err != nil {
			t.Fatalf("SetNodeHealth: %v", err)
		}
	}
	after, _ := s.ClusterStats(ctx, 3)

	if after.UnderReplicatedChunks <= before.UnderReplicatedChunks {
		t.Fatalf("under-replicated count did not rise after every node died (%d -> %d)",
			before.UnderReplicatedChunks, after.UnderReplicatedChunks)
	}
}

func TestReplicaCountsForObject(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "counts.bin"

	objID := putObject(t, ctx, s, bkt, key, nodes, 100, 200, 300)

	counts, err := s.ReplicaCountsForObject(ctx, objID)
	if err != nil {
		t.Fatalf("ReplicaCountsForObject: %v", err)
	}
	if len(counts) != 3 {
		t.Fatalf("got counts for %d chunks, want 3", len(counts))
	}
	for id, n := range counts {
		if n != 3 {
			t.Errorf("chunk %s has %d available replicas, want 3", id, n)
		}
	}
}

func TestPurgeCompletedMultipartUploads(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	mu, _ := s.CreateMultipartUpload(ctx, bucket(t), "purge-mpu.bin", "", 8<<20)
	uploadPart(t, ctx, s, mu.ID, 1, nodes, 100)
	if _, err := s.CompleteMultipartUpload(ctx, mu.ID, nil); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	// Not old enough yet.
	if n, err := s.PurgeCompletedMultipartUploads(ctx, time.Hour, 100); err != nil || n != 0 {
		t.Fatalf("purged %d recent uploads (err=%v), want 0", n, err)
	}

	if _, err := s.Pool().Exec(ctx,
		`UPDATE multipart_uploads SET updated_at = now() - interval '2 days' WHERE id = $1`, mu.ID); err != nil {
		t.Fatalf("ageing the upload: %v", err)
	}
	if _, err := s.PurgeCompletedMultipartUploads(ctx, time.Hour, 100); err != nil {
		t.Fatalf("PurgeCompletedMultipartUploads: %v", err)
	}
	if _, err := s.GetMultipartUpload(ctx, mu.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the completed session survived purging: %v", err)
	}
}

// ---- helpers -------------------------------------------------------------

func jobsForObject(t *testing.T, ctx context.Context, s *Store, jobs []PendingDeletion, objectID string) []PendingDeletion {
	t.Helper()
	wanted := map[string]bool{}
	rows, err := s.Pool().Query(ctx, `SELECT id FROM chunks WHERE object_id = $1`, objectID)
	if err != nil {
		t.Fatalf("listing chunks: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		wanted[id] = true
	}

	var out []PendingDeletion
	for _, j := range jobs {
		if wanted[j.ChunkID.String()] {
			out = append(out, j)
		}
	}
	return out
}

func deletionExists(t *testing.T, ctx context.Context, s *Store, id int64) bool {
	t.Helper()
	var exists bool
	if err := s.Pool().QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM chunk_deletions WHERE id = $1)`, id).Scan(&exists); err != nil {
		t.Fatalf("checking deletion job: %v", err)
	}
	return exists
}

// TestAbandonedDeletionReleasesTheChunk pins the fix for a leak that made an
// object permanently unreclaimable.
//
// When a deletion job exhausts its retries the queue row is dropped, but the
// DELETING replica row it refers to has to go with it. PurgeReclaimedChunks
// will not remove a chunk while any chunk_replicas row survives, and will not
// remove an object while any chunk survives -- so leaving the replica row
// behind pins both rows in PostgreSQL forever, which is precisely what giving
// up on the job was supposed to prevent.
func TestAbandonedDeletionReleasesTheChunk(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 1)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "abandoned.bin", nodes, 100)

	if _, _, err := s.DeleteObject(ctx, bkt, "abandoned.bin"); err != nil {
		t.Fatalf("DeleteObject: %v", err)
	}

	claimed, err := s.ClaimDeletions(ctx, 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDeletions: %v", err)
	}
	if len(claimed) == 0 {
		t.Fatal("no deletion jobs were queued for a deleted object")
	}

	// Exhaust the retry budget the way a permanently unreachable node would.
	for _, d := range claimed {
		if err := s.FailDeletion(ctx, d.ID, "node gone", time.Millisecond, 0); err != nil {
			t.Fatalf("FailDeletion: %v", err)
		}
	}

	var replicas int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM chunk_replicas r
		 JOIN chunks c ON c.id = r.chunk_id WHERE c.object_id = $1`, objID).Scan(&replicas); err != nil {
		t.Fatalf("counting replicas: %v", err)
	}
	if replicas != 0 {
		t.Fatalf("%d replica row(s) survived an abandoned deletion; the chunk can never be purged", replicas)
	}

	if _, _, err := s.PurgeReclaimedChunks(ctx, 100); err != nil {
		t.Fatalf("PurgeReclaimedChunks: %v", err)
	}
	var objects int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM objects WHERE id = $1`, objID).Scan(&objects); err != nil {
		t.Fatalf("counting objects: %v", err)
	}
	if objects != 0 {
		t.Error("the object row survived; an abandoned deletion pinned it permanently")
	}
}

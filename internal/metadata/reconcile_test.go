package metadata

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestReturningNodeReplicasAreHeldStale(t *testing.T) {
	// The core rejoin rule: a node that comes back does not get its replicas
	// trusted again just because it is answering heartbeats.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "rejoin.bin", nodes, 100, 200)

	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}

	staled, err := s.MarkReplicasStaleOnNode(ctx, nodes[0])
	if err != nil {
		t.Fatalf("MarkReplicasStaleOnNode: %v", err)
	}
	if staled != 2 {
		t.Fatalf("marked %d replicas stale, want 2", staled)
	}

	// STALE must not count towards durability...
	counts, err := s.ReplicaCountsForObject(ctx, objID)
	if err != nil {
		t.Fatalf("ReplicaCountsForObject: %v", err)
	}
	for id, n := range counts {
		if n != 2 {
			t.Fatalf("chunk %s counts %d replicas with one node unverified, want 2", id, n)
		}
	}

	// ...and must not be offered to readers.
	_, chunks, err := s.GetObject(ctx, bkt, "rejoin.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	for _, c := range chunks {
		for _, r := range c.Replicas {
			if r.NodeID == nodes[0] {
				t.Fatalf("unverified replica on %s offered as a read target", r.NodeID)
			}
		}
	}
}

func TestStaleReplicasArePagedForVerification(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	putObject(t, ctx, s, bkt, "paged.bin", nodes, 10, 20, 30, 40, 50)

	if _, err := s.MarkReplicasStaleOnNode(ctx, nodes[0]); err != nil {
		t.Fatalf("MarkReplicasStaleOnNode: %v", err)
	}
	// MarkReplicasStaleOnNode only touches UNAVAILABLE/WRITING, so demote first.
	if _, err := s.MarkReplicasUnavailableOnNode(ctx, nodes[0]); err != nil {
		t.Fatalf("MarkReplicasUnavailableOnNode: %v", err)
	}
	if _, err := s.MarkReplicasStaleOnNode(ctx, nodes[0]); err != nil {
		t.Fatalf("MarkReplicasStaleOnNode: %v", err)
	}

	// Page through with a small page size; a node holding millions of chunks
	// must never be materialised in one query.
	seen := map[uuid.UUID]bool{}
	after := uuid.Nil
	for {
		page, err := s.StaleReplicasOnNode(ctx, nodes[0], after, 2)
		if err != nil {
			t.Fatalf("StaleReplicasOnNode: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			if seen[r.ChunkID] {
				t.Fatalf("chunk %s returned twice by pagination", r.ChunkID)
			}
			seen[r.ChunkID] = true
			if r.Checksum == "" {
				t.Fatalf("chunk %s has no checksum; the reconciler could not verify it", r.ChunkID)
			}
			after = r.ChunkID
		}
		if len(page) < 2 {
			break
		}
	}
	if len(seen) != 5 {
		t.Fatalf("paged over %d stale replicas, want 5", len(seen))
	}
}

func TestPromoteVerifiedReplicaOnlyTouchesStale(t *testing.T) {
	// Promotion must never resurrect a replica demoted for another reason --
	// a CORRUPT copy is not made good by a reconciliation pass.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "promote.bin", nodes, 100)

	chunks, err := s.ChunksForObject(ctx, objID, false)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("ChunksForObject: %v", err)
	}
	chunkID := chunks[0].ID

	if _, err := s.MarkReplicaCorrupt(ctx, chunkID, nodes[0], "bad bytes"); err != nil {
		t.Fatalf("MarkReplicaCorrupt: %v", err)
	}
	promoted, err := s.PromoteVerifiedReplica(ctx, chunkID, nodes[0])
	if err != nil {
		t.Fatalf("PromoteVerifiedReplica: %v", err)
	}
	if promoted {
		t.Fatal("a CORRUPT replica was promoted to AVAILABLE")
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 2 {
		t.Fatalf("available_replicas = %d, want 2", got)
	}
}

func TestReconcileQueueIsIdempotentAndLeased(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 2)

	queued, err := s.EnqueueReconcile(ctx, nodes[0])
	if err != nil {
		t.Fatalf("EnqueueReconcile: %v", err)
	}
	if !queued {
		t.Fatal("first enqueue did not create a job")
	}
	// Re-enqueueing while one is live is a no-op: a node that flaps must not
	// accumulate a backlog of identical reconciliations.
	again, err := s.EnqueueReconcile(ctx, nodes[0])
	if err != nil {
		t.Fatalf("second EnqueueReconcile: %v", err)
	}
	if again {
		t.Fatal("duplicate reconciliation was queued for the same node")
	}

	job, err := s.ClaimReconcileJob(ctx, "worker-a", time.Minute)
	if err != nil {
		t.Fatalf("ClaimReconcileJob: %v", err)
	}
	if job.NodeID != nodes[0] {
		t.Fatalf("claimed node %s, want %s", job.NodeID, nodes[0])
	}
	if job.Address == "" {
		t.Fatal("claimed job has no address; the reconciler could not dial the node")
	}

	if _, err := s.ClaimReconcileJob(ctx, "worker-b", time.Minute); !errors.Is(err, ErrNoReconcileWork) {
		t.Fatalf("a leased reconciliation was handed to a second worker (err=%v)", err)
	}

	if err := s.CompleteReconcile(ctx, job.ID, ReconcileResult{
		ChunksSeen: 10, Verified: 8, PhantomsFound: 1, OrphansFound: 1,
	}); err != nil {
		t.Fatalf("CompleteReconcile: %v", err)
	}

	hist, err := s.RecentReconciliations(ctx, 5)
	if err != nil {
		t.Fatalf("RecentReconciliations: %v", err)
	}
	if len(hist) == 0 || hist[0].State != ReconcileSucceeded {
		t.Fatalf("reconciliation history not recorded: %+v", hist)
	}
	if hist[0].Result.Verified != 8 || hist[0].Result.PhantomsFound != 1 {
		t.Fatalf("findings not persisted: %+v", hist[0].Result)
	}
}

func TestReconcileClaimSkipsUnhealthyNodes(t *testing.T) {
	// Streaming an inventory from a node that is not answering just ties up a
	// worker for the full RPC deadline.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 1)

	if _, err := s.EnqueueReconcile(ctx, nodes[0]); err != nil {
		t.Fatalf("EnqueueReconcile: %v", err)
	}
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}

	if _, err := s.ClaimReconcileJob(ctx, "w", time.Minute); !errors.Is(err, ErrNoReconcileWork) {
		t.Fatalf("claimed a reconciliation for a DEAD node (err=%v)", err)
	}
}

func TestExpiredReconcileLeasesAreReclaimed(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 1)
	if _, err := s.EnqueueReconcile(ctx, nodes[0]); err != nil {
		t.Fatalf("EnqueueReconcile: %v", err)
	}
	job, err := s.ClaimReconcileJob(ctx, "doomed", time.Hour)
	if err != nil {
		t.Fatalf("ClaimReconcileJob: %v", err)
	}

	if _, err := s.Pool().Exec(ctx,
		`UPDATE node_reconciliations SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`,
		job.ID); err != nil {
		t.Fatalf("expiring lease: %v", err)
	}
	n, err := s.ReclaimExpiredReconcileLeases(ctx)
	if err != nil {
		t.Fatalf("ReclaimExpiredReconcileLeases: %v", err)
	}
	if n != 1 {
		t.Fatalf("reclaimed %d, want 1", n)
	}
	if _, err := s.ClaimReconcileJob(ctx, "new", time.Minute); err != nil {
		t.Fatalf("reconciliation was not reclaimable: %v", err)
	}
}

func TestOrphanDeletionSurvivesAMissingChunkRow(t *testing.T) {
	// The orphan case by definition involves a chunk row that no longer
	// exists, which is exactly why chunk_deletions has no foreign key onto
	// chunks. If it did, reconciliation could not clean up after a delete.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 1)

	ghost := uuid.New()
	queued, err := s.QueueOrphanDeletion(ctx, ghost, nodes[0])
	if err != nil {
		t.Fatalf("QueueOrphanDeletion for a nonexistent chunk: %v", err)
	}
	if !queued {
		t.Fatal("a file for a chunk metadata knows nothing about was not queued for deletion")
	}

	jobs, err := s.ClaimDeletions(ctx, 100, time.Minute)
	if err != nil {
		t.Fatalf("ClaimDeletions: %v", err)
	}
	var found bool
	for _, j := range jobs {
		if j.ChunkID == ghost {
			found = true
			if j.Address == "" {
				t.Fatal("orphan deletion job has no node address")
			}
		}
	}
	if !found {
		t.Fatal("orphan deletion was not queued")
	}
}

func TestChunkIDsKnownOnNodeReportsStates(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	putObject(t, ctx, s, bkt, "known.bin", nodes, 100, 200)

	known, err := s.ChunkIDsKnownOnNode(ctx, nodes[0])
	if err != nil {
		t.Fatalf("ChunkIDsKnownOnNode: %v", err)
	}
	if len(known) != 2 {
		t.Fatalf("node holds %d known chunks, want 2", len(known))
	}
	for id, state := range known {
		if state != ReplicaAvailable {
			t.Fatalf("chunk %s state = %s, want AVAILABLE", id, state)
		}
	}
}

func TestNodesWithStaleReplicasFindsUnreconciledNodes(t *testing.T) {
	// The self-correcting path. A reconciliation can FAIL (node still starting)
	// or be dropped by the idempotence check (node flapped while one was
	// running), and nothing would retry it. Sweeping on the *condition* rather
	// than the triggering event is what makes STALE resolve regardless.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	putObject(t, ctx, s, bucket(t), "sweep.bin", nodes, 100)

	if got, err := s.NodesWithStaleReplicas(ctx, 10); err != nil || len(got) != 0 {
		t.Fatalf("found %v stale nodes in a healthy cluster (err=%v)", got, err)
	}

	if _, err := s.MarkReplicasUnavailableOnNode(ctx, nodes[0]); err != nil {
		t.Fatalf("MarkReplicasUnavailableOnNode: %v", err)
	}
	if _, err := s.MarkReplicasStaleOnNode(ctx, nodes[0]); err != nil {
		t.Fatalf("MarkReplicasStaleOnNode: %v", err)
	}

	got, err := s.NodesWithStaleReplicas(ctx, 10)
	if err != nil {
		t.Fatalf("NodesWithStaleReplicas: %v", err)
	}
	if len(got) != 1 || got[0] != nodes[0] {
		t.Fatalf("got %v, want [%s]", got, nodes[0])
	}

	// A node that is not HEALTHY is not swept: streaming an inventory from it
	// would just tie up a worker for the full RPC deadline.
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if got, err := s.NodesWithStaleReplicas(ctx, 10); err != nil || len(got) != 0 {
		t.Fatalf("swept a DEAD node: %v (err=%v)", got, err)
	}
}

func TestRepairTargetReportsStaleReplicas(t *testing.T) {
	// A chunk whose every other node holds an unverified copy has no legal
	// repair destination. The worker needs to see that so it defers to
	// reconciliation instead of burning its retry budget on a placement that
	// cannot succeed.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	putObject(t, ctx, s, bucket(t), "stale-blocked.bin", nodes, 100)

	// Two nodes go away and come back unverified; one good copy remains.
	for _, n := range nodes[:2] {
		if _, err := s.MarkReplicasUnavailableOnNode(ctx, n); err != nil {
			t.Fatalf("MarkReplicasUnavailableOnNode: %v", err)
		}
		if _, err := s.MarkReplicasStaleOnNode(ctx, n); err != nil {
			t.Fatalf("MarkReplicasStaleOnNode: %v", err)
		}
	}

	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}
	target, err := s.ClaimRepairJob(ctx, "w", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}
	if target.StaleReplicas != 2 {
		t.Fatalf("StaleReplicas = %d, want 2", target.StaleReplicas)
	}
	if len(target.Occupied) != 3 {
		t.Fatalf("Occupied = %v, want all three nodes", target.Occupied)
	}
	if len(target.Sources) != 1 {
		t.Fatalf("Sources = %d, want the single remaining good copy", len(target.Sources))
	}
}

func TestOrphanDeletionRefusesInFlightWrites(t *testing.T) {
	// The worst thing reconciliation could do is delete a chunk the client is
	// still uploading. The inventory snapshot it works from can be seconds old,
	// so a write that landed in between looks exactly like an orphan: the file
	// is on disk and the snapshot has no replica row for it.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "in-flight.bin", "", 8<<20)
	chunkID, err := s.ReserveChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100)
	if err != nil {
		t.Fatalf("ReserveChunk: %v", err)
	}

	// The chunk is PENDING and has no replica row on this node yet -- exactly
	// the state an upload passes through between placement and commit.
	queued, err := s.QueueOrphanDeletion(ctx, chunkID, nodes[0])
	if err != nil {
		t.Fatalf("QueueOrphanDeletion: %v", err)
	}
	if queued {
		t.Fatal("queued deletion of a chunk that is still being written")
	}

	// Once committed it is a real replica, so still not an orphan.
	if err := s.RecordChunkTargets(ctx, chunkID, nodes); err != nil {
		t.Fatalf("RecordChunkTargets: %v", err)
	}
	if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 100, nodes, nil, 1); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}
	queued, err = s.QueueOrphanDeletion(ctx, chunkID, nodes[0])
	if err != nil {
		t.Fatalf("QueueOrphanDeletion after commit: %v", err)
	}
	if queued {
		t.Fatal("queued deletion of a chunk this node legitimately holds")
	}

	// But a committed chunk with no replica row on this node IS an orphan --
	// a leftover from a placement that was trimmed or repaired away.
	if err := s.DropReplica(ctx, chunkID, nodes[0]); err != nil {
		t.Fatalf("DropReplica: %v", err)
	}
	queued, err = s.QueueOrphanDeletion(ctx, chunkID, nodes[0])
	if err != nil {
		t.Fatalf("QueueOrphanDeletion for a genuine orphan: %v", err)
	}
	if !queued {
		t.Fatal("a genuine orphan was not queued for deletion")
	}
}

func TestInFlightWritesAreNotMarkedStale(t *testing.T) {
	// A WRITING replica belongs to an upload happening right now. Demoting it
	// to STALE would hand an in-flight write to the reconciler, which would
	// then find no file yet and drop the placement.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "writing.bin", "", 8<<20)
	chunkID, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	staled, err := s.MarkReplicasStaleOnNode(ctx, nodes[0])
	if err != nil {
		t.Fatalf("MarkReplicasStaleOnNode: %v", err)
	}
	if staled != 0 {
		t.Fatalf("marked %d in-flight replicas stale, want 0", staled)
	}

	known, err := s.ChunkIDsKnownOnNode(ctx, nodes[0])
	if err != nil {
		t.Fatalf("ChunkIDsKnownOnNode: %v", err)
	}
	if known[chunkID] != ReplicaWriting {
		t.Fatalf("replica state = %s, want WRITING", known[chunkID])
	}
}

package metadata

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDurabilityCounterIsMaintainedByTrigger(t *testing.T) {
	// The counter is what the repair scanner trusts. If it ever diverged from
	// the replica rows, damage would be invisible and nothing would be
	// repaired -- so the trigger's behaviour is asserted directly.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	obj, _ := s.BeginUpload(ctx, bucket(t), "counter.bin", "", 8<<20)
	chunkID, err := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if err != nil {
		t.Fatalf("AllocateChunk: %v", err)
	}

	// WRITING replicas do not count.
	if got := availableReplicas(t, ctx, s, chunkID); got != 0 {
		t.Fatalf("available_replicas = %d before commit, want 0", got)
	}

	if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 100, nodes, nil, 1); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 3 {
		t.Fatalf("available_replicas = %d after commit, want 3", got)
	}

	// Demotion decrements.
	if _, err := s.MarkReplicaCorrupt(ctx, chunkID, nodes[0], "test"); err != nil {
		t.Fatalf("MarkReplicaCorrupt: %v", err)
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 2 {
		t.Fatalf("available_replicas = %d after corruption, want 2", got)
	}

	// Deleting a replica row decrements.
	if err := s.DropReplica(ctx, chunkID, nodes[1]); err != nil {
		t.Fatalf("DropReplica: %v", err)
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 1 {
		t.Fatalf("available_replicas = %d after drop, want 1", got)
	}

	// And an audit finds no drift.
	if drift, err := s.VerifyDurabilityCounters(ctx); err != nil {
		t.Fatalf("VerifyDurabilityCounters: %v", err)
	} else if drift != 0 {
		t.Fatalf("audit corrected %d chunks; the trigger is not keeping counters exact", drift)
	}
}

func TestVerifyDurabilityCountersRepairsDrift(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	obj, _ := s.BeginUpload(ctx, bucket(t), "drift.bin", "", 8<<20)
	chunkID, _ := s.AllocateChunk(ctx, ChunkOwner{ObjectID: &obj.ID}, 0, 100, nodes)
	if _, err := s.CommitChunk(ctx, chunkID, fakeChecksum(0), 100, nodes, nil, 1); err != nil {
		t.Fatalf("CommitChunk: %v", err)
	}

	// Simulate something bypassing the write path.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE chunks SET available_replicas = 99 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("injecting drift: %v", err)
	}

	drift, err := s.VerifyDurabilityCounters(ctx)
	if err != nil {
		t.Fatalf("VerifyDurabilityCounters: %v", err)
	}
	if drift < 1 {
		t.Fatal("audit did not detect injected drift")
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 3 {
		t.Fatalf("available_replicas = %d after audit, want 3", got)
	}
}

func TestEnqueueRepairsIsIdempotent(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "damaged.bin", nodes, 100, 200)

	// Lose one node's replicas: both chunks drop to 2 of 3.
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}

	first, err := s.EnqueueRepairs(ctx, 3, 100)
	if err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}
	if first < 2 {
		t.Fatalf("enqueued %d jobs for 2 damaged chunks, want at least 2", first)
	}

	// Running the scanner again while jobs are live must not duplicate them --
	// this is what stops a short scan interval from flooding the queue.
	second, err := s.EnqueueRepairs(ctx, 3, 100)
	if err != nil {
		t.Fatalf("second EnqueueRepairs: %v", err)
	}
	if second != 0 {
		t.Fatalf("re-scan enqueued %d duplicate jobs", second)
	}

	_ = objID
}

func TestClaimRepairJobLeasesExclusively(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 4)
	putObject(t, ctx, s, bucket(t), "claim.bin", nodes[:3], 100)

	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}

	got, err := s.ClaimRepairJob(ctx, "worker-a", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}
	if got.Job.State != RepairRunning {
		t.Fatalf("claimed job state = %s, want RUNNING", got.Job.State)
	}
	if got.Job.Attempts != 1 {
		t.Fatalf("attempts = %d after first claim, want 1", got.Job.Attempts)
	}
	if got.Checksum == "" {
		t.Fatal("claimed job carries no checksum; the worker could not verify the copy")
	}
	if len(got.Sources) == 0 {
		t.Fatal("claimed job has no source replicas")
	}
	// The dead node must not be offered as a source.
	for _, src := range got.Sources {
		if src.NodeID == nodes[0] {
			t.Fatalf("dead node %s offered as a repair source", src.NodeID)
		}
	}
	// Occupied must include every node touching the chunk, so the destination
	// cannot be one that already holds a copy.
	if len(got.Occupied) < 3 {
		t.Fatalf("occupied = %v, expected all three original nodes", got.Occupied)
	}
	if got.DesiredReplicas != 1 {
		t.Fatalf("DesiredReplicas = %d, want 1", got.DesiredReplicas)
	}

	// A second worker must not get the same job.
	if _, err := s.ClaimRepairJob(ctx, "worker-b", time.Minute, 3); !errors.Is(err, ErrNoRepairWork) {
		t.Fatalf("a leased job was handed to a second worker (err=%v)", err)
	}
}

func TestExpiredRepairLeasesAreReclaimed(t *testing.T) {
	// This is what makes a coordinator crash mid-repair recoverable.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 4)
	putObject(t, ctx, s, bucket(t), "lease.bin", nodes[:3], 100)

	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}

	claimed, err := s.ClaimRepairJob(ctx, "doomed-worker", time.Hour, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}

	// Nothing to reclaim while the lease is live.
	if n, err := s.ReclaimExpiredRepairLeases(ctx); err != nil || n != 0 {
		t.Fatalf("reclaimed %d live leases (err=%v), want 0", n, err)
	}

	// Simulate the coordinator dying: age the lease out.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE repair_jobs SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`,
		claimed.Job.ID); err != nil {
		t.Fatalf("expiring lease: %v", err)
	}

	n, err := s.ReclaimExpiredRepairLeases(ctx)
	if err != nil {
		t.Fatalf("ReclaimExpiredRepairLeases: %v", err)
	}
	if n != 1 {
		t.Fatalf("reclaimed %d jobs, want 1", n)
	}

	// And it is claimable again -- the repair work was not lost.
	again, err := s.ClaimRepairJob(ctx, "new-worker", time.Minute, 3)
	if err != nil {
		t.Fatalf("job was not reclaimable after lease expiry: %v", err)
	}
	if again.Job.ID != claimed.Job.ID {
		t.Fatalf("claimed a different job (%d vs %d)", again.Job.ID, claimed.Job.ID)
	}
	if again.Job.Attempts != 2 {
		t.Fatalf("attempts = %d on reclaim, want 2", again.Job.Attempts)
	}
}

func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	// SELECT ... FOR UPDATE SKIP LOCKED is the entire concurrency model; if it
	// ever handed one job to two workers they would both copy the same chunk.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 5)
	bkt := bucket(t)
	for i := 0; i < 6; i++ {
		putObject(t, ctx, s, bkt, "concurrent-"+string(rune('a'+i))+".bin", nodes[:3], 100)
	}
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 100); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}

	const workers = 8
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				got, err := s.ClaimRepairJob(context.Background(), "w", time.Minute, 3)
				if errors.Is(err, ErrNoRepairWork) {
					return
				}
				if err != nil {
					return
				}
				mu.Lock()
				seen[got.Job.ID]++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	for id, n := range seen {
		if n > 1 {
			t.Fatalf("job %d was claimed %d times concurrently", id, n)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no jobs were claimed at all")
	}
	t.Logf("%d jobs claimed exactly once across %d concurrent workers", len(seen), workers)
}

func TestFailRepairBacksOffThenGivesUp(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 4)
	putObject(t, ctx, s, bucket(t), "failing.bin", nodes[:3], 100)
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}
	claimed, err := s.ClaimRepairJob(ctx, "w", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}

	terminal, err := s.FailRepair(ctx, claimed.Job.ID, "nowhere to put it", 0, 5)
	if err != nil {
		t.Fatalf("FailRepair: %v", err)
	}
	if terminal {
		t.Fatal("job went terminal on its first failure")
	}
	if got := repairJobState(t, ctx, s, claimed.Job.ID); got != string(RepairPending) {
		t.Fatalf("state = %s after a retryable failure, want PENDING", got)
	}

	// Past the attempt cap it becomes FAILED and stops consuming workers.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE repair_jobs SET attempts = 5 WHERE id = $1`, claimed.Job.ID); err != nil {
		t.Fatalf("bumping attempts: %v", err)
	}
	terminal, err = s.FailRepair(ctx, claimed.Job.ID, "still nowhere", 0, 5)
	if err != nil {
		t.Fatalf("FailRepair at cap: %v", err)
	}
	if !terminal {
		t.Fatal("job did not go terminal at the attempt cap")
	}
	if got := repairJobState(t, ctx, s, claimed.Job.ID); got != string(RepairFailed) {
		t.Fatalf("state = %s at the cap, want FAILED", got)
	}

	// A failed job must not block a fresh one for the same chunk once requeued.
	n, err := s.RequeueFailedRepairs(ctx, 10)
	if err != nil {
		t.Fatalf("RequeueFailedRepairs: %v", err)
	}
	if n != 1 {
		t.Fatalf("requeued %d jobs, want 1", n)
	}
	if got := repairJobState(t, ctx, s, claimed.Job.ID); got != string(RepairPending) {
		t.Fatalf("state = %s after requeue, want PENDING", got)
	}
}

func TestCompleteRepairIsIdempotent(t *testing.T) {
	// A worker that copied successfully but crashed before closing the job
	// must not double-count the replica when the job is retried.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 4)
	bkt := bucket(t)
	putObject(t, ctx, s, bkt, "idem.bin", nodes[:3], 100)
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}
	claimed, err := s.ClaimRepairJob(ctx, "w", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}

	first, err := s.CompleteRepair(ctx, claimed.Job.ID, claimed.Job.ChunkID, nodes[3])
	if err != nil {
		t.Fatalf("CompleteRepair: %v", err)
	}
	second, err := s.CompleteRepair(ctx, claimed.Job.ID, claimed.Job.ChunkID, nodes[3])
	if err != nil {
		t.Fatalf("repeated CompleteRepair: %v", err)
	}
	if first != second {
		t.Fatalf("replica count changed on a repeated completion: %d then %d", first, second)
	}
	if first != 3 {
		t.Fatalf("available replicas = %d after repair, want 3", first)
	}
}

func TestUnderReplicatedExcludesChunksWithNoSource(t *testing.T) {
	// A chunk with zero usable replicas is reported as unavailable, not merely
	// under-replicated -- and it is deliberately NOT enqueued for repair, since
	// there is nothing to copy from.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	putObject(t, ctx, s, bkt, "doomed.bin", nodes, 100)

	for _, id := range nodes {
		if _, err := s.SetNodeHealth(ctx, id, "DEAD"); err != nil {
			t.Fatalf("SetNodeHealth: %v", err)
		}
	}

	under, unavailable, err := s.DurabilityCounts(ctx, 3)
	if err != nil {
		t.Fatalf("DurabilityCounts: %v", err)
	}
	if under < 1 || unavailable < 1 {
		t.Fatalf("under=%d unavailable=%d, expected both >= 1", under, unavailable)
	}

	// EnqueueRepairs skips available_replicas = 0 because a repair with no
	// source would just fail and burn retries.
	before := repairJobCount(t, ctx, s)
	if _, err := s.EnqueueRepairs(ctx, 3, 100); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}
	if after := repairJobCount(t, ctx, s); after != before {
		t.Fatalf("enqueued %d jobs for chunks with no source", after-before)
	}
}

func TestMarkReplicaCorruptQueuesRemoval(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "corrupt.bin", nodes, 100)

	chunks, err := s.ChunksForObject(ctx, objID, false)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("ChunksForObject: %v (%d chunks)", err, len(chunks))
	}
	chunkID := chunks[0].ID

	remaining, err := s.MarkReplicaCorrupt(ctx, chunkID, nodes[0], "sha mismatch on read")
	if err != nil {
		t.Fatalf("MarkReplicaCorrupt: %v", err)
	}
	if remaining != 2 {
		t.Fatalf("remaining = %d, want 2", remaining)
	}

	// The corrupt file must be queued for removal, or it lingers and keeps
	// occupying the node in placement decisions.
	var queued bool
	if err := s.Pool().QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM chunk_deletions WHERE chunk_id = $1 AND node_id = $2)`,
		chunkID, nodes[0]).Scan(&queued); err != nil {
		t.Fatalf("checking deletion queue: %v", err)
	}
	if !queued {
		t.Fatal("corrupt replica was not queued for deletion")
	}

	// The read path must stop offering it.
	_, readable, err := s.GetObject(ctx, bkt, "corrupt.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	for _, r := range readable[0].Replicas {
		if r.NodeID == nodes[0] {
			t.Fatal("a corrupt replica is still offered as a read target")
		}
	}
}

// ---- helpers -------------------------------------------------------------

func availableReplicas(t *testing.T, ctx context.Context, s *Store, chunkID uuid.UUID) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(ctx,
		`SELECT available_replicas FROM chunks WHERE id = $1`, chunkID).Scan(&n); err != nil {
		t.Fatalf("reading available_replicas: %v", err)
	}
	return n
}

func repairJobState(t *testing.T, ctx context.Context, s *Store, id int64) string {
	t.Helper()
	var state string
	if err := s.Pool().QueryRow(ctx,
		`SELECT state FROM repair_jobs WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatalf("reading repair job state: %v", err)
	}
	return state
}

func repairJobCount(t *testing.T, ctx context.Context, s *Store) int {
	t.Helper()
	var n int
	if err := s.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM repair_jobs`).Scan(&n); err != nil {
		t.Fatalf("counting repair jobs: %v", err)
	}
	return n
}

func TestTrimOverReplicatedChunks(t *testing.T) {
	// Recovery leaves chunks at RF+1: the dead node's replicas were rebuilt
	// elsewhere, then the node came back and its copies were verified. Without
	// trimming, every node failure permanently inflates storage.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 5)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "over.bin", nodes[:3], 100)

	chunks, err := s.ChunksForObject(ctx, objID, false)
	if err != nil || len(chunks) != 1 {
		t.Fatalf("ChunksForObject: %v", err)
	}
	chunkID := chunks[0].ID

	// Simulate a repair having added two more copies.
	for _, extra := range nodes[3:] {
		if _, err := s.CompleteRepairForTest(ctx, chunkID, extra); err != nil {
			t.Fatalf("adding replica on %s: %v", extra, err)
		}
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 5 {
		t.Fatalf("available_replicas = %d, want 5 before trimming", got)
	}

	trimmed, err := s.TrimOverReplicatedChunks(ctx, 3, 50)
	if err != nil {
		t.Fatalf("TrimOverReplicatedChunks: %v", err)
	}
	if trimmed != 2 {
		t.Fatalf("trimmed %d replicas, want 2", trimmed)
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 3 {
		t.Fatalf("available_replicas = %d after trimming, want exactly the replication factor", got)
	}

	// The excess files must actually be reclaimed, not just forgotten.
	var queued int
	if err := s.Pool().QueryRow(ctx,
		`SELECT COUNT(*) FROM chunk_deletions WHERE chunk_id = $1`, chunkID).Scan(&queued); err != nil {
		t.Fatalf("counting deletions: %v", err)
	}
	if queued != 2 {
		t.Fatalf("%d excess replicas queued for deletion, want 2", queued)
	}

	// Idempotent: a second pass has nothing to do and must not go below RF.
	again, err := s.TrimOverReplicatedChunks(ctx, 3, 50)
	if err != nil {
		t.Fatalf("second trim: %v", err)
	}
	if again != 0 {
		t.Fatalf("second trim removed %d more replicas", again)
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 3 {
		t.Fatalf("available_replicas = %d after a second trim, want 3", got)
	}
}

func TestTrimNeverGoesBelowReplicationFactor(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	objID := putObject(t, ctx, s, bkt, "exact.bin", nodes, 100)

	chunks, _ := s.ChunksForObject(ctx, objID, false)
	chunkID := chunks[0].ID

	trimmed, err := s.TrimOverReplicatedChunks(ctx, 3, 50)
	if err != nil {
		t.Fatalf("TrimOverReplicatedChunks: %v", err)
	}
	if trimmed != 0 {
		t.Fatalf("trimmed %d replicas from an exactly-replicated chunk", trimmed)
	}
	if got := availableReplicas(t, ctx, s, chunkID); got != 3 {
		t.Fatalf("available_replicas = %d, want 3", got)
	}
}

func TestResolvedFailedJobsArePurged(t *testing.T) {
	// A repair usually fails because there was nowhere to put a copy. Once the
	// chunk is healthy again by another route, that FAILED row is stale
	// history. Leaving it would keep repair_jobs_failed permanently non-zero
	// and make the "repairs are stuck" alert fire forever after any transient
	// shortage.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 4)
	putObject(t, ctx, s, bucket(t), "resolved.bin", nodes[:3], 100)

	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}
	claimed, err := s.ClaimRepairJob(ctx, "w", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}

	// Drive it to terminal failure.
	if _, err := s.Pool().Exec(ctx, `UPDATE repair_jobs SET attempts = 99 WHERE id = $1`, claimed.Job.ID); err != nil {
		t.Fatalf("bumping attempts: %v", err)
	}
	if _, err := s.FailRepair(ctx, claimed.Job.ID, "nowhere to place", 0, 6); err != nil {
		t.Fatalf("FailRepair: %v", err)
	}

	// While the chunk is still damaged the failure must be retained: it is a
	// real outstanding problem an operator should see.
	if _, err := s.PurgeFinishedRepairJobs(ctx, time.Hour, 3, 100); err != nil {
		t.Fatalf("PurgeFinishedRepairJobs: %v", err)
	}
	if got := repairJobState(t, ctx, s, claimed.Job.ID); got != string(RepairFailed) {
		t.Fatalf("a failed job for a still-damaged chunk was purged (state=%s)", got)
	}

	// Now heal the chunk by another route.
	if _, err := s.CompleteRepairForTest(ctx, claimed.Job.ChunkID, nodes[3]); err != nil {
		t.Fatalf("adding replica: %v", err)
	}
	if got := availableReplicas(t, ctx, s, claimed.Job.ChunkID); got != 3 {
		t.Fatalf("available_replicas = %d, want 3", got)
	}

	if _, err := s.PurgeFinishedRepairJobs(ctx, time.Hour, 3, 100); err != nil {
		t.Fatalf("PurgeFinishedRepairJobs: %v", err)
	}
	counts, err := s.RepairCounts(ctx)
	if err != nil {
		t.Fatalf("RepairCounts: %v", err)
	}
	if counts[RepairFailed] != 0 {
		t.Fatalf("%d FAILED jobs remain for a healthy chunk", counts[RepairFailed])
	}
}

// TestTransientOccupancyDistinguishesBlockedFromImpossible pins down the signal
// the repair manager uses to decide between "retry this" and "wait for
// something else to finish". A chunk whose remaining nodes all hold CORRUPT or
// UNAVAILABLE copies has nowhere to put a replica *right now*, but the GC and
// the health monitor will free those nodes shortly -- so the job must be
// deferred, not retried into FAILED.
func TestTransientOccupancyDistinguishesBlockedFromImpossible(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	putObject(t, ctx, s, bucket(t), "occupied.bin", nodes, 100)

	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}

	got, err := s.ClaimRepairJob(ctx, "worker-a", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob: %v", err)
	}
	// Marking the node DEAD demoted its replica to UNAVAILABLE: exactly one
	// transiently occupied node, and nothing STALE (nothing has rejoined).
	if got.TransientOccupancy != 1 {
		t.Fatalf("TransientOccupancy = %d after one node died, want 1", got.TransientOccupancy)
	}
	if got.StaleReplicas != 0 {
		t.Fatalf("StaleReplicas = %d, want 0", got.StaleReplicas)
	}
	if len(got.Occupied) != 3 {
		t.Fatalf("Occupied = %v, want all three nodes: a destination must avoid them all",
			got.Occupied)
	}

	// Corrupt a second replica, so two of the three nodes are now blocked by
	// rows that the GC and the health monitor will clear on their own.
	if _, err := s.MarkReplicaCorrupt(ctx, got.Job.ChunkID, nodes[1], "test"); err != nil {
		t.Fatalf("MarkReplicaCorrupt: %v", err)
	}
	if err := s.FinishRepairAsNoop(ctx, got.Job.ID, "deferred by test"); err != nil {
		t.Fatalf("FinishRepairAsNoop: %v", err)
	}
	if _, err := s.EnqueueRepairs(ctx, 3, 10); err != nil {
		t.Fatalf("EnqueueRepairs: %v", err)
	}

	got, err = s.ClaimRepairJob(ctx, "worker-b", time.Minute, 3)
	if err != nil {
		t.Fatalf("ClaimRepairJob after corruption: %v", err)
	}
	if got.TransientOccupancy != 2 {
		t.Fatalf("TransientOccupancy = %d with one dead and one corrupt replica, want 2",
			got.TransientOccupancy)
	}
	// The single surviving good copy is still a usable source, so this is a
	// placement problem, not a data-loss problem.
	if len(got.Sources) != 1 {
		t.Fatalf("Sources = %d, want the one replica that is still AVAILABLE", len(got.Sources))
	}
}

// TestTrimNeverGoesBelowTheReplicationFactor is a regression test for a data-loss
// bug, and it deliberately lies to the trimmer to prove the guarantee is
// structural rather than arithmetic.
//
// The trimmer used to compute "how many to drop" from the denormalised
// chunks.available_replicas counter while selecting the rows to drop from
// chunk_replicas. When those two disagree -- which is exactly the drift
// VerifyDurabilityCounters exists to catch -- the LIMIT could cover every
// AVAILABLE replica and a routine trim would delete the only copies of a chunk.
//
// Here the counter is corrupted to claim far more replicas than exist. A
// count-based trim would demote all three; a rank-based one demotes none.
// TestTrimIsSafeAgainstADriftedCounter drives the counter out of sync on
// purpose: even if available_replicas lies high, trimming must never demote a
// replica the chunk actually needs.
func TestTrimIsSafeAgainstADriftedCounter(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt := bucket(t)
	putObject(t, ctx, s, bkt, "trim-safety.bin", nodes, 100)

	var chunkID uuid.UUID
	if err := s.Pool().QueryRow(ctx,
		`SELECT c.id FROM chunks c JOIN objects o ON o.id = c.object_id
		 WHERE o.bucket = $1 AND o.key = $2`, bkt, "trim-safety.bin").Scan(&chunkID); err != nil {
		t.Fatalf("locating the chunk: %v", err)
	}

	// Drift the counter high, as a lost trigger or an out-of-band write would.
	if _, err := s.Pool().Exec(ctx,
		`UPDATE chunks SET available_replicas = 9 WHERE id = $1`, chunkID); err != nil {
		t.Fatalf("corrupting the durability counter: %v", err)
	}

	if _, err := s.TrimOverReplicatedChunks(ctx, 3, 10); err != nil {
		t.Fatalf("TrimOverReplicatedChunks: %v", err)
	}

	var available int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM chunk_replicas WHERE chunk_id = $1 AND state = 'AVAILABLE'`,
		chunkID).Scan(&available); err != nil {
		t.Fatalf("counting replicas: %v", err)
	}
	if available != 3 {
		t.Fatalf("trim left %d AVAILABLE replicas, want 3: a drifted counter must not be able to destroy data", available)
	}
}

// TestTrimRemovesOnlyTheSurplus checks the trimmer still does its job: a
// genuinely over-replicated chunk comes back down to exactly RF.
func TestTrimRemovesOnlyTheSurplus(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 5)
	bkt := bucket(t)
	putObject(t, ctx, s, bkt, "trim-surplus.bin", nodes, 100)

	var chunkID uuid.UUID
	if err := s.Pool().QueryRow(ctx,
		`SELECT c.id FROM chunks c JOIN objects o ON o.id = c.object_id
		 WHERE o.bucket = $1 AND o.key = $2`, bkt, "trim-surplus.bin").Scan(&chunkID); err != nil {
		t.Fatalf("locating the chunk: %v", err)
	}

	if _, err := s.TrimOverReplicatedChunks(ctx, 3, 10); err != nil {
		t.Fatalf("TrimOverReplicatedChunks: %v", err)
	}
	var available int
	if err := s.Pool().QueryRow(ctx,
		`SELECT count(*) FROM chunk_replicas WHERE chunk_id = $1 AND state = 'AVAILABLE'`,
		chunkID).Scan(&available); err != nil {
		t.Fatalf("counting replicas: %v", err)
	}
	if available != 3 {
		t.Fatalf("trim left %d AVAILABLE replicas of a 5-replica chunk, want exactly 3", available)
	}
}

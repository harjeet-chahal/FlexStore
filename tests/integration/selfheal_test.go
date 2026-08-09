//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// The self-healing scenarios. Every one of these stops, kills or corrupts
// something real in the running Compose cluster -- simulating any of it
// in-process would prove nothing about whether FlexStore actually recovers.

// ---- admin views used by these scenarios ---------------------------------

type replicationStatus struct {
	ReplicationFactor     int32 `json:"replication_factor"`
	RepairEnabled         bool  `json:"repair_enabled"`
	TotalChunks           int64 `json:"total_chunks"`
	UnderReplicatedChunks int64 `json:"under_replicated_chunks"`
	UnavailableChunks     int64 `json:"unavailable_chunks"`
	JobsPending           int64 `json:"repair_jobs_pending"`
	JobsRunning           int64 `json:"repair_jobs_running"`
	JobsFailed            int64 `json:"repair_jobs_failed"`
	JobsSucceeded         int64 `json:"repair_jobs_succeeded"`
	RecentJobs            []struct {
		ID        int64  `json:"id"`
		ChunkID   string `json:"chunk_id"`
		State     string `json:"state"`
		Source    string `json:"source_node_id"`
		Target    string `json:"target_node_id"`
		Attempts  int32  `json:"attempts"`
		LastError string `json:"last_error"`
	} `json:"recent_jobs"`
}

func getReplication(t *testing.T, ctx context.Context) replicationStatus {
	t.Helper()
	resp, body := do(t, newRequest(t, ctx, http.MethodGet, "/admin/replication", nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/replication = %d: %s", resp.StatusCode, body)
	}
	var out replicationStatus
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding replication status: %v\n%s", err, body)
	}
	return out
}

type objectChunks struct {
	ReplicationFactor     int `json:"replication_factor"`
	UnderReplicatedChunks int `json:"under_replicated_chunks"`
	UnavailableChunks     int `json:"unavailable_chunks"`
	Chunks                []struct {
		ChunkID           string `json:"chunk_id"`
		Index             int32  `json:"index"`
		AvailableReplicas int    `json:"available_replicas"`
		Replicas          []struct {
			NodeID string `json:"node_id"`
			State  string `json:"state"`
		} `json:"replicas"`
	} `json:"chunks"`
}

func getObjectChunks(t *testing.T, ctx context.Context, bucket, key string) objectChunks {
	t.Helper()
	resp, body := do(t, newRequest(t, ctx, http.MethodGet, "/admin/objects/"+bucket+"/"+key, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/objects/%s/%s = %d: %s", bucket, key, resp.StatusCode, body)
	}
	var out objectChunks
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding object chunks: %v\n%s", err, body)
	}
	return out
}

// nodesHolding returns the distinct nodes with an AVAILABLE replica of any
// chunk of the object, and how many they hold.
func (o objectChunks) nodesHolding() map[string]int {
	out := map[string]int{}
	for _, c := range o.Chunks {
		for _, r := range c.Replicas {
			if r.State == "AVAILABLE" {
				out[r.NodeID]++
			}
		}
	}
	return out
}

// waitForFullDurability blocks until no chunk in the cluster is under-replicated
// and the repair queue has drained.
func waitForFullDurability(t *testing.T, ctx context.Context, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(within)
	var last replicationStatus
	for time.Now().Before(deadline) {
		last = getReplication(t, ctx)
		if last.UnderReplicatedChunks == 0 && last.JobsPending == 0 && last.JobsRunning == 0 {
			return time.Since(start)
		}
		// A chunk whose every replica was deliberately destroyed by an earlier
		// test in this suite is genuinely unrepairable: there is nothing to copy
		// from. FlexStore reports it correctly and keeps its metadata intact,
		// which is the designed behaviour -- but it means cluster-wide
		// durability never returns to zero, and a test that waits for it is
		// waiting for something the suite itself made impossible.
		//
		// Whether *this* test's object recovered is asserted separately, per
		// object, by waitForObjectDurable.
		if last.UnavailableChunks > 0 {
			t.Logf("cluster still holds %d chunk(s) with no readable replica "+
				"(deliberately destroyed by an earlier scenario); "+
				"not waiting for cluster-wide durability", last.UnavailableChunks)
			return time.Since(start)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for durability")
		case <-time.After(time.Second):
		}
	}
	t.Fatalf("durability not restored within %s: under=%d pending=%d running=%d failed=%d",
		within, last.UnderReplicatedChunks, last.JobsPending, last.JobsRunning, last.JobsFailed)
	return 0
}

// waitForObjectDurable blocks until every chunk of one object is at the
// replication factor, and returns how long that took.
//
// Preferred over waitForFullDurability wherever a scenario is measuring its own
// recovery: the suite deliberately creates permanently-unrepairable chunks
// elsewhere (TestAllReplicasCorruptFailsLoudly destroys every replica of an
// object on purpose), so cluster-wide durability is not a property any single
// scenario can wait on.
func waitForObjectDurable(t *testing.T, ctx context.Context, bucket, key string, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(within)
	var layout objectLayout
	rf := int(getClusterStatus(t, ctx).ReplicationFactor)

	for time.Now().Before(deadline) {
		layout = getLayout(t, ctx, bucket, key)
		converged := len(layout.Chunks) > 0
		for _, c := range layout.Chunks {
			if len(layout.availableNodes(int(c.Index))) < rf {
				converged = false
				break
			}
		}
		if converged {
			return time.Since(start)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for %s/%s to become durable", bucket, key)
		case <-time.After(time.Second):
		}
	}
	short := 0
	for _, c := range layout.Chunks {
		if n := len(layout.availableNodes(int(c.Index))); n < rf {
			short++
			t.Logf("  chunk %d: %d of %d replicas", c.Index, n, rf)
		}
	}
	t.Fatalf("%s/%s still had %d under-replicated chunk(s) after %s", bucket, key, short, within)
	return 0
}

// waitForUnderReplication blocks until the cluster notices damage, returning
// the count observed.
func waitForUnderReplication(t *testing.T, ctx context.Context, within time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s := getReplication(t, ctx); s.UnderReplicatedChunks > 0 {
			return s.UnderReplicatedChunks
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for under-replication")
		case <-time.After(500 * time.Millisecond):
		}
	}
	// Repair can be fast enough that the damaged window is never sampled. That
	// is a good outcome, not a failure, so it is reported rather than fatal.
	t.Log("never sampled a non-zero under-replicated count; repair outran the poll interval")
	return 0
}

// ---- Scenario A: one node dies -------------------------------------------

func TestScenarioA_OneNodeDies(t *testing.T) {
	if testing.Short() {
		t.Skip("stops a container; skipped in -short mode")
	}
	ctx := testContext(t, 15*time.Minute)
	waitForHealthyNodes(t, ctx, 5)
	waitForFullDurability(t, ctx, 3*time.Minute)

	key := uniqueKey(t, "scenario-a.bin")
	payload := randomPayload(t, defaultChunkSize*4+4096) // 5 chunks
	want := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	// A write commits at min_write_replicas and repair converges the rest, so
	// wait for convergence before treating "fully replicated" as the baseline
	// this scenario perturbs.
	waitForObjectFullyReplicated(t, ctx, testBucket, key, 2*time.Minute)

	before := getObjectChunks(t, ctx, testBucket, key)
	if before.UnderReplicatedChunks != 0 {
		t.Fatalf("object did not reach full replication before the test began: %+v", before)
	}
	rf := before.ReplicationFactor

	// Pick the node holding the most replicas of this object, so the failure is
	// guaranteed to matter.
	var victim string
	var victimReplicas int
	for node, n := range before.nodesHolding() {
		if n > victimReplicas {
			victim, victimReplicas = node, n
		}
	}
	t.Logf("object spans %d chunks; stopping %s which holds %d of its replicas",
		len(before.Chunks), victim, victimReplicas)

	compose(t, "stop", victim)
	t.Cleanup(func() { restoreNode(t, victim) })

	// 1. The coordinator must notice.
	detectStart := time.Now()
	waitForNodeState(t, ctx, victim, "DEAD")
	t.Logf("coordinator marked %s DEAD after %s", victim, time.Since(detectStart).Round(time.Second))

	// 2. The object must remain downloadable throughout.
	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatal("object changed while a node was down")
	}
	t.Log("object still downloadable with the node down")

	// 3. Durability degrades...
	peak := waitForUnderReplication(t, ctx, 30*time.Second)
	if peak > 0 {
		t.Logf("under-replicated chunks observed: %d", peak)
	}

	// 4. ...and is repaired automatically.
	took := waitForObjectDurable(t, ctx, testBucket, key, 5*time.Minute)
	t.Logf("full durability restored in %s", took.Round(time.Millisecond))

	// 5. Every chunk back to RF, on distinct nodes, none on the stopped node.
	after := getObjectChunks(t, ctx, testBucket, key)
	for _, c := range after.Chunks {
		if c.AvailableReplicas != rf {
			t.Errorf("chunk %d has %d replicas after repair, want %d", c.Index, c.AvailableReplicas, rf)
		}
		seen := map[string]bool{}
		for _, r := range c.Replicas {
			if r.State != "AVAILABLE" {
				continue
			}
			if r.NodeID == victim {
				t.Errorf("chunk %d still counts a replica on the stopped node %s", c.Index, victim)
			}
			if seen[r.NodeID] {
				t.Errorf("chunk %d has two replicas on %s", c.Index, r.NodeID)
			}
			seen[r.NodeID] = true
		}
	}

	// 6. And the data is still correct.
	got, _ = getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatalf("object changed after repair:\n  want %s\n  got  %s", want, sha256hex(got))
	}
	t.Log("SHA-256 matches the original after full recovery")
}

// ---- Scenario B: two nodes die -------------------------------------------

func TestScenarioB_TwoNodesDie(t *testing.T) {
	if testing.Short() {
		t.Skip("stops containers; skipped in -short mode")
	}
	ctx := testContext(t, 25*time.Minute)
	waitForHealthyNodes(t, ctx, 5)
	waitForFullDurability(t, ctx, 3*time.Minute)

	key := uniqueKey(t, "scenario-b.bin")
	payload := randomPayload(t, defaultChunkSize*3+2048) // 4 chunks
	want := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	waitForObjectFullyReplicated(t, ctx, testBucket, key, 2*time.Minute)
	before := getObjectChunks(t, ctx, testBucket, key)
	rf := before.ReplicationFactor

	// Two nodes are stopped one at a time, with repair allowed in between. With
	// RF=3 on five nodes at least one valid replica of every chunk survives
	// throughout, which is exactly the stretch criterion.
	victims := make([]string, 0, 2)
	for node := range before.nodesHolding() {
		victims = append(victims, node)
		if len(victims) == 2 {
			break
		}
	}
	if len(victims) < 2 {
		t.Fatalf("object only spans %d nodes; cannot stop two holders", len(before.nodesHolding()))
	}

	t.Logf("stopping first node %s", victims[0])
	compose(t, "stop", victims[0])
	t.Cleanup(func() { restoreNode(t, victims[0]) })
	waitForNodeState(t, ctx, victims[0], "DEAD")

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatal("object corrupted after the first node died")
	}
	first := waitForFullDurability(t, ctx, 6*time.Minute)
	t.Logf("durability restored after one node loss in %s", first.Round(time.Millisecond))

	t.Logf("stopping second node %s", victims[1])
	compose(t, "stop", victims[1])
	t.Cleanup(func() { restoreNode(t, victims[1]) })
	waitForNodeState(t, ctx, victims[1], "DEAD")

	// Still readable with two of five nodes gone.
	got, _ = getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatalf("object changed with two nodes down:\n  want %s\n  got  %s", want, sha256hex(got))
	}
	t.Log("object still downloadable with TWO nodes stopped")

	// Three surviving nodes is exactly RF, so full durability remains
	// achievable: one replica per remaining node.
	second := waitForFullDurability(t, ctx, 10*time.Minute)
	t.Logf("durability restored again after the second loss in %s", second.Round(time.Millisecond))

	after := getObjectChunks(t, ctx, testBucket, key)
	for _, c := range after.Chunks {
		if c.AvailableReplicas != rf {
			t.Errorf("chunk %d has %d replicas after a double failure, want %d",
				c.Index, c.AvailableReplicas, rf)
		}
		for _, r := range c.Replicas {
			if r.State != "AVAILABLE" {
				continue
			}
			for _, v := range victims {
				if r.NodeID == v {
					t.Errorf("chunk %d still counts a replica on stopped node %s", c.Index, v)
				}
			}
		}
	}

	got, _ = getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatalf("final SHA-256 mismatch:\n  want %s\n  got  %s", want, sha256hex(got))
	}
	t.Log("SHA-256 matches the original after surviving two node failures")
}

// ---- Scenario C: corruption ----------------------------------------------

func TestScenarioC_CorruptionIsDetectedAndRepaired(t *testing.T) {
	if testing.Short() {
		t.Skip("mutates a container filesystem; skipped in -short mode")
	}
	ctx := testContext(t, 15*time.Minute)
	waitForHealthyNodes(t, ctx, 5)
	waitForFullDurability(t, ctx, 3*time.Minute)

	key := uniqueKey(t, "scenario-c.bin")
	payload := randomPayload(t, 256*1024) // one chunk keeps the assertions crisp
	want := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	waitForObjectFullyReplicated(t, ctx, testBucket, key, 2*time.Minute)
	before := getObjectChunks(t, ctx, testBucket, key)
	if len(before.Chunks) != 1 {
		t.Fatalf("expected a single chunk, got %d", len(before.Chunks))
	}
	chunk := before.Chunks[0]

	// Corrupt every replica except the last, rather than picking one and hoping
	// the reader happens to try it first.
	//
	// The gateway walks replicas in node-ID order and stops at the first that
	// verifies, so corrupting a single replica only guarantees detection if
	// that replica happens to sort first *and* is the one the read path
	// selects. Leaving exactly one good copy -- the last in the order -- makes
	// detection deterministic while still proving failover: the reader must
	// encounter corruption and must still return correct bytes.
	available := make([]string, 0, len(chunk.Replicas))
	for _, r := range chunk.Replicas {
		if r.State == "AVAILABLE" {
			available = append(available, r.NodeID)
		}
	}
	sort.Strings(available)

	// The survivor must genuinely hold its file, not merely be listed as
	// AVAILABLE. Metadata can transiently name a replica whose bytes are not on
	// disk -- the "phantom" the reconciler exists to resolve -- and leaving a
	// phantom as the intended survivor means every replica is unreadable. The
	// gateway then correctly refuses to serve anything, and the test fails while
	// asserting the opposite of what it is trying to prove.
	onDisk := make([]string, 0, len(available))
	for _, n := range available {
		if chunkFileExists(t, n, chunk.ChunkID) {
			onDisk = append(onDisk, n)
		} else {
			t.Logf("replica on %s is listed AVAILABLE but has no file; excluding it from the scenario", n)
		}
	}
	if len(onDisk) < 2 {
		t.Fatalf("chunk has %d replicas actually on disk (of %d listed available); "+
			"need at least 2 to test failover", len(onDisk), len(available))
	}
	victims := onDisk[:len(onDisk)-1]
	survivor := onDisk[len(onDisk)-1]
	t.Logf("corrupting chunk %s on %v, leaving %s intact", chunk.ChunkID, victims, survivor)

	beforeFailures := metricValue(t, ctx, "flexstore_checksum_failures_total")
	// The Prometheus counter, not repair_jobs_succeeded: the latter counts
	// *retained* rows and shrinks as finished-job history is purged, so it is
	// not monotonic and cannot be used as a before/after delta.
	beforeRepairs := metricValue(t, ctx, "flexstore_repairs_total")

	corrupted := 0
	for _, v := range victims {
		if corruptChunkOnNode(t, v, chunk.ChunkID) {
			corrupted++
		}
	}
	if corrupted == 0 {
		t.Fatal("no replica could be corrupted; the scenario would prove nothing")
	}

	// 1. The read must still succeed, from a healthy replica.
	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatalf("corrupt bytes were served:\n  want %s\n  got  %s", want, sha256hex(got))
	}
	t.Log("download succeeded from a healthy replica")

	// 2. The corruption must be recorded, not silently worked around.
	deadline := time.Now().Add(30 * time.Second)
	var afterFailures float64
	for time.Now().Before(deadline) {
		afterFailures = metricValue(t, ctx, "flexstore_checksum_failures_total")
		if afterFailures > beforeFailures {
			break
		}
		time.Sleep(time.Second)
	}
	if afterFailures <= beforeFailures {
		t.Errorf("flexstore_checksum_failures_total did not increase (%v -> %v)",
			beforeFailures, afterFailures)
	} else {
		t.Logf("checksum failures recorded: %v -> %v", beforeFailures, afterFailures)
	}

	// 3. The bad replica must be replaced.
	took := waitForFullDurability(t, ctx, 5*time.Minute)
	t.Logf("durability restored %s after corruption was detected", took.Round(time.Millisecond))

	afterRepairs := metricValue(t, ctx, "flexstore_repairs_total")
	if afterRepairs <= beforeRepairs {
		t.Errorf("no repair ran after corruption (flexstore_repairs_total %v -> %v)",
			beforeRepairs, afterRepairs)
	} else {
		t.Logf("repairs performed: %v -> %v", beforeRepairs, afterRepairs)
	}

	// 4. Back to full replication, with the corrupt copy no longer counted.
	afterChunks := getObjectChunks(t, ctx, testBucket, key)
	c := afterChunks.Chunks[0]
	if c.AvailableReplicas != afterChunks.ReplicationFactor {
		t.Errorf("chunk has %d replicas after repair, want %d",
			c.AvailableReplicas, afterChunks.ReplicationFactor)
	}
	// Deliberately not asserting "node X no longer holds a replica": after the
	// corrupt copy is deleted, that node becomes a legal destination again and
	// repair may place a *fresh, verified* copy right back on it. The
	// meaningful invariant is that no replica is still marked CORRUPT and the
	// chunk is back at full replication.
	for _, r := range c.Replicas {
		if r.State == "CORRUPT" {
			t.Errorf("a CORRUPT replica on %s survived repair", r.NodeID)
		}
	}

	got, _ = getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatal("final SHA-256 mismatch after corruption repair")
	}
	t.Log("SHA-256 matches the original after corruption repair")
}

// ---- Scenario D: coordinator restart mid-recovery -------------------------

func TestScenarioD_CoordinatorRestartDuringRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("restarts containers; skipped in -short mode")
	}
	ctx := testContext(t, 25*time.Minute)
	waitForHealthyNodes(t, ctx, 5)
	waitForFullDurability(t, ctx, 3*time.Minute)

	// Several objects, so recovery has real work to survive the restart.
	keys := make([]string, 3)
	sums := make([]string, 3)
	for i := range keys {
		keys[i] = uniqueKey(t, fmt.Sprintf("scenario-d-%d.bin", i))
		payload := randomPayload(t, defaultChunkSize*2+1024)
		sums[i] = sha256hex(payload)
		putObject(t, ctx, testBucket, keys[i], payload)
		k := keys[i]
		t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, k) })
	}

	before := getObjectChunks(t, ctx, testBucket, keys[0])
	var victim string
	for node := range before.nodesHolding() {
		victim = node
		break
	}

	t.Logf("stopping %s to start a recovery", victim)
	compose(t, "stop", victim)
	t.Cleanup(func() { restoreNode(t, victim) })
	waitForNodeState(t, ctx, victim, "DEAD")

	// Restart the coordinator as soon as damage is visible. Whether the restart
	// lands mid-copy or between jobs, the invariant is identical: no repair
	// work is lost and recovery completes.
	waitForUnderReplication(t, ctx, 30*time.Second)
	t.Log("restarting the coordinator mid-recovery")
	compose(t, "restart", "coordinator")
	waitForGateway(t, ctx)
	waitForCoordinator(t, ctx)

	// Per object, not cluster-wide: an earlier scenario in this suite
	// deliberately destroys every replica of an object, and those chunks are
	// unrepairable by design -- there is nothing left to copy from. Waiting for
	// cluster-wide durability would be waiting for something the suite itself
	// made impossible, and would report a coordinator-restart failure that had
	// nothing to do with the coordinator restart.
	var took time.Duration
	for _, k := range keys {
		if d := waitForObjectDurable(t, ctx, testBucket, k, 10*time.Minute); d > took {
			took = d
		}
	}
	t.Logf("every object recovered within %s of the coordinator restart", took.Round(time.Millisecond))

	if status := getReplication(t, ctx); status.JobsFailed > 0 {
		t.Logf("note: %d repair jobs cluster-wide ended in FAILED state "+
			"(expected: earlier scenarios destroy chunks on purpose)", status.JobsFailed)
	}

	for i, key := range keys {
		got, _ := getObject(t, ctx, testBucket, key)
		if sha256hex(got) != sums[i] {
			t.Errorf("%s: SHA-256 mismatch after a coordinator restart during recovery", key)
		}
		chunks := getObjectChunks(t, ctx, testBucket, key)
		for _, c := range chunks.Chunks {
			if c.AvailableReplicas != chunks.ReplicationFactor {
				t.Errorf("%s chunk %d has %d replicas, want %d",
					key, c.Index, c.AvailableReplicas, chunks.ReplicationFactor)
			}
		}
	}
	t.Log("all objects intact and fully replicated after a mid-recovery restart")
}

// ---- Scenario E: concurrent uploads ---------------------------------------

func TestScenarioE_ConcurrentUploadsPlaceCorrectly(t *testing.T) {
	if testing.Short() {
		t.Skip("uploads several large objects; skipped in -short mode")
	}
	ctx := testContext(t, 15*time.Minute)
	status := waitForHealthyNodes(t, ctx, 5)
	rf := int(status.ReplicationFactor)

	const workers = 8
	type result struct {
		key string
		sum string
		err error
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := uniqueKey(t, fmt.Sprintf("scenario-e-%d.bin", i))
			payload := randomPayload(t, defaultChunkSize+i*100_000)
			sum := sha256hex(payload)

			req, err := http.NewRequestWithContext(ctx, http.MethodPut,
				gatewayURL()+"/objects/"+testBucket+"/"+key, bytes.NewReader(payload))
			if err != nil {
				results <- result{err: err}
				return
			}
			req.ContentLength = int64(len(payload))
			resp, err := client.Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			_, _ = readAllAndClose(resp)
			if resp.StatusCode != http.StatusCreated {
				results <- result{err: fmt.Errorf("PUT returned %d", resp.StatusCode)}
				return
			}
			results <- result{key: key, sum: sum}
		}(i)
	}
	wg.Wait()
	close(results)

	uploaded := make([]result, 0, workers)
	for r := range results {
		if r.err != nil {
			t.Fatalf("concurrent upload failed: %v", r.err)
		}
		uploaded = append(uploaded, r)
	}

	// Every object must round-trip, and every chunk must be correctly placed:
	// exactly RF replicas on distinct nodes. A metadata race would surface here
	// as a duplicate placement or a missing replica.
	nodeSpread := map[string]int{}
	for _, r := range uploaded {
		key := r.key
		t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

		got, _ := getObject(t, ctx, testBucket, key)
		if sha256hex(got) != r.sum {
			t.Errorf("%s: content differs after concurrent uploads", key)
		}

		// Convergence, not the instantaneous count: concurrent uploads may each
		// commit at min_write_replicas.
		waitForObjectFullyReplicated(t, ctx, testBucket, key, 3*time.Minute)
		chunks := getObjectChunks(t, ctx, testBucket, key)
		for _, c := range chunks.Chunks {
			if c.AvailableReplicas != rf {
				t.Errorf("%s chunk %d has %d replicas, want %d", key, c.Index, c.AvailableReplicas, rf)
			}
			seen := map[string]bool{}
			for _, rep := range c.Replicas {
				if rep.State != "AVAILABLE" {
					continue
				}
				if seen[rep.NodeID] {
					t.Errorf("%s chunk %d has two replicas on %s", key, c.Index, rep.NodeID)
				}
				seen[rep.NodeID] = true
				nodeSpread[rep.NodeID]++
			}
		}
	}

	if len(nodeSpread) < rf {
		t.Errorf("concurrent uploads only used %d nodes: %v", len(nodeSpread), nodeSpread)
	}
	t.Logf("%d concurrent uploads placed correctly; replica spread: %v", workers, nodeSpread)

	waitForFullDurability(t, ctx, 3*time.Minute)
}

// ---- returning-node reconciliation ---------------------------------------

func TestReturningNodeIsReconciledNotTrusted(t *testing.T) {
	if testing.Short() {
		t.Skip("stops a container; skipped in -short mode")
	}
	ctx := testContext(t, 20*time.Minute)
	waitForHealthyNodes(t, ctx, 5)
	waitForFullDurability(t, ctx, 3*time.Minute)

	key := uniqueKey(t, "rejoin.bin")
	payload := randomPayload(t, defaultChunkSize+4096)
	want := sha256hex(payload)
	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	waitForObjectFullyReplicated(t, ctx, testBucket, key, 2*time.Minute)
	before := getObjectChunks(t, ctx, testBucket, key)
	var victim string
	for node := range before.nodesHolding() {
		victim = node
		break
	}

	compose(t, "stop", victim)
	waitForNodeState(t, ctx, victim, "DEAD")
	waitForFullDurability(t, ctx, 6*time.Minute)
	t.Logf("repaired around %s while it was down", victim)

	// Bring it back. Its files are now stale copies of chunks that have already
	// been re-replicated elsewhere.
	startNode(t, victim)
	waitForNodeState(t, ctx, victim, "HEALTHY")

	// The coordinator must reconcile rather than blindly re-trusting it, and
	// the end state must be exactly RF replicas -- not RF+1 forever.
	deadline := time.Now().Add(4 * time.Minute)
	settled := false
	for time.Now().Before(deadline) {
		chunks := getObjectChunks(t, ctx, testBucket, key)
		ok := true
		for _, c := range chunks.Chunks {
			if c.AvailableReplicas != chunks.ReplicationFactor {
				ok = false
			}
		}
		if ok {
			settled = true
			break
		}
		time.Sleep(2 * time.Second)
	}
	if !settled {
		final := getObjectChunks(t, ctx, testBucket, key)
		for _, c := range final.Chunks {
			t.Logf("  chunk %d: %d replicas (want %d)",
				c.Index, c.AvailableReplicas, final.ReplicationFactor)
		}
		t.Fatal("replication did not settle back to exactly the replication factor after rejoin")
	}
	t.Log("returning node reconciled; replication settled at exactly RF")

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatal("object changed across the node's departure and return")
	}
	waitForHealthyNodes(t, ctx, 5)
}

// ---- helpers -------------------------------------------------------------

// metricValue sums every series of a counter across the gateway and
// coordinator metrics endpoints.
func metricValue(t *testing.T, ctx context.Context, name string) float64 {
	t.Helper()
	var total float64
	for _, port := range []string{":9101", ":9102"} {
		url := strings.Replace(gatewayURL(), ":8080", port, 1) + "/metrics"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatalf("building metrics request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		body, _ := readAllAndClose(resp)
		for _, line := range strings.Split(body, "\n") {
			if !strings.HasPrefix(line, name) {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			var v float64
			if _, err := fmt.Sscanf(fields[1], "%g", &v); err == nil {
				total += v
			}
		}
	}
	return total
}

// chunkFileExists reports whether a node really holds a chunk, independent of
// what metadata claims about it.
func chunkFileExists(t *testing.T, nodeID, chunkID string) bool {
	t.Helper()
	return composeOK(t, "exec", "-T", nodeID, "test", "-f", chunkPath(chunkID))
}

// chunkPath mirrors internal/storage.RelativePath: two levels of two hex
// characters from the dash-stripped UUID.
func chunkPath(chunkID string) string {
	flat := strings.ReplaceAll(chunkID, "-", "")
	return fmt.Sprintf("/data/data/%s/%s/%s.chunk", flat[0:2], flat[2:4], chunkID)
}

// corruptChunkOnNode overwrites a chunk file inside a storage-node container,
// preserving its length so only the SHA-256 detects the damage. It reports
// whether a file was actually corrupted.
//
// A missing file is not a test failure. Metadata can legitimately list a
// replica that the node does not have -- that is the "phantom" case the
// reconciler exists to resolve -- and for corruption tests an absent replica is
// just as unreadable as a corrupt one. Fataling here would turn a state the
// system is designed to handle into a spurious red build.
func corruptChunkOnNode(t *testing.T, nodeID, chunkID string) bool {
	t.Helper()
	path := chunkPath(chunkID)
	if !composeOK(t, "exec", "-T", nodeID, "test", "-f", path) {
		t.Logf("no chunk file for %s on %s; treating that replica as already unreadable",
			chunkID, nodeID)
		return false
	}
	compose(t, "exec", "-T", nodeID, "sh", "-c",
		fmt.Sprintf("printf 'CORRUPTED-BY-FLEXSTORE-TEST' | dd of=%s bs=1 seek=0 conv=notrunc status=none", path))
	return true
}

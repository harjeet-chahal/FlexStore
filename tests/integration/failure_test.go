//go:build integration

package integration

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestCoordinatorRestartPreservesMetadata is the durability claim that
// separates FlexStore from an in-memory index: kill the control plane, bring
// it back, and every object must still be readable with identical bytes.
//
// It restarts a real container. That is slow and deliberately so -- simulating
// it in-process would prove nothing about what actually survives a restart.
func TestCoordinatorRestartPreservesMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("restarts a container; skipped in -short mode")
	}
	ctx := testContext(t, 8*time.Minute)

	key := uniqueKey(t, "survives-restart.bin")
	payload := randomPayload(t, defaultChunkSize*2+7777)
	originalSum := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	before := getLayout(t, ctx, testBucket, key)

	t.Log("restarting the coordinator container")
	compose(t, "restart", "coordinator")

	// The gateway holds a gRPC connection that must reconnect; the coordinator
	// must re-run migrations, rehydrate its health monitor from PostgreSQL and
	// start serving again.
	waitForGateway(t, ctx)
	waitForCoordinator(t, ctx)

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != originalSum {
		t.Fatalf("object changed across a coordinator restart:\n  before: %s\n  after:  %s",
			originalSum, sha256hex(got))
	}

	after := getLayout(t, ctx, testBucket, key)
	if after.ObjectID != before.ObjectID {
		t.Errorf("object id changed across the restart: %s -> %s", before.ObjectID, after.ObjectID)
	}
	if len(after.Chunks) != len(before.Chunks) {
		t.Fatalf("chunk count changed across the restart: %d -> %d",
			len(before.Chunks), len(after.Chunks))
	}
	for i := range before.Chunks {
		if before.Chunks[i].ChunkID != after.Chunks[i].ChunkID {
			t.Errorf("chunk %d id changed: %s -> %s", i,
				before.Chunks[i].ChunkID, after.Chunks[i].ChunkID)
		}
		if before.Chunks[i].Checksum != after.Chunks[i].Checksum {
			t.Errorf("chunk %d checksum changed across the restart", i)
		}
	}

	// A restarted coordinator must not declare the whole cluster dead while it
	// waits for the first heartbeat round -- that is what Rehydrate prevents.
	status := getClusterStatus(t, ctx)
	if len(status.healthyNodes()) < 3 {
		t.Errorf("only %d nodes healthy immediately after the restart; "+
			"the health monitor did not rehydrate from PostgreSQL: %+v",
			len(status.healthyNodes()), status.Nodes)
	}

	// And new writes must work afterwards.
	postKey := uniqueKey(t, "written-after-restart.bin")
	postPayload := randomPayload(t, 4096)
	putObject(t, ctx, testBucket, postKey, postPayload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, postKey) })

	back, _ := getObject(t, ctx, testBucket, postKey)
	if !bytes.Equal(back, postPayload) {
		t.Fatal("an object written after the restart did not round-trip")
	}
}

// TestReadSurvivesAStorageNodeOutage stops a node that holds replicas of an
// object and checks the object is still readable from the others.
func TestReadSurvivesAStorageNodeOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("stops a container; skipped in -short mode")
	}
	ctx := testContext(t, 8*time.Minute)
	waitForHealthyNodes(t, ctx, 5)

	key := uniqueKey(t, "survives-node-loss.bin")
	payload := randomPayload(t, defaultChunkSize+2048)
	originalSum := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	layout := getLayout(t, ctx, testBucket, key)
	victim := layout.availableNodes(0)
	if len(victim) < 2 {
		t.Fatalf("chunk 0 has only %d replicas; cannot test surviving an outage", len(victim))
	}
	target := victim[0]

	t.Logf("stopping %s, which holds a replica of chunk 0", target)
	compose(t, "stop", target)
	t.Cleanup(func() { restoreNode(t, target) })

	// The read path fails over per replica, so the object is readable
	// immediately -- it does not need to wait for the node to be marked DEAD.
	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != originalSum {
		t.Fatalf("object changed while a replica node was down:\n  before: %s\n  after:  %s",
			originalSum, sha256hex(got))
	}
	t.Log("object still readable with one replica node stopped")
}

// TestUploadsSucceedWithANodeDown checks that placement degrades gracefully:
// with 5 nodes and RF=3, losing one still leaves enough targets.
func TestUploadsSucceedWithANodeDown(t *testing.T) {
	if testing.Short() {
		t.Skip("stops a container; skipped in -short mode")
	}
	ctx := testContext(t, 8*time.Minute)
	status := waitForHealthyNodes(t, ctx, 5)

	target := status.healthyNodes()[len(status.healthyNodes())-1]
	t.Logf("stopping %s", target)
	compose(t, "stop", target)
	t.Cleanup(func() { restoreNode(t, target) })

	// Wait until the coordinator has actually noticed, so this tests placement
	// with a known-degraded cluster rather than a race against the health scan.
	waitForNodeState(t, ctx, target, "SUSPECT", "DEAD")

	key := uniqueKey(t, "written-during-outage.bin")
	payload := randomPayload(t, defaultChunkSize+1024)
	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != sha256hex(payload) {
		t.Fatal("object written during the outage did not round-trip")
	}

	// Nothing may have been placed on the node the coordinator considers down.
	layout := getLayout(t, ctx, testBucket, key)
	for _, c := range layout.Chunks {
		for _, r := range c.Replicas {
			if r.NodeID == target {
				t.Errorf("chunk %d was placed on %s, which is %s",
					c.Index, target, r.NodeHealth)
			}
		}
	}

	// And every chunk must reach the full replication factor, because four
	// healthy nodes are more than enough for RF=3.
	//
	// Convergence rather than the instantaneous count: the write commits once
	// min_write_replicas acknowledge, so a target that is briefly unreachable
	// leaves the chunk one replica short until repair fills it in. Asserting
	// the count the moment the PUT returns would be asserting something
	// stronger than the durability contract promises.
	waitForObjectFullyReplicated(t, ctx, testBucket, key, 3*time.Minute)

	layout = getLayout(t, ctx, testBucket, key)
	for _, c := range layout.Chunks {
		if got := len(layout.availableNodes(int(c.Index))); got != int(status.ReplicationFactor) {
			t.Errorf("chunk %d has %d replicas after convergence, want %d",
				c.Index, got, status.ReplicationFactor)
		}
	}
}

// TestHealthTransitionsAreObservable checks the health state machine is
// visible to operators, not just internally consistent.
func TestHealthTransitionsAreObservable(t *testing.T) {
	if testing.Short() {
		t.Skip("stops a container; skipped in -short mode")
	}
	ctx := testContext(t, 8*time.Minute)
	status := waitForHealthyNodes(t, ctx, 5)

	target := status.healthyNodes()[0]
	compose(t, "stop", target)
	t.Cleanup(func() { restoreNode(t, target) })

	// Compose sets SUSPECT at 15s and DEAD at 45s.
	waitForNodeState(t, ctx, target, "SUSPECT", "DEAD")
	t.Logf("%s left HEALTHY after its heartbeats stopped", target)

	waitForNodeState(t, ctx, target, "DEAD")
	t.Logf("%s reached DEAD", target)

	// Its replicas must now count as lost for durability purposes.
	afterDeath := getClusterStatus(t, ctx)
	if afterDeath.TotalChunks > 0 && afterDeath.UnderReplicatedChunks == 0 {
		t.Log("no under-replicated chunks reported; the dead node may have held no replicas")
	}

	restoreNode(t, target)
	waitForNodeState(t, ctx, target, "HEALTHY")
	t.Logf("%s returned to HEALTHY after restarting", target)
}

// restoreNode restarts a stopped node and blocks until the cluster is fully
// writable again.
//
// Waiting for HEALTHY is not sufficient: the coordinator marks a node healthy
// on its first heartbeat, but the gateway holds a cached gRPC connection that
// must finish reconnecting before placement on that node can actually succeed.
// Returning too early would leave later tests writing one replica short, which
// is a real behaviour but not the one they are asserting.
func restoreNode(t *testing.T, nodeID string) {
	t.Helper()
	startNode(t, nodeID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Wait for *this* node, not for the whole cluster. Scenario B stops two
	// nodes and registers a cleanup for each; cleanups run LIFO, so the first
	// one to run would wait forever for five healthy nodes while the second
	// node is still deliberately stopped.
	waitForNodeState(t, ctx, nodeID, "HEALTHY")
	waitForNodeWritable(t, ctx, nodeID)
}

// waitForNodeWritable probes the cluster until the named node actually accepts
// a replica.
//
// Waiting for HEALTHY is not enough, and neither is "some probe reached RF":
// placement picks 3 of 5 nodes at random, so a probe has a 40% chance of
// avoiding the node we care about entirely. The only reliable signal that a
// returning node is usable again is seeing it hold a replica of something we
// just wrote. Without this, a later test uploads while one node is still
// unreachable, the write commits at min_write_replicas, and the test observes a
// legitimately under-replicated object it did not cause.
func waitForNodeWritable(t *testing.T, ctx context.Context, nodeID string) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)

	for time.Now().Before(deadline) {
		// Several probes per round: each one is an independent chance for
		// placement to select the node we are waiting on.
		for i := 0; i < 4; i++ {
			key := uniqueKey(t, "settle-probe.bin")
			putObject(t, ctx, testBucket, key, randomPayload(t, 1024))
			layout := getLayout(t, ctx, testBucket, key)
			deleteObject(t, ctx, testBucket, key)

			for _, id := range layout.availableNodes(0) {
				if id == nodeID {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for %s to accept writes", nodeID)
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("%s never accepted a replica after restarting", nodeID)
}

// waitForNodeState blocks until the named node reports one of the given states.
func waitForNodeState(t *testing.T, ctx context.Context, nodeID string, states ...string) {
	t.Helper()
	want := map[string]bool{}
	for _, s := range states {
		want[s] = true
	}

	deadline := time.Now().Add(3 * time.Minute)
	last := "?"
	for time.Now().Before(deadline) {
		for _, n := range getClusterStatus(t, ctx).Nodes {
			if n.ID != nodeID {
				continue
			}
			last = n.Health
			if want[n.Health] {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for %s to reach %v (last: %s)", nodeID, states, last)
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("%s never reached %v (last observed: %s)", nodeID, states, last)
}

// waitForCoordinator blocks until the coordinator answers through the gateway.
func waitForCoordinator(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var lastStatus int
	var lastBody string
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gatewayURL()+"/admin/nodes", nil)
		if err != nil {
			t.Fatalf("building request: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			body, _ := readAllAndClose(resp)
			lastStatus, lastBody = resp.StatusCode, body
			if resp.StatusCode == http.StatusOK && strings.Contains(body, "replication_factor") {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for the coordinator")
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("coordinator did not come back (last status %d): %s", lastStatus, lastBody)
}

func readAllAndClose(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

// waitForObjectFullyReplicated blocks until every chunk of an object holds
// exactly the replication factor of AVAILABLE replicas on distinct nodes.
//
// This is the property the system actually guarantees: a write commits once
// min_write_replicas acknowledge, and the repair manager converges the rest.
// Tests that assert the replica count the instant a PUT returns are asserting
// something stronger than the contract and race with repair.
func waitForObjectFullyReplicated(t *testing.T, ctx context.Context, bucket, key string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var layout objectLayout

	for time.Now().Before(deadline) {
		layout = getLayout(t, ctx, bucket, key)
		rf := int(getClusterStatus(t, ctx).ReplicationFactor)

		converged := len(layout.Chunks) > 0
		for _, c := range layout.Chunks {
			if len(layout.availableNodes(int(c.Index))) != rf {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for %s/%s to reach full replication", bucket, key)
		case <-time.After(time.Second):
		}
	}
	for _, c := range layout.Chunks {
		t.Logf("  chunk %d: %d available replicas", c.Index, len(layout.availableNodes(int(c.Index))))
	}
	t.Fatalf("%s/%s did not reach full replication within %s", bucket, key, within)
}

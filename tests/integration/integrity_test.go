//go:build integration

package integration

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestCorruptReplicaIsDetectedAndBypassed is the strongest integrity claim
// FlexStore makes: corrupt one replica's bytes on disk, and the object still
// downloads correctly because the corrupt copy is detected and skipped.
//
// The corruption is real -- the chunk file inside the container is overwritten
// -- because faking it would test the mock, not the checksum pipeline.
func TestCorruptReplicaIsDetectedAndBypassed(t *testing.T) {
	if testing.Short() {
		t.Skip("mutates a container's filesystem; skipped in -short mode")
	}
	ctx := testContext(t, 8*time.Minute)
	waitForHealthyNodes(t, ctx, 3)

	key := uniqueKey(t, "corruption.bin")
	payload := randomPayload(t, 64*1024)
	want := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	layout := getLayout(t, ctx, testBucket, key)
	if len(layout.Chunks) != 1 {
		t.Fatalf("expected a single chunk, got %d", len(layout.Chunks))
	}
	chunk := layout.Chunks[0]
	nodes := layout.availableNodes(0)
	if len(nodes) < 2 {
		t.Fatalf("need at least two replicas to test failover, got %d", len(nodes))
	}

	// Corrupt the first replica that actually has a file on disk. Metadata may
	// list a replica the node does not hold (the "phantom" case), and
	// corrupting nothing would make the rest of this test vacuous.
	victim := ""
	for _, n := range nodes {
		if corruptChunkOnNode(t, n, chunk.ChunkID) {
			victim = n
			break
		}
	}
	if victim == "" {
		t.Fatalf("no replica of chunk %s could be corrupted (replicas: %v)", chunk.ChunkID, nodes)
	}
	t.Logf("corrupted chunk %s on %s (replicas: %v)", chunk.ChunkID, victim, nodes)

	// The gateway must fail over to a healthy replica and return correct bytes.
	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != want {
		t.Fatalf("download after corrupting one replica does not match:\n  want %s\n  got  %s",
			want, sha256hex(got))
	}
	t.Log("object still served correctly from a healthy replica")

	// The failure must be visible in metrics, not just silently worked around.
	if !checksumFailuresObserved(t, ctx) {
		t.Error("flexstore_checksum_failures_total did not increase; " +
			"corruption was handled but not reported")
	}
}

// TestAllReplicasCorruptFailsLoudly checks the other half of the contract:
// with no good copy, FlexStore must not serve the bad bytes.
//
// Since self-healing landed this is a race against the repair manager, which
// is busily rebuilding the copies we destroy. The test is written to win that
// race rather than to assume it does not exist -- see the loop below.
func TestAllReplicasCorruptFailsLoudly(t *testing.T) {
	if testing.Short() {
		t.Skip("mutates container filesystems; skipped in -short mode")
	}
	ctx := testContext(t, 8*time.Minute)
	waitForHealthyNodes(t, ctx, 3)

	key := uniqueKey(t, "all-corrupt.bin")
	payload := randomPayload(t, 32*1024)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	chunkID := getLayout(t, ctx, testBucket, key).Chunks[0].ChunkID

	// We are racing the repair manager. The moment a replica stops verifying,
	// repair starts rebuilding a fresh, correct copy somewhere else -- which is
	// exactly the behaviour the rest of this suite asserts. So a single pass
	// over a layout snapshot is not enough: by the time the last `docker exec`
	// returns, a new good replica may already exist on a node that was not in
	// the snapshot, and the object downloads perfectly.
	//
	// Instead, re-read the layout and corrupt every AVAILABLE replica each
	// round until a read genuinely cannot be satisfied. Winning the race is a
	// matter of rounds, not luck: each round strictly reduces the number of
	// good copies unless repair replaces them faster than we can damage them,
	// and repair is rate-limited per node.
	var (
		status int
		body   []byte
		err    error
		rounds int
	)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		rounds++
		nodes := getLayout(t, ctx, testBucket, key).availableNodes(0)
		// A file that is already missing counts as unreadable: the read path
		// cannot serve it either.
		for _, n := range nodes {
			corruptChunkOnNode(t, n, chunkID)
		}

		status, body, err = tryGet(ctx, testBucket, key)
		if err != nil {
			// A transport-level failure is acceptable: the gateway aborts the
			// connection rather than sending bytes it cannot vouch for.
			t.Logf("after %d round(s), download failed at the transport layer, as designed: %v",
				rounds, err)
			return
		}
		if status != http.StatusOK {
			t.Logf("after %d round(s), download rejected with HTTP %d: %s",
				rounds, status, truncate(string(body), 200))
			return
		}
		if len(body) != len(payload) {
			// The gateway had already sent 200 before the first chunk failed
			// verification, so it truncated the response. A short body is the
			// in-protocol way to say "do not trust this".
			t.Logf("after %d round(s), response truncated after %d of %d bytes, as designed",
				rounds, len(body), len(payload))
			return
		}
		if sha256hex(body) != sha256hex(payload) {
			t.Fatalf("full-length body served that is neither the object nor an error (%d bytes)",
				len(body))
		}
		// A correct full body means repair beat us to it. Go again.
		t.Logf("round %d: repair restored a good replica before we could damage them all", rounds)
		select {
		case <-ctx.Done():
			t.Fatal("context expired while racing the repair manager")
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("never observed a failed read after %d rounds of corrupting every replica", rounds)
}

// checksumFailuresObserved scrapes the gateway's metrics endpoint looking for
// a non-zero checksum failure counter.
func checksumFailuresObserved(t *testing.T, ctx context.Context) bool {
	t.Helper()

	metricsURL := strings.Replace(gatewayURL(), ":8080", ":9101", 1) + "/metrics"
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
		if err != nil {
			t.Fatalf("building metrics request: %v", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Logf("scraping %s: %v", metricsURL, err)
			return false
		}
		body, _ := readAllAndClose(resp)
		for _, line := range strings.Split(body, "\n") {
			if !strings.HasPrefix(line, "flexstore_checksum_failures_total") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[1] != "0" {
				t.Logf("observed %s", line)
				return true
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
	return false
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

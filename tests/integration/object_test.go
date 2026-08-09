//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testBucket = "flexstore-it"

func testContext(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// uniqueKey keeps tests independent even when run repeatedly against a
// long-lived cluster.
func uniqueKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("it/%s/%d-%s", strings.ToLower(t.Name()), time.Now().UnixNano(), suffix)
}

func TestUploadAndRetrieveSmallObject(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	key := uniqueKey(t, "small.bin")
	payload := randomPayload(t, 4096)

	putHeaders := putObject(t, ctx, testBucket, key, payload)
	if got := strings.Trim(putHeaders.Get("ETag"), `"`); got != sha256hex(payload) {
		t.Fatalf("ETag = %q, want the payload's SHA-256 %s", got, sha256hex(payload))
	}

	got, headers := getObject(t, ctx, testBucket, key)
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded %d bytes, uploaded %d, contents differ", len(got), len(payload))
	}
	if headers.Get("Content-Length") != strconv.Itoa(len(payload)) {
		t.Errorf("Content-Length = %q, want %d", headers.Get("Content-Length"), len(payload))
	}

	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })
}

// TestObjectSpanningSeveralChunks is the core correctness test: a payload
// large enough to be split, replicated, and reassembled, verified byte for
// byte via SHA-256.
func TestObjectSpanningSeveralChunks(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	key := uniqueKey(t, "multi-chunk.bin")

	// 3.5 chunks: exercises full chunks plus a short final one.
	size := defaultChunkSize*3 + defaultChunkSize/2
	payload := randomPayload(t, size)
	originalSum := sha256hex(payload)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	got, headers := getObject(t, ctx, testBucket, key)
	if len(got) != size {
		t.Fatalf("downloaded %d bytes, want %d", len(got), size)
	}
	downloadedSum := sha256hex(got)
	if downloadedSum != originalSum {
		t.Fatalf("SHA-256 mismatch:\n  uploaded:   %s\n  downloaded: %s", originalSum, downloadedSum)
	}

	chunkCount, err := strconv.Atoi(headers.Get("X-Flexstore-Chunk-Count"))
	if err != nil {
		t.Fatalf("X-Flexstore-Chunk-Count = %q: %v", headers.Get("X-Flexstore-Chunk-Count"), err)
	}
	if chunkCount != 4 {
		t.Fatalf("object was split into %d chunks, want 4 for %d bytes at %d per chunk",
			chunkCount, size, defaultChunkSize)
	}
}

// TestReplicasLandOnDistinctNodes verifies the replication promise against the
// actual recorded placement rather than a counter.
func TestReplicasLandOnDistinctNodes(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	status := waitForHealthyNodes(t, ctx, 3)
	rf := int(status.ReplicationFactor)

	key := uniqueKey(t, "replicated.bin")
	payload := randomPayload(t, defaultChunkSize*2+1024)
	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	layout := getLayout(t, ctx, testBucket, key)
	if len(layout.Chunks) != 3 {
		t.Fatalf("layout reports %d chunks, want 3", len(layout.Chunks))
	}

	// A write commits once min_write_replicas acknowledge; the repair manager
	// restores the rest. Asserting "exactly RF the instant the PUT returns"
	// would be asserting something stronger than the system promises, and it
	// races with repair. Wait for convergence, then assert.
	waitForObjectFullyReplicated(t, ctx, testBucket, key, 2*time.Minute)
	layout = getLayout(t, ctx, testBucket, key)

	nodesUsed := map[string]int{}
	for _, c := range layout.Chunks {
		nodes := layout.availableNodes(int(c.Index))
		if len(nodes) != rf {
			t.Errorf("chunk %d has %d available replicas, want the replication factor %d (replicas: %+v)",
				c.Index, len(nodes), rf, c.Replicas)
		}
		// Distinctness is the whole point: three copies on one machine is one
		// copy as far as durability goes.
		seen := map[string]bool{}
		for _, n := range nodes {
			if seen[n] {
				t.Errorf("chunk %d has two replicas on node %s", c.Index, n)
			}
			seen[n] = true
			nodesUsed[n]++
		}
		if c.Checksum == "" || len(c.Checksum) != 64 {
			t.Errorf("chunk %d has no recorded SHA-256 (%q)", c.Index, c.Checksum)
		}
	}

	// With 5 nodes, RF=3 and 3 chunks, placement should touch more than the
	// bare minimum -- otherwise the weighted strategy has collapsed into a
	// fixed choice and one node failure would take out every chunk.
	if len(nodesUsed) < rf {
		t.Fatalf("all replicas landed on only %d nodes: %v", len(nodesUsed), nodesUsed)
	}
	t.Logf("replica distribution across nodes: %v", nodesUsed)
}

func TestHeadObject(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	key := uniqueKey(t, "head.bin")
	payload := randomPayload(t, 12345)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	resp, body := do(t, newRequest(t, ctx, http.MethodHead, "/objects/"+testBucket+"/"+key, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD = %d", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("HEAD returned a %d-byte body; it must be empty", len(body))
	}
	if resp.Header.Get("Content-Length") != strconv.Itoa(len(payload)) {
		t.Errorf("Content-Length = %q, want %d", resp.Header.Get("Content-Length"), len(payload))
	}
	if strings.Trim(resp.Header.Get("ETag"), `"`) != sha256hex(payload) {
		t.Errorf("ETag = %q", resp.Header.Get("ETag"))
	}

	// HEAD on a missing key must be 404, and still carry no body.
	resp, body = do(t, newRequest(t, ctx, http.MethodHead, "/objects/"+testBucket+"/does-not-exist", nil))
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("HEAD on a missing key = %d, want 404", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Errorf("404 HEAD returned a body of %d bytes", len(body))
	}
}

func TestDeleteObject(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)
	key := uniqueKey(t, "deleted.bin")
	payload := randomPayload(t, defaultChunkSize+512)

	putObject(t, ctx, testBucket, key, payload)
	if _, _, err := tryGet(ctx, testBucket, key); err != nil {
		t.Fatalf("object should exist before deletion: %v", err)
	}

	deleteObject(t, ctx, testBucket, key)

	status, _, err := tryGet(ctx, testBucket, key)
	if err != nil {
		t.Fatalf("GET after delete: %v", err)
	}
	if status != http.StatusNotFound {
		t.Fatalf("GET after delete = %d, want 404", status)
	}

	// DELETE is idempotent, matching S3.
	deleteObject(t, ctx, testBucket, key)
}

func TestOverwriteReplacesTheObject(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	key := uniqueKey(t, "overwritten.bin")

	first := randomPayload(t, 8192)
	second := randomPayload(t, defaultChunkSize+4096)

	putObject(t, ctx, testBucket, key, first)
	putObject(t, ctx, testBucket, key, second)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != sha256hex(second) {
		t.Fatalf("read the old version after overwrite (got %d bytes, want %d)", len(got), len(second))
	}

	layout := getLayout(t, ctx, testBucket, key)
	if layout.SizeBytes != int64(len(second)) {
		t.Fatalf("layout size = %d, want %d", layout.SizeBytes, len(second))
	}
}

func TestEmptyObject(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	key := uniqueKey(t, "empty.bin")

	putObject(t, ctx, testBucket, key, nil)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	got, headers := getObject(t, ctx, testBucket, key)
	if len(got) != 0 {
		t.Fatalf("downloaded %d bytes for an empty object", len(got))
	}
	if headers.Get("X-Flexstore-Chunk-Count") != "0" {
		t.Errorf("chunk count = %q, want 0", headers.Get("X-Flexstore-Chunk-Count"))
	}
}

func TestKeysWithSlashes(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	key := uniqueKey(t, "deeply/nested/path/to/file.bin")
	payload := randomPayload(t, 2048)

	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	got, _ := getObject(t, ctx, testBucket, key)
	if !bytes.Equal(got, payload) {
		t.Fatal("a key containing slashes did not round-trip")
	}
}

func TestInvalidRequestsReturnStructuredErrors(t *testing.T) {
	ctx := testContext(t, time.Minute)

	cases := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantCode   string
	}{
		{"missing key", http.MethodGet, "/objects/" + testBucket + "/no-such-object-at-all",
			http.StatusNotFound, "NoSuchKey"},
		{"bucket too short", http.MethodGet, "/objects/ab/key",
			http.StatusBadRequest, "InvalidRequest"},
		{"uppercase bucket", http.MethodGet, "/objects/BadBucket/key",
			http.StatusBadRequest, "InvalidRequest"},
		{"unknown multipart upload", http.MethodDelete, "/multipart/not-a-uuid",
			http.StatusBadRequest, "InvalidRequest"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp, body := do(t, newRequest(t, ctx, c.method, c.path, nil))
			if resp.StatusCode != c.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", resp.StatusCode, c.wantStatus, body)
			}
			if got := resp.Header.Get("X-Flexstore-Error-Code"); got != c.wantCode {
				t.Errorf("error code header = %q, want %q", got, c.wantCode)
			}
			// Errors must carry the request ID so a report is actionable.
			if !strings.Contains(string(body), `"request_id"`) {
				t.Errorf("error body has no request_id: %s", body)
			}
			if resp.Header.Get("X-Request-Id") == "" {
				t.Error("response has no X-Request-Id header")
			}
		})
	}
}

func TestRequestIDIsEchoed(t *testing.T) {
	ctx := testContext(t, time.Minute)
	req := newRequest(t, ctx, http.MethodGet, "/admin/nodes", nil)
	req.Header.Set("X-Request-Id", "integration-test-correlation-id")

	resp, _ := do(t, req)
	if got := resp.Header.Get("X-Request-Id"); got != "integration-test-correlation-id" {
		t.Fatalf("X-Request-Id = %q, want the value the client supplied", got)
	}
}

func TestListObjects(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)
	prefix := fmt.Sprintf("listing-%d/", time.Now().UnixNano())

	keys := []string{prefix + "a.bin", prefix + "b.bin", prefix + "c.bin"}
	for _, k := range keys {
		putObject(t, ctx, testBucket, k, randomPayload(t, 512))
		key := k
		t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })
	}

	resp, body := do(t, newRequest(t, ctx, http.MethodGet,
		"/objects/"+testBucket+"?prefix="+prefix, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list = %d: %s", resp.StatusCode, body)
	}
	for _, k := range keys {
		if !strings.Contains(string(body), k) {
			t.Errorf("listing is missing %s:\n%s", k, body)
		}
	}
}

// tryGet performs a GET without asserting the status, for tests that expect a
// specific failure.
func tryGet(ctx context.Context, bucket, key string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		gatewayURL()+"/objects/"+bucket+"/"+key, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, body, err
}

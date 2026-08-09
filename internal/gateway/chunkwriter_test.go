package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

func newWriter(t *testing.T) *ChunkWriter {
	t.Helper()
	return NewChunkWriter(newPool(t), 10*time.Second, testLogger(), observability.NewMetrics("test-gw"))
}

func TestChunkWriterFansOutToEveryReplica(t *testing.T) {
	nodes := startNodes(t, 3)
	w := newWriter(t)

	chunkID := uuid.NewString()
	payload := bytes.Repeat([]byte("replicate-me-"), 50_000) // ~650 KiB, many frames
	sum := checksum.Sum(payload)

	res, err := w.Write(context.Background(), chunkID, payload, sum, nodeInfos(nodes), 2)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(res.Succeeded) != 3 {
		t.Fatalf("succeeded on %d nodes, want 3 (failed: %v)", len(res.Succeeded), res.Failed)
	}

	// The replication factor is only real if the bytes are actually on every
	// node's disk, so verify there rather than trusting the return value.
	for _, n := range nodes {
		if !n.has(t, chunkID) {
			t.Errorf("node %s does not hold chunk %s", n.id, chunkID)
		}
	}
}

func TestChunkWriterMeetsQuorumWhenOneNodeIsDown(t *testing.T) {
	nodes := startNodes(t, 3)
	nodes[1].kill() // one replica target is gone

	w := newWriter(t)
	chunkID := uuid.NewString()
	payload := []byte("two of three is enough")
	sum := checksum.Sum(payload)

	res, err := w.Write(context.Background(), chunkID, payload, sum, nodeInfos(nodes), 2)
	if err != nil {
		t.Fatalf("Write should succeed at quorum: %v", err)
	}
	if len(res.Succeeded) != 2 {
		t.Fatalf("succeeded = %v, want 2 nodes", res.Succeeded)
	}
	if len(res.Failed) != 1 || res.Failed[0] != nodes[1].id {
		t.Fatalf("failed = %v, want exactly the killed node %s", res.Failed, nodes[1].id)
	}
	// The failed node must be reported so the coordinator can record an
	// UNAVAILABLE replica; losing this is how chunks silently under-replicate.
	if !contains(res.Succeeded, nodes[0].id) || !contains(res.Succeeded, nodes[2].id) {
		t.Fatalf("wrong nodes succeeded: %v", res.Succeeded)
	}
}

func TestChunkWriterFailsBelowQuorum(t *testing.T) {
	nodes := startNodes(t, 3)
	nodes[0].kill()
	nodes[1].kill() // only one node left; quorum of 2 is unreachable

	w := newWriter(t)
	_, err := w.Write(context.Background(), uuid.NewString(), []byte("x"), checksum.Sum([]byte("x")),
		nodeInfos(nodes), 2)
	if err == nil {
		t.Fatal("expected a durability failure")
	}
	if !errors.Is(err, ErrDurability) {
		t.Fatalf("error should wrap ErrDurability so the gateway can return 503, got %v", err)
	}
}

func TestChunkWriterRejectsEmptyTargetList(t *testing.T) {
	w := newWriter(t)
	_, err := w.Write(context.Background(), uuid.NewString(), []byte("x"), checksum.Sum([]byte("x")),
		nil, 1)
	if !errors.Is(err, ErrDurability) {
		t.Fatalf("expected ErrDurability for an empty target list, got %v", err)
	}
}

func TestChunkWriterPropagatesCorruption(t *testing.T) {
	// Advertising a checksum that does not match the payload is exactly what a
	// corrupted gateway buffer would look like. Every node must reject it, so
	// the write must fail rather than storing bad data anywhere.
	nodes := startNodes(t, 3)
	w := newWriter(t)

	chunkID := uuid.NewString()
	payload := []byte("actual bytes")
	lyingSum := checksum.Sum([]byte("claimed bytes"))

	_, err := w.Write(context.Background(), chunkID, payload, lyingSum, nodeInfos(nodes), 2)
	if err == nil {
		t.Fatal("expected the write to fail on checksum mismatch")
	}
	for _, node := range nodes {
		if node.has(t, chunkID) {
			t.Errorf("node %s stored a chunk whose checksum did not match", node.id)
		}
	}
}

func TestChunkWriterIsIdempotentAcrossRetries(t *testing.T) {
	nodes := startNodes(t, 3)
	w := newWriter(t)

	chunkID := uuid.NewString()
	payload := bytes.Repeat([]byte("retry"), 1000)
	sum := checksum.Sum(payload)

	for attempt := 0; attempt < 3; attempt++ {
		res, err := w.Write(context.Background(), chunkID, payload, sum, nodeInfos(nodes), 2)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if len(res.Succeeded) != 3 {
			t.Fatalf("attempt %d: succeeded on %d nodes", attempt, len(res.Succeeded))
		}
	}
	// Repeated writes must not inflate reported usage, or capacity-aware
	// placement drifts away from reality.
	for _, node := range nodes {
		_, used, _, chunks, _, err := node.store.Stats()
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if chunks != 1 || used != int64(len(payload)) {
			t.Fatalf("node %s: chunks=%d used=%d after 3 identical writes, want 1/%d",
				node.id, chunks, used, len(payload))
		}
	}
}

func TestChunkWriterRespectsContextCancellation(t *testing.T) {
	nodes := startNodes(t, 3)
	w := newWriter(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // client hung up before we started

	_, err := w.Write(ctx, uuid.NewString(), []byte("payload"), checksum.Sum([]byte("payload")),
		nodeInfos(nodes), 2)
	if err == nil {
		t.Fatal("expected the write to fail on a cancelled context")
	}
}

func TestChunkWriterHandlesEmptyChunk(t *testing.T) {
	// Zero-length chunks are not produced by the splitter, but the writer must
	// not corrupt state if one ever reaches it.
	nodes := startNodes(t, 2)
	w := newWriter(t)

	chunkID := uuid.NewString()
	res, err := w.Write(context.Background(), chunkID, nil, checksum.Sum(nil), nodeInfos(nodes), 1)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(res.Succeeded) != 2 {
		t.Fatalf("succeeded = %v", res.Succeeded)
	}
}

// contains reports whether id appears in ids.
func contains(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

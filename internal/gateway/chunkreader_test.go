package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

func newReader(t *testing.T) *ChunkReader {
	t.Helper()
	return NewChunkReader(newPool(t), 10*time.Second, testLogger(), observability.NewMetrics("test-gw"))
}

// storeOn writes payload to the given nodes and returns the placement the
// reader would receive from the coordinator.
func storeOn(t *testing.T, nodes []*fakeNode, payload []byte) *flexstorev1.ChunkPlacement {
	t.Helper()
	chunkID := uuid.NewString()
	sum := checksum.Sum(payload)

	w := newWriter(t)
	res, err := w.Write(context.Background(), chunkID, payload, sum, nodeInfos(nodes), 1)
	if err != nil {
		t.Fatalf("seeding chunk: %v", err)
	}
	if len(res.Succeeded) != len(nodes) {
		t.Fatalf("seeded only %d of %d nodes", len(res.Succeeded), len(nodes))
	}
	return &flexstorev1.ChunkPlacement{
		ChunkId:        chunkID,
		SizeBytes:      int64(len(payload)),
		ChecksumSha256: sum,
		Nodes:          nodeInfos(nodes),
	}
}

func TestChunkReaderRoundTrip(t *testing.T) {
	nodes := startNodes(t, 3)
	payload := bytes.Repeat([]byte("download-me-"), 60_000) // ~720 KiB
	placement := storeOn(t, nodes, payload)

	got, err := newReader(t).Fetch(context.Background(), placement, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("fetched %d bytes, want %d, contents differ", len(got), len(payload))
	}
}

func TestChunkReaderFailsOverPastADeadNode(t *testing.T) {
	nodes := startNodes(t, 3)
	payload := []byte("survive a node failure")
	placement := storeOn(t, nodes, payload)

	// Kill the first two replicas: the reader must reach the third.
	nodes[0].kill()
	nodes[1].kill()

	got, err := newReader(t).Fetch(context.Background(), placement, nil)
	if err != nil {
		t.Fatalf("Fetch should have failed over to the surviving replica: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload differs after failover")
	}
}

func TestChunkReaderFailsOverPastACorruptReplica(t *testing.T) {
	// This is the property that matters most: a corrupt replica must never be
	// served, and its existence must not make the object unreadable.
	nodes := startNodes(t, 3)
	payload := []byte("bit rot happens")
	placement := storeOn(t, nodes, payload)

	nodes[0].corrupt(t, placement.ChunkId)

	got, err := newReader(t).Fetch(context.Background(), placement, nil)
	if err != nil {
		t.Fatalf("Fetch should have skipped the corrupt replica: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("corrupt bytes were served to the caller")
	}
}

func TestChunkReaderFailsWhenEveryReplicaIsCorrupt(t *testing.T) {
	// With no good copy the only correct answer is an explicit error. Serving
	// the corrupt bytes "because they are all we have" would be the worst
	// possible behaviour for a storage system.
	nodes := startNodes(t, 3)
	payload := []byte("all copies rotted")
	placement := storeOn(t, nodes, payload)

	for _, node := range nodes {
		node.corrupt(t, placement.ChunkId)
	}

	_, err := newReader(t).Fetch(context.Background(), placement, nil)
	if err == nil {
		t.Fatal("expected an error when every replica is corrupt")
	}
	if !errors.Is(err, ErrNoReplica) {
		t.Fatalf("error should wrap ErrNoReplica, got %v", err)
	}
}

func TestChunkReaderFailsWhenEveryReplicaIsDown(t *testing.T) {
	nodes := startNodes(t, 2)
	payload := []byte("nobody home")
	placement := storeOn(t, nodes, payload)
	for _, node := range nodes {
		node.kill()
	}

	_, err := newReader(t).Fetch(context.Background(), placement, nil)
	if !errors.Is(err, ErrNoReplica) {
		t.Fatalf("expected ErrNoReplica, got %v", err)
	}
}

func TestChunkReaderRejectsAPlacementWithNoLocations(t *testing.T) {
	_, err := newReader(t).Fetch(context.Background(), &flexstorev1.ChunkPlacement{
		ChunkId: uuid.NewString(),
	}, nil)
	if !errors.Is(err, ErrNoReplica) {
		t.Fatalf("expected ErrNoReplica, got %v", err)
	}
}

func TestChunkReaderDetectsASizeMismatch(t *testing.T) {
	// Metadata claiming a different size than the node holds means one of the
	// two is wrong; refusing to serve is the only safe answer.
	nodes := startNodes(t, 1)
	payload := []byte("ten bytes!")
	placement := storeOn(t, nodes, payload)
	placement.SizeBytes = int64(len(payload)) + 100

	_, err := newReader(t).Fetch(context.Background(), placement, nil)
	if err == nil {
		t.Fatal("expected a size-mismatch error")
	}
}

func TestChunkReaderReusesTheSuppliedBuffer(t *testing.T) {
	// Buffer reuse is what keeps a multi-chunk download at one chunk of memory
	// rather than one allocation per chunk.
	nodes := startNodes(t, 1)
	payload := bytes.Repeat([]byte("b"), 4096)
	placement := storeOn(t, nodes, payload)

	buf := make([]byte, 0, len(payload))
	r := newReader(t)

	got, err := r.Fetch(context.Background(), placement, buf)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if cap(got) != cap(buf) || &got[:1][0] != &buf[:1][0] {
		t.Fatal("Fetch allocated a new buffer despite one being supplied")
	}
}

func TestChunkReaderGrowsAnUndersizedBuffer(t *testing.T) {
	nodes := startNodes(t, 1)
	payload := bytes.Repeat([]byte("c"), 8192)
	placement := storeOn(t, nodes, payload)

	got, err := newReader(t).Fetch(context.Background(), placement, make([]byte, 0, 16))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload differs after buffer growth")
	}
}

func TestChunkReaderStopsOnClientCancellation(t *testing.T) {
	// A cancelled request is the client's doing; trying every replica would
	// burn work on a response nobody is waiting for.
	nodes := startNodes(t, 3)
	payload := []byte("client went away")
	placement := storeOn(t, nodes, payload)
	for _, node := range nodes {
		node.kill()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newReader(t).Fetch(ctx, placement, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

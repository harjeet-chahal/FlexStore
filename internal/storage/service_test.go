package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/checksum"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// testNode is a real storage node listening on a real (ephemeral) TCP port.
// Using the actual gRPC transport rather than a mock means the streaming
// framing, message limits and error codes are all under test.
type testNode struct {
	id      string
	addr    string
	store   *ChunkStore
	client  flexstorev1.StorageNodeServiceClient
	cleanup func()
}

func startTestNode(t *testing.T, id string) *testNode {
	t.Helper()

	store, err := NewChunkStore(t.TempDir(), false, 1<<30)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := NewService(id, store, log, observability.NewMetrics("test"))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	flexstorev1.RegisterStorageNodeServiceServer(srv, svc)
	go func() {
		// Serve returns when the server is stopped; that is not an error.
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	n := &testNode{
		id:     id,
		addr:   lis.Addr().String(),
		store:  store,
		client: flexstorev1.NewStorageNodeServiceClient(conn),
		cleanup: func() {
			_ = conn.Close()
			srv.Stop()
		},
	}
	t.Cleanup(n.cleanup)
	return n
}

func (n *testNode) put(t *testing.T, chunkID string, payload []byte, sum string) *flexstorev1.StoreChunkResponse {
	t.Helper()
	stream, err := n.client.StoreChunk(context.Background())
	if err != nil {
		t.Fatalf("StoreChunk: %v", err)
	}
	if err := stream.Send(&flexstorev1.StoreChunkRequest{
		Payload: &flexstorev1.StoreChunkRequest_Header{Header: &flexstorev1.ChunkHeader{
			ChunkId: chunkID, SizeBytes: int64(len(payload)), ChecksumSha256: sum,
		}},
	}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	const frame = 64 << 10
	for off := 0; off < len(payload); off += frame {
		end := min(off+frame, len(payload))
		if err := stream.Send(&flexstorev1.StoreChunkRequest{
			Payload: &flexstorev1.StoreChunkRequest_Data{Data: payload[off:end]},
		}); err != nil {
			t.Fatalf("send frame: %v", err)
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	return resp
}

func (n *testNode) get(chunkID, expectSum string) ([]byte, error) {
	stream, err := n.client.ReadChunk(context.Background(), &flexstorev1.ReadChunkRequest{
		ChunkId: chunkID, ExpectedChecksumSha256: expectSum,
	})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return buf.Bytes(), nil
		}
		if err != nil {
			return buf.Bytes(), err
		}
		buf.Write(msg.GetData())
	}
}

func TestServiceStoreAndReadRoundTrip(t *testing.T) {
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	// Multi-frame payload so the stream reassembly path is genuinely exercised.
	payload := bytes.Repeat([]byte("flexstore-chunk-"), 40_000) // ~640 KiB
	sum := checksum.Sum(payload)

	resp := n.put(t, id, payload, sum)
	if resp.BytesWritten != int64(len(payload)) {
		t.Fatalf("BytesWritten = %d, want %d", resp.BytesWritten, len(payload))
	}
	if resp.ChecksumSha256 != sum {
		t.Fatalf("node reported checksum %s, want %s", resp.ChecksumSha256, sum)
	}

	got, err := n.get(id, sum)
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-tripped %d bytes, want %d, and contents differ", len(got), len(payload))
	}
}

func TestServiceRejectsCorruptWriteWithFailedPrecondition(t *testing.T) {
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	payload := []byte("actual bytes")
	// Advertise a checksum that does not match: simulates a corrupted transfer.
	badSum := checksum.Sum([]byte("different bytes"))

	stream, err := n.client.StoreChunk(context.Background())
	if err != nil {
		t.Fatalf("StoreChunk: %v", err)
	}
	_ = stream.Send(&flexstorev1.StoreChunkRequest{
		Payload: &flexstorev1.StoreChunkRequest_Header{Header: &flexstorev1.ChunkHeader{
			ChunkId: id, SizeBytes: int64(len(payload)), ChecksumSha256: badSum,
		}},
	})
	_ = stream.Send(&flexstorev1.StoreChunkRequest{
		Payload: &flexstorev1.StoreChunkRequest_Data{Data: payload},
	})
	_, err = stream.CloseAndRecv()
	if err == nil {
		t.Fatal("expected the corrupt write to be rejected")
	}
	// FailedPrecondition is the agreed corruption signal; the gateway maps it
	// to 422 and the reader uses it to fail over to another replica.
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %s, want FailedPrecondition (err: %v)", got, err)
	}

	// And the chunk must not be readable.
	if _, err := n.get(id, ""); status.Code(err) != codes.NotFound {
		t.Fatalf("corrupt chunk is readable: %v", err)
	}
}

func TestServiceDetectsOnDiskCorruptionOnRead(t *testing.T) {
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	payload := []byte("healthy at write time")
	sum := checksum.Sum(payload)
	n.put(t, id, payload, sum)

	// Simulate bit-rot after the write succeeded.
	path, err := n.store.PathFor(id)
	if err != nil {
		t.Fatalf("PathFor: %v", err)
	}
	if err := os.WriteFile(path, []byte("rotted bytes here!!!!"), 0o644); err != nil {
		t.Fatalf("corrupting chunk: %v", err)
	}

	_, err = n.get(id, sum)
	if err == nil {
		t.Fatal("corrupt chunk was served without an error")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("status code = %s, want FailedPrecondition (err: %v)", got, err)
	}
}

func TestServiceReadWithoutExpectedChecksumStillStreams(t *testing.T) {
	// Verification is opt-in on the wire; the gateway always opts in, but the
	// repair path may want raw bytes.
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	payload := []byte("no expectation supplied")
	n.put(t, id, payload, checksum.Sum(payload))

	got, err := n.get(id, "")
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload differs")
	}
}

func TestServiceValidatesChunkIDs(t *testing.T) {
	n := startTestNode(t, "node-1")
	ctx := context.Background()

	for _, bad := range []string{"", "../../etc/passwd", "not-a-uuid"} {
		if _, err := n.client.CheckChunk(ctx, &flexstorev1.CheckChunkRequest{ChunkId: bad}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("CheckChunk(%q) code = %s, want InvalidArgument", bad, status.Code(err))
		}
		if _, err := n.client.DeleteChunk(ctx, &flexstorev1.DeleteChunkRequest{ChunkId: bad}); status.Code(err) != codes.InvalidArgument {
			t.Errorf("DeleteChunk(%q) code = %s, want InvalidArgument", bad, status.Code(err))
		}
	}
}

func TestServiceRejectsAStreamWithoutAHeader(t *testing.T) {
	n := startTestNode(t, "node-1")
	stream, err := n.client.StoreChunk(context.Background())
	if err != nil {
		t.Fatalf("StoreChunk: %v", err)
	}
	// Data before a header: a malformed client.
	_ = stream.Send(&flexstorev1.StoreChunkRequest{
		Payload: &flexstorev1.StoreChunkRequest_Data{Data: []byte("orphan")},
	})
	if _, err := stream.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status code = %s, want InvalidArgument (err: %v)", status.Code(err), err)
	}
}

func TestServiceReadMissingChunkIsNotFound(t *testing.T) {
	n := startTestNode(t, "node-1")
	if _, err := n.get(uuid.NewString(), ""); status.Code(err) != codes.NotFound {
		t.Fatalf("status code = %s, want NotFound (err: %v)", status.Code(err), err)
	}
}

func TestServiceCheckChunkVerifies(t *testing.T) {
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	payload := []byte("check me")
	sum := checksum.Sum(payload)
	n.put(t, id, payload, sum)

	resp, err := n.client.CheckChunk(context.Background(), &flexstorev1.CheckChunkRequest{
		ChunkId: id, VerifyChecksum: true,
	})
	if err != nil {
		t.Fatalf("CheckChunk: %v", err)
	}
	if !resp.Exists || !resp.ChecksumValid || resp.ChecksumSha256 != sum {
		t.Fatalf("CheckChunk = %+v, want a valid chunk with checksum %s", resp, sum)
	}
	if resp.SizeBytes != int64(len(payload)) {
		t.Fatalf("SizeBytes = %d, want %d", resp.SizeBytes, len(payload))
	}
}

func TestServiceDeleteChunk(t *testing.T) {
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	payload := []byte("delete me")
	n.put(t, id, payload, checksum.Sum(payload))

	resp, err := n.client.DeleteChunk(context.Background(), &flexstorev1.DeleteChunkRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("DeleteChunk: %v", err)
	}
	if !resp.Existed {
		t.Fatal("DeleteChunk reported the chunk as absent")
	}
	// Idempotent: the GC retries, so a second delete must succeed.
	resp, err = n.client.DeleteChunk(context.Background(), &flexstorev1.DeleteChunkRequest{ChunkId: id})
	if err != nil {
		t.Fatalf("second DeleteChunk: %v", err)
	}
	if resp.Existed {
		t.Fatal("second DeleteChunk should report existed=false")
	}
}

func TestServiceReplicateChunkPullsFromAPeer(t *testing.T) {
	source := startTestNode(t, "source")
	target := startTestNode(t, "target")

	id := uuid.NewString()
	payload := bytes.Repeat([]byte("replicate"), 20_000)
	sum := checksum.Sum(payload)
	source.put(t, id, payload, sum)

	resp, err := target.client.ReplicateChunk(context.Background(), &flexstorev1.ReplicateChunkRequest{
		ChunkId:        id,
		SourceAddress:  source.addr,
		ChecksumSha256: sum,
		SizeBytes:      int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("ReplicateChunk: %v", err)
	}
	if resp.BytesWritten != int64(len(payload)) {
		t.Fatalf("BytesWritten = %d, want %d", resp.BytesWritten, len(payload))
	}

	got, err := target.get(id, sum)
	if err != nil {
		t.Fatalf("reading the replicated chunk: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("replicated payload differs from the source")
	}
}

func TestServiceReplicateChunkRejectsAMissingSource(t *testing.T) {
	target := startTestNode(t, "target")
	_, err := target.client.ReplicateChunk(context.Background(), &flexstorev1.ReplicateChunkRequest{
		ChunkId:       uuid.NewString(),
		SourceAddress: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status code = %s, want InvalidArgument", status.Code(err))
	}
}

func TestServiceGetNodeStats(t *testing.T) {
	n := startTestNode(t, "node-1")
	payload := bytes.Repeat([]byte("s"), 4096)
	n.put(t, uuid.NewString(), payload, checksum.Sum(payload))

	resp, err := n.client.GetNodeStats(context.Background(), &flexstorev1.GetNodeStatsRequest{})
	if err != nil {
		t.Fatalf("GetNodeStats: %v", err)
	}
	if resp.NodeId != "node-1" {
		t.Errorf("NodeId = %q", resp.NodeId)
	}
	if resp.Stats.UsedBytes != int64(len(payload)) {
		t.Errorf("UsedBytes = %d, want %d", resp.Stats.UsedBytes, len(payload))
	}
	if resp.Stats.ChunkCount != 1 {
		t.Errorf("ChunkCount = %d, want 1", resp.Stats.ChunkCount)
	}
	if resp.Stats.TotalBytes <= 0 {
		t.Error("TotalBytes must be reported for capacity-aware placement to work")
	}
}

func TestServiceStoreChunkIsIdempotent(t *testing.T) {
	n := startTestNode(t, "node-1")
	id := uuid.NewString()
	payload := []byte("write me twice")
	sum := checksum.Sum(payload)

	first := n.put(t, id, payload, sum)
	if first.AlreadyExisted {
		t.Fatal("the first write should not report AlreadyExisted")
	}
	second := n.put(t, id, payload, sum)
	if !second.AlreadyExisted {
		t.Fatal("a repeated write should be recognised as a no-op so gateway retries are safe")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

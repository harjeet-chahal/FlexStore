package gateway

import (
	"io"
	"log/slog"
	"net"
	"os"
	"testing"

	"google.golang.org/grpc"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/nodeclient"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/storage"
)

// The gateway's data path is where the interesting distributed behaviour lives
// (fan-out, quorum, failover, end-to-end verification), so these tests run it
// against REAL storage nodes over REAL gRPC on loopback. Mocking the node
// would test the mock, not the protocol.

type fakeNode struct {
	id    string
	addr  string
	store *storage.ChunkStore
	srv   *grpc.Server
}

func startNode(t *testing.T, id string) *fakeNode {
	t.Helper()

	store, err := storage.NewChunkStore(t.TempDir(), false, 1<<30)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	svc := storage.NewService(id, store, testLogger(), observability.NewMetrics("test-node"))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	flexstorev1.RegisterStorageNodeServiceServer(srv, svc)
	go func() { _ = srv.Serve(lis) }()

	n := &fakeNode{id: id, addr: lis.Addr().String(), store: store, srv: srv}
	t.Cleanup(func() { srv.Stop() })
	return n
}

// kill stops the node abruptly, simulating a crashed container rather than a
// graceful shutdown.
func (n *fakeNode) kill() { n.srv.Stop() }

// corrupt overwrites a stored chunk's bytes on disk, simulating bit-rot.
func (n *fakeNode) corrupt(t *testing.T, chunkID string) {
	t.Helper()
	path, err := n.store.PathFor(chunkID)
	if err != nil {
		t.Fatalf("PathFor: %v", err)
	}
	if err := os.WriteFile(path, []byte("this is not the data you are looking for"), 0o644); err != nil {
		t.Fatalf("corrupting chunk: %v", err)
	}
}

func (n *fakeNode) info() *flexstorev1.NodeInfo {
	return &flexstorev1.NodeInfo{
		NodeId:      n.id,
		GrpcAddress: n.addr,
		Health:      flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY,
	}
}

func (n *fakeNode) has(t *testing.T, chunkID string) bool {
	t.Helper()
	res, err := n.store.Check(chunkID, true)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	return res.Exists
}

func startNodes(t *testing.T, n int) []*fakeNode {
	t.Helper()
	out := make([]*fakeNode, n)
	for i := range out {
		out[i] = startNode(t, string(rune('a'+i))+"-node")
	}
	return out
}

func nodeInfos(nodes []*fakeNode) []*flexstorev1.NodeInfo {
	out := make([]*flexstorev1.NodeInfo, len(nodes))
	for i, n := range nodes {
		out[i] = n.info()
	}
	return out
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newPool(t *testing.T) *nodeclient.Pool {
	t.Helper()
	p := nodeclient.NewPool(16 << 20)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

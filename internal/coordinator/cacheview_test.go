package coordinator

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
)

func sampleResponse() *flexstorev1.GetObjectResponse {
	created := time.Date(2026, 3, 4, 5, 6, 7, 8, time.UTC)
	return &flexstorev1.GetObjectResponse{
		Object: &flexstorev1.ObjectInfo{
			ObjectId:    "3f7a1f2c-0000-4000-8000-000000000001",
			Bucket:      "images",
			Key:         "cats/tabby.png",
			Version:     7,
			State:       flexstorev1.ObjectState_OBJECT_STATE_COMPLETE,
			SizeBytes:   12_582_912,
			ChunkSize:   8 << 20,
			ChunkCount:  2,
			Etag:        "abc123",
			ContentType: "image/png",
			CreatedAt:   timestamppb.New(created),
			CompletedAt: timestamppb.New(created.Add(time.Second)),
		},
		Chunks: []*flexstorev1.ChunkPlacement{
			{
				ChunkId: "aaaaaaaa-0000-4000-8000-000000000001", ChunkIndex: 0,
				SizeBytes: 8 << 20, ChecksumSha256: "deadbeef",
				Nodes: []*flexstorev1.NodeInfo{
					{NodeId: "n1", GrpcAddress: "n1:9100", Health: flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY},
					{NodeId: "n2", GrpcAddress: "n2:9100", Health: flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY},
				},
			},
			{
				ChunkId: "bbbbbbbb-0000-4000-8000-000000000002", ChunkIndex: 1,
				SizeBytes: 4 << 20, ChecksumSha256: "cafebabe",
				Nodes: []*flexstorev1.NodeInfo{
					{NodeId: "n3", GrpcAddress: "n3:9100", Health: flexstorev1.NodeHealth_NODE_HEALTH_HEALTHY},
				},
			},
		},
	}
}

func TestCachedObjectRoundTripsThroughJSON(t *testing.T) {
	original := sampleResponse()

	encoded, err := json.Marshal(newCachedObject(original))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded cachedObject
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := decoded.toProto()

	o, w := got.Object, original.Object
	if o.ObjectId != w.ObjectId || o.Bucket != w.Bucket || o.Key != w.Key ||
		o.Version != w.Version || o.SizeBytes != w.SizeBytes || o.ChunkSize != w.ChunkSize ||
		o.ChunkCount != w.ChunkCount || o.Etag != w.Etag || o.ContentType != w.ContentType {
		t.Fatalf("object metadata changed across the cache:\ngot  %+v\nwant %+v", o, w)
	}
	if !o.CreatedAt.AsTime().Equal(w.CreatedAt.AsTime()) {
		t.Errorf("CreatedAt = %s, want %s", o.CreatedAt.AsTime(), w.CreatedAt.AsTime())
	}
	if !o.CompletedAt.AsTime().Equal(w.CompletedAt.AsTime()) {
		t.Errorf("CompletedAt = %s, want %s", o.CompletedAt.AsTime(), w.CompletedAt.AsTime())
	}

	// Chunk order is the object's byte order. Losing it would silently corrupt
	// every download, so it is asserted explicitly.
	if len(got.Chunks) != len(original.Chunks) {
		t.Fatalf("chunk count = %d, want %d", len(got.Chunks), len(original.Chunks))
	}
	for i := range got.Chunks {
		g, wc := got.Chunks[i], original.Chunks[i]
		if g.ChunkId != wc.ChunkId || g.ChunkIndex != wc.ChunkIndex ||
			g.SizeBytes != wc.SizeBytes || g.ChecksumSha256 != wc.ChecksumSha256 {
			t.Fatalf("chunk %d changed:\ngot  %+v\nwant %+v", i, g, wc)
		}
		if len(g.Nodes) != len(wc.Nodes) {
			t.Fatalf("chunk %d: %d replica locations, want %d", i, len(g.Nodes), len(wc.Nodes))
		}
		for j := range g.Nodes {
			if g.Nodes[j].NodeId != wc.Nodes[j].NodeId || g.Nodes[j].GrpcAddress != wc.Nodes[j].GrpcAddress {
				t.Fatalf("chunk %d replica %d changed: %+v", i, j, g.Nodes[j])
			}
		}
	}
}

func TestCachedObjectDoesNotPreserveNodeHealth(t *testing.T) {
	// Health is deliberately dropped: a node can die inside the cache TTL, so
	// a cached "HEALTHY" would be a lie the gateway might act on. The gateway
	// treats cached locations as candidates and fails over instead.
	decoded := newCachedObject(sampleResponse()).toProto()
	for _, c := range decoded.Chunks {
		for _, n := range c.Nodes {
			if n.Health != flexstorev1.NodeHealth_NODE_HEALTH_UNSPECIFIED {
				t.Fatalf("cached replica %s carries health %s; it must be UNSPECIFIED", n.NodeId, n.Health)
			}
		}
	}
}

func TestCachedObjectHandlesMissingTimestamps(t *testing.T) {
	resp := sampleResponse()
	resp.Object.CreatedAt = nil
	resp.Object.CompletedAt = nil

	got := newCachedObject(resp).toProto()
	if got.Object.CreatedAt != nil || got.Object.CompletedAt != nil {
		t.Fatal("absent timestamps should stay absent rather than becoming the Unix epoch")
	}
}

func TestCachedObjectIsCompact(t *testing.T) {
	// The cache holds one entry per hot object; a bloated encoding turns a
	// small Redis into an eviction storm. Single-letter JSON tags are the
	// reason this is small, so guard against someone "tidying" them.
	encoded, err := json.Marshal(newCachedObject(sampleResponse()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if len(encoded) > 700 {
		t.Fatalf("cache entry is %d bytes for a 2-chunk object; encoding has regressed:\n%s",
			len(encoded), encoded)
	}
}

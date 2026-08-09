package coordinator

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
)

// cachedObject is the JSON shape stored in Redis for GetObject.
//
// A hand-written struct rather than protojson: the cache is an internal
// implementation detail, and a compact explicit shape makes it obvious what is
// (and is not) safe to serve from cache. Notably it stores replica *locations*
// but no capacity or load figures, because those go stale far faster than the
// layout does.
type cachedObject struct {
	ObjectID    string        `json:"o"`
	Bucket      string        `json:"b"`
	Key         string        `json:"k"`
	Version     int64         `json:"v"`
	SizeBytes   int64         `json:"s"`
	ChunkSize   int64         `json:"cs"`
	ChunkCount  int32         `json:"cc"`
	ETag        string        `json:"e"`
	ContentType string        `json:"ct"`
	CreatedAt   int64         `json:"ca"`
	CompletedAt int64         `json:"da"`
	Chunks      []cachedChunk `json:"c"`
}

type cachedChunk struct {
	ID       string         `json:"i"`
	Index    int32          `json:"x"`
	Size     int64          `json:"s"`
	Checksum string         `json:"h"`
	Nodes    []cachedTarget `json:"n"`
}

type cachedTarget struct {
	NodeID  string `json:"i"`
	Address string `json:"a"`
}

func newCachedObject(resp *flexstorev1.GetObjectResponse) cachedObject {
	o := resp.Object
	c := cachedObject{
		ObjectID:    o.ObjectId,
		Bucket:      o.Bucket,
		Key:         o.Key,
		Version:     o.Version,
		SizeBytes:   o.SizeBytes,
		ChunkSize:   o.ChunkSize,
		ChunkCount:  o.ChunkCount,
		ETag:        o.Etag,
		ContentType: o.ContentType,
		Chunks:      make([]cachedChunk, 0, len(resp.Chunks)),
	}
	if o.CreatedAt != nil {
		c.CreatedAt = o.CreatedAt.AsTime().UnixNano()
	}
	if o.CompletedAt != nil {
		c.CompletedAt = o.CompletedAt.AsTime().UnixNano()
	}
	for _, ch := range resp.Chunks {
		cc := cachedChunk{
			ID:       ch.ChunkId,
			Index:    ch.ChunkIndex,
			Size:     ch.SizeBytes,
			Checksum: ch.ChecksumSha256,
			Nodes:    make([]cachedTarget, 0, len(ch.Nodes)),
		}
		for _, n := range ch.Nodes {
			cc.Nodes = append(cc.Nodes, cachedTarget{NodeID: n.NodeId, Address: n.GrpcAddress})
		}
		c.Chunks = append(c.Chunks, cc)
	}
	return c
}

func (c cachedObject) toProto() *flexstorev1.GetObjectResponse {
	resp := &flexstorev1.GetObjectResponse{
		Object: &flexstorev1.ObjectInfo{
			ObjectId:    c.ObjectID,
			Bucket:      c.Bucket,
			Key:         c.Key,
			Version:     c.Version,
			State:       flexstorev1.ObjectState_OBJECT_STATE_COMPLETE,
			SizeBytes:   c.SizeBytes,
			ChunkSize:   c.ChunkSize,
			ChunkCount:  c.ChunkCount,
			Etag:        c.ETag,
			ContentType: c.ContentType,
		},
		Chunks: make([]*flexstorev1.ChunkPlacement, 0, len(c.Chunks)),
	}
	if c.CreatedAt != 0 {
		resp.Object.CreatedAt = timestamppb.New(time.Unix(0, c.CreatedAt))
	}
	if c.CompletedAt != 0 {
		resp.Object.CompletedAt = timestamppb.New(time.Unix(0, c.CompletedAt))
	}
	for _, ch := range c.Chunks {
		p := &flexstorev1.ChunkPlacement{
			ChunkId:        ch.ID,
			ChunkIndex:     ch.Index,
			SizeBytes:      ch.Size,
			ChecksumSha256: ch.Checksum,
			Nodes:          make([]*flexstorev1.NodeInfo, 0, len(ch.Nodes)),
		}
		for _, n := range ch.Nodes {
			p.Nodes = append(p.Nodes, &flexstorev1.NodeInfo{
				NodeId:      n.NodeID,
				GrpcAddress: n.Address,
				// Cached entries carry no health: a node can die inside the
				// cache TTL, so the gateway must treat these as candidates to
				// try, not as guaranteed-live endpoints.
				Health: flexstorev1.NodeHealth_NODE_HEALTH_UNSPECIFIED,
			})
		}
		resp.Chunks = append(resp.Chunks, p)
	}
	return resp
}

package metadata

import (
	"errors"
	"testing"
	"time"
)

func TestNodeRegistrationAndHeartbeat(t *testing.T) {
	s, ctx := testStore(t)
	ids := registerNodes(t, ctx, s, 1)
	id := ids[0]

	got, err := s.GetNode(ctx, id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Health != "HEALTHY" {
		t.Fatalf("health = %s, want HEALTHY", got.Health)
	}
	if got.LastHeartbeatAt.IsZero() {
		t.Fatal("registration did not record a heartbeat time")
	}

	// A heartbeat refreshes capacity, which is what placement weights on.
	later := time.Now().Add(time.Second)
	err = s.RecordHeartbeat(ctx, id, id+":9100", NodeStats{
		TotalBytes: 1000, UsedBytes: 400, AvailableBytes: 600,
		ChunkCount: 7, ActiveRequests: 2,
	}, "HEALTHY", later)
	if err != nil {
		t.Fatalf("RecordHeartbeat: %v", err)
	}

	got, err = s.GetNode(ctx, id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.UsedBytes != 400 || got.AvailableBytes != 600 || got.ChunkCount != 7 || got.ActiveRequests != 2 {
		t.Fatalf("heartbeat stats were not persisted: %+v", got)
	}
	if !got.LastHeartbeatAt.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("LastHeartbeatAt = %s, expected it to be refreshed", got.LastHeartbeatAt)
	}
}

func TestHeartbeatFromAnUnknownNodeIsNotFound(t *testing.T) {
	// This is what triggers the must_reregister response: after a metadata
	// reset the node must rejoin rather than heartbeat into the void.
	s, ctx := testStore(t)
	err := s.RecordHeartbeat(ctx, "never-registered-node", "x:9100", NodeStats{}, "HEALTHY", time.Now())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestReRegisterKeepsNodeIdentity(t *testing.T) {
	// A node restarting with the same ID must keep its recorded replicas --
	// otherwise every restart would look like data loss.
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "identity.bin"
	objID := putObject(t, ctx, s, bkt, key, nodes, 100)

	if err := s.UpsertNode(ctx, nodes[0], nodes[0]+":9100", "HEALTHY", NodeStats{
		TotalBytes: 1 << 40, AvailableBytes: 1 << 40,
	}, time.Now()); err != nil {
		t.Fatalf("re-registering: %v", err)
	}

	counts, err := s.ReplicaCountsForObject(ctx, objID)
	if err != nil {
		t.Fatalf("ReplicaCountsForObject: %v", err)
	}
	for id, n := range counts {
		if n != 3 {
			t.Fatalf("chunk %s lost replicas across a node restart: %d of 3", id, n)
		}
	}
}

func TestListNodesHealthyOnly(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)

	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}

	healthy, err := s.ListNodes(ctx, true)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	for _, n := range healthy {
		if n.ID == nodes[0] {
			t.Fatal("a DEAD node appeared in the healthy roster; it could be selected for placement")
		}
	}

	all, err := s.ListNodes(ctx, false)
	if err != nil {
		t.Fatalf("ListNodes(all): %v", err)
	}
	var found bool
	for _, n := range all {
		if n.ID == nodes[0] {
			found = true
			if n.Health != "DEAD" {
				t.Fatalf("health = %s, want DEAD", n.Health)
			}
		}
	}
	if !found {
		t.Fatal("the DEAD node vanished from the full roster")
	}
}

func TestNodeDeathAndRecoveryFlipReplicaAvailability(t *testing.T) {
	s, ctx := testStore(t)
	nodes := registerNodes(t, ctx, s, 3)
	bkt, key := bucket(t), "availability.bin"
	objID := putObject(t, ctx, s, bkt, key, nodes, 100, 200)

	// A node dies: its replicas are written off for durability accounting, but
	// the rows survive because the data probably still exists.
	n, err := s.MarkReplicasUnavailableOnNode(ctx, nodes[0])
	if err != nil {
		t.Fatalf("MarkReplicasUnavailableOnNode: %v", err)
	}
	if n != 2 {
		t.Fatalf("marked %d replicas unavailable, want 2", n)
	}

	counts, err := s.ReplicaCountsForObject(ctx, objID)
	if err != nil {
		t.Fatalf("ReplicaCountsForObject: %v", err)
	}
	for id, c := range counts {
		if c != 2 {
			t.Fatalf("chunk %s reports %d available replicas after a node death, want 2", id, c)
		}
	}

	// The read path must not offer the dead node's replicas.
	if _, err := s.SetNodeHealth(ctx, nodes[0], "DEAD"); err != nil {
		t.Fatalf("SetNodeHealth: %v", err)
	}
	_, chunks, err := s.GetObject(ctx, bkt, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	for _, c := range chunks {
		for _, r := range c.Replicas {
			if r.NodeID == nodes[0] {
				t.Fatalf("chunk %s still offers the dead node %s as a read target", c.ID, r.NodeID)
			}
		}
	}

	// Recovery does NOT restore them. A returning node's files are evidence,
	// not truth: the replicas become STALE and stay out of durability
	// accounting until the reconciler verifies each one against its recorded
	// checksum. Trusting them here would let a rolled-back disk silently become
	// authoritative again.
	staled, err := s.MarkReplicasStaleOnNode(ctx, nodes[0])
	if err != nil {
		t.Fatalf("MarkReplicasStaleOnNode: %v", err)
	}
	if staled != 2 {
		t.Fatalf("marked %d replicas stale, want 2", staled)
	}
	counts, _ = s.ReplicaCountsForObject(ctx, objID)
	for id, c := range counts {
		if c != 2 {
			t.Fatalf("chunk %s counts %d replicas while the returning node is unverified, want 2", id, c)
		}
	}

	// Verification is what promotes them back.
	for chunkID := range counts {
		promoted, err := s.PromoteVerifiedReplica(ctx, chunkID, nodes[0])
		if err != nil {
			t.Fatalf("PromoteVerifiedReplica: %v", err)
		}
		if !promoted {
			t.Fatalf("chunk %s: stale replica was not promoted after verification", chunkID)
		}
	}
	counts, _ = s.ReplicaCountsForObject(ctx, objID)
	for id, c := range counts {
		if c != 3 {
			t.Fatalf("chunk %s has %d replicas after verification, want 3", id, c)
		}
	}
}

func TestGetUnknownNode(t *testing.T) {
	s, ctx := testStore(t)
	if _, err := s.GetNode(ctx, "no-such-node"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

package placement

import (
	"errors"
	"fmt"
	"testing"
)

// node builds a healthy node with the given free ratio.
func node(id string, total, used int64) NodeInfo {
	return NodeInfo{
		ID:             id,
		Address:        id + ":9100",
		Health:         HealthHealthy,
		TotalBytes:     total,
		UsedBytes:      used,
		AvailableBytes: total - used,
	}
}

func newDeterministic(cfg Config) *Weighted {
	// A fixed seed makes every placement in these tests reproducible.
	return NewWeighted(cfg, 1)
}

func TestSelectNodesReturnsDistinctNodes(t *testing.T) {
	nodes := []NodeInfo{
		node("n1", 100, 0), node("n2", 100, 0), node("n3", 100, 0),
		node("n4", 100, 0), node("n5", 100, 0),
	}
	w := newDeterministic(DefaultConfig())

	// Repeat: weighted sampling is random, so a single pass could pass by luck.
	for i := 0; i < 200; i++ {
		got, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 3, ChunkSize: 1})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if len(got) != 3 {
			t.Fatalf("iteration %d: got %d nodes, want 3", i, len(got))
		}
		seen := map[string]bool{}
		for _, n := range got {
			if seen[n.ID] {
				t.Fatalf("iteration %d: node %s selected twice -- replication factor would be a lie", i, n.ID)
			}
			seen[n.ID] = true
		}
	}
}

func TestSelectNodesExcludesDeadAndSuspectByDefault(t *testing.T) {
	nodes := []NodeInfo{
		node("healthy-1", 100, 0),
		node("healthy-2", 100, 0),
		func() NodeInfo { n := node("dead", 100, 0); n.Health = HealthDead; return n }(),
		func() NodeInfo { n := node("suspect", 100, 0); n.Health = HealthSuspect; return n }(),
		func() NodeInfo { n := node("unknown", 100, 0); n.Health = HealthUnknown; return n }(),
	}
	w := newDeterministic(DefaultConfig())

	got, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 2, ChunkSize: 1})
	if err != nil {
		t.Fatalf("SelectNodes: %v", err)
	}
	for _, n := range got {
		if n.Health != HealthHealthy {
			t.Fatalf("selected a %s node: %s", n.Health, n.ID)
		}
	}

	// Only two healthy nodes exist, so asking for three must fail rather than
	// quietly returning two.
	if _, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 3, ChunkSize: 1}); !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("expected ErrInsufficientNodes, got %v", err)
	}
}

func TestSelectNodesAllowSuspectWhenConfigured(t *testing.T) {
	nodes := []NodeInfo{
		node("healthy-1", 100, 0),
		node("healthy-2", 100, 0),
		func() NodeInfo { n := node("suspect", 100, 0); n.Health = HealthSuspect; return n }(),
	}
	cfg := DefaultConfig()
	cfg.AllowSuspect = true
	w := newDeterministic(cfg)

	got, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 3, ChunkSize: 1})
	if err != nil {
		t.Fatalf("SelectNodes with AllowSuspect: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d nodes, want 3", len(got))
	}
}

func TestSelectNodesRespectsCapacityHeadroom(t *testing.T) {
	// Headroom 2.0 means a node needs 2x the chunk size free.
	nodes := []NodeInfo{
		node("roomy-1", 1000, 0), // 1000 free
		node("roomy-2", 1000, 0), // 1000 free
		node("tight", 1000, 900), // 100 free -- not enough for 2 * 100
		node("full", 1000, 1000), // 0 free
	}
	w := newDeterministic(DefaultConfig())

	got, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 2, ChunkSize: 100})
	if err != nil {
		t.Fatalf("SelectNodes: %v", err)
	}
	for _, n := range got {
		if n.ID == "tight" || n.ID == "full" {
			t.Fatalf("selected %s, which lacks the configured headroom", n.ID)
		}
	}
	if _, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 3, ChunkSize: 100}); !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("expected ErrInsufficientNodes when only 2 nodes have headroom, got %v", err)
	}
}

func TestWeightedSelectionFavoursEmptierNodes(t *testing.T) {
	// Statistical assertion: over many placements the near-empty node must be
	// chosen noticeably more often than the near-full one. This is the whole
	// point of capacity-weighted placement, so it deserves a real check rather
	// than a comment.
	nodes := []NodeInfo{
		node("empty", 1000, 0),         // free ratio 1.00
		node("half", 1000, 500),        // free ratio 0.50
		node("nearly-full", 1000, 950), // free ratio 0.05
	}
	w := newDeterministic(DefaultConfig())

	counts := map[string]int{}
	const iterations = 3000
	for i := 0; i < iterations; i++ {
		got, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 1, ChunkSize: 1})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		counts[got[0].ID]++
	}

	if counts["empty"] <= counts["half"] {
		t.Errorf("empty node chosen %d times, half-full %d: capacity weighting is not working",
			counts["empty"], counts["half"])
	}
	if counts["half"] <= counts["nearly-full"] {
		t.Errorf("half-full chosen %d times, nearly-full %d: capacity weighting is not working",
			counts["half"], counts["nearly-full"])
	}
	// Every node must remain reachable; a strategy that starves a node
	// entirely would never re-balance once it frees up.
	for _, id := range []string{"empty", "half", "nearly-full"} {
		if counts[id] == 0 {
			t.Errorf("node %s was never selected in %d iterations", id, iterations)
		}
	}
}

func TestLoadPenaltyDeprioritisesBusyNodes(t *testing.T) {
	idle := node("idle", 1000, 0)
	busy := node("busy", 1000, 0)
	busy.ActiveRequests = 20

	w := newDeterministic(DefaultConfig())
	counts := map[string]int{}
	for i := 0; i < 2000; i++ {
		got, err := w.SelectNodes([]NodeInfo{idle, busy}, Request{Key: "k", Replicas: 1, ChunkSize: 1})
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		counts[got[0].ID]++
	}
	if counts["idle"] <= counts["busy"] {
		t.Fatalf("idle chosen %d, busy %d: load penalty is not applied",
			counts["idle"], counts["busy"])
	}
}

func TestLoadPenaltyCanBeDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LoadPenalty = 0

	idle := node("idle", 1000, 0)
	busy := node("busy", 1000, 0)
	busy.ActiveRequests = 50

	w := NewWeighted(cfg, 7)
	counts := map[string]int{}
	for i := 0; i < 2000; i++ {
		got, _ := w.SelectNodes([]NodeInfo{idle, busy}, Request{Key: "k", Replicas: 1, ChunkSize: 1})
		counts[got[0].ID]++
	}
	// With the penalty off the two identical-capacity nodes should be close to
	// even; allow a generous margin so this is not flaky.
	ratio := float64(counts["idle"]) / float64(counts["busy"])
	if ratio < 0.7 || ratio > 1.4 {
		t.Fatalf("expected roughly even split with LoadPenalty=0, got idle=%d busy=%d",
			counts["idle"], counts["busy"])
	}
}

func TestSelectNodesRejectsInvalidRequests(t *testing.T) {
	w := newDeterministic(DefaultConfig())
	nodes := []NodeInfo{node("n1", 100, 0)}

	if _, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 0, ChunkSize: 1}); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("replicas=0 should be ErrInvalidRequest, got %v", err)
	}
	if _, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 1, ChunkSize: -1}); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("negative chunkSize should be ErrInvalidRequest, got %v", err)
	}
}

func TestSelectNodesOnEmptyCluster(t *testing.T) {
	w := newDeterministic(DefaultConfig())
	if _, err := w.SelectNodes(nil, Request{Key: "k", Replicas: 3, ChunkSize: 1}); !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("expected ErrInsufficientNodes on an empty cluster, got %v", err)
	}
}

func TestFreeRatioClamps(t *testing.T) {
	cases := []struct {
		n    NodeInfo
		want float64
	}{
		{NodeInfo{TotalBytes: 0, AvailableBytes: 100}, 0},   // unknown capacity
		{NodeInfo{TotalBytes: 100, AvailableBytes: -5}, 0},  // over-committed
		{NodeInfo{TotalBytes: 100, AvailableBytes: 500}, 1}, // stale over-report
		{NodeInfo{TotalBytes: 100, AvailableBytes: 25}, 0.25},
	}
	for _, c := range cases {
		if got := c.n.FreeRatio(); got != c.want {
			t.Errorf("FreeRatio(%+v) = %v, want %v", c.n, got, c.want)
		}
	}
}

func TestStrategyInterfaceIsSatisfied(t *testing.T) {
	// The upload path depends only on this interface; keeping the assertion in
	// a test means a signature change breaks here rather than in the gateway.
	var s Strategy = NewWeighted(DefaultConfig(), 1)
	if s.Name() == "" {
		t.Fatal("Name() must identify the strategy for logs and metrics")
	}
}

func TestSelectNodesIsDeterministicForAFixedSeed(t *testing.T) {
	nodes := []NodeInfo{
		node("a", 100, 10), node("b", 100, 20), node("c", 100, 30), node("d", 100, 40),
	}
	first := fmt.Sprint(mustSelect(t, NewWeighted(DefaultConfig(), 42), nodes))
	second := fmt.Sprint(mustSelect(t, NewWeighted(DefaultConfig(), 42), nodes))
	if first != second {
		t.Fatalf("same seed produced different placements:\n%s\n%s", first, second)
	}
}

func mustSelect(t *testing.T, w *Weighted, nodes []NodeInfo) []string {
	t.Helper()
	var out []string
	for i := 0; i < 5; i++ {
		got, err := w.SelectNodes(nodes, Request{Key: "k", Replicas: 2, ChunkSize: 1})
		if err != nil {
			t.Fatalf("SelectNodes: %v", err)
		}
		for _, n := range got {
			out = append(out, n.ID)
		}
	}
	return out
}

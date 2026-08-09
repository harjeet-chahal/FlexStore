package placement

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
)

// capacityUnit scales the small, readable numbers in these tests into byte
// counts large enough to clear the strategy's capacity-headroom filter, without
// changing the free-space *ratios* the weighting actually depends on.
const capacityUnit = 1 << 20

func cluster(n int, total, used int64) []NodeInfo {
	out := make([]NodeInfo, n)
	for i := range out {
		out[i] = node(fmt.Sprintf("storage-node-%d", i+1), total*capacityUnit, used*capacityUnit)
	}
	return out
}

func keys(n int) []string {
	// Deterministic pseudo-chunk-IDs: real chunk IDs are UUIDs, and using
	// real UUID formatting keeps the hash input distribution honest.
	out := make([]string, n)
	for i := range out {
		out[i] = uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("chunk-%d", i))).String()
	}
	return out
}

func selected(t *testing.T, s Strategy, nodes []NodeInfo, key string, replicas int) []string {
	t.Helper()
	got, err := s.SelectNodes(nodes, Request{Key: key, Replicas: replicas, ChunkSize: 1024})
	if err != nil {
		t.Fatalf("SelectNodes(%s): %v", key, err)
	}
	out := make([]string, len(got))
	for i, n := range got {
		out[i] = n.ID
	}
	return out
}

func TestRendezvousIsDeterministic(t *testing.T) {
	// The defining property: same key, same cluster, same answer -- every time,
	// in any process, with no shared state. Everything else rendezvous buys us
	// depends on this holding.
	nodes := cluster(5, 1000, 100)
	r := NewRendezvous(DefaultConfig())

	for _, key := range keys(50) {
		first := selected(t, r, nodes, key, 3)
		for i := 0; i < 5; i++ {
			again := selected(t, r, nodes, key, 3)
			for j := range first {
				if first[j] != again[j] {
					t.Fatalf("key %s: placement changed between calls: %v then %v", key, first, again)
				}
			}
		}
	}
}

func TestRendezvousIsIndependentOfNodeOrdering(t *testing.T) {
	// The coordinator loads nodes from SQL; a different ORDER BY must not
	// change where data lives.
	nodes := cluster(5, 1000, 100)
	reversed := make([]NodeInfo, len(nodes))
	for i, n := range nodes {
		reversed[len(nodes)-1-i] = n
	}
	r := NewRendezvous(DefaultConfig())

	for _, key := range keys(30) {
		a := selected(t, r, nodes, key, 3)
		b := selected(t, r, reversed, key, 3)
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("key %s: ordering changed placement: %v vs %v", key, a, b)
			}
		}
	}
}

func TestRendezvousReturnsDistinctNodes(t *testing.T) {
	nodes := cluster(5, 1000, 100)
	r := NewRendezvous(DefaultConfig())

	for _, key := range keys(200) {
		got := selected(t, r, nodes, key, 3)
		seen := map[string]bool{}
		for _, id := range got {
			if seen[id] {
				t.Fatalf("key %s: node %s selected twice", key, id)
			}
			seen[id] = true
		}
	}
}

func TestRendezvousDistributesAcrossNodes(t *testing.T) {
	// With equal-capacity nodes the load should be roughly even. A strategy
	// that piled everything onto one node would still be "deterministic", so
	// determinism alone is not enough to call it correct.
	nodes := cluster(5, 1000, 100)
	r := NewRendezvous(DefaultConfig())

	const n = 5000
	counts := map[string]int{}
	for _, key := range keys(n) {
		for _, id := range selected(t, r, nodes, key, 3) {
			counts[id]++
		}
	}

	expected := float64(n*3) / 5
	for id, got := range counts {
		deviation := math.Abs(float64(got)-expected) / expected
		if deviation > 0.10 {
			t.Errorf("node %s holds %d replicas, expected ~%.0f (%.1f%% off)",
				id, got, expected, deviation*100)
		}
	}
	if len(counts) != 5 {
		t.Fatalf("only %d of 5 nodes were ever selected", len(counts))
	}
	t.Logf("replica distribution over %d chunks at RF=3: %v", n, counts)
}

func TestRendezvousMinimalMovementWhenANodeLeaves(t *testing.T) {
	// The reason to prefer rendezvous over random placement: losing a node must
	// only reassign the chunks that node actually held. A naive re-shuffle
	// would move ~all of them, turning one node failure into a cluster-wide
	// data migration.
	before := cluster(6, 1000, 100)
	after := before[:5] // storage-node-6 leaves
	r := NewRendezvous(DefaultConfig())

	const n = 3000
	var moved, hadLeaver int
	for _, key := range keys(n) {
		a := selected(t, r, before, key, 3)
		b := selected(t, r, after, key, 3)

		inA := map[string]bool{}
		for _, id := range a {
			inA[id] = true
		}
		if inA["storage-node-6"] {
			hadLeaver++
		}
		for _, id := range b {
			if !inA[id] {
				moved++
			}
		}
	}

	// Every chunk that had a replica on the departed node needs exactly one new
	// placement; nothing else should move at all.
	if moved != hadLeaver {
		t.Fatalf("%d replicas moved but only %d chunks were on the departed node; "+
			"placement is reshuffling unaffected data", moved, hadLeaver)
	}
	t.Logf("node left: %d of %d chunks affected, %d replica moves (theoretical minimum)",
		hadLeaver, n, moved)
}

func TestRendezvousMinimalMovementWhenANodeJoins(t *testing.T) {
	before := cluster(5, 1000, 100)
	after := cluster(6, 1000, 100)
	r := NewRendezvous(DefaultConfig())

	const n = 3000
	moved := 0
	for _, key := range keys(n) {
		a := selected(t, r, before, key, 3)
		b := selected(t, r, after, key, 3)
		inA := map[string]bool{}
		for _, id := range a {
			inA[id] = true
		}
		for _, id := range b {
			if !inA[id] {
				moved++
			}
		}
	}

	// A 6th node should claim roughly 1/6 of replicas and cause no other
	// movement. Allow generous slack; the point is that it is nothing like the
	// ~100% a random strategy would produce.
	total := n * 3
	fraction := float64(moved) / float64(total)
	if fraction > 0.25 {
		t.Fatalf("adding one node moved %.1f%% of replicas; expected roughly 1/6", fraction*100)
	}
	t.Logf("node joined: %.1f%% of %d replicas moved (ideal ~16.7%%)", fraction*100, total)
}

func TestRendezvousRespectsCapacityWeighting(t *testing.T) {
	// Unweighted HRW would fill a small node at the same rate as a large one.
	nodes := []NodeInfo{
		node("big-1", 1000*capacityUnit, 0),                // free ratio 1.0
		node("big-2", 1000*capacityUnit, 0),                // free ratio 1.0
		node("big-3", 1000*capacityUnit, 0),                // free ratio 1.0
		node("small", 1000*capacityUnit, 900*capacityUnit), // free ratio 0.1
	}
	r := NewRendezvous(DefaultConfig())

	counts := map[string]int{}
	for _, key := range keys(4000) {
		for _, id := range selected(t, r, nodes, key, 2) {
			counts[id]++
		}
	}
	for _, big := range []string{"big-1", "big-2", "big-3"} {
		if counts[big] <= counts["small"] {
			t.Errorf("%s got %d replicas, nearly-full node got %d: capacity weighting is not applied",
				big, counts[big], counts["small"])
		}
	}
	t.Logf("capacity-weighted distribution: %v", counts)
}

func TestRendezvousExcludesUnhealthyNodes(t *testing.T) {
	nodes := []NodeInfo{
		node("healthy-1", 1000*capacityUnit, 0),
		node("healthy-2", 1000*capacityUnit, 0),
		node("healthy-3", 1000*capacityUnit, 0),
		func() NodeInfo { n := node("dead", 1000*capacityUnit, 0); n.Health = HealthDead; return n }(),
		func() NodeInfo { n := node("suspect", 1000*capacityUnit, 0); n.Health = HealthSuspect; return n }(),
	}
	r := NewRendezvous(DefaultConfig())

	for _, key := range keys(200) {
		for _, id := range selected(t, r, nodes, key, 3) {
			if id == "dead" || id == "suspect" {
				t.Fatalf("key %s: selected unhealthy node %s", key, id)
			}
		}
	}
}

func TestRendezvousHonoursExclusions(t *testing.T) {
	// The repair path depends on this: a re-replication must never "restore"
	// durability by adding a second copy to a node that already has one.
	nodes := cluster(5, 1000, 100)
	r := NewRendezvous(DefaultConfig())

	for _, key := range keys(100) {
		occupied := selected(t, r, nodes, key, 2)
		got, err := r.SelectNodes(nodes, Request{
			Key: key, Replicas: 1, ChunkSize: 1024, Exclude: occupied,
		})
		if err != nil {
			t.Fatalf("SelectNodes with exclusions: %v", err)
		}
		for _, n := range got {
			for _, ex := range occupied {
				if n.ID == ex {
					t.Fatalf("key %s: selected excluded node %s", key, n.ID)
				}
			}
		}
	}
}

func TestRendezvousRequiresAKey(t *testing.T) {
	// Silently falling back to random placement would destroy determinism in a
	// way nobody would notice until a scrubber disagreed with the data.
	r := NewRendezvous(DefaultConfig())
	_, err := r.SelectNodes(cluster(5, 1000, 100), Request{Replicas: 3, ChunkSize: 1})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest for an empty key, got %v", err)
	}
}

func TestRendezvousInsufficientNodes(t *testing.T) {
	r := NewRendezvous(DefaultConfig())
	_, err := r.SelectNodes(cluster(2, 1000, 100), Request{Key: "k", Replicas: 3, ChunkSize: 1})
	if !errors.Is(err, ErrInsufficientNodes) {
		t.Fatalf("expected ErrInsufficientNodes, got %v", err)
	}
}

func TestNewStrategy(t *testing.T) {
	for _, name := range []string{"", StrategyWeighted, StrategyRendezvous} {
		s, err := New(name, DefaultConfig(), 1)
		if err != nil {
			t.Fatalf("New(%q): %v", name, err)
		}
		if s.Name() == "" {
			t.Fatalf("New(%q) returned a strategy with no name", name)
		}
	}
	// An unknown strategy must fail loudly rather than silently defaulting:
	// running the wrong placement policy is not something to discover weeks
	// later from a dashboard.
	if _, err := New("consistent-hashing", DefaultConfig(), 1); err == nil {
		t.Fatal("expected an error for an unknown strategy name")
	}
}

func TestHashUnitIntervalStaysInOpenInterval(t *testing.T) {
	// ln(0) and ln(1) are both unusable in the weighted-rendezvous formula.
	for i := 0; i < 20000; i++ {
		u := hashUnitInterval(fmt.Sprintf("key-%d", i), fmt.Sprintf("node-%d", i%7))
		if u <= 0 || u >= 1 {
			t.Fatalf("hashUnitInterval produced %v, outside (0,1)", u)
		}
	}
}

func TestHashUnitIntervalIsCollisionResistantAcrossBoundaries(t *testing.T) {
	// Without length-prefixing, ("ab","c") and ("a","bc") would hash the same
	// and two different chunks would rank nodes identically.
	if hashUnitInterval("ab", "c") == hashUnitInterval("a", "bc") {
		t.Fatal("key/node boundary is not encoded in the hash")
	}
}

// ---- benchmarks ----------------------------------------------------------

func BenchmarkRendezvousSelect(b *testing.B) {
	nodes := cluster(5, 1000, 100)
	r := NewRendezvous(DefaultConfig())
	ks := keys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.SelectNodes(nodes, Request{
			Key: ks[i%len(ks)], Replicas: 3, ChunkSize: 8 << 20,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRendezvousSelectLargeCluster(b *testing.B) {
	nodes := cluster(100, 1000, 100)
	r := NewRendezvous(DefaultConfig())
	ks := keys(1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.SelectNodes(nodes, Request{
			Key: ks[i%len(ks)], Replicas: 3, ChunkSize: 8 << 20,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWeightedSelect(b *testing.B) {
	nodes := cluster(5, 1000, 100)
	w := NewWeighted(DefaultConfig(), 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.SelectNodes(nodes, Request{
			Key: "k", Replicas: 3, ChunkSize: 8 << 20,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

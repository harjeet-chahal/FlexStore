package placement

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
)

// Rendezvous implements Highest Random Weight (HRW) placement.
//
// For a chunk key K and node N the score is a hash of (K, N). The chunk goes to
// the R highest-scoring nodes. Two properties follow directly and are what make
// it worth having alongside the weighted strategy:
//
//   - Deterministic. The same key and node set always produce the same
//     placement, with no shared state and no coordination. Any process can
//     compute where a chunk *should* live, which is what a future scrubber or a
//     client-side router needs.
//   - Minimal movement. When a node leaves, only the chunks it actually held
//     are reassigned; when a node joins it takes roughly its fair share and
//     nothing else moves. Contrast with the weighted strategy, whose placements
//     are random and therefore unreproducible after the fact.
//
// Capacity awareness uses the standard weighted-rendezvous transform
// score = -weight / ln(u), where u is the hash mapped into (0,1). That
// preserves HRW's minimal-movement property while making a node with twice the
// free capacity twice as likely to win -- a plain unweighted HRW would fill
// small nodes at the same rate as large ones.
type Rendezvous struct {
	cfg Config
}

// NewRendezvous builds a rendezvous strategy.
func NewRendezvous(cfg Config) *Rendezvous { return &Rendezvous{cfg: cfg} }

// Name implements Strategy.
func (r *Rendezvous) Name() string { return StrategyRendezvous }

// SelectNodes implements Strategy.
func (r *Rendezvous) SelectNodes(nodes []NodeInfo, req Request) ([]NodeInfo, error) {
	if req.Replicas < 1 {
		return nil, fmt.Errorf("%w: replicas must be >= 1, got %d", ErrInvalidRequest, req.Replicas)
	}
	if req.ChunkSize < 0 {
		return nil, fmt.Errorf("%w: chunkSize must be >= 0, got %d", ErrInvalidRequest, req.ChunkSize)
	}
	if req.Key == "" {
		// Without a key there is nothing to hash, and silently falling back to
		// random placement would quietly destroy the determinism guarantee that
		// is the entire reason for choosing this strategy.
		return nil, fmt.Errorf("%w: rendezvous placement requires a non-empty key", ErrInvalidRequest)
	}

	candidates := eligibleNodes(nodes, req, r.cfg)
	if len(candidates) < req.Replicas {
		return nil, fmt.Errorf("%w: need %d, have %d eligible of %d known",
			ErrInsufficientNodes, req.Replicas, len(candidates), len(nodes))
	}

	scored := make([]scoredNode, 0, len(candidates))
	for _, n := range candidates {
		scored = append(scored, scoredNode{node: n, score: r.score(req.Key, n)})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		// Deterministic tie-break. Ties are vanishingly unlikely with a 64-bit
		// hash, but "vanishingly unlikely" is not "never", and a
		// non-deterministic tie-break would break the determinism test in a way
		// that only shows up occasionally.
		return scored[i].node.ID < scored[j].node.ID
	})

	out := make([]NodeInfo, 0, req.Replicas)
	for i := 0; i < req.Replicas; i++ {
		out = append(out, scored[i].node)
	}
	return out, nil
}

type scoredNode struct {
	node  NodeInfo
	score float64
}

// score computes the weighted rendezvous score for one (key, node) pair.
func (r *Rendezvous) score(key string, n NodeInfo) float64 {
	u := hashUnitInterval(key, n.ID)

	weight := r.weight(n)
	if weight <= 0 {
		// Keep the node reachable rather than unplaceable: an eligible node
		// with an unknown or exhausted weight should still rank, just last.
		weight = r.cfg.MinWeight
		if weight <= 0 {
			weight = 1e-9
		}
	}
	// -w / ln(u) with u in (0,1): ln(u) is negative, so the score is positive
	// and increases with weight.
	return -weight / math.Log(u)
}

// weight is the node's capacity share, damped by current load exactly as in the
// weighted strategy so the two agree on what "a good node" means.
func (r *Rendezvous) weight(n NodeInfo) float64 {
	w := n.FreeRatio()
	if r.cfg.LoadPenalty > 0 {
		w /= 1 + r.cfg.LoadPenalty*float64(n.ActiveRequests)
	}
	if n.Health == HealthSuspect {
		w *= 0.1
	}
	if w < r.cfg.MinWeight {
		w = r.cfg.MinWeight
	}
	return w
}

// hashUnitInterval maps (key, nodeID) to a value in the open interval (0, 1).
//
// FNV-1a is used rather than a cryptographic hash because this is a placement
// decision, not a security boundary, and it must be fast: it runs once per
// (chunk, node) pair on every placement. The endpoints are excluded because
// ln(0) and ln(1) are both unusable in the weighted-rendezvous formula.
func hashUnitInterval(key, nodeID string) float64 {
	h := fnv.New64a()
	// Length-prefix the key so ("ab", "c") and ("a", "bc") cannot collide.
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(key)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte(nodeID))

	// Map to (0,1): shift off the top bit for a clean 63-bit mantissa, then
	// nudge away from both endpoints.
	const maxUint63 = float64(1 << 63)
	u := float64(mix64(h.Sum64())>>1) / maxUint63
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	if u >= 1 {
		u = math.Nextafter(1, 0)
	}
	return u
}

// mix64 is the MurmurHash3 64-bit finalizer.
//
// It is not optional. FNV-1a's last processed byte reaches the high bits of the
// digest through only a single multiply, and node IDs in a real cluster differ
// exactly in their last character ("storage-node-1" vs "storage-node-2"). Since
// the unit-interval mapping samples the *high* bits, raw FNV-1a produced a
// visibly lopsided distribution -- measured at 26% above expectation for one
// node and 17% below for two others across 5 000 chunks. Running the digest
// through a proper avalanche step brings every node within a couple of percent.
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Rank returns every eligible node ordered by descending rendezvous score.
//
// Exported for diagnostics and the placement benchmark: seeing the full ranking
// is how you verify that removing the top node promotes the second rather than
// reshuffling everything.
func (r *Rendezvous) Rank(nodes []NodeInfo, req Request) []NodeInfo {
	candidates := eligibleNodes(nodes, req, r.cfg)
	scored := make([]scoredNode, 0, len(candidates))
	for _, n := range candidates {
		scored = append(scored, scoredNode{node: n, score: r.score(req.Key, n)})
	}
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score != scored[j].score {
			return scored[i].score > scored[j].score
		}
		return scored[i].node.ID < scored[j].node.ID
	})
	out := make([]NodeInfo, 0, len(scored))
	for _, s := range scored {
		out = append(out, s.node)
	}
	return out
}

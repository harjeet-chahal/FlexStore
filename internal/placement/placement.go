// Package placement decides which storage nodes hold each chunk's replicas.
//
// The Strategy interface is the extension point: the upload path only ever
// calls SelectNodes, so swapping the weighted strategy for rendezvous or
// consistent hashing later is a constructor change, not a rewrite of the write
// path.
package placement

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
)

// Health mirrors the coordinator's node health states. Duplicated here rather
// than imported from the generated protobuf so the placement engine stays a
// pure, dependency-free unit that is trivial to test.
type Health int

const (
	HealthUnknown Health = iota
	HealthHealthy
	HealthSuspect
	HealthDead
)

func (h Health) String() string {
	switch h {
	case HealthHealthy:
		return "HEALTHY"
	case HealthSuspect:
		return "SUSPECT"
	case HealthDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo is the placement engine's view of a storage node.
type NodeInfo struct {
	ID             string
	Address        string
	Health         Health
	TotalBytes     int64
	UsedBytes      int64
	AvailableBytes int64
	ActiveRequests int32
	ChunkCount     int64
}

// FreeRatio returns available capacity as a fraction of total capacity.
func (n NodeInfo) FreeRatio() float64 {
	if n.TotalBytes <= 0 {
		return 0
	}
	r := float64(n.AvailableBytes) / float64(n.TotalBytes)
	if r < 0 {
		return 0
	}
	if r > 1 {
		return 1
	}
	return r
}

var (
	// ErrInsufficientNodes means the cluster cannot satisfy the requested
	// replica count right now. It is a durability failure, not a bug, and the
	// upload path surfaces it to the client as 503.
	ErrInsufficientNodes = errors.New("insufficient healthy storage nodes")
	// ErrInvalidRequest covers nonsensical arguments (replicas < 1 etc).
	ErrInvalidRequest = errors.New("invalid placement request")
)

// Request is everything a strategy needs to place one chunk.
type Request struct {
	// Key is the stable identity being placed -- the chunk ID. Deterministic
	// strategies (rendezvous) hash it; the weighted strategy ignores it. It is
	// part of the interface rather than a rendezvous-only extra so the upload
	// path does not need to know which strategy is configured.
	Key string
	// Replicas is how many distinct nodes to return.
	Replicas int
	// ChunkSize is the payload size, used for capacity headroom checks.
	ChunkSize int64
	// Exclude lists nodes that must not be selected because they already hold
	// (or held) a copy of this chunk. The repair path depends on this: without
	// it, re-replication could "restore" durability by placing a second copy on
	// a node that already has one.
	Exclude []string
}

// excluded reports whether a node is in the exclusion set.
func (r Request) excluded(id string) bool {
	for _, e := range r.Exclude {
		if e == id {
			return true
		}
	}
	return false
}

// Strategy chooses nodes for a chunk's replicas.
//
// Implementations MUST return exactly `Replicas` distinct nodes or an error;
// returning fewer silently would let the upload path believe a chunk is more
// durable than it is.
type Strategy interface {
	// Name identifies the strategy in logs and metrics.
	Name() string
	// SelectNodes returns req.Replicas distinct eligible nodes, best-first.
	SelectNodes(nodes []NodeInfo, req Request) ([]NodeInfo, error)
}

// New builds the strategy named by cfg. Unknown names are an error rather than
// a silent fallback: running the wrong placement policy is not something an
// operator should discover from a dashboard weeks later.
func New(name string, cfg Config, randSeed int64) (Strategy, error) {
	switch name {
	case "", StrategyWeighted:
		return NewWeighted(cfg, randSeed), nil
	case StrategyRendezvous:
		return NewRendezvous(cfg), nil
	default:
		return nil, fmt.Errorf("unknown placement strategy %q (want %q or %q)",
			name, StrategyWeighted, StrategyRendezvous)
	}
}

// Strategy names accepted by New and FLEXSTORE_PLACEMENT_STRATEGY.
const (
	StrategyWeighted   = "weighted"
	StrategyRendezvous = "rendezvous"
)

// eligibleNodes applies the filters every strategy shares: health, exclusion
// and capacity headroom. Keeping it in one place means the two strategies can
// only differ in how they *rank*, never in what they consider legal.
func eligibleNodes(nodes []NodeInfo, req Request, cfg Config) []NodeInfo {
	headroom := cfg.CapacityHeadroom
	if headroom < 1 {
		headroom = 1
	}
	required := int64(float64(req.ChunkSize) * headroom)

	out := make([]NodeInfo, 0, len(nodes))
	for _, n := range nodes {
		if n.Health == HealthDead || n.Health == HealthUnknown {
			continue
		}
		if n.Health == HealthSuspect && !cfg.AllowSuspect {
			continue
		}
		if n.AvailableBytes < required {
			continue
		}
		if req.excluded(n.ID) {
			continue
		}
		out = append(out, n)
	}
	return out
}

// Config tunes the weighted strategy.
type Config struct {
	// AllowSuspect lets SUSPECT nodes be selected when there are not enough
	// HEALTHY ones. Off by default: writing to a node we already doubt trades
	// durability for availability, which should be an explicit choice.
	AllowSuspect bool
	// CapacityHeadroom is the multiple of chunkSize a node must have free to be
	// eligible, leaving room for the temp file plus a margin.
	CapacityHeadroom float64
	// LoadPenalty scales how strongly in-flight requests reduce a node's
	// weight. 0 disables load awareness (pure capacity balancing).
	LoadPenalty float64
	// MinWeight keeps every eligible node reachable by the sampler even when
	// nearly full, so placement degrades gradually instead of cliff-edging.
	MinWeight float64
}

// DefaultConfig returns the tuning used by the coordinator.
func DefaultConfig() Config {
	return Config{
		AllowSuspect:     false,
		CapacityHeadroom: 2.0,
		LoadPenalty:      1.0,
		MinWeight:        0.01,
	}
}

// Weighted implements capacity- and load-aware weighted random selection
// without replacement.
//
// Weighted random (rather than strict "pick the emptiest N") matters because
// deterministic greedy placement creates hotspots: every concurrent upload
// would pick the same emptiest nodes at the same instant, then all migrate
// together. Randomising proportional to free capacity spreads writes while
// still filling empty nodes faster than full ones.
type Weighted struct {
	cfg Config
	// rng is injectable so tests are deterministic. Guarded by the caller:
	// SelectNodes is documented as not safe for concurrent use with a custom
	// rng, and the coordinator wraps it in a mutex-free per-call source.
	newRand func() *rand.Rand
}

// NewWeighted builds a Weighted strategy. randSeed of 0 means "seed from the
// runtime's global source", which is what production uses; tests pass a fixed
// seed for reproducible placements.
func NewWeighted(cfg Config, randSeed int64) *Weighted {
	w := &Weighted{cfg: cfg}
	if randSeed != 0 {
		// A single shared source would need locking; instead derive a fresh
		// deterministic source per call from a counter-free seed sequence.
		src := rand.New(rand.NewSource(randSeed)) // #nosec G404 -- deterministic test placement
		w.newRand = func() *rand.Rand { return src }
	} else {
		w.newRand = func() *rand.Rand {
			return rand.New(rand.NewSource(rand.Int63())) // #nosec G404 -- placement load-spreading, not crypto
		}
	}
	return w
}

// Name implements Strategy.
func (w *Weighted) Name() string { return "weighted-capacity" }

// SelectNodes implements Strategy.
func (w *Weighted) SelectNodes(nodes []NodeInfo, req Request) ([]NodeInfo, error) {
	replicas, chunkSize := req.Replicas, req.ChunkSize
	if replicas < 1 {
		return nil, fmt.Errorf("%w: replicas must be >= 1, got %d", ErrInvalidRequest, replicas)
	}
	if chunkSize < 0 {
		return nil, fmt.Errorf("%w: chunkSize must be >= 0, got %d", ErrInvalidRequest, chunkSize)
	}

	candidates := w.eligible(nodes, req)
	if len(candidates) < replicas {
		return nil, fmt.Errorf("%w: need %d, have %d eligible of %d known",
			ErrInsufficientNodes, replicas, len(candidates), len(nodes))
	}

	rng := w.newRand()
	selected := make([]NodeInfo, 0, replicas)
	// Track chosen IDs so a node can never hold two replicas of one chunk --
	// that would make the replication factor a lie.
	chosen := make(map[string]struct{}, replicas)

	remaining := make([]weighted, len(candidates))
	copy(remaining, candidates)

	for len(selected) < replicas {
		idx := sampleIndex(remaining, rng)
		if idx < 0 {
			// Every remaining weight was zero; fall back to the highest-capacity
			// node so we degrade rather than fail while candidates remain.
			idx = 0
		}
		pick := remaining[idx]
		if _, dup := chosen[pick.node.ID]; !dup {
			chosen[pick.node.ID] = struct{}{}
			selected = append(selected, pick.node)
		}
		remaining = append(remaining[:idx], remaining[idx+1:]...)
		if len(remaining) == 0 {
			break
		}
	}

	if len(selected) < replicas {
		return nil, fmt.Errorf("%w: selected %d of %d requested", ErrInsufficientNodes, len(selected), replicas)
	}
	return selected, nil
}

type weighted struct {
	node   NodeInfo
	weight float64
}

// eligible filters and scores nodes, returning them sorted by descending
// weight so the zero-weight fallback path picks the best available node.
func (w *Weighted) eligible(nodes []NodeInfo, req Request) []weighted {
	candidates := eligibleNodes(nodes, req, w.cfg)

	out := make([]weighted, 0, len(candidates))
	for _, n := range candidates {
		out = append(out, weighted{node: n, weight: w.score(n)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].weight != out[j].weight {
			return out[i].weight > out[j].weight
		}
		// Stable tie-break keeps placement reproducible for a fixed cluster.
		return out[i].node.ID < out[j].node.ID
	})
	return out
}

// score combines free capacity with current load. Free capacity dominates;
// load acts as a damping factor so a node that is winning on capacity but is
// currently saturated stops attracting every new write.
func (w *Weighted) score(n NodeInfo) float64 {
	weight := n.FreeRatio()
	if w.cfg.LoadPenalty > 0 {
		weight /= 1 + w.cfg.LoadPenalty*float64(n.ActiveRequests)
	}
	if n.Health == HealthSuspect {
		// Strongly deprioritised but not excluded when AllowSuspect is on.
		weight *= 0.1
	}
	if weight < w.cfg.MinWeight {
		weight = w.cfg.MinWeight
	}
	return weight
}

// sampleIndex draws one index proportional to weight. Returns -1 when all
// weights are non-positive.
func sampleIndex(items []weighted, rng *rand.Rand) int {
	total := 0.0
	for _, it := range items {
		if it.weight > 0 {
			total += it.weight
		}
	}
	if total <= 0 {
		return -1
	}
	target := rng.Float64() * total
	acc := 0.0
	for i, it := range items {
		if it.weight <= 0 {
			continue
		}
		acc += it.weight
		if target < acc {
			return i
		}
	}
	return len(items) - 1
}

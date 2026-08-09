// Package health implements the storage-node health state machine.
//
// The machine is deliberately pure: it maps (last heartbeat, now, thresholds)
// to a state. Keeping the transition logic free of I/O means the interesting
// part -- when does a node become SUSPECT vs DEAD -- is unit-testable with a
// fake clock instead of real sleeps.
package health

import (
	"fmt"
	"sync"
	"time"
)

// State is a storage node's liveness classification.
type State int

const (
	// StateUnknown is a node the coordinator has never heard from.
	StateUnknown State = iota
	// StateHealthy: heartbeat received within SuspectAfter.
	StateHealthy
	// StateSuspect: heartbeat is late. The node keeps serving reads (its data
	// is probably still there) but is excluded from new placements.
	StateSuspect
	// StateDead: heartbeat is long gone. Its replicas are treated as lost for
	// durability accounting and it is excluded from reads and writes.
	StateDead
)

func (s State) String() string {
	switch s {
	case StateHealthy:
		return "HEALTHY"
	case StateSuspect:
		return "SUSPECT"
	case StateDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// ParseState converts the database/proto string form back into a State.
func ParseState(s string) (State, error) {
	switch s {
	case "HEALTHY":
		return StateHealthy, nil
	case "SUSPECT":
		return StateSuspect, nil
	case "DEAD":
		return StateDead, nil
	case "UNKNOWN", "":
		return StateUnknown, nil
	default:
		return StateUnknown, fmt.Errorf("unknown health state %q", s)
	}
}

// Thresholds configures the state machine.
type Thresholds struct {
	// SuspectAfter is the age of the last heartbeat past which a node is
	// SUSPECT. Should be a small multiple of the heartbeat interval so a single
	// dropped heartbeat does not flap the cluster.
	SuspectAfter time.Duration
	// DeadAfter is the age past which a node is DEAD.
	DeadAfter time.Duration
}

// Classify maps a heartbeat age to a state. A zero lastHeartbeat means the
// node has never reported and is treated as DEAD rather than UNKNOWN, because
// for placement purposes "never heard from" and "long gone" are equivalent.
func Classify(lastHeartbeat, now time.Time, t Thresholds) State {
	if lastHeartbeat.IsZero() {
		return StateDead
	}
	age := now.Sub(lastHeartbeat)
	switch {
	case age >= t.DeadAfter:
		return StateDead
	case age >= t.SuspectAfter:
		return StateSuspect
	default:
		return StateHealthy
	}
}

// Transition is an observed state change, emitted so the coordinator can log
// and export it without the monitor knowing about slog or Prometheus.
type Transition struct {
	NodeID  string
	From    State
	To      State
	At      time.Time
	Age     time.Duration
	Address string
}

// Snapshot is the monitor's current view of one node.
type Snapshot struct {
	NodeID        string
	Address       string
	State         State
	LastHeartbeat time.Time
}

// Monitor tracks per-node health in memory and reports transitions.
//
// PostgreSQL remains the durable record of last_heartbeat_at; the Monitor is a
// fast in-process cache plus the transition detector. On coordinator restart it
// is rehydrated from Postgres, so a restart does not spuriously mark the whole
// cluster dead.
type Monitor struct {
	mu         sync.RWMutex
	thresholds Thresholds
	now        func() time.Time
	nodes      map[string]*nodeState
}

type nodeState struct {
	address       string
	state         State
	lastHeartbeat time.Time
}

// NewMonitor builds a Monitor. clock may be nil, meaning time.Now.
func NewMonitor(t Thresholds, clock func() time.Time) *Monitor {
	if clock == nil {
		clock = time.Now
	}
	return &Monitor{
		thresholds: t,
		now:        clock,
		nodes:      make(map[string]*nodeState),
	}
}

// Observe records a heartbeat (or registration) and returns a Transition when
// the node's state changed as a result -- typically SUSPECT/DEAD -> HEALTHY.
func (m *Monitor) Observe(nodeID, address string, at time.Time) *Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	ns, ok := m.nodes[nodeID]
	if !ok {
		ns = &nodeState{state: StateUnknown}
		m.nodes[nodeID] = ns
	}
	// Ignore heartbeats that are older than what we already have. Out-of-order
	// delivery would otherwise make a node look stale and flap it to SUSPECT.
	if at.Before(ns.lastHeartbeat) {
		return nil
	}
	prev := ns.state
	ns.address = address
	ns.lastHeartbeat = at
	ns.state = Classify(at, m.now(), m.thresholds)

	if ns.state == prev {
		return nil
	}
	return &Transition{
		NodeID: nodeID, From: prev, To: ns.state,
		At: m.now(), Age: m.now().Sub(at), Address: address,
	}
}

// Seed installs known state without emitting transitions. Used to rehydrate
// from Postgres at startup.
func (m *Monitor) Seed(nodeID, address string, lastHeartbeat time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[nodeID] = &nodeState{
		address:       address,
		lastHeartbeat: lastHeartbeat,
		state:         Classify(lastHeartbeat, m.now(), m.thresholds),
	}
}

// Sweep re-evaluates every node against the current clock and returns all
// transitions. The coordinator calls this on a ticker; it is the only thing
// that can move a node forward into SUSPECT or DEAD.
func (m *Monitor) Sweep() []Transition {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	var out []Transition
	for id, ns := range m.nodes {
		next := Classify(ns.lastHeartbeat, now, m.thresholds)
		if next == ns.state {
			continue
		}
		out = append(out, Transition{
			NodeID: id, From: ns.state, To: next,
			At: now, Age: now.Sub(ns.lastHeartbeat), Address: ns.address,
		})
		ns.state = next
	}
	return out
}

// State returns the current state of a node, or StateUnknown if untracked.
func (m *Monitor) State(nodeID string) State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if ns, ok := m.nodes[nodeID]; ok {
		return ns.state
	}
	return StateUnknown
}

// Snapshot returns the full current view, sorted by node ID by the caller if
// ordering matters.
func (m *Monitor) Snapshot() []Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Snapshot, 0, len(m.nodes))
	for id, ns := range m.nodes {
		out = append(out, Snapshot{
			NodeID:        id,
			Address:       ns.address,
			State:         ns.state,
			LastHeartbeat: ns.lastHeartbeat,
		})
	}
	return out
}

// Forget drops a node entirely (used when a node is deregistered).
func (m *Monitor) Forget(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

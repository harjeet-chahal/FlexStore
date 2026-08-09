package health

import (
	"sync"
	"testing"
	"time"
)

var thresholds = Thresholds{
	SuspectAfter: 15 * time.Second,
	DeadAfter:    60 * time.Second,
}

// fakeClock lets the tests drive time forward without sleeping, which is what
// makes the health-machine tests fast and deterministic.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
}

func TestClassifyBoundaries(t *testing.T) {
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		age  time.Duration
		want State
	}{
		{"fresh", 0, StateHealthy},
		{"just under suspect", 14999 * time.Millisecond, StateHealthy},
		{"exactly suspect", 15 * time.Second, StateSuspect},
		{"between suspect and dead", 30 * time.Second, StateSuspect},
		{"just under dead", 59999 * time.Millisecond, StateSuspect},
		{"exactly dead", 60 * time.Second, StateDead},
		{"long dead", 10 * time.Minute, StateDead},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Classify(base, base.Add(c.age), thresholds)
			if got != c.want {
				t.Fatalf("age %s: got %s, want %s", c.age, got, c.want)
			}
		})
	}
}

func TestClassifyNeverHeardFromIsDead(t *testing.T) {
	// "Never registered" and "gone for an hour" are equivalent for placement,
	// so both must classify as DEAD rather than UNKNOWN.
	if got := Classify(time.Time{}, time.Now(), thresholds); got != StateDead {
		t.Fatalf("zero heartbeat classified as %s, want DEAD", got)
	}
}

func TestMonitorFullLifecycle(t *testing.T) {
	clock := newClock()
	m := NewMonitor(thresholds, clock.Now)

	// Registration: UNKNOWN -> HEALTHY is a real transition and must be
	// reported so the coordinator can persist it.
	tr := m.Observe("node-a", "node-a:9100", clock.Now())
	if tr == nil || tr.From != StateUnknown || tr.To != StateHealthy {
		t.Fatalf("expected UNKNOWN->HEALTHY, got %+v", tr)
	}

	// A second heartbeat with no state change reports nothing.
	clock.Advance(5 * time.Second)
	if tr := m.Observe("node-a", "node-a:9100", clock.Now()); tr != nil {
		t.Fatalf("steady-state heartbeat should not emit a transition, got %+v", tr)
	}

	// Heartbeats stop. Only a sweep can demote.
	clock.Advance(16 * time.Second)
	transitions := m.Sweep()
	if len(transitions) != 1 || transitions[0].To != StateSuspect {
		t.Fatalf("expected one HEALTHY->SUSPECT transition, got %+v", transitions)
	}
	if m.State("node-a") != StateSuspect {
		t.Fatalf("state = %s, want SUSPECT", m.State("node-a"))
	}

	// A repeated sweep with no further time passing must be a no-op; emitting
	// duplicate transitions would spam logs and rewrite the database forever.
	if got := m.Sweep(); len(got) != 0 {
		t.Fatalf("repeated sweep emitted %d transitions, want 0", len(got))
	}

	clock.Advance(50 * time.Second)
	transitions = m.Sweep()
	if len(transitions) != 1 || transitions[0].From != StateSuspect || transitions[0].To != StateDead {
		t.Fatalf("expected SUSPECT->DEAD, got %+v", transitions)
	}

	// Recovery.
	tr = m.Observe("node-a", "node-a:9100", clock.Now())
	if tr == nil || tr.From != StateDead || tr.To != StateHealthy {
		t.Fatalf("expected DEAD->HEALTHY on recovery, got %+v", tr)
	}
}

func TestMonitorIgnoresOutOfOrderHeartbeats(t *testing.T) {
	clock := newClock()
	m := NewMonitor(thresholds, clock.Now)

	now := clock.Now()
	m.Observe("node-a", "node-a:9100", now)

	// A delayed heartbeat from 30s ago must not make the node look stale.
	if tr := m.Observe("node-a", "node-a:9100", now.Add(-30*time.Second)); tr != nil {
		t.Fatalf("stale heartbeat produced a transition: %+v", tr)
	}
	if m.State("node-a") != StateHealthy {
		t.Fatalf("state = %s after a stale heartbeat, want HEALTHY", m.State("node-a"))
	}
}

func TestSeedDoesNotEmitTransitions(t *testing.T) {
	clock := newClock()
	m := NewMonitor(thresholds, clock.Now)

	// Rehydration after a coordinator restart: a node whose last heartbeat is
	// recent must come back HEALTHY, without a spurious transition.
	m.Seed("node-a", "node-a:9100", clock.Now().Add(-2*time.Second))
	if m.State("node-a") != StateHealthy {
		t.Fatalf("seeded state = %s, want HEALTHY", m.State("node-a"))
	}
	if got := m.Sweep(); len(got) != 0 {
		t.Fatalf("sweep right after seeding emitted %+v", got)
	}

	// A node that went away while the coordinator was down comes back DEAD.
	m.Seed("node-b", "node-b:9100", clock.Now().Add(-5*time.Minute))
	if m.State("node-b") != StateDead {
		t.Fatalf("stale seeded node = %s, want DEAD", m.State("node-b"))
	}
}

func TestSnapshotAndForget(t *testing.T) {
	clock := newClock()
	m := NewMonitor(thresholds, clock.Now)
	m.Observe("a", "a:9100", clock.Now())
	m.Observe("b", "b:9100", clock.Now())

	if got := m.Snapshot(); len(got) != 2 {
		t.Fatalf("Snapshot returned %d entries, want 2", len(got))
	}
	m.Forget("a")
	if m.State("a") != StateUnknown {
		t.Fatal("forgotten node should read as UNKNOWN")
	}
	if got := m.Snapshot(); len(got) != 1 {
		t.Fatalf("Snapshot returned %d entries after Forget, want 1", len(got))
	}
}

func TestMonitorIsSafeForConcurrentUse(t *testing.T) {
	// The coordinator observes heartbeats from many nodes while a sweeper
	// ticks; -race turns any missing lock into a test failure.
	clock := newClock()
	m := NewMonitor(thresholds, clock.Now)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := string(rune('a' + i))
			for j := 0; j < 200; j++ {
				m.Observe(id, id+":9100", clock.Now())
				m.State(id)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			m.Sweep()
			m.Snapshot()
		}
	}()
	wg.Wait()
}

func TestParseState(t *testing.T) {
	for in, want := range map[string]State{
		"HEALTHY": StateHealthy,
		"SUSPECT": StateSuspect,
		"DEAD":    StateDead,
		"UNKNOWN": StateUnknown,
		"":        StateUnknown,
	} {
		got, err := ParseState(in)
		if err != nil {
			t.Fatalf("ParseState(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("ParseState(%q) = %s, want %s", in, got, want)
		}
	}
	if _, err := ParseState("BANANA"); err == nil {
		t.Fatal("expected an error for an unknown state")
	}
}

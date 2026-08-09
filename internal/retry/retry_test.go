package retry

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"
)

// fastPolicy keeps the tests quick while preserving the semantics under test.
func fastPolicy(attempts int) Policy {
	return Policy{
		MaxAttempts: attempts,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
		Multiplier:  2,
		Jitter:      0,
	}
}

func TestDoSucceedsFirstTry(t *testing.T) {
	calls := 0
	err := Do(context.Background(), fastPolicy(3), func(context.Context, int) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if calls != 1 {
		t.Fatalf("called %d times, want 1", calls)
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := Do(context.Background(), fastPolicy(5), func(_ context.Context, attempt int) error {
		calls++
		if attempt < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if calls != 3 {
		t.Fatalf("called %d times, want 3", calls)
	}
}

func TestDoIsBounded(t *testing.T) {
	// The whole point of the package: retries must terminate. A regression here
	// would turn a partial outage into an infinite loop.
	calls := 0
	err := Do(context.Background(), fastPolicy(4), func(context.Context, int) error {
		calls++
		return errors.New("always fails")
	})
	if err == nil {
		t.Fatal("expected an error after exhausting attempts")
	}
	if calls != 4 {
		t.Fatalf("called %d times, want exactly 4", calls)
	}
}

func TestPermanentErrorsStopImmediately(t *testing.T) {
	sentinel := errors.New("bad request")
	calls := 0
	err := Do(context.Background(), fastPolicy(5), func(context.Context, int) error {
		calls++
		return Permanent(sentinel)
	})
	if calls != 1 {
		t.Fatalf("called %d times, want 1", calls)
	}
	// Do unwraps Permanent so callers see their original error.
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the wrapped sentinel, got %v", err)
	}
	if IsPermanent(err) {
		t.Fatal("the returned error should be unwrapped, not still marked permanent")
	}
}

func TestDoRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0

	err := Do(ctx, Policy{MaxAttempts: 100, BaseDelay: 50 * time.Millisecond, Multiplier: 2},
		func(context.Context, int) error {
			calls++
			if calls == 2 {
				cancel()
			}
			return errors.New("transient")
		})

	if err == nil {
		t.Fatal("expected a context error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error should wrap context.Canceled, got %v", err)
	}
	if calls > 3 {
		t.Fatalf("kept retrying after cancellation: %d calls", calls)
	}
}

func TestDoStopsBeforeTheFirstAttemptOnADeadContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := Do(ctx, fastPolicy(3), func(context.Context, int) error {
		calls++
		return nil
	})
	if calls != 0 {
		t.Fatalf("ran %d attempts on an already-cancelled context", calls)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDelayGrowsExponentiallyAndIsCapped(t *testing.T) {
	p := Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: 1 * time.Second, Multiplier: 2, Jitter: 0}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second, // capped
		time.Second, // still capped
	}
	for i, w := range want {
		if got := p.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %s, want %s", i+1, got, w)
		}
	}
}

func TestJitterStaysWithinBoundsAndIsDeterministicWithAFixedSource(t *testing.T) {
	p := Policy{
		BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second,
		Multiplier: 2, Jitter: 0.5,
		Rand: rand.New(rand.NewSource(99)), //nolint:gosec // deterministic test
	}
	first := make([]time.Duration, 5)
	for i := range first {
		first[i] = p.Delay(2)
		// 200ms +/- 50% => [100ms, 300ms]
		if first[i] < 100*time.Millisecond || first[i] > 300*time.Millisecond {
			t.Fatalf("jittered delay %s is outside the expected band", first[i])
		}
	}

	p.Rand = rand.New(rand.NewSource(99)) //nolint:gosec // deterministic test
	for i := range first {
		if got := p.Delay(2); got != first[i] {
			t.Fatalf("same seed produced %s then %s", first[i], got)
		}
	}
}

func TestDelayHandlesDegenerateConfig(t *testing.T) {
	// A zero/one multiplier would otherwise produce a constant or shrinking
	// backoff; the package normalises it instead of misbehaving.
	p := Policy{BaseDelay: 10 * time.Millisecond, MaxDelay: time.Second, Multiplier: 0}
	if p.Delay(3) <= p.Delay(1) {
		t.Fatal("degenerate multiplier should still produce growth")
	}
	if p.Delay(0) != p.Delay(1) {
		t.Fatal("attempt 0 should be treated as attempt 1")
	}
}

func TestDefaultAndStartupPoliciesAreBounded(t *testing.T) {
	for name, p := range map[string]Policy{"default": DefaultPolicy(), "startup": StartupPolicy()} {
		if p.MaxAttempts <= 0 {
			t.Errorf("%s policy has no attempt bound", name)
		}
		if p.MaxDelay <= 0 {
			t.Errorf("%s policy has no delay cap", name)
		}
	}
	if StartupPolicy().MaxAttempts <= DefaultPolicy().MaxAttempts {
		t.Error("the startup policy should be more patient than the default one")
	}
}

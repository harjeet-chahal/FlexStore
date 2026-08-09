// Package retry provides bounded exponential backoff with jitter.
//
// Every retry loop in FlexStore is bounded. Unbounded retries turn a partial
// outage into a retry storm and hide real failures from the caller, so the
// policy always has a maximum attempt count and always respects context
// cancellation.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Policy describes a bounded backoff schedule.
type Policy struct {
	// MaxAttempts includes the first attempt. Must be >= 1.
	MaxAttempts int
	// BaseDelay is the delay after the first failure.
	BaseDelay time.Duration
	// MaxDelay caps the exponential growth.
	MaxDelay time.Duration
	// Multiplier is the growth factor per attempt.
	Multiplier float64
	// Jitter is the fraction (0..1) of the computed delay that is randomised,
	// preventing synchronised retries across many gateways from hammering a
	// recovering node in lockstep.
	Jitter float64
	// Rand supplies the jitter. Nil means "use the global source"; tests inject
	// a deterministic source so backoff schedules are assertable.
	Rand *rand.Rand
}

// DefaultPolicy is a good general-purpose policy for internal RPCs.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		Multiplier:  2,
		Jitter:      0.2,
	}
}

// StartupPolicy is used when waiting for a dependency (Postgres, coordinator)
// to become reachable at process start. It is more patient than DefaultPolicy
// but still bounded so a misconfigured service fails visibly instead of
// hanging forever in a container restart loop.
func StartupPolicy() Policy {
	return Policy{
		MaxAttempts: 30,
		BaseDelay:   250 * time.Millisecond,
		MaxDelay:    3 * time.Second,
		Multiplier:  1.5,
		Jitter:      0.2,
	}
}

// Delay returns the backoff duration to wait *after* the given attempt number
// (1-based). Exported so it can be unit tested directly.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := p.Multiplier
	if mult <= 1 {
		mult = 2
	}
	d := float64(p.BaseDelay) * math.Pow(mult, float64(attempt-1))
	if p.MaxDelay > 0 && d > float64(p.MaxDelay) {
		d = float64(p.MaxDelay)
	}
	if p.Jitter > 0 {
		// Symmetric jitter around d, clamped at zero.
		frac := p.Jitter
		if frac > 1 {
			frac = 1
		}
		var r float64
		if p.Rand != nil {
			r = p.Rand.Float64()
		} else {
			r = rand.Float64() // #nosec G404 -- jitter, not security-sensitive
		}
		d *= 1 + frac*(2*r-1)
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// permanent marks an error as non-retryable.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent wraps err so Do stops immediately instead of retrying. Use it for
// errors that cannot possibly succeed on a retry (bad request, not found).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err: err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// Do runs fn until it succeeds, returns a Permanent error, exhausts the
// policy, or ctx is done. The returned error wraps the last attempt's error.
func Do(ctx context.Context, p Policy, fn func(ctx context.Context, attempt int) error) error {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	var last error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("retry aborted after %d attempts: %w (last error: %w)", attempt-1, err, last)
			}
			return err
		}
		last = fn(ctx, attempt)
		if last == nil {
			return nil
		}
		if IsPermanent(last) {
			return errors.Unwrap(last)
		}
		if attempt == p.MaxAttempts {
			break
		}
		// Sleep on a timer we always stop, so a cancelled context does not leak
		// a pending runtime timer for the full backoff duration.
		t := time.NewTimer(p.Delay(attempt))
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("retry aborted after %d attempts: %w (last error: %w)", attempt, ctx.Err(), last)
		case <-t.C:
		}
	}
	return fmt.Errorf("giving up after %d attempts: %w", p.MaxAttempts, last)
}

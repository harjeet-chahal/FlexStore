// Package runtime holds the process lifecycle glue every FlexStore binary
// shares: signal handling, ordered startup of long-running goroutines, and a
// graceful shutdown that actually waits for them.
package runtime

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Task is one long-running component. It must return when ctx is cancelled;
// returning nil means "shut down cleanly".
type Task func(ctx context.Context) error

// Group runs tasks concurrently and cancels all of them as soon as one exits.
//
// This is a deliberately small stand-in for errgroup: the semantics FlexStore
// needs (first exit wins, everything else is cancelled, Wait blocks until all
// goroutines have actually returned) are ~50 lines, and owning them keeps the
// shutdown story explicit rather than implicit in a dependency.
type Group struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	errOnce sync.Once
	err     error
	names   []string
	log     *slog.Logger
}

// NewGroup creates a Group bound to a derived, cancellable context.
func NewGroup(ctx context.Context, log *slog.Logger) (*Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{cancel: cancel, log: log}, ctx
}

// Go starts a named task.
func (g *Group) Go(ctx context.Context, name string, fn Task) {
	g.mu.Lock()
	g.names = append(g.names, name)
	g.mu.Unlock()

	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			g.errOnce.Do(func() { g.err = err })
			g.log.Error("component failed", slog.String("component", name), slog.String("error", err.Error()))
		} else {
			g.log.Debug("component stopped", slog.String("component", name))
		}
		// Any exit -- clean or not -- stops the process. A gateway with a dead
		// HTTP listener but a live metrics endpoint would look healthy while
		// serving nothing.
		g.cancel()
	}()
}

// Wait blocks until every task has returned and reports the first error.
func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

// Stop cancels the group's context without waiting.
func (g *Group) Stop() { g.cancel() }

// SignalContext returns a context cancelled on SIGINT or SIGTERM. The returned
// stop func must be called to release the signal handler.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// ShutdownContext returns a context with the configured grace period, detached
// from the (already cancelled) parent so cleanup work can still run.
func ShutdownContext(wait time.Duration) (context.Context, context.CancelFunc) {
	if wait <= 0 {
		wait = 15 * time.Second
	}
	return context.WithTimeout(context.Background(), wait)
}

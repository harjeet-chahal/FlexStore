package cache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNoopAlwaysMisses(t *testing.T) {
	var c Cache = Noop{}
	ctx := context.Background()

	var dst string
	if err := c.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// The Noop must not pretend to store anything; if it did, code paths that
	// only work because of a cache hit would go untested without Redis.
	if err := c.Get(ctx, "k", &dst); !errors.Is(err, ErrMiss) {
		t.Fatalf("expected ErrMiss, got %v", err)
	}
	if c.Enabled() {
		t.Error("Noop should report itself disabled")
	}
	if err := c.Delete(ctx, "k"); err != nil {
		t.Errorf("Delete: %v", err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Errorf("Ping: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestNoopGrantsLocks(t *testing.T) {
	// Without Redis there is no cross-process mutual exclusion. Granting the
	// lease is correct because every lock-guarded operation is also inside a
	// database transaction; refusing it would deadlock single-node runs.
	var c Cache = Noop{}
	ok, release, err := c.AcquireLock(context.Background(), "obj", time.Second)
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	if !ok {
		t.Fatal("Noop must grant the lease so operations are not blocked")
	}
	release(context.Background()) // must not panic
}

func TestKeyNamespacing(t *testing.T) {
	// Cache keys are derived from user-supplied buckets and keys; two distinct
	// objects must never collide.
	if ObjectKey("a", "b/c") == ObjectKey("a/b", "c") {
		t.Fatal("object cache keys collide across a bucket/key boundary")
	}
	if ObjectKey("bucket", "nested/key") != "obj:bucket:nested/key" {
		t.Fatalf("unexpected object key format: %s", ObjectKey("bucket", "nested/key"))
	}
	if MultipartKey("abc") != "mpu:abc" {
		t.Fatalf("unexpected multipart key format: %s", MultipartKey("abc"))
	}
	// Different namespaces must not overlap.
	if ObjectKey("x", "y") == MultipartKey("x/y") || NodesKey == ObjectKey("nodes", "healthy") {
		t.Fatal("cache namespaces overlap")
	}
}

func TestNewWithoutAddressReturnsNoop(t *testing.T) {
	c, err := New(context.Background(), Options{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Enabled() {
		t.Fatal("an empty address must yield the Noop cache, not a live client")
	}
}

func TestNewWithUnreachableRedisFailsFast(t *testing.T) {
	// Redis is an optional dependency, but a *configured* Redis that is
	// unreachable is a misconfiguration and must not be silently downgraded.
	// Port 1 is reserved and never listening.
	_, err := New(context.Background(), Options{Addr: "127.0.0.1:1"}, testLogger())
	if err == nil {
		t.Fatal("expected an error when the configured Redis is unreachable")
	}
}

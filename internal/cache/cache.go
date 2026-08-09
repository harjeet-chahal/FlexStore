// Package cache is a thin Redis layer for metadata that is expensive to
// recompute and cheap to lose.
//
// PostgreSQL is always the source of truth. Every cache entry has a TTL *and*
// an explicit invalidation on the write path, so a stale entry is bounded by
// the TTL even if an invalidation is missed. Nothing in FlexStore is correct
// only because Redis was available.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrMiss is returned when a key is absent. It is a normal control-flow signal,
// not a failure.
var ErrMiss = errors.New("cache miss")

// Cache is the interface the coordinator and gateway depend on. Having an
// interface here (rather than *redis.Client everywhere) is what lets every
// unit test run without Redis and lets the services start when Redis is not
// configured at all.
type Cache interface {
	Get(ctx context.Context, key string, dst any) error
	Set(ctx context.Context, key string, val any, ttl time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	// AcquireLock takes a short-lived mutual-exclusion lease. It returns a
	// release func that is safe to call even when ok is false.
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (ok bool, release func(context.Context), err error)
	Ping(ctx context.Context) error
	Enabled() bool
	Close() error
}

// Noop is the always-miss implementation used when Redis is not configured and
// in unit tests.
type Noop struct{}

func (Noop) Get(context.Context, string, any) error                { return ErrMiss }
func (Noop) Set(context.Context, string, any, time.Duration) error { return nil }
func (Noop) Delete(context.Context, ...string) error               { return nil }
func (Noop) AcquireLock(context.Context, string, time.Duration) (bool, func(context.Context), error) {
	// Without Redis there is no cross-process mutual exclusion, so we grant the
	// lease. Every operation guarded by a lock is *also* protected by a
	// database transaction; the lock only reduces wasted work.
	return true, func(context.Context) {}, nil
}
func (Noop) Ping(context.Context) error { return nil }
func (Noop) Enabled() bool              { return false }
func (Noop) Close() error               { return nil }

// Redis is the production implementation.
type Redis struct {
	client *redis.Client
	log    *slog.Logger
	prefix string
}

// Options configures the Redis cache.
type Options struct {
	Addr   string
	DB     int
	Prefix string
}

// New returns a Redis-backed Cache, or a Noop when Addr is empty. Redis being
// optional is deliberate: it is a performance dependency, not a correctness
// one, and the cluster must still work when it is down.
func New(ctx context.Context, opts Options, log *slog.Logger) (Cache, error) {
	if opts.Addr == "" {
		log.Info("redis not configured; running without metadata cache")
		return Noop{}, nil
	}
	client := redis.NewClient(&redis.Options{
		Addr:         opts.Addr,
		DB:           opts.DB,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     16,
	})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis at %s unreachable: %w", opts.Addr, err)
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "fs"
	}
	return &Redis{client: client, log: log, prefix: prefix}, nil
}

func (r *Redis) key(k string) string { return r.prefix + ":" + k }

// Get decodes the cached JSON value into dst, or returns ErrMiss.
func (r *Redis) Get(ctx context.Context, key string, dst any) error {
	b, err := r.client.Get(ctx, r.key(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return ErrMiss
	}
	if err != nil {
		return fmt.Errorf("cache get %s: %w", key, err)
	}
	if err := json.Unmarshal(b, dst); err != nil {
		// A value we cannot decode is worse than no value: drop it so the next
		// request repopulates from Postgres instead of failing forever.
		r.log.Warn("dropping undecodable cache entry",
			slog.String("key", key), slog.String("error", err.Error()))
		_ = r.client.Del(ctx, r.key(key)).Err()
		return ErrMiss
	}
	return nil
}

// Set stores val as JSON with the given TTL.
func (r *Redis) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("cache encode %s: %w", key, err)
	}
	if err := r.client.Set(ctx, r.key(key), b, ttl).Err(); err != nil {
		return fmt.Errorf("cache set %s: %w", key, err)
	}
	return nil
}

// Delete removes keys.
func (r *Redis) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	full := make([]string, len(keys))
	for i, k := range keys {
		full[i] = r.key(k)
	}
	if err := r.client.Del(ctx, full...).Err(); err != nil {
		return fmt.Errorf("cache delete: %w", err)
	}
	return nil
}

// releaseScript deletes the lock only if we still own it, so a lease that
// expired and was re-acquired by someone else is not stolen back.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

// AcquireLock implements Cache.
func (r *Redis) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, func(context.Context), error) {
	token := fmt.Sprintf("%d-%p", time.Now().UnixNano(), &key)
	full := r.key("lock:" + key)
	ok, err := r.client.SetNX(ctx, full, token, ttl).Result()
	if err != nil {
		return false, func(context.Context) {}, fmt.Errorf("acquire lock %s: %w", key, err)
	}
	if !ok {
		return false, func(context.Context) {}, nil
	}
	release := func(rctx context.Context) {
		if err := releaseScript.Run(rctx, r.client, []string{full}, token).Err(); err != nil && !errors.Is(err, redis.Nil) {
			r.log.Warn("failed to release lock",
				slog.String("key", key), slog.String("error", err.Error()))
		}
	}
	return true, release, nil
}

// Ping implements Cache.
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }

// Enabled implements Cache.
func (r *Redis) Enabled() bool { return true }

// Close implements Cache.
func (r *Redis) Close() error { return r.client.Close() }

// ObjectKey is the cache key for an object's metadata + chunk layout.
//
// The bucket and key are separated by ':' rather than '/'. Object keys may
// contain '/' but bucket names may not (see coordinator.ValidateBucket), so
// ':' is the only separator that makes the encoding unambiguous -- with '/',
// ("a", "b/c") and ("a/b", "c") would map to the same entry.
func ObjectKey(bucket, key string) string { return "obj:" + bucket + ":" + key }

// NodesKey is the cache key for the healthy-node roster used by placement.
const NodesKey = "nodes:healthy"

// MultipartKey is the cache key for a multipart session header.
func MultipartKey(uploadID string) string { return "mpu:" + uploadID }

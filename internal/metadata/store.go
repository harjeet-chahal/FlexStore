package metadata

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/harjeetschahal/flexstore/internal/retry"
)

// Store is the coordinator's PostgreSQL-backed metadata repository.
type Store struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// Open connects to PostgreSQL, waiting (with bounded backoff) for it to accept
// connections. Compose starts everything at once, so "not up yet" is the
// normal case at boot, not an error worth crashing over -- but it is bounded,
// so a genuinely misconfigured DSN still fails the container fast enough to be
// noticed.
func Open(ctx context.Context, dsn string, log *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	// Bounded pool: the coordinator is metadata-only, so a modest pool with
	// short-lived idle connections is plenty and keeps Postgres' own limits
	// from becoming the cluster's scaling ceiling.
	cfg.MaxConns = 16
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	err = retry.Do(ctx, retry.StartupPolicy(), func(ctx context.Context, attempt int) error {
		pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			log.Debug("waiting for postgres",
				slog.Int("attempt", attempt), slog.String("error", err.Error()))
			return err
		}
		return nil
	})
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres never became reachable: %w", err)
	}

	return &Store{pool: pool, log: log}, nil
}

// NewStore wraps an existing pool. Used by integration tests that manage their
// own database lifecycle.
func NewStore(pool *pgxpool.Pool, log *slog.Logger) *Store {
	return &Store{pool: pool, log: log}
}

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// Pool exposes the underlying pool for migrations and tests.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Ping is the coordinator's readiness probe.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// inTx runs fn inside a transaction, rolling back on error. pgx.BeginFunc
// already handles commit/rollback; this wrapper adds consistent error context.
func (s *Store) inTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	err := pgx.BeginFunc(ctx, s.pool, fn)
	if err != nil {
		return err
	}
	return nil
}

// nullTime converts a possibly-NULL timestamp into a zero-valued time.Time,
// which is how the domain types represent "not set".
func nullTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

func nullString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

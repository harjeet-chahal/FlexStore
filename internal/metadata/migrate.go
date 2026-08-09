package metadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/harjeetschahal/flexstore/migrations"
)

type migration struct {
	version  int
	name     string
	body     string
	checksum string
}

// Migrate applies every unapplied migration inside a transaction, using a
// session-level advisory lock so several coordinator replicas starting at once
// cannot race. Applied migrations are checksummed: editing a shipped migration
// after the fact is a hard error rather than silent schema drift.
func Migrate(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	// Arbitrary but stable lock key derived from the project name.
	const advisoryLockKey int64 = 0x464C4558 // "FLEX"
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	defer func() {
		// Best effort: releasing on a broken connection is meaningless because
		// the lock dies with the session anyway.
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     INTEGER PRIMARY KEY,
			name        TEXT NOT NULL,
			checksum    TEXT NOT NULL,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied := map[int]string{}
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v int
		var c string
		if err := rows.Scan(&v, &c); err != nil {
			rows.Close()
			return fmt.Errorf("scan schema_migrations: %w", err)
		}
		applied[v] = c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema_migrations: %w", err)
	}

	for _, m := range migs {
		if have, ok := applied[m.version]; ok {
			if have != m.checksum {
				return fmt.Errorf(
					"migration %d (%s) was modified after being applied (recorded %s, found %s); "+
						"add a new migration instead of editing a released one",
					m.version, m.name, have, m.checksum)
			}
			continue
		}

		log.Info("applying migration",
			slog.Int("version", m.version), slog.String("name", m.name))

		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, m.body); err != nil {
				return fmt.Errorf("execute: %w", err)
			}
			_, err := tx.Exec(ctx,
				`INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
				m.version, m.name, m.checksum)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %d (%s): %w", m.version, m.name, err)
		}
	}
	return nil
}

func loadMigrations() ([]migration, error) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	var out []migration
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		version, name, err := parseMigrationName(e.Name())
		if err != nil {
			return nil, err
		}
		body, err := migrations.FS.ReadFile(e.Name())
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(body)
		out = append(out, migration{
			version:  version,
			name:     name,
			body:     string(body),
			checksum: hex.EncodeToString(sum[:]),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })

	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d", out[i].version)
		}
	}
	return out, nil
}

func parseMigrationName(filename string) (int, string, error) {
	base := strings.TrimSuffix(filename, ".sql")
	idx := strings.Index(base, "_")
	if idx <= 0 {
		return 0, "", fmt.Errorf("migration %q must be named <version>_<name>.sql", filename)
	}
	v, err := strconv.Atoi(base[:idx])
	if err != nil {
		return 0, "", fmt.Errorf("migration %q has a non-numeric version: %w", filename, err)
	}
	return v, base[idx+1:], nil
}

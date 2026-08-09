package metadata

import (
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestLoadMigrationsIsOrderedAndUnique(t *testing.T) {
	migs, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	if len(migs) == 0 {
		t.Fatal("no migrations were embedded; the coordinator would start against an empty schema")
	}
	for i := 1; i < len(migs); i++ {
		if migs[i].version <= migs[i-1].version {
			t.Fatalf("migrations are not strictly ordered: %d then %d",
				migs[i-1].version, migs[i].version)
		}
	}
	for _, m := range migs {
		if m.checksum == "" || m.name == "" || strings.TrimSpace(m.body) == "" {
			t.Fatalf("migration %d is incomplete: %+v", m.version, m)
		}
	}
}

func TestParseMigrationName(t *testing.T) {
	v, name, err := parseMigrationName("0001_init.sql")
	if err != nil {
		t.Fatalf("parseMigrationName: %v", err)
	}
	if v != 1 || name != "init" {
		t.Fatalf("got (%d, %q), want (1, \"init\")", v, name)
	}

	for _, bad := range []string{"init.sql", "abc_init.sql", "0001.sql"} {
		if _, _, err := parseMigrationName(bad); err == nil {
			t.Errorf("parseMigrationName(%q) should have failed", bad)
		}
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	// Every coordinator restart runs Migrate. Running it twice must be a no-op,
	// not an error and not a duplicate-object failure.
	s, ctx := testStore(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	for i := 0; i < 3; i++ {
		if err := Migrate(ctx, s.Pool(), log); err != nil {
			t.Fatalf("Migrate run %d: %v", i+1, err)
		}
	}

	migs, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	var applied int
	if err := s.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&applied); err != nil {
		t.Fatalf("counting applied migrations: %v", err)
	}
	if applied != len(migs) {
		t.Fatalf("%d rows in schema_migrations for %d migrations", applied, len(migs))
	}
}

func TestMigrateDetectsAnEditedMigration(t *testing.T) {
	// Editing an already-released migration is silent schema drift: some
	// databases have the old version, some the new. Migrate must refuse.
	s, ctx := testStore(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	if err := Migrate(ctx, s.Pool(), log); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Simulate an edit by corrupting the recorded checksum.
	var version int
	if err := s.Pool().QueryRow(ctx,
		`SELECT version FROM schema_migrations ORDER BY version LIMIT 1`).Scan(&version); err != nil {
		t.Fatalf("reading a migration row: %v", err)
	}
	original := ""
	if err := s.Pool().QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = $1`, version).Scan(&original); err != nil {
		t.Fatalf("reading the checksum: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.Pool().Exec(ctx,
			`UPDATE schema_migrations SET checksum = $2 WHERE version = $1`, version, original)
	})

	if _, err := s.Pool().Exec(ctx,
		`UPDATE schema_migrations SET checksum = 'tampered' WHERE version = $1`, version); err != nil {
		t.Fatalf("tampering: %v", err)
	}

	err := Migrate(ctx, s.Pool(), log)
	if err == nil {
		t.Fatal("Migrate accepted a modified migration")
	}
	if !strings.Contains(err.Error(), "modified after being applied") {
		t.Fatalf("unexpected error: %v", err)
	}
}

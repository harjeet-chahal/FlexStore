package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"8MiB", 8 << 20},
		{"8mib", 8 << 20},
		{"8 MiB", 8 << 20},
		{"512KiB", 512 << 10},
		{"1GiB", 1 << 30},
		{"1GB", 1_000_000_000},
		{"2M", 2 << 20},
		{"1024B", 1024},
		{"0", 0},
		{"1.5MiB", 1572864},
	}
	for _, c := range cases {
		got, err := ParseBytes(c.in)
		if err != nil {
			t.Errorf("ParseBytes(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseBytes(%q) = %d, want %d", c.in, got, c.want)
		}
	}

	for _, bad := range []string{"", "abc", "-5MiB", "MiB", "8XiB"} {
		if _, err := ParseBytes(bad); err == nil {
			t.Errorf("ParseBytes(%q) should have failed", bad)
		}
	}
}

func TestLoadGatewayDefaults(t *testing.T) {
	cfg, err := LoadGateway()
	if err != nil {
		t.Fatalf("LoadGateway: %v", err)
	}
	if cfg.ChunkSize != DefaultChunkSize {
		t.Errorf("default chunk size = %d, want %d", cfg.ChunkSize, DefaultChunkSize)
	}
	if cfg.HTTPAddr == "" || cfg.MetricsAddr == "" {
		t.Error("addresses must have defaults so `docker compose up` works out of the box")
	}
	// Zero read/write timeouts are load-bearing: a fixed cap would kill
	// legitimate multi-gigabyte transfers.
	if cfg.ReadTimeout != 0 || cfg.WriteTimeout != 0 {
		t.Errorf("expected unlimited body timeouts by default, got read=%s write=%s",
			cfg.ReadTimeout, cfg.WriteTimeout)
	}
}

func TestLoadCoordinatorRequiresDSN(t *testing.T) {
	_, err := LoadCoordinator()
	if err == nil {
		t.Fatal("expected an error when FLEXSTORE_POSTGRES_DSN is unset")
	}
	if !strings.Contains(err.Error(), "FLEXSTORE_POSTGRES_DSN") {
		t.Fatalf("error should name the missing variable, got: %v", err)
	}
}

func TestLoadCoordinatorValidatesDurabilityPolicy(t *testing.T) {
	t.Setenv("FLEXSTORE_POSTGRES_DSN", "postgres://x/y")

	t.Run("min replicas above replication factor", func(t *testing.T) {
		t.Setenv("FLEXSTORE_REPLICATION_FACTOR", "3")
		t.Setenv("FLEXSTORE_MIN_WRITE_REPLICAS", "5")
		_, err := LoadCoordinator()
		if err == nil || !strings.Contains(err.Error(), "MIN_WRITE_REPLICAS") {
			t.Fatalf("expected a durability-policy error, got %v", err)
		}
	})

	t.Run("zero replication factor", func(t *testing.T) {
		t.Setenv("FLEXSTORE_REPLICATION_FACTOR", "0")
		t.Setenv("FLEXSTORE_MIN_WRITE_REPLICAS", "1")
		if _, err := LoadCoordinator(); err == nil {
			t.Fatal("expected an error for replication factor 0")
		}
	})

	t.Run("valid policy", func(t *testing.T) {
		t.Setenv("FLEXSTORE_REPLICATION_FACTOR", "3")
		t.Setenv("FLEXSTORE_MIN_WRITE_REPLICAS", "2")
		cfg, err := LoadCoordinator()
		if err != nil {
			t.Fatalf("LoadCoordinator: %v", err)
		}
		if cfg.ReplicationFactor != 3 || cfg.MinWriteReplicas != 2 {
			t.Fatalf("got RF=%d min=%d", cfg.ReplicationFactor, cfg.MinWriteReplicas)
		}
	})
}

func TestLoadCoordinatorValidatesHealthThresholdOrdering(t *testing.T) {
	t.Setenv("FLEXSTORE_POSTGRES_DSN", "postgres://x/y")

	// Suspect must be later than a heartbeat interval, or a single dropped
	// heartbeat flaps the whole cluster.
	t.Setenv("FLEXSTORE_HEARTBEAT_INTERVAL", "10s")
	t.Setenv("FLEXSTORE_SUSPECT_TIMEOUT", "5s")
	t.Setenv("FLEXSTORE_DEAD_TIMEOUT", "60s")
	if _, err := LoadCoordinator(); err == nil || !strings.Contains(err.Error(), "SUSPECT_TIMEOUT") {
		t.Fatalf("expected a suspect/heartbeat ordering error, got %v", err)
	}

	// Dead must be later than suspect.
	t.Setenv("FLEXSTORE_HEARTBEAT_INTERVAL", "5s")
	t.Setenv("FLEXSTORE_SUSPECT_TIMEOUT", "30s")
	t.Setenv("FLEXSTORE_DEAD_TIMEOUT", "20s")
	if _, err := LoadCoordinator(); err == nil || !strings.Contains(err.Error(), "DEAD_TIMEOUT") {
		t.Fatalf("expected a dead/suspect ordering error, got %v", err)
	}
}

func TestLoadRejectsMalformedValuesRatherThanSilentlyDefaulting(t *testing.T) {
	// Silently falling back to a default after a typo is far harder to debug
	// than refusing to start, so this behaviour is asserted explicitly.
	t.Setenv("FLEXSTORE_CHUNK_SIZE", "banana")
	if _, err := LoadGateway(); err == nil {
		t.Fatal("expected an error for a malformed chunk size")
	}

	t.Setenv("FLEXSTORE_CHUNK_SIZE", "8MiB")
	t.Setenv("FLEXSTORE_HTTP_IDLE_TIMEOUT", "soon")
	if _, err := LoadGateway(); err == nil {
		t.Fatal("expected an error for a malformed duration")
	}
}

func TestChunkSizeBounds(t *testing.T) {
	t.Setenv("FLEXSTORE_CHUNK_SIZE", "1KiB") // below MinChunkSize
	if _, err := LoadGateway(); err == nil {
		t.Fatal("expected an error for an undersized chunk")
	}
	t.Setenv("FLEXSTORE_CHUNK_SIZE", "1GiB") // above MaxChunkSize
	if _, err := LoadGateway(); err == nil {
		t.Fatal("expected an error for an oversized chunk")
	}
	t.Setenv("FLEXSTORE_CHUNK_SIZE", "64KiB") // exactly MinChunkSize
	if _, err := LoadGateway(); err != nil {
		t.Fatalf("MinChunkSize should be accepted: %v", err)
	}
}

func TestLoadStorageNodeRequiresIdentity(t *testing.T) {
	if _, err := LoadStorageNode(); err == nil {
		t.Fatal("expected errors for the missing node id and advertise address")
	}

	t.Setenv("FLEXSTORE_NODE_ID", "storage-node-1")
	t.Setenv("FLEXSTORE_ADVERTISE_ADDR", "storage-node-1:9100")
	cfg, err := LoadStorageNode()
	if err != nil {
		t.Fatalf("LoadStorageNode: %v", err)
	}
	if cfg.NodeID != "storage-node-1" {
		t.Errorf("NodeID = %q", cfg.NodeID)
	}
	if !cfg.FsyncData {
		t.Error("fsync must default to on; durability claims depend on it")
	}
	if cfg.HeartbeatInterval != 5*time.Second {
		t.Errorf("HeartbeatInterval = %s, want 5s", cfg.HeartbeatInterval)
	}
}

func TestErrorsAreAggregated(t *testing.T) {
	// Several bad values should all be reported at once so an operator fixes
	// the config in one pass rather than one restart per typo.
	t.Setenv("FLEXSTORE_CHUNK_SIZE", "nope")
	t.Setenv("FLEXSTORE_HTTP_IDLE_TIMEOUT", "nope")
	t.Setenv("FLEXSTORE_CHUNK_READ_TIMEOUT", "nope")
	_, err := LoadGateway()
	if err == nil {
		t.Fatal("expected errors")
	}
	msg := err.Error()
	for _, want := range []string{"FLEXSTORE_CHUNK_SIZE", "FLEXSTORE_HTTP_IDLE_TIMEOUT", "FLEXSTORE_CHUNK_READ_TIMEOUT"} {
		if !strings.Contains(msg, want) {
			t.Errorf("aggregated error is missing %s:\n%s", want, msg)
		}
	}
}

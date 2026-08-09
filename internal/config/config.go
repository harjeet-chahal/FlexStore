// Package config loads service configuration from the environment.
//
// Every knob has a documented default so a bare `docker compose up` works, and
// every knob is overridable so the same binaries can be tuned per deployment.
// Parsing failures are hard errors: a service that silently falls back to a
// default after a typo in an env var is far harder to debug than one that
// refuses to start.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// DefaultChunkSize is 8 MiB, matching the project spec. It is the unit of
	// placement, replication and checksumming.
	DefaultChunkSize int64 = 8 << 20

	// MinChunkSize guards against pathological configurations that would make
	// metadata dominate payload.
	MinChunkSize int64 = 64 << 10
	// MaxChunkSize bounds gateway memory: the gateway holds at most one chunk
	// per in-flight request.
	MaxChunkSize int64 = 256 << 20
)

// Common holds settings shared by all three binaries.
type Common struct {
	ServiceName  string
	LogLevel     string
	LogFormat    string // "json" or "text"
	MetricsAddr  string
	ShutdownWait time.Duration
}

// Gateway is the client-facing HTTP service configuration.
type Gateway struct {
	Common
	HTTPAddr           string
	CoordinatorAddr    string
	ChunkSize          int64
	MaxObjectSize      int64
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	IdleTimeout        time.Duration
	ChunkWriteTimeout  time.Duration
	ChunkReadTimeout   time.Duration
	CoordinatorTimeout time.Duration
}

// Coordinator is the control-plane configuration.
type Coordinator struct {
	Common
	GRPCAddr           string
	PostgresDSN        string
	RedisAddr          string
	RedisDB            int
	ReplicationFactor  int
	MinWriteReplicas   int
	ChunkSize          int64
	HeartbeatInterval  time.Duration
	SuspectTimeout     time.Duration
	DeadTimeout        time.Duration
	HealthScanInterval time.Duration
	GCInterval         time.Duration
	GCBatchSize        int
	// UploadStaleAfter is how long an UPLOADING object may sit untouched before
	// the reaper marks it FAILED and reclaims its chunks.
	UploadStaleAfter time.Duration
	NodeCallTimeout  time.Duration
	MigrateOnStart   bool
	NodeCacheTTL     time.Duration
	ObjectCacheTTL   time.Duration

	// PlacementStrategy selects the replica placement algorithm:
	// "weighted" (capacity/load-aware random) or "rendezvous" (HRW hashing).
	PlacementStrategy string

	// --- self-healing ---
	RepairEnabled bool
	// RepairWorkers bounds total concurrent re-replications.
	RepairWorkers int
	// RepairMaxPerNode bounds concurrent repairs targeting one node. This is
	// the guard against a replication storm burying a single recovery target.
	RepairMaxPerNode   int
	RepairScanInterval time.Duration
	RepairScanBatch    int
	RepairJobTimeout   time.Duration
	RepairLease        time.Duration
	RepairMaxAttempts  int
	RepairBaseBackoff  time.Duration
	RepairMaxBackoff   time.Duration
	RepairHistoryTTL   time.Duration

	ReconcileEnabled  bool
	ReconcileInterval time.Duration
	ReconcilePageSize int
	ReconcileTimeout  time.Duration
	ReconcileLease    time.Duration
	ReconcileVerify   bool
	// DurabilityAuditInterval is how often the denormalised replica counters
	// are recomputed from the underlying rows to catch drift.
	DurabilityAuditInterval time.Duration
}

// StorageNode is the data-plane configuration.
type StorageNode struct {
	Common
	NodeID          string
	GRPCAddr        string
	AdvertiseAddr   string
	DataDir         string
	CoordinatorAddr string
	// CapacityBytes overrides the filesystem-reported capacity. Useful in
	// Docker, where every node shares the host filesystem and would otherwise
	// all report the same (huge) free space, defeating capacity-aware
	// placement.
	CapacityBytes     int64
	HeartbeatInterval time.Duration
	RegisterTimeout   time.Duration
	// FsyncData controls whether chunk payloads are fsynced before rename.
	// Always true in production; tests flip it off for speed.
	FsyncData bool
}

// LoadGateway reads gateway configuration from the process environment.
func LoadGateway() (Gateway, error) {
	var errs []error
	c := Gateway{
		Common:             loadCommon("gateway", "0.0.0.0:9101", &errs),
		HTTPAddr:           envString("FLEXSTORE_HTTP_ADDR", "0.0.0.0:8080"),
		CoordinatorAddr:    envString("FLEXSTORE_COORDINATOR_ADDR", "coordinator:9090"),
		ChunkSize:          envBytes("FLEXSTORE_CHUNK_SIZE", DefaultChunkSize, &errs),
		MaxObjectSize:      envBytes("FLEXSTORE_MAX_OBJECT_SIZE", 5<<40, &errs),
		ReadTimeout:        envDuration("FLEXSTORE_HTTP_READ_TIMEOUT", 0, &errs),
		WriteTimeout:       envDuration("FLEXSTORE_HTTP_WRITE_TIMEOUT", 0, &errs),
		IdleTimeout:        envDuration("FLEXSTORE_HTTP_IDLE_TIMEOUT", 120*time.Second, &errs),
		ChunkWriteTimeout:  envDuration("FLEXSTORE_CHUNK_WRITE_TIMEOUT", 60*time.Second, &errs),
		ChunkReadTimeout:   envDuration("FLEXSTORE_CHUNK_READ_TIMEOUT", 60*time.Second, &errs),
		CoordinatorTimeout: envDuration("FLEXSTORE_COORDINATOR_TIMEOUT", 10*time.Second, &errs),
	}

	// Read/Write timeouts of 0 are deliberate: an S3-style API streams
	// arbitrarily large bodies and a fixed wall-clock timeout would kill valid
	// slow uploads. Per-chunk timeouts provide the liveness guarantee instead.
	if err := validateChunkSize(c.ChunkSize); err != nil {
		errs = append(errs, err)
	}
	return c, errors.Join(errs...)
}

// LoadCoordinator reads coordinator configuration from the process environment.
func LoadCoordinator() (Coordinator, error) {
	var errs []error
	c := Coordinator{
		Common:             loadCommon("coordinator", "0.0.0.0:9102", &errs),
		GRPCAddr:           envString("FLEXSTORE_GRPC_ADDR", "0.0.0.0:9090"),
		PostgresDSN:        envString("FLEXSTORE_POSTGRES_DSN", ""),
		RedisAddr:          envString("FLEXSTORE_REDIS_ADDR", ""),
		RedisDB:            envInt("FLEXSTORE_REDIS_DB", 0, &errs),
		ReplicationFactor:  envInt("FLEXSTORE_REPLICATION_FACTOR", 3, &errs),
		MinWriteReplicas:   envInt("FLEXSTORE_MIN_WRITE_REPLICAS", 2, &errs),
		ChunkSize:          envBytes("FLEXSTORE_CHUNK_SIZE", DefaultChunkSize, &errs),
		HeartbeatInterval:  envDuration("FLEXSTORE_HEARTBEAT_INTERVAL", 5*time.Second, &errs),
		SuspectTimeout:     envDuration("FLEXSTORE_SUSPECT_TIMEOUT", 15*time.Second, &errs),
		DeadTimeout:        envDuration("FLEXSTORE_DEAD_TIMEOUT", 60*time.Second, &errs),
		HealthScanInterval: envDuration("FLEXSTORE_HEALTH_SCAN_INTERVAL", 3*time.Second, &errs),
		GCInterval:         envDuration("FLEXSTORE_GC_INTERVAL", 20*time.Second, &errs),
		GCBatchSize:        envInt("FLEXSTORE_GC_BATCH_SIZE", 200, &errs),
		UploadStaleAfter:   envDuration("FLEXSTORE_UPLOAD_STALE_AFTER", 30*time.Minute, &errs),
		NodeCallTimeout:    envDuration("FLEXSTORE_NODE_CALL_TIMEOUT", 15*time.Second, &errs),
		MigrateOnStart:     envBool("FLEXSTORE_MIGRATE_ON_START", true, &errs),
		NodeCacheTTL:       envDuration("FLEXSTORE_NODE_CACHE_TTL", 2*time.Second, &errs),
		ObjectCacheTTL:     envDuration("FLEXSTORE_OBJECT_CACHE_TTL", 30*time.Second, &errs),
		PlacementStrategy:  envString("FLEXSTORE_PLACEMENT_STRATEGY", "weighted"),

		RepairEnabled:      envBool("FLEXSTORE_REPAIR_ENABLED", true, &errs),
		RepairWorkers:      envInt("FLEXSTORE_REPAIR_WORKERS", 4, &errs),
		RepairMaxPerNode:   envInt("FLEXSTORE_REPAIR_MAX_PER_NODE", 2, &errs),
		RepairScanInterval: envDuration("FLEXSTORE_REPAIR_SCAN_INTERVAL", 5*time.Second, &errs),
		RepairScanBatch:    envInt("FLEXSTORE_REPAIR_SCAN_BATCH", 200, &errs),
		RepairJobTimeout:   envDuration("FLEXSTORE_REPAIR_JOB_TIMEOUT", 120*time.Second, &errs),
		RepairLease:        envDuration("FLEXSTORE_REPAIR_LEASE", 5*time.Minute, &errs),
		RepairMaxAttempts:  envInt("FLEXSTORE_REPAIR_MAX_ATTEMPTS", 6, &errs),
		RepairBaseBackoff:  envDuration("FLEXSTORE_REPAIR_BASE_BACKOFF", 2*time.Second, &errs),
		RepairMaxBackoff:   envDuration("FLEXSTORE_REPAIR_MAX_BACKOFF", 2*time.Minute, &errs),
		RepairHistoryTTL:   envDuration("FLEXSTORE_REPAIR_HISTORY_TTL", 6*time.Hour, &errs),

		ReconcileEnabled:        envBool("FLEXSTORE_RECONCILE_ENABLED", true, &errs),
		ReconcileInterval:       envDuration("FLEXSTORE_RECONCILE_INTERVAL", 5*time.Second, &errs),
		ReconcilePageSize:       envInt("FLEXSTORE_RECONCILE_PAGE_SIZE", 500, &errs),
		ReconcileTimeout:        envDuration("FLEXSTORE_RECONCILE_TIMEOUT", 10*time.Minute, &errs),
		ReconcileLease:          envDuration("FLEXSTORE_RECONCILE_LEASE", 15*time.Minute, &errs),
		ReconcileVerify:         envBool("FLEXSTORE_RECONCILE_VERIFY_CHECKSUMS", true, &errs),
		DurabilityAuditInterval: envDuration("FLEXSTORE_DURABILITY_AUDIT_INTERVAL", 10*time.Minute, &errs),
	}

	if c.PostgresDSN == "" {
		errs = append(errs, errors.New("FLEXSTORE_POSTGRES_DSN is required"))
	}
	if c.ReplicationFactor < 1 {
		errs = append(errs, fmt.Errorf("FLEXSTORE_REPLICATION_FACTOR must be >= 1, got %d", c.ReplicationFactor))
	}
	if c.MinWriteReplicas < 1 || c.MinWriteReplicas > c.ReplicationFactor {
		errs = append(errs, fmt.Errorf(
			"FLEXSTORE_MIN_WRITE_REPLICAS must be in [1, replication_factor=%d], got %d",
			c.ReplicationFactor, c.MinWriteReplicas))
	}
	if c.SuspectTimeout <= c.HeartbeatInterval {
		errs = append(errs, fmt.Errorf(
			"FLEXSTORE_SUSPECT_TIMEOUT (%s) must exceed FLEXSTORE_HEARTBEAT_INTERVAL (%s)",
			c.SuspectTimeout, c.HeartbeatInterval))
	}
	if c.DeadTimeout <= c.SuspectTimeout {
		errs = append(errs, fmt.Errorf(
			"FLEXSTORE_DEAD_TIMEOUT (%s) must exceed FLEXSTORE_SUSPECT_TIMEOUT (%s)",
			c.DeadTimeout, c.SuspectTimeout))
	}
	if err := validateChunkSize(c.ChunkSize); err != nil {
		errs = append(errs, err)
	}
	if c.RepairWorkers < 1 {
		errs = append(errs, fmt.Errorf("FLEXSTORE_REPAIR_WORKERS must be >= 1, got %d", c.RepairWorkers))
	}
	if c.RepairMaxPerNode < 1 {
		errs = append(errs, fmt.Errorf("FLEXSTORE_REPAIR_MAX_PER_NODE must be >= 1, got %d", c.RepairMaxPerNode))
	}
	// A lease shorter than the job timeout would let a worker's own job be
	// stolen while it is still copying, producing duplicate work and confusing
	// metrics.
	if c.RepairLease <= c.RepairJobTimeout {
		errs = append(errs, fmt.Errorf(
			"FLEXSTORE_REPAIR_LEASE (%s) must exceed FLEXSTORE_REPAIR_JOB_TIMEOUT (%s)",
			c.RepairLease, c.RepairJobTimeout))
	}
	if c.ReconcileLease <= c.ReconcileTimeout {
		errs = append(errs, fmt.Errorf(
			"FLEXSTORE_RECONCILE_LEASE (%s) must exceed FLEXSTORE_RECONCILE_TIMEOUT (%s)",
			c.ReconcileLease, c.ReconcileTimeout))
	}
	switch c.PlacementStrategy {
	case "weighted", "rendezvous":
	default:
		errs = append(errs, fmt.Errorf(
			"FLEXSTORE_PLACEMENT_STRATEGY must be \"weighted\" or \"rendezvous\", got %q",
			c.PlacementStrategy))
	}
	return c, errors.Join(errs...)
}

// LoadStorageNode reads storage-node configuration from the process environment.
func LoadStorageNode() (StorageNode, error) {
	var errs []error
	c := StorageNode{
		Common:            loadCommon("storage-node", "0.0.0.0:9103", &errs),
		NodeID:            envString("FLEXSTORE_NODE_ID", ""),
		GRPCAddr:          envString("FLEXSTORE_GRPC_ADDR", "0.0.0.0:9100"),
		AdvertiseAddr:     envString("FLEXSTORE_ADVERTISE_ADDR", ""),
		DataDir:           envString("FLEXSTORE_DATA_DIR", "/data"),
		CoordinatorAddr:   envString("FLEXSTORE_COORDINATOR_ADDR", "coordinator:9090"),
		CapacityBytes:     envBytes("FLEXSTORE_NODE_CAPACITY_BYTES", 0, &errs),
		HeartbeatInterval: envDuration("FLEXSTORE_HEARTBEAT_INTERVAL", 5*time.Second, &errs),
		RegisterTimeout:   envDuration("FLEXSTORE_REGISTER_TIMEOUT", 10*time.Second, &errs),
		FsyncData:         envBool("FLEXSTORE_FSYNC_DATA", true, &errs),
	}

	if c.NodeID == "" {
		errs = append(errs, errors.New("FLEXSTORE_NODE_ID is required"))
	}
	if c.AdvertiseAddr == "" {
		errs = append(errs, errors.New("FLEXSTORE_ADVERTISE_ADDR is required (host:port other services dial)"))
	}
	if c.Common.ServiceName == "storage-node" && c.NodeID != "" {
		c.Common.ServiceName = "storage-node"
	}
	return c, errors.Join(errs...)
}

func loadCommon(service, defaultMetricsAddr string, errs *[]error) Common {
	return Common{
		ServiceName:  service,
		LogLevel:     envString("FLEXSTORE_LOG_LEVEL", "info"),
		LogFormat:    envString("FLEXSTORE_LOG_FORMAT", "json"),
		MetricsAddr:  envString("FLEXSTORE_METRICS_ADDR", defaultMetricsAddr),
		ShutdownWait: envDuration("FLEXSTORE_SHUTDOWN_TIMEOUT", 20*time.Second, errs),
	}
}

func validateChunkSize(n int64) error {
	if n < MinChunkSize || n > MaxChunkSize {
		return fmt.Errorf("FLEXSTORE_CHUNK_SIZE must be within [%d, %d] bytes, got %d",
			MinChunkSize, MaxChunkSize, n)
	}
	return nil
}

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func envInt(key string, def int, errs *[]error) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not an integer", key, v))
		return def
	}
	return n
}

func envBool(key string, def bool, errs *[]error) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a boolean", key, v))
		return def
	}
	return b
}

func envDuration(key string, def time.Duration, errs *[]error) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a duration (e.g. 5s, 1m)", key, v))
		return def
	}
	return d
}

// envBytes accepts a plain byte count or a human suffix (KiB/MiB/GiB/KB/MB/GB).
func envBytes(key string, def int64, errs *[]error) int64 {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := ParseBytes(strings.TrimSpace(v))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

// ParseBytes converts a size string such as "8MiB", "512kb" or "1048576" into
// a byte count. Exported so tests and tooling share exactly one parser.
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size")
	}
	// Longest suffixes first so "MiB" is not matched as "B".
	units := []struct {
		suffix string
		mult   int64
	}{
		{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
		{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000}, {"TB", 1000 * 1000 * 1000 * 1000},
		{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
		{"B", 1},
	}
	upper := strings.ToUpper(s)
	for _, u := range units {
		if strings.HasSuffix(upper, u.suffix) {
			numPart := strings.TrimSpace(upper[:len(upper)-len(u.suffix)])
			if numPart == "" {
				continue
			}
			n, err := strconv.ParseFloat(numPart, 64)
			if err != nil {
				return 0, fmt.Errorf("%q is not a valid size", s)
			}
			if n < 0 {
				return 0, fmt.Errorf("%q is negative", s)
			}
			return int64(n * float64(u.mult)), nil
		}
	}
	n, err := strconv.ParseInt(upper, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid size", s)
	}
	return n, nil
}

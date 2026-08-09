package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every FlexStore collector. Services construct one and pass it
// down explicitly rather than reaching for package-level globals, which keeps
// tests isolated (each test gets its own registry).
type Metrics struct {
	Registry *prometheus.Registry

	// --- HTTP (gateway) ---
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec
	HTTPInFlight        prometheus.Gauge

	// --- Object data volume (gateway) ---
	UploadBytesTotal   prometheus.Counter
	DownloadBytesTotal prometheus.Counter

	// --- Chunk lifecycle ---
	ChunksStoredTotal   *prometheus.CounterVec
	ChunkWriteDuration  *prometheus.HistogramVec
	ChunkReadDuration   *prometheus.HistogramVec
	ChunkWriteFailures  *prometheus.CounterVec
	ChecksumFailures    *prometheus.CounterVec
	ReplicaWriteResults *prometheus.CounterVec

	// --- Replication / durability (coordinator) ---
	ReplicaCount          *prometheus.HistogramVec
	UnderReplicatedChunks prometheus.Gauge
	ChunksTotal           prometheus.Gauge
	ObjectsTotal          *prometheus.GaugeVec

	// --- Capacity & health ---
	StorageUsedBytes     *prometheus.GaugeVec
	StorageCapacityBytes *prometheus.GaugeVec
	NodeHealth           *prometheus.GaugeVec
	NodeActiveRequests   *prometheus.GaugeVec

	// --- gRPC (all services) ---
	GRPCRequestsTotal   *prometheus.CounterVec
	GRPCRequestDuration *prometheus.HistogramVec

	// --- Metadata cache (coordinator) ---
	//
	// Two separate counters rather than one with a hit/miss label, because the
	// question these answer is "is the cache earning its place", and a ratio of
	// two independently-named series is what every dashboard and alert
	// expression for that question already expects.
	MetadataCacheHits   *prometheus.CounterVec
	MetadataCacheMisses *prometheus.CounterVec
	MetadataCacheErrors *prometheus.CounterVec

	// --- Placement / GC ---
	PlacementFailures *prometheus.CounterVec
	GCDeletedChunks   prometheus.Counter
	GCErrors          prometheus.Counter

	// --- Self-healing (coordinator) ---
	RepairsTotal           *prometheus.CounterVec
	RepairsFailedTotal     prometheus.Counter
	RepairDuration         *prometheus.HistogramVec
	RepairBytesTotal       prometheus.Counter
	RepairsInFlight        prometheus.Gauge
	RepairJobs             *prometheus.GaugeVec
	UnavailableChunks      prometheus.Gauge
	UnavailableChunkEvents prometheus.Counter
	ReconcileTotal         *prometheus.CounterVec
	ReconcileActions       *prometheus.CounterVec
	ReconcileDuration      prometheus.Histogram
	DurabilityDrift        prometheus.Counter
	ReplicasTrimmed        prometheus.Counter
}

// latencyBuckets spans 1 ms .. ~16 s.
var latencyBuckets = prometheus.ExponentialBuckets(0.001, 3, 10)

// NewMetrics creates and registers all collectors on a fresh registry.
func NewMetrics(service string) *Metrics {
	reg := prometheus.NewRegistry()
	constLabels := prometheus.Labels{"service": service}

	m := &Metrics{
		Registry: reg,

		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_http_requests_total",
			Help:        "Total client-facing HTTP requests, by route/method/status class.",
			ConstLabels: constLabels,
		}, []string{"method", "route", "status"}),

		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "flexstore_http_request_duration_seconds",
			Help:        "Client-facing HTTP request latency in seconds.",
			Buckets:     latencyBuckets,
			ConstLabels: constLabels,
		}, []string{"method", "route", "status"}),

		HTTPInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "flexstore_http_requests_in_flight",
			Help:        "HTTP requests currently being served.",
			ConstLabels: constLabels,
		}),

		UploadBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_upload_bytes_total",
			Help:        "Total object bytes accepted from clients (logical, pre-replication).",
			ConstLabels: constLabels,
		}),

		DownloadBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_download_bytes_total",
			Help:        "Total object bytes served to clients.",
			ConstLabels: constLabels,
		}),

		ChunksStoredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_chunks_stored_total",
			Help:        "Chunk replicas durably written on this node.",
			ConstLabels: constLabels,
		}, []string{"node_id", "result"}),

		ChunkWriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "flexstore_chunk_write_duration_seconds",
			Help:        "Time to durably write one chunk replica.",
			Buckets:     latencyBuckets,
			ConstLabels: constLabels,
		}, []string{"result"}),

		ChunkReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "flexstore_chunk_read_duration_seconds",
			Help:        "Time to read and verify one chunk replica.",
			Buckets:     latencyBuckets,
			ConstLabels: constLabels,
		}, []string{"result"}),

		ChunkWriteFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_chunk_write_failures_total",
			Help:        "Chunk replica writes that failed, by reason.",
			ConstLabels: constLabels,
		}, []string{"reason"}),

		ChecksumFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_checksum_failures_total",
			Help:        "Detected SHA-256 mismatches, by stage (write/read/scrub).",
			ConstLabels: constLabels,
		}, []string{"stage"}),

		ReplicaWriteResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_replica_writes_total",
			Help:        "Per-replica write outcomes observed by the gateway.",
			ConstLabels: constLabels,
		}, []string{"node_id", "result"}),

		ReplicaCount: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "flexstore_replica_count",
			Help:        "Number of available replicas observed per committed chunk.",
			Buckets:     []float64{0, 1, 2, 3, 4, 5, 6, 8, 10},
			ConstLabels: constLabels,
		}, []string{"phase"}),

		UnderReplicatedChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "flexstore_under_replicated_chunks",
			Help:        "Committed chunks with fewer available replicas than the replication factor.",
			ConstLabels: constLabels,
		}),

		ChunksTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "flexstore_chunks_total",
			Help:        "Committed chunks known to the coordinator.",
			ConstLabels: constLabels,
		}),

		ObjectsTotal: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "flexstore_objects_total",
			Help:        "Objects known to the coordinator, by state.",
			ConstLabels: constLabels,
		}, []string{"state"}),

		StorageUsedBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "flexstore_storage_used_bytes",
			Help:        "Bytes of chunk data stored.",
			ConstLabels: constLabels,
		}, []string{"node_id"}),

		StorageCapacityBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "flexstore_storage_capacity_bytes",
			Help:        "Total storage capacity.",
			ConstLabels: constLabels,
		}, []string{"node_id"}),

		NodeHealth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "flexstore_node_health",
			Help:        "Storage node health: 1=HEALTHY, 2=SUSPECT, 3=DEAD.",
			ConstLabels: constLabels,
		}, []string{"node_id"}),

		NodeActiveRequests: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "flexstore_node_active_requests",
			Help:        "In-flight chunk operations on a storage node.",
			ConstLabels: constLabels,
		}, []string{"node_id"}),

		GRPCRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_grpc_requests_total",
			Help:        "Internal gRPC calls served, by method and status code.",
			ConstLabels: constLabels,
		}, []string{"method", "code"}),

		GRPCRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "flexstore_grpc_request_duration_seconds",
			Help:        "Internal gRPC handler latency in seconds.",
			Buckets:     latencyBuckets,
			ConstLabels: constLabels,
		}, []string{"method"}),

		PlacementFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_placement_failures_total",
			Help:        "Placement attempts that could not satisfy the replication policy.",
			ConstLabels: constLabels,
		}, []string{"reason"}),

		GCDeletedChunks: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_gc_deleted_chunks_total",
			Help:        "Chunk replicas reclaimed by the coordinator's garbage collector.",
			ConstLabels: constLabels,
		}),

		GCErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_gc_errors_total",
			Help:        "Garbage-collection cycles or deletions that returned an error.",
			ConstLabels: constLabels,
		}),

		RepairsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_repairs_total",
			Help:        "Chunk re-replications attempted, by outcome.",
			ConstLabels: constLabels,
		}, []string{"result"}),

		RepairsFailedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_repairs_failed_total",
			Help:        "Chunk re-replication attempts that failed.",
			ConstLabels: constLabels,
		}),

		RepairDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:        "flexstore_repair_duration_seconds",
			Help:        "Wall-clock time to re-replicate one chunk.",
			Buckets:     latencyBuckets,
			ConstLabels: constLabels,
		}, []string{"result"}),

		RepairBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_repair_bytes_total",
			Help:        "Bytes copied by the repair manager.",
			ConstLabels: constLabels,
		}),

		RepairsInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "flexstore_repairs_in_flight",
			Help:        "Chunk re-replications currently executing.",
			ConstLabels: constLabels,
		}),

		RepairJobs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name:        "flexstore_repair_jobs",
			Help:        "Repair jobs by state (PENDING/RUNNING/SUCCEEDED/FAILED).",
			ConstLabels: constLabels,
		}, []string{"state"}),

		UnavailableChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "flexstore_unavailable_chunks",
			Help: "Committed chunks with zero usable replicas. " +
				"Non-zero means data is currently unreadable, not merely at risk.",
			ConstLabels: constLabels,
		}),

		MetadataCacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_metadata_cache_hits_total",
			Help:        "Metadata cache lookups served from Redis, by cache.",
			ConstLabels: constLabels,
		}, []string{"cache"}),

		MetadataCacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_metadata_cache_misses_total",
			Help:        "Metadata cache lookups that fell through to PostgreSQL, by cache.",
			ConstLabels: constLabels,
		}, []string{"cache"}),

		MetadataCacheErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "flexstore_metadata_cache_errors_total",
			Help: "Metadata cache operations that failed. Counted separately from " +
				"misses because a broken cache and a cold cache are different problems.",
			ConstLabels: constLabels,
		}, []string{"cache", "op"}),

		UnavailableChunkEvents: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "flexstore_unavailable_chunk_events_total",
			Help:        "Times the repair manager found a chunk with no readable source.",
			ConstLabels: constLabels,
		}),

		ReconcileTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "flexstore_reconciliations_total",
			Help:        "Node inventory reconciliations, by outcome.",
			ConstLabels: constLabels,
		}, []string{"result"}),

		ReconcileActions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "flexstore_reconcile_actions_total",
			Help: "Reconciliation outcomes per replica " +
				"(verified/phantom/orphan/corrupt).",
			ConstLabels: constLabels,
		}, []string{"action"}),

		ReconcileDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "flexstore_reconcile_duration_seconds",
			Help:        "Time to reconcile one returning node's full inventory.",
			Buckets:     latencyBuckets,
			ConstLabels: constLabels,
		}),

		ReplicasTrimmed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "flexstore_replicas_trimmed_total",
			Help: "Excess replicas removed after recovery left a chunk above the " +
				"replication factor.",
			ConstLabels: constLabels,
		}),

		DurabilityDrift: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "flexstore_durability_counter_drift_total",
			Help: "Chunks whose cached replica count disagreed with the underlying rows. " +
				"Should always be zero; non-zero means something bypassed the write path.",
			ConstLabels: constLabels,
		}),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequestsTotal, m.HTTPRequestDuration, m.HTTPInFlight,
		m.UploadBytesTotal, m.DownloadBytesTotal,
		m.ChunksStoredTotal, m.ChunkWriteDuration, m.ChunkReadDuration,
		m.ChunkWriteFailures, m.ChecksumFailures, m.ReplicaWriteResults,
		m.ReplicaCount, m.UnderReplicatedChunks, m.ChunksTotal, m.ObjectsTotal,
		m.StorageUsedBytes, m.StorageCapacityBytes, m.NodeHealth, m.NodeActiveRequests,
		m.GRPCRequestsTotal, m.GRPCRequestDuration,
		m.PlacementFailures, m.GCDeletedChunks, m.GCErrors,
		m.RepairsTotal, m.RepairsFailedTotal, m.RepairDuration, m.RepairBytesTotal,
		m.RepairsInFlight, m.RepairJobs, m.UnavailableChunks, m.UnavailableChunkEvents,
		m.MetadataCacheHits, m.MetadataCacheMisses, m.MetadataCacheErrors,
		m.ReconcileTotal, m.ReconcileActions, m.ReconcileDuration, m.DurabilityDrift,
		m.ReplicasTrimmed,
	)

	return m
}

// Package metadata is the coordinator's durable state layer. It owns every
// read and write against PostgreSQL and exposes domain types -- never
// database rows or protobuf messages -- to the rest of the coordinator.
package metadata

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// Sentinel errors the coordinator maps onto gRPC status codes.
var (
	ErrNotFound = errors.New("not found")
	// ErrConflict covers optimistic-concurrency and state-machine violations,
	// e.g. committing a chunk for an object that is no longer UPLOADING.
	ErrConflict = errors.New("conflicting state")
	// ErrDurabilityNotMet means fewer replicas landed than the policy requires.
	ErrDurabilityNotMet = errors.New("durability policy not satisfied")
)

// ObjectState is the object lifecycle. Mirrors the CHECK constraint in SQL.
type ObjectState string

const (
	ObjectUploading ObjectState = "UPLOADING"
	ObjectComplete  ObjectState = "COMPLETE"
	ObjectDeleting  ObjectState = "DELETING"
	ObjectFailed    ObjectState = "FAILED"
)

// ChunkState is the chunk lifecycle.
type ChunkState string

const (
	// ChunkPending: allocated and being written; not yet safe to read.
	ChunkPending ChunkState = "PENDING"
	// ChunkCommitted: durability policy met, checksum recorded.
	ChunkCommitted ChunkState = "COMMITTED"
	// ChunkOrphaned: owner went away; queued for reclamation.
	ChunkOrphaned ChunkState = "ORPHANED"
)

// ReplicaState is a single copy's lifecycle on a single node.
//
// Only AVAILABLE counts towards durability. The distinctions between the
// non-available states are not cosmetic -- each implies a different remedy:
// UNAVAILABLE waits for the node, CORRUPT deletes the file, STALE verifies it.
type ReplicaState string

const (
	ReplicaWriting     ReplicaState = "WRITING"
	ReplicaAvailable   ReplicaState = "AVAILABLE"
	ReplicaUnavailable ReplicaState = "UNAVAILABLE"
	ReplicaDeleting    ReplicaState = "DELETING"
	// ReplicaCorrupt: bytes failed SHA-256 verification.
	ReplicaCorrupt ReplicaState = "CORRUPT"
	// ReplicaStale: the holding node rejoined; not trusted until reconciled.
	ReplicaStale ReplicaState = "STALE"
)

// RepairJobState is the lifecycle of one re-replication job.
type RepairJobState string

const (
	RepairPending   RepairJobState = "PENDING"
	RepairRunning   RepairJobState = "RUNNING"
	RepairSucceeded RepairJobState = "SUCCEEDED"
	RepairFailed    RepairJobState = "FAILED"
)

// ReconcileState is the lifecycle of one node inventory comparison.
type ReconcileState string

const (
	ReconcilePending   ReconcileState = "PENDING"
	ReconcileRunning   ReconcileState = "RUNNING"
	ReconcileSucceeded ReconcileState = "SUCCEEDED"
	ReconcileFailed    ReconcileState = "FAILED"
)

// MultipartState is the upload-session lifecycle.
type MultipartState string

const (
	MultipartInProgress MultipartState = "IN_PROGRESS"
	MultipartCompleting MultipartState = "COMPLETING"
	MultipartCompleted  MultipartState = "COMPLETED"
	MultipartAborted    MultipartState = "ABORTED"
)

// PartState is a single part's lifecycle.
type PartState string

const (
	PartUploading PartState = "UPLOADING"
	PartComplete  PartState = "COMPLETE"
	PartFailed    PartState = "FAILED"
)

// Node is a registered storage node.
type Node struct {
	ID              string
	Address         string
	Health          string
	TotalBytes      int64
	UsedBytes       int64
	AvailableBytes  int64
	ChunkCount      int64
	ActiveRequests  int32
	LastHeartbeatAt time.Time
	RegisteredAt    time.Time
}

// NodeStats is the mutable portion a node reports each heartbeat.
type NodeStats struct {
	TotalBytes     int64
	UsedBytes      int64
	AvailableBytes int64
	ChunkCount     int64
	ActiveRequests int32
}

// Object is one version of a stored object.
type Object struct {
	ID            uuid.UUID
	Bucket        string
	Key           string
	Version       int64
	State         ObjectState
	SizeBytes     int64
	ChunkSize     int64
	ChunkCount    int32
	ETag          string
	ContentType   string
	FailureReason string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CompletedAt   time.Time
}

// Chunk is one fixed-size slice of an object.
type Chunk struct {
	ID          uuid.UUID
	ObjectID    *uuid.UUID
	PartID      *uuid.UUID
	Index       int32
	SizeBytes   int64
	Checksum    string
	State       ChunkState
	CreatedAt   time.Time
	CommittedAt time.Time
}

// Replica is one copy of a chunk on one node.
type Replica struct {
	ChunkID    uuid.UUID
	NodeID     string
	Address    string
	NodeHealth string
	State      ReplicaState
	UpdatedAt  time.Time
	VerifiedAt time.Time
}

// ChunkWithReplicas is the read-path projection: a chunk plus every location
// that currently claims to hold it.
type ChunkWithReplicas struct {
	Chunk
	Replicas []Replica
}

// MultipartUpload is an in-flight multipart session.
type MultipartUpload struct {
	ID          uuid.UUID
	Bucket      string
	Key         string
	State       MultipartState
	ChunkSize   int64
	ContentType string
	ObjectID    *uuid.UUID
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// Part is one uploaded part of a multipart session.
type Part struct {
	ID         uuid.UUID
	UploadID   uuid.UUID
	PartNumber int32
	State      PartState
	SizeBytes  int64
	ChunkCount int32
	ETag       string
	CreatedAt  time.Time
}

// PendingDeletion is one queued "remove this chunk from this node" job.
type PendingDeletion struct {
	ID       int64
	ChunkID  uuid.UUID
	NodeID   string
	Address  string
	Attempts int32
}

// ClusterStats is the coordinator's periodic durability report.
type ClusterStats struct {
	TotalChunks           int64
	UnderReplicatedChunks int64
	// UnavailableChunks have zero usable replicas: the data is currently
	// unreadable. Reported separately because it is a categorically worse
	// condition than under-replication and needs a different response.
	UnavailableChunks int64
	ObjectsByState    map[ObjectState]int64
	RepairJobsByState map[RepairJobState]int64
}

// RepairJob is one queued re-replication of a chunk.
type RepairJob struct {
	ID           int64
	ChunkID      uuid.UUID
	State        RepairJobState
	SourceNodeID string
	TargetNodeID string
	Attempts     int32
	LastError    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
	FinishedAt   time.Time
}

// RepairTarget is everything a worker needs to execute a repair, loaded in one
// query so the worker does not fan out to the database mid-repair.
type RepairTarget struct {
	Job      RepairJob
	Checksum string
	Size     int64
	// Sources are AVAILABLE replicas on non-DEAD nodes, best-first.
	Sources []Replica
	// Occupied lists every node already associated with this chunk in any
	// state. The destination must avoid all of them, or "3 replicas" could
	// mean two copies on one machine.
	Occupied []string
	// DesiredReplicas is how many more AVAILABLE replicas the chunk needs.
	DesiredReplicas int
	// StaleReplicas counts copies awaiting verification. A chunk whose only
	// non-available copies are STALE does not need a byte copied -- it needs
	// the reconciler to run -- and every one of them also *occupies* a node,
	// so treating this as a placement failure would burn retries on a
	// situation no amount of retrying can fix.
	StaleReplicas int
	// TransientOccupancy counts occupied nodes whose replica is not AVAILABLE:
	// CORRUPT and DELETING copies awaiting garbage collection, STALE copies
	// awaiting verification, UNAVAILABLE copies on a node that is currently
	// down. Each one blocks its node from being a repair destination, but none
	// of them is permanent -- the row disappears once the GC, the reconciler or
	// the health monitor has done its work.
	//
	// This is the difference between "there is genuinely nowhere to put a
	// copy" and "there is nowhere to put a copy *yet*". Retrying the latter
	// burns the job's attempts and lands it in FAILED, where it stays until a
	// human or a node join wakes it up. Deferring instead lets the scanner
	// re-enqueue once the blocking rows clear, which is usually seconds later.
	TransientOccupancy int
}

// ReconcileJob is one queued node inventory comparison.
type ReconcileJob struct {
	ID     int64
	NodeID string
	State  ReconcileState
	// Address is joined in at claim time so the worker can dial the node.
	Address    string
	Attempts   int32
	CreatedAt  time.Time
	FinishedAt time.Time
}

// ReconcileResult is what a completed reconciliation found.
type ReconcileResult struct {
	ChunksSeen    int64
	OrphansFound  int64
	PhantomsFound int64
	Verified      int64
	CorruptFound  int64
}

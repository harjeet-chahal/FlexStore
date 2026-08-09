package gateway

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
)

// The /admin surface is deliberately read-only apart from an explicit
// reconcile trigger, and it exposes *logical* state only -- node IDs, replica
// states, chunk IDs, capacity. It never returns filesystem paths, data
// directories or connection strings: an introspection API that leaks where
// bytes live on disk is an attack surface, and everything an operator actually
// needs to diagnose durability is expressible without it.

// AdminNodes implements GET /admin/nodes.
func (h *Handler) AdminNodes(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coord.Raw().GetClusterStatus(r.Context(), &flexstorev1.GetClusterStatusRequest{})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	type nodeView struct {
		NodeID         string  `json:"node_id"`
		Address        string  `json:"address"`
		Health         string  `json:"health"`
		TotalBytes     int64   `json:"total_bytes"`
		UsedBytes      int64   `json:"used_bytes"`
		AvailableBytes int64   `json:"available_bytes"`
		UsedPercent    float64 `json:"used_percent"`
		ChunkCount     int64   `json:"chunk_count"`
		ActiveRequests int32   `json:"active_requests"`
		LastHeartbeat  string  `json:"last_heartbeat_at,omitempty"`
		HeartbeatAgeMS int64   `json:"heartbeat_age_ms,omitempty"`
	}
	out := struct {
		Nodes                 []nodeView `json:"nodes"`
		Healthy               int        `json:"healthy"`
		Suspect               int        `json:"suspect"`
		Dead                  int        `json:"dead"`
		Total                 int        `json:"total"`
		ReplicationFactor     int32      `json:"replication_factor"`
		TotalObjects          int64      `json:"total_objects"`
		TotalChunks           int64      `json:"total_chunks"`
		UnderReplicatedChunks int64      `json:"under_replicated_chunks"`
	}{
		Nodes:                 make([]nodeView, 0, len(resp.Nodes)),
		ReplicationFactor:     resp.ReplicationFactor,
		TotalObjects:          resp.TotalObjects,
		TotalChunks:           resp.TotalChunks,
		UnderReplicatedChunks: resp.UnderReplicatedChunks,
	}

	now := time.Now()
	for _, n := range resp.Nodes {
		stats := n.GetStats()
		v := nodeView{
			NodeID: n.NodeId, Address: n.GrpcAddress, Health: shortHealth(n.Health),
			TotalBytes:     stats.GetTotalBytes(),
			UsedBytes:      stats.GetUsedBytes(),
			AvailableBytes: stats.GetAvailableBytes(),
			ChunkCount:     stats.GetChunkCount(),
			ActiveRequests: stats.GetActiveRequests(),
		}
		if stats.GetTotalBytes() > 0 {
			v.UsedPercent = float64(stats.GetUsedBytes()) / float64(stats.GetTotalBytes()) * 100
		}
		if n.LastHeartbeatAt != nil {
			hb := n.LastHeartbeatAt.AsTime()
			v.LastHeartbeat = hb.UTC().Format(time.RFC3339Nano)
			v.HeartbeatAgeMS = now.Sub(hb).Milliseconds()
		}
		switch v.Health {
		case "HEALTHY":
			out.Healthy++
		case "SUSPECT":
			out.Suspect++
		case "DEAD":
			out.Dead++
		}
		out.Nodes = append(out.Nodes, v)
	}
	out.Total = len(out.Nodes)
	writeJSON(w, http.StatusOK, out, h.log)
}

// AdminObjectChunks implements GET /admin/objects/{bucket}/{key...}/chunks.
//
// This is the endpoint that makes replication auditable: it shows, per chunk,
// exactly which nodes hold a copy and in what state.
func (h *Handler) AdminObjectChunks(w http.ResponseWriter, r *http.Request) {
	resp, err := h.lookupLayout(r, r.PathValue("bucket"), r.PathValue("key"))
	if err != nil {
		h.fail(w, r, err)
		return
	}

	replStatus, err := h.coord.Raw().GetReplicationStatus(r.Context(),
		&flexstorev1.GetReplicationStatusRequest{RecentJobLimit: 1})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	rf := int(replStatus.ReplicationFactor)

	type replicaView struct {
		NodeID     string `json:"node_id"`
		State      string `json:"state"`
		NodeHealth string `json:"node_health"`
	}
	type chunkView struct {
		ChunkID           string        `json:"chunk_id"`
		Index             int32         `json:"index"`
		SizeBytes         int64         `json:"size_bytes"`
		Checksum          string        `json:"checksum_sha256"`
		AvailableReplicas int           `json:"available_replicas"`
		UnderReplicated   bool          `json:"under_replicated"`
		Unavailable       bool          `json:"unavailable"`
		Replicas          []replicaView `json:"replicas"`
	}
	out := struct {
		Bucket                string      `json:"bucket"`
		Key                   string      `json:"key"`
		ObjectID              string      `json:"object_id"`
		SizeBytes             int64       `json:"size_bytes"`
		ChunkCount            int32       `json:"chunk_count"`
		ETag                  string      `json:"etag"`
		ReplicationFactor     int         `json:"replication_factor"`
		UnderReplicatedChunks int         `json:"under_replicated_chunks"`
		UnavailableChunks     int         `json:"unavailable_chunks"`
		DistinctNodes         []string    `json:"distinct_nodes"`
		Chunks                []chunkView `json:"chunks"`
	}{
		Bucket: resp.Object.Bucket, Key: resp.Object.Key, ObjectID: resp.Object.ObjectId,
		SizeBytes: resp.Object.SizeBytes, ChunkCount: resp.Object.ChunkCount,
		ETag:              quoteETag(resp.Object.Etag),
		ReplicationFactor: rf,
		Chunks:            make([]chunkView, 0, len(resp.Chunks)),
	}

	nodeSeen := map[string]bool{}
	for _, c := range resp.Chunks {
		cv := chunkView{
			ChunkID: c.ChunkId, Index: c.ChunkIndex, SizeBytes: c.SizeBytes,
			Checksum: c.ChecksumSha256,
			Replicas: make([]replicaView, 0, len(c.Replicas)),
		}
		for _, rep := range c.Replicas {
			state := shortReplicaState(rep.State)
			cv.Replicas = append(cv.Replicas, replicaView{
				NodeID: rep.NodeId, State: state, NodeHealth: shortHealth(rep.NodeHealth),
			})
			if state == "AVAILABLE" && shortHealth(rep.NodeHealth) != "DEAD" {
				cv.AvailableReplicas++
				if !nodeSeen[rep.NodeId] {
					nodeSeen[rep.NodeId] = true
					out.DistinctNodes = append(out.DistinctNodes, rep.NodeId)
				}
			}
		}
		cv.UnderReplicated = cv.AvailableReplicas < rf
		cv.Unavailable = cv.AvailableReplicas == 0
		if cv.UnderReplicated {
			out.UnderReplicatedChunks++
		}
		if cv.Unavailable {
			out.UnavailableChunks++
		}
		out.Chunks = append(out.Chunks, cv)
	}
	writeJSON(w, http.StatusOK, out, h.log)
}

// AdminReplication implements GET /admin/replication: durability plus repair
// progress, which is what an operator watches during a recovery.
func (h *Handler) AdminReplication(w http.ResponseWriter, r *http.Request) {
	limit := 25
	if v := r.URL.Query().Get("jobs"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			limit = n
		}
	}

	resp, err := h.coord.Raw().GetReplicationStatus(r.Context(),
		&flexstorev1.GetReplicationStatusRequest{RecentJobLimit: int32(limit)})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	type jobView struct {
		ID        int64  `json:"id"`
		ChunkID   string `json:"chunk_id"`
		State     string `json:"state"`
		Source    string `json:"source_node_id,omitempty"`
		Target    string `json:"target_node_id,omitempty"`
		Attempts  int32  `json:"attempts"`
		LastError string `json:"last_error,omitempty"`
		UpdatedAt string `json:"updated_at,omitempty"`
	}
	out := struct {
		ReplicationFactor     int32     `json:"replication_factor"`
		RepairEnabled         bool      `json:"repair_enabled"`
		TotalChunks           int64     `json:"total_chunks"`
		UnderReplicatedChunks int64     `json:"under_replicated_chunks"`
		UnavailableChunks     int64     `json:"unavailable_chunks"`
		Durable               bool      `json:"fully_durable"`
		JobsPending           int64     `json:"repair_jobs_pending"`
		JobsRunning           int64     `json:"repair_jobs_running"`
		JobsFailed            int64     `json:"repair_jobs_failed"`
		JobsSucceeded         int64     `json:"repair_jobs_succeeded"`
		RecentJobs            []jobView `json:"recent_jobs"`
	}{
		ReplicationFactor:     resp.ReplicationFactor,
		RepairEnabled:         resp.RepairEnabled,
		TotalChunks:           resp.TotalChunks,
		UnderReplicatedChunks: resp.UnderReplicatedChunks,
		UnavailableChunks:     resp.UnavailableChunks,
		Durable:               resp.UnderReplicatedChunks == 0,
		JobsPending:           resp.JobsPending,
		JobsRunning:           resp.JobsRunning,
		JobsFailed:            resp.JobsFailed,
		JobsSucceeded:         resp.JobsSucceededTotal,
		RecentJobs:            make([]jobView, 0, len(resp.RecentJobs)),
	}
	for _, j := range resp.RecentJobs {
		v := jobView{
			ID: j.Id, ChunkID: j.ChunkId, State: shortRepairState(j.State),
			Source: j.SourceNodeId, Target: j.TargetNodeId,
			Attempts: j.Attempts, LastError: j.LastError,
		}
		if j.UpdatedAt != nil {
			v.UpdatedAt = j.UpdatedAt.AsTime().UTC().Format(time.RFC3339Nano)
		}
		out.RecentJobs = append(out.RecentJobs, v)
	}
	writeJSON(w, http.StatusOK, out, h.log)
}

// AdminHealth implements GET /admin/health: a single-glance verdict.
//
// It returns 503 when the cluster cannot currently serve all its data, so it is
// usable directly as a probe rather than something a human has to interpret.
func (h *Handler) AdminHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	cluster, err := h.coord.Raw().GetClusterStatus(ctx, &flexstorev1.GetClusterStatusRequest{})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	repl, err := h.coord.Raw().GetReplicationStatus(ctx,
		&flexstorev1.GetReplicationStatusRequest{RecentJobLimit: 0})
	if err != nil {
		h.fail(w, r, err)
		return
	}

	var healthy, suspect, dead int
	for _, n := range cluster.Nodes {
		switch shortHealth(n.Health) {
		case "HEALTHY":
			healthy++
		case "SUSPECT":
			suspect++
		case "DEAD":
			dead++
		}
	}

	// Three distinct conditions, deliberately not collapsed into one boolean:
	//   degraded    some data is under-replicated but everything is readable
	//   unavailable some data cannot be read at all
	//   healthy     full replication
	statusText := "healthy"
	code := http.StatusOK
	switch {
	case repl.UnavailableChunks > 0:
		statusText = "unavailable"
		code = http.StatusServiceUnavailable
	case repl.UnderReplicatedChunks > 0:
		statusText = "degraded"
	case healthy < int(repl.ReplicationFactor):
		statusText = "degraded"
	}

	out := struct {
		Status                string `json:"status"`
		NodesHealthy          int    `json:"nodes_healthy"`
		NodesSuspect          int    `json:"nodes_suspect"`
		NodesDead             int    `json:"nodes_dead"`
		ReplicationFactor     int32  `json:"replication_factor"`
		TotalChunks           int64  `json:"total_chunks"`
		UnderReplicatedChunks int64  `json:"under_replicated_chunks"`
		UnavailableChunks     int64  `json:"unavailable_chunks"`
		RepairJobsPending     int64  `json:"repair_jobs_pending"`
		RepairJobsRunning     int64  `json:"repair_jobs_running"`
		RepairEnabled         bool   `json:"repair_enabled"`
	}{
		Status: statusText, NodesHealthy: healthy, NodesSuspect: suspect, NodesDead: dead,
		ReplicationFactor:     repl.ReplicationFactor,
		TotalChunks:           repl.TotalChunks,
		UnderReplicatedChunks: repl.UnderReplicatedChunks,
		UnavailableChunks:     repl.UnavailableChunks,
		RepairJobsPending:     repl.JobsPending,
		RepairJobsRunning:     repl.JobsRunning,
		RepairEnabled:         repl.RepairEnabled,
	}
	writeJSON(w, code, out, h.log)
}

// AdminReconcile implements POST /admin/reconcile[?node=ID].
//
// The one non-read-only admin route. It is safe by construction: reconciliation
// only ever makes the cluster's view *more* conservative (verifying replicas,
// dropping ones that turn out not to exist), so triggering it cannot destroy
// data even if invoked at a bad moment.
func (h *Handler) AdminReconcile(w http.ResponseWriter, r *http.Request) {
	resp, err := h.coord.Raw().TriggerReconcile(r.Context(), &flexstorev1.TriggerReconcileRequest{
		NodeId: r.URL.Query().Get("node"),
	})
	if err != nil {
		h.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, struct {
		Scheduled []string `json:"scheduled_node_ids"`
		Count     int      `json:"count"`
	}{resp.ScheduledNodeIds, len(resp.ScheduledNodeIds)}, h.log)
}

// lookupLayout resolves an object layout, accepting both
// /admin/objects/{bucket}/{key} and the documented
// /admin/objects/{bucket}/{key}/chunks form.
//
// Object keys may themselves contain slashes, so a trailing "/chunks" is
// genuinely ambiguous: it could be part of the key. Rather than guessing, the
// literal key is tried first and the suffix-stripped form only as a fallback on
// NotFound. A real object named ".../chunks" therefore still wins, and the
// convenience suffix still works for everything else.
func (h *Handler) lookupLayout(r *http.Request, bucket, key string) (*flexstorev1.GetObjectLayoutResponse, error) {
	resp, err := h.coord.Raw().GetObjectLayout(r.Context(), &flexstorev1.GetObjectLayoutRequest{
		Bucket: bucket, Key: key,
	})
	if err == nil {
		return resp, nil
	}
	const suffix = "/chunks"
	if status.Code(err) != codes.NotFound || !strings.HasSuffix(key, suffix) {
		return nil, err
	}
	return h.coord.Raw().GetObjectLayout(r.Context(), &flexstorev1.GetObjectLayoutRequest{
		Bucket: bucket, Key: strings.TrimSuffix(key, suffix),
	})
}

func shortRepairState(s flexstorev1.RepairJobState) string {
	switch s {
	case flexstorev1.RepairJobState_REPAIR_JOB_STATE_PENDING:
		return "PENDING"
	case flexstorev1.RepairJobState_REPAIR_JOB_STATE_RUNNING:
		return "RUNNING"
	case flexstorev1.RepairJobState_REPAIR_JOB_STATE_SUCCEEDED:
		return "SUCCEEDED"
	case flexstorev1.RepairJobState_REPAIR_JOB_STATE_FAILED:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

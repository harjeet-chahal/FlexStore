//go:build integration

// Package integration exercises FlexStore end to end against a running
// Docker Compose cluster: a real gateway, a real coordinator, five real
// storage nodes, real PostgreSQL and real Redis.
//
// Nothing here is stubbed. The point of these tests is to observe the
// distributed behaviour that unit tests cannot: replicas landing on distinct
// machines, metadata surviving a coordinator restart, and objects remaining
// readable when a storage node disappears.
//
//	make up && make integration-test
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	defaultGatewayURL = "http://localhost:8080"
	// Compose's chunk size. Sized so a few MiB of payload spans several chunks
	// without the tests moving gigabytes.
	defaultChunkSize = 8 << 20
)

func gatewayURL() string {
	if v := os.Getenv("FLEXSTORE_GATEWAY_URL"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return defaultGatewayURL
}

// client is a plain net/http client with no timeout: uploads of tens of
// megabytes through a five-node replication fan-out legitimately take a while,
// and per-test contexts provide the actual bound.
var client = &http.Client{}

func newRequest(t *testing.T, ctx context.Context, method, path string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, gatewayURL()+path, body)
	if err != nil {
		t.Fatalf("building %s %s: %v", method, path, err)
	}
	return req
}

func do(t *testing.T, req *http.Request) (*http.Response, []byte) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	return resp, body
}

// putObject uploads payload and asserts a 201.
func putObject(t *testing.T, ctx context.Context, bucket, key string, payload []byte) http.Header {
	t.Helper()
	req := newRequest(t, ctx, http.MethodPut, "/objects/"+bucket+"/"+key, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(payload))

	resp, body := do(t, req)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("PUT %s/%s = %d: %s", bucket, key, resp.StatusCode, body)
	}
	return resp.Header
}

// getObject downloads an object and asserts a 200.
func getObject(t *testing.T, ctx context.Context, bucket, key string) ([]byte, http.Header) {
	t.Helper()
	resp, body := do(t, newRequest(t, ctx, http.MethodGet, "/objects/"+bucket+"/"+key, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s/%s = %d: %s", bucket, key, resp.StatusCode, body)
	}
	return body, resp.Header
}

func deleteObject(t *testing.T, ctx context.Context, bucket, key string) {
	t.Helper()
	resp, body := do(t, newRequest(t, ctx, http.MethodDelete, "/objects/"+bucket+"/"+key, nil))
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE %s/%s = %d: %s", bucket, key, resp.StatusCode, body)
	}
}

// randomPayload returns n cryptographically random bytes, so a test can never
// pass because two buffers happened to be zeroed.
func randomPayload(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("generating payload: %v", err)
	}
	return b
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ---- admin views ---------------------------------------------------------

type clusterStatus struct {
	Nodes []struct {
		ID             string `json:"node_id"`
		Address        string `json:"address"`
		Health         string `json:"health"`
		TotalBytes     int64  `json:"total_bytes"`
		UsedBytes      int64  `json:"used_bytes"`
		AvailableBytes int64  `json:"available_bytes"`
		ChunkCount     int64  `json:"chunk_count"`
	} `json:"nodes"`
	TotalObjects          int64 `json:"total_objects"`
	TotalChunks           int64 `json:"total_chunks"`
	UnderReplicatedChunks int64 `json:"under_replicated_chunks"`
	ReplicationFactor     int32 `json:"replication_factor"`
}

func (c clusterStatus) healthyNodes() []string {
	var out []string
	for _, n := range c.Nodes {
		if n.Health == "HEALTHY" {
			out = append(out, n.ID)
		}
	}
	return out
}

func getClusterStatus(t *testing.T, ctx context.Context) clusterStatus {
	t.Helper()
	resp, body := do(t, newRequest(t, ctx, http.MethodGet, "/admin/nodes", nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/nodes = %d: %s", resp.StatusCode, body)
	}
	var out clusterStatus
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding cluster status: %v\n%s", err, body)
	}
	return out
}

type objectLayout struct {
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	ObjectID   string `json:"object_id"`
	SizeBytes  int64  `json:"size_bytes"`
	ChunkCount int32  `json:"chunk_count"`
	ETag       string `json:"etag"`
	Chunks     []struct {
		ChunkID  string `json:"chunk_id"`
		Index    int32  `json:"index"`
		Size     int64  `json:"size_bytes"`
		Checksum string `json:"checksum_sha256"`
		Replicas []struct {
			NodeID     string `json:"node_id"`
			State      string `json:"state"`
			NodeHealth string `json:"node_health"`
		} `json:"replicas"`
	} `json:"chunks"`
}

func getLayout(t *testing.T, ctx context.Context, bucket, key string) objectLayout {
	t.Helper()
	resp, body := do(t, newRequest(t, ctx, http.MethodGet, "/admin/objects/"+bucket+"/"+key, nil))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/objects/%s/%s = %d: %s", bucket, key, resp.StatusCode, body)
	}
	var out objectLayout
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding layout: %v\n%s", err, body)
	}
	return out
}

// availableNodes returns the distinct nodes holding an AVAILABLE replica of
// the given chunk.
func (l objectLayout) availableNodes(chunkIndex int) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range l.Chunks {
		if c.Index != int32(chunkIndex) {
			continue
		}
		for _, r := range c.Replicas {
			if r.State == "AVAILABLE" && !seen[r.NodeID] {
				seen[r.NodeID] = true
				out = append(out, r.NodeID)
			}
		}
	}
	return out
}

// ---- compose control -----------------------------------------------------

// compose runs a docker compose subcommand against the development cluster.
// It is used to genuinely stop and start containers, because "restart the
// coordinator and check the data survives" cannot be simulated in-process.
//
// The compose file and project directory are passed explicitly as absolute
// paths rather than relying on Compose's upward search from a working
// directory: the suite runs both natively and inside a container, and the
// implicit search resolves differently in each.
func compose(t *testing.T, args ...string) string {
	t.Helper()
	root := repoRoot(t)
	full := append([]string{
		"compose",
		"-f", filepath.Join(root, "docker-compose.yml"),
		"--project-directory", root,
	}, args...)

	cmd := exec.Command("docker", full...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

// composeOK runs a compose command and reports whether it succeeded, for
// probes where a non-zero exit is information rather than a failure.
func composeOK(t *testing.T, args ...string) bool {
	t.Helper()
	root := repoRoot(t)
	full := append([]string{
		"compose",
		"-f", filepath.Join(root, "docker-compose.yml"),
		"--project-directory", root,
	}, args...)

	cmd := exec.Command("docker", full...)
	cmd.Dir = root
	return cmd.Run() == nil
}

// startNode starts one storage node without touching its dependencies.
//
// `docker compose start <svc>` evaluates depends_on, and every storage node
// depends on `coordinator: service_healthy`. Several scenarios restart the
// coordinator, so a plain `start` can fail with "dependency failed to start"
// purely because the control plane happens to be restarting at that instant --
// which has nothing to do with the node being restarted. `up -d --no-deps`
// starts exactly the requested service.
func startNode(t *testing.T, nodeID string) {
	t.Helper()
	compose(t, "up", "-d", "--no-deps", nodeID)
}

// repoRootOnce caches the discovered repository root; every failure test needs
// it and shelling out to git per call is pure overhead.
var (
	repoRootOnce sync.Once
	repoRootPath string
	repoRootErr  error
)

func repoRoot(t *testing.T) string {
	t.Helper()
	repoRootOnce.Do(func() {
		// An explicit override wins, so the suite can run against a checkout
		// mounted at a different path than the one git reports.
		if v := os.Getenv("FLEXSTORE_REPO_ROOT"); v != "" {
			repoRootPath, repoRootErr = filepath.Abs(v)
			return
		}
		if out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
			candidate := strings.TrimSpace(string(out))
			if _, statErr := os.Stat(filepath.Join(candidate, "docker-compose.yml")); statErr == nil {
				repoRootPath = candidate
				return
			}
		}
		// Fall back to walking up from the test's own directory.
		dir, err := os.Getwd()
		if err != nil {
			repoRootErr = err
			return
		}
		for {
			if _, statErr := os.Stat(filepath.Join(dir, "docker-compose.yml")); statErr == nil {
				repoRootPath = dir
				return
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				repoRootErr = fmt.Errorf("docker-compose.yml not found above %s", dir)
				return
			}
			dir = parent
		}
	})
	if repoRootErr != nil {
		t.Fatalf("locating the repository root: %v (set FLEXSTORE_REPO_ROOT)", repoRootErr)
	}
	return repoRootPath
}

// waitForGateway blocks until the gateway's readiness probe passes.
func waitForGateway(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, gatewayURL()+"/", nil)
		if err != nil {
			t.Fatalf("building readiness request: %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for the gateway: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}
	t.Fatalf("gateway did not become reachable at %s: %v", gatewayURL(), lastErr)
}

// waitForHealthyNodes blocks until at least n nodes report HEALTHY.
//
// If the wait stalls it starts any stopped storage containers once and keeps
// waiting. The failure-injection tests stop containers and restore them in
// t.Cleanup, but a run that is interrupted -- Ctrl-C, a CI timeout, a killed
// harness -- never reaches its cleanup and leaves the cluster degraded. Without
// this the *next* run fails on a precondition rather than on anything it
// actually tested, which is a confusing way to learn that a previous run died.
func waitForHealthyNodes(t *testing.T, ctx context.Context, n int) clusterStatus {
	t.Helper()
	deadline := time.Now().Add(180 * time.Second)
	repaired := false
	var last clusterStatus

	for time.Now().Before(deadline) {
		last = getClusterStatus(t, ctx)
		if len(last.healthyNodes()) >= n {
			return last
		}
		if !repaired && time.Now().After(deadline.Add(-140*time.Second)) {
			repaired = true
			if started := startStoppedStorageNodes(t); len(started) > 0 {
				t.Logf("started storage nodes left stopped by a previous run: %v", started)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context expired waiting for %d healthy nodes", n)
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("only %d of %d nodes became healthy: %+v", len(last.healthyNodes()), n, last.Nodes)
	return last
}

// startStoppedStorageNodes starts every storage container that is not running
// and returns their service names.
func startStoppedStorageNodes(t *testing.T) []string {
	t.Helper()
	// `ps -a --services --filter status=stopped` lists services, not containers,
	// which is exactly what `compose start` takes.
	out := compose(t, "ps", "-a", "--services", "--filter", "status=stopped")
	var started []string
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		svc := strings.TrimSpace(line)
		if !strings.HasPrefix(svc, "storage-node-") {
			continue
		}
		startNode(t, svc)
		started = append(started, svc)
	}
	return started
}

// TestMain fails fast with an actionable message when the cluster is not up,
// rather than letting every test fail with a connection refused.
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gatewayURL()+"/", nil)
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"\nFlexStore cluster is not reachable at %s.\nRun `make up` first (or set FLEXSTORE_GATEWAY_URL).\nunderlying error: %v\n\n",
			gatewayURL(), err)
		os.Exit(1)
	}
	resp.Body.Close()

	os.Exit(m.Run())
}

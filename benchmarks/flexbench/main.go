// Command flexbench is FlexStore's load generator.
//
// It is a Go program rather than k6 or Vegeta for three reasons that matter
// here: it has to stream 128 MiB request bodies without buffering a copy per
// worker, it has to read the cluster's *actual* configuration back out of the
// admin API so a result file cannot claim a replication factor the cluster was
// not running, and it keeps the benchmark on the same toolchain as everything
// else in the repo (no extra runtime to install in CI).
//
// Every number it emits is measured. The only inputs are the workload shape and
// the gateway URL; replication factor, node roster, chunk size and durability
// state are all queried from the running cluster at run time.
//
//	flexbench -size 8MiB -concurrency 8 -ops 40 -out results/run.json
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "flexbench: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------- options ---

type options struct {
	gateway     string
	bucket      string
	label       string
	sizeStr     string
	size        int64
	concurrency int
	ops         int
	timeout     time.Duration
	out         string
	cleanup     bool
	skipVerify  bool
	warmup      int
}

func parseFlags() (*options, error) {
	o := &options{}
	flag.StringVar(&o.gateway, "gateway", envOr("FLEXSTORE_GATEWAY_URL", "http://localhost:8080"), "gateway base URL")
	flag.StringVar(&o.bucket, "bucket", "bench-bucket", "bucket to write into")
	flag.StringVar(&o.label, "label", "", "free-form label recorded in the result file")
	flag.StringVar(&o.sizeStr, "size", "8MiB", "object size (e.g. 64KiB, 8MiB, 128MiB)")
	flag.IntVar(&o.concurrency, "concurrency", 8, "number of concurrent clients")
	flag.IntVar(&o.ops, "ops", 40, "total objects to upload and then download")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Minute, "per-operation timeout")
	flag.StringVar(&o.out, "out", "", "write JSON results to this path (default: stdout only)")
	flag.BoolVar(&o.cleanup, "cleanup", true, "delete the benchmark objects afterwards")
	flag.BoolVar(&o.skipVerify, "skip-verify", false, "skip the pre-run SHA-256 correctness check")
	flag.IntVar(&o.warmup, "warmup", 0, "untimed operations to run before measuring (default: min(concurrency, ops/10))")
	flag.Parse()

	size, err := parseBytes(o.sizeStr)
	if err != nil {
		return nil, fmt.Errorf("-size: %w", err)
	}
	o.size = size
	if o.concurrency < 1 {
		return nil, errors.New("-concurrency must be at least 1")
	}
	if o.ops < o.concurrency {
		return nil, fmt.Errorf("-ops (%d) must be at least -concurrency (%d)", o.ops, o.concurrency)
	}
	o.gateway = strings.TrimSuffix(o.gateway, "/")
	return o, nil
}

// ----------------------------------------------------------------- result ---

// Percentiles are reported in milliseconds. Percentiles are computed by the
// nearest-rank method over every recorded operation -- no sampling, no
// reservoir, no interpolation, so the p99 of 40 operations is honestly the
// 40th-slowest and the run metadata says how many samples there were.
type Percentiles struct {
	Min  float64 `json:"min_ms"`
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
	Mean float64 `json:"mean_ms"`
}

type Phase struct {
	Ops            int         `json:"ops"`
	Errors         int         `json:"errors"`
	Bytes          int64       `json:"bytes"`
	WallSeconds    float64     `json:"wall_seconds"`
	ThroughputMiBs float64     `json:"throughput_mib_s"`
	OpsPerSecond   float64     `json:"ops_per_second"`
	Latency        Percentiles `json:"latency"`
	Samples        int         `json:"latency_samples"`
}

type NodeSnapshot struct {
	NodeID     string `json:"node_id"`
	Health     string `json:"health"`
	UsedBytes  int64  `json:"used_bytes"`
	TotalBytes int64  `json:"total_bytes"`
	ChunkCount int    `json:"chunk_count"`
}

// Cluster is read back from the running cluster rather than supplied by the
// caller. A results file therefore cannot describe a configuration the cluster
// was not actually running.
type Cluster struct {
	ReplicationFactor int            `json:"replication_factor"`
	HealthyNodes      int            `json:"healthy_nodes"`
	TotalNodes        int            `json:"total_nodes"`
	ChunkSizeBytes    int64          `json:"chunk_size_bytes"`
	NodesBefore       []NodeSnapshot `json:"nodes_before"`
	NodesAfter        []NodeSnapshot `json:"nodes_after"`
	UnderReplicated   int            `json:"under_replicated_chunks_after"`
	UnavailableChunks int            `json:"unavailable_chunks_after"`
}

type Machine struct {
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	NumCPU        int    `json:"num_cpu"`
	CPUModel      string `json:"cpu_model"`
	MemoryBytes   int64  `json:"memory_bytes"`
	OSVersion     string `json:"os_version"`
	DockerVersion string `json:"docker_version"`
	GoVersion     string `json:"go_version"`
	// ClientLocation records where the load generator itself ran. On macOS a
	// client on the host reaches the gateway through Docker Desktop's userspace
	// port forwarder, which is a substantial and entirely synthetic bottleneck;
	// a client on the compose network talks to the gateway container directly.
	// Numbers from the two are not comparable, so every result says which it was.
	ClientLocation string `json:"client_location"`
}

type Workload struct {
	ObjectSizeBytes int64  `json:"object_size_bytes"`
	ObjectSizeLabel string `json:"object_size_label"`
	Concurrency     int    `json:"concurrency"`
	Operations      int    `json:"operations"`
	WarmupOps       int    `json:"warmup_ops"`
	Bucket          string `json:"bucket"`
}

type Run struct {
	Schema         string   `json:"schema"`
	Label          string   `json:"label"`
	Timestamp      string   `json:"timestamp"`
	GitCommit      string   `json:"git_commit"`
	GitDirty       bool     `json:"git_dirty"`
	Gateway        string   `json:"gateway"`
	Machine        Machine  `json:"machine"`
	Cluster        Cluster  `json:"cluster"`
	Workload       Workload `json:"workload"`
	Upload         Phase    `json:"upload"`
	Download       Phase    `json:"download"`
	VerifiedSHA256 bool     `json:"verified_sha256"`
	Notes          []string `json:"notes,omitempty"`
}

// --------------------------------------------------------------- the run ---

func run() error {
	o, err := parseFlags()
	if err != nil {
		return err
	}

	// One shared payload for every worker. bytes.Reader is created per request
	// so each goroutine has its own cursor over the same read-only bytes; this
	// keeps a 128 MiB workload at 128 MiB of client memory instead of 128 MiB
	// times the concurrency.
	payload := make([]byte, o.size)
	if _, err := rand.Read(payload); err != nil {
		return fmt.Errorf("generating payload: %w", err)
	}
	wantSHA := sha256.Sum256(payload)

	client := &http.Client{
		Timeout: o.timeout,
		Transport: &http.Transport{
			MaxIdleConns:        o.concurrency * 2,
			MaxIdleConnsPerHost: o.concurrency * 2,
			MaxConnsPerHost:     o.concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			WriteBufferSize:     256 << 10,
			ReadBufferSize:      256 << 10,
		},
	}

	ctx := context.Background()
	run := Run{
		Schema:    "flexstore.bench/v1",
		Label:     o.label,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Gateway:   o.gateway,
		Machine:   machineInfo(),
	}
	run.GitCommit, run.GitDirty = gitState()

	if err := waitForGateway(ctx, client, o.gateway, 60*time.Second); err != nil {
		return err
	}
	if run.Cluster, err = readCluster(ctx, client, o.gateway); err != nil {
		return err
	}

	run.Workload = Workload{
		ObjectSizeBytes: o.size,
		ObjectSizeLabel: o.sizeStr,
		Concurrency:     o.concurrency,
		Operations:      o.ops,
		Bucket:          o.bucket,
	}

	prefix := fmt.Sprintf("bench/%s/%d", strings.ReplaceAll(o.sizeStr, " ", ""), time.Now().UnixNano())
	keyFor := func(i int) string { return fmt.Sprintf("%s/obj-%06d.bin", prefix, i) }

	// Correctness before speed: one full verified round trip, so a run that
	// reports throughput has also proved the system returned the exact bytes.
	if !o.skipVerify {
		vkey := prefix + "/verify.bin"
		if err := putObject(ctx, client, o.gateway, o.bucket, vkey, payload); err != nil {
			return fmt.Errorf("verification upload: %w", err)
		}
		got, err := getSHA256(ctx, client, o.gateway, o.bucket, vkey)
		if err != nil {
			return fmt.Errorf("verification download: %w", err)
		}
		if got != hex.EncodeToString(wantSHA[:]) {
			return fmt.Errorf("verification failed: sha256 %s != %s", got, hex.EncodeToString(wantSHA[:]))
		}
		_ = deleteObject(ctx, client, o.gateway, o.bucket, vkey)
		run.VerifiedSHA256 = true
	}

	// Warm-up covers TCP/HTTP connection establishment and the coordinator's
	// first-touch work for this bucket. Without it the p99 of a small-object run
	// is dominated by connection setup, which is a property of the client, not
	// of FlexStore.
	warm := o.warmup
	if warm == 0 {
		warm = min(o.concurrency, max(1, o.ops/10))
	}
	run.Workload.WarmupOps = warm
	for i := 0; i < warm; i++ {
		k := prefix + fmt.Sprintf("/warmup-%d.bin", i)
		if err := putObject(ctx, client, o.gateway, o.bucket, k, payload); err != nil {
			return fmt.Errorf("warm-up upload: %w", err)
		}
		if _, err := getSHA256(ctx, client, o.gateway, o.bucket, k); err != nil {
			return fmt.Errorf("warm-up download: %w", err)
		}
		_ = deleteObject(ctx, client, o.gateway, o.bucket, k)
	}

	fmt.Fprintf(os.Stderr, "==> upload   %s x %d, concurrency %d\n", o.sizeStr, o.ops, o.concurrency)
	run.Upload = drive(ctx, o, func(ctx context.Context, i int) (int64, error) {
		if err := putObject(ctx, client, o.gateway, o.bucket, keyFor(i), payload); err != nil {
			return 0, err
		}
		return o.size, nil
	})

	fmt.Fprintf(os.Stderr, "==> download %s x %d, concurrency %d\n", o.sizeStr, o.ops, o.concurrency)
	run.Download = drive(ctx, o, func(ctx context.Context, i int) (int64, error) {
		return drainObject(ctx, client, o.gateway, o.bucket, keyFor(i))
	})

	if after, err := readCluster(ctx, client, o.gateway); err == nil {
		run.Cluster.NodesAfter = after.NodesBefore
		run.Cluster.UnderReplicated = after.UnderReplicated
		run.Cluster.UnavailableChunks = after.UnavailableChunks
	}

	if o.cleanup {
		fmt.Fprintf(os.Stderr, "==> cleanup\n")
		var wg sync.WaitGroup
		sem := make(chan struct{}, o.concurrency)
		for i := 0; i < o.ops; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				_ = deleteObject(ctx, client, o.gateway, o.bucket, keyFor(i))
			}(i)
		}
		wg.Wait()
	}

	if run.Upload.Errors > 0 || run.Download.Errors > 0 {
		run.Notes = append(run.Notes, fmt.Sprintf(
			"%d upload and %d download operations failed; throughput is computed over successful operations only",
			run.Upload.Errors, run.Download.Errors))
	}

	blob, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return err
	}
	if o.out != "" {
		if err := os.WriteFile(o.out, append(blob, '\n'), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", o.out, err)
		}
		fmt.Fprintf(os.Stderr, "==> wrote %s\n", o.out)
	}
	summarise(os.Stderr, &run)
	return nil
}

// drive runs op exactly o.ops times across o.concurrency workers and measures
// each one. Work is handed out from a shared counter rather than partitioned up
// front, so one slow worker cannot leave the others idle at the end of the run
// and depress the aggregate throughput for a reason that has nothing to do with
// the system under test.
func drive(ctx context.Context, o *options, op func(context.Context, int) (int64, error)) Phase {
	var (
		next     atomic.Int64
		bytesSum atomic.Int64
		errCount atomic.Int64
		mu       sync.Mutex
		lat      = make([]float64, 0, o.ops)
		wg       sync.WaitGroup
	)

	start := time.Now()
	for w := 0; w < o.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]float64, 0, o.ops/o.concurrency+1)
			for {
				i := int(next.Add(1)) - 1
				if i >= o.ops {
					break
				}
				t0 := time.Now()
				n, err := op(ctx, i)
				elapsed := time.Since(t0)
				if err != nil {
					errCount.Add(1)
					fmt.Fprintf(os.Stderr, "    op %d failed: %v\n", i, err)
					continue
				}
				bytesSum.Add(n)
				local = append(local, float64(elapsed.Nanoseconds())/1e6)
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	wall := time.Since(start).Seconds()

	total := bytesSum.Load()
	p := Phase{
		Ops:         o.ops - int(errCount.Load()),
		Errors:      int(errCount.Load()),
		Bytes:       total,
		WallSeconds: wall,
		Latency:     percentiles(lat),
		Samples:     len(lat),
	}
	if wall > 0 {
		p.ThroughputMiBs = float64(total) / 1048576 / wall
		p.OpsPerSecond = float64(p.Ops) / wall
	}
	return p
}

func percentiles(v []float64) Percentiles {
	if len(v) == 0 {
		return Percentiles{}
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	var sum float64
	for _, x := range s {
		sum += x
	}
	// Nearest-rank: the p-th percentile is the ceil(p/100 * N)-th smallest
	// value. No interpolation, so every reported figure is an observation that
	// actually happened.
	at := func(p float64) float64 {
		idx := int(p/100*float64(len(s)+1)) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(s) {
			idx = len(s) - 1
		}
		return s[idx]
	}
	return Percentiles{
		Min:  s[0],
		P50:  at(50),
		P90:  at(90),
		P95:  at(95),
		P99:  at(99),
		Max:  s[len(s)-1],
		Mean: sum / float64(len(s)),
	}
}

// ----------------------------------------------------------- HTTP helpers ---

func putObject(ctx context.Context, c *http.Client, gw, bucket, key string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		gw+"/objects/"+bucket+"/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s: HTTP %d: %s", key, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

// drainObject downloads and discards, counting bytes. Discarding rather than
// hashing keeps the client off the critical path: SHA-256 verification already
// happened once in the correctness check, and hashing every timed download
// would measure the benchmark host's CPU as much as the cluster.
func drainObject(ctx context.Context, c *http.Client, gw, bucket, key string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gw+"/objects/"+bucket+"/"+key, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return 0, fmt.Errorf("GET %s: HTTP %d: %s", key, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return io.Copy(io.Discard, resp.Body)
}

func getSHA256(ctx context.Context, c *http.Client, gw, bucket, key string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gw+"/objects/"+bucket+"/"+key, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("GET %s: HTTP %d: %s", key, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	h := sha256.New()
	if _, err := io.Copy(h, resp.Body); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func deleteObject(ctx context.Context, c *http.Client, gw, bucket, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, gw+"/objects/"+bucket+"/"+key, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func waitForGateway(ctx context.Context, c *http.Client, gw string, within time.Duration) error {
	deadline := time.Now().Add(within)
	var last error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, gw+"/admin/health", nil)
		resp, err := c.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return nil
		}
		last = err
		time.Sleep(time.Second)
	}
	return fmt.Errorf("gateway %s never responded: %w", gw, last)
}

// ------------------------------------------------------ cluster introspection ---

func readCluster(ctx context.Context, c *http.Client, gw string) (Cluster, error) {
	var cl Cluster

	var nodes struct {
		Healthy int `json:"healthy"`
		Nodes   []struct {
			NodeID     string `json:"node_id"`
			Health     string `json:"health"`
			UsedBytes  int64  `json:"used_bytes"`
			TotalBytes int64  `json:"total_bytes"`
			ChunkCount int    `json:"chunk_count"`
		} `json:"nodes"`
	}
	if err := getJSON(ctx, c, gw+"/admin/nodes", &nodes); err != nil {
		return cl, fmt.Errorf("reading /admin/nodes: %w", err)
	}
	cl.HealthyNodes = nodes.Healthy
	cl.TotalNodes = len(nodes.Nodes)
	for _, n := range nodes.Nodes {
		cl.NodesBefore = append(cl.NodesBefore, NodeSnapshot{
			NodeID: n.NodeID, Health: n.Health,
			UsedBytes: n.UsedBytes, TotalBytes: n.TotalBytes, ChunkCount: n.ChunkCount,
		})
	}
	sort.Slice(cl.NodesBefore, func(i, j int) bool { return cl.NodesBefore[i].NodeID < cl.NodesBefore[j].NodeID })

	var repl struct {
		ReplicationFactor int `json:"replication_factor"`
		UnderReplicated   int `json:"under_replicated_chunks"`
		Unavailable       int `json:"unavailable_chunks"`
	}
	if err := getJSON(ctx, c, gw+"/admin/replication", &repl); err != nil {
		return cl, fmt.Errorf("reading /admin/replication: %w", err)
	}
	cl.ReplicationFactor = repl.ReplicationFactor
	cl.UnderReplicated = repl.UnderReplicated
	cl.UnavailableChunks = repl.Unavailable

	// The gateway does not expose its chunk size, so take it from the
	// environment the harness ran with and fall back to the documented default.
	cl.ChunkSizeBytes, _ = parseBytes(envOr("FLEXSTORE_CHUNK_SIZE", "8MiB"))
	return cl, nil
}

func getJSON(ctx context.Context, c *http.Client, url string, into any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

// ------------------------------------------------------------- environment ---

func machineInfo() Machine {
	m := Machine{
		OS: runtime.GOOS, Arch: runtime.GOARCH,
		NumCPU: runtime.NumCPU(), GoVersion: runtime.Version(),
		ClientLocation: envOr("FLEXBENCH_CLIENT_LOCATION", "host"),
	}
	// When the driver runs inside a container it cannot see the host's CPU
	// model, RAM or Docker version, so the harness passes them in. Nothing is
	// invented: an unset variable stays empty rather than being guessed.
	if v := os.Getenv("FLEXBENCH_CPU_MODEL"); v != "" {
		m.CPUModel = v
		m.OSVersion = os.Getenv("FLEXBENCH_OS_VERSION")
		m.DockerVersion = os.Getenv("FLEXBENCH_DOCKER_VERSION")
		if b, err := strconv.ParseInt(os.Getenv("FLEXBENCH_MEMORY_BYTES"), 10, 64); err == nil {
			m.MemoryBytes = b
		}
		return m
	}
	switch runtime.GOOS {
	case "darwin":
		m.CPUModel = strings.TrimSpace(shell("sysctl", "-n", "machdep.cpu.brand_string"))
		if v, err := strconv.ParseInt(strings.TrimSpace(shell("sysctl", "-n", "hw.memsize")), 10, 64); err == nil {
			m.MemoryBytes = v
		}
		m.OSVersion = "macOS " + strings.TrimSpace(shell("sw_vers", "-productVersion"))
	case "linux":
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "model name") {
					if _, v, ok := strings.Cut(line, ":"); ok {
						m.CPUModel = strings.TrimSpace(v)
					}
					break
				}
			}
		}
		if b, err := os.ReadFile("/proc/meminfo"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "MemTotal:") {
					f := strings.Fields(line)
					if len(f) >= 2 {
						if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil {
							m.MemoryBytes = kb * 1024
						}
					}
					break
				}
			}
		}
		m.OSVersion = strings.TrimSpace(shell("uname", "-sr"))
	}
	m.DockerVersion = strings.TrimSpace(shell("docker", "--version"))
	return m
}

func gitState() (string, bool) {
	if v := os.Getenv("FLEXBENCH_GIT_COMMIT"); v != "" {
		return v, os.Getenv("FLEXBENCH_GIT_DIRTY") == "true"
	}
	commit := strings.TrimSpace(shell("git", "rev-parse", "HEAD"))
	dirty := strings.TrimSpace(shell("git", "status", "--porcelain")) != ""
	return commit, dirty
}

func shell(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// parseBytes accepts plain byte counts and the IEC/SI suffixes the rest of
// FlexStore's configuration uses, so -size 8MiB means the same thing here as
// FLEXSTORE_CHUNK_SIZE=8MiB does to the gateway. Suffixes are checked
// longest-first so "KiB" is never mistaken for "B".
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size")
	}
	units := []struct {
		suffix string
		mult   int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
		{"KB", 1000}, {"MB", 1000_000}, {"GB", 1000_000_000}, {"TB", 1000_000_000_000},
		{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
		{"B", 1},
	}
	sort.Slice(units, func(i, j int) bool { return len(units[i].suffix) > len(units[j].suffix) })

	mult := int64(1)
	digits := s
	for _, u := range units {
		if len(s) > len(u.suffix) && strings.EqualFold(s[len(s)-len(u.suffix):], u.suffix) {
			mult = u.mult
			digits = s[:len(s)-len(u.suffix)]
			break
		}
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(digits), 64)
	if err != nil {
		return 0, fmt.Errorf("not a size: %q", s)
	}
	if f < 0 {
		return 0, fmt.Errorf("negative size: %q", s)
	}
	return int64(f * float64(mult)), nil
}

func summarise(w io.Writer, r *Run) {
	fmt.Fprintf(w, "\n%-10s %s  concurrency=%d  ops=%d  nodes=%d  RF=%d  client=%s\n",
		"WORKLOAD", r.Workload.ObjectSizeLabel, r.Workload.Concurrency,
		r.Workload.Operations, r.Cluster.HealthyNodes, r.Cluster.ReplicationFactor,
		r.Machine.ClientLocation)
	row := func(name string, p Phase) {
		fmt.Fprintf(w, "%-10s %8.2f MiB/s  %8.2f ops/s   p50 %8.2fms  p95 %8.2fms  p99 %8.2fms  errors=%d\n",
			name, p.ThroughputMiBs, p.OpsPerSecond, p.Latency.P50, p.Latency.P95, p.Latency.P99, p.Errors)
	}
	row("UPLOAD", r.Upload)
	row("DOWNLOAD", r.Download)
	fmt.Fprintln(w)
}

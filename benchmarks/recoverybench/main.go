// Command recoverybench measures FlexStore's flagship behaviour: how long the
// cluster takes to notice a dead storage node, and how long it then takes to
// rebuild every replica that node was holding -- while a client keeps reading.
//
// It produces a time series rather than a summary, because the summary is not
// the interesting part. The shape of "healthy -> node dies -> under-replication
// spikes -> repair drains it -> zero" is the claim; a single number is just a
// footnote on it.
//
// Everything is measured from the live cluster:
//
//	under-replication and repair progress   GET /admin/replication
//	node health                             GET /admin/nodes
//	repair volume                           coordinator /metrics
//	read availability                       a real GET, in a separate goroutine
//
// The read prober runs continuously and independently of the poller, so an
// unavailable read is recorded at the moment it happens rather than being
// sampled at whatever instant the poller woke up.
//
//	recoverybench -size 512MiB -trials 3 -out results/recovery.json
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
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "recoverybench: %v\n", err)
		os.Exit(1)
	}
}

// ------------------------------------------------------------------ types ---

// Sample is one observation of cluster state. Seconds is measured from the
// instant the storage-node container was stopped, so t=0 is the failure itself
// rather than the moment the cluster noticed it.
type Sample struct {
	Seconds         float64 `json:"t_seconds"`
	UnderReplicated int     `json:"under_replicated_chunks"`
	Unavailable     int     `json:"unavailable_chunks"`
	RepairsPending  int     `json:"repair_jobs_pending"`
	RepairsRunning  int     `json:"repair_jobs_running"`
	RepairsDone     int     `json:"repair_jobs_succeeded"`
	RepairsTotal    float64 `json:"repairs_total"`
	RepairBytes     float64 `json:"repair_bytes_total"`
	VictimHealth    string  `json:"victim_health"`
	HealthyNodes    int     `json:"healthy_nodes"`
}

// Probe is one client read attempted during the outage.
type Probe struct {
	Seconds   float64 `json:"t_seconds"`
	OK        bool    `json:"ok"`
	LatencyMs float64 `json:"latency_ms"`
	Status    int     `json:"status,omitempty"`
	Error     string  `json:"error,omitempty"`
}

type Trial struct {
	Trial  int    `json:"trial"`
	Victim string `json:"victim_node"`

	ObjectBytes  int64 `json:"object_bytes"`
	ObjectChunks int   `json:"object_chunks"`
	// ChunksOnVictim is how many of this object's chunks had a replica on the
	// node that was stopped -- the blast radius of the failure.
	ChunksOnVictim int `json:"object_chunks_on_victim"`

	DetectionSeconds float64 `json:"detection_seconds"`
	// RepairStartSeconds is when the first repair job was observed running or
	// completed, measured from the failure.
	RepairStartSeconds float64 `json:"repair_start_seconds"`
	// RecoverySeconds is when under-replication returned to zero, measured from
	// the failure. RepairSeconds excludes detection: it is the time from the
	// node being declared DEAD to full durability, which is the part the repair
	// subsystem is actually responsible for.
	RecoverySeconds float64 `json:"recovery_seconds"`
	RepairSeconds   float64 `json:"repair_seconds"`

	PeakUnderReplicated int     `json:"peak_under_replicated_chunks"`
	ChunksRepaired      int     `json:"chunks_repaired"`
	RepairBytes         float64 `json:"repair_bytes"`
	RepairMiBPerSecond  float64 `json:"repair_mib_per_second"`

	Reads          int     `json:"read_probes"`
	ReadFailures   int     `json:"read_probe_failures"`
	ReadAvailPct   float64 `json:"read_availability_pct"`
	ReadP50Ms      float64 `json:"read_p50_ms"`
	ReadP95Ms      float64 `json:"read_p95_ms"`
	ReadMaxMs      float64 `json:"read_max_ms"`
	ChecksumMatch  bool    `json:"checksum_matches_after_recovery"`
	UnavailableMax int     `json:"max_unavailable_chunks"`

	Samples []Sample `json:"samples"`
	Probes  []Probe  `json:"probes"`
}

type Result struct {
	Schema            string   `json:"schema"`
	Timestamp         string   `json:"timestamp"`
	GitCommit         string   `json:"git_commit"`
	GitDirty          bool     `json:"git_dirty"`
	Machine           machine  `json:"machine"`
	ReplicationFactor int      `json:"replication_factor"`
	NodeCount         int      `json:"node_count"`
	SuspectTimeout    string   `json:"suspect_timeout"`
	DeadTimeout       string   `json:"dead_timeout"`
	RepairScanEvery   string   `json:"repair_scan_interval"`
	RepairWorkers     string   `json:"repair_workers"`
	Trials            []Trial  `json:"trials"`
	Notes             []string `json:"notes,omitempty"`
}

type machine struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	NumCPU   int    `json:"num_cpu"`
	CPUModel string `json:"cpu_model"`
	Docker   string `json:"docker_version"`
}

// ------------------------------------------------------------------- main ---

type opts struct {
	gateway      string
	coordMetrics string
	bucket       string
	sizeStr      string
	size         int64
	trials       int
	pollEvery    time.Duration
	out          string
	settle       time.Duration
	maxWait      time.Duration
}

func run() error {
	o := &opts{}
	flag.StringVar(&o.gateway, "gateway", envOr("FLEXSTORE_GATEWAY_URL", "http://localhost:8080"), "gateway base URL")
	flag.StringVar(&o.coordMetrics, "coordinator-metrics", "", "coordinator metrics URL (default: gateway host with port 9102)")
	flag.StringVar(&o.bucket, "bucket", "recovery-bench", "bucket to write into")
	flag.StringVar(&o.sizeStr, "size", "512MiB", "size of the object whose replicas must be rebuilt")
	flag.IntVar(&o.trials, "trials", 3, "how many independent kill-and-recover trials to run")
	flag.DurationVar(&o.pollEvery, "poll", 250*time.Millisecond, "cluster state sampling interval")
	flag.StringVar(&o.out, "out", "", "write JSON results here")
	flag.DurationVar(&o.settle, "settle", 3*time.Second, "how long under-replication must stay at zero before declaring recovery complete")
	flag.DurationVar(&o.maxWait, "max-wait", 15*time.Minute, "give up on a trial after this long")
	flag.Parse()

	size, err := parseBytes(o.sizeStr)
	if err != nil {
		return fmt.Errorf("-size: %w", err)
	}
	o.size = size
	o.gateway = strings.TrimSuffix(o.gateway, "/")
	if o.coordMetrics == "" {
		o.coordMetrics = strings.Replace(o.gateway, ":8080", ":9102", 1)
	}

	ctx := context.Background()
	c := &http.Client{Timeout: 10 * time.Minute}

	res := Result{
		Schema:          "flexstore.recovery/v1",
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
		Machine:         machineInfo(),
		SuspectTimeout:  envOr("FLEXSTORE_SUSPECT_TIMEOUT", "15s"),
		DeadTimeout:     envOr("FLEXSTORE_DEAD_TIMEOUT", "45s"),
		RepairScanEvery: envOr("FLEXSTORE_REPAIR_SCAN_INTERVAL", "5s"),
		RepairWorkers:   envOr("FLEXSTORE_REPAIR_WORKERS", "4"),
	}
	res.GitCommit, res.GitDirty = gitState()

	nodes, err := readNodes(ctx, c, o.gateway)
	if err != nil {
		return err
	}
	res.NodeCount = len(nodes)
	if res.ReplicationFactor, err = readRF(ctx, c, o.gateway); err != nil {
		return err
	}
	if res.NodeCount < res.ReplicationFactor+1 {
		return fmt.Errorf("need more than RF=%d nodes to lose one and still repair; cluster has %d",
			res.ReplicationFactor, res.NodeCount)
	}
	fmt.Fprintf(os.Stderr, "cluster: %d nodes, RF=%d\n", res.NodeCount, res.ReplicationFactor)

	for t := 1; t <= o.trials; t++ {
		fmt.Fprintf(os.Stderr, "\n===== trial %d/%d =====\n", t, o.trials)
		trial, err := runTrial(ctx, c, o, t)
		if err != nil {
			return fmt.Errorf("trial %d: %w", t, err)
		}
		res.Trials = append(res.Trials, *trial)
		summariseTrial(os.Stderr, trial)
	}

	if o.out != "" {
		if err := os.MkdirAll(filepath.Dir(o.out), 0o755); err != nil {
			return err
		}
		blob, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(o.out, append(blob, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "\nwrote %s\n", o.out)
	}
	return nil
}

// ------------------------------------------------------------------ trial ---

func runTrial(ctx context.Context, c *http.Client, o *opts, n int) (*Trial, error) {
	tr := &Trial{Trial: n, ObjectBytes: o.size}

	if err := waitQuiet(ctx, c, o.gateway, 5*time.Minute); err != nil {
		return nil, err
	}

	// The object under test, and a small companion the prober reads. The
	// companion exists because reading a 512 MiB object takes seconds, and a
	// probe that slow cannot resolve a brief unavailability window.
	key := fmt.Sprintf("trial-%d/payload-%d.bin", n, time.Now().UnixNano())
	probeKey := fmt.Sprintf("trial-%d/probe-%d.bin", n, time.Now().UnixNano())

	fmt.Fprintf(os.Stderr, "uploading %s\n", o.sizeStr)
	payload := make([]byte, o.size)
	if _, err := rand.Read(payload); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(payload)
	wantSHA := hex.EncodeToString(sum[:])
	if err := put(ctx, c, o.gateway, o.bucket, key, payload); err != nil {
		return nil, fmt.Errorf("uploading payload: %w", err)
	}
	// The payload is deliberately dropped before the measurement phase: at
	// 512 MiB it would otherwise stay resident for the whole trial, competing
	// for memory with the cluster it is meant to be observing.
	//nolint:wastedassign // the assignment is the point: it frees the buffer.
	payload = nil
	_ = payload

	probePayload := make([]byte, 4<<20)
	if _, err := rand.Read(probePayload); err != nil {
		return nil, err
	}
	if err := put(ctx, c, o.gateway, o.bucket, probeKey, probePayload); err != nil {
		return nil, fmt.Errorf("uploading probe object: %w", err)
	}

	if err := waitQuiet(ctx, c, o.gateway, 5*time.Minute); err != nil {
		return nil, err
	}

	// Kill the node holding the most replicas of the object, so the failure has
	// the largest blast radius the placement happened to allow.
	layout, err := readLayout(ctx, c, o.gateway, o.bucket, key)
	if err != nil {
		return nil, err
	}
	tr.ObjectChunks = len(layout.Chunks)
	victim, onVictim := busiestNode(layout)
	tr.Victim, tr.ChunksOnVictim = victim, onVictim
	fmt.Fprintf(os.Stderr, "object spans %d chunks; stopping %s which holds %d of them\n",
		tr.ObjectChunks, victim, onVictim)

	base, err := poll(ctx, c, o, victim, time.Time{})
	if err != nil {
		return nil, err
	}

	probeCtx, stopProbe := context.WithCancel(ctx)
	var probeWG sync.WaitGroup
	var probeMu sync.Mutex
	probes := []Probe{}

	if err := composeCmd(ctx, "stop", victim); err != nil {
		stopProbe()
		return nil, fmt.Errorf("stopping %s: %w", victim, err)
	}
	t0 := time.Now()

	probeWG.Add(1)
	go func() {
		defer probeWG.Done()
		pc := &http.Client{Timeout: 30 * time.Second}
		for probeCtx.Err() == nil {
			started := time.Now()
			status, err := probeGet(probeCtx, pc, o.gateway, o.bucket, probeKey)
			if probeCtx.Err() != nil {
				return
			}
			p := Probe{
				Seconds:   started.Sub(t0).Seconds(),
				OK:        err == nil && status == http.StatusOK,
				LatencyMs: float64(time.Since(started).Nanoseconds()) / 1e6,
				Status:    status,
			}
			if err != nil {
				p.Error = err.Error()
			}
			probeMu.Lock()
			probes = append(probes, p)
			probeMu.Unlock()
			time.Sleep(200 * time.Millisecond)
		}
	}()

	var (
		samples    []Sample
		zeroSince  time.Time
		sawSpike   bool
		detectedAt = -1.0
		repairAt   = -1.0
	)
	deadline := time.Now().Add(o.maxWait)
	ticker := time.NewTicker(o.pollEvery)
	defer ticker.Stop()

	for {
		s, err := poll(ctx, c, o, victim, t0)
		if err == nil {
			samples = append(samples, s)
			if detectedAt < 0 && s.VictimHealth == "DEAD" {
				detectedAt = s.Seconds
				fmt.Fprintf(os.Stderr, "  t+%5.1fs  %s declared DEAD\n", s.Seconds, victim)
			}
			if s.UnderReplicated > 0 {
				sawSpike = true
				if s.UnderReplicated > tr.PeakUnderReplicated {
					tr.PeakUnderReplicated = s.UnderReplicated
				}
			}
			if s.Unavailable > tr.UnavailableMax {
				tr.UnavailableMax = s.Unavailable
			}
			if repairAt < 0 && (s.RepairsRunning > 0 || s.RepairsTotal > base.RepairsTotal) {
				repairAt = s.Seconds
				fmt.Fprintf(os.Stderr, "  t+%5.1fs  repair started\n", s.Seconds)
			}
			// Recovery counts only after we have actually seen under-replication:
			// otherwise a poll taken before the coordinator noticed would look
			// identical to a fully recovered cluster.
			if sawSpike && s.UnderReplicated == 0 && s.RepairsPending == 0 && s.RepairsRunning == 0 {
				if zeroSince.IsZero() {
					zeroSince = time.Now()
				} else if time.Since(zeroSince) >= o.settle {
					tr.RecoverySeconds = s.Seconds
					fmt.Fprintf(os.Stderr, "  t+%5.1fs  durability fully restored\n", s.Seconds)
					tr.RepairBytes = s.RepairBytes - base.RepairBytes
					tr.ChunksRepaired = int(s.RepairsTotal - base.RepairsTotal)
					break
				}
			} else {
				zeroSince = time.Time{}
			}
		}
		if time.Now().After(deadline) {
			stopProbe()
			probeWG.Wait()
			_ = composeCmd(ctx, "up", "-d", "--no-deps", victim)
			return nil, fmt.Errorf("durability was not restored within %s", o.maxWait)
		}
		<-ticker.C
	}

	stopProbe()
	probeWG.Wait()

	tr.Samples = samples
	tr.Probes = probes
	tr.DetectionSeconds = detectedAt
	tr.RepairStartSeconds = repairAt
	if detectedAt >= 0 {
		tr.RepairSeconds = tr.RecoverySeconds - detectedAt
	}
	if tr.RepairSeconds > 0 {
		tr.RepairMiBPerSecond = tr.RepairBytes / 1048576 / tr.RepairSeconds
	}

	tr.Reads = len(probes)
	var lat []float64
	for _, p := range probes {
		if !p.OK {
			tr.ReadFailures++
		}
		lat = append(lat, p.LatencyMs)
	}
	if tr.Reads > 0 {
		tr.ReadAvailPct = 100 * float64(tr.Reads-tr.ReadFailures) / float64(tr.Reads)
	}
	sort.Float64s(lat)
	if len(lat) > 0 {
		tr.ReadP50Ms = lat[len(lat)*50/100]
		tr.ReadP95Ms = lat[min(len(lat)*95/100, len(lat)-1)]
		tr.ReadMaxMs = lat[len(lat)-1]
	}

	// The object must still be byte-identical after the cluster rebuilt a third
	// of its replicas. A recovery that restores the *count* but not the *bytes*
	// is not a recovery.
	got, err := getSHA(ctx, c, o.gateway, o.bucket, key)
	if err != nil {
		return nil, fmt.Errorf("verifying object after recovery: %w", err)
	}
	tr.ChecksumMatch = got == wantSHA
	if !tr.ChecksumMatch {
		return nil, fmt.Errorf("data corrupted by recovery: sha256 %s != %s", got, wantSHA)
	}

	fmt.Fprintf(os.Stderr, "restarting %s and waiting for reconciliation\n", victim)
	if err := composeCmd(ctx, "up", "-d", "--no-deps", victim); err != nil {
		return nil, err
	}
	if err := waitQuiet(ctx, c, o.gateway, 5*time.Minute); err != nil {
		return nil, err
	}
	_ = del(ctx, c, o.gateway, o.bucket, key)
	_ = del(ctx, c, o.gateway, o.bucket, probeKey)
	if err := waitQuiet(ctx, c, o.gateway, 5*time.Minute); err != nil {
		return nil, err
	}
	return tr, nil
}

func summariseTrial(w io.Writer, t *Trial) {
	fmt.Fprintf(w, `
  victim                  %s (%d of %d chunks)
  detection               %.1fs
  repair began            %.1fs after failure
  full durability         %.1fs after failure  (%.1fs after detection)
  chunks repaired         %d  (peak under-replicated %d)
  repair throughput       %.1f MiB/s  (%.0f MiB)
  read availability       %.2f%% over %d probes  (p50 %.0fms, p95 %.0fms, max %.0fms)
  unavailable chunks      %d (max observed)
  checksum after recovery %v
`, t.Victim, t.ChunksOnVictim, t.ObjectChunks,
		t.DetectionSeconds, t.RepairStartSeconds, t.RecoverySeconds, t.RepairSeconds,
		t.ChunksRepaired, t.PeakUnderReplicated,
		t.RepairMiBPerSecond, t.RepairBytes/1048576,
		t.ReadAvailPct, t.Reads, t.ReadP50Ms, t.ReadP95Ms, t.ReadMaxMs,
		t.UnavailableMax, t.ChecksumMatch)
}

// ------------------------------------------------------------ cluster I/O ---

func poll(ctx context.Context, c *http.Client, o *opts, victim string, t0 time.Time) (Sample, error) {
	var s Sample
	if !t0.IsZero() {
		s.Seconds = time.Since(t0).Seconds()
	}

	var repl struct {
		Under       int `json:"under_replicated_chunks"`
		Unavailable int `json:"unavailable_chunks"`
		Pending     int `json:"repair_jobs_pending"`
		Running     int `json:"repair_jobs_running"`
		Succeeded   int `json:"repair_jobs_succeeded"`
	}
	if err := getJSON(ctx, c, o.gateway+"/admin/replication", &repl); err != nil {
		return s, err
	}
	s.UnderReplicated, s.Unavailable = repl.Under, repl.Unavailable
	s.RepairsPending, s.RepairsRunning, s.RepairsDone = repl.Pending, repl.Running, repl.Succeeded

	nodes, err := readNodes(ctx, c, o.gateway)
	if err != nil {
		return s, err
	}
	for _, n := range nodes {
		if n.NodeID == victim {
			s.VictimHealth = n.Health
		}
		if n.Health == "HEALTHY" {
			s.HealthyNodes++
		}
	}

	m := scrape(ctx, c, o.coordMetrics+"/metrics")
	s.RepairsTotal = m["flexstore_repairs_total"]
	s.RepairBytes = m["flexstore_repair_bytes_total"]
	return s, nil
}

type nodeInfo struct {
	NodeID     string `json:"node_id"`
	Health     string `json:"health"`
	ChunkCount int    `json:"chunk_count"`
}

func readNodes(ctx context.Context, c *http.Client, gw string) ([]nodeInfo, error) {
	var out struct {
		Nodes []nodeInfo `json:"nodes"`
	}
	if err := getJSON(ctx, c, gw+"/admin/nodes", &out); err != nil {
		return nil, fmt.Errorf("reading /admin/nodes: %w", err)
	}
	return out.Nodes, nil
}

func readRF(ctx context.Context, c *http.Client, gw string) (int, error) {
	var out struct {
		RF int `json:"replication_factor"`
	}
	err := getJSON(ctx, c, gw+"/admin/replication", &out)
	return out.RF, err
}

type layout struct {
	Chunks []struct {
		ChunkID  string `json:"chunk_id"`
		Replicas []struct {
			NodeID string `json:"node_id"`
			State  string `json:"state"`
		} `json:"replicas"`
	} `json:"chunks"`
}

func readLayout(ctx context.Context, c *http.Client, gw, bucket, key string) (layout, error) {
	var l layout
	err := getJSON(ctx, c, gw+"/admin/objects/"+bucket+"/"+key, &l)
	return l, err
}

func busiestNode(l layout) (string, int) {
	count := map[string]int{}
	for _, ch := range l.Chunks {
		for _, r := range ch.Replicas {
			if r.State == "AVAILABLE" {
				count[r.NodeID]++
			}
		}
	}
	best, n := "", -1
	for id, k := range count {
		// Ties broken by node ID so a repeated trial on an identical layout
		// picks the same victim and the trials stay comparable.
		if k > n || (k == n && id < best) {
			best, n = id, k
		}
	}
	return best, n
}

func waitQuiet(ctx context.Context, c *http.Client, gw string, within time.Duration) error {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		var repl struct {
			Under   int `json:"under_replicated_chunks"`
			Pending int `json:"repair_jobs_pending"`
			Running int `json:"repair_jobs_running"`
		}
		if err := getJSON(ctx, c, gw+"/admin/replication", &repl); err == nil {
			if repl.Under == 0 && repl.Pending == 0 && repl.Running == 0 {
				return nil
			}
		}
		time.Sleep(time.Second)
	}
	return errors.New("cluster did not become quiet in time")
}

// scrape pulls the numeric value of every metric family from a Prometheus
// exposition endpoint, summing across label sets.
func scrape(ctx context.Context, c *http.Client, url string) map[string]float64 {
	out := map[string]float64{}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return out
	}
	resp, err := c.Do(req)
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(body), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := fields[0]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		if v, err := strconv.ParseFloat(fields[1], 64); err == nil {
			out[name] += v
		}
	}
	return out
}

// ------------------------------------------------------------ HTTP helpers ---

func put(ctx context.Context, c *http.Client, gw, bucket, key string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		gw+"/objects/"+bucket+"/"+key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.ContentLength = int64(len(body))
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return nil
}

func probeGet(ctx context.Context, c *http.Client, gw, bucket, key string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gw+"/objects/"+bucket+"/"+key, nil)
	if err != nil {
		return 0, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	// The body must be fully drained: FlexStore verifies each chunk before
	// emitting it and aborts the connection on a mismatch, so a truncated read
	// is a failed read even though the status line said 200.
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func getSHA(ctx context.Context, c *http.Client, gw, bucket, key string) (string, error) {
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
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	h := sha256.New()
	if _, err := io.Copy(h, resp.Body); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func del(ctx context.Context, c *http.Client, gw, bucket, key string) error {
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
	// /admin/health answers 503 when data is unreadable; that is a state worth
	// recording, not a transport failure.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
		return fmt.Errorf("GET %s: HTTP %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(into)
}

// ----------------------------------------------------------------- system ---

func composeCmd(ctx context.Context, args ...string) error {
	root, err := repoRoot(ctx)
	if err != nil {
		return err
	}
	// Generous bound: `compose up` pulls nothing here (images exist), but a
	// wedged Docker daemon should fail the trial rather than hang it forever.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	full := append([]string{"compose", "--project-directory", root}, args...)
	cmd := exec.CommandContext(ctx, "docker", full...)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", strings.Join(full, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func repoRoot(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("locating repository root: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

func machineInfo() machine {
	m := machine{OS: runtime.GOOS, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU()}
	m.CPUModel = strings.TrimSpace(shell("sysctl", "-n", "machdep.cpu.brand_string"))
	if m.CPUModel == "" {
		m.CPUModel = strings.TrimSpace(shell("uname", "-p"))
	}
	m.Docker = strings.TrimSpace(shell("docker", "--version"))
	return m
}

func gitState() (string, bool) {
	return strings.TrimSpace(shell("git", "rev-parse", "HEAD")),
		strings.TrimSpace(shell("git", "status", "--porcelain")) != ""
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

func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30},
		{"KB", 1000}, {"MB", 1000_000}, {"GB", 1000_000_000},
		{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"B", 1},
	}
	sort.Slice(units, func(i, j int) bool { return len(units[i].suffix) > len(units[j].suffix) })
	mult, digits := int64(1), s
	for _, u := range units {
		if len(s) > len(u.suffix) && strings.EqualFold(s[len(s)-len(u.suffix):], u.suffix) {
			mult, digits = u.mult, s[:len(s)-len(u.suffix)]
			break
		}
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(digits), 64)
	if err != nil {
		return 0, fmt.Errorf("not a size: %q", s)
	}
	return int64(f * float64(mult)), nil
}

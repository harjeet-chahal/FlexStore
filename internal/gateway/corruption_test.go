package gateway

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
)

// recordingReporter captures corruption reports so tests can assert that
// detection actually reached the control plane.
type recordingReporter struct {
	mu      sync.Mutex
	reports []report
	err     error
}

type report struct{ chunkID, nodeID, detail string }

func (r *recordingReporter) ReportCorruptReplica(_ context.Context, chunkID, nodeID, detail string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report{chunkID, nodeID, detail})
	return r.err
}

func (r *recordingReporter) snapshot() []report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]report(nil), r.reports...)
}

func TestCorruptReplicaIsReportedForRepair(t *testing.T) {
	// Failing over is only half of self-healing. Without a report the bad copy
	// stays listed as a valid replica and every future read pays the same
	// failover cost -- forever.
	nodes := startNodes(t, 3)
	payload := []byte("the bytes that should come back")
	placement := storeOn(t, nodes, payload)

	nodes[0].corrupt(t, placement.ChunkId)

	rep := &recordingReporter{}
	reader := newReader(t)
	reader.SetCorruptionReporter(rep)

	got, err := reader.Fetch(context.Background(), placement, nil)
	if err != nil {
		t.Fatalf("Fetch should have failed over: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("corrupt bytes were served")
	}

	reports := rep.snapshot()
	if len(reports) != 1 {
		t.Fatalf("got %d corruption reports, want exactly 1: %+v", len(reports), reports)
	}
	if reports[0].chunkID != placement.ChunkId {
		t.Errorf("reported chunk %s, want %s", reports[0].chunkID, placement.ChunkId)
	}
	if reports[0].nodeID != nodes[0].id {
		t.Errorf("reported node %s, want the corrupt one %s", reports[0].nodeID, nodes[0].id)
	}
	if reports[0].detail == "" {
		t.Error("report carries no detail; an operator would have nothing to go on")
	}
}

func TestOnlyCorruptionIsReportedNotUnreachability(t *testing.T) {
	// A node being down is not a corruption event. Reporting it would demote a
	// perfectly good replica and trigger a pointless copy.
	nodes := startNodes(t, 3)
	payload := []byte("node is merely offline")
	placement := storeOn(t, nodes, payload)

	nodes[0].kill()

	rep := &recordingReporter{}
	reader := newReader(t)
	reader.SetCorruptionReporter(rep)

	if _, err := reader.Fetch(context.Background(), placement, nil); err != nil {
		t.Fatalf("Fetch should have failed over past the dead node: %v", err)
	}
	if got := rep.snapshot(); len(got) != 0 {
		t.Fatalf("an unreachable node was reported as corrupt: %+v", got)
	}
}

func TestEveryCorruptReplicaIsReported(t *testing.T) {
	// When all copies are bad the read must fail, but each bad replica should
	// still be reported so repair can act once a good copy reappears.
	nodes := startNodes(t, 3)
	payload := []byte("all copies rotted")
	placement := storeOn(t, nodes, payload)
	for _, n := range nodes {
		n.corrupt(t, placement.ChunkId)
	}

	rep := &recordingReporter{}
	reader := newReader(t)
	reader.SetCorruptionReporter(rep)

	if _, err := reader.Fetch(context.Background(), placement, nil); !errors.Is(err, ErrNoReplica) {
		t.Fatalf("expected ErrNoReplica, got %v", err)
	}
	if got := rep.snapshot(); len(got) != 3 {
		t.Fatalf("got %d reports for 3 corrupt replicas: %+v", len(got), got)
	}
}

func TestReadStillSucceedsWhenReportingFails(t *testing.T) {
	// The report is best-effort with respect to the client's request: a
	// coordinator blip must not turn a recoverable read into a failure.
	nodes := startNodes(t, 3)
	payload := []byte("serve me anyway")
	placement := storeOn(t, nodes, payload)
	nodes[0].corrupt(t, placement.ChunkId)

	rep := &recordingReporter{err: errors.New("coordinator unavailable")}
	reader := newReader(t)
	reader.SetCorruptionReporter(rep)

	got, err := reader.Fetch(context.Background(), placement, nil)
	if err != nil {
		t.Fatalf("Fetch failed because reporting failed: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload differs")
	}
}

func TestReaderWithoutAReporterStillFailsOver(t *testing.T) {
	// A nil reporter must degrade to "fail over but do not report", not panic.
	nodes := startNodes(t, 3)
	payload := []byte("no reporter configured")
	placement := storeOn(t, nodes, payload)
	nodes[0].corrupt(t, placement.ChunkId)

	got, err := newReader(t).Fetch(context.Background(), placement, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("payload differs")
	}
}

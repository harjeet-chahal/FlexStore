package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/checksum"
)

func TestWalkInventoryReportsEveryChunk(t *testing.T) {
	cs := newStore(t)

	want := map[string]int64{}
	for i := 0; i < 25; i++ {
		id := uuid.NewString()
		payload := bytes.Repeat([]byte("i"), 100+i)
		if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), checksum.Sum(payload)); err != nil {
			t.Fatalf("Write: %v", err)
		}
		want[id] = int64(len(payload))
	}

	got := map[string]int64{}
	batches := 0
	if err := cs.WalkInventory(4, func(entries []InventoryEntry) error {
		batches++
		for _, e := range entries {
			if _, dup := got[e.ChunkID]; dup {
				t.Fatalf("chunk %s reported twice", e.ChunkID)
			}
			got[e.ChunkID] = e.Size
		}
		return nil
	}); err != nil {
		t.Fatalf("WalkInventory: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("walked %d chunks, want %d", len(got), len(want))
	}
	for id, size := range want {
		if got[id] != size {
			t.Errorf("chunk %s size = %d, want %d", id, got[id], size)
		}
	}
	// Batching is the point: the reconciler must never receive one giant slice.
	if batches < 2 {
		t.Fatalf("walked in %d batches with batchSize=4 and 25 chunks", batches)
	}
}

func TestWalkInventoryIgnoresForeignFiles(t *testing.T) {
	// The reconciler deletes what the coordinator does not recognise, so the
	// node must never report a file that is not one of ours.
	cs := newStore(t)
	id := uuid.NewString()
	payload := []byte("real chunk")
	if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), checksum.Sum(payload)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	dir := filepath.Join(cs.Root(), dataDirName, "aa", "bb")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	for _, name := range []string{
		"not-a-uuid.chunk",
		"README.txt",
		"../escape.chunk",
		"8F1C9D2E-0000-4000-8000-000000000001.chunk", // uppercase
	} {
		safe := filepath.Join(dir, filepath.Base(name))
		if err := os.WriteFile(safe, []byte("junk"), 0o644); err != nil {
			t.Fatalf("writing %s: %v", safe, err)
		}
	}

	var seen []string
	if err := cs.WalkInventory(10, func(entries []InventoryEntry) error {
		for _, e := range entries {
			seen = append(seen, e.ChunkID)
		}
		return nil
	}); err != nil {
		t.Fatalf("WalkInventory: %v", err)
	}
	if len(seen) != 1 || seen[0] != id {
		t.Fatalf("inventory = %v, want exactly [%s]", seen, id)
	}
}

func TestWalkInventoryOnEmptyStore(t *testing.T) {
	cs := newStore(t)
	called := false
	if err := cs.WalkInventory(10, func([]InventoryEntry) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("WalkInventory: %v", err)
	}
	if called {
		t.Fatal("callback invoked for an empty store")
	}
}

func TestWalkInventoryPropagatesCallbackErrors(t *testing.T) {
	cs := newStore(t)
	for i := 0; i < 5; i++ {
		id := uuid.NewString()
		payload := []byte("x")
		if _, err := cs.Write(id, bytes.NewReader(payload), 1, checksum.Sum(payload)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	sentinel := errors.New("consumer went away")
	err := cs.WalkInventory(1, func([]InventoryEntry) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the callback error to propagate, got %v", err)
	}
}

func TestServiceListChunksStreamsInventory(t *testing.T) {
	n := startTestNode(t, "inventory-node")

	want := map[string]bool{}
	for i := 0; i < 12; i++ {
		id := uuid.NewString()
		payload := bytes.Repeat([]byte("s"), 64)
		n.put(t, id, payload, checksum.Sum(payload))
		want[id] = true
	}

	stream, err := n.client.ListChunks(context.Background(), &flexstorev1.ListChunksRequest{BatchSize: 5})
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	got := map[string]bool{}
	for {
		msg, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			t.Fatalf("Recv: %v", rerr)
		}
		for _, e := range msg.Chunks {
			if e.SizeBytes != 64 {
				t.Errorf("chunk %s size = %d, want 64", e.ChunkId, e.SizeBytes)
			}
			got[e.ChunkId] = true
		}
	}
	if len(got) != len(want) {
		t.Fatalf("streamed %d chunks, want %d", len(got), len(want))
	}
	for id := range want {
		if !got[id] {
			t.Errorf("chunk %s missing from the stream", id)
		}
	}
}

func TestServiceListChunksOnEmptyNode(t *testing.T) {
	n := startTestNode(t, "empty-node")
	stream, err := n.client.ListChunks(context.Background(), &flexstorev1.ListChunksRequest{})
	if err != nil {
		t.Fatalf("ListChunks: %v", err)
	}
	for {
		_, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			return
		}
		if rerr != nil {
			t.Fatalf("Recv: %v", rerr)
		}
	}
}

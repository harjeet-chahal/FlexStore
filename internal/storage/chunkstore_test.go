package storage

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/harjeetschahal/flexstore/internal/checksum"
)

func newStore(t *testing.T) *ChunkStore {
	t.Helper()
	// fsync off in tests: it is exercised by the production path and adds
	// hundreds of milliseconds per chunk on some filesystems.
	cs, err := NewChunkStore(t.TempDir(), false, 1<<30)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	return cs
}

func TestRelativePathFansOut(t *testing.T) {
	id := "8f1c9d2e-0000-4000-8000-000000000001"
	got, err := RelativePath(id)
	if err != nil {
		t.Fatalf("RelativePath: %v", err)
	}
	want := filepath.Join("8f", "1c", id+".chunk")
	if got != want {
		t.Fatalf("RelativePath = %q, want %q", got, want)
	}
}

func TestRelativePathIsDeterministic(t *testing.T) {
	id := uuid.NewString()
	a, err := RelativePath(id)
	if err != nil {
		t.Fatalf("RelativePath: %v", err)
	}
	b, _ := RelativePath(id)
	if a != b {
		t.Fatalf("path is not stable: %q vs %q", a, b)
	}
}

func TestValidateChunkIDRejectsTraversal(t *testing.T) {
	// Chunk IDs arrive over the network and become filesystem paths, so this
	// is the path-traversal guard, not a cosmetic check.
	bad := []string{
		"", "..", "../../etc/passwd",
		"8f1c9d2e-0000-4000-8000-00000000000",     // too short
		"8f1c9d2e-0000-4000-8000-0000000000012",   // too long
		"8F1C9D2E-0000-4000-8000-000000000001",    // uppercase
		"8f1c9d2e_0000-4000-8000-000000000001",    // wrong separator
		"8f1c9d2e-0000-4000-8000-00000000000g",    // non-hex
		"../8f1c9d2e-0000-4000-8000-000000000001", // prefixed traversal
	}
	for _, id := range bad {
		if err := ValidateChunkID(id); err == nil {
			t.Errorf("ValidateChunkID(%q) accepted an unsafe id", id)
		}
		if _, err := RelativePath(id); err == nil {
			t.Errorf("RelativePath(%q) accepted an unsafe id", id)
		}
	}
	if err := ValidateChunkID(uuid.NewString()); err != nil {
		t.Errorf("a real UUID was rejected: %v", err)
	}
}

func TestWriteThenRead(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := bytes.Repeat([]byte("flexstore"), 5000)
	sum := checksum.Sum(payload)

	res, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), sum)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if res.BytesWritten != int64(len(payload)) {
		t.Fatalf("BytesWritten = %d, want %d", res.BytesWritten, len(payload))
	}
	if res.Checksum != sum {
		t.Fatalf("Checksum = %s, want %s", res.Checksum, sum)
	}
	if res.AlreadyExisted {
		t.Fatal("first write should not report AlreadyExisted")
	}

	f, size, err := cs.Open(id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	if size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", size, len(payload))
	}
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("round-tripped payload differs")
	}
}

func TestWriteRejectsChecksumMismatchAndLeavesNoFile(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := []byte("real payload")
	wrongSum := checksum.Sum([]byte("something else"))

	_, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), wrongSum)
	if err == nil {
		t.Fatal("expected the write to be rejected")
	}
	if !errors.Is(err, checksum.ErrMismatch) {
		t.Fatalf("error should wrap ErrMismatch, got %v", err)
	}

	// The whole point of temp-file-then-rename: a rejected write must not be
	// visible as a chunk.
	if _, _, err := cs.Open(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a rejected chunk is readable; atomicity is broken (err=%v)", err)
	}
	assertTempDirEmpty(t, cs)
}

func TestWriteRejectsSizeMismatch(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := []byte("twelve bytes")

	// Header declares more than the stream delivers: a truncated upload.
	_, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload))+10, checksum.Sum(payload))
	if err == nil {
		t.Fatal("expected a size mismatch error")
	}
	if !strings.Contains(err.Error(), "header declared") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, _, err := cs.Open(id); !errors.Is(err, ErrNotFound) {
		t.Fatal("truncated chunk became visible")
	}
	assertTempDirEmpty(t, cs)
}

func TestWriteFailsCleanlyOnReaderError(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	boom := errors.New("network died mid-chunk")

	_, err := cs.Write(id, io.MultiReader(bytes.NewReader([]byte("partial")), errReader{boom}), -1, "")
	if !errors.Is(err, boom) {
		t.Fatalf("expected the reader error to surface, got %v", err)
	}
	if _, _, err := cs.Open(id); !errors.Is(err, ErrNotFound) {
		t.Fatal("a partially written chunk became visible")
	}
	assertTempDirEmpty(t, cs)
}

func TestWriteIsIdempotent(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := []byte("retry me")
	sum := checksum.Sum(payload)

	if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), sum); err != nil {
		t.Fatalf("first write: %v", err)
	}
	res, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), sum)
	if err != nil {
		t.Fatalf("second write: %v", err)
	}
	if !res.AlreadyExisted {
		t.Fatal("a repeated write of an identical chunk should be a no-op")
	}

	// Usage must not be double-counted, or capacity reporting drifts.
	_, used, _, chunks, _, err := cs.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if chunks != 1 {
		t.Fatalf("chunk count = %d after an idempotent rewrite, want 1", chunks)
	}
	if used != int64(len(payload)) {
		t.Fatalf("used bytes = %d, want %d", used, len(payload))
	}
}

func TestWriteRepairsACorruptExistingChunk(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := []byte("good data")
	sum := checksum.Sum(payload)

	if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), sum); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Simulate bit-rot on disk.
	path, _ := cs.PathFor(id)
	if err := os.WriteFile(path, []byte("CORRUPTED"), 0o644); err != nil {
		t.Fatalf("corrupting file: %v", err)
	}

	// A rewrite must overwrite the bad copy rather than short-circuit on it.
	res, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), sum)
	if err != nil {
		t.Fatalf("rewrite over a corrupt chunk: %v", err)
	}
	if res.AlreadyExisted {
		t.Fatal("a corrupt existing chunk must not be treated as already present")
	}
	check, err := cs.Check(id, true)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !check.Valid || check.Checksum != sum {
		t.Fatalf("chunk was not repaired: %+v", check)
	}
}

func TestCheckDetectsCorruption(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := []byte("verify me")
	sum := checksum.Sum(payload)
	if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), sum); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, err := cs.Check(id, true)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if !got.Exists || !got.Valid || got.Checksum != sum {
		t.Fatalf("healthy chunk reported as %+v", got)
	}

	path, _ := cs.PathFor(id)
	if err := os.WriteFile(path, []byte("bad!"), 0o644); err != nil {
		t.Fatalf("corrupting file: %v", err)
	}
	got, err = cs.Check(id, true)
	if err != nil {
		t.Fatalf("Check after corruption: %v", err)
	}
	if got.Checksum == sum {
		t.Fatal("Check reported the original checksum for corrupted bytes")
	}
}

func TestCheckOnMissingChunk(t *testing.T) {
	cs := newStore(t)
	got, err := cs.Check(uuid.NewString(), true)
	if err != nil {
		t.Fatalf("Check on a missing chunk should not error: %v", err)
	}
	if got.Exists {
		t.Fatal("missing chunk reported as existing")
	}
}

func TestDeleteIsIdempotentAndUpdatesUsage(t *testing.T) {
	cs := newStore(t)
	id := uuid.NewString()
	payload := bytes.Repeat([]byte("d"), 1024)
	if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), checksum.Sum(payload)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	existed, err := cs.Delete(id)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !existed {
		t.Fatal("Delete reported the chunk as absent")
	}

	// The GC retries deletions, so a second delete must succeed quietly.
	existed, err = cs.Delete(id)
	if err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if existed {
		t.Fatal("second Delete should report existed=false")
	}

	_, used, _, chunks, _, err := cs.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if used != 0 || chunks != 0 {
		t.Fatalf("after delete: used=%d chunks=%d, want 0/0", used, chunks)
	}
}

func TestStatsRespectConfiguredCapacity(t *testing.T) {
	dir := t.TempDir()
	const quota = 4096
	cs, err := NewChunkStore(dir, false, quota)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}

	payload := bytes.Repeat([]byte("x"), 1000)
	id := uuid.NewString()
	if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), checksum.Sum(payload)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	total, used, available, _, _, err := cs.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if total != quota {
		t.Fatalf("total = %d, want the configured quota %d", total, quota)
	}
	if used != 1000 {
		t.Fatalf("used = %d, want 1000", used)
	}
	// Available is min(quota - used, filesystem free); on any sane test machine
	// the quota is the binding constraint.
	if available > quota-used {
		t.Fatalf("available = %d exceeds quota headroom %d", available, quota-used)
	}
}

func TestRescanRecoversUsageAfterRestart(t *testing.T) {
	dir := t.TempDir()
	cs, err := NewChunkStore(dir, false, 1<<30)
	if err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	payload := bytes.Repeat([]byte("p"), 512)
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		if _, err := cs.Write(id, bytes.NewReader(payload), int64(len(payload)), checksum.Sum(payload)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Restart against the same directory.
	reopened, err := NewChunkStore(dir, false, 1<<30)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	_, used, _, chunks, _, err := reopened.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if chunks != 3 || used != 3*512 {
		t.Fatalf("after restart: chunks=%d used=%d, want 3/%d", chunks, used, 3*512)
	}
}

func TestStartupClearsStaleTempFiles(t *testing.T) {
	dir := t.TempDir()
	if _, err := NewChunkStore(dir, false, 1<<30); err != nil {
		t.Fatalf("NewChunkStore: %v", err)
	}
	stale := filepath.Join(dir, tempDirName, "leftover.tmp")
	if err := os.WriteFile(stale, []byte("crash debris"), 0o644); err != nil {
		t.Fatalf("writing stale temp file: %v", err)
	}

	if _, err := NewChunkStore(dir, false, 1<<30); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale temp file survived startup")
	}
}

func TestActiveRequestCounter(t *testing.T) {
	cs := newStore(t)
	if cs.ActiveRequests() != 0 {
		t.Fatal("expected 0 active requests initially")
	}
	cs.BeginRequest()
	cs.BeginRequest()
	if cs.ActiveRequests() != 2 {
		t.Fatalf("ActiveRequests = %d, want 2", cs.ActiveRequests())
	}
	cs.EndRequest()
	cs.EndRequest()
	if cs.ActiveRequests() != 0 {
		t.Fatalf("ActiveRequests = %d, want 0", cs.ActiveRequests())
	}
}

func assertTempDirEmpty(t *testing.T, cs *ChunkStore) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(cs.Root(), tempDirName))
	if err != nil {
		t.Fatalf("reading temp dir: %v", err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("temp files leaked: %v", names)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

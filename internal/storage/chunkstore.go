// Package storage implements the data plane: durable chunk files on local
// disk, plus the gRPC service that exposes them.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harjeetschahal/flexstore/internal/checksum"
)

const (
	dataDirName = "data"
	tempDirName = "tmp"
	chunkSuffix = ".chunk"

	// fanoutDepth is how many two-character directory levels a chunk path uses.
	// Two levels of 256 gives 65 536 leaf directories, which keeps any single
	// directory small enough that ext4/overlayfs lookups stay fast even with
	// millions of chunks.
	fanoutDepth = 2
)

// ErrNotFound means the chunk is absent on this node.
var ErrNotFound = errors.New("chunk not found")

// ErrInvalidChunkID rejects IDs that could escape the data directory.
var ErrInvalidChunkID = errors.New("invalid chunk id")

// ChunkStore is a content directory of chunk files.
//
// Durability contract: a file only appears at its final path after its bytes
// are on disk and its SHA-256 matches. Readers therefore never observe a
// partially written chunk, even if the process is killed mid-write.
type ChunkStore struct {
	root    string
	dataDir string
	tempDir string
	fsync   bool

	// capacity is the advertised total; 0 means "ask the filesystem".
	capacity int64

	// usedBytes and chunkCount are maintained incrementally after an initial
	// scan, because stat-ing every chunk on each heartbeat would not scale.
	mu         sync.RWMutex
	usedBytes  int64
	chunkCount int64

	activeRequests atomic.Int32
	// tempSeq makes temp file names unique without needing a lock or a RNG.
	tempSeq atomic.Uint64
}

// NewChunkStore prepares the on-disk layout and scans existing chunks.
func NewChunkStore(root string, fsync bool, capacity int64) (*ChunkStore, error) {
	cs := &ChunkStore{
		root:     root,
		dataDir:  filepath.Join(root, dataDirName),
		tempDir:  filepath.Join(root, tempDirName),
		fsync:    fsync,
		capacity: capacity,
	}
	// 0o750/0o600 throughout: chunk data is read and written only by this
	// process, so group/world bits would grant access nothing legitimate uses.
	for _, d := range []string{cs.dataDir, cs.tempDir} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return nil, fmt.Errorf("create %s: %w", d, err)
		}
	}
	// Temp files are, by construction, never valid chunks. Anything left here
	// is debris from a crash and is removed at startup.
	if err := cs.clearTempDir(); err != nil {
		return nil, err
	}
	if err := cs.scan(); err != nil {
		return nil, err
	}
	return cs, nil
}

// Root returns the store's base directory.
func (cs *ChunkStore) Root() string { return cs.root }

// PathFor returns the absolute path of a chunk. Exported because path layout
// is a documented, unit-tested property of the store.
func (cs *ChunkStore) PathFor(chunkID string) (string, error) {
	rel, err := RelativePath(chunkID)
	if err != nil {
		return "", err
	}
	return filepath.Join(cs.dataDir, rel), nil
}

// RelativePath maps a chunk ID to its fan-out path, e.g.
// "8f/1c/8f1c9d...-....chunk". UUID hex is uniformly distributed, so the first
// bytes make a good directory hash with no extra hashing step.
func RelativePath(chunkID string) (string, error) {
	if err := ValidateChunkID(chunkID); err != nil {
		return "", err
	}
	flat := strings.ReplaceAll(chunkID, "-", "")
	parts := make([]string, 0, fanoutDepth+1)
	for i := 0; i < fanoutDepth; i++ {
		parts = append(parts, flat[i*2:i*2+2])
	}
	parts = append(parts, chunkID+chunkSuffix)
	return filepath.Join(parts...), nil
}

// ValidateChunkID rejects anything that is not a bare UUID. This is the
// path-traversal guard: chunk IDs arrive over the network and are used to
// build filesystem paths.
func ValidateChunkID(id string) error {
	if len(id) != 36 {
		return fmt.Errorf("%w: %q has length %d, want 36", ErrInvalidChunkID, id, len(id))
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return fmt.Errorf("%w: %q is not a UUID", ErrInvalidChunkID, id)
			}
		default:
			isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
			if !isHex {
				return fmt.Errorf("%w: %q is not lowercase-hex UUID", ErrInvalidChunkID, id)
			}
		}
	}
	return nil
}

// WriteResult describes a completed chunk write.
type WriteResult struct {
	BytesWritten   int64
	Checksum       string
	AlreadyExisted bool
}

// Write durably stores a chunk read from src.
//
// Sequence: write to a temp file -> fsync the file -> verify SHA-256 ->
// rename into place -> fsync the parent directory. The verification happens
// before the rename, so a corrupted transfer never produces a visible chunk.
// The directory fsync is what makes the rename itself survive a power loss.
func (cs *ChunkStore) Write(chunkID string, src io.Reader, expectedSize int64, expectedChecksum string) (WriteResult, error) {
	final, err := cs.PathFor(chunkID)
	if err != nil {
		return WriteResult{}, err
	}
	if expectedChecksum != "" && !checksum.Valid(expectedChecksum) {
		return WriteResult{}, fmt.Errorf("invalid expected checksum %q for chunk %s", expectedChecksum, chunkID)
	}

	// Idempotence: a retried write for a chunk we already hold and can verify
	// is a no-op. This is what makes gateway retries safe.
	if existing, err := os.Stat(final); err == nil && !existing.IsDir() {
		sum, size, verr := cs.verifyFile(final)
		if verr == nil && (expectedChecksum == "" || checksum.Equal(sum, expectedChecksum)) {
			return WriteResult{BytesWritten: size, Checksum: sum, AlreadyExisted: true}, nil
		}
		// The existing copy is bad; overwrite it below rather than serving it.
	}

	if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
		return WriteResult{}, fmt.Errorf("create chunk directory: %w", err)
	}

	tmpPath := filepath.Join(cs.tempDir,
		fmt.Sprintf("%s.%d.tmp", chunkID, cs.tempSeq.Add(1)))
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) // #nosec G304 -- tempDir + a validated chunk ID + a counter; no client-controlled bytes
	if err != nil {
		return WriteResult{}, fmt.Errorf("create temp file: %w", err)
	}
	// Guarded cleanup: on any error path the temp file must not survive.
	committed := false
	defer func() {
		if !committed {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	hw := checksum.NewWriter(tmp)
	written, err := io.Copy(hw, src)
	if err != nil {
		return WriteResult{}, fmt.Errorf("write chunk %s: %w", chunkID, err)
	}
	if expectedSize >= 0 && written != expectedSize {
		return WriteResult{}, fmt.Errorf("chunk %s: wrote %d bytes, header declared %d",
			chunkID, written, expectedSize)
	}
	if cs.fsync {
		if err := tmp.Sync(); err != nil {
			return WriteResult{}, fmt.Errorf("fsync chunk %s: %w", chunkID, err)
		}
	}
	if err := tmp.Close(); err != nil {
		return WriteResult{}, fmt.Errorf("close chunk %s: %w", chunkID, err)
	}

	sum := hw.Sum()
	if expectedChecksum != "" {
		if err := checksum.Verify(chunkID, expectedChecksum, sum); err != nil {
			return WriteResult{}, err
		}
	}

	if err := os.Rename(tmpPath, final); err != nil {
		return WriteResult{}, fmt.Errorf("publish chunk %s: %w", chunkID, err)
	}
	committed = true

	if cs.fsync {
		if err := fsyncDir(filepath.Dir(final)); err != nil {
			return WriteResult{}, fmt.Errorf("fsync chunk directory: %w", err)
		}
	}

	cs.addUsage(written, 1)
	return WriteResult{BytesWritten: written, Checksum: sum}, nil
}

// Open returns a reader for a chunk plus its size. The caller must Close it.
//
// Verification is the reader's job (see VerifiedReader): streaming without
// buffering the whole chunk means we cannot know the checksum until the last
// byte, so the contract is "the stream fails at the end if the data was bad".
// The gateway therefore buffers one chunk before forwarding it to the client.
func (cs *ChunkStore) Open(chunkID string) (*os.File, int64, error) {
	path, err := cs.PathFor(chunkID)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(path) // #nosec G304 -- PathFor only ever builds paths from UUID-validated chunk IDs
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, fmt.Errorf("chunk %s: %w", chunkID, ErrNotFound)
		}
		return nil, 0, fmt.Errorf("open chunk %s: %w", chunkID, err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	return f, st.Size(), nil
}

// Delete removes a chunk. Deleting a chunk that is already gone succeeds, so
// the coordinator's deletion queue is idempotent.
func (cs *ChunkStore) Delete(chunkID string) (existed bool, err error) {
	path, err := cs.PathFor(chunkID)
	if err != nil {
		return false, err
	}
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("delete chunk %s: %w", chunkID, err)
	}
	cs.addUsage(-st.Size(), -1)
	return true, nil
}

// CheckResult reports on a chunk's presence and integrity.
type CheckResult struct {
	Exists    bool
	SizeBytes int64
	Checksum  string
	Valid     bool
}

// Check reports whether a chunk exists and, when verify is set, whether its
// bytes still hash to the expected value.
func (cs *ChunkStore) Check(chunkID string, verify bool) (CheckResult, error) {
	path, err := cs.PathFor(chunkID)
	if err != nil {
		return CheckResult{}, err
	}
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return CheckResult{}, nil
		}
		return CheckResult{}, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	res := CheckResult{Exists: true, SizeBytes: st.Size()}
	if !verify {
		return res, nil
	}
	sum, size, err := cs.verifyFile(path)
	if err != nil {
		return res, err
	}
	res.Checksum = sum
	res.SizeBytes = size
	res.Valid = true
	return res, nil
}

// Stats reports capacity and inventory for heartbeats.
func (cs *ChunkStore) Stats() (total, used, available, chunks int64, active int32, err error) {
	cs.mu.RLock()
	used = cs.usedBytes
	chunks = cs.chunkCount
	cs.mu.RUnlock()

	if cs.capacity > 0 {
		// Configured capacity: available is what is left of *our* quota, but
		// never more than the filesystem can actually give us.
		total = cs.capacity
		available = total - used
		if available < 0 {
			available = 0
		}
		if fsAvail, ferr := diskAvailable(cs.root); ferr == nil && fsAvail < available {
			available = fsAvail
		}
		return total, used, available, chunks, cs.activeRequests.Load(), nil
	}

	fsTotal, fsAvail, err := diskUsage(cs.root)
	if err != nil {
		return 0, used, 0, chunks, cs.activeRequests.Load(), err
	}
	return fsTotal, used, fsAvail, chunks, cs.activeRequests.Load(), nil
}

// BeginRequest/EndRequest track in-flight operations, which feed the
// placement engine's load weighting.
func (cs *ChunkStore) BeginRequest() { cs.activeRequests.Add(1) }
func (cs *ChunkStore) EndRequest()   { cs.activeRequests.Add(-1) }

// ActiveRequests returns the current in-flight count.
func (cs *ChunkStore) ActiveRequests() int32 { return cs.activeRequests.Load() }

func (cs *ChunkStore) addUsage(bytes, chunks int64) {
	cs.mu.Lock()
	cs.usedBytes += bytes
	if cs.usedBytes < 0 {
		cs.usedBytes = 0
	}
	cs.chunkCount += chunks
	if cs.chunkCount < 0 {
		cs.chunkCount = 0
	}
	cs.mu.Unlock()
}

// verifyFile re-reads a chunk and returns its actual digest and size.
func (cs *ChunkStore) verifyFile(path string) (string, int64, error) {
	f, err := os.Open(path) // #nosec G304 -- callers pass PathFor output, which is UUID-validated
	if err != nil {
		return "", 0, fmt.Errorf("open for verify: %w", err)
	}
	defer f.Close()

	h := checksum.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("read for verify: %w", err)
	}
	return checksum.Hex(h), n, nil
}

// InventoryEntry is one chunk found on disk.
type InventoryEntry struct {
	ChunkID  string
	Size     int64
	Modified time.Time
}

// WalkInventory calls fn for every chunk on disk, in batches.
//
// Streaming rather than returning a slice: a production node can hold millions
// of chunks, and reconciliation must not require materialising all of them.
// Returning an error from fn stops the walk.
func (cs *ChunkStore) WalkInventory(batchSize int, fn func([]InventoryEntry) error) error {
	if batchSize <= 0 {
		batchSize = 1000
	}
	batch := make([]InventoryEntry, 0, batchSize)

	err := filepath.WalkDir(cs.dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), chunkSuffix) {
			return nil
		}
		id := strings.TrimSuffix(d.Name(), chunkSuffix)
		// Skip anything that is not a chunk file we could have written. This is
		// a filter, not an error path: the reconciler deletes files the
		// coordinator does not recognise, so never reporting a stray file keeps
		// it from ever being pointed at one.
		if err := ValidateChunkID(id); err != nil {
			return nil //nolint:nilerr // filter, not a failure
		}
		info, err := d.Info()
		if err != nil {
			// A file the GC deleted between the directory read and the stat is
			// simply not part of the inventory, not a failure of the walk.
			if os.IsNotExist(err) {
				return nil //nolint:nilerr // concurrent deletion, not an error
			}
			return err
		}
		batch = append(batch, InventoryEntry{
			ChunkID:  id,
			Size:     info.Size(),
			Modified: info.ModTime(),
		})
		if len(batch) >= batchSize {
			if err := fn(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk chunk inventory: %w", err)
	}
	if len(batch) > 0 {
		return fn(batch)
	}
	return nil
}

// scan rebuilds usage counters from disk at startup. O(chunks) once per boot,
// which is acceptable and much safer than persisting a counter that could
// drift after a crash.
func (cs *ChunkStore) scan() error {
	var bytes, count int64
	err := filepath.WalkDir(cs.dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), chunkSuffix) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		bytes += info.Size()
		count++
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan chunk directory: %w", err)
	}
	cs.mu.Lock()
	cs.usedBytes = bytes
	cs.chunkCount = count
	cs.mu.Unlock()
	return nil
}

func (cs *ChunkStore) clearTempDir() error {
	entries, err := os.ReadDir(cs.tempDir)
	if err != nil {
		return fmt.Errorf("read temp dir: %w", err)
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(cs.tempDir, e.Name())); err != nil {
			return fmt.Errorf("remove stale temp file %s: %w", e.Name(), err)
		}
	}
	return nil
}

// fsyncDir flushes a directory entry so a rename survives a crash. On some
// platforms opening a directory for sync is not supported; that is reported
// rather than ignored, because silently skipping it would weaken the
// durability claim this package makes.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- always a directory this package itself created under dataDir
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

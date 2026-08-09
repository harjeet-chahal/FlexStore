//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"
)

// streamSizeMiB is the payload used for the streaming tests. Large enough to
// span several chunks and to make a "load it all into RAM" implementation
// obvious in container stats, small enough to keep CI honest.
const streamSizeMiB = 48

// generatedReader produces deterministic pseudo-random bytes without ever
// materialising them, so the test client's own memory use does not mask the
// gateway's. This is what makes the "never load the whole object into RAM"
// claim testable end to end.
type generatedReader struct {
	remaining int64
	seed      byte
	block     []byte
	offset    int
}

func newGeneratedReader(total int64, seed byte) *generatedReader {
	// One 64 KiB block, permuted per iteration, gives a stream that is not
	// trivially compressible and never repeats at chunk boundaries.
	block := make([]byte, 64<<10)
	for i := range block {
		block[i] = byte(i*31) ^ seed
	}
	return &generatedReader{remaining: total, seed: seed, block: block}
}

func (g *generatedReader) Read(p []byte) (int, error) {
	if g.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > g.remaining {
		n = int(g.remaining)
	}
	written := 0
	for written < n {
		if g.offset >= len(g.block) {
			g.offset = 0
			// Permute so successive blocks differ.
			for i := range g.block {
				g.block[i]++
			}
		}
		c := copy(p[written:n], g.block[g.offset:])
		g.offset += c
		written += c
	}
	g.remaining -= int64(written)
	return written, nil
}

// expectedSum replays the same generator to compute the digest the server
// should produce.
func expectedSum(total int64, seed byte) string {
	h := sha256.New()
	if _, err := io.Copy(h, newGeneratedReader(total, seed)); err != nil {
		panic(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestStreamingLargeObjectWithoutContentLength uploads with chunked transfer
// encoding, which is the case that breaks any implementation that buffers the
// whole body before deciding what to do with it.
func TestStreamingLargeObjectWithoutContentLength(t *testing.T) {
	if testing.Short() {
		t.Skip("moves tens of megabytes; skipped in -short mode")
	}
	ctx := testContext(t, 15*time.Minute)
	key := uniqueKey(t, "streamed.bin")

	total := int64(streamSizeMiB) << 20
	want := expectedSum(total, 0x5a)

	req := newRequest(t, ctx, http.MethodPut, "/objects/"+testBucket+"/"+key,
		newGeneratedReader(total, 0x5a))
	// ContentLength -1 forces chunked transfer encoding: the gateway must
	// stream without knowing the size in advance.
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/octet-stream")

	start := time.Now()
	resp, body := do(t, req)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("streaming PUT = %d: %s", resp.StatusCode, body)
	}
	t.Logf("uploaded %d MiB in %s", streamSizeMiB, time.Since(start).Round(time.Millisecond))
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	// Download without buffering either, hashing as we go.
	getReq := newRequest(t, ctx, http.MethodGet, "/objects/"+testBucket+"/"+key, nil)
	getResp, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("streaming GET: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("streaming GET = %d", getResp.StatusCode)
	}

	h := sha256.New()
	start = time.Now()
	n, err := io.Copy(h, getResp.Body)
	if err != nil {
		t.Fatalf("reading the download stream after %d bytes: %v", n, err)
	}
	t.Logf("downloaded %d MiB in %s", n>>20, time.Since(start).Round(time.Millisecond))

	if n != total {
		t.Fatalf("downloaded %d bytes, want %d", n, total)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != want {
		t.Fatalf("SHA-256 mismatch on a %d MiB streamed object:\n  want %s\n  got  %s",
			streamSizeMiB, want, got)
	}

	layout := getLayout(t, ctx, testBucket, key)
	wantChunks := (total + defaultChunkSize - 1) / defaultChunkSize
	if int64(len(layout.Chunks)) != wantChunks {
		t.Fatalf("object split into %d chunks, want %d", len(layout.Chunks), wantChunks)
	}
	t.Logf("object stored as %d chunks", len(layout.Chunks))
}

// TestConcurrentUploads checks that many simultaneous uploads do not interfere
// -- each must come back byte-identical, and none may steal another's chunks.
func TestConcurrentUploads(t *testing.T) {
	if testing.Short() {
		t.Skip("uploads several objects concurrently; skipped in -short mode")
	}
	ctx := testContext(t, 10*time.Minute)

	const workers = 6
	type result struct {
		key string
		sum string
		err error
	}
	results := make(chan result, workers)

	for i := 0; i < workers; i++ {
		go func(i int) {
			key := uniqueKey(t, "concurrent.bin")
			payload := make([]byte, defaultChunkSize+int64(i)*4096)
			if _, err := rand.Read(payload); err != nil {
				results <- result{err: err}
				return
			}
			sum := sha256hex(payload)

			req, err := http.NewRequestWithContext(ctx, http.MethodPut,
				gatewayURL()+"/objects/"+testBucket+"/"+key, newBytesReader(payload))
			if err != nil {
				results <- result{err: err}
				return
			}
			req.ContentLength = int64(len(payload))
			resp, err := client.Do(req)
			if err != nil {
				results <- result{err: err}
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				results <- result{err: errStatus(resp.StatusCode)}
				return
			}
			results <- result{key: key, sum: sum}
		}(i)
	}

	uploaded := make([]result, 0, workers)
	for i := 0; i < workers; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent upload failed: %v", r.err)
		}
		uploaded = append(uploaded, r)
	}

	for _, r := range uploaded {
		got, _ := getObject(t, ctx, testBucket, r.key)
		if sha256hex(got) != r.sum {
			t.Errorf("%s: content differs after concurrent uploads", r.key)
		}
		key := r.key
		t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })
	}
	t.Logf("%d concurrent uploads all round-tripped correctly", workers)
}

type statusError int

func (s statusError) Error() string { return "unexpected status " + http.StatusText(int(s)) }

func errStatus(code int) error { return statusError(code) }

func newBytesReader(b []byte) io.Reader { return &sliceReader{b: b} }

type sliceReader struct {
	b   []byte
	pos int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.pos:])
	r.pos += n
	return n, nil
}

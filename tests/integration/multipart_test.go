//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

type createUploadResponse struct {
	UploadID  string `json:"upload_id"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	ChunkSize int64  `json:"chunk_size"`
}

type uploadPartResponse struct {
	UploadID   string `json:"upload_id"`
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
	SizeBytes  int64  `json:"size_bytes"`
}

type completeUploadResponse struct {
	UploadID  string `json:"upload_id"`
	ObjectID  string `json:"object_id"`
	ETag      string `json:"etag"`
	SizeBytes int64  `json:"size_bytes"`
}

func createMultipart(t *testing.T, ctx context.Context, bucket, key string) createUploadResponse {
	t.Helper()
	req := newRequest(t, ctx, http.MethodPost, "/multipart/"+bucket+"/"+key, nil)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, body := do(t, req)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create multipart = %d: %s", resp.StatusCode, body)
	}
	var out createUploadResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding create response: %v\n%s", err, body)
	}
	if out.UploadID == "" {
		t.Fatalf("no upload id in response: %s", body)
	}
	return out
}

func uploadPart(t *testing.T, ctx context.Context, uploadID string, partNumber int, payload []byte) uploadPartResponse {
	t.Helper()
	req := newRequest(t, ctx, http.MethodPut,
		fmt.Sprintf("/multipart/%s/%d", uploadID, partNumber), bytes.NewReader(payload))
	req.ContentLength = int64(len(payload))

	resp, body := do(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload part %d = %d: %s", partNumber, resp.StatusCode, body)
	}
	var out uploadPartResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding part response: %v\n%s", err, body)
	}
	return out
}

func completeMultipart(t *testing.T, ctx context.Context, uploadID string, manifest any) completeUploadResponse {
	t.Helper()
	var reader *bytes.Reader
	if manifest != nil {
		raw, err := json.Marshal(manifest)
		if err != nil {
			t.Fatalf("encoding manifest: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}

	req := newRequest(t, ctx, http.MethodPost, "/multipart/"+uploadID+"/complete", reader)
	req.Header.Set("Content-Type", "application/json")

	resp, body := do(t, req)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete multipart = %d: %s", resp.StatusCode, body)
	}
	var out completeUploadResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decoding complete response: %v\n%s", err, body)
	}
	return out
}

// TestMultipartUploadRoundTrip uploads parts out of order and verifies the
// assembled object matches the concatenation in part-number order.
func TestMultipartUploadRoundTrip(t *testing.T) {
	ctx := testContext(t, 8*time.Minute)
	key := uniqueKey(t, "multipart.bin")

	// Each part spans more than one chunk, so assembly has to renumber chunks
	// across part boundaries -- the interesting case.
	partSize := defaultChunkSize + 4096
	parts := [][]byte{
		randomPayload(t, partSize),
		randomPayload(t, partSize),
		randomPayload(t, 9999), // short final part
	}

	upload := createMultipart(t, ctx, testBucket, key)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	// Upload 3, then 1, then 2: order of arrival must not affect assembly.
	etags := make([]string, len(parts))
	for _, i := range []int{2, 0, 1} {
		res := uploadPart(t, ctx, upload.UploadID, i+1, parts[i])
		if res.SizeBytes != int64(len(parts[i])) {
			t.Fatalf("part %d reported %d bytes, want %d", i+1, res.SizeBytes, len(parts[i]))
		}
		if res.ETag != sha256hex(parts[i]) {
			t.Fatalf("part %d etag = %s, want %s", i+1, res.ETag, sha256hex(parts[i]))
		}
		etags[i] = res.ETag
	}

	manifest := map[string]any{"parts": []map[string]any{
		{"part_number": 1, "etag": etags[0]},
		{"part_number": 2, "etag": etags[1]},
		{"part_number": 3, "etag": etags[2]},
	}}
	complete := completeMultipart(t, ctx, upload.UploadID, manifest)

	var want bytes.Buffer
	for _, p := range parts {
		want.Write(p)
	}
	if complete.SizeBytes != int64(want.Len()) {
		t.Fatalf("assembled size = %d, want %d", complete.SizeBytes, want.Len())
	}
	// Multipart ETags carry the S3-style "-N" suffix so clients can tell them
	// from a plain content hash.
	if !strings.HasSuffix(complete.ETag, "-3") {
		t.Errorf("multipart etag %q does not end with the part count", complete.ETag)
	}

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != sha256hex(want.Bytes()) {
		t.Fatalf("assembled object does not match the concatenated parts:\n  want %s\n  got  %s",
			sha256hex(want.Bytes()), sha256hex(got))
	}

	// Every assembled chunk must end up fully replicated. Parts commit at
	// min_write_replicas, so convergence is the property to assert, not the
	// replica count at the instant completion returned.
	waitForObjectFullyReplicated(t, ctx, testBucket, key, 2*time.Minute)
}

func TestMultipartCompleteWithoutAManifest(t *testing.T) {
	// The manifest is optional; with no body the coordinator uses whatever
	// parts it recorded.
	ctx := testContext(t, 5*time.Minute)
	key := uniqueKey(t, "no-manifest.bin")

	upload := createMultipart(t, ctx, testBucket, key)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	a := randomPayload(t, 5000)
	b := randomPayload(t, 6000)
	uploadPart(t, ctx, upload.UploadID, 1, a)
	uploadPart(t, ctx, upload.UploadID, 2, b)

	complete := completeMultipart(t, ctx, upload.UploadID, nil)
	if complete.SizeBytes != int64(len(a)+len(b)) {
		t.Fatalf("size = %d, want %d", complete.SizeBytes, len(a)+len(b))
	}

	got, _ := getObject(t, ctx, testBucket, key)
	if !bytes.Equal(got, append(append([]byte{}, a...), b...)) {
		t.Fatal("assembled object does not match a || b")
	}
}

func TestMultipartAbort(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	key := uniqueKey(t, "aborted.bin")

	upload := createMultipart(t, ctx, testBucket, key)
	uploadPart(t, ctx, upload.UploadID, 1, randomPayload(t, 4096))

	resp, body := do(t, newRequest(t, ctx, http.MethodDelete, "/multipart/"+upload.UploadID, nil))
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort = %d: %s", resp.StatusCode, body)
	}

	// The object must never have become visible.
	status, _, err := tryGet(ctx, testBucket, key)
	if err != nil {
		t.Fatalf("GET after abort: %v", err)
	}
	if status != http.StatusNotFound {
		t.Fatalf("GET after abort = %d, want 404", status)
	}

	// Completing an aborted upload must fail rather than resurrect it.
	req := newRequest(t, ctx, http.MethodPost, "/multipart/"+upload.UploadID+"/complete", nil)
	resp, _ = do(t, req)
	if resp.StatusCode < 400 {
		t.Fatalf("completing an aborted upload returned %d; it must fail", resp.StatusCode)
	}
}

func TestMultipartRejectsAWrongManifest(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	key := uniqueKey(t, "bad-manifest.bin")

	upload := createMultipart(t, ctx, testBucket, key)
	t.Cleanup(func() {
		req := newRequest(t, context.Background(), http.MethodDelete, "/multipart/"+upload.UploadID, nil)
		_, _ = do(t, req)
	})

	uploadPart(t, ctx, upload.UploadID, 1, randomPayload(t, 4096))

	// The client believes it uploaded two parts; silently completing with one
	// would hand back a truncated object.
	manifest := map[string]any{"parts": []map[string]any{
		{"part_number": 1}, {"part_number": 2},
	}}
	raw, _ := json.Marshal(manifest)
	req := newRequest(t, ctx, http.MethodPost, "/multipart/"+upload.UploadID+"/complete", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")

	resp, body := do(t, req)
	if resp.StatusCode < 400 {
		t.Fatalf("a mismatched manifest was accepted (%d): %s", resp.StatusCode, body)
	}
}

func TestMultipartRejectsInvalidPartNumbers(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)
	key := uniqueKey(t, "bad-part-number.bin")

	upload := createMultipart(t, ctx, testBucket, key)
	t.Cleanup(func() {
		req := newRequest(t, context.Background(), http.MethodDelete, "/multipart/"+upload.UploadID, nil)
		_, _ = do(t, req)
	})

	for _, part := range []string{"0", "-1", "10001", "abc"} {
		req := newRequest(t, ctx, http.MethodPut,
			"/multipart/"+upload.UploadID+"/"+part, bytes.NewReader([]byte("x")))
		resp, body := do(t, req)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("part number %q returned %d, want 400: %s", part, resp.StatusCode, body)
		}
	}
}

func TestMultipartOverwritesAnExistingObject(t *testing.T) {
	ctx := testContext(t, 8*time.Minute)
	key := uniqueKey(t, "mpu-overwrite.bin")

	original := randomPayload(t, 4096)
	putObject(t, ctx, testBucket, key, original)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	upload := createMultipart(t, ctx, testBucket, key)
	replacement := randomPayload(t, defaultChunkSize+1000)
	uploadPart(t, ctx, upload.UploadID, 1, replacement)
	completeMultipart(t, ctx, upload.UploadID, nil)

	got, _ := getObject(t, ctx, testBucket, key)
	if sha256hex(got) != sha256hex(replacement) {
		t.Fatal("the multipart upload did not replace the existing object")
	}
}

// TestMultipartSessionSurvivesTime is a light guard that a session opened and
// used across a few seconds still works, exercising the Redis-cached session
// header path (cache hit) as well as the initial miss.
func TestMultipartSessionSurvivesTime(t *testing.T) {
	ctx := testContext(t, 5*time.Minute)
	key := uniqueKey(t, "session.bin")

	upload := createMultipart(t, ctx, testBucket, key)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	for i := 1; i <= 3; i++ {
		uploadPart(t, ctx, upload.UploadID, i, randomPayload(t, 2048))
		time.Sleep(500 * time.Millisecond)
	}
	complete := completeMultipart(t, ctx, upload.UploadID, nil)
	if complete.SizeBytes != 3*2048 {
		t.Fatalf("size = %d, want %d", complete.SizeBytes, 3*2048)
	}
}

//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestRangeRequests exercises RFC 7233 against a real multi-chunk object, so the
// chunk-selection arithmetic is checked against bytes that actually crossed the
// network rather than against a unit-test fixture.
func TestRangeRequests(t *testing.T) {
	ctx := testContext(t, 3*time.Minute)
	key := uniqueKey(t, "ranged.bin")

	// Deliberately not a multiple of the 8 MiB chunk size, so the final chunk is
	// short and the last range lands inside it.
	payload := randomPayload(t, 20*1024*1024+12345)
	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	total := len(payload)
	tests := []struct {
		name       string
		header     string
		wantStatus int
		wantBody   []byte
		wantCR     string
	}{
		{"first bytes", "bytes=0-1023", http.StatusPartialContent, payload[:1024],
			fmt.Sprintf("bytes 0-1023/%d", total)},
		{"spanning a chunk boundary", "bytes=8388600-8388615", http.StatusPartialContent,
			payload[8388600 : 8388615+1], fmt.Sprintf("bytes 8388600-8388615/%d", total)},
		{"entirely inside the second chunk", "bytes=9000000-9000099", http.StatusPartialContent,
			payload[9000000 : 9000099+1], fmt.Sprintf("bytes 9000000-9000099/%d", total)},
		{"suffix", "bytes=-2048", http.StatusPartialContent, payload[total-2048:],
			fmt.Sprintf("bytes %d-%d/%d", total-2048, total-1, total)},
		{"open ended into the short final chunk", fmt.Sprintf("bytes=%d-", total-100),
			http.StatusPartialContent, payload[total-100:],
			fmt.Sprintf("bytes %d-%d/%d", total-100, total-1, total)},
		{"whole object by range", fmt.Sprintf("bytes=0-%d", total-1), http.StatusPartialContent,
			payload, fmt.Sprintf("bytes 0-%d/%d", total-1, total)},
		{"end past the object clamps", fmt.Sprintf("bytes=%d-99999999", total-10),
			http.StatusPartialContent, payload[total-10:],
			fmt.Sprintf("bytes %d-%d/%d", total-10, total-1, total)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := newRequest(t, ctx, http.MethodGet, "/objects/"+testBucket+"/"+key, nil)
			req.Header.Set("Range", tc.header)
			resp, body := do(t, req)

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			if got := resp.Header.Get("Content-Range"); got != tc.wantCR {
				t.Errorf("Content-Range = %q, want %q", got, tc.wantCR)
			}
			if got := resp.Header.Get("Content-Length"); got != strconv.Itoa(len(tc.wantBody)) {
				t.Errorf("Content-Length = %q, want %d", got, len(tc.wantBody))
			}
			if !bytes.Equal(body, tc.wantBody) {
				t.Fatalf("body mismatch: got %d bytes, want %d (sha %s vs %s)",
					len(body), len(tc.wantBody), sha256hex(body), sha256hex(tc.wantBody))
			}
		})
	}
}

func TestRangeUnsatisfiableAndIgnored(t *testing.T) {
	ctx := testContext(t, 2*time.Minute)
	key := uniqueKey(t, "range-edge.bin")
	payload := randomPayload(t, 4096)
	putObject(t, ctx, testBucket, key, payload)
	t.Cleanup(func() { deleteObject(t, context.Background(), testBucket, key) })

	t.Run("past the end is 416 with a Content-Range", func(t *testing.T) {
		req := newRequest(t, ctx, http.MethodGet, "/objects/"+testBucket+"/"+key, nil)
		req.Header.Set("Range", "bytes=99999-")
		resp, _ := do(t, req)
		if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("status = %d, want 416", resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Range"); got != fmt.Sprintf("bytes */%d", len(payload)) {
			t.Errorf("Content-Range = %q, want bytes */%d", got, len(payload))
		}
	})

	// RFC 7233 section 3.1: an unsatisfiable *syntax* is ignored, not rejected.
	for _, header := range []string{"items=0-10", "bytes=abc", "bytes=0-10,20-30", "bytes=500-100"} {
		t.Run("ignored: "+header, func(t *testing.T) {
			req := newRequest(t, ctx, http.MethodGet, "/objects/"+testBucket+"/"+key, nil)
			req.Header.Set("Range", header)
			resp, body := do(t, req)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200 (an unparseable Range must be ignored)", resp.StatusCode)
			}
			if !bytes.Equal(body, payload) {
				t.Fatal("ignoring the Range header must return the whole object")
			}
		})
	}

	t.Run("Accept-Ranges is advertised", func(t *testing.T) {
		req := newRequest(t, ctx, http.MethodGet, "/objects/"+testBucket+"/"+key, nil)
		resp, _ := do(t, req)
		if got := resp.Header.Get("Accept-Ranges"); !strings.Contains(got, "bytes") {
			t.Errorf("Accept-Ranges = %q, want bytes", got)
		}
	})
}

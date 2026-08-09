package gateway

import (
	"errors"
	"testing"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
)

func TestParseRange(t *testing.T) {
	const size = 1000
	tests := []struct {
		name       string
		header     string
		wantStart  int64
		wantEnd    int64
		wantErr    error
		wantIgnore bool
	}{
		{name: "closed range", header: "bytes=0-99", wantStart: 0, wantEnd: 99},
		{name: "mid range", header: "bytes=200-299", wantStart: 200, wantEnd: 299},
		{name: "open ended", header: "bytes=500-", wantStart: 500, wantEnd: 999},
		{name: "suffix", header: "bytes=-100", wantStart: 900, wantEnd: 999},
		{name: "suffix longer than object clamps", header: "bytes=-5000", wantStart: 0, wantEnd: 999},
		{name: "end past object clamps", header: "bytes=900-5000", wantStart: 900, wantEnd: 999},
		{name: "single byte", header: "bytes=0-0", wantStart: 0, wantEnd: 0},
		{name: "last byte", header: "bytes=999-999", wantStart: 999, wantEnd: 999},
		{name: "whole object", header: "bytes=0-999", wantStart: 0, wantEnd: 999},

		{name: "start past end of object", header: "bytes=1000-", wantErr: errUnsatisfiableRange},
		{name: "start past end, closed", header: "bytes=2000-3000", wantErr: errUnsatisfiableRange},

		{name: "empty header ignored", header: "", wantIgnore: true},
		{name: "wrong unit ignored", header: "items=0-10", wantIgnore: true},
		{name: "multi-range ignored", header: "bytes=0-10,20-30", wantIgnore: true},
		{name: "reversed ignored", header: "bytes=500-100", wantIgnore: true},
		{name: "garbage ignored", header: "bytes=abc-def", wantIgnore: true},
		{name: "no dash ignored", header: "bytes=100", wantIgnore: true},
		{name: "negative start ignored", header: "bytes=-0", wantIgnore: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRange(tc.header, size)
			switch {
			case tc.wantIgnore:
				if !errors.Is(err, errIgnorableRange) {
					t.Fatalf("err = %v, want errIgnorableRange (RFC 7233 says ignore, not reject)", err)
				}
			case tc.wantErr != nil:
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
			default:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got.start != tc.wantStart || got.end != tc.wantEnd {
					t.Fatalf("got [%d,%d], want [%d,%d]", got.start, got.end, tc.wantStart, tc.wantEnd)
				}
			}
		})
	}
}

func TestParseRangeOnEmptyObjectIsAlwaysUnsatisfiable(t *testing.T) {
	if _, err := parseRange("bytes=0-0", 0); !errors.Is(err, errUnsatisfiableRange) {
		t.Fatalf("err = %v, want errUnsatisfiableRange: a zero-length object has no byte 0", err)
	}
}

func chunksOf(sizes ...int64) []*flexstorev1.ChunkPlacement {
	out := make([]*flexstorev1.ChunkPlacement, 0, len(sizes))
	for i, s := range sizes {
		out = append(out, &flexstorev1.ChunkPlacement{
			ChunkId:    string(rune('a' + i)),
			ChunkIndex: int32(i),
			SizeBytes:  s,
		})
	}
	return out
}

func TestSelectChunksTrimsToTheWindow(t *testing.T) {
	chunks := chunksOf(100, 100, 100)

	tests := []struct {
		name  string
		br    byteRange
		spans []chunkSpan
	}{
		{"first chunk only", byteRange{0, 49},
			[]chunkSpan{{chunks[0], 0, 50}}},
		{"exactly the second chunk", byteRange{100, 199},
			[]chunkSpan{{chunks[1], 0, 100}}},
		{"straddling two chunks", byteRange{50, 149},
			[]chunkSpan{{chunks[0], 50, 100}, {chunks[1], 0, 50}}},
		{"all three", byteRange{0, 299},
			[]chunkSpan{{chunks[0], 0, 100}, {chunks[1], 0, 100}, {chunks[2], 0, 100}}},
		{"tail only", byteRange{250, 299},
			[]chunkSpan{{chunks[2], 50, 100}}},
		{"single byte in the middle chunk", byteRange{150, 150},
			[]chunkSpan{{chunks[1], 50, 51}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := selectChunks(chunks, tc.br)
			if len(got) != len(tc.spans) {
				t.Fatalf("selected %d chunks, want %d", len(got), len(tc.spans))
			}
			var total int64
			for i, s := range got {
				if s.chunk != tc.spans[i].chunk || s.from != tc.spans[i].from || s.to != tc.spans[i].to {
					t.Errorf("span %d = {%s %d:%d}, want {%s %d:%d}",
						i, s.chunk.ChunkId, s.from, s.to,
						tc.spans[i].chunk.ChunkId, tc.spans[i].from, tc.spans[i].to)
				}
				total += s.to - s.from
			}
			if total != tc.br.length() {
				t.Errorf("spans cover %d bytes, range asked for %d", total, tc.br.length())
			}
		})
	}
}

// TestSelectChunksHandlesUnevenChunks is the case that makes offset arithmetic
// on the nominal chunk size wrong.
//
// A multipart object is chunked per part, so a part whose length is not a
// multiple of the chunk size leaves a SHORT chunk in the middle of the
// assembled object. Computing a chunk's offset as index*chunkSize would put
// every chunk after that short one at the wrong offset -- and the bug would be
// invisible to checksums, because each chunk really is intact.
func TestSelectChunksHandlesUnevenChunks(t *testing.T) {
	// 100, 30 (end of part 1), 100, 100 -- total 330.
	chunks := chunksOf(100, 30, 100, 100)

	// Byte 130 is the first byte of chunk 2. Nominal arithmetic (index*100)
	// would place chunk 2 at offset 200 and return chunk 1's tail instead.
	got := selectChunks(chunks, byteRange{130, 139})
	if len(got) != 1 {
		t.Fatalf("selected %d chunks, want 1", len(got))
	}
	if got[0].chunk != chunks[2] {
		t.Fatalf("selected chunk %s, want chunk index 2: offsets must accumulate real sizes",
			got[0].chunk.ChunkId)
	}
	if got[0].from != 0 || got[0].to != 10 {
		t.Fatalf("span = %d:%d, want 0:10", got[0].from, got[0].to)
	}

	// And a range spanning the short chunk must include all three.
	got = selectChunks(chunks, byteRange{50, 199})
	if len(got) != 3 {
		t.Fatalf("selected %d chunks for a range across the short chunk, want 3", len(got))
	}
	var total int64
	for _, s := range got {
		total += s.to - s.from
	}
	if total != 150 {
		t.Fatalf("spans cover %d bytes, want 150", total)
	}
}

func TestAllChunksCoversEverything(t *testing.T) {
	chunks := chunksOf(100, 30, 7)
	spans := allChunks(chunks)
	var total int64
	for _, s := range spans {
		total += s.to - s.from
	}
	if total != objectSizeFromChunks(chunks) {
		t.Fatalf("allChunks covers %d bytes, object is %d", total, objectSizeFromChunks(chunks))
	}
}

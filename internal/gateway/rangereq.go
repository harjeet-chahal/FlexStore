package gateway

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
)

// errUnsatisfiableRange means the range is syntactically valid but does not
// overlap the object, which RFC 7233 requires be answered with 416 rather than
// with an empty 206.
var errUnsatisfiableRange = errors.New("range not satisfiable")

// errIgnorableRange means the header could not be parsed. RFC 7233 section 3.1
// says an unparseable Range header must be ignored and the whole object
// returned, not rejected -- so this is a signal to fall through, not an error
// to surface.
var errIgnorableRange = errors.New("range header ignored")

// byteRange is a resolved, absolute, inclusive range within an object.
type byteRange struct {
	start int64
	end   int64 // inclusive
}

func (b byteRange) length() int64 { return b.end - b.start + 1 }

func (b byteRange) contentRange(total int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", b.start, b.end, total)
}

// parseRange resolves a Range header against an object of the given size.
//
// Only a single byte range is supported. Multiple ranges would require a
// multipart/byteranges response, and the honest trade is that almost every real
// client (video players, resumable downloaders, S3 SDKs) sends exactly one --
// so the complexity buys compatibility with hand-written curl invocations and
// little else. A multi-range request is ignored rather than rejected, which
// RFC 7233 explicitly permits, so those clients still get correct data.
func parseRange(header string, size int64) (byteRange, error) {
	if header == "" {
		return byteRange{}, errIgnorableRange
	}
	const prefix = "bytes="
	if !strings.HasPrefix(header, prefix) {
		return byteRange{}, errIgnorableRange
	}
	spec := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	if spec == "" || strings.Contains(spec, ",") {
		return byteRange{}, errIgnorableRange
	}

	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return byteRange{}, errIgnorableRange
	}
	fromText := strings.TrimSpace(spec[:dash])
	toText := strings.TrimSpace(spec[dash+1:])

	// A zero-length object has no satisfiable range at all; every request
	// against it is a 416.
	if size == 0 {
		return byteRange{}, errUnsatisfiableRange
	}

	switch {
	case fromText == "":
		// "-N": the final N bytes.
		n, err := strconv.ParseInt(toText, 10, 64)
		if err != nil || n <= 0 {
			return byteRange{}, errIgnorableRange
		}
		if n > size {
			n = size
		}
		return byteRange{start: size - n, end: size - 1}, nil

	case toText == "":
		// "N-": from N to the end.
		start, err := strconv.ParseInt(fromText, 10, 64)
		if err != nil || start < 0 {
			return byteRange{}, errIgnorableRange
		}
		if start >= size {
			return byteRange{}, errUnsatisfiableRange
		}
		return byteRange{start: start, end: size - 1}, nil

	default:
		// "N-M", inclusive on both ends.
		start, err := strconv.ParseInt(fromText, 10, 64)
		if err != nil || start < 0 {
			return byteRange{}, errIgnorableRange
		}
		end, err := strconv.ParseInt(toText, 10, 64)
		if err != nil || end < start {
			return byteRange{}, errIgnorableRange
		}
		if start >= size {
			return byteRange{}, errUnsatisfiableRange
		}
		if end >= size {
			end = size - 1
		}
		return byteRange{start: start, end: end}, nil
	}
}

// chunkSpan is one chunk that overlaps a requested range, together with the
// slice of that chunk which the client actually asked for.
type chunkSpan struct {
	chunk *flexstorev1.ChunkPlacement
	from  int64 // inclusive offset within the chunk
	to    int64 // exclusive offset within the chunk
}

// selectChunks returns only the chunks overlapping the range, each trimmed to
// the requested window.
//
// Chunk offsets are computed by accumulating the recorded per-chunk sizes in
// index order, NOT by multiplying the index by the object's chunk size. Those
// two agree for a single-shot upload and disagree for a multipart one: each
// part is chunked independently, so a part whose length is not a multiple of
// the chunk size leaves a short chunk in the middle of the assembled object.
// Arithmetic on the nominal chunk size would silently return the wrong bytes
// for exactly those objects, and the checksums would still verify, because
// every individual chunk really is intact.
func selectChunks(chunks []*flexstorev1.ChunkPlacement, br byteRange) []chunkSpan {
	var out []chunkSpan
	var offset int64
	for _, c := range chunks {
		start, end := offset, offset+c.SizeBytes // [start, end)
		offset = end
		if end <= br.start {
			continue
		}
		if start > br.end {
			break
		}
		from := int64(0)
		if br.start > start {
			from = br.start - start
		}
		to := c.SizeBytes
		if br.end < end-1 {
			to = br.end - start + 1
		}
		out = append(out, chunkSpan{chunk: c, from: from, to: to})
	}
	return out
}

// objectSizeFromChunks sums the recorded chunk sizes. Used to cross-check the
// object's own size field before serving a range: if they disagree, the byte
// offsets a range implies are not trustworthy and the request is better served
// whole than wrongly.
func objectSizeFromChunks(chunks []*flexstorev1.ChunkPlacement) int64 {
	var n int64
	for _, c := range chunks {
		n += c.SizeBytes
	}
	return n
}

// allChunks is the whole-object span set: every chunk, full width.
func allChunks(chunks []*flexstorev1.ChunkPlacement) []chunkSpan {
	out := make([]chunkSpan, 0, len(chunks))
	for _, c := range chunks {
		out = append(out, chunkSpan{chunk: c, from: 0, to: c.SizeBytes})
	}
	return out
}

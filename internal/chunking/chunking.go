// Package chunking splits an arbitrarily large byte stream into fixed-size
// chunks without ever materialising the whole stream.
//
// The Splitter reuses a single buffer across iterations, so a 1 GiB upload
// costs one chunk-sized allocation, not 1 GiB. The buffer is only valid until
// the next Next() call -- that contract is what keeps the memory bound real.
package chunking

import (
	"errors"
	"fmt"
	"io"
)

// ErrChunkTooLarge is returned when the stream exceeds the configured maximum
// object size. Enforced during streaming so a hostile client cannot exhaust
// disk by omitting Content-Length.
var ErrChunkTooLarge = errors.New("object exceeds configured maximum size")

// Splitter reads from an underlying reader and yields fixed-size chunks.
type Splitter struct {
	r         io.Reader
	buf       []byte
	chunkSize int
	maxTotal  int64

	index     int
	totalRead int64
	done      bool
}

// Chunk is one unit of placement and replication. Data aliases the Splitter's
// internal buffer and is invalidated by the next call to Next.
type Chunk struct {
	Index int
	Data  []byte
}

// NewSplitter returns a Splitter producing chunks of exactly chunkSize bytes
// (the final chunk may be shorter). maxTotal <= 0 disables the size limit.
func NewSplitter(r io.Reader, chunkSize int, maxTotal int64) (*Splitter, error) {
	if chunkSize <= 0 {
		return nil, fmt.Errorf("chunk size must be positive, got %d", chunkSize)
	}
	return NewSplitterBuffer(r, make([]byte, chunkSize), maxTotal)
}

// NewSplitterBuffer is NewSplitter with the working buffer supplied by the
// caller, so it can come from a pool and be returned when the upload finishes.
// The chunk size is the buffer's length; the caller must not touch the buffer
// until the Splitter is done with it.
func NewSplitterBuffer(r io.Reader, buf []byte, maxTotal int64) (*Splitter, error) {
	if len(buf) == 0 {
		return nil, fmt.Errorf("chunk size must be positive, got %d", len(buf))
	}
	return &Splitter{
		r:         r,
		buf:       buf,
		chunkSize: len(buf),
		maxTotal:  maxTotal,
	}, nil
}

// Next returns the next chunk, or io.EOF when the stream is exhausted.
//
// A zero-byte stream yields io.EOF immediately with no chunks, which is how
// FlexStore represents an empty object: a COMPLETE object with chunk_count 0.
func (s *Splitter) Next() (Chunk, error) {
	if s.done {
		return Chunk{}, io.EOF
	}

	n, err := io.ReadFull(s.r, s.buf)
	switch {
	case err == nil:
		// Full chunk. There may or may not be more; the next call finds out.
	case errors.Is(err, io.EOF):
		// Clean end of stream exactly on a chunk boundary.
		s.done = true
		return Chunk{}, io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF):
		// Final short chunk.
		s.done = true
	default:
		s.done = true
		return Chunk{}, fmt.Errorf("reading chunk %d: %w", s.index, err)
	}

	s.totalRead += int64(n)
	if s.maxTotal > 0 && s.totalRead > s.maxTotal {
		s.done = true
		return Chunk{}, fmt.Errorf("%w: read %d bytes, limit %d", ErrChunkTooLarge, s.totalRead, s.maxTotal)
	}

	if n == 0 {
		// ErrUnexpectedEOF with zero bytes: the stream ended right at a
		// boundary on a reader that reports it this way.
		return Chunk{}, io.EOF
	}

	c := Chunk{Index: s.index, Data: s.buf[:n]}
	s.index++
	return c, nil
}

// TotalRead returns the number of payload bytes consumed so far.
func (s *Splitter) TotalRead() int64 { return s.totalRead }

// Count returns the number of chunks emitted so far.
func (s *Splitter) Count() int { return s.index }

// PlanChunks reports how many chunks an object of the given size will produce.
// Used by the gateway for Content-Length sanity checks and by tests.
func PlanChunks(objectSize, chunkSize int64) int {
	if chunkSize <= 0 || objectSize <= 0 {
		return 0
	}
	return int((objectSize + chunkSize - 1) / chunkSize)
}

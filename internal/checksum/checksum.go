// Package checksum centralises FlexStore's integrity primitives.
//
// Every chunk carries a SHA-256 computed by the gateway on ingest, re-verified
// by the storage node before the chunk becomes visible, and re-verified again
// on every read. Corruption therefore surfaces as an explicit error rather
// than as silently wrong bytes.
package checksum

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"regexp"
)

// Size is the length of a SHA-256 digest in hex characters.
const Size = sha256.Size * 2

// ErrMismatch is the sentinel wrapped by every corruption error so callers can
// distinguish "the bytes are wrong" from "the I/O failed".
var ErrMismatch = errors.New("checksum mismatch")

var hexPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Sum returns the lowercase hex SHA-256 of b.
func Sum(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// New returns a fresh SHA-256 hasher.
func New() hash.Hash { return sha256.New() }

// Hex renders a hasher's current digest as lowercase hex.
func Hex(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

// Valid reports whether s is a well-formed lowercase hex SHA-256.
func Valid(s string) bool { return hexPattern.MatchString(s) }

// Equal compares two hex digests in constant time. Overkill for corruption
// detection, but these digests double as ETags that clients can probe, and a
// constant-time compare costs nothing here.
func Equal(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// Verify checks actual against expected and returns an ErrMismatch-wrapped
// error naming the subject (a chunk ID, usually) when they differ.
func Verify(subject, expected, actual string) error {
	if Equal(expected, actual) {
		return nil
	}
	return fmt.Errorf("%w for %s: expected %s, computed %s", ErrMismatch, subject, expected, actual)
}

// Reader wraps r and accumulates a SHA-256 over everything read through it.
type Reader struct {
	r io.Reader
	h hash.Hash
	n int64
}

// NewReader returns a Reader that tees r into a SHA-256 hasher.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r, h: sha256.New()}
}

func (cr *Reader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	if n > 0 {
		// hash.Hash.Write never returns an error, per its documented contract.
		cr.h.Write(p[:n])
		cr.n += int64(n)
	}
	return n, err
}

// Sum returns the hex digest of everything read so far.
func (cr *Reader) Sum() string { return Hex(cr.h) }

// BytesRead returns how many bytes have passed through.
func (cr *Reader) BytesRead() int64 { return cr.n }

// Writer wraps w and accumulates a SHA-256 over everything written through it.
type Writer struct {
	w io.Writer
	h hash.Hash
	n int64
}

// NewWriter returns a Writer that tees writes into a SHA-256 hasher.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w, h: sha256.New()}
}

func (cw *Writer) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	if n > 0 {
		cw.h.Write(p[:n])
		cw.n += int64(n)
	}
	return n, err
}

// Sum returns the hex digest of everything written so far.
func (cw *Writer) Sum() string { return Hex(cw.h) }

// BytesWritten returns how many bytes have passed through.
func (cw *Writer) BytesWritten() int64 { return cw.n }

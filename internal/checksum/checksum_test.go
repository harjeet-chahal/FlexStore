package checksum

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
)

// emptySHA256 is the well-known digest of zero bytes. Hard-coding it catches a
// hasher that is silently not being fed.
const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func TestSumMatchesCryptoSHA256(t *testing.T) {
	payload := []byte("flexstore chunk payload")
	want := sha256.Sum256(payload)
	if got := Sum(payload); got != hex.EncodeToString(want[:]) {
		t.Fatalf("Sum = %s, want %s", got, hex.EncodeToString(want[:]))
	}
	if got := Sum(nil); got != emptySHA256 {
		t.Fatalf("Sum(nil) = %s, want %s", got, emptySHA256)
	}
}

func TestValid(t *testing.T) {
	cases := map[string]bool{
		emptySHA256:                  true,
		strings.ToUpper(emptySHA256): false, // uppercase is rejected: one canonical form only
		"":                           false,
		"abc":                        false,
		emptySHA256 + "0":            false,
		strings.Repeat("g", 64):      false,
	}
	for in, want := range cases {
		if got := Valid(in); got != want {
			t.Errorf("Valid(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestEqual(t *testing.T) {
	if !Equal(emptySHA256, emptySHA256) {
		t.Error("identical digests should compare equal")
	}
	if Equal(emptySHA256, strings.Repeat("a", 64)) {
		t.Error("different digests should not compare equal")
	}
	if Equal(emptySHA256, "short") {
		t.Error("different lengths should not compare equal")
	}
}

func TestVerifyWrapsErrMismatch(t *testing.T) {
	if err := Verify("chunk-1", emptySHA256, emptySHA256); err != nil {
		t.Fatalf("matching digests should verify: %v", err)
	}

	err := Verify("chunk-1", emptySHA256, strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("expected a mismatch error")
	}
	// Callers branch on ErrMismatch to decide "corrupt" vs "I/O failure", so
	// the wrapping must survive.
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("error does not wrap ErrMismatch: %v", err)
	}
	if !strings.Contains(err.Error(), "chunk-1") {
		t.Errorf("error should name the subject, got %q", err.Error())
	}
}

func TestReaderHashesEverythingItPassesThrough(t *testing.T) {
	payload := bytes.Repeat([]byte("stream"), 1000)
	cr := NewReader(bytes.NewReader(payload))

	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("Reader altered the payload")
	}
	if cr.Sum() != Sum(payload) {
		t.Fatalf("Reader digest = %s, want %s", cr.Sum(), Sum(payload))
	}
	if cr.BytesRead() != int64(len(payload)) {
		t.Fatalf("BytesRead = %d, want %d", cr.BytesRead(), len(payload))
	}
}

func TestWriterHashesEverythingItPassesThrough(t *testing.T) {
	payload := bytes.Repeat([]byte("write"), 777)
	var sink bytes.Buffer
	cw := NewWriter(&sink)

	if _, err := cw.Write(payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !bytes.Equal(sink.Bytes(), payload) {
		t.Fatal("Writer altered the payload")
	}
	if cw.Sum() != Sum(payload) {
		t.Fatalf("Writer digest = %s, want %s", cw.Sum(), Sum(payload))
	}
	if cw.BytesWritten() != int64(len(payload)) {
		t.Fatalf("BytesWritten = %d, want %d", cw.BytesWritten(), len(payload))
	}
}

func TestWriterOnlyHashesBytesActuallyWritten(t *testing.T) {
	// A writer that accepts a partial write must not hash the bytes the
	// underlying sink rejected -- otherwise a truncated chunk would still
	// produce a "valid" digest.
	sink := &shortWriter{limit: 4}
	cw := NewWriter(sink)
	n, err := cw.Write([]byte("abcdefgh"))
	if err == nil {
		t.Fatal("expected the short write to surface an error")
	}
	if n != 4 {
		t.Fatalf("wrote %d bytes, want 4", n)
	}
	if cw.Sum() != Sum([]byte("abcd")) {
		t.Fatalf("digest covers bytes that were never written: %s", cw.Sum())
	}
}

type shortWriter struct {
	limit int
	buf   bytes.Buffer
}

func (s *shortWriter) Write(p []byte) (int, error) {
	if len(p) > s.limit {
		s.buf.Write(p[:s.limit])
		return s.limit, io.ErrShortWrite
	}
	return s.buf.Write(p)
}

package chunking

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestSplitterExactMultipleOfChunkSize(t *testing.T) {
	// 4 chunks exactly: the splitter must emit 4 and then EOF, not 4 plus an
	// empty 5th. Off-by-one here would create a zero-length chunk in metadata.
	data := bytes.Repeat([]byte("A"), 40)
	s, err := NewSplitter(bytes.NewReader(data), 10, 0)
	if err != nil {
		t.Fatalf("NewSplitter: %v", err)
	}

	var got [][]byte
	for {
		c, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, append([]byte(nil), c.Data...))
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(got))
	}
	for i, c := range got {
		if len(c) != 10 {
			t.Errorf("chunk %d: len %d, want 10", i, len(c))
		}
	}
	if s.TotalRead() != 40 {
		t.Errorf("TotalRead = %d, want 40", s.TotalRead())
	}
}

func TestSplitterShortFinalChunk(t *testing.T) {
	data := bytes.Repeat([]byte("B"), 25)
	s, _ := NewSplitter(bytes.NewReader(data), 10, 0)

	var sizes []int
	for {
		c, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		sizes = append(sizes, len(c.Data))
	}
	want := []int{10, 10, 5}
	if len(sizes) != len(want) {
		t.Fatalf("got %v, want %v", sizes, want)
	}
	for i := range want {
		if sizes[i] != want[i] {
			t.Fatalf("got %v, want %v", sizes, want)
		}
	}
}

func TestSplitterEmptyStreamProducesNoChunks(t *testing.T) {
	// An empty object is legal: it becomes a COMPLETE object with 0 chunks.
	s, _ := NewSplitter(bytes.NewReader(nil), 10, 0)
	if _, err := s.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF on empty stream, got %v", err)
	}
	if s.Count() != 0 {
		t.Fatalf("Count = %d, want 0", s.Count())
	}
}

func TestSplitterIndicesAreSequential(t *testing.T) {
	s, _ := NewSplitter(bytes.NewReader(bytes.Repeat([]byte("x"), 33)), 8, 0)
	want := 0
	for {
		c, err := s.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if c.Index != want {
			t.Fatalf("chunk index = %d, want %d", c.Index, want)
		}
		want++
	}
	if want != 5 {
		t.Fatalf("emitted %d chunks, want 5", want)
	}
}

func TestSplitterReusesItsBuffer(t *testing.T) {
	// The memory bound depends on the buffer being reused, so assert it
	// directly rather than trusting the doc comment.
	s, _ := NewSplitter(bytes.NewReader(bytes.Repeat([]byte("z"), 30)), 10, 0)
	c1, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	first := &c1.Data[0]
	c2, err := s.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if &c2.Data[0] != first {
		t.Fatal("splitter allocated a new buffer per chunk; memory is not bounded")
	}
}

func TestSplitterEnforcesMaxTotal(t *testing.T) {
	s, _ := NewSplitter(bytes.NewReader(bytes.Repeat([]byte("q"), 100)), 10, 25)
	var err error
	for i := 0; i < 10; i++ {
		if _, err = s.Next(); err != nil {
			break
		}
	}
	if !errors.Is(err, ErrChunkTooLarge) {
		t.Fatalf("expected ErrChunkTooLarge, got %v", err)
	}
}

func TestSplitterPropagatesReaderErrors(t *testing.T) {
	sentinel := errors.New("disk on fire")
	r := io.MultiReader(strings.NewReader("hello"), errReader{sentinel})
	s, _ := NewSplitter(r, 100, 0)
	if _, err := s.Next(); !errors.Is(err, sentinel) {
		t.Fatalf("expected the underlying error to surface, got %v", err)
	}
}

func TestSplitterRejectsNonPositiveChunkSize(t *testing.T) {
	if _, err := NewSplitter(bytes.NewReader(nil), 0, 0); err == nil {
		t.Fatal("expected an error for chunkSize=0")
	}
}

func TestPlanChunks(t *testing.T) {
	cases := []struct {
		object, chunk int64
		want          int
	}{
		{0, 8, 0},
		{1, 8, 1},
		{8, 8, 1},
		{9, 8, 2},
		{1 << 30, 8 << 20, 128},
		{-5, 8, 0},
		{100, 0, 0},
	}
	for _, c := range cases {
		if got := PlanChunks(c.object, c.chunk); got != c.want {
			t.Errorf("PlanChunks(%d, %d) = %d, want %d", c.object, c.chunk, got, c.want)
		}
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

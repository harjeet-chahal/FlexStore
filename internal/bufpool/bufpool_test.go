package bufpool

import (
	"sync"
	"testing"
)

func TestGetReturnsRequestedLength(t *testing.T) {
	p := New(1024)
	b := p.Get(100)
	if len(b) != 100 {
		t.Fatalf("len = %d, want 100", len(b))
	}
	if cap(b) != 1024 {
		t.Fatalf("cap = %d, want the pooled size 1024", cap(b))
	}
}

func TestOversizedRequestIsNotPooled(t *testing.T) {
	p := New(64)
	b := p.Get(4096)
	if len(b) != 4096 {
		t.Fatalf("len = %d, want 4096", len(b))
	}
	// Returning it must not poison the pool with an oversized buffer.
	p.Put(b)
	again := p.Get(64)
	if cap(again) != 64 {
		t.Fatalf("cap = %d after returning an oversized buffer, want 64", cap(again))
	}
}

func TestWrongSizedBuffersAreDropped(t *testing.T) {
	p := New(64)
	p.Put(make([]byte, 8))
	if got := cap(p.Get(64)); got != 64 {
		t.Fatalf("cap = %d after returning an undersized buffer, want 64", got)
	}
}

func TestConcurrentUseIsSafe(t *testing.T) {
	p := New(4096)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				b := p.Get(4096)
				b[0] = byte(j)
				p.Put(b)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkChunkBuffer contrasts the per-request allocation the data paths used
// to make with the pooled one, at the default 8 MiB chunk size. Run with
// -benchmem; the allocation columns are the point, not ns/op.
func BenchmarkChunkBuffer(b *testing.B) {
	const chunkSize = 8 << 20

	b.Run("allocate-per-request", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := make([]byte, chunkSize)
			buf[0] = byte(i)
			_ = buf
		}
	})

	b.Run("pooled", func(b *testing.B) {
		p := New(chunkSize)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			buf := p.Get(chunkSize)
			buf[0] = byte(i)
			p.Put(buf)
		}
	})

	// Concurrency is where it matters: a single-threaded loop reuses the same
	// cache-warm allocation, which flatters the unpooled case.
	b.Run("allocate-per-request-parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := make([]byte, chunkSize)
				buf[0] = 1
				_ = buf
			}
		})
	})

	b.Run("pooled-parallel", func(b *testing.B) {
		p := New(chunkSize)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buf := p.Get(chunkSize)
				buf[0] = 1
				p.Put(buf)
			}
		})
	})
}

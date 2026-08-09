// Package bufpool recycles chunk-sized buffers across requests.
//
// Both data paths already reuse a single buffer *within* one request: the
// upload splitter fills one buffer per chunk, and the download path verifies
// one chunk at a time into one buffer. So a 1 GiB transfer costs one 8 MiB
// allocation, not 1 GiB. What was left was the allocation *per request*: at 32
// concurrent 8 MiB transfers that is 256 MiB of garbage produced and collected
// every request cycle, purely to hold bytes that are overwritten immediately.
//
// The pool exists because chunk buffers are unusually well suited to one:
// they are all the same size (the deployment's chunk size), they are large
// enough that the allocation genuinely costs something, and their lifetime is
// exactly one request, so they are returned promptly rather than pinned.
//
// It is deliberately not a general-purpose allocator. Buffers that do not match
// the pooled size are dropped rather than kept, because a pool that holds a
// mixture of sizes either wastes memory on the large ones or fails to satisfy
// requests with the small ones -- and silently returning a too-small buffer to
// the verify path would be a correctness bug, not a performance one.
package bufpool

import "sync"

// Pool hands out byte slices of a fixed capacity.
type Pool struct {
	size int
	pool sync.Pool
}

// New returns a pool of buffers with the given capacity. A size of zero or less
// yields a pool that allocates on every Get, so callers do not need to special
// case an unconfigured chunk size.
func New(size int) *Pool {
	p := &Pool{size: size}
	p.pool = sync.Pool{New: func() any {
		// Stored as *[]byte rather than []byte: putting a slice into a
		// sync.Pool boxes the slice header on every Put, which is an allocation
		// in the very path that exists to avoid one.
		b := make([]byte, size)
		return &b
	}}
	return p
}

// Get returns a buffer with at least n bytes of capacity, and length n.
//
// A request larger than the pool's size is satisfied by a fresh allocation
// rather than by growing the pooled buffers: oversized chunks are the
// exception (a deployment whose chunk size changed under a live object), and
// letting one resize the pooled buffers would permanently inflate every
// subsequent request.
func (p *Pool) Get(n int) []byte {
	if n > p.size {
		return make([]byte, n)
	}
	bp, ok := p.pool.Get().(*[]byte)
	if !ok {
		// Unreachable: only Put stores into this pool and it stores *[]byte.
		// Handled rather than asserted so a future change cannot turn a type
		// mistake into a panic on the data path.
		return make([]byte, n)
	}
	b := *bp
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}

// Put returns a buffer to the pool. Buffers of the wrong capacity are dropped,
// which is what keeps every pooled buffer interchangeable.
func (p *Pool) Put(b []byte) {
	if cap(b) != p.size || p.size <= 0 {
		return
	}
	b = b[:p.size]
	p.pool.Put(&b)
}

// Size reports the capacity this pool recycles.
func (p *Pool) Size() int { return p.size }

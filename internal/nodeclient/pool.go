// Package nodeclient is a shared, lazily-dialed connection pool for storage
// nodes. Both the gateway (data path) and the coordinator (garbage collection)
// need one, and re-dialing per RPC would dominate latency for 8 MiB chunks.
package nodeclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/observability"
)

// ErrClosed is returned once the pool has been shut down.
var ErrClosed = errors.New("node client pool is closed")

// Pool caches one gRPC connection per storage-node address.
//
// gRPC connections are multiplexed and self-healing (they reconnect in the
// background), so one per address is both sufficient and cheap. The pool keys
// on address rather than node ID so a node that moves is treated as a new
// endpoint automatically.
type Pool struct {
	mu     sync.RWMutex
	conns  map[string]*grpc.ClientConn
	closed bool
	opts   []grpc.DialOption
}

// NewPool builds a pool with sensible transport defaults.
func NewPool(maxRecvBytes int) *Pool {
	return &Pool{
		conns: make(map[string]*grpc.ClientConn),
		opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(maxRecvBytes),
				grpc.MaxCallSendMsgSize(maxRecvBytes),
			),
			// Keepalives surface a silently dead node (container killed, network
			// partition) as a failed RPC instead of a hang until the OS TCP
			// timeout, which can be minutes.
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                30 * time.Second,
				Timeout:             10 * time.Second,
				PermitWithoutStream: true,
			}),
			// gRPC's default reconnect backoff caps at 120s. That is far too
			// long here: a storage node that comes back must be usable within
			// about a heartbeat interval, or the coordinator will keep placing
			// chunks on a node the gateway still cannot reach, and every such
			// write silently lands one replica short of the replication factor.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  200 * time.Millisecond,
					Multiplier: 1.6,
					Jitter:     0.2,
					MaxDelay:   5 * time.Second,
				},
				MinConnectTimeout: 5 * time.Second,
			}),
			grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor()),
			grpc.WithChainStreamInterceptor(observability.StreamClientInterceptor()),
		},
	}
}

// Client returns a StorageNodeService client for addr, dialing on first use.
func (p *Pool) Client(addr string) (flexstorev1.StorageNodeServiceClient, error) {
	conn, err := p.conn(addr)
	if err != nil {
		return nil, err
	}
	return flexstorev1.NewStorageNodeServiceClient(conn), nil
}

func (p *Pool) conn(addr string) (*grpc.ClientConn, error) {
	if addr == "" {
		return nil, errors.New("empty storage node address")
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrClosed
	}
	if c, ok := p.conns[addr]; ok {
		p.mu.RUnlock()
		return c, nil
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	// Re-check: another goroutine may have dialed while we waited for the lock.
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("dial storage node %s: %w", addr, err)
	}
	p.conns[addr] = c
	return c, nil
}

// Close shuts every pooled connection. Safe to call more than once.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true

	var errs []error
	for addr, c := range p.conns {
		if err := c.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", addr, err))
		}
	}
	p.conns = map[string]*grpc.ClientConn{}
	return errors.Join(errs...)
}

// WaitReady blocks until the connection to addr is usable or ctx expires. Used
// by tests; the data path relies on gRPC's own wait-for-ready semantics.
func (p *Pool) WaitReady(ctx context.Context, addr string) error {
	c, err := p.conn(addr)
	if err != nil {
		return err
	}
	c.Connect()
	for {
		s := c.GetState()
		if s.String() == "READY" {
			return nil
		}
		if !c.WaitForStateChange(ctx, s) {
			return ctx.Err()
		}
	}
}

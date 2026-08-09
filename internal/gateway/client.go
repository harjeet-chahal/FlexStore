// Package gateway implements the client-facing S3-style HTTP API and the
// streaming data path between clients and storage nodes.
package gateway

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	flexstorev1 "github.com/harjeetschahal/flexstore/gen/flexstorev1"
	"github.com/harjeetschahal/flexstore/internal/observability"
	"github.com/harjeetschahal/flexstore/internal/retry"
)

// CoordinatorClient wraps the control-plane stub with per-call deadlines and a
// bounded retry policy for transient failures.
//
// Only idempotent, side-effect-free-on-failure calls are retried. Notably
// CommitChunk and CompleteUpload are *not* retried here: they are safe to
// repeat server-side, but a retry that masks a durability rejection would
// defeat the point of the policy, so the caller decides.
type CoordinatorClient struct {
	stub    flexstorev1.CoordinatorServiceClient
	conn    *grpc.ClientConn
	timeout time.Duration
	policy  retry.Policy
}

// NewCoordinatorClient dials the coordinator lazily.
func NewCoordinatorClient(addr string, timeout time.Duration) (*CoordinatorClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithChainUnaryInterceptor(observability.UnaryClientInterceptor()),
	)
	if err != nil {
		return nil, err
	}
	return &CoordinatorClient{
		stub:    flexstorev1.NewCoordinatorServiceClient(conn),
		conn:    conn,
		timeout: timeout,
		policy:  retry.DefaultPolicy(),
	}, nil
}

// Close releases the connection.
func (c *CoordinatorClient) Close() error { return c.conn.Close() }

// Raw exposes the underlying stub for calls that manage their own semantics.
func (c *CoordinatorClient) Raw() flexstorev1.CoordinatorServiceClient { return c.stub }

// Healthy reports whether the coordinator answers a trivial RPC. Used by
// /readyz.
func (c *CoordinatorClient) Healthy(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	_, err := c.stub.ListNodes(ctx, &flexstorev1.ListNodesRequest{HealthyOnly: true})
	return err
}

// call runs fn with a deadline and bounded retries on transient codes.
func (c *CoordinatorClient) call(ctx context.Context, retryable bool, fn func(context.Context) error) error {
	policy := c.policy
	if !retryable {
		policy.MaxAttempts = 1
	}
	return retry.Do(ctx, policy, func(ctx context.Context, _ int) error {
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		defer cancel()
		err := fn(callCtx)
		if err == nil {
			return nil
		}
		if !isTransient(err) {
			return retry.Permanent(err)
		}
		return err
	})
}

// isTransient decides what is worth retrying. Deliberately narrow: retrying a
// ResourceExhausted (placement could not satisfy durability) would just burn
// time before returning the same answer.
func isTransient(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}

// BeginUpload opens a new object version.
func (c *CoordinatorClient) BeginUpload(ctx context.Context, req *flexstorev1.BeginUploadRequest) (*flexstorev1.BeginUploadResponse, error) {
	var resp *flexstorev1.BeginUploadResponse
	err := c.call(ctx, true, func(ctx context.Context) error {
		r, err := c.stub.BeginUpload(ctx, req)
		resp = r
		return err
	})
	return resp, err
}

// AllocateChunk runs placement for one chunk.
func (c *CoordinatorClient) AllocateChunk(ctx context.Context, req *flexstorev1.AllocateChunkRequest) (*flexstorev1.AllocateChunkResponse, error) {
	var resp *flexstorev1.AllocateChunkResponse
	err := c.call(ctx, true, func(ctx context.Context) error {
		r, err := c.stub.AllocateChunk(ctx, req)
		resp = r
		return err
	})
	return resp, err
}

// CommitChunk records replica outcomes. Not retried: see type doc.
func (c *CoordinatorClient) CommitChunk(ctx context.Context, req *flexstorev1.CommitChunkRequest) (*flexstorev1.CommitChunkResponse, error) {
	var resp *flexstorev1.CommitChunkResponse
	err := c.call(ctx, false, func(ctx context.Context) error {
		r, err := c.stub.CommitChunk(ctx, req)
		resp = r
		return err
	})
	return resp, err
}

// CompleteUpload publishes the new version.
func (c *CoordinatorClient) CompleteUpload(ctx context.Context, req *flexstorev1.CompleteUploadRequest) (*flexstorev1.CompleteUploadResponse, error) {
	var resp *flexstorev1.CompleteUploadResponse
	err := c.call(ctx, false, func(ctx context.Context) error {
		r, err := c.stub.CompleteUpload(ctx, req)
		resp = r
		return err
	})
	return resp, err
}

// AbortUpload cancels an in-flight upload. Always retried, since it runs on
// error paths where the coordinator may also be struggling, and leaking an
// UPLOADING row costs real storage until the reaper notices.
func (c *CoordinatorClient) AbortUpload(ctx context.Context, objectID, reason string) error {
	return c.call(ctx, true, func(ctx context.Context) error {
		_, err := c.stub.AbortUpload(ctx, &flexstorev1.AbortUploadRequest{
			ObjectId: objectID, Reason: reason,
		})
		return err
	})
}

// GetObject fetches metadata plus chunk layout.
func (c *CoordinatorClient) GetObject(ctx context.Context, bucket, key string) (*flexstorev1.GetObjectResponse, error) {
	var resp *flexstorev1.GetObjectResponse
	err := c.call(ctx, true, func(ctx context.Context) error {
		r, err := c.stub.GetObject(ctx, &flexstorev1.GetObjectRequest{Bucket: bucket, Key: key})
		resp = r
		return err
	})
	return resp, err
}

// HeadObject fetches metadata only.
func (c *CoordinatorClient) HeadObject(ctx context.Context, bucket, key string) (*flexstorev1.HeadObjectResponse, error) {
	var resp *flexstorev1.HeadObjectResponse
	err := c.call(ctx, true, func(ctx context.Context) error {
		r, err := c.stub.HeadObject(ctx, &flexstorev1.HeadObjectRequest{Bucket: bucket, Key: key})
		resp = r
		return err
	})
	return resp, err
}

// ReportCorruptReplica implements CorruptionReporter. Retried, because the
// report is what schedules the repair -- losing it to a transient blip would
// leave a bad replica in service indefinitely.
func (c *CoordinatorClient) ReportCorruptReplica(ctx context.Context, chunkID, nodeID, detail string) error {
	return c.call(ctx, true, func(ctx context.Context) error {
		_, err := c.stub.ReportCorruptReplica(ctx, &flexstorev1.ReportCorruptReplicaRequest{
			ChunkId: chunkID, NodeId: nodeID, Detail: detail,
		})
		return err
	})
}

// DeleteObject retires the current version.
func (c *CoordinatorClient) DeleteObject(ctx context.Context, bucket, key string) (*flexstorev1.DeleteObjectResponse, error) {
	var resp *flexstorev1.DeleteObjectResponse
	err := c.call(ctx, true, func(ctx context.Context) error {
		r, err := c.stub.DeleteObject(ctx, &flexstorev1.DeleteObjectRequest{Bucket: bucket, Key: key})
		resp = r
		return err
	})
	return resp, err
}

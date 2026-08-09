package observability

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// requestIDMetadataKey propagates the gateway's request ID into internal RPCs
// so a single client request can be traced across all three services.
const requestIDMetadataKey = "x-flexstore-request-id"

// UnaryServerInterceptor records metrics, propagates the request ID into the
// handler context and logs failures. Successful calls are logged at debug to
// keep the data path quiet at default verbosity.
func UnaryServerInterceptor(m *Metrics, log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = adoptRequestID(ctx, log, info.FullMethod)
		start := time.Now()
		resp, err := handler(ctx, req)
		observeGRPC(m, log, ctx, info.FullMethod, start, err)
		return resp, err
	}
}

// StreamServerInterceptor is the streaming counterpart of
// UnaryServerInterceptor. Chunk transfers are streams, so without this the
// data path would be invisible in metrics.
func StreamServerInterceptor(m *Metrics, log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := adoptRequestID(ss.Context(), log, info.FullMethod)
		start := time.Now()
		err := handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
		observeGRPC(m, log, ctx, info.FullMethod, start, err)
		return err
	}
}

// UnaryClientInterceptor injects the request ID into outgoing calls.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(injectRequestID(ctx), method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor injects the request ID into outgoing streams.
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
		streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(injectRequestID(ctx), desc, cc, method, opts...)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func injectRequestID(ctx context.Context) context.Context {
	id := RequestIDFrom(ctx)
	if id == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, requestIDMetadataKey, id)
}

func adoptRequestID(ctx context.Context, log *slog.Logger, method string) context.Context {
	id := RequestIDFrom(ctx)
	if id == "" {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(requestIDMetadataKey); len(vals) > 0 && vals[0] != "" {
				id = vals[0]
			}
		}
	}
	if id != "" {
		ctx = WithRequestID(ctx, id)
	}
	return WithLogger(ctx, log.With(
		slog.String("rpc", method),
		slog.String("request_id", id),
	))
}

func observeGRPC(m *Metrics, log *slog.Logger, ctx context.Context, method string, start time.Time, err error) {
	code := status.Code(err)
	m.GRPCRequestsTotal.WithLabelValues(method, code.String()).Inc()
	m.GRPCRequestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())

	l := LoggerFrom(ctx, log)
	switch {
	case err == nil:
		l.Debug("rpc ok", slog.Duration("duration", time.Since(start)))
	case code == codes.NotFound || code == codes.InvalidArgument || code == codes.AlreadyExists || code == codes.Canceled:
		// Expected client-side outcomes: noisy at error level, useful at info.
		l.Info("rpc rejected",
			slog.String("code", code.String()),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
	default:
		l.Error("rpc failed",
			slog.String("code", code.String()),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
	}
}

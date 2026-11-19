package server

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/metrics"
)

// slowRequest is the handler duration above which a request is logged.
// It is two orders of magnitude above the local path and still well
// under a client attempt's 50 ms deadline, so the log stays quiet while
// the node is healthy and names the requests that were not.
const slowRequest = 100 * time.Millisecond

// Gate decides whether a node may serve at all, independently of which
// key a request names. Returning an error rejects the request before
// the handler runs.
//
// Phase 4 has no gate. Phase 10 installs one that returns ErrFenced
// while the node has lost its etcd lease, which is what stops it
// serving keys that have been handed to another node (FR-12).
type Gate func() error

// Interceptors builds the server-side interceptors for one node:
// metrics, slow-request logging, panic recovery and the gate.
type Interceptors struct {
	metrics *metrics.Metrics
	log     *slog.Logger
	clock   clock.Clock
	gate    Gate
}

// NewInterceptors returns the interceptors for a node. gate may be nil,
// which means the node always serves.
func NewInterceptors(m *metrics.Metrics, log *slog.Logger, c clock.Clock, gate Gate) *Interceptors {
	return &Interceptors{metrics: m, log: log, clock: c, gate: gate}
}

// Unary returns the unary server interceptor.
func (i *Interceptors) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := i.clock.Now()
		method := shortMethod(info.FullMethod)

		defer func() {
			if r := recover(); r != nil {
				err = i.recovered(method, r)
				resp = nil
			}
			i.observe(method, start, err)
		}()

		if err := i.closed(); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// Stream returns the stream server interceptor. A stream is measured
// over its whole life, so its latency is the duration of the stream and
// not of one message.
func (i *Interceptors) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		start := i.clock.Now()
		method := shortMethod(info.FullMethod)

		defer func() {
			if r := recover(); r != nil {
				err = i.recovered(method, r)
			}
			i.observe(method, start, err)
		}()

		if err := i.closed(); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// closed reports the gate's refusal, mapped to a status.
func (i *Interceptors) closed() error {
	if i.gate == nil {
		return nil
	}
	if err := i.gate(); err != nil {
		return toStatus(err)
	}
	return nil
}

// recovered turns a panic into an INTERNAL status and logs it with the
// stack. One bad request must not take the node down, since the keys it
// holds would move and have to be repaired.
func (i *Interceptors) recovered(method string, r any) error {
	i.metrics.Panics.Inc()
	i.log.Error("handler panicked",
		"method", method,
		"panic", fmt.Sprint(r),
		"stack", string(debug.Stack()),
	)
	return status.Error(codes.Internal, "internal error")
}

// observe records one finished call and logs it if it was slow.
func (i *Interceptors) observe(method string, start time.Time, err error) {
	elapsed := i.clock.Now().Sub(start)
	code := status.Code(err)

	i.metrics.Requests.WithLabelValues(method, code.String()).Inc()
	i.metrics.Latency.WithLabelValues(method).Observe(elapsed.Seconds())

	if elapsed >= slowRequest {
		i.log.Warn("slow request",
			"method", method,
			"duration_ms", elapsed.Milliseconds(),
			"code", code.String(),
		)
	}
}

// shortMethod turns "/ringcache.v1.Cache/Get" into "Cache/Get", which
// keeps the metric label readable without losing which service it names.
func shortMethod(full string) string {
	service, method := path.Split(full)
	service = path.Base(path.Clean(service))
	if service == "." || service == "/" || method == "" {
		return full
	}
	// Drop the proto package, keeping the service's own name.
	if dot := strings.LastIndexByte(service, '.'); dot >= 0 {
		service = service[dot+1:]
	}
	return service + "/" + method
}

// Package node wires one cache node together: store, sweeper, gRPC
// server and metrics endpoint, started and stopped as a unit.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/hlc"
	"github.com/rakeshlakshmanan/ringcache/internal/metrics"
	"github.com/rakeshlakshmanan/ringcache/internal/server"
	"github.com/rakeshlakshmanan/ringcache/internal/store"
)

const (
	// gracefulStopTimeout is how long GracefulStop may drain in-flight
	// requests before the server is stopped outright. It is long enough
	// for any request that is making progress, since a client attempt
	// times out after 50 ms.
	gracefulStopTimeout = 5 * time.Second

	// clientPingInterval is the shortest client keepalive ping the
	// server tolerates. gRPC otherwise closes connections that ping
	// "too often", which an idle cluster's peers do.
	clientPingInterval = 10 * time.Second
)

// Options are the collaborators a test may replace. The zero value is
// what the binary runs with.
type Options struct {
	// Clock is the time source for the store, the sweeper and the
	// request timers. Defaults to clock.Real.
	Clock clock.Clock
	// Logger receives the node's logs. Defaults to slog.Default.
	Logger *slog.Logger
	// Listener is an already-open gRPC listener. When it is nil the node
	// listens on Config.Listen itself. The in-process test cluster
	// passes a bufconn listener here.
	Listener net.Listener
	// Ownership decides which keys this node serves. Defaults to the
	// single-node owner that accepts everything.
	Ownership server.Ownership
	// Gate rejects every request while it returns an error, whatever key
	// the request names. Phase 10 passes the fencing gate.
	Gate server.Gate
}

// Node is one cache node. Build it with New, run it with Start and
// release it with Close.
type Node struct {
	cfg config.Config
	log *slog.Logger
	c   clock.Clock

	store   *store.Store
	metrics *metrics.Metrics
	gate    server.Gate

	grpc     *grpc.Server
	listener net.Listener
	// ownListener is true when Node opened the listener and must close
	// it; a listener supplied through Options belongs to its caller.
	ownListener bool

	metricsSrv *http.Server
	metricsLn  net.Listener

	// serving gates /healthz. Phase 10 clears it while the node is
	// fenced, which is what takes a fenced node out of a load balancer.
	serving atomic.Bool

	closeOnce sync.Once
}

// New builds a node from cfg. It opens the listeners so that a failure
// to bind is reported before Start, and so a caller that asked for port
// 0 can read the chosen address from Addr.
func New(cfg config.Config, opts Options) (*Node, error) {
	n := &Node{
		cfg:  cfg,
		log:  opts.Logger,
		c:    opts.Clock,
		gate: opts.Gate,
	}
	if n.log == nil {
		n.log = slog.Default()
	}
	n.log = n.log.With("node_id", cfg.NodeID)
	if n.c == nil {
		n.c = clock.Real{}
	}

	n.store = store.New(hlc.New(cfg.NodeID, n.c), n.c)
	n.metrics = metrics.New(cfg.NodeID)
	n.metrics.StoreSizes(cfg.NodeID, n.store.Len, n.store.Bytes)

	owner := opts.Ownership
	if owner == nil {
		owner = server.AllOwner(cfg.NodeID, cfg.Advertise)
	}

	in := server.NewInterceptors(n.metrics, n.log, n.c, n.gate)
	n.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(config.MaxGRPCMessage),
		grpc.MaxSendMsgSize(config.MaxGRPCMessage),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: clientPingInterval,
			// Peers hold idle streams open between replication bursts, so
			// a ping with no active stream has to be allowed.
			PermitWithoutStream: true,
		}),
		grpc.ChainUnaryInterceptor(in.Unary()),
		grpc.ChainStreamInterceptor(in.Stream()),
	)
	ringcachev1.RegisterCacheServer(n.grpc, server.NewCacheServer(cfg.NodeID, n.store, owner))
	if cfg.Reflection {
		reflection.Register(n.grpc)
	}

	if err := n.listen(opts.Listener); err != nil {
		return nil, err
	}
	return n, nil
}

// listen opens the gRPC and metrics listeners, closing whatever it
// opened if the second one fails.
func (n *Node) listen(supplied net.Listener) error {
	if supplied != nil {
		n.listener = supplied
	} else {
		ln, err := net.Listen("tcp", n.cfg.Listen)
		if err != nil {
			return fmt.Errorf("node: listen on %s: %w", n.cfg.Listen, err)
		}
		n.listener, n.ownListener = ln, true
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(n.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", n.healthz)

	ln, err := net.Listen("tcp", n.cfg.MetricsListen)
	if err != nil {
		n.closeListener()
		return fmt.Errorf("node: listen on %s: %w", n.cfg.MetricsListen, err)
	}
	n.metricsLn = ln
	n.metricsSrv = &http.Server{
		Handler: mux,
		// A scrape that stalls must not hold a connection forever.
		ReadHeaderTimeout: 5 * time.Second,
	}
	return nil
}

// healthz reports whether the node is serving. Phase 10 makes it answer
// 503 while the node is fenced, so an operator and a load balancer see
// the same state the gate enforces.
func (n *Node) healthz(w http.ResponseWriter, _ *http.Request) {
	if !n.serving.Load() {
		http.Error(w, "not serving", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// Addr returns the address the gRPC server is listening on. With a
// port of 0 in the config this is the port the kernel chose.
func (n *Node) Addr() net.Addr { return n.listener.Addr() }

// MetricsAddr returns the address the metrics server is listening on.
func (n *Node) MetricsAddr() net.Addr { return n.metricsLn.Addr() }

// Store returns the node's store. Tests and later phases' components
// read it; nothing outside the node writes to it directly.
func (n *Node) Store() *store.Store { return n.store }

// Metrics returns the node's collectors, for tests and for the
// components later phases attach.
func (n *Node) Metrics() *metrics.Metrics { return n.metrics }

// Start runs the node until ctx is cancelled or a server fails, then
// shuts everything down and returns. It blocks.
func (n *Node) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)
	n.serving.Store(true)

	g.Go(func() error {
		n.store.RunSweeper(gctx)
		return nil
	})
	g.Go(func() error {
		if err := n.grpc.Serve(n.listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("node: grpc server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		if err := n.metricsSrv.Serve(n.metricsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("node: metrics server: %w", err)
		}
		return nil
	})
	// Shutdown is its own goroutine rather than a deferred call, so the
	// servers are stopped as soon as either the caller cancels or one of
	// them fails. Without it the group would wait for servers that are
	// still happily serving.
	g.Go(func() error {
		<-gctx.Done()
		n.shutdown()
		return nil
	})

	n.log.Info("node started",
		"addr", n.Addr().String(),
		"advertise", n.cfg.Advertise,
		"metrics_addr", n.MetricsAddr().String(),
	)

	err := g.Wait()
	n.log.Info("node stopped")
	return err
}

// shutdown stops the servers. It is safe to call more than once.
func (n *Node) shutdown() {
	n.closeOnce.Do(func() {
		n.serving.Store(false)

		shutCtx, cancel := context.WithTimeout(context.Background(), gracefulStopTimeout)
		defer cancel()
		if err := n.metricsSrv.Shutdown(shutCtx); err != nil {
			n.log.Warn("metrics server shutdown", "error", err)
		}

		// GracefulStop lets in-flight requests finish, but it waits
		// indefinitely, so a wedged handler would hang the process. Stop
		// after the timeout bounds that.
		done := make(chan struct{})
		go func() {
			n.grpc.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(gracefulStopTimeout):
			n.log.Warn("graceful stop timed out, stopping now")
			n.grpc.Stop()
			<-done
		}
	})
}

// Close releases the node's resources. It is safe to call after Start
// has returned, and safe to call more than once.
func (n *Node) Close() error {
	n.shutdown()
	n.closeListener()
	return nil
}

func (n *Node) closeListener() {
	// GracefulStop and Serve already close the gRPC listener; this only
	// matters when New succeeded and Start was never called.
	if n.ownListener && n.listener != nil {
		_ = n.listener.Close()
	}
	if n.metricsLn != nil {
		_ = n.metricsLn.Close()
	}
}

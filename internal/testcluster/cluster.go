// Package testcluster runs several cache nodes in one process, so a
// test can exercise a real cluster over real gRPC without containers.
//
// Nothing in the project keeps package-level state, which is what makes
// this possible: each node has its own store, clock, metrics registry
// and listeners.
package testcluster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/membership"
	"github.com/rakeshlakshmanan/ringcache/internal/node"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

// readyTimeout is how long Start waits for every node to answer.
const readyTimeout = 20 * time.Second

// Options configure a cluster.
type Options struct {
	// Clock is shared by every node. A nil clock gives each node its own
	// real clock.
	Clock clock.Clock
	// Logger receives the nodes' logs. Nil discards them, which is what
	// a passing test wants.
	Logger *slog.Logger
	// ClientOptions are passed to the cluster's client.
	ClientOptions []client.Option
}

// Cluster is a set of nodes serving one keyspace from a static member
// list, with a client already routing to them.
type Cluster struct {
	t   *testing.T
	mu  sync.Mutex
	ids []string

	nodes   map[string]*entry
	members []ring.Member
	client  *client.Client
}

// entry is one running node and the handle to stop it.
type entry struct {
	node   *node.Node
	cancel context.CancelFunc
	done   chan error
	addr   string
	// stopped is set once the node has been taken down, so Cleanup does
	// not try again and a test can ask.
	stopped bool
}

// Start runs n nodes named node-1 to node-n and returns the cluster.
// Everything is stopped when the test ends.
//
// Listeners are opened first, because the static member list has to
// name the addresses the nodes will serve on, and only the kernel knows
// which ports those are.
func Start(t *testing.T, n int, opts Options) *Cluster {
	t.Helper()

	if n < 1 {
		t.Fatalf("testcluster: asked for %d nodes", n)
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	c := &Cluster{t: t, nodes: make(map[string]*entry, n)}

	listeners := make([]net.Listener, n)
	for i := range n {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("testcluster: listen: %v", err)
		}
		listeners[i] = ln

		id := fmt.Sprintf("node-%d", i+1)
		c.ids = append(c.ids, id)
		c.members = append(c.members, ring.Member{ID: id, Addr: ln.Addr().String()})
	}

	// Cleanup is registered before the nodes start, so a failure part
	// way through still releases what was already running.
	t.Cleanup(c.stopAll)

	for i, id := range c.ids {
		src, err := membership.NewStatic(c.members)
		if err != nil {
			t.Fatalf("testcluster: membership: %v", err)
		}
		c.startNode(id, listeners[i], src, opts)
	}

	c.waitReady()
	c.client = c.newClient(opts.ClientOptions...)
	return c
}

// startNode builds and starts one node on an already-open listener.
func (c *Cluster) startNode(id string, ln net.Listener, src membership.Source, opts Options) {
	c.t.Helper()

	cfg := config.Config{
		NodeID:    id,
		Listen:    ln.Addr().String(),
		Advertise: ln.Addr().String(),
		// Each node needs its own metrics listener, on a port the kernel
		// picks, or the second node would fail to bind.
		MetricsListen: "127.0.0.1:0",
	}

	n, err := node.New(cfg, node.Options{
		Listener:   ln,
		Membership: src,
		Clock:      opts.Clock,
		Logger:     opts.Logger,
	})
	if err != nil {
		c.t.Fatalf("testcluster: node.New(%s): %v", id, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Start(ctx) }()

	c.mu.Lock()
	c.nodes[id] = &entry{node: n, cancel: cancel, done: done, addr: ln.Addr().String()}
	c.mu.Unlock()
}

// waitReady blocks until every node answers GetRing, so a test never
// races a node that has not finished starting.
func (c *Cluster) waitReady() {
	c.t.Helper()

	deadline := time.Now().Add(readyTimeout)
	for _, m := range c.members {
		for {
			probe, err := client.New(context.Background(), []string{m.Addr},
				client.WithRefreshInterval(0))
			if err == nil {
				_ = probe.Close()
				break
			}
			if time.Now().After(deadline) {
				c.t.Fatalf("testcluster: %s never answered: %v", m.ID, err)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// newClient returns a client seeded with every node's address and
// closed when the test ends.
func (c *Cluster) newClient(opts ...client.Option) *client.Client {
	c.t.Helper()

	cl, err := client.New(context.Background(), c.Addrs(), opts...)
	if err != nil {
		c.t.Fatalf("testcluster: client.New: %v", err)
	}
	c.t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// Client returns the cluster's client.
func (c *Cluster) Client() *client.Client { return c.client }

// NewClient returns an extra client, for a test that needs one with
// different options.
func (c *Cluster) NewClient(opts ...client.Option) *client.Client { return c.newClient(opts...) }

// Members returns the static member list.
func (c *Cluster) Members() []ring.Member { return append([]ring.Member(nil), c.members...) }

// Addrs returns every node's address.
func (c *Cluster) Addrs() []string {
	out := make([]string, 0, len(c.members))
	for _, m := range c.members {
		out = append(out, m.Addr)
	}
	return out
}

// IDs returns every node's ID, in order.
func (c *Cluster) IDs() []string { return append([]string(nil), c.ids...) }

// Ring returns the ring the cluster's members agree on.
func (c *Cluster) Ring() *ring.Ring {
	c.t.Helper()

	r, err := ring.New(ring.View{
		Epoch:   membership.StaticEpoch,
		VNodes:  config.VNodes,
		RF:      config.ReplicationFactor,
		Members: c.members,
	})
	if err != nil {
		c.t.Fatalf("testcluster: ring: %v", err)
	}
	return r
}

// Node returns the running node with this ID.
func (c *Cluster) Node(id string) *node.Node {
	c.t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("testcluster: no node %q", id)
	}
	return e.node
}

// Addr returns the address of the node with this ID.
func (c *Cluster) Addr(id string) string {
	c.t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("testcluster: no node %q", id)
	}
	return e.addr
}

// Stop shuts a node down gracefully, as SIGTERM does. In-flight
// requests finish first.
func (c *Cluster) Stop(id string) {
	c.t.Helper()
	c.take(id).stop(c.t)
}

// Kill takes a node down abruptly, which is what a crash looks like to
// everyone else: in-flight requests are cut off and the port closes at
// once. It is how a test produces a death for phase 10's failover.
func (c *Cluster) Kill(id string) {
	c.t.Helper()

	e := c.take(id)
	// Close releases the listener and stops the gRPC server without
	// draining, so a client sees the connection go rather than a
	// graceful goodbye.
	_ = e.node.Close()
	e.cancel()
	<-e.done
}

// take removes a node from the cluster and returns it, failing the test
// if it is not running.
func (c *Cluster) take(id string) *entry {
	c.t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.nodes[id]
	if !ok {
		c.t.Fatalf("testcluster: no node %q", id)
	}
	if e.stopped {
		c.t.Fatalf("testcluster: node %q is already stopped", id)
	}
	e.stopped = true
	return e
}

// stop cancels the node's context and waits for Start to return.
func (e *entry) stop(t *testing.T) {
	t.Helper()

	e.cancel()
	select {
	case err := <-e.done:
		if err != nil {
			t.Errorf("testcluster: node.Start: %v", err)
		}
	case <-time.After(readyTimeout):
		t.Error("testcluster: a node did not shut down")
	}
	_ = e.node.Close()
}

// stopAll takes every node still running down. It runs from t.Cleanup.
func (c *Cluster) stopAll() {
	c.mu.Lock()
	entries := make([]*entry, 0, len(c.nodes))
	for _, e := range c.nodes {
		if !e.stopped {
			e.stopped = true
			entries = append(entries, e)
		}
	}
	c.mu.Unlock()

	for _, e := range entries {
		e.cancel()
	}
	for _, e := range entries {
		<-e.done
		_ = e.node.Close()
	}
}

// StaticFlag returns the member list as the --static-members flag
// spells it, which is what the manual check in the plan uses.
func (c *Cluster) StaticFlag() string {
	parts := make([]string, 0, len(c.members))
	for _, m := range c.members {
		parts = append(parts, m.ID+"="+m.Addr)
	}
	return strings.Join(parts, ",")
}

// The tests live in server_test rather than server so they can build a
// whole node, which imports this package: the node's wiring is what
// puts the interceptors and the metrics in front of the handlers, and
// testing the handlers without them would miss both.
package server_test

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/node"
	"github.com/rakeshlakshmanan/ringcache/internal/server"
)

const testNodeID = "node-1"

// harness is one node served over an in-process connection.
type harness struct {
	node   *node.Node
	client ringcachev1.CacheClient
	clock  *clock.Fake
}

// newHarness starts a node on a bufconn listener and dials it. The node
// and the connection are closed when the test ends.
func newHarness(t *testing.T, opts node.Options) *harness {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	fake := clock.NewFake()

	opts.Listener = lis
	if opts.Clock == nil {
		opts.Clock = fake
	}

	cfg := config.Config{
		NodeID: testNodeID,
		// Listen is unused because the listener is supplied; the metrics
		// server still binds, on a port the kernel picks.
		Advertise:     testNodeID + ":7000",
		MetricsListen: "127.0.0.1:0",
	}
	n, err := node.New(cfg, opts)
	if err != nil {
		t.Fatalf("node.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.Start(ctx) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("node.Start: %v", err)
		}
		_ = n.Close()
	})

	return &harness{node: n, client: ringcachev1.NewCacheClient(conn), clock: fake}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestCache_SetGetDelete(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	key := []byte("hello")
	value := []byte("world")

	setResp, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: key, Value: value})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if setResp.GetVersion() == nil {
		t.Fatal("Set returned no version")
	}
	if setResp.GetServedBy() != testNodeID {
		t.Errorf("ServedBy = %q, want %q", setResp.GetServedBy(), testNodeID)
	}

	getResp, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !getResp.GetFound() {
		t.Fatal("Get after Set: found = false")
	}
	if !bytes.Equal(getResp.GetValue(), value) {
		t.Errorf("Get value = %q, want %q", getResp.GetValue(), value)
	}
	if got, want := getResp.GetVersion().String(), setResp.GetVersion().String(); got != want {
		t.Errorf("Get version = %s, want the version Set returned, %s", got, want)
	}

	delResp, err := h.client.Delete(ctx, &ringcachev1.DeleteRequest{Key: key})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// The tombstone has to outrank the value it replaces, or a late copy
	// of that value would resurrect the key.
	if delResp.GetVersion().GetWallMs() < setResp.GetVersion().GetWallMs() {
		t.Errorf("Delete version %s is behind the Set version %s",
			delResp.GetVersion(), setResp.GetVersion())
	}

	getResp, err = h.client.Get(ctx, &ringcachev1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if getResp.GetFound() {
		t.Error("Get after Delete: found = true, want false")
	}
}

func TestCache_GetMissing(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	// A miss is not an error: the client must not retry it.
	resp, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: []byte("absent")})
	if err != nil {
		t.Fatalf("Get of a missing key: %v", err)
	}
	if resp.GetFound() {
		t.Error("found = true for a key that was never written")
	}
	if resp.GetValue() != nil {
		t.Errorf("value = %q, want nothing", resp.GetValue())
	}

	ttl, err := h.client.TTL(ctx, &ringcachev1.TTLRequest{Key: []byte("absent")})
	if err != nil {
		t.Fatalf("TTL of a missing key: %v", err)
	}
	if ttl.GetFound() {
		t.Error("TTL found = true for a key that was never written")
	}
}

// FR-10 on one node: a key with a TTL is gone once its expiry passes,
// and the TTL RPC reports the time left until then.
func TestCache_TTL(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	key := []byte("expiring")
	if _, err := h.client.Set(ctx, &ringcachev1.SetRequest{
		Key: key, Value: []byte("v"), TtlMs: 2000,
	}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	resp, err := h.client.TTL(ctx, &ringcachev1.TTLRequest{Key: key})
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if !resp.GetFound() || !resp.GetHasTtl() {
		t.Fatalf("TTL: found = %v, has_ttl = %v, want both true", resp.GetFound(), resp.GetHasTtl())
	}
	if resp.GetRemainingMs() != 2000 {
		t.Errorf("RemainingMs = %d, want 2000", resp.GetRemainingMs())
	}

	h.clock.Advance(500 * time.Millisecond)
	resp, err = h.client.TTL(ctx, &ringcachev1.TTLRequest{Key: key})
	if err != nil {
		t.Fatalf("TTL at 0.5 s: %v", err)
	}
	if resp.GetRemainingMs() != 1500 {
		t.Errorf("RemainingMs at 0.5 s = %d, want 1500", resp.GetRemainingMs())
	}

	// A key without an expiry reports found with no TTL.
	if _, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: []byte("forever"), Value: []byte("v")}); err != nil {
		t.Fatalf("Set without a TTL: %v", err)
	}
	resp, err = h.client.TTL(ctx, &ringcachev1.TTLRequest{Key: []byte("forever")})
	if err != nil {
		t.Fatalf("TTL of a key without one: %v", err)
	}
	if !resp.GetFound() || resp.GetHasTtl() {
		t.Errorf("TTL of a key without one: found = %v, has_ttl = %v, want true and false",
			resp.GetFound(), resp.GetHasTtl())
	}

	h.clock.Advance(1600 * time.Millisecond) // 2.1 s after the write
	getResp, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: key})
	if err != nil {
		t.Fatalf("Get at 2.1 s: %v", err)
	}
	if getResp.GetFound() {
		t.Error("Get at 2.1 s: found = true, want the key expired at 2 s")
	}
	ttlResp, err := h.client.TTL(ctx, &ringcachev1.TTLRequest{Key: key})
	if err != nil {
		t.Fatalf("TTL at 2.1 s: %v", err)
	}
	if ttlResp.GetFound() {
		t.Error("TTL at 2.1 s: found = true, want false")
	}
}

func TestCache_SizeLimits(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	maxKey := bytes.Repeat([]byte("k"), config.MaxKeyBytes)
	maxValue := bytes.Repeat([]byte("v"), config.MaxValueBytes)

	// The limits themselves are accepted; only going over them is not.
	if _, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: maxKey, Value: maxValue}); err != nil {
		t.Fatalf("Set at the key and value limits: %v", err)
	}
	if _, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: maxKey}); err != nil {
		t.Fatalf("Get at the key limit: %v", err)
	}

	tooLongKey := append(bytes.Clone(maxKey), 'k')
	tooLongValue := append(bytes.Clone(maxValue), 'v')

	rejected := []struct {
		name string
		call func() error
	}{
		{"empty key on Set", func() error {
			_, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: nil, Value: []byte("v")})
			return err
		}},
		{"empty key on Get", func() error {
			_, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: nil})
			return err
		}},
		{"empty key on Delete", func() error {
			_, err := h.client.Delete(ctx, &ringcachev1.DeleteRequest{Key: nil})
			return err
		}},
		{"empty key on TTL", func() error {
			_, err := h.client.TTL(ctx, &ringcachev1.TTLRequest{Key: nil})
			return err
		}},
		{"key one byte over", func() error {
			_, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: tooLongKey, Value: []byte("v")})
			return err
		}},
		{"value one byte over", func() error {
			_, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: []byte("k"), Value: tooLongValue})
			return err
		}},
	}

	for _, tt := range rejected {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.call()
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("code = %s, want InvalidArgument (error: %v)", got, err)
			}
		})
	}
}

func TestCache_Metrics(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	if _, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Fatalf("Set: %v", err)
	}

	m := h.node.Metrics()
	if got := testutil.ToFloat64(m.Requests.WithLabelValues("Cache/Set", "OK")); got != 1 {
		t.Errorf("requests_total{method=Cache/Set,code=OK} = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.Latency); got != 1 {
		t.Errorf("latency series = %d, want 1 (one method has been called)", got)
	}

	// A rejected request is counted under the code it failed with, so a
	// node refusing everything is visible rather than silently idle.
	if _, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: nil, Value: []byte("v")}); err == nil {
		t.Fatal("Set with an empty key succeeded")
	}
	if got := testutil.ToFloat64(m.Requests.WithLabelValues("Cache/Set", "InvalidArgument")); got != 1 {
		t.Errorf("requests_total{method=Cache/Set,code=InvalidArgument} = %v, want 1", got)
	}

	// The store gauges are gauge functions, so they read the live counts
	// at scrape time rather than a copy made when the metric was built.
	const want = `
# HELP ringcache_store_keys Entries held by this node, including unexpired tombstones.
# TYPE ringcache_store_keys gauge
ringcache_store_keys{node="node-1"} 1
`
	if err := testutil.GatherAndCompare(m.Registry, strings.NewReader(want), "ringcache_store_keys"); err != nil {
		t.Error(err)
	}
}

func TestCache_GetRing(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	view, err := h.client.GetRing(ctx, &ringcachev1.GetRingRequest{})
	if err != nil {
		t.Fatalf("GetRing: %v", err)
	}
	// Epoch 0 means no ring has been published; a lone node reports the
	// shape it would use so a client can route against it unchanged.
	if view.GetEpoch() != 0 {
		t.Errorf("Epoch = %d, want 0 before any ring is published", view.GetEpoch())
	}
	if n := len(view.GetMembers()); n != 1 {
		t.Fatalf("got %d members, want 1", n)
	}
	if got := view.GetMembers()[0]; got.GetId() != testNodeID || got.GetAddr() != testNodeID+":7000" {
		t.Errorf("member = %v, want the node's own id and advertised address", got)
	}
	if view.GetVnodes() != uint32(config.VNodes) || view.GetRf() != uint32(config.ReplicationFactor) {
		t.Errorf("vnodes = %d, rf = %d, want %d and %d",
			view.GetVnodes(), view.GetRf(), config.VNodes, config.ReplicationFactor)
	}
}

// stubOwner refuses every key, which is how a node behaves for keys it
// does not hold once phase 6 puts the ring behind this interface.
type stubOwner struct {
	err  error
	view *ringcachev1.RingView
}

func (o stubOwner) CheckWrite(uint64) error         { return o.err }
func (o stubOwner) CheckRead(uint64) error          { return o.err }
func (o stubOwner) RingView() *ringcachev1.RingView { return o.view }

// A client has to be able to read the node's epoch out of the error,
// because that is how it learns its own ring is stale.
func TestCache_WrongOwner(t *testing.T) {
	const epoch = 7
	h := newHarness(t, node.Options{
		Ownership: stubOwner{
			err:  &server.WrongOwnerError{Epoch: epoch},
			view: &ringcachev1.RingView{Epoch: epoch},
		},
	})
	ctx := testContext(t)

	_, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: []byte("k"), Value: []byte("v")})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("Set = %v, want FailedPrecondition", err)
	}

	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("got %d status details, want 1", len(details))
	}
	detail, ok := details[0].(*ringcachev1.WrongOwner)
	if !ok {
		t.Fatalf("detail is %T, want *ringcachev1.WrongOwner", details[0])
	}
	if detail.GetNodeEpoch() != epoch {
		t.Errorf("NodeEpoch = %d, want %d", detail.GetNodeEpoch(), epoch)
	}
}

// A node with no ring yet cannot tell whether it owns a key, so it
// reports UNAVAILABLE and the client tries elsewhere.
func TestCache_NoRing(t *testing.T) {
	h := newHarness(t, node.Options{Ownership: stubOwner{err: server.ErrNoRing}})
	ctx := testContext(t)

	_, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: []byte("k")})
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("Get = %s, want Unavailable (error: %v)", got, err)
	}
	if _, err := h.client.GetRing(ctx, &ringcachev1.GetRingRequest{}); status.Code(err) != codes.Unavailable {
		t.Errorf("GetRing = %v, want Unavailable when there is no ring", err)
	}
}

// The gate refuses every request whatever key it names, which is what
// phase 10 uses to fence a node that has lost its lease.
func TestCache_Gate(t *testing.T) {
	refuse := true
	h := newHarness(t, node.Options{
		Gate: func() error {
			if refuse {
				return server.ErrFenced
			}
			return nil
		},
	})
	ctx := testContext(t)

	_, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: []byte("k")})
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("Get while gated = %s, want Unavailable (error: %v)", got, err)
	}

	refuse = false
	if _, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: []byte("k")}); err != nil {
		t.Errorf("Get after the gate opened: %v", err)
	}
}

// panicOwner stands in for a handler bug. One bad request must not take
// the node down, because its keys would move and have to be repaired.
type panicOwner struct{ stubOwner }

func (panicOwner) CheckRead(uint64) error { panic("boom") }

func TestCache_PanicRecovered(t *testing.T) {
	h := newHarness(t, node.Options{
		Ownership: panicOwner{stubOwner{view: &ringcachev1.RingView{}}},
	})
	ctx := testContext(t)

	_, err := h.client.Get(ctx, &ringcachev1.GetRequest{Key: []byte("k")})
	if got := status.Code(err); got != codes.Internal {
		t.Fatalf("Get = %s, want Internal (error: %v)", got, err)
	}
	if msg := status.Convert(err).Message(); strings.Contains(msg, "boom") {
		t.Errorf("message %q leaks the panic value to the client", msg)
	}
	if got := testutil.ToFloat64(h.node.Metrics().Panics); got != 1 {
		t.Errorf("handler_panics_total = %v, want 1", got)
	}

	// The node is still serving: a write that does not touch the panic
	// path goes through.
	if _, err := h.client.Set(ctx, &ringcachev1.SetRequest{Key: []byte("k"), Value: []byte("v")}); err != nil {
		t.Errorf("Set after a recovered panic: %v", err)
	}
}

func TestCache_TTLOutOfRange(t *testing.T) {
	h := newHarness(t, node.Options{})
	ctx := testContext(t)

	// A TTL too large to hold in a Duration must be refused, not wrapped
	// into an expiry in the past.
	_, err := h.client.Set(ctx, &ringcachev1.SetRequest{
		Key: []byte("k"), Value: []byte("v"), TtlMs: ^uint64(0),
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %s, want InvalidArgument (error: %v)", got, err)
	}
}

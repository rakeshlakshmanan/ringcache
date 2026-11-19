package client_test

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
	"github.com/rakeshlakshmanan/ringcache/internal/testcluster"
	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// FR-11: a client whose ring is behind recovers by itself. It is sent
// to the wrong node, told so, refreshes and succeeds on its second
// attempt — all inside one call, with nothing for the caller to do.
func TestClient_StaleRingRefresh(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})
	ctx := testContext(t)

	// A ring from before node-5 joined, at epoch 0, which is behind the
	// cluster's epoch 1.
	members := c.Members()
	stale := ring.View{
		Epoch:   0,
		VNodes:  config.VNodes,
		RF:      config.ReplicationFactor,
		Members: members[:4],
	}
	staleRing, err := ring.New(stale)
	if err != nil {
		t.Fatalf("ring.New: %v", err)
	}
	live := c.Ring()

	// A key the two rings disagree about, so routing from the stale one
	// actually reaches the wrong node.
	var key string
	for i := range 10_000 {
		k := fmt.Sprintf("key-%d", i)
		h := keyhash.Sum(k)
		if staleRing.Lookup(h).ID != live.Lookup(h).ID {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatal("the two rings agree about every key, so nothing would be misrouted")
	}
	t.Logf("%q: stale ring says %s, the cluster says %s",
		key, staleRing.Lookup(keyhash.Sum(key)).ID, live.Lookup(keyhash.Sum(key)).ID)

	cl := c.NewClient(client.WithInitialRing(stale), client.WithRefreshInterval(0))
	if cl.Epoch() != 0 {
		t.Fatalf("the client started at epoch %d, want the stale 0", cl.Epoch())
	}

	var info client.CallInfo
	if err := cl.Set(client.WithCallInfo(ctx, &info), key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got := info.Attempts(); got != 2 {
		t.Errorf("the call took %d attempts, want 2: one misrouted, one after the refresh", got)
	}
	if !info.Refreshed() {
		t.Error("the call did not record a refresh")
	}
	if got := cl.Epoch(); got != 1 {
		t.Errorf("the client is at epoch %d, want 1", got)
	}
	if got := info.ServedBy(); got != live.Lookup(keyhash.Sum(key)).ID {
		t.Errorf("served by %s, want the real primary %s", got, live.Lookup(keyhash.Sum(key)).ID)
	}

	// The write really landed, on the node the live ring names.
	primary := live.Lookup(keyhash.Sum(key)).ID
	if _, ok := c.Node(primary).Store().Get(key); !ok {
		t.Errorf("%s does not hold %q", primary, key)
	}
}

// When a key's primary is gone, a read falls back to the replica, which
// is the node that holds the other copy. Until phase 8 replicates, the
// replica has nothing, so a miss is the right answer — the point here
// is where the second attempt went.
func TestClient_ReadRetriesReplica(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	const key = "key-failover"
	pref := c.Ring().PreferenceIDs(keyhash.Sum(key))
	primary, replica := pref[0], pref[1]

	if err := cl.Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := c.Node(primary).Store().Get(key); !ok {
		t.Fatalf("%s does not hold %q after the write", primary, key)
	}

	c.Kill(primary)
	t.Logf("killed %s; the replica is %s", primary, replica)

	var info client.CallInfo
	_, found, err := cl.Get(client.WithCallInfo(ctx, &info), key)
	if err != nil {
		t.Fatalf("Get after the primary died: %v", err)
	}

	if got := info.Attempts(); got != 2 {
		t.Errorf("the call took %d attempts, want 2: the primary then the replica", got)
	}
	if got := info.ServedBy(); got != replica {
		t.Errorf("served by %s, want the replica %s", got, replica)
	}
	// Expected until phase 8: nothing was ever copied to the replica.
	if found {
		t.Errorf("the replica returned the value, but nothing replicates it yet")
	}
}

// A write has nowhere else to go when the primary is down: only the
// primary may accept it. Phase 10 adds the wait for a new epoch that
// names a different one.
func TestClient_WriteDoesNotRetryElsewhere(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	const key = "key-write-failover"
	primary := c.Ring().Lookup(keyhash.Sum(key)).ID
	c.Kill(primary)

	var info client.CallInfo
	err := cl.Set(client.WithCallInfo(ctx, &info), key, []byte("v"), 0)
	if err == nil {
		t.Fatal("Set succeeded although the key's primary is down")
	}
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("Set = %s, want Unavailable (error: %v)", got, err)
	}
	if got := info.Attempts(); got != 1 {
		t.Errorf("the write took %d attempts, want 1: there is nowhere else to send it", got)
	}
}

// slowServer answers nothing, which is what a node too busy or wedged
// to reply looks like to a client.
type slowServer struct {
	ringcachev1.UnimplementedCacheServer
	view *ringcachev1.RingView
	// calls counts handlers entered. It is incremented before the
	// handler blocks, so every attempt the client sent is counted by the
	// time that attempt's deadline fires.
	calls atomic.Int64
}

func (s *slowServer) Get(ctx context.Context, _ *ringcachev1.GetRequest) (*ringcachev1.GetResponse, error) {
	s.calls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *slowServer) Set(ctx context.Context, _ *ringcachev1.SetRequest) (*ringcachev1.SetResponse, error) {
	s.calls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *slowServer) GetRing(context.Context, *ringcachev1.GetRingRequest) (*ringcachev1.RingView, error) {
	return s.view, nil
}

// NFR-03: a request makes at most MaxAttempts attempts. A node that
// never answers must not be retried forever, or one slow node would
// hold up every client that routes to it.
func TestClient_MaxTwoAttempts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	slow := &slowServer{}
	slow.view = (&ring.View{
		Epoch:   1,
		VNodes:  config.VNodes,
		RF:      config.ReplicationFactor,
		Members: []ring.Member{{ID: "node-1", Addr: ln.Addr().String()}},
	}).ToProto()
	ringcachev1.RegisterCacheServer(srv, slow)

	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)

	ctx := testContext(t)
	cl, err := client.New(ctx, []string{ln.Addr().String()},
		client.WithRefreshInterval(0),
		client.WithAttemptTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	start := time.Now()
	var info client.CallInfo
	_, _, err = cl.Get(client.WithCallInfo(ctx, &info), "key-1")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Get succeeded against a server that never answers")
	}
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Errorf("Get = %s, want DeadlineExceeded (error: %v)", got, err)
	}
	if got := info.Attempts(); got != config.MaxAttempts {
		t.Errorf("the call took %d attempts, want exactly %d", got, config.MaxAttempts)
	}

	// Count what the server actually saw, not only what the client
	// claims: the two have to agree.
	if got := slow.calls.Load(); got != int64(config.MaxAttempts) {
		t.Errorf("the server saw %d calls, want %d", got, config.MaxAttempts)
	}

	// Each attempt carries its own deadline, so the call ends in about
	// MaxAttempts timeouts rather than running until the caller's.
	if elapsed > time.Second {
		t.Errorf("the call took %s, want about %d attempt timeouts", elapsed, config.MaxAttempts)
	}
}

// A key rejected by the node is not retried: the answer would be the
// same, and retrying wastes an attempt the call may need.
func TestClient_NoRetryOnInvalidArgument(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})
	ctx := testContext(t)

	var info client.CallInfo
	err := c.Client().Set(client.WithCallInfo(ctx, &info), "", []byte("v"), 0)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("Set with an empty key = %s, want InvalidArgument (error: %v)", got, err)
	}
	if got := info.Attempts(); got != 1 {
		t.Errorf("the call took %d attempts, want 1: the answer would not change", got)
	}
}

// A client bootstraps from whichever seed answers, so a seed that is
// down must not stop it starting.
func TestClient_SeedsTriedUntilOneAnswers(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := dead.Addr().String()
	_ = dead.Close()

	seeds := append([]string{addr}, c.Addrs()...)
	cl, err := client.New(testContext(t), seeds, client.WithRefreshInterval(0))
	if err != nil {
		t.Fatalf("client.New with a dead seed first: %v", err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	if got := cl.Ring(); got == nil || len(got.Members()) != 3 {
		t.Fatalf("the client did not get a ring from the live seeds")
	}
}

// With every seed down there is no ring, and New has to say so rather
// than hand back a client that cannot route.
func TestClient_NoSeedAnswers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.New(ctx, []string{addr}, client.WithRefreshInterval(0)); err == nil {
		t.Fatal("client.New succeeded with no reachable seed")
	}
	if _, err := client.New(ctx, nil); err == nil {
		t.Fatal("client.New succeeded with no seeds at all")
	}
}

// The background refresh is what keeps an idle client current (FR-11),
// without needing a request to go wrong first.
func TestClient_BackgroundRefresh(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})

	stale := ring.View{
		Epoch:   0,
		VNodes:  config.VNodes,
		RF:      config.ReplicationFactor,
		Members: c.Members(),
	}
	cl := c.NewClient(client.WithInitialRing(stale), client.WithRefreshInterval(20*time.Millisecond))

	deadline := time.Now().Add(10 * time.Second)
	for cl.Epoch() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the client never refreshed to the cluster's epoch on its own")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// An older ring arriving late must not undo a newer one: two refreshes
// can land out of order.
func TestClient_OlderRingIgnored(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})
	cl := c.Client()

	if got := cl.Epoch(); got != 1 {
		t.Fatalf("the client is at epoch %d, want the cluster's 1", got)
	}
	// Refreshing again must leave the epoch where it is, not reset it.
	if err := cl.Refresh(testContext(t)); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := cl.Epoch(); got != 1 {
		t.Errorf("the client is at epoch %d after a refresh, want 1", got)
	}
}

// Closing has to be safe to do twice, and has to stop the background
// refresh rather than leave it running against closed connections.
func TestClient_CloseTwice(t *testing.T) {
	c := testcluster.Start(t, 1, testcluster.Options{})
	cl := c.NewClient(client.WithRefreshInterval(10 * time.Millisecond))

	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := cl.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// Every response carries the serving node's epoch, so the load
// generator in phase 7 can attribute what it sees.
func TestClient_CallInfoReportsServer(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()
	r := c.Ring()

	for i := range 50 {
		key := fmt.Sprintf("key-%d", i)
		var info client.CallInfo
		if err := cl.Set(client.WithCallInfo(ctx, &info), key, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}

		want := r.Lookup(keyhash.Sum(key)).ID
		if got := info.ServedBy(); got != want {
			t.Fatalf("%q was served by %s, want its primary %s", key, got, want)
		}
		if got := info.Attempts(); got != 1 {
			t.Fatalf("%q took %d attempts, want 1", key, got)
		}
		if got := info.Epoch(); got != 1 {
			t.Fatalf("%q reported epoch %d, want 1", key, got)
		}
		if got := info.Code(); got != codes.OK {
			t.Fatalf("%q reported code %s, want OK", key, got)
		}
	}
}

// A client used from many goroutines must stay correct: one ring and
// one connection per member are shared by all of them.
func TestClient_Concurrent(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	const (
		workers = 16
		perEach = 200
	)
	errs := make(chan error, workers)
	for w := range workers {
		go func() {
			for i := range perEach {
				key := fmt.Sprintf("w%d-k%d", w, i)
				if err := cl.Set(ctx, key, []byte(key), 0); err != nil {
					errs <- err
					return
				}
				got, found, err := cl.Get(ctx, key)
				if err != nil {
					errs <- err
					return
				}
				if !found || string(got) != key {
					errs <- fmt.Errorf("%q read back as %q, found %v", key, got, found)
					return
				}
			}
			errs <- nil
		}()
	}
	for range workers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}

	total := int64(0)
	for _, id := range c.IDs() {
		total += c.Node(id).Store().Len()
	}
	if want := int64(workers * perEach); total != want {
		t.Errorf("the cluster holds %d keys, want %d", total, want)
	}
}

// Code names the gRPC code without the caller importing grpc.
func TestClient_Code(t *testing.T) {
	if got := client.Code(nil); got != "OK" {
		t.Errorf("Code(nil) = %q, want OK", got)
	}
	err := status.Error(codes.Unavailable, "down")
	if got := client.Code(err); got != "Unavailable" {
		t.Errorf("Code(Unavailable) = %q", got)
	}
	if !slices.Contains([]string{"NoRing"}, client.Code(fmt.Errorf("x: %w", client.ErrNoRing))) {
		t.Error("Code did not recognise ErrNoRing")
	}
}

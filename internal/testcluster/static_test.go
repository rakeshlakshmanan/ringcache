package testcluster_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/membership"
	"github.com/rakeshlakshmanan/ringcache/internal/testcluster"
	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// forEachKey runs fn over every key with the given concurrency and
// fails the test on the first error, naming the key it happened on.
func forEachKey(t *testing.T, workers int, keys []string, fn func(string) error) {
	t.Helper()

	var (
		next atomic.Int64
		fail sync.Once
		wg   sync.WaitGroup
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(len(keys)) {
					return
				}
				if err := fn(keys[i]); err != nil {
					fail.Do(func() { t.Errorf("%q: %v", keys[i], err) })
					return
				}
			}
		}()
	}
	wg.Wait()

	if t.Failed() {
		t.FailNow()
	}
}

// FR-01: five nodes serve one keyspace. Everything written through the
// client reads back, and each key lives only on its primary, which is
// checked in every node's own store rather than through the API.
func TestStatic_FiveNodesReadBack(t *testing.T) {
	const keys = 100_000

	c := testcluster.Start(t, 5, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	rng := rand.New(rand.NewPCG(99, 7))
	written := make(map[string][]byte, keys)
	order := make([]string, 0, keys)
	for i := range keys {
		key := fmt.Sprintf("key-%d-%d", i, rng.Uint64())
		written[key] = fmt.Appendf(nil, "value-%d", i)
		order = append(order, key)
	}

	// The calls run concurrently, which is both how a cache is actually
	// used and what keeps 200,000 round trips inside the time a test may
	// take. One Client is shared, as a program should share one.
	const workers = 32
	forEachKey(t, workers, order, func(key string) error {
		return cl.Set(ctx, key, written[key], 0)
	})
	forEachKey(t, workers, order, func(key string) error {
		got, found, err := cl.Get(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("not found, although it was just written")
		}
		if !bytes.Equal(got, written[key]) {
			return fmt.Errorf("read back %q, want %q", got, written[key])
		}
		return nil
	})

	// Each key is stored on its primary alone: there is no replication
	// until phase 8, so a second copy would mean a key was accepted by a
	// node that does not own it.
	r := c.Ring()
	for key := range written {
		primary := r.Lookup(keyhash.Sum(key)).ID

		var holders []string
		for _, id := range c.IDs() {
			if _, ok := c.Node(id).Store().Get(key); ok {
				holders = append(holders, id)
			}
		}
		if !slices.Equal(holders, []string{primary}) {
			t.Fatalf("%q is held by %v, want only its primary %s", key, holders, primary)
		}
	}

	// The load should be spread, or the ring is not doing its job.
	for _, id := range c.IDs() {
		n := c.Node(id).Store().Len()
		t.Logf("%s holds %d keys", id, n)
		if n == 0 {
			t.Errorf("%s holds nothing", id)
		}
	}
}

// A node that is not the primary must refuse a write and say so in a
// way the client can act on: FAILED_PRECONDITION carrying the node's
// epoch.
func TestStatic_DirectNonOwner(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})
	ctx := testContext(t)

	const key = "key-direct"
	r := c.Ring()
	pref := r.PreferenceIDs(keyhash.Sum(key))

	var notOwner string
	for _, id := range c.IDs() {
		if !slices.Contains(pref, id) {
			notOwner = id
			break
		}
	}
	if notOwner == "" {
		t.Fatal("every node is on the preference list, so there is no non-owner to ask")
	}
	t.Logf("%q belongs to %v; asking %s directly", key, pref, notOwner)

	cc, err := grpc.NewClient(c.Addr(notOwner), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()

	_, err = ringcachev1.NewCacheClient(cc).Set(ctx, &ringcachev1.SetRequest{
		Key: []byte(key), Value: []byte("v"),
	})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("Set to a non-owner = %v, want FailedPrecondition", err)
	}

	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("got %d status details, want 1", len(details))
	}
	wo, ok := details[0].(*ringcachev1.WrongOwner)
	if !ok {
		t.Fatalf("detail is %T, want *ringcachev1.WrongOwner", details[0])
	}
	if wo.GetNodeEpoch() != membership.StaticEpoch {
		t.Errorf("NodeEpoch = %d, want %d", wo.GetNodeEpoch(), membership.StaticEpoch)
	}

	// The replica must refuse the write too, and allow the read.
	cc2, err := grpc.NewClient(c.Addr(pref[1]), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc2.Close()

	replica := ringcachev1.NewCacheClient(cc2)
	if _, err := replica.Set(ctx, &ringcachev1.SetRequest{Key: []byte(key), Value: []byte("v")}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Set to the replica = %v, want FailedPrecondition", err)
	}
	if _, err := replica.Get(ctx, &ringcachev1.GetRequest{Key: []byte(key)}); err != nil {
		t.Errorf("Get from the replica = %v, want it allowed: a replica holds a copy", err)
	}
}

// Every node of one epoch reports the same ring, which is what lets a
// client bootstrap from any seed.
func TestStatic_AllNodesAgree(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})

	want := c.Ring().View()
	for _, addr := range c.Addrs() {
		cl, err := client.New(testContext(t), []string{addr}, client.WithRefreshInterval(0))
		if err != nil {
			t.Fatalf("client from seed %s: %v", addr, err)
		}
		got := cl.Ring().View()
		_ = cl.Close()

		if !got.Equal(want) {
			t.Errorf("the ring from seed %s differs: %+v", addr, got)
		}
	}
}

// Deleting makes the key a miss, and deleting one key does not disturb
// another on the same node.
func TestStatic_Delete(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	for i := range 200 {
		if err := cl.Set(ctx, fmt.Sprintf("k-%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	for i := 0; i < 200; i += 2 {
		if err := cl.Delete(ctx, fmt.Sprintf("k-%d", i)); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	for i := range 200 {
		_, found, err := cl.Get(ctx, fmt.Sprintf("k-%d", i))
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if want := i%2 == 1; found != want {
			t.Fatalf("k-%d: found = %v, want %v", i, found, want)
		}
	}
}

// A TTL set through the client has to be honoured on whichever node
// owns the key.
func TestStatic_TTL(t *testing.T) {
	c := testcluster.Start(t, 3, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	if err := cl.Set(ctx, "temp", []byte("v"), 300*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}

	remaining, hasTTL, found, err := cl.TTL(ctx, "temp")
	if err != nil {
		t.Fatalf("TTL: %v", err)
	}
	if !found || !hasTTL || remaining <= 0 || remaining > 300*time.Millisecond {
		t.Fatalf("TTL = %s, hasTTL = %v, found = %v", remaining, hasTTL, found)
	}

	// The clock here is real, so this waits rather than advancing a fake
	// one: the sweep and the lazy expiry both run on it.
	time.Sleep(400 * time.Millisecond)
	if _, found, err := cl.Get(ctx, "temp"); err != nil || found {
		t.Errorf("Get after the TTL = found %v, err %v, want a miss", found, err)
	}
}

// A single-node cluster is a cluster: it owns the whole keyspace and
// its preference list is one node long.
func TestStatic_SingleNode(t *testing.T) {
	c := testcluster.Start(t, 1, testcluster.Options{})
	ctx := testContext(t)
	cl := c.Client()

	for i := range 500 {
		if err := cl.Set(ctx, fmt.Sprintf("k-%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if got := c.Node("node-1").Store().Len(); got != 500 {
		t.Errorf("the single node holds %d keys, want 500", got)
	}
}

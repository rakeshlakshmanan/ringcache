package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

// connLinger is how long a connection to a member that has left the
// ring is kept before it is closed. A member can reappear in the next
// epoch — a ring is republished for reasons that have nothing to do
// with it — and reopening costs a handshake on the next request.
const connLinger = 10 * time.Second

// router holds the client's view of the cluster: the current ring and
// one connection per member.
type router struct {
	// current is read on every request and replaced whole on a refresh,
	// so routing takes no lock.
	current atomic.Pointer[ring.Ring]

	dialOpts []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*conn
	// seeds are dialled when the client has no ring, which is its state
	// at startup and after every member it knew has gone.
	seeds  []string
	closed bool

	// refresh is deduplicated: a hundred requests meeting a stale ring
	// at once need one GetRing between them, not a hundred.
	sf singleflight.Group
}

// conn is one member's connection and the time it stopped being a
// member, if it has.
type conn struct {
	cc   *grpc.ClientConn
	addr string
	// leftAt is when the member last vanished from the ring, zero while
	// it is a member.
	leftAt time.Time
}

func newRouter(seeds []string, dialOpts []grpc.DialOption) *router {
	return &router{
		seeds: append([]string(nil), seeds...),
		dialOpts: append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(config.MaxGRPCMessage),
				grpc.MaxCallSendMsgSize(config.MaxGRPCMessage),
			),
		}, dialOpts...),
		conns: make(map[string]*conn),
	}
}

// ring returns the ring the client is routing with, or nil.
func (r *router) ring() *ring.Ring { return r.current.Load() }

// epoch returns the epoch the client is routing with.
func (r *router) epoch() uint64 {
	if cur := r.current.Load(); cur != nil {
		return cur.Epoch()
	}
	return 0
}

// install replaces the current ring when v is newer than what the
// client holds, and reports whether it did.
//
// An older or equal epoch is ignored: two refreshes can land out of
// order, and the newer one must not be undone by the slower one.
func (r *router) install(v ring.View) (bool, error) {
	cur := r.current.Load()
	if cur != nil && v.Epoch <= cur.Epoch() {
		return false, nil
	}

	next, err := ring.New(v)
	if err != nil {
		return false, fmt.Errorf("client: ring at epoch %d: %w", v.Epoch, err)
	}

	// CompareAndSwap rather than Store: another refresh may have
	// installed a newer ring since the check above.
	if !r.current.CompareAndSwap(cur, next) {
		return false, nil
	}
	r.retireAbsent(next)
	return true, nil
}

// retireAbsent marks connections to members that are no longer in the
// ring, and closes those that have been gone longer than connLinger.
func (r *router) retireAbsent(next *ring.Ring) {
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	for id, c := range r.conns {
		if next.Has(id) {
			c.leftAt = time.Time{}
			continue
		}
		if c.leftAt.IsZero() {
			c.leftAt = now
			continue
		}
		if now.Sub(c.leftAt) >= connLinger {
			_ = c.cc.Close()
			delete(r.conns, id)
		}
	}
}

// clientFor returns a Cache client for the member, dialling on first
// use. grpc.NewClient does not block, so the connection is established
// by the first RPC rather than here.
func (r *router) clientFor(m ring.Member) (ringcachev1.CacheClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, fmt.Errorf("client: closed")
	}

	// A member keeping its ID but changing address is a restarted node;
	// the old connection points at nothing.
	if c, ok := r.conns[m.ID]; ok {
		if c.addr == m.Addr {
			return ringcachev1.NewCacheClient(c.cc), nil
		}
		_ = c.cc.Close()
		delete(r.conns, m.ID)
	}

	cc, err := grpc.NewClient(m.Addr, r.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("client: dial %s at %s: %w", m.ID, m.Addr, err)
	}
	r.conns[m.ID] = &conn{cc: cc, addr: m.Addr}
	return ringcachev1.NewCacheClient(cc), nil
}

// refresh fetches a ring and installs it if it is newer. from names the
// member to ask, or is empty to pick one at random; several concurrent
// refreshes share one RPC.
func (r *router) refresh(ctx context.Context, from string) error {
	key := from
	if key == "" {
		key = "*"
	}

	_, err, _ := r.sf.Do(key, func() (any, error) {
		view, err := r.fetchRing(ctx, from)
		if err != nil {
			return nil, err
		}
		_, err = r.install(view)
		return nil, err
	})
	return err
}

// fetchRing calls GetRing on one member, or on the seeds when the
// client has no ring yet.
func (r *router) fetchRing(ctx context.Context, from string) (ring.View, error) {
	cur := r.current.Load()
	if cur == nil {
		return r.fetchFromSeeds(ctx)
	}

	members := cur.Members()
	var target ring.Member
	if from != "" {
		for _, m := range members {
			if m.ID == from {
				target = m
				break
			}
		}
	}
	if target.ID == "" {
		// A ring refresh asks one member, not all of them: every member
		// of an epoch reports the same ring.
		target = members[rand.IntN(len(members))]
	}

	cc, err := r.clientFor(target)
	if err != nil {
		return ring.View{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, config.AttemptTimeout)
	defer cancel()

	view, err := cc.GetRing(ctx, &ringcachev1.GetRingRequest{})
	if err != nil {
		return ring.View{}, fmt.Errorf("client: GetRing from %s: %w", target.ID, err)
	}
	return ring.FromProto(view), nil
}

// fetchFromSeeds tries the seeds in random order and takes the first
// ring that comes back. Seeds are addresses rather than members, since
// the client does not yet know any member's ID.
func (r *router) fetchFromSeeds(ctx context.Context) (ring.View, error) {
	r.mu.Lock()
	seeds := append([]string(nil), r.seeds...)
	r.mu.Unlock()

	if len(seeds) == 0 {
		return ring.View{}, fmt.Errorf("client: no seeds to fetch a ring from")
	}
	rand.Shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })

	var lastErr error
	for _, addr := range seeds {
		view, err := r.fetchFromSeed(ctx, addr)
		if err != nil {
			lastErr = err
			continue
		}
		return view, nil
	}
	return ring.View{}, fmt.Errorf("client: no seed answered GetRing: %w", lastErr)
}

// fetchFromSeed asks one seed address for the ring. The connection is
// not kept: once a ring exists, connections are keyed by member ID, and
// the seed's own entry opens on its first request.
func (r *router) fetchFromSeed(ctx context.Context, addr string) (ring.View, error) {
	cc, err := grpc.NewClient(addr, r.dialOpts...)
	if err != nil {
		return ring.View{}, fmt.Errorf("client: dial seed %s: %w", addr, err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(ctx, seedTimeout)
	defer cancel()

	view, err := ringcachev1.NewCacheClient(cc).GetRing(ctx, &ringcachev1.GetRingRequest{})
	if err != nil {
		return ring.View{}, fmt.Errorf("client: GetRing from seed %s: %w", addr, err)
	}
	return ring.FromProto(view), nil
}

// close releases every connection.
func (r *router) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	var firstErr error
	for id, c := range r.conns {
		if err := c.cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(r.conns, id)
	}
	return firstErr
}

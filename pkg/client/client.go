// Package client is the Go client for a ringcache cluster.
//
// A client routes by key from a ring it caches, and talks to nodes
// directly: nothing sits in between. It keeps that ring current by
// refreshing it in the background, and by reacting to the epoch every
// response carries, so a cluster that changes shape under it costs at
// most one extra attempt.
package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

// seedTimeout bounds the GetRing sent to one seed at startup. It is
// longer than an ordinary attempt because it includes the connection
// handshake, which a warmed-up client does not pay.
const seedTimeout = 2 * time.Second

// Options configure a Client. Use the Option functions rather than
// building this directly.
type Options struct {
	// AttemptTimeout is the deadline for one attempt.
	AttemptTimeout time.Duration
	// MaxAttempts is how many attempts a call may make.
	MaxAttempts int
	// RefreshInterval is how often the ring is refreshed in the
	// background. Zero turns the background refresh off.
	RefreshInterval time.Duration

	dialOpts []grpc.DialOption
	// initialRing, when set, is installed instead of fetching one from
	// the seeds. It exists for tests that need a client starting with a
	// deliberately stale ring.
	initialRing *ring.View
}

// Option configures a Client.
type Option func(*Options)

// WithAttemptTimeout sets the deadline for one attempt.
func WithAttemptTimeout(d time.Duration) Option {
	return func(o *Options) { o.AttemptTimeout = d }
}

// WithMaxAttempts sets how many attempts a call may make.
func WithMaxAttempts(n int) Option {
	return func(o *Options) { o.MaxAttempts = n }
}

// WithRefreshInterval sets how often the ring is refreshed in the
// background. Zero turns it off.
func WithRefreshInterval(d time.Duration) Option {
	return func(o *Options) { o.RefreshInterval = d }
}

// WithDialOptions adds gRPC dial options used for every node.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *Options) { o.dialOpts = append(o.dialOpts, opts...) }
}

// withInitialRing starts the client from a given ring instead of
// fetching one. It is unexported and reached from the export_test.go
// shim, because only tests have a reason to start a client with a ring
// that is already stale.
func withInitialRing(v ring.View) Option {
	return func(o *Options) { o.initialRing = &v }
}

// Client talks to a ringcache cluster. It is safe for concurrent use,
// and a program should keep one rather than making one per request:
// the ring and the connections are worth sharing.
type Client struct {
	opts   Options
	router *router

	// refreshes runs the background refreshes, both the ticker's and
	// the ones a newer epoch triggers.
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// New connects to a cluster. The seeds are node addresses; the client
// asks them in random order for a ring and keeps the first answer, then
// routes by key from then on. A seed need not stay in the cluster.
func New(ctx context.Context, seeds []string, opts ...Option) (*Client, error) {
	o := Options{
		AttemptTimeout:  config.AttemptTimeout,
		MaxAttempts:     config.MaxAttempts,
		RefreshInterval: config.RingRefreshInterval,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.MaxAttempts < 1 {
		return nil, fmt.Errorf("client: MaxAttempts is %d, want at least 1", o.MaxAttempts)
	}
	if o.AttemptTimeout <= 0 {
		return nil, fmt.Errorf("client: AttemptTimeout is %s, want a positive duration", o.AttemptTimeout)
	}
	if len(seeds) == 0 && o.initialRing == nil {
		return nil, fmt.Errorf("client: no seeds")
	}

	c := &Client{opts: o, router: newRouter(seeds, o.dialOpts)}

	if o.initialRing != nil {
		if _, err := c.router.install(*o.initialRing); err != nil {
			return nil, err
		}
	} else if err := c.router.refresh(ctx, ""); err != nil {
		_ = c.router.close()
		return nil, err
	}

	bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c.cancel = cancel
	if o.RefreshInterval > 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.refreshLoop(bg)
		}()
	}
	return c, nil
}

// refreshLoop refreshes the ring from a random member on a ticker, so a
// client that is idle, or whose requests all happen to succeed, still
// notices a cluster that changed shape (FR-11).
func (c *Client) refreshLoop(ctx context.Context) {
	t := time.NewTicker(c.opts.RefreshInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			refreshCtx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
			_ = c.router.refresh(refreshCtx, "")
			cancel()
		}
	}
}

// refreshInBackground fetches a newer ring without delaying the call
// that noticed it. The call already has its answer; the refresh is for
// the ones after it.
func (c *Client) refreshInBackground(from string) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), c.opts.AttemptTimeout)
		defer cancel()
		_ = c.router.refresh(ctx, from)
	}()
}

// Ring returns the ring the client is routing with, or nil before it
// has one. The ring is immutable.
func (c *Client) Ring() *ring.Ring { return c.router.ring() }

// Epoch returns the epoch of the ring the client is routing with.
func (c *Client) Epoch() uint64 { return c.router.epoch() }

// Refresh fetches the ring now. The background refresh makes this
// unnecessary in normal use; a test that must not wait for a tick calls
// it directly.
func (c *Client) Refresh(ctx context.Context) error { return c.router.refresh(ctx, "") }

// Get reads a key. A key that is absent, deleted or expired reports
// found=false with no error, because a miss is the cache working.
func (c *Client) Get(ctx context.Context, key string) (value []byte, found bool, err error) {
	err = c.call(ctx, key, read, func(ctx context.Context, cc ringcachev1.CacheClient, _ ring.Member) (uint64, error) {
		resp, err := cc.Get(ctx, &ringcachev1.GetRequest{
			Key:         []byte(key),
			ClientEpoch: c.router.epoch(),
		})
		if err != nil {
			return 0, err
		}
		value, found = resp.GetValue(), resp.GetFound()
		return resp.GetNodeEpoch(), nil
	})
	if err != nil {
		return nil, false, err
	}
	return value, found, nil
}

// Set stores a value. A ttl above zero makes the key expire that long
// after the primary accepts the write; zero means no expiry.
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.call(ctx, key, write, func(ctx context.Context, cc ringcachev1.CacheClient, _ ring.Member) (uint64, error) {
		resp, err := cc.Set(ctx, &ringcachev1.SetRequest{
			Key:         []byte(key),
			Value:       value,
			TtlMs:       uint64(max(ttl, 0).Milliseconds()),
			ClientEpoch: c.router.epoch(),
		})
		if err != nil {
			return 0, err
		}
		return resp.GetNodeEpoch(), nil
	})
}

// Delete removes a key. Deleting an absent key is not an error.
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.call(ctx, key, write, func(ctx context.Context, cc ringcachev1.CacheClient, _ ring.Member) (uint64, error) {
		resp, err := cc.Delete(ctx, &ringcachev1.DeleteRequest{
			Key:         []byte(key),
			ClientEpoch: c.router.epoch(),
		})
		if err != nil {
			return 0, err
		}
		return resp.GetNodeEpoch(), nil
	})
}

// TTL reports the time left on a key. found is false when Get would
// miss it; hasTTL is false when it is stored without an expiry.
func (c *Client) TTL(ctx context.Context, key string) (remaining time.Duration, hasTTL, found bool, err error) {
	err = c.call(ctx, key, read, func(ctx context.Context, cc ringcachev1.CacheClient, _ ring.Member) (uint64, error) {
		resp, err := cc.TTL(ctx, &ringcachev1.TTLRequest{
			Key:         []byte(key),
			ClientEpoch: c.router.epoch(),
		})
		if err != nil {
			return 0, err
		}
		remaining = time.Duration(resp.GetRemainingMs()) * time.Millisecond
		hasTTL, found = resp.GetHasTtl(), resp.GetFound()
		return resp.GetNodeEpoch(), nil
	})
	if err != nil {
		return 0, false, false, err
	}
	return remaining, hasTTL, found, nil
}

// Close releases the client's connections and stops its background
// refreshes. It is safe to call more than once.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.wg.Wait()
		err = c.router.close()
	})
	return err
}

// Code returns the gRPC code of an error a Client method returned, so a
// caller can tell a cluster that is unavailable from a key that was
// rejected without importing the gRPC status package.
func Code(err error) string {
	if err == nil {
		return "OK"
	}
	if errors.Is(err, ErrNoRing) {
		return "NoRing"
	}
	return statusCode(err)
}

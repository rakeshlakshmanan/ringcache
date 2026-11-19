package workload

import (
	"context"
	"time"

	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

// Call is what one operation cost and who answered it.
type Call struct {
	// Attempts is how many RPCs the client sent.
	Attempts int
	// ServedBy is the ID of the node that produced the result.
	ServedBy string
	// Code is the gRPC code of the result.
	Code string
	// Refreshed is true when the call refreshed the client's ring.
	Refreshed bool
}

// Retried reports whether the call needed more than one attempt.
func (c Call) Retried() bool { return c.Attempts > 1 }

// Target is the cluster under load. The driver works through this
// rather than through pkg/client directly, so a test can supply a fake
// that loses or reorders writes on purpose and check that the checker
// notices.
type Target interface {
	Get(ctx context.Context, key string) (value []byte, found bool, call Call, err error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) (Call, error)
	Delete(ctx context.Context, key string) (Call, error)
	Close() error
}

// ClientTarget drives a real cluster through the public Go client.
type ClientTarget struct {
	c *client.Client
}

// NewClientTarget wraps a client. The client is closed when the target
// is.
func NewClientTarget(c *client.Client) *ClientTarget { return &ClientTarget{c: c} }

func (t *ClientTarget) Get(ctx context.Context, key string) ([]byte, bool, Call, error) {
	var info client.CallInfo
	value, found, err := t.c.Get(client.WithCallInfo(ctx, &info), key)
	return value, found, callOf(&info), err
}

func (t *ClientTarget) Set(ctx context.Context, key string, value []byte, ttl time.Duration) (Call, error) {
	var info client.CallInfo
	err := t.c.Set(client.WithCallInfo(ctx, &info), key, value, ttl)
	return callOf(&info), err
}

func (t *ClientTarget) Delete(ctx context.Context, key string) (Call, error) {
	var info client.CallInfo
	err := t.c.Delete(client.WithCallInfo(ctx, &info), key)
	return callOf(&info), err
}

// Close releases the client.
func (t *ClientTarget) Close() error { return t.c.Close() }

// Ring exposes the cluster's ring, which the report records so a run
// can be read back against the cluster it ran on.
func (t *ClientTarget) Ring() []string {
	r := t.c.Ring()
	if r == nil {
		return nil
	}
	ids := make([]string, 0, len(r.Members()))
	for _, m := range r.Members() {
		ids = append(ids, m.ID)
	}
	return ids
}

func callOf(info *client.CallInfo) Call {
	return Call{
		Attempts:  info.Attempts(),
		ServedBy:  info.ServedBy(),
		Code:      info.Code().String(),
		Refreshed: info.Refreshed(),
	}
}

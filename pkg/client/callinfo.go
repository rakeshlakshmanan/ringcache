package client

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
)

// CallInfo records how one client call went: how many attempts it took,
// which node answered and what the final code was.
//
// It exists because a cache client hides routing and retries, and the
// load generator in phase 7 has to attribute what it measures — a stale
// read to the replica that served it, a retry to the failover that
// caused it. Attach one with WithCallInfo.
type CallInfo struct {
	mu sync.Mutex

	// Attempts is how many RPCs were sent, including the one that
	// succeeded.
	attempts int
	// ServedBy is the ID of the member that produced the final result.
	servedBy string
	// Code is the gRPC code of the final result.
	code codes.Code
	// Epoch is the node_epoch the final response carried.
	epoch uint64
	// Refreshed is true when the call refreshed the client's ring,
	// which is what a wrong-owner error makes it do.
	refreshed bool
}

// Attempts returns how many RPCs the call sent.
func (ci *CallInfo) Attempts() int {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.attempts
}

// ServedBy returns the ID of the member that produced the final result,
// or "" when no attempt reached a node.
func (ci *CallInfo) ServedBy() string {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.servedBy
}

// Code returns the gRPC code of the final result.
func (ci *CallInfo) Code() codes.Code {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.code
}

// Epoch returns the node_epoch the final response carried.
func (ci *CallInfo) Epoch() uint64 {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.epoch
}

// Refreshed reports whether the call refreshed the client's ring.
func (ci *CallInfo) Refreshed() bool {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	return ci.refreshed
}

func (ci *CallInfo) noteAttempt(member string) {
	if ci == nil {
		return
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.attempts++
	ci.servedBy = member
}

func (ci *CallInfo) noteResult(code codes.Code, epoch uint64) {
	if ci == nil {
		return
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.code = code
	if epoch != 0 {
		ci.epoch = epoch
	}
}

func (ci *CallInfo) noteRefresh() {
	if ci == nil {
		return
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.refreshed = true
}

type callInfoKey struct{}

// WithCallInfo returns a context that fills ci as the call proceeds.
// Use one CallInfo per call: it is not reset between calls.
func WithCallInfo(ctx context.Context, ci *CallInfo) context.Context {
	return context.WithValue(ctx, callInfoKey{}, ci)
}

// callInfoFrom returns the CallInfo attached to ctx, or nil. Every
// method on CallInfo tolerates a nil receiver, so callers need not
// check.
func callInfoFrom(ctx context.Context) *CallInfo {
	ci, _ := ctx.Value(callInfoKey{}).(*CallInfo)
	return ci
}

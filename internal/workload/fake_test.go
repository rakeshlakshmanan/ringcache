package workload_test

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/workload"
)

// fakeTarget is an in-memory cluster that can be told to misbehave in
// exactly the ways the checker is supposed to notice. Without one,
// every test of the checker would depend on actually breaking a real
// cluster, which is the thing the checker is meant to detect.
type fakeTarget struct {
	mu    sync.Mutex
	items map[string][]byte
	// previous keeps the value each key held before its last write, so
	// the target can serve a stale read on purpose.
	previous map[string][]byte

	// dropWrites is the share of writes acknowledged without being
	// stored, which is what a lost acknowledged write looks like.
	dropWrites float64
	// staleReads is the share of reads served from the previous value.
	staleReads float64
	// failWrites is the share of writes that return an error.
	failWrites float64

	rng   *rand.Rand
	rngMu sync.Mutex

	dropped atomic.Int64
	staled  atomic.Int64
	failed  atomic.Int64
	gets    atomic.Int64
	sets    atomic.Int64
}

func newFakeTarget(seed uint64) *fakeTarget {
	return &fakeTarget{
		items:    make(map[string][]byte),
		previous: make(map[string][]byte),
		rng:      rand.New(rand.NewPCG(seed, seed+1)),
	}
}

// chance returns true with probability p, from the target's own PRNG.
func (f *fakeTarget) chance(p float64) bool {
	if p <= 0 {
		return false
	}
	f.rngMu.Lock()
	defer f.rngMu.Unlock()
	return f.rng.Float64() < p
}

func (f *fakeTarget) Get(_ context.Context, key string) ([]byte, bool, workload.Call, error) {
	f.gets.Add(1)
	call := workload.Call{Attempts: 1, ServedBy: "fake", Code: "OK"}

	f.mu.Lock()
	defer f.mu.Unlock()

	if prev, ok := f.previous[key]; ok && f.chance(f.staleReads) {
		f.staled.Add(1)
		return prev, true, call, nil
	}
	v, ok := f.items[key]
	return v, ok, call, nil
}

func (f *fakeTarget) Set(_ context.Context, key string, value []byte, _ time.Duration) (workload.Call, error) {
	f.sets.Add(1)
	call := workload.Call{Attempts: 1, ServedBy: "fake", Code: "OK"}

	if f.chance(f.failWrites) {
		f.failed.Add(1)
		call.Code = "Unavailable"
		return call, errFakeUnavailable
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.chance(f.dropWrites) {
		// Acknowledged, but never stored: the failure NFR-06 forbids.
		f.dropped.Add(1)
		return call, nil
	}

	if old, ok := f.items[key]; ok {
		f.previous[key] = old
	}
	f.items[key] = append([]byte(nil), value...)
	return call, nil
}

func (f *fakeTarget) Delete(_ context.Context, key string) (workload.Call, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.items, key)
	return workload.Call{Attempts: 1, ServedBy: "fake", Code: "OK"}, nil
}

func (f *fakeTarget) Close() error { return nil }

// errFakeUnavailable stands in for a node that could not be reached.
var errFakeUnavailable = fakeErr("fake: unavailable")

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

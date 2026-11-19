package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/hlc"
)

// TestStore_ConcurrentOps runs every path over one store at once, with
// the sweeper alongside, and then checks the bookkeeping survived. It
// uses the real clock so short TTLs actually expire and the sweep really
// races the write paths.
func TestStore_ConcurrentOps(t *testing.T) {
	const (
		goroutines = 16
		duration   = 2 * time.Second
		keyspace   = 500
	)

	c := clock.Real{}
	s := New(hlc.New("node-1", c), c)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup

	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 0x5eed))

			for ctx.Err() == nil {
				key := fmt.Sprintf("k-%03d", rng.IntN(keyspace))

				switch rng.IntN(7) {
				case 0:
					s.SetLocal(key, []byte("value"), 0)
				case 1:
					s.SetLocal(key, []byte("value"), 10*time.Millisecond)
				case 2:
					s.Get(key)
				case 3:
					s.DeleteLocal(key)
				case 4:
					s.Apply(key, Entry{
						Value: []byte("replicated"),
						Version: hlc.Version{
							WallMs:  c.Now().UnixMilli(),
							Logical: rng.Uint32(),
							NodeID:  42,
						},
					})
				case 5:
					s.TTL(key)
				case 6:
					//nolint:errcheck // the scan is cancelled by design at the deadline
					s.Scan(ctx, matchAll, 50, 1<<16, func([]KeyedEntry) error { return nil })
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.RunSweeper(ctx)
	}()

	wg.Wait()

	// The totals and the expiry index must still agree with the maps.
	assertConsistent(t, s)

	if got := s.Len(); got < 0 {
		t.Errorf("Len() = %d, want a count that is not negative", got)
	}
	if got := s.Bytes(); got < 0 {
		t.Errorf("Bytes() = %d, want a count that is not negative", got)
	}
	t.Logf("after %v: Len() = %d, Bytes() = %d", duration, s.Len(), s.Bytes())

	// Wipe has to leave the bookkeeping clean after all that.
	s.Wipe()
	assertConsistent(t, s)
}

// Two goroutines writing one key must never be handed the same version,
// or newest-wins could not order them.
func TestStore_ConcurrentWritesDistinctVersions(t *testing.T) {
	const (
		goroutines = 8
		perG       = 2_000
	)

	c := clock.Real{}
	s := New(hlc.New("node-1", c), c)

	versions := make([][]hlc.Version, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := make([]hlc.Version, perG)
			for i := range got {
				got[i] = s.SetLocal("contended", []byte("v"), 0).Version
			}
			versions[g] = got
		}()
	}
	wg.Wait()

	seen := make(map[hlc.Version]struct{}, goroutines*perG)
	for _, vs := range versions {
		for _, v := range vs {
			if _, dup := seen[v]; dup {
				t.Fatalf("version %v was handed to two writes of the same key", v)
			}
			seen[v] = struct{}{}
		}
	}
}

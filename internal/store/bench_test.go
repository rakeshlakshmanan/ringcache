package store

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/hlc"
)

const (
	benchKeys  = 100_000
	benchValue = 100 // the benchmark value size used throughout the project
)

// benchFixture returns a store preloaded with benchKeys entries and the
// key slice, so the benchmark loop measures the store and not fmt.
func benchFixture(b *testing.B) (*Store, []string) {
	b.Helper()

	c := clock.Real{}
	s := New(hlc.New("node-1", c), c)

	keys := make([]string, benchKeys)
	value := make([]byte, benchValue)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%06d", i)
		s.SetLocal(keys[i], value, 0)
	}
	return s, keys
}

func BenchmarkStore_GetParallel(b *testing.B) {
	s, keys := benchFixture(b)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkStore_SetParallel(b *testing.B) {
	s, keys := benchFixture(b)
	value := make([]byte, benchValue)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.SetLocal(keys[i%len(keys)], value, 0)
			i++
		}
	})
}

func BenchmarkStore_ApplyParallel(b *testing.B) {
	s, keys := benchFixture(b)
	value := make([]byte, benchValue)

	// The synthetic versions have to beat the preloaded entries, which
	// carry real wall-clock versions, or every apply is rejected as
	// stale and this measures the reject path. 100 ms ahead clears them
	// and stays under the clock-skew warning threshold.
	base := time.Now().UnixMilli() + 100

	// One shared sequence keeps versions globally increasing so applies
	// are stored. Per-goroutine counters would collide on the same key.
	// The sequence is split across the wall and logical fields so the
	// counter can't overflow on a long run.
	var seq, applied atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var stored int64
		i := 0
		for pb.Next() {
			n := seq.Add(1)
			ok := s.Apply(keys[i%len(keys)], Entry{
				Value: value,
				Version: hlc.Version{
					WallMs:  base + n/1_000_000,
					Logical: uint32(n % 1_000_000),
					NodeID:  42,
				},
			})
			if ok {
				stored++
			}
			i++
		}
		applied.Add(stored)
	})
	b.StopTimer()

	// Reported so a low accept rate can't quietly flatter the result.
	if b.N > 0 {
		b.ReportMetric(100*float64(applied.Load())/float64(b.N), "%stored")
	}
}

package hlc

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
)

func TestHLC_MonotonicSingleGoroutine(t *testing.T) {
	k := New("node-1", clock.Real{})

	prev := k.Now()
	for i := 1; i < 1_000_000; i++ {
		got := k.Now()
		if Compare(got, prev) <= 0 {
			t.Fatalf("call %d: Now() = %v, not greater than previous %v", i, got, prev)
		}
		prev = got
	}
}

func TestHLC_MonotonicConcurrent(t *testing.T) {
	const (
		goroutines = 8
		perG       = 100_000
	)
	k := New("node-1", clock.Real{})

	// Each goroutine fills its own slice; they're merged afterwards, so
	// the test measures the clock's locking and not a shared map's.
	got := make([][]Version, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			vs := make([]Version, perG)
			for i := range vs {
				vs[i] = k.Now()
			}
			got[g] = vs
		}()
	}
	wg.Wait()

	seen := make(map[Version]struct{}, goroutines*perG)
	for _, vs := range got {
		for _, v := range vs {
			if _, dup := seen[v]; dup {
				t.Fatalf("duplicate version %v issued to two goroutines", v)
			}
			seen[v] = struct{}{}
		}
	}
	if len(seen) != goroutines*perG {
		t.Errorf("got %d distinct versions, want %d", len(seen), goroutines*perG)
	}
}

func TestHLC_ClockSetBack(t *testing.T) {
	f := clock.NewFake()
	k := New("node-1", f)

	before := k.Now()

	f.Advance(-10 * time.Second)

	for i := range 100 {
		got := k.Now()
		if Compare(got, before) <= 0 {
			t.Fatalf("call %d after the clock moved back: Now() = %v, not greater than %v", i, got, before)
		}
		before = got
	}
}

func TestHLC_UpdateRemoteAhead(t *testing.T) {
	f := clock.NewFake()
	k := New("node-1", f)

	remote := Version{WallMs: f.Now().UnixMilli() + 5_000, Logical: 7, NodeID: 42}
	k.Update(remote)

	got := k.Now()
	if Compare(got, remote) <= 0 {
		t.Errorf("Now() = %v after Update(%v), want something greater", got, remote)
	}
}

// A remote version behind local physical time carries no information, so
// Update must degenerate into an ordinary local tick: it may not adopt
// the remote's counter or drag the wall value anywhere.
func TestHLC_UpdateRemoteBehind(t *testing.T) {
	f := clock.NewFake()
	updated := New("node-1", f)
	control := New("node-1", f)

	// Both clocks take one version so they start level.
	if a, b := updated.Now(), control.Now(); Compare(a, b) != 0 {
		t.Fatalf("clocks did not start level: %v vs %v", a, b)
	}

	// A remote version 5 s behind, with a high counter that must not
	// leak into the local clock.
	remote := Version{WallMs: f.Now().UnixMilli() - 5_000, Logical: 9_999, NodeID: 42}
	updated.Update(remote)

	// The control clock just ticks locally the same number of times.
	control.Now()

	if a, b := updated.Now(), control.Now(); Compare(a, b) != 0 {
		t.Errorf("after Update with a version behind physical time: Now() = %v, want %v (an ordinary local tick)", a, b)
	}
}

func TestHLC_NowAfter(t *testing.T) {
	f := clock.NewFake()
	nowMs := f.Now().UnixMilli()

	tests := []struct {
		name string
		arg  Version
	}{
		{"ahead", Version{WallMs: nowMs + 5_000, Logical: 3, NodeID: 42}},
		{"level", Version{WallMs: nowMs, Logical: 3, NodeID: 42}},
		{"behind", Version{WallMs: nowMs - 5_000, Logical: 3, NodeID: 42}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := New("node-1", clock.NewFake())
			if got := k.NowAfter(tt.arg); Compare(got, tt.arg) <= 0 {
				t.Errorf("NowAfter(%v) = %v, want something greater", tt.arg, got)
			}
		})
	}
}

// The counter can only be exhausted by a peer sending a version at the
// top of its range, since no node issues four billion versions inside
// one millisecond. Carrying into the next millisecond keeps versions
// increasing instead of wrapping to zero.
func TestHLC_CounterOverflowCarries(t *testing.T) {
	f := clock.NewFake()
	k := New("node-1", f)
	nowMs := f.Now().UnixMilli()

	remote := Version{WallMs: nowMs, Logical: math.MaxUint32, NodeID: 42}
	got := k.NowAfter(remote)

	if Compare(got, remote) <= 0 {
		t.Fatalf("NowAfter(%v) = %v, want something greater", remote, got)
	}
	if got.WallMs != nowMs+1 {
		t.Errorf("WallMs = %d, want %d (the counter should carry into the next millisecond)", got.WallMs, nowMs+1)
	}
}

func TestVersion_Compare(t *testing.T) {
	tests := []struct {
		name string
		a, b Version
		want int
	}{
		{"identical", Version{10, 2, 7}, Version{10, 2, 7}, 0},
		{"zero equals zero", Version{}, Version{}, 0},
		{"wall older", Version{9, 5, 7}, Version{10, 0, 0}, -1},
		{"wall newer", Version{11, 0, 0}, Version{10, 9, 9}, 1},
		{"wall beats logical", Version{10, 0, 0}, Version{9, 4_000_000_000, 0}, 1},
		{"logical older at same wall", Version{10, 1, 7}, Version{10, 2, 0}, -1},
		{"logical newer at same wall", Version{10, 3, 0}, Version{10, 2, 9}, 1},
		{"logical beats node id", Version{10, 2, 0}, Version{10, 1, 65_535}, 1},
		{"node id breaks the tie, lower", Version{10, 2, 7}, Version{10, 2, 8}, -1},
		{"node id breaks the tie, higher", Version{10, 2, 9}, Version{10, 2, 8}, 1},
		{"zero is older than any real version", Version{}, Version{1, 0, 0}, -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Compare(tt.a, tt.b); got != tt.want {
				t.Errorf("Compare(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
			// Compare must be antisymmetric.
			if got, want := Compare(tt.b, tt.a), -tt.want; got != want {
				t.Errorf("Compare(%v, %v) = %d, want %d", tt.b, tt.a, got, want)
			}
		})
	}
}

func TestVersion_IsZeroAndString(t *testing.T) {
	if !(Version{}).IsZero() {
		t.Error("the zero Version reports IsZero() = false")
	}
	for _, v := range []Version{{WallMs: 1}, {Logical: 1}, {NodeID: 1}} {
		if v.IsZero() {
			t.Errorf("%#v reports IsZero() = true", v)
		}
	}
	if got, want := (Version{WallMs: 1763300000000, Logical: 5, NodeID: 42}).String(), "1763300000000.5@42"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

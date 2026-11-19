package ring

import (
	"math/big"
	"slices"
	"testing"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
)

// full is 2^64: the number of positions on the ring.
var full = new(big.Int).Lsh(big.NewInt(1), 64)

// assertCoversRing checks that ranges, in order, tile the ring exactly
// once: each begins where the previous one ended, and their sizes add
// up to 2^64.
func assertCoversRing(t *testing.T, ranges []Range) {
	t.Helper()

	if len(ranges) == 0 {
		t.Fatal("no ranges at all, so the ring is not covered")
	}

	total := new(big.Int)
	for i, r := range ranges {
		prev := ranges[(i-1+len(ranges))%len(ranges)]
		if prev.End != r.Start {
			t.Fatalf("range %d starts at %d but the previous one ends at %d: a gap or an overlap",
				i, r.Start, prev.End)
		}
		total.Add(total, r.Size())
	}

	if total.Cmp(full) != 0 {
		t.Errorf("ranges cover %s positions, want 2^64 = %s", total, full)
	}
}

func TestRing_ArcsCoverRing(t *testing.T) {
	for _, n := range []int{1, 2, 3, 5, 7} {
		t.Run("members="+string(rune('0'+n)), func(t *testing.T) {
			r := newRing(t, n)
			arcs := r.Arcs()

			ranges := make([]Range, len(arcs))
			for i, a := range arcs {
				ranges[i] = a.Range
			}
			assertCoversRing(t, ranges)

			t.Logf("%d members, %d tokens, %d arcs after merging", n, len(r.tokens), len(arcs))
		})
	}
}

// Every key must be held by the arc that contains it, with the same
// preference list the ring computes directly. The arcs and the lookup
// path are separate code, so this is what ties them together.
func TestRing_ArcsMatchPreferenceLists(t *testing.T) {
	r := newRing(t, 5)
	arcs := r.Arcs()

	for _, h := range keyHashes(20_000) {
		idx := slices.IndexFunc(arcs, func(a Arc) bool { return a.Contains(h) })
		if idx < 0 {
			t.Fatalf("no arc contains %d, although the arcs cover the ring", h)
		}
		if !slices.Equal(arcs[idx].Pref, r.PreferenceIDs(h)) {
			t.Fatalf("arc %s has %v but the ring gives %v for %d",
				arcs[idx].Range, arcs[idx].Pref, r.PreferenceIDs(h), h)
		}
		// Exactly one arc, since they tile the ring.
		for j, a := range arcs {
			if j != idx && a.Contains(h) {
				t.Fatalf("%d is in both arc %d and arc %d", h, idx, j)
			}
		}
	}
}

// Merging is what makes an arc the coarsest description of ownership:
// no two neighbours may be left with the same list.
func TestRing_ArcsMerged(t *testing.T) {
	r := newRing(t, 5)
	arcs := r.Arcs()

	if len(arcs) >= len(r.tokens) {
		t.Errorf("%d arcs from %d tokens: with RF 2 neighbouring tokens of one member share a list, so some must merge",
			len(arcs), len(r.tokens))
	}
	for i, a := range arcs {
		prev := arcs[(i-1+len(arcs))%len(arcs)]
		if len(arcs) > 1 && slices.Equal(prev.Pref, a.Pref) {
			t.Fatalf("arcs %s and %s both have %v and were not merged", prev.Range, a.Range, a.Pref)
		}
	}
}

// A single member owns the whole ring in one arc, even though it placed
// 256 tokens on it.
func TestRing_ArcsSingleMember(t *testing.T) {
	r, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: 2, Members: members(1)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	arcs := r.Arcs()
	if len(arcs) != 1 {
		t.Fatalf("got %d arcs for one member, want 1", len(arcs))
	}
	if !arcs[0].Contains(0) || !arcs[0].Contains(^uint64(0)) {
		t.Errorf("the single arc %s does not cover the whole ring", arcs[0].Range)
	}
	if !slices.Equal(arcs[0].Pref, []string{"node-1"}) {
		t.Errorf("Pref = %v, want [node-1]", arcs[0].Pref)
	}
}

func TestRing_ArcsFor(t *testing.T) {
	r := newRing(t, 5)
	all := r.Arcs()

	for _, id := range []string{"node-1", "node-3", "node-5"} {
		mine := r.ArcsFor(id)
		if len(mine) == 0 {
			t.Fatalf("ArcsFor(%q) returned nothing", id)
		}
		for _, a := range mine {
			if !slices.Contains(a.Pref, id) {
				t.Errorf("ArcsFor(%q) returned %s with %v", id, a.Range, a.Pref)
			}
		}

		// A node holds a copy of every key in an arc it appears in, and
		// of no other key, so the two sets have to agree exactly.
		want := 0
		for _, a := range all {
			if slices.Contains(a.Pref, id) {
				want++
			}
		}
		if len(mine) != want {
			t.Errorf("ArcsFor(%q) gave %d arcs, want %d", id, len(mine), want)
		}

		primary := r.PrimaryArcsFor(id)
		if len(primary) == 0 || len(primary) > len(mine) {
			t.Errorf("PrimaryArcsFor(%q) gave %d arcs out of %d", id, len(primary), len(mine))
		}
		for _, a := range primary {
			if a.Pref[0] != id {
				t.Errorf("PrimaryArcsFor(%q) returned %s whose primary is %s", id, a.Range, a.Pref[0])
			}
		}
	}

	if got := r.ArcsFor("node-9"); got != nil {
		t.Errorf("ArcsFor of a non-member gave %d arcs", len(got))
	}
}

// The first and last arcs are neighbours across the top of the ring,
// so a run of identical lists straddling the top has to be merged too.
// That only happens when the lowest and highest tokens belong to the
// same member, which the production shape (5 members, 256 tokens) does
// not do, so the case is pinned with an input that does: with two
// members and 23 tokens each, node-2 holds both ends of the ring.
func TestRing_ArcsMergeAcrossTopOfRing(t *testing.T) {
	r, err := New(View{Epoch: 1, VNodes: 23, RF: 2, Members: members(2)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	lowest, highest := r.tokens[0], r.tokens[len(r.tokens)-1]
	if lowest.member != highest.member {
		t.Fatalf("this input no longer puts one member at both ends of the ring, so it proves nothing")
	}

	arcs := r.Arcs()
	assertCoversRing(t, rangesOf(arcs))

	// The merged arc is the one that straddles the top, so it wraps.
	if !arcs[0].Wraps() {
		t.Errorf("arc %s does not wrap, so the run across the top was left unmerged", arcs[0].Range)
	}
	if len(arcs) > 1 && slices.Equal(arcs[len(arcs)-1].Pref, arcs[0].Pref) {
		t.Errorf("first arc %s and last arc %s both have %v and were not merged",
			arcs[0].Range, arcs[len(arcs)-1].Range, arcs[0].Pref)
	}

	// The merge must not lose any key: every hash still resolves to the
	// arc the ring agrees with.
	for _, h := range keyHashes(5_000) {
		idx := slices.IndexFunc(arcs, func(a Arc) bool { return a.Contains(h) })
		if idx < 0 {
			t.Fatalf("no arc contains %d after merging across the top", h)
		}
		if !slices.Equal(arcs[idx].Pref, r.PreferenceIDs(h)) {
			t.Fatalf("arc %s has %v but the ring gives %v for %d",
				arcs[idx].Range, arcs[idx].Pref, r.PreferenceIDs(h), h)
		}
	}
}

func rangesOf(arcs []Arc) []Range {
	out := make([]Range, len(arcs))
	for i, a := range arcs {
		out[i] = a.Range
	}
	return out
}

// mergeAdjacent is the one piece of ring code whose wrap-around branch
// most inputs never reach, so it is also tested directly.
func TestMergeAdjacent(t *testing.T) {
	type entry struct {
		Range
		pref string
	}
	merge := func(in []entry) []entry {
		return mergeAdjacent(in,
			func(e entry) Range { return e.Range },
			func(e *entry, r Range) { e.Range = r },
			func(a, b entry) bool { return a.pref == b.pref },
		)
	}

	tests := []struct {
		name string
		in   []entry
		want []entry
		// gappy marks an input that does not tile the ring, as Diff's
		// does: the result is then not expected to tile it either.
		gappy bool
	}{
		{
			name: "nothing merges",
			in:   []entry{{Range{30, 10}, "a"}, {Range{10, 20}, "b"}, {Range{20, 30}, "c"}},
			want: []entry{{Range{30, 10}, "a"}, {Range{10, 20}, "b"}, {Range{20, 30}, "c"}},
		},
		{
			name: "neighbours merge",
			in:   []entry{{Range{30, 10}, "a"}, {Range{10, 20}, "a"}, {Range{20, 30}, "b"}},
			want: []entry{{Range{30, 20}, "a"}, {Range{20, 30}, "b"}},
		},
		{
			// The run straddles the top of the ring: the last entry and
			// the first are neighbours there.
			name: "run across the top merges",
			in:   []entry{{Range{30, 10}, "a"}, {Range{10, 20}, "b"}, {Range{20, 30}, "a"}},
			want: []entry{{Range{20, 10}, "a"}, {Range{10, 20}, "b"}},
		},
		{
			// Everything is one list, so the ring collapses to a single
			// arc. Merging that with itself would lose the ring.
			name: "all the same collapses to the whole ring",
			in:   []entry{{Range{30, 10}, "a"}, {Range{10, 20}, "a"}, {Range{20, 30}, "a"}},
			want: []entry{{Range{30, 30}, "a"}},
		},
		{
			name: "two entries, one list",
			in:   []entry{{Range{20, 10}, "a"}, {Range{10, 20}, "a"}},
			want: []entry{{Range{20, 20}, "a"}},
		},
		{
			name: "two entries, two lists",
			in:   []entry{{Range{20, 10}, "a"}, {Range{10, 20}, "b"}},
			want: []entry{{Range{20, 10}, "a"}, {Range{10, 20}, "b"}},
		},
		{
			name: "a single entry is left alone",
			in:   []entry{{Range{7, 7}, "a"}},
			want: []entry{{Range{7, 7}, "a"}},
		},
		{
			name: "empty",
			in:   nil,
			want: nil,
		},
		{
			// Diff passes only the stretches that differ, so neighbours
			// in the slice can have a gap between them. Merging across
			// one would claim (10,15], whose ownership did not change.
			name:  "a gap blocks the merge",
			in:    []entry{{Range{30, 10}, "a"}, {Range{15, 20}, "a"}},
			want:  []entry{{Range{30, 10}, "a"}, {Range{15, 20}, "a"}},
			gappy: true,
		},
		{
			name:  "a gap blocks the merge across the top too",
			in:    []entry{{Range{35, 10}, "a"}, {Range{15, 20}, "b"}, {Range{25, 30}, "a"}},
			want:  []entry{{Range{35, 10}, "a"}, {Range{15, 20}, "b"}, {Range{25, 30}, "a"}},
			gappy: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := merge(slices.Clone(tt.in))
			if !slices.Equal(got, tt.want) {
				t.Fatalf("mergeAdjacent(%v) = %v, want %v", tt.in, got, tt.want)
			}
			// Merging must never leave a gap or an overlap in an input
			// that had none.
			if len(got) > 0 && !tt.gappy {
				ranges := make([]Range, len(got))
				for i, e := range got {
					ranges[i] = e.Range
				}
				assertCoversRing(t, ranges)
			}
		})
	}
}

// With RF equal to the member count every node is in every list, so
// every arc has the same membership but not the same order. They must
// not merge: the order is what names the primary.
func TestRing_ArcsOrderMatters(t *testing.T) {
	r, err := New(View{Epoch: 1, VNodes: 8, RF: 3, Members: members(3)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	arcs := r.Arcs()
	if len(arcs) < 2 {
		t.Fatalf("got %d arcs, want several: every node is in every list but the orders differ", len(arcs))
	}
	seen := map[string]bool{}
	for _, a := range arcs {
		if len(a.Pref) != 3 {
			t.Fatalf("arc %s has %v, want all three members", a.Range, a.Pref)
		}
		seen[a.Pref[0]] = true
	}
	if len(seen) != 3 {
		t.Errorf("only %d distinct primaries across the arcs, want 3", len(seen))
	}
}

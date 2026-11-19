package ring

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
)

func TestDiff_Identical(t *testing.T) {
	r := newRing(t, 5)
	if got := Diff(r, r); len(got) != 0 {
		t.Errorf("Diff of a ring with itself gave %d changes, want none", len(got))
	}

	// Two separately built rings with the same members are the same ring.
	again := newRing(t, 5)
	if got := Diff(r, again); len(got) != 0 {
		t.Errorf("Diff of two equal rings gave %d changes, want none", len(got))
	}
}

// The contract Diff has to meet: a key lies in a Change exactly when
// the two rings disagree about it, and that Change carries the two
// lists. Everything the migration planners do rests on this.
func TestDiff_MatchesPreferenceLists(t *testing.T) {
	const keys = 100_000

	before, after := newRing(t, 4), newRing(t, 5)
	changes := Diff(before, after)
	if len(changes) == 0 {
		t.Fatal("adding a node produced no changes")
	}

	rng := rand.New(rand.NewPCG(42, 1))
	differing, same := 0, 0

	for range keys {
		h := rng.Uint64()
		oldPref := before.PreferenceIDs(h)
		newPref := after.PreferenceIDs(h)
		shouldDiffer := !slices.Equal(oldPref, newPref)

		idx := slices.IndexFunc(changes, func(c Change) bool { return c.Contains(h) })

		if !shouldDiffer {
			same++
			if idx >= 0 {
				t.Fatalf("%d is in change %s although both rings give %v", h, changes[idx].Range, oldPref)
			}
			continue
		}

		differing++
		if idx < 0 {
			t.Fatalf("%d is in no change although the rings give %v and %v", h, oldPref, newPref)
		}
		c := changes[idx]
		if !slices.Equal(c.Old, oldPref) || !slices.Equal(c.New, newPref) {
			t.Fatalf("change %s carries %v -> %v but %d actually goes %v -> %v",
				c.Range, c.Old, c.New, h, oldPref, newPref)
		}
	}

	if differing == 0 || same == 0 {
		t.Fatalf("the sample was one-sided: %d differing, %d unchanged", differing, same)
	}
	t.Logf("%d changes; of %d random keys %d differed and %d did not", len(changes), keys, differing, same)
}

// The changes have to be disjoint, or a key would be planned for twice.
func TestDiff_ChangesDisjoint(t *testing.T) {
	changes := Diff(newRing(t, 4), newRing(t, 5))

	rng := rand.New(rand.NewPCG(7, 7))
	for range 50_000 {
		h := rng.Uint64()
		hits := 0
		for _, c := range changes {
			if c.Contains(h) {
				hits++
			}
		}
		if hits > 1 {
			t.Fatalf("%d falls in %d changes, want at most 1", h, hits)
		}
	}

	// Neighbouring changes with the same pair of lists must have been
	// merged, otherwise the plan would carry needless duplicates.
	for i, c := range changes {
		prev := changes[(i-1+len(changes))%len(changes)]
		if len(changes) > 1 && prev.End == c.Start &&
			slices.Equal(prev.Old, c.Old) && slices.Equal(prev.New, c.New) {
			t.Fatalf("changes %s and %s are adjacent with the same lists and were not merged",
				prev.Range, c.Range)
		}
	}
}

// A removal is the failover case: every change must hand the key to the
// node that already holds a copy.
func TestDiff_Removal(t *testing.T) {
	five := newRing(t, 5)
	remaining := slices.DeleteFunc(members(5), func(m Member) bool { return m.ID == "node-3" })
	four, err := New(View{Epoch: 2, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: remaining})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	changes := Diff(five, four)
	if len(changes) == 0 {
		t.Fatal("removing a node produced no changes")
	}
	for _, c := range changes {
		if slices.Contains(c.New, "node-3") {
			t.Errorf("change %s still lists the removed node: %v", c.Range, c.New)
		}
		if !slices.Contains(c.Old, "node-3") {
			t.Errorf("change %s does not involve the removed node: %v -> %v", c.Range, c.Old, c.New)
		}
	}
	t.Logf("removing node-3 from five produced %d changes", len(changes))
}

// The first published ring has no predecessor, so Diff has to accept a
// nil old ring: the whole ring is new.
func TestDiff_FromNil(t *testing.T) {
	r := newRing(t, 3)
	changes := Diff(nil, r)

	ranges := make([]Range, len(changes))
	for i, c := range changes {
		if c.Old != nil {
			t.Errorf("change %s has an old list %v, want none", c.Range, c.Old)
		}
		if len(c.New) != config.ReplicationFactor {
			t.Errorf("change %s has %v, want %d members", c.Range, c.New, config.ReplicationFactor)
		}
		ranges[i] = c.Range
	}
	// Nothing was owned before, so every position changed hands.
	assertCoversRing(t, ranges)

	if got := Diff(nil, nil); got != nil {
		t.Errorf("Diff(nil, nil) = %v, want nothing", got)
	}
}

// Only the epoch differing must not produce changes: the ring is the
// members, not the number it was published under.
func TestDiff_EpochOnly(t *testing.T) {
	a, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: 2, Members: members(4)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, err := New(View{Epoch: 99, VNodes: config.VNodes, RF: 2, Members: members(4)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := Diff(a, b); len(got) != 0 {
		t.Errorf("a new epoch with the same members gave %d changes, want none", len(got))
	}
}

func BenchmarkDiff(b *testing.B) {
	before, after := benchRing(b, 4), benchRing(b, 5)

	b.ReportAllocs()
	for b.Loop() {
		if len(Diff(before, after)) == 0 {
			b.Fatal("no changes")
		}
	}
}

func benchRing(b *testing.B, n int) *Ring {
	b.Helper()
	r, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: members(n)})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	return r
}

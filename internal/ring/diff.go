package ring

import "slices"

// Change is a stretch of the ring whose preference list differs between
// two rings. Old is the list the previous ring gave, New the list the
// next one gives; either may be empty when the corresponding ring did
// not exist.
//
// A Change says only that ownership differs. Whether data has to be
// copied, and in which direction, is for the planners in phases 11 and
// 12 to decide from Old and New.
type Change struct {
	Range
	Old, New []string
}

// Diff returns the stretches of the ring whose preference lists differ
// between old and next, in ring order, with neighbouring stretches that
// have the same pair of lists merged.
//
// Either ring may be nil, which stands for "no ring": the diff from nil
// is then the whole of next's ownership, as the first published ring is.
//
// It works on the sorted union of both rings' token positions. Between
// two neighbouring positions neither ring has a token, so neither
// ring's preference list can change there; evaluating both lists once
// at the stretch's end position therefore describes the whole stretch.
func Diff(old, next *Ring) []Change {
	positions := unionPositions(old, next)
	if len(positions) == 0 {
		return nil
	}

	changes := make([]Change, 0, len(positions))
	var oldBuf, nextBuf []int

	for i, end := range positions {
		start := positions[(i-1+len(positions))%len(positions)]

		var oldPref, nextPref []string
		if old != nil {
			oldBuf = old.prefIndexes(end, oldBuf)
			oldPref = old.idsOf(oldBuf)
		}
		if next != nil {
			nextBuf = next.prefIndexes(end, nextBuf)
			nextPref = next.idsOf(nextBuf)
		}
		if slices.Equal(oldPref, nextPref) {
			continue
		}
		changes = append(changes, Change{
			Range: Range{Start: start, End: end},
			Old:   oldPref,
			New:   nextPref,
		})
	}

	// Only stretches that differ are kept, so the kept ones are not
	// necessarily neighbours; mergeAdjacent joins the pairs that are.
	return mergeAdjacent(changes,
		func(c Change) Range { return c.Range },
		func(c *Change, rng Range) { c.Range = rng },
		func(a, b Change) bool {
			return slices.Equal(a.Old, b.Old) && slices.Equal(a.New, b.New)
		},
	)
}

// unionPositions returns both rings' token positions, sorted and
// deduplicated.
func unionPositions(old, next *Ring) []uint64 {
	n := 0
	if old != nil {
		n += len(old.tokens)
	}
	if next != nil {
		n += len(next.tokens)
	}

	positions := make([]uint64, 0, n)
	for _, r := range []*Ring{old, next} {
		if r == nil {
			continue
		}
		for _, t := range r.tokens {
			positions = append(positions, t.pos)
		}
	}
	slices.Sort(positions)
	return slices.Compact(positions)
}

// idsOf names the members at the given indexes. The result is a fresh
// slice, because the index buffer it reads is reused across calls.
func (r *Ring) idsOf(idx []int) []string {
	out := make([]string, len(idx))
	for i, m := range idx {
		out[i] = r.members[m].ID
	}
	return out
}

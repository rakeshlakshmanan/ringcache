package ring

import "slices"

// Arc is a stretch of the ring over which the preference list does not
// change. Every key whose hash falls in the arc is held by the same
// members in the same order, which is what makes an arc the unit the
// migration and repair planners move.
type Arc struct {
	Range
	// Pref is the preference list over this arc, primary first.
	Pref []string
}

// Arcs returns the ring's arcs in ring order, covering the ring exactly
// once. Neighbouring arcs with identical preference lists are merged,
// including across the top of the ring, so the result is the coarsest
// description of who holds what.
func (r *Ring) Arcs() []Arc {
	arcs := make([]Arc, len(r.tokens))
	buf := make([]int, 0, r.view.RF)

	for i, t := range r.tokens {
		// Arc i runs from the previous token's position to this one's,
		// wrapping at the start of the slice.
		prev := r.tokens[(i-1+len(r.tokens))%len(r.tokens)].pos

		buf = r.prefIndexes(t.pos, buf)
		pref := make([]string, len(buf))
		for j, m := range buf {
			pref[j] = r.members[m].ID
		}
		arcs[i] = Arc{Range: Range{Start: prev, End: t.pos}, Pref: pref}
	}

	return mergeAdjacent(arcs,
		func(a Arc) Range { return a.Range },
		func(a *Arc, rng Range) { a.Range = rng },
		func(a, b Arc) bool { return slices.Equal(a.Pref, b.Pref) },
	)
}

// ArcsFor returns the arcs whose preference list contains id, in ring
// order. A node asks for its own arcs to know which ranges it should
// hold; the position in Pref says whether it is the primary or a
// replica.
func (r *Ring) ArcsFor(id string) []Arc {
	var out []Arc
	for _, a := range r.Arcs() {
		if slices.Contains(a.Pref, id) {
			out = append(out, a)
		}
	}
	return out
}

// PrimaryArcsFor returns the arcs id is the primary of. Phase 11's
// repair and phase 12's moves both start from a node's primary arcs,
// because the primary is what has to be rebuilt first.
func (r *Ring) PrimaryArcsFor(id string) []Arc {
	var out []Arc
	for _, a := range r.Arcs() {
		if len(a.Pref) > 0 && a.Pref[0] == id {
			out = append(out, a)
		}
	}
	return out
}

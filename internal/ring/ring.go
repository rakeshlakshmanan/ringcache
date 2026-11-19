package ring

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
)

// token is one member's position on the ring.
type token struct {
	pos uint64
	// member indexes into Ring.members rather than naming the member, so
	// a preference list walk compares small integers.
	member int
}

// Ring maps hashes to the members that hold them. It is immutable once
// built, so any number of goroutines may read it; publishing a new ring
// means building another one and swapping the pointer.
type Ring struct {
	view    View
	members []Member // sorted by ID
	tokens  []token  // sorted by position
}

// New builds the ring described by v.
//
// Members are sorted by ID first, so the order they arrive in never
// changes the result. Token i of member m sits at
// keyhash.Sum(m.ID + "#" + i), and tokens are sorted by position with
// ties broken by member ID, so every process computes the same ring.
func New(v View) (*Ring, error) {
	if err := v.Validate(); err != nil {
		return nil, err
	}

	r := &Ring{
		view:    v.Clone(),
		members: sortedMembers(v.Members),
	}

	r.tokens = make([]token, 0, len(r.members)*v.VNodes)
	for i, m := range r.members {
		for j := range v.VNodes {
			r.tokens = append(r.tokens, token{
				pos:    keyhash.Sum(m.ID + "#" + strconv.Itoa(j)),
				member: i,
			})
		}
	}

	slices.SortFunc(r.tokens, func(a, b token) int {
		if a.pos != b.pos {
			if a.pos < b.pos {
				return -1
			}
			return 1
		}
		// Equal positions are broken by member ID, which is the members
		// slice's order, so the result stays deterministic.
		return a.member - b.member
	})

	// Two tokens at the same position would give the second one an arc
	// of (p, p], which the half-open rule reads as the whole ring. A
	// 64-bit collision is vanishingly unlikely, but the ring's own
	// invariants should not depend on that, so the duplicate is dropped
	// and its position stays with the member that sorted first.
	r.tokens = slices.CompactFunc(r.tokens, func(a, b token) bool { return a.pos == b.pos })

	if len(r.tokens) == 0 {
		return nil, fmt.Errorf("ring: no tokens for %d members", len(r.members))
	}
	return r, nil
}

// Epoch returns the ring's epoch.
func (r *Ring) Epoch() uint64 { return r.view.Epoch }

// RF returns the ring's replication factor.
func (r *Ring) RF() int { return r.view.RF }

// View returns the view this ring was built from.
func (r *Ring) View() View { return r.view.Clone() }

// Members returns the ring's members, ordered by ID.
func (r *Ring) Members() []Member { return slices.Clone(r.members) }

// Has reports whether id is a member of this ring.
func (r *Ring) Has(id string) bool {
	_, found := slices.BinarySearchFunc(r.members, id, func(m Member, id string) int {
		if m.ID != id {
			if m.ID < id {
				return -1
			}
			return 1
		}
		return 0
	})
	return found
}

// tokenIndex returns the index of the first token at or after h,
// wrapping to the first token past the end of the ring.
func (r *Ring) tokenIndex(h uint64) int {
	i, _ := slices.BinarySearchFunc(r.tokens, h, func(t token, h uint64) int {
		if t.pos != h {
			if t.pos < h {
				return -1
			}
			return 1
		}
		return 0
	})
	if i == len(r.tokens) {
		// Past the last token: the ring wraps to the first.
		return 0
	}
	return i
}

// Lookup returns the member that holds the hash h: the primary for any
// key with that hash. A key landing exactly on a token belongs to that
// token.
func (r *Ring) Lookup(h uint64) Member {
	return r.members[r.tokens[r.tokenIndex(h)].member]
}

// PreferenceList returns the members holding h, primary first. It has
// RF entries, or one per member when the ring is smaller than RF.
func (r *Ring) PreferenceList(h uint64) []Member {
	idx := r.prefIndexes(h, nil)
	out := make([]Member, len(idx))
	for i, m := range idx {
		out[i] = r.members[m]
	}
	return out
}

// PreferenceIDs is PreferenceList by ID. Diff and the arcs compare
// lists far more often than they read addresses, so they take this.
func (r *Ring) PreferenceIDs(h uint64) []string {
	idx := r.prefIndexes(h, nil)
	out := make([]string, len(idx))
	for i, m := range idx {
		out[i] = r.members[m].ID
	}
	return out
}

// prefIndexes walks the ring clockwise from h, collecting distinct
// members until it has RF of them or has run out. buf, when it has
// capacity, is reused, which keeps the hot paths in Diff and the
// balance tests from allocating per key.
func (r *Ring) prefIndexes(h uint64, buf []int) []int {
	rf := min(r.view.RF, len(r.members))
	out := buf[:0]

	start := r.tokenIndex(h)
	for i := range r.tokens {
		t := r.tokens[(start+i)%len(r.tokens)]
		if slices.Contains(out, t.member) {
			continue
		}
		out = append(out, t.member)
		if len(out) == rf {
			break
		}
	}
	return out
}

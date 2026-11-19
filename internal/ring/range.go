package ring

import (
	"fmt"
	"math/big"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
)

// Range is a half-open interval of the ring: the hashes h with
// Start < h <= End.
//
// The interval is open at the start and closed at the end because a key
// belongs to the first token at or after its hash, so a key landing
// exactly on a token belongs to that token's arc.
//
// Start >= End means the range wraps past the top of the ring. Start ==
// End is therefore the whole ring, which is what a ring with a single
// token is.
type Range struct {
	Start, End uint64
}

// Contains reports whether h falls in the range.
func (r Range) Contains(h uint64) bool {
	if r.Start < r.End {
		return h > r.Start && h <= r.End
	}
	// Wrapping, or the whole ring when Start == End.
	return h > r.Start || h <= r.End
}

// Wraps reports whether the range crosses the top of the ring.
func (r Range) Wraps() bool { return r.Start >= r.End }

// Size returns how many of the 2^64 hash positions the range holds. It
// is a big.Int because the whole ring does not fit in a uint64.
func (r Range) Size() *big.Int {
	if r.Start == r.End {
		return new(big.Int).Lsh(big.NewInt(1), 64)
	}
	if r.Start < r.End {
		return new(big.Int).SetUint64(r.End - r.Start)
	}
	// Wrapping: (2^64 - Start) + End, which the uint64 subtraction
	// already computes modulo 2^64.
	return new(big.Int).SetUint64(r.End - r.Start)
}

// adjacent reports whether next begins exactly where r ends, so the two
// can be merged into one range without leaving a gap.
func (r Range) adjacent(next Range) bool { return r.End == next.Start }

func (r Range) String() string { return fmt.Sprintf("(%d,%d]", r.Start, r.End) }

// ToProto converts the range to its wire form.
func (r Range) ToProto() *ringcachev1.Range {
	return &ringcachev1.Range{Start: r.Start, End: r.End}
}

// RangeFromProto converts a wire range back.
func RangeFromProto(p *ringcachev1.Range) Range {
	if p == nil {
		return Range{}
	}
	return Range{Start: p.GetStart(), End: p.GetEnd()}
}

// mergeAdjacent collapses entries of a ring-ordered slice that both
// touch and have payloads same calls equal. Arcs and Diff both need it
// over different payloads, which is why it is generic in the entry type
// and takes accessors for the range.
//
// It also merges the last entry into the first, since those two are
// neighbours across the top of the ring. When the input tiles the ring,
// as Arcs' does, the result still tiles it, beginning with the entry
// that straddles the top.
//
// The input need not tile the ring: Diff passes only the stretches that
// differ, so two neighbours in the slice can have a gap between them.
// That is why touching is checked and not assumed — merging across a
// gap would claim a stretch whose ownership did not change. No ring
// shape was found that produces two such neighbours with equal
// payloads, so the check is defensive; TestMergeAdjacent covers it
// directly.
func mergeAdjacent[T any](items []T, rangeOf func(T) Range, setRange func(*T, Range), same func(a, b T) bool) []T {
	if len(items) == 0 {
		return nil
	}

	out := make([]T, 0, len(items))
	for _, item := range items {
		if n := len(out); n > 0 {
			last := out[n-1]
			if same(last, item) && rangeOf(last).adjacent(rangeOf(item)) {
				setRange(&out[n-1], Range{Start: rangeOf(last).Start, End: rangeOf(item).End})
				continue
			}
		}
		out = append(out, item)
	}

	// A single entry already covers the ring; merging it with itself
	// would lose that.
	if n := len(out); n > 1 {
		first, last := out[0], out[n-1]
		if same(last, first) && rangeOf(last).adjacent(rangeOf(first)) {
			setRange(&out[0], Range{Start: rangeOf(last).Start, End: rangeOf(first).End})
			out = out[:n-1]
		}
	}
	return out
}

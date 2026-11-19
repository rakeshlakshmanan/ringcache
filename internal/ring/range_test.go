package ring

import (
	"math/big"
	"testing"
)

func TestRange_Contains(t *testing.T) {
	const top = ^uint64(0)

	tests := []struct {
		name string
		r    Range
		in   []uint64
		out  []uint64
	}{
		{
			// Open at the start, closed at the end: a key landing exactly
			// on a token belongs to that token.
			name: "normal",
			r:    Range{Start: 10, End: 20},
			in:   []uint64{11, 15, 20},
			out:  []uint64{0, 9, 10, 21, top},
		},
		{
			name: "normal at the bottom of the ring",
			r:    Range{Start: 0, End: 5},
			in:   []uint64{1, 5},
			out:  []uint64{0, 6, top},
		},
		{
			name: "normal at the top of the ring",
			r:    Range{Start: top - 2, End: top},
			in:   []uint64{top - 1, top},
			out:  []uint64{0, 1, top - 2},
		},
		{
			// Wrapping past the top of the ring.
			name: "wrapping",
			r:    Range{Start: top - 2, End: 10},
			in:   []uint64{top - 1, top, 0, 1, 10},
			out:  []uint64{top - 2, 11, 500},
		},
		{
			name: "wrapping with an end of zero",
			r:    Range{Start: 100, End: 0},
			in:   []uint64{101, top, 0},
			out:  []uint64{1, 99, 100},
		},
		{
			// A ring with a single token: its arc is the whole ring.
			name: "whole ring",
			r:    Range{Start: 42, End: 42},
			in:   []uint64{0, 41, 42, 43, top},
			out:  nil,
		},
		{
			name: "whole ring at zero",
			r:    Range{Start: 0, End: 0},
			in:   []uint64{0, 1, top},
			out:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, h := range tt.in {
				if !tt.r.Contains(h) {
					t.Errorf("%s.Contains(%d) = false, want true", tt.r, h)
				}
			}
			for _, h := range tt.out {
				if tt.r.Contains(h) {
					t.Errorf("%s.Contains(%d) = true, want false", tt.r, h)
				}
			}
		})
	}
}

func TestRange_Size(t *testing.T) {
	const top = ^uint64(0)
	full := new(big.Int).Lsh(big.NewInt(1), 64)

	tests := []struct {
		r    Range
		want *big.Int
	}{
		{Range{Start: 10, End: 20}, big.NewInt(10)},
		{Range{Start: 0, End: 1}, big.NewInt(1)},
		// Wrapping: the three positions above Start, plus the ten at or
		// below End.
		{Range{Start: top - 2, End: 10}, big.NewInt(13)},
		{Range{Start: 42, End: 42}, full},
		{Range{Start: 0, End: 0}, full},
	}

	for _, tt := range tests {
		if got := tt.r.Size(); got.Cmp(tt.want) != 0 {
			t.Errorf("%s.Size() = %s, want %s", tt.r, got, tt.want)
		}
	}
}

func TestRange_Proto(t *testing.T) {
	r := Range{Start: 7, End: 9}
	if got := RangeFromProto(r.ToProto()); got != r {
		t.Errorf("round trip gave %s, want %s", got, r)
	}
	if got := RangeFromProto(nil); got != (Range{}) {
		t.Errorf("RangeFromProto(nil) = %s, want the zero range", got)
	}
}

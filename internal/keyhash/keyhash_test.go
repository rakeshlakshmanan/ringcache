package keyhash

import "testing"

// Golden values pin the hash function. Nodes and clients must compute
// identical rings from the same member list, so changing the hash is a
// wire-visible change and has to be deliberate.
func TestKeyhash_Golden(t *testing.T) {
	tests := []struct {
		in   string
		want uint64
	}{
		// The empty-string value is xxhash64's canonical published
		// vector for seed 0, which pins this to standard xxhash64; the
		// other two are this implementation's output for a ring token
		// and a workload key.
		{"", 0xef46db3751d8e999},
		{"node-1#0", 0x872942a1b8224862},
		{"key-0", 0x12daf06715ffa373},
	}
	for _, tt := range tests {
		if got := Sum(tt.in); got != tt.want {
			t.Errorf("Sum(%q) = %#016x, want %#016x", tt.in, got, tt.want)
		}
	}
}

func TestKeyhash_Deterministic(t *testing.T) {
	const s = "node-3#255"
	first := Sum(s)
	for i := 0; i < 100; i++ {
		if got := Sum(s); got != first {
			t.Fatalf("Sum(%q) changed between calls: %#016x then %#016x", s, first, got)
		}
	}
}

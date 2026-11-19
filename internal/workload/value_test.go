package workload_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rakeshlakshmanan/ringcache/internal/workload"
)

func TestValue_RoundTrip(t *testing.T) {
	sizes := []int{16, 17, 100, 1000, 1 << 16}
	for _, size := range sizes {
		for _, index := range []uint64{0, 1, 999_999} {
			for _, seq := range []uint64{0, 1, 1 << 40} {
				v := workload.Encode(index, seq, size)
				if len(v) != size {
					t.Fatalf("Encode(%d, %d, %d) gave %d bytes", index, seq, size, len(v))
				}

				gotIndex, gotSeq, err := workload.Decode(v)
				if err != nil {
					t.Fatalf("Decode: %v", err)
				}
				if gotIndex != index || gotSeq != seq {
					t.Fatalf("Decode gave (%d, %d), want (%d, %d)", gotIndex, gotSeq, index, seq)
				}
				if err := workload.VerifyPadding(v); err != nil {
					t.Fatalf("VerifyPadding: %v", err)
				}
			}
		}
	}
}

// Encoding is a pure function of the header, so the same value is the
// same bytes everywhere. A run on one machine can be checked on
// another, and a difference means the data changed, not the generator.
func TestValue_Deterministic(t *testing.T) {
	a := workload.Encode(42, 7, 256)
	b := workload.Encode(42, 7, 256)
	if !bytes.Equal(a, b) {
		t.Fatal("encoding the same index and sequence twice gave different bytes")
	}

	// A different sequence must give different padding, or a corrupted
	// value could pass as a valid older one.
	c := workload.Encode(42, 8, 256)
	if bytes.Equal(a[16:], c[16:]) {
		t.Error("two sequences of one key share their padding")
	}
}

// A value carrying the wrong key index means data landed under the
// wrong key. That is corruption, not staleness, so it must not be
// mistaken for an old value.
func TestValue_WrongIndex(t *testing.T) {
	v := workload.Encode(7, 3, 64)

	seq, err := workload.DecodeExpect(v, 7)
	if err != nil {
		t.Fatalf("DecodeExpect with the right index: %v", err)
	}
	if seq != 3 {
		t.Errorf("sequence = %d, want 3", seq)
	}

	if _, err := workload.DecodeExpect(v, 8); err == nil {
		t.Fatal("DecodeExpect accepted a value belonging to another key")
	} else if !strings.Contains(err.Error(), "key index 7") {
		t.Errorf("error %v does not name the index it found", err)
	}
}

func TestValue_TooShort(t *testing.T) {
	for _, b := range [][]byte{nil, {}, make([]byte, 15)} {
		if _, _, err := workload.Decode(b); err == nil {
			t.Errorf("Decode accepted %d bytes", len(b))
		}
	}

	// A size below the header is raised to it rather than producing a
	// value that cannot be decoded.
	if got := len(workload.Encode(1, 1, 4)); got != workload.MinValueSize {
		t.Errorf("Encode with size 4 gave %d bytes, want %d", got, workload.MinValueSize)
	}
}

// A value whose padding was partly overwritten still has a valid
// header, so only the padding check catches it.
func TestValue_CorruptPadding(t *testing.T) {
	v := workload.Encode(5, 9, 64)
	if err := workload.VerifyPadding(v); err != nil {
		t.Fatalf("VerifyPadding on an untouched value: %v", err)
	}

	v[40] ^= 0xff
	if err := workload.VerifyPadding(v); err == nil {
		t.Error("VerifyPadding accepted a value with a flipped padding byte")
	}
}

func TestKeys(t *testing.T) {
	tests := []struct {
		i    int
		want string
	}{
		{0, "k-00000000"},
		{1, "k-00000001"},
		{999_999, "k-00999999"},
		{99_999_999, "k-99999999"},
		// Past the fixed width the key simply gets longer rather than
		// wrapping onto another key's name.
		{100_000_000, "k-100000000"},
	}
	for _, tt := range tests {
		if got := workload.Key(tt.i); got != tt.want {
			t.Errorf("Key(%d) = %q, want %q", tt.i, got, tt.want)
		}
	}

	// Keys must be distinct, or two workers would write to one key and
	// the checker could not order them.
	seen := make(map[string]int, 10_000)
	for i := range 10_000 {
		k := workload.Key(i)
		if prev, dup := seen[k]; dup {
			t.Fatalf("Key(%d) and Key(%d) are both %q", prev, i, k)
		}
		seen[k] = i
	}
}

// Every key must belong to exactly one worker, and the partitions must
// together cover the whole key space: that is what makes operations on
// a key ordered.
func TestKeys_Partitioning(t *testing.T) {
	for _, workers := range []int{1, 3, 7, 64} {
		for _, total := range []int{1, 10, 1000, 1001} {
			if workers > total {
				continue
			}

			owned := make([]int, total)
			sum := 0
			for w := range workers {
				n := workload.KeysOf(w, workers, total)
				sum += n
				for j := range n {
					i := workload.KeyOfWorker(w, workers, j)
					if i >= total {
						t.Fatalf("workers=%d total=%d: worker %d claims key %d", workers, total, w, i)
					}
					owned[i]++
					if got := workload.WorkerFor(i, workers); got != w {
						t.Fatalf("key %d is owned by worker %d but WorkerFor says %d", i, w, got)
					}
				}
			}

			if sum != total {
				t.Fatalf("workers=%d total=%d: the partitions hold %d keys", workers, total, sum)
			}
			for i, n := range owned {
				if n != 1 {
					t.Fatalf("workers=%d total=%d: key %d is owned by %d workers", workers, total, i, n)
				}
			}
		}
	}
}

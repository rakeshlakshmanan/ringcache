package workload

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
)

// headerSize is the fixed prefix of every value: the key index and the
// sequence number, each a big-endian uint64.
//
// The index travels inside the value so a read can tell that the bytes
// it got belong to the key it asked for. Without it, a bug that filed a
// value under the wrong key would look like an ordinary stale read.
const headerSize = 16

// MinValueSize is the smallest value the encoding can produce.
const MinValueSize = headerSize

// Encode builds the value for sequence seq of key index, padded to
// size bytes.
//
// The padding comes from a PRNG seeded with the index and the
// sequence, so the whole value is a pure function of the two: a
// corrupted byte anywhere in it is detectable, not just in the header.
func Encode(index, seq uint64, size int) []byte {
	if size < headerSize {
		size = headerSize
	}

	b := make([]byte, size)
	binary.BigEndian.PutUint64(b[0:8], index)
	binary.BigEndian.PutUint64(b[8:16], seq)

	fillPadding(b[headerSize:], index, seq)
	return b
}

// fillPadding writes the deterministic padding for one value.
func fillPadding(dst []byte, index, seq uint64) {
	// A PCG seeded from the two header fields: same value, same bytes,
	// on any machine and in any run.
	rng := rand.New(rand.NewPCG(index, seq))
	for i := 0; i < len(dst); i += 8 {
		var word [8]byte
		binary.LittleEndian.PutUint64(word[:], rng.Uint64())
		copy(dst[i:], word[:])
	}
}

// Decode reads a value's header.
func Decode(b []byte) (index, seq uint64, err error) {
	if len(b) < headerSize {
		return 0, 0, fmt.Errorf("workload: value is %d bytes, want at least %d", len(b), headerSize)
	}
	return binary.BigEndian.Uint64(b[0:8]), binary.BigEndian.Uint64(b[8:16]), nil
}

// DecodeExpect reads a value that should belong to key wantIndex, and
// rejects one that does not. A mismatch means data landed under the
// wrong key, which is corruption rather than staleness.
func DecodeExpect(b []byte, wantIndex uint64) (seq uint64, err error) {
	index, seq, err := Decode(b)
	if err != nil {
		return 0, err
	}
	if index != wantIndex {
		return 0, fmt.Errorf("workload: value carries key index %d, want %d", index, wantIndex)
	}
	return seq, nil
}

// VerifyPadding reports whether a value's padding is the padding its
// header calls for. It is the check that a value was not truncated or
// partly overwritten, which neither the header nor a length check
// would catch.
func VerifyPadding(b []byte) error {
	index, seq, err := Decode(b)
	if err != nil {
		return err
	}
	want := Encode(index, seq, len(b))
	for i := headerSize; i < len(b); i++ {
		if b[i] != want[i] {
			return fmt.Errorf("workload: value for key %d sequence %d differs at byte %d", index, seq, i)
		}
	}
	return nil
}

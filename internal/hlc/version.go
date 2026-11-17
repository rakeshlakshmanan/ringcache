// Package hlc provides a hybrid logical clock: versions that follow
// wall-clock time but never go backwards, and that always move past any
// version the node has seen from a peer.
//
// Every entry in the store carries a Version, and every path that stores
// an entry keeps the newest one, so duplicate and reordered writes do no
// harm.
package hlc

import "strconv"

// Version stamps a write. Within one epoch a key has a single primary,
// so its versions all come from one node; versions from different nodes
// meet only during failover and migration, where newest-wins settles
// them.
type Version struct {
	// WallMs is the physical part, in Unix milliseconds.
	WallMs int64
	// Logical counts events inside one millisecond, and keeps versions
	// increasing when the wall clock stands still or jumps back.
	Logical uint32
	// NodeID is the low 16 bits of keyhash.Sum(nodeID). It only breaks
	// ties between versions with equal wall time and counter.
	NodeID uint16
}

// Compare orders a before b by wall time, then by logical counter, then
// by node ID. It returns -1 when a is older, +1 when a is newer and 0
// when the two are identical.
func Compare(a, b Version) int {
	switch {
	case a.WallMs != b.WallMs:
		if a.WallMs < b.WallMs {
			return -1
		}
		return 1
	case a.Logical != b.Logical:
		if a.Logical < b.Logical {
			return -1
		}
		return 1
	case a.NodeID != b.NodeID:
		if a.NodeID < b.NodeID {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// IsZero reports whether v is the zero Version, which stands for "no
// version yet": no stored entry, or a field left unset on the wire.
func (v Version) IsZero() bool { return v == Version{} }

// String formats v as wall.logical@node.
func (v Version) String() string {
	return strconv.FormatInt(v.WallMs, 10) + "." +
		strconv.FormatUint(uint64(v.Logical), 10) + "@" +
		strconv.FormatUint(uint64(v.NodeID), 10)
}

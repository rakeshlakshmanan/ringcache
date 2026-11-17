// Package store holds the in-memory cache each node serves from:
// versioned entries, absolute expiry, tombstones for deletes,
// newest-wins applies, an expiry sweep and range scans for migration.
package store

import "github.com/rakeshlakshmanan/ringcache/internal/hlc"

// entryOverhead is the per-entry bookkeeping charged on top of the key
// and value lengths: map bucket space, the Entry struct and the slice
// header. It only has to be consistent, since the byte totals drive
// metrics and the eviction stretch goal rather than any guarantee.
const entryOverhead = 64

// Entry is one stored value, or a tombstone standing in for a delete.
//
// An Entry is immutable once stored: a write replaces the whole struct
// rather than editing it, so a copy handed to a caller stays valid even
// after the key is overwritten. Callers must not modify Value.
type Entry struct {
	// Value is the stored bytes, empty for a tombstone.
	Value []byte
	// ExpiresAt is the absolute expiry in Unix milliseconds, 0 for no
	// expiry. The primary stamps it when it accepts the write, and every
	// copy keeps that same absolute value, so all copies of a key expire
	// together however they were made (FR-10).
	ExpiresAt int64
	// Version orders this entry against any other version of the key.
	Version hlc.Version
	// Tombstone marks a delete. It's kept for TombstoneLifetime so a
	// late replication message or migration copy can't resurrect the key.
	Tombstone bool
	// Hash is keyhash.Sum of the key, computed once when the entry is
	// stored. It picks the shard, and it lets a migration scan filter by
	// ring range without hashing every key again. It is local
	// bookkeeping and never travels on the wire.
	Hash uint64
}

// Size returns the bytes this entry is charged for under the given key.
func (e Entry) Size(key string) int64 {
	return int64(len(key) + len(e.Value) + entryOverhead)
}

// Expired reports whether e has an expiry that has passed at nowMs. An
// entry exactly at its expiry counts as expired.
func (e Entry) Expired(nowMs int64) bool {
	return e.ExpiresAt != 0 && e.ExpiresAt <= nowMs
}

// KeyedEntry pairs a key with its entry, as range scans and migration
// batches carry them.
type KeyedEntry struct {
	Key   string
	Entry Entry
}

// Size returns the bytes this keyed entry is charged for.
func (ke KeyedEntry) Size() int64 { return ke.Entry.Size(ke.Key) }

package store

import "sync"

// shard is one lock-striped slice of the keyspace. Every store method
// holds at most one shard's lock at a time, so unrelated keys don't
// contend.
type shard struct {
	mu sync.RWMutex
	// items holds every entry in this shard, tombstones included.
	items map[string]Entry
	// expiring indexes the keys in items that have an expiry, so the
	// sweep samples only keys that can expire instead of the whole
	// shard. Tombstones are in here too, since they expire as well.
	expiring map[string]struct{}
	// bytes is the summed Size of this shard's entries.
	bytes int64
}

func newShard() shard {
	return shard{
		items:    make(map[string]Entry),
		expiring: make(map[string]struct{}),
	}
}

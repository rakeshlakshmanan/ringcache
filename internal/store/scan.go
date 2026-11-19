package store

import (
	"context"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
)

// Scan walks the entries whose ring hash satisfies pred and hands them
// to fn in batches, capped at maxEntries and maxBytes. A batch always
// holds at least one entry, even one larger than maxBytes on its own.
// Passing either cap as zero or less uses the streaming defaults.
//
// The predicate takes the hash rather than a ring range, so the store
// doesn't depend on the ring package.
//
// Locks are held only while the keys of one shard are listed and while
// one batch is copied, never while fn runs, which keeps client latency
// flat during migration. Entries that change mid-scan are fine: the
// destination applies newest-wins, and forwarded writes cover the arc
// for the rest of the move.
//
// Scan stops and returns the error from ctx or from fn.
func (s *Store) Scan(
	ctx context.Context,
	pred func(hash uint64) bool,
	maxEntries, maxBytes int,
	fn func([]KeyedEntry) error,
) error {
	if maxEntries <= 0 {
		maxEntries = config.StreamBatchEntries
	}
	if maxBytes <= 0 {
		maxBytes = config.StreamBatchBytes
	}

	for i := range s.shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		sh := &s.shards[i]

		keys := s.matchingKeys(sh, pred)
		for len(keys) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}

			var batch []KeyedEntry
			batch, keys = s.collectBatch(sh, keys, maxEntries, maxBytes)
			if len(batch) == 0 {
				// Every key in that run had vanished since it was
				// listed; collectBatch still consumed them, so this
				// makes progress.
				continue
			}
			if err := fn(batch); err != nil {
				return err
			}
		}
	}
	return nil
}

// matchingKeys lists the keys of one shard whose hash satisfies pred,
// holding the read lock only for the listing and copying no values.
func (s *Store) matchingKeys(sh *shard, pred func(uint64) bool) []string {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	var keys []string
	for key, e := range sh.items {
		if pred(e.Hash) {
			keys = append(keys, key)
		}
	}
	return keys
}

// collectBatch copies one batch of entries for the listed keys and
// returns it with the keys it did not consume. It always consumes at
// least one key when given any, so a scan can't stall.
func (s *Store) collectBatch(sh *shard, keys []string, maxEntries, maxBytes int) ([]KeyedEntry, []string) {
	nowMs := s.nowMs()
	batch := make([]KeyedEntry, 0, min(len(keys), maxEntries))
	size := 0

	sh.mu.RLock()
	defer sh.mu.RUnlock()

	i := 0
	for i < len(keys) {
		key := keys[i]

		e, ok := sh.items[key]
		// Skip keys that vanished or expired since they were listed.
		// Unexpired tombstones are kept: the destination needs them, or
		// a delete would be undone by the snapshot it raced.
		if !ok || e.Expired(nowMs) {
			i++
			continue
		}

		entrySize := int(e.Size(key))
		if len(batch) > 0 && size+entrySize > maxBytes {
			// This entry starts the next batch; leave it unconsumed.
			break
		}

		batch = append(batch, KeyedEntry{Key: key, Entry: e})
		size += entrySize
		i++

		if len(batch) >= maxEntries {
			break
		}
	}
	return batch, keys[i:]
}

// DeleteMatching removes every entry whose ring hash satisfies pred and
// returns how many it removed. A node uses it to drop data for arcs it
// no longer owns, once the grace period has passed.
func (s *Store) DeleteMatching(pred func(hash uint64) bool) int {
	removed := 0
	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.Lock()
		for key, e := range sh.items {
			if pred(e.Hash) {
				s.removeLocked(sh, key, e)
				removed++
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

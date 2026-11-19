package store

import (
	"bytes"
	"sync/atomic"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/hlc"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
)

// Store is one node's in-memory cache, split into config.StoreShards
// lock-striped shards.
//
// Every method is safe for concurrent use. Entries handed back by Get
// and Scan share their Value with the store and must not be modified.
type Store struct {
	shards []shard

	hlc *hlc.Clock
	c   clock.Clock

	keys  atomic.Int64
	bytes atomic.Int64
}

// New returns an empty Store stamping writes with h and reading time
// from c.
func New(h *hlc.Clock, c clock.Clock) *Store {
	// The shard is picked with a mask, so the count must be a power of
	// two. config.StoreShards is checked here rather than trusted.
	if config.StoreShards <= 0 || config.StoreShards&(config.StoreShards-1) != 0 {
		panic("store: config.StoreShards must be a power of two")
	}
	s := &Store{
		shards: make([]shard, config.StoreShards),
		hlc:    h,
		c:      c,
	}
	for i := range s.shards {
		s.shards[i] = newShard()
	}
	return s
}

func (s *Store) shardFor(hash uint64) *shard {
	return &s.shards[hash&uint64(config.StoreShards-1)]
}

func (s *Store) nowMs() int64 { return s.c.Now().UnixMilli() }

// Get returns the live entry for key. A missing key, a tombstone or an
// entry whose expiry has passed all report false.
//
// An expired entry is left in place for the sweep or the next write to
// remove, so a read never takes a write lock.
func (s *Store) Get(key string) (Entry, bool) {
	sh := s.shardFor(keyhash.Sum(key))
	nowMs := s.nowMs()

	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.items[key]
	if !ok || e.Tombstone || e.Expired(nowMs) {
		return Entry{}, false
	}
	return e, true
}

// TTL reports the time left on key. found is false when Get would miss
// the key; hasTTL is false when the key is stored without an expiry.
func (s *Store) TTL(key string) (remaining time.Duration, hasTTL, found bool) {
	e, ok := s.Get(key)
	if !ok {
		return 0, false, false
	}
	if e.ExpiresAt == 0 {
		return 0, false, true
	}
	left := time.Duration(e.ExpiresAt-s.nowMs()) * time.Millisecond
	return max(left, 0), true, true
}

// SetLocal stores value under key and returns the stored entry, which
// the caller replicates. This is the primary's write path.
//
// A ttl above zero is turned into an absolute expiry straight away, so
// every copy of the key expires at the same instant (FR-10).
func (s *Store) SetLocal(key string, value []byte, ttl time.Duration) Entry {
	h := keyhash.Sum(key)
	e := Entry{
		// gRPC may reuse the request buffer, so the value is copied.
		Value: bytes.Clone(value),
		Hash:  h,
	}
	if ttl > 0 {
		e.ExpiresAt = s.c.Now().Add(ttl).UnixMilli()
	}
	return s.writeLocal(key, h, e)
}

// DeleteLocal replaces key with a tombstone and returns it for
// replication. The tombstone outlives any in-flight write for the key,
// so a late replication message or migration copy can't bring it back.
func (s *Store) DeleteLocal(key string) Entry {
	h := keyhash.Sum(key)
	e := Entry{
		Hash:      h,
		Tombstone: true,
		ExpiresAt: s.c.Now().Add(config.TombstoneLifetime).UnixMilli(),
	}
	return s.writeLocal(key, h, e)
}

// writeLocal stamps e with a version that beats whatever is stored for
// key and puts it in place.
func (s *Store) writeLocal(key string, h uint64, e Entry) Entry {
	sh := s.shardFor(h)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	old, had := sh.items[key]
	if had {
		// Beat the stored version even if a peer's clock ran ahead of
		// this node's, so a local write always wins over what it
		// replaces.
		e.Version = s.hlc.NowAfter(old.Version)
	} else {
		e.Version = s.hlc.Now()
	}
	s.putLocked(sh, key, e, old, had)
	return e
}

// Apply stores an entry that came from a peer: a replicated write, a
// forwarded write or a migration copy. It reports whether the entry was
// stored.
//
// It drops the entry when its expiry has already passed, or when the
// stored version is at least as new. That makes every one of those paths
// idempotent and order-independent, which is what lets replication
// reconnect, migration restart from the beginning and forwarded writes
// race the snapshot they overlap.
func (s *Store) Apply(key string, e Entry) bool {
	if e.Expired(s.nowMs()) {
		return false
	}

	// Hash is local bookkeeping and never travels on the wire, so it's
	// computed here rather than trusted from the sender.
	h := keyhash.Sum(key)
	e.Hash = h
	e.Value = bytes.Clone(e.Value)

	sh := s.shardFor(h)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	old, had := sh.items[key]
	if had && hlc.Compare(old.Version, e.Version) >= 0 {
		return false
	}
	s.putLocked(sh, key, e, old, had)

	// Move this node's clock past the sender's, so a later local write
	// to the key outranks what was just applied.
	s.hlc.Update(e.Version)
	return true
}

// putLocked stores e under key and keeps the expiry index and the byte
// and key totals in step. The caller holds sh.mu for writing.
func (s *Store) putLocked(sh *shard, key string, e, old Entry, had bool) {
	delta := e.Size(key)
	if had {
		delta -= old.Size(key)
	} else {
		s.keys.Add(1)
	}

	sh.items[key] = e
	sh.bytes += delta
	s.bytes.Add(delta)

	if e.ExpiresAt != 0 {
		sh.expiring[key] = struct{}{}
	} else {
		delete(sh.expiring, key)
	}
}

// removeLocked drops key from the shard and keeps the totals in step.
// The caller holds sh.mu for writing and has read e from the shard.
func (s *Store) removeLocked(sh *shard, key string, e Entry) {
	delete(sh.items, key)
	delete(sh.expiring, key)

	size := e.Size(key)
	sh.bytes -= size
	s.bytes.Add(-size)
	s.keys.Add(-1)
}

// Len returns the number of stored entries, counting unexpired
// tombstones and entries whose expiry has passed but which the sweep
// hasn't reached yet.
func (s *Store) Len() int64 { return s.keys.Load() }

// Bytes returns the summed Size of the stored entries.
func (s *Store) Bytes() int64 { return s.bytes.Load() }

// Wipe empties the store. A node wipes before it registers, so a
// restarted or rejoining member never serves data from an earlier
// incarnation (FR-12).
func (s *Store) Wipe() {
	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.Lock()
		keys := int64(len(sh.items))
		bytes := sh.bytes
		sh.items = make(map[string]Entry)
		sh.expiring = make(map[string]struct{})
		sh.bytes = 0
		sh.mu.Unlock()

		// Subtract what this shard actually held rather than zeroing the
		// totals, so a write landing in an already-wiped shard can't
		// leave them wrong.
		s.keys.Add(-keys)
		s.bytes.Add(-bytes)
	}
}

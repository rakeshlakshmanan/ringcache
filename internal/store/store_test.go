package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/hlc"
)

func newTestStore(t *testing.T) (*Store, *clock.Fake) {
	t.Helper()
	f := clock.NewFake()
	return New(hlc.New("node-1", f), f), f
}

// assertConsistent walks every shard and checks the bookkeeping the
// store keeps outside its maps: the key and byte totals, each shard's
// byte count, and the expiry index. A write path that forgets one of
// them is otherwise invisible until a metric or the sweep goes wrong.
func assertConsistent(t *testing.T, s *Store) {
	t.Helper()

	var walkedKeys, walkedBytes int64
	for i := range s.shards {
		sh := &s.shards[i]

		sh.mu.RLock()
		var shardBytes int64
		for key, e := range sh.items {
			shardBytes += e.Size(key)
			if e.ExpiresAt != 0 {
				if _, indexed := sh.expiring[key]; !indexed {
					t.Errorf("shard %d: key %q has an expiry but is not in the expiring index", i, key)
				}
			}
		}
		for key := range sh.expiring {
			if e, ok := sh.items[key]; ok && e.ExpiresAt == 0 {
				t.Errorf("shard %d: key %q is in the expiring index without an expiry", i, key)
			}
		}
		if shardBytes != sh.bytes {
			t.Errorf("shard %d: bytes field = %d, walked %d", i, sh.bytes, shardBytes)
		}
		walkedKeys += int64(len(sh.items))
		walkedBytes += shardBytes
		sh.mu.RUnlock()
	}

	if got := s.Len(); got != walkedKeys {
		t.Errorf("Len() = %d, walked %d entries", got, walkedKeys)
	}
	if got := s.Bytes(); got != walkedBytes {
		t.Errorf("Bytes() = %d, walked %d bytes", got, walkedBytes)
	}
}

func TestStore_SetGet(t *testing.T) {
	s, _ := newTestStore(t)

	stored := s.SetLocal("hello", []byte("world"), 0)
	if stored.Version.IsZero() {
		t.Error("SetLocal returned an entry with the zero version")
	}
	if stored.ExpiresAt != 0 {
		t.Errorf("ExpiresAt = %d for a write with no TTL, want 0", stored.ExpiresAt)
	}

	got, ok := s.Get("hello")
	if !ok {
		t.Fatal("Get missed the key just written")
	}
	if string(got.Value) != "world" {
		t.Errorf("Value = %q, want %q", got.Value, "world")
	}
	if hlc.Compare(got.Version, stored.Version) != 0 {
		t.Errorf("stored version %v, read back %v", stored.Version, got.Version)
	}

	if _, ok := s.Get("missing"); ok {
		t.Error("Get reported an unknown key as found")
	}

	// An overwrite must carry a newer version.
	second := s.SetLocal("hello", []byte("again"), 0)
	if hlc.Compare(second.Version, stored.Version) <= 0 {
		t.Errorf("overwrite version %v is not newer than %v", second.Version, stored.Version)
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d after overwriting one key, want 1", s.Len())
	}

	assertConsistent(t, s)
}

// gRPC may reuse the buffer a request arrived in, so the store has to
// copy values rather than alias them.
func TestStore_SetLocalCopiesValue(t *testing.T) {
	s, _ := newTestStore(t)

	value := []byte("original")
	s.SetLocal("k", value, 0)
	copy(value, "mutated!")

	got, ok := s.Get("k")
	if !ok {
		t.Fatal("Get missed the key")
	}
	if string(got.Value) != "original" {
		t.Errorf("Value = %q after the caller's slice was overwritten, want %q", got.Value, "original")
	}
}

func TestStore_TTLExpiry(t *testing.T) {
	s, f := newTestStore(t)

	s.SetLocal("k", []byte("v"), 2*time.Second)

	remaining, hasTTL, found := s.TTL("k")
	if !found || !hasTTL {
		t.Fatalf("TTL right after the write: found = %v, hasTTL = %v, want both true", found, hasTTL)
	}
	if remaining != 2*time.Second {
		t.Errorf("remaining = %v, want 2s", remaining)
	}

	f.Advance(1900 * time.Millisecond)
	if _, ok := s.Get("k"); !ok {
		t.Error("the key is gone at 1.9s, before its 2s expiry")
	}
	if remaining, _, _ := s.TTL("k"); remaining != 100*time.Millisecond {
		t.Errorf("remaining at 1.9s = %v, want 100ms", remaining)
	}

	// An entry exactly at its expiry counts as expired.
	f.Advance(100 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Error("the key is still readable at its 2s expiry")
	}
	if _, _, found := s.TTL("k"); found {
		t.Error("TTL reports an expired key as found")
	}

	// A key stored without a TTL.
	s.SetLocal("forever", []byte("v"), 0)
	remaining, hasTTL, found = s.TTL("forever")
	if !found || hasTTL || remaining != 0 {
		t.Errorf("TTL of a key with no expiry: remaining = %v, hasTTL = %v, found = %v; want 0, false, true", remaining, hasTTL, found)
	}

	if _, _, found := s.TTL("missing"); found {
		t.Error("TTL reports an unknown key as found")
	}
}

func TestStore_ApplyNewestWins(t *testing.T) {
	s, f := newTestStore(t)
	nowMs := f.Now().UnixMilli()

	remote := func(logical uint32, value string) Entry {
		return Entry{
			Value:   []byte(value),
			Version: hlc.Version{WallMs: nowMs, Logical: logical, NodeID: 42},
		}
	}

	if !s.Apply("k", remote(5, "v5")) {
		t.Fatal("the first Apply was rejected")
	}

	if s.Apply("k", remote(4, "v4")) {
		t.Error("Apply stored an older version")
	}
	if s.Apply("k", remote(5, "v5-duplicate")) {
		t.Error("Apply stored a version equal to the stored one")
	}
	if got, _ := s.Get("k"); string(got.Value) != "v5" {
		t.Errorf("Value = %q after the rejected applies, want %q", got.Value, "v5")
	}

	if !s.Apply("k", remote(6, "v6")) {
		t.Error("Apply rejected a newer version")
	}
	if got, _ := s.Get("k"); string(got.Value) != "v6" {
		t.Errorf("Value = %q after applying a newer version, want %q", got.Value, "v6")
	}

	// A shuffled sequence must settle on the highest version, whatever
	// order it arrives in: this is what lets replication reconnect and
	// migration restart.
	for _, logical := range []uint32{3, 7, 1, 9, 4, 2, 8} {
		s.Apply("shuffled", remote(logical, fmt.Sprintf("v%d", logical)))
	}
	if got, _ := s.Get("shuffled"); string(got.Value) != "v9" {
		t.Errorf("Value = %q after a shuffled sequence, want %q", got.Value, "v9")
	}

	assertConsistent(t, s)
}

func TestStore_ApplyExpiredDropped(t *testing.T) {
	s, f := newTestStore(t)
	nowMs := f.Now().UnixMilli()

	version := hlc.Version{WallMs: nowMs, Logical: 1, NodeID: 42}

	past := Entry{Value: []byte("v"), ExpiresAt: nowMs - 1, Version: version}
	if s.Apply("past", past) {
		t.Error("Apply stored an entry whose expiry had passed")
	}

	// Exactly at the expiry counts as expired.
	atExpiry := Entry{Value: []byte("v"), ExpiresAt: nowMs, Version: version}
	if s.Apply("at-expiry", atExpiry) {
		t.Error("Apply stored an entry exactly at its expiry")
	}

	// One millisecond of life left is still alive.
	future := Entry{Value: []byte("v"), ExpiresAt: nowMs + 1, Version: version}
	if !s.Apply("future", future) {
		t.Error("Apply dropped an entry that had not expired yet")
	}

	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1 (only the unexpired entry)", s.Len())
	}
	assertConsistent(t, s)
}

func TestStore_TombstoneBlocksOlderWrite(t *testing.T) {
	s, f := newTestStore(t)

	first := s.SetLocal("k", []byte("v1"), 0)
	tombstone := s.DeleteLocal("k")

	if !tombstone.Tombstone {
		t.Error("DeleteLocal returned an entry that is not a tombstone")
	}
	if len(tombstone.Value) != 0 {
		t.Errorf("tombstone carries a %d byte value, want none", len(tombstone.Value))
	}
	if hlc.Compare(tombstone.Version, first.Version) <= 0 {
		t.Errorf("tombstone version %v does not beat the write it replaced, %v", tombstone.Version, first.Version)
	}
	if _, ok := s.Get("k"); ok {
		t.Error("the key is readable after DeleteLocal")
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d, want 1 (the tombstone is still stored)", s.Len())
	}

	// Replaying the write the delete replaced must not bring the key
	// back, whether it arrives from replication or a migration copy.
	if s.Apply("k", first) {
		t.Error("Apply accepted a write older than the tombstone")
	}
	if _, ok := s.Get("k"); ok {
		t.Error("an older write resurrected a deleted key")
	}

	// The tombstone is collected once its lifetime has passed.
	f.Advance(config.TombstoneLifetime + time.Second)
	s.SweepOnce()
	if s.Len() != 0 {
		t.Errorf("Len() = %d after the tombstone lifetime and a sweep, want 0", s.Len())
	}
	if s.Bytes() != 0 {
		t.Errorf("Bytes() = %d once the store is empty, want 0", s.Bytes())
	}
	assertConsistent(t, s)
}

// A peer whose clock ran ahead can leave a future version stored. A
// local write still has to win, or the primary could not overwrite its
// own key.
func TestStore_LocalWriteBeatsFutureVersion(t *testing.T) {
	s, f := newTestStore(t)

	future := hlc.Version{
		WallMs:  f.Now().Add(10 * time.Second).UnixMilli(),
		Logical: 3,
		NodeID:  42,
	}
	if !s.Apply("k", Entry{Value: []byte("remote"), Version: future}) {
		t.Fatal("Apply rejected the future version")
	}

	local := s.SetLocal("k", []byte("local"), 0)
	if hlc.Compare(local.Version, future) <= 0 {
		t.Errorf("local write version %v does not beat the stored future version %v", local.Version, future)
	}
	if got, _ := s.Get("k"); string(got.Value) != "local" {
		t.Errorf("Value = %q, want %q", got.Value, "local")
	}

	// The same has to hold for a delete.
	tombstone := s.DeleteLocal("k")
	if hlc.Compare(tombstone.Version, local.Version) <= 0 {
		t.Errorf("tombstone version %v does not beat the local write %v", tombstone.Version, local.Version)
	}
}

func TestStore_Wipe(t *testing.T) {
	s, _ := newTestStore(t)

	for i := range 1_000 {
		key := fmt.Sprintf("key-%04d", i)
		s.SetLocal(key, []byte("value"), 0)
		if i%3 == 0 {
			s.SetLocal(key+"-ttl", []byte("value"), time.Minute)
		}
	}
	if s.Len() == 0 || s.Bytes() == 0 {
		t.Fatalf("nothing stored before the wipe: Len() = %d, Bytes() = %d", s.Len(), s.Bytes())
	}

	s.Wipe()

	if got := s.Len(); got != 0 {
		t.Errorf("Len() = %d after Wipe, want 0", got)
	}
	if got := s.Bytes(); got != 0 {
		t.Errorf("Bytes() = %d after Wipe, want 0", got)
	}
	if _, ok := s.Get("key-0000"); ok {
		t.Error("a key survived Wipe")
	}
	assertConsistent(t, s)

	// The store stays usable afterwards, since a fenced node wipes and
	// then rejoins.
	s.SetLocal("fresh", []byte("v"), 0)
	if _, ok := s.Get("fresh"); !ok {
		t.Error("the store does not accept writes after a Wipe")
	}
	assertConsistent(t, s)
}

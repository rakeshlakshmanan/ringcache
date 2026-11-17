package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
)

func matchAll(uint64) bool { return true }

// topBit stands in for a ring range: a cheap predicate on the hash that
// selects roughly half the keyspace.
func topBit(h uint64) bool { return h>>63 == 1 }

func TestStore_ScanMatchesPredicate(t *testing.T) {
	s, _ := newTestStore(t)

	const (
		n          = 10_000
		maxEntries = 100
		maxBytes   = 64 << 10
	)

	want := make(map[string]bool)
	for i := range n {
		key := fmt.Sprintf("key-%05d", i)
		s.SetLocal(key, []byte("value"), 0)
		if topBit(keyhash.Sum(key)) {
			want[key] = true
		}
	}
	if len(want) == 0 || len(want) == n {
		t.Fatalf("the predicate selected %d of %d keys, which tests nothing", len(want), n)
	}

	got := make(map[string]bool)
	batches := 0
	err := s.Scan(context.Background(), topBit, maxEntries, maxBytes, func(batch []KeyedEntry) error {
		batches++

		if len(batch) == 0 {
			t.Error("Scan handed fn an empty batch")
		}
		if len(batch) > maxEntries {
			t.Errorf("batch of %d entries, over the cap of %d", len(batch), maxEntries)
		}

		var size int64
		for _, ke := range batch {
			size += ke.Size()
		}
		if size > maxBytes && len(batch) > 1 {
			t.Errorf("batch of %d entries is %d bytes, over the cap of %d", len(batch), size, maxBytes)
		}

		for _, ke := range batch {
			if got[ke.Key] {
				t.Errorf("Scan returned %q twice", ke.Key)
			}
			got[ke.Key] = true

			if !topBit(ke.Entry.Hash) {
				t.Errorf("Scan returned %q, whose hash does not match the predicate", ke.Key)
			}
			if string(ke.Entry.Value) != "value" {
				t.Errorf("%q came back with value %q", ke.Key, ke.Entry.Value)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan returned %v", err)
	}
	t.Logf("scanned %d of %d keys in %d batches", len(got), n, batches)

	if len(got) != len(want) {
		t.Errorf("Scan returned %d entries, want %d", len(got), len(want))
	}
	for key := range want {
		if !got[key] {
			t.Errorf("Scan missed matching key %q", key)
		}
	}
}

func TestStore_ScanBatchBytes(t *testing.T) {
	s, _ := newTestStore(t)

	const (
		count     = 10
		valueSize = 1 << 20
		maxBytes  = 2 << 20
	)
	for i := range count {
		s.SetLocal(fmt.Sprintf("big-%d", i), make([]byte, valueSize), 0)
	}

	total := 0
	err := s.Scan(context.Background(), matchAll, 1_000, maxBytes, func(batch []KeyedEntry) error {
		if len(batch) < 1 || len(batch) > 2 {
			t.Errorf("batch of %d entries of 1 MiB under a 2 MiB cap, want 1 or 2", len(batch))
		}

		var size int64
		for _, ke := range batch {
			size += ke.Size()
		}
		// A batch may exceed the cap only when a single entry does.
		if len(batch) > 1 && size > maxBytes {
			t.Errorf("batch of %d entries is %d bytes, over the cap of %d", len(batch), size, maxBytes)
		}

		total += len(batch)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan returned %v", err)
	}
	if total != count {
		t.Errorf("Scan returned %d entries, want %d", total, count)
	}
}

// An entry larger than the byte cap still has to be delivered, or a
// migration would silently skip it.
func TestStore_ScanOversizeEntry(t *testing.T) {
	s, _ := newTestStore(t)
	s.SetLocal("huge", make([]byte, 1<<20), 0)

	batches := 0
	err := s.Scan(context.Background(), matchAll, 1_000, 1_024, func(batch []KeyedEntry) error {
		batches++
		if len(batch) != 1 {
			t.Errorf("batch of %d entries, want 1", len(batch))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan returned %v", err)
	}
	if batches != 1 {
		t.Errorf("Scan delivered %d batches, want 1", batches)
	}
}

// Unexpired tombstones travel with a scan, or a delete would be undone
// by the snapshot it raced. Expired entries do not.
func TestStore_ScanKeepsTombstonesSkipsExpired(t *testing.T) {
	s, f := newTestStore(t)

	s.SetLocal("live", []byte("v"), 0)
	s.SetLocal("deleted", []byte("v"), 0)
	s.DeleteLocal("deleted")
	s.SetLocal("expiring", []byte("v"), time.Second)

	f.Advance(2 * time.Second) // "expiring" is now past its expiry

	seen := make(map[string]Entry)
	err := s.Scan(context.Background(), matchAll, 1_000, 1<<20, func(batch []KeyedEntry) error {
		for _, ke := range batch {
			seen[ke.Key] = ke.Entry
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan returned %v", err)
	}

	if _, ok := seen["live"]; !ok {
		t.Error("Scan skipped a live entry")
	}
	if e, ok := seen["deleted"]; !ok {
		t.Error("Scan skipped an unexpired tombstone")
	} else if !e.Tombstone {
		t.Error("the tombstone came back without its Tombstone flag")
	}
	if _, ok := seen["expiring"]; ok {
		t.Error("Scan returned an entry whose expiry had passed")
	}
}

func TestStore_ScanStopsOnError(t *testing.T) {
	s, _ := newTestStore(t)
	for i := range 5_000 {
		s.SetLocal(fmt.Sprintf("key-%05d", i), []byte("value"), 0)
	}

	sentinel := errors.New("stop here")
	batches := 0
	err := s.Scan(context.Background(), matchAll, 10, 1<<20, func([]KeyedEntry) error {
		batches++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("Scan returned %v, want the error from fn", err)
	}
	if batches != 1 {
		t.Errorf("fn ran %d times after returning an error, want 1", batches)
	}
}

func TestStore_ScanStopsOnCancel(t *testing.T) {
	s, _ := newTestStore(t)
	for i := range 5_000 {
		s.SetLocal(fmt.Sprintf("key-%05d", i), []byte("value"), 0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	err := s.Scan(ctx, matchAll, 10, 1<<20, func([]KeyedEntry) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Scan returned %v, want context.Canceled", err)
	}
}

func TestStore_DeleteMatching(t *testing.T) {
	s, _ := newTestStore(t)

	const n = 10_000
	wantRemoved := 0
	for i := range n {
		key := fmt.Sprintf("key-%05d", i)
		s.SetLocal(key, []byte("value"), 0)
		if topBit(keyhash.Sum(key)) {
			wantRemoved++
		}
	}

	removed := s.DeleteMatching(topBit)
	if removed != wantRemoved {
		t.Errorf("DeleteMatching removed %d entries, want %d", removed, wantRemoved)
	}
	if got, want := s.Len(), int64(n-wantRemoved); got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}

	// Nothing matching the predicate is left, and nothing else went.
	for i := range n {
		key := fmt.Sprintf("key-%05d", i)
		_, ok := s.Get(key)
		if match := topBit(keyhash.Sum(key)); match && ok {
			t.Fatalf("%q matches the predicate but survived", key)
		} else if !match && !ok {
			t.Fatalf("%q does not match the predicate but was removed", key)
		}
	}
	assertConsistent(t, s)
}

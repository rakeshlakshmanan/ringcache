package store

import (
	"fmt"
	"testing"
	"time"
)

func TestStore_SweepRemovesExpired(t *testing.T) {
	s, f := newTestStore(t)

	const n = 10_000

	var expiringBytes int64
	for i := range n {
		key := fmt.Sprintf("ttl-%05d", i)
		stored := s.SetLocal(key, []byte("value"), time.Second)
		expiringBytes += stored.Size(key)
	}
	for i := range n {
		s.SetLocal(fmt.Sprintf("keep-%05d", i), []byte("value"), 0)
	}

	if got := s.Len(); got != 2*n {
		t.Fatalf("Len() = %d before the sweep, want %d", got, 2*n)
	}
	bytesBefore := s.Bytes()

	f.Advance(2 * time.Second)

	// The sweep samples rather than scanning, so it takes repeated calls
	// to clear the store.
	removed, calls := 0, 0
	for range 100 {
		calls++
		removed += s.SweepOnce()
		if s.Len() == n {
			break
		}
	}
	t.Logf("cleared %d expired keys in %d SweepOnce calls", removed, calls)

	if removed != n {
		t.Errorf("the sweep removed %d entries, want exactly %d", removed, n)
	}
	if got := s.Len(); got != n {
		t.Errorf("Len() = %d after the sweep, want %d", got, n)
	}
	if got, want := s.Bytes(), bytesBefore-expiringBytes; got != want {
		t.Errorf("Bytes() = %d after the sweep, want %d (a drop of %d)", got, want, expiringBytes)
	}

	// Only the expired keys went.
	for i := range n {
		if _, ok := s.Get(fmt.Sprintf("keep-%05d", i)); !ok {
			t.Fatalf("the sweep removed keep-%05d, which has no expiry", i)
		}
	}

	// A further sweep over a store with nothing expired is a no-op.
	if got := s.SweepOnce(); got != 0 {
		t.Errorf("a sweep with nothing expired removed %d entries, want 0", got)
	}
	assertConsistent(t, s)
}

// The sweep must not touch an entry that still has time left, however
// many rounds it runs.
func TestStore_SweepKeepsUnexpired(t *testing.T) {
	s, f := newTestStore(t)

	for i := range 1_000 {
		s.SetLocal(fmt.Sprintf("k-%04d", i), []byte("value"), time.Minute)
	}
	before := s.Len()

	f.Advance(59 * time.Second)
	for range 10 {
		if got := s.SweepOnce(); got != 0 {
			t.Fatalf("the sweep removed %d entries a second before they expire, want 0", got)
		}
	}
	if got := s.Len(); got != before {
		t.Errorf("Len() = %d, want %d", got, before)
	}
	assertConsistent(t, s)
}

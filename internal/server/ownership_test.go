package server_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/membership"
	"github.com/rakeshlakshmanan/ringcache/internal/node"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
	"github.com/rakeshlakshmanan/ringcache/internal/server"
)

func testMembers(n int) []ring.Member {
	out := make([]ring.Member, n)
	for i := range out {
		id := fmt.Sprintf("node-%d", i+1)
		out[i] = ring.Member{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 7000+i)}
	}
	return out
}

func staticSource(t *testing.T, n int) *membership.Static {
	t.Helper()
	src, err := membership.NewStatic(testMembers(n))
	if err != nil {
		t.Fatalf("NewStatic: %v", err)
	}
	t.Cleanup(src.Close)
	return src
}

// The ownership rule in one table: a write is allowed only on the
// primary, a read anywhere on the preference list, and everywhere else
// the answer names the node's epoch so the client can tell its ring is
// behind.
func TestOwnership_Table(t *testing.T) {
	src := staticSource(t, 5)
	r := src.Current()

	// Find one key putting a single node in each of the three roles, so
	// the whole table is about the same node's point of view.
	const me = "node-1"
	var primaryKey, replicaKey, outsideKey string
	for i := range 10_000 {
		key := fmt.Sprintf("key-%d", i)
		pref := r.PreferenceIDs(keyhash.Sum(key))
		switch {
		case pref[0] == me && primaryKey == "":
			primaryKey = key
		case len(pref) > 1 && pref[1] == me && replicaKey == "":
			replicaKey = key
		case !slices.Contains(pref, me) && outsideKey == "":
			outsideKey = key
		}
		if primaryKey != "" && replicaKey != "" && outsideKey != "" {
			break
		}
	}
	if primaryKey == "" || replicaKey == "" || outsideKey == "" {
		t.Fatal("could not find a key for each role, so the table would be incomplete")
	}
	t.Logf("as seen by %s: primary of %q, replica of %q, outside %q",
		me, primaryKey, replicaKey, outsideKey)

	owner := server.RingOwnership(me, src)

	tests := []struct {
		name      string
		key       string
		wantWrite bool
		wantRead  bool
	}{
		{"primary", primaryKey, true, true},
		// A replica holds a copy, so it may serve the read; only the
		// primary may accept the write.
		{"replica", replicaKey, false, true},
		{"not on the preference list", outsideKey, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := keyhash.Sum(tt.key)
			assertOwnership(t, "CheckWrite", owner.CheckWrite(h), tt.wantWrite, r.Epoch())
			assertOwnership(t, "CheckRead", owner.CheckRead(h), tt.wantRead, r.Epoch())
		})
	}

	if got := owner.RingView(); got.GetEpoch() != membership.StaticEpoch || len(got.GetMembers()) != 5 {
		t.Errorf("RingView = epoch %d with %d members, want epoch %d with 5",
			got.GetEpoch(), len(got.GetMembers()), membership.StaticEpoch)
	}
}

func assertOwnership(t *testing.T, what string, err error, wantAllowed bool, epoch uint64) {
	t.Helper()

	if wantAllowed {
		if err != nil {
			t.Errorf("%s = %v, want it allowed", what, err)
		}
		return
	}
	if err == nil {
		t.Fatalf("%s was allowed, want a wrong-owner error", what)
	}

	// The epoch has to travel with the refusal: it is how a client
	// learns its own ring is behind.
	var wrong *server.WrongOwnerError
	if !errors.As(err, &wrong) {
		t.Fatalf("%s = %v, want a *server.WrongOwnerError", what, err)
	}
	if wrong.Epoch != epoch {
		t.Errorf("%s reported epoch %d, want the node's %d", what, wrong.Epoch, epoch)
	}
}

// A node missing from its own ring holds nothing anyone should read and
// owns nothing. A refresh would not change that, so it reports
// ErrNotMember, which maps to UNAVAILABLE, rather than a wrong-owner
// error that would send the client straight back to it.
func TestOwnership_NotInOwnRing(t *testing.T) {
	owner := server.RingOwnership("node-9", staticSource(t, 3))
	h := keyhash.Sum("key-1")

	for _, tt := range []struct {
		name string
		err  error
	}{
		{"CheckWrite", owner.CheckWrite(h)},
		{"CheckRead", owner.CheckRead(h)},
	} {
		if !errors.Is(tt.err, server.ErrNotMember) {
			t.Errorf("%s = %v, want ErrNotMember", tt.name, tt.err)
		}
		var wrong *server.WrongOwnerError
		if errors.As(tt.err, &wrong) {
			t.Errorf("%s gave a wrong-owner error, which would send the client back here", tt.name)
		}
	}
}

// Every key must have exactly one node accepting writes and exactly RF
// accepting reads. Each node decides alone from its own copy of the
// ring, so summing their decisions is what proves they agree (FR-01).
func TestOwnership_EveryKeyHasOneWriter(t *testing.T) {
	src := staticSource(t, 5)

	owners := make([]server.Ownership, 0, 5)
	for _, m := range testMembers(5) {
		owners = append(owners, server.RingOwnership(m.ID, src))
	}

	for i := range 5_000 {
		h := keyhash.Sum(fmt.Sprintf("key-%d", i))

		writers, readers := 0, 0
		for _, o := range owners {
			if o.CheckWrite(h) == nil {
				writers++
			}
			if o.CheckRead(h) == nil {
				readers++
			}
		}
		if writers != 1 {
			t.Fatalf("key-%d has %d nodes accepting writes, want exactly 1", i, writers)
		}
		if readers != config.ReplicationFactor {
			t.Fatalf("key-%d has %d nodes accepting reads, want %d", i, readers, config.ReplicationFactor)
		}
	}
}

// refreshSource is a Source whose Refresh reports that it was called.
type refreshSource struct {
	*membership.Static
	called chan struct{}
}

func (s refreshSource) Refresh(context.Context) error {
	select {
	case s.called <- struct{}{}:
	default:
	}
	return nil
}

// A node behind the cluster's epoch is the one case where a client
// knows something the node does not, so the client's epoch has to reach
// the membership source.
func TestCache_ClientEpochTriggersRefresh(t *testing.T) {
	static, err := membership.NewStatic([]ring.Member{{ID: testNodeID, Addr: "127.0.0.1:7000"}})
	if err != nil {
		t.Fatalf("NewStatic: %v", err)
	}
	t.Cleanup(static.Close)

	src := refreshSource{Static: static, called: make(chan struct{}, 8)}
	h := newHarness(t, node.Options{Membership: src})
	ctx := testContext(t)

	// Every ordinary request carries an epoch; one at or below the
	// node's must not start anything.
	if _, err := h.client.Get(ctx, &ringcachev1.GetRequest{
		Key: []byte("k"), ClientEpoch: membership.StaticEpoch,
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	select {
	case <-src.called:
		t.Fatal("a client at the node's own epoch triggered a refresh")
	default:
	}

	if _, err := h.client.Get(ctx, &ringcachev1.GetRequest{
		Key: []byte("k"), ClientEpoch: membership.StaticEpoch + 10,
	}); err != nil {
		t.Fatalf("Get: %v", err)
	}
	select {
	case <-src.called:
	case <-ctx.Done():
		t.Fatal("a client reporting a newer epoch did not trigger a refresh")
	}
}

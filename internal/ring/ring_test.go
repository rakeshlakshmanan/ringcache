package ring

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
)

// members returns node-1 to node-n, the IDs every threshold in this
// package's tests was measured against.
func members(n int) []Member {
	out := make([]Member, n)
	for i := range out {
		id := fmt.Sprintf("node-%d", i+1)
		out[i] = Member{ID: id, Addr: id + ":7000"}
	}
	return out
}

// newRing builds a ring of n members with the production token count
// and replication factor.
func newRing(t *testing.T, n int) *Ring {
	t.Helper()
	r, err := New(View{
		Epoch:   1,
		VNodes:  config.VNodes,
		RF:      config.ReplicationFactor,
		Members: members(n),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

// keyHashes returns the hashes of key-0 to key-(n-1), the key names the
// plan's balance and movement thresholds were measured with.
func keyHashes(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = keyhash.Sum("key-" + strconv.Itoa(i))
	}
	return out
}

func TestRing_Deterministic(t *testing.T) {
	ordered := members(5)
	shuffled := slices.Clone(ordered)
	rng := rand.New(rand.NewPCG(1, 2))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	if slices.Equal(ordered, shuffled) {
		t.Fatal("the shuffle left the order unchanged, so the test would prove nothing")
	}

	a, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: 2, Members: ordered})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: 2, Members: shuffled})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if !slices.Equal(a.tokens, b.tokens) {
		t.Fatal("member order changed the tokens; a client and a node would route differently")
	}
	for _, h := range keyHashes(1000) {
		if a.Lookup(h).ID != b.Lookup(h).ID {
			t.Fatalf("Lookup(%d) differs between the two orderings", h)
		}
	}
}

// New must not keep the caller's slice: a caller editing it afterwards
// would otherwise change a ring other goroutines are reading.
func TestRing_NewCopiesMembers(t *testing.T) {
	ms := members(3)
	r, err := New(View{Epoch: 1, VNodes: 4, RF: 2, Members: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ms[0] = Member{ID: "intruder", Addr: "intruder:7000"}

	if r.Has("intruder") {
		t.Error("editing the caller's slice changed the ring")
	}
	if !r.Has("node-1") {
		t.Error("the ring lost node-1")
	}
}

func TestRing_LookupWrap(t *testing.T) {
	r := newRing(t, 5)

	first, last := r.tokens[0], r.tokens[len(r.tokens)-1]

	// Past the last token the ring wraps round to the first.
	if got := r.Lookup(last.pos + 1); got.ID != r.members[first.member].ID {
		t.Errorf("Lookup past the last token = %s, want the first token's member %s",
			got.ID, r.members[first.member].ID)
	}
	if got := r.Lookup(^uint64(0)); got.ID != r.members[first.member].ID {
		t.Errorf("Lookup at the top of the ring = %s, want %s",
			got.ID, r.members[first.member].ID)
	}

	// A hash exactly on a token belongs to that token, not the next one:
	// arcs are closed at their end.
	for _, i := range []int{0, 1, len(r.tokens) / 2, len(r.tokens) - 1} {
		tk := r.tokens[i]
		if got := r.Lookup(tk.pos); got.ID != r.members[tk.member].ID {
			t.Errorf("Lookup on token %d = %s, want that token's member %s",
				i, got.ID, r.members[tk.member].ID)
		}
	}

	// A hash just below a token belongs to it as well, since the arc is
	// open at its start.
	second := r.tokens[1]
	if got := r.Lookup(second.pos - 1); got.ID != r.members[second.member].ID {
		t.Errorf("Lookup just below a token = %s, want %s", got.ID, r.members[second.member].ID)
	}
}

func TestRing_PreferenceListDistinct(t *testing.T) {
	r := newRing(t, 5)

	for _, h := range keyHashes(10_000) {
		pref := r.PreferenceList(h)
		if len(pref) != config.ReplicationFactor {
			t.Fatalf("PreferenceList(%d) has %d entries, want %d", h, len(pref), config.ReplicationFactor)
		}
		if pref[0].ID != r.Lookup(h).ID {
			t.Fatalf("PreferenceList(%d)[0] = %s, want Lookup's %s", h, pref[0].ID, r.Lookup(h).ID)
		}
		if pref[0].ID == pref[1].ID {
			t.Fatalf("PreferenceList(%d) holds %s twice; the two copies would land on one node",
				h, pref[0].ID)
		}
		if pref[0].Addr == "" || pref[1].Addr == "" {
			t.Fatalf("PreferenceList(%d) has a member with no address", h)
		}
	}
}

// A ring smaller than RF gives a shorter list rather than repeating a
// member: it is the normal state of a cluster that has just lost a node.
func TestRing_PreferenceListSmallRing(t *testing.T) {
	r, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: 2, Members: members(1)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, h := range keyHashes(100) {
		if pref := r.PreferenceList(h); len(pref) != 1 || pref[0].ID != "node-1" {
			t.Fatalf("PreferenceList(%d) = %v, want one entry for node-1", h, pref)
		}
	}
}

// NFR-08: with 256 tokens a node the largest share must stay within
// 1.15x the mean.
func TestRing_Balance(t *testing.T) {
	const (
		nodes    = 5
		keys     = 1_000_000
		maxRatio = 1.15
	)

	r := newRing(t, nodes)
	counts := make(map[string]int, nodes)
	for _, h := range keyHashes(keys) {
		counts[r.Lookup(h).ID]++
	}

	mean := float64(keys) / float64(nodes)
	worstID, worst := "", 0.0
	for id, n := range counts {
		if ratio := float64(n) / mean; ratio > worst {
			worstID, worst = id, ratio
		}
	}

	t.Logf("key counts over %d keys and %d nodes (mean %.0f): %v", keys, nodes, mean, counts)
	t.Logf("largest share: %s at %.4fx the mean", worstID, worst)
	if worst > maxRatio {
		t.Errorf("largest share is %.4fx the mean (%s), over the %.2fx limit (NFR-08)",
			worst, worstID, maxRatio)
	}
}

// FR-02: adding a node moves about 1/n of the keys, and every key that
// moves goes to the new node. That second half is what makes the move
// safe to plan: no key changes hands between two existing nodes.
func TestRing_MovementOnAdd(t *testing.T) {
	const keys = 1_000_000

	before, after := newRing(t, 4), newRing(t, 5)

	moved := 0
	for _, h := range keyHashes(keys) {
		was, now := before.Lookup(h).ID, after.Lookup(h).ID
		if was == now {
			continue
		}
		moved++
		if now != "node-5" {
			t.Fatalf("a key moved from %s to %s; adding a node must only move keys to it", was, now)
		}
	}

	share := float64(moved) / float64(keys) * 100
	t.Logf("adding node-5 to four nodes moved %.2f%% of %d keys", share, keys)
	if share < 17 || share > 23 {
		t.Errorf("moved %.2f%% of keys, want 17%% to 23%% (FR-02)", share)
	}
}

// The baseline consistent hashing replaces: with modulo hashing the
// divisor changes for every key at once.
func TestRing_ModuloBaseline(t *testing.T) {
	const keys = 1_000_000

	moved := 0
	for _, h := range keyHashes(keys) {
		if ModuloOwner(h, 4) != ModuloOwner(h, 5) {
			moved++
		}
	}

	share := float64(moved) / float64(keys) * 100
	t.Logf("modulo hashing from 4 to 5 nodes moved %.2f%% of %d keys", share, keys)
	if share < 79 || share > 81 {
		t.Errorf("modulo moved %.2f%% of keys, want 79%% to 81%%", share)
	}
}

// Phase 10's failover depends on this: when a node dies the ring is
// switched first and the new primary has to be the replica that already
// holds the data, or the switch would serve misses.
func TestRing_RemovalPromotesReplica(t *testing.T) {
	const keys = 200_000

	five := newRing(t, 5)
	remaining := slices.DeleteFunc(members(5), func(m Member) bool { return m.ID == "node-3" })
	four, err := New(View{Epoch: 2, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: remaining})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	promoted := 0
	for _, h := range keyHashes(keys) {
		was := five.PreferenceList(h)
		now := four.PreferenceList(h)

		if was[0].ID != "node-3" {
			if now[0].ID != was[0].ID {
				t.Fatalf("key at %d changed primary from %s to %s although node-3 was not its primary",
					h, was[0].ID, now[0].ID)
			}
			continue
		}
		promoted++
		// The old replica is the only node that already holds the key.
		if now[0].ID != was[1].ID {
			t.Fatalf("key at %d had primary node-3 and replica %s, but was promoted to %s",
				h, was[1].ID, now[0].ID)
		}
	}

	if promoted == 0 {
		t.Fatal("no key had node-3 as its primary, so the test proved nothing")
	}
	t.Logf("%d of %d keys had node-3 as primary and were promoted to their replica", promoted, keys)
}

func TestRing_HasAndView(t *testing.T) {
	r := newRing(t, 3)

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if !r.Has(id) {
			t.Errorf("Has(%q) = false", id)
		}
	}
	for _, id := range []string{"node-0", "node-4", "", "node-10"} {
		if r.Has(id) {
			t.Errorf("Has(%q) = true", id)
		}
	}

	if r.Epoch() != 1 || r.RF() != config.ReplicationFactor {
		t.Errorf("Epoch = %d, RF = %d", r.Epoch(), r.RF())
	}

	// View returns a copy: editing it must not reach the ring.
	v := r.View()
	v.Members[0] = Member{ID: "intruder", Addr: "x:1"}
	if r.Has("intruder") || !r.Has("node-1") {
		t.Error("View handed out the ring's own member slice")
	}
}

func TestRing_Invalid(t *testing.T) {
	tests := []struct {
		name string
		view View
		want string
	}{
		{"no members", View{VNodes: 4, RF: 2}, "no members"},
		{"no vnodes", View{RF: 2, Members: members(1)}, "VNodes"},
		{"no rf", View{VNodes: 4, Members: members(1)}, "RF"},
		{"empty id", View{VNodes: 4, RF: 2, Members: []Member{{Addr: "a:1"}}}, "no ID"},
		{"no address", View{VNodes: 4, RF: 2, Members: []Member{{ID: "node-1"}}}, "no address"},
		{"duplicate id", View{VNodes: 4, RF: 2, Members: []Member{
			{ID: "node-1", Addr: "a:1"}, {ID: "node-1", Addr: "b:1"},
		}}, "twice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.view); err == nil {
				t.Fatal("New succeeded, want an error")
			} else if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("New = %v, want it to mention %q", err, tt.want)
			}
		})
	}

	// RF above the member count is fine; the list is just shorter.
	if _, err := New(View{VNodes: 4, RF: 9, Members: members(2)}); err != nil {
		t.Errorf("New with RF above the member count: %v", err)
	}
}

func BenchmarkRing_PreferenceList(b *testing.B) {
	r, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: members(5)})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	hashes := keyHashes(1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if pref := r.PreferenceList(hashes[i&1023]); len(pref) != 2 {
			b.Fatalf("got %d members", len(pref))
		}
	}
}

func BenchmarkRing_Lookup(b *testing.B) {
	r, err := New(View{Epoch: 1, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: members(5)})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	hashes := keyHashes(1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if r.Lookup(hashes[i&1023]).ID == "" {
			b.Fatal("empty member")
		}
	}
}

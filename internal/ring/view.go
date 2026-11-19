// Package ring builds the consistent hash ring that nodes, clients and
// the coordinator all route with.
//
// Everything here is pure: the package does no I/O, reads no clock and
// starts no goroutines. Two processes given the same View compute
// byte-identical rings, which is what lets a client route to the right
// node without asking anyone, and what lets the coordinator reason
// about a ring the nodes have not received yet.
package ring

import (
	"fmt"
	"slices"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
)

// Member is one node in a ring: its identity and the address other
// nodes and clients dial.
type Member struct {
	ID   string
	Addr string
}

// View is everything needed to rebuild a ring. It is what travels on
// the wire, because members and parameters are small and the tokens
// derived from them are not.
type View struct {
	// Epoch of this ring. It only increases.
	Epoch uint64
	// VNodes is the number of tokens each member places on the ring.
	VNodes int
	// RF is the replication factor: how many distinct members a key's
	// preference list holds.
	RF int
	// Members of this ring, in any order. Ring sorts them.
	Members []Member
}

// Validate reports whether the view can build a ring.
func (v View) Validate() error {
	if v.VNodes <= 0 {
		return fmt.Errorf("ring: VNodes is %d, want at least 1", v.VNodes)
	}
	if v.RF <= 0 {
		return fmt.Errorf("ring: RF is %d, want at least 1", v.RF)
	}
	if len(v.Members) == 0 {
		return fmt.Errorf("ring: no members")
	}
	// RF above the member count is not an error: a ring smaller than RF
	// gives shorter preference lists until it grows. It is the normal
	// state of a cluster that has just lost a node.

	seen := make(map[string]struct{}, len(v.Members))
	for _, m := range v.Members {
		if m.ID == "" {
			return fmt.Errorf("ring: a member has no ID")
		}
		if m.Addr == "" {
			return fmt.Errorf("ring: member %q has no address", m.ID)
		}
		if _, dup := seen[m.ID]; dup {
			return fmt.Errorf("ring: member %q appears twice", m.ID)
		}
		seen[m.ID] = struct{}{}
	}
	return nil
}

// Clone returns a deep copy, so a caller holding a View cannot be
// affected by another one editing the member slice.
func (v View) Clone() View {
	v.Members = slices.Clone(v.Members)
	return v
}

// Equal reports whether two views would build the same ring. Member
// order does not count, since the ring sorts members by ID.
func (v View) Equal(other View) bool {
	if v.Epoch != other.Epoch || v.VNodes != other.VNodes || v.RF != other.RF {
		return false
	}
	a, b := sortedMembers(v.Members), sortedMembers(other.Members)
	return slices.Equal(a, b)
}

// ToProto converts the view to its wire form.
func (v View) ToProto() *ringcachev1.RingView {
	p := &ringcachev1.RingView{
		Epoch:   v.Epoch,
		Vnodes:  uint32(v.VNodes),
		Rf:      uint32(v.RF),
		Members: make([]*ringcachev1.Member, 0, len(v.Members)),
	}
	for _, m := range v.Members {
		p.Members = append(p.Members, &ringcachev1.Member{Id: m.ID, Addr: m.Addr})
	}
	return p
}

// FromProto converts a wire view back. It does not validate; call
// Validate, or New, which validates.
func FromProto(p *ringcachev1.RingView) View {
	if p == nil {
		return View{}
	}
	v := View{
		Epoch:   p.GetEpoch(),
		VNodes:  int(p.GetVnodes()),
		RF:      int(p.GetRf()),
		Members: make([]Member, 0, len(p.GetMembers())),
	}
	for _, m := range p.GetMembers() {
		v.Members = append(v.Members, Member{ID: m.GetId(), Addr: m.GetAddr()})
	}
	return v
}

// sortedMembers returns the members ordered by ID, without touching the
// caller's slice.
func sortedMembers(members []Member) []Member {
	out := slices.Clone(members)
	slices.SortFunc(out, func(a, b Member) int {
		if a.ID != b.ID {
			if a.ID < b.ID {
				return -1
			}
			return 1
		}
		return 0
	})
	return out
}

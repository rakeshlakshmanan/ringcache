// Package server holds the gRPC handlers a node serves: the client
// Cache API, the ownership check in front of it, the error mapping and
// the interceptors that measure and guard every call.
package server

import (
	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/membership"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

// Ownership decides whether this node may serve a key, and publishes
// the ring it decided from.
//
// Phase 4 has one node that owns everything. Phase 6 replaces the
// implementation with one backed by the hash ring; the handlers above
// it don't change.
type Ownership interface {
	// CheckWrite reports whether this node is the primary for the key
	// with this hash. It returns a WrongOwnerError when it is not, or
	// ErrNoRing when the node has no ring yet.
	CheckWrite(hash uint64) error
	// CheckRead reports whether this node is on the key's preference
	// list, under the same error contract as CheckWrite.
	CheckRead(hash uint64) error
	// RingView returns the ring this node is deciding from. Clients call
	// GetRing to bootstrap and to refresh after a WrongOwnerError.
	RingView() *ringcachev1.RingView
}

// allOwner owns every key. It is what a single node runs before the
// ring exists, and what the phase 4 binary runs throughout.
type allOwner struct {
	view *ringcachev1.RingView
}

// AllOwner returns an Ownership that accepts every key and reports a
// one-member ring at epoch 0.
//
// Epoch 0 means "no ring has been published". Clients treat any epoch
// they see as at least as new as their own, so a single node answering
// at epoch 0 never pushes a client into a refresh loop.
func AllOwner(nodeID, advertise string) Ownership {
	return &allOwner{view: &ringcachev1.RingView{
		Epoch: 0,
		Members: []*ringcachev1.Member{
			{Id: nodeID, Addr: advertise},
		},
		Vnodes: uint32(config.VNodes),
		Rf:     uint32(config.ReplicationFactor),
	}}
}

func (*allOwner) CheckWrite(uint64) error { return nil }
func (*allOwner) CheckRead(uint64) error  { return nil }

// RingView hands back the one view this owner was built with. It is
// never mutated after construction, so sharing it across concurrent
// calls is safe.
func (o *allOwner) RingView() *ringcachev1.RingView { return o.view }

// ringOwnership answers from the ring the node currently holds. It is
// what every node in a cluster runs: the source behind it is static in
// phase 6 and etcd-backed from phase 9, and this code does not care
// which.
type ringOwnership struct {
	nodeID string
	src    membership.Source
}

// RingOwnership returns an Ownership deciding from src's current ring.
func RingOwnership(nodeID string, src membership.Source) Ownership {
	return &ringOwnership{nodeID: nodeID, src: src}
}

// CheckWrite allows the write only on the key's primary. One node per
// key accepts writes within an epoch, which is what makes the version
// stamped on a write beat everything it replaces.
func (o *ringOwnership) CheckWrite(hash uint64) error {
	r, err := o.serving()
	if err != nil {
		return err
	}
	if r.Lookup(hash).ID != o.nodeID {
		return &WrongOwnerError{Epoch: r.Epoch()}
	}
	return nil
}

// CheckRead allows the read anywhere on the key's preference list,
// since every node in it holds a copy.
func (o *ringOwnership) CheckRead(hash uint64) error {
	r, err := o.serving()
	if err != nil {
		return err
	}
	for _, m := range r.PreferenceList(hash) {
		if m.ID == o.nodeID {
			return nil
		}
	}
	return &WrongOwnerError{Epoch: r.Epoch()}
}

// serving returns the ring to decide from, or the reason this node
// cannot decide at all.
func (o *ringOwnership) serving() (*ring.Ring, error) {
	r := o.src.Current()
	if r == nil {
		return nil, ErrNoRing
	}
	// A node missing from its own ring owns nothing and holds nothing
	// anyone should read: its keys have been handed to others. Answering
	// WRONG_OWNER would send the client back here after a refresh, so it
	// reports UNAVAILABLE and the client tries elsewhere.
	if !r.Has(o.nodeID) {
		return nil, ErrNotMember
	}
	return r, nil
}

// RingView returns the view the node is deciding from, or nil when it
// has no ring. GetRing turns nil into UNAVAILABLE.
func (o *ringOwnership) RingView() *ringcachev1.RingView {
	r := o.src.Current()
	if r == nil {
		return nil
	}
	return r.View().ToProto()
}

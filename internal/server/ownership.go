// Package server holds the gRPC handlers a node serves: the client
// Cache API, the ownership check in front of it, the error mapping and
// the interceptors that measure and guard every call.
package server

import (
	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
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

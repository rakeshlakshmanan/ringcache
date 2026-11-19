package server

import (
	"context"
	"math"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/hlc"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/store"
)

// maxTTL caps the ttl_ms field. It is the largest duration that still
// fits in a time.Duration, so a client sending a nonsense TTL gets
// INVALID_ARGUMENT rather than an expiry that wrapped into the past.
const maxTTL = uint64(math.MaxInt64 / int64(time.Millisecond))

// CacheServer serves the client-facing Cache API from one node's store.
//
// It holds no state of its own: identity, storage and ownership are all
// injected, so later phases swap the Ownership without touching the
// handlers.
type CacheServer struct {
	ringcachev1.UnimplementedCacheServer

	nodeID string
	store  *store.Store
	owner  Ownership
}

// NewCacheServer returns a CacheServer for the node with the given ID.
func NewCacheServer(nodeID string, st *store.Store, owner Ownership) *CacheServer {
	return &CacheServer{nodeID: nodeID, store: st, owner: owner}
}

// epoch reports the ring epoch this node is deciding from. Every
// response carries it, so a client learns its ring is stale even from a
// request that succeeded.
func (s *CacheServer) epoch() uint64 {
	if v := s.owner.RingView(); v != nil {
		return v.GetEpoch()
	}
	return 0
}

// Get returns the live value for a key. A key that is absent, deleted
// or expired reports found=false rather than an error, so a miss costs
// the client no retry.
func (s *CacheServer) Get(_ context.Context, req *ringcachev1.GetRequest) (*ringcachev1.GetResponse, error) {
	key, _, err := s.admitRead(req.GetKey())
	if err != nil {
		return nil, err
	}

	resp := &ringcachev1.GetResponse{
		NodeEpoch: s.epoch(),
		ServedBy:  s.nodeID,
	}
	if e, ok := s.store.Get(key); ok {
		resp.Found = true
		resp.Value = e.Value
		resp.Version = versionToProto(e.Version)
	}
	return resp, nil
}

// Set stores a value, stamping it with a version that beats whatever
// the key held. A ttl above zero becomes an absolute expiry here, so
// every copy of the key expires at the same instant (FR-10).
func (s *CacheServer) Set(_ context.Context, req *ringcachev1.SetRequest) (*ringcachev1.SetResponse, error) {
	if err := checkValue(req.GetValue()); err != nil {
		return nil, err
	}
	ttl, err := ttlDuration(req.GetTtlMs())
	if err != nil {
		return nil, err
	}
	key, _, err := s.admitWrite(req.GetKey())
	if err != nil {
		return nil, err
	}

	e := s.store.SetLocal(key, req.GetValue(), ttl)
	return &ringcachev1.SetResponse{
		Version:   versionToProto(e.Version),
		NodeEpoch: s.epoch(),
		ServedBy:  s.nodeID,
	}, nil
}

// Delete replaces a key with a tombstone and returns the tombstone's
// version. Deleting an absent key is not an error: the tombstone still
// has to exist, so a write already in flight cannot land behind it.
func (s *CacheServer) Delete(_ context.Context, req *ringcachev1.DeleteRequest) (*ringcachev1.DeleteResponse, error) {
	key, _, err := s.admitWrite(req.GetKey())
	if err != nil {
		return nil, err
	}

	e := s.store.DeleteLocal(key)
	return &ringcachev1.DeleteResponse{
		Version:   versionToProto(e.Version),
		NodeEpoch: s.epoch(),
		ServedBy:  s.nodeID,
	}, nil
}

// TTL reports the time left on a key. A key Get would miss reports
// found=false; a key stored without an expiry reports has_ttl=false.
func (s *CacheServer) TTL(_ context.Context, req *ringcachev1.TTLRequest) (*ringcachev1.TTLResponse, error) {
	key, _, err := s.admitRead(req.GetKey())
	if err != nil {
		return nil, err
	}

	remaining, hasTTL, found := s.store.TTL(key)
	return &ringcachev1.TTLResponse{
		Found:       found,
		HasTtl:      hasTTL,
		RemainingMs: remaining.Milliseconds(),
		NodeEpoch:   s.epoch(),
	}, nil
}

// GetRing returns the ring this node decides ownership from. Clients
// call it to bootstrap, to refresh after a wrong-owner error, and while
// waiting for a new epoch during a failover.
func (s *CacheServer) GetRing(context.Context, *ringcachev1.GetRingRequest) (*ringcachev1.RingView, error) {
	view := s.owner.RingView()
	if view == nil {
		return nil, toStatus(ErrNoRing)
	}
	return view, nil
}

// admitRead validates a key from the wire and asks whether this node
// may read it, returning the key as a string and its hash. The hash is
// computed once per request and handed back for the caller to reuse.
func (s *CacheServer) admitRead(key []byte) (string, uint64, error) {
	if err := checkKey(key); err != nil {
		return "", 0, err
	}
	k := string(key)
	h := keyhash.Sum(k)
	if err := s.owner.CheckRead(h); err != nil {
		return "", 0, toStatus(err)
	}
	return k, h, nil
}

// admitWrite is admitRead for a write: the key must be one this node is
// the primary for, not merely a replica of.
func (s *CacheServer) admitWrite(key []byte) (string, uint64, error) {
	if err := checkKey(key); err != nil {
		return "", 0, err
	}
	k := string(key)
	h := keyhash.Sum(k)
	if err := s.owner.CheckWrite(h); err != nil {
		return "", 0, toStatus(err)
	}
	return k, h, nil
}

// ttlDuration converts a ttl_ms field to a Duration, rejecting a value
// too large to represent.
func ttlDuration(ttlMs uint64) (time.Duration, error) {
	if ttlMs > maxTTL {
		return 0, status.Errorf(codes.InvalidArgument, "ttl_ms %d is out of range", ttlMs)
	}
	return time.Duration(ttlMs) * time.Millisecond, nil
}

// versionToProto converts an internal version to its wire form. The
// zero version converts to nil, which is how an unstamped entry travels.
func versionToProto(v hlc.Version) *ringcachev1.Version {
	if v.IsZero() {
		return nil
	}
	return &ringcachev1.Version{
		WallMs:  v.WallMs,
		Logical: v.Logical,
		NodeId:  uint32(v.NodeID),
	}
}

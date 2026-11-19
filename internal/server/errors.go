package server

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
)

// ErrNoRing is returned by an Ownership that has not been given a ring
// yet. It maps to UNAVAILABLE: the client should try another node,
// because this one cannot tell whether it owns the key.
var ErrNoRing = errors.New("server: no ring yet")

// ErrFenced is returned while a node has lost its etcd lease. Phase 10
// sets it; it maps to UNAVAILABLE for the same reason as ErrNoRing.
var ErrFenced = errors.New("server: node is fenced")

// ErrNotMember is returned by a node missing from its own current ring.
// It owns nothing and holds nothing anyone should read, and a refresh
// would not change that, so it maps to UNAVAILABLE rather than to a
// wrong-owner error that would send the client straight back.
var ErrNotMember = errors.New("server: node is not in its own ring")

// WrongOwnerError says this node does not own the key. Epoch is the
// ring epoch the node decided from, so a client can tell whether its
// own ring is behind.
type WrongOwnerError struct {
	Epoch uint64
}

func (e *WrongOwnerError) Error() string {
	return fmt.Sprintf("server: not the owner at epoch %d", e.Epoch)
}

// wrongOwner builds the FAILED_PRECONDITION status a client refreshes
// its ring on, with the node's epoch attached as a WrongOwner detail.
func wrongOwner(epoch uint64) error {
	st := status.New(codes.FailedPrecondition, "wrong owner")
	// A status with a detail that cannot be marshalled is still a useful
	// FAILED_PRECONDITION, so the plain status is the fallback rather
	// than an error for the caller to handle.
	withDetail, err := st.WithDetails(&ringcachev1.WrongOwner{NodeEpoch: epoch})
	if err != nil {
		return st.Err()
	}
	return withDetail.Err()
}

// toStatus maps an internal error to the gRPC status a client sees. It
// is the one place that mapping lives, so every handler reports the
// same thing for the same cause.
func toStatus(err error) error {
	if err == nil {
		return nil
	}

	var wrong *WrongOwnerError
	if errors.As(err, &wrong) {
		return wrongOwner(wrong.Epoch)
	}
	if errors.Is(err, ErrNoRing) || errors.Is(err, ErrFenced) || errors.Is(err, ErrNotMember) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		// Already a status: an argument check, or an error from a nested
		// call that was mapped where it happened.
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

// checkKey validates a key from the wire. A key must be present and
// within MaxKeyBytes; neither is retryable, so both are
// INVALID_ARGUMENT.
func checkKey(key []byte) error {
	if len(key) == 0 {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len(key) > config.MaxKeyBytes {
		return status.Errorf(codes.InvalidArgument,
			"key is %d bytes, over the %d byte limit", len(key), config.MaxKeyBytes)
	}
	return nil
}

// checkValue validates a value from the wire. An empty value is a
// legitimate value; only the upper bound is enforced.
func checkValue(value []byte) error {
	if len(value) > config.MaxValueBytes {
		return status.Errorf(codes.InvalidArgument,
			"value is %d bytes, over the %d byte limit", len(value), config.MaxValueBytes)
	}
	return nil
}

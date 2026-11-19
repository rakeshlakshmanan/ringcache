package client

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

// ErrNoRing is returned when the client has no ring and cannot get one.
var ErrNoRing = errors.New("client: no ring")

// kind says whether a call must go to the key's primary or may go to
// any member holding a copy.
type kind int

const (
	// write goes to the primary alone: one node per key accepts writes
	// within an epoch.
	write kind = iota
	// read may go to any member of the preference list.
	read
)

// outcome is what one attempt produced, and what the retry policy
// decides from.
type outcome int

const (
	// done: return this result, success or not.
	done outcome = iota
	// retryElsewhere: this node did not answer; try the next member of
	// the preference list.
	retryElsewhere
	// retryAfterRefresh: this node says it is not the owner; refresh
	// the ring and route again.
	retryAfterRefresh
)

// call runs one client request: it picks the target from the ring,
// makes up to MaxAttempts attempts and applies the retry policy
// between them.
//
// do is the RPC itself. It is given the member it is talking to so it
// can report it, and returns the node_epoch its response carried so the
// client can tell its ring is behind even from a call that succeeded.
func (c *Client) call(ctx context.Context, key string, k kind, do func(context.Context, ringcachev1.CacheClient, ring.Member) (uint64, error)) error {
	ci := callInfoFrom(ctx)
	hash := keyhash.Sum(key)

	var lastErr error
	// skip holds the members already tried, so a second attempt caused
	// by an unreachable node goes somewhere else.
	var tried []string

	for attempt := range c.opts.MaxAttempts {
		member, err := c.target(ctx, hash, k, tried)
		if err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		tried = append(tried, member.ID)

		cc, err := c.router.clientFor(member)
		if err != nil {
			lastErr = err
			continue
		}

		attemptCtx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		ci.noteAttempt(member.ID)
		epoch, err := do(attemptCtx, cc, member)
		cancel()

		ci.noteResult(status.Code(err), epoch)
		lastErr = err

		switch c.classify(err, epoch, k, attempt) {
		case done:
			// A successful response still reports the node's epoch, so a
			// client whose ring is behind learns it without an error.
			c.noteNodeEpoch(ctx, member.ID, epoch)
			return err
		case retryAfterRefresh:
			ci.noteRefresh()
			// Refresh from the node that objected: it is the one that
			// knows the newer ring.
			if rerr := c.router.refresh(ctx, member.ID); rerr != nil {
				return fmt.Errorf("%w (refreshing the ring also failed: %w)", err, rerr)
			}
			// The refreshed ring may route to a node already tried, and
			// that is correct now: it objected because the client was
			// routing with an old ring.
			tried = tried[:0]
		case retryElsewhere:
		}
	}

	return lastErr
}

// classify decides what to do after one attempt.
func (c *Client) classify(err error, epoch uint64, k kind, attempt int) outcome {
	if attempt >= c.opts.MaxAttempts-1 {
		// Out of attempts: whatever this is, it is the answer.
		return done
	}
	if err == nil {
		return done
	}

	switch status.Code(err) {
	case codes.FailedPrecondition:
		// Wrong owner. Only worth retrying when the node's ring is
		// actually newer than the client's; otherwise the client would
		// refresh to the same ring and route to the same node.
		if wrongOwnerEpoch(err, epoch) > c.router.epoch() {
			return retryAfterRefresh
		}
		return done

	case codes.Unavailable, codes.DeadlineExceeded:
		// The node did not answer: fenced, shutting down, unreachable or
		// too slow. A read has somewhere else to go; a write does not,
		// because only the primary may accept it. Phase 10 makes a write
		// wait for the epoch that names a new primary.
		if k == read {
			return retryElsewhere
		}
		return done

	default:
		// INVALID_ARGUMENT, INTERNAL and the rest: retrying changes
		// nothing.
		return done
	}
}

// statusCode names the gRPC code of an error, for Code.
func statusCode(err error) string { return status.Code(err).String() }

// wrongOwnerEpoch reads the epoch out of a wrong-owner error, falling
// back to the response's node_epoch when the detail is missing.
func wrongOwnerEpoch(err error, fallback uint64) uint64 {
	st, ok := status.FromError(err)
	if !ok {
		return fallback
	}
	for _, d := range st.Details() {
		if wo, ok := d.(*ringcachev1.WrongOwner); ok {
			return wo.GetNodeEpoch()
		}
	}
	return fallback
}

// target picks the member for the next attempt: the primary for a
// write, and for a read the first member of the preference list that
// has not been tried yet.
func (c *Client) target(ctx context.Context, hash uint64, k kind, tried []string) (ring.Member, error) {
	r := c.router.ring()
	if r == nil {
		if err := c.router.refresh(ctx, ""); err != nil {
			return ring.Member{}, fmt.Errorf("%w: %w", ErrNoRing, err)
		}
		if r = c.router.ring(); r == nil {
			return ring.Member{}, ErrNoRing
		}
	}

	pref := r.PreferenceList(hash)
	if len(pref) == 0 {
		return ring.Member{}, ErrNoRing
	}
	if k == write {
		return pref[0], nil
	}

	for _, m := range pref {
		if !contains(tried, m.ID) {
			return m, nil
		}
	}
	// Every copy has been tried. Returning the primary again is pointless
	// but harmless; the attempt budget ends the call either way.
	return pref[0], nil
}

func contains(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

// noteNodeEpoch starts a background refresh when a node reports an
// epoch newer than the client's ring. It is what keeps a client current
// without a wrong-owner error, which matters for reads: a read to a
// member that is still on the preference list succeeds even when the
// ring has moved on.
func (c *Client) noteNodeEpoch(ctx context.Context, member string, epoch uint64) {
	if epoch == 0 || epoch <= c.router.epoch() {
		return
	}
	callInfoFrom(ctx).noteRefresh()
	c.refreshInBackground(member)
}

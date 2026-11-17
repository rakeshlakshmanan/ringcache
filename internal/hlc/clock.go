package hlc

import (
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/clock"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
)

const (
	// skewWarnThreshold is how far ahead of local physical time a remote
	// version has to be before it's worth a warning. Beyond this, the
	// hosts' clocks disagree, which skews TTLs and can make newest-wins
	// pick the wrong write.
	skewWarnThreshold = 500 * time.Millisecond

	// skewWarnInterval rate-limits that warning, since a skewed peer
	// sends a version with every write.
	skewWarnInterval = time.Minute
)

// Clock issues Versions for one node. Every method is safe for
// concurrent use.
type Clock struct {
	nodeName string // the node ID as a string, for logs
	nodeID   uint16 // its 16-bit form, stamped into every Version
	c        clock.Clock

	mu           sync.Mutex
	last         Version   // the newest version this clock has issued or seen
	lastSkewWarn time.Time // zero until the first warning
}

// New returns a Clock for the given node ID, such as "node-3", reading
// physical time from c.
func New(nodeID string, c clock.Clock) *Clock {
	return &Clock{
		nodeName: nodeID,
		nodeID:   uint16(keyhash.Sum(nodeID)),
		c:        c,
	}
}

// NodeID returns the 16-bit node ID this clock stamps into versions.
func (k *Clock) NodeID() uint16 { return k.nodeID }

// Now returns a Version strictly greater than every version this clock
// has issued or seen.
func (k *Clock) Now() Version {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.nowLocked()
}

// Update moves this clock past remote, so the next Now is greater than
// it. A remote version behind local time contributes nothing beyond an
// ordinary local tick.
func (k *Clock) Update(remote Version) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.updateLocked(remote)
}

// NowAfter returns a Version strictly greater than both v and everything
// this clock has issued. The store uses it to stamp a local write that
// has to beat the version already held for the key.
//
// It takes the lock once for both steps, so no concurrent caller can
// slip a version in between and leave the result at or below v.
func (k *Clock) NowAfter(v Version) Version {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.updateLocked(v)
	return k.nowLocked()
}

// nowLocked issues the next version. The caller holds k.mu.
func (k *Clock) nowLocked() Version {
	phys := k.physMs()
	if phys > k.last.WallMs {
		// Physical time has moved on: restart the counter.
		k.last = Version{WallMs: phys, NodeID: k.nodeID}
	} else {
		// The wall clock stood still or jumped back. The counter keeps
		// versions increasing.
		k.last = k.tick(k.last.WallMs, k.last.Logical)
	}
	return k.last
}

// updateLocked merges remote into this clock. The caller holds k.mu.
func (k *Clock) updateLocked(remote Version) {
	phys := k.physMs()
	k.warnOnSkewLocked(remote, phys)

	// The new wall value is whichever of the three is furthest ahead.
	wall := max(k.last.WallMs, remote.WallMs, phys)
	lastAtWall := k.last.WallMs == wall
	remoteAtWall := remote.WallMs == wall

	switch {
	case lastAtWall && remoteAtWall:
		// Both sit in this millisecond: beat the higher counter.
		k.last = k.tick(wall, max(k.last.Logical, remote.Logical))
	case lastAtWall:
		// Only local time is at the front; remote is behind and
		// contributes nothing, so this is an ordinary tick.
		k.last = k.tick(wall, k.last.Logical)
	case remoteAtWall:
		// Remote is ahead of local time: adopt its millisecond and beat
		// its counter.
		k.last = k.tick(wall, remote.Logical)
	default:
		// Physical time alone is at the front, so the counter restarts.
		k.last = Version{WallMs: wall, NodeID: k.nodeID}
	}
}

// tick returns the version one step past wall.logical, carrying into the
// next millisecond if the counter is exhausted.
func (k *Clock) tick(wall int64, logical uint32) Version {
	if logical == math.MaxUint32 {
		return Version{WallMs: wall + 1, NodeID: k.nodeID}
	}
	return Version{WallMs: wall, Logical: logical + 1, NodeID: k.nodeID}
}

func (k *Clock) physMs() int64 { return k.c.Now().UnixMilli() }

// warnOnSkewLocked logs at most once per skewWarnInterval when a peer's
// version is well ahead of local physical time. The caller holds k.mu.
func (k *Clock) warnOnSkewLocked(remote Version, physMs int64) {
	skew := remote.WallMs - physMs
	if skew <= skewWarnThreshold.Milliseconds() {
		return
	}
	now := k.c.Now()
	if !k.lastSkewWarn.IsZero() && now.Sub(k.lastSkewWarn) < skewWarnInterval {
		return
	}
	k.lastSkewWarn = now
	slog.Warn("remote version is ahead of local physical time, check clock skew between hosts",
		"node_id", k.nodeName,
		"remote_version", remote.String(),
		"skew_ms", skew,
	)
}

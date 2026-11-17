package store

import (
	"context"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
)

// maxSweepRounds caps how many sampling rounds one SweepOnce call spends
// on a single shard, so a shard full of expired keys can't hold its
// write lock long enough to show up in client latency.
const maxSweepRounds = 16

// sweepRepeatThreshold is how many expired keys in a full sample make
// the sweep look at the same shard again.
const sweepRepeatThreshold = int(config.SweepRepeatAbove * float64(config.SweepSample))

// SweepOnce samples every shard for expired entries and removes the ones
// it finds, returning how many it removed.
//
// Reads expire entries lazily, so this only has to bound how long an
// expired entry keeps its memory, not whether it can still be read. It
// samples rather than scanning, and looks again at a shard while a full
// sample came back more than SweepRepeatAbove expired, which spends the
// work where the expired keys actually are.
func (s *Store) SweepOnce() int {
	nowMs := s.nowMs()
	removed := 0
	for i := range s.shards {
		removed += s.sweepShard(&s.shards[i], nowMs)
	}
	return removed
}

func (s *Store) sweepShard(sh *shard, nowMs int64) int {
	removed := 0
	for range maxSweepRounds {
		sampled, expired := 0, 0

		sh.mu.Lock()
		// Go randomises where a map range starts, which supplies the
		// sample. Deleting during the range is allowed.
		for key := range sh.expiring {
			if sampled == config.SweepSample {
				break
			}
			sampled++

			e, ok := sh.items[key]
			if !ok {
				// The index outlived the entry; drop the stale key.
				delete(sh.expiring, key)
				continue
			}
			if e.Expired(nowMs) {
				s.removeLocked(sh, key, e)
				expired++
			}
		}
		sh.mu.Unlock()

		removed += expired

		// Only a full sample says anything about how much is left, and
		// the threshold has to be passed to justify another round.
		if sampled < config.SweepSample || expired <= sweepRepeatThreshold {
			break
		}
	}
	return removed
}

// RunSweeper calls SweepOnce every config.SweepInterval until ctx is
// cancelled. Tests drive SweepOnce directly instead.
func (s *Store) RunSweeper(ctx context.Context) {
	t := time.NewTicker(config.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.SweepOnce()
		}
	}
}

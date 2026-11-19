package workload

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Outcome is what the checker made of one read.
type Outcome int

const (
	// Fine: the read returned what the writer last acknowledged, or a
	// write that was in doubt and turns out to have landed.
	Fine Outcome = iota
	// Stale: the read returned a sequence below the highest one
	// acknowledged for the key. An acknowledged write went backwards,
	// which is what a read from a replica that has not caught up looks
	// like (NFR-06).
	Stale
	// Corrupt: the read returned a sequence nobody wrote, or a value
	// filed under the wrong key. A run that sees one fails outright:
	// it is not a timing problem but a wrong answer.
	Corrupt
)

func (o Outcome) String() string {
	switch o {
	case Fine:
		return "fine"
	case Stale:
		return "stale"
	case Corrupt:
		return "corrupt"
	default:
		return "unknown"
	}
}

// StaleRead records one read that went backwards, so a run can say not
// only how many there were but when and from whom.
type StaleRead struct {
	Key string `json:"key"`
	// Got is the sequence the read returned, 0 for a miss.
	Got uint64 `json:"got"`
	// Acked is the highest sequence the writer had been told was
	// stored.
	Acked uint64 `json:"acked"`
	// ServedBy is the node that answered.
	ServedBy string `json:"served_by"`
	// At is how far into the run it happened.
	At time.Duration `json:"at"`
}

// LostWrite records an acknowledged write that was not there at the
// end of the run. This is the failure NFR-06 forbids.
type LostWrite struct {
	Key   string `json:"key"`
	Acked uint64 `json:"acked"`
	Found uint64 `json:"found"`
}

// keyRecord is what the checker remembers about one key.
//
// Each key is written by one worker, so a record is touched by one
// goroutine during the run and read by one goroutine after it. It
// carries no lock of its own.
type keyRecord struct {
	// acked is the highest sequence whose write was acknowledged.
	acked uint64
	// uncertain holds sequences whose write returned an error. The
	// write may or may not have reached the cluster, so a read
	// returning one of them is not evidence of anything going wrong.
	uncertain []uint64
	// hadTTL marks a key written with an expiry. Such a key is left out
	// of the loss checks, because it is allowed to vanish.
	hadTTL bool
}

// Checker decides what each read means and what the run proved.
type Checker struct {
	records []keyRecord
	// maxStale caps how many stale reads are kept in full. The count is
	// exact; only the detail is capped, so a badly broken run does not
	// fill memory with examples of the same thing.
	maxStale int

	mu         sync.Mutex
	staleReads []StaleRead
	staleCount int
	corrupt    []string
}

// NewChecker returns a checker for keys keys, remembering at most
// maxStale stale reads in detail.
func NewChecker(keys, maxStale int) *Checker {
	return &Checker{records: make([]keyRecord, keys), maxStale: maxStale}
}

// NoteWrite records the result of a write of sequence seq to key
// index. ok says whether the cluster acknowledged it.
//
// An acknowledged write settles everything below it: a write that was
// in doubt and is older than something now acknowledged can no longer
// explain a read.
func (c *Checker) NoteWrite(index int, seq uint64, ttl bool, ok bool) {
	rec := &c.records[index]
	if ttl {
		rec.hadTTL = true
	}

	if !ok {
		// The write may have landed or not; a later read seeing it is
		// not evidence of a fault.
		rec.uncertain = append(rec.uncertain, seq)
		return
	}

	if seq > rec.acked {
		rec.acked = seq
	}
	rec.uncertain = slices.DeleteFunc(rec.uncertain, func(s uint64) bool { return s <= rec.acked })
}

// NoteRead classifies one read. value is what came back, found says
// whether the key was there, and servedBy and at are recorded with a
// stale read.
func (c *Checker) NoteRead(index int, key string, value []byte, found bool, servedBy string, at time.Duration) (Outcome, error) {
	rec := &c.records[index]

	var got uint64
	if found {
		seq, err := DecodeExpect(value, uint64(index))
		if err != nil {
			c.noteCorrupt(fmt.Sprintf("%s: %v", key, err))
			return Corrupt, err
		}
		got = seq
	}

	switch {
	case got == rec.acked:
		return Fine, nil

	case got < rec.acked:
		// An acknowledged write went backwards. A miss counts here too:
		// got is 0, which is below anything acknowledged.
		c.noteStale(StaleRead{Key: key, Got: got, Acked: rec.acked, ServedBy: servedBy, At: at})
		return Stale, nil

	case slices.Contains(rec.uncertain, got):
		// A write that returned an error did land after all.
		return Fine, nil

	default:
		err := fmt.Errorf("workload: %s returned sequence %d, which was never written (acked %d)",
			key, got, rec.acked)
		c.noteCorrupt(err.Error())
		return Corrupt, err
	}
}

func (c *Checker) noteStale(s StaleRead) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.staleCount++
	if len(c.staleReads) < c.maxStale {
		c.staleReads = append(c.staleReads, s)
	}
}

func (c *Checker) noteCorrupt(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.corrupt) < c.maxStale {
		c.corrupt = append(c.corrupt, msg)
	}
}

// StaleCount returns how many stale reads the run saw.
func (c *Checker) StaleCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.staleCount
}

// StaleReads returns the stale reads kept in detail.
func (c *Checker) StaleReads() []StaleRead {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.staleReads)
}

// Corruptions returns the corruption messages seen.
func (c *Checker) Corruptions() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.corrupt)
}

// FinalCheck is what the sweep at the end of a run found.
type FinalCheck struct {
	// Checked is how many keys were read.
	Checked int `json:"checked"`
	// Lost are acknowledged writes that were not there at the end.
	Lost []LostWrite `json:"lost,omitempty"`
	// LostCount is the number of them, which may exceed len(Lost).
	LostCount int `json:"lost_count"`
	// UncertainLanded is how many writes that returned an error turned
	// out to have been stored.
	UncertainLanded int `json:"uncertain_landed"`
	// UncertainLost is how many returned an error and were not stored.
	// Neither is a fault: the write was never acknowledged.
	UncertainLost int `json:"uncertain_lost"`
	// Unreadable is how many keys could not be read at all, after
	// retries. Such a key proves nothing either way.
	Unreadable int `json:"unreadable"`
	// SkippedTTL is how many keys were left out because they were
	// written with an expiry.
	SkippedTTL int `json:"skipped_ttl"`
}

// finalReadAttempts is how many times the final sweep tries a key
// before giving up on it. The cluster has had the settle time to
// recover, so a key that still will not read is reported as unreadable
// rather than counted against the run.
const finalReadAttempts = 3

// NoteFinal records the final read of one key. found and value are
// what the sweep got; readErr is set when the key could not be read.
func (c *Checker) NoteFinal(fc *FinalCheck, index int, key string, value []byte, found bool, readErr error) {
	rec := &c.records[index]

	if rec.hadTTL {
		// A key with an expiry is allowed to vanish, so it cannot show
		// that anything was lost.
		fc.SkippedTTL++
		return
	}
	if readErr != nil {
		fc.Unreadable++
		return
	}
	fc.Checked++

	var got uint64
	if found {
		seq, err := DecodeExpect(value, uint64(index))
		if err != nil {
			c.noteCorrupt(fmt.Sprintf("final read of %s: %v", key, err))
			return
		}
		got = seq
	}

	switch {
	case got < rec.acked:
		// An acknowledged write is gone. This is the failure NFR-06
		// forbids.
		fc.LostCount++
		if len(fc.Lost) < c.maxStale {
			fc.Lost = append(fc.Lost, LostWrite{Key: key, Acked: rec.acked, Found: got})
		}
	case slices.Contains(rec.uncertain, got):
		fc.UncertainLanded++
	}

	// Writes that were in doubt and are not the value that survived
	// were not stored. That is not a fault either: they were never
	// acknowledged.
	for _, s := range rec.uncertain {
		if s != got {
			fc.UncertainLost++
		}
	}
}

// Acked returns the highest acknowledged sequence for a key, which the
// final sweep needs to know whether a key was ever written.
func (c *Checker) Acked(index int) uint64 { return c.records[index].acked }

// EverWritten reports whether anything was ever acknowledged or
// attempted for a key.
func (c *Checker) EverWritten(index int) bool {
	rec := &c.records[index]
	return rec.acked > 0 || len(rec.uncertain) > 0
}

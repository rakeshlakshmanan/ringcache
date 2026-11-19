package workload_test

import (
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/workload"
)

// step is one operation in a checker scenario.
type step struct {
	// write, when set, records a write of seq; ok says whether the
	// cluster acknowledged it.
	write bool
	seq   uint64
	ok    bool

	// read, when set, records a read returning seq, or a miss when
	// found is false.
	read  bool
	found bool

	want workload.Outcome
}

func writeOK(seq uint64) step   { return step{write: true, seq: seq, ok: true} }
func writeFail(seq uint64) step { return step{write: true, seq: seq, ok: false} }
func readsSeq(seq uint64, want workload.Outcome) step {
	return step{read: true, seq: seq, found: true, want: want}
}
func readsMiss(want workload.Outcome) step { return step{read: true, want: want} }

// The whole classification rule in one table. Each case is a sequence
// of operations on one key, and the outcome the checker should reach.
func TestChecker_Classification(t *testing.T) {
	const index = 3
	key := workload.Key(index)

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name:  "a read of what was last acknowledged is fine",
			steps: []step{writeOK(1), readsSeq(1, workload.Fine)},
		},
		{
			name: "a read of an older acknowledged write is stale",
			// The cluster said it stored 2, then answered with 1. An
			// acknowledged write went backwards (NFR-06).
			steps: []step{writeOK(1), writeOK(2), readsSeq(1, workload.Stale)},
		},
		{
			name: "a miss after an acknowledged write is stale",
			// A miss is sequence 0, which is below anything
			// acknowledged, so it is the same failure as any other
			// backwards read.
			steps: []step{writeOK(1), readsMiss(workload.Stale)},
		},
		{
			name:  "a miss before anything was written is fine",
			steps: []step{readsMiss(workload.Fine)},
		},
		{
			name: "a write that returned an error may or may not be there",
			// Either answer is consistent with a write whose result was
			// never learned, so neither is evidence of a fault.
			steps: []step{writeOK(1), writeFail(2), readsSeq(2, workload.Fine)},
		},
		{
			name:  "the older value is also fine while a write is in doubt",
			steps: []step{writeOK(1), writeFail(2), readsSeq(1, workload.Fine)},
		},
		{
			name: "a sequence nobody wrote is corruption",
			// 9 was never sent, so this is a wrong answer rather than a
			// timing problem, and the run fails.
			steps: []step{writeOK(1), readsSeq(9, workload.Corrupt)},
		},
		{
			name: "an acknowledged write settles the ones in doubt below it",
			// Once 3 is acknowledged, the in-doubt 2 can no longer
			// explain a read of 2: anything below 3 is stale.
			steps: []step{writeFail(2), writeOK(3), readsSeq(2, workload.Stale)},
		},
		{
			name: "a write in doubt above the acknowledged one still explains a read",
			steps: []step{writeOK(1), writeFail(3), writeFail(4),
				readsSeq(3, workload.Fine), readsSeq(4, workload.Fine)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := workload.NewChecker(10, 100)

			for i, s := range tt.steps {
				switch {
				case s.write:
					c.NoteWrite(index, s.seq, false, s.ok)
				case s.read:
					var value []byte
					if s.found {
						value = workload.Encode(index, s.seq, 32)
					}
					got, _ := c.NoteRead(index, key, value, s.found, "node-1", time.Second)
					if got != s.want {
						t.Fatalf("step %d: read gave %s, want %s", i, got, s.want)
					}
				}
			}
		})
	}
}

// A value filed under the wrong key is corruption however plausible
// its sequence looks.
func TestChecker_WrongKeyIsCorruption(t *testing.T) {
	c := workload.NewChecker(10, 100)
	c.NoteWrite(3, 1, false, true)

	// The bytes are a perfectly good value — for key 4.
	value := workload.Encode(4, 1, 32)
	got, err := c.NoteRead(3, workload.Key(3), value, true, "node-1", 0)
	if got != workload.Corrupt {
		t.Errorf("a value from another key gave %s, want corrupt", got)
	}
	if err == nil {
		t.Error("no error was returned for a value from another key")
	}
	if len(c.Corruptions()) != 1 {
		t.Errorf("the checker recorded %d corruptions, want 1", len(c.Corruptions()))
	}
}

// A stale read has to say when it happened and who served it, or a
// failure phase cannot attribute it to the failover that caused it.
func TestChecker_StaleReadDetail(t *testing.T) {
	c := workload.NewChecker(10, 100)
	c.NoteWrite(1, 5, false, true)

	if got, _ := c.NoteRead(1, "k-1", workload.Encode(1, 2, 32), true, "node-4", 3*time.Second); got != workload.Stale {
		t.Fatalf("got %s, want stale", got)
	}

	if c.StaleCount() != 1 {
		t.Fatalf("StaleCount = %d, want 1", c.StaleCount())
	}
	detail := c.StaleReads()
	if len(detail) != 1 {
		t.Fatalf("got %d stale reads in detail, want 1", len(detail))
	}
	s := detail[0]
	if s.Got != 2 || s.Acked != 5 || s.ServedBy != "node-4" || s.At != 3*time.Second {
		t.Errorf("stale read recorded as %+v", s)
	}
}

// The count stays exact once the detail cap is reached: a badly broken
// run must still report how bad it was.
func TestChecker_StaleDetailCapped(t *testing.T) {
	const detailCap = 5
	c := workload.NewChecker(10, detailCap)
	c.NoteWrite(0, 100, false, true)

	for range 50 {
		c.NoteRead(0, "k-0", workload.Encode(0, 1, 32), true, "node-1", 0)
	}

	if got := c.StaleCount(); got != 50 {
		t.Errorf("StaleCount = %d, want 50", got)
	}
	if got := len(c.StaleReads()); got != detailCap {
		t.Errorf("kept %d stale reads in detail, want the cap of %d", got, detailCap)
	}
}

func TestChecker_Final(t *testing.T) {
	tests := []struct {
		name string
		// setup records the writes before the sweep.
		setup func(*workload.Checker)
		// found and seq are what the final read returned.
		found bool
		seq   uint64
		ttl   bool

		wantLost      int
		wantLanded    int
		wantUncLost   int
		wantSkipped   int
		wantCheckedOK bool
	}{
		{
			name:          "the acknowledged write survived",
			setup:         func(c *workload.Checker) { c.NoteWrite(0, 3, false, true) },
			found:         true,
			seq:           3,
			wantCheckedOK: true,
		},
		{
			name:          "an acknowledged write is gone",
			setup:         func(c *workload.Checker) { c.NoteWrite(0, 3, false, true) },
			found:         false,
			wantLost:      1,
			wantCheckedOK: true,
		},
		{
			name:          "an older value survived instead of the acknowledged one",
			setup:         func(c *workload.Checker) { c.NoteWrite(0, 3, false, true) },
			found:         true,
			seq:           1,
			wantLost:      1,
			wantCheckedOK: true,
		},
		{
			name: "a write that was in doubt turns out to have landed",
			setup: func(c *workload.Checker) {
				c.NoteWrite(0, 1, false, true)
				c.NoteWrite(0, 2, false, false)
			},
			found:         true,
			seq:           2,
			wantLanded:    1,
			wantCheckedOK: true,
		},
		{
			// An acknowledged write settles every write in doubt below
			// it: those can no longer explain anything, so they must
			// not be reported as writes that failed to land. Only the
			// pruning in NoteWrite keeps this from counting 2.
			name: "a write in doubt that an acknowledged write superseded is not reported",
			setup: func(c *workload.Checker) {
				c.NoteWrite(0, 2, false, false)
				c.NoteWrite(0, 3, false, true)
			},
			found:         true,
			seq:           3,
			wantUncLost:   0,
			wantCheckedOK: true,
		},
		{
			// A write in doubt above the acknowledged one is still
			// unresolved, so when it is not what survived it really did
			// fail to land.
			name: "a write in doubt above the acknowledged one that did not land is reported",
			setup: func(c *workload.Checker) {
				c.NoteWrite(0, 3, false, true)
				c.NoteWrite(0, 4, false, false)
			},
			found:         true,
			seq:           3,
			wantUncLost:   1,
			wantCheckedOK: true,
		},
		{
			name: "a key with a TTL is left out: it is allowed to vanish",
			setup: func(c *workload.Checker) {
				c.NoteWrite(0, 3, true, true)
			},
			found:       false,
			ttl:         true,
			wantSkipped: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := workload.NewChecker(4, 100)
			tt.setup(c)

			var value []byte
			if tt.found {
				value = workload.Encode(0, tt.seq, 32)
			}

			var fc workload.FinalCheck
			c.NoteFinal(&fc, 0, workload.Key(0), value, tt.found, nil)

			if fc.LostCount != tt.wantLost {
				t.Errorf("LostCount = %d, want %d", fc.LostCount, tt.wantLost)
			}
			if fc.UncertainLanded != tt.wantLanded {
				t.Errorf("UncertainLanded = %d, want %d", fc.UncertainLanded, tt.wantLanded)
			}
			if fc.UncertainLost != tt.wantUncLost {
				t.Errorf("UncertainLost = %d, want %d", fc.UncertainLost, tt.wantUncLost)
			}
			if fc.SkippedTTL != tt.wantSkipped {
				t.Errorf("SkippedTTL = %d, want %d", fc.SkippedTTL, tt.wantSkipped)
			}
			if got := fc.Checked == 1; got != tt.wantCheckedOK {
				t.Errorf("Checked = %d", fc.Checked)
			}
		})
	}
}

// A key that could not be read at the end proves nothing either way,
// so it must not be counted as a loss.
func TestChecker_FinalUnreadable(t *testing.T) {
	c := workload.NewChecker(4, 100)
	c.NoteWrite(0, 3, false, true)

	var fc workload.FinalCheck
	c.NoteFinal(&fc, 0, workload.Key(0), nil, false, errFakeUnavailable)

	if fc.Unreadable != 1 {
		t.Errorf("Unreadable = %d, want 1", fc.Unreadable)
	}
	if fc.LostCount != 0 {
		t.Errorf("LostCount = %d, want 0: an unreadable key is not evidence of loss", fc.LostCount)
	}
}

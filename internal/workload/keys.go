// Package workload drives load at a ringcache cluster and checks what
// comes back.
//
// Every later phase's failure test measures the same way: start the
// load, break something, and ask the checker whether any acknowledged
// write was lost or any read went backwards. That is why this comes
// before the failure-handling phases rather than after them.
package workload

import "strconv"

// keyPrefix and keyDigits fix the key names. A fixed width keeps every
// key the same length, so the value size is the only thing that varies
// between runs.
const (
	keyPrefix = "k-"
	keyDigits = 8
)

// Key returns the name of key i.
func Key(i int) string {
	s := strconv.Itoa(i)
	if pad := keyDigits - len(s); pad > 0 {
		return keyPrefix + zeros[:pad] + s
	}
	return keyPrefix + s
}

var zeros = "00000000"

// WorkerFor says which worker owns key i.
//
// Each key belongs to exactly one worker, so operations on a key happen
// one after another. That is what lets the checker call a read stale
// without having to order concurrent writes to the same key: there are
// none.
func WorkerFor(i, workers int) int { return i % workers }

// KeysOf returns how many keys worker w owns out of total, and lets the
// caller map its own index onto the global key space with KeyOfWorker.
func KeysOf(w, workers, total int) int {
	if w >= total%workers {
		return total / workers
	}
	return total/workers + 1
}

// KeyOfWorker returns the global index of worker w's n-th key.
func KeyOfWorker(w, workers, n int) int { return n*workers + w }

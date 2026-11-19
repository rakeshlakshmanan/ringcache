package workload

import (
	"encoding/csv"
	"fmt"
	"io"
	"slices"
	"strconv"
	"sync"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Op names an operation in the stats and the CSV.
type Op string

const (
	OpGet    Op = "get"
	OpSet    Op = "set"
	OpDelete Op = "delete"
)

// AllOps is the order operations appear in the CSV.
var AllOps = []Op{OpGet, OpSet, OpDelete}

// Histogram bounds: 1 µs to 10 s at 3 significant digits. The low end
// is well below a local hit and the high end well above a request that
// waited out a failover, so nothing of interest is clipped.
const (
	histMin    = 1
	histMax    = int64(10 * time.Second / time.Microsecond)
	histDigits = 3
)

func newHist() *hdr.Histogram { return hdr.New(histMin, histMax, histDigits) }

// counters is one operation's tally in one second.
type counters struct {
	// ok is requests that succeeded on their first attempt.
	ok int64
	// retried is requests that succeeded, but only after a retry.
	retried int64
	// failed is requests that still failed after their retry.
	failed int64
	hist   *hdr.Histogram
}

// bucket is one second of the run.
type bucket struct {
	// sec is the second of the run this bucket covers.
	sec int
	// unixMs is the wall-clock start of that second.
	unixMs int64
	ops    map[Op]*counters
}

// codeCounts tallies the gRPC codes requests failed with. A run that
// reports failures is not diagnosable without them: a timeout, a node
// that was unavailable and a rejected argument mean entirely different
// things.
type codeCounts map[string]int64

// Stats collects per-second latency and outcome counts.
//
// One mutex guards everything. At the 20,000 requests per second the
// plan targets that is 20,000 lock acquisitions a second around a
// histogram record of some tens of nanoseconds, which is far below the
// cost of the RPC being measured, so sharding it would buy nothing and
// make merging the buckets harder.
type Stats struct {
	start time.Time

	mu      sync.Mutex
	buckets map[int]*bucket
	totals  map[Op]*counters
	// failCodes tallies the codes failures came back with, across every
	// operation.
	failCodes codeCounts
}

// NewStats returns a collector for a run starting at start.
func NewStats(start time.Time) *Stats {
	s := &Stats{
		start:     start,
		buckets:   make(map[int]*bucket),
		totals:    make(map[Op]*counters),
		failCodes: make(codeCounts),
	}
	for _, op := range AllOps {
		s.totals[op] = &counters{hist: newHist()}
	}
	return s
}

// Record adds one finished request: how long it took, how many
// attempts it needed and whether it succeeded in the end.
func (s *Stats) Record(op Op, at time.Duration, latency time.Duration, attempts int, ok bool, code string) {
	sec := int(at / time.Second)
	micros := latency.Microseconds()
	if micros < histMin {
		// A sub-microsecond request, which the in-memory fake target
		// produces, still has to be counted somewhere.
		micros = histMin
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !ok {
		if code == "" {
			code = "Unknown"
		}
		s.failCodes[code]++
	}

	b, exists := s.buckets[sec]
	if !exists {
		b = &bucket{
			sec:    sec,
			unixMs: s.start.Add(time.Duration(sec) * time.Second).UnixMilli(),
			ops:    make(map[Op]*counters, len(AllOps)),
		}
		s.buckets[sec] = b
	}
	c, exists := b.ops[op]
	if !exists {
		c = &counters{hist: newHist()}
		b.ops[op] = c
	}

	for _, target := range []*counters{c, s.totals[op]} {
		switch {
		case !ok:
			target.failed++
		case attempts > 1:
			target.retried++
		default:
			target.ok++
		}
		// A value above the histogram's ceiling is clamped rather than
		// dropped: losing the slowest requests would flatter the tail
		// that matters most.
		_ = target.hist.RecordValue(min(micros, histMax))
	}
}

// Summary is one operation's totals over the whole run.
type Summary struct {
	Op      Op    `json:"op"`
	OK      int64 `json:"ok"`
	Retried int64 `json:"retried"`
	Failed  int64 `json:"failed"`

	// Latencies are microseconds, as whole numbers because that is the
	// histogram's resolution; the mean is the one value that is not.
	P50  int64   `json:"p50_us"`
	P99  int64   `json:"p99_us"`
	P999 int64   `json:"p999_us"`
	Max  int64   `json:"max_us"`
	Mean float64 `json:"mean_us"`
}

// Total returns how many requests of this operation were made.
func (s Summary) Total() int64 { return s.OK + s.Retried + s.Failed }

// Summaries returns the run's totals, one per operation that saw
// traffic.
func (s *Stats) Summaries() []Summary {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []Summary
	for _, op := range AllOps {
		c := s.totals[op]
		if c.ok+c.retried+c.failed == 0 {
			continue
		}
		out = append(out, Summary{
			Op:      op,
			OK:      c.ok,
			Retried: c.retried,
			Failed:  c.failed,
			P50:     c.hist.ValueAtQuantile(50),
			P99:     c.hist.ValueAtQuantile(99),
			P999:    c.hist.ValueAtQuantile(99.9),
			Max:     c.hist.Max(),
			Mean:    c.hist.Mean(),
		})
	}
	return out
}

// Failures returns how many requests failed after their retry, across
// every operation.
func (s *Stats) Failures() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int64
	for _, c := range s.totals {
		n += c.failed
	}
	return n
}

// FailureCodes returns how many requests failed with each gRPC code.
func (s *Stats) FailureCodes() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]int64, len(s.failCodes))
	for k, v := range s.failCodes {
		out[k] = v
	}
	return out
}

// Total returns how many requests were made.
func (s *Stats) Total() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int64
	for _, c := range s.totals {
		n += c.ok + c.retried + c.failed
	}
	return n
}

// csvHeader is the CSV's first row.
var csvHeader = []string{
	"unix_ms", "t_sec", "op", "ok", "retried", "failed",
	"p50_us", "p99_us", "p999_us", "max_us",
}

// WriteCSV writes one row per second per operation, in time order. A
// chart of the run is a plot of these rows, so the failure phases can
// show latency and errors against the moment something was broken.
func (s *Stats) WriteCSV(w io.Writer) error {
	s.mu.Lock()
	secs := make([]int, 0, len(s.buckets))
	for sec := range s.buckets {
		secs = append(secs, sec)
	}
	slices.Sort(secs)
	rows := make([][]string, 0, len(secs)*len(AllOps))
	for _, sec := range secs {
		b := s.buckets[sec]
		for _, op := range AllOps {
			c, ok := b.ops[op]
			if !ok {
				continue
			}
			rows = append(rows, []string{
				strconv.FormatInt(b.unixMs, 10),
				strconv.Itoa(b.sec),
				string(op),
				strconv.FormatInt(c.ok, 10),
				strconv.FormatInt(c.retried, 10),
				strconv.FormatInt(c.failed, 10),
				strconv.FormatInt(c.hist.ValueAtQuantile(50), 10),
				strconv.FormatInt(c.hist.ValueAtQuantile(99), 10),
				strconv.FormatInt(c.hist.ValueAtQuantile(99.9), 10),
				strconv.FormatInt(c.hist.Max(), 10),
			})
		}
	}
	s.mu.Unlock()

	cw := csv.NewWriter(w)
	if err := cw.Write(csvHeader); err != nil {
		return fmt.Errorf("workload: write CSV header: %w", err)
	}
	if err := cw.WriteAll(rows); err != nil {
		return fmt.Errorf("workload: write CSV rows: %w", err)
	}
	cw.Flush()
	return cw.Error()
}

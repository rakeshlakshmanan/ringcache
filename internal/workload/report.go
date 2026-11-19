package workload

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime/debug"
	"time"
)

// Report is everything a run proved, written as report.json beside the
// CSV. It is what the README's results tables and every failure
// phase's assertion are read from.
type Report struct {
	// Commit is the build the run was made with, from the embedded
	// build info. A result is worth nothing without knowing which code
	// produced it.
	Commit string `json:"commit"`
	// Members are the cluster's node IDs at the start of the run.
	Members []string `json:"members,omitempty"`

	Config    Config    `json:"config"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	// Duration is how long the load actually ran, which can differ from
	// the configured duration when the run was cut short.
	Duration time.Duration `json:"duration"`

	// Operations is the per-operation totals and latencies.
	Operations []Summary `json:"operations"`
	// Throughput is requests per second over the run.
	Throughput float64 `json:"throughput_per_sec"`

	// Failures is requests that failed after their retry.
	Failures int64 `json:"failures"`
	// FailureCodes says what they failed with, which is the difference
	// between a cluster that was slow and one that was broken.
	FailureCodes map[string]int64 `json:"failure_codes,omitempty"`
	// StaleReads is reads that returned a sequence below what had been
	// acknowledged (NFR-06).
	StaleReads int `json:"stale_reads"`
	// StaleDetail holds the first stale reads in full.
	StaleDetail []StaleRead `json:"stale_detail,omitempty"`
	// Corruptions are values that could not have been written. Any is a
	// failed run.
	Corruptions []string `json:"corruptions,omitempty"`

	// Final is the sweep after the run settled.
	Final FinalCheck `json:"final"`
}

// Clean reports whether the run found nothing wrong: no failed
// requests, no stale reads, no lost acknowledged writes and no
// corruption. It is what --expect-zero-failures tests.
func (r *Report) Clean() bool {
	return r.Failures == 0 &&
		r.StaleReads == 0 &&
		r.Final.LostCount == 0 &&
		len(r.Corruptions) == 0
}

// Problems lists what the run found wrong, for a message a human reads.
func (r *Report) Problems() []string {
	var out []string
	if r.Failures > 0 {
		out = append(out, fmt.Sprintf("%d requests failed after their retry %v", r.Failures, r.FailureCodes))
	}
	if r.StaleReads > 0 {
		out = append(out, fmt.Sprintf("%d stale reads", r.StaleReads))
	}
	if r.Final.LostCount > 0 {
		out = append(out, fmt.Sprintf("%d acknowledged writes were lost", r.Final.LostCount))
	}
	if n := len(r.Corruptions); n > 0 {
		out = append(out, fmt.Sprintf("%d corrupt values (first: %s)", n, r.Corruptions[0]))
	}
	return out
}

// WriteJSON writes the report, indented so a human and a diff can both
// read it.
func (r *Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return fmt.Errorf("workload: write report: %w", err)
	}
	return nil
}

// String is the one-screen summary printed at the end of a run.
func (r *Report) String() string {
	s := fmt.Sprintf("ran %s at %.0f ops/s over %d keys\n",
		r.Duration.Round(time.Millisecond), r.Throughput, r.Config.Keys)
	for _, op := range r.Operations {
		s += fmt.Sprintf("  %-7s %8d ok  %6d retried  %6d failed   p50 %6dus  p99 %7dus  p99.9 %8dus  max %8dus\n",
			op.Op, op.OK, op.Retried, op.Failed, op.P50, op.P99, op.P999, op.Max)
	}
	s += fmt.Sprintf("  checked %d keys after settling: %d lost, %d uncertain landed, %d unreadable\n",
		r.Final.Checked, r.Final.LostCount, r.Final.UncertainLanded, r.Final.Unreadable)

	if r.Clean() {
		s += "  clean: no failures, no stale reads, no lost writes"
		return s
	}
	for _, p := range r.Problems() {
		s += "  PROBLEM: " + p + "\n"
	}
	return s
}

// buildCommit returns the commit the binary was built from, or
// "unknown" outside a VCS build, which is what `go test` is.
func buildCommit() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			if len(s.Value) > 12 {
				return s.Value[:12]
			}
			return s.Value
		}
	}
	return "unknown"
}

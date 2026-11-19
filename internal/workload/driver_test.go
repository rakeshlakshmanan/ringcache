package workload_test

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/testcluster"
	"github.com/rakeshlakshmanan/ringcache/internal/workload"
)

func testConfig() workload.Config {
	c := workload.DefaultConfig()
	c.Keys = 1_000
	c.ValueSize = 64
	c.Workers = 8
	c.Rate = 5_000
	c.Duration = 2 * time.Second
	c.Settle = 0
	c.Preload = true
	return c
}

func runDriver(t *testing.T, cfg workload.Config, target workload.Target) (*workload.Driver, *workload.Report) {
	t.Helper()

	d, err := workload.NewDriver(cfg, target)
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(),
		cfg.Duration+cfg.Settle+60*time.Second)
	defer cancel()

	if cfg.Preload {
		failed, err := d.Preload(ctx)
		if err != nil {
			t.Fatalf("Preload: %v", err)
		}
		if failed > 0 {
			t.Logf("%d of %d preload writes failed", failed, cfg.Keys)
		}
	}
	report, err := d.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return d, report
}

// The driver has to hit the rate it was asked for: every later phase's
// numbers are read as "at 20,000 ops/s", and a driver that quietly ran
// slower would make them meaningless.
func TestDriver_FakeTargetRate(t *testing.T) {
	cfg := testConfig()
	_, report := runDriver(t, cfg, newFakeTarget(1))

	if report.Failures != 0 {
		t.Errorf("%d requests failed against a target that never fails", report.Failures)
	}
	if n := report.StaleReads; n != 0 {
		t.Errorf("%d stale reads against a target that never serves one", n)
	}
	if err := workload.Check(report); err != nil {
		t.Errorf("the run was not clean: %v", err)
	}

	ratio := report.Throughput / cfg.Rate
	t.Logf("asked for %.0f ops/s, achieved %.0f (%.1f%% of target) over %s",
		cfg.Rate, report.Throughput, ratio*100, report.Duration.Round(time.Millisecond))
	if math.Abs(ratio-1) > 0.05 {
		t.Errorf("throughput was %.0f ops/s, want within 5%% of %.0f", report.Throughput, cfg.Rate)
	}

	// The read ratio should come out roughly as configured, or the
	// workload is not the one the run claims.
	var gets, sets int64
	for _, op := range report.Operations {
		switch op.Op {
		case workload.OpGet:
			gets = op.Total()
		case workload.OpSet:
			sets = op.Total()
		}
	}
	if gets+sets == 0 {
		t.Fatal("no operations were recorded")
	}
	readShare := float64(gets) / float64(gets+sets)
	t.Logf("%d reads, %d writes: read share %.3f (asked for %.2f)", gets, sets, readShare, cfg.ReadRatio)
	if math.Abs(readShare-cfg.ReadRatio) > 0.03 {
		t.Errorf("read share was %.3f, want about %.2f", readShare, cfg.ReadRatio)
	}
}

// A cluster that acknowledges writes without storing them is exactly
// the failure NFR-06 forbids. The checker has to find every one, or
// the failure phases would pass while losing data.
func TestDriver_DetectsLostWrites(t *testing.T) {
	target := newFakeTarget(2)
	target.dropWrites = 0.01

	cfg := testConfig()
	cfg.Settle = 10 * time.Millisecond
	_, report := runDriver(t, cfg, target)

	dropped := target.dropped.Load()
	if dropped == 0 {
		t.Fatal("the target dropped no writes, so the test proved nothing")
	}
	t.Logf("the target silently dropped %d of %d writes; the checker found %d lost keys",
		dropped, target.sets.Load(), report.Final.LostCount)

	if report.Final.LostCount == 0 {
		t.Fatal("the checker found no lost writes although writes were dropped")
	}
	if report.Clean() {
		t.Error("the report says the run was clean although writes were lost")
	}
	if err := workload.Check(report); err == nil {
		t.Error("Check accepted a run with lost writes")
	}

	// A dropped write is only lost if nothing later overwrote the key.
	// The final count is per key, so it cannot exceed the number
	// dropped.
	if int64(report.Final.LostCount) > dropped {
		t.Errorf("the checker reported %d lost keys but only %d writes were dropped",
			report.Final.LostCount, dropped)
	}
}

// A read served from a node that has not caught up is a stale read.
// Phase 8 onwards has to be able to count them, so the checker is
// shown one on purpose here.
func TestDriver_DetectsStaleReads(t *testing.T) {
	target := newFakeTarget(3)
	target.staleReads = 0.05

	_, report := runDriver(t, testConfig(), target)

	served := target.staled.Load()
	if served == 0 {
		t.Fatal("the target served no stale values, so the test proved nothing")
	}
	t.Logf("the target served %d stale values of %d reads; the checker counted %d stale reads",
		served, target.gets.Load(), report.StaleReads)

	if report.StaleReads == 0 {
		t.Fatal("the checker counted no stale reads although stale values were served")
	}
	if report.Clean() {
		t.Error("the report says the run was clean although reads went backwards")
	}

	// Every stale read must say who served it and when, or a failure
	// phase cannot attribute it.
	for _, s := range report.StaleDetail {
		if s.ServedBy == "" {
			t.Errorf("a stale read records no server: %+v", s)
		}
		if s.Got >= s.Acked {
			t.Errorf("a stale read is not actually behind: %+v", s)
		}
	}

	// Not every stale value served is a stale read: a value is only
	// behind if a newer one was acknowledged first.
	if report.StaleReads > int(served) {
		t.Errorf("the checker counted %d stale reads but only %d stale values were served",
			report.StaleReads, served)
	}
}

// A write whose result the client never learned may or may not have
// landed. Neither outcome is a fault, and neither may be counted as
// one.
func TestDriver_UncertainWritesAreNotFailures(t *testing.T) {
	target := newFakeTarget(4)
	target.failWrites = 0.05

	cfg := testConfig()
	cfg.Settle = 10 * time.Millisecond
	_, report := runDriver(t, cfg, target)

	if target.failed.Load() == 0 {
		t.Fatal("no writes failed, so the test proved nothing")
	}
	t.Logf("%d writes returned an error; %d of them landed, %d did not; %d acknowledged writes were lost",
		target.failed.Load(), report.Final.UncertainLanded, report.Final.UncertainLost, report.Final.LostCount)

	// The writes failed, so they count as failures — but nothing was
	// lost or went backwards, because nothing was ever acknowledged.
	if report.Failures == 0 {
		t.Error("the failed writes were not counted as failures")
	}
	if report.Final.LostCount != 0 {
		t.Errorf("%d writes were called lost, but a write that returned an error was never acknowledged",
			report.Final.LostCount)
	}
	if report.StaleReads != 0 {
		t.Errorf("%d stale reads, but reading either value is consistent with a write in doubt",
			report.StaleReads)
	}
}

// The real thing: a five-node static cluster under load for ten
// seconds has to lose nothing and go backwards nowhere.
func TestDriver_StaticCluster(t *testing.T) {
	c := testcluster.Start(t, 5, testcluster.Options{})

	cfg := testConfig()
	cfg.Keys = 20_000
	cfg.Rate = 5_000
	cfg.Duration = 10 * time.Second
	cfg.Settle = 500 * time.Millisecond
	cfg.Workers = 32

	target := workload.NewClientTarget(c.NewClient())
	_, report := runDriver(t, cfg, target)

	t.Logf("%s", report)

	if err := workload.Check(report); err != nil {
		t.Fatalf("a healthy five-node cluster under load was not clean: %v", err)
	}
	if report.Final.Checked == 0 {
		t.Fatal("the final sweep checked no keys")
	}
	if got, want := report.Throughput, cfg.Rate*0.95; got < want {
		t.Errorf("throughput was %.0f ops/s, want at least %.0f", got, want)
	}
	if len(report.Members) != 5 {
		t.Errorf("the report names %d members, want 5", len(report.Members))
	}
}

// The CSV is what the charts in phases 13 and 14 are drawn from, so
// its shape is part of the contract.
func TestDriver_CSV(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = 2 * time.Second
	d, report := runDriver(t, cfg, newFakeTarget(5))

	var buf strings.Builder
	if err := d.WriteCSV(&buf); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}

	var reported int64
	for _, op := range report.Operations {
		reported += op.Total()
	}

	rows, err := csv.NewReader(strings.NewReader(buf.String())).ReadAll()
	if err != nil {
		t.Fatalf("the CSV does not parse: %v", err)
	}
	if len(rows) < 2 {
		t.Fatalf("the CSV has %d rows, want a header and at least one second", len(rows))
	}

	wantHeader := []string{"unix_ms", "t_sec", "op", "ok", "retried", "failed",
		"p50_us", "p99_us", "p999_us", "max_us"}
	for i, want := range wantHeader {
		if rows[0][i] != want {
			t.Fatalf("column %d is %q, want %q", i, rows[0][i], want)
		}
	}

	// Seconds must come out in order, and every row must be a real
	// operation with sane numbers.
	lastSec := -1
	totalOps := 0
	for _, row := range rows[1:] {
		sec, err := strconv.Atoi(row[1])
		if err != nil {
			t.Fatalf("t_sec %q does not parse: %v", row[1], err)
		}
		if sec < lastSec {
			t.Fatalf("the CSV goes backwards: second %d after %d", sec, lastSec)
		}
		lastSec = sec

		switch workload.Op(row[2]) {
		case workload.OpGet, workload.OpSet, workload.OpDelete:
		default:
			t.Fatalf("unknown operation %q", row[2])
		}

		for _, i := range []int{3, 4, 5} {
			n, err := strconv.Atoi(row[i])
			if err != nil || n < 0 {
				t.Fatalf("column %d of %v is %q", i, row[2], row[i])
			}
			totalOps += n
		}

		p50, _ := strconv.Atoi(row[6])
		p99, _ := strconv.Atoi(row[7])
		maxUS, _ := strconv.Atoi(row[9])
		if p50 > p99 || p99 > maxUS {
			t.Fatalf("percentiles are out of order in %v: p50 %d, p99 %d, max %d", row, p50, p99, maxUS)
		}
	}

	// The rows have to account for every request the report counted,
	// or the chart and the summary would disagree.
	if int64(totalOps) != reported {
		t.Errorf("the CSV rows account for %d requests, but the report counted %d", totalOps, reported)
	}
	t.Logf("%d rows covering %d seconds and %d requests", len(rows)-1, lastSec+1, totalOps)
}

// The report is what later phases read their results from, so it has
// to survive a round trip through JSON.
func TestReport_JSON(t *testing.T) {
	_, report := runDriver(t, testConfig(), newFakeTarget(6))

	var buf strings.Builder
	if err := report.WriteJSON(&buf); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}

	var got workload.Report
	if err := json.Unmarshal([]byte(buf.String()), &got); err != nil {
		t.Fatalf("the report does not parse: %v", err)
	}

	if got.Config.Keys != report.Config.Keys || got.Config.Rate != report.Config.Rate {
		t.Errorf("the config did not survive the round trip: %+v", got.Config)
	}
	if len(got.Operations) != len(report.Operations) {
		t.Errorf("got %d operations, want %d", len(got.Operations), len(report.Operations))
	}
	if got.Throughput != report.Throughput {
		t.Errorf("throughput %v, want %v", got.Throughput, report.Throughput)
	}
	if !got.Clean() {
		t.Errorf("the round-tripped report is not clean: %v", got.Problems())
	}
	if got.Commit == "" {
		t.Error("the report carries no commit")
	}
}

func TestDriver_ConfigValidation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*workload.Config)
		want string
	}{
		{"no keys", func(c *workload.Config) { c.Keys = 0 }, "keys"},
		{"no workers", func(c *workload.Config) { c.Workers = 0 }, "workers"},
		{"no rate", func(c *workload.Config) { c.Rate = 0 }, "rate"},
		{"no duration", func(c *workload.Config) { c.Duration = 0 }, "duration"},
		{"read ratio above one", func(c *workload.Config) { c.ReadRatio = 1.5 }, "read-ratio"},
		{"value too small", func(c *workload.Config) { c.ValueSize = 4 }, "value-size"},
		// More workers than keys would leave a worker spinning with
		// nothing of its own to write.
		{"more workers than keys", func(c *workload.Config) { c.Keys = 4; c.Workers = 8 }, "each worker needs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.edit(&cfg)
			_, err := workload.NewDriver(cfg, newFakeTarget(7))
			if err == nil {
				t.Fatal("NewDriver accepted the config")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewDriver = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

// Cancelling has to be reported as a cancelled run, not as a short one
// that happened to finish.
func TestDriver_Cancellation(t *testing.T) {
	cfg := testConfig()
	cfg.Duration = time.Minute

	d, err := workload.NewDriver(cfg, newFakeTarget(8))
	if err != nil {
		t.Fatalf("NewDriver: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	if _, err := d.Run(ctx); err == nil {
		t.Fatal("Run returned a report for a cancelled run")
	}
}

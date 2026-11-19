package workload

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Config describes a run. The defaults are the plan's, and loadgen's
// flags map onto it one for one.
type Config struct {
	// Keys is how many keys the run uses.
	Keys int `json:"keys"`
	// ValueSize is the size of each value in bytes.
	ValueSize int `json:"value_size"`
	// Rate is the target requests per second across all workers.
	Rate float64 `json:"rate"`
	// Duration is how long to drive load for.
	Duration time.Duration `json:"duration"`
	// ReadRatio is the share of operations that are reads.
	ReadRatio float64 `json:"read_ratio"`
	// Workers is how many goroutines drive the load.
	Workers int `json:"workers"`
	// Preload writes every key once before the run, so reads hit rather
	// than miss from the start.
	Preload bool `json:"preload"`
	// Settle is how long to wait after the load stops before the final
	// sweep, giving replication and repair time to finish.
	Settle time.Duration `json:"settle"`
	// TTL, when above zero, is set on every write. Keys written with
	// one are left out of the loss checks.
	TTL time.Duration `json:"ttl"`
	// Seed makes a run repeatable.
	Seed uint64 `json:"seed"`
	// MaxDetail caps how many stale reads and lost writes are kept in
	// full in the report.
	MaxDetail int `json:"max_detail"`
}

// DefaultConfig is the plan's workload.
func DefaultConfig() Config {
	return Config{
		Keys:      1_000_000,
		ValueSize: 100,
		Rate:      20_000,
		Duration:  180 * time.Second,
		ReadRatio: 0.8,
		Workers:   64,
		Settle:    5 * time.Second,
		MaxDetail: 100,
	}
}

func (c Config) validate() error {
	switch {
	case c.Keys <= 0:
		return fmt.Errorf("workload: keys is %d, want at least 1", c.Keys)
	case c.Workers <= 0:
		return fmt.Errorf("workload: workers is %d, want at least 1", c.Workers)
	case c.Rate <= 0:
		return fmt.Errorf("workload: rate is %v, want a positive number", c.Rate)
	case c.Duration <= 0:
		return fmt.Errorf("workload: duration is %s, want a positive duration", c.Duration)
	case c.ReadRatio < 0 || c.ReadRatio > 1:
		return fmt.Errorf("workload: read-ratio is %v, want between 0 and 1", c.ReadRatio)
	case c.ValueSize < MinValueSize:
		return fmt.Errorf("workload: value-size is %d, want at least %d", c.ValueSize, MinValueSize)
	case c.Workers > c.Keys:
		// A worker with no keys would spin without doing anything.
		return fmt.Errorf("workload: %d workers for %d keys; each worker needs at least one key",
			c.Workers, c.Keys)
	}
	return nil
}

// Driver runs the workload against a Target and checks what comes back.
type Driver struct {
	cfg     Config
	target  Target
	checker *Checker
	stats   *Stats

	// seqs holds the next sequence number for each key. Only the key's
	// own worker touches its entry, so it needs no lock.
	seqs []uint64

	// opCount counts finished requests, for the Observer.
	opCount atomic.Int64

	// Observer, when set, is called for every finished request. loadgen
	// uses it to export Prometheus metrics for the client side.
	Observer func(op Op, latency time.Duration, call Call, ok bool)
}

// NewDriver returns a driver for cfg against target.
func NewDriver(cfg Config, target Target) (*Driver, error) {
	if cfg.MaxDetail <= 0 {
		cfg.MaxDetail = 100
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Driver{
		cfg:     cfg,
		target:  target,
		checker: NewChecker(cfg.Keys, cfg.MaxDetail),
		seqs:    make([]uint64, cfg.Keys),
	}, nil
}

// Checker exposes the checker, so a test can ask what it concluded
// without going through a report.
func (d *Driver) Checker() *Checker { return d.checker }

// WriteCSV writes the run's per-second rows. It is on the driver
// rather than the report because the rows are the stats, which the
// report summarises rather than carries.
func (d *Driver) WriteCSV(w io.Writer) error {
	if d.stats == nil {
		return fmt.Errorf("workload: no stats yet; call Run first")
	}
	return d.stats.WriteCSV(w)
}

// Preload writes every key once, so reads hit from the first second
// rather than spending the start of the run missing. It returns how
// many of those writes failed.
//
// A failed preload write is not fatal and does not stop the preload: it
// is an uncertain write like any other, and the checker already treats
// it as one. Aborting on the first would also make the result depend on
// which goroutine happened to hit the failure. The error is reserved
// for the caller cancelling.
//
// Preload does not go through the rate limiter: it is setup, not load,
// and its latencies are not part of the measurement.
func (d *Driver) Preload(ctx context.Context) (failed int, err error) {
	var (
		next    atomic.Int64
		failCnt atomic.Int64
		wg      sync.WaitGroup
	)

	for range d.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= d.cfg.Keys || ctx.Err() != nil {
					return
				}
				if !d.preloadKey(ctx, i) {
					failCnt.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if ctx.Err() != nil {
		return int(failCnt.Load()), fmt.Errorf("workload: preload cancelled: %w", ctx.Err())
	}
	return int(failCnt.Load()), nil
}

// preloadKey writes sequence 1 of one key and reports whether the
// cluster acknowledged it.
func (d *Driver) preloadKey(ctx context.Context, i int) bool {
	seq := uint64(1)
	d.seqs[i] = seq

	value := Encode(uint64(i), seq, d.cfg.ValueSize)
	_, err := d.target.Set(ctx, Key(i), value, d.cfg.TTL)
	d.checker.NoteWrite(i, seq, d.cfg.TTL > 0, err == nil)
	return err == nil
}

// Run drives the load, then waits out the settle time and sweeps every
// key. It returns what the run proved.
func (d *Driver) Run(ctx context.Context) (*Report, error) {
	start := time.Now()
	d.stats = NewStats(start)

	// Burst of one keeps the limiter from handing out a second's worth
	// of tokens at once after a pause, which would show up as a spike
	// the cluster never actually saw spread over that second.
	limiter := rate.NewLimiter(rate.Limit(d.cfg.Rate), 1)

	// The duration bounds when workers stop *issuing* requests, not the
	// requests themselves. Giving a request the run's deadline would
	// cancel whichever one was in flight when the clock ran out and
	// count it as a failure of the cluster, which it is not. At the
	// plan's rate roughly one request is in flight at any instant, so
	// that is one spurious failure in every run — enough to make a
	// "zero failures" assertion fail for no reason.
	//
	// Requests take the caller's context instead. They are still
	// bounded: the client gives every attempt its own AttemptTimeout.
	issueCtx, cancel := context.WithTimeout(ctx, d.cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup
	for w := range d.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.runWorker(ctx, issueCtx, w, limiter, start)
		}()
	}
	wg.Wait()

	ended := time.Now()
	elapsed := ended.Sub(start)

	// The caller cancelling is different from the run finishing: say so
	// rather than reporting a short run as a complete one.
	if ctx.Err() != nil {
		return nil, fmt.Errorf("workload: run cancelled after %s: %w", elapsed.Round(time.Millisecond), ctx.Err())
	}

	report := &Report{
		Commit:       buildCommit(),
		Config:       d.cfg,
		StartedAt:    start,
		EndedAt:      ended,
		Duration:     elapsed,
		Operations:   d.stats.Summaries(),
		Failures:     d.stats.Failures(),
		FailureCodes: d.stats.FailureCodes(),
		StaleReads:   d.checker.StaleCount(),
		StaleDetail:  d.checker.StaleReads(),
		Corruptions:  d.checker.Corruptions(),
	}
	if elapsed > 0 {
		report.Throughput = float64(d.stats.Total()) / elapsed.Seconds()
	}
	if t, ok := d.target.(*ClientTarget); ok {
		report.Members = t.Ring()
	}

	final, err := d.finalSweep(ctx)
	if err != nil {
		return nil, err
	}
	report.Final = final
	// Corruption found during the sweep belongs in the report too.
	report.Corruptions = d.checker.Corruptions()

	return report, nil
}

// runWorker drives one worker's share of the load. reqCtx is the
// caller's context, which the requests themselves take; issueCtx
// carries the run's duration and only decides when to stop.
func (d *Driver) runWorker(reqCtx, issueCtx context.Context, w int, limiter *rate.Limiter, start time.Time) {
	mine := KeysOf(w, d.cfg.Workers, d.cfg.Keys)
	if mine == 0 {
		return
	}

	// Each worker has its own PRNG, seeded from the run's seed and the
	// worker number, so a run repeats exactly.
	rng := rand.New(rand.NewPCG(d.cfg.Seed, uint64(w)+1))

	for {
		if err := limiter.Wait(issueCtx); err != nil {
			// The run's duration is up, or the caller cancelled.
			return
		}

		// A key from this worker's own partition: one worker per key
		// means operations on a key are ordered, so a read that goes
		// backwards cannot be explained by a concurrent write.
		i := KeyOfWorker(w, d.cfg.Workers, rng.IntN(mine))

		if rng.Float64() < d.cfg.ReadRatio {
			d.doRead(reqCtx, i, start)
		} else {
			d.doWrite(reqCtx, i, start)
		}
	}
}

func (d *Driver) doRead(ctx context.Context, i int, start time.Time) {
	key := Key(i)

	t0 := time.Now()
	value, found, call, err := d.target.Get(ctx, key)
	latency := time.Since(t0)
	at := t0.Sub(start)

	d.finish(OpGet, at, latency, call, err == nil)
	if err != nil {
		return
	}
	d.checker.NoteRead(i, key, value, found, call.ServedBy, at)
}

func (d *Driver) doWrite(ctx context.Context, i int, start time.Time) {
	d.seqs[i]++
	seq := d.seqs[i]
	key := Key(i)
	value := Encode(uint64(i), seq, d.cfg.ValueSize)

	t0 := time.Now()
	call, err := d.target.Set(ctx, key, value, d.cfg.TTL)
	latency := time.Since(t0)

	d.finish(OpSet, t0.Sub(start), latency, call, err == nil)
	d.checker.NoteWrite(i, seq, d.cfg.TTL > 0, err == nil)
}

// finish records one request in the stats and tells the observer.
func (d *Driver) finish(op Op, at, latency time.Duration, call Call, ok bool) {
	d.opCount.Add(1)
	d.stats.Record(op, at, latency, max(call.Attempts, 1), ok, call.Code)
	if d.Observer != nil {
		d.Observer(op, latency, call, ok)
	}
}

// finalSweep waits out the settle time, then reads every key once to
// see whether any acknowledged write is gone.
func (d *Driver) finalSweep(ctx context.Context) (FinalCheck, error) {
	if d.cfg.Settle > 0 {
		select {
		case <-time.After(d.cfg.Settle):
		case <-ctx.Done():
			return FinalCheck{}, fmt.Errorf("workload: cancelled while settling: %w", ctx.Err())
		}
	}

	var (
		fc   FinalCheck
		mu   sync.Mutex
		next atomic.Int64
		wg   sync.WaitGroup
	)

	for range d.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= d.cfg.Keys {
					return
				}
				if !d.checker.EverWritten(i) {
					// A key the run never touched proves nothing.
					continue
				}
				key := Key(i)
				value, found, readErr := d.readWithRetries(ctx, key)

				// The checker's per-key records are written by one
				// goroutine each, but the FinalCheck totals are shared.
				mu.Lock()
				d.checker.NoteFinal(&fc, i, key, value, found, readErr)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return fc, ctx.Err()
}

// readWithRetries reads one key for the final sweep. The cluster has
// had the settle time to recover, so a key that still will not read
// after three tries is reported as unreadable rather than counted as a
// loss.
func (d *Driver) readWithRetries(ctx context.Context, key string) ([]byte, bool, error) {
	var lastErr error
	for attempt := range finalReadAttempts {
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
		value, found, _, err := d.target.Get(ctx, key)
		if err == nil {
			return value, found, nil
		}
		lastErr = err
	}
	return nil, false, fmt.Errorf("workload: final read of %s: %w", key, lastErr)
}

// ErrNotClean is returned by Check when a run found something wrong.
var ErrNotClean = errors.New("workload: the run was not clean")

// Check returns ErrNotClean, with what went wrong, when the report is
// not clean. It is what --expect-zero-failures and the failure phases'
// tests both use.
func Check(r *Report) error {
	if r.Clean() {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrNotClean, r.Problems())
}

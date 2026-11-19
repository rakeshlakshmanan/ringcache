// Command loadgen drives load at a ringcache cluster and reports what
// it found: throughput, latency percentiles per second, and whether any
// acknowledged write was lost or any read went backwards.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rakeshlakshmanan/ringcache/internal/workload"
	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)

	cfg := workload.DefaultConfig()
	targets := fs.String("targets", "127.0.0.1:7000", "comma-separated node addresses to start from")
	fs.IntVar(&cfg.Keys, "keys", cfg.Keys, "how many keys the run uses")
	fs.IntVar(&cfg.ValueSize, "value-size", cfg.ValueSize, "value size in bytes")
	fs.Float64Var(&cfg.Rate, "rate", cfg.Rate, "target requests per second")
	fs.DurationVar(&cfg.Duration, "duration", cfg.Duration, "how long to drive load")
	fs.Float64Var(&cfg.ReadRatio, "read-ratio", cfg.ReadRatio, "share of operations that are reads")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "how many goroutines drive the load")
	fs.BoolVar(&cfg.Preload, "preload", false, "write every key once before the run")
	fs.DurationVar(&cfg.Settle, "settle", cfg.Settle, "wait after the load before the final sweep")
	fs.DurationVar(&cfg.TTL, "ttl", 0, "TTL for every write; keys written with one are left out of the loss checks")
	fs.Uint64Var(&cfg.Seed, "seed", 0, "PRNG seed, so a run repeats")
	outDir := fs.String("out-dir", "", "directory for loadgen.csv and report.json; empty writes nothing")
	metricsListen := fs.String("metrics-listen", ":9200", "HTTP address serving /metrics; empty disables it")
	expectZero := fs.Bool("expect-zero-failures", false,
		"exit 1 when there are failures, stale reads or lost writes")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if rest := fs.Args(); len(rest) > 0 {
		return fmt.Errorf("unexpected argument %q", rest[0])
	}

	seeds := splitAddrs(*targets)
	if len(seeds) == 0 {
		return fmt.Errorf("--targets is empty")
	}

	// SIGINT stops the run and still writes what it has, so a run cut
	// short by hand is not thrown away.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cl, err := client.New(ctx, seeds)
	if err != nil {
		return err
	}
	target := workload.NewClientTarget(cl)
	defer target.Close()

	driver, err := workload.NewDriver(cfg, target)
	if err != nil {
		return err
	}

	m := newMetrics()
	driver.Observer = m.observe
	if *metricsListen != "" {
		srv := serveMetrics(*metricsListen, m)
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutCtx)
		}()
	}

	if cfg.Preload {
		fmt.Fprintf(os.Stderr, "preloading %d keys...\n", cfg.Keys)
		t0 := time.Now()
		failed, err := driver.Preload(ctx)
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "preloaded in %s", time.Since(t0).Round(time.Millisecond))
		if failed > 0 {
			// Not fatal: these count as writes in doubt, and the final
			// sweep will say whether they landed.
			fmt.Fprintf(os.Stderr, " (%d of %d writes failed)", failed, cfg.Keys)
		}
		fmt.Fprintln(os.Stderr)
	}

	fmt.Fprintf(os.Stderr, "running %s at %.0f ops/s with %d workers...\n",
		cfg.Duration, cfg.Rate, cfg.Workers)

	report, err := driver.Run(ctx)
	if err != nil {
		return err
	}

	fmt.Println(report)

	if *outDir != "" {
		if err := writeResults(*outDir, report, driver); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "wrote %s and %s\n",
			filepath.Join(*outDir, "loadgen.csv"), filepath.Join(*outDir, "report.json"))
	}

	if *expectZero {
		return workload.Check(report)
	}
	return nil
}

// writeResults writes the CSV and the report to dir.
func writeResults(dir string, report *workload.Report, driver *workload.Driver) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}

	csvFile, err := os.Create(filepath.Join(dir, "loadgen.csv"))
	if err != nil {
		return fmt.Errorf("create the CSV: %w", err)
	}
	defer csvFile.Close()
	if err := driver.WriteCSV(csvFile); err != nil {
		return err
	}

	jsonFile, err := os.Create(filepath.Join(dir, "report.json"))
	if err != nil {
		return fmt.Errorf("create the report: %w", err)
	}
	defer jsonFile.Close()
	return report.WriteJSON(jsonFile)
}

func splitAddrs(s string) []string {
	var out []string
	for _, addr := range strings.Split(s, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			out = append(out, addr)
		}
	}
	return out
}

// metrics exports the client side of the run, so Grafana can show what
// the load generator saw next to what the nodes report.
type metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	retries  *prometheus.CounterVec
}

func newMetrics() *metrics {
	reg := prometheus.NewRegistry()
	m := &metrics{
		registry: reg,
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ringcache_loadgen",
			Name:      "requests_total",
			Help:      "Requests the load generator finished, by operation and result.",
		}, []string{"op", "result", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "ringcache_loadgen",
			Name:      "request_duration_seconds",
			Help:      "Client-side request duration, including retries.",
			Buckets:   prometheus.ExponentialBucketsRange(50e-6, 10, 20),
		}, []string{"op"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "ringcache_loadgen",
			Name:      "retries_total",
			Help:      "Requests that needed more than one attempt.",
		}, []string{"op"}),
	}
	reg.MustRegister(m.requests, m.latency, m.retries)
	return m
}

func (m *metrics) observe(op workload.Op, latency time.Duration, call workload.Call, ok bool) {
	result := "ok"
	if !ok {
		result = "failed"
	}
	code := call.Code
	if code == "" {
		code = "Unknown"
	}

	m.requests.WithLabelValues(string(op), result, code).Inc()
	m.latency.WithLabelValues(string(op)).Observe(latency.Seconds())
	if call.Retried() {
		m.retries.WithLabelValues(string(op)).Inc()
	}
}

// serveMetrics starts the /metrics endpoint. A failure to bind is
// reported but does not stop the run: the measurement matters more
// than the dashboard.
func serveMetrics(addr string, m *metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "loadgen: metrics server: %v\n", err)
		}
	}()
	return srv
}

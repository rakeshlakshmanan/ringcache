package metrics_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/rakeshlakshmanan/ringcache/internal/metrics"
)

// exported is every metric the project publishes, with the labels the
// Grafana dashboard and the chaos scripts select on.
//
// This list is a contract, not a description. The dashboard is built
// against these names, so renaming one here without changing the
// dashboard would leave a panel silently empty — which is worse than a
// broken build, because nothing reports it.
var exported = []struct {
	name   string
	labels []string
}{
	{"ringcache_requests_total", []string{"node", "method", "code"}},
	{"ringcache_request_duration_seconds", []string{"node", "method"}},
	{"ringcache_handler_panics_total", []string{"node"}},
	{"ringcache_store_keys", []string{"node"}},
	{"ringcache_store_bytes", []string{"node"}},
	{"ringcache_underreplicated_keys", []string{"node"}},
	{"ringcache_replication_duration_seconds", []string{"node", "mode"}},
	{"ringcache_replication_acks_total", []string{"node", "status"}},
	{"ringcache_async_queue_depth", []string{"node", "peer"}},
	{"ringcache_fenced", []string{"node"}},
	{"ringcache_ring_epoch", []string{"node"}},
	{"ringcache_migration_bytes_total", []string{"node", "direction"}},
	{"ringcache_migration_entries_total", []string{"node", "direction"}},
	{"ringcache_tasks", []string{"node", "status"}},
	{"ringcache_repair_seconds", []string{"node"}},
	{"ringcache_evictions_total", []string{"node"}},
}

// newFull returns a Metrics with every late-bound gauge registered, as
// a running node has.
func newFull(t *testing.T) *metrics.Metrics {
	t.Helper()

	m := metrics.New("node-1")
	m.StoreSizes("node-1", func() int64 { return 7 }, func() int64 { return 11 })
	m.Underreplicated("node-1", func() int64 { return 0 })
	return m
}

// Every declared metric must actually reach /metrics. A collector that
// is built but never registered is invisible, and nothing else would
// catch it.
func TestMetrics_AllExported(t *testing.T) {
	m := newFull(t)

	// Touch the vectors so they have at least one series: a CounterVec
	// with no children exports nothing.
	m.Requests.WithLabelValues("Cache/Get", "OK").Inc()
	m.Latency.WithLabelValues("Cache/Get").Observe(0.001)
	m.Panics.Inc()
	m.ReplicationLatency.WithLabelValues(metrics.ModeSync).Observe(0.001)
	m.ReplicationAcks.WithLabelValues("OK").Inc()
	m.AsyncQueueDepth.WithLabelValues("node-2").Set(3)
	m.Fenced.Set(0)
	m.RingEpoch.Set(1)
	m.MigrationBytes.WithLabelValues(metrics.DirectionSent).Add(1024)
	m.MigrationEntries.WithLabelValues(metrics.DirectionReceived).Add(10)
	m.Tasks.WithLabelValues("RUNNING").Set(1)
	m.RepairSeconds.Observe(1.5)
	m.Evictions.Inc()

	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	got := make(map[string][]string, len(families))
	for _, f := range families {
		var labels []string
		if len(f.GetMetric()) > 0 {
			for _, l := range f.GetMetric()[0].GetLabel() {
				labels = append(labels, l.GetName())
			}
		}
		slices.Sort(labels)
		got[f.GetName()] = labels
	}

	for _, want := range exported {
		labels, ok := got[want.name]
		if !ok {
			t.Errorf("%s is not exported; it is declared but never registered, or never given a series", want.name)
			continue
		}
		wantLabels := slices.Clone(want.labels)
		slices.Sort(wantLabels)
		if !slices.Equal(labels, wantLabels) {
			t.Errorf("%s has labels %v, want %v", want.name, labels, wantLabels)
		}
	}

	// Anything exported that is not on the list is either a new metric
	// nobody added to the dashboard, or a leak from the default
	// registry.
	for name := range got {
		if !slices.ContainsFunc(exported, func(e struct {
			name   string
			labels []string
		}) bool {
			return e.name == name
		}) {
			t.Errorf("%s is exported but not in the contract list; add it there and to the dashboard", name)
		}
	}
}

// Every metric carries the ringcache_ prefix, so a Prometheus scraping
// nodes and the load generator together can tell them apart.
func TestMetrics_Namespaced(t *testing.T) {
	m := newFull(t)

	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(families) == 0 {
		t.Fatal("nothing is exported at all")
	}
	for _, f := range families {
		if !strings.HasPrefix(f.GetName(), metrics.Namespace+"_") {
			t.Errorf("%s does not start with %s_", f.GetName(), metrics.Namespace)
		}
	}
}

// Each node owns its registry. Five nodes in one process, which the
// test cluster and the benchmarks both run, must report separately
// rather than colliding or double-counting.
func TestMetrics_PerNodeRegistry(t *testing.T) {
	a := metrics.New("node-1")
	b := metrics.New("node-2")

	a.Requests.WithLabelValues("Cache/Get", "OK").Inc()
	a.Requests.WithLabelValues("Cache/Get", "OK").Inc()
	b.Requests.WithLabelValues("Cache/Get", "OK").Inc()

	if got := testutil.ToFloat64(a.Requests.WithLabelValues("Cache/Get", "OK")); got != 2 {
		t.Errorf("node-1 counted %v requests, want 2", got)
	}
	if got := testutil.ToFloat64(b.Requests.WithLabelValues("Cache/Get", "OK")); got != 1 {
		t.Errorf("node-2 counted %v requests, want 1", got)
	}

	// The registries must be distinct objects, or one node's gauge
	// registration would fail as a duplicate.
	if a.Registry == b.Registry {
		t.Fatal("two nodes share one registry")
	}
	a.StoreSizes("node-1", func() int64 { return 1 }, func() int64 { return 2 })
	b.StoreSizes("node-2", func() int64 { return 3 }, func() int64 { return 4 })
}

// The store gauges read their values at scrape time, so the store
// keeps no reference to the metrics and the numbers cannot go stale.
func TestMetrics_StoreSizesAreLive(t *testing.T) {
	m := metrics.New("node-1")

	var keys, bytes int64
	m.StoreSizes("node-1", func() int64 { return keys }, func() int64 { return bytes })

	keys, bytes = 5, 500
	if got := gaugeValue(t, m, "ringcache_store_keys"); got != 5 {
		t.Errorf("store_keys = %v, want 5", got)
	}

	keys, bytes = 9, 900
	if got := gaugeValue(t, m, "ringcache_store_keys"); got != 9 {
		t.Errorf("store_keys = %v after the store changed, want 9", got)
	}
	if got := gaugeValue(t, m, "ringcache_store_bytes"); got != 900 {
		t.Errorf("store_bytes = %v, want 900", got)
	}
}

// gaugeValue reads one gauge out of the registry by name, which is how
// a gauge function's value is observed: the test does not hold the
// collector, only the registry it was registered in.
func gaugeValue(t *testing.T, m *metrics.Metrics, name string) float64 {
	t.Helper()

	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		if len(f.GetMetric()) == 0 {
			t.Fatalf("%s has no series", name)
		}
		return f.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("%s is not exported", name)
	return 0
}

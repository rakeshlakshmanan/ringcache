// Package metrics holds each node's Prometheus collectors.
//
// Every node owns its registry rather than using the default one, so
// several nodes running in one process — which the test cluster and the
// benchmarks both do — report separate numbers instead of colliding.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace prefixes every metric this project exports.
const Namespace = "ringcache"

// Metrics is one node's collectors and the registry they live in.
type Metrics struct {
	// Registry is what /metrics serves.
	Registry *prometheus.Registry

	// Requests counts finished RPCs by method and gRPC status code.
	Requests *prometheus.CounterVec
	// Latency measures handler duration by method. Its buckets run from
	// 50 µs to 1 s, which brackets the sub-millisecond local path and
	// still catches a request waiting out a failover.
	Latency *prometheus.HistogramVec
	// Panics counts handlers that panicked and were recovered.
	Panics prometheus.Counter
}

// New returns the collectors for the node with the given ID, registered
// in a fresh registry. Every metric carries the node ID as a constant
// label, so scraping several nodes gives comparable series.
func New(nodeID string) *Metrics {
	labels := prometheus.Labels{"node": nodeID}
	reg := prometheus.NewRegistry()

	m := &Metrics{
		Registry: reg,
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   Namespace,
			Name:        "requests_total",
			Help:        "Finished gRPC requests by method and status code.",
			ConstLabels: labels,
		}, []string{"method", "code"}),
		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   Namespace,
			Name:        "request_duration_seconds",
			Help:        "Server-side duration of gRPC requests by method.",
			ConstLabels: labels,
			// 50 µs to 1 s, four buckets per decade.
			Buckets: prometheus.ExponentialBucketsRange(50e-6, 1, 18),
		}, []string{"method"}),
		Panics: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   Namespace,
			Name:        "handler_panics_total",
			Help:        "Handlers that panicked and were recovered.",
			ConstLabels: labels,
		}),
	}
	reg.MustRegister(m.Requests, m.Latency, m.Panics)
	return m
}

// StoreSizes registers gauges reporting the live key and byte counts.
// They are gauge functions rather than gauges the store updates, so the
// store keeps no reference to the metrics and the numbers are read at
// scrape time from the counters it already maintains.
func (m *Metrics) StoreSizes(nodeID string, keys, bytes func() int64) {
	labels := prometheus.Labels{"node": nodeID}
	m.Registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "store_keys",
			Help:        "Entries held by this node, including unexpired tombstones.",
			ConstLabels: labels,
		}, func() float64 { return float64(keys()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "store_bytes",
			Help:        "Bytes held by this node: keys, values and per-entry overhead.",
			ConstLabels: labels,
		}, func() float64 { return float64(bytes()) }),
	)
}

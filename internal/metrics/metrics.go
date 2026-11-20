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
//
// Every collector the project exports is declared here, including the
// ones only later phases populate. They are named and registered up
// front for two reasons: the Grafana dashboard is built against these
// names, and this file would otherwise be edited by the replication,
// fencing, migration and eviction work at the same time.
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

	// --- replication (phase 8) ---

	// ReplicationLatency measures how long a replication took, by mode:
	// "sync" waits for the replica's ack, "async" does not.
	ReplicationLatency *prometheus.HistogramVec
	// ReplicationAcks counts replica acknowledgements by AckStatus, so
	// a replica refusing writes is visible rather than silent.
	ReplicationAcks *prometheus.CounterVec
	// AsyncQueueDepth is how many writes are waiting to be sent to each
	// peer in async mode.
	AsyncQueueDepth *prometheus.GaugeVec

	// --- fencing and membership (phases 9 and 10) ---

	// Fenced is 1 while the node has stopped serving because it could
	// not renew its etcd lease.
	Fenced prometheus.Gauge
	// RingEpoch is the epoch of the ring the node is serving from.
	RingEpoch prometheus.Gauge

	// --- migration and repair (phase 11) ---

	// MigrationBytes and MigrationEntries count what this node has
	// streamed, by direction: "sent" as a source, "received" as a
	// destination.
	MigrationBytes   *prometheus.CounterVec
	MigrationEntries *prometheus.CounterVec
	// Tasks is how many migration tasks this node has in each status.
	Tasks *prometheus.GaugeVec
	// RepairSeconds measures how long a repair took, from the epoch
	// change to the last task finishing. It is the number NFR-04 and
	// FR-07 are read from.
	RepairSeconds prometheus.Histogram

	// --- eviction (phase 15, stretch) ---

	// Evictions counts entries dropped to stay under the memory limit.
	Evictions prometheus.Counter
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

		ReplicationLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   Namespace,
			Name:        "replication_duration_seconds",
			Help:        "Time to replicate one write, by mode.",
			ConstLabels: labels,
			// 50 µs to 1 s, as for requests: a sync replication is part
			// of the client's latency, so the same scale applies.
			Buckets: prometheus.ExponentialBucketsRange(50e-6, 1, 18),
		}, []string{"mode"}),
		ReplicationAcks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   Namespace,
			Name:        "replication_acks_total",
			Help:        "Replica acknowledgements by status.",
			ConstLabels: labels,
		}, []string{"status"}),
		AsyncQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "async_queue_depth",
			Help:        "Writes waiting to be sent to each peer in async mode.",
			ConstLabels: labels,
		}, []string{"peer"}),

		Fenced: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "fenced",
			Help:        "1 while the node has stopped serving after failing to renew its lease.",
			ConstLabels: labels,
		}),
		RingEpoch: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "ring_epoch",
			Help:        "Epoch of the ring this node is serving from.",
			ConstLabels: labels,
		}),

		MigrationBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   Namespace,
			Name:        "migration_bytes_total",
			Help:        "Bytes streamed for migration or repair, by direction.",
			ConstLabels: labels,
		}, []string{"direction"}),
		MigrationEntries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   Namespace,
			Name:        "migration_entries_total",
			Help:        "Entries streamed for migration or repair, by direction.",
			ConstLabels: labels,
		}, []string{"direction"}),
		Tasks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   Namespace,
			Name:        "tasks",
			Help:        "Migration tasks this node holds, by status.",
			ConstLabels: labels,
		}, []string{"status"}),
		RepairSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace:   Namespace,
			Name:        "repair_seconds",
			Help:        "Time from an epoch change to the last repair task finishing.",
			ConstLabels: labels,
			// 100 ms to 10 minutes: a repair moves data, so it is orders
			// of magnitude slower than a request.
			Buckets: prometheus.ExponentialBucketsRange(0.1, 600, 16),
		}),

		Evictions: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   Namespace,
			Name:        "evictions_total",
			Help:        "Entries dropped to stay under the memory limit.",
			ConstLabels: labels,
		}),
	}

	reg.MustRegister(
		m.Requests, m.Latency, m.Panics,
		m.ReplicationLatency, m.ReplicationAcks, m.AsyncQueueDepth,
		m.Fenced, m.RingEpoch,
		m.MigrationBytes, m.MigrationEntries, m.Tasks, m.RepairSeconds,
		m.Evictions,
	)
	return m
}

// Direction values for the migration counters.
const (
	// DirectionSent is this node streaming as a migration source.
	DirectionSent = "sent"
	// DirectionReceived is this node applying as a destination.
	DirectionReceived = "received"
)

// Replication modes for ReplicationLatency.
const (
	// ModeSync waits for the replica's acknowledgement before the
	// client is answered.
	ModeSync = "sync"
	// ModeAsync queues the write and answers the client at once.
	ModeAsync = "async"
)

// Underreplicated registers the gauge reporting how many keys have
// fewer than RF copies on their current preference list.
//
// It is a gauge function, like the store sizes, because the number is
// summed from sources that do not exist when the metrics are built:
// replication's retry set in phase 8, and the keys awaiting repair in
// phase 11. The node composes them into one closure.
func (m *Metrics) Underreplicated(nodeID string, count func() int64) {
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace:   Namespace,
		Name:        "underreplicated_keys",
		Help:        "Keys with fewer than rf copies on their current preference list.",
		ConstLabels: prometheus.Labels{"node": nodeID},
	}, func() float64 { return float64(count()) }))
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

// Package config holds ringcache's defaults and, from phase 4 on, flag
// parsing. Timing constants and sizes live only here; code and tests
// refer to them by name.
package config

import "time"

// Ring and replication shape.
const (
	// VNodes is the number of tokens each node places on the hash ring.
	// 256 tokens keep the largest node within 1.15x the mean key count
	// (FR-02, NFR-08).
	VNodes int = 256

	// ReplicationFactor is the number of copies of every key: primary
	// plus one replica (FR-03). Quorum mode (FR-15) raises it to 3.
	ReplicationFactor int = 2
)

// Storage engine.
const (
	// StoreShards is the number of lock-sharded maps in the store. It
	// keeps lock contention low enough for the latency targets (NFR-01).
	// Must be a power of two: the shard is picked by Hash & (StoreShards-1).
	StoreShards int = 256

	// TombstoneLifetime is how long a DELETE's tombstone is kept so a
	// late replication message or migration copy can't resurrect the
	// key, which would surface as a lost write (NFR-06).
	TombstoneLifetime time.Duration = 60 * time.Second

	// SweepInterval is how often the expiry sweep runs. Together with
	// lazy expiry on reads it makes every copy of a key vanish at its
	// absolute expiry time (FR-10).
	SweepInterval time.Duration = 100 * time.Millisecond

	// SweepSample is how many expiring keys the sweep samples per shard
	// per round (FR-10).
	SweepSample int = 20

	// SweepRepeatAbove makes the sweep sample a shard again while more
	// than this share of a full sample had expired (FR-10).
	SweepRepeatAbove float64 = 0.25
)

// Request limits (enforced from phase 4).
const (
	// MaxKeyBytes is the largest accepted key: string keys up to 1 KiB
	// (spec section 2); larger gives INVALID_ARGUMENT.
	MaxKeyBytes int = 1 << 10

	// MaxValueBytes is the largest accepted value: byte values up to
	// 1 MiB (spec section 2); larger gives INVALID_ARGUMENT.
	MaxValueBytes int = 1 << 20

	// MaxGRPCMessage is the gRPC server's maximum receive and send
	// size. It leaves room for a full migration batch (StreamBatchBytes)
	// plus overhead.
	MaxGRPCMessage int = 8 << 20
)

// Client routing and retries.
const (
	// AttemptTimeout is the deadline for one client attempt. A first
	// guess to be set from the p99.9 measured in the first benchmark
	// (spec section 18); serves FR-04 and FR-05.
	AttemptTimeout time.Duration = 50 * time.Millisecond

	// MaxAttempts is the number of attempts per client request: one
	// retry, whatever caused it (NFR-03).
	MaxAttempts int = 2

	// RingRefreshInterval is how often the client refreshes its cached
	// ring in the background (FR-11).
	RingRefreshInterval time.Duration = 5 * time.Second
)

// Replication.
const (
	// ReplicaAckTimeout is how long the primary waits for the replica's
	// ack in sync mode (FR-03, NFR-02), and how long an async enqueue
	// may block when the queue is full.
	ReplicaAckTimeout time.Duration = 20 * time.Millisecond

	// AsyncQueueSize is the per-peer queue of writes in async mode
	// (FR-13).
	AsyncQueueSize int = 10_000

	// ReplicationRetryInterval is how often keys whose replication
	// failed are retried, restoring two copies (FR-07).
	ReplicationRetryInterval time.Duration = 200 * time.Millisecond
)

// Membership and failure detection.
const (
	// LeaseTTL is the requested etcd lease TTL. The lease is the
	// heartbeat: a dead node is out of the ring within the granted TTL
	// plus 1 s (FR-06, NFR-04). Fencing sizes itself from the granted
	// TTL, which etcd may set higher than this request.
	LeaseTTL time.Duration = 2 * time.Second

	// BootstrapExpect is how many live JOINING nodes the coordinator
	// waits for before publishing the first ring (phase 9 bootstrap).
	BootstrapExpect int = 3

	// FenceRatio is the share of the granted lease TTL after which a
	// node that hasn't renewed stops serving (FR-12), so it fences
	// before etcd can expire its lease and hand its keys away.
	FenceRatio float64 = 0.75

	// EpochPollInterval is how often a client waiting out a dead
	// primary polls GetRing for a newer epoch (FR-05).
	EpochPollInterval time.Duration = 100 * time.Millisecond

	// EpochWaitMax caps that wait; with failover inside LeaseTTL + 1 s
	// the retry lands well before this (FR-05, NFR-05).
	EpochWaitMax time.Duration = 4 * time.Second
)

// Range streaming (migration and repair).
const (
	// StreamBatchEntries is the most entries in one StreamRange batch
	// (NFR-09, NFR-10).
	StreamBatchEntries int = 1_000

	// StreamBatchBytes is the most bytes in one StreamRange batch
	// (NFR-10).
	StreamBatchBytes int = 2 << 20

	// StreamRate caps a migration source's send rate in bytes per
	// second, keeping client p99 within NFR-10 while a join streams.
	StreamRate int = 20 << 20

	// TaskRetryMax caps the destination worker's exponential backoff
	// between attempts at a MOVE or REPAIR task (FR-07).
	TaskRetryMax time.Duration = 5 * time.Second
)

// Membership changes.
const (
	// GCGrace is how long a node keeps data for arcs it no longer owns
	// before deleting it, so a cancelled change can't lose keys (FR-08).
	GCGrace time.Duration = 30 * time.Second

	// LeaveDrain is how long a leaving node answers WRONG_OWNER after
	// the ring without it is published, so clients refresh before it
	// exits (FR-09).
	LeaveDrain time.Duration = 3 * time.Second
)

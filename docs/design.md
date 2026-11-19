# ringcache design log

Decisions are recorded here with the phase that made them. The first
section restates how the system works, copied from the implementation
plan so this file stands on its own.

## How the system works

ringcache runs as 3 to 7 node processes that clients use as one cache.
Keys sit on a 64-bit hash ring. Each node places 256 tokens on the ring,
and a key belongs to the first token clockwise from `xxhash(key)`.
Walking clockwise from that token and collecting distinct nodes gives
the key's preference list. The first node in it is the primary and the
second is the replica, so every key has two copies.

Each published ring has an epoch number. Within one epoch only the
primary accepts writes for a key, and it stamps each write with a hybrid
logical clock (HLC) version. Every path that stores an entry (client
writes, replication, migration) keeps the newest version, so duplicates
and reordering do no harm. Replication is synchronous by default: the
primary waits up to 20 ms for the replica's acknowledgement before it
answers the client.

Membership lives in etcd. Each node holds a 2 s lease and renews it in
the background. When a node dies its lease expires and etcd deletes its
key. One node at a time is the coordinator, chosen by etcd election, and
only the coordinator publishes new rings. A node that can't renew its
lease for 75% of the lease TTL stops serving, wipes its data and rejoins
as a new member, so it can't be serving after its keys have been handed
to another node.

Membership changes follow two patterns. A death switches the ring at
once, which makes the old replica the primary, and then copies a new
second replica in the background. A join or leave copies data to the new
owners first and switches the ring when every copy is done.

## Decision log

### Phase 0

- Module path: `github.com/rakeshlakshmanan/ringcache` (GitHub account
  confirmed by the owner).
- Toolchain: Go 1.27.4. Developer tools are pinned as Go tool
  dependencies in `go.mod` and run with `go tool`: staticcheck (v0.8.2),
  buf (v1.74.0; v1.75.0 required a pre-release protobuf, so the resolver
  settled one version back), protoc-gen-go (v1.36.12) and
  protoc-gen-go-grpc (v1.6.2).
- `clock.Fake` starts at a fixed date (2026-01-01 UTC) so tests are
  reproducible.
- An empty stray `s.go` at the repository root predated phase 0 and was
  removed; it would have broken `go build ./...`.
- Packages are created in the phase that first needs them; phase 0 adds
  only `internal/clock` and `internal/config`.

### Phase 1

- Dependency added: `github.com/cespare/xxhash/v2` v2.3.0, named by the
  plan. `keyhash.Sum` is the only hash function; the ring, the store and
  the migration range filter all go through it.
- `Clock.NowAfter` takes the mutex once and runs the update and the
  issue under it, rather than calling the exported `Update` then `Now`
  as the plan words it. The result is the same, but holding the lock
  across both steps stops a concurrent caller slipping a version in
  between and leaving the result at or below the argument. The store's
  `SetLocal` relies on that, so the guarantee has to be atomic.
- The plan's `TestHLC_UpdateRemoteBehind` is described as "a remote
  version behind physical time doesn't change what `Now` returns next".
  Taken literally that can't hold: the plan's own `Update` rules say the
  counter is raised in every case, so `Update` always advances the
  clock. The test instead asserts the property that matters and is
  provable: a behind remote makes `Update` degenerate into an ordinary
  local tick, contributing neither its counter nor its wall value. The
  assertion compares against a control clock that only ticks locally. A
  mutation check confirmed it has teeth: making the behind-remote branch
  adopt `max(last, remote)` counters fails the test.
- The counter-overflow carry is unreachable through local calls (no node
  issues 2^32 versions in one millisecond), so
  `TestHLC_CounterOverflowCarries` drives it through a peer version at
  the top of the counter range. Added beyond the plan's test table,
  along with golden-value tests for `keyhash`, which pin the hash
  function: changing it is a wire-visible change, since nodes and
  clients must compute identical rings.
- The clock-skew warning uses `slog.Default()` with `node_id` as an
  attribute, because the plan fixes `New(nodeID string, c clock.Clock)`
  and takes no logger. Its once-a-minute rate limit reads the injected
  `clock.Clock`, so a fake clock makes the suppression deterministic.

### Phase 2

- `Entry.Size` takes the key, as `Size(key string) int64`, because an
  `Entry` doesn't hold its key and the plan's figure is key plus value
  plus 64. `KeyedEntry.Size()` is the no-argument form.
- `Apply` computes `Hash` itself rather than trusting the sender. The
  proto `Entry` carries no hash field, so `Hash` is local bookkeeping;
  recomputing it also means a sender can't steer an entry into the wrong
  shard. `Apply` clones the value for the same reason `SetLocal` does:
  entries arrive in gRPC buffers that may be reused.
- `Apply` calls `hlc.Update` while holding the shard write lock, so the
  lock order is shard then HLC. `writeLocal` takes them in the same
  order through `NowAfter`, so the two paths can't deadlock.
- An entry exactly at its `ExpiresAt` counts as expired, matching the
  plan's "at or before now" for reads. `Entry.Expired(nowMs)` is the one
  place that rule lives, shared by `Get`, `Apply`, the sweep and scans.
- `Wipe` subtracts what each shard actually held instead of zeroing the
  totals, so a write landing in an already-wiped shard can't leave the
  counters wrong. `SweepOnce` returns how many entries it removed, which
  the plan leaves unspecified; tests and the later metrics both want it.
- `Scan` batches per shard and never spans two shards, since it lists
  one shard's keys and drains them before moving on. With 256 shards and
  a 100-entry cap, 5,001 matching keys came back as 256 batches of about
  20 rather than 51 full ones. Batches are therefore smaller than the
  cap at small data sizes, which costs a few extra RPCs during migration
  and keeps each lock hold short.
- Unexpired tombstones travel with a scan; expired entries, tombstones
  included, are skipped. Sending the tombstone is what stops a delete
  being undone by the snapshot it raced.
- `Entry` is immutable once stored, so `Get` and `Scan` hand back copies
  of the struct that share `Value` with the store without a copy. A
  write replaces the whole entry rather than editing it, so a shared
  slice stays valid after the key is overwritten. Callers must not
  modify `Value`.
- Test additions beyond the plan's table: `assertConsistent` walks every
  shard and checks the atomic totals, each shard's byte count and the
  expiry index against the actual contents. A mutation that dropped the
  `delta -= old.Size(key)` subtraction on overwrite was caught by four
  tests, so the accounting is genuinely covered. Also added: value-copy
  on write, oversize scan entries, tombstone and expiry handling in
  scans, `Scan` error and cancellation propagation, `DeleteMatching`,
  and distinct versions under concurrent writes to one key.

#### Phase 2 benchmarks

Apple M4, 10 cores, darwin/arm64, Go 1.27.4, `-benchtime 3s`:

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkStore_GetParallel` | 18.94 | 0 | 0 |
| `BenchmarkStore_SetParallel` | 195.9 | 112 | 1 |
| `BenchmarkStore_ApplyParallel` | 166.7 | 112 | 1 |

No thresholds attach to these; they are recorded as the plan asks.

Reads allocate nothing and cost about 19 ns. Both write paths cost about
170 to 200 ns and allocate once, for the value copy. The gap is the HLC:
it is one mutex for the whole node, so writes serialise through it
however many shards they land in. That caps a node at roughly 5 million
writes per second, about three orders of magnitude above the 4,000
writes per second the 20,000 ops/s target implies at a 20% write share,
so it is recorded as a known ceiling rather than a problem to fix.

`BenchmarkStore_ApplyParallel` reports a `%stored` metric, which earned
its place immediately: the first version of the benchmark built
synthetic versions from a fixed `1 << 40` wall value, below the real
wall-clock versions of the preloaded entries, so every apply was
rejected as stale and the benchmark timed the reject path at 30 ns while
appearing to succeed. The metric now reads 99.96%, the remainder being
genuine races where a higher sequence reached the same key first.

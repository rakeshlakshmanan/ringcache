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

### Phase 3

- Three buf STANDARD lint rules are excepted in `proto/buf.yaml`, with
  the reason recorded there. The specification fixes these names in
  section 12 and later phases refer to them, so the wire contract wins
  over buf's naming conventions:
  - `SERVICE_SUFFIX`: the spec declares `service Cache` and
    `service Peer`, not `CacheService` and `PeerService`.
  - `RPC_RESPONSE_STANDARD_NAME`: `GetRing` returns the shared
    `RingView` rather than a `GetRingResponse` wrapper; `Replicate`
    returns `ReplicateAck` and `StreamRange` returns `EntryBatch`, each
    named for what it carries.
  - `RPC_REQUEST_STANDARD_NAME`: `StreamRange` takes `RangeRequest`.

  Every other STANDARD rule applies, including the enum prefix and
  zero-value rules that `AckStatus` already satisfies.
- The protos live at `proto/ringcache/v1/`, which buf's
  `PACKAGE_DIRECTORY_MATCH` requires for package `ringcache.v1`, but the
  plan puts the generated Go in `gen/ringcachev1/`. Passing
  `module=github.com/rakeshlakshmanan/ringcache/gen` to both plugins
  strips that prefix from each file's `go_package`, so generation lands
  in `gen/ringcachev1/` instead of a tree mirroring the import path.
  `paths=source_relative` would have produced `gen/ringcache/v1/`.
- `buf.gen.yaml` sits in `proto/` as the plan says, so `make proto`
  passes it explicitly with `--template`; buf otherwise looks for it in
  the working directory. Both plugins are invoked as
  `["go", "tool", "protoc-gen-go"]`, so the versions pinned in `go.mod`
  are the ones that run and nothing has to be installed separately.
- `make proto` lints before it generates, so a malformed proto fails
  before it can produce Go.
- Dependencies promoted to direct requirements by `go mod tidy`:
  `google.golang.org/grpc` and `google.golang.org/protobuf`, both
  already present as tool dependencies.
- The CI job now regenerates and runs `git diff --exit-code`. The check
  was verified by appending a message to `types.proto` without
  regenerating: the diff reported both the proto and the stale
  `types.pb.go`, so drift fails the build rather than passing quietly.
  Regeneration is byte-identical, which that check depends on.

### Phase 4

- Dependencies added, both named by the plan:
  `github.com/prometheus/client_golang` v1.25.0 and
  `golang.org/x/sync` (errgroup).
- `Config` holds only what varies between nodes. The tuning constants in
  `defaults.go` stay compile-time, so every node in a cluster agrees on
  them without having to exchange them.
- `--advertise` defaults to `<node-id>:<port from --listen>` rather than
  to `--listen` itself, because the default listen address is a wildcard
  that no peer can dial. Under docker-compose the node ID is the
  container name, which resolves, so the default works unchanged there.
- `--listen` rejects port 0. An ephemeral port cannot be advertised: a
  node has to publish its address before its listener exists. Tests that
  want an arbitrary port pass a listener through `node.Options` instead,
  which is the same door the phase 6 test cluster uses for bufconn.
- The plan's `TestConfig_Invalid` asks for "bad durations and unknown
  flags". This phase defines no duration flag — timing lives in
  `defaults.go` — so the test covers the invalid values that do exist:
  an unknown flag, a stray argument, a bad log level, an empty or
  slash-bearing node ID, and each address flag without a port.
- Metrics are `ringcache_requests_total{method,code}`,
  `ringcache_request_duration_seconds{method}`,
  `ringcache_handler_panics_total` and the `ringcache_store_keys` and
  `ringcache_store_bytes` gauges. Every one carries a `node` label, and
  they live in a per-node registry, so several nodes in one process
  report separate numbers. The method label is shortened to
  `Cache/Set`, keeping the service name without the proto package.
- The store gauges are `GaugeFunc`s reading `Store.Len` and
  `Store.Bytes` at scrape time, so the store holds no reference to the
  metrics and the numbers cannot go stale.
- The latency histogram uses `ExponentialBucketsRange(50e-6, 1, 18)`:
  the plan's 50 µs to 1 s, at about four buckets per decade.
- `versionToProto` lives in `internal/server` as an unexported helper
  rather than in a conversion package of its own. Phase 8 is the first
  phase that needs the reverse direction, so the pair moves somewhere
  shared then rather than being invented ahead of a second caller.
- `Ownership` errors are plain Go errors — `WrongOwnerError`,
  `ErrNoRing`, `ErrFenced` — mapped to statuses in one place,
  `toStatus`. An implementation never builds a gRPC status itself, so
  phase 6 can replace `allOwner` without touching the error contract.
- `AllOwner` reports epoch 0, which means "no ring published". A client
  treats any epoch it sees as at least as new as its own, so a lone node
  answering at epoch 0 never pushes a client into a refresh loop.
- The `Gate` hook is wired now and left nil. It rejects a request before
  the handler runs, whatever key the request names, which is what phase
  10 needs for fencing; `TestCache_Gate` exercises it with a stub.
- `Node.Start` runs the sweeper and both servers under an errgroup, with
  a fourth goroutine that waits on the group context and shuts the
  servers down. Shutdown has to be a goroutine rather than a deferred
  call: a deferred one would not run until `Wait` returned, and `Wait`
  cannot return while the servers are still serving.
- `GracefulStop` has no timeout of its own, so a wedged handler would
  hang the process. It runs in a goroutine with `Stop` as the 5 s
  fallback the plan asks for.
- Listeners are opened in `New`, not `Start`, so a port already in use
  fails before the process claims to be running and the binary exits
  non-zero. `TestNode_ListenConflict` covers it.
- `/healthz` is gated on an `atomic.Bool` that `Start` sets and
  `shutdown` clears. Phase 10 clears it while fenced, so an operator and
  a load balancer see the same state the gate enforces.
- `--healthcheck` is handled before flag parsing, by scanning argv, so
  the probe can take the same flags as the node it probes and read the
  right metrics port. The final image has no shell, so running the
  binary again is the only way to probe it. A wildcard metrics address
  is probed over the loopback.
- The Dockerfile stamps `main.version` with the commit through a
  `COMMIT` build argument and builds every binary under `cmd/` in one
  step, so later phases' binaries need no change to it. The final image
  is `distroless/static-debian12:nonroot`, 25.3 MB.
- A mutation check confirmed the size-limit test has teeth: changing the
  key check from `>` to `>=` made `TestCache_SizeLimits` fail on the
  1 KiB key it is supposed to accept.
- Tests beyond the plan's table: `TestCache_WrongOwner` (the epoch is
  readable from the status detail, which is how a client learns its ring
  is stale), `TestCache_NoRing`, `TestCache_Gate`,
  `TestCache_PanicRecovered`, `TestCache_TTLOutOfRange`,
  `TestCache_GetRing`, and the `internal/node` tests for `/healthz`,
  `/metrics`, shutdown on cancellation, two nodes in one process and the
  listen conflict.
- On macOS the default `--listen :7000` collides with the AirPlay
  Receiver, which also listens there. The default is left as the plan
  fixes it, since the clusters run in containers; a local run needs an
  explicit `--listen`.

#### Phase 4 manual verification

A node was run and driven with grpcurl: reflection listed
`ringcache.v1.Cache`, Set returned a version, Get read it back, TTL
reported 4,988 ms left on a 5 s key, Delete made the following Get a
miss, an empty key gave INVALID_ARGUMENT, and `/metrics` carried the
`ringcache_` series. SIGTERM shut the node down cleanly with exit 0.

The image was built and run: the container reached `healthy` through the
`--healthcheck` probe, served RPCs on its published ports, ran as
`nonroot`, and exited 0 on `docker stop`.

### Phase 5

- No dependencies added. The package is pure: no I/O, no clock, no
  goroutines, so a node, a client and the coordinator all reach the
  same conclusions from the same `View`.
- A `Ring` is immutable once built. Publishing a new ring means building
  another and swapping the pointer, so readers need no lock.
- `New` rejects a member without an address. A member that cannot be
  dialled is useless in a preference list, and every real source of
  members — etcd registrations in phase 9 — has one. `RF` above the
  member count is allowed: a shorter preference list is the normal state
  of a cluster that has just lost a node.
- `New` drops a token whose position equals the previous one's. A 64-bit
  collision is vanishingly unlikely, but the second token's arc would be
  `(p, p]`, which the half-open rule reads as the whole ring, so the
  ring's invariants should not rest on it not happening.
- `PreferenceIDs` sits beside `PreferenceList` because the arcs and
  `Diff` compare lists far more often than they read addresses, and
  `prefIndexes` takes a reusable buffer so neither allocates per key.
- `Arcs` and `Diff` merge through one generic helper, `mergeAdjacent`,
  which also closes a run across the top of the ring.
- `Diff` accepts a nil old ring, which is the first published ring's
  case in phase 9: the whole ring is then new.
- `Diff` compares preference lists, not members, so a new epoch with the
  same members produces no changes. The ring is who holds what, not the
  number it was published under.

#### Phase 5 measurements

All with node-1 to node-5, 256 tokens each, RF 2, keys named `key-<i>`,
as the plan fixes them.

| Measurement | Result | Target |
|---|---|---|
| Balance, largest share of 1,000,000 keys | 1.0533x the mean (node-5) | at most 1.15x (NFR-08) |
| Keys changing primary, 4 nodes to 5 | 21.07% | 17% to 23% (FR-02) |
| Modulo baseline, 4 nodes to 5 | 80.00% | 79% to 81% |
| Removing node-3 from 5 | all 39,477 affected keys promoted to their replica | every one (phase 10 failover) |

`go run ./cmd/ringctl sim-move` prints:

```
1000000 keys, 4 nodes -> 5 nodes, 256 tokens per node

  consistent hashing    21.07%  (210656 keys change primary)
  modulo hashing        80.00%  (800022 keys change primary)

  consistent hashing moves 3.8x less data
```

Benchmarks (Apple M4, 10 cores, darwin/arm64, `-benchtime 2s`):

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkRing_Lookup` | 21.91 | 0 | 0 |
| `BenchmarkRing_PreferenceList` | 64.13 | 88 | 3 |
| `BenchmarkDiff` (4 to 5 nodes) | 138,491 | 223,281 | 2,567 |

A lookup costs about the same as a store read, so routing adds nothing
measurable to a request. `Diff` runs once per published ring, not per
request, so its 0.14 ms is irrelevant to the latency targets.

#### Phase 5 mutation checks

Three mutations were run against the tests; the first two were caught,
the third exposed a real gap that has since been closed.

1. Making `Lookup` land on the token after an exact hit, rather than on
   it, failed `TestRing_LookupWrap`,
   `TestRing_ArcsMatchPreferenceLists` and
   `TestDiff_MatchesPreferenceLists`.
2. Removing the merge across the top of the ring passed everything at
   first. The production shape never reaches that branch: it fires only
   when the lowest and highest tokens belong to one member, which five
   members with 256 tokens do not do. A sweep found inputs that do —
   two members with 23 tokens each puts node-2 at both ends — and
   `TestRing_ArcsMergeAcrossTopOfRing` now pins that case.
   `TestMergeAdjacent` covers the helper directly as well. With both,
   the mutation fails.
3. Removing the touching check from the merge also passed at first.
   `Arcs` hands in a slice that tiles the ring, so the check is always
   true there; `Diff` hands in only the stretches that differ, where two
   neighbours can have a gap. A sweep over member counts 2 to 6 and
   token counts 1 to 128, plus the production shapes, found no ring pair
   where two *neighbouring* changes share a payload across a gap, so the
   check is defensive rather than reachable from a ring. It is kept —
   merging across a gap would claim a stretch whose ownership did not
   change — and `TestMergeAdjacent` now covers it directly, which makes
   the mutation fail.

### Phase 6

- Dependency added: `golang.org/x/sync` (already present from phase 4's
  errgroup; phase 6 uses its `singleflight` as well), named by the plan.
- `--static-members` is kept in `Config` as the raw flag string.
  Parsing it needs `ring`, and `config` sits below that: making the
  lowest package depend on the ring would invert the layering for one
  field. `membership.ParseStatic` reads it and `node.New` reports what
  it says, so a bad list still fails before the node serves.
- An empty `--static-members` keeps phase 4's behaviour: the node owns
  the whole keyspace through `AllOwner`. That is what a single node
  with no cluster around it is, and it keeps the phase 4 manual
  commands working unchanged.
- `node.New` fails when the node's own ID is missing from its member
  list. It would otherwise start happily and answer every request with
  UNAVAILABLE, which is a confusing way to learn of a typo.
- A node missing from its current ring reports `ErrNotMember`, which
  maps to UNAVAILABLE, not a wrong-owner error. A wrong-owner error
  tells the client to refresh and route again, and the refreshed ring
  would send it straight back to the same node.
- `Source.Subscribe` hands a new watcher the current ring at once
  rather than only future ones, so a component that subscribes after
  the first ring was published does not wait for a change to learn
  there is one. The channel holds a single value, newest wins: only the
  newest ring decides anything.
- The client's ring lives in an `atomic.Pointer`, so routing takes no
  lock, and `install` uses `CompareAndSwap` and ignores an epoch at or
  below the current one. Two refreshes can land out of order, and the
  slower one must not undo the newer.
- Ring refreshes are deduplicated with `singleflight`. A hundred
  requests meeting a stale ring at once need one `GetRing` between
  them, not a hundred.
- A refresh triggered by a response's epoch runs in the background. The
  call that noticed already has its answer; making it wait would turn
  one node's slow refresh into every client's latency. The same holds
  on the node side, where `WithEpochRefresher` guards with an atomic so
  a flood of clients at a newer epoch starts one refresh, not thousands.
- The retry policy distinguishes reads from writes. A read that meets
  UNAVAILABLE or a timeout goes to the next member of the preference
  list, which holds a copy. A write has nowhere else to go, because
  only the primary may accept it; phase 10 adds the wait for the epoch
  that names a new one. A wrong-owner error is only worth retrying when
  the node's epoch is actually newer than the client's, or the client
  would refresh to the same ring and route to the same node.
- After a refresh the list of already-tried nodes is cleared. The
  refreshed ring may route back to a node that objected, and that is
  correct: it objected because the client was routing with an old ring.
- Connections are keyed by member ID and kept for 10 s after a member
  leaves the ring, since a ring is republished for reasons that have
  nothing to do with that member and reopening costs a handshake. A
  member whose address changed is a restarted node, so its old
  connection is closed at once rather than lingering.
- `withInitialRing` is unexported and reached through `export_test.go`.
  Only a test wants to start a client with a deliberately stale ring,
  so the option stays out of the public API.
- `CallInfo` records attempts, the member that answered, the final code
  and whether the ring was refreshed. A cache client hides routing and
  retries, and phase 7's checker has to attribute what it measures.
- The test cluster opens every listener before building the member
  list, because the list has to name the addresses the nodes will serve
  on and only the kernel knows which ports those are. `Stop` drains as
  SIGTERM does; `Kill` closes the server outright, which is what a
  crash looks like to everyone else.
- `TestStatic_FiveNodesReadBack` drives its 100,000 keys through 32
  goroutines sharing one client. Sequentially the 200,000 round trips
  took 16.2 s, close to the plan's 20 s ceiling and worse under
  `-race`; concurrently it takes 2.7 s, and sharing one client is how a
  program should use it anyway.

#### Phase 6 measurements

100,000 keys written through the client across five nodes, each key
checked in every node's store:

| Node | Keys held |
|---|---|
| node-1 | 18,969 |
| node-2 | 19,994 |
| node-3 | 19,755 |
| node-4 | 20,270 |
| node-5 | 21,012 |

Every key was held by its primary and by no one else, which is what
FR-01 asks for before replication exists.

#### Phase 6 manual verification

Three nodes were started with one `--static-members` list and driven
with `ringctl`. `ringctl ring` reported epoch 1 with three members.
Five keys were written through one node's address and read back through
a different one; each was served by its own primary in one attempt, and
the per-node `ringcache_store_keys` gauges showed 3, 2 and 0, so the
client routed rather than any node forwarding. A missing key reported
`(not found)` rather than an error.

#### Phase 6 mutation checks

All three were caught.

1. Letting `CheckWrite` accept any member of the preference list, not
   only the primary, failed `TestOwnership_Table/replica` and
   `TestOwnership_EveryKeyHasOneWriter`.
2. Letting a write retry elsewhere when the primary is down failed
   `TestClient_WriteDoesNotRetryElsewhere`.
3. Dropping the refresh-and-retry on a wrong-owner error failed
   `TestClient_StaleRingRefresh`.

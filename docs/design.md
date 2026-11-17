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

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

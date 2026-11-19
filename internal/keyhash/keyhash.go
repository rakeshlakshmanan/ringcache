// Package keyhash holds the one hash function ringcache uses for cache
// keys and ring tokens.
package keyhash

import "github.com/cespare/xxhash/v2"

// Sum returns the 64-bit xxhash of s.
//
// Every hash of a key or a ring token goes through this function, so the
// store, the ring and the migration range filter all agree on where a
// key sits. xxhash is stable across machines and Go versions, which lets
// nodes and clients compute the same ring from the same member list.
func Sum(s string) uint64 { return xxhash.Sum64String(s) }

package ring

// ModuloOwner returns the node index a hash maps to under modulo
// hashing, the obvious scheme consistent hashing replaces.
//
// It exists only as the baseline the ring is measured against: adding
// one node to n changes the owner of about (n-1)/n of all keys, because
// the divisor changes for every key at once. Nothing routes with it.
func ModuloOwner(h uint64, n int) int {
	if n <= 0 {
		return 0
	}
	return int(h % uint64(n))
}

package main

import (
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

func init() {
	register(command{
		name:    "sim-move",
		summary: "how many keys change primary when the cluster grows",
		group:   groupOffline,
		run:     simMove,
	})
}

// simMove counts the keys whose primary changes when the cluster grows
// from one size to another, under consistent hashing and under the
// modulo hashing it replaces.
func simMove(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("sim-move", flag.ContinueOnError)
	fs.SetOutput(out)

	keys := fs.Int("keys", 1_000_000, "how many keys to simulate, named key-0 upwards")
	from := fs.Int("from", 4, "cluster size before the change")
	to := fs.Int("to", 5, "cluster size after the change")
	vnodes := fs.Int("vnodes", config.VNodes, "tokens each node places on the ring")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if *keys <= 0 {
		return fmt.Errorf("--keys must be positive")
	}
	if *from <= 0 || *to <= 0 {
		return fmt.Errorf("--from and --to must be positive")
	}

	before, err := buildRing(*from, *vnodes)
	if err != nil {
		return err
	}
	after, err := buildRing(*to, *vnodes)
	if err != nil {
		return err
	}

	ringMoved, moduloMoved := 0, 0
	for i := range *keys {
		h := keyhash.Sum("key-" + strconv.Itoa(i))
		if before.Lookup(h).ID != after.Lookup(h).ID {
			ringMoved++
		}
		if ring.ModuloOwner(h, *from) != ring.ModuloOwner(h, *to) {
			moduloMoved++
		}
	}

	share := func(n int) float64 { return float64(n) / float64(*keys) * 100 }

	fmt.Fprintf(out, "%d keys, %d nodes -> %d nodes, %d tokens per node\n\n",
		*keys, *from, *to, *vnodes)
	fmt.Fprintf(out, "  consistent hashing  %7.2f%%  (%d keys change primary)\n",
		share(ringMoved), ringMoved)
	fmt.Fprintf(out, "  modulo hashing      %7.2f%%  (%d keys change primary)\n",
		share(moduloMoved), moduloMoved)
	if ringMoved > 0 {
		fmt.Fprintf(out, "\n  consistent hashing moves %.1fx less data\n",
			float64(moduloMoved)/float64(ringMoved))
	}
	return nil
}

// buildRing makes a ring of n nodes named node-1 upwards, which is how
// the project's clusters are named throughout.
func buildRing(n, vnodes int) (*ring.Ring, error) {
	members := make([]ring.Member, n)
	for i := range members {
		id := "node-" + strconv.Itoa(i+1)
		members[i] = ring.Member{ID: id, Addr: id + ":7000"}
	}
	return ring.New(ring.View{
		Epoch:   1,
		VNodes:  vnodes,
		RF:      config.ReplicationFactor,
		Members: members,
	})
}

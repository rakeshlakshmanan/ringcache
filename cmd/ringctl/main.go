// Command ringctl inspects and simulates a ringcache cluster.
//
// Later phases add subcommands that talk to a running cluster. The
// first one is offline: it measures what consistent hashing buys over
// the modulo hashing it replaces.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/keyhash"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "ringctl:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		usage(out)
		return fmt.Errorf("no subcommand")
	}

	switch args[0] {
	case "sim-move":
		return simMove(args[1:], out)
	case "get":
		return cmdGet(args[1:], out)
	case "set":
		return cmdSet(args[1:], out)
	case "ring":
		return cmdRing(args[1:], out)
	case "help", "-h", "--help":
		usage(out)
		return nil
	default:
		usage(out)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func usage(out io.Writer) {
	fmt.Fprint(out, `ringctl inspects and simulates a ringcache cluster.

Subcommands:
  get <key>           read a key, printing the member that answered
  set <key> <value>   write a key, printing the member that answered
  ring                print the cluster's current ring
  sim-move            how many keys change primary when the cluster grows,
                      under consistent hashing and under modulo hashing

The cluster commands take --nodes, a comma-separated list of node
addresses to start from. Any node will do; the ring comes from it.
`)
}

// dialFlags adds the flags every cluster subcommand takes.
func dialFlags(fs *flag.FlagSet) (nodes *string, timeout *time.Duration) {
	nodes = fs.String("nodes", "127.0.0.1:7000", "comma-separated node addresses to start from")
	timeout = fs.Duration("timeout", 5*time.Second, "overall deadline for the command")
	return nodes, timeout
}

// connect builds a client from the --nodes addresses. The background
// refresh is off: a one-shot command has no use for it.
func connect(ctx context.Context, nodes string) (*client.Client, error) {
	var seeds []string
	for _, addr := range strings.Split(nodes, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			seeds = append(seeds, addr)
		}
	}
	if len(seeds) == 0 {
		return nil, fmt.Errorf("--nodes is empty")
	}
	return client.New(ctx, seeds, client.WithRefreshInterval(0))
}

func cmdGet(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	fs.SetOutput(out)
	nodes, timeout := dialFlags(fs)

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: ringctl get <key>")
	}
	key := fs.Arg(0)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	c, err := connect(ctx, *nodes)
	if err != nil {
		return err
	}
	defer c.Close()

	var info client.CallInfo
	value, found, err := c.Get(client.WithCallInfo(ctx, &info), key)
	if err != nil {
		return fmt.Errorf("get %q: %w", key, err)
	}
	if !found {
		fmt.Fprintf(out, "(not found)\n")
	} else {
		fmt.Fprintf(out, "%s\n", value)
	}
	printServedBy(out, &info)
	return nil
}

func cmdSet(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("set", flag.ContinueOnError)
	fs.SetOutput(out)
	nodes, timeout := dialFlags(fs)
	ttl := fs.Duration("ttl", 0, "time to live; zero means no expiry")

	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return fmt.Errorf("usage: ringctl set <key> <value>")
	}
	key, value := fs.Arg(0), fs.Arg(1)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	c, err := connect(ctx, *nodes)
	if err != nil {
		return err
	}
	defer c.Close()

	var info client.CallInfo
	if err := c.Set(client.WithCallInfo(ctx, &info), key, []byte(value), *ttl); err != nil {
		return fmt.Errorf("set %q: %w", key, err)
	}
	fmt.Fprintf(out, "OK\n")
	printServedBy(out, &info)
	return nil
}

func cmdRing(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("ring", flag.ContinueOnError)
	fs.SetOutput(out)
	nodes, timeout := dialFlags(fs)

	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	c, err := connect(ctx, *nodes)
	if err != nil {
		return err
	}
	defer c.Close()

	r := c.Ring()
	if r == nil {
		return fmt.Errorf("no ring")
	}
	v := r.View()
	fmt.Fprintf(out, "epoch %d, %d members, %d tokens each, rf %d\n\n", v.Epoch, len(v.Members), v.VNodes, v.RF)
	for _, m := range r.Members() {
		fmt.Fprintf(out, "  %-12s %s\n", m.ID, m.Addr)
	}
	return nil
}

// printServedBy reports which node answered and what it cost, which is
// the point of running these commands by hand.
func printServedBy(out io.Writer, info *client.CallInfo) {
	fmt.Fprintf(out, "served by %s (epoch %d, %d attempt",
		info.ServedBy(), info.Epoch(), info.Attempts())
	if info.Attempts() != 1 {
		fmt.Fprint(out, "s")
	}
	if info.Refreshed() {
		fmt.Fprint(out, ", refreshed the ring")
	}
	fmt.Fprintln(out, ")")
}

// simMove counts the keys whose primary changes when the cluster grows
// from one size to another, under both schemes.
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

	fmt.Fprintf(out, "%d keys, %d nodes -> %d nodes, %d tokens per node\n\n", *keys, *from, *to, *vnodes)
	fmt.Fprintf(out, "  consistent hashing  %7.2f%%  (%d keys change primary)\n", share(ringMoved), ringMoved)
	fmt.Fprintf(out, "  modulo hashing      %7.2f%%  (%d keys change primary)\n", share(moduloMoved), moduloMoved)
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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
)

func init() {
	register(command{
		name:    "ring",
		summary: "print the cluster's current ring",
		group:   groupCluster,
		run:     cmdRing,
	})
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
	fmt.Fprintf(out, "epoch %d, %d members, %d tokens each, rf %d\n\n",
		v.Epoch, len(v.Members), v.VNodes, v.RF)
	for _, m := range r.Members() {
		fmt.Fprintf(out, "  %-12s %s\n", m.ID, m.Addr)
	}
	return nil
}

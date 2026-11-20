package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

// dialFlags adds the flags every cluster subcommand takes. Keeping
// them here means a new subcommand gets the same interface as the
// others for free.
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

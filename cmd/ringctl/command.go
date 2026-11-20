package main

import (
	"fmt"
	"io"
	"slices"
)

// group sorts subcommands in the usage text.
type group int

const (
	// groupCluster talks to a running cluster.
	groupCluster group = iota
	// groupOffline needs no cluster.
	groupOffline
)

// command is one ringctl subcommand.
//
// Each subcommand lives in its own file and registers itself, so the
// phases that add one — membership and tasks and changes and the
// benchmark CSV — never edit a shared switch statement or this file.
type command struct {
	// name is the word typed after ringctl.
	name string
	// summary is the one-line description in the usage text.
	summary string
	// group decides which heading it appears under.
	group group
	// run executes it. args excludes the subcommand name itself.
	run func(args []string, out io.Writer) error
}

// registry holds every registered subcommand, keyed by name.
var registry = map[string]command{}

// register adds a subcommand. It panics on a duplicate name, which can
// only be a mistake made while adding one.
func register(c command) {
	if c.name == "" || c.run == nil {
		panic("ringctl: a subcommand needs a name and a run function")
	}
	if _, dup := registry[c.name]; dup {
		panic("ringctl: duplicate subcommand " + c.name)
	}
	registry[c.name] = c
}

// commandNames returns the registered names in usage order: cluster
// commands first, then offline ones, each alphabetically.
func commandNames() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	slices.SortFunc(names, func(a, b string) int {
		if g := int(registry[a].group) - int(registry[b].group); g != 0 {
			return g
		}
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})
	return names
}

// usage prints the subcommands, built from the registry so a new one
// appears without anybody remembering to document it here.
func usage(out io.Writer) {
	fmt.Fprint(out, "ringctl inspects and simulates a ringcache cluster.\n")

	lastGroup := group(-1)
	for _, name := range commandNames() {
		c := registry[name]
		if c.group != lastGroup {
			switch c.group {
			case groupCluster:
				fmt.Fprint(out, "\nAgainst a running cluster:\n")
			case groupOffline:
				fmt.Fprint(out, "\nOffline:\n")
			}
			lastGroup = c.group
		}
		fmt.Fprintf(out, "  %-12s %s\n", c.name, c.summary)
	}

	fmt.Fprint(out, `
The cluster commands take --nodes, a comma-separated list of node
addresses to start from. Any node will do; the ring comes from it.
`)
}

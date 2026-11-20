// Command ringctl inspects and simulates a ringcache cluster.
//
// Each subcommand lives in its own file and registers itself in an
// init function; main only dispatches. See command.go.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
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
	case "help", "-h", "--help":
		usage(out)
		return nil
	}

	c, ok := registry[args[0]]
	if !ok {
		usage(out)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
	return c.run(args[1:], out)
}

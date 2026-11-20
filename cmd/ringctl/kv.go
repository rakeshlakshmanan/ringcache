package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/rakeshlakshmanan/ringcache/pkg/client"
)

func init() {
	register(command{
		name:    "get",
		summary: "read a key, printing the member that answered",
		group:   groupCluster,
		run:     cmdGet,
	})
	register(command{
		name:    "set",
		summary: "write a key, printing the member that answered",
		group:   groupCluster,
		run:     cmdSet,
	})
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

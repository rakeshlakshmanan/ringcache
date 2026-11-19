package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
)

// Config is one node's runtime configuration. Everything in it comes
// from flags; the tuning constants in defaults.go are compile-time and
// deliberately not configurable, so every node in a cluster agrees on
// them without having to exchange them.
type Config struct {
	// NodeID identifies this node in the ring and in its logs. It
	// defaults to the hostname, which is the container name under
	// docker-compose.
	NodeID string
	// Listen is the gRPC listen address.
	Listen string
	// Advertise is the address other nodes and clients dial. It differs
	// from Listen whenever the node listens on a wildcard, which it does
	// by default.
	Advertise string
	// MetricsListen is the HTTP address serving /metrics and /healthz.
	MetricsListen string
	// LogLevel is the minimum slog level: debug, info, warn or error.
	LogLevel slog.Level
	// Reflection registers the gRPC reflection service, so grpcurl can
	// talk to the node without a copy of the protos.
	Reflection bool
	// StaticMembers is the fixed member list, as
	// "node-1=host:7001,node-2=host:7002". It is used when no etcd
	// endpoint is given. Empty means the node owns the whole keyspace
	// on its own, which is what a single node without a cluster is.
	//
	// It is kept as the raw flag value: parsing it needs the ring
	// package, and config sits below that. membership.ParseStatic reads
	// it, and node.New reports what it says.
	StaticMembers string
}

// Load parses args, which excludes the program name, into a Config. It
// returns flag.ErrHelp when the caller asked for usage.
func Load(args []string) (Config, error) {
	var c Config

	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	// Errors are returned rather than printed, so a test can assert on
	// them and main can decide how to report them.
	fs.SetOutput(io.Discard)

	var logLevel string
	fs.StringVar(&c.NodeID, "node-id", defaultNodeID(), "this node's identity in the ring (default: hostname)")
	fs.StringVar(&c.Listen, "listen", ":7000", "gRPC listen address")
	fs.StringVar(&c.Advertise, "advertise", "", "address other nodes and clients dial (default: <node-id>:<listen port>)")
	fs.StringVar(&c.MetricsListen, "metrics-listen", ":9100", "HTTP address serving /metrics and /healthz")
	fs.StringVar(&logLevel, "log-level", "info", "minimum log level: debug, info, warn or error")
	fs.BoolVar(&c.Reflection, "reflection", true, "register the gRPC reflection service")
	fs.StringVar(&c.StaticMembers, "static-members", "",
		"fixed member list as id=host:port,...; empty means this node owns the whole keyspace")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if rest := fs.Args(); len(rest) > 0 {
		return Config{}, fmt.Errorf("config: unexpected argument %q", rest[0])
	}

	lvl, err := parseLevel(logLevel)
	if err != nil {
		return Config{}, err
	}
	c.LogLevel = lvl

	if err := c.finish(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// finish fills in what defaults to another field's value and validates
// the result.
func (c *Config) finish() error {
	if c.NodeID == "" {
		return errors.New("config: --node-id must not be empty")
	}
	// The node ID goes into log fields, etcd keys and the ring's member
	// list, where a space or a slash would be ambiguous.
	if strings.ContainsAny(c.NodeID, " \t\n/") {
		return fmt.Errorf("config: --node-id %q must not contain spaces or slashes", c.NodeID)
	}

	port, err := listenPort(c.Listen)
	if err != nil {
		return err
	}
	if c.Advertise == "" {
		// Listen is a wildcard by default, which no peer can dial, so the
		// advertised address pairs the node ID with the listen port. Under
		// docker-compose the node ID is the container name, which resolves.
		c.Advertise = net.JoinHostPort(c.NodeID, port)
	}
	if _, _, err := net.SplitHostPort(c.Advertise); err != nil {
		return fmt.Errorf("config: --advertise %q is not host:port: %w", c.Advertise, err)
	}
	if _, _, err := net.SplitHostPort(c.MetricsListen); err != nil {
		return fmt.Errorf("config: --metrics-listen %q is not host:port: %w", c.MetricsListen, err)
	}
	return nil
}

// listenPort returns the port from a listen address, rejecting port 0:
// an ephemeral port can't be advertised, since the node has to publish
// its address before the listener exists.
func listenPort(listen string) (string, error) {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("config: --listen %q is not host:port: %w", listen, err)
	}
	if port == "" || port == "0" {
		return "", fmt.Errorf("config: --listen %q needs a fixed port", listen)
	}
	return port, nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("config: --log-level %q is not debug, info, warn or error", s)
	}
}

func defaultNodeID() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "node"
	}
	return h
}

// Command node runs one ringcache cache node.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/node"
)

// version is stamped by the build with -ldflags; it identifies the
// commit a running node was built from.
var version = "dev"

// healthcheckTimeout bounds the --healthcheck probe. Docker's health
// check kills it anyway, but a probe that hangs would report nothing.
const healthcheckTimeout = 2 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintln(os.Stderr, "ringcache node:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// --healthcheck is handled before the flags are parsed, because the
	// probe runs in the shell-less final image where the only way to ask
	// the node how it is doing is to run the binary again.
	if i := slices.Index(args, "--healthcheck"); i >= 0 {
		return healthcheck(append(slices.Clone(args[:i]), args[i+1:]...))
	}

	cfg, err := config.Load(args)
	if err != nil {
		return err
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)
	log.Info("starting", "version", version, "node_id", cfg.NodeID)

	n, err := node.New(cfg, node.Options{Logger: log})
	if err != nil {
		return err
	}
	defer n.Close()

	// SIGINT and SIGTERM cancel the context, which stops the servers.
	// Phase 12 replaces this with a drain that hands the node's keys on
	// before it exits.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return n.Start(ctx)
}

// healthcheck makes a GET to the node's /healthz and reports the
// result as the process exit status, so a distroless image with no
// shell and no curl can still have a Docker health check.
func healthcheck(args []string) error {
	cfg, err := config.Load(args)
	if err != nil {
		return err
	}

	host, port, err := net.SplitHostPort(cfg.MetricsListen)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		// The probe runs inside the container, so a wildcard listen
		// address is reached over the loopback.
		host = "127.0.0.1"
	}

	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()

	url := "http://" + net.JoinHostPort(host, port) + "/healthz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("healthcheck: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthcheck: %s returned %s", url, resp.Status)
	}
	return nil
}

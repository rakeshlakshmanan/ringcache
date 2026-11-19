package config

import (
	"errors"
	"flag"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestConfig_Defaults(t *testing.T) {
	c, err := Load(nil)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname: %v", err)
	}
	if c.NodeID != host {
		t.Errorf("NodeID = %q, want the hostname %q", c.NodeID, host)
	}
	if c.Listen != ":7000" {
		t.Errorf("Listen = %q, want :7000", c.Listen)
	}
	if c.MetricsListen != ":9100" {
		t.Errorf("MetricsListen = %q, want :9100", c.MetricsListen)
	}
	if c.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want info", c.LogLevel)
	}
	if !c.Reflection {
		t.Error("Reflection = false, want true")
	}
	// Listen is a wildcard, which no peer can dial, so the advertised
	// address has to be derived rather than copied from it.
	want := host + ":7000"
	if c.Advertise != want {
		t.Errorf("Advertise = %q, want %q", c.Advertise, want)
	}
}

func TestConfig_Flags(t *testing.T) {
	c, err := Load([]string{
		"--node-id", "node-3",
		"--listen", "127.0.0.1:7003",
		"--metrics-listen", "127.0.0.1:9103",
		"--log-level", "DEBUG",
		"--reflection=false",
	})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.NodeID != "node-3" || c.Listen != "127.0.0.1:7003" || c.MetricsListen != "127.0.0.1:9103" {
		t.Errorf("addresses not taken from flags: %+v", c)
	}
	if c.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v, want debug (the level name is case-insensitive)", c.LogLevel)
	}
	if c.Reflection {
		t.Error("Reflection = true, want false")
	}
	if c.Advertise != "node-3:7003" {
		t.Errorf("Advertise = %q, want node-3:7003: the port comes from --listen", c.Advertise)
	}
}

func TestConfig_AdvertiseExplicit(t *testing.T) {
	c, err := Load([]string{"--node-id", "node-1", "--advertise", "10.0.0.4:7000"})
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Advertise != "10.0.0.4:7000" {
		t.Errorf("Advertise = %q, want the flag's value", c.Advertise)
	}
}

func TestConfig_Invalid(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"unknown flag", []string{"--nope"}, "not defined"},
		{"stray argument", []string{"serve"}, "unexpected argument"},
		{"bad log level", []string{"--log-level", "chatty"}, "--log-level"},
		{"empty node id", []string{"--node-id", ""}, "--node-id"},
		{"node id with a slash", []string{"--node-id", "a/b"}, "--node-id"},
		{"listen without a port", []string{"--listen", "7000"}, "--listen"},
		// An ephemeral port can't be advertised: the node publishes its
		// address before the listener exists.
		{"listen on port zero", []string{"--listen", ":0"}, "fixed port"},
		{"advertise without a port", []string{"--advertise", "node-1"}, "--advertise"},
		{"metrics listen without a port", []string{"--metrics-listen", "9100"}, "--metrics-listen"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(tt.args)
			if err == nil {
				t.Fatalf("Load(%q) succeeded, want an error", tt.args)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Load(%q) = %v, want it to mention %q", tt.args, err, tt.want)
			}
		})
	}
}

// Load returns flag.ErrHelp rather than exiting, so main can decide
// that usage is not a failure.
func TestConfig_Help(t *testing.T) {
	_, err := Load([]string{"--help"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Load(--help) = %v, want flag.ErrHelp", err)
	}
}

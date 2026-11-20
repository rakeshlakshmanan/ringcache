package main

import (
	"io"
	"slices"
	"strings"
	"testing"
)

// Every subcommand registers itself from an init function, so the
// registry is also the check that a new file was actually wired in.
func TestRegistry_HasTheExpectedCommands(t *testing.T) {
	// Phases 9, 11, 12 and 14 add to this list; each adds its own file,
	// not an edit to a shared switch.
	want := []string{"get", "ring", "set", "sim-move"}

	got := commandNames()
	sorted := slices.Clone(got)
	slices.Sort(sorted)
	if !slices.Equal(sorted, want) {
		t.Fatalf("registered commands are %v, want %v", sorted, want)
	}

	for _, name := range got {
		c := registry[name]
		if c.summary == "" {
			t.Errorf("%q has no summary, so it would appear blank in the usage text", name)
		}
		if c.run == nil {
			t.Errorf("%q has no run function", name)
		}
	}
}

// Cluster commands come before offline ones in the usage text, so a
// reader sees the ones they are most likely to want first.
func TestRegistry_UsageOrder(t *testing.T) {
	names := commandNames()

	lastGroup := groupCluster
	for _, name := range names {
		g := registry[name].group
		if g < lastGroup {
			t.Fatalf("%q in group %d comes after group %d", name, g, lastGroup)
		}
		lastGroup = g
	}
}

// The usage text is built from the registry, so a subcommand cannot be
// added without appearing in it.
func TestUsage_ListsEveryCommand(t *testing.T) {
	var out strings.Builder
	usage(&out)
	text := out.String()

	for name := range registry {
		if !strings.Contains(text, name) {
			t.Errorf("the usage text does not mention %q", name)
		}
	}
	if !strings.Contains(text, "--nodes") {
		t.Error("the usage text does not explain --nodes")
	}
}

func TestRun_UnknownCommand(t *testing.T) {
	err := run([]string{"nope"}, io.Discard)
	if err == nil {
		t.Fatal("run accepted an unknown subcommand")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("error %v does not name the subcommand", err)
	}
}

func TestRun_NoCommand(t *testing.T) {
	if err := run(nil, io.Discard); err == nil {
		t.Fatal("run accepted no subcommand")
	}
}

func TestRun_Help(t *testing.T) {
	var out strings.Builder
	for _, arg := range []string{"help", "-h", "--help"} {
		out.Reset()
		if err := run([]string{arg}, &out); err != nil {
			t.Errorf("run(%q) = %v, want no error", arg, err)
		}
		if !strings.Contains(out.String(), "ringctl") {
			t.Errorf("run(%q) printed no usage", arg)
		}
	}
}

// sim-move needs no cluster, so it is the one subcommand that can be
// run end to end in a unit test.
func TestSimMove(t *testing.T) {
	var out strings.Builder
	// A small ring and few keys: the thresholds are checked in the ring
	// package's own tests, which measure the real shape.
	if err := simMove([]string{"--keys", "2000", "--from", "4", "--to", "5", "--vnodes", "32"}, &out); err != nil {
		t.Fatalf("sim-move: %v", err)
	}

	text := out.String()
	for _, want := range []string{"consistent hashing", "modulo hashing", "less data"} {
		if !strings.Contains(text, want) {
			t.Errorf("the output does not mention %q:\n%s", want, text)
		}
	}
}

func TestSimMove_Invalid(t *testing.T) {
	for _, args := range [][]string{
		{"--keys", "0"},
		{"--from", "0"},
		{"--to", "-1"},
	} {
		if err := simMove(args, io.Discard); err == nil {
			t.Errorf("sim-move%v succeeded, want an error", args)
		}
	}
}

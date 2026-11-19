package membership_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/membership"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

func TestParseStatic(t *testing.T) {
	members, err := membership.ParseStatic("node-1=127.0.0.1:7001,node-2=host:7002")
	if err != nil {
		t.Fatalf("ParseStatic: %v", err)
	}
	want := []ring.Member{
		{ID: "node-1", Addr: "127.0.0.1:7001"},
		{ID: "node-2", Addr: "host:7002"},
	}
	if len(members) != len(want) {
		t.Fatalf("got %d members, want %d", len(members), len(want))
	}
	for i := range want {
		if members[i] != want[i] {
			t.Errorf("member %d = %+v, want %+v", i, members[i], want[i])
		}
	}

	// Whitespace around entries is what a shell or a compose file
	// leaves behind, so it must not change the result.
	spaced, err := membership.ParseStatic("  node-1 = 127.0.0.1:7001 , node-2 = host:7002  ")
	if err != nil {
		t.Fatalf("ParseStatic with spaces: %v", err)
	}
	if len(spaced) != 2 || spaced[0] != want[0] || spaced[1] != want[1] {
		t.Errorf("spacing changed the result: %+v", spaced)
	}
}

func TestParseStatic_Invalid(t *testing.T) {
	tests := []struct {
		name string
		spec string
		want string
	}{
		{"empty", "", "no members"},
		{"only spaces", "   ", "no members"},
		{"no equals", "node-1", "id=host:port"},
		{"no port", "node-1=host", "address of"},
		{"empty id", "=host:7000", "no ID"},
		{"empty address", "node-1=", "address of"},
		{"duplicate id", "node-1=a:1,node-1=b:2", "twice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := membership.ParseStatic(tt.spec)
			if err == nil {
				t.Fatalf("ParseStatic(%q) succeeded, want an error", tt.spec)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("ParseStatic(%q) = %v, want it to mention %q", tt.spec, err, tt.want)
			}
		})
	}
}

func TestStatic_Current(t *testing.T) {
	src, err := membership.NewStaticFromFlag("node-1=a:1,node-2=b:2,node-3=c:3")
	if err != nil {
		t.Fatalf("NewStaticFromFlag: %v", err)
	}
	defer src.Close()

	r := src.Current()
	if r == nil {
		t.Fatal("Current returned nil")
	}
	if r.Epoch() != membership.StaticEpoch {
		t.Errorf("epoch = %d, want %d", r.Epoch(), membership.StaticEpoch)
	}
	if len(r.Members()) != 3 || r.RF() != config.ReplicationFactor {
		t.Errorf("ring has %d members and rf %d", len(r.Members()), r.RF())
	}

	// A static ring never changes, so Current must keep handing back the
	// same one and Refresh must not disturb it.
	if err := src.Refresh(context.Background()); err != nil {
		t.Errorf("Refresh: %v", err)
	}
	if src.Current() != r {
		t.Error("Refresh replaced the ring, but a static ring never changes")
	}
}

// A watcher that subscribes after the ring was published must not have
// to wait for a change to learn there is one: a static source never
// publishes again.
func TestStatic_SubscribeDeliversCurrent(t *testing.T) {
	src, err := membership.NewStaticFromFlag("node-1=a:1,node-2=b:2")
	if err != nil {
		t.Fatalf("NewStaticFromFlag: %v", err)
	}
	defer src.Close()

	for i := range 3 {
		ch := src.Subscribe()
		select {
		case r := <-ch:
			if r == nil || r.Epoch() != membership.StaticEpoch {
				t.Fatalf("subscriber %d got %v", i, r)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("subscriber %d never received the current ring", i)
		}
	}
}

// Close releases the watchers, so a component waiting on one stops
// rather than blocking forever.
func TestStatic_CloseReleasesWatchers(t *testing.T) {
	src, err := membership.NewStaticFromFlag("node-1=a:1")
	if err != nil {
		t.Fatalf("NewStaticFromFlag: %v", err)
	}

	ch := src.Subscribe()
	<-ch // the current ring

	src.Close()
	select {
	case _, open := <-ch:
		if open {
			t.Error("the channel carried a value after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release the watcher")
	}

	// Closing twice must be safe: a node closes its source on shutdown
	// and a test may close it again.
	src.Close()

	// A subscriber arriving after Close gets a closed channel rather
	// than one that never fires.
	select {
	case _, open := <-src.Subscribe():
		if open {
			t.Error("subscribing after Close gave an open channel")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscribing after Close blocked")
	}
}

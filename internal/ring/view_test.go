package ring

import (
	"slices"
	"testing"

	ringcachev1 "github.com/rakeshlakshmanan/ringcache/gen/ringcachev1"
	"github.com/rakeshlakshmanan/ringcache/internal/config"
)

// A view has to survive the wire unchanged, because a client builds its
// ring from one and must reach the same conclusions as the nodes.
func TestView_ProtoRoundTrip(t *testing.T) {
	v := View{Epoch: 12, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: members(5)}

	got := FromProto(v.ToProto())
	if !got.Equal(v) {
		t.Fatalf("round trip gave %+v, want %+v", got, v)
	}

	// The rings built from the two must be identical, not merely equal
	// field by field.
	a, err := New(v)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, err := New(got)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !slices.Equal(a.tokens, b.tokens) {
		t.Error("the round-tripped view builds a different ring")
	}
}

func TestView_FromProtoNil(t *testing.T) {
	if got := FromProto(nil); !got.Equal(View{}) {
		t.Errorf("FromProto(nil) = %+v, want the zero view", got)
	}
	if _, err := New(FromProto(&ringcachev1.RingView{})); err == nil {
		t.Error("New from an empty wire view succeeded, want an error")
	}
}

func TestView_Equal(t *testing.T) {
	base := View{Epoch: 1, VNodes: 4, RF: 2, Members: members(3)}

	shuffled := base.Clone()
	shuffled.Members = []Member{shuffled.Members[2], shuffled.Members[0], shuffled.Members[1]}
	if !base.Equal(shuffled) {
		t.Error("member order made two equal views compare unequal; the ring sorts them")
	}

	for _, tt := range []struct {
		name string
		edit func(v *View)
	}{
		{"epoch", func(v *View) { v.Epoch = 2 }},
		{"vnodes", func(v *View) { v.VNodes = 8 }},
		{"rf", func(v *View) { v.RF = 3 }},
		{"a member's address", func(v *View) { v.Members[0].Addr = "elsewhere:7000" }},
		{"a member's id", func(v *View) { v.Members[0].ID = "node-9" }},
		{"an extra member", func(v *View) { v.Members = append(v.Members, Member{ID: "node-4", Addr: "node-4:7000"}) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			other := base.Clone()
			tt.edit(&other)
			if base.Equal(other) {
				t.Errorf("views compare equal although %s differs", tt.name)
			}
		})
	}
}

func TestView_Clone(t *testing.T) {
	v := View{Epoch: 1, VNodes: 4, RF: 2, Members: members(3)}
	c := v.Clone()
	c.Members[0] = Member{ID: "intruder", Addr: "x:1"}

	if v.Members[0].ID != "node-1" {
		t.Error("Clone shares the member slice with the original")
	}
}

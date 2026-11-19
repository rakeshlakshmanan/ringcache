package membership

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/rakeshlakshmanan/ringcache/internal/config"
	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

// StaticEpoch is the epoch every static ring carries. A static cluster
// never republishes, so one fixed epoch above zero is enough: zero
// means "no ring yet", which a static node is never in.
const StaticEpoch uint64 = 1

// Static is a Source whose ring never changes. It is what a cluster
// runs on before etcd, and it stays afterwards: a test of one feature
// uses it because it needs no etcd and no coordinator.
type Static struct {
	r *ring.Ring
	n notifier
}

// NewStatic builds a static source from a member list.
func NewStatic(members []ring.Member) (*Static, error) {
	r, err := ring.New(ring.View{
		Epoch:   StaticEpoch,
		VNodes:  config.VNodes,
		RF:      config.ReplicationFactor,
		Members: members,
	})
	if err != nil {
		return nil, err
	}

	s := &Static{r: r}
	s.n.publish(r)
	return s, nil
}

// ParseStatic reads the --static-members flag:
// "node-1=host:7001,node-2=host:7002".
func ParseStatic(spec string) ([]ring.Member, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return nil, fmt.Errorf("membership: no members given")
	}

	var members []ring.Member
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, addr, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("membership: %q is not id=host:port", entry)
		}
		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("membership: address of %q: %w", id, err)
		}
		members = append(members, ring.Member{ID: id, Addr: addr})
	}

	// ring.New rejects an empty ID, a duplicate ID and an empty address,
	// so the member list is validated where every other ring is.
	if _, err := ring.New(ring.View{
		Epoch: StaticEpoch, VNodes: config.VNodes, RF: config.ReplicationFactor, Members: members,
	}); err != nil {
		return nil, err
	}
	return members, nil
}

// NewStaticFromFlag builds a static source from the flag's raw value.
func NewStaticFromFlag(spec string) (*Static, error) {
	members, err := ParseStatic(spec)
	if err != nil {
		return nil, err
	}
	return NewStatic(members)
}

// Current returns the fixed ring.
func (s *Static) Current() *ring.Ring { return s.r }

// Subscribe returns a channel holding the fixed ring. It never carries
// a second value, because a static ring never changes.
func (s *Static) Subscribe() <-chan *ring.Ring { return s.n.subscribe() }

// Refresh does nothing: there is nowhere to fetch a newer ring from.
func (*Static) Refresh(context.Context) error { return nil }

// Close releases the subscribers.
func (s *Static) Close() { s.n.close() }

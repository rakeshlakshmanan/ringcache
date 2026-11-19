// Package membership supplies the ring a node routes with, and keeps
// it up to date.
//
// A node never builds a ring itself: it takes one from a Source. Phase
// 6 has one implementation, Static, built from a fixed member list.
// Phase 9 adds the etcd-backed one, and because both satisfy the same
// interface the server above them does not change.
package membership

import (
	"context"
	"sync"

	"github.com/rakeshlakshmanan/ringcache/internal/ring"
)

// Source supplies the current ring and tells watchers when it changes.
type Source interface {
	// Current returns the ring in force, or nil when none has been
	// published yet. The returned ring is immutable.
	Current() *ring.Ring

	// Subscribe returns a channel that receives each new ring. The
	// channel holds one value: a watcher that falls behind sees the
	// newest ring rather than a queue of old ones, which is what a
	// watcher wants, since only the newest ring decides anything.
	//
	// The channel is closed when the source is closed.
	Subscribe() <-chan *ring.Ring

	// Refresh asks the source to fetch the current ring now, rather
	// than waiting for its own schedule. It is what a node calls when a
	// client reports an epoch newer than its own.
	Refresh(ctx context.Context) error
}

// notifier is the Subscribe half of a Source, shared by the static and
// etcd implementations.
type notifier struct {
	mu     sync.Mutex
	subs   []chan *ring.Ring
	closed bool
	// last is handed to a new subscriber straight away, so a watcher
	// that subscribes after the first ring was published does not wait
	// for the next change to learn there is a ring.
	last *ring.Ring
}

// subscribe registers a watcher. It receives the current ring at once
// when there is one.
func (n *notifier) subscribe() <-chan *ring.Ring {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan *ring.Ring, 1)
	if n.closed {
		close(ch)
		return ch
	}
	if n.last != nil {
		ch <- n.last
	}
	n.subs = append(n.subs, ch)
	return ch
}

// publish hands r to every watcher, replacing any value a watcher has
// not collected yet.
func (n *notifier) publish(r *ring.Ring) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return
	}
	n.last = r
	for _, ch := range n.subs {
		// Drop the value the watcher has not taken yet: it is older
		// than this one and nothing decides from an old ring.
		select {
		case <-ch:
		default:
		}
		ch <- r
	}
}

// close releases the watchers. Later publishes are ignored.
func (n *notifier) close() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return
	}
	n.closed = true
	for _, ch := range n.subs {
		close(ch)
	}
	n.subs = nil
}

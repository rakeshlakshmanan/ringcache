package clock

import (
	"sync"
	"time"
)

// fakeStart is the instant every new Fake starts at. A fixed date keeps
// tests reproducible.
var fakeStart = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

// Fake is a Clock whose time moves only when a test moves it.
type Fake struct {
	mu  sync.Mutex
	now time.Time
}

// NewFake returns a Fake starting at a fixed date.
func NewFake() *Fake {
	return &Fake{now: fakeStart}
}

// Now returns the fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Set replaces the fake's current time with t.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
}

// Advance moves the fake's current time forward by d. A negative d moves
// it backwards, which tests use to simulate a wall clock set back.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

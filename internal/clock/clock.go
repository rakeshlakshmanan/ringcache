// Package clock provides a small clock abstraction so components that
// read the time can be driven by a fake clock in tests.
package clock

import "time"

// Clock supplies the current time.
type Clock interface {
	Now() time.Time
}

// Real reads the system clock.
type Real struct{}

// Now returns time.Now(). The returned value carries Go's monotonic
// reading, so time.Since on values from Real ignores wall-clock jumps.
func (Real) Now() time.Time { return time.Now() }

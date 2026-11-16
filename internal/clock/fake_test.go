package clock

import (
	"testing"
	"time"
)

func TestFake_AdvanceAndSet(t *testing.T) {
	f := NewFake()
	start := f.Now()

	f.Advance(1500 * time.Millisecond)
	if got, want := f.Now(), start.Add(1500*time.Millisecond); !got.Equal(want) {
		t.Errorf("after Advance(1.5s): Now() = %v, want %v", got, want)
	}

	target := time.Date(2026, time.March, 9, 12, 0, 0, 0, time.UTC)
	f.Set(target)
	if got := f.Now(); !got.Equal(target) {
		t.Errorf("after Set: Now() = %v, want %v", got, target)
	}
}

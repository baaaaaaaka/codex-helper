package hoststate

import (
	"testing"
	"time"
)

func TestWallElapsedUsesWallClockComponent(t *testing.T) {
	previous := time.Now()
	now := previous.Add(3 * time.Second)
	if got := wallElapsed(now, previous); got != 3*time.Second {
		t.Fatalf("wallElapsed = %s, want 3s", got)
	}
	if got := wallElapsed(previous, now); got != -3*time.Second {
		t.Fatalf("backward wallElapsed = %s, want -3s", got)
	}
	if got := wallElapsed(time.Time{}, previous); got != 0 {
		t.Fatalf("zero current wallElapsed = %s, want 0", got)
	}
	if got := wallElapsed(now, time.Time{}); got != 0 {
		t.Fatalf("zero previous wallElapsed = %s, want 0", got)
	}
}

package stack

import (
	"testing"
	"time"
)

func TestProbeGapDetectedStripsMonotonicClock(t *testing.T) {
	previous := time.Now()
	now := previous.Add(3 * time.Second)
	if !probeGapDetected(now, previous, time.Second) {
		t.Fatal("three-second wall gap was not detected for a one-second interval")
	}
	if probeGapDetected(previous.Add(2*time.Second), previous, time.Second) {
		t.Fatal("exactly two intervals should not be treated as a gap")
	}
	if probeGapDetected(previous, now, time.Second) {
		t.Fatal("backward clock movement should not be treated as a gap")
	}
	if probeGapDetected(now, previous, 0) {
		t.Fatal("zero interval should disable gap detection")
	}
}

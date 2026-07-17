package hoststate

import (
	"testing"
	"time"
)

func TestChannelObserverCoalescesAndCloses(t *testing.T) {
	o := NewChannelObserver(1)
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	o.Emit(Event{Kind: EventNetworkChanged})
	o.Emit(Event{Kind: EventPowerDidWake})
	select {
	case event := <-o.Events():
		if event.Kind != EventNetworkChanged {
			t.Fatalf("first event = %q, want network change", event.Kind)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
	o.Emit(Event{Kind: EventPowerWillSleep})
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	o.Emit(Event{Kind: EventPowerDidWake})
	for event := range o.Events() {
		_ = event
	}
}

func TestDefaultObserverCanStartAndClose(t *testing.T) {
	o := NewDefaultObserver(Options{Interval: time.Millisecond})
	if err := o.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := o.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

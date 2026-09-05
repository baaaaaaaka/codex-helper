//go:build !linux

package teams

import "testing"

func TestFallbackHistoryWatchEventSourceIsAlwaysUncertain(t *testing.T) {
	source := newHistoryWatchEventSource()
	dirty, uncertain, err := source.Update([]string{"/tmp/session.jsonl"}, true)
	if err != nil {
		t.Fatalf("fallback update: %v", err)
	}
	if !uncertain {
		t.Fatal("fallback watcher reported a trusted event stream")
	}
	if len(dirty) != 0 {
		t.Fatalf("fallback dirty paths = %#v, want none", dirty)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("fallback close: %v", err)
	}
}

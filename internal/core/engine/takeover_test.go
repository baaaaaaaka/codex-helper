package engine

import (
	"testing"
	"time"
)

// A takeover by another host of a shared home is announced once in the
// control chat; the first run and restarts on the same host say nothing.
func TestTakeoverNoticeOncePerHostChange(t *testing.T) {
	host, why := "host-a", "cold"
	r := newRig(t, func(c *Config) { c.Host, c.TakeoverWhy = host, why })
	ctl := r.control()
	r.stop()
	r.start()
	time.Sleep(500 * time.Millisecond)
	if n := len(r.own(ctl, "took over")); n != 0 {
		t.Fatalf("same-host restart announced a takeover (%d)", n)
	}
	r.stop()

	host, why = "host-b", "stale"
	r.start()
	r.wait("takeover notice", func() bool {
		return len(r.own(ctl, "🔁 The helper now runs on host-b: it took over from host-a, whose helper stopped answering.")) == 1
	})
	r.stop()
	r.start()
	time.Sleep(500 * time.Millisecond)
	r.stop()
	host, why = "host-a", "released"
	r.start()
	r.wait("takeover back", func() bool {
		return len(r.own(ctl, "now runs on host-a: it took over from host-b, whose helper stopped.")) == 1
	})
	if n := len(r.own(ctl, "took over")); n != 2 {
		t.Fatalf("%d takeover notices, want 2", n)
	}
}

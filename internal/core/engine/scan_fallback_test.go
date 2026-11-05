package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
)

// failScanner is an activity scan that fails (a Graph 502/504) while fail
// is set and otherwise shows nothing; it logs the chat reads.
type failScanner struct {
	*channeltest.Fake
	mu    sync.Mutex
	fail  bool
	scans int
	reads map[string][]time.Time
}

func (s *failScanner) ScanActivity(context.Context, time.Time) ([]channel.ChatActivity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scans++
	if s.fail {
		return nil, errors.New("HTTP 504: UnknownError")
	}
	return nil, nil
}

func (s *failScanner) Receive(ctx context.Context, stream, cursor string) (channel.Window, error) {
	s.mu.Lock()
	if s.reads == nil {
		s.reads = map[string][]time.Time{}
	}
	s.reads[stream] = append(s.reads[stream], time.Now())
	s.mu.Unlock()
	return s.Fake.Receive(ctx, stream, cursor)
}

func (s *failScanner) set(fail bool) {
	s.mu.Lock()
	s.fail = fail
	s.mu.Unlock()
}

func (s *failScanner) readsIn(stream string, from, to time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, at := range s.reads[stream] {
		if !at.Before(from) && at.Before(to) {
			n++
		}
	}
	return n
}

// Live canary 2026-11-05: while the activity scan fails (Graph 502/504), a
// prompt in an idle work chat must not wait for the SafetyRead. After the
// scan has failed for scanBlindAfter, the idle chats are read directly at
// the fallback interval (12 scan intervals here); once the scan works
// again they go back to the SafetyRead.
func TestScanFailuresFallBackToDirectReads(t *testing.T) {
	var sc *failScanner
	r := newRig(t, func(c *Config) {
		sc = &failScanner{Fake: c.Channel.(*channeltest.Fake)}
		c.Channel, c.ActivityScan = sc, true
		c.Timing.HotFor, c.Timing.SafetyRead, c.Timing.ScanInterval = 200*time.Millisecond, time.Minute, 50*time.Millisecond
		c.ReadRate = 100 // a scan every 50 ms must not take every read token
	})
	work := r.newWork(t.TempDir())
	r.control()
	time.Sleep(time.Second) // the work chat goes idle: read only at the SafetyRead (>= 30 s away)
	sc.set(true)
	time.Sleep(500 * time.Millisecond) // failing for longer than scanBlindAfter (300 ms)
	posted := time.Now()
	r.ch.PostUser(work, "prompt during the scan outage")
	// The fallback interval is 600 ms (12 scan intervals).
	r.waitFor("prompt answered while the scan fails", 5*time.Second, func() bool {
		return len(r.own(work, "ANSWER to prompt during the scan outage")) == 1
	})
	if d := time.Since(posted); d > 3*time.Second {
		t.Fatalf("prompt answered %s after it was posted, want within the fallback interval", d)
	}
	sc.set(false)
	time.Sleep(time.Second) // a working scan; the chat cools down (HotFor), one fallback read may remain
	from := time.Now()
	time.Sleep(2 * time.Second)
	if n := sc.readsIn(work, from, time.Now()); n != 0 {
		t.Fatalf("%d direct reads of the idle work chat in 2 s after the scan works again, want 0 (SafetyRead)", n)
	}
}

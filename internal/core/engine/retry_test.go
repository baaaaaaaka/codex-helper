package engine

import (
	"errors"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// An ownership loss stops the engine at once with the fence error (the
// process exits and its supervisor restarts it); nothing keeps retrying
// against a store another process owns.
func TestOwnershipLossStopsEngine(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.Heartbeat = 50 * time.Millisecond })
	r.control()

	other, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	var runErr error
	select {
	case runErr = <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("engine kept running after losing ownership")
	}
	if !errors.Is(runErr, store.ErrFenced) && !errors.Is(runErr, store.ErrNotAcquired) {
		t.Fatalf("Run returned %v, want the fence error", runErr)
	}
	r.done <- runErr // for r.stop
	r.logMu.Lock()
	n := strings.Count(r.log.String(), "fatal:")
	r.logMu.Unlock()
	if n != 1 {
		t.Fatalf("%d fatal lines", n)
	}
}

// Invariant: an operation that keeps failing with the same error is retried
// on a growing backoff, never in a loop (the old helper spun at ~3/s on a
// lost lease). Every read and send fails here for three seconds; each
// stream and each outgoing message is tried a bounded number of times, the
// gaps grow, and no log line repeats more than a few times.
func TestRepeatedFailuresBackOff(t *testing.T) {
	var mu sync.Mutex
	var on atomic.Bool
	reads := map[string][]time.Time{}
	sends := map[string][]time.Time{}
	boom := errors.New("connection reset by peer")
	r := newRig(t, func(c *Config) {
		f := c.Channel.(*channeltest.Fake)
		f.ReceiveFault = func(stream string) error {
			if !on.Load() {
				return nil
			}
			mu.Lock()
			reads[stream] = append(reads[stream], time.Now())
			mu.Unlock()
			return boom
		}
		f.SendFault = func(chat string, p channel.Part) (bool, error) {
			if !on.Load() {
				return false, nil
			}
			mu.Lock()
			k := chat + "\x00" + p.Body
			sends[k] = append(sends[k], time.Now())
			mu.Unlock()
			return false, boom
		}
	})
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["long"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long job")
	r.wait("running", func() bool { return r.ag.calls("long") == 1 })
	on.Store(true)
	close(release) // the final answer now has to be sent
	time.Sleep(3 * time.Second)
	on.Store(false)

	mu.Lock()
	defer mu.Unlock()
	if len(reads) == 0 || len(sends) == 0 {
		t.Fatalf("faults not exercised: %d read streams, %d sends", len(reads), len(sends))
	}
	check := func(what string, at []time.Time) {
		// FastTiming(20ms): backoff Hot<<n capped at MaxBackoff (600 ms)
		// gives about 10 attempts in 3 s; a spin would give hundreds.
		if len(at) > 16 {
			t.Errorf("%s: %d attempts in 3s", what, len(at))
		}
		for i := 4; i < len(at); i++ {
			if gap := at[i].Sub(at[i-1]); gap < 100*time.Millisecond {
				t.Errorf("%s: attempt %d only %s after the previous one", what, i+1, gap)
				return
			}
		}
	}
	for s, at := range reads {
		check("read "+s, at)
	}
	for k, at := range sends {
		check("send "+strings.SplitN(k, "\x00", 2)[0], at)
	}

	r.logMu.Lock()
	logText := r.log.String()
	r.logMu.Unlock()
	digits := regexp.MustCompile(`[0-9]+`)
	count := map[string]int{}
	for _, l := range strings.Split(logText, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			count[digits.ReplaceAllString(l, "#")]++
		}
	}
	for l, n := range count {
		if n > 8 {
			t.Errorf("log line repeated %d times: %s", n, l)
		}
	}
}

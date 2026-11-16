package engine

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// A notice that already acquired the per-turn ordering lock must finish
// committing before the wait is registered and its notice can be committed.
func TestScheduledWaitOrdersInFlightSilenceNotice(t *testing.T) {
	started := make(chan TurnRequest, 1)
	r := newRig(t, func(c *Config) {
		c.Timing.SilenceHint, c.Timing.SilenceStuck = 0, 0
		a := c.Agent.(*fakeAgent)
		a.block["ordered wait"] = make(chan struct{})
		a.during = func(req TurnRequest, _ TurnHooks) { started <- req }
	})
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "ordered wait")
	var req TurnRequest
	select {
	case req = <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("agent did not start")
	}
	r.e.mu.Lock()
	cs := r.e.convs[req.ConvID]
	rs := cs.running
	r.e.mu.Unlock()
	if rs == nil {
		t.Fatal("turn not running")
	}

	locked, unblock := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(unblock) })
	blockerDone := make(chan error, 1)
	go func() {
		blockerDone <- r.st.Update(func(*store.Tx) error {
			close(locked)
			<-unblock
			return nil
		})
	}()
	select {
	case <-locked:
	case <-time.After(10 * time.Second):
		t.Fatal("store transaction did not start")
	}

	noticeDone := make(chan bool, 1)
	go func() {
		noticeDone <- r.e.silenceNoticeForWaitState(cs, rs, "", time.Time{}, "test:ordered-silence", "controlled silence")
	}()
	r.wait("silence notice enters commit", func() bool {
		if rs.silenceMu.TryLock() {
			rs.silenceMu.Unlock()
			return false
		}
		return true
	})
	waitStarted, waitDone := make(chan struct{}), make(chan struct{})
	go func() {
		close(waitStarted)
		r.e.nativeWait(req.ConvID, rs.turnID, rs, "ordered-sleep", time.Hour, true)
		close(waitDone)
	}()
	<-waitStarted
	select {
	case <-waitDone:
		t.Fatal("wait overtook the in-flight silence commit")
	default:
	}
	release.Do(func() { close(unblock) })
	select {
	case err := <-blockerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("store transaction did not finish")
	}
	select {
	case accepted := <-noticeDone:
		if !accepted {
			t.Fatal("current silence notice was rejected")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("silence notice did not finish")
	}
	select {
	case <-waitDone:
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not complete after the store was released")
	}
	r.wait("both ordered notices delivered", func() bool {
		return len(r.own(work, "controlled silence")) == 1 && len(r.own(work, "entered an in-session wait")) == 1
	})
	msgs := r.ch.Messages(work)
	var silence, wait int = -1, -1
	for i, msg := range msgs {
		if strings.Contains(msg.Text, "controlled silence") {
			silence = i
		}
		if strings.Contains(msg.Text, "entered an in-session wait") {
			wait = i
		}
	}
	if silence < 0 || wait < 0 || silence >= wait {
		t.Fatalf("notices out of order: silence=%d wait=%d", silence, wait)
	}
}

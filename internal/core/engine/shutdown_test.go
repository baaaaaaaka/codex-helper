package engine

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Owner ruling 2026-11-05: `shutdown` in the control chat stops the helper
// on all machines like `cxp teams stop`. It records the stop in the
// kill-switch file (source: the Teams control chat, this host), posts its
// reply once, and the engine stops with store.ErrDisabled once the reply is
// out. From the command on nothing else starts, nothing is read and nothing
// but the reply is sent: a turn that ends meanwhile keeps its answer queued
// and a message posted meanwhile is not run.
func TestShutdownStopsEverywhereAfterItsReply(t *testing.T) {
	var (
		holdCtl  atomic.Bool // fail the control chat's sends
		mu       sync.Mutex
		ctlChat  string
		attempts = map[string]int{} // send attempts per chat while held
	)
	r := newRig(t, func(c *Config) {
		c.Timing.Heartbeat = 50 * time.Millisecond
		c.Channel.(*channeltest.Fake).SendFault = func(chat string, _ channel.Part) (bool, error) {
			if !holdCtl.Load() {
				return false, nil
			}
			mu.Lock()
			defer mu.Unlock()
			attempts[chat]++
			if chat == ctlChat {
				return false, errors.New("connection reset by peer")
			}
			return false, nil
		}
	})
	ctl := r.control()
	r.ch.PostUser(ctl, "help")
	r.wait("help", func() bool { return len(r.own(ctl, "Commands in this chat")) == 1 })
	if help := r.own(ctl, "Commands in this chat")[0].Text; !strings.Contains(help,
		"- `shutdown`: stop the helper on all machines until `cxp teams start` is run on a machine") {
		t.Fatalf("control help does not list shutdown:\n%s", help)
	}
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["long job"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long job")
	r.wait("long job running", func() bool { return r.ag.calls("long job") == 1 })
	r.wait("long job acknowledged", func() bool { return len(r.own(work, "Request accepted")) == 1 })
	workBefore := len(r.ch.Messages(work))

	mu.Lock()
	ctlChat = ctl
	mu.Unlock()
	holdCtl.Store(true)
	r.ch.PostUser(ctl, "shutdown")
	dir := filepath.Dir(r.path)
	r.wait("the stop recorded and the reply tried", func() bool {
		_, ok, _ := store.ReadStop(dir)
		mu.Lock()
		defer mu.Unlock()
		return ok && attempts[ctl] > 0
	})
	// Stopping: the running turn ends and a new message arrives; neither
	// its answer nor the message's ACK is sent and the message is not run.
	close(release)
	r.ch.PostUser(work, "posted after shutdown")
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	sentWork := attempts[work]
	mu.Unlock()
	if sentWork != 0 {
		t.Fatalf("%d sends to the work chat while stopping, want 0", sentWork)
	}
	if n := r.ag.calls("posted after shutdown"); n != 0 {
		t.Fatalf("a message posted after shutdown ran %d times", n)
	}

	holdCtl.Store(false)
	var runErr error
	select {
	case runErr = <-r.done:
	case <-time.After(10 * time.Second):
		t.Fatal("engine kept running after shutdown")
	}
	r.done <- runErr // for r.stop
	if !errors.Is(runErr, store.ErrDisabled) {
		t.Fatalf("Run returned %v, want store.ErrDisabled", runErr)
	}
	replies := r.own(ctl, "Stopped on all machines")
	if len(replies) != 1 || replies[0].Text != "🔧 Helper:\n"+textShutdown {
		t.Fatalf("shutdown replies %q, want one %q", msgTexts(replies), textShutdown)
	}
	msgs := r.ch.Messages(ctl)
	if last := msgs[len(msgs)-1]; last.Text != replies[0].Text {
		t.Fatalf("last control-chat message %q, want the shutdown reply", last.Text)
	}
	if got := r.ch.Messages(work)[workBefore:]; len(got) != 1 || got[0].Marker != "" {
		t.Fatalf("work chat after shutdown: %q, want only the owner's message", msgTexts(got))
	}
	rec, ok, err := store.ReadStop(dir)
	if err != nil || !ok || rec.Source != store.StopByTeams || rec.Host != "m" || rec.At.IsZero() {
		t.Fatalf("stop record %+v ok=%v err=%v, want source teams on host m with a time", rec, ok, err)
	}
	if !strings.HasPrefix(rec.Describe(""), "Stopped on all machines by `shutdown` in the Teams control chat (m, ") {
		t.Fatalf("stop description %q", rec.Describe(""))
	}
}

func msgTexts(ms []channel.InboundMsg) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Text)
	}
	return out
}

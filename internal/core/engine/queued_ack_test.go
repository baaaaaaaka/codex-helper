package engine

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// A request that waits behind a running one gets v0.1.20's queued ACK word
// for word (it used to get "⏳ Codex is working. Request accepted. It is
// queued behind 1 earlier request."), and `helper cancel running`, which
// that ACK names, interrupts the running request and leaves the queued one
// to run.
func TestQueuedAckIsV0120AndCancelRunningKeepsQueue(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["first job"] = release
	r.ag.mu.Unlock()
	defer close(release)
	r.ch.PostUser(work, "first job")
	r.wait("first job running", func() bool { return r.ag.calls("first job") == 1 })
	r.ch.PostUser(work, "second   job with `code`")
	r.wait("queued ACK", func() bool { return len(r.own(work, "Your request is queued.")) == 1 })
	want := strings.Join([]string{
		"🔧 Helper:",
		"⚠️ **Your request is queued.**",
		"",
		"**Next step:** wait. I will run your request after the current Codex request finishes.",
		"",
		"Another Codex request is already running in this Work chat, so I will run yours after it finishes.",
		"", "---", "",
		"**▶️ Running now:**",
		"1. first job",
		"",
		"**⏳ Queued requests:**",
		"1. second job with 'code'",
		"", "---", "",
		"**Options:**",
		"- Run this request next: do nothing. It is already submitted.",
		"- Need it immediately: send it in a different 💬 Work chat.",
		"- Drop the newest queued request: `helper cancel queued`.",
		"- Interrupt the request ahead only if it looks stuck: `helper cancel running`.",
	}, "\n")
	if got := r.own(work, "Your request is queued.")[0].Text; got != want {
		t.Fatalf("queued ACK:\n%s\nwant:\n%s", got, want)
	}
	if n := len(r.own(work, "Request accepted")); n != 1 {
		t.Fatalf("%d plain ACKs, want 1 (the first job's)", n)
	}

	r.ch.PostUser(work, "helper cancel running")
	r.wait("running cancelled", func() bool { return len(r.own(work, "Request cancelled.")) == 1 })
	r.wait("queued one answered", func() bool { return len(r.own(work, "ANSWER to second")) == 1 })
	if n := len(r.own(work, "Queued request cancelled.")); n != 0 {
		t.Fatalf("`helper cancel running` cancelled %d queued requests", n)
	}
}

// R-4-29: after a restart, the turn the previous process left running is
// not running any more. A request that arrives before that turn is settled
// is queued behind the queued requests only: its ACK does not list the
// dead turn under "Running now" (it did, followed by the turn's
// "interrupted when the helper stopped" notice).
func TestQueuedAckAfterRestartDoesNotListTheInterruptedTurn(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["long job"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long job")
	r.wait("running", func() bool { return r.ag.calls("long job") == 1 })
	r.ch.PostUser(work, "queued job")
	r.wait("queued ACK", func() bool { return len(r.own(work, "Your request is queued.")) == 1 })
	var key string
	r.wait("turn running in store", func() bool {
		turns, _ := r.st.OpenTurns()
		for _, tr := range turns {
			if tr.State == store.TurnRunning {
				key = tr.TurnKey
				return true
			}
		}
		return false
	})
	r.stop()
	close(release)
	gate := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.lookup[key] = TurnOutcome{Status: StatusInterrupted}
	r.ag.lookupGate = gate // the long job stays unsettled until the ACK is out
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "sent while down")
	r.start()
	r.wait("ACK of the request sent while down", func() bool { return len(r.own(work, "Your request is queued.")) == 2 })
	ack := r.own(work, "Your request is queued.")[1].Text
	close(gate)
	if strings.Contains(ack, "Running now") || strings.Contains(ack, "long job") {
		t.Fatalf("the ACK lists the turn the previous process left running:\n%s", ack)
	}
	if !strings.Contains(ack, "**Currently blocking:**\nCodex/helper work already in progress.") ||
		!strings.Contains(ack, "**⏳ Queued requests:**\n1. queued job\n2. sent while down") {
		t.Fatalf("queued ACK after restart:\n%s", ack)
	}
	r.wait("interruption notice", func() bool { return len(r.own(work, "interrupted when the helper stopped")) == 1 })
	r.wait("both answered", func() bool {
		return len(r.own(work, "ANSWER to queued job")) == 1 && len(r.own(work, "ANSWER to sent while down")) == 1
	})
}

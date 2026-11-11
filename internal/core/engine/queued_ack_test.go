package engine

import (
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// A request that waits behind a running one gets the short queued ACK
// (owner ruling 2026-11-05: it replaced v0.1.20's long one): when it runs,
// what runs now, and how to cancel it. A third request counts the one
// queued before it. `helper cancel running`, which the ACK names,
// interrupts the running request and leaves the queued ones to run.
func TestQueuedAckIsShortAndCancelRunningKeepsQueue(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["with `code`"] = release
	r.ag.mu.Unlock()
	defer close(release)
	r.ch.PostUser(work, "first   job with `code` "+strings.Repeat("x", 200))
	r.wait("first job running", func() bool { return r.ag.calls("with `code`") == 1 })
	r.ch.PostUser(work, "second job")
	r.wait("queued ACK", func() bool { return len(r.own(work, "**Queued.**")) == 1 })
	running := "first job with 'code' " + strings.Repeat("x", 179-len("first job with 'code' ")) + "…"
	want := strings.Join([]string{
		"🔧 Helper:",
		"⏳ **Queued.** Your request will run after the current one finishes.",
		"",
		"**Running now:** " + running,
		"",
		"To cancel it: `helper cancel` (cancels the newest queued request). To interrupt the running one: `helper cancel running`.",
	}, "\n")
	if got := r.own(work, "**Queued.**")[0].Text; strings.Split(got, "\n\nReceived at ")[0] != want {
		t.Fatalf("queued ACK:\n%s\nwant:\n%s", got, want)
	}
	r.ch.PostUser(work, "third job")
	r.wait("second queued ACK", func() bool { return len(r.own(work, "**Queued.**")) == 2 })
	want = strings.Replace(want, "finishes.", "finishes, behind 1 other queued request.", 1)
	if got := r.own(work, "**Queued.**")[1].Text; strings.Split(got, "\n\nReceived at ")[0] != want {
		t.Fatalf("queued ACK behind another queued request:\n%s\nwant:\n%s", got, want)
	}
	if n := len(r.own(work, "Request accepted")); n != 1 {
		t.Fatalf("%d plain ACKs, want 1 (the first job's)", n)
	}

	r.ch.PostUser(work, "helper cancel running")
	r.wait("running cancelled", func() bool { return len(r.own(work, "Codex request canceled.")) == 1 })
	r.wait("queued ones answered", func() bool {
		return len(r.own(work, "ANSWER to second")) == 1 && len(r.own(work, "ANSWER to third")) == 1
	})
	if n := len(r.own(work, "turn canceled:")); n != 0 {
		t.Fatalf("`helper cancel running` cancelled %d queued requests", n)
	}
}

// The ACK's `helper cancel` cancels the newest queued request: the one it
// acknowledged, while no other arrived after it.
func TestQueuedAckCancelCancelsTheAcknowledgedRequest(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["first job"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "first job")
	r.wait("first job running", func() bool { return r.ag.calls("first job") == 1 })
	r.ch.PostUser(work, "second job")
	r.wait("queued ACK", func() bool { return len(r.own(work, "**Queued.**")) == 1 })
	r.ch.PostUser(work, "helper cancel")
	r.wait("queued one cancelled", func() bool { return len(r.own(work, "turn canceled:")) == 1 })
	if n := len(r.own(work, "Canceled prompt:**\nsecond job")); n != 1 {
		t.Fatalf("the cancel reply does not name the cancelled prompt (%d)", n)
	}
	close(release)
	r.wait("first answered", func() bool { return len(r.own(work, "ANSWER to first job")) == 1 })
	if n := r.ag.calls("second job"); n != 0 {
		t.Fatalf("the cancelled request ran %d times", n)
	}
}

// R-4-29: after a restart, the turn the previous process left running is
// not running any more. A request that arrives before that turn is settled
// is queued behind the queued requests only: its ACK does not name the
// dead turn as running (it did, followed by the turn's "interrupted when
// the helper stopped" notice), nor offer to interrupt it.
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
	r.wait("queued ACK", func() bool { return len(r.own(work, "**Queued.**")) == 1 })
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
	r.wait("ACK of the request sent while down", func() bool { return len(r.own(work, "**Queued.**")) == 2 })
	ack := r.own(work, "**Queued.**")[1].Text
	close(gate)
	if strings.Contains(ack, "Running now") || strings.Contains(ack, "long job") {
		t.Fatalf("the ACK lists the turn the previous process left running:\n%s", ack)
	}
	if want := "🔧 Helper:\n⏳ **Queued.** Your request will run when the current work in this chat finishes, behind 1 other queued request.\n\n" +
		"To cancel it: `helper cancel` (cancels the newest queued request)."; strings.Split(ack, "\n\nReceived at ")[0] != want {
		t.Fatalf("queued ACK after restart:\n%s\nwant:\n%s", ack, want)
	}
	r.wait("interruption notice", func() bool {
		return len(r.own(work, "Codex request interrupted because the Teams helper stopped")) == 1
	})
	r.wait("both answered", func() bool {
		return len(r.own(work, "ANSWER to queued job")) == 1 && len(r.own(work, "ANSWER to sent while down")) == 1
	})
}

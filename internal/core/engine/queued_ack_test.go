package engine

import (
	"strings"
	"testing"
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

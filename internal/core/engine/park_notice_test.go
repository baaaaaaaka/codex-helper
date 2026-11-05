package engine

import (
	"strings"
	"testing"
)

// Owner ruling 2026-11-05: the park notice says what happens to messages
// sent while the chat is parked: they are read and run once it wakes. Both
// forms of park say so, and a message sent while parked does run after
// `r <session>`.
func TestParkNoticeSaysParkedMessagesRunAfterTheWake(t *testing.T) {
	r := newRig(t)
	ctl := r.control()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "helper park")
	r.wait("parked from the work chat", func() bool { return len(r.own(work, "Parked s001")) == 1 })
	want := "🔧 Helper:\nParked s001: this chat is no longer read. Messages sent here while it is parked are read and run once it wakes. " +
		"Wake it from the control chat with `r s001`, or by using the session in Codex."
	if got := r.own(work, "Parked s001")[0].Text; got != want {
		t.Fatalf("work-chat park notice:\n%q\nwant:\n%q", got, want)
	}
	r.ch.PostUser(work, "sent while parked")
	r.ch.PostUser(ctl, "r s001")
	r.wait("the message sent while parked runs after the wake", func() bool {
		return len(r.own(work, "ANSWER to sent while parked")) == 1
	})

	r.ch.PostUser(ctl, "park s001")
	r.wait("parked from the control chat", func() bool { return len(r.own(ctl, "Parked s001")) == 1 })
	want = "🔧 Helper:\nParked s001: its chat is no longer read. Messages sent to it while it is parked are read and run once it wakes. " +
		"Wake it with `r s001`, or by using the session in Codex."
	if got := r.own(ctl, "Parked s001")[0].Text; got != want {
		t.Fatalf("control-chat park notice:\n%q\nwant:\n%q", got, want)
	}
	if n := r.ag.calls("sent while parked"); n != 1 || !strings.Contains(r.own(work, "ANSWER to sent while parked")[0].Text, "ANSWER") {
		t.Fatalf("%d runs of the message sent while parked, want 1", n)
	}
}

package engine

import (
	"strings"
	"testing"
	"time"
)

// A Teams-run turn posts as v0.1.20 did: its commentary and each context
// compaction as "🤖 ⏳ Codex status", its answer as "🤖 ✅ Codex answer" with
// the completion footer, and no @mention in the work chat (the owner is
// notified in the control chat).
func TestTeamsRunTurnPostsV0120StatusAndAnswer(t *testing.T) {
	r := newRig(t)
	r.ag.mu.Lock()
	r.ag.notes = []string{"step A"}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "compaction and progress job")
	r.wait("answered", func() bool { return len(r.own(work, "ANSWER to compaction")) == 1 })
	var got []string
	for _, m := range r.own(work, "") {
		if strings.HasPrefix(m.Text, "🤖") {
			for i, block := range strings.Split(m.Text, "\n\n🤖 ⏳ Codex status:\n") {
				if i > 0 {
					block = "🤖 ⏳ Codex status:\n" + block
				}
				got = append(got, block)
			}
		}
		if m.MentionsOwner {
			t.Errorf("work-chat post mentions the owner: %q", m.Text)
		}
	}
	if len(got) != 3 ||
		got[0] != "🤖 ⏳ Codex status:\nContext compacted. Earlier turns were summarized so Codex can continue the thread." ||
		got[1] != "🤖 ⏳ Codex status:\nstep A (compaction and progress job)" ||
		!strings.HasPrefix(got[2], "🤖 ✅ Codex answer:\nANSWER to compaction and progress job") ||
		!strings.HasSuffix(got[2], "\n🔧 Helper: ✅ Codex finished responding.") || strings.Count(got[2], "finished responding") != 1 {
		t.Fatalf("Codex posts: %q", got)
	}
	r.wait("owner notified in the control chat", func() bool { return len(r.own(r.control(), "Codex finished")) == 1 })
}

// Codex retrying within a turn (a dropped stream it reconnects) is posted
// as v0.1.20's Codex status, at most once per RetryStatus per turn; a
// delegated turn posts none.
func TestRetryingPostsV0120ReconnectStatusRateLimited(t *testing.T) {
	r := newRig(t, func(c *Config) { c.Timing.RetryStatus = 300 * time.Millisecond })
	r.ag.mu.Lock()
	r.ag.during = func(req TurnRequest, h TurnHooks) {
		if !strings.Contains(req.Text, "flaky") {
			return
		}
		h.Retrying("Reconnecting... 1/5: stream disconnected before completion", "responseStreamDisconnected")
		h.Retrying("Reconnecting... 2/5", "responseStreamDisconnected") // within RetryStatus: not posted
		time.Sleep(400 * time.Millisecond)
		h.Retrying("", "")
		h.Retrying("connection dropped. codex is reconnecting.", "")
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "flaky job")
	r.wait("answered", func() bool { return len(r.own(work, "ANSWER to flaky")) == 1 })
	var got []string
	for _, m := range r.own(work, "") {
		if strings.HasPrefix(m.Text, "🤖 ⏳") {
			got = append(got, m.Text)
		}
	}
	want := []string{
		"🤖 ⏳ Codex status:\nConnection dropped. Codex is reconnecting.\n\nReconnecting... 1/5: stream disconnected before completion\nReason: responseStreamDisconnected",
		"🤖 ⏳ Codex status:\nConnection dropped. Codex is reconnecting.\n\nReconnecting...",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("statuses:\n%q\nwant\n%q", got, want)
	}
}

func TestTextReconnectingMatchesV0120(t *testing.T) {
	for _, c := range []struct{ message, code, want string }{
		{"", "", "Connection dropped. Codex is reconnecting.\n\nReconnecting..."},
		{"Connection dropped. Codex is reconnecting.", "", "Connection dropped. Codex is reconnecting."},
		{" Reconnecting... 1/5 ", " responseStreamDisconnected ", "Connection dropped. Codex is reconnecting.\n\nReconnecting... 1/5\nReason: responseStreamDisconnected"},
	} {
		if got := textReconnecting(c.message, c.code); got != c.want {
			t.Errorf("textReconnecting(%q, %q) = %q, want %q", c.message, c.code, got, c.want)
		}
	}
}

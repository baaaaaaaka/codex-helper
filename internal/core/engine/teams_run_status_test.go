package engine

import (
	"strings"
	"testing"
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
			got = append(got, m.Text)
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
	r.wait("owner notified in the control chat", func() bool { return len(r.own(r.control(), "Codex finished in")) == 1 })
}

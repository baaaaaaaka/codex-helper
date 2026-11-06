package engine

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// appendCompaction appends a context compaction as Codex writes it: the
// compacted record (replacement history) and its context_compacted event.
func appendCompaction(t *testing.T, path string) {
	t.Helper()
	ts := `{"timestamp":"` + time.Now().UTC().Format("2006-01-02T15:04:05.000Z") + `",`
	appendFile(t, path, []byte(ts+`"type":"compacted","payload":{"message":"","replacement_history":[{"type":"message","role":"user","content":[{"type":"input_text","text":"earlier"}]}]}}`+"\n"+
		ts+`"type":"event_msg","payload":{"type":"context_compacted"}}`+"\n"))
}

// mentionsCovered counts the control-chat @mentions of finals.
func (r *rig) mentionsCovered(ctl string) int {
	n := 0
	for _, m := range r.own(ctl, "Codex finished") {
		if !m.MentionsOwner {
			r.t.Fatalf("notification without mention: %q", m.Text)
		}
		n++ // one final each (owner ruling 2026-11-05, Q3)
	}
	return n
}

// v0.1.20 mirrored a live local session's commentary and compactions as
// Codex status and notified the owner of every live mirrored final and of
// a live local session (turn_completed and local_session_started; here without a
// Workflow card, so as control-chat @mentions), never in the work chat.
func TestLiveMirroredFinalsNotifyTheOwner(t *testing.T) {
	r, sessions := mirrorRig(t)
	ctl := r.control()
	r.mirrorInitialised() // a live session, not one found at start
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "live prompt one", Commentary: "live commentary one", Answer: "live answer one"}); err != nil {
		t.Fatal(err)
	}
	local := r.localChat()
	r.wait("first final notified", func() bool { return r.mentionsCovered(ctl) == 1 })
	r.wait("session notified", func() bool { return len(r.own(ctl, "New local Codex chat detected")) == 1 })
	if m := r.own(ctl, "New local Codex chat detected")[0]; !m.MentionsOwner || !strings.Contains(m.Text, "Chat:") {
		t.Fatalf("session notification: %+v", m)
	}
	appendCompaction(t, w.Path)
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "live prompt two", Answer: "live answer two"}); err != nil {
		t.Fatal(err)
	}
	r.wait("second answer", func() bool { return len(r.own(local, "live answer two")) == 1 })
	// v0.1.20's live mirror: commentary and compaction as Codex status,
	// in file order, notifying nobody.
	var got []string
	for _, m := range r.own(local, "") {
		got = append(got, m.Text)
	}
	want := []string{
		"🔧 Helper:\nLocal Codex chat detected: s001.",
		"🧑\u200d💻 User:\nlive prompt one",
		"🤖 ⏳ Codex status:\nlive commentary one",
		"🤖 ✅ Codex answer:\nlive answer one\n🔧 Helper: ✅ Codex finished responding.",
		"🤖 ⏳ Codex status:\n" + transcript.CompactText,
		"🧑\u200d💻 User:\nlive prompt two",
		"🤖 ✅ Codex answer:\nlive answer two\n🔧 Helper: ✅ Codex finished responding.",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("local chat:\n%q\nwant:\n%q", got, want)
	}
	r.wait("second final notified", func() bool { return r.mentionsCovered(ctl) == 2 })
	time.Sleep(500 * time.Millisecond)
	if n := r.mentionsCovered(ctl); n != 2 {
		t.Fatalf("mentions cover %d finals, want 2", n)
	}
	if n := len(r.own(ctl, "New local Codex chat detected")); n != 1 {
		t.Fatalf("%d session notifications", n)
	}
	for _, m := range r.own(local, "") {
		if m.MentionsOwner {
			t.Fatalf("work-chat post mentions the owner: %q", m.Text)
		}
	}
}

// Catch-up: records already in a rollout when its source starts catching
// up (a session found at start, or a source behind its file at a restart) are mirrored without notifying anyone; what is appended after
// the source reached the end of its file is live and notifies.
func TestCatchUpFinalsDoNotNotify(t *testing.T) {
	r, sessions := mirrorRig(t)
	ctl := r.control()
	// A session that began while the helper was down (not its first run,
	// which baselines what exists): found at start, caught up.
	r.mirrorInitialised()
	r.stop()
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"old one", "old two"} {
		if _, err := w.AppendTurn(rollout.Turn{Prompt: "prompt " + p, Commentary: "commentary " + p, Answer: "answer " + p}); err != nil {
			t.Fatal(err)
		}
		appendCompaction(t, w.Path)
	}
	r.start()
	local := r.localChat()
	r.wait("catch-up mirrored", func() bool { return len(r.own(local, "answer old two")) == 1 })
	time.Sleep(time.Second)
	if n := r.mentionsCovered(ctl); n != 0 {
		t.Fatalf("catch-up finals notified (%d)", n)
	}
	if n := len(r.own(ctl, "New local Codex chat detected")); n != 0 {
		t.Fatalf("session found at start notified (%d)", n)
	}
	if n := len(r.own(local, "Codex status")); n != 0 {
		t.Fatalf("catch-up posted %d status notices", n)
	}

	// Live from here on.
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "prompt new", Commentary: "commentary new", Answer: "answer new"}); err != nil {
		t.Fatal(err)
	}
	r.wait("live final notified", func() bool { return r.mentionsCovered(ctl) == 1 })
	if got := r.own(local, "Codex status"); len(got) != 1 || got[0].Text != "🤖 ⏳ Codex status:\ncommentary new" {
		t.Fatalf("live status notices: %+v", got)
	}

	// Behind its file across a restart: catch-up again.
	r.stop()
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "prompt while down", Commentary: "commentary while down", Answer: "answer while down"}); err != nil {
		t.Fatal(err)
	}
	r.start()
	r.wait("restart catch-up mirrored", func() bool { return len(r.own(local, "answer while down")) == 1 })
	time.Sleep(time.Second)
	if n := r.mentionsCovered(ctl); n != 1 {
		t.Fatalf("mentions cover %d finals after the restart catch-up, want 1", n)
	}
	if n := len(r.own(local, "Codex status")); n != 1 {
		t.Fatalf("%d status notices after the restart catch-up, want 1 (the live one)", n)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "prompt after", Answer: "answer after"}); err != nil {
		t.Fatal(err)
	}
	r.wait("live final after restart notified", func() bool { return r.mentionsCovered(ctl) == 2 })
	if n := len(r.own(local, "answer")); n != 5 {
		t.Fatalf("%d answers in the chat, want 5", n)
	}
}

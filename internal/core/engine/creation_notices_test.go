package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// The notices of a new work chat are v0.1.20's (fidelity run): `new` posts
// "Work chat created: sNNN." and the ready anchor in the work chat and one
// "✅ Work chat created" answer in the control chat (no interim "Creating…"
// reply); a local session found on disk posts only "Local Codex chat
// detected: sNNN." in its own chat and nothing in the control chat.
func TestCreationNoticesAreV0120(t *testing.T) {
	r, sessions := mirrorRig(t)
	ctl := r.control()
	ctlBefore := len(r.own(ctl, ""))

	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "local prompt", Answer: "local answer"}); err != nil {
		t.Fatal(err)
	}
	local := r.localChat()
	r.wait("local records mirrored", func() bool { return len(r.own(local, "local answer")) == 1 })
	msgs := r.own(local, "")
	if len(msgs) == 0 || msgs[0].Text != "🔧 Helper:\nLocal Codex chat detected: s001." {
		t.Fatalf("first message of the local chat: %+v", msgs)
	}
	if n := len(r.own(local, "Work chat")); n != 0 {
		t.Errorf("%d `new`-style notices in the local chat", n)
	}
	time.Sleep(300 * time.Millisecond)
	if got := r.own(ctl, ""); len(got) != ctlBefore {
		t.Errorf("control chat got %d messages for a discovered session: %+v", len(got)-ctlBefore, got[ctlBefore:])
	}

	dir := t.TempDir()
	work := r.newWork(dir)
	want := []string{
		"🔧 Helper:\nWork chat created: s002.\nWorking directory: " + dir,
		"🔧 Helper:\n" + strings.Join([]string{
			"💬 Work chat is ready.",
			"Send a task in this chat. Codex will start automatically and continue this session.",
			"Status: waiting for your first task.",
			"Session: s002",
			"Project: " + projectLabel(dir),
			"Working directory: " + dir,
			"Commands: `helper status` or `!status`, `helper stats`, `helper help`, `helper close` to close this Codex session in Teams.",
			"Need more details? Send `helper status`.",
		}, "\n"),
	}
	got := r.own(work, "")
	if len(got) != len(want) {
		t.Fatalf("work chat: %+v", got)
	}
	for i := range want {
		if got[i].Text != want[i] {
			t.Errorf("work chat message %d:\n%s\nwant:\n%s", i+1, got[i].Text, want[i])
		}
	}
	created := r.own(ctl, "Work chat created")
	if len(created) != 1 || !strings.HasPrefix(created[0].Text, "🔧 Helper:\n✅ Work chat created: s002\n\nOpen this Teams link and send your task there:\n") ||
		!strings.HasSuffix(created[0].Text, "\n\nIf Teams does not show it right away, search for: s002") {
		t.Fatalf("control answer to `new`: %+v", created)
	}
	if n := len(r.own(ctl, "Creating")); n != 0 {
		t.Errorf("%d interim replies to `new`", n)
	}
}

func TestProjectLabel(t *testing.T) {
	for in, want := range map[string]string{
		"/home/baka":                    "baka",
		"/home/baka/a:b":                "a-b",
		`C:\work\proj\`:                 "proj",
		"/":                             "root",
		"":                              "helper default directory",
		"/x/" + strings.Repeat("长", 70): strings.Repeat("长", 61) + "...",
	} {
		if got := projectLabel(in); got != want {
			t.Errorf("projectLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

// v0.1.20 filterEmptySessions: a discovered session with nothing to mirror
// gets no chat (it used to get one at once, with only the notice in it, as
// for s598). Its chat comes with its first record, the notice first.
func TestEmptyDiscoveredSessionGetsNoChatUntilItsFirstRecord(t *testing.T) {
	r, sessions := mirrorRig(t)
	w, err := rollout.New(sessions, t.TempDir(), rollout.Options{})
	if err != nil {
		t.Fatal(err)
	}
	r.wait("session tracked", func() bool {
		srcs, _ := r.st.Sources()
		for _, s := range srcs {
			if s.ConvID != 0 {
				return true
			}
		}
		return false
	})
	time.Sleep(time.Second) // several stat, discovery and maintenance ticks
	if chat := r.chatOf("s001"); chat != "" {
		t.Fatalf("empty session got chat %s: %+v", chat, r.own(chat, ""))
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "first prompt", Answer: "first answer"}); err != nil {
		t.Fatal(err)
	}
	chat := r.localChat()
	r.wait("records mirrored", func() bool { return len(r.own(chat, "first answer")) == 1 })
	got := r.own(chat, "")
	if len(got) != 3 || got[0].Text != "🔧 Helper:\nLocal Codex chat detected: s001." ||
		!strings.Contains(got[1].Text, "first prompt") || !strings.Contains(got[2].Text, "first answer") {
		t.Fatalf("chat: %+v", got)
	}
}

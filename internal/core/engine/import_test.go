package engine

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// importRig starts an engine whose sessions directory already holds the
// rollouts write makes (a fresh install baselines them: only `continue`
// brings them to Teams).
func importRig(t *testing.T, write func(sessions string), tweak ...func(*Config)) *rig {
	sessions := t.TempDir()
	write(sessions)
	r := newRig(t, append([]func(*Config){func(c *Config) { c.SessionsDir = sessions }}, tweak...)...)
	r.control()
	r.mirrorInitialised()
	return r
}

// F-MIR-5: an import stopped half way (the helper stops while the cursor
// is in the middle of a rollout larger than one mirror pass) goes on after
// the restart from the persisted cursor: every record is posted exactly
// once, and "Import complete" and the control result once.
func TestImportStoppedMidwayFinishesOnceAfterRestart(t *testing.T) {
	var path, thread string
	r := importRig(t, func(sessions string) {
		w, err := rollout.New(sessions, t.TempDir(), rollout.Options{Start: time.Now().Add(-48 * time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		path, thread = w.Path, w.SessionID
		if _, err := w.AppendTurn(rollout.Turn{Prompt: "early prompt", Answer: "early answer"}); err != nil {
			t.Fatal(err)
		}
		appendFile(t, path, filler(20<<20))
		if _, err := w.AppendTurn(rollout.Turn{Prompt: "late prompt", Answer: "late answer"}); err != nil {
			t.Fatal(err)
		}
		_ = os.Chtimes(path, time.Now().Add(-47*time.Hour), time.Now().Add(-47*time.Hour))
	}, func(c *Config) { c.Timing.MirrorStat = 400 * time.Millisecond })
	ctl := r.control()
	r.ch.PostUser(ctl, "continue "+thread)
	info, _ := os.Stat(path)
	stoppedMidway := false
	r.waitFor("import under way", 20*time.Second, func() bool {
		srcs, err := r.st.Sources()
		if err != nil {
			return false
		}
		for _, s := range srcs {
			if s.Path == path && s.ConvID != 0 && s.Offset > 0 {
				stoppedMidway = s.Offset < info.Size()
				return true
			}
		}
		return false
	})
	r.stop()
	if !stoppedMidway {
		t.Log("the import finished before the stop; the restart only re-checks it")
	}
	r.start()
	r.wait("restarted", func() bool { return r.chatOf("s001") != "" })
	work := r.chatOf("s001")
	r.waitFor("import complete", 20*time.Second, func() bool { return len(r.own(work, "Import complete")) > 0 })
	r.wait("published", func() bool { return len(r.own(ctl, "Published local Codex session as s001")) > 0 })
	time.Sleep(time.Second)
	// The import title names the session by its first prompt, so "early
	// prompt" is counted outside the title.
	for _, s := range []string{"early prompt", "early answer", "late prompt", "late answer", "Import complete", "Imported Codex session history", "Work chat created: s001"} {
		n := len(r.own(work, s))
		if s == "early prompt" {
			n -= len(r.own(work, "Session: early prompt"))
		}
		if n != 1 {
			t.Errorf("%q posted %d times in the work chat, want 1", s, n)
		}
	}
	for _, s := range []string{"Published local Codex session as s001", "Preparing local Codex history now"} {
		if n := len(r.own(ctl, s)); n != 1 {
			t.Errorf("%q posted %d times in the control chat, want 1", s, n)
		}
	}
	if early, late := r.own(work, "early answer"), r.own(work, "late answer"); len(early) == 1 && len(late) == 1 {
		msgs := r.ch.Messages(work)
		at := func(sub string) int {
			for i, m := range msgs {
				if m.Marker != "" && strings.Contains(m.Text, sub) {
					return i
				}
			}
			return -1
		}
		if !(at("Imported Codex session history") < at("early answer") && at("late answer") < at("Import complete")) {
			t.Errorf("work chat order: title %d, early %d, late %d, complete %d", at("Imported Codex session history"), at("early answer"), at("late answer"), at("Import complete"))
		}
	}
}

// F-CTL-3: the numbered list is state with an expiry, not a timer: a
// number sent after 10 minutes gets v0.1.20's expiry answer.
func TestDashboardNumberExpires(t *testing.T) {
	var shift atomic.Int64
	r := importRig(t, func(sessions string) {
		w, err := rollout.New(sessions, t.TempDir(), rollout.Options{Start: time.Now().Add(-48 * time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.AppendTurn(rollout.Turn{Prompt: "some prompt", Answer: "some answer"}); err != nil {
			t.Fatal(err)
		}
	}, func(c *Config) { c.Now = func() time.Time { return time.Now().Add(time.Duration(shift.Load())) } })
	ctl := r.control()
	r.ch.PostUser(ctl, "p")
	r.wait("workspaces", func() bool { return len(r.own(ctl, "Workspaces")) == 1 })
	shift.Store(int64(11 * time.Minute))
	r.ch.PostUser(ctl, "1")
	r.wait("expired", func() bool { return len(r.own(ctl, "This numbered list expired")) == 1 })
	r.ch.PostUser(ctl, "1")
	r.wait("no list", func() bool { return len(r.own(ctl, "I do not have a current list yet")) == 1 })
}

func TestResolveUserPath(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home")
	}
	wd, _ := os.Getwd()
	t.Setenv("CXP_TEST_DIR_VAR", "/var/x")
	for in, want := range map[string]string{
		"/abs/dir/":            "/abs/dir",
		"~":                    home,
		"~/proj":               filepath.Join(home, "proj"),
		`"/with space/dir"`:    "/with space/dir",
		"'rel/dir'":            filepath.Join(wd, "rel/dir"),
		"`./a/../b`":           filepath.Join(wd, "b"),
		"$CXP_TEST_DIR_VAR/ok": "/var/x/ok",
	} {
		got, err := resolveUserPath(in)
		if err != nil || got != want {
			t.Errorf("resolveUserPath(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := resolveUserPath("  "); err == nil {
		t.Error("an empty path resolved")
	}
}

func TestPathHintText(t *testing.T) {
	got := pathHintText("./my project")
	want := "📁 Detected path: `./my project`\n\nChoose one:\n\n- `new \"./my project\"` - create a 💬 Work chat there\n- `mkdir \"./my project\"` - only create the directory"
	if got != want {
		t.Errorf("pathHintText = %q\nwant %q", got, want)
	}
}

// F-CTL-6 / F-CTL-5: after a session is continued, the control chat and
// the work chat keep answering (`st`, `s`, `o <n>`, `continue` again, a
// prompt in the imported chat).
func TestDashboardAfterContinueKeepsAnswering(t *testing.T) {
	var thread string
	r := importRig(t, func(sessions string) {
		w, err := rollout.New(sessions, t.TempDir(), rollout.Options{Start: time.Now().Add(-48 * time.Hour)})
		if err != nil {
			t.Fatal(err)
		}
		thread = w.SessionID
		if _, err := w.AppendTurn(rollout.Turn{Prompt: "old prompt", Answer: "old answer"}); err != nil {
			t.Fatal(err)
		}
	})
	ctl := r.control()
	step := func(text, want string) {
		t.Helper()
		n := len(r.own(ctl, want))
		r.ch.PostUser(ctl, text)
		r.wait(text, func() bool { return len(r.own(ctl, want)) > n })
	}
	step("p", "Workspaces")
	step("1", "Reply with a number to continue")
	step("1", "Published local Codex session as s001")
	work := r.chatOf("s001")
	r.wait("import complete", func() bool { return len(r.own(work, "Import complete")) > 0 })
	step("st", "Active Work chats")
	step("s", "Reply with a number to continue")
	step("o 1", "s001 [active]")
	step("continue "+thread, "No new local history was imported.")
	step("d s001", "Session: s001")
}

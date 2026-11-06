package engine

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// A takeover by another host of a shared home is announced once in the
// control chat; the first run and restarts on the same host say nothing.
func TestTakeoverNoticeOncePerHostChange(t *testing.T) {
	host, why := "host-a", "cold"
	r := newRig(t, func(c *Config) { c.Host, c.TakeoverWhy = host, why })
	ctl := r.control()
	r.stop()
	r.start()
	time.Sleep(500 * time.Millisecond)
	if n := len(r.own(ctl, "took over")); n != 0 {
		t.Fatalf("same-host restart announced a takeover (%d)", n)
	}
	r.stop()

	host, why = "host-b", "stale"
	r.start()
	r.wait("takeover notice", func() bool {
		return len(r.own(ctl, "🔁 The helper now runs on host-b: it took over from host-a, whose helper stopped answering.")) == 1
	})
	r.stop()
	r.start()
	time.Sleep(500 * time.Millisecond)
	r.stop()
	host, why = "host-a", "released"
	r.start()
	r.wait("takeover back", func() bool {
		return len(r.own(ctl, "now runs on host-a: it took over from host-b, whose helper stopped.")) == 1
	})
	if n := len(r.own(ctl, "took over")); n != 2 {
		t.Fatalf("%d takeover notices, want 2", n)
	}
}

// siblingMarkers returns the markers of another copy of the rig's store:
// our prefix, a token none of our sends used. Call it while the store is
// open.
func (r *rig) siblingMarkers() func(tok string) string {
	p, ok, err := r.st.Meta(store.MetaTurnKeyPrefix)
	if err != nil || !ok {
		r.t.Fatalf("marker prefix: %q %v %v", p, ok, err)
	}
	return func(tok string) string { return p + ".sibling-" + tok }
}

// Move guard, rule 1: the older copy stands down when another copy's
// takeover notice appears in the control chat. Its own notice, a notice
// naming this host and other sibling posts do nothing; the stop is
// recorded and survives a restart until `cxp teams start`, after which
// the notice already read does not stop it again. A copy that was not
// running when the notice was posted stands down when it starts.
func TestMoveGuardOlderCopyStandsDown(t *testing.T) {
	host := "host-a"
	r := newRig(t, func(c *Config) { c.Host = host })
	ctl := r.control()
	sib := r.siblingMarkers()
	dir := filepath.Dir(r.path)
	r.ch.PostOtherHelper(ctl, "🔧 Helper: 🔁 The helper now runs on host-a: it took over from host-z, whose helper stopped.", sib("1"))
	r.ch.PostOtherHelper(ctl, "🔧 Helper: ✅ Helper restart completed", sib("2"))
	r.ch.PostOtherHelper(ctl, "🔁 The helper now runs on host-b: it took over from host-a, whose helper stopped.", "cxp-ffff.other-store")
	r.ch.PostUser(ctl, "st")
	r.wait("status", func() bool { return len(r.own(ctl, "Control status:")) == 1 })
	if _, ok, _ := store.ReadStop(dir); ok {
		t.Fatal("stopped by a notice that does not supersede this helper")
	}

	r.ch.PostOtherHelper(ctl, "🔧 Helper: 🔁 The helper now runs on host-b: it took over from host-a, whose helper stopped answering.", sib("3"))
	posts := len(r.ch.Messages(ctl))
	if err := r.exited(); !errors.Is(err, store.ErrDisabled) {
		t.Fatalf("Run returned %v, want ErrDisabled", err)
	}
	rec, ok, err := store.ReadStop(dir)
	if err != nil || !ok || rec.Source != store.StopBySuperseded || rec.Host != "host-b" {
		t.Fatalf("stop record %+v %v %v", rec, ok, err)
	}
	if got := rec.Describe(store.DisabledPath(dir)); !strings.Contains(got, "the helper now runs on host-b") || !strings.HasSuffix(got, "Start again with `cxp teams start`.") {
		t.Fatalf("describe: %q", got)
	}
	if n := len(r.ch.Messages(ctl)); n != posts {
		t.Fatalf("%d posts after standing down", n-posts)
	}

	// `cxp teams start`: the notice was read, it does not stop it again.
	if _, err := store.RemoveStop(dir); err != nil {
		t.Fatal(err)
	}
	r.start()
	r.ch.PostUser(ctl, "st")
	r.wait("status after start", func() bool { return len(r.own(ctl, "Control status:")) == 2 })
	r.stop()

	// Not running when the other copy took over: it stands down at start.
	r.ch.PostOtherHelper(ctl, "🔧 Helper: 🔁 The helper now runs on host-c: it took over from host-a, whose helper stopped.", sib("4"))
	r.start()
	if err := r.exited(); !errors.Is(err, store.ErrDisabled) {
		t.Fatalf("Run returned %v, want ErrDisabled", err)
	}
	if rec, _, _ := store.ReadStop(dir); rec.Host != "host-c" {
		t.Fatalf("stop record %+v", rec)
	}
}

// Move guard, rule 2: after taking over by a copy, the first read of each
// chat closes the requests the other copy took (followed by one of its
// posts) and lists them in one notice; a request after its last post, and
// a chat it never posted in, are answered normally, and later reads change
// nothing.
func TestMoveGuardNewCopySkipsTakenRequests(t *testing.T) {
	host := "host-a"
	r := newRig(t, func(c *Config) { c.Host, c.TakeoverWhy = host, "stale" })
	ctl := r.control()
	work := r.newWork(t.TempDir())
	quiet := r.newWork(t.TempDir())
	sib := r.siblingMarkers()
	r.ch.PostUser(work, "before the copy")
	r.wait("answer", func() bool { return len(r.own(work, "ANSWER to before the copy")) == 1 })
	time.Sleep(100 * time.Millisecond)
	r.stop()

	// The old copy goes on: it answers two prompts and a status command.
	r.ch.PostUser(work, "taken one")
	r.ch.PostOtherHelper(work, "🔧 Helper: ⏳ Codex is working. Request accepted.", sib("1"))
	r.ch.PostUser(work, "taken two")
	r.ch.PostUser(ctl, "st")
	r.ch.PostOtherHelper(ctl, "🔧 Helper: old copy status", sib("2"))
	r.ch.PostOtherHelper(work, "🤖 Codex: ANSWER to taken two", sib("3"))
	r.ch.PostUser(work, "after it stopped")
	r.ch.PostUser(quiet, "gap prompt")

	host = "host-b"
	r.start()
	r.wait("answers", func() bool {
		return len(r.own(work, "ANSWER to after it stopped")) == 1 && len(r.own(quiet, "ANSWER to gap prompt")) == 1
	})
	r.wait("notices", func() bool { return len(r.own(work, "Not run here")) == 1 && len(r.own(ctl, "Not run here")) == 1 })
	r.ch.PostUser(work, "live prompt")
	r.ch.PostOtherHelper(work, "🔧 Helper: something", sib("4"))
	r.wait("live answer", func() bool { return len(r.own(work, "ANSWER to live prompt")) == 1 })
	time.Sleep(300 * time.Millisecond)
	for _, p := range []string{"taken one", "taken two"} {
		if n := r.ag.calls(p); n != 0 {
			t.Fatalf("%q ran %d times", p, n)
		}
	}
	for _, p := range []string{"before the copy", "after it stopped", "gap prompt", "live prompt"} {
		if n := r.ag.calls(p); n != 1 {
			t.Fatalf("%q ran %d times", p, n)
		}
	}
	note := r.own(work, "Not run here")[0].Text
	want := "⏭️ Not run here: the helper on host-a already took these requests after its state was copied to this machine. If one of them got no answer, send it again.\n\n- taken one\n- taken two"
	if !strings.Contains(note, want) {
		t.Fatalf("notice %q", note)
	}
	if n := len(r.own(ctl, "Control status:")); n != 0 { // the old copy answered it
		t.Fatalf("status answered %d times", n)
	}
	if !strings.HasSuffix(r.own(ctl, "Not run here")[0].Text, "\n- st") {
		t.Fatalf("control notice %q", r.own(ctl, "Not run here")[0].Text)
	}
	if n := len(r.own(quiet, "Not run here")); n != 0 {
		t.Fatalf("%d notices in an untouched chat", n)
	}
}

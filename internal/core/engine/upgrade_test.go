package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// fakeUpdater offers tag (once set) and counts installs.
type fakeUpdater struct {
	mu         sync.Mutex
	tag        string
	installs   []string
	checks     int
	manual     int
	installErr error
}

func (u *fakeUpdater) Check(_ context.Context, _, manual bool) (UpdateOffer, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.checks++
	if manual {
		u.manual++
	}
	return UpdateOffer{Tag: u.tag}, nil
}

func (u *fakeUpdater) Install(_ context.Context, o UpdateOffer) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.installs = append(u.installs, o.Tag)
	return u.installErr
}

func (u *fakeUpdater) set(tag string) {
	u.mu.Lock()
	u.tag = tag
	u.mu.Unlock()
}

func (u *fakeUpdater) installed() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.installs)
}

type fakeCodexUpgrader struct {
	mu    sync.Mutex
	n     int
	err   error
	after func()
}

func (c *fakeCodexUpgrader) Upgrade(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	if c.after != nil {
		c.after()
	}
	return c.err
}

func (c *fakeCodexUpgrader) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// exited waits until Run returned (an ErrRestart) and releases the store,
// as the service would before starting the next process.
func (r *rig) exited() error {
	r.t.Helper()
	select {
	case err := <-r.done:
		r.cancel()
		r.st.Release()
		r.st.Close()
		r.cancel = nil
		return err
	case <-time.After(10 * time.Second):
		r.t.Fatal("the engine did not exit")
		return nil
	}
}

func (r *rig) logHas(sub string) bool {
	r.logMu.Lock()
	defer r.logMu.Unlock()
	return strings.Contains(r.log.String(), sub)
}

// An automatic helper update waits for the running turn (and holds new
// ones) without interrupting it, installs, restarts, and the next process
// announces it exactly once (R-16).
func TestHelperUpdateDrainsInstallsRestartsAndAnnouncesOnce(t *testing.T) {
	up := &fakeUpdater{}
	var mu sync.Mutex
	version := "v1.0.0"
	r := newRig(t, func(c *Config) {
		mu.Lock()
		c.Version = version
		mu.Unlock()
		c.Updater = up
		c.Timing.UpdateCheck = 100 * time.Millisecond
	})
	work := r.newWork(t.TempDir())
	other := r.newWork(t.TempDir())
	release := make(chan struct{})
	r.ag.mu.Lock()
	r.ag.block["long"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long job")
	r.wait("running", func() bool { return r.ag.calls("long") == 1 })

	up.set("v2.0.0")
	r.wait("drain", func() bool { return r.logHas("update: v2.0.0 waits for running turns") })
	r.ch.PostUser(other, "other job")
	time.Sleep(300 * time.Millisecond)
	if r.ag.calls("other job") != 0 {
		t.Fatal("a turn started while the update drained")
	}
	if up.installed() != 0 {
		t.Fatal("installed while a turn was running")
	}
	close(release)
	if err := r.exited(); !errors.Is(err, ErrRestart) {
		t.Fatalf("exit = %v, want ErrRestart", err)
	}
	if up.installed() != 1 {
		t.Fatalf("installs = %d", up.installed())
	}

	// A release build reports its commit and build date after the
	// version (cli's buildVersion), as the field report showed.
	mu.Lock()
	version = "2.0.0 (771b7c5965b13d3a4bed432a143640f0e4b2e7f0) 2026-11-07T12:05:26Z"
	mu.Unlock()
	r.start()
	ctl := r.control()
	r.wait("completion", func() bool { return len(r.own(ctl, "Helper update completed")) == 1 })
	r.wait("long answer", func() bool { return len(r.own(work, "ANSWER to long job")) == 1 })
	r.wait("held turn runs after the restart", func() bool { return len(r.own(other, "ANSWER to other job")) == 1 })
	// The held turn is started by the new process, not taken for a turn
	// the old one left open: no "did not run" notice while it runs (the
	// reconciler used to read the open turns after the runner started).
	if n := len(r.own(other, "did not run")); n != 0 || r.logHas("mark running") {
		t.Fatalf("held turn reported not run (%d notices) while it ran", n)
	}
	r.stop()
	r.start()
	ctl = r.control()
	time.Sleep(400 * time.Millisecond)
	if n := len(r.own(ctl, "Helper update completed")); n != 1 {
		t.Fatalf("%d completion notices", n)
	}
	if n := len(r.own(work, "interrupted")); n != 0 {
		t.Fatal("the update interrupted a turn")
	}
	if !strings.Contains(r.own(ctl, "Helper update completed")[0].Text, "`v2.0.0`") {
		t.Fatalf("notice %q", r.own(ctl, "Helper update completed")[0].Text)
	}
	if up.installed() != 1 {
		t.Fatalf("installs = %d after the update", up.installed())
	}
	if s, err := ReadStoreStatus(r.st); err != nil || !sameVersion(s.RunningVersion, "v2.0.0") || s.UpdateInstalled != "" || s.UpdateScheduled != "" {
		t.Fatalf("store status %+v %v", s, err)
	}
}

// When the restarted helper does not run the target version, the
// activation failure is reported once and automatic checks stop offering
// that release.
func TestHelperUpdateActivationFailureReportedOnce(t *testing.T) {
	up := &fakeUpdater{tag: "v2.0.0"}
	r := newRig(t, func(c *Config) {
		c.Version = "v1.0.0"
		c.Updater = up
		c.Timing.UpdateCheck = 100 * time.Millisecond
	})
	if err := r.exited(); !errors.Is(err, ErrRestart) {
		t.Fatalf("exit = %v", err)
	}
	if st, err := store.Open(r.path); err != nil {
		t.Fatal(err)
	} else {
		s, err := ReadStoreStatus(st)
		st.Close()
		if err != nil || s.UpdateInstalled != "v2.0.0" || s.RunningVersion != "v1.0.0" {
			t.Fatalf("store status before the restart %+v %v", s, err)
		}
	}
	r.start()
	ctl := r.control()
	r.wait("activation failure", func() bool { return len(r.own(ctl, "Helper update activation failed")) == 1 })
	time.Sleep(500 * time.Millisecond)
	if up.installed() != 1 {
		t.Fatalf("installs = %d, want 1 (the failed release is skipped)", up.installed())
	}
	if n := len(r.own(ctl, "Helper update activation failed")); n != 1 {
		t.Fatalf("%d failure notices", n)
	}
	if txt := r.own(ctl, "activation failed")[0].Text; !strings.Contains(txt, "`v1.0.0`") || !strings.Contains(txt, "`v2.0.0`") {
		t.Fatalf("notice %q", txt)
	}
}

// `helper update now` posts the schedule; a turn that outlives the drain
// bound is not interrupted, the update is not installed and the command
// is told once; held turns then run.
func TestManualHelperUpdateGivesUpAfterDrainBound(t *testing.T) {
	up := &fakeUpdater{}
	r := newRig(t, func(c *Config) {
		c.Version = "v1.0.0"
		c.Updater = up
		c.Timing.UpdateDrain = 400 * time.Millisecond
	})
	work := r.newWork(t.TempDir())
	other := r.newWork(t.TempDir())
	release := make(chan struct{})
	defer close(release)
	r.ag.mu.Lock()
	r.ag.block["long"] = release
	r.ag.mu.Unlock()
	r.ch.PostUser(work, "long job")
	r.wait("running", func() bool { return r.ag.calls("long") == 1 })
	ctl := r.control()
	r.ch.PostUser(ctl, "helper update now")
	r.wait("up to date", func() bool { return len(r.own(ctl, "already on the latest stable release")) == 1 })
	up.set("v2.0.0")
	r.ch.PostUser(ctl, "update now")
	r.wait("scheduled", func() bool { return len(r.own(ctl, "Helper update scheduled")) == 1 })
	r.ch.PostUser(other, "other job")
	r.wait("gave up", func() bool { return len(r.own(ctl, "Helper update failed")) == 1 })
	r.wait("held turn runs", func() bool { return len(r.own(other, "ANSWER to other job")) == 1 })
	if up.installed() != 0 || r.ag.calls("long") != 1 || len(r.own(work, "interrupted")) != 0 {
		t.Fatalf("installs %d, long runs %d", up.installed(), r.ag.calls("long"))
	}
	if txt := r.own(ctl, "Helper update failed")[0].Text; !strings.Contains(txt, "still running") {
		t.Fatalf("failure %q", txt)
	}
}

const tooOld = "The 'gpt-9' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again."

// Codex rejecting a request as too old: one notice in the work chat, the
// CLI upgraded after running turns end, the outcome posted to the control
// and the work chat, a restart, and `helper retry last` runs the request
// again.
func TestCodexTooOldUpgradesAndRetryLast(t *testing.T) {
	var mu sync.Mutex
	fixed := false
	cu := &fakeCodexUpgrader{after: func() { mu.Lock(); fixed = true; mu.Unlock() }}
	r := newRig(t, func(c *Config) { c.CodexUpgrader = cu })
	r.ag.mu.Lock()
	r.ag.outcome = func(req TurnRequest) (TurnOutcome, error) {
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(req.Text, "gpt-9 task") && !fixed {
			return TurnOutcome{Status: StatusFailed, Error: tooOld}, nil
		}
		return TurnOutcome{Status: StatusCompleted, Final: "ANSWER to " + strings.SplitN(req.Text, "\n", 2)[0]}, nil
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "gpt-9 task")
	// The upgrade runs at once (nothing else runs) and restarts; the
	// notices are sent by whichever process is running.
	if err := r.exited(); !errors.Is(err, ErrRestart) {
		t.Fatalf("exit = %v", err)
	}
	if cu.calls() != 1 {
		t.Fatalf("upgrades = %d", cu.calls())
	}
	r.start()
	ctl := r.control()
	r.wait("too-old notice", func() bool { return len(r.own(work, "Codex CLI needs an update")) == 1 })
	if txt := r.own(work, "Codex CLI needs an update")[0].Text; !strings.Contains(txt, "I will upgrade Codex") {
		t.Fatalf("notice %q", txt)
	}
	r.wait("control outcome", func() bool { return len(r.own(ctl, "Codex CLI upgraded")) == 1 })
	r.wait("work outcome", func() bool { return len(r.own(work, "Retry this request with `helper retry last`")) == 1 })
	if !strings.Contains(r.own(ctl, "Codex CLI upgraded")[0].Text, "helper retry last") {
		t.Fatal("control notice without the retry hint")
	}
	r.ch.PostUser(work, "helper retry last")
	r.wait("retried", func() bool { return len(r.own(work, "ANSWER to gpt-9 task")) == 1 })
	time.Sleep(300 * time.Millisecond)
	if cu.calls() != 1 || len(r.own(ctl, "Codex CLI upgraded")) != 1 || len(r.own(work, "Codex CLI needs an update")) != 1 {
		t.Fatalf("upgrades %d", cu.calls())
	}
}

// Without an upgrader the notice says an administrator updates Codex, and
// nothing restarts; `helper retry last` still works.
func TestCodexTooOldWithoutUpgrader(t *testing.T) {
	r := newRig(t)
	r.ag.mu.Lock()
	r.ag.outcome = func(req TurnRequest) (TurnOutcome, error) {
		return TurnOutcome{Status: StatusFailed, Error: tooOld}, nil
	}
	r.ag.mu.Unlock()
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "gpt-9 task")
	r.wait("notice", func() bool { return len(r.own(work, "administrator needs to update Codex")) == 1 })
	r.ch.PostUser(work, "helper retry last")
	r.wait("retried", func() bool { return r.ag.calls("gpt-9 task") == 2 })
}

// `codex update now` from the control chat; a failed upgrade is reported
// once and nothing restarts.
func TestCodexUpdateCommand(t *testing.T) {
	cu := &fakeCodexUpgrader{err: errors.New("npm failed in /usr/lib/node_modules/@openai/codex")}
	r := newRig(t, func(c *Config) { c.CodexUpgrader = cu })
	ctl := r.control()
	r.ch.PostUser(ctl, "codex update")
	r.wait("help", func() bool { return len(r.own(ctl, "Codex CLI update")) == 1 })
	r.ch.PostUser(ctl, "codex update now")
	r.wait("failure", func() bool { return len(r.own(ctl, "Codex CLI upgrade failed")) == 1 })
	if txt := r.own(ctl, "Codex CLI upgrade failed")[0].Text; strings.Contains(txt, "/usr/lib") {
		t.Fatalf("path in notice: %q", txt)
	}
	if len(r.own(ctl, "Codex CLI update scheduled")) != 1 {
		t.Fatal("no schedule notice")
	}
	work := r.newWork(t.TempDir())
	r.ch.PostUser(work, "after")
	r.wait("turns run again", func() bool { return len(r.own(work, "ANSWER to after")) == 1 })

	cu.mu.Lock()
	cu.err = nil
	cu.mu.Unlock()
	r.ch.PostUser(ctl, "codex upgrade now")
	if err := r.exited(); !errors.Is(err, ErrRestart) {
		t.Fatalf("exit = %v", err)
	}
	r.start()
	r.wait("done", func() bool { return len(r.own(ctl, "New Teams requests will use the updated Codex CLI")) == 1 })
}

func TestCodexTooOldMatches(t *testing.T) {
	for _, s := range []string{tooOld, "codex: this model requires a newer version", "Codex CLI too old: upgrade the cli for this model"} {
		if !codexTooOld(s) {
			t.Errorf("not matched: %q", s)
		}
	}
	for _, s := range []string{"rate limited", "requires a newer version of node", ""} {
		if codexTooOld(s) {
			t.Errorf("matched: %q", s)
		}
	}
	for arg, want := range map[string][3]bool{"now": {false, false, true}, "prerelease": {true, false, true}, "status": {false, true, true}, "": {}, "now please": {}} {
		p, s, ok := parseUpdateArg(arg)
		if [3]bool{p, s, ok} != want {
			t.Errorf("parseUpdateArg(%q) = %v %v %v", arg, p, s, ok)
		}
	}
}

// sameVersion reads a release build's version string (cli's buildVersion:
// "<version> (<commit>) <date>") as its version.
func TestSameVersionReadsBuildVersionStrings(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"v0.1.22-rc.68", "0.1.22-rc.68 (771b7c5965b13d3a4bed432a143640f0e4b2e7f0) 2026-11-07T12:05:26Z", true},
		{"v0.1.22-rc.68", "0.1.22-rc.68", true},
		{"0.1.22-rc.68 (abc) 2026-11-07", "v0.1.22-rc.68", true},
		{"v0.1.22-rc.69", "0.1.22-rc.68 (771b7c5965b13d3a4bed432a143640f0e4b2e7f0) 2026-11-07T12:05:26Z", false},
		{"v0.1.22-rc.6", "0.1.22-rc.68", false},
		{"", "", false},
		{"  ", "v1", false},
	}
	for _, c := range cases {
		if got := sameVersion(c.a, c.b); got != c.want {
			t.Errorf("sameVersion(%q, %q) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

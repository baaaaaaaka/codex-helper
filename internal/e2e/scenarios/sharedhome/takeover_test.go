package sharedhome

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Takeover scenarios (owner 2026-11-05: automatic takeover, made robust).
// The lease's stale bound is shortened to takeoverStale (30 s); production
// timing is STANDBY-TAKEOVER's.

// The owner's process is killed. A standby on another host cannot see that
// process; it takes over once the lease has not been renewed for the stale
// bound, announces it once, and a prompt posted while no helper ran is
// answered once. Status on the old host names the new owner.
func TestTakeoverAfterOwnerKilled(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-KILLED", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1"},
		Title:      "shared home: the owner is killed; the standby on another host takes over within the stale bound plus the quiet wait, announces it once and answers a prompt posted meanwhile once",
		Budget:     150 * time.Second,
		Seam:       takeoverSeam("host-a"),
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "killed")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b"), Args: serviceChild})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	s.Settle(2*time.Second, 15*time.Second)
	s.CrashSUT()
	killedAt := time.Now()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After A was killed "+held)
	_, ok := waitServing(s, b, 3*takeoverStale)
	took := time.Since(killedAt).Round(time.Second)
	limit := takeoverStale + takeoverPoll + takeoverQuiet + 15*time.Second
	s.Check("B takes over within the stale bound plus the quiet wait", ok && took >= takeoverStale-takeoverStale/9 && took <= limit,
		fmt.Sprintf("B serves %s to %s after the kill", takeoverStale-takeoverStale/9, limit), fmt.Sprintf("serving=%v after %s; output %s", ok, took, clipTail(b.Output(), 600)))
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("the prompt posted while no helper ran is answered by B", byB, "answered by B", fmt.Sprint(byB), s.Transcript(work.ID, 12)...)
	exactlyOnce(s, work.ID, held)
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the new host", len(notices) == 1 && strings.Contains(notices[0], "host-b"),
		"1 notice naming host-b", fmt.Sprintf("%d: %q", len(notices), notices))
	st := s.RunCLIAs(20*time.Second, harness.CLIAs{Hostname: "host-a", Poll: true}, "teams", "status")
	s.Check("status on the old host names the new owner and does not open its store", st.Code == 0 && strings.Contains(st.Combined, "Owner: host-b"),
		"exit 0, Owner: host-b", fmt.Sprintf("exit %d; %s", st.Code, clipTail(st.Combined, 600)))
}

// The owner hangs for less than the stale bound and resumes: it keeps the
// state, answers what was posted meanwhile, and the standby never takes
// over.
func TestOwnerThawedBeforeStaleKeepsTheState(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-THAW-EARLY", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1"},
		Title:      "shared home: an owner that hangs for less than the stale bound keeps the state; the standby never takes over",
		Budget:     150 * time.Second,
		Seam:       takeoverSeam("host-a"),
	})
	defer s.Finish()
	t.Cleanup(func() { harness.Thaw(s.SUT) })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "thaw")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b"), Args: serviceChild})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	s.Settle(2*time.Second, 15*time.Second)
	if !harness.Freeze(s.SUT) {
		t.Fatal("cannot freeze host A")
	}
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While A hangs "+held)
	time.Sleep(takeoverStale / 2)
	harness.Thaw(s.SUT)
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	s.Check("A answers the prompt posted while it hung", heldAnswered, "answered", fmt.Sprint(heldAnswered))
	// Longer than a takeover would take.
	time.Sleep(takeoverStale + takeoverPoll + takeoverQuiet)
	bReqs := peerRequests(s, bFrom, "-B-")
	s.Check("the standby never takes over a resumed owner", !serving(b) && b.Running() && s.SUT.Running() && len(bReqs) == 0,
		"A running, B standing by with no Graph request", fmt.Sprintf("A running=%v; B serving=%v running=%v requests=%d; output %s",
			s.SUT.Running(), serving(b), b.Running(), len(bReqs), clipTail(b.Output(), 400)))
	s.Check("no takeover notice", len(takeoverNotices(s, control.ID)) == 0, "0 notices", fmt.Sprint(len(takeoverNotices(s, control.ID))))
	exactlyOnce(s, work.ID, held)
}

// Two hosts' services start at the same moment on a home with no state:
// exactly one serves, the other stands by without Graph traffic.
func TestSimultaneousStartOneOwner(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-SIMULTANEOUS", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1"},
		Title:      "shared home: two hosts' services start at once; exactly one serves, the other stands by without Graph traffic",
		Budget:     90 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Args:       serviceChild,
		NoStart:    true,
	})
	defer s.Finish()
	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b")})
	go func() { _ = b.Start(s.Ctx()) }()
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	s.Settle(2*time.Second, 15*time.Second)
	a, bb := serving(s.SUT), serving(b)
	s.Check("exactly one host serves", a != bb && s.SUT.Running() && b.Running(), "one serving, both running",
		fmt.Sprintf("A serving=%v running=%v; B serving=%v running=%v", a, s.SUT.Running(), bb, b.Running()),
		clipTail(s.SUT.Output(), 400), clipTail(b.Output(), 400))
	standby, tag := b, "-B-"
	if bb {
		standby, tag = s.SUT, s.SUT.Instance()
	}
	s.Check("the other stands by", strings.Contains(standby.Output(), "standing by"), "standing by", clipTail(standby.Output(), 300))
	reqs := peerRequests(s, 0, tag)
	s.Check("the standby sends no Graph request", len(reqs) == 0, "0 requests", fmt.Sprintf("%d: %q", len(reqs), reqs))
	work, _ := steps.NewWorkChat(s, control, "simultaneous")
	held := s.Token("held")
	steps.MustPost(s, work.ID, "One owner answers "+held)
	ok := s.Poll(steps.AnswerTimeout, func() bool { return len(steps.AnswerPosts(s, work.ID, held)) > 0 })
	s.Check("the owner answers", ok, "answered", fmt.Sprint(ok))
	exactlyOnce(s, work.ID, held)
	s.Check("no takeover notice", len(takeoverNotices(s, control.ID)) == 0, "0 notices", fmt.Sprint(len(takeoverNotices(s, control.ID))))
}

// A second helper on the owner's own host: a foreground start exits at
// once, a service-run one stands by and takes over in place as soon as the
// owner stops (no wait, no copy, no notice).
func TestSameHostSecondHelper(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-SAME-HOST", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1", "R-13"},
		Title:      "one host, two helpers: a foreground start exits at once, a service-run one stands by and takes over in place without waiting when the owner stops",
		Budget:     90 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `^cxp-F-\d+ exited on its own with "exit status 1"$`,
				Reason: "the foreground start next to a live owner is refused with an error"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "samehost")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("the owner answers", answered, "answered", fmt.Sprint(answered))

	f := s.NewPeer("F", harness.PeerOptions{Seam: takeoverSeam("host-a")})
	t0 := time.Now()
	if err := f.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	fExited := s.Poll(10*time.Second, func() bool { return !f.Running() })
	fAfter := time.Since(t0)
	s.Check("a foreground start on the owner's host exits at once", fExited && fAfter < 5*time.Second && strings.Contains(strings.ToLower(f.Output()), "live"),
		"exits within 5 s naming the live owner", fmt.Sprintf("exited=%v after %s; output %s", fExited, fAfter.Round(100*time.Millisecond), clipTail(f.Output(), 300)))

	seam := takeoverSeam("host-a")
	seam.Knobs["RefusalRecheck"] = "500ms"
	b := s.NewPeer("B", harness.PeerOptions{Seam: seam, Args: serviceChild})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	standing := s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	s.Check("a service-run second helper stands by", standing && b.Running(), "running, standing by", clipTail(b.Output(), 300))
	quiet := peerRequests(s, bFrom, "-B-")
	s.Check("it sends no Graph request", len(quiet) == 0, "0 requests", fmt.Sprintf("%d: %q", len(quiet), quiet))

	s.StopSUT()
	stoppedAt := time.Now()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After the owner stopped "+held)
	_, ok := waitServing(s, b, 20*time.Second)
	took := time.Since(stoppedAt)
	s.Check("it takes over in place right after the owner stops", ok && took < 5*time.Second && !strings.Contains(b.Output(), "copying the store") && !strings.Contains(b.Output(), "taking over generation"),
		"serves within 5 s, no wait, no copy", fmt.Sprintf("serving=%v after %s; output %s", ok, took.Round(100*time.Millisecond), clipTail(b.Output(), 400)))
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("the new helper answers", byB, "answered by B", fmt.Sprint(byB))
	exactlyOnce(s, work.ID, held)
	s.Check("no takeover notice on the same host", len(takeoverNotices(s, control.ID)) == 0, "0 notices", fmt.Sprint(len(takeoverNotices(s, control.ID))))
}

// A batch-job host (SLURM_JOB_ID set) leaves the takeover to an
// interactive host even when it noticed the stale owner first. Both
// standbys check every 500 ms, so the batch host, watching from before the
// hang, finds the owner stale before the interactive host, started 5 s
// after the hang, can; it defers and the interactive host takes over.
func TestBatchHostDefersToInteractive(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-BATCH", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1"},
		Title:      "shared home: when the owner hangs, an interactive standby takes over before a batch-job standby that noticed first",
		Budget:     180 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the resumed old owner is fenced and exits with an error"},
		}},
	})
	defer s.Finish()
	t.Cleanup(func() { harness.Thaw(s.SUT) })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "batch")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	fast := func(host string) harness.Seam {
		seam := takeoverSeam(host)
		seam.Knobs["RefusalRecheck"] = "500ms"
		return seam
	}
	batch := s.NewPeer("B", harness.PeerOptions{Seam: fast("node-17"), Args: serviceChild,
		UserEnv: map[string]string{"SLURM_JOB_ID": "4242"}})
	bFrom := s.Graph.LastSeq()
	if err := batch.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	s.Poll(10*time.Second, func() bool { return strings.Contains(batch.Output(), "standing by") })
	s.Settle(2*time.Second, 15*time.Second)
	if !harness.Freeze(s.SUT) {
		t.Fatal("cannot freeze host A")
	}
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After A hangs "+held)
	time.Sleep(5 * time.Second)
	inter := s.NewPeer("C", harness.PeerOptions{Seam: fast("host-c"), Args: serviceChild})
	if err := inter.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	_, ok := waitServing(s, inter, 3*takeoverStale)
	s.Check("the interactive host takes over", ok, "C serves", fmt.Sprintf("serving=%v; output %s", ok, clipTail(inter.Output(), 400)))
	byC := answeredBy(s, work.ID, held, "-C-", steps.AnswerTimeout)
	s.Check("the interactive host answers", byC, "answered by C", fmt.Sprint(byC))
	bReqs := peerRequests(s, bFrom, "-B-")
	s.Check("the batch-job host noticed first, deferred and stands by, with no Graph request", !serving(batch) && batch.Running() &&
		strings.Contains(batch.Output(), "batch-job host") && len(bReqs) == 0,
		"B running, deferred, never served, 0 requests", fmt.Sprintf("serving=%v running=%v requests=%d; output %s",
			serving(batch), batch.Running(), len(bReqs), clipTail(batch.Output(), 600)))
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the interactive host", len(notices) == 1 && strings.Contains(notices[0], "host-c"),
		"1 notice naming host-c", fmt.Sprintf("%d: %q", len(notices), notices))
	checkFencedAndSilent(s, s.SUT)
	exactlyOnce(s, work.ID, held)
}

// The lease left by a killed owner: a restart on the same host serves at
// once (its predecessor is a dead process there); a first start on another
// host waits for the stale bound and takes over.
func TestLeftoverLeaseAtColdStart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-LEFTOVER", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1", "R-13"},
		Title:      "shared home: a killed owner's lease does not block: a restart on its host serves at once, a start on another host takes over after the stale bound",
		Budget:     180 * time.Second,
		Seam:       takeoverSeam("host-a"),
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "leftover")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	s.Settle(2*time.Second, 15*time.Second)
	s.CrashSUT()
	restarted := time.Now()
	s.StartSUT()
	_, ok := waitServing(s, s.SUT, 30*time.Second)
	took := time.Since(restarted)
	s.Check("a restart on the killed owner's host serves at once", ok && took < 8*time.Second && !strings.Contains(s.SUT.Output(), "taking over generation"),
		"serves within 8 s, no wait", fmt.Sprintf("serving=%v after %s; output %s", ok, took.Round(100*time.Millisecond), clipTail(s.SUT.Output(), 400)))
	again := s.Token("again")
	steps.MustPost(s, work.ID, "After the restart "+again)
	_, ok = steps.WaitAnswered(s, work.ID, again, steps.AnswerTimeout)
	s.Check("the restarted owner answers", ok, "answered", fmt.Sprint(ok))

	s.Settle(2*time.Second, 15*time.Second)
	s.CrashSUT()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After A was killed again "+held)
	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b")})
	started := time.Now()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	_, ok = waitServing(s, b, 3*takeoverStale)
	took = time.Since(started).Round(time.Second)
	limit := takeoverStale + takeoverPoll + takeoverQuiet + 15*time.Second
	s.Check("a foreground start on another host says it waits, then takes over after the stale bound",
		ok && took >= takeoverStale && took <= limit && strings.Contains(b.Output(), "Teams helper waiting"),
		fmt.Sprintf("serves %s to %s after its start", takeoverStale, limit), fmt.Sprintf("serving=%v after %s; output %s", ok, took, clipTail(b.Output(), 600)))
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("B answers the prompt posted after the kill", byB, "answered by B", fmt.Sprint(byB))
	exactlyOnce(s, work.ID, held)
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the new host", len(notices) == 1 && strings.Contains(notices[0], "host-b"),
		"1 notice naming host-b", fmt.Sprintf("%d: %q", len(notices), notices))
}

// A standby dies in the middle of its takeover (after it claimed the
// lease, before it copied the store). Its claim does not block: another
// host takes over once the claim has not been renewed for the stale bound,
// from the last owner's store.
func TestCrashMidTakeover(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TAKEOVER-CRASH-MID", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1", "R-13"},
		Title:      "shared home: a standby killed in the middle of a takeover leaves no state that blocks: a third host takes over from the last owner's store",
		Budget:     240 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the resumed old owner is fenced and exits with an error"},
		}},
	})
	defer s.Finish()
	t.Cleanup(func() { harness.Thaw(s.SUT) })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "crashmid")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b"), Args: serviceChild})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	s.Settle(2*time.Second, 15*time.Second)
	if !harness.Freeze(s.SUT) {
		t.Fatal("cannot freeze host A")
	}
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After A hangs "+held)
	claimed := s.Poll(3*takeoverStale, func() bool { return strings.Contains(b.Output(), "taking over generation") })
	if err := b.Crash(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	s.Check("B claims the lease and is killed before it serves", claimed && !serving(b), "claimed, not serving",
		fmt.Sprintf("claimed=%v serving=%v; output %s", claimed, serving(b), clipTail(b.Output(), 400)))

	c := s.NewPeer("C", harness.PeerOptions{Seam: takeoverSeam("host-c"), Args: serviceChild})
	started := time.Now()
	if err := c.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	_, ok := waitServing(s, c, 3*takeoverStale)
	took := time.Since(started).Round(time.Second)
	limit := takeoverStale + takeoverPoll + takeoverQuiet + 15*time.Second
	s.Check("C takes over after B's claim went stale", ok && took >= takeoverStale && took <= limit,
		fmt.Sprintf("C serves %s to %s after its start", takeoverStale, limit), fmt.Sprintf("serving=%v after %s; output %s", ok, took, clipTail(c.Output(), 600)))
	byC := answeredBy(s, work.ID, held, "-C-", steps.AnswerTimeout)
	s.Check("C answers in the existing work chat", byC, "answered by C", fmt.Sprint(byC), s.Transcript(work.ID, 12)...)
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the new host", len(notices) == 1 && strings.Contains(notices[0], "host-c"),
		"1 notice naming host-c", fmt.Sprintf("%d: %q", len(notices), notices))
	checkFencedAndSilent(s, s.SUT)
	exactlyOnce(s, work.ID, held)
}

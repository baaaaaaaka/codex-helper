package sharedhome

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Shared home, the owner frozen inside a store write (F-XM-1, R-13). A
// helper stopped inside a write transaction keeps SQLite's write lock: no
// other process can write the store, so no standby can take over, until
// that process resumes or exits. Nothing can break the lock. What the user
// gets instead: the standby (a service child) reports once that the owner
// stopped while holding the lock, keeps waiting in-process with no Graph
// traffic and next to no CPU, never exits to be restarted, and takes over
// as soon as the lock is gone.
//
// The harness stands in for the frozen owner's open transaction: it freezes
// host A and holds the store's write lock itself (HoldStoreWriteLock); to
// every other process that is the same state. Releasing it is the frozen
// process going away. A stays frozen until B owns the state; then it
// resumes, is fenced, and posts nothing.
func TestStandbyWaitsOutAFrozenWriter(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "STANDBY-LOCKED", harness.Options{
		Guarantees: []string{"F-XM-1", "R-13", "R-1"},
		Title:      "shared home: a standby facing an owner frozen inside a store write reports it once and waits in-process without Graph traffic, takes over once the write lock is gone, and the old owner posts nothing",
		Budget:     240 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"Heartbeat": "500ms"}, Hostname: "host-a"},
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the resumed old owner is fenced and exits with an error"},
		}},
	})
	defer s.Finish()
	t.Cleanup(func() { harness.Thaw(s.SUT) })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "locked")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))
	aTag := s.SUT.Instance()
	store := s.StorePath()
	if store == "" {
		t.Fatal("no store under " + s.Env.Root)
	}

	b := s.NewPeer("B", harness.PeerOptions{
		Seam: harness.Seam{Hostname: "host-b", Knobs: map[string]string{"RefusalRecheck": "500ms"}},
		Args: []string{"--managed-service-child"},
	})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	standing := s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	s.Check("B stands by while A is live", standing && b.Running(), "B running, standing by",
		fmt.Sprintf("running=%v; output %s", b.Running(), clipTail(b.Output(), 300)))

	// A freezes inside a store write.
	s.Settle(2*time.Second, 15*time.Second)
	release, err := harness.HoldStoreWriteLock(store, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	if !harness.Freeze(s.SUT) {
		t.Fatal("cannot freeze host A")
	}
	frozenAt := time.Now()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While A is frozen "+held)

	// Once A's heartbeat is stale B would take over, but the lock stops it:
	// it says so once and keeps waiting.
	const frozenWord = "frozen?"
	reported := s.Poll(150*time.Second, func() bool { return strings.Contains(b.Output(), frozenWord) })
	reportedAfter := time.Since(frozenAt).Round(time.Second)
	cpu, measured := b.(interface{ CPUTime() time.Duration })
	var cpu0 time.Duration
	if measured {
		cpu0 = cpu.CPUTime()
	}
	waitFrom := time.Now()
	time.Sleep(15 * time.Second) // a few more checks, each refused
	var used time.Duration
	if measured {
		used = cpu.CPUTime() - cpu0
	}
	waited := time.Since(waitFrom)
	bOut := b.Output()
	s.Check("B reports the frozen owner once, after its heartbeat went stale",
		reported && strings.Count(bOut, frozenWord) == 1 && reportedAfter >= 80*time.Second,
		"one report, ≥80 s after the freeze", fmt.Sprintf("reported=%v after %s, %d reports; output %s",
			reported, reportedAfter, strings.Count(bOut, frozenWord), clipTail(bOut, 600)))
	s.Check("B keeps waiting in-process", b.Running() && strings.Count(bOut, "Teams helper (new core) running") == 0,
		"B running, never started serving", fmt.Sprintf("running=%v", b.Running()))
	s.Check("B waits with next to no CPU", !measured || used <= waited/200,
		fmt.Sprintf("≤0.5%% of a core (%s over %s)", waited.Round(time.Second)/200, waited.Round(time.Second)),
		fmt.Sprintf("%s (measured=%v)", used, measured))
	bReqs := peerRequests(s, bFrom, "-B-")
	s.Check("the waiting standby sends no Graph request", len(bReqs) == 0, "0 requests from B", fmt.Sprintf("%d: %q", len(bReqs), bReqs))
	s.Check("nothing runs while the state is locked", len(s.Codex.CallsWithToken(held)) == 0, "0 turns",
		fmt.Sprint(len(s.Codex.CallsWithToken(held))))

	// The frozen process lets go of the lock: B takes over and answers.
	release()
	released = true
	byB := s.Poll(steps.AnswerTimeout, func() bool {
		for _, p := range steps.AnswerPosts(s, work.ID, held) {
			if strings.Contains(p.Instance, "-B-") {
				return true
			}
		}
		return false
	})
	s.Check("once the lock is gone B takes over and answers the held prompt", byB && b.Running(), "answered by B",
		fmt.Sprintf("byB=%v running=%v", byB, b.Running()), s.Transcript(work.ID, 12)...)

	// A resumes: fenced, exits, posts nothing.
	thawFrom := s.Graph.LastSeq()
	harness.Thaw(s.SUT)
	aExited := s.Poll(30*time.Second, func() bool { return !s.SUT.Running() })
	s.Settle(2*time.Second, 6*time.Second)
	var aWrites []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: thawFrom}) {
		if r.Instance == aTag && r.Method != "GET" {
			aWrites = append(aWrites, r.Route)
		}
	}
	aOut := s.SUT.Output()
	s.Check("the resumed old owner is fenced and exits", aExited && strings.Contains(strings.ToLower(aOut), "fence"),
		"A exits with the fence error", fmt.Sprintf("exited=%v; output %s", aExited, clipTail(aOut, 300)))
	s.Check("the fenced old owner posts nothing", len(aWrites) == 0, "0 writes from A after it resumed", fmt.Sprintf("%d: %q", len(aWrites), aWrites))
	n := len(steps.AnswerPosts(s, work.ID, held))
	s.Check("R-1 the held prompt is answered exactly once", n == 1 && len(s.Codex.CallsWithToken(held)) == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", n, len(s.Codex.CallsWithToken(held))))
}

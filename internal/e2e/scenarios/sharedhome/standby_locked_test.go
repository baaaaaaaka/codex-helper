package sharedhome

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Shared home, the owner frozen inside a store write (F-XM-1, R-13). A
// helper stopped inside a write transaction keeps SQLite's write lock,
// which nothing can break. The standby does not need it: it never opens the
// old owner's store with SQLite. Once the owner's lease is stale it copies
// the store's database and WAL as raw bytes into a store of its own, where
// SQLite's recovery keeps every committed transaction and drops the frozen
// one, and serves from that. The lock is irrelevant to it.
//
// The harness stands in for the frozen owner's open transaction: it freezes
// host A and holds the store's write lock itself (HoldStoreWriteLock); to
// every other process that is the same state. B (a service child) takes
// over with the lock still held and answers in the existing work chat (the
// session survived the copy). Then the lock is let go, A resumes, is
// fenced and makes no Graph write.
func TestStandbyWaitsOutAFrozenWriter(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "STANDBY-LOCKED", harness.Options{
		Guarantees: []string{"F-XM-1", "R-13", "R-1"},
		Title:      "shared home: an owner frozen inside a store write does not block the takeover: the standby copies the store without its lock, keeps every session, answers once, and the old owner makes no Graph write",
		Budget:     200 * time.Second,
		Seam:       takeoverSeam("host-a"),
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
	store := s.StorePath()
	if store == "" {
		t.Fatal("no store under " + s.Env.Root)
	}

	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b"), Args: serviceChild})
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
	quiet := peerRequests(s, bFrom, "-B-")
	s.Check("the standby sends no Graph request", len(quiet) == 0, "0 requests from B", fmt.Sprintf("%d: %q", len(quiet), quiet))

	_, ok := waitServing(s, b, 3*takeoverStale)
	took := time.Since(frozenAt).Round(time.Second)
	s.Check("B takes over with the write lock still held, after the stale bound", ok && took >= takeoverStale,
		fmt.Sprintf("B serves ≥%s after the freeze", takeoverStale), fmt.Sprintf("serving=%v after %s; output %s", ok, took, clipTail(b.Output(), 600)))
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("B answers the held prompt in the existing work chat", byB, "answered by B",
		fmt.Sprintf("byB=%v", byB), s.Transcript(work.ID, 12)...)
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the new host", len(notices) == 1 && strings.Contains(notices[0], "host-b"),
		"1 notice naming host-b", fmt.Sprintf("%d: %q", len(notices), notices))

	// The frozen process lets go of its lock and resumes.
	release()
	released = true
	checkFencedAndSilent(s, s.SUT)
	exactlyOnce(s, work.ID, held)
}

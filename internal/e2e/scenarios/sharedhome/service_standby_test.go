package sharedhome

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Restart loops (R-13; the owner forbids loops, the old service restarted
// about 3,000 times). A helper run by the service (--managed-service-child)
// that is refused for an expected reason does not exit to be restarted: it
// stands by in-process, sends nothing to Graph, and takes over by itself
// when the refusal is gone.
//
// Host A owns the state and answers. Host B starts as a service child: it
// stands by (A is live) and keeps running. DISABLED appears: A stops; B
// keeps standing by (now for the kill-switch file). A prompt is posted.
// DISABLED is removed: B takes the state at its next check and answers the
// prompt once.
func TestServiceChildStandsByInsteadOfRestarting(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "SERVICE-STANDBY", harness.Options{
		Guarantees: []string{"R-13", "F-XM-1", "F-XM-4"},
		Title:      "a service-run helper refused by a live owner or the kill-switch file stands by in-process with no Graph traffic, and takes over once the refusal is gone",
		Budget:     120 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"Heartbeat": "500ms"}, Hostname: "host-a"},
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the owner (not a service child) stops with an error when DISABLED appears"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "svcstandby")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	b := s.NewPeer("B", harness.PeerOptions{
		Seam: harness.Seam{Hostname: "host-b", Knobs: map[string]string{"RefusalRecheck": "500ms"}},
		Args: []string{"--managed-service-child"},
	})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	exitedEarly := s.Poll(6*time.Second, func() bool { return !b.Running() })
	bOut := b.Output()
	s.Check("a service child refused by a live owner keeps running", !exitedEarly && strings.Contains(bOut, "standing by"),
		"B still running after 6 s, standing by", fmt.Sprintf("exited=%v; output %s", exitedEarly, clipTail(bOut, 300)))

	store := s.StorePath()
	s.Require("store found", 5*time.Second, func() (bool, string) { return store != "", "no store under " + s.Env.Root })
	disabled := filepath.Join(filepath.Dir(store), "DISABLED")
	if err := os.WriteFile(disabled, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	aStopped := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("the owner stops when DISABLED appears", aStopped, "A exited", fmt.Sprint(aStopped))
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While disabled "+held)
	// B checks every 500 ms; give it a few checks.
	sawKillSwitch := s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "kill-switch") })
	time.Sleep(2 * time.Second)
	bOut = b.Output()
	s.Check("the service child stands by for the kill-switch file without exiting", b.Running() && sawKillSwitch,
		"B running, naming the kill-switch file", fmt.Sprintf("running=%v; output %s", b.Running(), clipTail(bOut, 300)))
	beforeRemove := peerRequests(s, bFrom, "-B-")
	s.Check("a standing-by service child sends no Graph request", len(beforeRemove) == 0, "0 requests from B",
		fmt.Sprintf("%d: %q", len(beforeRemove), beforeRemove))
	s.Check("nothing runs while disabled", len(s.Codex.CallsWithToken(held)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(held))))

	if err := os.Remove(disabled); err != nil {
		t.Fatal(err)
	}
	// Host A has exited, so wait without the harness's main-SUT exit check.
	heldAnswered := s.Poll(steps.AnswerTimeout, func() bool {
		for _, c := range s.Codex.CallsWithToken(held) {
			if c.Answer != "" && len(s.SUTMessagesContaining(work.ID, c.Answer)) > 0 {
				return true
			}
		}
		return false
	})
	s.Settle(2*time.Second, 8*time.Second)
	posts := steps.AnswerPosts(s, work.ID, held)
	byB := 0
	for _, p := range posts {
		if strings.Contains(p.Instance, "-B-") {
			byB++
		}
	}
	s.Check("once DISABLED is removed the standing-by child takes over and answers once", heldAnswered && len(posts) == 1 && byB == 1 && b.Running(),
		"1 answer, by B, B still running", fmt.Sprintf("answered=%v posts=%d byB=%d running=%v", heldAnswered, len(posts), byB, b.Running()),
		s.Transcript(work.ID, 15)...)
}

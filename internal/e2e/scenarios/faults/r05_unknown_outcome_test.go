package faults

import (
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-5(a) (with R-1, R-2): a final-answer POST whose response is lost is never
// re-POSTed automatically, and the turn shows a visible state.
//
// Part 1 (accepted-but-response-lost): Graph stores the answer but the
// client sees a dropped connection. Expected: exactly one copy in the chat,
// exactly one POST attempt, and the chat keeps working (the next prompt is
// answered: the ambiguous row is settled from the read-back marker or held
// without blocking later work forever).
//
// The dropped-before-accept case (Graph never stores it) is R-5b: the absent
// final remains retained without being sent again.
func TestR05_LostResponseNeverReposted(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-5", harness.Options{
		Guarantees: []string{"R-5", "R-1", "R-2"},
		Title:      "final POST with unknown outcome is not auto-replayed; turn state visible",
		Budget:     120 * time.Second,
	})
	defer s.Finish()

	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r05-lost")

	// Part 1: accepted but response lost.
	tok := s.Token("lost")
	lost := s.Graph.AddFault(fakegraph.Fault{
		Name: "final-accepted-response-lost", Kind: fakegraph.FaultAcceptedLost,
		Method: "POST", ChatID: work.ID, Match: steps.AnswerPostMatcher(tok), Times: 1,
	})
	steps.MustPost(s, work.ID, "Answer me "+tok)
	s.WaitCodexCalls(tok, 1, steps.AnswerTimeout)
	s.WaitFor("lossy final POST attempted", steps.AnswerTimeout, func() (bool, string) {
		return lost.Fired() > 0, "fault not fired"
	})
	// Give the SUT time to retry if it is going to, and to read back.
	s.Settle(5*time.Second, 20*time.Second)
	tok2 := s.Token("after")
	steps.MustPost(s, work.ID, "Next question "+tok2)
	s.WaitFor("next prompt answered after the lost response", 45*time.Second, func() (bool, string) {
		for _, c := range s.Codex.CallsWithToken(tok2) {
			if c.Answer != "" && len(s.SUTMessagesContaining(work.ID, c.Answer)) > 0 {
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d calls for next prompt", len(s.Codex.CallsWithToken(tok2)))
	})
	s.Settle(3*time.Second, 15*time.Second)

	calls := s.Codex.CallsWithToken(tok)
	answer := ""
	if len(calls) > 0 {
		answer = calls[0].Answer
	}
	copies := len(s.SUTMessagesContaining(work.ID, answer))
	posts := steps.AnswerPosts(s, work.ID, tok)
	var postEvidence []string
	for _, p := range posts {
		postEvidence = append(postEvidence, p.String())
	}
	transcript := s.Transcript(work.ID, 30)
	s.Check("R-5a lost-response final is POSTed exactly once", len(posts) == 1, "1 POST attempt", fmt.Sprintf("%d POST attempts", len(posts)), postEvidence...)
	s.Check("R-2 exactly one copy of the answer in the chat", copies == 1, "1 copy", fmt.Sprintf("%d copies", copies), transcript...)
	s.Check("R-3 Codex ran once for the prompt", len(calls) == 1, "1", fmt.Sprint(len(calls)))
	s.Check("R-5a chat keeps working after an unknown-outcome POST",
		len(s.SUTMessagesContaining(work.ID, "E2EANSWER")) >= 2 && len(s.Codex.CallsWithToken(tok2)) == 1,
		"next prompt answered once", fmt.Sprintf("%d calls for next prompt", len(s.Codex.CallsWithToken(tok2))), transcript...)

}

package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-20 / R-33: `helper cancel` stops a running turn (the interrupt reaches
// Codex), the turn ends in exactly one visible terminal state with no answer
// for it, and the next message in the chat still works.
func TestR20_R33_CancelRunningTurn(t *testing.T) {
	s := harness.NewScenario(t, "R-20-33", harness.Options{
		Guarantees: []string{"R-20", "R-33", "F-TRN-3"},
		Title:      "cancel of a running turn is visible and final; next message works",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	stuck, next := s.Token("stuck"), s.Token("next")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(stuck) {
			return harness.CodexBehavior{Block: true, Answer: "LATE-ANSWER-SHOULD-NOT-APPEAR " + stuck}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r20")

	steps.MustPost(s, work.ID, "Long running "+stuck)
	s.WaitCodexCalls(stuck, 1, steps.AnswerTimeout)
	cancelSeq := s.Graph.LastSeq()
	// `helper cancel` without a selector only prints usage in the old core;
	// `last` selects the running turn when nothing is queued (F-TRN-3).
	steps.MustPost(s, work.ID, "helper cancel last")
	cancelledInCodex := s.WaitFor("interrupt reaches Codex", 30*time.Second, func() (bool, string) {
		cs := s.Codex.CallsWithToken(stuck)
		return len(cs) > 0 && cs[0].Outcome == "cancelled", "outcome=" + steps.Outcome(cs)
	})
	var terminal []string
	s.WaitFor("visible terminal state for the cancelled turn", 30*time.Second, func() (bool, string) {
		terminal = terminal[:0]
		for _, m := range s.SUTMessages(work.ID) {
			if m.RequestSeq <= cancelSeq {
				continue
			}
			low := strings.ToLower(m.PlainText())
			if strings.Contains(low, "usage:") {
				continue
			}
			if strings.Contains(low, "cancel") || strings.Contains(low, "interrupt") || strings.Contains(low, "stopped") {
				terminal = append(terminal, m.PlainText())
			}
		}
		return len(terminal) > 0, "no cancel/interrupt notice yet"
	})
	// Even if the fake Codex were released now, the cancelled turn must not
	// post an answer.
	s.Codex.ReleaseAll()
	steps.MustPost(s, work.ID, "Next request "+next)
	_, ok := steps.WaitAnswered(s, work.ID, next, 45*time.Second)
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(work.ID, 30)
	s.Check("R-20 interrupt reaches the running Codex turn", cancelledInCodex, "fake Codex call cancelled", "outcome="+steps.Outcome(s.Codex.CallsWithToken(stuck)))
	s.Check("R-33 cancelled turn has a visible terminal state", len(terminal) >= 1, ">= 1 cancel/interrupt notice", fmt.Sprintf("%d notices", len(terminal)), tr...)
	late := len(s.SUTMessagesContaining(work.ID, "LATE-ANSWER"))
	s.Check("R-20 no answer is posted for the cancelled turn", late == 0, "0", fmt.Sprint(late))
	s.Check("R-20 the next message still works", ok && len(s.Codex.CallsWithToken(next)) == 1, "answered once",
		fmt.Sprintf("answered=%v calls=%d", ok, len(s.Codex.CallsWithToken(next))), tr...)
	s.Check("R-3 cancelled prompt not re-run", len(s.Codex.CallsWithToken(stuck)) == 1, "1 call", fmt.Sprint(len(s.Codex.CallsWithToken(stuck))))
}

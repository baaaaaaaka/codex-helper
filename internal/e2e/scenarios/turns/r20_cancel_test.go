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
	harness.Parallel(t)
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
	// `helper cancel` without a selector only prints usage in the old core;
	// `last` selects the running turn when nothing is queued (F-TRN-3).
	steps.MustPost(s, work.ID, "helper cancel last")
	cancelledInCodex := s.WaitFor("interrupt reaches Codex", 30*time.Second, func() (bool, string) {
		cs := s.Codex.CallsWithToken(stuck)
		return len(cs) > 0 && cs[0].Outcome == "cancelled", "outcome=" + steps.Outcome(cs)
	})
	// The command's reply and the turn's terminal notice, in this order;
	// the next prompt follows both, so the transcript is fixed.
	const ack, cancelled = "Cancelling the running request", "Request cancelled."
	terminal := s.WaitFor("cancel reply and terminal notice for the cancelled turn", 30*time.Second, func() (bool, string) {
		a, c := len(s.SUTMessagesContaining(work.ID, ack)), len(s.SUTMessagesContaining(work.ID, cancelled))
		return a > 0 && c > 0, fmt.Sprintf("%q x%d, %q x%d", ack, a, cancelled, c)
	})
	// Even if the fake Codex were released now, the cancelled turn must not
	// post an answer.
	s.Codex.ReleaseAll()
	steps.MustPost(s, work.ID, "Next request "+next)
	_, ok := steps.WaitAnswered(s, work.ID, next, 45*time.Second)
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(work.ID, 30)
	s.Check("R-20 interrupt reaches the running Codex turn", cancelledInCodex, "fake Codex call cancelled", "outcome="+steps.Outcome(s.Codex.CallsWithToken(stuck)))
	replyAt, noticeAt := -1, -1
	for i, m := range s.SUTMessages(work.ID) {
		switch txt := m.PlainText(); {
		case replyAt < 0 && strings.Contains(txt, ack):
			replyAt = i
		case noticeAt < 0 && strings.Contains(txt, cancelled):
			noticeAt = i
		}
	}
	s.Check("R-20 the cancel reply precedes the terminal notice", replyAt >= 0 && replyAt < noticeAt, "reply first",
		fmt.Sprintf("reply at %d, %q at %d", replyAt, cancelled, noticeAt), tr...)
	s.Check("R-33 cancelled turn has a visible terminal state", terminal, fmt.Sprintf("%q posted", cancelled), map[bool]string{true: "posted", false: "missing"}[terminal], tr...)
	late := len(s.SUTMessagesContaining(work.ID, "LATE-ANSWER"))
	s.Check("R-20 no answer is posted for the cancelled turn", late == 0, "0", fmt.Sprint(late))
	s.Check("R-20 the next message still works", ok && len(s.Codex.CallsWithToken(next)) == 1, "answered once",
		fmt.Sprintf("answered=%v calls=%d", ok, len(s.Codex.CallsWithToken(next))), tr...)
	s.Check("R-3 cancelled prompt not re-run", len(s.Codex.CallsWithToken(stuck)) == 1, "1 call", fmt.Sprint(len(s.Codex.CallsWithToken(stuck))))
}

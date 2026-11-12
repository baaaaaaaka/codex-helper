package commands

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Busy settings changes interrupt and automatically continue the same task;
// the original prompt is not replayed. Explicit queued work still waits.
func TestModelSwitchWhileBusy(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MODEL-SWITCH-BUSY", harness.Options{
		Guarantees: []string{"R-31"}, Title: "busy model/effort switch automatically continues the same thread without replay; queued work waits and settings survive restart", Budget: 120 * time.Second,
	})
	defer s.Finish()
	original := s.Token("running")
	resumedAnswer := "E2EANSWER-resumed task completed " + original
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(original) {
			return harness.CodexBehavior{Block: true}
		}
		if c.Prompt == harness.ExpectedContinuationPrompt {
			return harness.CodexBehavior{Block: true, Answer: resumedAnswer}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "modelbusy")
	steps.MustPost(s, work.ID, "Long task "+original)
	s.Require("original reached Codex", steps.AnswerTimeout, func() (bool, string) { return len(s.Codex.CallsWithToken(original)) == 1, "not started" })
	reply, ok := command(s, work.ID, "model use gpt-5-mini low", "Model profile switched")
	s.Check("switch promises automatic continuation", ok && strings.Contains(reply, "continue automatically"), "automatic continuation", clip(reply, 300))
	var resumed harness.CodexCall
	s.Require("continuation starts without another user message", steps.AnswerTimeout, func() (bool, string) {
		for _, c := range s.Codex.Calls() {
			if c.Prompt == harness.ExpectedContinuationPrompt {
				resumed = c
				return true, ""
			}
		}
		return false, "no continuation"
	})
	first := s.Codex.CallsWithToken(original)[0]
	s.Check("original was interrupted without being replayed", first.Outcome == "cancelled" && len(s.Codex.CallsWithToken(original)) == 1, "one interrupted original", fmt.Sprintf("%s, %d calls", first.Outcome, len(s.Codex.CallsWithToken(original))))
	s.Check("continuation uses new settings on same thread", resumed.Model == "gpt-5-mini" && resumed.Effort == "low" && resumed.ThreadID == first.ThreadID && resumed.ClientID != "" && resumed.ClientID != first.ClientID, "gpt-5-mini/low, same thread, distinct execution", fmt.Sprintf("%+v", resumed))
	s.Check("old execution ended before continuation", !first.Ended.IsZero() && !resumed.Started.Before(first.Ended), "no overlapping execution", first.Ended.String()+" -> "+resumed.Started.String())
	queued := s.Token("queued")
	steps.MustPost(s, work.ID, "helper queue Next task "+queued)
	s.Require("queued request acknowledged", steps.CommandTimeout, func() (bool, string) {
		return len(steps.PromptConfirmations(s, work.ID, "Queued.")) > 0, "no queued acknowledgement"
	})
	s.Check("queued receipt names the original active task", len(steps.PromptConfirmations(s, work.ID, "Running now: Long task "+original)) == 1, "original task label", "receipt task label")
	s.Check("explicit queue waits behind continuation", len(s.Codex.CallsWithToken(queued)) == 0, "not started", fmt.Sprint(len(s.Codex.CallsWithToken(queued))))
	s.Codex.Release(resumed.Seq)
	s.Require("resumed answer delivered once", steps.AnswerTimeout, func() (bool, string) {
		return len(steps.AnswerPosts(s, work.ID, original)) == 1 && len(s.SUTMessagesContaining(work.ID, resumedAnswer)) == 1, "resumed answer pending or duplicated"
	})
	steps.WaitAnswered(s, work.ID, queued, steps.AnswerTimeout)
	s.Require("queued completion notification delivered", steps.CommandTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(control.ID, "Request: Next task "+queued)) > 0, "notification pending"
	})
	s.CrashSUT()
	s.StartSUT()
	after := s.Token("aftercrash")
	steps.MustPost(s, work.ID, "After the crash "+after)
	steps.WaitAnswered(s, work.ID, after, steps.AnswerTimeout)
	s.Settle(2*time.Second, 8*time.Second)
	q, a := s.Codex.CallsWithToken(queued), s.Codex.CallsWithToken(after)
	s.Check("queued and restarted work retain settings", len(q) == 1 && len(a) == 1 && q[0].Model == "gpt-5-mini" && a[0].Model == "gpt-5-mini" && q[0].ThreadID == first.ThreadID && a[0].ThreadID == first.ThreadID, "new model, same thread", fmt.Sprintf("queued=%+v after=%+v", q, a))
	s.Check("original task answered once", len(steps.AnswerPosts(s, work.ID, original)) == 1, "one answer", fmt.Sprint(len(steps.AnswerPosts(s, work.ID, original))))
	for _, m := range s.SUTMessages(work.ID) {
		if strings.Contains(m.PlainText(), "🧑‍💻 User") && strings.Contains(m.PlainText(), harness.ExpectedContinuationPrompt) {
			s.Check("internal continuation is not mirrored", false, "no user mirror", m.PlainText())
		}
	}
}

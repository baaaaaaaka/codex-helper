package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// F-CTL-11 / R-18 / F-SVC-10: `helper restart` (v0.1.20
// restartHelperFromControl).
//
//	helper restart        -> the safe-restart confirmation; nothing restarts
//	helper restart now    -> while chat A's turn runs: "Codex work is still
//	                         active", the helper waits for it
//	prompt in chat B      -> "…I saved this message and will resume it…"
//	                         (once); it does not start before the restart
//	A finishes            -> the helper restarts; one "Helper restart
//	                         completed"; B runs once in the new process
//	helper restart force  -> while chat A's next turn runs: restarts at once;
//	                         that turn gets one interruption notice
func TestHelperRestartDrain(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "RESTART-DRAIN", harness.Options{
		Guarantees: []string{"F-CTL-11", "R-18", "F-SVC-10"},
		Title:      "`helper restart` confirms; `now` waits for running work and saves new prompts for after the restart; `force` restarts at once",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	long, held, forced, after := s.Token("long"), s.Token("held"), s.Token("forced"), s.Token("after")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(long) || c.HasToken(forced) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	chatA, _ := steps.NewWorkChat(s, control, "drain-a")
	chatB, _ := steps.NewWorkChat(s, control, "drain-b")
	s.Settle(2*time.Second, 10*time.Second)

	firstInst := s.SUT.Instance()
	confirm, okConfirm := commandIn(s, control.ID, "helper restart", "Helper restart")
	s.Check("F-CTL-11 bare `helper restart` asks for confirmation and does not restart",
		okConfirm && strings.Contains(confirm, "To restart, send: helper restart now") && strings.Contains(confirm, "helper restart force") && s.SUT.Instance() == firstInst,
		"the v0.1.20 confirmation; same instance", clip(confirm, 200))

	steps.MustPost(s, chatA.ID, "Long job "+long)
	s.WaitCodexCalls(long, 1, steps.AnswerTimeout)
	waiting, okWaiting := commandIn(s, control.ID, "helper restart now", "Codex work is still active")
	s.Check("R-18 `helper restart now` while work runs waits for it",
		okWaiting && strings.Contains(waiting, "I will restart the helper when the running work finishes"), "⏳ Codex work is still active. I will restart …", clip(waiting, 200))
	steps.MustPost(s, chatB.ID, "Held job "+held)
	okSaved := steps.WaitPromptConfirmation(s, chatB.ID, "I saved this message", steps.CommandTimeout)
	var saved string
	if notices := steps.PromptConfirmations(s, chatB.ID, "I saved this message"); len(notices) > 0 {
		saved = harness.NormalizeSpace(notices[0].PlainText())
	}
	s.Check("F-SVC-10 a prompt sent while the restart waits gets the saved-message notice",
		okSaved && strings.Contains(saved, "helper restart in progress. I saved this message and will resume it after the helper is back online.") && strings.Contains(saved, "Received at "),
		"helper restart in progress. I saved this message and will resume it after the helper is back online.", saved)
	time.Sleep(3 * time.Second) // room for a (wrong) start before the restart
	early := len(s.Codex.CallsWithToken(held))
	s.Check("F-SVC-10 the saved prompt does not start before the restart", early == 0 && s.SUT.Instance() == firstInst, "0 Codex calls; same instance",
		fmt.Sprintf("calls=%d instance changed=%v", early, s.SUT.Instance() != firstInst))

	s.Codex.Release(s.Codex.CallsWithToken(long)[0].Seq) // not ReleaseAll: the forced job below must block
	_, okLong := steps.WaitAnswered(s, chatA.ID, long, steps.AnswerTimeout)
	_, restarted := s.WaitSUTMessage(control.ID, "Helper restart completed", 60*time.Second)
	_, okHeld := steps.WaitAnswered(s, chatB.ID, held, 60*time.Second)
	s.Settle(3*time.Second, 15*time.Second)
	secondInst := s.SUT.Instance()
	heldCalls := s.Codex.CallsWithToken(held)
	s.Check("R-18 the running turn finishes before the restart and is answered once", okLong && len(steps.AnswerPosts(s, chatA.ID, long)) == 1 && len(s.Codex.CallsWithToken(long)) == 1,
		"answered once", fmt.Sprintf("answers=%d calls=%d", len(steps.AnswerPosts(s, chatA.ID, long)), len(s.Codex.CallsWithToken(long))))
	s.Check("R-18 the helper restarted once the work finished; one completion notice",
		restarted && secondInst != firstInst && len(s.SUTMessagesContaining(control.ID, "Helper restart completed")) == 1, "a new instance; 1 notice",
		fmt.Sprintf("restarted=%v notices=%d", restarted, len(s.SUTMessagesContaining(control.ID, "Helper restart completed"))))
	s.Check("F-SVC-10 the saved prompt runs once, in the new process, and is answered once",
		okHeld && len(heldCalls) == 1 && heldCalls[0].Instance != firstInst && len(steps.AnswerPosts(s, chatB.ID, held)) == 1 && len(steps.PromptConfirmations(s, chatB.ID, "I saved this message")) == 1,
		"1 call after the restart, 1 answer, 1 notice", fmt.Sprintf("calls=%d answers=%d notices=%d", len(heldCalls), len(steps.AnswerPosts(s, chatB.ID, held)), len(steps.PromptConfirmations(s, chatB.ID, "I saved this message"))),
		s.Transcript(chatB.ID, 8)...)

	steps.MustPost(s, chatA.ID, "Forced job "+forced)
	s.WaitCodexCalls(forced, 1, steps.AnswerTimeout)
	_, okForce := commandIn(s, control.ID, "helper restart force", "Helper restart scheduled")
	s.WaitFor("the forced restart completes", 60*time.Second, func() (bool, string) {
		n := len(s.SUTMessagesContaining(control.ID, "Helper restart completed"))
		return n >= 2, fmt.Sprintf("%d completion notices", n)
	})
	_, interrupted := s.WaitSUTMessage(chatA.ID, "Codex request interrupted", 45*time.Second)
	steps.MustPost(s, chatA.ID, "After the restart "+after)
	_, okAfter := steps.WaitAnswered(s, chatA.ID, after, steps.AnswerTimeout)
	s.Settle(3*time.Second, 15*time.Second)
	s.Check("R-18 `helper restart force` restarts at once; the running turn gets one interruption notice and is not re-run",
		okForce && interrupted && s.SUT.Instance() != secondInst && len(s.SUTMessagesContaining(chatA.ID, "Codex request interrupted")) == 1 && len(s.Codex.CallsWithToken(forced)) == 1,
		"new instance, 1 notice, 1 call", fmt.Sprintf("notices=%d calls=%d", len(s.SUTMessagesContaining(chatA.ID, "Codex request interrupted")), len(s.Codex.CallsWithToken(forced))),
		s.Transcript(chatA.ID, 10)...)
	s.Check("R-18 one completion notice per restart", len(s.SUTMessagesContaining(control.ID, "Helper restart completed")) == 2, "2",
		fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Helper restart completed"))), s.Transcript(control.ID, 12)...)
	s.Check("work continues after the restarts", okAfter, "answered", fmt.Sprint(okAfter))
}

// commandIn posts text and waits for a new helper reply containing sub; it
// returns the reply's whitespace-normalized text.
func commandIn(s *harness.Scenario, chat, text, sub string) (string, bool) {
	seq := s.Graph.LastSeq()
	steps.MustPost(s, chat, text)
	var got string
	ok := s.WaitFor(fmt.Sprintf("reply to %q containing %q", text, sub), steps.CommandTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(chat) {
			if m.RequestSeq > seq && strings.Contains(harness.NormalizeSpace(m.PlainText()), sub) {
				got = strings.TrimPrefix(harness.NormalizeSpace(m.PlainText()), "🔧 Helper: ")
				return true, ""
			}
		}
		return false, strings.Join(s.Transcript(chat, 4), " | ")
	})
	return got, ok
}

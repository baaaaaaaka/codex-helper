package crash

import (
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner decision: a turn interrupted by a helper restart gets exactly one
// interruption notice per interrupted turn, posted by restart recovery,
// before the chat's next turn starts, and the notice shows no internal ids
// (Teams chat/message ids, Codex thread/turn ids, turn keys) or helper
// state paths.
//
//	chat A: one running turn, lost with the helper   -> 1 notice
//	chat B: one running turn, lost with the helper   -> 1 notice
//	chat C: idle at the crash (its turn had finished) -> 0 notices
//
// B-5 covers the count for one chat; this scenario adds the per-turn
// scope (two chats, one idle) and the notice text.
func TestInterruptionNoticeText(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "INT-NOTICE", harness.Options{
		Guarantees: []string{"R-33", "B-5", "interrupt-notice-once-no-internal-ids"},
		Title:      "interruption notice: one per interrupted turn, at restart, before the next turn, without internal ids",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	tokA, tokB, tokC := s.Token("runa"), s.Token("runb"), s.Token("idlec")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tokA) || c.HasToken(tokB) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	chatA, _ := steps.NewWorkChat(s, control, "int-a")
	chatB, _ := steps.NewWorkChat(s, control, "int-b")
	chatC, _ := steps.NewWorkChat(s, control, "int-c")
	steps.MustPost(s, chatC.ID, "Quick task "+tokC)
	steps.WaitAnswered(s, chatC.ID, tokC, steps.AnswerTimeout)
	steps.MustPost(s, chatA.ID, "Long task "+tokA)
	steps.MustPost(s, chatB.ID, "Long task "+tokB)
	s.Require("both long turns reached Codex", steps.AnswerTimeout, func() (bool, string) {
		a, b := len(s.Codex.CallsWithToken(tokA)), len(s.Codex.CallsWithToken(tokB))
		return a > 0 && b > 0, fmt.Sprintf("A=%d B=%d", a, b)
	})
	s.Settle(2*time.Second, 8*time.Second)

	s.CrashSUT()
	killedAt := time.Now()
	s.Require("Codex lost both turns with the helper", 15*time.Second, func() (bool, string) {
		a, b := s.Codex.CallsWithToken(tokA)[0], s.Codex.CallsWithToken(tokB)[0]
		return a.Outcome != "running" && b.Outcome != "running", "A=" + a.Outcome + " B=" + b.Outcome
	})
	s.StartSUT()
	s.WaitFor("restart recovery posts a notice in A and B", 45*time.Second, func() (bool, string) {
		a, b := len(noticesAfter(s, chatA.ID, killedAt)), len(noticesAfter(s, chatB.ID, killedAt))
		return a > 0 && b > 0, fmt.Sprintf("A=%d B=%d", a, b)
	})
	s.Settle(3*time.Second, 15*time.Second)

	next := map[string]string{chatA.ID: s.Token("nexta"), chatB.ID: s.Token("nextb"), chatC.ID: s.Token("nextc")}
	for chat, tok := range next {
		steps.MustPost(s, chat, "Next request "+tok)
	}
	for chat, tok := range next {
		steps.WaitAnswered(s, chat, tok, 45*time.Second)
	}
	s.Settle(3*time.Second, 15*time.Second)

	for _, c := range []struct{ name, chat string }{{"A", chatA.ID}, {"B", chatB.ID}} {
		tr := s.Transcript(c.chat, 20)
		var texts []string
		var leaks []string
		for _, m := range s.SUTMessages(c.chat) {
			if m.Created.Before(killedAt) || !isNotice(m) {
				continue
			}
			text := harness.NormalizeSpace(m.PlainText())
			texts = append(texts, clip(text, 200))
			leaks = append(leaks, steps.InternalLeaks(s, text)...)
		}
		s.Check("chat "+c.name+": exactly one interruption notice for its interrupted turn", len(texts) == 1, "1", fmt.Sprintf("%d: %q", len(texts), texts), tr...)
		if calls := s.Codex.CallsWithToken(next[c.chat]); len(calls) > 0 && len(texts) > 0 {
			first := firstNoticeTime(s, c.chat, killedAt)
			s.Check("chat "+c.name+": the notice precedes the next turn", first.Before(calls[0].Started), "notice before next turn/start",
				fmt.Sprintf("notice %s, next turn %s", first.Format("15:04:05.000"), calls[0].Started.Format("15:04:05.000")))
		}
		s.Check("chat "+c.name+": the notice shows no internal ids or paths", len(texts) > 0 && len(leaks) == 0, "no Teams/Codex ids, turn keys or state paths",
			fmt.Sprintf("leaks %q in %q", leaks, texts))
	}
	idle := noticesAfter(s, chatC.ID, killedAt)
	s.Check("chat C (idle at the crash) gets no interruption notice", len(idle) == 0, "0", fmt.Sprintf("%d: %q", len(idle), idle), s.Transcript(chatC.ID, 10)...)
}

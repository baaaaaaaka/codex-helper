package turns

import (
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Codex loses a turn's completion callback, or its app-server dies in the
// middle of a turn: the first is still answered once, the second ends in
// one interrupted notice and is never re-run, and both chats keep working
// on the same Codex thread (R-5, R-33, R-32).
//
// History: turns that finished in Codex but never in Teams ("47 minutes
// without an update", U-I09; lost turn/completed, U-I27) and helpers that
// re-ran a turn after Codex died, running the same request twice (R-5b,
// INC-02). The fake's lost-callback and crash knobs existed but no
// scenario used them.
func TestCodexLostCompletionAndCrash(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CODEX-LOST", harness.Options{
		Guarantees: []string{"R-5", "R-33", "R-32"},
		Title:      "Codex drops turn/completed in one chat and its app-server dies mid-turn in another: answered once / interrupted once, never re-run, same thread next",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	lost, crash := s.Token("lost"), s.Token("crash")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		switch {
		case c.HasToken(lost):
			return harness.CodexBehavior{HoldCompletion: true}
		case c.HasToken(crash):
			return harness.CodexBehavior{Crash: true}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	a, _ := steps.NewWorkChat(s, control, "lostcallback")
	b, _ := steps.NewWorkChat(s, control, "codexcrash")
	steps.MustPost(s, a.ID, "Finish quietly "+lost)
	steps.MustPost(s, b.ID, "Crash in the middle "+crash)
	steps.WaitAnswered(s, a.ID, lost, 45*time.Second)
	s.WaitFor("the crashed turn ends visibly", 45*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(b.ID, "Codex request interrupted")) > 0, "no notice yet"
	})
	next := map[string]string{a.ID: s.Token("nexta"), b.ID: s.Token("nextb")}
	for _, chat := range []string{a.ID, b.ID} {
		steps.MustPost(s, chat, "Next question "+next[chat])
	}
	for _, chat := range []string{a.ID, b.ID} {
		steps.WaitAnswered(s, chat, next[chat], 45*time.Second)
	}
	s.Settle(3*time.Second, 12*time.Second)

	trA, trB := s.Transcript(a.ID, 10), s.Transcript(b.ID, 10)
	n := len(steps.AnswerPosts(s, a.ID, lost))
	s.Check("the turn whose completion was lost is answered once", n == 1, "1", fmt.Sprint(n), trA...)
	calls := s.Codex.CallsWithToken(lost)
	s.Check("the turn whose completion was lost is not re-run", len(calls) == 1, "1 Codex call", fmt.Sprint(len(calls)))
	notices := len(s.SUTMessagesContaining(b.ID, "Codex request interrupted"))
	s.Check("the crashed turn ends in one interrupted notice", notices == 1, "1", fmt.Sprint(notices), trB...)
	calls = s.Codex.CallsWithToken(crash)
	s.Check("the crashed turn is never re-run", len(calls) == 1, "1 Codex call", fmt.Sprint(len(calls)))
	for _, c := range []struct{ name, chat, first string }{{"lost-callback chat", a.ID, lost}, {"crash chat", b.ID, crash}} {
		first, second := s.Codex.CallsWithToken(c.first), s.Codex.CallsWithToken(next[c.chat])
		same := len(first) == 1 && len(second) == 1 && first[0].ThreadID != "" && first[0].ThreadID == second[0].ThreadID
		obs := "missing calls"
		if len(first) == 1 && len(second) == 1 {
			obs = fmt.Sprintf("%s then %s", first[0].ThreadID, second[0].ThreadID)
		}
		s.Check(c.name+": the next prompt is answered once on the same thread", same && len(steps.AnswerPosts(s, c.chat, next[c.chat])) == 1,
			"same thread, 1 answer", obs)
	}
}

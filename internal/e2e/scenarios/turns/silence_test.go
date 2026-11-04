package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Silence hints and the stall bound (R-19, SPEC acceptance: a silent
// Codex gets one hint at 7 min, one at 15 min, and is stopped at the 60
// min stall bound with one visible message). The bounds are scaled down
// through E2E_SUT_TIMING (3 s / 6 s / 10 s); the code path is the
// production one.
func TestSilenceHintsAndStallBound(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SILENCE", harness.Options{
		Guarantees: []string{"R-19", "R-20", "R-22"},
		Title:      "a silent Codex turn: one hint at SilenceHint, one at SilenceStuck, stopped at StallLimit with one notice; the chat keeps working",
		Budget:     90 * time.Second,
		SUTEnv:     map[string]string{"E2E_SUT_TIMING": "SilenceHint=3s,SilenceStuck=6s,StallLimit=10s"},
	})
	defer s.Finish()
	silent := s.Token("silent")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(silent) {
			return harness.CodexBehavior{Block: true}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "silence")
	steps.MustPost(s, work.ID, "A task that goes silent "+silent)
	s.Require("the silent turn reached Codex", steps.AnswerTimeout, func() (bool, string) {
		return len(s.Codex.CallsWithToken(silent)) == 1, "not yet"
	})
	started := s.Codex.CallsWithToken(silent)[0].Started

	const hint, stuck, stalled = "Still working. No new Codex update yet", "It may be stuck", "so the helper stopped the request"
	firstAt := func(sub string) time.Duration {
		for _, m := range s.SUTMessages(work.ID) {
			if strings.Contains(harness.NormalizeSpace(m.PlainText()), sub) {
				return m.Created.Sub(started).Round(100 * time.Millisecond)
			}
		}
		return -1
	}
	s.Poll(30*time.Second, func() bool { return len(s.SUTMessagesContaining(work.ID, stalled)) > 0 })
	s.Settle(2*time.Second, 6*time.Second)
	tr := s.Transcript(work.ID, 15)
	nh, ns, nst := len(s.SUTMessagesContaining(work.ID, hint)), len(s.SUTMessagesContaining(work.ID, stuck)), len(s.SUTMessagesContaining(work.ID, stalled))
	th, ts, tst := firstAt(hint), firstAt(stuck), firstAt(stalled)
	s.Check("one silence hint after SilenceHint", nh == 1 && th >= 2500*time.Millisecond && th < 6*time.Second,
		"1 hint, 3 s after the start", fmt.Sprintf("%d hints, first at %s", nh, th), tr...)
	s.Check("one 'may be stuck' notice after SilenceStuck", ns == 1 && ts >= 5500*time.Millisecond && ts < 10*time.Second,
		"1 notice, 6 s after the start", fmt.Sprintf("%d notices, first at %s", ns, ts))
	outcome := steps.Outcome(s.Codex.CallsWithToken(silent))
	s.Check("the turn is stopped at StallLimit with one notice", nst == 1 && tst >= 9500*time.Millisecond && outcome != "running",
		"1 stop notice ≥10 s after the start; Codex turn ended", fmt.Sprintf("%d notices, first at %s; codex outcome %s", nst, tst, outcome))
	finals := len(s.SUTMessagesContaining(work.ID, "E2EANSWER"))
	s.Check("the stopped turn posts no answer", finals == 0, "0 answers", fmt.Sprint(finals))

	next := s.Token("next")
	steps.MustPost(s, work.ID, "Next prompt "+next)
	_, ok := steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Check("the chat keeps working after the stall stop", ok, "next prompt answered", fmt.Sprint(ok))
	s.Settle(time.Second, 4*time.Second)
	s.Check("a turn that answers in time gets no hint", len(s.SUTMessagesContaining(work.ID, hint)) == 1,
		"still 1 hint in the chat", fmt.Sprint(len(s.SUTMessagesContaining(work.ID, hint))))
}

package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Silence hints and no stall kill (R-19, F-TRN-18; owner/coordinator
// ruling: v0.1.20 parity). A silent Codex gets one hint at 7 min and one
// at 15 min ("I will not retry automatically. To stop the running request,
// send `helper cancel running`") and is never stopped by the helper: it
// runs on until the user cancels it. The thresholds are scaled down
// through the test seam's timing knobs (3 s / 6 s); the turn is then left
// silent well past the old 10 s stall bound of this scenario.
func TestSilenceHintsAndNoStallKill(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SILENCE", harness.Options{
		Guarantees: []string{"R-19", "R-20", "R-22", "F-TRN-18"},
		Title:      "a silent Codex turn: one hint at SilenceHint, one at SilenceStuck, never stopped by the helper; `helper cancel running` stops it and the chat keeps working",
		Budget:     90 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"SilenceHint": "3s", "SilenceStuck": "6s"}},
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

	const hint, stuck, noRetry = "Still working. No new Codex update yet", "It may be stuck", "I will not retry automatically. To stop the running request, send helper cancel running"
	firstAt := func(sub string) time.Duration {
		for _, m := range s.SUTMessages(work.ID) {
			if strings.Contains(harness.NormalizeSpace(m.PlainText()), sub) {
				return m.Created.Sub(started).Round(100 * time.Millisecond)
			}
		}
		return -1
	}
	s.Poll(30*time.Second, func() bool { return len(s.SUTMessagesContaining(work.ID, stuck)) > 0 })
	// Well past the former 10 s stall bound: the turn still runs.
	for time.Since(started) < 16*time.Second {
		s.Settle(time.Second, 2*time.Second)
	}
	tr := s.Transcript(work.ID, 15)
	nh, ns := len(s.SUTMessagesContaining(work.ID, hint)), len(s.SUTMessagesContaining(work.ID, stuck))
	th, ts := firstAt(hint), firstAt(stuck)
	s.Check("one silence hint after SilenceHint", nh == 1 && th >= 2500*time.Millisecond && th < 6*time.Second,
		"1 hint, 3 s after the start", fmt.Sprintf("%d hints, first at %s", nh, th), tr...)
	s.Check("one 'may be stuck' notice after SilenceStuck", ns == 1 && ts >= 5500*time.Millisecond && ts < 10*time.Second,
		"1 notice, 6 s after the start", fmt.Sprintf("%d notices, first at %s", ns, ts))
	s.Check("the 'may be stuck' notice says the helper will not retry and how to stop it (v0.1.20)",
		len(s.SUTMessagesContaining(work.ID, noRetry)) == 1, "1 notice with "+noRetry, fmt.Sprint(len(s.SUTMessagesContaining(work.ID, noRetry))))
	outcome := steps.Outcome(s.Codex.CallsWithToken(silent))
	stops := len(s.SUTMessagesContaining(work.ID, "stopped")) + len(s.SUTMessagesContaining(work.ID, "canceled")) + len(s.SUTMessagesContaining(work.ID, "interrupted"))
	s.Check("the helper never stops a silent turn by itself", outcome == "running" && stops == 0,
		"Codex turn still running, no stop notice after 16 s", fmt.Sprintf("codex outcome %s; %d stop notices", outcome, stops))

	steps.MustPost(s, work.ID, "helper cancel running")
	cancelled := s.Poll(steps.CommandTimeout, func() bool { return len(s.SUTMessagesContaining(work.ID, "Codex request canceled.")) == 1 })
	s.Check("`helper cancel running` stops it with one notice", cancelled && steps.Outcome(s.Codex.CallsWithToken(silent)) == "cancelled",
		"1 \"Codex request canceled.\"; Codex turn cancelled", fmt.Sprintf("%v; codex outcome %s", cancelled, steps.Outcome(s.Codex.CallsWithToken(silent))))
	finals := len(s.SUTMessagesContaining(work.ID, "E2EANSWER"))
	s.Check("the cancelled turn posts no answer", finals == 0, "0 answers", fmt.Sprint(finals))

	next := s.Token("next")
	steps.MustPost(s, work.ID, "Next prompt "+next)
	_, ok := steps.WaitAnswered(s, work.ID, next, steps.AnswerTimeout)
	s.Check("the chat keeps working after the cancel", ok, "next prompt answered", fmt.Sprint(ok))
	s.Settle(time.Second, 4*time.Second)
	s.Check("a turn that answers in time gets no hint", len(s.SUTMessagesContaining(work.ID, hint)) == 1,
		"still 1 hint in the chat", fmt.Sprint(len(s.SUTMessagesContaining(work.ID, hint))))
}

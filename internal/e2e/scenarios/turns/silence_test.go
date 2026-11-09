package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Silence hints and no stall kill (R-19, F-TRN-18). A silent Codex gets
// notices at 2 min, then every 5 min; cancel guidance starts at 7 min and
// stuck wording starts at the 15 min threshold. Since sends remain on the
// 5 min cadence, the first stuck-worded notice is due around 17 min. The
// seam scales those waits while preserving their spacing.
func TestSilenceHintsAndNoStallKill(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "SILENCE", harness.Options{
		Guarantees: []string{"R-19", "R-20", "R-22", "F-TRN-18"},
		Title:      "a silent Codex turn: notices at 2 min then every 5 min, cancel guidance from 7 min and stuck wording from 15 min (first due around 17 min); never stopped by the helper",
		Budget:     90 * time.Second,
		Seam: harness.Seam{Knobs: map[string]string{
			"SilenceHint": "2s", "SilenceRepeat": "5s", "SilenceCancelHint": "7s", "SilenceStuck": "15s",
		}},
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

	const hint, cancelGuidance, stuck, noRetry = "Still working. No new Codex update yet", "This is taking longer than usual", "It may be stuck", "I will not retry automatically. To stop the running request, send helper cancel running"
	timesAt := func(sub string) []time.Duration {
		var times []time.Duration
		for _, m := range s.SUTMessages(work.ID) {
			if strings.Contains(harness.NormalizeSpace(m.PlainText()), sub) {
				times = append(times, m.Created.Sub(started))
			}
		}
		return times
	}
	firstAt := func(sub string) time.Duration {
		times := timesAt(sub)
		if len(times) == 0 {
			return -1
		}
		return times[0].Round(100 * time.Millisecond)
	}
	s.Poll(30*time.Second, func() bool { return len(s.SUTMessagesContaining(work.ID, stuck)) >= 2 })
	// The threshold is 15 s in this scaled scenario. The 17 s cadence point
	// must use stuck wording, then repeat once at 22 s; both leave it running.
	tr := s.Transcript(work.ID, 20)
	nh, ns := len(s.SUTMessagesContaining(work.ID, hint)), len(s.SUTMessagesContaining(work.ID, stuck))
	th, tc, ts := firstAt(hint), firstAt(cancelGuidance), firstAt(stuck)
	cn, nr := len(s.SUTMessagesContaining(work.ID, cancelGuidance)), len(s.SUTMessagesContaining(work.ID, noRetry))
	hintTimes := timesAt(hint)
	repeatsEveryFive := len(hintTimes) == 3 &&
		hintTimes[1]-hintTimes[0] >= 4500*time.Millisecond && hintTimes[1]-hintTimes[0] < 6200*time.Millisecond &&
		hintTimes[2]-hintTimes[1] >= 4500*time.Millisecond && hintTimes[2]-hintTimes[1] < 6200*time.Millisecond
	s.Check("the first status is due after SilenceHint", nh == 3 && th >= 1500*time.Millisecond && th < 4*time.Second,
		"3 notices at about 2, 7, and 12 s", fmt.Sprintf("%d still-working notices, first at %s", nh, th), tr...)
	s.Check("notices repeat every SilenceRepeat", repeatsEveryFive,
		"about 5 s between the three still-working notices", fmt.Sprintf("notice times %v", hintTimes))
	s.Check("cancel guidance starts at 7 s, not on the first notice", cn == 2 && tc >= 6*time.Second && tc < 10*time.Second,
		"2 notices with cancel guidance, first at about 7 s", fmt.Sprintf("%d notices, first at %s", cn, tc))
	s.Check("the first stuck wording is due at the 17 s cadence point", ns == 2 && ts >= 16*time.Second && ts < 20*time.Second,
		"2 stuck-worded notices, first at about 17 s and next at 22 s", fmt.Sprintf("%d notices, first at %s", ns, ts))
	s.Check("stuck notices say the helper will not retry and how to stop it",
		nr == 2, "2 notices with "+noRetry, fmt.Sprint(nr))
	outcome := steps.Outcome(s.Codex.CallsWithToken(silent))
	stops := len(s.SUTMessagesContaining(work.ID, "stopped")) + len(s.SUTMessagesContaining(work.ID, "canceled")) + len(s.SUTMessagesContaining(work.ID, "interrupted"))
	s.Check("the helper never stops a silent turn by itself", outcome == "running" && stops == 0,
		"Codex turn still running, no stop notice after 22 s", fmt.Sprintf("codex outcome %s; %d stop notices", outcome, stops))

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
	s.Check("the cancelled turn gets no later silence notices", len(s.SUTMessagesContaining(work.ID, hint)) == 3 && len(s.SUTMessagesContaining(work.ID, stuck)) == 2,
		"the notice counts stay at 3 still-working and 2 stuck", fmt.Sprintf("%d still-working, %d stuck", len(s.SUTMessagesContaining(work.ID, hint)), len(s.SUTMessagesContaining(work.ID, stuck))))
}

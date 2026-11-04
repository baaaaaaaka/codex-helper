package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner decision: when Codex refuses a turn because another process holds
// the thread's writer (the session is open in the TUI), the user gets
// exactly one notice, the prompt is not retried, and the next prompt in the
// chat still works on the same thread. The notice must not dump the raw
// Codex error with the thread id.
func TestActiveWriterOneNoticeNoRetry(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ACTIVE-WRITER", harness.Options{
		Guarantees: []string{"active-writer-one-notice"},
		Title:      "active writer: one notice, no retry, the next prompt still works",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	warm, busy, next := s.Token("warm"), s.Token("busy"), s.Token("next")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(busy) {
			return harness.CodexBehavior{FailKind: harness.FailActiveWriter}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "writer")
	steps.MustPost(s, work.ID, "First task "+warm)
	steps.WaitAnswered(s, work.ID, warm, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)

	from := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "Second task "+busy)
	s.Require("the refused turn reached Codex", steps.AnswerTimeout, func() (bool, string) {
		c := s.Codex.CallsWithToken(busy)
		return len(c) > 0 && c[0].Outcome != "running", fmt.Sprintf("%d calls", len(c))
	})
	s.WaitFor("a notice for the refused prompt", 20*time.Second, func() (bool, string) {
		return len(sutAfter(s, work.ID, from)) > 0, "nothing posted"
	})
	time.Sleep(10 * time.Second) // room for a (wrong) retry or a second notice
	s.Settle(3*time.Second, 15*time.Second)
	notices := sutAfter(s, work.ID, from)

	steps.MustPost(s, work.ID, "Next task "+next)
	steps.WaitAnswered(s, work.ID, next, 45*time.Second)
	s.Settle(2*time.Second, 10*time.Second)

	tr := s.Transcript(work.ID, 15)
	var leaks []string
	for _, n := range notices {
		leaks = append(leaks, steps.InternalLeaks(s, n)...)
	}
	s.Check("active writer: exactly one notice", len(notices) == 1, "1", fmt.Sprintf("%d: %q", len(notices), notices), tr...)
	calls := s.Codex.CallsWithToken(busy)
	var outcomes []string
	for _, c := range calls {
		outcomes = append(outcomes, c.Outcome)
	}
	s.Check("active writer: the prompt is not retried", len(calls) == 1, "1 Codex call", fmt.Sprintf("%d: %v", len(calls), outcomes))
	s.Check("active writer: the notice shows no internal ids", len(notices) > 0 && len(leaks) == 0, "no thread id or other internal id", fmt.Sprintf("leaks %q in %q", leaks, notices))
	w, nx := s.Codex.CallsWithToken(warm), s.Codex.CallsWithToken(next)
	answered := len(s.SUTMessagesContaining(work.ID, "answer for "+next)) == 1
	same := len(w) > 0 && len(nx) > 0 && w[0].ThreadID == nx[0].ThreadID
	s.Check("active writer: the next prompt is answered once on the same thread", answered && same, "answered once, same thread",
		fmt.Sprintf("answered=%v same thread=%v (%d calls)", answered, same, len(nx)), tr...)
}

// sutAfter returns the texts of SUT messages in chatID created by requests
// after seq that report a problem with the request. Ordinary turn progress
// ("request accepted", "now running") is not a notice.
func sutAfter(s *harness.Scenario, chatID string, seq int64) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if m.RequestSeq <= seq {
			continue
		}
		text := harness.NormalizeSpace(m.PlainText())
		low := strings.ToLower(harness.TokenPattern.ReplaceAllString(text, ""))
		for _, k := range []string{"writer", "error", "fail", "attention", "problem", "in use", "open in", "another", "busy", "could not", "couldn't", "not run", "tui", "elsewhere"} {
			if strings.Contains(low, k) {
				out = append(out, text)
				break
			}
		}
	}
	return out
}

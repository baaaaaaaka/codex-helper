package chats

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Duplicate prompt (decision: keep the 3-minute duplicate guard; `helper
// retry` forces a rerun). The same text sent twice within 3 minutes runs
// once and the second copy gets one "duplicate ignored" notice; `helper
// retry` then runs it once more.
func TestDuplicatePromptIgnoredThenRetried(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DUP", harness.Options{
		Guarantees: []string{"F-WRK-dup", "R-3", "R-22"},
		Title:      "same prompt twice within 3 min: one run + one notice; `helper retry` forces a rerun",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "dup")
	tok := s.Token("same")
	text := "Run the nightly check " + tok
	steps.MustPost(s, work.ID, text)
	steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)
	steps.MustPost(s, work.ID, text)
	noticed := steps.WaitPromptConfirmation(s, work.ID, "Duplicate request ignored", 20*time.Second)
	s.Settle(2*time.Second, 10*time.Second)
	callsBefore := len(s.Codex.CallsWithToken(tok))
	notices := len(steps.PromptConfirmations(s, work.ID, "Duplicate request ignored"))

	retrySeq := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "helper retry")
	retried := s.Poll(25*time.Second, func() bool { return len(s.Codex.CallsWithToken(tok)) >= 2 })
	s.Settle(3*time.Second, 10*time.Second)
	var replies []string
	for _, m := range s.SUTMessages(work.ID) {
		if m.RequestSeq > retrySeq {
			replies = append(replies, clip(harness.NormalizeSpace(m.PlainText()), 160))
		}
	}

	tr := s.Transcript(work.ID, 15)
	s.Check("R-3 duplicate within 3 min does not run Codex again", callsBefore == 1, "1 call", fmt.Sprint(callsBefore), tr...)
	s.Check("duplicate gets exactly one 'duplicate ignored' notice", noticed && notices == 1, "1 notice", fmt.Sprint(notices))
	s.Check("`helper retry` forces the ignored duplicate to run", retried && len(s.Codex.CallsWithToken(tok)) == 2, "a second Codex call",
		fmt.Sprintf("%d calls; helper replies: %q", len(s.Codex.CallsWithToken(tok)), replies))
	if !retried {
		usage := false
		for _, r := range replies {
			usage = usage || strings.Contains(strings.ToLower(r), "usage") || strings.Contains(strings.ToLower(r), "retry")
		}
		s.Note("helper retry did not rerun; reply mentions retry/usage: %v", usage)
	}
}

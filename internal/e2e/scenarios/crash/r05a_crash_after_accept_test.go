package crash

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-5a / R-1 / R-2 / R-7: the helper is SIGKILLed after Graph accepted the
// POST of a final answer but before the helper learned the message id. After
// restart the answer must exist exactly once (never re-POSTed), the helper's
// own unrecorded message must not become input, and the chat keeps working.
func TestR05a_CrashAfterPostAccepted(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-5a-crash", harness.Options{
		Guarantees: []string{"R-5a", "R-1", "R-2", "R-7"},
		Title:      "SIGKILL between Graph accepting the final and the helper recording it: never re-posted, never echoed",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r05a-crash")

	tok := s.Token("final")
	hold := s.Graph.AddFault(fakegraph.Fault{
		Name:   "accept-then-hold",
		Kind:   fakegraph.FaultAcceptedHold,
		Method: "POST",
		ChatID: work.ID,
		Match:  steps.AnswerPostMatcher(tok),
		Times:  1,
	})
	steps.MustPost(s, work.ID, "Question "+tok)
	s.Require("final POST accepted by Graph and held", steps.AnswerTimeout, func() (bool, string) {
		return hold.Fired() == 1, fmt.Sprintf("fired=%d codex=%s", hold.Fired(), steps.Outcome(s.Codex.CallsWithToken(tok)))
	})
	s.CrashSUT()
	s.StartSUT()

	// Give the restarted helper time to reconcile, then prove the chat is
	// alive with a second prompt.
	tok2 := s.Token("after")
	if _, ok := s.WaitPolled(work.ID, s.Graph.LastSeq(), 30*time.Second); ok {
		steps.MustPost(s, work.ID, "Second question "+tok2)
		steps.WaitAnswered(s, work.ID, tok2, 45*time.Second)
	}
	s.Settle(4*time.Second, 15*time.Second)

	tr := s.Transcript(work.ID, 25)
	posts := steps.AnswerPosts(s, work.ID, tok)
	var outcomes []string
	for _, p := range posts {
		outcomes = append(outcomes, fmt.Sprintf("#%d %s inst=%s", p.Seq, p.Outcome, p.Instance))
	}
	s.Check("R-5a the accepted final is never POSTed again", len(posts) == 1, "1 POST carrying the final",
		fmt.Sprintf("%d: %s", len(posts), strings.Join(outcomes, ", ")), tr...)
	calls := s.Codex.CallsWithToken(tok)
	answer := ""
	if len(calls) > 0 {
		answer = calls[0].Answer
	}
	copies := len(s.SUTMessagesContaining(work.ID, answer))
	s.Check("R-1/R-2 the final is visible exactly once", answer != "" && copies == 1, "1 copy", fmt.Sprint(copies))
	s.Check("R-7 the helper's own unrecorded message is not treated as input", len(calls) == 1, "1 Codex call for the prompt", fmt.Sprint(len(calls)))
	n2 := len(s.Codex.CallsWithToken(tok2))
	s.Check("R-5a the chat keeps working after the crash", n2 == 1 && len(steps.AnswerPosts(s, work.ID, tok2)) == 1, "next prompt answered once",
		fmt.Sprintf("calls=%d posts=%d", n2, len(steps.AnswerPosts(s, work.ID, tok2))))
	var extra []string
	for _, m := range s.SUTMessages(work.ID) {
		if m.RequestSeq > posts[0].Seq && isNotice(m) {
			extra = append(extra, clip(harness.NormalizeSpace(m.PlainText()), 140))
		}
	}
	s.Note("notices after the crash: %q", extra)
}

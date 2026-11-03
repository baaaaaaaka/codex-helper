package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-6 (with R-30): several messages in one chat are answered in order.
// Codex runs one turn at a time per session (FIFO), and each answer appears
// in Teams in the same order as the prompts.
func TestR06_AnswersInOrder(t *testing.T) {
	s := harness.NewScenario(t, "R-6", harness.Options{
		Guarantees: []string{"R-6", "R-30", "R-3"},
		Title:      "three prompts in one chat are executed FIFO and answered in order",
	})
	defer s.Finish()
	// Make each turn take a little time so later prompts arrive while
	// earlier ones are running.
	s.Codex.SetScript(func(harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Delay: 700 * time.Millisecond}
	})

	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r06")

	tokens := []string{s.Token("first"), s.Token("second"), s.Token("third")}
	for i, tok := range tokens {
		if _, err := s.Graph.PostAsUser(work.ID, fmt.Sprintf("Step %d: %s", i+1, tok)); err != nil {
			t.Fatalf("post: %v", err)
		}
		time.Sleep(150 * time.Millisecond) // distinct Teams timestamps, same poll window
	}
	s.WaitFor("all three answers posted", 60*time.Second, func() (bool, string) {
		done := 0
		for _, tok := range tokens {
			for _, c := range s.Codex.CallsWithToken(tok) {
				if c.Answer != "" && len(s.SUTMessagesContaining(work.ID, c.Answer)) > 0 {
					done++
					break
				}
			}
		}
		return done == len(tokens), fmt.Sprintf("%d/3 answered", done)
	})
	s.Settle(3*time.Second, 15*time.Second)

	// Codex order and non-overlap.
	var calls []harness.CodexCall
	for _, tok := range tokens {
		cs := s.Codex.CallsWithToken(tok)
		s.Check("R-3 one Codex call for "+tok, len(cs) == 1, "1", fmt.Sprint(len(cs)))
		if len(cs) > 0 {
			calls = append(calls, cs[0])
		}
	}
	if len(calls) == 3 {
		inOrder := calls[0].Seq < calls[1].Seq && calls[1].Seq < calls[2].Seq
		s.Check("R-30 Codex executes prompts FIFO", inOrder, "seq(first) < seq(second) < seq(third)",
			fmt.Sprintf("seqs %d,%d,%d", calls[0].Seq, calls[1].Seq, calls[2].Seq))
		overlap := false
		for i := 1; i < 3; i++ {
			if !calls[i-1].Ended.IsZero() && calls[i].Started.Before(calls[i-1].Ended) {
				overlap = true
			}
		}
		s.Check("R-30 at most one running turn per session", !overlap, "no overlap", fmt.Sprintf("overlap=%v", overlap))

		// Teams order of the finals.
		msgs := s.SUTMessages(work.ID)
		pos := func(sub string) int {
			for i, m := range msgs {
				if strings.Contains(m.PlainText(), sub) {
					return i
				}
			}
			return -1
		}
		p := []int{pos(calls[0].Answer), pos(calls[1].Answer), pos(calls[2].Answer)}
		ordered := p[0] >= 0 && p[0] < p[1] && p[1] < p[2]
		s.Check("R-6 answers appear in prompt order", ordered, "answer1 < answer2 < answer3 in chat",
			fmt.Sprintf("positions %v", p), s.Transcript(work.ID, 30)...)
		for i, c := range calls {
			n := len(s.SUTMessagesContaining(work.ID, c.Answer))
			s.Check(fmt.Sprintf("R-2 answer %d posted once", i+1), n == 1, "1", fmt.Sprint(n))
		}
	}
}

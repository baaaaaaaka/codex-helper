package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// A turn whose agent messages carry no phase posts its last message once,
// as the answer, never also as Codex status (F-TRN-15, R-33, R-2; INC-01).
//
// History: "status + answer" doubles were the most reported Teams symptom
// (INC-01, U-I01: the same text twice, once as progress and once as the
// final). Codex streams agent messages without a phase for models that
// have no message phases; v0.1.20 kept a one-message look-behind for such
// streams ("their last agent message may be the final answer, so sending
// the newest message eagerly could expose final text as Teams progress",
// internal/teams/bridge.go:11959 at v0.1.20).
//
// The turn streams two unphased intermediate messages and an unphased
// final. Each intermediate message is posted once as status, before the
// answer; the final is posted once, as the answer.
func TestUnphasedFinalPostedOnce(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "TURN-UNPHASED", harness.Options{
		Guarantees: []string{"F-TRN-15", "R-33", "R-2"},
		Title:      "agent messages without a phase: intermediate ones are status, the last is posted once as the answer",
		Budget:     60 * time.Second,
	})
	defer s.Finish()
	tok := s.Token("unphased")
	step1, step2 := "Looking at the build files "+s.Token("stepone"), "Running the unit tests "+s.Token("steptwo")
	answer := "All tests pass now " + s.Token("answer")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Unphased: true, Commentary: step1, MoreCommentary: []string{step2}, Answer: answer}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "unphased")
	steps.MustPost(s, work.ID, "Fix the failing tests "+tok)
	s.WaitFor("the answer is posted", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, answer)) > 0, "not yet"
	})
	s.Settle(2*time.Second, 8*time.Second)

	tr := s.Transcript(work.ID, 12)
	label := func(m fakegraph.Message) string {
		text := m.PlainText()
		switch {
		case strings.Contains(text, "Codex answer"):
			return "answer"
		case strings.Contains(text, "Codex status"):
			return "status"
		}
		return "other"
	}
	describe := func(ms []fakegraph.Message) []string {
		var out []string
		for _, m := range ms {
			out = append(out, label(m))
		}
		return out
	}
	finals := s.SUTMessagesContaining(work.ID, answer)
	s.Check("the unphased final is posted once, as the answer", len(finals) == 1 && label(finals[0]) == "answer",
		"[answer]", fmt.Sprint(describe(finals)), tr...)
	order := []string{}
	for _, text := range []string{step1, step2} {
		ms := s.SUTMessagesContaining(work.ID, text)
		s.Check(fmt.Sprintf("intermediate message %q is posted once, as status", text[:20]), len(ms) == 1 && label(ms[0]) == "status",
			"[status]", fmt.Sprint(describe(ms)), tr...)
		if len(ms) > 0 {
			order = append(order, ms[0].ID)
		}
	}
	if len(finals) > 0 {
		order = append(order, finals[0].ID)
	}
	var seq []string
	for _, m := range s.Graph.Messages(work.ID) {
		for i, id := range order {
			if m.ID == id {
				seq = append(seq, fmt.Sprint(i+1))
			}
		}
	}
	s.Check("status messages come before the answer, in stream order", strings.Join(seq, ",") == "1,2,3",
		"1,2,3", strings.Join(seq, ","), tr...)
}

package turns

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner ruling 2026-11-05: a capacity retry sends the original prompt again
// only while Codex never started the turn; once Codex accepted an attempt
// (its user message is in the thread, even without progress) the retry
// sends the literal `continue`. Here turn/start is first refused with the
// capacity error (nothing recorded: the prompt is resent), then the turn
// is accepted and fails with the capacity error (recorded: `continue`),
// then turn/start is refused once more (still `continue`), then it answers.
// The window and backoff are scaled (seam knobs).
func TestCapacityRetryContinuesOnlyAfterCodexRecordedThePrompt(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CAP-CONTINUE", harness.Options{
		Guarantees: []string{"capacity-retry-5min"},
		Title:      "capacity: refused turn/start resends the prompt; after Codex accepted the turn every retry sends `continue`",
		Budget:     90 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"CapacityRetry": "60s", "CapacityBackoff": "300ms"}},
	})
	defer s.Finish()
	tok := s.Token("capcont")
	answer := "Answered after continuing " + tok
	refused := harness.CodexBehavior{FailKind: harness.FailActiveWriter, Err: errors.New("Selected model is at capacity. Please try a different model.")}
	var mu sync.Mutex
	thread, n := "", 0
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		mu.Lock()
		defer mu.Unlock()
		if c.Kind != "work" || !(c.HasToken(tok) || (thread != "" && c.ThreadID == thread)) {
			return harness.CodexBehavior{}
		}
		thread = c.ThreadID
		n++
		switch n {
		case 1, 3:
			return refused
		case 2:
			return harness.CodexBehavior{FailKind: harness.FailCapacity}
		}
		return harness.CodexBehavior{Answer: answer}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "capcontinue")
	steps.MustPost(s, work.ID, "Task refused then accepted at capacity "+tok)
	answered := s.Poll(60*time.Second, func() bool {
		return len(s.SUTMessagesContaining(work.ID, answer)) > 0 || len(s.SUTMessagesContaining(work.ID, "over capacity")) > 0
	})
	s.Settle(2*time.Second, 8*time.Second)
	calls := attempts(s, tok)
	var lines []string
	for _, c := range calls {
		lines = append(lines, fmt.Sprintf("#%d %s prompt=%.50q", c.Seq, c.Outcome, c.Prompt))
	}
	tr := s.Transcript(work.ID, 15)
	finals := len(s.SUTMessagesContaining(work.ID, answer))
	notices := len(s.SUTMessagesContaining(work.ID, "at capacity right now"))
	s.Check("capacity: refused, accepted-and-failed and refused again, then answered once", answered && len(calls) == 4 && finals == 1,
		"4 attempts, answered once", fmt.Sprintf("%d attempts, %d answers: %s", len(calls), finals, strings.Join(lines, " | ")), tr...)
	s.Check("capacity: one retry notice", notices == 1, "1", fmt.Sprint(notices), tr...)
	checkRetriesContinue(s, work.ID, calls, tok, 2)
}

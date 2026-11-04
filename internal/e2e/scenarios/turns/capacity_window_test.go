package turns

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Capacity retry window (decision: "满载重试：按 08-11 的规格 … 修复 5 分钟
// 计时从原始开始时间算起的问题"). The 08-11 spec (ORIGINS T4a) retries up to
// 10 consecutive capacity failures and resets after substantive progress;
// the 5-minute bound limits waiting for capacity, it must not fail a long
// task whose continuation made progress for longer than that. The window
// is scaled to 5 s (E2E_SUT_TIMING CapacityRetry=5s, CapacityBackoff=300ms).
//
// Attempt 1 fails with capacity at once. Attempt 2 makes progress for 8 s
// (longer than the window) and then fails with capacity again. Attempt 3
// answers. The request must be answered once with no failure notice.
func TestCapacityWindowResetsAfterProgress(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CAP-WINDOW", harness.Options{
		Guarantees: []string{"capacity-retry-5min"},
		Title:      "capacity error, then a continuation that works longer than the retry window and hits capacity again: retried again, answered once",
		Budget:     90 * time.Second,
		SUTEnv:     map[string]string{"E2E_SUT_TIMING": "CapacityRetry=5s,CapacityBackoff=300ms"},
	})
	defer s.Finish()
	tok := s.Token("capwin")
	answer := "Finished the long task " + tok
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
		case 1:
			return harness.CodexBehavior{FailKind: harness.FailCapacity}
		case 2:
			return harness.CodexBehavior{Commentary: "Step one of the long task is done.", Delay: 8 * time.Second, FailKind: harness.FailCapacity}
		}
		return harness.CodexBehavior{Answer: answer}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "capwindow")
	steps.MustPost(s, work.ID, "Long task hit by capacity twice "+tok)
	answered := s.Poll(60*time.Second, func() bool {
		return len(s.SUTMessagesContaining(work.ID, answer)) > 0 || len(s.SUTMessagesContaining(work.ID, "over capacity")) > 0
	})
	s.Settle(2*time.Second, 8*time.Second)
	calls := attempts(s, tok)
	var lines []string
	for _, c := range calls {
		lines = append(lines, fmt.Sprintf("#%d %s start=%s end=%s prompt=%.50q", c.Seq, c.Outcome, c.Started.Format("15:04:05.000"), c.Ended.Format("15:04:05.000"), c.Prompt))
	}
	tr := s.Transcript(work.ID, 15)
	failed := len(s.SUTMessagesContaining(work.ID, "over capacity"))
	finals := len(s.SUTMessagesContaining(work.ID, answer))
	s.Check("a capacity error after progress longer than the window is retried", answered && len(calls) == 3 && finals == 1,
		"3 attempts, answered once", fmt.Sprintf("%d attempts, %d answers: %s", len(calls), finals, strings.Join(lines, " | ")), tr...)
	s.Check("no capacity failure notice", failed == 0, "0", fmt.Sprint(failed), tr...)
	if len(calls) >= 2 {
		s.Note("retry prompt after an accepted turn failed: %.120q", calls[1].Prompt)
	}
}

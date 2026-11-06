package turns

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Codex reconnecting within a turn is shown as a Codex status (F-TRN-15
// "a stream reconnect is shown once"; v0.1.20 sendStreamRetryStatus,
// rc.15 B3). Codex sends an `error` notification with willRetry when its
// response stream drops and it reconnects by itself. v0.1.20 posted
// "Connection dropped. Codex is reconnecting." with Codex's message and
// reason, at most once per 5 minutes per turn (scaled to 3 s through the
// seam's RetryStatus knob), and the turn's answer followed. The message is
// sanitized: a cxp proxy token in a URL is redacted and the text is kept
// on one line.
func TestCodexRetryStatus(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "RETRY-STATUS", harness.Options{
		Guarantees: []string{"F-TRN-15"},
		Title:      "Codex reconnects within a turn: a \"Connection dropped. Codex is reconnecting.\" status with Codex's message and reason, at most once per RetryStatus per turn, then the answer",
		Budget:     90 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"RetryStatus": "3s"}},
	})
	defer s.Finish()
	tok := s.Token("retry")
	answer := "Answered after reconnecting " + tok
	dropped := map[string]any{"responseStreamDisconnected": map[string]any{"httpStatusCode": nil}}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if !c.HasToken(tok) {
			return harness.CodexBehavior{}
		}
		return harness.CodexBehavior{Answer: answer, Retries: []fakecodex.Retry{
			{Message: "Reconnecting... 1/5", Details: "stream disconnected before completion:\n  error sending request for url (http://127.0.0.1:41234/_cxp/s3cr3t-token/v1/responses)", Info: dropped},
			{Delay: 200 * time.Millisecond, Message: "Reconnecting... 2/5", Details: "stream disconnected before completion", Info: dropped},
			{Delay: 4 * time.Second, Message: "Reconnecting... 3/5", Info: dropped},
		}}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "retry")
	steps.MustPost(s, work.ID, "A task over a flaky connection "+tok)
	s.Poll(steps.AnswerTimeout, func() bool { return len(s.SUTMessagesContaining(work.ID, answer)) > 0 })
	s.Settle(time.Second, 4*time.Second)
	tr := s.Transcript(work.ID, 12)

	var statuses []string
	for _, m := range s.SUTMessagesContaining(work.ID, "Connection dropped. Codex is reconnecting.") {
		statuses = append(statuses, harness.NormalizeSpace(m.PlainText()))
	}
	s.Check("two reconnect statuses: the first, and the next after RetryStatus (the one in between is rate-limited)", len(statuses) == 2,
		"2 statuses", fmt.Sprintf("%d: %q", len(statuses), statuses), tr...)
	first, second := "", ""
	if len(statuses) == 2 {
		first, second = statuses[0], statuses[1]
	}
	s.Check("the status carries Codex's message, details and reason",
		strings.Contains(first, "Reconnecting... 1/5: stream disconnected before completion: error sending request") &&
			strings.Contains(first, "Reason: responseStreamDisconnected") && strings.Contains(second, "Reconnecting... 3/5"),
		"1/5 with details and reason; then 3/5", fmt.Sprintf("%.300q / %.200q", first, second), tr...)
	s.Check("a cxp proxy token in Codex's message is redacted", !strings.Contains(strings.Join(statuses, " "), "s3cr3t-token"),
		"no token", fmt.Sprintf("%.300q", first), tr...)
	answers := len(s.SUTMessagesContaining(work.ID, answer))
	call := s.Codex.CallsWithToken(tok)
	s.Check("the turn is answered once, after the statuses", answers == 1 && len(call) == 1 && call[0].Outcome == "answered",
		"1 answer", fmt.Sprintf("%d answers", answers), tr...)
}

package turns

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/continuation"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Owner decision: a model-capacity failure is retried automatically and the
// turn is answered within 5 minutes of the FIRST capacity error; the user
// gets the answer once and no failure message. Here Codex fails the turn
// twice with the capacity error and then succeeds. Codex recorded the
// prompt with the first attempt, so both retries send the shared instruction on the
// same thread (owner ruling 2026-11-05); every call on that thread after
// the first failure counts as a retry.
func TestCapacityRetryWithinFiveMinutes(t *testing.T) {
	harness.Parallel(t)
	const window = 5 * time.Minute
	s := harness.NewScenario(t, "CAP-RETRY", harness.Options{
		Guarantees: []string{"capacity-retry-5min"},
		Title:      "capacity error twice, then success: retried automatically, answered once within 5 min of the first error",
		Budget:     window + 90*time.Second,
	})
	defer s.Finish()
	tok := s.Token("capacity")
	answer := "Recovered after the capacity wait " + tok
	var mu sync.Mutex
	thread, fails := "", 0
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		mu.Lock()
		defer mu.Unlock()
		if c.Kind != "work" || !(c.HasToken(tok) || (thread != "" && c.ThreadID == thread)) {
			return harness.CodexBehavior{}
		}
		if fails < 2 {
			fails++
			thread = c.ThreadID
			return harness.CodexBehavior{FailKind: harness.FailCapacity}
		}
		return harness.CodexBehavior{Answer: answer}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "capacity")
	posted := time.Now()
	steps.MustPost(s, work.ID, "Task hit by capacity "+tok)
	s.Require("the first attempt reached Codex", steps.AnswerTimeout, func() (bool, string) {
		return len(s.Codex.CallsWithToken(tok)) > 0, "no call"
	})
	answered := s.WaitFor("the answer arrives after automatic retries", window+30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, answer)) > 0, fmt.Sprintf("%d Codex calls on the thread", len(attempts(s, tok)))
	})
	s.Settle(3*time.Second, 15*time.Second)

	calls := attempts(s, tok)
	var firstErr, success time.Time
	var lines []string
	for _, c := range calls {
		lines = append(lines, fmt.Sprintf("#%d %s start=%s end=%s err=%.60q", c.Seq, c.Outcome, c.Started.Format("15:04:05.000"), c.Ended.Format("15:04:05.000"), c.Err))
		if c.Outcome == "failed" && firstErr.IsZero() {
			firstErr = c.Ended
		}
		if c.Outcome == "answered" && success.IsZero() {
			success = c.Started
		}
	}
	tr := s.Transcript(work.ID, 15)
	ev := append(lines, tr...)
	n := len(s.SUTMessagesContaining(work.ID, answer))
	s.Check("capacity: the turn is retried without user action and answered once", answered && n == 1, "1 answer", fmt.Sprintf("%d answers, %d attempts", n, len(calls)), ev...)
	within := !firstErr.IsZero() && !success.IsZero() && success.Sub(firstErr) <= window
	s.Check("capacity: the successful retry starts within 5 min of the first capacity error", within, "<= 5m0s",
		fmt.Sprintf("first error %s, successful attempt %s (prompt posted %s)", firstErr.Format("15:04:05.000"), success.Format("15:04:05.000"), posted.Format("15:04:05.000")), ev...)
	var failures []string
	for _, m := range s.SUTMessages(work.ID) {
		low := strings.ToLower(m.PlainText())
		if strings.Contains(low, "failed") || strings.Contains(low, "error") || strings.Contains(low, "could not") {
			failures = append(failures, harness.NormalizeSpace(m.PlainText()))
		}
	}
	s.Check("capacity: no failure message once the retry succeeded", len(failures) == 0, "0 failure messages", fmt.Sprintf("%q", failures))
	checkRetriesContinue(s, work.ID, calls, tok, 1)
}

// mirroredContinue matches a mirrored Codex prompt ("🧑‍💻 User: …") that is
// the `continue` retry.
var mirroredContinue = regexp.MustCompile(`(?is)\bUser\b\W*continue\b`)

// checkRetriesContinue checks the owner ruling of 2026-11-05: the prompt
// reaches Codex only until Codex records it (the first sent calls carry
// it), every later retry sends the short continuation instruction on the same thread
// with the turn's own client id, and that continuation is never mirrored
// to Teams as a prompt typed in Codex.
func checkRetriesContinue(s *harness.Scenario, chatID string, calls []harness.CodexCall, tok string, sent int) {
	var got []string
	ok := len(calls) > sent
	for i, c := range calls {
		got = append(got, fmt.Sprintf("#%d %s %.40q", i+1, c.Outcome, c.Prompt))
		switch {
		case i < sent:
			ok = ok && c.HasToken(tok)
		default:
			ok = ok && c.Prompt == continuation.Prompt && !c.HasToken(tok) && strings.Contains(c.Prompt, "Keep the previous response language unless the user requests otherwise.") && c.ThreadID == calls[sent-1].ThreadID
		}
		ok = ok && c.ClientID == calls[0].ClientID && c.ClientID != ""
	}
	s.Check("capacity: once Codex recorded the prompt, every retry sends `continue` in the same thread, with the same client id", ok,
		fmt.Sprintf("the prompt %d time(s), then only `continue`", sent), strings.Join(got, " | "))
	var mirrored []string
	for _, m := range s.SUTMessages(chatID) {
		if mirroredContinue.MatchString(m.PlainText()) {
			mirrored = append(mirrored, harness.NormalizeSpace(m.PlainText()))
		}
	}
	s.Check("capacity: the `continue` retry is not mirrored to Teams", len(mirrored) == 0, "no message shows it", fmt.Sprintf("%q", mirrored))
}

// attempts returns the work calls that carried tok or ran on its thread.
func attempts(s *harness.Scenario, tok string) []harness.CodexCall {
	first := s.Codex.CallsWithToken(tok)
	if len(first) == 0 {
		return nil
	}
	var out []harness.CodexCall
	for _, c := range s.Codex.Calls() {
		if c.Kind == "work" && (c.HasToken(tok) || (first[0].ThreadID != "" && c.ThreadID == first[0].ThreadID)) {
			out = append(out, c)
		}
	}
	return out
}

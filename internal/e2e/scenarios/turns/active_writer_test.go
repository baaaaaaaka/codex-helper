package turns

import (
	"fmt"
	"strings"
	"sync/atomic"
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
//
// Real Codex 0.166 refuses at thread/resume (TestActiveWriterOnResume);
// this variant keeps the turn/start refusal (FailActiveWriter) so the SUT
// is tested on both paths.
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

// writerNotice is the helper's notice for a prompt Codex refused because
// another process writes to the session (the exact sentence; the golden
// pins the rest of the transcript).
const writerNotice = "Codex did not run this request: the session is open in another Codex window that is writing to it."

// sutAfter returns the texts of the active-writer notices in chatID created
// by requests after seq.
func sutAfter(s *harness.Scenario, chatID string, seq int64) []string {
	var out []string
	for _, m := range s.SUTMessages(chatID) {
		if text := harness.NormalizeSpace(m.PlainText()); m.RequestSeq > seq && strings.Contains(text, writerNotice) {
			out = append(out, text)
		}
	}
	return out
}

// ACTIVE-WRITER-RESUME: the same decision on the path real Codex 0.166
// takes. The helper restarts (so its new app-server must resume the
// thread), the session is open in "the TUI" (another process holds the
// writer), and the fake refuses thread/resume with the real error. The
// prompt must not run anywhere (no retry, no silent new thread); after the
// TUI lets go, the next prompt runs on the same thread.
func TestActiveWriterOnResume(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ACTIVE-WRITER-RESUME", harness.Options{
		Guarantees: []string{"active-writer-one-notice"},
		Title:      "active writer at thread/resume (real 0.166 path): one notice, no retry, no new thread, next prompt works",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	warm, busy, next := s.Token("warm"), s.Token("busy"), s.Token("next")
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "writer-resume")
	steps.MustPost(s, work.ID, "First task "+warm)
	steps.WaitAnswered(s, work.ID, warm, steps.AnswerTimeout)
	thread := ""
	if c := s.Codex.CallsWithToken(warm); len(c) > 0 {
		thread = c[0].ThreadID
	}
	s.Settle(2*time.Second, 10*time.Second)

	// Restart the helper while "the TUI" opens the session.
	var held atomic.Bool
	held.Store(true)
	s.Codex.SetResumeScript(func(c harness.ResumeCall) error {
		if c.ThreadID == thread && held.Load() {
			return harness.ErrActiveWriter
		}
		return nil
	})
	s.StopSUT()
	s.StartSUT()
	s.WaitPolled(work.ID, s.Graph.LastSeq(), steps.StartupTimeout)
	s.Settle(2*time.Second, 10*time.Second)

	from := s.Graph.LastSeq()
	resumesBefore := len(s.Codex.Resumes())
	steps.MustPost(s, work.ID, "Second task "+busy)
	s.WaitFor("a notice for the refused prompt", 30*time.Second, func() (bool, string) {
		return len(sutAfter(s, work.ID, from)) > 0, fmt.Sprintf("%d resumes since the prompt", len(s.Codex.Resumes())-resumesBefore)
	})
	time.Sleep(10 * time.Second) // room for a (wrong) retry or a second notice
	s.Settle(3*time.Second, 15*time.Second)
	notices := sutAfter(s, work.ID, from)
	var rejected, allResumes []string
	for _, r := range s.Codex.Resumes()[resumesBefore:] {
		allResumes = append(allResumes, fmt.Sprintf("%s %s rejected=%v", r.At.Format("15:04:05.000"), r.Kind, r.Rejected != ""))
		if r.ThreadID == thread && r.Rejected != "" {
			rejected = append(rejected, r.Rejected)
		}
	}
	busyCalls := s.Codex.CallsWithToken(busy)

	held.Store(false) // the TUI closes the session
	steps.MustPost(s, work.ID, "Next task "+next)
	steps.WaitAnswered(s, work.ID, next, 45*time.Second)
	s.Settle(2*time.Second, 10*time.Second)

	tr := s.Transcript(work.ID, 15)
	var leaks []string
	for _, n := range notices {
		leaks = append(leaks, steps.InternalLeaks(s, n)...)
	}
	s.Check("active writer (resume): the refusal path was exercised", len(rejected) > 0, ">= 1 refused thread/resume", fmt.Sprintf("%d refused; resumes: %v", len(rejected), allResumes))
	s.Check("active writer (resume): exactly one notice", len(notices) == 1, "1", fmt.Sprintf("%d: %q", len(notices), notices), tr...)
	var where []string
	for _, c := range busyCalls {
		where = append(where, c.ThreadID+"/"+c.Outcome)
	}
	s.Check("active writer (resume): the prompt is neither retried nor run on another thread", len(rejected) == 1 && len(busyCalls) == 0,
		"1 refused resume, 0 turns", fmt.Sprintf("%d refused resumes, turns: %v", len(rejected), where))
	s.Check("active writer (resume): the notice shows no internal ids", len(notices) > 0 && len(leaks) == 0, "no thread id or other internal id", fmt.Sprintf("leaks %q in %q", leaks, notices))
	nx := s.Codex.CallsWithToken(next)
	answered := len(s.SUTMessagesContaining(work.ID, "answer for "+next)) == 1
	same := len(nx) > 0 && nx[0].ThreadID == thread
	s.Check("active writer (resume): once the TUI lets go, the next prompt is answered once on the same thread", answered && same, "answered once, thread "+thread,
		fmt.Sprintf("answered=%v same thread=%v (%d calls)", answered, same, len(nx)), tr...)
}

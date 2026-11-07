package crash

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// A write throttle (429 with Retry-After) outlives a crash: the restarted
// helper sends no Graph write before the Retry-After ends, neither the
// throttled answer nor anything for a prompt that arrives meanwhile in
// another chat, then delivers both answers once (R-12, R-2).
//
// History: throttling storms, where restarts and retries ignored
// Retry-After and kept the account throttled for hours (INC-06, U-I05;
// v0.1.22 backlog recovery under throttling, 3b3ada19). SPEC SC-06c. The
// global retry-after invariant compares requests of one helper process
// only, so a restart that forgets the throttle passes it.
func TestWriteThrottleSurvivesCrash(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "THROTTLE-CRASH", harness.Options{
		Guarantees: []string{"R-12", "R-2"},
		Title:      "an answer POST gets 429 Retry-After 12 s, then SIGKILL, restart and a prompt in another chat: no Graph write before the 12 s end, each answer arrives once",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "throttlecrash")
	other, _ := steps.NewWorkChat(s, control, "otherchat")
	tok := s.Token("throttled")
	const retryAfter = 12 * time.Second
	fault := s.Graph.AddFault(fakegraph.Fault{
		Name: "answer-429", Kind: fakegraph.FaultStatus, Status: http.StatusTooManyRequests, RetryAfter: retryAfter,
		Method: "POST", ChatID: work.ID, Match: steps.AnswerPostMatcher(tok), Times: 1,
	})
	steps.MustPost(s, work.ID, "Answer me through the throttle "+tok)
	s.Require("the answer POST is throttled", steps.AnswerTimeout, func() (bool, string) {
		return fault.Fired() == 1, fmt.Sprintf("fired=%d", fault.Fired())
	})
	// The fault counts when the request arrives; the 429 is recorded once
	// its response is written. The crash comes after the helper had a
	// moment to take the 429 in: a throttle it never received cannot
	// outlive the crash (CI: SIGKILL with the POST still in flight, and
	// the restarted helper rightly knew of no throttle).
	var throttledAt time.Time
	s.Require("the 429 is answered", 10*time.Second, func() (bool, string) {
		for _, r := range s.Graph.Requests(fakegraph.RequestFilter{}) {
			if r.Fault == "answer-429" {
				throttledAt = r.At.Add(r.Duration)
				return true, ""
			}
		}
		return false, "not answered yet"
	})
	time.Sleep(time.Second)
	s.CrashSUT()
	s.StartSUT()
	// The throttle is account-wide (no scope header): it also holds the
	// acknowledgement and answer of a prompt read after the restart.
	tok2 := s.Token("meanwhile")
	steps.MustPost(s, other.ID, "Another question during the throttle "+tok2)
	steps.WaitAnswered(s, work.ID, tok, 45*time.Second)
	steps.WaitAnswered(s, other.ID, tok2, 45*time.Second)
	s.Settle(3*time.Second, 12*time.Second)

	until := throttledAt.Add(retryAfter)
	var early []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{}) {
		if r.Host != "graph.microsoft.com" || r.Method == "GET" || !r.At.After(throttledAt) || !r.At.Before(until) {
			continue
		}
		early = append(early, fmt.Sprintf("%s %s %.1fs after the 429 (%s)", r.Method, r.Route, r.At.Sub(throttledAt).Seconds(), r.Instance))
	}
	s.Check("no Graph write before the Retry-After ends, across the restart", len(early) == 0,
		fmt.Sprintf("no write within %s of the 429", retryAfter), strings.Join(early, "; "))
	for _, c := range []struct{ name, chat, tok string }{{"the throttled answer", work.ID, tok}, {"the answer of the prompt sent meanwhile", other.ID, tok2}} {
		n := 0
		for _, r := range steps.AnswerPosts(s, c.chat, c.tok) {
			if r.Status >= 200 && r.Status < 300 {
				n++
			}
		}
		calls := len(s.Codex.CallsWithToken(c.tok))
		s.Check(c.name+" is posted once and Codex ran once", n == 1 && calls == 1,
			"1 accepted answer POST, 1 Codex call", fmt.Sprintf("%d accepted answer POSTs, %d calls", n, calls), s.Transcript(c.chat, 8)...)
	}
}

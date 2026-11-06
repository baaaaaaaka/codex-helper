package faults

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-11 (with R-21): a chat whose message list keeps returning 5xx does not
// block answers in another chat.
func TestR11_BadChatDoesNotBlockOthers(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-11", harness.Options{
		Guarantees: []string{"R-11", "R-21"},
		Title:      "persistent 503 on one chat's message list does not delay another chat",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	bad, _ := steps.NewWorkChat(s, control, "r11-bad")
	good, _ := steps.NewWorkChat(s, control, "r11-good")

	// Baseline latency on the good chat before any fault.
	base := s.Token("baseline")
	steps.MustPost(s, good.ID, "Baseline "+base)
	baseLatency, _ := steps.WaitAnswered(s, good.ID, base, steps.AnswerTimeout)

	fault := s.Graph.AddFault(fakegraph.Fault{
		Name: "bad-chat-503", Kind: fakegraph.FaultStatus, Status: http.StatusServiceUnavailable,
		Method: "GET", Route: "GET /chats/{chat}/messages", ChatID: bad.ID,
	})
	faultAt := s.Graph.LastSeq()
	faultStart := time.Now()
	badTok := s.Token("inbad")
	steps.MustPost(s, bad.ID, "Stuck chat message "+badTok)
	// Let the SUT hit the broken chat a few times first.
	s.WaitFor("SUT hits the broken chat", 20*time.Second, func() (bool, string) {
		return fault.Fired() >= 2, fmt.Sprintf("%d faulted reads", fault.Fired())
	})
	goodTok := s.Token("ingood")
	steps.MustPost(s, good.ID, "Healthy chat message "+goodTok)
	latency, ok := steps.WaitAnswered(s, good.ID, goodTok, 45*time.Second)
	limit := 3*baseLatency + 5*time.Second
	s.Check("R-11 healthy chat answered while another chat's reads fail", ok && latency <= limit,
		fmt.Sprintf("answered within %s (3x baseline %s + 5s)", limit.Round(time.Millisecond), baseLatency.Round(time.Millisecond)),
		fmt.Sprintf("answered=%v after %s", ok, latency.Round(time.Millisecond)), s.RequestsSummary(fakegraph.RequestFilter{Since: faultAt})...)
	reads := len(s.Graph.Requests(fakegraph.RequestFilter{Route: "GET /chats/{chat}/messages", ChatID: bad.ID, Since: faultAt}))
	s.Note("broken chat: %d list reads (%d faulted) in %s since the fault; bad-chat message codex calls=%d",
		reads, fault.Fired(), time.Since(faultStart).Round(time.Second), len(s.Codex.CallsWithToken(badTok)))
	s.Note("SUT messages in broken chat (incl. setup): %d; control tail: %v", len(s.SUTMessages(bad.ID)), s.Transcript(control.ID, 3))
}

// R-12: sustained 429 on GET message lists does not stop POSTs of pending
// answers (a read 429 never blocks writes).
func TestR12_Read429DoesNotBlockWrites(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-12", harness.Options{
		Guarantees: []string{"R-12"},
		Title:      "all message-list reads throttled (429, Retry-After 30s) while answers are pending",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	a, _ := steps.NewWorkChat(s, control, "r12-a")
	b, _ := steps.NewWorkChat(s, control, "r12-b")
	tokA, tokB := s.Token("pendingA"), s.Token("pendingB")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		return harness.CodexBehavior{Block: true}
	})
	steps.MustPost(s, a.ID, "Pending answer "+tokA)
	steps.MustPost(s, b.ID, "Pending answer "+tokB)
	s.WaitCodexCalls(tokA, 1, steps.AnswerTimeout)
	s.WaitCodexCalls(tokB, 1, steps.AnswerTimeout)

	throttle := s.Graph.AddFault(fakegraph.Fault{
		Name: "read-429", Kind: fakegraph.FaultStatus, Status: http.StatusTooManyRequests, RetryAfter: 30 * time.Second,
		Method: "GET", Route: "GET /chats/{chat}/messages",
	})
	since := s.Graph.LastSeq()
	installedAt := time.Now()
	s.WaitFor("SUT sees the read throttle", 20*time.Second, func() (bool, string) {
		return throttle.Fired() >= 1, "no throttled read yet"
	})
	releasedAt := time.Now()
	s.Codex.ReleaseAll()
	_, okA := steps.WaitAnswered(s, a.ID, tokA, 25*time.Second)
	_, okB := steps.WaitAnswered(s, b.ID, tokB, 25*time.Second)
	elapsed := time.Since(releasedAt)
	s.Check("R-12 pending answers are POSTed while every read is throttled", okA && okB,
		"both answers posted within 25s of Codex finishing (Retry-After is 30s)",
		fmt.Sprintf("A=%v B=%v after %s", okA, okB, elapsed.Round(time.Millisecond)),
		s.RequestsSummary(fakegraph.RequestFilter{Since: since})...)

	// Reads per chat must respect Retry-After (no tight retry loop).
	worst := 0
	worstChat := ""
	for _, c := range s.Graph.Chats() {
		n := 0
		for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Route: "GET /chats/{chat}/messages", ChatID: c.ID, Since: since}) {
			if r.Fault == "read-429" {
				n++
			}
		}
		if n > worst {
			worst, worstChat = n, c.Topic
		}
	}
	window := time.Since(installedAt)
	allowed := 1 + int(window/(30*time.Second))
	s.Check("R-12 throttled reads per chat honour Retry-After", worst <= allowed+1,
		fmt.Sprintf("<= %d throttled reads per chat in %s", allowed+1, window.Round(time.Second)),
		fmt.Sprintf("worst chat %q: %d throttled reads", worstChat, worst))
	throttle.Disarm()
}

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

// Graph fails every request for a while the helper runs: the helper stays
// up without a request storm, and once Graph is back the prompt sent
// meanwhile is answered once and the local session started meanwhile gets
// one chat (R-11, R-9, R-2).
//
// History: Teams outages restarted the helper in a loop or left it
// "unhealthy" until a manual restart (INC-09, SC-09a; the 05-29 restart
// storm), and sessions created while Graph was failing were filed as old
// and never mirrored (SC-20a).
func TestGraphOutageWhileRunning(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "GRAPH-OUTAGE", harness.Options{
		Guarantees: []string{"R-11", "R-9", "R-2"},
		Title:      "Graph returns 503 to everything for 25 s: no restart, no storm; afterwards the prompt is answered once and a new local session gets one chat",
		Budget:     120 * time.Second,
		// Polling goes on at the poll cadence through an outage so that
		// recovery takes one poll (SC-09a); over a 25 s outage the same
		// chat list and message reads fail more than CapFailingRepeats
		// times. The scenario bounds the failed-request rate itself.
		Invariants: harness.Invariants{Except: []harness.Exception{{
			Invariant: harness.InvNoLoop,
			Match:     `the same request failed \d+\+ times: GET GET /(me/chats|chats/\{chat\}/messages) \(last status 503\)`,
			Reason:    "deliberate 25 s Graph-wide 503: reads continue at the poll cadence",
		}}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		s.Check("history watcher initialised", false, "probe session mirrored", "never mirrored")
		return
	}
	work, _ := steps.NewWorkChat(s, control, "outage")
	pid := 0
	if p, ok := s.SUT.(interface{ Pid() int }); ok {
		pid = p.Pid()
	}

	from := s.Graph.LastSeq()
	fault := s.Graph.AddFault(fakegraph.Fault{
		Name: "graph-down", Kind: fakegraph.FaultStatus, Status: http.StatusServiceUnavailable, Host: "graph.microsoft.com",
	})
	start := time.Now()
	tok := s.Token("duringoutage")
	steps.MustPost(s, work.ID, "Sent while Teams is down "+tok)
	var answers []string
	for i := 1; i <= 1; i++ { // one: two found together get their chats in either order
		r := harness.NewRollout(t, s.Env, s.Env.WorkDir(t, fmt.Sprintf("outage-tui-%d", i)))
		answers = append(answers, s.Token(fmt.Sprintf("tui%d", i)))
		r.AppendTurn(t, fmt.Sprintf("Local session %d started during the outage", i), "", "Local answer "+answers[i-1])
	}
	s.Poll(25*time.Second, func() bool { return false })
	fault.Disarm()
	outage := time.Since(start)
	faulted := fault.Fired()
	s.Note("outage %.0fs: %d Graph requests failed", outage.Seconds(), faulted)

	steps.WaitAnswered(s, work.ID, tok, 60*time.Second)
	s.WaitFor("the local session is mirrored", 60*time.Second, func() (bool, string) {
		for _, a := range answers {
			if len(steps.ChatsWithSUTText(s, a)) == 0 {
				return false, "not all mirrored"
			}
		}
		return true, ""
	})
	s.Settle(3*time.Second, 15*time.Second)

	now := 0
	if p, ok := s.SUT.(interface{ Pid() int }); ok {
		now = p.Pid()
	}
	s.Check("the helper stays up through the outage", pid != 0 && now == pid && s.SUT.Running(), "same process, running",
		fmt.Sprintf("pid %d -> %d, running=%v", pid, now, s.SUT.Running()))
	// The steady state is a few requests per poll interval; a storm is
	// hundreds per second.
	rate := float64(faulted) / outage.Seconds()
	s.Check("no request storm while Graph fails", rate <= 10, "≤ 10 failed requests/s", fmt.Sprintf("%.1f/s (%d in %.0fs)", rate, faulted, outage.Seconds()))
	n := len(steps.AnswerPosts(s, work.ID, tok))
	calls := len(s.Codex.CallsWithToken(tok))
	s.Check("the prompt sent during the outage is answered once", n == 1 && calls == 1, "1 Codex call, 1 answer",
		fmt.Sprintf("%d calls, %d answers", calls, n), s.Transcript(work.ID, 8)...)
	for i, a := range answers {
		chats := steps.ChatsWithSUTText(s, a)
		copies := 0
		for _, c := range chats {
			copies += len(s.SUTMessagesContaining(c.ID, a))
		}
		s.Check(fmt.Sprintf("local session %d started during the outage is mirrored once", i+1), len(chats) == 1 && copies == 1,
			"1 chat, 1 copy", fmt.Sprintf("%d chats, %d copies", len(chats), copies))
	}
	workReads := 0
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: from}) {
		if r.Fault == "graph-down" && r.Route == "GET /chats/{chat}/messages" && r.ChatID == work.ID {
			workReads++
		}
	}
	s.Note("requests since the outage began: %d; failed reads of the work chat: %d", len(s.Graph.Requests(fakegraph.RequestFilter{Since: from})), workReads)
}

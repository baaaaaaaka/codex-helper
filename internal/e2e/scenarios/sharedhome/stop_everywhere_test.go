package sharedhome

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Stopped everywhere until started (owner ruling 2026-11-05). `cxp teams
// stop` and the control-chat command `shutdown` create the kill-switch
// file DISABLED next to the store with who stopped the helper, where and
// when: the owner stops at its next heartbeat, standbys and services stay
// idle with no Graph traffic, and `cxp teams status` says so on its first
// line. `cxp teams start` removes the file; the helper that starts next
// reads the messages sent meanwhile and answers each once.

const textShutdownReply = "Stopped on all machines. Teams is no longer read, so it cannot be restarted from here: run `cxp teams start` on any machine."

// stopSeam is the seam of these scenarios: a short heartbeat, so the owner
// notices the file within a second.
func stopSeam(host string) harness.Seam {
	return harness.Seam{Hostname: host, Knobs: map[string]string{"Heartbeat": "500ms"}}
}

// helperRequests lists the Graph requests since seq made by any helper
// process (not the scenario's CLI commands).
func helperRequests(s *harness.Scenario, since int64) []string {
	var out []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: since}) {
		if strings.HasPrefix(r.Instance, "cxp-") && r.Instance != "cxp-cli" {
			out = append(out, r.Instance+" "+r.Method+" "+r.Route)
		}
	}
	return out
}

// statusFirstLine is the first line `cxp teams status` prints (not part of
// the golden: the rest of the status has timings).
func statusFirstLine(s *harness.Scenario) string {
	res := s.RunCLIAs(20*time.Second, harness.CLIAs{Poll: true}, "teams", "status")
	line, _, _ := strings.Cut(res.Stdout, "\n")
	return strings.TrimSpace(line)
}

func stoppedLineOK(line, by, host string) bool {
	return strings.HasPrefix(line, "Stopped on all machines by "+by+" ("+host+", ") &&
		strings.HasSuffix(line, "). Start again with `cxp teams start`.")
}

// `cxp teams stop` / `cxp teams start` from the command line.
func TestStopAndStartFromTheCLI(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "STOP-START", harness.Options{
		Guarantees: []string{"F-XM-4", "R-1"},
		Title:      "`cxp teams stop` stops the helper with no Graph traffic until `cxp teams start`, which resumes it: the prompt sent meanwhile is answered once",
		Budget:     150 * time.Second,
		Seam:       stopSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: a foreground helper exits when it is stopped on all machines"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "stopstart")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Before the stop "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("the helper answers before the stop", answered, "answered", fmt.Sprint(answered))

	out, code := s.RunCLI(120*time.Second, "teams", "stop")
	stopped := s.Poll(5*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("`cxp teams stop` stops the running helper, says so and how to undo it",
		code == 0 && stopped && strings.Contains(out, "The helper on host-a (pid ") && strings.Contains(out, ") stopped.") &&
			strings.Contains(out, "To start it again: cxp teams start"),
		"exit 0, owner exited, output names it and `cxp teams start`", fmt.Sprintf("exit %d, owner exited=%v; output %s", code, stopped, clipTail(out, 400)))
	ownerOut := s.SUT.Output()
	s.Check("the owner's exit names `cxp teams stop` and `cxp teams start`",
		strings.Contains(ownerOut, "Stopped on all machines by `cxp teams stop` (host-a, ") && strings.Contains(ownerOut, "cxp teams start"),
		"its error carries the stop record", clipTail(ownerOut, 300))

	from := s.Graph.LastSeq()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While stopped "+held)
	line := statusFirstLine(s)
	s.Check("`cxp teams status` says first that the helper is stopped everywhere, by whom and how to start it",
		stoppedLineOK(line, "`cxp teams stop`", "host-a"), "Stopped on all machines by `cxp teams stop` (host-a, <time>). Start again with `cxp teams start`.", line)
	again, code := s.RunCLI(30*time.Second, "teams", "stop")
	s.Check("`cxp teams stop` again changes nothing", code == 0 && strings.Contains(again, "Already stopped; nothing changed."),
		"exit 0, already stopped", fmt.Sprintf("exit %d; %s", code, clipTail(again, 300)))
	s.Poll(4*time.Second, func() bool { return false })
	quiet := helperRequests(s, from)
	s.Check("no Graph request while stopped", len(quiet) == 0, "0 requests", fmt.Sprintf("%d: %q", len(quiet), quiet))
	s.Check("nothing runs while stopped", len(s.Codex.CallsWithToken(held)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(held))))

	started, code := s.RunCLI(30*time.Second, "teams", "start")
	s.Check("`cxp teams start` removes the stop and says who picks up", code == 0 &&
		strings.Contains(started, "Started the Teams helper again on all machines") && strings.Contains(started, "cxp teams run"),
		"exit 0, started, how to run a helper", fmt.Sprintf("exit %d; %s", code, clipTail(started, 400)))
	again, code = s.RunCLI(30*time.Second, "teams", "start")
	s.Check("`cxp teams start` again changes nothing", code == 0 && strings.Contains(again, "was not stopped"),
		"exit 0, not stopped", fmt.Sprintf("exit %d; %s", code, clipTail(again, 300)))
	if line := statusFirstLine(s); strings.HasPrefix(line, "Stopped") {
		s.Check("status no longer says stopped", false, "Teams helper", line)
	}

	s.StartSUT()
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	s.Check("after `cxp teams start` the prompt sent while stopped is answered", heldAnswered, "answered", fmt.Sprint(heldAnswered))
	exactlyOnce(s, work.ID, held)
	n := len(steps.AnswerPosts(s, work.ID, first))
	s.Check("the prompt answered before the stop is not answered again", n == 1 && len(s.Codex.CallsWithToken(first)) == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", n, len(s.Codex.CallsWithToken(first))))
}

// `shutdown` in the control chat: the same stop, with one reply posted
// before the helper goes silent.
func TestShutdownFromTheControlChat(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "SHUTDOWN", harness.Options{
		Guarantees: []string{"F-XM-4", "R-1"},
		Title:      "`shutdown` in the control chat stops the helper on all machines: one reply, then silence; `cxp teams start` resumes it and the command does not run again",
		Budget:     150 * time.Second,
		Seam:       stopSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: `shutdown` stops the foreground helper"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "shutdown")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Before the shutdown "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("the helper answers before the shutdown", answered, "answered", fmt.Sprint(answered))
	s.Settle(2*time.Second, 8*time.Second)

	steps.MustPost(s, control.ID, "help")
	helpMsg, _ := s.WaitSUTMessage(control.ID, "Commands in this chat", steps.CommandTimeout)
	s.Check("the control-chat help lists `shutdown`", strings.Contains(helpMsg.PlainText(), "shutdown"), "`shutdown` listed",
		clipTail(harness.NormalizeSpace(helpMsg.PlainText()), 600))

	steps.MustPost(s, control.ID, "shutdown")
	reply, replied := s.WaitSUTMessage(control.ID, "Stopped on all machines", steps.CommandTimeout)
	stopped := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	// PlainText drops the code markup: compare without the backquotes.
	wantReply := harness.NormalizeSpace("🔧 Helper: " + strings.ReplaceAll(textShutdownReply, "`", ""))
	s.Check("`shutdown` is answered with the stop reply", replied && harness.NormalizeSpace(reply.PlainText()) == wantReply,
		wantReply, harness.NormalizeSpace(reply.PlainText()))
	s.Check("`shutdown` stops the helper after its reply", stopped, "exited", fmt.Sprint(stopped), s.OutputTail(400))
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After the shutdown "+held)
	steps.MustPost(s, control.ID, "help") // a command is not read either (answered after the start)
	s.Poll(4*time.Second, func() bool { return false })
	after := helperRequests(s, reply.RequestSeq)
	var writes []string
	for _, r := range after {
		if !strings.Contains(r, " GET ") {
			writes = append(writes, r)
		}
	}
	s.Check("after the reply the helper writes nothing to Teams", len(writes) == 0, "0 writes after the reply", fmt.Sprintf("%d: %q", len(writes), writes))
	s.Check("the reply is posted once", len(s.SUTMessagesContaining(control.ID, "Stopped on all machines")) == 1, "1",
		fmt.Sprint(len(s.SUTMessagesContaining(control.ID, "Stopped on all machines"))))
	line := statusFirstLine(s)
	s.Check("`cxp teams status` names the control-chat shutdown", stoppedLineOK(line, "`shutdown` in the Teams control chat", "host-a"),
		"Stopped on all machines by `shutdown` in the Teams control chat (host-a, <time>). Start again with `cxp teams start`.", line)
	s.Check("nothing runs while stopped", len(s.Codex.CallsWithToken(held)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(held))))

	if out, code := s.RunCLI(30*time.Second, "teams", "start"); code != 0 {
		s.Check("`cxp teams start` succeeds", false, "exit 0", fmt.Sprintf("exit %d; %s", code, out))
	}
	s.StartSUT()
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	s.Check("after `cxp teams start` the prompt sent while stopped is answered", heldAnswered, "answered", fmt.Sprint(heldAnswered))
	exactlyOnce(s, work.ID, held)
	s.Poll(4*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("the `shutdown` command does not run again after the start", s.SUT.Running() &&
		len(s.SUTMessagesContaining(control.ID, "Stopped on all machines")) == 1, "helper running, 1 reply",
		fmt.Sprintf("running=%v replies=%d", s.SUT.Running(), len(s.SUTMessagesContaining(control.ID, "Stopped on all machines"))))
}

// A helper restarted while stopped stays stopped: a foreground start
// exits at once and a service-run helper stands by, both with no Graph
// traffic, until `cxp teams start`.
func TestStoppedAcrossAHelperRestart(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "STOP-RESTART", harness.Options{
		Guarantees: []string{"F-XM-4", "R-13", "R-1"},
		Title:      "stopped across a restart: a foreground start exits and a service-run helper stands by, with no Graph traffic, until `cxp teams start`",
		Budget:     150 * time.Second,
		Seam:       stopSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `^cxp-[12] exited on its own with "exit status 1"$`,
				Reason: "the point of the scenario: the stopped owner exits, and a foreground start while stopped is refused"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "stoprestart")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Before the stop "+first)
	steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	if _, code := s.RunCLI(120*time.Second, "teams", "stop"); code != 0 {
		s.Check("`cxp teams stop` succeeds", false, "exit 0", fmt.Sprint(code))
	}
	s.Poll(5*time.Second, func() bool { return !s.SUT.Running() })
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While stopped "+held)

	from := s.Graph.LastSeq()
	s.StartSUT()
	exited := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	out := s.SUT.Output()
	reqs := helperRequests(s, from)
	s.Check("a foreground start while stopped exits at once, naming the stop and `cxp teams start`, with no Graph request",
		exited && len(reqs) == 0 && strings.Contains(out, "Stopped on all machines by `cxp teams stop` (host-a, ") && strings.Contains(out, "cxp teams start"),
		"exited, 0 requests", fmt.Sprintf("exited=%v requests=%q; output %s", exited, reqs, clipTail(out, 300)))

	b := s.NewPeer("B", harness.PeerOptions{
		Seam: harness.Seam{Hostname: "host-a", Knobs: map[string]string{"RefusalRecheck": "500ms"}},
		Args: serviceChild,
	})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	exitedEarly := s.Poll(6*time.Second, func() bool { return !b.Running() })
	cpu, measured := standbyCPU(b, 5*time.Second)
	bOut := b.Output()
	bReqs := peerRequests(s, bFrom, "-B-")
	s.Check("a service-run helper started while stopped stands by, naming the stop, with no Graph request",
		!exitedEarly && b.Running() && len(bReqs) == 0 && strings.Contains(bOut, "stopped on all machines by `cxp teams stop` (host-a, "),
		"running, standing by, 0 requests", fmt.Sprintf("exited=%v requests=%q; output %s", exitedEarly, bReqs, clipTail(bOut, 300)))
	s.Check("the standing-by helper uses negligible CPU", !measured || cpu < 500*time.Millisecond, "< 0.5 s CPU in 5 s", cpu.String())
	s.Check("nothing runs while stopped", len(s.Codex.CallsWithToken(held)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(held))))

	started, code := s.RunCLI(30*time.Second, "teams", "start")
	s.Check("`cxp teams start` succeeds", code == 0 && strings.Contains(started, "Started the Teams helper again"), "exit 0", fmt.Sprintf("exit %d; %s", code, clipTail(started, 300)))
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("after `cxp teams start` the standing-by service helper takes over and answers the prompt sent while stopped", byB && b.Running(),
		"answered by B", fmt.Sprintf("byB=%v running=%v", byB, b.Running()), s.Transcript(work.ID, 12)...)
	exactlyOnce(s, work.ID, held)
}

// Shared home: `cxp teams stop` on one machine stops the owner and the
// standby on another machine does not take over; `cxp teams start` lets it.
func TestStopOnASharedHomeKeepsTheStandbyIdle(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "STOP-SHARED", harness.Options{
		Guarantees: []string{"F-XM-4", "F-XM-1", "R-1"},
		Title:      "shared home: `cxp teams stop` stops the owner and the other host's standby does not take over; after `cxp teams start` it does, and answers once",
		Budget:     180 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the owner (a foreground helper) exits when it is stopped on all machines"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "stopshared")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)

	seamB := takeoverSeam("host-b")
	seamB.Knobs["RefusalRecheck"] = "500ms"
	b := s.NewPeer("B", harness.PeerOptions{Seam: seamB, Args: serviceChild})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	standing := s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	s.Check("host B stands by while A owns the state", standing && !serving(b), "standing by", clipTail(b.Output(), 300))

	out, code := s.RunCLI(120*time.Second, "teams", "stop")
	aStopped := s.Poll(5*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("`cxp teams stop` stops the owner", code == 0 && aStopped, "exit 0, A exited", fmt.Sprintf("exit %d, exited=%v; %s", code, aStopped, clipTail(out, 300)))
	from := s.Graph.LastSeq()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While stopped "+held)
	// A released its lease: without the stop, B would take over at its
	// next check (every takeoverPoll).
	cpu, measured := standbyCPU(b, 3*takeoverPoll)
	bReqs := peerRequests(s, from, "-B-")
	s.Check("the standby on host B does not take over while stopped", !serving(b) && b.Running() && len(bReqs) == 0 &&
		strings.Contains(b.Output(), "stopped on all machines by `cxp teams stop` (host-a, "),
		"B running, not serving, 0 requests, naming the stop", fmt.Sprintf("serving=%v running=%v requests=%q; output %s", serving(b), b.Running(), bReqs, clipTail(b.Output(), 300)))
	s.Check("the standby uses negligible CPU while stopped", !measured || cpu < time.Second, "< 1 s CPU", cpu.String())
	s.Check("nothing runs while stopped", len(s.Codex.CallsWithToken(held)) == 0, "0 turns", fmt.Sprint(len(s.Codex.CallsWithToken(held))))
	line := statusFirstLine(s)
	s.Check("`cxp teams status` on the shared home says it is stopped everywhere", stoppedLineOK(line, "`cxp teams stop`", "host-a"),
		"Stopped on all machines by `cxp teams stop` (host-a, <time>). ...", line)

	started := s.RunCLIAs(30*time.Second, harness.CLIAs{Hostname: "host-c"}, "teams", "start")
	s.Check("`cxp teams start` on a third host succeeds", started.Code == 0 && strings.Contains(started.Stdout, "Started the Teams helper again"),
		"exit 0", fmt.Sprintf("exit %d; %s", started.Code, clipTail(started.Combined, 300)))
	_, ok := waitServing(s, b, 60*time.Second)
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("after `cxp teams start` host B takes over and answers the prompt sent while stopped", ok && byB, "B serves and answers",
		fmt.Sprintf("serving=%v answered by B=%v", ok, byB), s.Transcript(work.ID, 12)...)
	exactlyOnce(s, work.ID, held)
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the new host", len(notices) == 1 && strings.Contains(notices[0], "host-b"),
		"1 notice naming host-b", fmt.Sprintf("%d: %q", len(notices), notices))
}

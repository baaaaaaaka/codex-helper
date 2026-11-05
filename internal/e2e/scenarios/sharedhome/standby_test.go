package sharedhome

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Shared NFS home, two hosts (owner 2026-11-05: automatic takeover, made
// robust; F-XM-1). Production lease timing: the stale bound is 90 s.
//
// Host A owns the state and answers. A foreground start on host B is
// refused (A renews its lease) and sends nothing to Graph. B started by its
// service stands by with no Graph traffic and next to no CPU. A hangs
// (SIGSTOP, anywhere: in a store write or not). B takes over once A's lease
// has not been renewed for 90 s by B's own clock, waits 30 s more, copies
// the store and serves: a prompt posted after the hang is answered once, by
// B, and the control chat gets one notice naming host B. A resumes: its
// lease is gone, it is fenced and exits without a Graph write.
func TestStandbyTakeoverOnStaleOwner(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "STANDBY-TAKEOVER", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1", "R-13"},
		Title:      "shared home: a foreground start on a second host is refused, a service-run standby waits with no Graph traffic, takes over within 2-3 min after the owner hangs and announces it once; the old owner is fenced and makes no Graph write",
		Budget:     360 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"Heartbeat": "500ms"}, Hostname: "host-a"},
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: the resumed old owner is fenced and exits with an error"},
			{Invariant: harness.InvCleanStop, Match: `^cxp-F-\d+ exited on its own with "exit status 1"$`,
				Reason: "the foreground start on host B is refused by the live owner with an error"},
		}},
	})
	defer s.Finish()
	t.Cleanup(func() { harness.Thaw(s.SUT) })
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "standby")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Owner A answers "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("host A answers", answered, "answered", fmt.Sprint(answered))

	// A foreground start on B sees A renew its lease and exits.
	f := s.NewPeer("F", harness.PeerOptions{Seam: harness.Seam{Hostname: "host-b"}})
	fFrom := s.Graph.LastSeq()
	if err := f.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	fExited := s.Poll(30*time.Second, func() bool { return !f.Running() })
	fOut := f.Output()
	fReqs := peerRequests(s, fFrom, "-F-")
	s.Check("a foreground start on a second host is refused while the owner is live", fExited && strings.Contains(strings.ToLower(fOut), "live"),
		"exits within 30 s naming the live owner", fmt.Sprintf("exited=%v; output %s", fExited, clipTail(fOut, 300)))
	s.Check("the refused start sends no Graph request", len(fReqs) == 0, "0 requests", fmt.Sprintf("%d: %q", len(fReqs), fReqs))

	// B's service starts it: it stands by.
	b := s.NewPeer("B", harness.PeerOptions{Seam: harness.Seam{Hostname: "host-b"}, Args: serviceChild})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	standing := s.Poll(10*time.Second, func() bool { return strings.Contains(b.Output(), "standing by") })
	used, measured := standbyCPU(b, 20*time.Second)
	s.Check("the service-run standby says it stands by and keeps running", standing && b.Running() && !serving(b),
		"running, standing by", fmt.Sprintf("running=%v; output %s", b.Running(), clipTail(b.Output(), 300)))
	s.Check("the standby uses next to no CPU", !measured || used <= 20*time.Second/500,
		"≤0.2% of a core over 20 s (40ms)", fmt.Sprintf("%s (measured=%v)", used, measured))

	// A hangs.
	s.Settle(2*time.Second, 15*time.Second)
	if !harness.Freeze(s.SUT) {
		t.Fatal("cannot freeze host A")
	}
	frozenAt := time.Now()
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After A hangs "+held)
	bQuiet := peerRequests(s, bFrom, "-B-")
	s.Check("the standby sends no Graph request", len(bQuiet) == 0, "0 requests from B", fmt.Sprintf("%d: %q", len(bQuiet), bQuiet))
	_, ok := waitServing(s, b, 200*time.Second)
	took := time.Since(frozenAt).Round(time.Second)
	s.Check("B takes over 90 s to 3 min after A hung", ok && took >= 90*time.Second && took <= 180*time.Second,
		"B serves after ≥90 s and ≤180 s", fmt.Sprintf("serving=%v after %s; output %s", ok, took, clipTail(b.Output(), 600)))
	byB := answeredBy(s, work.ID, held, "-B-", steps.AnswerTimeout)
	s.Check("a prompt posted after the owner hung is answered by B", byB, "answered by B",
		fmt.Sprintf("byB=%v", byB), s.Transcript(work.ID, 12)...)
	notices := takeoverNotices(s, control.ID)
	s.Check("the control chat gets one notice naming the new host", len(notices) == 1 && strings.Contains(notices[0], "host-b"),
		"1 notice naming host-b", fmt.Sprintf("%d: %q", len(notices), notices))

	checkFencedAndSilent(s, s.SUT)
	exactlyOnce(s, work.ID, held)
	var controls []string
	for _, c := range s.Graph.Chats() {
		if strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
			controls = append(controls, c.Topic)
		}
	}
	s.Check("both hosts share one control chat", len(controls) == 1, "1 control chat", fmt.Sprintf("%d: %q", len(controls), controls))
	after := s.Token("after")
	steps.MustPost(s, work.ID, "B keeps serving "+after)
	ok = answeredBy(s, work.ID, after, "-B-", steps.AnswerTimeout)
	evidence := []string{clipTail(b.Output(), 1500)}
	if !ok {
		if d, isProc := b.(interface {
			DumpStacks(context.Context) string
		}); isProc {
			evidence = append(evidence, clipTail(d.DumpStacks(s.Ctx()), 6000))
		}
	}
	s.Check("B keeps serving after A was fenced", ok, "answered", fmt.Sprintf("answered=%v B running=%v", ok, b.Running()), evidence...)
}

// peerRequests lists routes of requests after seq whose instance tag
// contains sub.
func peerRequests(s *harness.Scenario, since int64, sub string) []string {
	var out []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: since}) {
		if strings.Contains(r.Instance, sub) {
			out = append(out, r.Route)
		}
	}
	return out
}

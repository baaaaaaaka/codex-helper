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

// Shared NFS home, two hosts (decision "共用 home: 保留待命 + 失效接管";
// F-XM-1; CORE-DESIGN §6: one owner row with a generation fence, a standby
// on another host takes over only on a stale heartbeat).
//
// Host A owns the state and answers. Host B starts on the same home: it
// stands by (exits with "live owner") and sends nothing to Graph. A hangs
// (SIGSTOP: no heartbeat, no request); B's supervisor keeps starting it
// (Restart=always) and B takes over once A's heartbeat is stale (90 s).
// A prompt posted after the hang is answered once, by B. A resumes
// (SIGCONT): it is past its lease, so its first write renews the
// heartbeat and finds B's generation; it is fenced and exits without
// posting anything. Both helpers use one control chat.
func TestStandbyTakeoverOnStaleOwner(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "STANDBY-TAKEOVER", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1", "R-13"},
		Title:      "shared home: a second host stands by with no Graph traffic, takes over after the owner's heartbeat is stale, the old owner is fenced and posts nothing",
		Budget:     220 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"Heartbeat": "500ms"}, Hostname: "host-a"},
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: the resumed old owner is fenced and exits with an error"},
			{Invariant: harness.InvCleanStop, Match: `^cxp-B-\d+ exited on its own with "exit status 1"$`,
				Reason: "host B is started repeatedly; each start refused by the live owner exits with an error (the takeover is checked by the scenario)"},
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
	aTag := s.SUT.Instance()

	// B starts while A is live: it stands by and touches nothing.
	b := s.NewPeer("B", harness.PeerOptions{Seam: harness.Seam{Hostname: "host-b"}})
	bFrom := s.Graph.LastSeq()
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	bExited := s.Poll(20*time.Second, func() bool { return !b.Running() })
	bOut := b.Output()
	bReqs := peerRequests(s, bFrom, "-B-")
	s.Check("a second host does not run while the owner is live", bExited && strings.Contains(strings.ToLower(bOut), "live"),
		"B exits naming the live owner", fmt.Sprintf("exited=%v; output %s", bExited, clipTail(bOut, 300)))
	s.Check("the standby sends no Graph request", len(bReqs) == 0, "0 requests from B", fmt.Sprintf("%d: %q", len(bReqs), bReqs))

	// A hangs. B's supervisor restarts it until it owns the state.
	// A is frozen once it settled (no post in flight), outside any store
	// write: a helper frozen inside one keeps the SQLite write lock, which
	// nothing can break (STANDBY-LOCKED covers that case).
	s.Settle(2*time.Second, 15*time.Second)
	store := s.StorePath()
	if store == "" {
		t.Fatal("no store under " + s.Env.Root)
	}
	if err := harness.FreezeOutsideStoreWrite(s.SUT, store); err != nil {
		t.Fatal(err)
	}
	frozenAt := time.Now()
	s.Note("froze host A (%s)", aTag)
	held := s.Token("held")
	steps.MustPost(s, work.ID, "After A hangs "+held)
	tookOver := false
	starts := 1
	var stayedFrom time.Time
	s.Poll(150*time.Second, func() bool {
		if b.Running() {
			if stayedFrom.IsZero() {
				stayedFrom = time.Now()
			}
			// Running for 3 s past the acquire: it owns the state.
			if time.Since(stayedFrom) > 3*time.Second {
				tookOver = true
				return true
			}
			return false
		}
		stayedFrom = time.Time{}
		if starts < 60 {
			starts++
			_ = b.Start(s.Ctx())
		}
		time.Sleep(2 * time.Second)
		return false
	})
	takeoverAfter := time.Since(frozenAt).Round(time.Second)
	s.Check("B takes over once A's heartbeat is stale (90 s)", tookOver && takeoverAfter >= 80*time.Second,
		"B owns the state after ≥80 s and ≤150 s", fmt.Sprintf("took over=%v after %s, %d starts of B", tookOver, takeoverAfter, starts))
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	bAnswer := false
	for _, r := range steps.AnswerPosts(s, work.ID, held) {
		if strings.Contains(r.Instance, "-B-") {
			bAnswer = true
		}
	}
	s.Check("a prompt posted after the owner hung is answered by B", heldAnswered && bAnswer, "answered by B",
		fmt.Sprintf("answered=%v byB=%v", heldAnswered, bAnswer), s.Transcript(work.ID, 12)...)

	// A resumes: fenced at its next heartbeat, exits, posts nothing.
	thawFrom := s.Graph.LastSeq()
	harness.Thaw(s.SUT)
	aExited := s.Poll(30*time.Second, func() bool { return !s.SUT.Running() })
	s.Settle(2*time.Second, 6*time.Second)
	var aWrites []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: thawFrom}) {
		if r.Instance == aTag && r.Method != "GET" {
			aWrites = append(aWrites, r.Route)
		}
	}
	aOut := s.SUT.Output()
	s.Check("the resumed old owner is fenced and exits", aExited && strings.Contains(strings.ToLower(aOut), "fence"),
		"A exits with the fence error", fmt.Sprintf("exited=%v; output %s", aExited, clipTail(aOut, 300)))
	s.Check("the fenced old owner posts nothing", len(aWrites) == 0, "0 writes from A after it resumed", fmt.Sprintf("%d: %q", len(aWrites), aWrites))
	n := len(steps.AnswerPosts(s, work.ID, held))
	s.Check("R-1 the prompt is answered exactly once across both hosts", n == 1 && len(s.Codex.CallsWithToken(held)) == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", n, len(s.Codex.CallsWithToken(held))))
	var controls []string
	for _, c := range s.Graph.Chats() {
		if strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
			controls = append(controls, c.Topic)
		}
	}
	s.Check("both hosts share one control chat", len(controls) == 1, "1 control chat", fmt.Sprintf("%d: %q", len(controls), controls))
	after := s.Token("after")
	steps.MustPost(s, work.ID, "B keeps serving "+after)
	// Poll, not WaitAnswered: the scenario's own SUT (A) has exited.
	ok := s.Poll(steps.AnswerTimeout, func() bool { return len(s.SUTMessagesContaining(work.ID, "answer for "+after)) > 0 })
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

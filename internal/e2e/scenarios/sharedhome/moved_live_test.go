package sharedhome

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// The home copied to a new machine while the old machine's helper still
// runs (MACHINE-MOVE-REPORT.md cases b and c). Both copies share the
// marker prefix, so each sees the other's posts as "ours, but not sent by
// us". The move guard (core/engine/takeover.go):
//   - the new copy does not run again what the old copy took after the
//     copy, and lists those requests in one notice per chat;
//   - the old copy stands down when it reads the new copy's takeover
//     notice: it records the stop (`cxp teams start` undoes it) and exits.

// copyLive copies the running helper's home to another machine's home and
// returns that machine's environment.
func copyLive(s *harness.Scenario) *harness.Env {
	s.T.Helper()
	home := filepath.Join(s.Env.Root, "machine-b", "home")
	if err := copyHome(s.Env.Home, home, ".ssh"); err != nil {
		s.T.Fatalf("copy the home: %v", err)
	}
	return s.Env.MovedTo(s.T, home)
}

// skipNotices are the helper's notices in chat listing requests the other
// copy took.
func skipNotices(s *harness.Scenario, chat string) []string {
	var out []string
	for _, m := range s.SUTMessagesContaining(chat, "Not run here: the helper on host-a already took these requests") {
		out = append(out, harness.NormalizeSpace(m.PlainText()))
	}
	return out
}

func checkSkipNotice(s *harness.Scenario, chat string, listed []string, notListed ...string) {
	notes := skipNotices(s, chat)
	ok := len(notes) == 1
	for _, tok := range listed {
		ok = ok && strings.Contains(notes[0], tok)
	}
	for _, tok := range notListed {
		ok = ok && !strings.Contains(notes[0], tok)
	}
	s.Check("one notice lists the requests the old machine took after the copy, and only those", ok,
		fmt.Sprintf("1 notice listing %q", listed), fmt.Sprintf("%d: %q", len(notes), notes), s.Transcript(chat, 14)...)
}

// supersededLine is the status line of a helper that stood down.
func supersededLine(line string) bool {
	return strings.HasPrefix(line, "Stopped here: a copy of this state took over and the helper now runs on host-b (") &&
		strings.HasSuffix(line, "). Start again with `cxp teams start`.")
}

// Case b: the copy is taken while the old machine's helper runs; it
// answers one more prompt and is stopped; the new machine starts.
// Later the old machine is switched on again.
func TestCopiedHomeOldMachineStoppedFirst(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "MOVED-LIVE-STOPPED", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1"},
		Title:      "home copied while the helper runs, old helper stopped later: the new machine does not answer again what the old one took after the copy, lists it once, answers the gap prompt once; the old machine started again stands down",
		Budget:     150 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-2 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: the old machine's helper stands down when another copy took over"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "moved")
	quiet, _ := steps.NewWorkChat(s, control, "quiet")
	before := s.Token("before")
	steps.MustPost(s, work.ID, "Before the copy "+before)
	steps.WaitAnswered(s, work.ID, before, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)

	moved := copyLive(s)
	taken := s.Token("taken")
	steps.MustPost(s, work.ID, "After the copy "+taken)
	_, answered := steps.WaitAnswered(s, work.ID, taken, steps.AnswerTimeout)
	s.Check("the old machine answers after the copy", answered, "answered", fmt.Sprint(answered))
	s.Settle(2*time.Second, 10*time.Second)
	s.StopSUT()
	// A prompt no helper saw. (A gap prompt in a chat the old machine did
	// not post in is the engine tests': two chats read at once would make
	// the turn numbers in the transcript vary.)
	after := s.Token("after")
	steps.MustPost(s, work.ID, "After the old machine stopped "+after)

	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b"), Env: moved})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	_, ok := waitServing(s, b, 90*time.Second)
	s.Check("the new machine serves after the copied lease went stale", ok, "serving", clipTail(b.Output(), 600))
	by := answeredBy(s, work.ID, after, "-B-", steps.AnswerTimeout)
	s.Check("a prompt no helper saw is answered by the new machine", by, "answered by B", fmt.Sprint(by), s.Transcript(work.ID, 12)...)
	s.Settle(3*time.Second, 15*time.Second)
	checkSkipNotice(s, work.ID, []string{taken}, before, after)
	s.Check("no notice in a chat the old machine did not post in", len(skipNotices(s, quiet.ID))+len(skipNotices(s, control.ID)) == 0,
		"none", fmt.Sprintf("%d", len(skipNotices(s, quiet.ID))+len(skipNotices(s, control.ID))))

	// The old machine is switched on again: it reads the new machine's
	// notice in the control chat before it runs anything, and stands down.
	from := s.Graph.LastSeq()
	s.StartSUT()
	exited := s.Poll(30*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("the old machine started again stands down and says why", exited &&
		strings.Contains(s.SUT.Output(), "a copy of this state took over and the helper now runs on host-b"),
		"exits naming host-b", fmt.Sprintf("exited=%v; output %s", exited, clipTail(s.SUT.Output(), 400)))
	writes := writesBy(s, from, s.SUT.Instance())
	s.Check("the old machine makes no Graph write when it stands down", len(writes) == 0, "0 writes", fmt.Sprintf("%d: %q", len(writes), writes))
	line := statusFirstLine(s)
	s.Check("`cxp teams status` on the old machine says it stood down and how to undo it", supersededLine(line),
		"Stopped here: a copy of this state took over and the helper now runs on host-b (<time>). Start again with `cxp teams start`.", line)

	for _, tok := range []string{before, taken, after} {
		exactlyOnce(s, work.ID, tok)
	}
}

// Case c: the copy is taken while the old machine's helper runs, and both
// keep running.
func TestCopiedHomeBothMachinesRun(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "MOVED-LIVE-BOTH", harness.Options{
		Guarantees: []string{"F-XM-1", "R-1"},
		Title:      "home copied while the helper runs, both machines keep running: the new machine does not answer again what the old one took after the copy and lists it once; the old one stands down at the new one's notice, posting nothing",
		Budget:     150 * time.Second,
		Seam:       takeoverSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: the old machine's helper stands down when another copy took over"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "moved")
	before := s.Token("before")
	steps.MustPost(s, work.ID, "Before the copy "+before)
	steps.WaitAnswered(s, work.ID, before, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)

	moved := copyLive(s)
	taken := s.Token("taken")
	steps.MustPost(s, work.ID, "After the copy "+taken)
	_, answered := steps.WaitAnswered(s, work.ID, taken, steps.AnswerTimeout)
	s.Check("the old machine answers after the copy", answered, "answered", fmt.Sprint(answered))
	s.Settle(2*time.Second, 10*time.Second)

	b := s.NewPeer("B", harness.PeerOptions{Seam: takeoverSeam("host-b"), Env: moved})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	_, ok := waitServing(s, b, 90*time.Second)
	s.Check("the new machine serves after the copied lease went stale", ok, "serving", clipTail(b.Output(), 600))
	exited := s.Poll(60*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("the old machine stands down at the new machine's notice and says why", exited &&
		strings.Contains(s.SUT.Output(), "a copy of this state took over and the helper now runs on host-b"),
		"exits naming host-b", fmt.Sprintf("exited=%v; output %s", exited, clipTail(s.SUT.Output(), 400)))
	var notice int64
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", ChatID: control.ID, Instance: b.Instance()}) {
		if notice == 0 && strings.Contains(r.Body, "now runs on host-b") {
			notice = r.Seq
		}
	}
	var posts []string
	for _, w := range writesBy(s, notice, s.SUT.Instance()) {
		if strings.HasPrefix(w, "POST") {
			posts = append(posts, w)
		}
	}
	s.Check("the old machine posts nothing after the notice", notice != 0 && len(posts) == 0, "0 posts",
		fmt.Sprintf("notice seq %d; %d: %q", notice, len(posts), posts))
	line := statusFirstLine(s)
	s.Check("`cxp teams status` on the old machine says it stood down and how to undo it", supersededLine(line),
		"Stopped here: a copy of this state took over and the helper now runs on host-b (<time>). Start again with `cxp teams start`.", line)

	next := s.Token("next")
	steps.MustPost(s, work.ID, "After the stand-down "+next)
	by := answeredBy(s, work.ID, next, "-B-", steps.AnswerTimeout)
	s.Check("the new machine answers the next prompt", by, "answered by B", fmt.Sprint(by))
	s.Settle(3*time.Second, 15*time.Second)
	checkSkipNotice(s, work.ID, []string{taken}, before, next)
	for _, tok := range []string{before, taken, next} {
		exactlyOnce(s, work.ID, tok)
	}
}

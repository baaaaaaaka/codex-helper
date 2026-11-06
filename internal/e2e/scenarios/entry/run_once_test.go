package entry

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// RUN-ONCE (F-CLI-10, F-CLI-3): `cxp teams run --once` reads Teams once,
// runs and answers what that brought in, sends everything and exits 0;
// nothing is read after it exits, and the next helper does not answer the
// prompt again.
func TestRunOnce(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "RUN-ONCE", harness.Options{
		Guarantees: []string{"F-CLI-10", "F-CLI-3"},
		Title:      "`cxp teams run --once`: one poll, the prompt it finds answered once, then exit 0; the next helper does not answer it again",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "once")
	s.Settle(time.Second, 6*time.Second)
	s.StopSUT()

	first := s.Token("first")
	steps.MustPost(s, chat.ID, "While no helper runs "+first)
	once := s.NewPeer("once", harness.PeerOptions{Args: []string{"--once"}})
	if err := once.Start(s.Ctx()); err != nil {
		t.Fatalf("start --once: %v", err)
	}
	exited := s.Poll(120*time.Second, func() bool { return !once.Running() })
	s.Check("`run --once` exits by itself, with status 0", exited && once.ExitErr() == nil, "exited, status 0",
		fmt.Sprintf("exited=%v err=%v", exited, once.ExitErr()), clipTail(once.Output(), 400))
	s.Check("it says the cycle is complete", strings.Contains(once.Output(), "Teams poll cycle complete (--once); exiting"),
		"Teams poll cycle complete (--once); exiting", clipTail(once.Output(), 400))
	posts := steps.AnswerPosts(s, chat.ID, first)
	s.Check("the prompt sent while no helper ran is answered once by `run --once`", len(posts) == 1 && len(s.Codex.CallsWithToken(first)) == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", len(posts), len(s.Codex.CallsWithToken(first))))

	from := s.Graph.LastSeq()
	held := s.Token("held")
	steps.MustPost(s, chat.ID, "After --once exited "+held)
	s.Poll(3*time.Second, func() bool { return false })
	var after []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: from}) {
		if strings.HasPrefix(r.Instance, "cxp-") && r.Instance != "cxp-cli" {
			after = append(after, r.Instance+" "+r.Route)
		}
	}
	s.Check("nothing reads Teams after `run --once` exited", len(after) == 0, "0 helper requests", fmt.Sprintf("%d: %q", len(after), after))

	s.StartSUT()
	_, ok := steps.WaitAnswered(s, chat.ID, held, steps.AnswerTimeout)
	s.Check("the next helper answers the newer prompt", ok, "answered", fmt.Sprint(ok))
	s.Settle(time.Second, 6*time.Second)
	s.Check("the prompt `run --once` answered is not answered again", len(steps.AnswerPosts(s, chat.ID, first)) == 1 && len(s.Codex.CallsWithToken(first)) == 1,
		"1 answer, 1 turn", fmt.Sprintf("posts=%d turns=%d", len(steps.AnswerPosts(s, chat.ID, first)), len(s.Codex.CallsWithToken(first))))
}

// RUN-INTERVAL (F-CLI-3): `cxp teams run --interval 3s` reads Teams at
// that pace: an idle helper makes a few reads where the default pace
// makes many, and still answers the owner.
func TestRunInterval(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "RUN-INTERVAL", harness.Options{
		Guarantees: []string{"F-CLI-3"},
		Title:      "`cxp teams run --interval 3s`: an idle helper reads Teams every 3 s instead of the default pace, and still answers",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "interval")
	s.Settle(time.Second, 6*time.Second)
	gets := func(instance string, from int64) int {
		return len(s.Graph.Requests(fakegraph.RequestFilter{Since: from, Method: "GET", Instance: instance}))
	}
	const window = 9 * time.Second
	from := s.Graph.LastSeq()
	s.Poll(window, func() bool { return false })
	base := gets(s.SUT.Instance(), from)
	s.StopSUT()

	slow := s.NewPeer("slow", harness.PeerOptions{Args: []string{"--interval", "3s"}})
	if err := slow.Start(s.Ctx()); err != nil {
		t.Fatalf("start --interval: %v", err)
	}
	s.WaitFor("the --interval helper runs", 60*time.Second, func() (bool, string) {
		return strings.Contains(slow.Output(), "Teams helper (new core) running on"), clipTail(slow.Output(), 300)
	})
	s.Poll(10*time.Second, func() bool { return false }) // its start-up reads
	from = s.Graph.LastSeq()
	s.Poll(window, func() bool { return false })
	paced := gets(slow.Instance(), from)
	s.Check("an idle helper with --interval 3s reads Teams a few times in 9 s, far less than at the default pace",
		paced >= 1 && paced <= 12 && base >= 3*paced, "1..12 reads, default ≥ 3×", fmt.Sprintf("--interval 3s: %d reads; default pace: %d", paced, base))

	tok := s.Token("paced")
	steps.MustPost(s, chat.ID, "With a slow pace "+tok)
	_, ok := steps.WaitAnswered(s, chat.ID, tok, steps.AnswerTimeout)
	s.Check("the --interval helper answers the owner", ok, "answered", fmt.Sprint(ok))
}

func clipTail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…" + s[len(s)-n:]
}

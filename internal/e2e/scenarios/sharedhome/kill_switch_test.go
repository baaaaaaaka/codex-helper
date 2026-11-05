package sharedhome

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Kill-switch file (decision "共用 home … 新增停用开关文件"; owner 06-25: the
// service stops as soon as it sees the file, on every host; removing the
// file is the un-kill).
//
// A running helper answers a prompt. DISABLED appears next to the store:
// the helper stops by itself within a few heartbeats. A start while the
// file exists is refused before any Graph request. A prompt posted while
// disabled is answered once after the file is removed and the helper
// starts again.
func TestKillSwitchFile(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "KILL-SWITCH", harness.Options{
		Guarantees: []string{"F-XM-4", "F-XM-1", "R-1"},
		Title:      "DISABLED next to the store stops the running helper, refuses a start with no Graph traffic; removing it resumes and answers the held prompt once",
		Budget:     120 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"Heartbeat": "500ms"}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "killswitch")
	first := s.Token("first")
	steps.MustPost(s, work.ID, "Before the kill switch "+first)
	_, answered := steps.WaitAnswered(s, work.ID, first, steps.AnswerTimeout)
	s.Check("the helper answers before the kill switch", answered, "answered", fmt.Sprint(answered))

	store := s.StorePath()
	s.Require("store found", 5*time.Second, func() (bool, string) { return store != "", "no store under " + s.Env.Root })
	disabled := filepath.Join(filepath.Dir(store), "DISABLED")
	if err := os.WriteFile(disabled, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	s.Note("wrote %s", disabled)
	t0 := time.Now()
	stopped := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	stopAfter := time.Since(t0).Round(100 * time.Millisecond)
	out := s.SUT.Output()
	s.Check("a running helper stops by itself when DISABLED appears", stopped,
		"exited within 15 s (heartbeat 500 ms)", fmt.Sprintf("stopped=%v after %s", stopped, stopAfter), s.OutputTail(600))
	s.Check("its exit names the kill-switch file", strings.Contains(out, "DISABLED") || strings.Contains(out, "kill-switch file"),
		"output mentions DISABLED or the kill-switch file", clipTail(out, 300))

	// A start while disabled is refused before any Graph request, and the
	// prompt posted meanwhile is not run.
	held := s.Token("held")
	steps.MustPost(s, work.ID, "While disabled "+held)
	refusedFrom := s.Graph.LastSeq()
	s.StartSUT()
	refusedTag := s.SUT.Instance()
	exited := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	var refusedReqs []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: refusedFrom}) {
		if r.Instance == refusedTag {
			refusedReqs = append(refusedReqs, r.Route)
		}
	}
	ranWhileDisabled := len(s.Codex.CallsWithToken(held))
	s.Check("a start while DISABLED exists exits at once", exited, "the refused start exits", fmt.Sprintf("exited=%v (%s)", exited, refusedTag))
	s.Check("the refused start sends no Graph request", len(refusedReqs) == 0, "0 requests", fmt.Sprintf("%d: %q", len(refusedReqs), refusedReqs))
	s.Check("a prompt sent while disabled is not run", ranWhileDisabled == 0, "0 turns", fmt.Sprint(ranWhileDisabled))

	// Un-kill: remove the file and start again.
	if err := os.Remove(disabled); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	s.Settle(2*time.Second, 8*time.Second)
	n := len(steps.AnswerPosts(s, work.ID, held))
	s.Check("after DISABLED is removed the held prompt is answered once", heldAnswered && n == 1 && len(s.Codex.CallsWithToken(held)) == 1,
		"1 answer, 1 turn", fmt.Sprintf("answered=%v posts=%d turns=%d", heldAnswered, n, len(s.Codex.CallsWithToken(held))), s.Transcript(work.ID, 15)...)
}

func clipTail(s string, n int) string {
	if len(s) > n {
		return "…" + s[len(s)-n:]
	}
	return s
}

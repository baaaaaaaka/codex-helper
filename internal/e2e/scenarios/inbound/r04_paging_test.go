package inbound

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-4 (page boundaries) / R-3: while the SUT is stopped, a prompt P1 is
// followed by 30 Teams system-event messages and then a prompt P2, so P1 is
// only reachable on the second page of the message list (the helper asks for
// 20 per page). After restart both prompts must be answered exactly once and
// in order.
func TestR04_PagedBacklogAfterRestart(t *testing.T) {
	s := harness.NewScenario(t, "R-4-paging", harness.Options{
		Guarantees: []string{"R-4", "R-3", "R-6"},
		Title:      "a prompt reachable only via @odata.nextLink after downtime is answered once",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r04-paging")

	s.StopSUT()
	p1, p2 := s.Token("pageone"), s.Token("pagetwo")
	steps.MustPost(s, work.ID, "Older request "+p1)
	for i := 0; i < 30; i++ {
		if _, err := s.Graph.PostSystemEvent(work.ID); err != nil {
			t.Fatal(err)
		}
	}
	steps.MustPost(s, work.ID, "Newer request "+p2)
	restartSeq := s.Graph.LastSeq()
	s.StartSUT()

	_, ok1 := steps.WaitAnswered(s, work.ID, p1, 45*time.Second)
	_, ok2 := steps.WaitAnswered(s, work.ID, p2, 30*time.Second)
	s.Settle(3*time.Second, 15*time.Second)

	c1, c2 := s.Codex.CallsWithToken(p1), s.Codex.CallsWithToken(p2)
	tr := s.Transcript(work.ID, 12)
	s.Check("R-4 prompt behind a page boundary is answered once", ok1 && len(c1) == 1, "answered once", fmt.Sprintf("answered=%v calls=%d", ok1, len(c1)), tr...)
	s.Check("R-4 newest prompt is answered once", ok2 && len(c2) == 1, "answered once", fmt.Sprintf("answered=%v calls=%d", ok2, len(c2)))
	if len(c1) == 1 && len(c2) == 1 {
		s.Check("R-6 older prompt runs first", c1[0].Seq < c2[0].Seq, "seq(P1) < seq(P2)", fmt.Sprintf("Codex call seq P1=%d P2=%d", c1[0].Seq, c2[0].Seq), s.Transcript(work.ID, 10)...)
	}
	var paged []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Route: "GET /chats/{chat}/messages", ChatID: work.ID, Since: restartSeq}) {
		if strings.Contains(r.Query, "skiptoken") {
			paged = append(paged, r.String())
		}
	}
	s.Note("nextLink follow-ups on the work chat after restart: %d", len(paged))
	for i, p := range paged {
		if i == 3 {
			break
		}
		s.Note("  %s", p)
	}
}

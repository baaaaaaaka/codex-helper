package notify

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Workflow cards switched on and off while the helper runs (the old core
// applied `cxp teams workflow enable/disable` without a restart; the new
// core hands the change to the running helper through a request file next
// to the store). Each final still notifies the owner exactly once: a
// control-chat @mention while cards are off, a card while they are on.
func TestWorkflowToggledWhileRunning(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "WORKFLOW-LIVE", harness.Options{
		Guarantees: []string{"R-49", "F-RND-6", "owner-notify-every-final"},
		Title:      "`cxp teams workflow enable/disable` while the helper runs: applied without a restart; each final notifies once (mention, card, mention)",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "wflive")
	restarts := func() int {
		if p, ok := s.SUT.(interface{ ServiceRestarts() int }); ok {
			return p.ServiceRestarts()
		}
		return 0
	}
	pid0 := 0
	if p, ok := s.SUT.(interface{ Pid() int }); ok {
		pid0 = p.Pid()
	}

	final := func(i int) (cards, mentions int) {
		tok := s.Token(fmt.Sprintf("final%d", i))
		seen := map[string]bool{}
		for _, m := range ownerMentions(s, nil) {
			seen[m.ID] = true
		}
		cardsBefore := len(s.Graph.WorkflowCards())
		steps.MustPost(s, work.ID, "Short question "+tok)
		steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)
		s.Poll(15*time.Second, func() bool { return len(s.Graph.WorkflowCards()) > cardsBefore || len(ownerMentions(s, seen)) > 0 })
		time.Sleep(4 * time.Second)
		s.Settle(2*time.Second, 8*time.Second)
		for _, c := range s.Graph.WorkflowCards()[cardsBefore:] {
			if c.Status >= 200 && c.Status < 300 {
				cards++
			}
		}
		return cards, len(ownerMentions(s, seen))
	}

	c1, m1 := final(1)
	s.Check("cards off: the final is announced by one control-chat @mention", c1 == 0 && m1 == 1, "0 cards, 1 mention", fmt.Sprintf("%d cards, %d mentions", c1, m1))

	dir := filepath.Join(s.Env.Home, ".config", "cxp-e2e-workflow")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(dir, "webhook-url")
	if err := os.WriteFile(secret, []byte(s.Graph.NewWorkflowWebhook()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, code := s.RunCLI(40*time.Second, "teams", "workflow", "enable", "--webhook-url-file", secret)
	s.Check("`teams workflow enable` while running is applied by the running helper", code == 0 && strings.Contains(out, "applied the change"),
		"exit 0, \"applied the change\"", fmt.Sprintf("exit %d: %s", code, clipOut(out)))
	c2, m2 := final(2)
	s.Check("cards on: the next final is announced by one card and no mention", c2 == 1 && m2 == 0, "1 card, 0 mentions", fmt.Sprintf("%d cards, %d mentions", c2, m2))

	out, code = s.RunCLI(40*time.Second, "teams", "workflow", "disable")
	s.Check("`teams workflow disable` while running is applied by the running helper", code == 0 && strings.Contains(out, "applied the change"),
		"exit 0, \"applied the change\"", fmt.Sprintf("exit %d: %s", code, clipOut(out)))
	c3, m3 := final(3)
	s.Check("cards off again: the next final is announced by one mention", c3 == 0 && m3 == 1, "0 cards, 1 mention", fmt.Sprintf("%d cards, %d mentions", c3, m3))

	pid1 := 0
	if p, ok := s.SUT.(interface{ Pid() int }); ok {
		pid1 = p.Pid()
	}
	s.Check("the helper was not restarted", pid0 == pid1 && restarts() == 0, "same process", fmt.Sprintf("pid %d -> %d, %d service restarts", pid0, pid1, restarts()))
}

func clipOut(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 400 {
		return s[:400] + "…"
	}
	return s
}

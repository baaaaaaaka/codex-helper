package restart

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestCatchUpKeepsCommentaryAndCompaction(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MIR-PROGRESS-CATCHUP", harness.Options{Guarantees: []string{"F-MIR-4", "R-1", "R-2"}, Title: "commentary written while helper is stopped is caught up once before its answer", Budget: 90 * time.Second})
	defer s.Finish()
	steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		t.Fatal("history watcher not ready")
	}
	r := harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "progress-catchup"))
	first := s.Token("liveanswer")
	r.AppendTurn(t, "A local prompt", "live commentary", "live answer "+first)
	chat := ""
	s.Require("the local session has a work chat", 25*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, first)
		if len(cs) == 1 {
			chat = cs[0].ID
		}
		return chat != "", fmt.Sprint(len(cs))
	})
	s.StopSUT()
	prompt, progress, answer := s.Token("oldprompt"), s.Token("oldprogress"), s.Token("oldanswer")
	r.AppendTurn(t, "history prompt "+prompt, "history progress "+progress, "history answer "+answer)
	if err := r.AppendRecord("event_msg", map[string]any{"type": "context_compacted"}); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	s.Require("the historical answer is caught up", 30*time.Second, func() (bool, string) { return len(s.SUTMessagesContaining(chat, answer)) == 1, "waiting" })
	s.StopSUT()
	s.StartSUT()
	s.Settle(2*time.Second, 8*time.Second)
	var all strings.Builder
	for _, m := range s.SUTMessages(chat) {
		all.WriteString(m.PlainText())
		all.WriteByte('\n')
	}
	text := all.String()
	ok := strings.Count(text, prompt) == 1 && strings.Count(text, progress) == 1 && strings.Count(text, answer) == 1 && strings.Index(text, prompt) < strings.Index(text, progress) && strings.Index(text, progress) < strings.Index(text, answer)
	s.Check("historical compaction is preserved once", strings.Count(text, "Context compacted") == 1, "1 compaction", fmt.Sprint(strings.Count(text, "Context compacted")))
	s.Check("history preserves complete source order across two restarts", ok, "prompt, progress, answer once each", fmt.Sprint(ok), s.Transcript(chat, 12)...)
}

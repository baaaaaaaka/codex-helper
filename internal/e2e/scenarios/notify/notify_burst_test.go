package notify

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Decision "@ 通知: 每条最终答案都 @（维持现状）". Three finals land at the
// same moment in three work chats (no webhook). The core coalesces the
// control-chat @mentions of finals within MentionWindow (benchmark F2), so
// this checks the decision's substance under a burst: every final is named
// in an owner @mention in the control chat exactly once, none is dropped,
// and there is no @mention in a work chat.
func TestNotify_BurstOfFinals(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NOTIFY-BURST", harness.Options{
		Guarantees: []string{"owner-notify-every-final"},
		Title:      "three finals at once in three work chats: each named in a control-chat owner @mention exactly once, none in work chats",
		Budget:     120 * time.Second,
		Golden:     harness.GoldenOptions{FinishedNotices: "three finals complete at once; the scenario checks each is named exactly once"},
	})
	defer s.Finish()
	toks := []string{s.Token("burst1"), s.Token("burst2"), s.Token("burst3")}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		for _, tok := range toks {
			if c.Kind == "work" && c.HasToken(tok) {
				return harness.CodexBehavior{Block: true}
			}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	var works []fakegraph.ChatInfo
	for i := range toks {
		w, _ := steps.NewWorkChat(s, control, fmt.Sprintf("burst-%c", 'a'+i))
		works = append(works, w)
	}
	seen := map[string]bool{}
	for _, m := range ownerMentions(s, nil) {
		seen[m.ID] = true
	}
	for i, w := range works {
		steps.MustPost(s, w.ID, "Burst question "+toks[i])
	}
	s.Require("three turns running", 30*time.Second, func() (bool, string) {
		n := 0
		for _, tok := range toks {
			n += len(s.Codex.CallsWithToken(tok))
		}
		return n == 3, fmt.Sprintf("%d of 3", n)
	})
	s.Codex.ReleaseAll()
	for i, w := range works {
		steps.WaitAnswered(s, w.ID, toks[i], steps.AnswerTimeout)
	}
	s.Poll(40*time.Second, func() bool {
		named := 0
		for _, w := range works {
			for _, m := range ownerMentions(s, seen) {
				if strings.Contains(m.PlainText(), w.Topic) {
					named++
					break
				}
			}
		}
		return named == len(works)
	})
	s.Settle(3*time.Second, 15*time.Second)
	mentions := ownerMentions(s, seen)
	ev := describe(s, control.ID, mentions)
	var inControl, elsewhere int
	for _, m := range mentions {
		if m.ChatID == control.ID {
			inControl++
		} else {
			elsewhere++
		}
	}
	for _, w := range works {
		n := 0
		for _, m := range mentions {
			if m.ChatID == control.ID {
				n += strings.Count(m.PlainText(), w.Topic)
			}
		}
		s.Check(fmt.Sprintf("the final in %q is named in an owner @mention exactly once", w.Topic), n == 1, "1", fmt.Sprint(n), ev...)
	}
	s.Check("owner @mentions are in the control chat only", elsewhere == 0, "0 elsewhere", fmt.Sprint(elsewhere), ev...)
	s.Note("%d control-chat @mention message(s) for 3 simultaneous finals", inControl)
}

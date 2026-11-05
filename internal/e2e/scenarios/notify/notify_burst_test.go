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

// Decision "@ 通知: 每条最终答案都 @（维持现状）" and owner ruling 2026-11-05
// (Q3): every final gets its own notification, never merged. Three finals
// land at the same moment in three work chats (no webhook): the control
// chat gets three owner @mentions, each naming exactly one of the chats in
// v0.1.20's "✅ Codex finished" text, and there is no @mention in a work
// chat.
func TestNotify_BurstOfFinals(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NOTIFY-BURST", harness.Options{
		Guarantees: []string{"owner-notify-every-final"},
		Title:      "three finals at once in three work chats: three control-chat owner @mentions, one per final, none in work chats",
		Budget:     120 * time.Second,
		Golden:     harness.GoldenOptions{FinishedNotices: "three finals complete at once"},
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
			if m.ChatID == control.ID && strings.Contains(m.PlainText(), w.Topic) {
				n++
			}
		}
		s.Check(fmt.Sprintf("the final in %q has its own owner @mention, exactly one", w.Topic), n == 1, "1", fmt.Sprint(n), ev...)
	}
	for _, m := range mentions {
		named := 0
		for _, w := range works {
			named += strings.Count(m.PlainText(), w.Topic)
		}
		s.Check("an owner @mention names one final", m.ChatID != control.ID || named == 1, "1", fmt.Sprint(named), ev...)
	}
	s.Check("one control-chat owner @mention per final", inControl == len(works), fmt.Sprint(len(works)), fmt.Sprint(inControl), ev...)
	s.Check("owner @mentions are in the control chat only", elsewhere == 0, "0 elsewhere", fmt.Sprint(elsewhere), ev...)
}

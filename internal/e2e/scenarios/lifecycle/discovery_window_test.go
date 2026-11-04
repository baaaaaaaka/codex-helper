package lifecycle

import (
	"fmt"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Outage discovery window (decision: 120 days; F-MIR-3, R-9). Local TUI
// sessions last updated while the helper was down are discovered after a
// restart if they were updated within 120 days; older ones are baselined
// and never posted.
//
// The old core looks back 31 days and reconciles at most 4 recovery
// discoveries every 5 minutes, so the wait is bounded and a miss is a
// finding, not a hang.
func TestDiscoveryWindow120Days(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "WINDOW", harness.Options{
		Guarantees: []string{"F-MIR-3", "R-9", "R-2"},
		Title:      "after an outage, sessions updated 20 and 119 days ago are mirrored once; 121 days ago is not",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		s.Check("history watcher initialised", false, "probe session mirrored", "never mirrored")
		return
	}
	s.StopSUT()
	now := time.Now()
	type aged struct {
		days   int
		answer string
		want   bool
	}
	sessions := []*aged{{days: 20, want: true}, {days: 119, want: true}, {days: 121, want: false}}
	for _, a := range sessions {
		at := now.Add(-time.Duration(a.days) * 24 * time.Hour)
		r := harness.NewRolloutAt(t, s.Env, s.Env.WorkDir(t, fmt.Sprintf("aged-%d", a.days)), at)
		a.answer = s.Token(fmt.Sprintf("aged%d", a.days))
		r.AppendTurn(t, fmt.Sprintf("Session from %d days ago", a.days), "", "Old answer "+a.answer)
		if err := r.SetModTime(at.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	s.StartSUT()
	s.Poll(45*time.Second, func() bool {
		for _, a := range sessions {
			if a.want && len(steps.ChatsWithSUTText(s, a.answer)) == 0 {
				return false
			}
		}
		return true
	})
	s.Settle(3*time.Second, 10*time.Second)
	for _, a := range sessions {
		chats := steps.ChatsWithSUTText(s, a.answer)
		copies := 0
		for _, c := range chats {
			copies += len(s.SUTMessagesContaining(c.ID, a.answer))
		}
		if a.want {
			s.Check(fmt.Sprintf("session updated %d days ago is mirrored once", a.days), len(chats) == 1 && copies == 1,
				"1 chat, 1 copy", fmt.Sprintf("%d chats, %d copies (45 s after restart)", len(chats), copies))
		} else {
			s.Check(fmt.Sprintf("session updated %d days ago is not posted", a.days), len(chats) == 0, "0 chats", fmt.Sprintf("%d chats", len(chats)))
		}
	}
	s.Check("discovery never runs Codex", len(s.Codex.Calls()) == 0, "0 Codex calls", fmt.Sprint(len(s.Codex.Calls())))
}

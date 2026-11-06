package crash

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// SIGKILL twice in the middle of the silent catch-up after an outage: every
// local record still arrives exactly once, in order, and silently, with no
// command from the owner (R-2, R-10, F-MIR-4).
//
// History: catch-up after downtime was the most reported Teams problem
// (U-I10, about 28 reports; INC-03/INC-04/INC-07): records re-posted after
// a restart, gaps, @-notifications for old turns and catch-ups that only
// a manual `teams recover` finished. SPEC SC-03b.
//
// One session is mirrored live; the helper stops; that session gets 20
// more turns and a new session gets 20 turns. The helper restarts and is
// killed after about a third of the catch-up, restarted and killed again
// after about two thirds, then restarted for good.
func TestCatchUpSurvivesCrashes(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CATCHUP-CRASH", harness.Options{
		Guarantees: []string{"R-2", "R-10", "F-MIR-4"},
		Title:      "two SIGKILLs during the catch-up of 40 local turns: each record once, in order, silent",
		Budget:     180 * time.Second,
	})
	defer s.Finish()
	steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		s.Check("history watcher initialised", false, "probe session mirrored", "never mirrored")
		return
	}
	type rec struct{ prompt, answer string }
	type session struct {
		r    *harness.Rollout
		recs []rec
	}
	add := func(ss *session, label string, n int) {
		for i := 0; i < n; i++ {
			rc := rec{fmt.Sprintf("%s question %d %s", label, len(ss.recs)+1, s.Token(label+"p")), fmt.Sprintf("%s answer %d %s", label, len(ss.recs)+1, s.Token(label+"a"))}
			ss.r.AppendTurn(t, rc.prompt, "", rc.answer)
			ss.recs = append(ss.recs, rc)
		}
	}
	live := &session{r: harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "catchup-live"))}
	add(live, "Live", 1)
	var liveChat fakegraph.ChatInfo
	s.Require("the live session is mirrored", 30*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, live.recs[0].answer)
		if len(cs) == 1 {
			liveChat = cs[0]
			return true, ""
		}
		return false, fmt.Sprintf("%d chats", len(cs))
	})
	s.Settle(2*time.Second, 10*time.Second)

	s.StopSUT()
	const turns = 20
	add(live, "Live", turns)
	down := &session{r: harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "catchup-down"))}
	add(down, "Down", turns)
	total := 4 * turns // prompt + answer per turn, two sessions
	posted := func() int {
		n := 0
		for _, ss := range []*session{live, down} {
			for _, rc := range ss.recs {
				for _, text := range []string{rc.prompt, rc.answer} {
					if len(s.SUTMessagesContaining("", text)) > 0 {
						n++
					}
				}
			}
		}
		return n - 2 // the live session's first turn
	}
	var kills []string
	for i, at := range []int{total / 3, 2 * total / 3} {
		s.StartSUT()
		s.WaitFor(fmt.Sprintf("catch-up reaches %d of %d records", at, total), 60*time.Second, func() (bool, string) {
			n := posted()
			return n >= at, fmt.Sprintf("%d posted", n)
		})
		n := posted()
		s.CrashSUT()
		kills = append(kills, fmt.Sprintf("kill %d at %d of %d records", i+1, n, total))
	}
	s.Note("%s", strings.Join(kills, "; "))
	s.StartSUT()
	s.WaitFor("the catch-up completes", 90*time.Second, func() (bool, string) {
		n := posted()
		return n >= total, fmt.Sprintf("%d of %d posted", n, total)
	})
	s.Settle(3*time.Second, 15*time.Second)

	var downChats []fakegraph.ChatInfo
	if len(down.recs) > 0 {
		downChats = steps.ChatsWithSUTText(s, down.recs[0].answer)
	}
	s.Check("the session created while down gets one chat", len(downChats) == 1, "1", fmt.Sprint(len(downChats)))
	for _, c := range []struct {
		name string
		ss   *session
		chat string
	}{{"live session", live, liveChat.ID}, {"down session", down, firstID(downChats)}} {
		var bad []string
		var at []int
		msgs := s.SUTMessages(c.chat)
		for _, rc := range c.ss.recs {
			for _, text := range []string{rc.prompt, rc.answer} {
				n, idx := 0, -1
				for i, m := range msgs {
					if strings.Contains(harness.NormalizeSpace(m.PlainText()), text) {
						n++
						idx = i
					}
				}
				if n != 1 {
					bad = append(bad, fmt.Sprintf("%q ×%d", text, n))
				}
				at = append(at, idx)
			}
		}
		s.Check(c.name+": every record exactly once in its chat", len(bad) == 0, "none missing or doubled", strings.Join(bad, ", "), s.Transcript(c.chat, 12)...)
		inOrder := true
		for i := 1; i < len(at); i++ {
			inOrder = inOrder && at[i] > at[i-1]
		}
		s.Check(c.name+": records in rollout order", inOrder, "increasing message positions", fmt.Sprint(at))
	}
	s.Check("catch-up never runs Codex", len(s.Codex.Calls()) == 0, "0", fmt.Sprint(len(s.Codex.Calls())))
}

func firstID(cs []fakegraph.ChatInfo) string {
	if len(cs) == 0 {
		return ""
	}
	return cs[0].ID
}

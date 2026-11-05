package chats

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Duplicate guard edges (decision §2.6: keep v0.1.20's 3-minute duplicate
// guard). v0.1.20 (internal/teams/bridge.go recentDuplicateSessionPrompt)
// suppresses a repeat only when the earlier prompt's turn is queued,
// running or completed, never when it failed or was cancelled, and never
// for a message with attachments or pasted images; the window is counted
// from the earlier prompt. DuplicateFor is scaled to 6 s through
// the test seam's timing knob.
func TestDuplicateGuardEdges(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DUP-EDGE", harness.Options{
		Guarantees: []string{"F-WRK-dup", "R-3", "R-1"},
		Title:      "duplicate guard: repeat after the window runs; repeat of a failed prompt runs; same caption with a different image runs; other chat runs; plain repeat is ignored",
		Budget:     120 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"DuplicateFor": "6s"}},
	})
	defer s.Finish()
	failTok := s.Token("fails")
	var failed atomic.Bool
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(failTok) && failed.CompareAndSwap(false, true) {
			return harness.CodexBehavior{Err: errors.New("scripted failure")}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "dupedge")
	other, _ := steps.NewWorkChat(s, control, "dupother")
	const dupNotice = "Duplicate request ignored"
	dupNotices := func() int { return len(s.SUTMessagesContaining(work.ID, dupNotice)) }

	// Sanity: a plain repeat inside the window is ignored.
	plain := s.Token("plain")
	steps.MustPost(s, work.ID, "Plain repeat "+plain)
	steps.WaitAnswered(s, work.ID, plain, steps.AnswerTimeout)
	steps.MustPost(s, work.ID, "Plain repeat "+plain)
	_, ignored := s.WaitSUTMessage(work.ID, dupNotice, 15*time.Second)
	s.Check("a plain repeat within the window is ignored with one notice", ignored && len(s.Codex.CallsWithToken(plain)) == 1,
		"1 turn, 1 notice", fmt.Sprintf("turns=%d notices=%d", len(s.Codex.CallsWithToken(plain)), dupNotices()))

	// The same text in another chat is not a duplicate.
	steps.MustPost(s, other.ID, "Plain repeat "+plain)
	_, otherOK := steps.WaitAnswered(s, other.ID, plain, steps.AnswerTimeout)
	s.Check("the same text in another chat runs", otherOK && len(s.Codex.CallsWithToken(plain)) == 2, "2 turns in total",
		fmt.Sprintf("answered=%v turns=%d", otherOK, len(s.Codex.CallsWithToken(plain))))

	// After the window the same text runs again.
	s.Poll(8*time.Second, func() bool { return false })
	steps.MustPost(s, work.ID, "Plain repeat "+plain)
	s.Poll(steps.AnswerTimeout, func() bool { return len(s.Codex.CallsWithToken(plain)) == 3 })
	s.Check("a repeat after the window runs", len(s.Codex.CallsWithToken(plain)) == 3, "3 turns in total",
		fmt.Sprint(len(s.Codex.CallsWithToken(plain))))

	// A prompt whose turn failed is not a duplicate when resent.
	notices0 := dupNotices()
	steps.MustPost(s, work.ID, "This one fails first "+failTok)
	s.Poll(steps.AnswerTimeout, func() bool { return len(s.SUTMessagesContaining(work.ID, "scripted failure")) > 0 })
	steps.MustPost(s, work.ID, "This one fails first "+failTok)
	s.Poll(15*time.Second, func() bool { return len(s.Codex.CallsWithToken(failTok)) == 2 || dupNotices() > notices0 })
	s.Settle(time.Second, 4*time.Second)
	s.Check("resending a failed prompt within the window runs it", len(s.Codex.CallsWithToken(failTok)) == 2 && dupNotices() == notices0,
		"2 turns, no duplicate notice (v0.1.20: only queued/running/completed turns suppress)",
		fmt.Sprintf("turns=%d new duplicate notices=%d", len(s.Codex.CallsWithToken(failTok)), dupNotices()-notices0), s.Transcript(work.ID, 10)...)

	// The same caption with a different pasted image is not a duplicate.
	notices1 := dupNotices()
	img := s.Token("image")
	png2 := append(tinyPNG(), []byte("second screenshot")...)
	if _, err := s.Graph.PostImageAsUser(work.ID, "Look at this "+img, tinyPNG()); err != nil {
		t.Fatal(err)
	}
	steps.WaitAnswered(s, work.ID, img, steps.AnswerTimeout)
	if _, err := s.Graph.PostImageAsUser(work.ID, "Look at this "+img, png2); err != nil {
		t.Fatal(err)
	}
	s.Poll(15*time.Second, func() bool { return len(s.Codex.CallsWithToken(img)) == 2 || dupNotices() > notices1 })
	s.Settle(time.Second, 4*time.Second)
	s.Check("the same caption with a different image runs", len(s.Codex.CallsWithToken(img)) == 2 && dupNotices() == notices1,
		"2 turns, no duplicate notice (v0.1.20: messages with attachments are never deduplicated)",
		fmt.Sprintf("turns=%d new duplicate notices=%d", len(s.Codex.CallsWithToken(img)), dupNotices()-notices1), s.Transcript(work.ID, 10)...)
}

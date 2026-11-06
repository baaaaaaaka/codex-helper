package lifecycle

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Parked chats (decision: no Graph polling while parked; `r <id>` wakes the
// chat; a local rollout update reactivates it; a message typed while parked
// is seen after the wake). Two parked work chats:
//
//	A: the user types a prompt while A is parked, then sends `r <id>` in the
//	   control chat     -> no turn before the wake, exactly one after it
//	B: the user types a prompt while B is parked, then the session is
//	   continued in the local TUI (rollout append)
//	                    -> mirrored into B and B is active again: the
//	                       prompt typed while parked runs once (the park
//	                       notice promises it for both wake triggers) and
//	                       a new prompt there is answered without `r`
//	C: an active (never parked) chat whose session gets the same kind of
//	   TUI update   -> mirrored (F-MIR-2); separates "parked" from
//	                   "linked-transcript mirroring" when B fails
//
// Over the parked window neither chat may be read from Graph.
func TestParkedChat_NoPollingAndWake(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "PARK", harness.Options{
		Guarantees: []string{"F-WRK-6", "F-WRK-7", "F-WRK-8", "F-CTL-12", "R-14", "R-4", "R-11", "R-3"},
		Title:      "parked chat: no Graph reads; `r` wakes it and runs the parked prompt once; local TUI update reactivates it",
		Budget:     150 * time.Second,
		Golden:     harness.GoldenOptions{FinishedNotices: "several chats finish close together"},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chatA, _ := steps.NewWorkChat(s, control, "park-a")
	chatB, _ := steps.NewWorkChat(s, control, "park-b")
	chatC, _ := steps.NewWorkChat(s, control, "park-c")
	warmA, warmB, warmC := s.Token("warma"), s.Token("warmb"), s.Token("warmc")
	steps.MustPost(s, chatA.ID, "First task "+warmA)
	steps.MustPost(s, chatB.ID, "First task "+warmB)
	steps.MustPost(s, chatC.ID, "First task "+warmC)
	steps.WaitAnswered(s, chatA.ID, warmA, steps.AnswerTimeout)
	steps.WaitAnswered(s, chatB.ID, warmB, steps.AnswerTimeout)
	steps.WaitAnswered(s, chatC.ID, warmC, steps.AnswerTimeout)
	idA, idB := steps.SessionID(s, chatA.ID), steps.SessionID(s, chatB.ID)
	s.Require("session ids announced", 5*time.Second, func() (bool, string) {
		return idA != "" && idB != "", fmt.Sprintf("A=%q B=%q", idA, idB)
	})
	s.Settle(2*time.Second, 10*time.Second) // let the finals' status updates finish

	// Park from inside each work chat (`helper park`). The control-chat form
	// `park <id>` is not used here: the old core silently holds it (see the
	// `r <id>` check below for the same path).
	for _, c := range []struct{ chat, id string }{{chatA.ID, idA}, {chatB.ID, idB}} {
		steps.MustPost(s, c.chat, "helper park")
		if _, ok := s.WaitSUTMessage(c.chat, "Parked "+c.id, steps.CommandTimeout); !ok {
			s.Check("`helper park` in "+c.id+" is acknowledged", false, "\"Parked "+c.id+"\" reply", "no reply", s.Transcript(c.chat, 8)...)
			return
		}
	}
	// Reads already in flight when the park landed are fine; count from a
	// short grace period after the last ack.
	time.Sleep(2 * time.Second)
	parkedFrom := s.Graph.LastSeq()
	parkedAt := time.Now()

	held := s.Token("whileparked")
	steps.MustPost(s, chatA.ID, "Typed while parked "+held)
	heldB := s.Token("whileparkedb")
	steps.MustPost(s, chatB.ID, "Typed while parked "+heldB)
	const window = 15 * time.Second
	s.Poll(window, func() bool { return false })
	readsA := chatReads(s, chatA.ID, parkedFrom)
	readsB := chatReads(s, chatB.ID, parkedFrom)
	callsHeld := len(s.Codex.CallsWithToken(held)) + len(s.Codex.CallsWithToken(heldB))
	s.Note("parked window %.0fs: reads A=%d B=%d", time.Since(parkedAt).Seconds(), len(readsA), len(readsB))

	// Wake A explicitly; continue B in the local TUI.
	wakeSeq := s.Graph.LastSeq()
	wokeAt := time.Now()
	steps.MustPost(s, control.ID, "r "+idA)
	tuiA := continueInTUI(s, warmB)
	tuiC := continueInTUI(s, warmC)

	var heldAnswered, heldBAnswered, mirrored, mirroredC bool
	s.Poll(25*time.Second, func() bool {
		heldAnswered = len(s.SUTMessagesContaining(chatA.ID, "answer for "+held)) > 0
		heldBAnswered = len(s.SUTMessagesContaining(chatB.ID, "answer for "+heldB)) > 0
		mirrored = len(s.SUTMessagesContaining(chatB.ID, tuiA)) > 0
		mirroredC = len(s.SUTMessagesContaining(chatC.ID, tuiC)) > 0
		return heldAnswered && heldBAnswered && mirrored && mirroredC
	})
	after := s.Token("afterlocal")
	steps.MustPost(s, chatB.ID, "Follow-up after the local work "+after)
	afterAnswered := s.Poll(15*time.Second, func() bool { return len(s.SUTMessagesContaining(chatB.ID, "answer for "+after)) > 0 })
	s.Settle(2*time.Second, 8*time.Second)

	trA, trB := s.Transcript(chatA.ID, 15), s.Transcript(chatB.ID, 15)
	var wakeReplies []string
	for _, m := range s.SUTMessages(control.ID) {
		if m.RequestSeq > wakeSeq {
			wakeReplies = append(wakeReplies, clip(harness.NormalizeSpace(m.PlainText()), 120))
		}
	}
	s.Check("`r <id>` in the control chat is answered", len(s.SUTMessagesContaining(control.ID, "Resumed "+idA)) == 1, "\"Resumed "+idA+"\"",
		fmt.Sprintf("control replies after `r %s`: %q", idA, wakeReplies), s.Transcript(control.ID, 6)...)
	s.Check("parked chats get no Graph reads", len(readsA) == 0 && len(readsB) == 0,
		fmt.Sprintf("0 reads of A and B in %.0fs parked", window.Seconds()),
		fmt.Sprintf("A=%d B=%d; first A reads: %v", len(readsA), len(readsB), firstN(readsA, 3)))
	s.Check("a prompt typed while parked does not run before the wake", callsHeld == 0, "0 turns while parked", fmt.Sprint(callsHeld), trA...)
	nHeld := len(s.Codex.CallsWithToken(held))
	startedAfterWake := nHeld == 1 && !s.Codex.CallsWithToken(held)[0].Started.Before(wokeAt)
	s.Check("`r <id>` wakes the chat and the parked prompt runs exactly once", heldAnswered && startedAfterWake,
		"1 turn, started after `r`", fmt.Sprintf("%d turns, answered=%v, started after wake=%v", nHeld, heldAnswered, startedAfterWake))
	resumeNotices := 0
	for _, m := range s.SUTMessages(chatA.ID) {
		if m.RequestSeq > wakeSeq && strings.Contains(m.PlainText(), "resumed") {
			resumeNotices++
		}
	}
	s.Check("R-22 the wake posts one resume notice", resumeNotices == 1, "1", fmt.Sprint(resumeNotices))
	s.Check("F-MIR-2 control: a TUI update of an active Teams session is mirrored", mirroredC, "TUI answer in C", fmt.Sprint(mirroredC), s.Transcript(chatC.ID, 10)...)
	s.Check("a local TUI update of a parked session is mirrored into its chat", mirrored, "TUI answer in B", fmt.Sprint(mirrored), trB...)
	nHeldB := len(s.Codex.CallsWithToken(heldB))
	startedAfterLocal := nHeldB == 1 && !s.Codex.CallsWithToken(heldB)[0].Started.Before(wokeAt)
	s.Check("a local TUI update wakes the chat and the prompt typed while parked runs exactly once", heldBAnswered && startedAfterLocal,
		"1 turn, started after the local update", fmt.Sprintf("%d turns, answered=%v, started after the update=%v", nHeldB, heldBAnswered, startedAfterLocal), trB...)
	s.Check("a local TUI update reactivates the parked chat", afterAnswered, "next prompt in B answered without `r`",
		fmt.Sprintf("answered=%v, turns=%d, reads of B since the TUI update=%d", afterAnswered, len(s.Codex.CallsWithToken(after)), len(chatReads(s, chatB.ID, wakeSeq))))
}

// chatReads returns the SUT's message-list reads of chatID after seq.
func chatReads(s *harness.Scenario, chatID string, since int64) []string {
	var out []string
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "GET", ChatID: chatID, Since: since}) {
		if strings.HasPrefix(r.Route, "GET /chats/{chat}/messages") {
			out = append(out, fmt.Sprintf("%s %s", r.At.Format("15:04:05.000"), r.Route))
		}
	}
	return out
}

func firstN(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

// continueInTUI appends a local TUI turn to the Codex thread that ran the
// turn carrying warmTok (as if the user resumed that session in the TUI) and
// returns the answer token.
func continueInTUI(s *harness.Scenario, warmTok string) string {
	thread := ""
	if cs := s.Codex.CallsWithToken(warmTok); len(cs) > 0 {
		thread = cs[0].ThreadID
	}
	p, a := s.Token("tuiprompt"), s.Token("tuianswer")
	w, err := steps.ThreadRollout(s, thread)
	if err != nil {
		s.Check("Codex thread "+thread+" has a rollout to continue", false, "a rollout", err.Error())
		return a
	}
	if _, err := w.AppendTurn(rollout.Turn{Prompt: "Continued in the TUI " + p, Answer: "Local answer " + a}); err != nil {
		s.T.Fatal(err)
	}
	return a
}

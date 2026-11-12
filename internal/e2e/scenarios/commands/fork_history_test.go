package commands

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Fork history (gap #20; SPEC F-WRK-10 steps 3-4, v0.1.21-rc.6 1a210e6):
// the fork chat gets the parent's visible history up to the cutoff, then
// "History import complete", and only after that marker the parent chat
// gets "Fork complete" with the link and the control chat its "Work chat
// created" notice.

const forkMarker = "History import complete. The new Codex chat is ready."

// helperSeqs returns the request sequence of each helper message in chatID
// containing sub, in order.
func helperSeqs(s *harness.Scenario, chatID, sub string) []int64 {
	var out []int64
	for _, m := range s.SUTMessages(chatID) {
		if strings.Contains(harness.NormalizeSpace(m.PlainText()), harness.NormalizeSpace(sub)) {
			out = append(out, m.RequestSeq)
		}
	}
	return out
}

// forkHistoryOnce checks the fork chat holds each history text exactly
// once, then the marker once after all of them, and that the parent's
// "Fork complete" and the control chat's "Work chat created: <label>" come
// once each, after the marker.
func forkHistoryOnce(s *harness.Scenario, forkID, parentID, controlID, label string, history ...string) {
	s.T.Helper()
	var last int64
	for _, h := range history {
		seqs := helperSeqs(s, forkID, h)
		s.Check(fmt.Sprintf("F-WRK-10 the fork chat has %q once", clip(h, 60)), len(seqs) == 1, "1", fmt.Sprint(len(seqs)))
		for _, q := range seqs {
			last = max(last, q)
		}
	}
	marks := helperSeqs(s, forkID, forkMarker)
	s.Check("F-WRK-10 the fork chat gets the history marker once, after the history",
		len(marks) == 1 && marks[0] > last, "1 marker after the history", fmt.Sprintf("markers=%v last history=%d", marks, last), s.Transcript(forkID, 6)...)
	mark := int64(1 << 62)
	if len(marks) > 0 {
		mark = marks[0]
	}
	done := helperSeqs(s, parentID, "Fork complete")
	s.Check("F-CTL-13 the parent gets the link once, only after the history marker", len(done) == 1 && done[0] > mark,
		"1 \"Fork complete\" after the marker", fmt.Sprintf("complete=%v marker=%d", done, mark))
	created := helperSeqs(s, controlID, "Work chat created: "+label)
	s.Check("the control chat announces the fork once, after the history marker", len(created) == 1 && created[0] > mark,
		"1 notice after the marker", fmt.Sprintf("notices=%v marker=%d", created, mark))
}

// newForkChat waits for a work chat not in before.
func newForkChat(s *harness.Scenario, before map[string]bool) (fakegraph.ChatInfo, bool) {
	var fork fakegraph.ChatInfo
	ok := s.WaitFor("fork chat created", steps.CommandTimeout, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !before[c.ID] && !strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
				fork = c
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d chats", len(s.Graph.Chats()))
	})
	return fork, ok
}

func chatsNow(s *harness.Scenario) map[string]bool {
	out := map[string]bool{}
	for _, c := range s.Graph.Chats() {
		out[c.ID] = true
	}
	return out
}

// FORK-CTL: control `fork s001` (F-CTL-13): the control chat gets "Fork
// requested", the fork chat the session's history and the marker, the
// parent work chat the link (rc.6 sends it to the parent), and the fork
// works on its own thread.
func TestControlForkHistory(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "FORK-CTL", harness.Options{
		Guarantees: []string{"F-CTL-13", "F-WRK-10"},
		Title:      "control `fork <session>`: the fork chat gets the history, then the marker; the link follows in the parent",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "forkctl")
	one, ok1 := askNotified(s, control.ID, work.ID, "one")
	two, ok2 := askNotified(s, control.ID, work.ID, "two")
	s.Check("the original session answers twice", ok1 && ok2 && one.ThreadID != "" && two.ThreadID == one.ThreadID,
		"2 answers on one thread", fmt.Sprintf("ok=%v,%v threads=%q,%q", ok1, ok2, one.ThreadID, two.ThreadID))
	s.WaitSUTMessage(control.ID, "Codex finished", steps.CommandTimeout)

	before := chatsNow(s)
	forkSeq := s.Graph.LastSeq()
	reply, replied := command(s, control.ID, "fork s001", "Fork requested")
	s.Check("control `fork s001` is answered in the control chat", replied && strings.Contains(reply, "⏳ Fork requested."), "⏳ Fork requested. …", clip(reply, 160))
	fork, created := newForkChat(s, before)
	s.Check("control `fork s001` creates exactly one new chat", created, "one new chat", fork.Topic)
	if !created {
		return
	}
	done, doneOK := replyAfter(s, work.ID, forkSeq, "Fork complete", steps.CommandTimeout)
	s.Check("F-CTL-13 the parent work chat gets the link", doneOK && strings.Contains(done, "https://teams.microsoft.com/l/chat/"+url.QueryEscape(fork.ID)+"/"),
		"✅ Fork complete … <fork url>", clip(done, 200))
	s.WaitSUTMessage(control.ID, "Work chat created: s002", steps.CommandTimeout)
	var history []string
	for _, c := range []harness.CodexCall{one, two} {
		if len(c.Tokens) > 0 {
			history = append(history, "Please answer "+c.Tokens[0])
		}
		history = append(history, c.Answer)
	}
	forkHistoryOnce(s, fork.ID, work.ID, control.ID, "s002", history...)
	n := len(helperSeqs(s, control.ID, "Fork complete"))
	s.Check("the link is not sent to the control chat", n == 0, "0", fmt.Sprint(n))

	s.WaitPolled(fork.ID, forkSeq, steps.CommandTimeout)
	inFork, okFork := ask(s, fork.ID, "infork")
	s.Check("R-41 a prompt in the fork runs on a new thread", okFork && inFork.ThreadID != "" && inFork.ThreadID != one.ThreadID,
		"thread != original", fmt.Sprintf("ok=%v fork=%q original=%q", okFork, inFork.ThreadID, one.ThreadID))
	s.Settle(2*time.Second, 8*time.Second)
	n = len(s.SUTMessagesContaining(fork.ID, inFork.Answer))
	s.Check("the fork's own answer is posted once", inFork.Answer != "" && n == 1, "1", fmt.Sprint(n))
	n = len(s.SUTMessagesContaining(work.ID, inFork.Answer))
	s.Check("the fork's own answer stays out of the parent", n == 0, "0", fmt.Sprint(n))
	leaks := steps.InternalLeaks(s, reply)
	s.Check("the fork request reply carries no internal ids or paths", len(leaks) == 0, "none", fmt.Sprint(leaks))
}

// FORK-CRASH: the helper is killed while a fork's history is being posted
// (a 40-turn local session brought to Teams by `continue`, then `helper
// fork`); after the restart every record is in the fork chat exactly once,
// the marker once after them, and the link once after the marker.
func TestForkHistorySurvivesACrash(t *testing.T) {
	harness.Parallel(t)
	const turns = 40
	s := harness.NewScenario(t, "FORK-CRASH", harness.Options{
		Guarantees: []string{"F-WRK-10", "R-41", "R-2"},
		Title:      "kill -9 in the middle of a fork's history; after the restart every record is posted exactly once, then the marker, then the link",
		Budget:     5 * time.Minute,
		// History pages multiply each logical read. A 250ms hot poll can
		// exceed the HTTP no-loop cap after own posts warm the full tiers.
		// Keep accelerated timing without weakening that cap.
		Interval: 500 * time.Millisecond,
		NoStart:  true,
	})
	defer s.Finish()
	dir := s.Env.WorkDir(t, "longfork")
	var texts []string
	start := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	r := harness.NewRolloutAt(s.T, s.Env, dir, start)
	for i := 1; i <= turns; i++ {
		p, a := fmt.Sprintf("Forked session prompt number %02d", i), fmt.Sprintf("Forked session answer number %02d", i)
		r.AppendTurn(s.T, p, "", a)
		texts = append(texts, p, a)
	}
	if err := r.SetModTime(start.Add(time.Hour)); err != nil {
		s.T.Fatal(err)
	}
	// The fake app-server resumes and forks only the threads in its own
	// store (real Codex: any rollout under $CODEX_HOME/sessions).
	st := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	if err := st.Save(&fakecodex.Thread{ID: r.SessionID, Cwd: dir, CreatedAt: start.Unix(), UpdatedAt: start.Add(time.Hour).Unix(),
		RolloutPath: r.Path, Turns: []*fakecodex.Turn{}}); err != nil {
		s.T.Fatal(err)
	}

	s.StartSUT()
	control := steps.ReadyControlChat(s)
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+r.SessionID)
	work, created := newForkChat(s, before)
	s.Check("`continue` creates the parent chat", created, "one new chat", work.Topic)
	if !created {
		return
	}
	_, imported := s.WaitSUTMessage(work.ID, "Import complete", 2*time.Minute)
	s.Check("the parent's import finishes", imported, "Import complete", strings.Join(s.Transcript(work.ID, 4), " | "))
	s.WaitSUTMessage(control.ID, "Published local Codex session as s001", steps.CommandTimeout)

	before = chatsNow(s)
	forkSeq := s.Graph.LastSeq()
	reply, replied := command(s, work.ID, "helper fork", "Fork requested")
	s.Check("`helper fork` is answered", replied, "⏳ Fork requested. …", clip(reply, 160))
	fork, created := newForkChat(s, before)
	s.Check("`helper fork` creates exactly one new chat", created, "one new chat", fork.Topic)
	if !created {
		return
	}
	s.Require("the fork's history is being posted", steps.CommandTimeout, func() (bool, string) {
		n := len(helperSeqs(s, fork.ID, "Forked session answer number"))
		return n >= 4, fmt.Sprintf("%d answers posted", n)
	})
	posted := len(helperSeqs(s, fork.ID, "Forked session"))
	marks := len(helperSeqs(s, fork.ID, forkMarker))
	done := len(helperSeqs(s, work.ID, "Fork complete"))
	s.Note("killed with %d of %d records posted, marker %d, Fork complete %d", posted, 2*turns, marks, done)
	s.Check("the kill comes in the middle of the fork's history", posted < 2*turns && marks == 0 && done == 0,
		fmt.Sprintf("< %d records, no marker, no link", 2*turns), fmt.Sprintf("%d records, %d markers, %d links", posted, marks, done))
	s.CrashSUT()
	s.StartSUT()
	_, ok := replyAfter(s, work.ID, forkSeq, "Fork complete", 2*time.Minute)
	s.Check("the fork finishes after the restart", ok, "Fork complete", strings.Join(s.Transcript(fork.ID, 6), " | "))
	s.WaitSUTMessage(control.ID, "Work chat created: s002", steps.CommandTimeout)
	s.Settle(2*time.Second, 20*time.Second)
	forkHistoryOnce(s, fork.ID, work.ID, control.ID, "s002", texts...)
	n := len(helperSeqs(s, fork.ID, "Forked from s001"))
	s.Check("the fork chat's notice is posted once", n == 1, "1", fmt.Sprint(n))
	n = len(helperSeqs(s, work.ID, "Forked session answer number 01"))
	s.Check("the parent's history is not posted again in the parent", n == 1, "1", fmt.Sprint(n))
}

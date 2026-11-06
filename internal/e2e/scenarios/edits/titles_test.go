package edits

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Work-chat titles (F-WRK-2, v0.1.20 maybeUpdateWorkChatTitleFromLocalSession
// and renameSessionChat): after an answer the title follows the first
// prompt, then the Codex thread name once Codex has one; `helper rename`
// sets it and it is no longer changed automatically.
func TestWorkChatTitle(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-TITLE", harness.Options{
		Guarantees: []string{"F-WRK-2"},
		Title:      "a work chat's title follows its first prompt, then the Codex thread name; `helper rename` fixes it",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "title")
	tok, _, _ := ask(s, work.ID, "Fix the login bug", "first")
	want := "💬 " + harness.MachineLabel + " - Fix the login bug " + tok
	s.Check("after the first answer the title is the first prompt", waitTopic(s, work.ID, want, steps.CommandTimeout),
		fmt.Sprintf("%q", want), fmt.Sprintf("%q", topic(s, work.ID)))

	thread := ""
	if cs := s.Codex.CallsWithToken(tok); len(cs) > 0 {
		thread = cs[0].ThreadID
	}
	writeIndex(t, s, thread, "Login bug triage")
	ask(s, work.ID, "Keep going", "second")
	want = "💬 " + harness.MachineLabel + " - Login bug triage"
	s.Check("after the next answer the title is the Codex thread name", waitTopic(s, work.ID, want, steps.CommandTimeout),
		fmt.Sprintf("%q", want), fmt.Sprintf("%q", topic(s, work.ID)))

	reply, ok := command(s, work.ID, "helper rename Release prep", "Work chat renamed.")
	want = "💬 " + harness.MachineLabel + " - Release prep"
	s.Check("`helper rename` renames the chat and says so", ok && topic(s, work.ID) == want && strings.Contains(harness.NormalizeSpace(reply), want),
		fmt.Sprintf("title %q, reply naming it", want), fmt.Sprintf("title %q; reply %q", topic(s, work.ID), clip(reply, 200)))

	from := s.Graph.LastSeq()
	writeIndex(t, s, thread, "Something else")
	ask(s, work.ID, "One more thing", "third")
	s.Settle(2*time.Second, 10*time.Second)
	s.Check("after `helper rename` the title is not changed automatically", topic(s, work.ID) == want && len(renames(s, work.ID, from)) == 0,
		fmt.Sprintf("%q, no rename", want), fmt.Sprintf("%q, %d renames", topic(s, work.ID), len(renames(s, work.ID, from))))
	_, ok = command(s, work.ID, "helper rename", "usage: helper rename <title>")
	s.Check("`helper rename` without a title shows its usage", ok, "usage", fmt.Sprint(ok))
}

// writeIndex records a thread name in Codex's session index, as Codex does
// when a thread is named.
func writeIndex(t *testing.T, s *harness.Scenario, thread, name string) {
	t.Helper()
	line, _ := json.Marshal(map[string]string{"id": thread, "thread_name": name, "updated_at": time.Now().UTC().Format(time.RFC3339)})
	f, err := os.OpenFile(filepath.Join(s.Env.CodexHome, "session_index.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		t.Fatal(err)
	}
}

// The control chat's title and the renames (F-CTL-1, F-CTL-8, v0.1.20
// ControlChatTitle, renameControlChat, renameMachineHostname): the control
// chat is "🏠 Codex Control - <machine>"; `rename <title>` adds the user
// title; `rename hostname <name>` renames the machine in the control chat
// and every work chat without nesting the old prefix, reports a chat it
// could not rename, and the new name outlives a restart.
func TestRenames(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-RENAME", harness.Options{
		Guarantees: []string{"F-CTL-1", "F-CTL-8"},
		Title:      "control title 🏠; `rename <title>`; `rename hostname <name>` across all chats with a partial failure reported; kept over a restart",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	want := "🏠 Codex Control - " + harness.MachineLabel
	s.Check("the control chat title has 🏠 and the machine", topic(s, control.ID) == want, fmt.Sprintf("%q", want), fmt.Sprintf("%q", topic(s, control.ID)))

	_, ok := command(s, control.ID, "rename", "usage: helper rename <title> or rename <title>")
	s.Check("`rename` without a title shows its usage", ok, "usage", fmt.Sprint(ok))
	reply, ok := command(s, control.ID, "rename Ops desk", "Control chat renamed.")
	want = "🏠 Codex Control - " + harness.MachineLabel + " - Ops desk"
	s.Check("`rename <title>` adds the user title", ok && topic(s, control.ID) == want && strings.Contains(harness.NormalizeSpace(reply), want),
		fmt.Sprintf("%q", want), fmt.Sprintf("title %q; reply %q", topic(s, control.ID), clip(reply, 160)))

	a, _ := steps.NewWorkChat(s, control, "host-a")
	b, _ := steps.NewWorkChat(s, control, "host-b")
	tok, _, _ := ask(s, a.ID, "Tidy the build", "a")
	autoA := "💬 " + harness.MachineLabel + " - Tidy the build " + tok
	waitTopic(s, a.ID, autoA, steps.CommandTimeout)
	topicB := topic(s, b.ID)

	fault := s.Graph.AddFault(fakegraph.Fault{Name: "rename-b", Kind: fakegraph.FaultStatus, Method: "PATCH", Route: "PATCH /chats/{chat}",
		ChatID: b.ID, Status: 403})
	reply, ok = command(s, a.ID, "helper rename hostname newbox", "Hostname renamed.")
	fault.Disarm()
	idB := steps.SessionID(s, b.ID)
	norm := harness.NormalizeSpace(reply)
	s.Check("`rename hostname` reports the renamed chats and the one it could not rename", ok &&
		strings.Contains(norm, "New hostname: newbox") && strings.Contains(norm, "Control chat: updated") &&
		strings.Contains(norm, "Work chats: 1 updated") && strings.Contains(norm, "Some chat titles were not updated:") &&
		strings.Contains(norm, idB+": "), "control updated, 1 work chat, "+idB+" listed as not updated", fmt.Sprintf("%q", clip(reply, 400)))
	wantCtl := "🏠 Codex Control - newbox - Ops desk"
	wantA := "💬 newbox - Tidy the build " + tok
	s.Check("the machine is renamed in every title, without nesting", topic(s, control.ID) == wantCtl && topic(s, a.ID) == wantA && topic(s, b.ID) == topicB,
		fmt.Sprintf("%q, %q, B unchanged", wantCtl, wantA), fmt.Sprintf("%q, %q, %q", topic(s, control.ID), topic(s, a.ID), topic(s, b.ID)))

	s.StopSUT()
	from := s.Graph.LastSeq()
	s.StartSUT()
	status, ok := command(s, control.ID, "status", "newbox")
	ask(s, a.ID, "And the tests", "after")
	s.Settle(2*time.Second, 10*time.Second)
	s.Check("the new hostname outlives a restart", ok && topic(s, control.ID) == wantCtl && len(renames(s, control.ID, from)) == 0,
		"status names newbox; control title kept", fmt.Sprintf("status %q; title %q; %d renames", clip(status, 160), topic(s, control.ID), len(renames(s, control.ID, from))))
}

// The unread mark (F-TRN-1, v0.1.20 markChatUnreadForSentAnswer): once an
// answer is posted the chat is marked unread for the owner from that
// answer on (read up to 1 ms before it), once per answer. Records caught
// up after an outage are posted without any mark, rename or edit.
func TestUnreadMark(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-UNREAD", harness.Options{
		Guarantees: []string{"F-TRN-1"},
		Title:      "each answer marks its chat unread from that answer on, once; caught-up records make no mark, rename or edit",
		Budget:     150 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		s.Check("history watcher initialised", false, "probe session mirrored", "never mirrored")
		return
	}
	// The probe session's "Codex finished" notice is posted before `new`
	// (it would race the reply in the control chat).
	s.WaitFor("the probe session's notice", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(control.ID, "Codex finished")) > 0, "no notice yet"
	})
	work, _ := steps.NewWorkChat(s, control, "unread")
	t1, _, _ := ask(s, work.ID, "Please answer", "one")
	t2, _, _ := ask(s, work.ID, "Please answer", "two")
	s.Settle(2*time.Second, 10*time.Second)
	marks := s.Graph.Requests(fakegraph.RequestFilter{Route: "POST /chats/{chat}/markChatUnreadForUser", ChatID: work.ID})
	var got []string
	for _, r := range marks {
		var body struct {
			At string `json:"lastMessageReadDateTime"`
		}
		_ = json.Unmarshal([]byte(r.Body), &body)
		got = append(got, body.At)
	}
	var want []string
	for _, tok := range []string{t1, t2} {
		m := answerMessage(s, work.ID, tok)
		want = append(want, m.Created.Add(-time.Millisecond).UTC().Format(time.RFC3339Nano))
	}
	same := len(got) == len(want)
	for i := 0; same && i < len(got); i++ {
		g, err1 := time.Parse(time.RFC3339Nano, got[i])
		w, err2 := time.Parse(time.RFC3339Nano, want[i])
		same = err1 == nil && err2 == nil && g.Equal(w)
	}
	s.Check("each answer marks the chat unread once, from that answer on", same, fmt.Sprintf("%v", want), fmt.Sprintf("%v", got))
	ctl := s.Graph.Requests(fakegraph.RequestFilter{Route: "POST /chats/{chat}/markChatUnreadForUser", ChatID: control.ID})
	s.Check("the control chat is not marked unread", len(ctl) == 0, "0", fmt.Sprint(len(ctl)))

	s.StopSUT()
	at := time.Now().Add(-2 * time.Hour)
	r := harness.NewRolloutAt(t, s.Env, s.Env.WorkDir(t, "caught-up"), at)
	old := s.Token("old")
	r.AppendTurn(t, "Earlier local work", "", "Old answer "+old)
	if err := r.SetModTime(at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	from := s.Graph.LastSeq()
	s.StartSUT()
	var caught fakegraph.ChatInfo
	s.WaitFor("caught-up session mirrored", 45*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, old)
		if len(cs) == 1 {
			caught = cs[0]
			return true, ""
		}
		return false, fmt.Sprintf("%d chats", len(cs))
	})
	s.Settle(3*time.Second, 10*time.Second)
	writes := 0
	for _, rq := range s.Graph.Requests(fakegraph.RequestFilter{ChatID: caught.ID, Since: from}) {
		if rq.Route == "POST /chats/{chat}/markChatUnreadForUser" || rq.Route == "PATCH /chats/{chat}" || rq.Route == "PATCH /chats/{chat}/messages/{message}" {
			writes++
		}
	}
	s.Check("caught-up records make no unread mark, rename or edit", caught.ID != "" && writes == 0, "0", fmt.Sprint(writes))
}

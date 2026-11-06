package dashboard

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// DASH-FLOW: the owner finds an old local Codex session from the control
// chat and continues it in Teams, as with v0.1.20: `p` lists the
// workspaces, a number opens one, its sessions are numbered, `details`
// shows one, and a session number creates a work chat that receives the
// session's history (the lifecycle notice, the import title, the prompts
// and answers, "Import complete") while the control chat says the chat
// was created and its history is coming, then that it is published. The
// list numbers last one command; a wrong, used-up or missing number gets
// v0.1.20's answers. Then `st` lists the active work chat, `o` shows its
// link, `continue` again imports nothing, and a prompt in the imported
// chat continues the same Codex session.
func TestDashboardContinue(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DASH-FLOW", harness.Options{
		Guarantees: []string{"F-CTL-3", "F-CTL-5", "F-CTL-6", "F-MIR-5", "F-WRK-3"},
		Title:      "workspaces, sessions, number selection, details, continue with history import, status list, open",
		Budget:     3 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	alpha, beta := s.Env.WorkDir(t, "alpha"), s.Env.WorkDir(t, "beta")
	older := localSession(s, alpha, base, "Plan the alpha release", "Release plan drafted", "List the open risks", "Three risks remain")
	localSession(s, alpha, base.Add(2*time.Hour), "Fix the alpha login bug", "Login bug fixed")
	localSession(s, beta, base.Add(time.Hour), "Write the beta docs", "Docs written")

	s.StartSUT()
	control := steps.ReadyControlChat(s)

	ws := command(s, control.ID, "p", "📁 Workspaces Reply with a number to open a workspace.")
	// v0.1.20 lists the items in reverse: number 1 (the most recent) is
	// last, next to the input box.
	s.Check("the workspaces are numbered most recent first", strings.Index(ws, "2. beta") >= 0 && strings.Index(ws, "1. alpha") > strings.Index(ws, "2. beta"),
		"2. beta, then 1. alpha", clip(ws, 400))
	sessions := command(s, control.ID, "1", "Reply with a number to continue a session in Teams.")
	s.Check("a workspace number lists its sessions, newest first", strings.Index(sessions, "2. Plan the alpha release") >= 0 &&
		strings.Index(sessions, "1. Fix the alpha login bug") > strings.Index(sessions, "2. Plan the alpha release"),
		"2. Plan…, then 1. Fix…", clip(sessions, 500))
	command(s, control.ID, "9", "That number is not in the current list.")
	command(s, control.ID, "2", "I do not have a current list yet.")
	command(s, control.ID, "s", "Reply with a number to continue a session in Teams.")
	details := command(s, control.ID, "d 2", "Local Codex session")
	s.Check("details of an unpublished session", strings.Contains(details, "Title: Plan the alpha release") && strings.Contains(details, "Teams chat: not published") &&
		strings.Contains(details, "Codex session ID: "+older.SessionID), "title, session id, not published", clip(details, 400))
	command(s, control.ID, "o 1", "I do not have a current list yet.")

	// Continue session 2 of alpha.
	command(s, control.ID, "s", "Reply with a number to continue a session in Teams.")
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "2")
	work := newChat(s, before, "the imported session's work chat")
	_, done := s.WaitSUTMessage(work.ID, "Import complete. This chat is ready; reply here to continue the Codex session.", steps.CommandTimeout)
	s.Check("the work chat gets \"Import complete\"", done, "Import complete", strings.Join(s.Transcript(work.ID, 8), " | "))
	_, published := s.WaitSUTMessage(control.ID, "Published local Codex session as s001", steps.CommandTimeout)
	s.Check("the control chat says the session is published", published, "Published local Codex session as s001", strings.Join(s.Transcript(control.ID, 4), " | "))
	onceEach(s, work.ID, "the imported chat", "Work chat created: s001", "Imported Codex session history",
		"Plan the alpha release", "Release plan drafted", "List the open risks", "Three risks remain", "Import complete")
	s.Check("only that session is imported", countIn(s, work.ID, "Fix the alpha login bug") == 0, "0", fmt.Sprint(countIn(s, work.ID, "Fix the alpha login bug")))
	onceEach(s, control.ID, "the control chat", "✅ Work chat created: s001", "Preparing local Codex history now")
	s.Settle(time.Second, 10*time.Second)

	st := command(s, control.ID, "st", "Active Work chats")
	s.Check("`st` lists the work chat with its link", strings.Contains(st, "💬 e2ehost - Plan the alpha release [active] Session: s001"), "s001", clip(st, 400))
	listed := command(s, control.ID, "s", "Reply with a number to continue a session in Teams.")
	s.Check("the published session shows as active with its chat", strings.Contains(listed, "Session: active") && strings.Contains(listed, "to open this Teams chat or import updates"),
		"active, open hint", clip(listed, 600))
	command(s, control.ID, "o 2", "s001 [active]")
	command(s, control.ID, "continue "+older.SessionID, "No new local history was imported.")
	command(s, control.ID, "d s001", "Session: s001")

	// The imported chat continues the same Codex session.
	token := s.Token("continued")
	steps.MustPost(s, work.ID, "Please answer "+token)
	_, answered := steps.WaitAnswered(s, work.ID, token, steps.AnswerTimeout)
	calls := s.Codex.CallsWithToken(token)
	s.Check("a prompt in the imported chat continues the local session", answered && len(calls) == 1 && calls[0].ThreadID == older.SessionID,
		older.SessionID, fmt.Sprintf("answered=%v calls=%d", answered, len(calls)))
	s.Settle(time.Second, 10*time.Second)
	onceEach(s, work.ID, "the imported chat after the continuation", "Plan the alpha release", "Three risks remain")
	topic := ""
	for _, c := range s.Graph.Chats() {
		if c.ID == work.ID {
			topic = c.Topic
		}
	}
	s.Check("the imported chat keeps its local session's title after a Teams turn", topic == "💬 e2ehost - Plan the alpha release",
		"💬 e2ehost - Plan the alpha release", topic)
}

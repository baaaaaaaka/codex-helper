package dashboard

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// IMPORT-AGAIN: importing a session twice never posts a record twice, and
// a session continued in a new chat gets its whole history there once.
//
//  1. `continue <id>` imports the session into s001; `continue <id>`
//     again imports nothing ("No new local history was imported.").
//  2. In s001, `helper publish-history` has nothing new, `helper
//     publish-history full` re-reads the whole rollout and posts nothing
//     already there (title, "Import complete", "Full local Codex history
//     is now up to date"), and a second `full` says it is already
//     published; a wrong argument gets the usage.
//  3. A prompt runs in s001 (a helper-run turn in the rollout); s001 is
//     closed; `continue <id>` makes s002, which gets the local history
//     and the helper-run turn exactly once, while s001 keeps its copy.
func TestImportTwiceAndAgainInANewChat(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "IMPORT-AGAIN", harness.Options{
		Guarantees: []string{"F-MIR-5", "F-MIR-6", "F-CTL-5"},
		Title:      "a second continue / publish-history posts nothing twice; a closed session continued anew gets its history once",
		Budget:     3 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	dir := s.Env.WorkDir(t, "again")
	local := localSession(s, dir, base, "Sketch the parser", "Parser sketched", "Add error recovery", "Recovery added")

	s.StartSUT()
	control := steps.ReadyControlChat(s)
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+local.SessionID)
	first := newChat(s, before, "s001")
	_, ok := s.WaitSUTMessage(first.ID, "Import complete", steps.CommandTimeout)
	s.Check("s001 gets the history", ok, "Import complete", strings.Join(s.Transcript(first.ID, 8), " | "))
	s.WaitSUTMessage(control.ID, "Published local Codex session as s001", steps.CommandTimeout)
	command(s, control.ID, "continue "+local.SessionID, "Already published as s001")
	s.Check("a second continue imports nothing", strings.Contains(lastSUT(s, control.ID), "No new local history was imported."), "No new local history was imported.", clip(lastSUT(s, control.ID), 300))

	command(s, first.ID, "helper publish-history", "No new visible local Codex history needed to be imported. This chat is ready.")
	seq := s.Graph.LastSeq()
	steps.MustPost(s, first.ID, "helper publish-history full")
	_, ok = replyAfter(s, first.ID, seq, "Full local Codex history is now up to date in this chat.", steps.CommandTimeout)
	s.Check("`publish-history full` completes", ok, "Full local Codex history is now up to date in this chat.", strings.Join(s.Transcript(first.ID, 6), " | "))
	command(s, first.ID, "helper publish-history full", "Full local Codex history is already published to this chat.")
	command(s, first.ID, "!ph everything", "Usage: helper publish-history imports the pending backlog; helper publish-history full imports the complete local Codex history into this chat.")
	onceEach(s, first.ID, "s001", "Sketch the parser", "Parser sketched", "Add error recovery", "Recovery added")

	token := s.Token("teams-turn")
	steps.MustPost(s, first.ID, "Please answer "+token)
	_, answered := steps.WaitAnswered(s, first.ID, token, steps.AnswerTimeout)
	s.Check("s001 answers on the session", answered, "answered", strings.Join(s.Transcript(first.ID, 4), " | "))
	s.Settle(time.Second, 10*time.Second)
	command(s, first.ID, "helper close", "Session closed.")

	before = chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+local.SessionID)
	second := newChat(s, before, "s002")
	_, ok = s.WaitSUTMessage(second.ID, "Import complete", steps.CommandTimeout)
	s.Check("s002 gets the history", ok, "Import complete", strings.Join(s.Transcript(second.ID, 8), " | "))
	s.WaitSUTMessage(control.ID, "Published local Codex session as s002", steps.CommandTimeout)
	s.Settle(2*time.Second, 15*time.Second)
	onceEach(s, second.ID, "s002", "Work chat created: s002", "Imported Codex session history", "Sketch the parser", "Parser sketched",
		"Add error recovery", "Recovery added", "Please answer "+token, "Import complete")
	n := len(steps.AnswerPosts(s, second.ID, token))
	s.Check("s002 gets the answer of the turn s001 ran, once", n == 1, "1", fmt.Sprint(n))
	onceEach(s, first.ID, "s001 after the new import", "Sketch the parser", "Recovery added")
}

// lastSUT is the plain text of the helper's last message in chatID.
func lastSUT(s *harness.Scenario, chatID string) string {
	msgs := s.SUTMessages(chatID)
	if len(msgs) == 0 {
		return ""
	}
	return harness.NormalizeSpace(msgs[len(msgs)-1].PlainText())
}

// IMPORT-CRASH: the helper is killed while a long history import is being
// posted, at the moment Teams has stored a record the helper has not yet
// recorded as sent; after the restart the import finishes and every record
// is in the chat exactly once (the held one is never posted again), with
// one "Import complete", one control-chat result and one notice that the
// held send was unconfirmed at the restart.
//
// The kill lands in that window deterministically: a kill at an arbitrary
// moment makes the control chat depend on whether a send was in flight.
func TestImportSurvivesACrash(t *testing.T) {
	harness.Parallel(t)
	const turns = 40
	s := harness.NewScenario(t, "IMPORT-CRASH", harness.Options{
		Guarantees: []string{"F-MIR-5", "R-2"},
		Title:      "kill -9 in the middle of a history import, with a record accepted by Teams and unrecorded; after the restart every record is posted exactly once",
		Budget:     4 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	dir := s.Env.WorkDir(t, "long")
	var pairs, texts []string
	for i := 1; i <= turns; i++ {
		p, a := fmt.Sprintf("Long session prompt number %02d", i), fmt.Sprintf("Long session answer number %02d", i)
		pairs = append(pairs, p, a)
		texts = append(texts, p, a)
	}
	local := localSession(s, dir, base, pairs...)
	const held = "Long session answer number 05"
	hold := s.Graph.AddFault(fakegraph.Fault{Name: "import-record-accepted-hold", Kind: fakegraph.FaultAcceptedHold, Method: "POST", Times: 1,
		Match: func(r fakegraph.Request) bool { return strings.Contains(r.Body, held) }})

	s.StartSUT()
	control := steps.ReadyControlChat(s)
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+local.SessionID)
	work := newChat(s, before, "the import's chat")
	s.Require("a record of the import is accepted by Teams and held", steps.CommandTimeout, func() (bool, string) {
		return hold.Fired() == 1, fmt.Sprintf("fired=%d, %d answers posted", hold.Fired(), countIn(s, work.ID, "Long session answer number"))
	})
	posted := countIn(s, work.ID, "Long session")
	complete := countIn(s, work.ID, "Import complete")
	s.Note("killed with %d of %d records posted, Import complete posted %d time(s)", posted, 2*turns, complete)
	s.Check("the kill comes in the middle of the import", posted < 2*turns && complete == 0, fmt.Sprintf("< %d records, no Import complete", 2*turns),
		fmt.Sprintf("%d records, %d Import complete", posted, complete))
	s.CrashSUT()
	s.StartSUT()
	_, ok := s.WaitSUTMessage(work.ID, "Import complete", 2*time.Minute)
	s.Check("the import finishes after the restart", ok, "Import complete", strings.Join(s.Transcript(work.ID, 6), " | "))
	s.WaitSUTMessage(control.ID, "Published local Codex session as s001", steps.CommandTimeout)
	s.Settle(2*time.Second, 20*time.Second)
	onceEach(s, work.ID, "the imported chat", append(texts, "Work chat created: s001", "Imported Codex session history", "Import complete")...)
	onceEach(s, control.ID, "the control chat", "✅ Work chat created: s001", "Published local Codex session as s001", "Delivery unconfirmed in s001")
	posts := 0
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST", ChatID: work.ID}) {
		if strings.Contains(r.Body, held) {
			posts++
		}
	}
	s.Check("the held record is never posted again", posts == 1, "1 POST", fmt.Sprintf("%d POSTs", posts))
}

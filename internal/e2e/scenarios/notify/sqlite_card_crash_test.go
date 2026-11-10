package notify

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	_ "modernc.org/sqlite"
)

func TestNotifySQLiteAcceptedCardCrashDoesNotRetry(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NOTIFY-SQLITE-CARD-CRASH", harness.Options{
		Title:  "accepted Workflow card with no response retains its attempt fence across a process crash",
		Budget: 100 * time.Second, WorkflowWebhook: true,
		Golden: harness.GoldenOptions{Skip: "assert durable attempt before restart, one card POST and one permitted fallback"},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "card-crash")
	s.Require("ready card accepted", steps.CommandTimeout, func() (bool, string) {
		return len(s.Graph.WorkflowCards()) == 1, fmt.Sprint(len(s.Graph.WorkflowCards()))
	})
	path := s.StorePath()
	if path == "" {
		t.Fatal("no published store")
	}
	before := len(s.Graph.WorkflowCards())
	seen := map[string]bool{}
	for _, m := range ownerMentions(s, nil) {
		seen[m.ID] = true
	}
	fault := s.Graph.AddFault(fakegraph.Fault{Name: "card-accepted-hold", Host: fakegraph.WorkflowHost, Method: "POST", Kind: fakegraph.FaultAcceptedHold, Times: 1})
	token := s.Token("ambiguous-card")
	steps.MustPost(s, work.ID, "Finish before the card response "+token)
	steps.WaitAnswered(s, work.ID, token, steps.AnswerTimeout)
	s.Require("server accepted the held card", steps.CommandTimeout, func() (bool, string) {
		return fault.Fired() == 1 && len(s.Graph.WorkflowCards()) == before+1, fmt.Sprint(len(s.Graph.WorkflowCards()) - before)
	})
	// The server has accepted the request and is still holding its response.
	// The kill therefore falls inside the intended ambiguous delivery window.
	s.CrashSUT()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var attempted int
	err = db.QueryRow(`SELECT count(*) FROM pending_notifications WHERE attempted=1 AND request LIKE ?`, "%"+token+"%").Scan(&attempted)
	db.Close()
	if err != nil || attempted != 1 {
		t.Fatal("card attempt was not durable before POST", attempted, err)
	}
	fault.Disarm()
	s.StartSUT()
	s.Require("recovery sends the permitted control-chat fallback", steps.CommandTimeout, func() (bool, string) {
		return len(ownerMentions(s, seen)) == 1, fmt.Sprint(len(ownerMentions(s, seen)))
	})
	s.Settle(2*time.Second, 6*time.Second)
	s.StopSUT()
	mentions := ownerMentions(s, seen)
	s.Check("card accepted once and never retried", len(s.Graph.WorkflowCards()) == before+1, "one POST", fmt.Sprint(len(s.Graph.WorkflowCards())-before))
	s.Check("one permitted fallback in control chat", len(mentions) == 1 && mentions[0].ChatID == control.ID && strings.Contains(mentions[0].PlainText(), "Codex finished"), "one control-chat fallback", fmt.Sprint(describe(s, control.ID, mentions)))
	s.Check("the final was neither executed nor posted twice", len(s.Codex.CallsWithToken(token)) == 1 && len(steps.AnswerPosts(s, work.ID, token)) == 1, "one execution and one final POST", fmt.Sprint(len(s.Codex.CallsWithToken(token))))
}

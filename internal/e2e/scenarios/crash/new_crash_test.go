package crash

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// SIGKILL after Teams created the work chat for `new` but before the helper
// learned its id: after the restart there is still exactly one work chat,
// announced once, and it works (R-39, R-2).
//
// History: duplicate work chats for one `new` or one session (INC-01,
// U-I01 "a second chat for the same session", 4e1d31b) came from chat
// creation that was not idempotent across a crash; the rebuilt core keys
// createOrGet by a stable externalId for exactly this.
const newCrashCommand = "   | <p>new <root>/work/newcrash</p>\n"

var newCrashCommandEdit = regexp.MustCompile(
	`(?:   \| <p><strong>🧑‍💻 User:</strong></p>\n)?` + regexp.QuoteMeta(newCrashCommand) +
		`(?:   edited 1 time\(s\); earlier versions:\n   \[version 1\]\n` + regexp.QuoteMeta(newCrashCommand) + `)?`)

func TestNewCrashCommandMask(t *testing.T) {
	label := "   | <p><strong>🧑‍💻 User:</strong></p>\n"
	history := "   edited 1 time(s); earlier versions:\n   [version 1]\n" + newCrashCommand
	for _, input := range []string{newCrashCommand, label + newCrashCommand + history} {
		if got := newCrashCommandEdit.ReplaceAllString(input, newCrashCommand); got != newCrashCommand {
			t.Fatalf("unexpected command snapshot: %q", got)
		}
	}
	if newCrashCommandEdit.MatchString("   | <p>new another-directory</p>\n") {
		t.Fatal("mask must preserve other commands")
	}
}

func TestNewSurvivesCrashDuringChatCreation(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "NEW-CRASH", harness.Options{
		Guarantees: []string{"R-39", "R-2"},
		Title:      "SIGKILL between Teams creating the work chat for `new` and the helper recording it: one chat, announced once, working",
		Budget:     120 * time.Second,
		Golden: harness.GoldenOptions{Masks: []harness.Mask{{
			Pattern: newCrashCommandEdit, Replace: newCrashCommand,
			Reason: "the crash can interrupt the independent user-label edit",
		}}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	hold := s.Graph.AddFault(fakegraph.Fault{
		Name:  "create-then-hold",
		Kind:  fakegraph.FaultAcceptedHold,
		Route: "POST /me/onlineMeetings/createOrGet",
		Times: 1,
	})
	before := len(s.Graph.Chats())
	dir := s.Env.WorkDir(t, "newcrash")
	if _, err := s.Graph.PostAsUser(control.ID, "new "+dir); err != nil {
		t.Fatal(err)
	}
	s.Require("Teams created the chat and the response is held", steps.CommandTimeout, func() (bool, string) {
		return hold.Fired() == 1, fmt.Sprintf("fired=%d chats=%d", hold.Fired(), len(s.Graph.Chats())-before)
	})
	s.CrashSUT()
	s.StartSUT()

	var work fakegraph.ChatInfo
	s.WaitFor("the work chat is announced ready", 45*time.Second, func() (bool, string) {
		for _, c := range s.Graph.Chats() {
			if !strings.Contains(c.Topic, harness.ControlChatTopicMarker) && len(s.SUTMessagesContaining(c.ID, "Work chat is ready")) > 0 {
				work = c
				return true, ""
			}
		}
		return false, fmt.Sprintf("%d chats", len(s.Graph.Chats()))
	})
	tok := s.Token("afternew")
	if work.ID != "" {
		if _, ok := s.WaitPolled(work.ID, s.Graph.LastSeq(), 30*time.Second); ok {
			steps.MustPost(s, work.ID, "First task "+tok)
			steps.WaitAnswered(s, work.ID, tok, 45*time.Second)
		}
	}
	s.Settle(3*time.Second, 15*time.Second)

	var created []string
	for _, c := range s.Graph.Chats() {
		if !strings.Contains(c.Topic, harness.ControlChatTopicMarker) {
			created = append(created, c.Topic)
		}
	}
	s.Check("exactly one work chat exists for the `new`", len(created) == 1 && len(s.Graph.Chats()) == before+1,
		"1 work chat", fmt.Sprintf("%d: %q", len(created), created))
	if work.ID != "" {
		n := len(s.SUTMessagesContaining(work.ID, "Work chat is ready"))
		s.Check("the work chat is announced ready once", n == 1, "1", fmt.Sprint(n), s.Transcript(work.ID, 10)...)
	}
	n := len(s.SUTMessagesContaining(control.ID, "Work chat created"))
	s.Check("the control chat reports the new chat once", n == 1, "1", fmt.Sprint(n), s.Transcript(control.ID, 10)...)
	calls := s.Codex.CallsWithToken(tok)
	s.Check("the new chat answers its first task once", len(calls) == 1 && len(steps.AnswerPosts(s, work.ID, tok)) == 1,
		"1 call, 1 answer", fmt.Sprintf("%d calls, %d answers", len(calls), len(steps.AnswerPosts(s, work.ID, tok))))
}

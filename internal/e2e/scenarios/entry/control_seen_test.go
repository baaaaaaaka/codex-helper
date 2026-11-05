package entry

import (
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

// ENTRY-CONTROLSEEN (live canary 2026-11-05): the old helper handled a
// control command just before its cursor and recorded it only in its
// registry's seen ids, not as an inbound row. The first start re-reads the
// control chat 2 minutes before the converted cursor; the command it had
// already run (here `new <dir>`) must not run again.
func TestEntryControlCommandNotReplayed(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-CONTROLSEEN", harness.Options{
		Guarantees: []string{"auto-migrate", "R-1"},
		Title:      "first start on an rc.59 store: a control command the old helper had handled before its cursor does not run again",
		Budget:     120 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	const control = "19:control@thread.v2"
	const seenID = "1788256829000" // 1 s before the control chat's cursor
	scope, _, _ := rc59Store(s, nil)
	dir := s.Env.WorkDir(t, "handled-before-cutover")
	if err := s.Graph.SeedMessage(control, seenID, time.Date(2026, 9, 1, 10, 0, 29, 0, time.UTC), "<p>new "+dir+"</p>"); err != nil {
		t.Fatal(err)
	}
	reg := `{"control_chat_id":"` + control + `","chats":{"` + control + `":{"seen_message_ids":["1788256815000","` + seenID + `"]}}}`
	if err := os.WriteFile(filepath.Join(scope, "registry.json"), []byte(reg), 0o600); err != nil {
		t.Fatal(err)
	}
	chats := len(s.Graph.Chats())
	s.StartSUT()
	s.WaitFor("the helper polls the control chat", 60*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(300)
	})
	// The conversion notice is posted asynchronously after the running
	// line; wait for it so the transcript order does not race the post.
	s.WaitSUTMessage(control, "Teams helper data converted", 60*time.Second)
	// A command posted now is answered: the control chat was read past the
	// old command.
	steps.MustPost(s, control, "status")
	_, answered := s.WaitSUTMessage(control, "sessions (", 60*time.Second)
	s.Check("a new control command is answered", answered, "status reply", fmt.Sprint(answered), s.Transcript(control, 8)...)
	time.Sleep(2 * time.Second)

	created := 0
	for _, m := range s.Graph.Messages(control) {
		if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), "Work chat created") {
			created++
		}
	}
	s.Check("the handled command does not run again", created == 0, "no work chat created", fmt.Sprint(created), s.Transcript(control, 8)...)
	s.Check("no chat is created", len(s.Graph.Chats()) == chats, fmt.Sprint(chats), fmt.Sprint(len(s.Graph.Chats())))
}

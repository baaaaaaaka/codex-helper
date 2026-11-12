package lifecycle

import (
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestParkPolicyCommandsAndRestart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "PARK-POLICY", harness.Options{Guarantees: []string{"F-WRK-6"}, Title: "automatic park policy persists and preserves manual park", Budget: 120 * time.Second})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "park-policy")
	id := steps.SessionID(s, chat.ID)
	out, code := s.RunCLI(30*time.Second, "teams", "service", "park", id, "status")
	s.Check("existing chat defaults to automatic park", code == 0 && strings.Contains(out, "Auto park: on"), "on", out)
	steps.MustPost(s, chat.ID, "helper park off")
	s.WaitSUTMessage(chat.ID, "Auto park: off", steps.CommandTimeout)
	out, code = s.RunCLI(30*time.Second, "teams", "service", "park", id, "status")
	s.Check("CLI observes chat policy", code == 0 && strings.Contains(out, "Auto park: off"), "off", out)
	s.StopSUT()
	seq := s.Graph.LastSeq()
	s.StartSUT()
	// Wait for the restarted engine to load and read its control chat.
	// Process creation alone does not mean the CLI exchange is ready.
	_, ready := s.WaitPolled(control.ID, seq, steps.CommandTimeout)
	if !ready {
		t.Fatal("restarted helper did not read control chat")
	}
	out, code = s.RunCLI(30*time.Second, "teams", "service", "park", id, "status")
	s.Check("restart preserves off", code == 0 && strings.Contains(out, "Auto park: off"), "off", out)
	steps.MustPost(s, control.ID, "park "+id)
	s.WaitSUTMessage(control.ID, "Parked "+id, steps.CommandTimeout)
	out, code = s.RunCLI(30*time.Second, "teams", "service", "park", id, "off")
	s.Check("setting off preserves manual park", code == 0 && strings.Contains(out, "Chat state: parked") && strings.Contains(out, "The chat remains parked"), "parked", out)
	steps.MustPost(s, control.ID, "r "+id)
	s.WaitSUTMessage(control.ID, "Resumed "+id, steps.CommandTimeout)
	out, code = s.RunCLI(30*time.Second, "teams", "service", "park", id, "on")
	s.Check("CLI restores automatic park", code == 0 && strings.Contains(out, "Auto park: on"), "on", out)
}

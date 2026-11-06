package sharedhome

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// `/shutdown` (and `!shutdown`) is `shutdown` (the command grammar of
// v0.1.20, dashboard_commands.go): the same one reply and stop, and, like
// `shutdown`, the command message is never labelled.
func TestShutdownSlashForm(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "SHUTDOWN-FORMS", harness.Options{
		Guarantees: []string{"F-XM-4"},
		Title:      "`/shutdown` in the control chat is `shutdown`: one reply, the helper stops, the message is not labelled",
		Budget:     120 * time.Second,
		Seam:       stopSeam("host-a"),
		Invariants: harness.Invariants{Except: []harness.Exception{
			{Invariant: harness.InvCleanStop, Match: `cxp-1 exited on its own with "exit status 1"`,
				Reason: "the point of the scenario: `/shutdown` stops the foreground helper"},
		}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	s.Settle(2*time.Second, 8*time.Second)
	seq := s.Graph.LastSeq()
	steps.MustPost(s, control.ID, "/shutdown")
	reply, replied := s.WaitSUTMessage(control.ID, "Stopped on all machines", steps.CommandTimeout)
	stopped := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	wantReply := harness.NormalizeSpace("🔧 Helper: " + strings.ReplaceAll(textShutdownReply, "`", ""))
	s.Check("`/shutdown` is answered with the stop reply", replied && harness.NormalizeSpace(reply.PlainText()) == wantReply,
		wantReply, harness.NormalizeSpace(reply.PlainText()))
	s.Check("`/shutdown` stops the helper", stopped, "exited", fmt.Sprint(stopped), s.OutputTail(400))
	edits := s.Graph.Requests(fakegraph.RequestFilter{Route: "PATCH /chats/{chat}/messages/{message}", ChatID: control.ID, Since: seq})
	s.Check("the `/shutdown` message is not labelled", len(edits) == 0, "0 message edits", fmt.Sprint(len(edits)))
	line := statusFirstLine(s)
	s.Check("`cxp teams status` names the control-chat shutdown", stoppedLineOK(line, "`shutdown` in the Teams control chat", "host-a"),
		"Stopped on all machines by `shutdown` in the Teams control chat (host-a, <time>). Start again with `cxp teams start`.", line)
	out, code := s.RunCLI(30*time.Second, "teams", "start")
	s.Check("`cxp teams start` succeeds", code == 0, "exit 0", fmt.Sprintf("exit %d; %s", code, clipTail(out, 200)))
}

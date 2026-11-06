package entry

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

const unsafeScopes = "unexpected Teams Graph scope(s): Mail.Read, Sites.FullControl.All; set CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 to override"

// AUTH-SCOPES (F-SET-2): Graph scopes broader than the helper needs fail
// closed with v0.1.20's message: `cxp teams auth config` reports not
// ready, `cxp teams status` says why, and `cxp teams run` stops before it
// reads Teams. CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 opts in; back on
// the default scopes the helper serves the owner.
func TestBroaderScopesFailClosed(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "AUTH-SCOPES", harness.Options{
		Guarantees: []string{"F-SET-2"},
		Title:      "Graph scopes broader than the defaults are refused (auth config, status, run) unless CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1",
		Budget:     120 * time.Second,
		NoStart:    true,
		Invariants: harness.Invariants{Except: []harness.Exception{{Invariant: harness.InvCleanStop, Match: "exited on its own with \"exit status 1\"",
			Reason: "the point of the scenario: broader scopes stop the helper with an error"}}},
	})
	defer s.Finish()
	broad := "openid profile offline_access User.Read Chat.ReadWrite Mail.Read Sites.FullControl.All"
	out, code := s.RunCLI(20*time.Second, "teams", "auth", "config", "--scopes", broad)
	s.Check("`cxp teams auth config` saves the scopes and reports them refused",
		code == 0 && strings.Contains(out, "Not ready: "+unsafeScopes), "Not ready: "+unsafeScopes, fmt.Sprintf("exit %d: %s", code, out))
	st := s.RunCLIAs(20*time.Second, harness.CLIAs{Poll: true}, "teams", "status")
	s.Check("`cxp teams status` says auth is not ready and why", strings.Contains(st.Stdout, "Auth: not ready ("+unsafeScopes+")"),
		"Auth: not ready (…)", st.Stdout)

	s.StartSUT()
	s.WaitFor("the helper exits", 30*time.Second, func() (bool, string) { return !s.SUT.Running(), s.OutputTail(300) })
	s.Check("`cxp teams run` refuses the scopes", strings.Contains(s.SUT.Output(), unsafeScopes), unsafeScopes, s.OutputTail(400))
	reqs := s.Graph.Requests(fakegraph.RequestFilter{})
	s.Check("Teams is not contacted with the broader scopes", len(reqs) == 0, "0 Graph requests", fmt.Sprint(len(reqs)), s.RequestTail(5)...)

	opt := s.RunCLIAs(20*time.Second, harness.CLIAs{Env: map[string]string{"CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES": "1"}}, "teams", "auth", "config")
	s.Check("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 accepts them", opt.Code == 0 && strings.Contains(opt.Stdout, "Scopes: "+broad),
		"Scopes: "+broad, opt.Combined)

	out, code = s.RunCLI(20*time.Second, "teams", "auth", "config", "--scopes", "")
	s.Check("back on the default scopes auth is ready", code == 0 && strings.Contains(out, "Scopes: openid profile offline_access User.Read Chat.ReadWrite"),
		"default scopes", fmt.Sprintf("exit %d: %s", code, out))
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	s.Check("the helper serves the owner on the default scopes", control.ID != "", "control chat", control.ID)
}

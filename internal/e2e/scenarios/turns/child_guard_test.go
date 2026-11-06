package turns

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// A Codex turn launched from Teams runs in the user's environment with the
// helper's CLI at hand, and cannot stop, restart or update the helper that
// runs it (F-TRN-13, R-25, R-24).
//
// History: Codex turns asked to "restart the service" or "upgrade cxp"
// killed the helper they ran in, mid-turn, and the answer never came back
// (7182812c added CODEX_HELPER_TEAMS_CHILD; INC-14, U-G23). The guard was
// only ever unit-tested with a faked environment variable.
//
// The turn, like Codex's shell tool, records its environment and runs
// `$CODEX_HELPER_CLI_PATH teams stop`, `teams pause`, `teams service
// restart` and `upgrade`. Each is refused with the advice to use the
// control chat, nothing is stopped, the turn is answered and the chat
// keeps working.
func TestCodexChildCannotManageHelper(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CHILD-GUARD", harness.Options{
		Guarantees: []string{"F-TRN-13", "R-25", "R-24"},
		Title:      "a Codex turn runs `cxp teams stop/pause`, `teams service restart` and `cxp upgrade`: all refused, helper keeps working",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	out := t.TempDir()
	tok := s.Token("selfmanage")
	commands := map[string]string{
		"stop":    "teams stop",
		"pause":   "teams pause testing",
		"restart": "teams service restart",
		"upgrade": "upgrade",
	}
	exec := []string{fmt.Sprintf("env > %s/env.txt; pwd > %s/pwd.txt", out, out)}
	for _, name := range []string{"stop", "pause", "restart", "upgrade"} {
		exec = append(exec, fmt.Sprintf(`"$CODEX_HELPER_CLI_PATH" %s > %s/%s.out 2>&1; echo $? > %s/%s.rc`, commands[name], out, name, out, name))
	}
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Exec: exec}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, dir := steps.NewWorkChat(s, control, "childguard")
	steps.MustPost(s, work.ID, "Restart the Teams helper and upgrade cxp "+tok)
	steps.WaitAnswered(s, work.ID, tok, 90*time.Second)

	read := func(name string) string {
		b, _ := os.ReadFile(filepath.Join(out, name))
		return strings.TrimSpace(string(b))
	}
	for _, name := range []string{"stop", "pause", "restart", "upgrade"} {
		rc, text := read(name+".rc"), read(name+".out")
		refused := rc != "" && rc != "0" && strings.Contains(text, "from a Codex turn launched by Teams helper")
		s.Check(fmt.Sprintf("`cxp %s` from the turn is refused", commands[name]), refused,
			"non-zero exit, \"refusing to … from a Codex turn launched by Teams helper\"", fmt.Sprintf("exit %s: %.160q", rc, text))
	}
	s.Check("`teams stop` advises the control chat's shutdown", strings.Contains(read("stop.out"), "`shutdown` in the Teams control chat"),
		"advice: send `shutdown` in the Teams control chat", fmt.Sprintf("%.160q", read("stop.out")))

	env := map[string]string{}
	for _, line := range strings.Split(read("env.txt"), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			env[k] = v
		}
	}
	cli := env["CODEX_HELPER_CLI_PATH"]
	st, err := os.Stat(cli)
	s.Check("the turn has the helper CLI at CODEX_HELPER_CLI_PATH", cli != "" && err == nil && st.Mode()&0o111 != 0,
		"an executable", fmt.Sprintf("%q (%v)", cli, err))
	s.Check("the turn is marked as a Teams Codex child", env["CODEX_HELPER_TEAMS_CHILD"] == "1", "CODEX_HELPER_TEAMS_CHILD=1",
		fmt.Sprintf("%q", env["CODEX_HELPER_TEAMS_CHILD"]))
	s.Check("the turn uses the user's CODEX_HOME", env["CODEX_HOME"] == s.Env.CodexHome, s.Env.CodexHome, env["CODEX_HOME"])
	s.Check("the turn runs in the work chat's directory", read("pwd.txt") == dir, dir, read("pwd.txt"))

	var stopFiles []string
	_ = filepath.WalkDir(s.Env.StateDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "DISABLED" {
			stopFiles = append(stopFiles, path)
		}
		return nil
	})
	s.Check("nothing stopped the helper", len(stopFiles) == 0, "no DISABLED file", fmt.Sprint(stopFiles))

	tok2 := s.Token("after")
	steps.MustPost(s, work.ID, "Still there? "+tok2)
	steps.WaitAnswered(s, work.ID, tok2, 45*time.Second)
	s.Settle(2*time.Second, 8*time.Second)
	n := len(steps.AnswerPosts(s, work.ID, tok)) + len(steps.AnswerPosts(s, work.ID, tok2))
	s.Check("both prompts are answered once", n == 2, "2 answers", fmt.Sprint(n), s.Transcript(work.ID, 10)...)
}

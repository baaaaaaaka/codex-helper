package turns

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Codex config changes apply from the next turn, without a helper restart
// (SPEC R-24, F-TRN-12, SC-12b; rc.15 58c6bc0). Codex reads config.toml
// (its MCP servers) when a thread is loaded into an app-server process,
// and the helper keeps a conversation's app-server warm between turns. The
// user adds an MCP server to $CODEX_HOME/config.toml while a turn runs.
// Expected: the running turn is not interrupted and finishes with the
// config it started with; the next turn runs on a new app-server with the
// added server. A helper that reuses the warm app-server keeps the old MCP
// list until it idles out (10 min) or the helper restarts.
func TestCodexConfigChangeAppliesNextTurn(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "CODEX-CONFIG", harness.Options{
		Guarantees: []string{"R-24", "F-TRN-12"},
		Title:      "an MCP server added to config.toml while a turn runs: that turn finishes undisturbed, the next turn sees it on a fresh app-server, no helper restart",
		Budget:     90 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	config := filepath.Join(s.Env.CodexHome, "config.toml")
	write := func(names ...string) {
		t.Helper()
		var b strings.Builder
		b.WriteString("# e2e\n")
		for _, n := range names {
			fmt.Fprintf(&b, "\n[mcp_servers.%s]\ncommand = %q\n", n, n+"-mcp")
		}
		if err := os.WriteFile(config, []byte(b.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("alpha")
	tok1, tok2, tok3 := s.Token("cfg1"), s.Token("cfg2"), s.Token("cfg3")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		var tok string
		for _, k := range []string{tok1, tok2, tok3} {
			if c.HasToken(k) {
				tok = k
			}
		}
		if tok == "" {
			return harness.CodexBehavior{}
		}
		return harness.CodexBehavior{
			Answer: "MCP servers: " + strings.Join(c.MCPServers, ", ") + " (" + tok + ")",
			Block:  tok == tok2,
		}
	})
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "config")

	steps.MustPost(s, work.ID, "Which MCP servers do you have? "+tok1)
	s.Require("turn 1 answered", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, tok1+")")) == 1, "not yet"
	})
	steps.MustPost(s, work.ID, "A longer task "+tok2)
	s.Require("turn 2 is running", steps.AnswerTimeout, func() (bool, string) {
		cs := s.Codex.CallsWithToken(tok2)
		return len(cs) == 1 && cs[0].Outcome == "running", fmt.Sprint(len(cs))
	})
	write("alpha", "beta")
	s.Codex.Release(s.Codex.CallsWithToken(tok2)[0].Seq)
	s.Require("turn 2 answered", steps.AnswerTimeout, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, tok2+")")) == 1, "not yet"
	})
	steps.MustPost(s, work.ID, "Which MCP servers now? "+tok3)
	s.Poll(steps.AnswerTimeout, func() bool { return len(s.SUTMessagesContaining(work.ID, tok3+")")) > 0 })
	s.Settle(time.Second, 4*time.Second)
	tr := s.Transcript(work.ID, 12)

	call := func(tok string) harness.CodexCall {
		if cs := s.Codex.CallsWithToken(tok); len(cs) == 1 {
			return cs[0]
		}
		return harness.CodexCall{}
	}
	c1, c2, c3 := call(tok1), call(tok2), call(tok3)
	mcp := func(c harness.CodexCall) string { return strings.Join(c.MCPServers, ", ") }
	s.Check("the running turn is not interrupted and keeps the config it started with",
		c2.Outcome == "answered" && mcp(c2) == "alpha" && c2.Pid == c1.Pid,
		"answered, alpha, same app-server as turn 1", fmt.Sprintf("%s, %q, same=%v", c2.Outcome, mcp(c2), c2.Pid == c1.Pid), tr...)
	s.Check("the next turn sees the added MCP server", c3.Outcome == "answered" && mcp(c3) == "alpha, beta",
		"answered, alpha, beta", fmt.Sprintf("%s, %q", c3.Outcome, mcp(c3)), tr...)
	s.Check("the next turn runs on a new app-server", c3.Pid != 0 && c3.Pid != c2.Pid,
		"a new pid", fmt.Sprintf("pid %d after %d", c3.Pid, c2.Pid))
	answers := len(s.SUTMessagesContaining(work.ID, "MCP servers: alpha, beta ("+tok3+")"))
	s.Check("the user sees the new MCP list in the answer", answers == 1, "1 answer", fmt.Sprint(answers), tr...)
	restarts := len(s.SUTMessagesContaining(control.ID, "restart"))
	s.Check("no helper restart", restarts == 0, "no restart notice", fmt.Sprint(restarts), s.Transcript(control.ID, 8)...)
}

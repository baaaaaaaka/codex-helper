package commands

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Removed Teams commands (decision 4 / 定稿 "删除": `helper restore-thread`,
// `helper reload` (keep `restart`), `model provider/catalog` and the
// third-party model API). Each gets a pointer to the help instead of doing
// anything, and none of them reaches Codex as a prompt.
func TestRemovedTeamsCommands(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "DELETED-CMDS", harness.Options{
		Guarantees: []string{"removed-commands"},
		Title:      "`helper restore-thread`, `helper reload`, `helper model provider|catalog`, control `reload`: refused with a pointer to help, no Codex turn",
		Budget:     90 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "removed")
	before := len(s.Codex.Calls())
	for _, c := range []struct{ chat, text, want string }{
		{work.ID, "helper restore-thread", "Unknown helper command"},
		{work.ID, "helper reload", "Unknown helper command"},
		{work.ID, "helper model provider", "Unknown model"},
		{work.ID, "helper model catalog", "Unknown model"},
		{control.ID, "reload", "Unknown command"},
		{control.ID, "restore-thread", "Unknown command"},
	} {
		got, ok := command(s, c.chat, c.text, c.want)
		s.Check(fmt.Sprintf("`%s` is refused", c.text), ok, c.want, clip(got, 160))
	}
	s.Settle(time.Second, 4*time.Second)
	var work0 []string
	for _, c := range s.Codex.Calls()[before:] {
		if c.Kind == "work" {
			work0 = append(work0, clip(c.Prompt, 60))
		}
	}
	s.Check("no removed command reaches Codex", len(work0) == 0, "0 work turns", strings.Join(work0, " | "))
}

// Removed CLI commands (decision 4: `cxp teams recover`, `probe-chat`, the
// separate read/write/file tokens, `teams path` modes captured-terminal
// and service; decision 熔断: the offline breaker CLI). Each exits non-zero
// without touching anything; the kept modes still work.
func TestRemovedCLICommands(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "DELETED-CLI", harness.Options{
		Guarantees: []string{"removed-commands"},
		Title:      "`cxp teams recover|probe-chat|pause|drain`, `auth read|file-write`, `path mode captured-terminal|service`: unknown, non-zero exit; kept `path mode account-default` works",
		Budget:     60 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	for _, args := range [][]string{
		{"teams", "recover"},
		{"teams", "probe-chat"},
		{"teams", "pause"},
		{"teams", "drain"},
		{"teams", "auth", "read"},
		{"teams", "auth", "file-write"},
		{"teams", "auth", "read-status"},
		{"teams", "path", "mode", "captured-terminal"},
		{"teams", "path", "mode", "service"},
	} {
		out, code := s.RunCLI(20*time.Second, args...)
		low := strings.ToLower(out)
		s.Check(fmt.Sprintf("`cxp %s` is gone", strings.Join(args, " ")),
			code != 0 && (strings.Contains(low, "unknown") || strings.Contains(low, "accepts 0 arg")),
			"non-zero exit, unknown", fmt.Sprintf("exit %d: %s", code, clip(out, 160)))
	}
	out, code := s.RunCLI(20*time.Second, "teams", "path", "mode", "account-default")
	s.Check("kept `cxp teams path mode account-default` works", code == 0, "exit 0", fmt.Sprintf("exit %d: %s", code, clip(out, 160)))
}

package lifecycle

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// Teams runs the CXP-managed Codex (SPEC R-24, SC-12a; rc.15 e9d67c16).
// The helper starts without --codex-path, as a user's service does. The
// user has an old `codex` first on PATH and the CXP-managed Codex under
// HOME (the one `codex update now` and a too-old rejection upgrade). The
// fake Codex records which executable ran each turn; the PATH copy rejects
// every request as too old, as an outdated global install does. Expected:
// every turn runs the managed Codex and is answered, with no "needs an
// update" notice. A helper that takes `codex` from PATH gets the too-old
// rejection and then upgrades a binary it does not run.
func TestTeamsRunsManagedCodex(t *testing.T) {
	harness.Parallel(t)
	globalBin := t.TempDir()
	global := filepath.Join(globalBin, "codex")
	installCodexCopy(t, global)
	s := harness.NewScenario(t, "CODEX-MANAGED", harness.Options{
		Guarantees: []string{"R-24"},
		Title:      "without --codex-path the helper runs the CXP-managed Codex, not an older `codex` first on PATH: answered, no too-old notice",
		Budget:     90 * time.Second,
		Factory:    sut.InstalledCodexFactory(),
		NoStart:    true,
		UserEnv:    map[string]string{"PATH": globalBin + string(os.PathListSeparator) + os.Getenv("PATH")},
	})
	defer s.Finish()
	managed := filepath.Join(s.Env.Home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	installCodexCopy(t, managed)
	which := func(exe string) string {
		switch exe {
		case managed:
			return "managed"
		case global:
			return "PATH"
		}
		return "other"
	}
	tok1, tok2 := s.Token("managed1"), s.Token("managed2")
	ans1, ans2 := "Answered by the managed Codex "+tok1, "Answered by the managed Codex "+tok2
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.Exe == global {
			return harness.CodexBehavior{Err: errors.New("The 'gpt-9' model requires a newer version of Codex. Please upgrade to the latest app or CLI and try again.")}
		}
		switch {
		case c.HasToken(tok1):
			return harness.CodexBehavior{Answer: ans1}
		case c.HasToken(tok2):
			return harness.CodexBehavior{Answer: ans2}
		}
		return harness.CodexBehavior{}
	})
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "managed")
	steps.MustPost(s, work.ID, "First request "+tok1)
	s.Poll(steps.AnswerTimeout, func() bool {
		return len(s.SUTMessagesContaining(work.ID, ans1)) > 0 || len(s.SUTMessagesContaining(work.ID, "Codex CLI needs an update")) > 0
	})
	steps.MustPost(s, work.ID, "Second request "+tok2)
	s.Poll(steps.AnswerTimeout, func() bool {
		return len(s.SUTMessagesContaining(work.ID, ans2)) > 0 || len(s.SUTMessagesContaining(work.ID, "Codex CLI needs an update")) > 1
	})
	s.Settle(2*time.Second, 6*time.Second)
	tr := s.Transcript(work.ID, 12)

	var ran []string
	for _, c := range s.Codex.Calls() {
		if c.Kind == "work" && (c.HasToken(tok1) || c.HasToken(tok2)) {
			ran = append(ran, which(c.Exe))
		}
	}
	allManaged := len(ran) == 2
	for _, r := range ran {
		allManaged = allManaged && r == "managed"
	}
	s.Check("every turn runs the CXP-managed Codex, not the `codex` first on PATH", allManaged,
		"2 turns: managed, managed", fmt.Sprintf("%d turns: %s", len(ran), strings.Join(ran, ", ")), tr...)
	n1, n2 := len(s.SUTMessagesContaining(work.ID, ans1)), len(s.SUTMessagesContaining(work.ID, ans2))
	s.Check("both requests are answered once", n1 == 1 && n2 == 1, "1 + 1 answers", fmt.Sprintf("%d + %d answers", n1, n2), tr...)
	tooOld := len(s.SUTMessagesContaining(work.ID, "Codex CLI needs an update"))
	s.Check("no too-old notice (the managed Codex is current)", tooOld == 0, "0 notices", fmt.Sprintf("%d notices", tooOld), tr...)
	upgrade := len(s.SUTMessagesContaining(control.ID, "Codex CLI upgrade"))
	s.Check("no Codex upgrade is attempted", upgrade == 0, "0 upgrade notices in the control chat", fmt.Sprintf("%d", upgrade), s.Transcript(control.ID, 8)...)
}

// installCodexCopy puts a copy of the fake Codex executable at path (a
// copy, so the executable path it reports is path).
func installCodexCopy(t *testing.T, path string) {
	t.Helper()
	src, err := os.Open(sut.FakeCodexBinary())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	dst, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		t.Fatal(err)
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}
}

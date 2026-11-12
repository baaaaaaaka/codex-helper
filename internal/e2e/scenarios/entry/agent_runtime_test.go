package entry

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// Keep an older stable dispatcher while a newer immutable runtime serves
// Teams. The agent must re-enter that dispatcher with a clean environment,
// rather than bypassing activation or bootstrapping a nested installation.
func TestTeamsAgentUsesStableCLIEntry(t *testing.T) {
	harness.Parallel(t)
	var entry, root string
	factory := func(cfg harness.SUTConfig) (harness.SUT, error) {
		entry = filepath.Join(cfg.Env.Home, "安装 with spaces", "cxp")
		root = filepath.Join(filepath.Dir(entry), ".cxp-runtime")
		for _, version := range []string{"v9.9.0", "v9.9.1"} {
			built, err := sut.VersionedCLI(version)
			if err != nil {
				return nil, err
			}
			raw, err := os.ReadFile(built)
			if err != nil {
				return nil, err
			}
			path := filepath.Join(root, "versions", version, "cxp")
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				return nil, err
			}
			if err := os.WriteFile(path, raw, 0700); err != nil {
				return nil, err
			}
			if version == "v9.9.0" {
				if err := os.WriteFile(entry, raw, 0700); err != nil {
					return nil, err
				}
			}
		}
		if err := publishBootFixture(root, "v9.9.1", "v9.9.0", 1); err != nil {
			return nil, err
		}
		return sut.Factory(entry, sut.FakeCodexBinary())(cfg)
	}
	s := harness.NewScenario(t, "AGENT-RUNTIME", harness.Options{
		Title:  "Teams agents invoke the active runtime through an older stable entry without leaking launcher markers",
		Budget: 180 * time.Second, NoStart: true, Factory: factory,
		UserEnv: map[string]string{"CXP_RUNTIME_DISABLE": "", "CXP_RUNTIME_FORCE": "1"},
		Golden:  harness.GoldenOptions{Skip: "assert the real child environment, CLI output and unchanged runtime files; existing child-guard goldens cover the chat"},
	})
	defer s.Finish()
	// The disposable Docker account has no login shell. Exercise the real
	// POSIX probe with an explicit supported shell instead of using its
	// service-PATH fallback.
	if out, code := s.RunCLI(20*time.Second, "teams", "path", "shell", "/bin/bash"); code != 0 {
		t.Fatalf("configure the fixture login shell: %s", out)
	}
	probe := s.RunCLIAs(30*time.Second, harness.CLIAs{Poll: true, Binary: entry,
		Env: map[string]string{"CXP_RUNTIME_DISABLE": "", "CXP_RUNTIME_FORCE": "1"}}, "teams", "path", "status")
	if !s.Check("the managed entry can run the login-shell PATH probe", probe.Code == 0 && strings.Contains(probe.Stdout, "PATH fingerprint:"),
		"successful PATH probe", probe.Combined) {
		return
	}
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	work, dir := steps.NewWorkChat(s, control, "agent-runtime")
	read := func(path string) []byte {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	bootBefore := read(filepath.Join(root, "boot.sqlite"))
	sealBefore := read(filepath.Join(root, "boot.sqlite.published"))
	tok := s.Token("entry")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if !c.HasToken(tok) {
			return harness.CodexBehavior{}
		}
		return harness.CodexBehavior{Exec: []string{
			`env > agent.env`,
			`"$CODEX_HELPER_CLI_PATH" --version > agent.version 2>&1`,
			`"$CODEX_HELPER_CLI_PATH" teams read --help > agent.help 2>&1`,
			`"$CODEX_HELPER_CLI_PATH" teams service restart > agent.guard 2>&1; echo $? > agent.guard-code`,
		}}
	})
	steps.MustPost(s, work.ID, "Inspect the installed CLI "+tok)
	if _, ok := steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout); !ok {
		return
	}
	env := map[string]string{}
	for _, line := range strings.Split(string(read(filepath.Join(dir, "agent.env"))), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			env[key] = value
		}
	}
	s.Check("the agent receives the owning stable entry", env["CODEX_HELPER_CLI_PATH"] == entry && env["CODEX_HELPER_CLI_DIR"] == filepath.Dir(entry), entry, env["CODEX_HELPER_CLI_PATH"])
	for _, key := range []string{"CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH", "CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE", "CXP_BOOT_MANAGED", "CXP_RUNTIME_PROBE_PROTOCOL", "CXP_BOOT_ADDRESS", "CXP_BOOT_TOKEN"} {
		_, present := env[key]
		s.Check("the agent does not inherit "+key, !present, "absent", fmt.Sprint(present))
	}
	s.Check("the old entry dispatches to the activated version", strings.Contains(string(read(filepath.Join(dir, "agent.version"))), "v9.9.1"), "v9.9.1", string(read(filepath.Join(dir, "agent.version"))))
	s.Check("the agent sees Teams read command help", strings.Contains(string(read(filepath.Join(dir, "agent.help"))), "--before") && strings.Contains(string(read(filepath.Join(dir, "agent.help"))), "--replies"), "Teams read help with --before and --replies", string(read(filepath.Join(dir, "agent.help"))))
	guard := string(read(filepath.Join(dir, "agent.guard")))
	s.Check("self-management is still refused", strings.TrimSpace(string(read(filepath.Join(dir, "agent.guard-code")))) != "0" && strings.Contains(guard, "from a Codex turn launched by Teams helper"), "nonzero exit and child protection", guard)
	s.Check("the helper stays running", s.SUT.Running(), "running", fmt.Sprint(s.SUT.Running()))
	s.Check("login-shell PATH resolution did not fall back", !strings.Contains(s.SUT.Output(), "login-shell PATH could not be resolved"), "no PATH fallback", s.OutputTail(1000))
	s.Check("read-only agent commands leave the boot record unchanged", bytes.Equal(bootBefore, read(filepath.Join(root, "boot.sqlite"))) && bytes.Equal(sealBefore, read(filepath.Join(root, "boot.sqlite.published"))), "unchanged boot database and publication seal", "compared bytes")
	var nested []string
	err := filepath.WalkDir(filepath.Join(root, "versions"), func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && d.Name() == ".cxp-runtime" {
			nested = append(nested, path)
		}
		return err
	})
	s.Check("no nested runtime installation is created", err == nil && len(nested) == 0, "no nested .cxp-runtime", fmt.Sprintf("%v: %v", nested, err))

	// Keep the helper and its app-server alive across an external activation.
	// The entry must use the newly confirmed version, not the helper's pinned
	// launch version. Rolling back also tests selection of an older runtime.
	if err := publishBootFixture(root, "v9.9.0", "v9.9.1", 2); err != nil {
		t.Fatal(err)
	}
	tok2 := s.Token("activation")
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok2) {
			return harness.CodexBehavior{Exec: []string{`"$CODEX_HELPER_CLI_PATH" --version > agent.activated-version 2>&1`}}
		}
		return harness.CodexBehavior{}
	})
	steps.MustPost(s, work.ID, "Inspect the CLI after activation "+tok2)
	if _, ok := steps.WaitAnswered(s, work.ID, tok2, steps.AnswerTimeout); !ok {
		return
	}
	selected := string(read(filepath.Join(dir, "agent.activated-version")))
	s.Check("the existing agent follows the new confirmed activation", strings.Contains(selected, "v9.9.0"), "v9.9.0", selected)
	s.Check("activation does not stop the existing helper", s.SUT.Running(), "running", fmt.Sprint(s.SUT.Running()))
}

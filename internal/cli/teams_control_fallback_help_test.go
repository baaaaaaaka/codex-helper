package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTeamsControlFallbackHelpContextCoversOperationalCommands(t *testing.T) {
	got := teamsControlFallbackHelpContext()
	for _, want := range []string{
		"cxp / codex-proxy CLI digest:",
		"`cxp proxy reset`",
		"`cxp run -- codex`",
		"`cxp run --aaa -- codex`",
		"`cxp run --model-profile <name> -- codex`",
		"`cxp model` and `cxp model-profile`",
		"`cxp model list`",
		"`cxp model-profile setup [name]",
		"`cxp responses serve`",
		"`cxp app --model-profile <name>`",
		"`Ctrl+K`",
		"`Ctrl+A`",
		"`cxp teams status`",
		"`cxp teams doctor`",
		"`cxp teams service bootstrap`",
		"`cxp teams workflow status|enable|disable|test`",
		"`cxp teams send-file <path> --session <session-id>`",
		"`cxp teams probe-chat --chat <teams-chat-id-or-link>`",
		"`cxp teams pause|resume|drain|recover`",
		"`cxp teams chat recreate <session-id> --history full --yes`",
		"`cxp teams chat publish-history <session-id> --full --yes`",
		"$CODEX_HELPER_CLI_PATH",
		"`cxp skills install-builtin`",
		"`cxp skills migrate`",
		"`helper skills add <github/gitlab/git-url>`",
		"`helper skills sync [name]`",
		"`helper skills push [name]`",
		"`new <directory> --model <profile>`",
		"`model list`",
		"`model status`",
		"`model reset`",
		"`default status`",
		"`default model status|list|set|reset`",
		"`default effort status|list|set|reset`",
		"`helper update prerelease`",
		"`codex update now`",
		"`helper cancel last`",
		"`helper cancel all`",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("teamsControlFallbackHelpContext missing %q:\n%s", want, got)
		}
	}
	for _, forbidden := range []string{
		"access_token",
		"refresh_token",
		"client_secret",
		"Webhook URL:",
	} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("teamsControlFallbackHelpContext contains sensitive placeholder %q:\n%s", forbidden, got)
		}
	}
}

func TestTeamsControlFallbackDigestStaysAlignedWithDocsAndSkill(t *testing.T) {
	fallback := teamsControlFallbackHelpContext()
	repoRoot := sourceCheckoutRootForDocsDriftTest(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
	readme := read(filepath.Join(repoRoot, "README.md"))
	skill := read(filepath.Join(repoRoot, "internal", "skills", "builtin", "cxp", "references", "commands.md"))
	checks := []struct {
		name     string
		fallback []string
		docs     []string
	}{
		{
			name:     "model profiles and standard approvals",
			fallback: []string{"run -- codex", "run --aaa -- codex", "run --model-profile <name> -- codex", "model list", "model-profile setup [name]", "responses serve", "app --model-profile <name>", "Ctrl+A", "Ctrl+K"},
			docs:     []string{"run -- codex", "run --aaa -- codex", "run --model-profile <name> -- codex", "model list", "model-profile setup [name]", "responses serve", "app --model-profile <name>", "Ctrl+A", "Ctrl+K"},
		},
		{
			name:     "teams model commands",
			fallback: []string{"new <directory> --model <profile>", "model list", "model status", "model switch <profile>", "model reset", "model fork <profile>", "default model status|list|set|reset", "default effort status|list|set|reset"},
			docs:     []string{"new <directory> --model", "model list", "model status", "model switch", "model reset", "model fork", "default model status|list|set|reset", "default effort status|list|set|reset"},
		},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			for _, want := range check.fallback {
				if !strings.Contains(fallback, want) {
					t.Fatalf("fallback help missing %q:\n%s", want, fallback)
				}
			}
			for _, want := range check.docs {
				if !strings.Contains(readme, want) {
					t.Fatalf("README missing %q", want)
				}
				if !strings.Contains(skill, want) {
					t.Fatalf("built-in cxp skill reference missing %q", want)
				}
			}
		})
	}
}

func TestTeamsLocalSupervisorFallbackDocsStayAligned(t *testing.T) {
	fallback := teamsControlFallbackHelpContext()
	repoRoot := sourceCheckoutRootForDocsDriftTest(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
	docs := map[string]string{
		"README.md":                               read(filepath.Join(repoRoot, "README.md")),
		"deployment guide":                        read(filepath.Join(repoRoot, "docs", "teams_source_deployment_guide.md")),
		"feature interference matrix":             read(filepath.Join(repoRoot, "docs", "cxp_feature_interference_matrix.md")),
		"security threat model":                   read(filepath.Join(repoRoot, "docs", "teams_security_threat_model.md")),
		"integration execution plan":              read(filepath.Join(repoRoot, "docs", "teams_integration_execution_plan.md")),
		"integration plan":                        read(filepath.Join(repoRoot, "docs", "teams_integration_plan.md")),
		"built-in cxp skill command reference":    read(filepath.Join(repoRoot, "internal", "skills", "builtin", "cxp", "references", "commands.md")),
		"Teams control fallback help command set": fallback,
	}
	for name, doc := range docs {
		for _, want := range []string{
			"local-supervisor",
			"systemd --user",
			"terminal close",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing %q", name, want)
			}
		}
	}
	for name, doc := range map[string]string{
		"README.md":                   docs["README.md"],
		"deployment guide":            docs["deployment guide"],
		"security threat model":       docs["security threat model"],
		"integration execution plan":  docs["integration execution plan"],
		"integration plan":            docs["integration plan"],
		"feature interference matrix": docs["feature interference matrix"],
	} {
		if !strings.Contains(doc, "machine") || !strings.Contains(doc, "reboot") {
			t.Fatalf("%s missing local-supervisor reboot limitation", name)
		}
	}
	for name, doc := range docs {
		if !strings.Contains(doc, "sticky") && !strings.Contains(doc, "backend flapping") {
			t.Fatalf("%s missing local-supervisor sticky/backend-flapping semantics", name)
		}
	}
	for name, doc := range map[string]string{
		"README.md":                               docs["README.md"],
		"deployment guide":                        docs["deployment guide"],
		"integration plan":                        docs["integration plan"],
		"built-in cxp skill command reference":    docs["built-in cxp skill command reference"],
		"Teams control fallback help command set": docs["Teams control fallback help command set"],
	} {
		if !strings.Contains(doc, "WSL") || !strings.Contains(doc, "local-supervisor") {
			t.Fatalf("%s missing WSL local-supervisor backend semantics", name)
		}
	}
}

func sourceCheckoutRootForDocsDriftTest(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	start := dir
	for {
		if regularFileExists(filepath.Join(dir, "README.md")) &&
			regularFileExists(filepath.Join(dir, "internal", "skills", "builtin", "cxp", "references", "commands.md")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	t.Skipf("source checkout not available from %s; docs drift test runs in source-tree CI", start)
	return ""
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

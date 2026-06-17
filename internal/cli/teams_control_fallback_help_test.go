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
		"`cxp run --yolo -- codex`",
		"`cxp run --model-profile <name> -- codex`",
		"`cxp model` and `cxp model-profile`",
		"`cxp model list`",
		"`cxp model-profile setup [name]",
		"`cxp responses serve`",
		"`cxp app --model-profile <name>`",
		"`Ctrl+Y`",
		"`Ctrl+K`",
		"`cxp teams status`",
		"`cxp teams doctor`",
		"`cxp teams service bootstrap`",
		"`cxp teams workflow status|enable|disable|test`",
		"`cxp teams send-file <path> --session <session-id>`",
		"`cxp teams probe-chat --chat <teams-chat-id-or-link>`",
		"`cxp teams pause|resume|drain|recover`",
		"`cxp teams chat recreate <session-id> --yes`",
		"`cxp beacon profile create <name>",
		"`cxp beacon profile update <name>",
		"`cxp beacon profile history <name>`",
		"`cxp beacon profile rollback <name> <revision>`",
		"`cxp beacon profile gc <name>`",
		"--query-command <script>",
		"`cxp beacon profile doctor <name>`",
		"`cxp beacon profile confirm <name>`",
		"`cxp beacon switch-profile <name> --session <id>`",
		"`cxp beacon switch-profile <name> --session <id> --after-current-turn`",
		"`cxp beacon release <profile|allocation|provider-job|machine> [--force] [--confirm <token>]`",
		"`cxp beacon allocation list|status|cancel|reconcile`",
		"`cxp beacon allocation reconcile-all`",
		"`cxp beacon provider template slurm|lsf`",
		"`cxp beacon worker run-once --machine <id>`",
		"`cxp beacon worker serve --allocation <request-id>`",
		"--shared-store",
		"--codex-path <codex-or-wrapper>",
		"--no-yolo",
		"--skip-git-repo-check",
		"CXP_BEACON_CODEX_BIN",
		"launch Codex in yolo mode by default",
		"Teams service `--codex-arg` settings do not automatically reach remote beacon workers",
		"Keep exactly one `exec`",
		"`--adapter-shell direct`",
		"user-shell capture is incompatible",
		"CODEX_HELPER_BEACON_SLURM_QUERY",
		"Beacon execution profiles are separate from SSH proxy profiles",
		"$CODEX_HELPER_CLI_PATH",
		"`cxp skills install-builtin`",
		"`cxp skills migrate`",
		"`helper skills add <github/gitlab/git-url>`",
		"`helper skills sync [name]`",
		"`helper skills push [name]`",
		"`new <directory> --model <profile>`",
		"`model list`",
		"`model status`",
		"`helper update prerelease`",
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

func TestTeamsControlFallbackBeaconDigestStaysAlignedWithDocsAndSkill(t *testing.T) {
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
			name:     "model profiles and yolo",
			fallback: []string{"run --yolo -- codex", "run --model-profile <name> -- codex", "model list", "model-profile setup [name]", "responses serve", "app --model-profile <name>", "Ctrl+Y", "Ctrl+K"},
			docs:     []string{"run --yolo -- codex", "run --model-profile <name> -- codex", "model list", "model-profile setup [name]", "responses serve", "app --model-profile <name>", "Ctrl+Y", "Ctrl+K"},
		},
		{
			name:     "teams model commands",
			fallback: []string{"new <directory> --model <profile>", "model list", "model status", "model switch <profile>", "model fork <profile>"},
			docs:     []string{"new <directory> --model", "model list", "model status", "model switch", "model fork"},
		},
		{
			name:     "profile lifecycle",
			fallback: []string{"beacon profile create <name>", "beacon profile update <name>", "beacon profile history <name>", "beacon profile rollback <name> <revision>", "beacon profile gc <name>", "beacon profile doctor <name>", "beacon profile confirm <name>"},
			docs:     []string{"beacon profile create <name>", "beacon profile update <name>", "beacon profile history <name>", "beacon profile rollback <name> <revision>", "beacon profile gc <name>", "beacon profile doctor <name>", "beacon profile confirm <name>"},
		},
		{
			name:     "profile adapter commands",
			fallback: []string{"--query-command <script>", "--submit-command <script>", "--cancel-command <script>", "--renew-command <script>"},
			docs:     []string{"--query-command", "--submit-command", "--cancel-command", "--renew-command"},
		},
		{
			name:     "profile switching",
			fallback: []string{"beacon switch-profile <name> --session <id>", "beacon switch-profile <name> --session <id> --after-current-turn"},
			docs:     []string{"beacon switch-profile <name> --session", "--after-current-turn"},
		},
		{
			name:     "managed allocation",
			fallback: []string{"beacon release <profile|allocation|provider-job|machine>", "beacon allocation list|status|cancel|reconcile", "beacon allocation reconcile-all"},
			docs:     []string{"beacon release <profile|allocation|provider-job|machine>", "beacon allocation list", "beacon allocation status", "beacon allocation cancel", "beacon allocation reconcile", "beacon allocation reconcile-all"},
		},
		{
			name:     "provider templates",
			fallback: []string{"beacon provider template slurm|lsf"},
			docs:     []string{"beacon provider template slurm", "beacon provider template lsf"},
		},
		{
			name:     "worker execution",
			fallback: []string{"beacon worker run-once --machine", "beacon worker run-once --allocation", "beacon worker serve --allocation"},
			docs:     []string{"beacon worker run-once --machine", "beacon worker run-once --allocation", "beacon worker serve --allocation"},
		},
		{
			name:     "worker adapter troubleshooting",
			fallback: []string{"--shared-store", "--codex-path <codex-or-wrapper>", "--no-yolo", "--skip-git-repo-check", "CXP_BEACON_CODEX_BIN", "launch Codex in yolo mode by default", "Keep exactly one `exec`", "`--adapter-shell direct`"},
			docs:     []string{"--shared-store", "--codex-path <codex-or-wrapper>", "--no-yolo", "--skip-git-repo-check", "CXP_BEACON_CODEX_BIN", "launch Codex in yolo mode by default", "Keep exactly one `exec`", "`--adapter-shell direct`"},
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

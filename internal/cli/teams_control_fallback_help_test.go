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
		"`cxp teams status`",
		"`cxp teams doctor`",
		"`cxp teams service bootstrap`",
		"`cxp beacon profile create <name>",
		"`cxp beacon profile doctor <name>`",
		"`cxp beacon profile confirm <name>`",
		"`cxp beacon switch-profile <name> --session <id>`",
		"`cxp beacon switch-profile <name> --session <id> --after-current-turn`",
		"`cxp beacon allocation list|status|reconcile`",
		"`cxp beacon allocation reconcile-all`",
		"`cxp beacon provider template slurm|lsf`",
		"`cxp beacon worker run-once --machine <id>`",
		"`cxp beacon worker serve --allocation <request-id>`",
		"CODEX_HELPER_BEACON_SLURM_QUERY",
		"Beacon execution profiles are separate from SSH proxy profiles",
		"$CODEX_HELPER_CLI_PATH",
		"`cxp skills install-builtin`",
		"`helper skills add <github/gitlab/git-url>`",
		"`helper skills sync [name]`",
		"`helper skills push [name]`",
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
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
	readme := read(filepath.Join("..", "..", "README.md"))
	skill := read(filepath.Join("..", "skills", "builtin", "cxp", "references", "commands.md"))
	checks := []struct {
		name     string
		fallback []string
		docs     []string
	}{
		{
			name:     "profile lifecycle",
			fallback: []string{"beacon profile create <name>", "beacon profile doctor <name>", "beacon profile confirm <name>"},
			docs:     []string{"beacon profile create <name>", "beacon profile doctor <name>", "beacon profile confirm <name>"},
		},
		{
			name:     "profile switching",
			fallback: []string{"beacon switch-profile <name> --session <id>", "beacon switch-profile <name> --session <id> --after-current-turn"},
			docs:     []string{"beacon switch-profile <name> --session", "--after-current-turn"},
		},
		{
			name:     "managed allocation",
			fallback: []string{"beacon allocation list|status|reconcile", "beacon allocation reconcile-all"},
			docs:     []string{"beacon allocation list", "beacon allocation status", "beacon allocation reconcile", "beacon allocation reconcile-all"},
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

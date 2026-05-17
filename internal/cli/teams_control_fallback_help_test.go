package cli

import (
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
		"Beacon execution profiles are separate from SSH proxy profiles",
		"$CODEX_HELPER_CLI_PATH",
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

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
		"`helper update prerelease`",
		"`helper cancel last`",
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

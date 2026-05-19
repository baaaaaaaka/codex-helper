package cli

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestTeamsServiceRendererOutputsUseStableExecutable(t *testing.T) {
	tmp := t.TempDir()
	stable := filepath.Join(tmp, "codex-proxy")
	raw := filepath.Join(tmp, ".nfs802014de01c482a9000004bf")
	spec := teamsServiceSpec{
		Executable:   stable,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "registry.json"),
		Environment: map[string]string{
			"NO_COLOR":                   "1",
			"CODEX_HELPER_TEAMS_SERVICE": "1",
		},
	}
	rendered := map[string]string{
		"systemd main":         buildTeamsServiceUnit(spec),
		"systemd watchdog":     buildTeamsServiceWatchdogUnit(spec),
		"launchd main":         buildTeamsServiceLaunchAgentPlist(spec),
		"launchd watchdog":     buildTeamsServiceWatchdogLaunchAgentPlist(spec),
		"windows task":         buildTeamsServiceWindowsTaskXML(spec),
		"windows watchdog":     buildTeamsServiceWindowsWatchdogTaskXML(spec),
		"wsl task":             buildTeamsServiceWSLTaskConfig(teamsServiceWindowsTaskName, buildTeamsServiceWSLArguments(spec)),
		"wsl watchdog":         buildTeamsServiceWSLTaskConfig(teamsServiceWindowsWatchdogTaskName, buildTeamsServiceWSLWatchdogArguments(spec)),
		"wsl startup fallback": buildTeamsServiceWSLStartupFallbackConfig(teamsServiceWindowsTaskName, buildTeamsServiceWSLArguments(spec)),
	}
	for name, body := range rendered {
		if !containsRenderedPath(body, stable) {
			t.Fatalf("%s renderer missing stable executable %q:\n%s", name, stable, body)
		}
		for _, forbidden := range []string{raw, ".nfs802014de01c482a9000004bf", " (deleted)", ".reload-backup-", "go-build"} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("%s renderer contains transient marker %q:\n%s", name, forbidden, body)
			}
		}
	}
}

func containsRenderedPath(body string, path string) bool {
	if strings.Contains(body, path) {
		return true
	}
	escapedBackslashes := strings.ReplaceAll(path, `\`, `\\`)
	return escapedBackslashes != path && strings.Contains(body, escapedBackslashes)
}

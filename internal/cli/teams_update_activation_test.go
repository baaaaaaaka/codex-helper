package cli

import (
	"strings"
	"testing"
)

func TestWindowsTeamsPendingHelperProcessRestartRedirectsOutput(t *testing.T) {
	script := windowsTeamsPendingHelperProcessRestartPowerShell(
		`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
		`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
		"v1.2.3",
		[]string{"teams", "run"},
	)
	for _, want := range []string{
		"Start-Process -FilePath $dest",
		"-WindowStyle Hidden",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"teams-helper-process-restart.stdout.log",
		"teams-helper-process-restart.stderr.log",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("pending helper restart script missing %q:\n%s", want, script)
		}
	}
}

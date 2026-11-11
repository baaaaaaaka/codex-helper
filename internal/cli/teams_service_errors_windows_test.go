package cli

import (
	"os/exec"
	"strings"
	"testing"
)

// Native PowerShell errors can be non-terminating even when a scheduled-task
// cmdlet fails. A mocked Go runner that merely returns exit 1 cannot catch that.
func TestWindowsServiceTaskPermissionStopsAtFirstFailure(t *testing.T) {
	prefix := `function Get-ScheduledTask { [CmdletBinding()] param($TaskName); [pscustomobject]@{State='Disabled';TaskName=$TaskName;TaskPath='\'} }; function Enable-ScheduledTask { [CmdletBinding()] param($TaskName); Write-Error 'simulated task access denied' }; function Stop-ScheduledTask { [CmdletBinding()] param($TaskName); Write-Error 'simulated task access denied' }; function Start-ScheduledTask { throw 'must not reach start' }; `
	for name, script := range map[string]string{"start": teamsServiceWindowsStartTasksPowerShell(), "stop": teamsServiceWindowsStopTasksPowerShell()} {
		t.Run(name, func(t *testing.T) {
			out, err := exec.Command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command", prefix+script).CombinedOutput()
			if err == nil || !strings.Contains(string(out), "simulated task access denied") || strings.Contains(string(out), "must not reach start") {
				t.Fatalf("permission error not authoritative: %v %s", err, out)
			}
		})
	}
}

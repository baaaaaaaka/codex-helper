//go:build windows

package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

var teamsUserPathRunPowerShell = teamsServiceRunPowerShell

func teamsUserPathExpectedSID(ctx context.Context, mode string, environment []string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "" && mode != string(userpath.ModeAccountDefault) {
		return "", nil
	}
	service, _ := userpath.EnvironmentValue(environment, "CODEX_HELPER_TEAMS_SERVICE", true)
	if strings.TrimSpace(service) == "" {
		return "", nil
	}
	command := "$task = Get-ScheduledTask -TaskPath '\\' -TaskName " + powershellSingleQuote(teamsServiceWindowsTaskName) + " -ErrorAction Stop; " +
		"$principal = [string]$task.Principal.UserId; " +
		"if ([string]::IsNullOrWhiteSpace($principal)) { throw 'Teams task principal is empty' }; " +
		"try { $sid = [System.Security.Principal.SecurityIdentifier]::new($principal).Value } " +
		"catch { $sid = ([System.Security.Principal.NTAccount]::new($principal)).Translate([System.Security.Principal.SecurityIdentifier]).Value }; " +
		"$sid"
	data, err := teamsUserPathRunPowerShell(ctx, command)
	if err != nil {
		return "", fmt.Errorf("read configured Teams Scheduled Task principal SID: %w", err)
	}
	sid := strings.TrimSpace(string(data))
	if !strings.HasPrefix(strings.ToUpper(sid), "S-") {
		return "", fmt.Errorf("configured Teams Scheduled Task returned invalid SID %q", sid)
	}
	return sid, nil
}

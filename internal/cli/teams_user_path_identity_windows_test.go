//go:build windows

package cli

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestTeamsUserPathExpectedSIDReadsScheduledTaskPrincipal(t *testing.T) {
	previous := teamsUserPathRunPowerShell
	t.Cleanup(func() { teamsUserPathRunPowerShell = previous })
	teamsUserPathRunPowerShell = func(_ context.Context, command string) ([]byte, error) {
		if !strings.Contains(command, teamsServiceWindowsTaskName) || !strings.Contains(command, "Principal.UserId") {
			t.Fatalf("principal query = %q", command)
		}
		return []byte("S-1-5-21-1000\r\n"), nil
	}
	sid, err := teamsUserPathExpectedSID(context.Background(), "account-default", []string{"CODEX_HELPER_TEAMS_SERVICE=1"})
	if err != nil {
		t.Fatal(err)
	}
	if sid != "S-1-5-21-1000" {
		t.Fatalf("SID = %q", sid)
	}
}

func TestTeamsUserPathExpectedSIDSkipsNonServiceAndNonAccountModes(t *testing.T) {
	previous := teamsUserPathRunPowerShell
	t.Cleanup(func() { teamsUserPathRunPowerShell = previous })
	teamsUserPathRunPowerShell = func(context.Context, string) ([]byte, error) {
		return nil, errors.New("must not run")
	}
	for _, test := range []struct {
		mode string
		env  []string
	}{{mode: "service", env: []string{"CODEX_HELPER_TEAMS_SERVICE=1"}}, {mode: "account-default"}} {
		if sid, err := teamsUserPathExpectedSID(context.Background(), test.mode, test.env); err != nil || sid != "" {
			t.Fatalf("mode=%q env=%v SID=%q err=%v", test.mode, test.env, sid, err)
		}
	}
}

func TestTeamsUserPathExpectedSIDMatchesLiveScheduledTaskPrincipalCI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST") != "1" {
		t.Skip("set CODEX_HELPER_WINDOWS_TASK_REGISTER_TEST=1 to run native Task Scheduler principal verification")
	}
	existing, err := teamsServiceRunPowerShell(context.Background(), "$task = Get-ScheduledTask -TaskPath '\\' -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName)+" -ErrorAction SilentlyContinue; if ($null -ne $task) { 'exists' }")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(existing)) != "" {
		t.Skipf("refusing to replace an existing %q task", teamsServiceWindowsTaskName)
	}
	register := "$ErrorActionPreference = 'Stop'; " +
		"$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name; " +
		"$action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument '/c exit 0'; " +
		"$trigger = New-ScheduledTaskTrigger -Once -At ((Get-Date).AddYears(1)); " +
		"$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited; " +
		"Register-ScheduledTask -TaskPath '\\' -TaskName " + powershellSingleQuote(teamsServiceWindowsTaskName) + " -Action $action -Trigger $trigger -Principal $principal -ErrorAction Stop | Out-Null"
	if _, err := teamsServiceRunPowerShell(context.Background(), register); err != nil {
		t.Fatalf("register live Teams task: %v", err)
	}
	t.Cleanup(func() {
		_, cleanupErr := teamsServiceRunPowerShell(context.Background(), "Unregister-ScheduledTask -TaskPath '\\' -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName)+" -Confirm:$false -ErrorAction SilentlyContinue | Out-Null")
		if cleanupErr != nil {
			t.Errorf("unregister live Teams task: %v", cleanupErr)
		}
	})
	expectedData, err := teamsServiceRunPowerShell(context.Background(), "[System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value")
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.TrimSpace(string(expectedData))
	got, err := teamsUserPathExpectedSID(context.Background(), "account-default", []string{"CODEX_HELPER_TEAMS_SERVICE=1"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(got, expected) {
		t.Fatalf("live task principal SID = %q, want process SID %q", got, expected)
	}
}

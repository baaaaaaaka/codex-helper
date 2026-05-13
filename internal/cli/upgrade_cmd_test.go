package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestUpgradeCmdAlreadyUpToDateSkipsDownload(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		if opts.Repo != "owner/name" {
			t.Fatalf("expected repo owner/name, got %q", opts.Repo)
		}
		if opts.InstalledVersion == "" {
			t.Fatal("expected installed version")
		}
		if opts.Timeout != 8*time.Second {
			t.Fatalf("expected 8s check timeout, got %s", opts.Timeout)
		}
		return update.Status{Supported: true, UpdateAvailable: false}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("PerformUpdate should not run when latest is already installed")
		return update.ApplyResult{}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Already up to date.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdExplicitVersionCallsPerformUpdate(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name", "--version", "v1.2.3", "--install-path", "/tmp/codex-proxy"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}

	if got.Repo != "owner/name" || got.Version != "v1.2.3" || got.InstallPath != "/tmp/codex-proxy" {
		t.Fatalf("unexpected update options: %+v", got)
	}
	if got.Timeout != 120*time.Second {
		t.Fatalf("expected 120s update timeout, got %s", got.Timeout)
	}
	if !got.ValidateBinary {
		t.Fatal("upgrade command must validate the downloaded binary before replacing itself")
	}
	if !strings.Contains(out.String(), "Updated to v1.2.3.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdLatestCanIncludePrerelease(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(_ context.Context, opts update.CheckOptions) update.Status {
		if !opts.IncludePrerelease {
			t.Fatalf("CheckForUpdate IncludePrerelease = false, want true")
		}
		return update.Status{Supported: true, UpdateAvailable: true}
	}
	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{Version: "1.2.4-rc.1"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--repo", "owner/name", "--include-prerelease"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if got.Repo != "owner/name" || got.Version != "" || !got.IncludePrerelease || !got.ValidateBinary {
		t.Fatalf("PerformUpdate options = %#v, want latest prerelease lookup", got)
	}
	if !strings.Contains(out.String(), "Updated to v1.2.4-rc.1.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdRestartRequiredMessage(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)
	tmp := t.TempDir()

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		return update.Status{Supported: true, UpdateAvailable: true}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true, PendingReplacePath: filepath.Join(tmp, ".codex-proxy_1.2.3.tmp")}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Update replacement for v1.2.3 is pending.") ||
		!strings.Contains(out.String(), "verify `codex-proxy --version`") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdPropagatesUpdateError(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})

	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		return update.Status{Supported: false, Error: "network unavailable"}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{}, errors.New("download failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("expected update error, got %v", err)
	}
}

func isolateUpgradeTeamsServiceForTest(t *testing.T) *recordingTeamsServiceRunner {
	t.Helper()
	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	return runner
}

func isolateUpgradeTeamsStateForTest(t *testing.T, tmp string) {
	t.Helper()
	t.Setenv(envTeamsCodexChild, "")
	t.Setenv(envTeamsCodexParentPID, "")
	isolateTeamsUserDirsForTest(t, tmp)
}

func teamsServiceJoinedCalls(calls []teamsServiceCommandCall) string {
	var parts []string
	for _, call := range calls {
		parts = append(parts, call.name+" "+strings.Join(call.args, " "))
	}
	return strings.Join(parts, "\n")
}

func TestUpgradeCmdDrainsLiveTeamsOwnerBeforeUpdate(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		performCalled = true
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	st := seedLiveTeamsOwnerForUpgradeTest(t)
	cleared := make(chan struct{})
	go func() {
		defer close(cleared)
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			control, err := st.ReadControl(context.Background())
			if err == nil && control.Draining {
				_ = st.ClearOwner(context.Background())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1s"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	<-cleared
	if !performCalled {
		t.Fatal("performUpdate was not called after Teams drain")
	}
	if !strings.Contains(out.String(), "Waiting for active Teams bridge to drain") || !strings.Contains(out.String(), "Teams bridge drained.") {
		t.Fatalf("upgrade output missing Teams drain messages:\n%s", out.String())
	}
	control, err := st.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if control.Draining {
		t.Fatalf("drain should be cleared after update: %#v", control)
	}
	upgrade, ok, err := st.ReadUpgrade(context.Background())
	if err != nil {
		t.Fatalf("ReadUpgrade error: %v", err)
	}
	if !ok || upgrade.Phase != teamsstore.UpgradePhaseCompleted {
		t.Fatalf("upgrade state = %#v ok=%v, want completed", upgrade, ok)
	}
}

func TestUpgradeCmdDrainsScopedTeamsStateBeforeUpdate(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		performCalled = true
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	st := seedScopedLiveTeamsOwnerForUpgradeTest(t, "scope-upgrade")
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			control, err := st.ReadControl(context.Background())
			if err == nil && control.Draining {
				_ = st.ClearOwner(context.Background())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1s"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !performCalled {
		t.Fatal("performUpdate was not called after scoped Teams drain")
	}
	upgrade, ok, err := st.ReadUpgrade(context.Background())
	if err != nil {
		t.Fatalf("ReadUpgrade error: %v", err)
	}
	if !ok || upgrade.Phase != teamsstore.UpgradePhaseCompleted {
		t.Fatalf("scoped upgrade state = %#v ok=%v, want completed", upgrade, ok)
	}
}

func TestUpgradeCmdAllowsDeferredTeamsInboundWithoutOwner(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})
	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		performCalled = true
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	if _, _, err := st.PersistInbound(context.Background(), teamsstore.InboundEvent{
		TeamsChatID:    "chat-1",
		TeamsMessageID: "deferred-1",
		Status:         teamsstore.InboundStatusDeferred,
	}); err != nil {
		t.Fatalf("PersistInbound deferred error: %v", err)
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !performCalled {
		t.Fatal("performUpdate was not called with deferred-only Teams state")
	}
}

func TestUpgradeCmdStopsAndRestartsActiveTeamsServiceAroundUpdate(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:38471")

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	st := seedLiveTeamsOwnerForUpgradeTest(t)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			control, err := st.ReadControl(context.Background())
			if err == nil && control.Draining {
				_ = st.ClearOwner(context.Background())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1s"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("expected is-active, stop, and start calls, got %#v", runner.calls)
	}
	if !strings.Contains(out.String(), "Stopping Teams service before upgrade") ||
		!strings.Contains(out.String(), "Refreshing Teams service config before restart") ||
		!strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing service restart messages:\n%s", out.String())
	}
	unit, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read refreshed unit: %v", err)
	}
	if !strings.Contains(string(unit), "Environment=HTTP_PROXY=http://127.0.0.1:38471") {
		t.Fatalf("upgrade should refresh Teams service env with current proxy, unit:\n%s", string(unit))
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshUsesUACRepair(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			nil,
			nil,
			nil,
			nil,
			[]byte("DESKTOP\\alice\n"),
			nil,
			nil,
		},
		errs: []error{
			nil,
			nil,
			errors.New("task config mismatch"),
			errTeamsKeepaliveAccessDeniedForTest{},
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	cmd.SetIn(strings.NewReader("yes\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		!strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing UAC and restart messages:\n%s", out.String())
	}
	if strings.Contains(out.String(), "Register-ScheduledTask") {
		t.Fatalf("upgrade output should not include noisy PowerShell registration details:\n%s", out.String())
	}
	joined := teamsServiceJoinedCalls(runner.calls)
	if !strings.Contains(joined, "Start-Process -FilePath 'powershell.exe'") {
		t.Fatalf("expected elevated repair command, calls=%#v", runner.calls)
	}
	lastCall := runner.calls[len(runner.calls)-1]
	lastJoined := lastCall.name + " " + strings.Join(lastCall.args, " ")
	if !strings.Contains(lastJoined, "Start-ScheduledTask -TaskName $taskName") || strings.Contains(lastJoined, "Register-ScheduledTask") {
		t.Fatalf("expected normal Scheduled Task start after elevated repair, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
}

func TestUpgradeCmdWSLMatchingTaskRefreshSkipsRepairAndUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") {
		t.Fatalf("matching task refresh should be quiet, got output:\n%s", out.String())
	}
	joined := teamsServiceJoinedCalls(runner.calls)
	if strings.Contains(joined, "Register-ScheduledTask") {
		t.Fatalf("matching task refresh should not re-register Scheduled Tasks, calls=%#v", runner.calls)
	}
	if !strings.Contains(joined, "$allMatched = $true") {
		t.Fatalf("expected read-only task match probe before repair, calls=%#v", runner.calls)
	}
	for _, want := range []string{
		"Test-CodexHelperTaskDurationMinutes",
		"$task.Principal.UserId -ne $expectedPrincipalUser",
		"$task.Principal.LogonType -ne 'Interactive'",
		"$task.Principal.RunLevel -ne 'Limited'",
		"$settings.MultipleInstances -ne 'IgnoreNew'",
		"$settings.RestartCount -ne 999",
		"$settings.RestartInterval 1",
		"$settings.ExecutionTimeLimit 0",
		"$expectedActionExecute = 'wscript.exe'",
		"Test-Path -LiteralPath $launcherPowerShellPath",
		"Test-Path -LiteralPath $launcherVbsPath",
		"Get-Content -LiteralPath $launcherPowerShellPath -Raw",
		"Get-Content -LiteralPath $launcherVbsPath -Raw",
		"$expectedLauncherPowerShell",
		"$expectedLauncherVbs",
		"$hasLogonTrigger",
		"$hasRepeatingTrigger",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("matching probe should verify %q before skipping repair, calls=%#v", want, runner.calls)
		}
	}
	lastCall := runner.calls[len(runner.calls)-1]
	lastJoined := lastCall.name + " " + strings.Join(lastCall.args, " ")
	if !strings.Contains(lastJoined, "Start-ScheduledTask -TaskName $taskName") {
		t.Fatalf("expected normal Scheduled Task start, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
}

func TestUpgradeCmdWSLMismatchRefreshRepairsWithoutUACWhenAllowed(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			nil,
			nil,
			errors.New("task config mismatch"),
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	cmd.SetIn(strings.NewReader("yes\n"))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") {
		t.Fatalf("repairable mismatch should not ask for UAC or fallback:\n%s", out.String())
	}
	joined := teamsServiceJoinedCalls(runner.calls)
	if !strings.Contains(joined, "Register-ScheduledTask -TaskName $taskName") {
		t.Fatalf("mismatched task should be repaired with normal registration, calls=%#v", runner.calls)
	}
	if strings.Contains(joined, "Start-Process -FilePath 'powershell.exe'") {
		t.Fatalf("normal registration success should not use elevated repair, calls=%#v", runner.calls)
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshFallsBackWithoutUACWhenCleanupIsAllowed(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			nil,
			nil,
			errors.New("task config mismatch"),
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Stop-ScheduledTask -TaskName $taskName") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	cmd.SetIn(strings.NewReader(""))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "NOTICE: USING STARTUP WATCHDOG FALLBACK") ||
		!strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing fallback and restart messages:\n%s", out.String())
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") ||
		strings.Contains(out.String(), "UAC prompt") {
		t.Fatalf("upgrade should use quiet fallback without asking for UAC when old tasks can be retired normally:\n%s", out.String())
	}
	lastCall := runner.calls[len(runner.calls)-1]
	lastJoined := lastCall.name + " " + strings.Join(lastCall.args, " ")
	if !strings.Contains(teamsServiceJoinedCalls(runner.calls), "Disable-ScheduledTask") {
		t.Fatalf("fallback should retire old WSL Scheduled Tasks before installing Startup fallback, calls=%#v", runner.calls)
	}
	if strings.Contains(lastJoined, "Start-ScheduledTask -TaskName $taskName") {
		t.Fatalf("fallback start should not call Scheduled Task start after access denied, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
	if strings.Contains(teamsServiceJoinedCalls(runner.calls), "-Verb RunAs") {
		t.Fatalf("quiet fallback must not use elevated PowerShell, calls=%#v", runner.calls)
	}
	if !strings.Contains(lastJoined, "Start-Process -FilePath 'wscript.exe'") || !strings.Contains(lastJoined, "WScript.Shell") ||
		strings.Contains(lastJoined, "Set-Content -LiteralPath $legacyCmdLauncherPath") || strings.Contains(lastJoined, "Start-Process -FilePath 'cmd.exe'") {
		t.Fatalf("expected Startup fallback start command, last call=%#v all calls=%#v", lastCall, runner.calls)
	}
	backend := teamsServiceWSLWindowsTaskBackend{}
	if installed, err := backend.StartupFallbackMarkerExists(); err != nil || !installed {
		t.Fatalf("Startup fallback marker installed=%v err=%v", installed, err)
	}
	if _, err := os.Stat(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-task-"+teamsServiceWSLTaskIdentity().Suffix+".txt")); !os.IsNotExist(err) {
		t.Fatalf("stale Scheduled Task config should be removed after fallback, err=%v", err)
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshUsesElevatedRetireBeforeFallback(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("NVIDIA\\jason\n"),
			nil,
			nil,
			nil,
		},
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			errTeamsKeepaliveAccessDeniedForTest{},
			nil,
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out bytes.Buffer
	refresh, err := recoverWSLTeamsServiceRefreshAccessDenied(context.Background(), nil, strings.NewReader("yes\n"), &out, errTeamsKeepaliveAccessDeniedForTest{})
	if err != nil {
		t.Fatalf("recover refresh access denied: %v\n%s", err, out.String())
	}
	if !refresh.StartupFallback {
		t.Fatalf("refresh should install Startup fallback when elevated repair is denied")
	}
	if len(runner.calls) != 5 {
		t.Fatalf("refresh calls=%#v, want normal retire, user query, elevated repair, elevated retire, fallback", runner.calls)
	}
	elevatedRetire := strings.Join(runner.calls[3].args, " ")
	for _, want := range []string{
		"Start-Process",
		"-Verb RunAs",
		"Disable-ScheduledTask",
		"Codex Helper Teams Bridge",
		"Codex Helper Teams Watchdog",
	} {
		if !strings.Contains(elevatedRetire, want) {
			t.Fatalf("elevated retire command missing %q:\n%s", want, elevatedRetire)
		}
	}
	if strings.Contains(elevatedRetire, "Register-ScheduledTask") {
		t.Fatalf("elevated retire must not try to create or repair tasks:\n%s", elevatedRetire)
	}
	if !strings.Contains(out.String(), "Old WSL Scheduled Tasks were disabled using Windows permission") {
		t.Fatalf("upgrade fallback output missing elevated cleanup explanation:\n%s", out.String())
	}
}

func TestUpgradeCmdWSLAccessDeniedRefreshFailsWhenCleanupNeedsUACAndUACDeclined(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	runner := &scriptedTeamsServiceRunner{
		errs: []error{
			errTeamsKeepaliveAccessDeniedForTest{},
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	var out bytes.Buffer
	refresh, err := recoverWSLTeamsServiceRefreshAccessDenied(context.Background(), nil, strings.NewReader("no\n"), &out, errTeamsKeepaliveAccessDeniedForTest{})
	if err == nil {
		t.Fatalf("recover refresh should fail when normal cleanup needs UAC and UAC is declined")
	}
	if refresh.StartupFallback {
		t.Fatalf("refresh must not install Startup fallback when old Scheduled Tasks remain active")
	}
	if !strings.Contains(err.Error(), "UAC was not confirmed") {
		t.Fatalf("error should explain that UAC was needed and declined, got %v", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("refresh calls=%#v, want only normal retire before declined UAC", runner.calls)
	}
	if !strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") {
		t.Fatalf("cleanup failure should ask for UAC before failing:\n%s", out.String())
	}
}

func TestUpgradeCmdDelaysTeamsServiceRestartWhenUpdateNeedsProcessExit(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
		teamsServiceStartDetached = prevDetached
	})
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true}, nil
	}

	st := seedLiveTeamsOwnerForUpgradeTest(t)
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			control, err := st.ReadControl(context.Background())
			if err == nil && control.Draining {
				_ = st.ClearOwner(context.Background())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1s"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") {
		t.Fatalf("expected is-active and stop calls, got %#v", runner.calls)
	}
	if teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("restart-required upgrade must not immediately start service, calls=%#v", runner.calls)
	}
	if detachedName != "sh" || len(detachedArgs) != 2 || !strings.Contains(detachedArgs[1], "systemctl --user start '"+teamsServiceUnitName+"'") {
		t.Fatalf("unexpected detached restart: name=%q args=%#v", detachedName, detachedArgs)
	}
	if !strings.Contains(out.String(), "Scheduling Teams service restart after the pending helper replacement") ||
		!strings.Contains(out.String(), "Update replacement for v1.2.3 is pending.") ||
		!strings.Contains(out.String(), "verify `codex-proxy --version`") {
		t.Fatalf("upgrade output missing delayed restart/restart-required messages:\n%s", out.String())
	}
	upgrade, ok, err := st.ReadUpgrade(context.Background())
	if err != nil {
		t.Fatalf("ReadUpgrade error: %v", err)
	}
	if !ok || upgrade.Phase != teamsstore.UpgradePhaseAborted || !strings.Contains(upgrade.AbortReason, "did not complete") {
		t.Fatalf("restart-required upgrade state = %#v ok=%v, want aborted pending install", upgrade, ok)
	}
}

func TestUpgradeCmdDoesNotUpdateWhenTeamsServiceStopFails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &scriptedTeamsServiceRunner{errs: []error{nil, errors.New("stop failed")}}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("performUpdate should not run if Teams service stop fails")
		return update.ApplyResult{}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "stop failed") {
		t.Fatalf("execute upgrade error = %v, want stop failed\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") {
		t.Fatalf("expected is-active and stop calls, got %#v", runner.calls)
	}
	if teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("service should not be restarted after stop failure, calls=%#v", runner.calls)
	}
}

func TestUpgradeCmdRestartsTeamsServiceAfterUpdateFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{}, errors.New("download failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "download failed") {
		t.Fatalf("execute upgrade error = %v, want download failed\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("expected is-active, stop, and restart after failed update, got %#v", runner.calls)
	}
}

func TestUpgradeCmdReturnsDelayedTeamsRestartFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsServiceStartDetached = prevDetached
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true}, nil
	}
	var detachedCalled bool
	teamsServiceStartDetached = func(context.Context, string, ...string) error {
		detachedCalled = true
		return errors.New("detached restart failed")
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "detached restart failed") {
		t.Fatalf("execute upgrade error = %v, want detached restart failed\n%s", err, out.String())
	}
	if !detachedCalled {
		t.Fatal("delayed restart launcher was not called")
	}
	if teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("restart-required upgrade must not immediately start service, calls=%#v", runner.calls)
	}
}

func TestScheduleDelayedTeamsServiceStartUsesWSLWindowsTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "work")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu-22.04",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	prevDetached := teamsServiceStartDetached
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })

	if err := scheduleDelayedTeamsServiceStart(context.Background(), ""); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStart error: %v", err)
	}
	joined := strings.Join(detachedArgs, " ")
	if detachedName != "powershell.exe" ||
		!strings.Contains(joined, "Start-ScheduledTask") ||
		!strings.Contains(joined, "Codex Helper Teams Bridge (WSL Ubuntu-22.04 alice work ") ||
		!strings.Contains(joined, "Codex Helper Teams Watchdog (WSL Ubuntu-22.04 alice work ") ||
		!strings.Contains(joined, "$task.State -ne 'Running'") {
		t.Fatalf("unexpected WSL delayed restart: name=%q args=%#v", detachedName, detachedArgs)
	}
}

func TestScheduleDelayedTeamsServiceStartUsesConfiguredPowerShell(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	configuredPowerShell := "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "codex-proxy"),
		cwd:                  tmp,
		windowsTaskDir:       filepath.Join(tmp, "wsl-task"),
		isWSL:                true,
		wslDistro:            "Ubuntu-22.04",
		wslLinuxUser:         "alice",
		powerShellExecutable: configuredPowerShell,
		runner:               runner,
	})
	prevDetached := teamsServiceStartDetached
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })

	if err := scheduleDelayedTeamsServiceStart(context.Background(), ""); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStart error: %v", err)
	}
	if detachedName != configuredPowerShell || !strings.Contains(strings.Join(detachedArgs, " "), "Start-ScheduledTask") {
		t.Fatalf("unexpected delayed restart command: name=%q args=%#v", detachedName, detachedArgs)
	}
}

func TestScheduleDelayedTeamsServiceStartUsesWindowsPendingActivation(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	pending := filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.123")
	if err := os.WriteFile(pending, []byte("new"), 0o600); err != nil {
		t.Fatalf("write pending helper: %v", err)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		windowsTaskDir: filepath.Join(tmp, "tasks"),
	})
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}

	if err := scheduleDelayedTeamsServiceStartAfterUpgrade(context.Background(), nil, teamsUpgradeServiceRefreshResult{}, pending, filepath.Join(tmp, "codex-proxy.exe")); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStartAfterUpgrade error: %v", err)
	}
	if detachedName != "powershell.exe" {
		t.Fatalf("detached name = %q, want powershell.exe", detachedName)
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		filepath.Join(tmp, "codex-proxy.exe"),
		teamsServiceWindowsWatchdogTaskName,
		teamsServiceWindowsTaskName,
		"$want='1.2.3'",
		"Move-Item -Force",
		"if (Test-DestVersion) { $ready=$true }",
		"if ($ready) { foreach",
		"Start-ScheduledTask",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("windows activation command missing %q:\n%s", want, joined)
		}
	}
}

func TestRestartTeamsHelperFromTeamsUsesPendingActivationForWindowsService(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	pending := filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.123")
	if err := os.WriteFile(pending, []byte("new"), 0o600); err != nil {
		t.Fatalf("write pending helper: %v", err)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		windowsTaskDir: filepath.Join(tmp, "tasks"),
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	prevDetached := teamsServiceStartDetached
	prevExit := exitFunc
	prevStart := startSelf
	t.Cleanup(func() {
		teamsServiceStartDetached = prevDetached
		exitFunc = prevExit
		startSelf = prevStart
	})
	var detachedName string
	var detachedArgs []string
	var exitCode *int
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}
	exitFunc = func(code int) {
		exitCode = &code
	}
	startSelf = func(string, []string) error {
		t.Fatal("pending Windows service restart must not start the old helper entry directly")
		return nil
	}

	if err := restartTeamsHelperFromTeamsAfterPendingReplacement(context.Background(), pending, filepath.Join(tmp, "codex-proxy.exe")); err != nil {
		t.Fatalf("restartTeamsHelperFromTeamsAfterPendingReplacement error: %v", err)
	}
	if exitCode == nil || *exitCode != 0 {
		t.Fatalf("exitCode = %v, want 0", exitCode)
	}
	if detachedName != "powershell.exe" {
		t.Fatalf("detached name = %q, want powershell.exe", detachedName)
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		filepath.Join(tmp, "codex-proxy.exe"),
		"$want='1.2.3'",
		"Stop-ScheduledTask",
		"Move-Item -Force",
		"if (Test-DestVersion) { $ready=$true }",
		"if ($ready) { foreach",
		"Start-ScheduledTask",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("pending activation restart command missing %q:\n%s", want, joined)
		}
	}
}

func TestRestartTeamsHelperFromTeamsUsesPendingProcessRestartForWindowsManualRun(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	pending := filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.123")
	if err := os.WriteFile(pending, []byte("new"), 0o600); err != nil {
		t.Fatalf("write pending helper: %v", err)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		windowsTaskDir: filepath.Join(tmp, "tasks"),
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")
	prevDetached := teamsServiceStartDetached
	prevExit := exitFunc
	prevStart := startSelf
	prevArgs := os.Args
	t.Cleanup(func() {
		teamsServiceStartDetached = prevDetached
		exitFunc = prevExit
		startSelf = prevStart
		os.Args = prevArgs
	})
	os.Args = []string{filepath.Join(tmp, "codex-proxy.exe"), "teams", "run", "--auto-service=false"}
	var detachedName string
	var detachedArgs []string
	var exitCode *int
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}
	exitFunc = func(code int) {
		exitCode = &code
	}
	startSelf = func(string, []string) error {
		t.Fatal("pending Windows manual restart must not start the old helper entry directly")
		return nil
	}

	if err := restartTeamsHelperFromTeamsAfterPendingReplacement(context.Background(), pending, filepath.Join(tmp, "codex-proxy.exe")); err != nil {
		t.Fatalf("restartTeamsHelperFromTeamsAfterPendingReplacement error: %v", err)
	}
	if exitCode == nil || *exitCode != 0 {
		t.Fatalf("exitCode = %v, want 0", exitCode)
	}
	if detachedName != "powershell.exe" {
		t.Fatalf("detached name = %q, want powershell.exe", detachedName)
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		filepath.Join(tmp, "codex-proxy.exe"),
		"$want='1.2.3'",
		"Move-Item -Force",
		"if (Test-DestVersion) { $ready=$true }",
		"if ($ready) { try { Start-Process",
		"Start-Process -FilePath $dest",
		"'teams'",
		"'run'",
		"'--auto-service=false'",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("pending process restart command missing %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "Start-ScheduledTask") {
		t.Fatalf("manual pending restart should not start scheduled tasks:\n%s", joined)
	}
}

func TestScheduleDelayedTeamsStartupFallbackStartUsesWSLStartupCommand(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
	})
	prevDetached := teamsServiceStartDetached
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string{}, args...)
		return nil
	}
	t.Cleanup(func() { teamsServiceStartDetached = prevDetached })

	if err := scheduleDelayedTeamsStartupFallbackStart(context.Background(), nil); err != nil {
		t.Fatalf("scheduleDelayedTeamsStartupFallbackStart error: %v", err)
	}
	joined := strings.Join(detachedArgs, " ")
	if detachedName != "powershell.exe" ||
		!strings.Contains(joined, "Start-Sleep -Seconds 3") ||
		!strings.Contains(joined, "Start-Process -FilePath 'wscript.exe'") ||
		strings.Contains(joined, "Start-ScheduledTask") {
		t.Fatalf("unexpected delayed Startup fallback command: name=%q args=%#v", detachedName, detachedArgs)
	}
}

func TestUpgradeCmdStopsActiveTeamsServiceWithoutStateFile(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte("unit"), 0o600); err != nil {
		t.Fatalf("write unit file: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	prevCheck := checkForUpdate
	prevPerform := performUpdate
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
	})
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		if !teamsServiceCallSeen(runner.calls, "stop") {
			t.Fatalf("Teams service should be stopped before performUpdate, calls=%#v", runner.calls)
		}
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	if !teamsServiceCallSeen(runner.calls, "is-active") || !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "start") {
		t.Fatalf("expected is-active, stop, and start calls, got %#v", runner.calls)
	}
}

func TestUpgradeCmdPreservesExistingTeamsDrain(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})
	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status {
		t.Fatal("CheckForUpdate should not run for explicit versions")
		return update.Status{}
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{Version: "1.2.3"}, nil
	}

	st := seedLiveTeamsOwnerForUpgradeTest(t)
	if _, err := st.SetDraining(context.Background(), "maintenance"); err != nil {
		t.Fatalf("SetDraining error: %v", err)
	}
	go func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			control, err := st.ReadControl(context.Background())
			if err == nil && control.Draining {
				_ = st.ClearOwner(context.Background())
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1s"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v\n%s", err, out.String())
	}
	control, err := st.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if !control.Draining || control.Reason != "maintenance" {
		t.Fatalf("existing drain should be preserved, got %#v", control)
	}
}

func TestUpgradeCmdRestoresTeamsDrainOnTimeout(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status { return update.Status{} }
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("performUpdate should not run before Teams drain")
		return update.ApplyResult{}, nil
	}

	st := seedLiveTeamsOwnerForUpgradeTest(t)
	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1ms"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for Teams bridge to drain") {
		t.Fatalf("expected drain timeout, got %v", err)
	}
	control, err := st.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if control.Draining {
		t.Fatalf("upgrade-owned drain should be restored after timeout: %#v", control)
	}
	upgrade, ok, err := st.ReadUpgrade(context.Background())
	if err != nil {
		t.Fatalf("ReadUpgrade error: %v", err)
	}
	if !ok || upgrade.Phase != teamsstore.UpgradePhaseAborted {
		t.Fatalf("upgrade state = %#v ok=%v, want aborted", upgrade, ok)
	}
}

func TestUpgradeCmdTimesOutBeforeUpdatingWhenTeamsOwnerStaysLive(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	prevCheck := checkForUpdate
	prevPerform := performUpdate
	prevPoll := teamsUpgradePollInterval
	t.Cleanup(func() {
		checkForUpdate = prevCheck
		performUpdate = prevPerform
		teamsUpgradePollInterval = prevPoll
	})
	teamsUpgradePollInterval = time.Millisecond
	checkForUpdate = func(context.Context, update.CheckOptions) update.Status { return update.Status{} }
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("performUpdate should not run before Teams drain")
		return update.ApplyResult{}, nil
	}
	st := seedLiveTeamsOwnerForUpgradeTest(t)

	cmd := newUpgradeCmd(&rootOptions{})
	cmd.SetArgs([]string{"--version", "v1.2.3", "--teams-drain-timeout", "1ms"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for Teams bridge to drain") {
		t.Fatalf("expected drain timeout, got %v", err)
	}
	control, err := st.ReadControl(context.Background())
	if err != nil {
		t.Fatalf("ReadControl error: %v", err)
	}
	if control.Draining {
		t.Fatalf("upgrade-owned drain should be restored after timeout: %#v", control)
	}
}

func seedLiveTeamsOwnerForUpgradeTest(t *testing.T) *teamsstore.Store {
	t.Helper()
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("openTeamsStore error: %v", err)
	}
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn-1", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	return st
}

func seedScopedLiveTeamsOwnerForUpgradeTest(t *testing.T, scopeID string) *teamsstore.Store {
	t.Helper()
	defaultPath, err := teamsStorePath()
	if err != nil {
		t.Fatalf("teamsStorePath error: %v", err)
	}
	path := filepath.Join(filepath.Dir(defaultPath), "scopes", scopeID, "state.json")
	st, err := teamsstore.Open(path)
	if err != nil {
		t.Fatalf("Open scoped store error: %v", err)
	}
	if _, err := st.RecordScope(context.Background(), teamsstore.ScopeIdentity{ID: scopeID, AccountID: "user-1", OSUser: "alice", Profile: "default"}); err != nil {
		t.Fatalf("RecordScope error: %v", err)
	}
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn-1", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	owner.ScopeID = scopeID
	owner.MachineID = "machine-1"
	owner.LeaseGeneration = 1
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	return st
}

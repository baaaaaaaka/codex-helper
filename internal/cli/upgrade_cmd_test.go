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
	if !strings.Contains(out.String(), "Updated to v1.2.3.") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestUpgradeCmdRestartRequiredMessage(t *testing.T) {
	lockCLITestHooks(t)
	isolateUpgradeTeamsServiceForTest(t)

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
		return update.ApplyResult{Version: "1.2.3", RestartRequired: true}, nil
	}

	cmd := newUpgradeCmd(&rootOptions{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute upgrade: %v", err)
	}
	if !strings.Contains(out.String(), "Update scheduled for v1.2.3. Please restart `codex-proxy`.") {
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
	isolateTeamsUserDirsForTest(t, tmp)
}

func TestUpgradeCmdDrainsLiveTeamsOwnerBeforeUpdate(t *testing.T) {
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
	if !strings.Contains(out.String(), "Stopping Teams service before upgrade") || !strings.Contains(out.String(), "Restarting Teams service after upgrade") {
		t.Fatalf("upgrade output missing service restart messages:\n%s", out.String())
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
	if !strings.Contains(out.String(), "Scheduling Teams service restart after the updated helper is ready") ||
		!strings.Contains(out.String(), "Update scheduled for v1.2.3. Please restart `codex-proxy`.") {
		t.Fatalf("upgrade output missing delayed restart/restart-required messages:\n%s", out.String())
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

	if err := scheduleDelayedTeamsServiceStart(context.Background()); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStart error: %v", err)
	}
	joined := strings.Join(detachedArgs, " ")
	if detachedName != "powershell.exe" ||
		!strings.Contains(joined, "Start-ScheduledTask") ||
		!strings.Contains(joined, "Codex Helper Teams Bridge (WSL Ubuntu-22.04 alice work ") {
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

	if err := scheduleDelayedTeamsServiceStart(context.Background()); err != nil {
		t.Fatalf("scheduleDelayedTeamsServiceStart error: %v", err)
	}
	if detachedName != configuredPowerShell || !strings.Contains(strings.Join(detachedArgs, " "), "Start-ScheduledTask") {
		t.Fatalf("unexpected delayed restart command: name=%q args=%#v", detachedName, detachedArgs)
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

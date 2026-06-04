package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func isolateTeamsUserDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	t.Setenv("APPDATA", filepath.Join(tmp, "AppData", "Roaming"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "AppData", "Local"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "chat-client")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "file-client")
	configBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("os.UserConfigDir: %v", err)
	}
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("os.UserCacheDir: %v", err)
	}
	return configBase, cacheBase
}

func setTeamsAuthIDsForCLITest(t *testing.T) {
	t.Helper()
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "chat-client")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "file-client")
}

func teamsServiceTestAbsPath(t *testing.T, path string) string {
	t.Helper()
	out, err := filepath.Abs(path)
	if err != nil {
		t.Fatalf("filepath.Abs(%q): %v", path, err)
	}
	return out
}

func teamsServiceTestRegistryPath(cwd string, registryPath string) string {
	registryPath = strings.TrimSpace(registryPath)
	if filepath.IsAbs(registryPath) {
		return registryPath
	}
	return filepath.Join(cwd, registryPath)
}

func assertTeamsServiceWindowsElevatedScriptCommand(t *testing.T, command string) {
	t.Helper()
	for _, want := range []string{
		"Start-Process",
		"-Verb RunAs",
		"-File",
		"codex-helper-teams-uac-",
		"elevated.ps1",
		"stdout.log",
		"stderr.log",
		"elevated Teams service bootstrap failed with exit code",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("elevated launcher missing %q:\n%s", want, command)
		}
	}
	for _, forbidden := range []string{"Register-ScheduledTask", "Start-ScheduledTask", "Enable-ScheduledTask"} {
		if strings.Contains(command, forbidden) {
			t.Fatalf("elevated launcher should execute a script file instead of inline %q:\n%s", forbidden, command)
		}
	}
}

func assertTeamsServiceWindowsElevatedScriptForRepair(t *testing.T, script string, expectEnable bool, expectStart bool) {
	t.Helper()
	for _, want := range []string{
		"$ErrorActionPreference = 'Stop'",
		"$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity",
		"Register-ScheduledTask",
		teamsServiceWindowsTaskName,
		teamsServiceWindowsWatchdogTaskName,
		"exit 1",
		"exit 0",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("elevated script missing %q:\n%s", want, script)
		}
	}
	if expectEnable && !strings.Contains(script, "Enable-ScheduledTask") {
		t.Fatalf("elevated script should enable repaired tasks:\n%s", script)
	}
	if !expectEnable && strings.Contains(script, "Enable-ScheduledTask") {
		t.Fatalf("elevated script should not enable install-only tasks:\n%s", script)
	}
	if expectStart && !strings.Contains(script, "Start-ScheduledTask") {
		t.Fatalf("elevated script should start repaired tasks:\n%s", script)
	}
	if !expectStart {
		for _, forbidden := range []string{"Start-ScheduledTask", "Start-CodexHelperScheduledTaskIfStopped"} {
			if strings.Contains(script, forbidden) {
				t.Fatalf("elevated script should not start tasks before returning to current-user context, found %q:\n%s", forbidden, script)
			}
		}
	}
	for _, forbidden := range []string{"RunLevel Highest", "HighestAvailable", "NT AUTHORITY\\SYSTEM", "-UserId 'SYSTEM'", "LogonType Password"} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("elevated script must stay current-user least-privilege, found %q:\n%s", forbidden, script)
		}
	}
}

func assertTeamsServiceWindowsElevatedScriptFile(t *testing.T, taskDir string, expectEnable bool, expectStart bool) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(taskDir, "codex-helper-teams-uac-*", "elevated.ps1"))
	if err != nil {
		t.Fatalf("glob elevated script: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("elevated script matches = %#v, want exactly one", matches)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatalf("read elevated script %s: %v", matches[0], err)
	}
	assertTeamsServiceWindowsElevatedScriptForRepair(t, string(data), expectEnable, expectStart)
}

func TestConfirmTeamsServiceUACPromptRejectsTeamsServiceModeWithoutReading(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")

	var out strings.Builder
	if confirmTeamsServiceUACPrompt(strings.NewReader("yes\n"), &out, false) {
		t.Fatal("Teams service mode must not accept an interactive UAC confirmation")
	}
	if !strings.Contains(out.String(), "stdin is non-interactive") {
		t.Fatalf("expected non-interactive explanation, got:\n%s", out.String())
	}
}

func TestConfirmTeamsServiceUACPromptRejectsNonTerminalStdinWithoutBlocking(t *testing.T) {
	lockCLITestHooks(t)
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")

	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	prevStdin := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() {
		os.Stdin = prevStdin
		_ = reader.Close()
		_ = writer.Close()
	})

	done := make(chan bool, 1)
	var out strings.Builder
	go func() {
		done <- confirmTeamsServiceUACPrompt(nil, &out, false)
	}()
	select {
	case confirmed := <-done:
		if confirmed {
			t.Fatal("non-terminal stdin must not confirm UAC")
		}
	case <-time.After(time.Second):
		t.Fatal("UAC confirmation blocked on non-terminal stdin")
	}
	if !strings.Contains(out.String(), "stdin is non-interactive") {
		t.Fatalf("expected non-interactive explanation, got:\n%s", out.String())
	}
}

func TestTeamsServiceInstallWritesSystemdUserUnitWithoutEnabling(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd user")
	exePath := filepath.Join(tmp, "bin", "codex proxy")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	unitPath := filepath.Join(unitDir, teamsServiceUnitName)
	data, err := os.ReadFile(unitPath)
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"[Unit]",
		"Description=Codex Helper Teams bridge",
		"WorkingDirectory=" + strconv.Quote(cwd),
		"ExecStart=" + systemdQuoteArg(exePath) + " teams run --owner-stale-after 1m30s --auto-service=false --registry " + strconv.Quote(registryPath),
		"Restart=on-failure",
		"RestartSec=10s",
		"Environment=NO_COLOR=1",
		"Environment=" + systemdQuoteArg(update.EnvInstallDir+"="+exePath),
		"Environment=CODEX_HELPER_TEAMS_SERVICE=1",
		"[Install]",
		"WantedBy=default.target",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}

	wantCalls := []teamsServiceCommandCall{{
		name: "systemctl",
		args: []string{"--user", "daemon-reload"},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", runner.calls, wantCalls)
	}
	for _, call := range runner.calls {
		joined := strings.Join(call.args, " ")
		if strings.Contains(joined, "enable") || strings.Contains(joined, "start") {
			t.Fatalf("install should not enable or start service, calls=%#v", runner.calls)
		}
	}
	if !strings.Contains(out.String(), "not enabled or started automatically") {
		t.Fatalf("install output should state no auto enable/start:\n%s", out.String())
	}
}

func TestTeamsServiceLinuxAutoFallsBackToLocalSupervisorWhenSystemdUserUnavailable(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "local-supervisor" {
		t.Fatalf("backend ID = %q, want local-supervisor", got)
	}
}

func TestTeamsServiceLinuxAutoFailsClosedOnUnknownSystemdProbeError(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                    "linux",
		exe:                     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                     tmp,
		unitDir:                 filepath.Join(tmp, "systemd", "user"),
		runner:                  &recordingTeamsServiceRunner{},
		systemdUserAvailableErr: errors.New("systemctl --user show-environment failed: exit status 1"),
	})

	_, err := teamsServiceBackendForCurrentPlatform()
	if err == nil || !strings.Contains(err.Error(), "verify systemd --user availability") {
		t.Fatalf("teamsServiceBackendForCurrentPlatform err = %v, want fail-closed systemd probe error", err)
	}
}

func TestTeamsServiceSystemdUserUnavailableErrorMatrix(t *testing.T) {
	unavailable := []string{
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: No such file or directory",
		"systemctl --user show-environment failed: exit status 1: Failed to get D-Bus connection: Operation not permitted",
		"systemctl --user show-environment failed: exit status 1: Failed to get environment: Process org.freedesktop.systemd1 exited with status 1",
		"systemctl --user show-environment failed: exit status 1: org.freedesktop.DBus.Error.NameHasNoOwner",
		"systemctl --user show-environment failed: exit status 1: The name org.freedesktop.systemd1 was not provided by any .service files",
		"systemctl --user show-environment failed: exit status 1: Unit dbus-org.freedesktop.systemd1.service not found",
		"systemctl --user show-environment failed: exit status 1: Cannot autolaunch D-Bus without X11 $DISPLAY",
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: No medium found",
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: Host is down",
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: Connection refused",
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: Transport endpoint is not connected",
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: $XDG_RUNTIME_DIR not set",
		"systemctl --user show-environment failed: context deadline exceeded",
		"systemctl --user show-environment failed: signal: killed",
		"systemctl --user show-environment failed: exit status 1: System has not been booted with systemd as init system",
		"systemctl --user show-environment failed: exec: \"systemctl\": executable file not found in $PATH",
	}
	for _, sample := range unavailable {
		if !teamsServiceSystemdUserUnavailableError(errors.New(sample)) {
			t.Fatalf("systemd unavailable sample was not classified for fallback:\n%s", sample)
		}
	}

	unknown := []string{
		"systemctl --user show-environment failed: exit status 1",
		"systemctl --user show-environment failed: exit status 1: invalid option --definitely-not-real",
		"systemctl --user show-environment failed: exit status 1: malformed unit file",
	}
	for _, sample := range unknown {
		if teamsServiceSystemdUserUnavailableError(errors.New(sample)) {
			t.Fatalf("unknown systemd sample was incorrectly classified for fallback:\n%s", sample)
		}
	}
}

func TestTeamsServiceLinuxAutoFallsBackForKnownSystemdProbeFailures(t *testing.T) {
	lockCLITestHooks(t)

	samples := []string{
		"systemctl --user show-environment failed: exit status 1: Failed to get environment: Process org.freedesktop.systemd1 exited with status 1",
		"systemctl --user show-environment failed: exit status 1: Failed to connect to bus: Connection refused",
		"systemctl --user show-environment failed: exit status 1: Cannot autolaunch D-Bus without X11 $DISPLAY",
		"systemctl --user show-environment failed: context deadline exceeded",
	}
	for _, sample := range samples {
		t.Run(sample, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:                    "linux",
				exe:                     filepath.Join(tmp, "bin", "codex-proxy"),
				cwd:                     tmp,
				unitDir:                 filepath.Join(tmp, "systemd", "user"),
				runner:                  &recordingTeamsServiceRunner{},
				systemdUserAvailableErr: errors.New(sample),
			})

			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
			}
			if got := backend.ID(); got != "local-supervisor" {
				t.Fatalf("backend ID = %q, want local-supervisor", got)
			}
		})
	}
}

func TestTeamsServiceLinuxAutoUsesSystemdWhenOnlyDisabledLocalConfigExists(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	systemdAvailable := true
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
	})
	path, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(path, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: false,
		Spec:    teamsServiceSpec{Executable: filepath.Join(tmp, "bin", "codex-proxy"), WorkingDir: tmp},
	}); err != nil {
		t.Fatalf("write disabled local config: %v", err)
	}

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "systemd-user" {
		t.Fatalf("backend ID = %q, want systemd-user", got)
	}
}

func TestTeamsServiceLinuxAutoStickyCheckDoesNotCreateLocalSupervisorRuntimeDir(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	systemdAvailable := true
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
	})

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "systemd-user" {
		t.Fatalf("backend ID = %q, want systemd-user", got)
	}
	runDir := filepath.Join(configBase, "codex-helper", "teams", "run")
	if _, err := os.Stat(runDir); !os.IsNotExist(err) {
		t.Fatalf("sticky check created runtime dir %s: %v", runDir, err)
	}
}

func TestTeamsServiceLinuxAutoKeepsEnabledLocalSupervisorConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	systemdAvailable := true
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
	})
	path, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(path, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    teamsServiceSpec{Executable: filepath.Join(tmp, "bin", "codex-proxy"), WorkingDir: tmp},
	}); err != nil {
		t.Fatalf("write enabled local config: %v", err)
	}

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "local-supervisor" {
		t.Fatalf("backend ID = %q, want local-supervisor", got)
	}
}

func TestTeamsServiceLinuxAutoIgnoresStaleLocalSupervisorStatusIdentityMismatch(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	systemdAvailable := true
	identityErr := errors.New("not the local supervisor")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
		localVerifyProcessIdentity: func(int, string) error {
			return identityErr
		},
	})
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: false,
		Spec:    teamsServiceSpec{Executable: filepath.Join(tmp, "bin", "codex-proxy"), WorkingDir: tmp},
	}); err != nil {
		t.Fatalf("write disabled local config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: os.Getpid(),
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write stale local status: %v", err)
	}

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "systemd-user" {
		t.Fatalf("backend ID = %q, want systemd-user", got)
	}
}

func TestTeamsServiceWSLExplicitSystemdDoesNotFallBackToLocalSupervisor(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND", "systemd")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		isWSL:                true,
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "systemd-user" {
		t.Fatalf("backend ID = %q, want systemd-user", got)
	}
}

func TestTeamsServiceWSLExplicitLocalSupervisorOptIn(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND", "local-supervisor")
	systemdAvailable := true
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		isWSL:                true,
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
	})

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "local-supervisor" {
		t.Fatalf("backend ID = %q, want local-supervisor", got)
	}
}

func TestTeamsServiceWSLHonorsLinuxLocalSupervisorWhenWSLModeUnset(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	systemdAvailable := true
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		isWSL:                true,
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
	})
	t.Setenv("CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND", "local-supervisor")

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "local-supervisor" {
		t.Fatalf("backend ID = %q, want local-supervisor", got)
	}
}

func TestTeamsServiceWSLSpecificModeWinsOverLinuxMode(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND", "systemd")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		isWSL:                true,
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})
	t.Setenv("CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND", "local-supervisor")

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("teamsServiceBackendForCurrentPlatform error: %v", err)
	}
	if got := backend.ID(); got != "systemd-user" {
		t.Fatalf("backend ID = %q, want systemd-user", got)
	}
}

func TestTeamsServiceWSLAutoKeepsEnabledLocalSupervisorConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         &recordingTeamsServiceRunner{},
	})
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    teamsServiceSpec{Executable: filepath.Join(tmp, "bin", "codex-proxy"), WorkingDir: tmp},
	}); err != nil {
		t.Fatalf("write local config: %v", err)
	}

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	if got := backend.ID(); got != "local-supervisor" {
		t.Fatalf("backend ID = %q, want local-supervisor sticky in WSL auto", got)
	}
}

func TestTeamsServiceWSLExplicitWindowsTaskOverridesLocalSupervisorSticky(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND", "windows-task")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         &recordingTeamsServiceRunner{},
	})
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    teamsServiceSpec{Executable: filepath.Join(tmp, "bin", "codex-proxy"), WorkingDir: tmp},
	}); err != nil {
		t.Fatalf("write local config: %v", err)
	}

	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	if got := backend.ID(); got != "wsl-windows-task-scheduler" {
		t.Fatalf("backend ID = %q, want explicit Windows Task backend", got)
	}
}

func TestTeamsServiceWSLInvalidSpecificModeFailsClosed(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND", "typo")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		isWSL:   true,
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})

	_, err := teamsServiceBackendForCurrentPlatform()
	if err == nil {
		t.Fatal("teamsServiceBackendForCurrentPlatform error = nil, want unsupported WSL backend error")
	}
	if got := err.Error(); !strings.Contains(got, "unsupported WSL Teams service backend") ||
		!strings.Contains(got, "CODEX_HELPER_TEAMS_WSL_SERVICE_BACKEND") {
		t.Fatalf("error = %v, want WSL backend env error", err)
	}
}

func TestTeamsServiceWSLInvalidLinuxModeFailsClosed(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		isWSL:   true,
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})
	t.Setenv("CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND", "typo")

	_, err := teamsServiceBackendForCurrentPlatform()
	if err == nil {
		t.Fatal("teamsServiceBackendForCurrentPlatform error = nil, want unsupported Linux backend error")
	}
	if got := err.Error(); !strings.Contains(got, "unsupported WSL Teams service backend") ||
		!strings.Contains(got, "CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND") {
		t.Fatalf("error = %v, want Linux backend env error", err)
	}
}

func TestTeamsServiceInstallWritesLocalSupervisorConfigWhenSystemdUserUnavailable(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work")
	registryPath := filepath.Join(tmp, "teams-registry.json")
	systemdUnavailable := false
	runner := &recordingTeamsServiceRunner{}
	systemdUnitDir := filepath.Join(tmp, "systemd", "user")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  cwd,
		unitDir:              systemdUnitDir,
		runner:               runner,
		systemdUserAvailable: &systemdUnavailable,
	})
	for _, name := range []string{teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName} {
		if err := os.MkdirAll(systemdUnitDir, 0o700); err != nil {
			t.Fatalf("mkdir systemd unit dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(systemdUnitDir, name), []byte("stale systemd unit"), 0o600); err != nil {
			t.Fatalf("write stale systemd unit: %v", err)
		}
	}

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	configPath := filepath.Join(configBase, "codex-helper", "teams", teamsServiceLocalSupervisorConfigName)
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read local supervisor config: %v", err)
	}
	var cfg teamsServiceLocalSupervisorConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("unmarshal local supervisor config: %v\n%s", err, data)
	}
	if cfg.Enabled {
		t.Fatal("install should write local supervisor config disabled")
	}
	if cfg.Spec.Executable != exePath {
		t.Fatalf("config executable = %q, want %q", cfg.Spec.Executable, exePath)
	}
	if cfg.Spec.WorkingDir != cwd {
		t.Fatalf("config working dir = %q, want %q", cfg.Spec.WorkingDir, cwd)
	}
	if cfg.Spec.RegistryPath != registryPath {
		t.Fatalf("config registry path = %q, want %q", cfg.Spec.RegistryPath, registryPath)
	}
	if cfg.Spec.Environment["CODEX_HELPER_TEAMS_SERVICE"] != "1" {
		t.Fatalf("config environment missing Teams service marker: %#v", cfg.Spec.Environment)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("local supervisor install should not call systemctl, calls=%#v", runner.calls)
	}
	for _, name := range []string{teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName} {
		if _, err := os.Stat(filepath.Join(systemdUnitDir, name)); err != nil {
			t.Fatalf("install should not retire stale systemd unit %s before start, stat err=%v", name, err)
		}
	}
	if got := out.String(); !strings.Contains(got, configPath) || !strings.Contains(got, "not enabled or started automatically") {
		t.Fatalf("install output missing local config path or no-start text:\n%s", got)
	}
}

func TestTeamsServiceLocalSupervisorRetireSystemdFailureBlocksStart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	retireErr := errors.New("systemd stop failed")
	runner := &recordingTeamsServiceRunner{err: retireErr}
	systemdAvailable := true
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               runner,
		systemdUserAvailable: &systemdAvailable,
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})

	path, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp})
	if err != nil {
		t.Fatalf("install should only write local supervisor config before retiring systemd: %v", err)
	}
	if strings.TrimSpace(path) == "" {
		t.Fatalf("install path is empty")
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err == nil || !strings.Contains(err.Error(), "stop old systemd") {
		t.Fatalf("start err = %v, want systemd retire failure", err)
	}
	if started {
		t.Fatal("local supervisor started after systemd retire failure")
	}
}

func TestTeamsServiceLocalSupervisorRetireSystemdUnavailableAllowsStart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	startedPID := 6124
	runner := &recordingTeamsServiceRunner{
		output: []byte("Failed to connect to bus: Connection refused\n"),
		err:    errors.New("exit status 1"),
	}
	systemdAvailable := true
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               runner,
		systemdUserAvailable: &systemdAvailable,
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			started = true
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: 7124,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == startedPID }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
		t.Fatalf("install should only write local supervisor config before retiring systemd: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err != nil {
		t.Fatalf("start should allow local-supervisor when systemd disappears during retire: %v", err)
	}
	if !started {
		t.Fatal("local supervisor did not start after systemd became unavailable during retire")
	}
	if !teamsServiceCallSeen(runner.calls, "stop") || !teamsServiceCallSeen(runner.calls, "disable") {
		t.Fatalf("retire calls = %#v, want stop and disable attempted before fallback start", runner.calls)
	}
}

func TestTeamsServiceLocalSupervisorRetireSystemdTimeoutBlocksStart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	runner := &recordingTeamsServiceRunner{err: context.DeadlineExceeded}
	systemdAvailable := true
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               runner,
		systemdUserAvailable: &systemdAvailable,
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
		t.Fatalf("install should only write local supervisor config before retiring systemd: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err == nil || !strings.Contains(err.Error(), "stop old systemd") {
		t.Fatalf("start err = %v, want fail-closed systemd stop timeout", err)
	}
	if started {
		t.Fatal("local supervisor started after systemd stop timeout")
	}
}

func TestTeamsServiceLocalSupervisorSystemdProbeUnknownBlocksFallbackStart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                    "linux",
		exe:                     exePath,
		cwd:                     tmp,
		unitDir:                 filepath.Join(tmp, "systemd", "user"),
		runner:                  &recordingTeamsServiceRunner{},
		systemdUserAvailableErr: errors.New("systemctl --user show-environment failed: exit status 1"),
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor config: %v", err)
	}
	_, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start")
	if err == nil || !strings.Contains(err.Error(), "verify systemd --user is unavailable") {
		t.Fatalf("start err = %v, want fail-closed systemd probe error", err)
	}
	if started {
		t.Fatal("local supervisor started after unknown systemd probe failure")
	}
}

func TestTeamsServiceLocalSupervisorSystemdUnavailableProbeAllowsFallbackStart(t *testing.T) {
	lockCLITestHooks(t)

	tests := []struct {
		name string
		err  string
	}{
		{name: "dbus permission denied", err: "systemctl --user show-environment failed: exit status 1: Failed to get D-Bus connection: Operation not permitted"},
		{name: "environment probe exited", err: "systemctl --user show-environment failed: exit status 1: Failed to get environment: Process org.freedesktop.systemd1 exited with status 1"},
		{name: "bus connection refused", err: "systemctl --user show-environment failed: exit status 1: Failed to connect to bus: Connection refused"},
		{name: "dbus cannot autolaunch", err: "systemctl --user show-environment failed: exit status 1: Cannot autolaunch D-Bus without X11 $DISPLAY"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			exePath := filepath.Join(tmp, "bin", "codex-proxy")
			startedPID := 6123
			started := false
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:                    "linux",
				exe:                     exePath,
				cwd:                     tmp,
				unitDir:                 filepath.Join(tmp, "systemd", "user"),
				runner:                  &recordingTeamsServiceRunner{},
				systemdUserAvailableErr: errors.New(tc.err),
				localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
					started = true
					return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
						Version:        teamsServiceLocalSupervisorStatusVersion,
						ConfigPath:     configPath,
						SupervisorPID:  startedPID,
						SupervisorPGID: 7123,
						State:          "running",
						UpdatedAt:      time.Now(),
					})
				},
				localVerifyProcessIdentity: func(int, string) error {
					return nil
				},
			})
			prevAlive := teamsLocalSupervisorProcessAlive
			teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == startedPID }
			t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })
			if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
				t.Fatalf("install local supervisor config: %v", err)
			}
			if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err != nil {
				t.Fatalf("start should allow confirmed unavailable systemd probe: %v", err)
			}
			if !started {
				t.Fatal("local supervisor did not start after confirmed unavailable systemd probe")
			}
		})
	}
}

func TestTeamsServiceLocalSupervisorStartPreflightFailureDoesNotRetireSystemd(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	runner := &recordingTeamsServiceRunner{}
	systemdAvailable := true
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               runner,
		systemdUserAvailable: &systemdAvailable,
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor config: %v", err)
	}
	badCacheHome := filepath.Join(tmp, "cache-file")
	if err := os.WriteFile(badCacheHome, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write bad cache home: %v", err)
	}
	badHome := filepath.Join(tmp, "home-file")
	if err := os.WriteFile(badHome, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("write bad home: %v", err)
	}
	t.Setenv("HOME", badHome)
	t.Setenv("XDG_CACHE_HOME", badCacheHome)
	t.Setenv("LOCALAPPDATA", badCacheHome)
	_, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start")
	if err == nil {
		t.Fatal("start err = nil, want log preflight failure")
	}
	if started {
		t.Fatal("local supervisor started after log preflight failure")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("start should not retire systemd before local preflight succeeds, calls=%#v", runner.calls)
	}
}

func TestTeamsServiceLocalSupervisorStartRetiresExistingLinuxBridgeProcess(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	startedPID := 6201
	var terminated []int
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: 7201,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
	})
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 6301, Args: []string{exePath, "teams", "run", "--registry", filepath.Join(tmp, "registry.json")}, Env: map[string]string{}},
			{PID: 6302, Args: []string{exePath, "teams", "service", "watchdog", "--loop"}, Env: map[string]string{}},
		}, nil
	}
	teamsServiceTerminateLocalProcess = func(pid int, _ time.Duration) error {
		terminated = append(terminated, pid)
		return nil
	}
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == startedPID }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable:   exePath,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "registry.json"),
	}); err != nil {
		t.Fatalf("install local supervisor config: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err != nil {
		t.Fatalf("start local supervisor: %v", err)
	}
	if !reflect.DeepEqual(terminated, []int{6301}) {
		t.Fatalf("terminated = %#v, want only matching direct teams run", terminated)
	}
}

func TestTeamsServiceLocalSupervisorWSLStartRetiresScheduledTasks(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	startedPID := 6401
	var events []string
	runner := teamsServiceCommandRunnerFunc(func(_ context.Context, name string, args ...string) ([]byte, error) {
		events = append(events, name+" "+strings.Join(args, " "))
		return nil, nil
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            exePath,
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			events = append(events, "start-local")
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: 7401,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == startedPID }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor config: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err != nil {
		t.Fatalf("start local supervisor: %v", err)
	}
	if len(events) < 2 || !strings.Contains(events[0], "Disable-ScheduledTask") || events[1] != "start-local" {
		t.Fatalf("events = %#v, want WSL Scheduled Task retire before local start", events)
	}
}

func TestTeamsServiceLocalSupervisorEnableStartStatus(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	var startedConfigPath string
	var startedLogPath string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localStartDetached: func(_ context.Context, configPath string, logPath string, _ teamsServiceSpec) (int, error) {
			startedConfigPath = configPath
			startedLogPath = logPath
			return os.Getpid(), writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				LogPath:        logPath,
				SupervisorPID:  os.Getpid(),
				SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID(),
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}

	cmd = newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"enable"})
	var enableOut bytes.Buffer
	cmd.SetOut(&enableOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("enable local supervisor: %v", err)
	}
	if !strings.Contains(enableOut.String(), "does not provide machine/container reboot autostart") {
		t.Fatalf("enable output missing local fallback limitation:\n%s", enableOut.String())
	}

	cmd = newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"start"})
	var startOut bytes.Buffer
	cmd.SetOut(&startOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("start local supervisor: %v", err)
	}
	if startedConfigPath == "" || startedLogPath == "" {
		t.Fatalf("start hook did not receive config/log paths: config=%q log=%q", startedConfigPath, startedLogPath)
	}
	if !strings.Contains(startOut.String(), "Started Teams service: Codex Helper Teams local supervisor") {
		t.Fatalf("start output missing backend name:\n%s", startOut.String())
	}

	cmd = newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"status"})
	var statusOut bytes.Buffer
	cmd.SetOut(&statusOut)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("status local supervisor: %v", err)
	}
	for _, want := range []string{
		"Active: true",
		"SupervisorPID:",
		"Autostart: not guaranteed after machine/container reboot",
		filepath.Join(configBase, "codex-helper", "teams", "run", "local-supervisor", teamsServiceLocalSupervisorStatusName),
	} {
		if !strings.Contains(statusOut.String(), want) {
			t.Fatalf("status output missing %q:\n%s", want, statusOut.String())
		}
	}
}

func TestTeamsServiceLocalSupervisorRepairRestartsActiveForNewSpec(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	stops := 0
	starts := 0
	activePID := 1111
	activePGID := 2111
	startedPID := 2222
	startedPGID := 3222
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			starts++
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: startedPGID,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevCurrentProcessGroupID := teamsLocalSupervisorCurrentProcessGroupID
	prevTerminate := teamsLocalSupervisorTerminateProcessGroup
	teamsLocalSupervisorProcessAlive = func(pid int) bool {
		return pid == activePID || pid == startedPID
	}
	teamsLocalSupervisorProcessGroupID = func(pid int) (int, error) {
		switch pid {
		case activePID:
			return activePGID, nil
		case startedPID:
			return startedPGID, nil
		default:
			return 0, fmt.Errorf("unexpected pid %d", pid)
		}
	}
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return 999 }
	teamsLocalSupervisorTerminateProcessGroup = func(int, int, time.Duration) error {
		stops++
		return nil
	}
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorCurrentProcessGroupID = prevCurrentProcessGroupID
		teamsLocalSupervisorTerminateProcessGroup = prevTerminate
	})

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    teamsServiceSpec{Executable: exePath, WorkingDir: tmp, RegistryPath: "old"},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  activePID,
		SupervisorPGID: activePGID,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}

	path, err := (teamsServiceLocalSupervisorBackend{}).Repair(context.Background(), teamsServiceSpec{
		Executable:   exePath,
		WorkingDir:   tmp,
		RegistryPath: "new",
		Environment:  map[string]string{"CODEX_HELPER_TEAMS_SERVICE": "1"},
	}, teamsServiceRepairOptions{Enable: true, Start: true})
	if err != nil {
		t.Fatalf("repair local supervisor: %v", err)
	}
	if path != configPath {
		t.Fatalf("repair path = %q, want %q", path, configPath)
	}
	if stops != 1 || starts != 1 {
		t.Fatalf("repair stops=%d starts=%d, want one restart", stops, starts)
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !cfg.Enabled || cfg.Spec.RegistryPath != "new" {
		t.Fatalf("config after repair = %#v, want enabled new spec", cfg)
	}
}

func TestTeamsServiceLocalSupervisorRepairStopsChildWithOldSpecBeforeWritingNewConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	activePID := 3101
	activePGID := 4101
	childPID := 3102
	childPGID := 4102
	startedPID := 3201
	startedPGID := 4201
	var verifiedChildRegistry string
	var terminated []int
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
		localVerifyChildIdentity: func(pid int, spec teamsServiceSpec) error {
			if pid != childPID {
				t.Fatalf("verified child pid = %d, want %d", pid, childPID)
			}
			verifiedChildRegistry = spec.RegistryPath
			return nil
		},
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: startedPGID,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevCurrentProcessGroupID := teamsLocalSupervisorCurrentProcessGroupID
	prevTerminate := teamsLocalSupervisorTerminateProcessGroup
	teamsLocalSupervisorProcessAlive = func(pid int) bool {
		return pid == activePID || pid == childPID || pid == startedPID
	}
	teamsLocalSupervisorProcessGroupID = func(pid int) (int, error) {
		switch pid {
		case activePID:
			return activePGID, nil
		case childPID:
			return childPGID, nil
		case startedPID:
			return startedPGID, nil
		default:
			return 0, fmt.Errorf("unexpected pid %d", pid)
		}
	}
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return 999 }
	teamsLocalSupervisorTerminateProcessGroup = func(_ int, pid int, _ time.Duration) error {
		terminated = append(terminated, pid)
		return nil
	}
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorCurrentProcessGroupID = prevCurrentProcessGroupID
		teamsLocalSupervisorTerminateProcessGroup = prevTerminate
	})

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    teamsServiceSpec{Executable: exePath, WorkingDir: tmp, RegistryPath: "old"},
	}); err != nil {
		t.Fatalf("write old config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  activePID,
		SupervisorPGID: activePGID,
		ChildPID:       childPID,
		ChildPGID:      childPGID,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}

	if _, err := (teamsServiceLocalSupervisorBackend{}).Repair(context.Background(), teamsServiceSpec{
		Executable:   exePath,
		WorkingDir:   tmp,
		RegistryPath: "new",
	}, teamsServiceRepairOptions{Enable: true, Start: true}); err != nil {
		t.Fatalf("repair local supervisor: %v", err)
	}
	if verifiedChildRegistry != "old" {
		t.Fatalf("verified child registry = %q, want old config before rewrite", verifiedChildRegistry)
	}
	if !reflect.DeepEqual(terminated, []int{childPID, activePID}) {
		t.Fatalf("terminated pids = %#v, want child then supervisor", terminated)
	}
	cfg, err := readTeamsServiceLocalSupervisorConfig(configPath)
	if err != nil {
		t.Fatalf("read new config: %v", err)
	}
	if cfg.Spec.RegistryPath != "new" {
		t.Fatalf("config registry = %q, want new", cfg.Spec.RegistryPath)
	}
}

func TestTeamsServiceHelperUpgradeRestartsActiveLocalSupervisorAfterConfigRefresh(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdAvailable := true
	activePID := 8101
	activePGID := 9101
	startedPID := 8102
	startedPGID := 9102
	stops := 0
	starts := 0
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			starts++
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: startedPGID,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevCurrentProcessGroupID := teamsLocalSupervisorCurrentProcessGroupID
	prevTerminate := teamsLocalSupervisorTerminateProcessGroup
	teamsLocalSupervisorProcessAlive = func(pid int) bool {
		return pid == activePID || pid == startedPID
	}
	teamsLocalSupervisorProcessGroupID = func(pid int) (int, error) {
		switch pid {
		case activePID:
			return activePGID, nil
		case startedPID:
			return startedPGID, nil
		default:
			return 0, fmt.Errorf("unexpected pid %d", pid)
		}
	}
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return 999 }
	teamsLocalSupervisorTerminateProcessGroup = func(int, int, time.Duration) error {
		stops++
		return nil
	}
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorCurrentProcessGroupID = prevCurrentProcessGroupID
		teamsLocalSupervisorTerminateProcessGroup = prevTerminate
	})

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    teamsServiceSpec{Executable: exePath, WorkingDir: tmp, RegistryPath: "old"},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  activePID,
		SupervisorPGID: activePGID,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}

	if _, err := refreshTeamsServiceForHelperUpgrade(context.Background(), stringPtr(filepath.Join(tmp, "registry.json")), strings.NewReader(""), io.Discard); err != nil {
		t.Fatalf("refresh Teams service for helper upgrade: %v", err)
	}
	if stops != 1 || starts != 1 {
		t.Fatalf("upgrade refresh stops=%d starts=%d, want active local supervisor restarted", stops, starts)
	}
}

func TestTeamsServiceLocalSupervisorActiveRejectsIdentityMismatch(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "config", "codex-helper", "teams", teamsServiceLocalSupervisorConfigName)
	systemdUnavailable := false
	verifyErr := errors.New("identity mismatch")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return verifyErr
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: os.Getpid(),
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}
	active, err := (teamsServiceLocalSupervisorBackend{}).Active(context.Background())
	if err != nil {
		t.Fatalf("active error: %v", err)
	}
	if active {
		t.Fatal("Active returned true for a PID that failed local supervisor identity verification")
	}
}

func TestTeamsServiceLocalSupervisorActiveRejectsCurrentStatusWithoutHeartbeat(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "config", "codex-helper", "teams", teamsServiceLocalSupervisorConfigName)
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec:    teamsServiceSpec{Executable: filepath.Join(tmp, "bin", "codex-proxy"), WorkingDir: tmp},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: os.Getpid(),
		State:         "running",
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}
	active, err := (teamsServiceLocalSupervisorBackend{}).Active(context.Background())
	if err != nil {
		t.Fatalf("active error: %v", err)
	}
	if active {
		t.Fatal("Active returned true for current-version status without heartbeat UpdatedAt")
	}
}

func TestTeamsServiceLocalSupervisorRuntimeDirIgnoresXDGRuntimeDir(t *testing.T) {
	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(tmp, "runtime"))

	statusPath, err := teamsServiceLocalSupervisorStatusPath()
	if err != nil {
		t.Fatalf("status path: %v", err)
	}
	want := filepath.Join(configBase, "codex-helper", "teams", "run", "local-supervisor", teamsServiceLocalSupervisorStatusName)
	if statusPath != want {
		t.Fatalf("status path = %q, want stable config-root path %q", statusPath, want)
	}
}

func TestTeamsServiceLocalSupervisorStartRequiresReadyStatus(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localReadyTimeout:    20 * time.Millisecond,
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			return os.Getpid(), nil
		},
	})
	path, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp})
	if err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	_, err = (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start")
	if err == nil || !strings.Contains(err.Error(), "did not report ready") {
		t.Fatalf("start err = %v, want ready timeout", err)
	}
	if path == "" {
		t.Fatal("install path should not be empty")
	}
}

func TestTeamsServiceLocalSupervisorReleaseFailureTerminatesStartedSupervisor(t *testing.T) {
	lockCLITestHooks(t)

	releaseErr := errors.New("release failed")
	configPath := filepath.Join(t.TempDir(), "local-supervisor.json")
	prevVerify := teamsLocalSupervisorVerifyProcessIdentity
	prevAlive := teamsLocalSupervisorProcessAlive
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevCurrentProcessGroupID := teamsLocalSupervisorCurrentProcessGroupID
	prevTerminate := teamsLocalSupervisorTerminateProcessGroup
	teamsLocalSupervisorVerifyProcessIdentity = func(pid int, gotConfigPath string) error {
		if pid != 8123 || gotConfigPath != configPath {
			t.Fatalf("verify pid/config = %d %q, want 8123 %q", pid, gotConfigPath, configPath)
		}
		return nil
	}
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == 8123 }
	teamsLocalSupervisorProcessGroupID = func(pid int) (int, error) {
		if pid != 8123 {
			t.Fatalf("process group pid = %d, want 8123", pid)
		}
		return 9123, nil
	}
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return 100 }
	var terminatedPGID int
	var terminatedPID int
	teamsLocalSupervisorTerminateProcessGroup = func(pgid int, leaderPID int, _ time.Duration) error {
		terminatedPGID = pgid
		terminatedPID = leaderPID
		return nil
	}
	t.Cleanup(func() {
		teamsLocalSupervisorVerifyProcessIdentity = prevVerify
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorCurrentProcessGroupID = prevCurrentProcessGroupID
		teamsLocalSupervisorTerminateProcessGroup = prevTerminate
	})
	waited := false
	err := handleTeamsServiceLocalSupervisorReleaseFailure(8123, configPath, releaseErr, func() error {
		waited = true
		return nil
	})
	if !errors.Is(err, releaseErr) {
		t.Fatalf("release failure err = %v, want %v", err, releaseErr)
	}
	if terminatedPGID != 9123 || terminatedPID != 8123 {
		t.Fatalf("terminated pgid/pid = %d/%d, want 9123/8123", terminatedPGID, terminatedPID)
	}
	if !waited {
		t.Fatal("release failure cleanup did not wait after successful termination")
	}
}

func TestTeamsServiceLocalSupervisorStopRejectsIdentityMismatch(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "config", "codex-helper", "teams", teamsServiceLocalSupervisorConfigName)
	systemdUnavailable := false
	verifyErr := errors.New("identity mismatch")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return verifyErr
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  os.Getpid(),
		SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID() + 100000,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}
	_, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "stop")
	if !errors.Is(err, verifyErr) {
		t.Fatalf("stop err = %v, want %v", err, verifyErr)
	}
}

func TestTeamsServiceLocalSupervisorStopRejectsStaleSupervisorPGID(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "config", "codex-helper", "teams", teamsServiceLocalSupervisorConfigName)
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return nil
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  os.Getpid(),
		SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID() + 100000,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}
	_, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "stop")
	if err == nil || !strings.Contains(err.Error(), "stale local supervisor process group") {
		t.Fatalf("stop err = %v, want stale PGID rejection", err)
	}
}

func TestTeamsServiceLocalSupervisorUninstallPropagatesStopError(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	systemdUnavailable := false
	verifyErr := errors.New("identity mismatch")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return verifyErr
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: os.Getpid(),
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}

	_, err = (teamsServiceLocalSupervisorBackend{}).Uninstall(context.Background())
	if !errors.Is(err, verifyErr) {
		t.Fatalf("uninstall err = %v, want %v", err, verifyErr)
	}
	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("config should remain after failed stop, stat err=%v", err)
	}
}

func TestTeamsServiceLocalSupervisorRestartDoesNotStartAfterStopError(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	systemdUnavailable := false
	verifyErr := errors.New("identity mismatch")
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyProcessIdentity: func(int, string) error {
			return verifyErr
		},
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: os.Getpid(),
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}

	_, err = (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "restart")
	if !errors.Is(err, verifyErr) {
		t.Fatalf("restart err = %v, want %v", err, verifyErr)
	}
	if started {
		t.Fatal("restart started local supervisor after stop failed")
	}
}

func TestTeamsServiceLocalSupervisorStopDeadSupervisorRejectsUnverifiedChild(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "config", "codex-helper", "teams", teamsServiceLocalSupervisorConfigName)
	systemdUnavailable := false
	verifyErr := errors.New("child identity mismatch")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localVerifyChildIdentity: func(int, teamsServiceSpec) error {
			return verifyErr
		},
	})
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  99999999,
		SupervisorPGID: 99999999,
		ChildPID:       os.Getpid(),
		ChildPGID:      teamsLocalSupervisorCurrentProcessGroupID() + 100000,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}
	_, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "stop")
	if !errors.Is(err, verifyErr) {
		t.Fatalf("stop err = %v, want %v", err, verifyErr)
	}
}

func TestTeamsServiceLocalSupervisorStopMalformedStatusFailsClosedWhenLockHeld(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	statusPath, err := teamsServiceLocalSupervisorStatusPath()
	if err != nil {
		t.Fatalf("status path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(statusPath), 0o700); err != nil {
		t.Fatalf("mkdir status dir: %v", err)
	}
	if err := os.WriteFile(statusPath, []byte("{"), 0o600); err != nil {
		t.Fatalf("write malformed status: %v", err)
	}
	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		t.Fatalf("lock path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	lock := flock.New(lockPath)
	locked, err := lock.TryLock()
	if err != nil || !locked {
		t.Fatalf("acquire test lock locked=%v err=%v", locked, err)
	}
	defer lock.Unlock()

	_, err = (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "stop")
	if !errors.Is(err, errTeamsServiceLocalSupervisorStatusMalformed) {
		t.Fatalf("stop err = %v, want malformed status error", err)
	}
}

func TestTeamsServiceLocalSupervisorStopMalformedStatusWithoutLockTreatsStopped(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	statusPath, err := teamsServiceLocalSupervisorStatusPath()
	if err != nil {
		t.Fatalf("status path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(statusPath), 0o700); err != nil {
		t.Fatalf("mkdir status dir: %v", err)
	}
	if err := os.WriteFile(statusPath, []byte("{"), 0o600); err != nil {
		t.Fatalf("write malformed status: %v", err)
	}
	out, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "stop")
	if err != nil {
		t.Fatalf("stop err = %v, want nil without held lock", err)
	}
	if !strings.Contains(string(out), "malformed status") {
		t.Fatalf("stop output = %q, want malformed status note", out)
	}
}

func TestTeamsServiceLocalSupervisorStartLockHeldWithoutReadyStatusFails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exePath,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localReadyTimeout:    20 * time.Millisecond,
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exePath, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		t.Fatalf("lock path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	lock := flock.New(lockPath)
	locked, err := lock.TryLock()
	if err != nil || !locked {
		t.Fatalf("acquire test lock locked=%v err=%v", locked, err)
	}
	defer lock.Unlock()

	_, err = (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start")
	if err == nil || !strings.Contains(err.Error(), "lock is held but no verified supervisor status") {
		t.Fatalf("start err = %v, want lock-held readiness failure", err)
	}
}

func TestTeamsServiceLocalSupervisorChildHealthRestartsChild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("local supervisor child process-group termination is Unix-only")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	fake := filepath.Join(tmp, "fake-cxp")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}
	prevHeartbeat := teamsServiceLocalSupervisorHeartbeatEvery
	prevTerminate := teamsServiceLocalSupervisorTerminationWait
	prevHealth := teamsServiceLocalSupervisorCheckChildHealth
	teamsServiceLocalSupervisorHeartbeatEvery = 10 * time.Millisecond
	teamsServiceLocalSupervisorTerminationWait = 200 * time.Millisecond
	teamsServiceLocalSupervisorCheckChildHealth = func(context.Context, *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error) {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionRestart, Reason: "test stale child"}, nil
	}
	t.Cleanup(func() {
		teamsServiceLocalSupervisorHeartbeatEvery = prevHeartbeat
		teamsServiceLocalSupervisorTerminationWait = prevTerminate
		teamsServiceLocalSupervisorCheckChildHealth = prevHealth
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status := teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     filepath.Join(tmp, "local-supervisor.json"),
		SupervisorPID:  os.Getpid(),
		SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID(),
		UpdatedAt:      time.Now(),
	}
	err := runTeamsServiceLocalSupervisorChild(ctx, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: fake,
			WorkingDir: tmp,
		},
	}, &status, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "health check restarted child") {
		t.Fatalf("run child err = %v, want health restart", err)
	}
	if status.ChildPID != 0 || status.ChildPGID != 0 || status.State != "waiting" {
		t.Fatalf("status after health restart = %#v, want cleared waiting child", status)
	}
	if status.LastHealthReason != "test stale child" || status.LastHealthAction != teamsServiceWatchdogActionRestart {
		t.Fatalf("health status = reason %q action %q", status.LastHealthReason, status.LastHealthAction)
	}
}

func TestTeamsServiceLocalSupervisorChildHealthTerminateErrorReturns(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("local supervisor child process-group termination is Unix-only")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	fake := filepath.Join(tmp, "fake-cxp")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nwhile :; do sleep 1; done\n"), 0o700); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}
	prevHeartbeat := teamsServiceLocalSupervisorHeartbeatEvery
	prevTerminate := teamsServiceLocalSupervisorTerminationWait
	prevHealth := teamsServiceLocalSupervisorCheckChildHealth
	prevTerminateTarget := teamsServiceLocalSupervisorTerminateTarget
	terminateErr := errors.New("test terminate failed")
	var captured *exec.Cmd
	teamsServiceLocalSupervisorHeartbeatEvery = 10 * time.Millisecond
	teamsServiceLocalSupervisorTerminationWait = 50 * time.Millisecond
	teamsServiceLocalSupervisorCheckChildHealth = func(context.Context, *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error) {
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionRestart, Reason: "test stale child"}, nil
	}
	teamsServiceLocalSupervisorTerminateTarget = func(cmd *exec.Cmd, _ time.Duration) error {
		captured = cmd
		return terminateErr
	}
	t.Cleanup(func() {
		if captured != nil {
			_ = terminateTargetCommand(captured, 200*time.Millisecond)
		}
		teamsServiceLocalSupervisorHeartbeatEvery = prevHeartbeat
		teamsServiceLocalSupervisorTerminationWait = prevTerminate
		teamsServiceLocalSupervisorCheckChildHealth = prevHealth
		teamsServiceLocalSupervisorTerminateTarget = prevTerminateTarget
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	status := teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     filepath.Join(tmp, "local-supervisor.json"),
		SupervisorPID:  os.Getpid(),
		SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID(),
		UpdatedAt:      time.Now(),
	}
	err := runTeamsServiceLocalSupervisorChild(ctx, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: fake,
			WorkingDir: tmp,
		},
	}, &status, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "could not terminate child") {
		t.Fatalf("run child err = %v, want terminate failure", err)
	}
	if !strings.Contains(status.LastError, terminateErr.Error()) {
		t.Fatalf("status LastError = %q, want terminate error", status.LastError)
	}
}

func TestTeamsServiceLocalSupervisorProcessEnvUsesControlledEnvironment(t *testing.T) {
	t.Setenv("CODEX_HOME", filepath.Join(t.TempDir(), "parent-home"))
	t.Setenv("CODEX_PROXY_DEBUG", "1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:9999")
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "runtime"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "config"))
	specHome := filepath.Join(t.TempDir(), "spec-home")
	env := teamsServiceLocalSupervisorProcessEnv(map[string]string{
		"CODEX_HOME":                 specHome,
		"CODEX_HELPER_TEAMS_SERVICE": "1",
		"HTTPS_PROXY":                "http://configured-proxy.example.test:8080",
	})
	if got, ok := testEnvValue(env, "CODEX_HOME"); !ok || got != specHome {
		t.Fatalf("CODEX_HOME env = %q ok=%v, want %q", got, ok, specHome)
	}
	if got, ok := testEnvValue(env, "CODEX_HELPER_TEAMS_SERVICE"); !ok || got != "1" {
		t.Fatalf("CODEX_HELPER_TEAMS_SERVICE env = %q ok=%v, want 1", got, ok)
	}
	if got, ok := testEnvValue(env, "HTTPS_PROXY"); !ok || got != "http://configured-proxy.example.test:8080" {
		t.Fatalf("HTTPS_PROXY env = %q ok=%v, want configured service proxy", got, ok)
	}
	if _, ok := testEnvValue(env, "CODEX_PROXY_DEBUG"); ok {
		t.Fatalf("unexpected non-service CODEX_PROXY_DEBUG in local supervisor env: %#v", env)
	}
	if _, ok := testEnvValue(env, "HTTP_PROXY"); ok {
		t.Fatalf("unexpected parent HTTP_PROXY in local supervisor env: %#v", env)
	}
	if _, ok := testEnvValue(env, "XDG_RUNTIME_DIR"); ok {
		t.Fatalf("unexpected XDG_RUNTIME_DIR in local supervisor env: %#v", env)
	}
	if got, ok := testEnvValue(env, "XDG_CONFIG_HOME"); !ok || got == "" {
		t.Fatalf("XDG_CONFIG_HOME env = %q ok=%v, want preserved", got, ok)
	}
}

func TestTeamsServiceLocalSupervisorChildProcessEnvMarksSupervisorVersion(t *testing.T) {
	env := teamsServiceLocalSupervisorChildProcessEnv(map[string]string{
		"CODEX_HELPER_TEAMS_SERVICE": "1",
	})
	if got, ok := testEnvValue(env, envTeamsLocalSupervisorVersion); !ok || got != buildVersion() {
		t.Fatalf("%s env = %q ok=%v, want %q", envTeamsLocalSupervisorVersion, got, ok, buildVersion())
	}
	if got, ok := testEnvValue(env, "CODEX_HELPER_TEAMS_SERVICE"); !ok || got != "1" {
		t.Fatalf("CODEX_HELPER_TEAMS_SERVICE env = %q ok=%v, want 1", got, ok)
	}
}

func TestMaybeScheduleLegacyLocalSupervisorRestartSchedulesForOldSupervisor(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	supervisorPID := 7001
	childPID := 7002
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  exe,
		cwd:  tmp,
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	t.Setenv(envTeamsLinuxServiceBackend, "local-supervisor")
	t.Setenv(envTeamsLocalSupervisorVersion, "")

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec: teamsServiceSpec{
			Executable: exe,
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write local supervisor config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: supervisorPID,
		ChildPID:      childPID,
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write local supervisor status: %v", err)
	}

	prevAlive := teamsLocalSupervisorProcessAlive
	prevVerify := teamsLocalSupervisorVerifyProcessIdentity
	prevDetached := teamsServiceStartDetached
	prevWait := teamsLegacyLocalSupervisorActivationWait
	prevCurrentProcessID := teamsCurrentProcessID
	prevParentProcessID := teamsParentProcessID
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorVerifyProcessIdentity = prevVerify
		teamsServiceStartDetached = prevDetached
		teamsLegacyLocalSupervisorActivationWait = prevWait
		teamsCurrentProcessID = prevCurrentProcessID
		teamsParentProcessID = prevParentProcessID
		teamsLegacyLocalSupervisorRestartScheduled.Store(false)
	})
	teamsLegacyLocalSupervisorActivationWait = time.Minute
	teamsLegacyLocalSupervisorRestartScheduled.Store(false)
	teamsCurrentProcessID = func() int { return childPID }
	teamsParentProcessID = func() int { return supervisorPID }
	teamsLocalSupervisorProcessAlive = func(pid int) bool {
		return pid == supervisorPID || pid == childPID
	}
	teamsLocalSupervisorVerifyProcessIdentity = func(pid int, gotConfigPath string) error {
		if pid != supervisorPID || gotConfigPath != configPath {
			return fmt.Errorf("unexpected supervisor identity probe pid=%d config=%q", pid, gotConfigPath)
		}
		return nil
	}
	var gotName string
	var gotArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		gotName = name
		gotArgs = append([]string(nil), args...)
		return nil
	}

	scheduled, err := maybeScheduleLegacyLocalSupervisorRestart(context.Background())
	if err != nil {
		t.Fatalf("maybeScheduleLegacyLocalSupervisorRestart error: %v", err)
	}
	if !scheduled {
		t.Fatal("scheduled = false, want true")
	}
	joined := strings.Join(gotArgs, " ")
	if gotName != "sh" ||
		!strings.Contains(joined, shellQuote(exe)+" teams service restart") ||
		!strings.Contains(joined, envTeamsLinuxServiceBackend+"=local-supervisor") {
		t.Fatalf("detached restart command = %q %#v, want local-supervisor restart", gotName, gotArgs)
	}
	activation, ok, err := readTeamsServiceLocalSupervisorActivation()
	if err != nil || !ok {
		t.Fatalf("read activation = ok %v err %v, want activation", ok, err)
	}
	if activation.Status != "scheduled" || activation.TargetVersion != buildVersion() || activation.ObservedSupervisorEnv != "" || activation.OldSupervisorPID != supervisorPID || activation.OldChildPID != childPID {
		t.Fatalf("activation = %#v, want scheduled current upgrade handoff", activation)
	}
	if activation.DeadlineAt.Sub(activation.ScheduledAt) != time.Minute {
		t.Fatalf("activation deadline delta = %s, want 1m", activation.DeadlineAt.Sub(activation.ScheduledAt))
	}

	scheduled, err = maybeScheduleLegacyLocalSupervisorRestart(context.Background())
	if err != nil {
		t.Fatalf("second maybeScheduleLegacyLocalSupervisorRestart error: %v", err)
	}
	if scheduled {
		t.Fatal("second scheduled = true, want false after one-shot guard")
	}
}

func TestMaybeScheduleLegacyLocalSupervisorRestartFailureQueuesControlNotice(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	supervisorPID := 7101
	childPID := 7102
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  exe,
		cwd:  tmp,
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	t.Setenv(envTeamsLinuxServiceBackend, "local-supervisor")
	t.Setenv(envTeamsLocalSupervisorVersion, "")

	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.ControlChat.TeamsChatID = "control-chat"
		return nil
	}); err != nil {
		t.Fatalf("seed control chat: %v", err)
	}

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec: teamsServiceSpec{
			Executable: exe,
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write local supervisor config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: supervisorPID,
		ChildPID:      childPID,
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write local supervisor status: %v", err)
	}

	prevAlive := teamsLocalSupervisorProcessAlive
	prevVerify := teamsLocalSupervisorVerifyProcessIdentity
	prevDetached := teamsServiceStartDetached
	prevWait := teamsLegacyLocalSupervisorActivationWait
	prevCurrentProcessID := teamsCurrentProcessID
	prevParentProcessID := teamsParentProcessID
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorVerifyProcessIdentity = prevVerify
		teamsServiceStartDetached = prevDetached
		teamsLegacyLocalSupervisorActivationWait = prevWait
		teamsCurrentProcessID = prevCurrentProcessID
		teamsParentProcessID = prevParentProcessID
		teamsLegacyLocalSupervisorRestartScheduled.Store(false)
	})
	teamsLegacyLocalSupervisorActivationWait = time.Minute
	teamsLegacyLocalSupervisorRestartScheduled.Store(false)
	teamsCurrentProcessID = func() int { return childPID }
	teamsParentProcessID = func() int { return supervisorPID }
	teamsLocalSupervisorProcessAlive = func(pid int) bool {
		return pid == supervisorPID || pid == childPID
	}
	teamsLocalSupervisorVerifyProcessIdentity = func(pid int, gotConfigPath string) error {
		if pid != supervisorPID || gotConfigPath != configPath {
			return fmt.Errorf("unexpected supervisor identity probe pid=%d config=%q", pid, gotConfigPath)
		}
		return nil
	}
	teamsServiceStartDetached = func(context.Context, string, ...string) error {
		return errors.New("detached restart failed")
	}

	scheduled, err := maybeScheduleLegacyLocalSupervisorRestart(context.Background())
	if err == nil || !strings.Contains(err.Error(), "detached restart failed") {
		t.Fatalf("maybeScheduleLegacyLocalSupervisorRestart error = %v, want detached failure", err)
	}
	if scheduled {
		t.Fatal("scheduled = true, want false on detached failure")
	}
	for i := 0; i < 2; i++ {
		if err := queueLegacyLocalSupervisorActivationAttentionNoticeFromDisk(context.Background(), ""); err != nil {
			t.Fatalf("queue activation notice attempt %d: %v", i, err)
		}
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	count := 0
	for _, msg := range state.OutboxMessages {
		if msg.Kind != "local-supervisor-activation-failed" {
			continue
		}
		count++
		if msg.TeamsChatID != "control-chat" || !msg.MentionOwner || !msg.UpgradeNonBlocking || !strings.Contains(msg.Body, "detached restart failed") {
			t.Fatalf("queued failed activation notice = %#v, want control attention notice", msg)
		}
	}
	if count != 1 {
		t.Fatalf("failed activation notices = %d, want exactly one", count)
	}
}

func TestMaybeScheduleLegacyLocalSupervisorRestartSkipsCurrentSupervisorVersion(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:  tmp,
	})
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	t.Setenv(envTeamsLinuxServiceBackend, "local-supervisor")
	t.Setenv(envTeamsLocalSupervisorVersion, buildVersion())
	if err := writeTeamsServiceLocalSupervisorActivation(teamsServiceLocalSupervisorActivation{
		Version:       teamsServiceLocalSupervisorActivationVersion,
		Status:        "scheduled",
		TargetVersion: buildVersion(),
		ScheduledAt:   time.Now().Add(-time.Minute),
		DeadlineAt:    time.Now().Add(time.Minute),
		UpdatedAt:     time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("write activation: %v", err)
	}
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		teamsServiceStartDetached = prevDetached
		teamsLegacyLocalSupervisorRestartScheduled.Store(false)
	})
	teamsLegacyLocalSupervisorRestartScheduled.Store(false)
	teamsServiceStartDetached = func(context.Context, string, ...string) error {
		t.Fatal("matching local-supervisor version marker must not schedule a restart")
		return nil
	}

	scheduled, err := maybeScheduleLegacyLocalSupervisorRestart(context.Background())
	if err != nil {
		t.Fatalf("maybeScheduleLegacyLocalSupervisorRestart error: %v", err)
	}
	if scheduled {
		t.Fatal("scheduled = true, want false")
	}
	activation, ok, err := readTeamsServiceLocalSupervisorActivation()
	if err != nil || !ok {
		t.Fatalf("read activation = ok %v err %v, want activation", ok, err)
	}
	if activation.Status != "success" || activation.ObservedSupervisorEnv != buildVersion() {
		t.Fatalf("activation = %#v, want success with current marker", activation)
	}
}

func TestWaitForLegacyLocalSupervisorActivationHandoffMarksExpired(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	prevWait := teamsLegacyLocalSupervisorActivationWait
	t.Cleanup(func() {
		teamsLegacyLocalSupervisorActivationWait = prevWait
	})
	teamsLegacyLocalSupervisorActivationWait = time.Nanosecond
	now := time.Now().Add(-time.Minute)
	if err := writeTeamsServiceLocalSupervisorActivation(teamsServiceLocalSupervisorActivation{
		Version:       teamsServiceLocalSupervisorActivationVersion,
		Status:        "scheduled",
		TargetVersion: buildVersion(),
		ScheduledAt:   now,
		DeadlineAt:    now.Add(time.Nanosecond),
		UpdatedAt:     now,
	}); err != nil {
		t.Fatalf("write activation: %v", err)
	}

	if err := waitForLegacyLocalSupervisorActivationHandoff(context.Background()); err != nil {
		t.Fatalf("waitForLegacyLocalSupervisorActivationHandoff error: %v", err)
	}
	activation, ok, err := readTeamsServiceLocalSupervisorActivation()
	if err != nil || !ok {
		t.Fatalf("read activation = ok %v err %v, want activation", ok, err)
	}
	if activation.Status != "expired" || !strings.Contains(activation.Message, "continuing under the existing supervisor") {
		t.Fatalf("activation = %#v, want expired warning", activation)
	}
}

func TestTeamsServiceLocalSupervisorStatusReportsActivation(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:  tmp,
	})
	t.Setenv(envTeamsLinuxServiceBackend, "local-supervisor")
	if err := writeTeamsServiceLocalSupervisorConfig(filepath.Join(tmp, "config", "codex-helper", "teams", teamsServiceLocalSupervisorConfigName), teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write local supervisor config: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorActivation(teamsServiceLocalSupervisorActivation{
		Version:          teamsServiceLocalSupervisorActivationVersion,
		Status:           "expired",
		TargetVersion:    buildVersion(),
		OldSupervisorPID: 1234,
		Message:          "handoff timed out",
		UpdatedAt:        time.Now(),
	}); err != nil {
		t.Fatalf("write activation: %v", err)
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	data, err := backend.Run(context.Background(), "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	got := string(data)
	for _, want := range []string{"Activation: expired", "target=" + buildVersion(), "old_supervisor_pid=1234", "handoff timed out"} {
		if !strings.Contains(got, want) {
			t.Fatalf("status output missing %q:\n%s", want, got)
		}
	}
}

func TestQueueLegacyLocalSupervisorActivationAttentionNotice(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.ControlChat.TeamsChatID = "control-chat"
		return nil
	}); err != nil {
		t.Fatalf("seed control chat: %v", err)
	}
	activation := teamsServiceLocalSupervisorActivation{
		Version:          teamsServiceLocalSupervisorActivationVersion,
		Status:           "expired",
		TargetVersion:    buildVersion(),
		OldSupervisorPID: 1234,
		Message:          "handoff timed out",
	}
	if err := queueLegacyLocalSupervisorActivationAttentionNotice(context.Background(), "", activation); err != nil {
		t.Fatalf("queue notice: %v", err)
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("load state: %v", err)
	}
	found := false
	for _, msg := range state.OutboxMessages {
		if msg.Kind != "local-supervisor-activation-expired" {
			continue
		}
		found = true
		if msg.TeamsChatID != "control-chat" || !msg.MentionOwner || !msg.UpgradeNonBlocking || !strings.Contains(msg.Body, "handoff timed out") {
			t.Fatalf("queued notice = %#v, want control attention notice", msg)
		}
	}
	if !found {
		t.Fatalf("local-supervisor activation notice was not queued: %#v", state.OutboxMessages)
	}
}

func TestTeamsServiceLocalSupervisorRecordedIdentityChecksArgsStartTimeAndEnv(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos: "linux",
		exe:  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:  tmp,
		localProcessArgs: func(pid int) ([]string, error) {
			if pid != 7001 {
				t.Fatalf("args pid = %d, want 7001", pid)
			}
			return []string{filepath.Join(tmp, "bin", "codex-proxy"), "teams", "run", "--registry", filepath.Join(tmp, "registry.json")}, nil
		},
		localProcessStartTime: func(pid int) (string, error) {
			if pid != 7001 {
				t.Fatalf("start pid = %d, want 7001", pid)
			}
			return "12345", nil
		},
		localProcessEnvironment: func(pid int) (map[string]string, error) {
			if pid != 7001 {
				t.Fatalf("env pid = %d, want 7001", pid)
			}
			return map[string]string{
				"CODEX_HELPER_TEAMS_SERVICE":      "1",
				"CODEX_HELPER_TEAMS_PROFILE":      "work",
				"CODEX_HELPER_TEAMS_MACHINE_ID":   "machine-a",
				"CODEX_HELPER_TEAMS_AUTH_PROFILE": "auth",
			}, nil
		},
	})
	err := teamsServiceLocalSupervisorVerifyRecordedIdentity(7001, &teamsServiceLocalSupervisorProcessIdentity{
		Executable:    filepath.Join(tmp, "bin", "codex-proxy"),
		Args:          []string{"teams", "run", "--registry", filepath.Join(tmp, "registry.json")},
		ProcStartTime: "12345",
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE":      "1",
			"CODEX_HELPER_TEAMS_PROFILE":      "work",
			"CODEX_HELPER_TEAMS_MACHINE_ID":   "machine-a",
			"CODEX_HELPER_TEAMS_AUTH_PROFILE": "auth",
		},
	}, "local supervisor child")
	if err != nil {
		t.Fatalf("verify recorded identity: %v", err)
	}
	err = teamsServiceLocalSupervisorVerifyRecordedIdentity(7001, &teamsServiceLocalSupervisorProcessIdentity{
		Args:          []string{"teams", "run"},
		ProcStartTime: "different",
	}, "local supervisor child")
	if err == nil || !strings.Contains(err.Error(), "start time changed") {
		t.Fatalf("verify stale start time err = %v, want start time rejection", err)
	}
	err = teamsServiceLocalSupervisorVerifyRecordedIdentity(7001, &teamsServiceLocalSupervisorProcessIdentity{
		Args: []string{"teams", "run"},
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_PROFILE": "other",
		},
	}, "local supervisor child")
	if err == nil || !strings.Contains(err.Error(), "environment CODEX_HELPER_TEAMS_PROFILE") {
		t.Fatalf("verify env mismatch err = %v, want env rejection", err)
	}
}

func TestTeamsServiceLocalSupervisorLogRotationFailureKeepsCurrentFile(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "local-supervisor.log")
	large := bytes.Repeat([]byte("x"), teamsServiceLocalSupervisorMaxLogBytes+1)
	if err := os.WriteFile(logPath, large, 0o600); err != nil {
		t.Fatalf("write large log: %v", err)
	}
	current, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open current log: %v", err)
	}
	defer current.Close()
	if err := os.WriteFile(logPath+".2", []byte("backup"), 0o600); err != nil {
		t.Fatalf("write backup log: %v", err)
	}
	if err := os.Mkdir(logPath+".3", 0o700); err != nil {
		t.Fatalf("mkdir conflicting backup path: %v", err)
	}
	if err := os.WriteFile(filepath.Join(logPath+".3", "block"), []byte("block"), 0o600); err != nil {
		t.Fatalf("write conflicting backup child: %v", err)
	}

	next, err := reopenTeamsServiceLocalSupervisorLogIfNeeded(logPath, current)
	if err == nil {
		t.Fatal("reopen log succeeded, want rotation error")
	}
	if next != current {
		t.Fatalf("reopen returned different file after failure")
	}
	if _, err := current.Write([]byte("still-open\n")); err != nil {
		t.Fatalf("current log file was closed after rotation failure: %v", err)
	}
}

func TestTeamsServiceLocalSupervisorLogWriterRotatesDuringLongRunningOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("local-supervisor log rotation renames an open append handle; Windows uses the Task Scheduler backend")
	}
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "local-supervisor.log")
	writer, err := openTeamsServiceLocalSupervisorLogWriter(logPath)
	if err != nil {
		t.Fatalf("open log writer: %v", err)
	}
	large := bytes.Repeat([]byte("x"), teamsServiceLocalSupervisorMaxLogBytes+1)
	if _, err := writer.Write(large); err != nil {
		t.Fatalf("write large log chunk: %v", err)
	}
	if _, err := writer.Write([]byte("next\n")); err != nil {
		t.Fatalf("write post-rotation log chunk: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close log writer: %v", err)
	}
	current, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read current log: %v", err)
	}
	if string(current) != "next\n" {
		t.Fatalf("current log = %q, want post-rotation chunk", current)
	}
	rotated, err := os.Stat(logPath + ".1")
	if err != nil {
		t.Fatalf("stat rotated log: %v", err)
	}
	if rotated.Size() <= teamsServiceLocalSupervisorMaxLogBytes {
		t.Fatalf("rotated log size = %d, want > %d", rotated.Size(), teamsServiceLocalSupervisorMaxLogBytes)
	}
}

func TestTeamsServiceLocalSupervisorDirRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation is not reliably available on Windows test hosts")
	}
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("mkdir target: %v", err)
	}
	link := filepath.Join(tmp, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	err := ensureTeamsServiceLocalSupervisorDir(link)
	if err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("ensure symlink dir err = %v, want symlink rejection", err)
	}
}

func TestTeamsServiceLocalSupervisorRejectsSymlinkStatusLockAndLogFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation is not reliably available on Windows test hosts")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	statusPath, err := teamsServiceLocalSupervisorStatusPath()
	if err != nil {
		t.Fatalf("status path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(statusPath), 0o700); err != nil {
		t.Fatalf("mkdir status dir: %v", err)
	}
	statusTarget := filepath.Join(tmp, "status-target.json")
	if err := os.WriteFile(statusTarget, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write status target: %v", err)
	}
	if err := os.Symlink(statusTarget, statusPath); err != nil {
		t.Fatalf("status symlink: %v", err)
	}
	if _, _, err := readTeamsServiceLocalSupervisorStatus(); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("read symlink status err = %v, want symlink rejection", err)
	}

	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		t.Fatalf("lock path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	lockTarget := filepath.Join(tmp, "lock-target")
	if err := os.WriteFile(lockTarget, []byte("lock"), 0o600); err != nil {
		t.Fatalf("write lock target: %v", err)
	}
	if err := os.Symlink(lockTarget, lockPath); err != nil {
		t.Fatalf("lock symlink: %v", err)
	}
	if _, err := teamsServiceLocalSupervisorLockHeld(); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("lock symlink err = %v, want symlink rejection", err)
	}

	logPath, err := teamsServiceLocalSupervisorLogPath()
	if err != nil {
		t.Fatalf("log path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	logTarget := filepath.Join(tmp, "log-target")
	if err := os.WriteFile(logTarget, []byte("log"), 0o600); err != nil {
		t.Fatalf("write log target: %v", err)
	}
	if err := os.Symlink(logTarget, logPath); err != nil {
		t.Fatalf("log symlink: %v", err)
	}
	if file, err := openTeamsServiceLocalSupervisorLog(logPath); err == nil {
		_ = file.Close()
		t.Fatal("open symlink log succeeded, want rejection")
	} else if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("log symlink err = %v, want symlink rejection", err)
	}
}

func TestTeamsServiceLocalSupervisorFileAtomicReplacesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatalf("write old file: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorFileAtomic(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("atomic write: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read replaced file: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("file content = %q, want new", data)
	}
}

func TestTeamsServiceInstallUsesStablePathWhenExecutableIsNFSSillyRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("NFS silly rename handling is for Unix service units")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd")
	binDir := filepath.Join(tmp, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create bin dir: %v", err)
	}
	stable := filepath.Join(binDir, "codex-proxy")
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	running := filepath.Join(binDir, ".nfs802014de01c482a800000492")
	cwd := filepath.Join(tmp, "work")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     running,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"ExecStart=" + systemdQuoteArg(stable) + " teams run --owner-stale-after 1m30s --auto-service=false",
		"Environment=" + systemdQuoteArg(update.EnvInstallDir+"="+stable),
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
	}
	if strings.Contains(unit, running) {
		t.Fatalf("unit should not retain NFS silly-renamed path %q:\n%s", running, unit)
	}
}

func TestTeamsServiceBootstrapDryRunDoesNotWriteOrStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("systemd dry-run rendering is Unix-focused")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	if err := os.MkdirAll(filepath.Dir(exePath), 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := os.WriteFile(exePath, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write exe: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"bootstrap", "--dry-run"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap --dry-run error: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("dry-run should not call service runner, calls=%#v", runner.calls)
	}
	if _, err := os.Stat(filepath.Join(unitDir, teamsServiceUnitName)); !os.IsNotExist(err) {
		t.Fatalf("dry-run should not write unit, stat err=%v", err)
	}
	if got := out.String(); !strings.Contains(got, "Teams service dry-run") || !strings.Contains(got, "--- systemd main unit ---") || !strings.Contains(got, exePath) {
		t.Fatalf("dry-run output missing rendered unit details:\n%s", got)
	}
}

func TestTeamsServiceBootstrapNoStartRepairsButDoesNotStart(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("systemd no-start rendering is Unix-focused")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	if err := os.MkdirAll(filepath.Dir(exePath), 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := os.WriteFile(exePath, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write exe: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"bootstrap", "--no-start"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap --no-start error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(unitDir, teamsServiceUnitName)); err != nil {
		t.Fatalf("no-start should write unit: %v", err)
	}
	for _, forbidden := range []string{"start", "restart"} {
		if teamsServiceCallSeen(runner.calls, forbidden) {
			t.Fatalf("no-start should not %s service, calls=%#v", forbidden, runner.calls)
		}
	}
	if !teamsServiceCallSeen(runner.calls, "enable") {
		t.Fatalf("no-start should enable repaired service, calls=%#v", runner.calls)
	}
	if !strings.Contains(out.String(), "Service was repaired but not started.") {
		t.Fatalf("no-start output missing safety message:\n%s", out.String())
	}
}

func TestTeamsServiceInstallWithoutRegistryLetsBridgeUseScopedDefaults(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     cwd,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	if strings.Contains(unit, "--registry") {
		t.Fatalf("default service unit should not force a shared legacy registry:\n%s", unit)
	}
	if !strings.Contains(unit, "ExecStart="+systemdQuoteArg(exePath)+" teams run") {
		t.Fatalf("unit missing teams run ExecStart:\n%s", unit)
	}
}

func TestTeamsServiceInstallRejectsGoRunTemporaryExecutable(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "go-build123", "b001", "exe", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "temporary helper executable path") {
		t.Fatalf("expected go run executable rejection, got %v", err)
	}
}

func TestTeamsServiceAuthPreflightRequiresForegroundLogin(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	err := defaultTeamsServiceAuthPreflight()
	if err == nil || !strings.Contains(err.Error(), "teams auth") {
		t.Fatalf("expected missing auth preflight error, got %v", err)
	}
}

func TestTeamsServiceStartRequiresAuthPreflight(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("started\n")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	teamsServiceAuthPreflight = func() error { return errors.New("auth missing") }

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"start"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "auth missing") {
		t.Fatalf("expected auth preflight error, got %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("service start should not reach supervisor when auth preflight fails: %#v", runner.calls)
	}
}

func TestTeamsServiceRestartForceRecoversActiveOwnerBeforeRestart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st := seedRecoverableTeamsState(t)
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn:manual", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}

	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"restart", "--force"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service restart --force: %v\n%s", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		"Force recovering Teams state before service restart",
		"Cleared stale owners: 1",
		"Recovered interrupted turns: 1",
		"Restarted Teams service",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("restart --force output missing %q:\n%s", want, got)
		}
	}
	if !teamsServiceCallSeen(runner.calls, "restart") {
		t.Fatalf("restart --force did not restart service, calls=%#v", runner.calls)
	}
	if _, ok, err := st.ReadOwner(context.Background()); err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	} else if ok {
		t.Fatal("owner should be cleared after forced service restart")
	}
	state, err := st.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.Turns["turn:manual"].Status; got != teamsstore.TurnStatusInterrupted {
		t.Fatalf("turn status = %q, want interrupted", got)
	}
}

func TestTeamsServiceDoctorReportsAuthAndExecutableWithoutFailingFast(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "go-build123", "b001", "exe", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})
	teamsServiceAuthPreflight = func() error { return errors.New("auth missing") }

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("service doctor should report auth/executable issues without failing fast: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Teams service backend: systemd-user",
		"Teams service raw executable:",
		"Teams service stable executable: unresolved",
		"temporary helper executable path",
		"Teams service auth: not ready",
		"auth missing",
		"Linux: systemd --user keeps the Teams bridge independent of the terminal",
		"if lingering is disabled",
		"no user service can guarantee survival",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
}

func TestTeamsServiceInstallPreservesScopedEnvironment(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex home"))
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "work-profile")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "1")
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", filepath.Join(tmp, "read-token.json"))
	t.Setenv(envTeamsASRCommand, filepath.Join(tmp, "bin", "teams-asr"))
	t.Setenv(envTeamsASRArgsJSON, `["--input={input}","--threads={threads}"]`)
	t.Setenv(envTeamsASRBackend, "llama")
	t.Setenv(envTeamsASRLlamaBinary, filepath.Join(tmp, "bin", "llama-mtmd-cli"))
	t.Setenv(envTeamsASRLlamaModel, filepath.Join(tmp, "models", "qwen.gguf"))
	t.Setenv(envTeamsASRLlamaMMProj, filepath.Join(tmp, "models", "mmproj.gguf"))
	t.Setenv(envTeamsASRLlamaDevice, "cpu")
	t.Setenv(envTeamsASRFFmpeg, filepath.Join(tmp, "bin", "ffmpeg"))
	t.Setenv(envTeamsASRNativeLibraryPath, filepath.Join(tmp, "native libs"))
	t.Setenv("HTTPS_PROXY", "http://proxy.example.test:8080")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"Environment=" + systemdQuoteArg("CODEX_HOME="+filepath.Join(tmp, "codex home")),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_PROFILE=work-profile"),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1"),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE="+filepath.Join(tmp, "read-token.json")),
		"Environment=" + systemdQuoteArg(envTeamsASRCommand+"="+filepath.Join(tmp, "bin", "teams-asr")),
		"Environment=" + systemdQuoteArg(envTeamsASRArgsJSON+`=["--input={input}","--threads={threads}"]`),
		"Environment=" + systemdQuoteArg(envTeamsASRBackend+"=llama"),
		"Environment=" + systemdQuoteArg(envTeamsASRLlamaBinary+"="+filepath.Join(tmp, "bin", "llama-mtmd-cli")),
		"Environment=" + systemdQuoteArg(envTeamsASRLlamaModel+"="+filepath.Join(tmp, "models", "qwen.gguf")),
		"Environment=" + systemdQuoteArg(envTeamsASRLlamaMMProj+"="+filepath.Join(tmp, "models", "mmproj.gguf")),
		"Environment=" + systemdQuoteArg(envTeamsASRLlamaDevice+"=cpu"),
		"Environment=" + systemdQuoteArg(envTeamsASRFFmpeg+"="+filepath.Join(tmp, "bin", "ffmpeg")),
		"Environment=" + systemdQuoteArg(envTeamsASRNativeLibraryPath+"="+filepath.Join(tmp, "native libs")),
		"Environment=" + systemdQuoteArg("HTTPS_PROXY=http://proxy.example.test:8080"),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_SERVICE_MODE=background"),
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing preserved env %q:\n%s", want, unit)
		}
	}
}

func TestTeamsASRArgsFromEnvAndServiceOverrides(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv(envTeamsASRArgsJSON, `["--input={input}","--prefix=  keep spaces  ","--threads={threads}"]`)
	t.Setenv(envTeamsASRBackend, "llama")
	t.Setenv(envTeamsASRLlamaModel, filepath.Join(tmp, "models", "qwen.gguf"))
	t.Setenv(envTeamsASRLlamaMMProj, filepath.Join(tmp, "models", "mmproj.gguf"))
	args, err := teamsASRArgsFromFlagsOrEnv(nil, true)
	if err != nil {
		t.Fatalf("teamsASRArgsFromFlagsOrEnv: %v", err)
	}
	if strings.Join(args, "\n") != "--input={input}\n--prefix=  keep spaces  \n--threads={threads}" {
		t.Fatalf("ASR args = %#v", args)
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  &recordingTeamsServiceRunner{},
	})
	spec, err := buildTeamsServiceSpec(
		stringPtr("registry.json"),
		teamsServiceSpecEnvironmentOverrides(teamsASRServiceEnvironmentOverrides(filepath.Join(tmp, "asr"), []string{"--input={input}", "--threads={threads}"})),
	)
	if err != nil {
		t.Fatalf("buildTeamsServiceSpec: %v", err)
	}
	if spec.Environment[envTeamsASRCommand] != filepath.Join(tmp, "asr") {
		t.Fatalf("ASR command env = %q", spec.Environment[envTeamsASRCommand])
	}
	if spec.Environment[envTeamsASRArgsJSON] != `["--input={input}","--threads={threads}"]` {
		t.Fatalf("ASR args env = %q", spec.Environment[envTeamsASRArgsJSON])
	}
	if spec.Environment[envTeamsASRBackend] != "llama" ||
		spec.Environment[envTeamsASRLlamaModel] != filepath.Join(tmp, "models", "qwen.gguf") ||
		spec.Environment[envTeamsASRLlamaMMProj] != filepath.Join(tmp, "models", "mmproj.gguf") {
		t.Fatalf("managed ASR env = %#v", spec.Environment)
	}
}

func TestTeamsServiceInstallPreservesBeaconAndUpdateEnvironmentAndBlocksVolatile(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(tmp, "beacon.json"))
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", filepath.Join(tmp, "auth.json"))
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", filepath.Join(tmp, "full-token.json"))
	t.Setenv(beacon.BeaconSlurmQueryCommandEnv, filepath.Join(tmp, "slurm-query"))
	t.Setenv(beacon.BeaconSlurmSubmitCommandEnv, filepath.Join(tmp, "slurm-submit"))
	t.Setenv(beacon.BeaconLSFCancelCommandEnv, filepath.Join(tmp, "lsf-cancel"))
	t.Setenv(update.EnvRepo, "owner/name")
	t.Setenv(update.EnvUpdateIndexURL, "https://example.test/releases.json")
	t.Setenv("HTTPS_PROXY", "http://user:pass@proxy.example.test:8080")
	t.Setenv(envTeamsHelperCLIPath, "/tmp/.nfs802014de01c482a800000492")
	t.Setenv(envTeamsHelperCLIDir, "/tmp")
	t.Setenv(envCodexProxyWrapperExe, "/tmp/.nfswrapper")
	t.Setenv(update.EnvInstallDir, "/tmp/.nfsbad")
	t.Setenv(update.EnvVersion, "v0.0.0-bad")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_SECRET", "secret")
	t.Setenv("CODEX_HELPER_TEAMS_BEARER_TOKEN", "token")

	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"Environment=" + systemdQuoteArg("CODEX_HELPER_BEACON_STORE="+filepath.Join(tmp, "beacon.json")),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_AUTH_CONFIG="+filepath.Join(tmp, "auth.json")),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE="+filepath.Join(tmp, "full-token.json")),
		"Environment=" + systemdQuoteArg(beacon.BeaconSlurmQueryCommandEnv+"="+filepath.Join(tmp, "slurm-query")),
		"Environment=" + systemdQuoteArg(beacon.BeaconSlurmSubmitCommandEnv+"="+filepath.Join(tmp, "slurm-submit")),
		"Environment=" + systemdQuoteArg(beacon.BeaconLSFCancelCommandEnv+"="+filepath.Join(tmp, "lsf-cancel")),
		"Environment=" + systemdQuoteArg(update.EnvRepo+"=owner/name"),
		"Environment=" + systemdQuoteArg(update.EnvUpdateIndexURL+"=https://example.test/releases.json"),
		"Environment=" + systemdQuoteArg(update.EnvInstallDir+"="+exePath),
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing env %q:\n%s", want, unit)
		}
	}
	for _, forbidden := range []string{
		envTeamsHelperCLIPath + "=",
		envTeamsHelperCLIDir + "=",
		envCodexProxyWrapperExe + "=",
		update.EnvInstallDir + "=/tmp/.nfsbad",
		update.EnvVersion + "=",
		"CODEX_HELPER_TEAMS_CLIENT_SECRET=",
		"CODEX_HELPER_TEAMS_BEARER_TOKEN=",
		"HTTPS_PROXY=",
		"user:pass@proxy.example.test",
	} {
		if strings.Contains(unit, forbidden) {
			t.Fatalf("unit contains forbidden env %q:\n%s", forbidden, unit)
		}
	}
}

func TestTeamsServiceRepairMergesAllowedEnvironmentFromExistingSystemdUnit(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("systemd unit merge test is Unix-focused")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	oldUnit := strings.Join([]string{
		"[Service]",
		"Environment=" + systemdQuoteArg("CODEX_HELPER_BEACON_STORE="+filepath.Join(tmp, "old-beacon.json")),
		"Environment=" + systemdQuoteArg(beacon.BeaconSlurmQueryCommandEnv+"="+filepath.Join(tmp, "old-slurm-query")),
		"Environment=" + systemdQuoteArg(update.EnvUpdateIndexURL+"=https://old.example.test/index.json"),
		"Environment=" + systemdQuoteArg("CODEX_HOME="+filepath.Join(tmp, "old-codex-home")),
		"Environment=" + systemdQuoteArg(envTeamsHelperCLIPath+"=/tmp/.nfsbad"),
		"Environment=" + systemdQuoteArg("HTTPS_PROXY=http://user:pass@proxy.example.test:8080"),
		"Environment=" + systemdQuoteArg("HTTP_PROXY=user:pass@proxy.example.test:8080"),
	}, "\n")
	if err := os.WriteFile(filepath.Join(unitDir, teamsServiceUnitName), []byte(oldUnit), 0o600); err != nil {
		t.Fatalf("write old unit: %v", err)
	}

	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"Environment=" + systemdQuoteArg("CODEX_HELPER_BEACON_STORE="+filepath.Join(tmp, "old-beacon.json")),
		"Environment=" + systemdQuoteArg(beacon.BeaconSlurmQueryCommandEnv+"="+filepath.Join(tmp, "old-slurm-query")),
		"Environment=" + systemdQuoteArg(update.EnvUpdateIndexURL+"=https://old.example.test/index.json"),
		"Environment=" + systemdQuoteArg("CODEX_HOME="+filepath.Join(tmp, "old-codex-home")),
		"Environment=" + systemdQuoteArg("CODEX_DIR="+filepath.Join(tmp, "old-codex-home")),
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("merged unit missing %q:\n%s", want, unit)
		}
	}
	for _, forbidden := range []string{envTeamsHelperCLIPath + "=/tmp/.nfsbad", "user:pass@proxy.example.test"} {
		if strings.Contains(unit, forbidden) {
			t.Fatalf("merged unit contains forbidden %q:\n%s", forbidden, unit)
		}
	}
}

func TestTeamsServiceInstallDefaultsCodexHomeForScope(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	prevUserHome := effectivePathsUserHomeDir
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { effectivePathsUserHomeDir = prevUserHome })
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     filepath.Join(tmp, "work"),
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	want := filepath.Join(home, ".codex")
	unit := string(data)
	for _, envLine := range []string{
		"Environment=" + systemdQuoteArg("CODEX_HOME="+want),
		"Environment=" + systemdQuoteArg("CODEX_DIR="+want),
	} {
		if !strings.Contains(unit, envLine) {
			t.Fatalf("unit missing default Codex home env %q:\n%s", envLine, unit)
		}
	}
}

func TestTeamsServiceInstallDoesNotFailWhenDefaultCodexHomeCannotBeDerived(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	prevUserHome := effectivePathsUserHomeDir
	effectivePathsUserHomeDir = func() (string, error) { return "", errors.New("home unavailable") }
	t.Cleanup(func() { effectivePathsUserHomeDir = prevUserHome })
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     filepath.Join(tmp, "work"),
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("service install should not fail only because default Codex home cannot be derived: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	if strings.Contains(unit, "CODEX_HOME=") || strings.Contains(unit, "CODEX_DIR=") {
		t.Fatalf("unit should omit default Codex home env when it cannot be derived:\n%s", unit)
	}
	if !strings.Contains(unit, "CODEX_HELPER_TEAMS_SERVICE=1") {
		t.Fatalf("unit missing required helper service env:\n%s", unit)
	}
}

func TestTeamsServiceEnvironmentPreservesLoopbackProxyByDefault(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:38471")
	t.Setenv("HTTPS_PROXY", "http://localhost:38471")
	t.Setenv("ALL_PROXY", "socks5://[::1]:38471")
	t.Setenv("http_proxy", "http://127.0.0.1:38471")
	t.Setenv("https_proxy", "http://localhost:38471")
	t.Setenv("all_proxy", "socks5://[::1]:38471")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1,::1")
	t.Setenv("no_proxy", "localhost,127.0.0.1,::1")

	env := teamsServiceEnvironment()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if value := env[name]; value == "" {
			t.Fatalf("%s should be preserved for the background service, got %#v", name, env)
		}
	}
	if env["NO_PROXY"] == "" || env["no_proxy"] == "" {
		t.Fatalf("NO_PROXY values should be preserved, got %#v", env)
	}
}

func TestTeamsServiceEnvironmentPreservesModelProfileProviderKeys(t *testing.T) {
	t.Setenv("MIMO_API_KEY", "sk-mimo-test")
	t.Setenv("DEEPSEEK_API_KEY", "sk-deepseek-test")
	t.Setenv("UNRELATED_MODEL_KEY", "sk-ignored")

	env := teamsServiceEnvironment()
	if env["MIMO_API_KEY"] != "sk-mimo-test" {
		t.Fatalf("MIMO_API_KEY not preserved for Teams service model profiles: %#v", env)
	}
	if env["DEEPSEEK_API_KEY"] != "sk-deepseek-test" {
		t.Fatalf("DEEPSEEK_API_KEY not preserved for Teams service model profiles: %#v", env)
	}
	if env["UNRELATED_MODEL_KEY"] != "" {
		t.Fatalf("unexpected non-allowlisted model key preserved: %#v", env)
	}
}

func TestTeamsServiceInstallRendersModelProfileProviderKeysForBackgroundService(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_CONFIG", filepath.Join(tmp, "cxp-config.json"))
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "thirdparty-service")
	t.Setenv("DEEPSEEK_API_KEY", "sk-deepseek-background-test")
	t.Setenv("MIMO_API_KEY", "sk-mimo-background-test")
	t.Setenv("QWEN_API_KEY", "sk-qwen-background-test")
	t.Setenv("UNRELATED_MODEL_KEY", "sk-ignored")
	unitDir := filepath.Join(tmp, "systemd", "user")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	registryPath := filepath.Join(tmp, "teams-registry.json")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exePath,
		cwd:     tmp,
		unitDir: unitDir,
		runner:  &recordingTeamsServiceRunner{},
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(unitDir, teamsServiceUnitName))
	if err != nil {
		t.Fatalf("read unit file: %v", err)
	}
	unit := string(data)
	for _, want := range []string{
		"ExecStart=" + systemdQuoteArg(exePath) + " teams run --owner-stale-after 1m30s --auto-service=false --registry " + systemdQuoteArg(registryPath),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_CONFIG="+filepath.Join(tmp, "cxp-config.json")),
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_PROFILE=thirdparty-service"),
		"Environment=" + systemdQuoteArg("DEEPSEEK_API_KEY=sk-deepseek-background-test"),
		"Environment=" + systemdQuoteArg("MIMO_API_KEY=sk-mimo-background-test"),
		"Environment=" + systemdQuoteArg("QWEN_API_KEY=sk-qwen-background-test"),
		"Environment=CODEX_HELPER_TEAMS_SERVICE=1",
		"Environment=" + systemdQuoteArg("CODEX_HELPER_TEAMS_SERVICE_MODE=background"),
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing model-profile service entry %q:\n%s", want, unit)
		}
	}
	if strings.Contains(unit, "UNRELATED_MODEL_KEY") || strings.Contains(unit, "sk-ignored") {
		t.Fatalf("unit preserved non-allowlisted model key:\n%s", unit)
	}
}

func TestTeamsServiceEnvironmentMirrorsSingleCodexHomeEnv(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex home"))
	t.Setenv("CODEX_DIR", "")

	env, err := teamsServiceEnvironmentForWorkingDir(tmp)
	if err != nil {
		t.Fatalf("teamsServiceEnvironmentForWorkingDir: %v", err)
	}
	want := filepath.Join(tmp, "codex home")
	if env["CODEX_HOME"] != want || env["CODEX_DIR"] != want {
		t.Fatalf("Codex home env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", env["CODEX_HOME"], env["CODEX_DIR"], want)
	}
}

func TestTeamsServiceEnvironmentResolvesRelativeCodexDirEnv(t *testing.T) {
	tmp := t.TempDir()
	work := filepath.Join(tmp, "work")
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "relative-codex")

	env, err := teamsServiceEnvironmentForWorkingDir(work)
	if err != nil {
		t.Fatalf("teamsServiceEnvironmentForWorkingDir: %v", err)
	}
	want := filepath.Join(work, "relative-codex")
	if env["CODEX_HOME"] != want || env["CODEX_DIR"] != want {
		t.Fatalf("Codex home env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", env["CODEX_HOME"], env["CODEX_DIR"], want)
	}
}

func TestTeamsServiceEnvironmentPreservesConflictingExplicitCodexHomes(t *testing.T) {
	tmp := t.TempDir()
	home := filepath.Join(tmp, "codex-home")
	dir := filepath.Join(tmp, "codex-dir")
	t.Setenv("CODEX_HOME", home)
	t.Setenv("CODEX_DIR", dir)

	env, err := teamsServiceEnvironmentForWorkingDir(tmp)
	if err != nil {
		t.Fatalf("teamsServiceEnvironmentForWorkingDir: %v", err)
	}
	if env["CODEX_HOME"] != home || env["CODEX_DIR"] != dir {
		t.Fatalf("explicit Codex env should be preserved, got CODEX_HOME:%q CODEX_DIR:%q", env["CODEX_HOME"], env["CODEX_DIR"])
	}
}

func TestTeamsServiceEnvironmentCanDropLoopbackProxyWhenExplicit(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_DROP_LOCAL_PROXY", "1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:38471")
	t.Setenv("HTTPS_PROXY", "http://localhost:38471")
	t.Setenv("ALL_PROXY", "socks5://[::1]:38471")
	t.Setenv("http_proxy", "http://127.0.0.1:38471")
	t.Setenv("https_proxy", "http://localhost:38471")
	t.Setenv("all_proxy", "socks5://[::1]:38471")
	t.Setenv("NO_PROXY", "localhost,127.0.0.1,::1")
	t.Setenv("no_proxy", "localhost,127.0.0.1,::1")

	env := teamsServiceEnvironment()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if value := env[name]; value != "" {
			t.Fatalf("%s = %q, want dropped loopback proxy", name, value)
		}
	}
	if env["NO_PROXY"] == "" || env["no_proxy"] == "" {
		t.Fatalf("NO_PROXY values should be preserved, got %#v", env)
	}
}

func TestTeamsServiceEnvironmentCanKeepLoopbackProxyWhenExplicit(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_DROP_LOCAL_PROXY", "1")
	t.Setenv("CODEX_HELPER_TEAMS_KEEP_LOCAL_PROXY", "1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:38471")

	env := teamsServiceEnvironment()
	if got := env["HTTPS_PROXY"]; got != "http://127.0.0.1:38471" {
		t.Fatalf("HTTPS_PROXY = %q, want explicit loopback proxy preserved", got)
	}
}

func TestTeamsServiceWSLArgumentsUseExecToBypassLoginShell(t *testing.T) {
	lockCLITestHooks(t)

	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		wslDistro:    "Debian",
		wslLinuxUser: "baka",
	})
	spec := teamsServiceSpec{
		Executable: "/home/baka/.local/bin/codex-proxy",
		WorkingDir: "/home/baka",
		Environment: map[string]string{
			"CODEX_HELPER_TEAMS_SERVICE": "1",
			"NO_PROXY":                   "*.nvidia.com,nvidia.com,<local>",
		},
	}
	args := buildTeamsServiceWSLArguments(spec)
	joined := strings.Join(args, "\x00")
	if strings.Contains(joined, "\x00--\x00env\x00") {
		t.Fatalf("WSL service args should not route env through the login shell: %#v", args)
	}
	if !strings.Contains(joined, "\x00--exec\x00env\x00") {
		t.Fatalf("WSL service args should use --exec env to preserve glob proxy values: %#v", args)
	}
	if !slices.Contains(args, "NO_PROXY=*.nvidia.com,nvidia.com,<local>") {
		t.Fatalf("WSL service args should preserve raw NO_PROXY value for env: %#v", args)
	}
}

func TestTeamsServiceWSLTaskIdentityStableAcrossWorkingConfigAndCodexHome(t *testing.T) {
	lockCLITestHooks(t)

	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:         "linux",
		isWSL:        true,
		wslDistro:    "Debian",
		wslLinuxUser: "baka",
	})
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "default")
	t.Setenv("CODEX_HELPER_TEAMS_MACHINE_ID", "")
	t.Setenv("CODEX_HELPER_CONFIG", "/home/baka/project/a/config.json")
	t.Setenv("CODEX_HOME", "/home/baka/project/a/.codex")
	first := teamsServiceWSLTaskIdentity()

	t.Setenv("CODEX_HELPER_CONFIG", "/home/baka/project/b/config.json")
	t.Setenv("CODEX_HOME", "/home/baka/project/b/.codex")
	second := teamsServiceWSLTaskIdentity()

	if first.Suffix != second.Suffix || first.Display != second.Display {
		t.Fatalf("WSL task identity changed across cwd/config/codex home: first=%#v second=%#v", first, second)
	}
	if !strings.Contains(first.Display, "Debian baka default") {
		t.Fatalf("display = %q, want distro/user/profile", first.Display)
	}
}

func TestTeamsServiceWSLTaskNamePrefixIncludesTrailingSpaceBeforeSuffix(t *testing.T) {
	got := teamsServiceWSLTaskNamePrefix("Codex Helper Teams Bridge (WSL Debian baka default bd54a914bb9b)")
	want := "Codex Helper Teams Bridge (WSL Debian baka default "
	if got != want {
		t.Fatalf("prefix = %q, want %q", got, want)
	}
}

func TestTeamsServiceIsWSLFromSignalsRequiresInteropForKernelOnlyMatch(t *testing.T) {
	cases := []struct {
		name             string
		goos             string
		procVersion      string
		interopAvailable bool
		want             bool
	}{
		{
			name:             "real WSL signal with interop",
			goos:             "linux",
			procVersion:      "Linux version 6.6.87.2-microsoft-standard-WSL2",
			interopAvailable: true,
			want:             true,
		},
		{
			name:             "docker container on WSL kernel without interop",
			goos:             "linux",
			procVersion:      "Linux version 6.6.87.2-microsoft-standard-WSL2",
			interopAvailable: false,
			want:             false,
		},
		{
			name:             "ordinary linux",
			goos:             "linux",
			procVersion:      "Linux version 6.8.0-azure",
			interopAvailable: true,
			want:             false,
		},
		{
			name:             "windows host",
			goos:             "windows",
			procVersion:      "Linux version 6.6.87.2-microsoft-standard-WSL2",
			interopAvailable: true,
			want:             false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := teamsServiceIsWSLFromSignals(tc.goos, tc.procVersion, tc.interopAvailable); got != tc.want {
				t.Fatalf("isWSL = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestDefaultTeamsServiceIsWSLRequiresLinuxBeforeEnv(t *testing.T) {
	prevGOOS := teamsServiceGOOS
	t.Cleanup(func() { teamsServiceGOOS = prevGOOS })
	t.Setenv("WSL_DISTRO_NAME", "Ubuntu")
	t.Setenv("WSL_INTEROP", "/run/WSL/1_interop")

	teamsServiceGOOS = func() string { return "windows" }
	if got := defaultTeamsServiceIsWSL(); got {
		t.Fatalf("defaultTeamsServiceIsWSL on windows with WSL env = %t, want false", got)
	}

	teamsServiceGOOS = func() string { return "linux" }
	if got := defaultTeamsServiceIsWSL(); !got {
		t.Fatalf("defaultTeamsServiceIsWSL on linux with WSL env = %t, want true", got)
	}
}

func TestTeamsServiceSystemctlSubcommandsUseUserService(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{output: []byte("active\n")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	for _, tc := range []struct {
		name      string
		wantCalls []teamsServiceCommandCall
	}{
		{name: "enable", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "enable", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
		{name: "disable", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "disable", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
		{name: "status", wantCalls: []teamsServiceCommandCall{
			{name: "systemctl", args: []string{"--user", "status", "--no-pager", teamsServiceUnitName}},
			{name: "systemctl", args: []string{"--user", "status", "--no-pager", teamsServiceWatchdogUnitName}},
			{name: "systemctl", args: []string{"--user", "status", "--no-pager", teamsServiceWatchdogTimerName}},
		}},
		{name: "start", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "start", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
		{name: "stop", wantCalls: []teamsServiceCommandCall{
			{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogTimerName}},
			{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogUnitName}},
			{name: "systemctl", args: []string{"--user", "stop", teamsServiceUnitName}},
		}},
		{name: "restart", wantCalls: []teamsServiceCommandCall{{name: "systemctl", args: []string{"--user", "restart", teamsServiceUnitName, teamsServiceWatchdogUnitName, teamsServiceWatchdogTimerName}}}},
	} {
		runner.calls = nil
		cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
		cmd.SetArgs([]string{tc.name})
		var out bytes.Buffer
		cmd.SetOut(&out)
		if err := cmd.Execute(); err != nil {
			t.Fatalf("execute service %s: %v", tc.name, err)
		}
		if !reflect.DeepEqual(runner.calls, tc.wantCalls) {
			t.Fatalf("%s calls = %#v, want %#v", tc.name, runner.calls, tc.wantCalls)
		}
		if tc.name == "status" && !strings.Contains(out.String(), "active") {
			t.Fatalf("status should print systemctl output:\n%s", out.String())
		}
	}
}

func TestTeamsServiceSystemctlStopIgnoresMissingWatchdogUnits(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Unit codex-helper-teams-watchdog.timer not loaded.\n"),
			nil,
			[]byte("stopped main\n"),
		},
		errs: []error{
			errors.New("exit status 5"),
			errors.New("systemctl --user stop failed: unit could not be found"),
			nil,
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"stop"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service stop: %v\n%s", err, out.String())
	}
	wantCalls := []teamsServiceCommandCall{
		{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogTimerName}},
		{name: "systemctl", args: []string{"--user", "stop", teamsServiceWatchdogUnitName}},
		{name: "systemctl", args: []string{"--user", "stop", teamsServiceUnitName}},
	}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("stop calls = %#v, want %#v", runner.calls, wantCalls)
	}
	if strings.Contains(out.String(), "not loaded") || strings.Contains(out.String(), "could not be found") {
		t.Fatalf("missing watchdog errors should be suppressed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Stopped Teams service") {
		t.Fatalf("stop success output missing:\n%s", out.String())
	}
}

func TestTeamsServiceSystemctlStopStillFailsWhenPrimaryStopFails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		errs: []error{nil, nil, errors.New("exit status 5")},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"stop"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "exit status 5") {
		t.Fatalf("execute service stop error = %v, want primary stop failure\n%s", err, out.String())
	}
}

func TestTeamsServiceUninstallRemovesUnitAndReloads(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	unitDir := filepath.Join(tmp, "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o700); err != nil {
		t.Fatalf("mkdir unit dir: %v", err)
	}
	unitPath := filepath.Join(unitDir, teamsServiceUnitName)
	if err := os.WriteFile(unitPath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale unit: %v", err)
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: unitDir,
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetArgs([]string{"uninstall"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service uninstall: %v", err)
	}
	if _, err := os.Stat(unitPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected unit to be removed, stat err=%v", err)
	}
	wantCalls := []teamsServiceCommandCall{{
		name: "systemctl",
		args: []string{"--user", "daemon-reload"},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("systemctl calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestTeamsServiceUnsupportedPlatformInjection(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "freebsd",
		exe:     filepath.Join(tmp, "codex-proxy"),
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd", "user"),
		runner:  runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"install"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected unsupported platform error")
	}
	if !strings.Contains(err.Error(), `unsupported platform "freebsd"`) ||
		!strings.Contains(err.Error(), "Linux systemd --user, macOS LaunchAgent, and Windows per-user Task Scheduler") {
		t.Fatalf("unexpected unsupported error: %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("unsupported platform should not invoke systemctl, calls=%#v", runner.calls)
	}
}

func TestTeamsServiceInstallWritesMacOSLaunchAgentPlist(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	agentDir := filepath.Join(tmp, "LaunchAgents")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "darwin",
		exe:            exePath,
		cwd:            cwd,
		launchAgentDir: agentDir,
		userID:         "501",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	plistPath := filepath.Join(agentDir, teamsServiceLaunchAgentPlistName)
	data, err := os.ReadFile(plistPath)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	plist := string(data)
	for _, want := range []string{
		"<string>" + teamsServiceLaunchAgentLabel + "</string>",
		"<key>Disabled</key>",
		"<key>ProgramArguments</key>",
		"<string>" + exePath + "</string>",
		"<string>teams</string>",
		"<string>run</string>",
		"<string>--owner-stale-after</string>",
		"<string>1m30s</string>",
		"<string>--auto-service=false</string>",
		"<string>--registry</string>",
		"<string>" + registryPath + "</string>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
		"<key>CODEX_HELPER_TEAMS_SERVICE</key>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("plist missing %q:\n%s", want, plist)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatalf("install should only write plist, calls=%#v", runner.calls)
	}
	watchdogPlistPath := filepath.Join(agentDir, teamsServiceLaunchAgentWatchdogPlistName)
	watchdogData, err := os.ReadFile(watchdogPlistPath)
	if err != nil {
		t.Fatalf("read watchdog plist: %v", err)
	}
	watchdogPlist := string(watchdogData)
	for _, want := range []string{
		"<string>" + teamsServiceLaunchAgentWatchdogLabel + "</string>",
		"<string>teams</string>",
		"<string>service</string>",
		"<string>watchdog</string>",
		"<string>--loop</string>",
		"<string>--interval</string>",
		"<string>10s</string>",
		"<string>--quiet</string>",
		"<key>KeepAlive</key>",
		"<key>SuccessfulExit</key>",
		"<false/>",
	} {
		if !strings.Contains(watchdogPlist, want) {
			t.Fatalf("watchdog plist missing %q:\n%s", want, watchdogPlist)
		}
	}

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"start"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service start: %v", err)
	}
	wantCalls := []teamsServiceCommandCall{{
		name: "launchctl",
		args: []string{"bootstrap", "gui/501", plistPath},
	}, {
		name: "launchctl",
		args: []string{"bootstrap", "gui/501", watchdogPlistPath},
	}}
	if !reflect.DeepEqual(runner.calls, wantCalls) {
		t.Fatalf("launchctl start calls = %#v, want %#v", runner.calls, wantCalls)
	}
}

func TestTeamsServiceInstallWritesWindowsTaskXMLAndRegistersTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	xmlDir := filepath.Join(tmp, "config")
	exePath := filepath.Join(tmp, "bin", "codex-proxy.exe")
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exePath,
		cwd:            cwd,
		windowsTaskDir: xmlDir,
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}

	xmlPath := filepath.Join(xmlDir, teamsServiceWindowsTaskXMLName)
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		t.Fatalf("read task xml: %v", err)
	}
	taskXML := string(data)
	for _, want := range []string{
		"<LogonType>InteractiveToken</LogonType>",
		"<RunLevel>LeastPrivilege</RunLevel>",
		"<Enabled>false</Enabled>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<Hidden>true</Hidden>",
		"<Command>wscript.exe</Command>",
		"//B //Nologo",
		"codex-helper-teams-task.vbs",
		"<WorkingDirectory>" + cwd + "</WorkingDirectory>",
		"<RestartOnFailure>",
		"<Count>999</Count>",
	} {
		if !strings.Contains(taskXML, want) {
			t.Fatalf("task xml missing %q:\n%s", want, taskXML)
		}
	}
	watchdogXMLPath := filepath.Join(xmlDir, teamsServiceWindowsWatchdogTaskXMLName)
	watchdogData, err := os.ReadFile(watchdogXMLPath)
	if err != nil {
		t.Fatalf("read watchdog task xml: %v", err)
	}
	watchdogXML := string(watchdogData)
	for _, want := range []string{
		"<Description>Codex Helper Teams service watchdog</Description>",
		"<CalendarTrigger>",
		"<Interval>PT1M</Interval>",
		"<RestartOnFailure>",
		"<Interval>PT1M</Interval>",
		"<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>",
		"<Hidden>true</Hidden>",
		"<Command>wscript.exe</Command>",
		"//B //Nologo",
		"codex-helper-teams-watchdog-task.vbs",
	} {
		if !strings.Contains(watchdogXML, want) {
			t.Fatalf("watchdog task xml missing %q:\n%s", want, watchdogXML)
		}
	}
	for _, unexpected := range []string{"<Command>powershell.exe</Command>", "-WindowStyle Hidden -Command"} {
		if strings.Contains(taskXML, unexpected) || strings.Contains(watchdogXML, unexpected) {
			t.Fatalf("task XML should use no-console wscript launchers, found %q:\nbridge=%s\nwatchdog=%s", unexpected, taskXML, watchdogXML)
		}
	}
	launcherPS, err := os.ReadFile(filepath.Join(xmlDir, "codex-helper-teams-task.ps1"))
	if err != nil {
		t.Fatalf("read bridge launcher powershell: %v", err)
	}
	launcherVBS, err := os.ReadFile(filepath.Join(xmlDir, "codex-helper-teams-task.vbs"))
	if err != nil {
		t.Fatalf("read bridge launcher vbs: %v", err)
	}
	watchdogPS, err := os.ReadFile(filepath.Join(xmlDir, "codex-helper-teams-watchdog-task.ps1"))
	if err != nil {
		t.Fatalf("read watchdog launcher powershell: %v", err)
	}
	for _, want := range []string{
		"codex-helper\\teams",
		"starting hidden Teams helper process",
		"Start-Process -FilePath",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"& '" + exePath + "' 'teams' 'run' '--owner-stale-after' '1m30s' '--auto-service=false' '--registry' '" + registryPath + "'",
		"$code = $LASTEXITCODE",
		"exit $code",
		"$env:CODEX_HELPER_TEAMS_SERVICE = '1'",
	} {
		if !strings.Contains(string(launcherPS), want) {
			t.Fatalf("bridge launcher powershell missing %q:\n%s", want, string(launcherPS))
		}
	}
	if !strings.Contains(string(watchdogPS), "& '"+exePath+"' 'teams' 'service' 'watchdog' '--loop' '--interval' '10s' '--quiet'") ||
		!strings.Contains(string(watchdogPS), "$env:CODEX_HELPER_TEAMS_SERVICE = '1'") {
		t.Fatalf("watchdog launcher powershell missing expected command/env:\n%s", string(watchdogPS))
	}
	if !strings.Contains(string(launcherVBS), "shell.Run(cmd, 0, True)") || !strings.Contains(string(launcherVBS), "-WindowStyle Hidden -File") {
		t.Fatalf("bridge launcher vbs should run PowerShell hidden:\n%s", string(launcherVBS))
	}
	if len(runner.calls) != 2 || runner.calls[0].name != "powershell.exe" || runner.calls[1].name != "powershell.exe" {
		t.Fatalf("install should cleanup then register task with powershell, calls=%#v", runner.calls)
	}
	cleanup := strings.Join(runner.calls[0].args, " ")
	if !strings.Contains(cleanup, "$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity") ||
		!strings.Contains(cleanup, "Stop-Process -Id $proc.ProcessId -Force") ||
		strings.Contains(cleanup, "Register-ScheduledTask") {
		t.Fatalf("install should cleanup old bridge children before registering task, cleanup=%s", cleanup)
	}
	joined := strings.Join(runner.calls[1].args, " ")
	if !strings.Contains(joined, "Register-ScheduledTask") {
		t.Fatalf("install should register task with powershell, calls=%#v", runner.calls)
	}
	if !strings.Contains(joined, teamsServiceWindowsWatchdogTaskName) {
		t.Fatalf("install should register watchdog task too, call=%s", joined)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask", "Enable-ScheduledTask")

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"enable"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service enable: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Enable-ScheduledTask") {
		t.Fatalf("enable should call powershell scheduled task command, calls=%#v", runner.calls)
	}
	if joined := strings.Join(runner.calls[0].args, " "); !strings.Contains(joined, teamsServiceWindowsTaskName) || !strings.Contains(joined, teamsServiceWindowsWatchdogTaskName) {
		t.Fatalf("enable should include main and watchdog tasks, call=%s", joined)
	}

	runner.calls = nil
	cmd = newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetArgs([]string{"uninstall"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service uninstall: %v", err)
	}
	for _, path := range []string{
		xmlPath,
		watchdogXMLPath,
		filepath.Join(xmlDir, "codex-helper-teams-task.ps1"),
		filepath.Join(xmlDir, "codex-helper-teams-task.vbs"),
		filepath.Join(xmlDir, "codex-helper-teams-watchdog-task.ps1"),
		filepath.Join(xmlDir, "codex-helper-teams-watchdog-task.vbs"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("uninstall should remove %s, stat err=%v", path, err)
		}
	}
}

func TestTeamsServiceRepairCleansWindowsBridgeChildrenBeforeTaskRewrite(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "bin", "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work dir"),
		windowsTaskDir: filepath.Join(tmp, "windows-task"),
		runner:         runner,
	})

	if _, err := repairTeamsService(context.Background(), stringPtr(filepath.Join(tmp, "teams registry.json")), teamsServiceRepairOptions{Enable: true}); err != nil {
		t.Fatalf("repairTeamsService error: %v", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("repair calls = %#v, want cleanup, register, enable", runner.calls)
	}
	cleanup := strings.Join(runner.calls[0].args, " ")
	for _, want := range []string{
		"$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity",
		"$expectedBridgeArgsForCleanup",
		"Get-CimInstance Win32_Process",
		"$proc.ProcessId -ne $PID",
		"Stop-Process -Id $proc.ProcessId -Force",
	} {
		if !strings.Contains(cleanup, want) {
			t.Fatalf("repair cleanup command missing %q:\n%s", want, cleanup)
		}
	}
	if strings.Contains(cleanup, "Register-ScheduledTask") || strings.Contains(cleanup, "$name -ieq 'codex-proxy.exe'") || strings.Contains(cleanup, "$name -ieq 'cxp.exe'") {
		t.Fatalf("repair cleanup should use old task identity and fail closed:\n%s", cleanup)
	}
	allJoined := teamsServiceJoinedCalls(runner.calls)
	requireSubstringsInOrder(t, allJoined,
		"$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity",
		"Register-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
		"Register-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
		"Enable-ScheduledTask -TaskName "+powershellSingleQuote(teamsServiceWindowsTaskName),
	)
}

func TestTeamsServiceWindowsPowerShellPropagatesChildExitCodeCI(t *testing.T) {
	shell, err := exec.LookPath("pwsh")
	if err != nil {
		shell, err = exec.LookPath("powershell.exe")
	}
	if err != nil {
		t.Skip("PowerShell is not available")
	}

	tmp := t.TempDir()
	child := filepath.Join(tmp, "exit-37")
	if runtime.GOOS == "windows" {
		child += ".cmd"
		if err := os.WriteFile(child, []byte("@echo visible stdout\r\n@echo visible stderr 1>&2\r\n@exit /b 37\r\n"), 0o700); err != nil {
			t.Fatalf("write child command: %v", err)
		}
	} else {
		if err := os.WriteFile(child, []byte("#!/bin/sh\necho visible stdout\necho visible stderr >&2\nexit 37\n"), 0o700); err != nil {
			t.Fatalf("write child command: %v", err)
		}
	}
	script := buildTeamsServiceWindowsPowerShell(teamsServiceSpec{
		Executable:  child,
		Environment: map[string]string{"CODEX_HELPER_TEAMS_SERVICE": "1"},
	}, []string{"teams", "run"})
	cmd := exec.Command(shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", script)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("PowerShell error = %T %v, want exit error\noutput:\n%s", err, err, string(out))
	}
	if got := exitErr.ExitCode(); got != 37 {
		t.Fatalf("PowerShell exit code = %d, want child exit code 37\noutput:\n%s", got, string(out))
	}
	if strings.TrimSpace(string(out)) != "" {
		t.Fatalf("PowerShell leaked child output instead of redirecting it:\n%s", string(out))
	}
}

func TestTeamsServiceWindowsStartTasksRefreshesTaskState(t *testing.T) {
	got := teamsServiceWindowsStartTasksPowerShell()
	requireSubstringsInOrder(t, got,
		"function Test-CodexHelperScheduledTaskRunning",
		"function Start-CodexHelperScheduledTaskIfStopped",
		"try { Start-ScheduledTask -TaskName $taskName -ErrorAction Stop | Out-Null } catch",
		"if (-not (Test-CodexHelperScheduledTaskRunning $taskName 10)) { throw }",
		"Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsTaskName),
		"Start-CodexHelperScheduledTaskIfStopped "+powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
	)
	if strings.Contains(got, "$watchdogTask.State -ne 'Running') { Start-ScheduledTask") {
		t.Fatalf("watchdog start must not use stale task state:\n%s", got)
	}
}

func TestTeamsServiceWSLStartTasksRefreshTaskState(t *testing.T) {
	for name, got := range map[string]string{
		"start":            teamsServiceWSLStartTaskAndVerifyPowerShell(),
		"start_if_stopped": teamsServiceWSLStartTaskIfStoppedAndVerifyPowerShell(),
	} {
		t.Run(name, func(t *testing.T) {
			requireSubstringsInOrder(t, got,
				"function Test-CodexHelperScheduledTaskRunning",
				"function Start-CodexHelperScheduledTaskIfStopped",
				"try { Start-ScheduledTask -TaskName $taskName -ErrorAction Stop | Out-Null } catch",
				"if (-not (Test-CodexHelperScheduledTaskRunning $taskName 10)) { throw }",
				"Start-CodexHelperScheduledTaskIfStopped $taskName",
				"Teams WSL Scheduled Task did not stay running after start",
			)
			if strings.Contains(got, "$task.State -ne 'Running') { Start-ScheduledTask -TaskName $taskName") {
				t.Fatalf("WSL start must not use stale task state:\n%s", got)
			}
		})
	}
}

func TestTeamsServiceBootstrapShowsControlChatInForeground(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	var openValues []bool
	cwd := filepath.Join(tmp, "work dir")
	registryPath := filepath.Join(tmp, "teams registry.json")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            cwd,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, gotRegistry *string, openControl bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			openValues = append(openValues, openControl)
			if gotRegistry == nil || *gotRegistry != registryPath {
				got := "<nil>"
				if gotRegistry != nil {
					got = *gotRegistry
				}
				t.Fatalf("registry path = %q, want %q", got, registryPath)
			}
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
				Opened: openControl,
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	if len(openValues) != 1 || !openValues[0] {
		t.Fatalf("bootstrap control open values = %#v, want one true", openValues)
	}
	got := out.String()
	for _, want := range []string{
		"BOOTSTRAP COMPLETE",
		"Teams service bootstrap ready: wsl-windows-task-scheduler",
		"NEXT STEP: OPEN THE TEAMS CONTROL CHAT",
		"Open:",
		"https://teams.microsoft.com/l/chat/control",
		"Then send:",
		"help",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("bootstrap output missing %q:\n%s", want, got)
		}
	}
	completeIdx := strings.Index(got, "BOOTSTRAP COMPLETE")
	nextIdx := strings.Index(got, "NEXT STEP: OPEN THE TEAMS CONTROL CHAT")
	if completeIdx < 0 || nextIdx < 0 || completeIdx > nextIdx {
		t.Fatalf("bootstrap should put the user's next step last:\n%s", got)
	}
	for _, unwanted := range []string{
		"Title: 🏠 Codex Control - workstation",
		"I also tried to open it automatically.",
	} {
		if strings.Contains(got, unwanted) {
			t.Fatalf("bootstrap next-step output should not mix in secondary info %q:\n%s", unwanted, got)
		}
	}
}

func TestTeamsServiceBootstrapPreparesControlChatWithServiceCodexHomeEnv(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	prevUserHome := effectivePathsUserHomeDir
	effectivePathsUserHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { effectivePathsUserHomeDir = prevUserHome })
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "")

	runner := &recordingTeamsServiceRunner{}
	var gotHome string
	var gotDir string
	var gotService string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     filepath.Join(tmp, "work"),
		unitDir: filepath.Join(tmp, "systemd-user"),
		runner:  runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			gotHome = os.Getenv("CODEX_HOME")
			gotDir = os.Getenv("CODEX_DIR")
			gotService = os.Getenv("CODEX_HELPER_TEAMS_SERVICE")
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	wantCodexHome := filepath.Join(home, ".codex")
	if gotHome != wantCodexHome || gotDir != wantCodexHome {
		t.Fatalf("bootstrap control chat Codex env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", gotHome, gotDir, wantCodexHome)
	}
	if gotService != "1" {
		t.Fatalf("bootstrap control chat CODEX_HELPER_TEAMS_SERVICE = %q, want 1", gotService)
	}
	if os.Getenv("CODEX_HOME") != "" || os.Getenv("CODEX_DIR") != "" {
		t.Fatalf("bootstrap control chat should restore caller Codex env, got CODEX_HOME:%q CODEX_DIR:%q", os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR"))
	}
}

func TestTeamsServiceBootstrapPreparesControlChatWithRelativeCodexDirEnv(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cwd := filepath.Join(tmp, "work")
	t.Setenv("CODEX_HOME", "")
	t.Setenv("CODEX_DIR", "relative-codex")

	runner := &recordingTeamsServiceRunner{}
	var gotHome string
	var gotDir string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     cwd,
		unitDir: filepath.Join(tmp, "systemd-user"),
		runner:  runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			gotHome = os.Getenv("CODEX_HOME")
			gotDir = os.Getenv("CODEX_DIR")
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	wantCodexHome := filepath.Join(cwd, "relative-codex")
	if gotHome != wantCodexHome || gotDir != wantCodexHome {
		t.Fatalf("bootstrap control chat Codex env = CODEX_HOME:%q CODEX_DIR:%q, want both %q", gotHome, gotDir, wantCodexHome)
	}
	if os.Getenv("CODEX_HOME") != "" || os.Getenv("CODEX_DIR") != "relative-codex" {
		t.Fatalf("bootstrap control chat should restore caller Codex env, got CODEX_HOME:%q CODEX_DIR:%q", os.Getenv("CODEX_HOME"), os.Getenv("CODEX_DIR"))
	}
}

func TestTeamsServiceBootstrapPreparesControlChatWithResolvedRegistryPath(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	cwd := filepath.Join(tmp, "work dir")
	registryPath := "teams registry.json"
	runner := &recordingTeamsServiceRunner{}
	var gotRegistry string
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:     cwd,
		unitDir: filepath.Join(tmp, "systemd-user"),
		runner:  runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, gotRegistryPath *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			if gotRegistryPath != nil {
				gotRegistry = *gotRegistryPath
			}
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	wantRegistry := filepath.Join(cwd, registryPath)
	if gotRegistry != wantRegistry {
		t.Fatalf("bootstrap control chat registry path = %q, want %q", gotRegistry, wantRegistry)
	}
}

func TestTeamsServiceBootstrapSuppressesControlChatDiagnosticOutput(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	var sawDiscardedDiagnostic bool
	registryPath := "/home/alice/teams registry.json"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(ctx context.Context, root *rootOptions, gotRegistryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			_, _ = fmt.Fprintln(errOut, "Teams Graph proxy: using http://127.0.0.1:44438")
			sawDiscardedDiagnostic = true
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "🏠 Codex Control - workstation",
				ChatID: "control-chat",
				Opened: true,
			}, nil
		},
	})

	var out strings.Builder
	var errOut strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s\nstderr:\n%s", err, out.String(), errOut.String())
	}
	if !sawDiscardedDiagnostic {
		t.Fatal("bootstrap control chat hook was not called")
	}
	if got := out.String() + errOut.String(); strings.Contains(got, "Teams Graph proxy: using") {
		t.Fatalf("bootstrap should suppress control chat diagnostics, got:\n%s", got)
	}
}

func TestTeamsServiceBootstrapPrintsControlChatRecoveryAsFinalStep(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	registryPath := "/home/alice/teams registry.json"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(ctx context.Context, root *rootOptions, gotRegistryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			return teamsServiceBootstrapControlChatResult{}, errors.New("Teams Graph proxy is enabled but no unambiguous proxy profile is configured")
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		"BOOTSTRAP COMPLETE",
		"NEXT STEP: PRINT THE TEAMS CONTROL CHAT LINK",
		"Reason: Teams Graph proxy is enabled",
		"codex-proxy teams control",
		"Then open the printed link and send:",
		"help",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("bootstrap output missing %q:\n%s", want, got)
		}
	}
	completeIdx := strings.Index(got, "BOOTSTRAP COMPLETE")
	nextIdx := strings.Index(got, "NEXT STEP: PRINT THE TEAMS CONTROL CHAT LINK")
	if completeIdx < 0 || nextIdx < 0 || completeIdx > nextIdx {
		t.Fatalf("control chat recovery should be the final next-step block:\n%s", got)
	}
}

func TestTeamsServiceBootstrapWindowsAccessDeniedPromptsForUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	taskDir := filepath.Join(tmp, "tasks")
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("Register-ScheduledTask : Access is denied.\n"),
			[]byte("NVIDIA\\jason\n"),
			nil,
			nil,
		},
		errs: []error{
			nil,
			errors.New("exit status 1"),
			nil,
			nil,
			nil,
		},
		onRun: func(index int, _ string, _ []string) {
			if index == 3 {
				assertTeamsServiceWindowsElevatedScriptFile(t, taskDir, true, false)
			}
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work"),
		windowsTaskDir: taskDir,
		runner:         runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			return teamsServiceBootstrapControlChatResult{
				URL:    "https://teams.microsoft.com/l/chat/control",
				Topic:  "Codex Control - workstation",
				ChatID: "control-chat",
			}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetIn(strings.NewReader("yes\n"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	gotOut := out.String()
	for _, want := range []string{
		"NEXT STEP: TYPE yes TO CONTINUE",
		"Windows needs permission to create or repair the current-user Scheduled Task.",
		"Teams service bootstrap ready: windows-task-scheduler-uac",
	} {
		if !strings.Contains(gotOut, want) {
			t.Fatalf("bootstrap output missing %q:\n%s", want, gotOut)
		}
	}
	if len(runner.calls) != 5 {
		t.Fatalf("PowerShell calls = %#v, want cleanup, failed register, current-user lookup, elevated repair, and normal start", runner.calls)
	}
	elevated := strings.Join(runner.calls[3].args, " ")
	assertTeamsServiceWindowsElevatedScriptCommand(t, elevated)
	start := strings.Join(runner.calls[4].args, " ")
	for _, want := range []string{
		"Enable-ScheduledTask",
		"Start-CodexHelperScheduledTaskIfStopped " + powershellSingleQuote(teamsServiceWindowsTaskName),
		"Start-CodexHelperScheduledTaskIfStopped " + powershellSingleQuote(teamsServiceWindowsWatchdogTaskName),
	} {
		if !strings.Contains(start, want) {
			t.Fatalf("post-UAC start command missing %q:\n%s", want, start)
		}
	}
	for _, forbidden := range []string{"RunLevel Highest", "HighestAvailable", "NT AUTHORITY\\SYSTEM", "-UserId 'SYSTEM'", "LogonType Password"} {
		if strings.Contains(elevated, forbidden) || strings.Contains(start, forbidden) {
			t.Fatalf("UAC repair/start must stay current-user least-privilege, found %q:\nelevated=%s\nstart=%s", forbidden, elevated, start)
		}
	}
	for _, path := range []string{
		filepath.Join(taskDir, teamsServiceWindowsTaskXMLName),
		filepath.Join(taskDir, teamsServiceWindowsWatchdogTaskXMLName),
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read task XML %s: %v", path, err)
		}
		xml := string(data)
		for _, want := range []string{
			"<UserId>NVIDIA\\jason</UserId>",
			"<LogonType>InteractiveToken</LogonType>",
			"<RunLevel>LeastPrivilege</RunLevel>",
		} {
			if !strings.Contains(xml, want) {
				t.Fatalf("task XML %s missing %q:\n%s", path, want, xml)
			}
		}
	}
}

func TestTeamsServiceBootstrapWindowsAccessDeniedNoUACFailsWithoutPrompt(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("Register-ScheduledTask : Access is denied.\n"),
		},
		errs: []error{
			nil,
			errors.New("exit status 1"),
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work"),
		windowsTaskDir: filepath.Join(tmp, "tasks"),
		runner:         runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, _ bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			return teamsServiceBootstrapControlChatResult{}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&out)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control", "--no-uac"})
	err := cmd.Execute()
	if err == nil {
		t.Fatalf("bootstrap --no-uac error = nil, want access-denied failure\noutput:\n%s", out.String())
	}
	if !strings.Contains(err.Error(), "UAC is disabled by --no-uac") {
		t.Fatalf("bootstrap --no-uac error = %v, want no-uac access-denied explanation", err)
	}
	if strings.Contains(out.String(), "NEXT STEP: TYPE yes TO CONTINUE") || strings.Contains(out.String(), "Type yes and press Enter") {
		t.Fatalf("bootstrap --no-uac must not prompt for UAC:\n%s", out.String())
	}
	if len(runner.calls) != 2 {
		t.Fatalf("PowerShell calls = %#v, want cleanup and failed register only", runner.calls)
	}
}

func TestTeamsServiceBootstrapNoStartWindowsAccessDeniedPromptsForUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	taskDir := filepath.Join(tmp, "tasks")
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("Register-ScheduledTask : Access is denied.\n"),
			[]byte("NVIDIA\\jason\n"),
			nil,
		},
		errs: []error{
			nil,
			errors.New("exit status 1"),
			nil,
			nil,
		},
		onRun: func(index int, _ string, _ []string) {
			if index == 3 {
				assertTeamsServiceWindowsElevatedScriptFile(t, taskDir, true, false)
			}
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work"),
		windowsTaskDir: taskDir,
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetIn(strings.NewReader("yes\n"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-start"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap --no-start error: %v\noutput:\n%s", err, out.String())
	}
	gotOut := out.String()
	for _, want := range []string{
		"NEXT STEP: TYPE yes TO CONTINUE",
		"Teams service bootstrap ready: windows-task-scheduler-uac-no-start",
		"Service was repaired but not started.",
	} {
		if !strings.Contains(gotOut, want) {
			t.Fatalf("bootstrap --no-start output missing %q:\n%s", want, gotOut)
		}
	}
	if len(runner.calls) != 4 {
		t.Fatalf("PowerShell calls = %#v, want cleanup, failed register, current-user lookup, and elevated repair", runner.calls)
	}
	elevated := strings.Join(runner.calls[3].args, " ")
	assertTeamsServiceWindowsElevatedScriptCommand(t, elevated)
}

func TestTeamsServiceInstallWindowsAccessDeniedPromptsForUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	taskDir := filepath.Join(tmp, "tasks")
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("Register-ScheduledTask : Access is denied.\n"),
			[]byte("NVIDIA\\jason\n"),
			nil,
		},
		errs: []error{
			nil,
			errors.New("exit status 1"),
			nil,
			nil,
		},
		onRun: func(index int, _ string, _ []string) {
			if index == 3 {
				assertTeamsServiceWindowsElevatedScriptFile(t, taskDir, false, false)
			}
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work"),
		windowsTaskDir: taskDir,
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetIn(strings.NewReader("yes\n"))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("install error: %v\noutput:\n%s", err, out.String())
	}
	gotOut := out.String()
	for _, want := range []string{
		"NEXT STEP: TYPE yes TO CONTINUE",
		"Installed Teams service config:",
		"Service was not enabled or started automatically.",
	} {
		if !strings.Contains(gotOut, want) {
			t.Fatalf("install output missing %q:\n%s", want, gotOut)
		}
	}
	if len(runner.calls) != 4 {
		t.Fatalf("PowerShell calls = %#v, want cleanup, failed register, current-user lookup, and elevated install", runner.calls)
	}
	elevated := strings.Join(runner.calls[3].args, " ")
	assertTeamsServiceWindowsElevatedScriptCommand(t, elevated)
}

func TestTeamsServiceInstallWindowsAccessDeniedYesUsesUACWithoutPrompt(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	taskDir := filepath.Join(tmp, "tasks")
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("Register-ScheduledTask : Access is denied.\n"),
			[]byte("NVIDIA\\jason\n"),
			nil,
		},
		errs: []error{
			nil,
			errors.New("exit status 1"),
			nil,
			nil,
		},
		onRun: func(index int, _ string, _ []string) {
			if index == 3 {
				assertTeamsServiceWindowsElevatedScriptFile(t, taskDir, false, false)
			}
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work"),
		windowsTaskDir: taskDir,
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"install", "--yes"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("install --yes error: %v\noutput:\n%s", err, out.String())
	}
	gotOut := out.String()
	for _, want := range []string{
		"UAC prompt approved by --yes.",
		"Installed Teams service config:",
	} {
		if !strings.Contains(gotOut, want) {
			t.Fatalf("install --yes output missing %q:\n%s", want, gotOut)
		}
	}
	if strings.Contains(gotOut, "Type yes and press Enter") {
		t.Fatalf("install --yes should not require stdin confirmation:\n%s", gotOut)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("PowerShell calls = %#v, want cleanup, failed register, current-user lookup, and elevated install", runner.calls)
	}
	elevated := strings.Join(runner.calls[3].args, " ")
	assertTeamsServiceWindowsElevatedScriptCommand(t, elevated)
}

func TestTeamsServiceInstallWindowsCleanupAccessDeniedUsesUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	taskDir := filepath.Join(tmp, "tasks")
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			[]byte("Stop-ScheduledTask : Access is denied.\n"),
			[]byte("NVIDIA\\jason\n"),
			nil,
		},
		errs: []error{
			errors.New("exit status 1"),
			nil,
			nil,
		},
		onRun: func(index int, _ string, _ []string) {
			if index == 2 {
				assertTeamsServiceWindowsElevatedScriptFile(t, taskDir, false, false)
			}
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		cwd:            filepath.Join(tmp, "work"),
		windowsTaskDir: taskDir,
		runner:         runner,
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
	cmd.SetIn(strings.NewReader(""))
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"install", "--yes"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("install --yes with cleanup access denied error: %v\noutput:\n%s", err, out.String())
	}
	if len(runner.calls) != 3 {
		t.Fatalf("PowerShell calls = %#v, want failed cleanup, current-user lookup, and elevated install", runner.calls)
	}
	elevated := strings.Join(runner.calls[2].args, " ")
	assertTeamsServiceWindowsElevatedScriptCommand(t, elevated)
}

func TestTeamsServiceBootstrapErrorSummaryKeepsFailuresReadable(t *testing.T) {
	longPowerShellError := strings.Repeat("Register-ScheduledTask failed with noisy diagnostic details. ", 20)
	got := teamsServiceBootstrapErrorSummary(errors.New(longPowerShellError))
	if strings.Contains(got, "\n") {
		t.Fatalf("summary should be single-line, got %q", got)
	}
	if len(got) > 323 {
		t.Fatalf("summary should be bounded, got len=%d text=%q", len(got), got)
	}
	if !strings.HasSuffix(got, "...") {
		t.Fatalf("long summary should be truncated, got %q", got)
	}

	accessDenied := teamsServiceBootstrapErrorSummary(errors.New("Register-ScheduledTask : Access is denied.\nAt line:1 char:1\nlarge command"))
	if strings.Contains(accessDenied, "Register-ScheduledTask") || !strings.Contains(accessDenied, "Windows denied permission") {
		t.Fatalf("access denied should be summarized without raw PowerShell noise, got %q", accessDenied)
	}
}

func TestTeamsServiceWindowsElevatedScriptCapturesInnerErrors(t *testing.T) {
	script := buildTeamsServiceWindowsElevatedScript(
		"Register-ScheduledTask -TaskName 'Codex Helper Teams Bridge'; Start-ScheduledTask -TaskName 'Codex Helper Teams Bridge'",
		`C:\Users\alice\AppData\Local\Temp\codex-uac\stdout.log`,
		`C:\Users\alice\AppData\Local\Temp\codex-uac\stderr.log`,
	)
	for _, want := range []string{
		"$ErrorActionPreference = 'Stop'",
		"Register-ScheduledTask -TaskName 'Codex Helper Teams Bridge'",
		"Start-ScheduledTask -TaskName 'Codex Helper Teams Bridge'",
		"Set-Content -LiteralPath $stderrLog -Value $message -Encoding UTF8",
		"exit 1",
		"exit 0",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("elevated script missing %q:\n%s", want, script)
		}
	}

	launcher := buildTeamsServiceWindowsElevatedScriptCommand(
		`C:\Users\alice\AppData\Local\Temp\codex-uac\elevated.ps1`,
		`C:\Users\alice\AppData\Local\Temp\codex-uac\stdout.log`,
		`C:\Users\alice\AppData\Local\Temp\codex-uac\stderr.log`,
	)
	for _, want := range []string{
		"Start-Process",
		"-Verb RunAs",
		"-File",
		`C:\Users\alice\AppData\Local\Temp\codex-uac\elevated.ps1`,
		`C:\Users\alice\AppData\Local\Temp\codex-uac\stdout.log`,
		`C:\Users\alice\AppData\Local\Temp\codex-uac\stderr.log`,
		"Get-Content -LiteralPath $uacLog -Raw",
		"elevated Teams service bootstrap failed with exit code",
	} {
		if !strings.Contains(launcher, want) {
			t.Fatalf("elevated launcher missing %q:\n%s", want, launcher)
		}
	}
	for _, forbidden := range []string{"Register-ScheduledTask", "Start-ScheduledTask -TaskName"} {
		if strings.Contains(launcher, forbidden) {
			t.Fatalf("elevated launcher should not inline inner command %q:\n%s", forbidden, launcher)
		}
	}
}

func TestTeamsServiceWindowsAccessDeniedErrorMatrix(t *testing.T) {
	accessDenied := []string{
		"Register-ScheduledTask : Access is denied.",
		"powershell.exe -Command Register-ScheduledTask failed: exit status 1\nRegister-ScheduledTask : Access is denied.\nAt line:1 char:1\n+ Register-ScheduledTask ...",
		"Register-ScheduledTask : Permission denied.",
		"UnauthorizedAccessException: Access to the path is denied.",
		"FullyQualifiedErrorId : AccessDenied",
		"Exception from HRESULT: 0x80070005 (E_ACCESSDENIED)",
		"Register-ScheduledTask : 拒绝访问。",
		"Register-ScheduledTask : 存取被拒。",
		"Register-ScheduledTask : Zugriff verweigert.",
		"Register-ScheduledTask : Acceso denegado.",
		"Register-ScheduledTask : Accès refusé.",
		"Register-ScheduledTask : Accesso negato.",
	}
	for _, sample := range accessDenied {
		if !isTeamsServiceWindowsAccessDeniedError(errors.New(sample)) {
			t.Fatalf("access denied sample was not classified:\n%s", sample)
		}
	}

	unknown := []string{
		"Register-ScheduledTask failed: exit status 1",
		"powershell.exe -Command \"if (-not $actionMatches) { throw 'Teams WSL Scheduled Task action did not refresh; access is denied or task is protected' }\" failed: exit status 1",
		"Register-ScheduledTask : The task XML contains a value which is incorrectly formatted.",
		"Get-ScheduledTask : The system cannot find the file specified.",
	}
	for _, sample := range unknown {
		if isTeamsServiceWindowsAccessDeniedError(errors.New(sample)) {
			t.Fatalf("unknown Windows Scheduled Task error was incorrectly classified as access denied:\n%s", sample)
		}
	}
}

func TestTeamsServiceWindowsScheduledTasksUnavailableErrorMatrix(t *testing.T) {
	unavailable := []string{
		"Register-ScheduledTask : The term 'Register-ScheduledTask' is not recognized as the name of a cmdlet",
		"powershell.exe -Command Register-ScheduledTask failed: exit status 1\nRegister-ScheduledTask : The term 'Register-ScheduledTask' is not recognized as the name of a cmdlet.\nAt line:1 char:1\n+ Register-ScheduledTask ...\nCommandNotFoundException",
		"Get-ScheduledTask : The term 'Get-ScheduledTask' is not recognized as the name of a cmdlet",
		"The module 'ScheduledTasks' could not be loaded. For more information, run 'Import-Module ScheduledTasks'.",
		"Import-Module : The specified module 'ScheduledTasks' was not loaded because no valid module file was found",
	}
	for _, sample := range unavailable {
		if !isTeamsServiceWindowsScheduledTasksUnavailableError(errors.New(sample)) {
			t.Fatalf("ScheduledTasks unavailable sample was not classified:\n%s", sample)
		}
	}

	unknown := []string{
		"Register-ScheduledTask failed: exit status 1",
		"Register-ScheduledTask : The task XML contains a value which is incorrectly formatted.",
		"Register-ScheduledTask : Access is denied.",
	}
	for _, sample := range unknown {
		if isTeamsServiceWindowsScheduledTasksUnavailableError(errors.New(sample)) {
			t.Fatalf("unknown Windows Scheduled Task error was incorrectly classified as cmdlet unavailable:\n%s", sample)
		}
	}
}

func TestTeamsServiceBootstrapCanSkipAutomaticControlChatOpen(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	var openValues []bool
	registryPath := "/home/alice/teams registry.json"
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		bootstrapControlChat: func(_ context.Context, _ *rootOptions, _ *string, openControl bool, _ io.Writer) (teamsServiceBootstrapControlChatResult, error) {
			openValues = append(openValues, openControl)
			return teamsServiceBootstrapControlChatResult{URL: "https://teams.microsoft.com/l/chat/control"}, nil
		},
	})

	var out strings.Builder
	cmd := newTeamsServiceCmd(&rootOptions{}, &registryPath)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"bootstrap", "--no-open-control"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap error: %v\noutput:\n%s", err, out.String())
	}
	if len(openValues) != 1 || openValues[0] {
		t.Fatalf("bootstrap control open values = %#v, want one false", openValues)
	}
	if got := out.String(); !strings.Contains(got, "NEXT STEP: OPEN THE TEAMS CONTROL CHAT") || !strings.Contains(got, "https://teams.microsoft.com/l/chat/control") || strings.Contains(got, "I also tried to open it automatically") {
		t.Fatalf("bootstrap --no-open-control output is wrong:\n%s", got)
	}
}

func TestTeamsServiceBootstrapSchedulesPendingHelperActivationBeforeStartingOldWindowsEntry(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.73_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
		teamsServiceStartDetached = prevDetached
	})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		if filepath.Clean(path) != filepath.Clean(exe) || goos != "windows" {
			t.Fatalf("FindPendingReplacements path/goos = %q/%q, want %q/windows", path, goos, exe)
		}
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.73", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.68"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.73"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exe,
		windowsTaskDir: filepath.Join(tmp, "tasks"),
		runner:         runner,
	})

	result, err := bootstrapTeamsService(context.Background(), nil, teamsServiceBootstrapOptions{})
	if err != nil {
		t.Fatalf("bootstrapTeamsService error: %v", err)
	}
	if result.Mode != "windows-pending-helper-activation" || !strings.HasSuffix(result.Path, teamsServiceWindowsTaskXMLName) {
		t.Fatalf("bootstrap result = %#v, want pending activation", result)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("PowerShell calls = %#v, want cleanup, install/register, and enable only before activation", runner.calls)
	}
	joinedCalls := teamsServiceJoinedCalls(runner.calls)
	requireSubstringsInOrder(t, joinedCalls,
		"$bridgeIdentityForCleanup = Get-CodexHelperBridgeTaskIdentity",
		"Register-ScheduledTask",
		"Enable-ScheduledTask",
	)
	if !strings.Contains(joinedCalls, "Register-ScheduledTask") || !strings.Contains(joinedCalls, "Enable-ScheduledTask") {
		t.Fatalf("bootstrap should repair and enable tasks before pending activation, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask")
	if detachedName == "" {
		t.Fatal("pending activation did not schedule detached PowerShell")
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		exe,
		teamsServiceWindowsTaskName,
		teamsServiceWindowsWatchdogTaskName,
		"$want='0.1.0-rc.73'",
		".activation.json",
		"Move-Item -Force",
		"if (Test-DestVersion) { $ready=$true }",
		"Write-Status 'failed'",
		"Start-CodexHelperScheduledTaskIfStopped",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("activation command missing %q:\nname=%s\nargs=%s", want, detachedName, joined)
		}
	}
}

func TestTeamsServiceBootstrapPendingActivationWindowsAccessDeniedPromptsForUAC(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.73_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
		teamsServiceStartDetached = prevDetached
	})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		if filepath.Clean(path) != filepath.Clean(exe) || goos != "windows" {
			t.Fatalf("FindPendingReplacements path/goos = %q/%q, want %q/windows", path, goos, exe)
		}
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.73", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.68"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.73"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}
	taskDir := filepath.Join(tmp, "tasks")
	runner := &scriptedTeamsServiceRunner{
		outputs: [][]byte{
			nil,
			[]byte("Register-ScheduledTask : Access is denied.\n"),
			[]byte("NVIDIA\\jason\n"),
			nil,
		},
		errs: []error{
			nil,
			errors.New("exit status 1"),
			nil,
			nil,
		},
		onRun: func(index int, _ string, _ []string) {
			if index == 3 {
				assertTeamsServiceWindowsElevatedScriptFile(t, taskDir, true, false)
			}
		},
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exe,
		windowsTaskDir: taskDir,
		runner:         runner,
	})

	var out strings.Builder
	result, err := bootstrapTeamsService(context.Background(), nil, teamsServiceBootstrapOptions{
		AssumeYes: true,
		Out:       &out,
	})
	if err != nil {
		t.Fatalf("bootstrapTeamsService error: %v\noutput:\n%s", err, out.String())
	}
	if result.Mode != "windows-pending-helper-activation-uac" || !strings.HasSuffix(result.Path, teamsServiceWindowsTaskXMLName) {
		t.Fatalf("bootstrap result = %#v, want pending activation via UAC", result)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("PowerShell calls = %#v, want cleanup, failed register, current-user lookup, and elevated repair", runner.calls)
	}
	elevated := strings.Join(runner.calls[3].args, " ")
	assertTeamsServiceWindowsElevatedScriptCommand(t, elevated)
	if detachedName == "" {
		t.Fatal("pending activation did not schedule detached PowerShell")
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{
		pending,
		exe,
		teamsServiceWindowsTaskName,
		teamsServiceWindowsWatchdogTaskName,
		"Start-CodexHelperScheduledTaskIfStopped",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("activation command missing %q:\nname=%s\nargs=%s", want, detachedName, joined)
		}
	}
}

func TestTeamsServiceStartPendingActivationUsesStableExecutablePath(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	if err := os.WriteFile(exe, []byte("stable"), 0o644); err != nil {
		t.Fatalf("write exe: %v", err)
	}
	raw := exe + ".reload-backup-111-222"
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.75_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
		teamsServiceStartDetached = prevDetached
	})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		if filepath.Clean(path) != filepath.Clean(exe) || goos != "windows" {
			t.Fatalf("FindPendingReplacements path/goos = %q/%q, want stable %q/windows", path, goos, exe)
		}
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.75", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.70"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.75"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedArgs = append([]string{name}, args...)
		return nil
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            raw,
		windowsTaskDir: filepath.Join(tmp, "tasks"),
		runner:         &recordingTeamsServiceRunner{},
	})

	scheduled, err := schedulePendingTeamsServiceActivationBeforeStart(context.Background(), io.Discard, "start")
	if err != nil {
		t.Fatalf("schedulePendingTeamsServiceActivationBeforeStart error: %v", err)
	}
	if !scheduled {
		t.Fatal("expected pending activation to be scheduled")
	}
	if len(detachedArgs) == 0 {
		t.Fatal("expected detached activation process")
	}
}

func TestPendingHelperActivationAllowsTempSourceButRequiresStableDestination(t *testing.T) {
	tmp := t.TempDir()
	stableDest := filepath.Join(tmp, "codex-proxy.exe")
	pendingSource := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.75_windows_amd64.exe.123")
	activation, err := normalizeTeamsPendingHelperActivation(teamsPendingHelperActivation{
		InstallPath: stableDest,
		PendingPath: pendingSource,
		Version:     "0.1.0-rc.75",
	})
	if err != nil {
		t.Fatalf("pending source with stable destination should be allowed: %v", err)
	}
	if activation.PendingPath != pendingSource || activation.InstallPath != stableDest {
		t.Fatalf("activation normalized unexpectedly: %#v", activation)
	}

	_, err = normalizeTeamsPendingHelperActivation(teamsPendingHelperActivation{
		InstallPath: filepath.Join(tmp, ".nfs802014de01c482a9000004bf"),
		PendingPath: pendingSource,
		Version:     "0.1.0-rc.75",
	})
	if err == nil || !strings.Contains(err.Error(), "install path is not stable") {
		t.Fatalf("transient destination should be rejected, got %v", err)
	}
}

func TestDiscoverTeamsPendingHelperActivationRejectsStalePendingVersion(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	stale := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.73_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{goos: "windows", exe: exe})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		return []update.PendingReplacement{{Path: stale, Version: "0.1.0-rc.73", ModTime: time.Now()}}, nil
	}
	var pendingProbeCount int
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.74"}, nil
		case filepath.Clean(stale):
			pendingProbeCount++
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.73"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}

	activation, ok, err := discoverTeamsPendingHelperActivation(context.Background(), exe, "")
	if err != nil {
		t.Fatalf("discoverTeamsPendingHelperActivation error: %v", err)
	}
	if ok {
		t.Fatalf("activation = %#v, want stale pending replacement ignored", activation)
	}
	if pendingProbeCount != 0 {
		t.Fatalf("stale pending was probed %d time(s), want skipped before probe", pendingProbeCount)
	}
}

func TestDiscoverTeamsPendingHelperActivationAcceptsNewerPendingVersion(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.75_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{goos: "windows", exe: exe})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.75", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.74"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.75"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}

	activation, ok, err := discoverTeamsPendingHelperActivation(context.Background(), exe, "")
	if err != nil {
		t.Fatalf("discoverTeamsPendingHelperActivation error: %v", err)
	}
	if !ok {
		t.Fatal("discoverTeamsPendingHelperActivation ok = false, want newer pending activation")
	}
	if activation.PendingPath != pending || activation.InstallPath != exe || activation.Version != "0.1.0-rc.75" {
		t.Fatalf("activation = %#v, want pending %q for %q", activation, pending, exe)
	}
}

func TestDiscoverTeamsPendingHelperActivationRejectsPendingWhenFormalVersionIsNotComparable(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.75_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{goos: "windows", exe: exe})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.75", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "dev"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.75"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}

	activation, ok, err := discoverTeamsPendingHelperActivation(context.Background(), exe, "")
	if err != nil {
		t.Fatalf("discoverTeamsPendingHelperActivation error: %v", err)
	}
	if ok {
		t.Fatalf("activation = %#v, want pending ignored when formal entry is not comparable", activation)
	}
}

func TestDiscoverTeamsPendingHelperActivationRejectsPendingWhenFormalProbeFails(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.75_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{goos: "windows", exe: exe})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.75", ModTime: time.Now()}}, nil
	}
	var pendingProbeCount int
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{}, errors.New("formal entry probe timed out")
		case filepath.Clean(pending):
			pendingProbeCount++
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.75"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}

	activation, ok, err := discoverTeamsPendingHelperActivation(context.Background(), exe, "")
	if err != nil {
		t.Fatalf("discoverTeamsPendingHelperActivation error: %v", err)
	}
	if ok {
		t.Fatalf("activation = %#v, want pending ignored when formal entry probe fails", activation)
	}
	if pendingProbeCount != 0 {
		t.Fatalf("pending was probed %d time(s), want skipped after formal probe failure", pendingProbeCount)
	}
}

func TestTeamsServiceStartAndRestartActivatePendingWindowsHelperBeforeOldEntry(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "start", args: []string{"teams", "service", "start"}, want: "Scheduled Teams service start after activating pending helper v0.1.0-rc.74."},
		{name: "restart", args: []string{"teams", "service", "restart"}, want: "Scheduled Teams service restart after activating pending helper v0.1.0-rc.74."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lockCLITestHooks(t)

			tmp := t.TempDir()
			exe := filepath.Join(tmp, "codex-proxy.exe")
			pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.74_windows_amd64.exe.123")
			prevFind := teamsUpdateFindPendingReplacementsForPlatform
			prevProbe := teamsUpdateProbeBinaryVersion
			prevDetached := teamsServiceStartDetached
			t.Cleanup(func() {
				teamsUpdateFindPendingReplacementsForPlatform = prevFind
				teamsUpdateProbeBinaryVersion = prevProbe
				teamsServiceStartDetached = prevDetached
			})
			teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
				if filepath.Clean(path) != filepath.Clean(exe) || goos != "windows" {
					t.Fatalf("FindPendingReplacements path/goos = %q/%q, want %q/windows", path, goos, exe)
				}
				return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.74", ModTime: time.Now()}}, nil
			}
			teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
				switch filepath.Clean(path) {
				case filepath.Clean(exe):
					return update.BinaryVersion{Path: path, Version: "0.1.0-rc.68"}, nil
				case filepath.Clean(pending):
					return update.BinaryVersion{Path: path, Version: "0.1.0-rc.74"}, nil
				default:
					t.Fatalf("unexpected probe path %q", path)
					return update.BinaryVersion{}, nil
				}
			}
			var detachedName string
			var detachedArgs []string
			teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
				detachedName = name
				detachedArgs = append([]string(nil), args...)
				return nil
			}
			runner := &recordingTeamsServiceRunner{}
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           "windows",
				exe:            exe,
				windowsTaskDir: filepath.Join(tmp, "tasks"),
				runner:         runner,
			})

			cmd := newRootCmd()
			cmd.SetArgs(tc.args)
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&out)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("%s command error: %v", tc.name, err)
			}
			if strings.TrimSpace(out.String()) != tc.want {
				t.Fatalf("%s output = %q, want %q", tc.name, strings.TrimSpace(out.String()), tc.want)
			}
			if len(runner.calls) != 0 {
				t.Fatalf("%s should not start old scheduled task directly, calls=%#v", tc.name, runner.calls)
			}
			if detachedName != "powershell.exe" {
				t.Fatalf("%s detached name = %q, want powershell.exe", tc.name, detachedName)
			}
			joined := strings.Join(detachedArgs, " ")
			for _, want := range []string{pending, exe, "$want='0.1.0-rc.74'", ".activation.json", "Move-Item -Force", "Write-Status 'failed'", "Enable-ScheduledTask", "Start-CodexHelperScheduledTaskIfStopped"} {
				if !strings.Contains(joined, want) {
					t.Fatalf("%s activation command missing %q:\n%s", tc.name, want, joined)
				}
			}
		})
	}
}

func TestTeamsServiceRestartForceRecoversBeforePendingWindowsActivation(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	st := seedRecoverableTeamsState(t)
	owner, err := teamsstore.CurrentOwner("v-test", "s1", "turn:manual", time.Now())
	if err != nil {
		t.Fatalf("CurrentOwner error: %v", err)
	}
	if _, err := st.RecordOwnerHeartbeat(context.Background(), owner, time.Minute, time.Now()); err != nil {
		t.Fatalf("RecordOwnerHeartbeat error: %v", err)
	}
	exe := filepath.Join(tmp, "codex-proxy.exe")
	pending := filepath.Join(tmp, ".codex-proxy_0.1.0-rc.75_windows_amd64.exe.123")
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
		teamsServiceStartDetached = prevDetached
	})
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		if filepath.Clean(path) != filepath.Clean(exe) || goos != "windows" {
			t.Fatalf("FindPendingReplacements path/goos = %q/%q, want %q/windows", path, goos, exe)
		}
		return []update.PendingReplacement{{Path: pending, Version: "0.1.0-rc.75", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch filepath.Clean(path) {
		case filepath.Clean(exe):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.68"}, nil
		case filepath.Clean(pending):
			return update.BinaryVersion{Path: path, Version: "0.1.0-rc.75"}, nil
		default:
			t.Fatalf("unexpected probe path %q", path)
			return update.BinaryVersion{}, nil
		}
	}
	var detachedName string
	var detachedArgs []string
	teamsServiceStartDetached = func(_ context.Context, name string, args ...string) error {
		detachedName = name
		detachedArgs = append([]string(nil), args...)
		return nil
	}
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            exe,
		windowsTaskDir: filepath.Join(tmp, "tasks"),
		runner:         runner,
	})

	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "service", "restart", "--force"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("restart --force command error: %v\n%s", err, out.String())
	}
	got := out.String()
	for _, want := range []string{
		"Force recovering Teams state before service restart",
		"Recovered interrupted turns: 1",
		"Scheduled Teams service restart after activating pending helper v0.1.0-rc.75.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("restart --force output missing %q:\n%s", want, got)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatalf("pending restart --force should not start old scheduled task directly, calls=%#v", runner.calls)
	}
	if detachedName != "powershell.exe" {
		t.Fatalf("detached name = %q, want powershell.exe", detachedName)
	}
	joined := strings.Join(detachedArgs, " ")
	for _, want := range []string{pending, exe, "$want='0.1.0-rc.75'", "Move-Item -Force", "Start-CodexHelperScheduledTaskIfStopped"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("activation command missing %q:\n%s", want, joined)
		}
	}
	if _, ok, err := st.ReadOwner(context.Background()); err != nil {
		t.Fatalf("ReadOwner error: %v", err)
	} else if ok {
		t.Fatal("owner should be cleared before pending activation")
	}
}

func TestTeamsServiceOpenURLCommandMatrix(t *testing.T) {
	lockCLITestHooks(t)

	tests := []struct {
		name       string
		goos       string
		isWSL      bool
		wantExe    string
		wantArg    string
		wantErr    string
		wantCmdExe bool
	}{
		{name: "windows", goos: "windows", wantExe: "rundll32.exe", wantArg: "url.dll,FileProtocolHandler"},
		{name: "macos", goos: "darwin", wantExe: "open"},
		{name: "linux desktop", goos: "linux", wantExe: "xdg-open"},
		{name: "wsl", goos: "linux", isWSL: true, wantCmdExe: true, wantArg: "start"},
		{name: "unsupported", goos: "freebsd", wantErr: "not supported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:      tc.goos,
				exe:       "/tmp/codex-proxy",
				cwd:       "/tmp",
				isWSL:     tc.isWSL,
				runner:    &recordingTeamsServiceRunner{},
				unitDir:   filepath.Join(t.TempDir(), "systemd"),
				userID:    "501",
				wslDistro: "Ubuntu",
			})
			name, args, err := teamsServiceOpenURLCommand("https://teams.microsoft.com/l/chat/control")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("open URL error = %v, want containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("open URL command error: %v", err)
			}
			if tc.wantCmdExe {
				if filepath.Base(name) != "cmd.exe" {
					t.Fatalf("WSL opener = %q, want cmd.exe or absolute cmd.exe", name)
				}
			} else if name != tc.wantExe {
				t.Fatalf("opener = %q, want %q", name, tc.wantExe)
			}
			joined := strings.Join(args, " ")
			if !strings.Contains(joined, "https://teams.microsoft.com/l/chat/control") {
				t.Fatalf("opener args missing URL: %#v", args)
			}
			if tc.wantArg != "" && !strings.Contains(joined, tc.wantArg) {
				t.Fatalf("opener args missing %q: %#v", tc.wantArg, args)
			}
		})
	}
}

func TestTeamsServiceDoctorReportsWSLHint(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		unitDir:        filepath.Join(tmp, "systemd", "user"),
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service doctor: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"Teams service backend: wsl-windows-task-scheduler",
		"Codex Helper Teams Bridge (WSL Ubuntu alice default ",
		"WSL: detected",
		"wsl.exe",
		"WSL supervisor readiness",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Get-Command wsl.exe") || !strings.Contains(strings.Join(runner.calls[0].args, " "), "Get-ScheduledTask") {
		t.Fatalf("doctor should run a non-destructive WSL readiness probe, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Register-ScheduledTask", "Start-ScheduledTask", "Enable-ScheduledTask", "Disable-ScheduledTask")
}

func TestTeamsServiceRunPowerShellIncludesCombinedOutputOnFailure(t *testing.T) {
	lockCLITestHooks(t)

	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:  "linux",
		exe:   "/tmp/codex-proxy",
		cwd:   "/tmp",
		isWSL: true,
		runner: &recordingTeamsServiceRunner{
			output: []byte("Register-ScheduledTask : Access is denied.\n"),
			err:    errors.New("exit status 1"),
		},
	})

	_, err := teamsServiceRunPowerShell(context.Background(), "Register-ScheduledTask -TaskName 'Codex Helper Teams Bridge'")
	if err == nil {
		t.Fatal("teamsServiceRunPowerShell error = nil, want failure")
	}
	if !strings.Contains(err.Error(), "exit status 1") || !strings.Contains(err.Error(), "Access is denied") {
		t.Fatalf("PowerShell error did not preserve combined output:\n%v", err)
	}
	if !isTeamsServiceWindowsAccessDeniedError(err) {
		t.Fatalf("PowerShell access denied output should be classified as access denied:\n%v", err)
	}
}

func TestTeamsServiceDoctorReportsWSLReadinessFailure(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := &recordingTeamsServiceRunner{err: errors.New("powershell missing")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		unitDir:        filepath.Join(tmp, "systemd", "user"),
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"doctor"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "WSL Windows Scheduled Task readiness check failed") {
		t.Fatalf("doctor error = %v, want WSL readiness failure", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "powershell.exe" {
		t.Fatalf("doctor readiness calls=%#v", runner.calls)
	}
}

func TestTeamsServiceRunPowerShellCanUseExplicitExecutablePath(t *testing.T) {
	lockCLITestHooks(t)

	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  "/home/alice/bin/codex-proxy",
		cwd:                  "/home/alice/work",
		isWSL:                true,
		runner:               runner,
		powerShellExecutable: "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe",
	})

	if _, err := teamsServiceRunPowerShell(context.Background(), "Get-Command wsl.exe"); err != nil {
		t.Fatalf("teamsServiceRunPowerShell error: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0].name != "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe" {
		t.Fatalf("powershell executable call = %#v", runner.calls)
	}
	if joined := strings.Join(runner.calls[0].args, " "); !strings.Contains(joined, "-WindowStyle Hidden -Command") {
		t.Fatalf("powershell args should hide helper status probes, got %q", joined)
	}
}

func TestTeamsServiceInstallWritesWSLWindowsTask(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HOME", filepath.Join(tmp, "codex-home"))
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work dir",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu-22.04",
		wslLinuxUser:   "alice",
		runner:         runner,
	})

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr("/home/alice/registry.json"))
	cmd.SetArgs([]string{"install"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute service install: %v", err)
	}
	files, err := filepath.Glob(filepath.Join(tmp, "wsl-task", "codex-helper-teams-wsl-task-*.txt"))
	if err != nil {
		t.Fatalf("glob WSL task config: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("WSL task config files = %#v, want one", files)
	}
	configPath := files[0]
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read WSL task config: %v", err)
	}
	config := string(data)
	wantCWD := teamsServiceTestAbsPath(t, "/home/alice/work dir")
	wantExe := teamsServiceTestAbsPath(t, "/home/alice/bin/codex-proxy")
	wantRegistry := teamsServiceTestRegistryPath(wantCWD, "/home/alice/registry.json")
	for _, want := range []string{
		"TaskName=Codex Helper Teams Bridge (WSL Ubuntu-22.04 alice default ",
		"Command=wsl.exe",
		"-d Ubuntu-22.04",
		"-u alice",
		"--cd ",
		"--exec env",
		wantCWD,
		"CODEX_HOME=" + filepath.Join(tmp, "codex-home"),
		wantExe + " teams run --owner-stale-after 1m30s --auto-service=false --registry",
		wantRegistry,
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("WSL config missing %q:\n%s", want, config)
		}
	}
	if len(runner.calls) != 1 {
		t.Fatalf("install should register WSL scheduled task, calls=%#v", runner.calls)
	}
	call := strings.Join(runner.calls[0].args, " ")
	if runner.calls[0].name != "powershell.exe" || !strings.Contains(call, "Register-ScheduledTask") || !strings.Contains(call, "wsl.exe") {
		t.Fatalf("install should register WSL scheduled task, calls=%#v", runner.calls)
	}
	for _, want := range []string{
		"$expectedActionExecute = 'wscript.exe'",
		"$expectedActionArgument = '//B //Nologo",
		"WScript.Shell",
		"shell.Run(cmd, 0, True)",
		"WScript.Quit code",
		"-WindowStyle Hidden",
		"$wslArgumentLine",
		"Start-Process -FilePath ''wsl.exe''",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
	} {
		if !strings.Contains(call, want) {
			t.Fatalf("WSL scheduled task should run wsl.exe through a hidden child process while preserving task lifetime; missing %q, calls=%#v", want, runner.calls)
		}
	}
	if strings.Contains(call, "& wsl.exe @wslArgs") {
		t.Fatalf("WSL scheduled task should not launch wsl.exe directly in a visible console path, calls=%#v", runner.calls)
	}
	if strings.Contains(call, "RepetitionInterval") || strings.Contains(call, "Trigger @($logon, $watchdog)") {
		t.Fatalf("WSL scheduled task should not use repeated Task Scheduler triggers, calls=%#v", runner.calls)
	}
	if !strings.Contains(call, "RestartCount 999") {
		t.Fatalf("WSL task should use high restart count for background keepalive, calls=%#v", runner.calls)
	}
	if !strings.Contains(call, "Disable-ScheduledTask") {
		t.Fatalf("install should leave WSL task disabled, calls=%#v", runner.calls)
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls, "Start-ScheduledTask", "Enable-ScheduledTask")
}

func TestTeamsServiceWSLTaskNameSeparatesUsersAndProfiles(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "research/profile")
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            "/home/alice/bin/codex-proxy",
		cwd:            "/home/alice/work",
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Ubuntu/Dev",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	aliceResearch := teamsServiceWSLWindowsTaskBackend{}.Name()

	teamsServiceWSLLinuxUserName = func() string { return "bob" }
	bobResearch := teamsServiceWSLWindowsTaskBackend{}.Name()
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "default")
	bobDefault := teamsServiceWSLWindowsTaskBackend{}.Name()

	if aliceResearch == bobResearch || bobResearch == bobDefault || aliceResearch == bobDefault {
		t.Fatalf("WSL task names should be isolated by Linux user and profile: alice=%q bob=%q default=%q", aliceResearch, bobResearch, bobDefault)
	}
	for _, name := range []string{aliceResearch, bobResearch, bobDefault} {
		if strings.ContainsAny(name, `\/:*?"<>|`) {
			t.Fatalf("WSL task name contains Windows-unsafe character: %q", name)
		}
	}
}

func TestTeamsServiceActiveQueriesSupervisorWithoutGeneratedConfig(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	for _, tc := range []struct {
		name     string
		goos     string
		runner   string
		wantName string
	}{
		{name: "darwin", goos: "darwin", runner: "launchctl", wantName: "print"},
		{name: "windows", goos: "windows", runner: "powershell.exe", wantName: "Get-ScheduledTask"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingTeamsServiceRunner{}
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           tc.goos,
				exe:            filepath.Join(tmp, "codex-proxy"),
				cwd:            tmp,
				launchAgentDir: filepath.Join(tmp, "missing-launch-agents"),
				windowsTaskDir: filepath.Join(tmp, "missing-task-xml"),
				userID:         "501",
				runner:         runner,
			})
			active, err := teamsServiceActive(context.Background())
			if err != nil {
				t.Fatalf("teamsServiceActive error: %v", err)
			}
			if !active {
				t.Fatal("teamsServiceActive = false, want true from supervisor")
			}
			if len(runner.calls) != 1 || runner.calls[0].name != tc.runner || !strings.Contains(strings.Join(runner.calls[0].args, " "), tc.wantName) {
				t.Fatalf("unexpected supervisor calls: %#v", runner.calls)
			}
		})
	}
}

type teamsServiceCommandCall struct {
	name string
	args []string
}

type recordingTeamsServiceRunner struct {
	calls  []teamsServiceCommandCall
	output []byte
	err    error
}

func (r *recordingTeamsServiceRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, teamsServiceCommandCall{
		name: name,
		args: append([]string{}, args...),
	})
	return append([]byte{}, r.output...), r.err
}

func teamsServiceCallSeen(calls []teamsServiceCommandCall, action string) bool {
	for _, call := range calls {
		for _, arg := range call.args {
			if arg == action {
				return true
			}
		}
	}
	return false
}

func assertTeamsServiceCallsDoNotContain(t *testing.T, calls []teamsServiceCommandCall, forbidden ...string) {
	t.Helper()
	for _, call := range calls {
		joined := call.name + " " + strings.Join(call.args, " ")
		for _, value := range forbidden {
			if strings.Contains(joined, value) {
				t.Fatalf("service command contained forbidden %q: %#v", value, calls)
			}
		}
	}
}

type teamsServiceTestHooks struct {
	goos                       string
	exe                        string
	argv0                      string
	cwd                        string
	unitDir                    string
	launchAgentDir             string
	windowsTaskDir             string
	userID                     string
	isWSL                      bool
	wslDistro                  string
	wslLinuxUser               string
	powerShellExecutable       string
	systemdUserAvailable       *bool
	systemdUserAvailableErr    error
	runner                     teamsServiceCommandRunner
	bootstrapControlChat       func(context.Context, *rootOptions, *string, bool, io.Writer) (teamsServiceBootstrapControlChatResult, error)
	localStartDetached         func(context.Context, string, string, teamsServiceSpec) (int, error)
	localVerifyProcessIdentity func(int, string) error
	localVerifyChildIdentity   func(int, teamsServiceSpec) error
	localProcessStartTime      func(int) (string, error)
	localProcessArgs           func(int) ([]string, error)
	localProcessEnvironment    func(int) (map[string]string, error)
	localCheckChildHealth      func(context.Context, *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error)
	localTerminateTarget       func(*exec.Cmd, time.Duration) error
	localReadyTimeout          time.Duration
}

func withTeamsServiceTestHooks(t *testing.T, hooks teamsServiceTestHooks) {
	t.Helper()
	t.Setenv(envTeamsCodexChild, "")
	t.Setenv(envTeamsCodexParentPID, "")
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE_MODE", "")
	t.Setenv("CODEX_HELPER_TEAMS_AUTO_SERVICE", "")
	prevGOOS := teamsServiceGOOS
	prevExecutable := teamsServiceExecutable
	prevArgv0 := teamsServiceArgv0
	prevGetwd := teamsServiceGetwd
	prevSystemdUserDir := teamsServiceSystemdUserDir
	prevLaunchAgentDir := teamsServiceLaunchAgentDir
	prevWindowsTaskXMLDir := teamsServiceWindowsTaskXMLDir
	prevUserID := teamsServiceUserID
	prevIsWSL := teamsServiceIsWSL
	prevWSLDistroName := teamsServiceWSLDistroName
	prevWSLLinuxUserName := teamsServiceWSLLinuxUserName
	prevPowerShellExecutable := teamsServicePowerShellExecutable
	prevSystemctl := teamsServiceSystemctl
	prevSystemdUserAvailable := teamsServiceSystemdUserAvailable
	prevAuthPreflight := teamsServiceAuthPreflight
	prevBootstrapControlChat := teamsServiceBootstrapControlChat
	prevLocalSupervisorStartDetached := teamsServiceLocalSupervisorStartDetached
	prevLocalSupervisorReleaseProcess := teamsServiceLocalSupervisorReleaseProcess
	prevLocalSupervisorCheckChildHealth := teamsServiceLocalSupervisorCheckChildHealth
	prevLocalSupervisorTerminateTarget := teamsServiceLocalSupervisorTerminateTarget
	prevLocalSupervisorReadyTimeout := teamsServiceLocalSupervisorReadyTimeout
	prevLocalSupervisorVerifyProcessIdentity := teamsLocalSupervisorVerifyProcessIdentity
	prevLocalSupervisorVerifyChildIdentity := teamsLocalSupervisorVerifyChildIdentity
	prevLocalSupervisorProcessStartTime := teamsLocalSupervisorProcessStartTime
	prevLocalSupervisorProcessArgs := teamsLocalSupervisorProcessArgs
	prevLocalSupervisorProcessEnvironment := teamsLocalSupervisorProcessEnvironment
	prevListLocalProcesses := teamsServiceListLocalProcesses
	prevTerminateLocalProcess := teamsServiceTerminateLocalProcess
	prevLocalProcessGraceDelay := teamsServiceLocalProcessGraceDelay
	prevCurrentProcessID := teamsCurrentProcessID
	prevParentProcessID := teamsParentProcessID
	t.Setenv("CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND", "")
	teamsCurrentProcessID = os.Getpid
	teamsParentProcessID = os.Getppid
	teamsServiceGOOS = func() string { return hooks.goos }
	teamsServiceExecutable = func() (string, error) { return hooks.exe, nil }
	teamsServiceArgv0 = func() string { return hooks.argv0 }
	teamsServiceGetwd = func() (string, error) { return hooks.cwd, nil }
	teamsServiceSystemdUserDir = func() (string, error) { return hooks.unitDir, nil }
	teamsServiceLaunchAgentDir = func() (string, error) { return hooks.launchAgentDir, nil }
	teamsServiceWindowsTaskXMLDir = func() (string, error) { return hooks.windowsTaskDir, nil }
	teamsServiceUserID = func() string { return hooks.userID }
	teamsServiceIsWSL = func() bool { return hooks.isWSL }
	teamsServiceWSLDistroName = func() string { return hooks.wslDistro }
	teamsServiceWSLLinuxUserName = func() string { return hooks.wslLinuxUser }
	teamsServicePowerShellExecutable = func() string {
		if hooks.powerShellExecutable != "" {
			return hooks.powerShellExecutable
		}
		return "powershell.exe"
	}
	teamsServiceSystemctl = hooks.runner
	teamsServiceSystemdUserAvailable = func(context.Context) (bool, error) {
		if hooks.systemdUserAvailableErr != nil {
			return false, hooks.systemdUserAvailableErr
		}
		if hooks.systemdUserAvailable != nil {
			return *hooks.systemdUserAvailable, nil
		}
		return true, nil
	}
	teamsServiceAuthPreflight = func() error { return nil }
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) { return nil, nil }
	teamsServiceTerminateLocalProcess = func(int, time.Duration) error { return nil }
	teamsServiceLocalProcessGraceDelay = 0
	teamsServiceBootstrapControlChat = func(ctx context.Context, root *rootOptions, registryPath *string, openControl bool, errOut io.Writer) (teamsServiceBootstrapControlChatResult, error) {
		if hooks.bootstrapControlChat != nil {
			return hooks.bootstrapControlChat(ctx, root, registryPath, openControl, errOut)
		}
		return teamsServiceBootstrapControlChatResult{}, nil
	}
	teamsServiceLocalSupervisorStartDetached = func(ctx context.Context, configPath string, logPath string, spec teamsServiceSpec) (int, error) {
		if hooks.localStartDetached != nil {
			return hooks.localStartDetached(ctx, configPath, logPath, spec)
		}
		return 4242, nil
	}
	teamsLocalSupervisorVerifyProcessIdentity = func(pid int, configPath string) error {
		if hooks.localVerifyProcessIdentity != nil {
			return hooks.localVerifyProcessIdentity(pid, configPath)
		}
		return nil
	}
	teamsLocalSupervisorVerifyChildIdentity = func(pid int, spec teamsServiceSpec) error {
		if hooks.localVerifyChildIdentity != nil {
			return hooks.localVerifyChildIdentity(pid, spec)
		}
		return nil
	}
	teamsLocalSupervisorProcessStartTime = func(pid int) (string, error) {
		if hooks.localProcessStartTime != nil {
			return hooks.localProcessStartTime(pid)
		}
		return "", nil
	}
	teamsLocalSupervisorProcessArgs = func(pid int) ([]string, error) {
		if hooks.localProcessArgs != nil {
			return hooks.localProcessArgs(pid)
		}
		return nil, nil
	}
	teamsLocalSupervisorProcessEnvironment = func(pid int) (map[string]string, error) {
		if hooks.localProcessEnvironment != nil {
			return hooks.localProcessEnvironment(pid)
		}
		return nil, nil
	}
	teamsServiceLocalSupervisorCheckChildHealth = func(ctx context.Context, state *teamsServiceWatchdogState) (teamsServiceWatchdogDecision, error) {
		if hooks.localCheckChildHealth != nil {
			return hooks.localCheckChildHealth(ctx, state)
		}
		return teamsServiceWatchdogDecision{Action: teamsServiceWatchdogActionNoop, Reason: "test health check disabled"}, nil
	}
	teamsServiceLocalSupervisorTerminateTarget = func(cmd *exec.Cmd, grace time.Duration) error {
		if hooks.localTerminateTarget != nil {
			return hooks.localTerminateTarget(cmd, grace)
		}
		return terminateTargetCommand(cmd, grace)
	}
	if hooks.localReadyTimeout > 0 {
		teamsServiceLocalSupervisorReadyTimeout = hooks.localReadyTimeout
	}
	t.Cleanup(func() {
		teamsServiceGOOS = prevGOOS
		teamsServiceExecutable = prevExecutable
		teamsServiceArgv0 = prevArgv0
		teamsServiceGetwd = prevGetwd
		teamsServiceSystemdUserDir = prevSystemdUserDir
		teamsServiceLaunchAgentDir = prevLaunchAgentDir
		teamsServiceWindowsTaskXMLDir = prevWindowsTaskXMLDir
		teamsServiceUserID = prevUserID
		teamsServiceIsWSL = prevIsWSL
		teamsServiceWSLDistroName = prevWSLDistroName
		teamsServiceWSLLinuxUserName = prevWSLLinuxUserName
		teamsServicePowerShellExecutable = prevPowerShellExecutable
		teamsServiceSystemctl = prevSystemctl
		teamsServiceSystemdUserAvailable = prevSystemdUserAvailable
		teamsServiceAuthPreflight = prevAuthPreflight
		teamsServiceBootstrapControlChat = prevBootstrapControlChat
		teamsServiceLocalSupervisorStartDetached = prevLocalSupervisorStartDetached
		teamsServiceLocalSupervisorReleaseProcess = prevLocalSupervisorReleaseProcess
		teamsServiceLocalSupervisorCheckChildHealth = prevLocalSupervisorCheckChildHealth
		teamsServiceLocalSupervisorTerminateTarget = prevLocalSupervisorTerminateTarget
		teamsServiceLocalSupervisorReadyTimeout = prevLocalSupervisorReadyTimeout
		teamsLocalSupervisorVerifyProcessIdentity = prevLocalSupervisorVerifyProcessIdentity
		teamsLocalSupervisorVerifyChildIdentity = prevLocalSupervisorVerifyChildIdentity
		teamsLocalSupervisorProcessStartTime = prevLocalSupervisorProcessStartTime
		teamsLocalSupervisorProcessArgs = prevLocalSupervisorProcessArgs
		teamsLocalSupervisorProcessEnvironment = prevLocalSupervisorProcessEnvironment
		teamsServiceListLocalProcesses = prevListLocalProcesses
		teamsServiceTerminateLocalProcess = prevTerminateLocalProcess
		teamsServiceLocalProcessGraceDelay = prevLocalProcessGraceDelay
		teamsCurrentProcessID = prevCurrentProcessID
		teamsParentProcessID = prevParentProcessID
	})
}

func stringPtr(s string) *string {
	return &s
}

func testEnvValue(env []string, key string) (string, bool) {
	prefix := key + "="
	var out string
	ok := false
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			out = strings.TrimPrefix(entry, prefix)
			ok = true
		}
	}
	return out, ok
}

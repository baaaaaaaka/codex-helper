package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestTeamsRuntimeSafetyPackageTestMainIsolatesEveryUserDirectoryCI(t *testing.T) {
	home := strings.TrimSpace(os.Getenv("HOME"))
	if home == "" {
		t.Fatal("TestMain must provide an isolated HOME")
	}
	testRoot := filepath.Dir(home)
	for _, name := range []string{
		"USERPROFILE",
		"APPDATA",
		"LOCALAPPDATA",
		"XDG_CONFIG_HOME",
		"XDG_CACHE_HOME",
		"XDG_DATA_HOME",
		"XDG_STATE_HOME",
	} {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			t.Fatalf("TestMain did not isolate %s", name)
		}
		rel, err := filepath.Rel(testRoot, value)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			t.Fatalf("TestMain left %s outside the package test root: %q (root %q)", name, value, testRoot)
		}
	}
	for _, name := range []string{"CODEX_HOME", "CODEX_DIR", "CODEX_CONFIG_DIR"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			t.Fatalf("TestMain retained ambient %s=%q instead of using the isolated HOME fallback", name, value)
		}
	}
}

func TestTeamsRuntimeSafetyServiceSpecDoesNotPersistInvocationWorkingDirCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	invocationDir := filepath.Join(tmp, "disposable-checkout")
	if err := os.MkdirAll(invocationDir, 0o755); err != nil {
		t.Fatalf("mkdir invocation dir: %v", err)
	}
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     exe,
		argv0:   exe,
		cwd:     invocationDir,
		unitDir: filepath.Join(tmp, "systemd"),
		runner:  &recordingTeamsServiceRunner{},
	})

	spec, err := buildTeamsServiceSpec(stringPtr(""))
	if err != nil {
		t.Fatalf("buildTeamsServiceSpec: %v", err)
	}
	if teamsServiceLocalSupervisorSamePath(spec.WorkingDir, invocationDir) {
		t.Fatalf("service WorkingDir %q is permanently bound to the invocation cwd; use a stable user-owned directory", spec.WorkingDir)
	}
	stableHome := strings.TrimSpace(os.Getenv("HOME"))
	if !teamsServiceLocalSupervisorSamePath(spec.WorkingDir, stableHome) {
		t.Fatalf("service WorkingDir = %q, want isolated stable user home %q", spec.WorkingDir, stableHome)
	}
	if info, err := os.Stat(spec.WorkingDir); err != nil || !info.IsDir() {
		t.Fatalf("service WorkingDir must be an existing directory: path=%q info=%v err=%v", spec.WorkingDir, info, err)
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsPersistedServiceWorkingDirCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	missingWorkingDir := filepath.Join(tmp, "deleted-bootstrap-directory")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exe,
		argv0:                exe,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable:  exe,
		WorkingDir:  missingWorkingDir,
		Environment: map[string]string{},
	}); err != nil {
		t.Fatalf("install invalid local-supervisor fixture: %v", err)
	}

	prevPerform := performUpdate
	prevResolve := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolve
		teamsAutoUpdateExecutable = prevExecutable
	})
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return exe, nil }
	teamsAutoUpdateExecutable = func() (string, error) { return exe, nil }
	updateCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		updateCalled = true
		return update.ApplyResult{Version: "1.2.4", InstallPath: exe}, nil
	}

	_, err := (teamsReleaseAutoUpdater{repo: "owner/repo"}).Apply(
		context.Background(),
		teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before the persisted service configuration was proven restartable")
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "working") {
		t.Fatalf("Apply error = %v, want an actionable WorkingDir preflight failure", err)
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsPersistedServiceExecutableCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	currentExe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, currentExe, "1.2.3")
	missingServiceExe := filepath.Join(tmp, "deleted-runtime", "codex-proxy")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  currentExe,
		argv0:                currentExe,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable:  missingServiceExe,
		WorkingDir:  tmp,
		Environment: map[string]string{},
	}); err != nil {
		t.Fatalf("install invalid local-supervisor fixture: %v", err)
	}

	prevPerform := performUpdate
	prevResolve := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolve
		teamsAutoUpdateExecutable = prevExecutable
	})
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return currentExe, nil }
	teamsAutoUpdateExecutable = func() (string, error) { return currentExe, nil }
	updateCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		updateCalled = true
		return update.ApplyResult{Version: "1.2.4", InstallPath: currentExe}, nil
	}

	_, err := (teamsReleaseAutoUpdater{repo: "owner/repo"}).Apply(
		context.Background(),
		teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before the persisted service executable was proven restartable")
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "executable") {
		t.Fatalf("Apply error = %v, want an actionable executable preflight failure", err)
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsPersistedRegistryIdentityCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	currentExe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, currentExe, "1.2.3")
	missingRegistry := filepath.Join(tmp, "foreign-scope", "registry.json")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  currentExe,
		argv0:                currentExe,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable:   currentExe,
		WorkingDir:   tmp,
		RegistryPath: missingRegistry,
		Environment:  map[string]string{},
	}); err != nil {
		t.Fatalf("install invalid local-supervisor fixture: %v", err)
	}

	prevPerform := performUpdate
	prevResolve := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolve
		teamsAutoUpdateExecutable = prevExecutable
	})
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return currentExe, nil }
	teamsAutoUpdateExecutable = func() (string, error) { return currentExe, nil }
	updateCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		updateCalled = true
		return update.ApplyResult{Version: "1.2.4", InstallPath: currentExe}, nil
	}

	_, err := (teamsReleaseAutoUpdater{repo: "owner/repo"}).Apply(
		context.Background(),
		teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before persisted registry/store identity was validated")
	}
	lower := strings.ToLower(fmt.Sprint(err))
	if err == nil || (!strings.Contains(lower, "registry") && !strings.Contains(lower, "store")) {
		t.Fatalf("Apply error = %v, want actionable registry/store identity preflight failure", err)
	}
}

func TestTeamsRuntimeSafetyAutoUpdateDryRunsSupervisorBeforeReplacingBinaryCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	currentExe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, currentExe, "1.2.3")
	systemdUnavailable := false
	dryRunAttempted := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     currentExe,
		argv0:   currentExe,
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd"),
		runner: teamsServiceCommandRunnerFunc(func(context.Context, string, ...string) ([]byte, error) {
			dryRunAttempted = true
			return nil, errors.New("supervisor dry-run failed")
		}),
		systemdUserAvailable: &systemdUnavailable,
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable:  currentExe,
		WorkingDir:  tmp,
		Environment: map[string]string{},
	}); err != nil {
		t.Fatalf("install local-supervisor fixture: %v", err)
	}

	prevPerform := performUpdate
	prevResolve := teamsAutoUpdateResolveInstallPath
	prevExecutable := teamsAutoUpdateExecutable
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsAutoUpdateResolveInstallPath = prevResolve
		teamsAutoUpdateExecutable = prevExecutable
	})
	teamsAutoUpdateResolveInstallPath = func(string) (string, error) { return currentExe, nil }
	teamsAutoUpdateExecutable = func() (string, error) { return currentExe, nil }
	updateCalled := false
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		updateCalled = true
		return update.ApplyResult{Version: "1.2.4", InstallPath: currentExe}, nil
	}

	_, err := (teamsReleaseAutoUpdater{repo: "owner/repo"}).Apply(
		context.Background(),
		teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before a supervisor dry-run proved the service restartable")
	}
	if !dryRunAttempted {
		t.Fatal("auto-update did not attempt a supervisor dry-run before binary replacement")
	}
	lower := strings.ToLower(fmt.Sprint(err))
	if err == nil || (!strings.Contains(lower, "dry-run") && !strings.Contains(lower, "restart")) {
		t.Fatalf("Apply error = %v, want actionable supervisor dry-run failure", err)
	}
}

func TestTeamsRuntimeSafetyServiceConfigRollsBackWhenActivationStartFailsCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	oldExe := filepath.Join(tmp, "runtime", "v1.2.3", "codex-proxy")
	newExe := filepath.Join(tmp, "runtime", "v1.2.4", "codex-proxy")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    "linux",
		exe:     oldExe,
		argv0:   oldExe,
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd"),
		runner:  &recordingTeamsServiceRunner{},
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			return 0, errors.New("activation start failed")
		},
	})

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("local supervisor config path: %v", err)
	}
	oldConfig := teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec: teamsServiceSpec{
			Executable:   oldExe,
			WorkingDir:   filepath.Join(tmp, "stable-working-dir"),
			RegistryPath: filepath.Join(tmp, "registry-old.json"),
			Environment:  map[string]string{"KEEP": "old"},
		},
		UpdatedAt: time.Unix(1_700_000_000, 0),
	}
	if err := os.MkdirAll(oldConfig.Spec.WorkingDir, 0o700); err != nil {
		t.Fatalf("mkdir old working dir: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, oldConfig); err != nil {
		t.Fatalf("write old supervisor config: %v", err)
	}
	before, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read old supervisor config: %v", err)
	}
	beforeInfo, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat old supervisor config: %v", err)
	}

	_, repairErr := (teamsServiceLocalSupervisorBackend{}).Repair(context.Background(), teamsServiceSpec{
		Executable:   newExe,
		WorkingDir:   tmp,
		RegistryPath: filepath.Join(tmp, "registry-new.json"),
		Environment:  map[string]string{"KEEP": "new"},
	}, teamsServiceRepairOptions{Enable: true, Start: true})
	if repairErr == nil || !strings.Contains(repairErr.Error(), "activation start failed") {
		t.Fatalf("Repair error = %v, want injected activation failure", repairErr)
	}

	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read supervisor config after failed activation: %v", err)
	}
	afterInfo, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("stat supervisor config after failed activation: %v", err)
	}
	if !bytes.Equal(before, after) || beforeInfo.Mode() != afterInfo.Mode() {
		t.Errorf(
			"failed activation did not restore the exact previous service config: bytes_equal=%t mode=%v->%v\nbefore=%s\nafter=%s",
			bytes.Equal(before, after),
			beforeInfo.Mode(),
			afterInfo.Mode(),
			before,
			after,
		)
	}
	if leftovers, err := filepath.Glob(configPath + ".*"); err != nil {
		t.Fatalf("glob config transaction leftovers: %v", err)
	} else if len(leftovers) != 0 {
		t.Errorf("failed activation left config transaction artifacts: %v", leftovers)
	}
}

func TestTeamsRuntimeSafetyDoesNotRestoreStaleLoopbackProxyFromServiceConfigCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(name, "")
	}
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		isWSL:                true,
		exe:                  exe,
		argv0:                exe,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd"),
		windowsTaskDir:       filepath.Join(tmp, "wsl-task"),
		wslDistro:            "Ubuntu",
		wslLinuxUser:         "alice",
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
	})
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable: exe,
		WorkingDir: tmp,
		Environment: map[string]string{
			"HTTP_PROXY":  "http://127.0.0.1:44411",
			"HTTPS_PROXY": "http://localhost:44411",
		},
	}); err != nil {
		t.Fatalf("install stale proxy fixture: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "enable"); err != nil {
		t.Fatalf("enable stale proxy fixture: %v", err)
	}

	env := teamsServiceEnvironment()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY"} {
		if value := strings.TrimSpace(env[name]); value != "" {
			t.Fatalf("%s resurrected stale WSL loopback proxy %q from persisted service configuration", name, value)
		}
	}
}

func TestTeamsRuntimeSafetyExplicitStableLoopbackProxyOptInIsPreservedCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            filepath.Join(tmp, "bin", "codex-proxy"),
		argv0:          filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         &recordingTeamsServiceRunner{},
	})
	t.Setenv("CODEX_HELPER_TEAMS_KEEP_LOCAL_PROXY", "1")
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:3128")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:3128")

	env := teamsServiceEnvironment()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY"} {
		if env[name] != "http://127.0.0.1:3128" {
			t.Fatalf("%s = %q, want explicitly opted-in stable loopback proxy", name, env[name])
		}
	}
}

func TestTeamsRuntimeSafetyWSLRetireSkipsAlreadyDisabledTasksCI(t *testing.T) {
	command := buildTeamsServiceWSLRetireTaskCommand("Codex Helper Teams Bridge", true)
	lower := strings.ToLower(command)
	stateCheck := strings.Index(lower, ".state -eq 'disabled'")
	disableCall := strings.Index(lower, "disable-scheduledtask")
	if stateCheck < 0 || disableCall < 0 || stateCheck > disableCall || !strings.Contains(lower, "continue") {
		t.Fatalf("retire command must skip already-disabled tasks instead of requiring Disable-ScheduledTask again:\n%s", command)
	}
}

func TestTeamsRuntimeSafetyWSLInteropFailureWithoutProofOfTaskRetirementFailsClosedCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	startedPID := 7401
	started := false
	runner := teamsServiceCommandRunnerFunc(func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("fork/exec powershell.exe: exec format error")
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            exe,
		argv0:          exe,
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "no-task-config"),
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner:         runner,
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			started = true
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: 8401,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error { return nil },
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == startedPID }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exe, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err == nil {
		t.Fatal("local start must fail closed when WSLInterop is unavailable and the Windows task state cannot be proven")
	}
	if started {
		t.Fatal("local supervisor started while an unobservable Windows task could still launch a competing supervisor")
	}
}

func TestTeamsRuntimeSafetyWSLInteropFailureWithTrustedRetirementReceiptStartsLocalSupervisorCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	taskDir := filepath.Join(tmp, "wsl-task")
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatalf("mkdir task dir: %v", err)
	}
	receipt := `{"version":1,"distro":"Ubuntu","linux_user":"alice","task_prefix":"Codex Helper Teams Bridge","retired":true}`
	if err := os.WriteFile(filepath.Join(taskDir, "retirement-receipt.json"), []byte(receipt), 0o600); err != nil {
		t.Fatalf("write trusted retirement receipt: %v", err)
	}
	startedPID := 7402
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            exe,
		argv0:          exe,
		cwd:            tmp,
		windowsTaskDir: taskDir,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner: teamsServiceCommandRunnerFunc(func(context.Context, string, ...string) ([]byte, error) {
			return nil, errors.New("fork/exec powershell.exe: exec format error")
		}),
		localStartDetached: func(_ context.Context, configPath string, _ string, _ teamsServiceSpec) (int, error) {
			started = true
			return startedPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				SupervisorPID:  startedPID,
				SupervisorPGID: 8402,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error { return nil },
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == startedPID }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })

	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exe, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err != nil {
		t.Fatalf("trusted retirement receipt should allow local availability while WSLInterop is temporarily unavailable: %v", err)
	}
	if !started {
		t.Fatal("local supervisor did not start despite a trusted matching retirement receipt")
	}
}

func TestTeamsRuntimeSafetyWSLEnabledTaskAccessDeniedStillFailsClosedCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	taskDir := filepath.Join(tmp, "wsl-task")
	backend := teamsServiceWSLWindowsTaskBackend{}
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		exe:            exe,
		argv0:          exe,
		cwd:            tmp,
		windowsTaskDir: taskDir,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		runner: teamsServiceCommandRunnerFunc(func(context.Context, string, ...string) ([]byte, error) {
			return []byte("Disable-ScheduledTask: Access is denied"), errors.New("exit status 1")
		}),
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return 0, nil
		},
	})
	if _, err := backend.writeTaskConfig([]string{exe, "teams", "run"}); err != nil {
		t.Fatalf("write WSL task fixture: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{Executable: exe, WorkingDir: tmp}); err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err == nil {
		t.Fatal("start must fail closed while a configured Windows task may still launch a competing supervisor")
	}
	if started {
		t.Fatal("local supervisor started despite an enabled task that could not be disabled")
	}
}

func TestTeamsRuntimeSafetyDoctorDistinguishesMissingWSLInteropCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	interopAvailable := false
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                "linux",
		exe:                 filepath.Join(tmp, "codex-proxy"),
		cwd:                 tmp,
		isWSL:               true,
		wslInteropAvailable: &interopAvailable,
		runner:              runner,
	})

	var out bytes.Buffer
	err := runTeamsServiceWSLReadinessCheck(context.Background(), &out)
	diagnostic := strings.ToLower(out.String() + "\n" + fmt.Sprint(err))
	for _, want := range []string{"wslinterop", "binfmt", "unavailable"} {
		if !strings.Contains(diagnostic, want) {
			t.Errorf("missing-interop doctor diagnostic omitted %q:\n%s", want, diagnostic)
		}
	}
	if len(runner.calls) != 0 {
		t.Errorf("doctor tried to execute PowerShell despite missing WSLInterop: %#v", runner.calls)
	}
}

func TestTeamsRuntimeSafetyDoctorDistinguishesMissingPowerShellExecutableCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	interopAvailable := true
	runner := &recordingTeamsServiceRunner{err: errors.New("executable file not found")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                "linux",
		exe:                 filepath.Join(tmp, "codex-proxy"),
		cwd:                 tmp,
		isWSL:               true,
		wslInteropAvailable: &interopAvailable,
		runner:              runner,
	})

	var out bytes.Buffer
	err := runTeamsServiceWSLReadinessCheck(context.Background(), &out)
	diagnostic := strings.ToLower(out.String() + "\n" + fmt.Sprint(err))
	for _, want := range []string{"powershell", "executable", "unavailable"} {
		if !strings.Contains(diagnostic, want) {
			t.Errorf("missing-PowerShell doctor diagnostic omitted %q:\n%s", want, diagnostic)
		}
	}
}

func TestTeamsRuntimeSafetyDoctorClassifiesPowerShellExecFormatAsBrokenInteropCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	interopAvailable := true
	runner := &recordingTeamsServiceRunner{err: errors.New("fork/exec powershell.exe: exec format error")}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                "linux",
		exe:                 filepath.Join(tmp, "codex-proxy"),
		cwd:                 tmp,
		isWSL:               true,
		wslInteropAvailable: &interopAvailable,
		runner:              runner,
	})

	var out bytes.Buffer
	err := runTeamsServiceWSLReadinessCheck(context.Background(), &out)
	diagnostic := strings.ToLower(out.String() + "\n" + fmt.Sprint(err))
	for _, want := range []string{"wslinterop", "broken", "exec format"} {
		if !strings.Contains(diagnostic, want) {
			t.Errorf("exec-format doctor diagnostic omitted %q:\n%s", want, diagnostic)
		}
	}
	if strings.Contains(diagnostic, "access denied") {
		t.Errorf("exec-format failure was misclassified as a Scheduled Task permission error:\n%s", diagnostic)
	}
}

func TestTeamsRuntimeSafetyDoctorDistinguishesMissingScheduledTaskCmdletsCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	interopAvailable := true
	runner := &recordingTeamsServiceRunner{
		output: []byte("Get-ScheduledTask : The term 'Get-ScheduledTask' is not recognized"),
		err:    errors.New("exit status 1"),
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                "linux",
		exe:                 filepath.Join(tmp, "codex-proxy"),
		cwd:                 tmp,
		isWSL:               true,
		wslInteropAvailable: &interopAvailable,
		runner:              runner,
	})

	var out bytes.Buffer
	err := runTeamsServiceWSLReadinessCheck(context.Background(), &out)
	diagnostic := strings.ToLower(out.String() + "\n" + fmt.Sprint(err))
	for _, want := range []string{"scheduledtask", "cmdlet", "unavailable"} {
		if !strings.Contains(diagnostic, want) {
			t.Errorf("missing-ScheduledTask-cmdlet diagnostic omitted %q:\n%s", want, diagnostic)
		}
	}
	assertTeamsServiceCallsDoNotContain(t, runner.calls,
		"Register-ScheduledTask",
		"Disable-ScheduledTask",
		"Enable-ScheduledTask",
		"Start-ScheduledTask",
	)
}

func TestTeamsRuntimeSafetyLocalStartWaitsForChildListenerReadinessCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "local-supervisor.json")
	supervisorPID := 7501
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  supervisorPID,
		SupervisorPGID: 8501,
		ChildPID:       7502,
		ChildPGID:      8502,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write supervisor-only status: %v", err)
	}
	prevAlive := teamsLocalSupervisorProcessAlive
	prevVerify := teamsLocalSupervisorVerifyProcessIdentity
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == supervisorPID }
	teamsLocalSupervisorVerifyProcessIdentity = func(int, string) error { return nil }
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorVerifyProcessIdentity = prevVerify
	})

	ticks := installTeamsRuntimeSafetyReadinessTicker(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- waitTeamsServiceLocalSupervisorReady(ctx, supervisorPID, configPath, time.Hour)
	}()
	advanceTeamsRuntimeSafetyReadiness(t, ticks, result, "a child owner, listener, and current-generation poll existed")
	cancel()
	var err error
	select {
	case err = <-result:
	case <-time.After(time.Second):
		t.Fatal("readiness waiter did not stop after context cancellation")
	}
	if err == nil {
		t.Fatal("service start reported ready with no child owner, listener, or first control poll")
	}
}

func TestTeamsRuntimeSafetyLocalStartReadinessRejectsHistoricalPollThenAcceptsCurrentGenerationCI(t *testing.T) {
	lockCLITestHooks(t)
	fixture := newTeamsRuntimeSafetyReadinessFixture(t)
	ticks := installTeamsRuntimeSafetyReadinessTicker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- waitTeamsServiceLocalSupervisorReady(ctx, fixture.supervisorPID, fixture.configPath, time.Hour)
	}()

	advanceTeamsRuntimeSafetyReadiness(t, ticks, result, "a live owner and control lease existed")

	fixture.seedAuthority(t, fixture.ownerStartedAt.Add(-time.Hour))
	advanceTeamsRuntimeSafetyReadiness(t, ticks, result, "a current-generation control poll existed")

	if err := fixture.store.Update(context.Background(), func(state *teamsstore.State) error {
		poll := state.ChatPolls["control-chat"]
		poll.LastSuccessfulPollAt = fixture.ownerStartedAt.Add(time.Second)
		poll.UpdatedAt = fixture.ownerStartedAt.Add(time.Second)
		state.ChatPolls["control-chat"] = poll
		return nil
	}); err != nil {
		t.Fatalf("record current-generation control poll: %v", err)
	}
	ticks <- time.Now()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("fully ready listener did not satisfy readiness: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("fully ready listener did not satisfy readiness before timeout")
	}
}

func TestTeamsRuntimeSafetyLocalStartReadinessTimeoutNamesMissingLayerCI(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, *teamsRuntimeSafetyReadinessFixture)
		want    string
	}{
		{
			name: "child",
			prepare: func(t *testing.T, fixture *teamsRuntimeSafetyReadinessFixture) {
				t.Helper()
				status, ok, err := readTeamsServiceLocalSupervisorStatus()
				if err != nil || !ok {
					t.Fatalf("read supervisor status: ok=%v err=%v", ok, err)
				}
				status.ChildPID = 0
				status.ChildPGID = 0
				if err := writeTeamsServiceLocalSupervisorStatus(status); err != nil {
					t.Fatalf("remove child from status: %v", err)
				}
			},
			want: "child",
		},
		{name: "owner", want: "owner"},
		{
			name: "lease",
			prepare: func(t *testing.T, fixture *teamsRuntimeSafetyReadinessFixture) {
				t.Helper()
				fixture.seedOwner(t)
			},
			want: "lease",
		},
		{
			name: "first_control_poll",
			prepare: func(t *testing.T, fixture *teamsRuntimeSafetyReadinessFixture) {
				t.Helper()
				fixture.seedAuthority(t, fixture.ownerStartedAt.Add(-time.Hour))
			},
			want: "poll",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lockCLITestHooks(t)
			fixture := newTeamsRuntimeSafetyReadinessFixture(t)
			ticks := installTeamsRuntimeSafetyReadinessTicker(t)
			if tc.prepare != nil {
				tc.prepare(t, fixture)
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				result <- waitTeamsServiceLocalSupervisorReady(ctx, fixture.supervisorPID, fixture.configPath, time.Hour)
			}()
			advanceTeamsRuntimeSafetyReadiness(t, ticks, result, "the missing readiness layer became available")
			cancel()
			var err error
			select {
			case err = <-result:
			case <-time.After(time.Second):
				t.Fatal("readiness waiter did not stop after context cancellation")
			}
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("readiness error = %v, want missing %s layer", err, tc.want)
			}
		})
	}
}

func installTeamsRuntimeSafetyReadinessTicker(t *testing.T) chan time.Time {
	t.Helper()
	ticks := make(chan time.Time)
	previous := teamsServiceLocalSupervisorReadyTicker
	teamsServiceLocalSupervisorReadyTicker = func(time.Duration) (<-chan time.Time, func()) {
		return ticks, func() {}
	}
	t.Cleanup(func() {
		teamsServiceLocalSupervisorReadyTicker = previous
	})
	return ticks
}

func advanceTeamsRuntimeSafetyReadiness(t *testing.T, ticks chan<- time.Time, result <-chan error, before string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("readiness returned before %s: %v", before, err)
	case ticks <- time.Now():
	}
}

type teamsRuntimeSafetyReadinessFixture struct {
	configPath     string
	supervisorPID  int
	childPID       int
	store          *teamsstore.Store
	ownerStartedAt time.Time
}

func newTeamsRuntimeSafetyReadinessFixture(t *testing.T) *teamsRuntimeSafetyReadinessFixture {
	t.Helper()
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	configPath, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable: exe,
		WorkingDir: tmp,
	})
	if err != nil {
		t.Fatalf("install readiness supervisor config: %v", err)
	}
	supervisorPID := 7511
	childPID := 7512
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  supervisorPID,
		SupervisorPGID: 8511,
		ChildPID:       childPID,
		ChildPGID:      8512,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write readiness supervisor status: %v", err)
	}
	st, err := openTeamsStore()
	if err != nil {
		t.Fatalf("open readiness store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close readiness store: %v", err)
		}
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	prevVerify := teamsLocalSupervisorVerifyProcessIdentity
	prevVerifyChild := teamsLocalSupervisorVerifyChildIdentity
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == supervisorPID || pid == childPID }
	teamsLocalSupervisorVerifyProcessIdentity = func(int, string) error { return nil }
	teamsLocalSupervisorVerifyChildIdentity = func(int, teamsServiceSpec) error { return nil }
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorVerifyProcessIdentity = prevVerify
		teamsLocalSupervisorVerifyChildIdentity = prevVerifyChild
	})
	return &teamsRuntimeSafetyReadinessFixture{
		configPath:     configPath,
		supervisorPID:  supervisorPID,
		childPID:       childPID,
		store:          st,
		ownerStartedAt: time.Now().Add(-time.Second),
	}
}

func (f *teamsRuntimeSafetyReadinessFixture) seedOwner(t *testing.T) {
	t.Helper()
	owner, err := teamsstore.CurrentOwner("v1.2.4", "", "", f.ownerStartedAt)
	if err != nil {
		t.Fatalf("CurrentOwner: %v", err)
	}
	owner.PID = f.childPID
	owner.ScopeID = "scope-readiness"
	owner.MachineID = "machine-readiness"
	owner.LeaseGeneration = 11
	owner.StartedAt = f.ownerStartedAt
	owner.LastHeartbeat = time.Now()
	if err := f.store.Update(context.Background(), func(state *teamsstore.State) error {
		state.Scope = teamsstore.ScopeIdentity{ID: owner.ScopeID, Profile: "default"}
		state.ServiceOwner = &owner
		state.ControlChat = teamsstore.ControlChatBinding{ScopeID: owner.ScopeID, MachineID: owner.MachineID, TeamsChatID: "control-chat"}
		return nil
	}); err != nil {
		t.Fatalf("seed readiness owner: %v", err)
	}
}

func (f *teamsRuntimeSafetyReadinessFixture) seedAuthority(t *testing.T, pollAt time.Time) {
	t.Helper()
	f.seedOwner(t)
	if err := f.store.Update(context.Background(), func(state *teamsstore.State) error {
		state.ControlLease = teamsstore.ControlLease{
			ScopeID:         "scope-readiness",
			HolderMachineID: "machine-readiness",
			HolderKind:      teamsstore.MachineKindPrimary,
			Generation:      11,
			Status:          teamsstore.ControlLeaseStatusActive,
			LeaseUntil:      time.Now().Add(time.Minute),
			LastHeartbeat:   time.Now(),
			UpdatedAt:       time.Now(),
		}
		state.ChatPolls["control-chat"] = teamsstore.ChatPollState{
			ChatID:               "control-chat",
			Seeded:               true,
			PollState:            "warm",
			LastSuccessfulPollAt: pollAt,
			UpdatedAt:            pollAt,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed readiness authority: %v", err)
	}
}

func TestTeamsRuntimeSafetyLocalStatusDistinguishesInvisiblePIDNamespaceCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable: filepath.Join(tmp, "bin", "codex-proxy"),
		WorkingDir: tmp,
	})
	if err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  7601,
		SupervisorPGID: 8601,
		ChildPID:       7602,
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write fresh status: %v", err)
	}
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(int) bool { return false }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })

	out, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	text := strings.ToLower(string(out))
	if !strings.Contains(text, "unknown") || !strings.Contains(text, "pid namespace") {
		t.Fatalf("status conflated an invisible PID with a stopped process:\n%s", out)
	}
}

func TestTeamsRuntimeSafetySupervisorLogsCarryTimestampPIDAndComponentCI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local-supervisor.log")
	writer, err := openTeamsServiceLocalSupervisorLogWriter(path)
	if err != nil {
		t.Fatalf("open log writer: %v", err)
	}
	if _, err := writer.Write([]byte("control chat polling is stale\n")); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	line := string(raw)
	timestamp := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`)
	if !timestamp.MatchString(line) ||
		!strings.Contains(line, "pid=") ||
		!strings.Contains(line, "component=") ||
		!strings.Contains(line, "generation=") ||
		!strings.Contains(line, "scope=") {
		t.Fatalf("log line lacks required timestamp/PID/component/generation/scope metadata: %q", line)
	}
}

func TestTeamsRuntimeSafetyRecoverableErrorIncludesOperationContextCI(t *testing.T) {
	lockCLITestHooks(t)

	prevDelay := teamsRunServiceRetryDelay
	prevSleep := teamsRunServiceSleep
	t.Cleanup(func() {
		teamsRunServiceRetryDelay = prevDelay
		teamsRunServiceSleep = prevSleep
	})
	teamsRunServiceRetryDelay = time.Millisecond
	teamsRunServiceSleep = func(context.Context, time.Duration) error { return nil }
	attempts := 0
	var out bytes.Buffer
	if err := runTeamsServiceRetryLoop(context.Background(), &out, func() error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}); err != nil {
		t.Fatalf("retry loop: %v", err)
	}
	lower := strings.ToLower(out.String())
	for _, want := range []string{"operation=", "scope=", "attempt=", "deadline="} {
		if !strings.Contains(lower, want) {
			t.Fatalf("recoverable error omitted %q context:\n%s", want, out.String())
		}
	}
}

func TestTeamsRuntimeSafetyStatusReportsAuthoritativeStoreAndReadinessLayersCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "codex-proxy")
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    runtime.GOOS,
		exe:     exe,
		argv0:   exe,
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd"),
		runner:  &recordingTeamsServiceRunner{output: []byte("inactive\n")},
	})

	out := executeRootForTeamsTest(t, "teams", "status")
	for _, want := range []string{
		"Authoritative store:",
		"Store identity:",
		"Desired service state:",
		"Supervisor state:",
		"Child state:",
		"Listener state:",
		"Live owner:",
		"Control lease:",
		"Active sessions:",
		"Parked sessions:",
		"Historical sessions:",
		"Pollable sessions:",
		"Remediation:",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("teams status omitted %q:\n%s", want, out)
		}
	}
}

func TestTeamsRuntimeSafetyStatusEnumeratesCanonicalAndBrokenLegacyStoresReadOnlyCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	configBase, _ := isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "codex-proxy")
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    runtime.GOOS,
		exe:     exe,
		argv0:   exe,
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd"),
		runner:  &recordingTeamsServiceRunner{output: []byte("inactive\n")},
	})

	scopeID := "scope-runtime-safety-diagnostics"
	canonicalPath := cliStatePathForTest(t, "teams", "scopes", scopeID, "state.json")
	canonical, err := teamsstore.Open(canonicalPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if err := canonical.Update(context.Background(), func(state *teamsstore.State) error {
		state.Scope = teamsstore.ScopeIdentity{ID: scopeID, Profile: "default"}
		state.ControlChat = teamsstore.ControlChatBinding{ScopeID: scopeID, TeamsChatID: "canonical-control"}
		return nil
	}); err != nil {
		_ = canonical.Close()
		t.Fatalf("seed canonical store: %v", err)
	}
	if err := canonical.Close(); err != nil {
		t.Fatalf("close canonical store: %v", err)
	}

	legacyPath := filepath.Join(configBase, "codex-helper", "teams", "scopes", scopeID, "state.json")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o700); err != nil {
		t.Fatalf("mkdir legacy scope: %v", err)
	}
	if err := os.WriteFile(legacyPath, []byte(`{"schema_version":`), 0o640); err != nil {
		t.Fatalf("write broken legacy store: %v", err)
	}

	before := snapshotCLITreeForReadOnlyTest(t, tmp)
	out, statusErr := executeRootForTeamsTestAllowError(t, "teams", "status")
	if statusErr != nil {
		t.Errorf("teams status aborted on one broken retained store instead of reporting every candidate: %v", statusErr)
	}
	for _, want := range []string{
		canonicalPath,
		legacyPath,
		"canonical",
		"legacy",
		"authoritative",
		"load error",
	} {
		if !strings.Contains(strings.ToLower(out), strings.ToLower(want)) {
			t.Errorf("teams status omitted %q while enumerating canonical and broken legacy stores:\n%s", want, out)
		}
	}
	after := snapshotCLITreeForReadOnlyTest(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("read-only status modified a canonical/broken-legacy store family:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestTeamsRuntimeSafetyStatusReportsExactOwnerLeaseAndSessionLayersCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "codex-proxy")
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:    runtime.GOOS,
		exe:     exe,
		argv0:   exe,
		cwd:     tmp,
		unitDir: filepath.Join(tmp, "systemd"),
		runner:  &recordingTeamsServiceRunner{output: []byte("inactive\n")},
	})

	now := time.Now()
	scopeID := "scope-runtime-safety-layers"
	statePath := cliStatePathForTest(t, "teams", "scopes", scopeID, "state.json")
	st, err := teamsstore.Open(statePath)
	if err != nil {
		t.Fatalf("open layered status store: %v", err)
	}
	owner, err := teamsstore.CurrentOwner("v1.2.3", "s-active", "", now)
	if err != nil {
		_ = st.Close()
		t.Fatalf("CurrentOwner: %v", err)
	}
	owner.ScopeID = scopeID
	owner.MachineID = "machine-current"
	owner.LeaseGeneration = 7
	if err := st.Update(context.Background(), func(state *teamsstore.State) error {
		state.Scope = teamsstore.ScopeIdentity{ID: scopeID, Profile: "default"}
		state.ServiceOwner = &owner
		state.ControlLease = teamsstore.ControlLease{
			ScopeID:         scopeID,
			HolderMachineID: "machine-current",
			HolderKind:      teamsstore.MachineKindPrimary,
			Generation:      7,
			Status:          teamsstore.ControlLeaseStatusActive,
			LeaseUntil:      now.Add(time.Minute),
			LastHeartbeat:   now,
			UpdatedAt:       now,
		}
		state.ControlChat = teamsstore.ControlChatBinding{ScopeID: scopeID, MachineID: "machine-current", TeamsChatID: "control-chat"}
		state.Sessions["s-active"] = teamsstore.SessionContext{ID: "s-active", Status: teamsstore.SessionStatusActive, TeamsChatID: "chat-active", CreatedAt: now, UpdatedAt: now}
		state.Sessions["s-parked"] = teamsstore.SessionContext{ID: "s-parked", Status: teamsstore.SessionStatusActive, TeamsChatID: "chat-parked", CreatedAt: now, UpdatedAt: now}
		state.Sessions["s-historical"] = teamsstore.SessionContext{ID: "s-historical", Status: teamsstore.SessionStatusClosed, TeamsChatID: "chat-historical", CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour)}
		state.ChatPolls["control-chat"] = teamsstore.ChatPollState{ChatID: "control-chat", Seeded: true, PollState: "warm", LastSuccessfulPollAt: now, UpdatedAt: now}
		state.ChatPolls["chat-active"] = teamsstore.ChatPollState{ChatID: "chat-active", Seeded: true, PollState: "catchup", LastSuccessfulPollAt: now, UpdatedAt: now}
		state.ChatPolls["chat-parked"] = teamsstore.ChatPollState{ChatID: "chat-parked", Seeded: true, PollState: "parked", ParkedAt: now.Add(-time.Minute), ContinuationPath: "/next", LastSuccessfulPollAt: now, UpdatedAt: now}
		return nil
	}); err != nil {
		_ = st.Close()
		t.Fatalf("seed layered status store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close layered status store: %v", err)
	}

	out := executeRootForTeamsTest(t, "teams", "status")
	for _, check := range []struct {
		prefix string
		wants  []string
	}{
		{prefix: "Authoritative store:", wants: []string{statePath}},
		{prefix: "Live owner:", wants: []string{"machine-current", "generation=7"}},
		{prefix: "Control lease:", wants: []string{"machine-current", "generation=7"}},
		{prefix: "Active sessions:", wants: []string{"2"}},
		{prefix: "Parked sessions:", wants: []string{"1"}},
		{prefix: "Historical sessions:", wants: []string{"1"}},
		{prefix: "Pollable sessions:", wants: []string{"1"}},
		{prefix: "Listener state:", wants: []string{"running"}},
	} {
		runtimeSafetyRequireStatusLine(t, out, check.prefix, check.wants...)
	}
}

func runtimeSafetyRequireStatusLine(t *testing.T, out string, prefix string, wants ...string) {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		for _, want := range wants {
			if !strings.Contains(line, want) {
				t.Errorf("status line %q omitted %q", prefix, want)
			}
		}
		return
	}
	t.Errorf("status output omitted line %q", prefix)
}

func TestTeamsRuntimeSafetyLocalStatusNamesAutostartGuaranteesPreciselyCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable: filepath.Join(tmp, "bin", "codex-proxy"),
		WorkingDir: tmp,
	})
	if err != nil {
		t.Fatalf("install local supervisor: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:        teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:     configPath,
		SupervisorPID:  os.Getpid(),
		SupervisorPGID: teamsLocalSupervisorCurrentProcessGroupID(),
		State:          "running",
		UpdatedAt:      time.Now(),
	}); err != nil {
		t.Fatalf("write status: %v", err)
	}
	prevAlive := teamsLocalSupervisorProcessAlive
	prevVerify := teamsLocalSupervisorVerifyProcessIdentity
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == os.Getpid() }
	teamsLocalSupervisorVerifyProcessIdentity = func(int, string) error { return nil }
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorVerifyProcessIdentity = prevVerify
	})

	out, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	for _, want := range []string{
		"Local supervisor: terminal-independent",
		"Windows autostart:",
		"Reboot persistence: not guaranteed",
	} {
		if !strings.Contains(string(out), want) {
			t.Fatalf("local status omitted %q:\n%s", want, out)
		}
	}
}

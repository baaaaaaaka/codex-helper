package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/teams"
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
	if strings.TrimSpace(spec.WorkingDir) == "" {
		t.Fatal("service WorkingDir must remain explicit and stable")
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

func TestTeamsRuntimeSafetyWSLRetireSkipsAlreadyDisabledTasksCI(t *testing.T) {
	command := buildTeamsServiceWSLRetireTaskCommand("Codex Helper Teams Bridge", true)
	lower := strings.ToLower(command)
	if !strings.Contains(lower, "disabled") || !strings.Contains(lower, "continue") {
		t.Fatalf("retire command must skip already-disabled tasks instead of requiring Disable-ScheduledTask again:\n%s", command)
	}
}

func TestTeamsRuntimeSafetyWSLInteropFailureWithoutTaskConfigStillStartsLocalSupervisorCI(t *testing.T) {
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
	if _, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "start"); err != nil {
		t.Fatalf("local start must degrade safely when WSLInterop is unavailable and no Scheduled Task is configured: %v", err)
	}
	if !started {
		t.Fatal("local supervisor was not started after optional WSL cleanup became unavailable")
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

	err := waitTeamsServiceLocalSupervisorReady(context.Background(), supervisorPID, configPath, 20*time.Millisecond)
	if err == nil {
		t.Fatal("service start reported ready with no child owner, listener, or first control poll")
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
		!strings.Contains(line, "generation=") {
		t.Fatalf("log line lacks required timestamp/PID/component/generation metadata: %q", line)
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
	if !strings.Contains(strings.ToLower(out.String()), "operation=") {
		t.Fatalf("recoverable error omitted the failed operation:\n%s", out.String())
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
		"Supervisor state:",
		"Listener state:",
		"Pollable sessions:",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("teams status omitted %q:\n%s", want, out)
		}
	}
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

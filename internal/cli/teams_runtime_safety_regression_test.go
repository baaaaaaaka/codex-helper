package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/selfupdate"
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
		"CODEX_HOME",
		"CODEX_DIR",
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
	for _, name := range []string{"CODEX_CONFIG_DIR", "CODEX_HELPER_STATE_DIR"} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			t.Fatalf("TestMain retained ambient %s=%q", name, value)
		}
	}
}

func TestTeamsRuntimeSafetyManagedServiceArgsAlwaysCarryMigrationAuthorityMarkerCI(t *testing.T) {
	args := buildTeamsServiceRunArgs(teamsServiceSpec{})
	found := false
	for _, arg := range args {
		if arg == "--managed-service-child" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("managed service args omitted hidden migration authority marker: %v", args)
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsNativeServiceWorkingDirCI(t *testing.T) {
	lockCLITestHooks(t)
	for _, tc := range []struct {
		name  string
		goos  string
		write func(teamsServiceSpec) string
	}{
		{name: "systemd", goos: "linux", write: buildTeamsServiceUnit},
		{name: "launchagent", goos: "darwin", write: buildTeamsServiceLaunchAgentPlist},
		{name: "windows-task", goos: "windows", write: buildTeamsServiceWindowsTaskXML},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			exe := filepath.Join(tmp, "bin", "codex-proxy")
			writeVersionedHelperForServiceTest(t, exe, "1.2.3")
			available := true
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:                 tc.goos,
				exe:                  exe,
				argv0:                exe,
				cwd:                  tmp,
				unitDir:              filepath.Join(tmp, "systemd"),
				launchAgentDir:       filepath.Join(tmp, "launchagents"),
				windowsTaskDir:       filepath.Join(tmp, "windows-task"),
				runner:               &recordingTeamsServiceRunner{},
				systemdUserAvailable: &available,
			})
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				t.Fatalf("select backend: %v", err)
			}
			path, err := backend.Path()
			if err != nil {
				t.Fatalf("backend path: %v", err)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("mkdir backend config: %v", err)
			}
			missing := filepath.Join(tmp, "deleted-working-directory")
			spec := teamsServiceSpec{Executable: exe, WorkingDir: missing}
			if err := os.WriteFile(path, []byte(tc.write(spec)), 0o600); err != nil {
				t.Fatalf("write backend config: %v", err)
			}
			if err := preflightPersistedTeamsServiceForUpdate(); err == nil ||
				!strings.Contains(strings.ToLower(err.Error()), "working") {
				t.Fatalf("native preflight error = %v, want WorkingDir failure", err)
			}
		})
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsWSLServiceExecutableCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		exe:            exe,
		argv0:          exe,
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		runner:         &recordingTeamsServiceRunner{},
	})
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("select WSL backend: %v", err)
	}
	path, err := backend.Path()
	if err != nil {
		t.Fatalf("WSL backend path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir WSL task dir: %v", err)
	}
	spec := teamsServiceSpec{Executable: exe, WorkingDir: tmp, Environment: map[string]string{"HTTPS_PROXY": "https://proxy.example"}}
	if err := os.WriteFile(path, []byte(buildTeamsServiceWSLTaskConfig(backend.Name(), buildTeamsServiceWSLArguments(spec))), 0o600); err != nil {
		t.Fatalf("write WSL task config: %v", err)
	}
	if err := preflightPersistedTeamsServiceForUpdate(); err != nil {
		t.Fatalf("valid WSL task preflight: %v", err)
	}
	if err := os.Remove(exe); err != nil {
		t.Fatalf("remove WSL helper executable: %v", err)
	}
	if err := preflightPersistedTeamsServiceForUpdate(); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "executable") {
		t.Fatalf("missing WSL executable preflight error = %v", err)
	}
	if err := os.WriteFile(path, []byte(
		"TaskName="+backend.Name()+"\nCommand=wsl.exe\nArguments=-d Ubuntu -- true\n",
	), 0o600); err != nil {
		t.Fatalf("write unknown WSL task format: %v", err)
	}
	if err := preflightPersistedTeamsServiceForUpdate(); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "repair") {
		t.Fatalf("unknown WSL format preflight error = %v", err)
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsWSLStartupFallbackCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, exe, "1.2.3")
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		isWSL:          true,
		wslDistro:      "Ubuntu",
		wslLinuxUser:   "alice",
		exe:            exe,
		argv0:          exe,
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		runner:         &recordingTeamsServiceRunner{},
	})
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("select WSL backend: %v", err)
	}
	wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend)
	if !ok {
		t.Fatalf("backend = %T, want WSL Windows Task backend", backend)
	}
	fallbackPath, err := wslBackend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("fallback marker path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(fallbackPath), 0o700); err != nil {
		t.Fatalf("mkdir fallback marker directory: %v", err)
	}
	taskPath, err := backend.Path()
	if err != nil {
		t.Fatalf("task marker path: %v", err)
	}
	validTaskSpec := teamsServiceSpec{Executable: exe, WorkingDir: tmp}
	if err := os.WriteFile(
		taskPath,
		[]byte(buildTeamsServiceWSLTaskConfig(backend.Name(), buildTeamsServiceWSLArguments(validTaskSpec))),
		0o600,
	); err != nil {
		t.Fatalf("write stale valid WSL task marker: %v", err)
	}
	missingWorkingDir := filepath.Join(tmp, "deleted-working-directory")
	spec := teamsServiceSpec{Executable: exe, WorkingDir: missingWorkingDir}
	config := buildTeamsServiceWSLStartupFallbackConfig(backend.Name(), buildTeamsServiceWSLArguments(spec))
	if err := os.WriteFile(fallbackPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write WSL fallback marker: %v", err)
	}
	if err := preflightPersistedTeamsServiceForUpdate(); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "working") {
		t.Fatalf("WSL fallback preflight error = %v, want WorkingDir failure", err)
	}
}

func TestTeamsRuntimeSafetyAutoUpdatePreflightsWindowsTaskLaunchChainCI(t *testing.T) {
	lockCLITestHooks(t)
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, path string, spec teamsServiceSpec)
	}{
		{
			name: "direct",
			write: func(t *testing.T, path string, spec teamsServiceSpec) {
				t.Helper()
				if err := os.WriteFile(path, []byte(buildTeamsServiceWindowsTaskXML(spec)), 0o600); err != nil {
					t.Fatalf("write direct Windows task: %v", err)
				}
			},
		},
		{
			name: "inline-powershell",
			write: func(t *testing.T, path string, spec teamsServiceSpec) {
				t.Helper()
				spec.Environment = map[string]string{"HTTPS_PROXY": "https://proxy.example"}
				if err := os.WriteFile(path, []byte(buildTeamsServiceWindowsTaskXML(spec)), 0o600); err != nil {
					t.Fatalf("write inline PowerShell task: %v", err)
				}
			},
		},
		{
			name: "vbs-powershell-chain",
			write: func(t *testing.T, path string, spec teamsServiceSpec) {
				t.Helper()
				spec = teamsServiceSpecWithWindowsTaskLaunchers(spec, path, filepath.Join(filepath.Dir(path), "watchdog.xml"))
				if err := writeTeamsServiceWindowsTaskLauncherFiles(
					spec.WindowsTaskLauncherPath,
					spec,
					buildTeamsServiceRunArgs(spec),
				); err != nil {
					t.Fatalf("write Windows launcher chain: %v", err)
				}
				if err := os.WriteFile(path, []byte(buildTeamsServiceWindowsTaskXML(spec)), 0o600); err != nil {
					t.Fatalf("write launcher Windows task: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			exe := filepath.Join(tmp, "bin", "codex-proxy.exe")
			writeVersionedHelperForServiceTest(t, exe, "1.2.3")
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           "windows",
				exe:            exe,
				argv0:          exe,
				cwd:            tmp,
				windowsTaskDir: filepath.Join(tmp, "tasks"),
				runner:         &recordingTeamsServiceRunner{},
			})
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				t.Fatalf("select Windows backend: %v", err)
			}
			path, err := backend.Path()
			if err != nil {
				t.Fatalf("Windows backend path: %v", err)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("mkdir Windows task dir: %v", err)
			}
			tc.write(t, path, teamsServiceSpec{Executable: exe, WorkingDir: tmp})
			if err := preflightPersistedTeamsServiceForUpdate(); err != nil {
				t.Fatalf("valid Windows task preflight: %v", err)
			}
		})
	}
}

func TestTeamsRuntimeSafetyAutoUpdateRejectsMissingOrUnknownWindowsTaskLauncherCI(t *testing.T) {
	lockCLITestHooks(t)
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, path string, spec teamsServiceSpec)
	}{
		{
			name: "missing-vbs",
			mutate: func(t *testing.T, _ string, spec teamsServiceSpec) {
				t.Helper()
				if err := os.Remove(spec.WindowsTaskLauncherPath); err != nil {
					t.Fatalf("remove VBS launcher: %v", err)
				}
			},
		},
		{
			name: "unknown-vbs",
			mutate: func(t *testing.T, _ string, spec teamsServiceSpec) {
				t.Helper()
				if err := os.WriteFile(spec.WindowsTaskLauncherPath, []byte(`WScript.Echo "unknown"`), 0o600); err != nil {
					t.Fatalf("replace VBS launcher: %v", err)
				}
			},
		},
		{
			name: "missing-powershell",
			mutate: func(t *testing.T, _ string, spec teamsServiceSpec) {
				t.Helper()
				psPath := strings.TrimSuffix(spec.WindowsTaskLauncherPath, filepath.Ext(spec.WindowsTaskLauncherPath)) + ".ps1"
				if err := os.Remove(psPath); err != nil {
					t.Fatalf("remove PowerShell launcher: %v", err)
				}
			},
		},
		{
			name: "missing-helper",
			mutate: func(t *testing.T, _ string, spec teamsServiceSpec) {
				t.Helper()
				if err := os.Remove(spec.Executable); err != nil {
					t.Fatalf("remove helper executable: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			exe := filepath.Join(tmp, "bin", "codex-proxy.exe")
			writeVersionedHelperForServiceTest(t, exe, "1.2.3")
			withTeamsServiceTestHooks(t, teamsServiceTestHooks{
				goos:           "windows",
				exe:            exe,
				argv0:          exe,
				cwd:            tmp,
				windowsTaskDir: filepath.Join(tmp, "tasks"),
				runner:         &recordingTeamsServiceRunner{},
			})
			backend, err := teamsServiceBackendForCurrentPlatform()
			if err != nil {
				t.Fatalf("select Windows backend: %v", err)
			}
			path, err := backend.Path()
			if err != nil {
				t.Fatalf("Windows backend path: %v", err)
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
				t.Fatalf("mkdir Windows task dir: %v", err)
			}
			spec := teamsServiceSpecWithWindowsTaskLaunchers(
				teamsServiceSpec{Executable: exe, WorkingDir: tmp},
				path,
				filepath.Join(filepath.Dir(path), "watchdog.xml"),
			)
			if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsTaskLauncherPath, spec, buildTeamsServiceRunArgs(spec)); err != nil {
				t.Fatalf("write Windows launcher chain: %v", err)
			}
			if err := os.WriteFile(path, []byte(buildTeamsServiceWindowsTaskXML(spec)), 0o600); err != nil {
				t.Fatalf("write Windows task: %v", err)
			}
			tc.mutate(t, path, spec)
			if err := preflightPersistedTeamsServiceForUpdate(); err == nil ||
				(!strings.Contains(strings.ToLower(err.Error()), "launcher") &&
					!strings.Contains(strings.ToLower(err.Error()), "repair") &&
					!strings.Contains(strings.ToLower(err.Error()), "executable")) {
				t.Fatalf("invalid Windows launcher preflight error = %v", err)
			}
		})
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

func TestTeamsRuntimeSafetyLocalStartReportsSupervisorOnlyUntilListenerIsReadyCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND", "local-supervisor")
	exe := filepath.Join(tmp, "bin", "codex-proxy")
	supervisorPID := 7501
	systemdUnavailable := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  exe,
		argv0:                exe,
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdUnavailable,
		localStartDetached: func(_ context.Context, configPath string, logPath string, _ teamsServiceSpec) (int, error) {
			return supervisorPID, writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
				Version:        teamsServiceLocalSupervisorStatusVersion,
				ConfigPath:     configPath,
				LogPath:        logPath,
				SupervisorPID:  supervisorPID,
				SupervisorPGID: 8501,
				State:          "running",
				UpdatedAt:      time.Now(),
			})
		},
		localVerifyProcessIdentity: func(int, string) error { return nil },
	})
	prevAlive := teamsLocalSupervisorProcessAlive
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == supervisorPID }
	t.Cleanup(func() { teamsLocalSupervisorProcessAlive = prevAlive })
	if _, err := (teamsServiceLocalSupervisorBackend{}).Install(context.Background(), teamsServiceSpec{
		Executable: exe,
		WorkingDir: tmp,
	}); err != nil {
		t.Fatalf("install local supervisor fixture: %v", err)
	}

	cmd := newTeamsServiceCmd(&rootOptions{})
	cmd.SetArgs([]string{"start"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("start local supervisor: %v", err)
	}
	lower := strings.ToLower(out.String())
	for _, want := range []string{"supervisor started", "child starting"} {
		if !strings.Contains(lower, want) {
			t.Errorf("start output omitted %q:\n%s", want, out.String())
		}
	}
	for _, forbidden := range []string{"teams ready", "listener ready", "first poll successful"} {
		if strings.Contains(lower, forbidden) {
			t.Errorf("start output claimed %q before listener readiness was observed:\n%s", forbidden, out.String())
		}
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

func TestTeamsRuntimeSafetyAutoUpdatePreflightsUnixExecutableModeWithoutExtraProbeCI(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix executable mode contract")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	currentExe := filepath.Join(tmp, "bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, currentExe, "1.2.3")
	serviceExe := filepath.Join(tmp, "service-bin", "codex-proxy")
	writeVersionedHelperForServiceTest(t, serviceExe, "1.2.3")
	if err := os.Chmod(serviceExe, 0o600); err != nil {
		t.Fatalf("remove service executable bits: %v", err)
	}
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
		Executable:  serviceExe,
		WorkingDir:  tmp,
		Environment: map[string]string{},
	}); err != nil {
		t.Fatalf("install non-executable local-supervisor fixture: %v", err)
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
		selfupdate.Candidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before executable mode validation")
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "not executable") {
		t.Fatalf("Apply error = %v, want executable-mode preflight failure", err)
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
		selfupdate.Candidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before the persisted service executable was proven restartable")
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "executable") {
		t.Fatalf("Apply error = %v, want an actionable executable preflight failure", err)
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
		selfupdate.Candidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before the persisted service configuration was proven restartable")
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "working") {
		t.Fatalf("Apply error = %v, want an actionable WorkingDir preflight failure", err)
	}
}

func TestTeamsRuntimeSafetyServiceSpecNormalizesAllPersistedPathOverridesCI(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	invocationDir := filepath.Join(tmp, "invocation")
	if err := os.MkdirAll(invocationDir, 0o700); err != nil {
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

	spec, err := buildTeamsServiceSpec(
		teamsServiceSpecEnvironmentOverrides(map[string]string{
			"CODEX_HELPER_TEAMS_AUTH_CONFIG":      "auth/config.json",
			"CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE": "tokens/chat.json",
			envTeamsASRLlamaModel:                 "models/qwen.gguf",
			envTeamsASRCommand:                    "bin/asr",
			envTeamsASRFFmpeg:                     "ffmpeg",
		}),
	)
	if err != nil {
		t.Fatalf("buildTeamsServiceSpec: %v", err)
	}
	for name, relative := range map[string]string{
		"CODEX_HELPER_TEAMS_AUTH_CONFIG":      "auth/config.json",
		"CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE": "tokens/chat.json",
		envTeamsASRLlamaModel:                 "models/qwen.gguf",
		envTeamsASRCommand:                    "bin/asr",
	} {
		want := filepath.Join(invocationDir, filepath.FromSlash(relative))
		if got := spec.Environment[name]; got != want || !filepath.IsAbs(got) {
			t.Fatalf("%s = %q, want invocation-time absolute path %q", name, got, want)
		}
	}
	if got := spec.Environment[envTeamsASRFFmpeg]; got != "ffmpeg" {
		t.Fatalf("bare ffmpeg command was incorrectly treated as a path: %q", got)
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

	spec, err := buildTeamsServiceSpec()
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

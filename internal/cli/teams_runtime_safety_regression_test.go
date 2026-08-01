package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
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
		stringPtr("registry.json"),
		teamsServiceSpecEnvironmentOverrides(map[string]string{
			"CODEX_HELPER_TEAMS_AUTH_CONFIG": "auth/config.json",
			"CODEX_HELPER_TEAMS_TOKEN_CACHE": "tokens/chat.json",
			"CODEX_HELPER_BEACON_STORE":      "beacon/state.json",
			envTeamsASRLlamaModel:            "models/qwen.gguf",
			envTeamsASRCommand:               "bin/asr",
			envTeamsASRFFmpeg:                "ffmpeg",
		}),
	)
	if err != nil {
		t.Fatalf("buildTeamsServiceSpec: %v", err)
	}
	for name, relative := range map[string]string{
		"CODEX_HELPER_TEAMS_AUTH_CONFIG": "auth/config.json",
		"CODEX_HELPER_TEAMS_TOKEN_CACHE": "tokens/chat.json",
		"CODEX_HELPER_BEACON_STORE":      "beacon/state.json",
		envTeamsASRLlamaModel:            "models/qwen.gguf",
		envTeamsASRCommand:               "bin/asr",
	} {
		want := filepath.Join(invocationDir, filepath.FromSlash(relative))
		if got := spec.Environment[name]; got != want || !filepath.IsAbs(got) {
			t.Fatalf("%s = %q, want invocation-time absolute path %q", name, got, want)
		}
	}
	if got := spec.Environment[envTeamsASRFFmpeg]; got != "ffmpeg" {
		t.Fatalf("bare ffmpeg command was incorrectly treated as a path: %q", got)
	}
	if spec.RegistryPath != filepath.Join(invocationDir, "registry.json") {
		t.Fatalf("registry path = %q, want invocation-time absolute path", spec.RegistryPath)
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
		teams.HelperAutoUpdateCandidate{TagName: "v1.2.4", Version: "1.2.4"},
	)
	if updateCalled {
		t.Fatal("helper binary was replaced before executable mode validation")
	}
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "not executable") {
		t.Fatalf("Apply error = %v, want executable-mode preflight failure", err)
	}
}

func TestTeamsRuntimeSafetyForegroundDualStoreFailsClosedWithoutMutationCI(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "")
	scope, canonicalPath, legacyPath := seedCLIRuntimeSafetyDualStore(t, "foreground")

	before := snapshotCLITreeForReadOnlyTest(t, tmp)
	migrated, err := prepareManagedTeamsRuntimeStore(context.Background(), scope, true, false)
	if err == nil || !strings.Contains(err.Error(), "managed Teams service") {
		t.Fatalf("foreground dual-store error = %v, want managed-service guidance", err)
	}
	if migrated {
		t.Fatal("foreground dual-store path reported a completed migration")
	}
	after := snapshotCLITreeForReadOnlyTest(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("foreground dual-store check modified files: before=%v after=%v", before, after)
	}
	for _, path := range []string{canonicalPath, legacyPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("foreground dual-store check removed %s: %v", path, err)
		}
	}
}

func TestTeamsRuntimeSafetyManagedChildCompletesOfflineDualStoreTakeoverCI(t *testing.T) {
	lockCLITestHooks(t)
	previousGOOS := teamsServiceGOOS
	t.Cleanup(func() { teamsServiceGOOS = previousGOOS })
	for _, goos := range []string{"linux", "darwin", "windows"} {
		for _, managedServiceChild := range []bool{true, false} {
			spec := "new-spec"
			if !managedServiceChild {
				spec = "persisted-old-spec"
			}
			t.Run(goos+"/"+spec, func(t *testing.T) {
				teamsServiceGOOS = func() string { return goos }
				tmp := t.TempDir()
				isolateTeamsUserDirsForTest(t, tmp)
				t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
				t.Setenv("CODEX_HELPER_TEAMS_SERVICE_MODE", "background")
				t.Setenv(envTeamsCodexChild, "")
				suffix := "managed-" + goos + "-" + spec
				scope, canonicalPath, legacyPath := seedCLIRuntimeSafetyDualStore(t, suffix)

				migrated, err := prepareManagedTeamsRuntimeStore(context.Background(), scope, false, managedServiceChild)
				if err != nil {
					t.Fatalf("managed offline takeover: %v", err)
				}
				if !migrated {
					t.Fatal("managed dual-store path did not report completed migration")
				}
				if _, err := os.Stat(canonicalPath); err != nil {
					t.Fatalf("managed offline takeover removed canonical store: %v", err)
				}
				if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
					t.Fatalf("managed offline takeover left legacy store in candidate path: %v", err)
				}
				backupPath, err := appdirs.LegacyConfigPath(
					"teams",
					"migration-backups",
					scope.ID,
					"state.json",
				)
				if err != nil {
					t.Fatalf("managed offline takeover backup path: %v", err)
				}
				if _, err := os.Stat(backupPath); err != nil {
					t.Fatalf("managed offline takeover backup missing at %s: %v", backupPath, err)
				}
				state, err := teamsstore.LoadPathReadOnly(context.Background(), canonicalPath)
				if err != nil {
					t.Fatalf("load canonical store after managed takeover: %v", err)
				}
				if state.ControlChat.TeamsChatID != "canonical-"+suffix {
					t.Fatalf("managed takeover changed canonical business data: %#v", state.ControlChat)
				}
			})
		}
	}
}

func seedCLIRuntimeSafetyDualStore(t *testing.T, suffix string) (teamsstore.ScopeIdentity, string, string) {
	t.Helper()
	scope := teamsstore.ScopeIdentity{
		ID:            "scope-cli-offline-" + suffix,
		AccountID:     "account-" + suffix,
		UserPrincipal: suffix + "@example.test",
		Profile:       "default",
	}
	canonicalPath, err := teams.DefaultStorePathForScope(scope.ID)
	if err != nil {
		t.Fatalf("canonical store path: %v", err)
	}
	legacyPath, err := appdirs.LegacyConfigPath(
		"teams",
		"scopes",
		scope.ID,
		"state.json",
	)
	if err != nil {
		t.Fatalf("legacy store path: %v", err)
	}
	for path, chatID := range map[string]string{
		canonicalPath: "canonical-" + suffix,
		legacyPath:    "legacy-" + suffix,
	} {
		store, err := teamsstore.Open(path)
		if err != nil {
			t.Fatalf("open store %s: %v", path, err)
		}
		if err := store.Update(context.Background(), func(state *teamsstore.State) error {
			state.Scope = scope
			state.ControlChat.TeamsChatID = chatID
			return nil
		}); err != nil {
			_ = store.Close()
			t.Fatalf("seed store %s: %v", path, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close store %s: %v", path, err)
		}
	}
	return scope, canonicalPath, legacyPath
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

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(""))
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

func TestTeamsRuntimeSafetyDeferredMigrationUsesBoundedExponentialRetryCI(t *testing.T) {
	lockCLITestHooks(t)
	previousBase := teamsRunServiceRetryDelay
	previousMax := teamsRunServiceMaxRetryDelay
	t.Cleanup(func() {
		teamsRunServiceRetryDelay = previousBase
		teamsRunServiceMaxRetryDelay = previousMax
	})
	teamsRunServiceRetryDelay = time.Second
	teamsRunServiceMaxRetryDelay = 5 * time.Second
	err := &teams.RuntimeStoreTakeoverDeferredError{Reason: "writer lock is still held"}
	for attempt, want := range map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 5 * time.Second,
		8: 5 * time.Second,
	} {
		if got := teamsRunRetryDelay(err, attempt); got != want {
			t.Fatalf("attempt %d delay = %s, want %s", attempt, got, want)
		}
	}
	if got := teamsRunRetryDelay(&teams.GraphStatusError{StatusCode: 502}, 8); got != time.Second {
		t.Fatalf("ordinary Graph retry delay = %s, want fixed base delay", got)
	}
}

func TestTeamsRuntimeSafetyActiveMigrationLeaseIsSingleWriteAndClearsOnSuccessCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	previousComplete := teamsCompleteOfflineStorePlan
	previousNow := teamsServiceMigrationNow
	previousStart := teamsLocalSupervisorProcessStartTime
	previousAlive := teamsLocalSupervisorProcessAlive
	t.Cleanup(func() {
		teamsCompleteOfflineStorePlan = previousComplete
		teamsServiceMigrationNow = previousNow
		teamsLocalSupervisorProcessStartTime = previousStart
		teamsLocalSupervisorProcessAlive = previousAlive
		_ = clearTeamsServiceMigrationBlockedState()
	})

	now := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	teamsServiceMigrationNow = func() time.Time { return now }
	teamsLocalSupervisorProcessStartTime = func(pid int) (string, error) {
		if pid != os.Getpid() {
			return "", fmt.Errorf("unexpected pid %d", pid)
		}
		return "self-start", nil
	}
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == os.Getpid() }

	var attempts int
	teamsCompleteOfflineStorePlan = func(context.Context, teams.RuntimeStorePlan) error {
		attempts++
		state, exists, err := readTeamsServiceMigrationState()
		if err != nil || !exists || state.Phase != teamsServiceMigrationPhaseActive {
			t.Fatalf("migration attempt %d state=(%+v,%t,%v), want active lease", attempts, state, exists, err)
		}
		if attempts < 3 {
			return &teams.RuntimeStoreTakeoverDeferredError{Reason: "writer lock is still held"}
		}
		return nil
	}

	plan := teams.RuntimeStorePlan{Action: teams.RuntimeStoreActionMigrateLegacy}
	if err := completeManagedTeamsRuntimeStorePlan(context.Background(), plan); err == nil {
		t.Fatal("first migration attempt succeeded, want deferred")
	}
	first, exists, err := readTeamsServiceMigrationState()
	if err != nil || !exists {
		t.Fatalf("read first active lease: state=%+v exists=%t err=%v", first, exists, err)
	}

	now = now.Add(time.Minute)
	if err := completeManagedTeamsRuntimeStorePlan(context.Background(), plan); err == nil {
		t.Fatal("second migration attempt succeeded, want deferred")
	}
	second, exists, err := readTeamsServiceMigrationState()
	if err != nil || !exists {
		t.Fatalf("read second active lease: state=%+v exists=%t err=%v", second, exists, err)
	}
	if !second.StartedAt.Equal(first.StartedAt) || !second.UpdatedAt.Equal(first.UpdatedAt) {
		t.Fatalf("deferred retry rewrote active lease: first=%+v second=%+v", first, second)
	}

	now = now.Add(time.Minute)
	if err := completeManagedTeamsRuntimeStorePlan(context.Background(), plan); err != nil {
		t.Fatalf("successful migration attempt: %v", err)
	}
	if _, exists, err := readTeamsServiceMigrationState(); err != nil || exists {
		t.Fatalf("active lease remained after success: exists=%t err=%v", exists, err)
	}
}

func TestTeamsRuntimeSafetyWatchdogHonorsOnlyLiveBoundedMigrationLeaseCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	previousNow := teamsServiceMigrationNow
	previousStart := teamsLocalSupervisorProcessStartTime
	previousAlive := teamsLocalSupervisorProcessAlive
	previousInstalled := teamsServiceWatchdogInstalled
	previousActive := teamsServiceWatchdogActive
	previousPaths := teamsServiceWatchdogStorePaths
	t.Cleanup(func() {
		teamsServiceMigrationNow = previousNow
		teamsLocalSupervisorProcessStartTime = previousStart
		teamsLocalSupervisorProcessAlive = previousAlive
		teamsServiceWatchdogInstalled = previousInstalled
		teamsServiceWatchdogActive = previousActive
		teamsServiceWatchdogStorePaths = previousPaths
		_ = clearTeamsServiceMigrationBlockedState()
	})

	startedAt := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	teamsServiceMigrationNow = func() time.Time { return startedAt }
	teamsLocalSupervisorProcessStartTime = func(pid int) (string, error) {
		if pid == os.Getpid() {
			return "self-start", nil
		}
		return "other-start", nil
	}
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == os.Getpid() }
	teamsServiceWatchdogInstalled = func() (bool, error) { return true, nil }
	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return true, nil }
	pathCalls := 0
	teamsServiceWatchdogStorePaths = func() ([]string, error) {
		pathCalls++
		return nil, nil
	}

	if err := ensureTeamsServiceMigrationActiveState(); err != nil {
		t.Fatalf("write active migration lease: %v", err)
	}
	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: startedAt.Add(time.Minute)})
	snapshot, err := collectTeamsServiceWatchdogSnapshot(context.Background(), opts)
	if err != nil {
		t.Fatalf("collect active migration snapshot: %v", err)
	}
	if !snapshot.MigrationActive || pathCalls != 0 {
		t.Fatalf("active snapshot=%+v pathCalls=%d, want protected lease without store scan", snapshot, pathCalls)
	}
	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || decision.Stale || !strings.Contains(decision.Reason, "offline store migration") {
		t.Fatalf("active migration decision=%+v, want non-stale noop", decision)
	}

	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return false, nil }
	snapshot, err = collectTeamsServiceWatchdogSnapshot(context.Background(), opts)
	if err != nil {
		t.Fatalf("collect inactive-service migration snapshot: %v", err)
	}
	if snapshot.MigrationActive || pathCalls != 1 {
		t.Fatalf("inactive-service snapshot=%+v pathCalls=%d, want normal inspection", snapshot, pathCalls)
	}
	decision = evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("inactive-service decision=%+v, want start despite stale active marker", decision)
	}
	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return true, nil }

	opts.Now = startedAt.Add(defaultTeamsServiceWatchdogMigrationActiveFor + time.Second)
	snapshot, err = collectTeamsServiceWatchdogSnapshot(context.Background(), opts)
	if err != nil {
		t.Fatalf("collect expired migration snapshot: %v", err)
	}
	if snapshot.MigrationActive || pathCalls != 2 {
		t.Fatalf("expired snapshot=%+v pathCalls=%d, want normal store inspection", snapshot, pathCalls)
	}

	teamsLocalSupervisorProcessStartTime = func(int) (string, error) { return "reused-pid", nil }
	opts.Now = startedAt.Add(time.Minute)
	snapshot, err = collectTeamsServiceWatchdogSnapshot(context.Background(), opts)
	if err != nil {
		t.Fatalf("collect reused-pid migration snapshot: %v", err)
	}
	if snapshot.MigrationActive || pathCalls != 3 {
		t.Fatalf("reused-pid snapshot=%+v pathCalls=%d, want no migration protection", snapshot, pathCalls)
	}
}

func TestTeamsRuntimeSafetyWatchdogFailsClosedForInvalidMigrationStateCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	path, err := teamsServiceMigrationBlockedPath()
	if err != nil {
		t.Fatalf("migration state path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create migration state directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("{not-json\n"), 0o600); err != nil {
		t.Fatalf("write invalid migration state: %v", err)
	}

	previousInstalled := teamsServiceWatchdogInstalled
	previousActive := teamsServiceWatchdogActive
	previousPaths := teamsServiceWatchdogStorePaths
	t.Cleanup(func() {
		teamsServiceWatchdogInstalled = previousInstalled
		teamsServiceWatchdogActive = previousActive
		teamsServiceWatchdogStorePaths = previousPaths
		_ = os.Remove(path)
	})
	active := true
	pathCalls := 0
	teamsServiceWatchdogInstalled = func() (bool, error) { return true, nil }
	teamsServiceWatchdogActive = func(context.Context) (bool, error) { return active, nil }
	teamsServiceWatchdogStorePaths = func() ([]string, error) {
		pathCalls++
		return nil, nil
	}

	opts := normalizeTeamsServiceWatchdogOptions(teamsServiceWatchdogOptions{Now: time.Now()})
	snapshot, err := collectTeamsServiceWatchdogSnapshot(context.Background(), opts)
	if err != nil {
		t.Fatalf("collect invalid migration snapshot: %v", err)
	}
	if !snapshot.MigrationStateUnknown {
		t.Fatalf("invalid migration snapshot=%+v, want unknown state", snapshot)
	}
	if pathCalls != 0 {
		t.Fatalf("active service inspected stores %d times with invalid migration state, want 0", pathCalls)
	}
	decision := evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{ConsecutiveStale: 2}, opts)
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "invalid") {
		t.Fatalf("invalid migration decision=%+v, want fail-closed noop", decision)
	}

	active = false
	snapshot, err = collectTeamsServiceWatchdogSnapshot(context.Background(), opts)
	if err != nil {
		t.Fatalf("collect stopped-service migration snapshot: %v", err)
	}
	if snapshot.MigrationStateUnknown || pathCalls != 1 {
		t.Fatalf("stopped-service migration snapshot=%+v pathCalls=%d, want normal inspection", snapshot, pathCalls)
	}
	decision = evaluateTeamsServiceWatchdog(snapshot, teamsServiceWatchdogState{}, opts)
	if decision.Action != teamsServiceWatchdogActionStart {
		t.Fatalf("stopped-service migration decision=%+v, want start", decision)
	}
}

func TestTeamsRuntimeSafetyLegacyMigrationMarkerDefaultsToBlockedCI(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)

	previousStart := teamsLocalSupervisorProcessStartTime
	previousAlive := teamsLocalSupervisorProcessAlive
	t.Cleanup(func() {
		teamsLocalSupervisorProcessStartTime = previousStart
		teamsLocalSupervisorProcessAlive = previousAlive
		_ = clearTeamsServiceMigrationBlockedState()
	})
	teamsLocalSupervisorProcessStartTime = func(int) (string, error) { return "self-start", nil }
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == os.Getpid() }

	path, err := teamsServiceMigrationBlockedPath()
	if err != nil {
		t.Fatalf("migration state path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create migration state directory: %v", err)
	}
	legacy, err := json.Marshal(teamsServiceMigrationBlockedState{
		PID:          os.Getpid(),
		ProcessStart: "self-start",
		Reason:       "legacy blocked marker",
		UpdatedAt:    time.Now(),
	})
	if err != nil {
		t.Fatalf("marshal legacy migration marker: %v", err)
	}
	if err := os.WriteFile(path, append(legacy, '\n'), 0o600); err != nil {
		t.Fatalf("write legacy migration marker: %v", err)
	}
	state, ok := liveTeamsServiceMigrationBlockedState()
	if !ok || state.Phase != teamsServiceMigrationPhaseBlocked {
		t.Fatalf("legacy migration marker state=%+v ok=%t, want blocked compatibility", state, ok)
	}
}

func TestTeamsRuntimeSafetyBlockedMigrationWaitsWithoutRetryingCI(t *testing.T) {
	lockCLITestHooks(t)
	ctx, cancel := context.WithCancel(context.Background())
	var attempts atomic.Int32
	firstAttempt := make(chan struct{})
	var out bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- runTeamsServiceRetryLoop(ctx, &out, func() error {
			if attempts.Add(1) == 1 {
				close(firstAttempt)
			}
			return &teams.RuntimeStoreMigrationBlockedError{Err: errors.New("staging validation failed")}
		})
	}()
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("blocked migration was not attempted")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := liveTeamsServiceMigrationBlockedState(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocked migration did not publish watchdog coordination state")
		}
		time.Sleep(time.Millisecond)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("blocked migration attempts = %d, want exactly one", got)
	}
	decision := evaluateTeamsServiceWatchdog(
		teamsServiceWatchdogSnapshot{Installed: true, Active: true, MigrationBlocked: true},
		teamsServiceWatchdogState{ConsecutiveStale: 2},
		teamsServiceWatchdogOptions{Now: time.Now()},
	)
	if decision.Action != teamsServiceWatchdogActionNoop || !strings.Contains(decision.Reason, "blocked store migration") {
		t.Fatalf("blocked migration watchdog decision = %+v, want noop", decision)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked migration wait error = %v, want context cancellation", err)
	}
	if _, ok := liveTeamsServiceMigrationBlockedState(); ok {
		t.Fatal("blocked migration marker remained after child exit")
	}
	if !strings.Contains(out.String(), "waiting for service restart") {
		t.Fatalf("blocked migration output = %q, want restart guidance", out.String())
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

func TestTeamsRuntimeSafetyStatusReportsAmbiguousCanonicalStoresWithoutManagedOwnerCI(t *testing.T) {
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

	for _, scopeID := range []string{"scope-status-a", "scope-status-b"} {
		path := cliStatePathForTest(t, "teams", "scopes", scopeID, "state.json")
		store, err := teamsstore.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", scopeID, err)
		}
		if err := store.Update(context.Background(), func(state *teamsstore.State) error {
			state.Scope = teamsstore.ScopeIdentity{ID: scopeID, Profile: "default"}
			return nil
		}); err != nil {
			_ = store.Close()
			t.Fatalf("seed %s: %v", scopeID, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("close %s: %v", scopeID, err)
		}
	}

	out := executeRootForTeamsTest(t, "teams", "status")
	if !strings.Contains(out, "Authoritative store: unknown (multiple canonical stores are ambiguous)") {
		t.Fatalf("status guessed an authoritative canonical store:\n%s", out)
	}
	if got := strings.Count(out, "Store candidate: canonical unknown "); got != 2 {
		t.Fatalf("canonical unknown candidates = %d, want 2:\n%s", got, out)
	}
}

func TestTeamsRuntimeSafetyStatusReportsBrokenLegacyPresenceWithoutOpeningItCI(t *testing.T) {
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
	if runtime.GOOS != "windows" {
		makeCLITreesReadOnlyForTest(t, filepath.Dir(canonicalPath), filepath.Dir(legacyPath))
	}

	before := snapshotCLITreeForReadOnlyTest(t, tmp)
	out, statusErr := executeRootForTeamsTestAllowError(t, "teams", "status")
	if statusErr != nil {
		t.Errorf("teams status aborted while reporting retained legacy presence: %v", statusErr)
	}
	for _, want := range []string{
		canonicalPath,
		legacyPath,
		"canonical",
		"legacy",
		"authoritative",
		"non-authoritative",
	} {
		if !strings.Contains(strings.ToLower(out), strings.ToLower(want)) {
			t.Errorf("teams status omitted %q while reporting canonical and retained legacy paths:\n%s", want, out)
		}
	}
	if strings.Contains(strings.ToLower(out), "load error") {
		t.Errorf("default status deep-read the broken legacy store instead of reporting path presence only:\n%s", out)
	}
	after := snapshotCLITreeForReadOnlyTest(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("read-only status modified a canonical/legacy store family:\nbefore=%#v\nafter=%#v", before, after)
	}
}

func TestTeamsRuntimeSafetyDoctorDeepReadsBrokenLegacyAndReportsPerStoreErrorReadOnlyCI(t *testing.T) {
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

	scopeID := "scope-runtime-safety-doctor"
	canonicalPath := cliStatePathForTest(t, "teams", "scopes", scopeID, "state.json")
	canonical, err := teamsstore.Open(canonicalPath)
	if err != nil {
		t.Fatalf("open canonical store: %v", err)
	}
	if err := canonical.Update(context.Background(), func(state *teamsstore.State) error {
		state.Scope = teamsstore.ScopeIdentity{ID: scopeID, Profile: "default"}
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
	if runtime.GOOS != "windows" {
		makeCLITreesReadOnlyForTest(t, filepath.Dir(canonicalPath), filepath.Dir(legacyPath))
	}

	before := snapshotCLITreeForReadOnlyTest(t, tmp)
	out, doctorErr := executeRootForTeamsTestAllowError(t, "teams", "service", "doctor")
	diagnostic := strings.ToLower(out + "\n" + fmt.Sprint(doctorErr))
	for _, want := range []string{canonicalPath, legacyPath, "legacy", "load error"} {
		if !strings.Contains(diagnostic, strings.ToLower(want)) {
			t.Errorf("teams service doctor omitted %q:\n%s", want, diagnostic)
		}
	}
	after := snapshotCLITreeForReadOnlyTest(t, tmp)
	if !reflect.DeepEqual(before, after) {
		t.Errorf("doctor modified a canonical/broken-legacy store family:\nbefore=%#v\nafter=%#v", before, after)
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

func makeCLITreesReadOnlyForTest(t *testing.T, roots ...string) {
	t.Helper()
	var readOnlyPaths []string
	for _, root := range roots {
		if err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			readOnlyPaths = append(readOnlyPaths, path)
			if info.IsDir() {
				return os.Chmod(path, 0o555)
			}
			return os.Chmod(path, 0o444)
		}); err != nil {
			t.Fatalf("make diagnostic store family read-only: %v", err)
		}
	}
	t.Cleanup(func() {
		sort.Slice(readOnlyPaths, func(i, j int) bool { return len(readOnlyPaths[i]) > len(readOnlyPaths[j]) })
		for _, path := range readOnlyPaths {
			_ = os.Chmod(path, 0o700)
		}
	})
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

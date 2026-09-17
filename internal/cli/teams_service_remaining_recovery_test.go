package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTeamsServiceLocalSupervisorWSLRetiresStartupFallbackAndPreservesOtherMarker(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	runner := &recordingTeamsServiceRunner{}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "bin", "cxp"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "baka",
		runner:         runner,
	})

	backend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startup fallback marker path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker directory: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("TaskName="+backend.Name()+"\n"), 0o600); err != nil {
		t.Fatalf("write current marker: %v", err)
	}
	otherMarker := filepath.Join(filepath.Dir(markerPath), "codex-helper-teams-wsl-startup-unrelated.txt")
	if err := os.WriteFile(otherMarker, []byte("TaskName=Codex Helper Teams Bridge (WSL Other user profile other)\n"), 0o600); err != nil {
		t.Fatalf("write unrelated marker: %v", err)
	}

	if err := retireTeamsServiceConflictingBackendsForLocalSupervisor(context.Background(), teamsServiceSpec{}); err != nil {
		t.Fatalf("retire conflicting WSL backends: %v", err)
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("current Startup fallback marker remains, stat err=%v", err)
	}
	if _, err := os.Stat(teamsServiceWSLStartupFallbackStopPath(markerPath)); err != nil {
		t.Fatalf("current Startup fallback stop fence missing: %v", err)
	}
	if _, err := os.Stat(otherMarker); err != nil {
		t.Fatalf("unrelated Startup fallback marker was removed: %v", err)
	}
	if len(runner.calls) != 2 {
		t.Fatalf("cleanup calls = %#v, want Scheduled Task retirement plus Startup fallback cleanup", runner.calls)
	}
	if got := strings.Join(runner.calls[0].args, " "); !strings.Contains(got, "Disable-ScheduledTask") {
		t.Fatalf("first cleanup call = %q, want Scheduled Task retirement", got)
	}
	if got := strings.Join(runner.calls[1].args, " "); !strings.Contains(got, "Remove-Item") || !strings.Contains(got, "Get-CimInstance Win32_Process") {
		t.Fatalf("second cleanup call = %q, want Startup fallback/process cleanup", got)
	}
}

func TestTeamsServiceLocalSupervisorWSLStartFailsClosedWhenStartupFallbackCleanupFails(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	var calls int
	cleanupErr := errors.New("Windows Startup cleanup unavailable")
	runner := teamsServiceCommandRunnerFunc(func(_ context.Context, _ string, _ ...string) ([]byte, error) {
		calls++
		if calls == 2 {
			return nil, cleanupErr
		}
		return nil, nil
	})
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "bin", "cxp"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "baka",
		runner:         runner,
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})

	localBackend := teamsServiceLocalSupervisorBackend{}
	if _, err := localBackend.Install(context.Background(), teamsServiceSpec{
		Executable: filepath.Join(tmp, "bin", "cxp"),
		WorkingDir: tmp,
	}); err != nil {
		t.Fatalf("install local supervisor config: %v", err)
	}
	wslBackend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := wslBackend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startup fallback marker path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker directory: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("TaskName="+wslBackend.Name()+"\n"), 0o600); err != nil {
		t.Fatalf("write current marker: %v", err)
	}

	_, err = localBackend.Run(context.Background(), "start")
	if err == nil || !strings.Contains(err.Error(), "remove old WSL Startup fallback") {
		t.Fatalf("local-supervisor start err = %v, want fallback cleanup failure", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("local-supervisor start err = %v, want cleanup cause %v", err, cleanupErr)
	}
	if started {
		t.Fatal("local supervisor started after Startup fallback cleanup failed")
	}
	if _, statErr := os.Stat(markerPath); statErr != nil {
		t.Fatalf("fallback marker should remain available for a later cleanup retry: %v", statErr)
	}
}

func TestTeamsServiceLocalSupervisorWSLStartupCleanupHonorsContext(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	runner := teamsServiceCommandRunnerFunc(func(ctx context.Context, _ string, _ ...string) ([]byte, error) {
		calls++
		if calls == 1 {
			cancel()
			return nil, nil
		}
		return nil, ctx.Err()
	})
	started := false
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "bin", "cxp"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "baka",
		runner:         runner,
		localStartDetached: func(context.Context, string, string, teamsServiceSpec) (int, error) {
			started = true
			return os.Getpid(), nil
		},
	})

	localBackend := teamsServiceLocalSupervisorBackend{}
	if _, err := localBackend.Install(context.Background(), teamsServiceSpec{
		Executable: filepath.Join(tmp, "bin", "cxp"),
		WorkingDir: tmp,
	}); err != nil {
		t.Fatalf("install local supervisor config: %v", err)
	}
	wslBackend := teamsServiceWSLWindowsTaskBackend{}
	markerPath, err := wslBackend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startup fallback marker path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(markerPath), 0o700); err != nil {
		t.Fatalf("mkdir marker directory: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("TaskName="+wslBackend.Name()+"\n"), 0o600); err != nil {
		t.Fatalf("write current marker: %v", err)
	}

	_, err = localBackend.Run(ctx, "start")
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("local-supervisor start err = %v, want context cancellation from fallback cleanup", err)
	}
	if started {
		t.Fatal("local supervisor started after fallback cleanup context was canceled")
	}
}

func TestTeamsServiceWSLUninstallPropagatesStartupFallbackCleanupFailure(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	var calls int
	cleanupErr := errors.New("fallback cleanup failed")
	runner := teamsServiceCommandRunnerFunc(func(_ context.Context, _ string, _ ...string) ([]byte, error) {
		calls++
		if calls == 2 {
			return nil, cleanupErr
		}
		return nil, nil
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "bin", "cxp"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "baka",
		runner:         runner,
	})

	backend := teamsServiceWSLWindowsTaskBackend{}
	configPath, err := backend.Path()
	if err != nil {
		t.Fatalf("task config path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(configPath), 0o700); err != nil {
		t.Fatalf("mkdir task config directory: %v", err)
	}
	if err := os.WriteFile(configPath, []byte("task config\n"), 0o600); err != nil {
		t.Fatalf("write task config: %v", err)
	}
	markerPath, err := backend.startupFallbackMarkerPath()
	if err != nil {
		t.Fatalf("startup fallback marker path: %v", err)
	}
	if err := os.WriteFile(markerPath, []byte("TaskName="+backend.Name()+"\n"), 0o600); err != nil {
		t.Fatalf("write current marker: %v", err)
	}

	_, err = backend.Uninstall(context.Background())
	if err == nil || !strings.Contains(err.Error(), "remove WSL Startup fallback during uninstall") {
		t.Fatalf("WSL uninstall err = %v, want fallback cleanup failure", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("WSL uninstall err = %v, want cleanup cause %v", err, cleanupErr)
	}
	if _, statErr := os.Stat(configPath); statErr != nil {
		t.Fatalf("task config should remain after incomplete uninstall: %v", statErr)
	}
}

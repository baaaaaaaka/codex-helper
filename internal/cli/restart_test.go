package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/gofrs/flock"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/update"
	"github.com/spf13/cobra"
)

func TestCurrentRuntimeOwnsUpdateTarget(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	entry := filepath.Join(dir, helperruntime.BinaryName(runtime.GOOS))
	legacy := filepath.Join(dir, helperpath.BinaryName(runtime.GOOS))
	ctx := helperruntime.Context{
		Root:        filepath.Join(dir, ".cxp-runtime"),
		EntryPath:   entry,
		RuntimePath: helperruntime.VersionPath(filepath.Join(dir, ".cxp-runtime"), "v1.2.3", runtime.GOOS),
	}
	for _, path := range []string{"", entry, legacy, ctx.RuntimePath} {
		if !currentRuntimeOwnsUpdateTarget(ctx, path) {
			t.Fatalf("current runtime should own %q", path)
		}
	}
	other := filepath.Join(t.TempDir(), helperpath.BinaryName(runtime.GOOS))
	if currentRuntimeOwnsUpdateTarget(ctx, other) {
		t.Fatalf("current runtime must not capture explicit external install path %q", other)
	}
}

func TestRestartSelfExecsSameBinaryWithCurrentArgs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("non-Windows restart uses exec; Windows is covered by service restart tests")
	}

	prevExecutablePath := executablePath
	prevExecSelf := execSelf
	prevStartSelf := startSelf
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		execSelf = prevExecSelf
		startSelf = prevStartSelf
	})

	exe := filepath.Join(t.TempDir(), "codex-proxy")
	executablePath = func() (string, error) {
		return exe, nil
	}
	startSelf = func(string, []string) error {
		t.Fatal("startSelf should not be used on non-Windows restart")
		return nil
	}

	wantErr := errors.New("stop before real exec")
	var gotExe string
	var gotArgs []string
	var gotEnv []string
	execSelf = func(exe string, args []string, env []string) error {
		gotExe = exe
		gotArgs = append([]string{}, args...)
		gotEnv = append([]string{}, env...)
		return wantErr
	}

	prevArgs := os.Args
	os.Args = []string{"old-codex-proxy", "teams", "run", "--registry", "state.json"}
	t.Cleanup(func() { os.Args = prevArgs })

	err := restartSelf()
	if !errors.Is(err, wantErr) {
		t.Fatalf("restartSelf error = %v, want %v", err, wantErr)
	}
	if gotExe != exe {
		t.Fatalf("exec exe = %q, want %q", gotExe, exe)
	}
	wantArgs := []string{exe, "teams", "run", "--registry", "state.json"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("exec args = %#v, want %#v", gotArgs, wantArgs)
	}
	if len(gotEnv) == 0 {
		t.Fatal("exec env was empty")
	}
}

func TestHandleUpdateAndRestartValidatesBinaryAndReportsPendingReplacement(t *testing.T) {
	lockCLITestHooks(t)

	prevPerform := performUpdate
	t.Cleanup(func() { performUpdate = prevPerform })

	var got update.UpdateOptions
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		got = opts
		return update.ApplyResult{
			Version:            "1.2.3",
			RestartRequired:    true,
			PendingReplacePath: filepath.Join(t.TempDir(), ".codex-proxy_1.2.3.tmp"),
		}, nil
	}

	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := handleUpdateAndRestart(context.Background(), cmd); err != nil {
		t.Fatalf("handleUpdateAndRestart error: %v", err)
	}
	if !got.ValidateBinary {
		t.Fatal("handleUpdateAndRestart must validate the downloaded binary before replacing itself")
	}
	if !strings.Contains(out.String(), "Update replacement for v1.2.3 is pending.") ||
		!strings.Contains(out.String(), "verify `codex-proxy --version`") {
		t.Fatalf("unexpected pending replacement output: %q", out.String())
	}
}

func TestHandleUpdateAndRestartDoesNotTouchTeamsServiceForPlainPendingReplacement(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	prevPerform := performUpdate
	prevDetached := teamsServiceStartDetached
	t.Cleanup(func() {
		performUpdate = prevPerform
		teamsServiceStartDetached = prevDetached
	})
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		return update.ApplyResult{
			Version:            "1.2.3",
			InstallPath:        filepath.Join(tmp, "codex-proxy.exe"),
			RestartRequired:    true,
			PendingReplacePath: filepath.Join(tmp, ".codex-proxy_1.2.3_windows_amd64.exe.123"),
		}, nil
	}
	var detachedCalls int
	teamsServiceStartDetached = func(_ context.Context, _ string, args ...string) error {
		detachedCalls++
		return nil
	}
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "windows",
		exe:            filepath.Join(tmp, "codex-proxy.exe"),
		windowsTaskDir: filepath.Join(tmp, "tasks"),
	})

	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := handleUpdateAndRestart(context.Background(), cmd); err != nil {
		t.Fatalf("handleUpdateAndRestart error: %v", err)
	}
	if detachedCalls != 0 {
		t.Fatalf("plain --upgrade scheduled Teams service activation %d time(s), want 0", detachedCalls)
	}
	if !strings.Contains(out.String(), "Update replacement for v1.2.3 is pending.") {
		t.Fatalf("unexpected pending replacement output: %q", out.String())
	}
}

func TestHandleUpdateAndRestartUsesSharedInstallLock(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	installPath := filepath.Join(tmp, "codex-proxy")
	if err := os.WriteFile(installPath, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write install path: %v", err)
	}

	prevExecutablePath := executablePath
	prevArgv0 := restartArgv0
	prevResolve := resolveInstallPathForCLI
	prevPerform := performUpdate
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		restartArgv0 = prevArgv0
		resolveInstallPathForCLI = prevResolve
		performUpdate = prevPerform
	})
	executablePath = func() (string, error) { return installPath, nil }
	restartArgv0 = func() string { return "" }
	resolveInstallPathForCLI = func(path string) (string, error) {
		if path != "" {
			return update.ResolveInstallPath(path)
		}
		return installPath, nil
	}
	performUpdate = func(context.Context, update.UpdateOptions) (update.ApplyResult, error) {
		t.Fatal("PerformUpdate should not run while shared install lock is held")
		return update.ApplyResult{}, nil
	}

	lock := flock.New(installPath + ".auto-update.lock")
	locked, err := lock.TryLock()
	if err != nil {
		t.Fatalf("TryLock error: %v", err)
	}
	if !locked {
		t.Fatal("failed to acquire test auto-update lock")
	}
	t.Cleanup(func() { _ = lock.Unlock() })

	cmd := &cobra.Command{}
	var stderr bytes.Buffer
	cmd.SetErr(&stderr)
	err = handleUpdateAndRestart(context.Background(), cmd)
	if err == nil || !strings.Contains(err.Error(), "another codex-helper upgrade is already using install path") {
		t.Fatalf("expected shared install lock error, got %v stderr=%q", err, stderr.String())
	}
}

func TestHandleUpdateAndRestartUnifiesCXPShimBeforeExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX cxp shim repair")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	installPath := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	cxpPath := filepath.Join(tmp, ".local", "bin", "cxp")
	writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.2"), 0o755)
	writeCLIFile(t, cxpPath, upgradeCXPShimTestScript("1.2.1"), 0o755)

	prevExecutablePath := executablePath
	prevArgv0 := restartArgv0
	prevResolve := resolveInstallPathForCLI
	prevPerform := performUpdate
	prevExecSelf := execSelf
	prevStartSelf := startSelf
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		restartArgv0 = prevArgv0
		resolveInstallPathForCLI = prevResolve
		performUpdate = prevPerform
		execSelf = prevExecSelf
		startSelf = prevStartSelf
	})
	executablePath = func() (string, error) { return installPath, nil }
	restartArgv0 = func() string { return cxpPath }
	resolveInstallPathForCLI = func(path string) (string, error) {
		if path != "" {
			return update.ResolveInstallPath(path)
		}
		return installPath, nil
	}
	performUpdate = func(_ context.Context, opts update.UpdateOptions) (update.ApplyResult, error) {
		if opts.InstallPath != installPath {
			t.Fatalf("InstallPath = %q, want %q", opts.InstallPath, installPath)
		}
		if !opts.ValidateBinary {
			t.Fatal("TUI update must validate the downloaded binary before activation")
		}
		writeCLIFile(t, installPath, upgradeCXPShimTestScript("1.2.4"), 0o755)
		return update.ApplyResult{Version: "1.2.4", InstallPath: installPath}, nil
	}
	startSelf = func(string, []string) error {
		t.Fatal("startSelf should not be used on non-Windows restart")
		return nil
	}

	wantErr := errors.New("stop before real exec")
	var gotExe string
	var gotArgs []string
	execSelf = func(exe string, args []string, env []string) error {
		gotExe = exe
		gotArgs = append([]string{}, args...)
		return wantErr
	}
	prevArgs := os.Args
	os.Args = []string{cxpPath, "tui"}
	t.Cleanup(func() { os.Args = prevArgs })

	cmd := &cobra.Command{}
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := handleUpdateAndRestart(context.Background(), cmd)
	if !errors.Is(err, wantErr) {
		t.Fatalf("handleUpdateAndRestart error = %v, want %v", err, wantErr)
	}
	if !strings.Contains(out.String(), "Updated to v1.2.4. Restarting...") {
		t.Fatalf("unexpected output: %q", out.String())
	}
	versionOut, err := exec.Command(cxpPath, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("cxp --version failed: %v\n%s", err, versionOut)
	}
	if !strings.Contains(string(versionOut), "1.2.4") {
		t.Fatalf("cxp version output = %q, want updated version", versionOut)
	}
	if gotExe != installPath {
		t.Fatalf("exec exe = %q, want installed path %q", gotExe, installPath)
	}
	wantArgs := []string{installPath, "tui"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("exec args = %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestRestartTeamsHelperAfterActivationPendingUsesInstalledPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("non-Windows restart uses exec; Windows is covered by process start tests")
	}
	lockCLITestHooks(t)

	tmp := t.TempDir()
	isolateUpgradeTeamsStateForTest(t, tmp)
	installPath := filepath.Join(tmp, ".local", "bin", "codex-proxy")
	if err := os.MkdirAll(filepath.Dir(installPath), 0o755); err != nil {
		t.Fatalf("mkdir install dir: %v", err)
	}
	if err := os.WriteFile(installPath, []byte("updated"), 0o755); err != nil {
		t.Fatalf("write install path: %v", err)
	}

	prevExecSelf := execSelf
	prevStartSelf := startSelf
	t.Cleanup(func() {
		execSelf = prevExecSelf
		startSelf = prevStartSelf
	})
	startSelf = func(string, []string) error {
		t.Fatal("startSelf should not be used on non-Windows restart")
		return nil
	}

	wantErr := errors.New("stop before real exec")
	var gotExe string
	var gotArgs []string
	execSelf = func(exe string, args []string, env []string) error {
		gotExe = exe
		gotArgs = append([]string{}, args...)
		return wantErr
	}

	prevArgs := os.Args
	os.Args = []string{"old-codex-proxy", "teams", "run", "--auto-service=false"}
	t.Cleanup(func() { os.Args = prevArgs })

	err := restartTeamsHelperFromTeamsAfterPendingReplacement(context.Background(), "", installPath)
	if !errors.Is(err, wantErr) {
		t.Fatalf("restart error = %v, want %v", err, wantErr)
	}
	if gotExe != installPath {
		t.Fatalf("exec exe = %q, want installed path %q", gotExe, installPath)
	}
	wantArgs := []string{installPath, "teams", "run", "--auto-service=false"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("exec args = %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestRestartSelfUsesStablePathWhenRunningReloadBackup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("non-Windows restart uses exec; Windows is covered by service restart tests")
	}

	prevExecutablePath := executablePath
	prevExecSelf := execSelf
	prevStartSelf := startSelf
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		execSelf = prevExecSelf
		startSelf = prevStartSelf
	})

	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy-teams-dev")
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	backup := stable + ".reload-backup-111-222.reload-backup-111-333"
	executablePath = func() (string, error) {
		return backup, nil
	}
	startSelf = func(string, []string) error {
		t.Fatal("startSelf should not be used on non-Windows restart")
		return nil
	}

	wantErr := errors.New("stop before real exec")
	var gotExe string
	var gotArgs []string
	execSelf = func(exe string, args []string, env []string) error {
		gotExe = exe
		gotArgs = append([]string{}, args...)
		return wantErr
	}

	prevArgs := os.Args
	os.Args = []string{"old-codex-proxy", "teams", "run", "--auto-service=false"}
	t.Cleanup(func() { os.Args = prevArgs })

	err := restartSelf()
	if !errors.Is(err, wantErr) {
		t.Fatalf("restartSelf error = %v, want %v", err, wantErr)
	}
	if gotExe != stable {
		t.Fatalf("exec exe = %q, want stable path %q", gotExe, stable)
	}
	wantArgs := []string{stable, "teams", "run", "--auto-service=false"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("exec args = %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestRestartSelfUsesStablePathWhenRunningNFSSillyRename(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("NFS silly rename handling is for Unix service restarts")
	}

	prevExecutablePath := executablePath
	prevExecSelf := execSelf
	prevStartSelf := startSelf
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		execSelf = prevExecSelf
		startSelf = prevStartSelf
	})

	dir := t.TempDir()
	stable := filepath.Join(dir, "codex-proxy")
	if err := os.WriteFile(stable, []byte("stable"), 0o755); err != nil {
		t.Fatalf("write stable binary: %v", err)
	}
	running := filepath.Join(dir, ".nfs802014de01c482a800000492")
	executablePath = func() (string, error) {
		return running, nil
	}
	startSelf = func(string, []string) error {
		t.Fatal("startSelf should not be used on non-Windows restart")
		return nil
	}

	wantErr := errors.New("stop before real exec")
	var gotExe string
	var gotArgs []string
	execSelf = func(exe string, args []string, env []string) error {
		gotExe = exe
		gotArgs = append([]string{}, args...)
		return wantErr
	}

	prevArgs := os.Args
	os.Args = []string{"old-codex-proxy", "teams", "run", "--auto-service=false"}
	t.Cleanup(func() { os.Args = prevArgs })

	err := restartSelf()
	if !errors.Is(err, wantErr) {
		t.Fatalf("restartSelf error = %v, want %v", err, wantErr)
	}
	if gotExe != stable {
		t.Fatalf("exec exe = %q, want stable path %q", gotExe, stable)
	}
	wantArgs := []string{stable, "teams", "run", "--auto-service=false"}
	if !reflect.DeepEqual(gotArgs, wantArgs) {
		t.Fatalf("exec args = %#v, want %#v", gotArgs, wantArgs)
	}
}

func TestRestartSelfFailsClosedWhenNFSSillyRenameCannotRecover(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("NFS silly rename handling is for Unix service restarts")
	}

	prevExecutablePath := executablePath
	prevExecSelf := execSelf
	t.Cleanup(func() {
		executablePath = prevExecutablePath
		execSelf = prevExecSelf
	})

	running := filepath.Join(t.TempDir(), ".nfs802014de01c482a800000492")
	executablePath = func() (string, error) { return running, nil }
	execSelf = func(string, []string, []string) error {
		t.Fatal("restartSelf must not exec an unrecoverable transient path")
		return nil
	}

	err := restartSelf()
	if err == nil || !strings.Contains(err.Error(), "cannot recover") {
		t.Fatalf("restartSelf error = %v, want unrecoverable transient path", err)
	}
}

package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/update"
	"github.com/spf13/cobra"
)

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

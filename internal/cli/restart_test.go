package cli

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
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

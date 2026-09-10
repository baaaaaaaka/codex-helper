//go:build !windows

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

func TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup(t *testing.T) {
	sleepPath, err := exec.LookPath("sleep")
	if err != nil {
		t.Skip("sleep is required by the process-group fixture")
	}
	dir := t.TempDir()
	childPIDPath := filepath.Join(dir, "child.pid")
	codexPath := filepath.Join(dir, "codex")
	script := fmt.Sprintf(`#!/bin/sh
set -eu
if [ "${1:-}" = 'migrate-rollouts' ]; then
  %s 60 &
  child=$!
  printf '%%s\n' "$child" > %s
  while :; do
    %s 1
  done
fi
exit 64
`, shellSingleQuoteForBeaconCLITest(sleepPath), shellSingleQuoteForBeaconCLITest(childPIDPath), shellSingleQuoteForBeaconCLITest(sleepPath))
	if err := os.WriteFile(codexPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- migrateCodexRolloutBeforeTUI(ctx, codexrunner.AppServerLaunchContext{
			Command:    codexPath,
			WorkingDir: dir,
		}, nil, "11111111-2222-3333-4444-555555555555")
	}()

	// The child is a shell fixture, so readiness is not the process-group
	// invariant itself. Give the hosted race binary a finite startup margin;
	// otherwise race instrumentation can spend the whole two-second window
	// before the shell publishes its child PID and turn a valid cleanup check
	// into a false "did not start" failure.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(childPIDPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration fixture did not start its child process")
		}
		time.Sleep(10 * time.Millisecond)
	}
	pidRaw, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
	if err != nil {
		t.Fatal(err)
	}
	childStartTime := ""
	if runtime.GOOS == "linux" {
		childStartTime, err = teamsLocalSupervisorProcessStartTime(pid)
		if err != nil {
			t.Fatalf("read migration child process %d start time: %v", pid, err)
		}
	}
	childIsOriginalProcess := func() bool {
		if !proc.IsAlive(pid) {
			return false
		}
		if childStartTime == "" {
			return true
		}
		currentStartTime, startErr := teamsLocalSupervisorProcessStartTime(pid)
		if startErr != nil {
			// The liveness and /proc identity reads are not atomic. If the
			// original process exits between them, ENOENT is definitive evidence
			// that this PID no longer names the original process. Keep other
			// errors conservative so permission or malformed procfs failures do
			// not turn a cleanup assertion into a false pass.
			if errors.Is(startErr, os.ErrNotExist) {
				return false
			}
			return true
		}
		return currentStartTime == childStartTime
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("migration cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("migration did not stop after context cancellation")
	}

	deadline = time.Now().Add(2 * time.Second)
	for childIsOriginalProcess() && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if childIsOriginalProcess() {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		currentStartTime, startErr := teamsLocalSupervisorProcessStartTime(pid)
		currentPGID, pgidErr := syscall.Getpgid(pid)
		commandLine, commandLineErr := proc.CommandLine(pid)
		t.Fatalf("migration child process %d survived process-group cancellation (start=%q current_start=%q start_err=%v pgid=%d pgid_err=%v command=%q command_err=%v)", pid, childStartTime, currentStartTime, startErr, currentPGID, pgidErr, commandLine, commandLineErr)
	}
}

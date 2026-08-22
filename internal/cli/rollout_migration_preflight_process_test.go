//go:build !windows

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(childPIDPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration fixture did not start its child process")
		}
		time.Sleep(10 * time.Millisecond)
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

	pidRaw, err := os.ReadFile(childPIDPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidRaw)))
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for proc.IsAlive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if proc.IsAlive(pid) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatalf("migration child process %d survived process-group cancellation", pid)
	}
}

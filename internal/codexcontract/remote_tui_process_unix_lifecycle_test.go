//go:build !windows

package codexcontract

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUnixRemoteTUIProcessStopTerminatesDescendants(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "descendant-writes")
	childPID := filepath.Join(root, "descendant-pid")
	command := filepath.Join(root, "codex-fixture")
	fixture := `#!/bin/sh
(
  while :; do
    printf x >> "$CXP_DESCENDANT_MARKER"
    sleep 0.02
  done
) &
printf '%s' "$!" > "$CXP_DESCENDANT_PID"
wait
`
	if err := os.WriteFile(command, []byte(fixture), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CXP_DESCENDANT_MARKER", marker)
	t.Setenv("CXP_DESCENDANT_PID", childPID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	process, err := startRemoteTUIProcess(ctx, command, "ws://127.0.0.1:1", "", "", filepath.Join(root, "codex-home"))
	if err != nil {
		t.Fatalf("start PTY fixture: %v", err)
	}
	defer process.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for {
		pidRaw, pidErr := os.ReadFile(childPID)
		markerInfo, markerErr := os.Stat(marker)
		if pidErr == nil && len(pidRaw) > 0 && markerErr == nil && markerInfo.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PTY fixture did not start its writing descendant: pid_err=%v marker_err=%v output=%s", pidErr, markerErr, process.Output())
		}
		time.Sleep(20 * time.Millisecond)
	}

	process.Stop()
	_ = process.Wait()
	before, err := os.Stat(marker)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	after, err := os.Stat(marker)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("remote TUI descendant kept running after Stop: marker grew from %d to %d bytes", before.Size(), after.Size())
	}
}

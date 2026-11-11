//go:build !windows

package codexcontract

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

func TestUnixRemoteTUIProcessStopTerminatesDescendants(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "descendant-writes")
	childPID := filepath.Join(root, "descendant-pid")
	command := filepath.Join(root, "codex-fixture")
	fixture := `#!/bin/sh
(
  trap 'sleep 0.1; printf x >> "$CXP_DESCENDANT_MARKER"; exit 0' HUP
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
	if ctx.Err() != nil {
		t.Fatal("Stop did not finish before context cancellation")
	}
	raw, err := os.ReadFile(childPID)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 1 {
		t.Fatalf("invalid descendant pid %q: %v", raw, err)
	}
	// Wait reaps script, not its separate PTY session. Signal delivery and
	// descendant exit are asynchronous; a last write before exit is allowed.
	// Require actual termination within a bound, rather than guessing from
	// two file sizes immediately after the supervisor exits.
	deadline = time.Now().Add(3 * time.Second)
	for {
		alive, err := proc.CheckAlive(pid)
		if err != nil {
			t.Fatal(err)
		}
		if !alive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("remote TUI descendant %d remained alive after Stop", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
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

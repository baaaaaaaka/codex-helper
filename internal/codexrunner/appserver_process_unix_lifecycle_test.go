//go:build !windows

package codexrunner

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAppServerProcessCloseTerminatesWrapperDescendants(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "descendant-writes")
	childPID := filepath.Join(root, "descendant-pid")
	wrapper := filepath.Join(root, "codex-wrapper")
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
	if err := os.WriteFile(wrapper, []byte(fixture), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CXP_DESCENDANT_MARKER", marker)
	t.Setenv("CXP_DESCENDANT_PID", childPID)

	transport, err := (AppServerProcessStarter{}).StartAppServer(context.Background(), AppServerStartRequest{Command: wrapper})
	if err != nil {
		t.Fatalf("start wrapper fixture: %v", err)
	}
	defer transport.Close()

	deadline := time.Now().Add(5 * time.Second)
	for {
		pidRaw, pidErr := os.ReadFile(childPID)
		markerInfo, markerErr := os.Stat(marker)
		if pidErr == nil && len(pidRaw) > 0 && markerErr == nil && markerInfo.Size() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("wrapper descendant did not start: pid_err=%v marker_err=%v", pidErr, markerErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := transport.Close(); err != nil {
		t.Fatalf("close app-server transport: %v", err)
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
		t.Fatalf("app-server descendant kept running after Close: marker grew from %d to %d bytes", before.Size(), after.Size())
	}
}

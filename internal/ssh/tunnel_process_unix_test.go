//go:build !windows

package ssh

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTunnelStopTerminatesProcessGroupDescendants(t *testing.T) {
	dir := t.TempDir()
	childPIDFile := filepath.Join(dir, "child.pid")
	writeTestExecutable(t, filepath.Join(dir, "ssh"), "#!/bin/sh\n"+
		"trap '' INT TERM\n"+
		"sleep 30 &\n"+
		"child=$!\n"+
		"echo $child > \"$TUNNEL_CHILD_PID_FILE\"\n"+
		"wait $child\n")
	t.Setenv("PATH", dir)
	t.Setenv("TUNNEL_CHILD_PID_FILE", childPIDFile)
	setAcceptNewSupportForTest(t, true)

	tun, err := NewTunnel(TunnelConfig{Host: "example.com", Port: 22, User: "alice", SocksPort: 12345})
	if err != nil {
		t.Fatalf("NewTunnel: %v", err)
	}
	if err := tun.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	childPID := waitForTestPID(t, childPIDFile)
	if err := tun.Stop(50 * time.Millisecond); err == nil {
		t.Fatal("Stop unexpectedly reported a clean exit after forced process-group termination")
	}
	if processGroupExists(tun.PID()) {
		t.Fatalf("tunnel process group %d still exists after Stop", tun.PID())
	}
	if err := syscall.Kill(childPID, 0); err == nil || !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("descendant process %d survived Stop: %v", childPID, err)
	}
}

func waitForTestPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for child PID file %s", path)
	return 0
}

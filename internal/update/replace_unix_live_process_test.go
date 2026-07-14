//go:build !windows

package update

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplaceBinaryDoesNotInterruptLiveProcess(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "codex-proxy")
	ready := filepath.Join(dir, "ready")
	old := "#!/bin/sh\nprintf old > \"$READY_FILE\"\nsleep 5\n"
	new := "#!/bin/sh\nprintf new\n"
	if err := os.WriteFile(dest, []byte(old), 0o755); err != nil {
		t.Fatalf("write old binary: %v", err)
	}
	cmd := exec.Command(dest)
	cmd.Env = append(os.Environ(), "READY_FILE="+ready)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start old binary: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(ready); err != nil {
		t.Fatalf("old binary did not become live: %v", err)
	}

	tmp := filepath.Join(dir, "codex-proxy.new")
	if err := os.WriteFile(tmp, []byte(new), 0o755); err != nil {
		t.Fatalf("write new binary: %v", err)
	}
	if _, err := replaceBinary(tmp, dest, replaceOptions{}); err != nil {
		t.Fatalf("replace live binary: %v", err)
	}

	output, err := exec.Command(dest).Output()
	if err != nil {
		t.Fatalf("run replaced binary: %v", err)
	}
	if strings.TrimSpace(string(output)) != "new" {
		t.Fatalf("new launch output = %q, want new", output)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("stop old process: %v", err)
	}
	_ = cmd.Wait()
}

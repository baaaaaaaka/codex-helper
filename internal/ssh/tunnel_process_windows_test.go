//go:build windows

package ssh

import (
	"os/exec"
	"testing"
	"time"
)

func TestConfigureTunnelCommandUsesHiddenNoWindowCreation(t *testing.T) {
	cmd := exec.Command("ssh")
	configureTunnelCommand(cmd)
	if cmd.SysProcAttr == nil {
		t.Fatal("tunnel command did not receive Windows process attributes")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatal("tunnel command should not create a visible console window")
	}
	if cmd.SysProcAttr.CreationFlags&windowsCreateNoWindow == 0 {
		t.Fatalf("CreationFlags=%#x does not include CREATE_NO_WINDOW", cmd.SysProcAttr.CreationFlags)
	}
}

func TestTunnelProcessJobObjectBoundsDescendantLifecycle(t *testing.T) {
	cmd := exec.Command("cmd.exe", "/C", "ping -t 127.0.0.1 >NUL")
	configureTunnelCommand(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child tree: %v", err)
	}
	handle := attachTunnelProcess(cmd)
	if handle.job == 0 {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Skip("runner does not permit assigning a nested Job Object")
	}
	if err := terminateTunnelProcess(cmd, handle, 25*time.Millisecond); err != nil {
		t.Fatalf("terminate job: %v", err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("job-controlled command exited cleanly after forced termination")
	}
}

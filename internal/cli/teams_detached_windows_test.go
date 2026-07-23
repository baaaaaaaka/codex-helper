//go:build windows

package cli

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestTeamsServiceDetachedCommandSetsHideWindowAndPreservesSysProcAttr(t *testing.T) {
	cmd := exec.Command("codex-proxy.exe")
	attr := &syscall.SysProcAttr{CreationFlags: 0x40000000}
	cmd.SysProcAttr = attr

	configureTeamsServiceDetachedCommand(cmd)

	if cmd.SysProcAttr != attr {
		t.Fatalf("configureTeamsServiceDetachedCommand replaced SysProcAttr")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatalf("configureTeamsServiceDetachedCommand did not set HideWindow")
	}
	if cmd.SysProcAttr.CreationFlags&windowsCreateNoWindow == 0 {
		t.Fatalf("configureTeamsServiceDetachedCommand did not set CREATE_NO_WINDOW: %#x", cmd.SysProcAttr.CreationFlags)
	}
	if cmd.SysProcAttr.CreationFlags&0x40000000 == 0 {
		t.Fatalf("configureTeamsServiceDetachedCommand dropped existing creation flags: %#x", cmd.SysProcAttr.CreationFlags)
	}
}

func assertDetachedCommandConfigured(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.SysProcAttr == nil {
		t.Fatal("detached launch did not configure SysProcAttr")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatal("detached launch did not set HideWindow")
	}
	if cmd.SysProcAttr.CreationFlags&windowsCreateNoWindow == 0 {
		t.Fatalf("detached launch did not set CREATE_NO_WINDOW: %#x", cmd.SysProcAttr.CreationFlags)
	}
}

//go:build windows

package cli

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestTeamsServiceDetachedCommandSetsHideWindowAndPreservesSysProcAttr(t *testing.T) {
	cmd := exec.Command("codex-proxy.exe")
	attr := &syscall.SysProcAttr{}
	cmd.SysProcAttr = attr

	configureTeamsServiceDetachedCommand(cmd)

	if cmd.SysProcAttr != attr {
		t.Fatalf("configureTeamsServiceDetachedCommand replaced SysProcAttr")
	}
	if !cmd.SysProcAttr.HideWindow {
		t.Fatalf("configureTeamsServiceDetachedCommand did not set HideWindow")
	}
}

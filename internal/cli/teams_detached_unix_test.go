//go:build !windows

package cli

import (
	"os/exec"
	"syscall"
	"testing"
)

func TestConfigureTeamsServiceDetachedCommandStartsNewSession(t *testing.T) {
	cmd := exec.Command("true")
	attr := &syscall.SysProcAttr{Setpgid: true}
	cmd.SysProcAttr = attr
	configureTeamsServiceDetachedCommand(cmd)
	if cmd.SysProcAttr != attr {
		t.Fatalf("configureTeamsServiceDetachedCommand replaced SysProcAttr")
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Fatalf("detached service command SysProcAttr = %#v, want Setsid", cmd.SysProcAttr)
	}
	if !cmd.SysProcAttr.Setpgid {
		t.Fatalf("configureTeamsServiceDetachedCommand dropped existing SysProcAttr fields")
	}
}

func assertDetachedCommandConfigured(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Fatalf("detached SysProcAttr = %#v, want Setsid", cmd.SysProcAttr)
	}
}

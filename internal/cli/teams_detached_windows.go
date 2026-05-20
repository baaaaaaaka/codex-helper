//go:build windows

package cli

import (
	"os/exec"
	"syscall"
)

func configureTeamsServiceDetachedCommand(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
}

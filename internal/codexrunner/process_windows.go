//go:build windows

package codexrunner

import (
	"os/exec"
	"syscall"
)

const windowsCreateNoWindow = 0x08000000

func configureBackgroundProcess(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.HideWindow = true
	attr.CreationFlags |= windowsCreateNoWindow
	cmd.SysProcAttr = attr
}

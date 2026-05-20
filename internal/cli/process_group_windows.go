//go:build windows

package cli

import (
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

const windowsCreateNoWindow = 0x08000000

func configureTargetProcessGroup(cmd *exec.Cmd) {
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

func terminateTargetCommand(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run(); err == nil {
		return nil
	}
	return terminateProcess(cmd.Process, grace)
}

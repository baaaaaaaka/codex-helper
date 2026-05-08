//go:build !windows

package cli

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

func configureTargetProcessGroup(cmd *exec.Cmd) {
	if cmd == nil {
		return
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	attr.Setpgid = true
	cmd.SysProcAttr = attr
}

func terminateTargetCommand(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	pid := cmd.Process.Pid
	// configureTargetProcessGroup sets the child pgid to its pid. Use the pid
	// directly so descendants are still signalled even if the group leader exits
	// before we ask the kernel for its pgid.
	if err := syscall.Kill(-pid, syscall.SIGINT); err != nil {
		return terminateProcess(cmd.Process, grace)
	}
	deadline := time.Now().Add(grace)
	for grace > 0 && time.Now().Before(deadline) {
		if !processGroupExists(pid) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = cmd.Process.Kill()
	return nil
}

func processGroupExists(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

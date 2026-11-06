//go:build !windows

package harness

import (
	"errors"
	"os/exec"
	"syscall"
)

// setProcessGroup starts cmd in its own process group.
func setProcessGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

func killPID(pid int) { _ = syscall.Kill(pid, syscall.SIGKILL) }

func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

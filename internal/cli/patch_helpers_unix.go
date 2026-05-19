//go:build !windows

package cli

import (
	"os/exec"
	"syscall"
	"time"
)

func configureCodexProbeCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		return cmd.Process.Kill()
	}
	cmd.WaitDelay = 100 * time.Millisecond
}

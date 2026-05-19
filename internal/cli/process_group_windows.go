//go:build windows

package cli

import (
	"os/exec"
	"strconv"
	"time"
)

func configureTargetProcessGroup(cmd *exec.Cmd) {}

func terminateTargetCommand(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	if err := exec.Command("taskkill", "/PID", strconv.Itoa(cmd.Process.Pid), "/T", "/F").Run(); err == nil {
		return nil
	}
	return terminateProcess(cmd.Process, grace)
}

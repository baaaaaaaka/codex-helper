//go:build windows

package cli

import (
	"os/exec"
	"time"
)

func configureTargetProcessGroup(cmd *exec.Cmd) {}

func terminateTargetCommand(cmd *exec.Cmd, grace time.Duration) error {
	if cmd == nil {
		return nil
	}
	return terminateProcess(cmd.Process, grace)
}

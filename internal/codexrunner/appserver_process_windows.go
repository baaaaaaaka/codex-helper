//go:build windows

package codexrunner

import "os/exec"

func configureAppServerProcess(cmd *exec.Cmd) {
	configureBackgroundProcess(cmd)
}

func terminateAppServerProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}

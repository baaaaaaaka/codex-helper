//go:build !windows

package codexrunner

import (
	"os/exec"
	"syscall"
)

// configureAppServerProcess gives the complete app-server process tree its own
// process group. The npm Codex entry point can remain as a wrapper process with
// the native Codex binary as a child, so killing only cmd.Process is not enough
// to prevent the child from continuing to write into CODEX_HOME after Close.
func configureAppServerProcess(cmd *exec.Cmd) {
	configureBackgroundProcess(cmd)
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

func terminateAppServerProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// A process group remains addressable by the original leader's PID while
	// descendants are alive, even if CommandContext won the race and already
	// terminated the wrapper process.
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		_ = cmd.Process.Kill()
	}
}

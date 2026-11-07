package codex

import (
	"os/exec"
	"syscall"
)

// Shared WebSocket servers must not outlive a crashed helper. Unlike stdio,
// closing the helper connection does not terminate the native server.
func configureSharedLifetime(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
}

func attachSharedLifetime(*exec.Cmd) (func(), error) { return func() {}, nil }

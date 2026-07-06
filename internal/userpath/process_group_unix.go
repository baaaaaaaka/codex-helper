//go:build !windows

package userpath

import (
	"context"
	"os/exec"
	"sync"
	"syscall"
)

func configureProbeProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

func killProbeProcessGroup(cmd *exec.Cmd) {
	if cmd != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Process.Kill()
	}
}

func killProbeGroupOnContext(ctx context.Context, cmd *exec.Cmd) func() {
	done := make(chan struct{})
	var once sync.Once
	go func() {
		select {
		case <-ctx.Done():
			killProbeProcessGroup(cmd)
		case <-done:
		}
	}()
	return func() { once.Do(func() { close(done) }) }
}

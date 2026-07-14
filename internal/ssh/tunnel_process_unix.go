//go:build !windows

package ssh

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

type tunnelProcessHandle struct{}

func configureTunnelCommand(cmd *exec.Cmd) {
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

func attachTunnelProcess(*exec.Cmd) tunnelProcessHandle { return tunnelProcessHandle{} }

func closeTunnelProcess(tunnelProcessHandle) {}

func terminateTunnelProcess(cmd *exec.Cmd, _ tunnelProcessHandle, done <-chan struct{}, grace time.Duration) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	pid := cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGINT); err != nil && !errors.Is(err, syscall.ESRCH) {
		_ = cmd.Process.Signal(syscall.SIGINT)
	}
	deadline := time.Now().Add(grace)
	for grace > 0 && time.Now().Before(deadline) {
		select {
		case <-done:
			return nil
		default:
		}
		if !processGroupExists(pid) {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = cmd.Process.Kill()
	return nil
}

func processGroupExists(pgid int) bool {
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

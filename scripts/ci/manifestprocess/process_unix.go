//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package manifestprocess

import (
	"os/exec"
	"syscall"
)

type processHandle struct{}

func configure(cmd *exec.Cmd) {
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

func attach(*exec.Cmd) processHandle { return processHandle{} }

func terminate(cmd *exec.Cmd, _ processHandle) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// configure puts the direct child in a process group whose ID is its PID.
	// Signal the group first so a go wrapper cannot leave its test binary or
	// compiler descendants behind. The direct kill is a fallback for a child
	// that exited before the group signal reached the kernel.
	if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
		_ = cmd.Process.Kill()
	}
}

func closeHandle(processHandle) {}

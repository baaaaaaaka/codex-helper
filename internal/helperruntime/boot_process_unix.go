//go:build !windows

package helperruntime

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

func prepareBootTerminal(cmd *exec.Cmd) func() {
	fd := int(os.Stdin.Fd())
	tty := term.IsTerminal(fd)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if !tty {
		return func() {}
	}
	old, err := term.GetState(fd)
	group, groupErr := unix.IoctlGetInt(fd, unix.TIOCGPGRP)
	if err != nil || groupErr != nil {
		return func() {}
	}
	cmd.SysProcAttr.Foreground, cmd.SysProcAttr.Ctty = true, fd
	return func() {
		// tcsetpgrp from a background process must ignore/block SIGTTOU.
		// Catching it with Notify only interrupts the ioctl and leaves the tty
		// assigned to the dead child; the fallback then fails with EIO.
		ignored := signal.Ignored(syscall.SIGTTOU)
		signal.Ignore(syscall.SIGTTOU)
		_ = unix.IoctlSetPointerInt(fd, unix.TIOCSPGRP, group)
		_ = term.Restore(fd, old)
		_, _ = fmt.Fprint(os.Stdout, "\x1b[?1049l\x1b[?25h\x1b[0m")
		if !ignored {
			signal.Reset(syscall.SIGTTOU)
		}
	}
}

func configureBootProbe(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		return cmd.Process.Kill()
	}
}

func abortBootStartup() {
	// The launcher gave this pre-handoff process its own group. If that
	// launcher was killed, terminate initialization descendants as well.
	if syscall.Getpgrp() == os.Getpid() {
		_ = syscall.Kill(-os.Getpid(), syscall.SIGKILL)
	}
	os.Exit(1)
}

func attachBootProcess(cmd *exec.Cmd) (func(), func(), func() error) {
	return func() {}, func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); _ = cmd.Process.Kill() }, func() error { return nil }
}

func signalBootProcess(cmd *exec.Cmd, sig os.Signal) {
	if s, ok := sig.(syscall.Signal); ok {
		_ = syscall.Kill(-cmd.Process.Pid, s)
	}
}

func bootCancellation(exit *exec.ExitError) (bool, int) {
	if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		switch status.Signal() {
		case syscall.SIGINT:
			return true, 130
		case syscall.SIGTERM:
			return true, 143
		}
	}
	return false, 0
}

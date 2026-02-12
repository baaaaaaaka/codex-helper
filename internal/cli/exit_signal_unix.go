//go:build !windows

package cli

import (
	"errors"
	"os/exec"
	"syscall"
)

// fatalSignals are signals that indicate a binary crash (e.g. from a bad patch).
var fatalSignals = []syscall.Signal{
	syscall.SIGABRT,
	syscall.SIGBUS,
	syscall.SIGFPE,
	syscall.SIGILL,
	syscall.SIGSEGV,
	syscall.SIGSYS,
	syscall.SIGTRAP,
}

// exitDueToFatalSignal returns true if err wraps an exec.ExitError whose
// process was killed by a fatal signal (SIGABRT, SIGSEGV, etc.).
func exitDueToFatalSignal(err error) bool {
	if err == nil {
		return false
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	ws, ok := exitErr.Sys().(syscall.WaitStatus)
	if !ok {
		return false
	}
	if !ws.Signaled() {
		return false
	}
	sig := ws.Signal()
	for _, fs := range fatalSignals {
		if sig == fs {
			return true
		}
	}
	return false
}

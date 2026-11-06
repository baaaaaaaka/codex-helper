//go:build windows

package harness

import (
	"os"
	"os/exec"
)

// The scenarios run on Linux; on Windows the harness only has to build.

func setProcessGroup(*exec.Cmd) {}

func killPID(pid int) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
		_ = p.Release()
	}
}

func pidAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = p.Release()
	return true
}

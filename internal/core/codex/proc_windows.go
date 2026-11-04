//go:build windows

package codex

import (
	"errors"
	"os"
	"os/exec"
)

// On Windows the app-server is killed through its process handle; the PID
// file still lets the next start kill a leftover server.
func configure(*exec.Cmd) {}

func terminateGroup(pid int) error { return killGroup(pid) }

func killGroup(pid int) error {
	if pid <= 0 {
		return errors.New("codex: bad pid")
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return p.Kill()
}

func groupAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = p.Release()
	return false
}

func startTime(int) string { return "" }

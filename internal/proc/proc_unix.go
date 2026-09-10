//go:build !windows

package proc

import (
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

func IsAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	if err != nil {
		return false
	}
	if runtime.GOOS != "linux" {
		return true
	}
	return !isLinuxZombie(pid)
}

func isLinuxZombie(pid int) bool {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		// kill(pid, 0) and this procfs read are not atomic. If the process
		// exits between them, the missing stat entry is definitive evidence
		// that it is no longer alive; treating it as non-zombie creates a
		// false positive for callers that use IsAlive for cleanup checks.
		return errors.Is(err, os.ErrNotExist)
	}
	state, ok := linuxProcStateFromStat(string(raw))
	return ok && state == 'Z'
}

func linuxProcStateFromStat(text string) (byte, bool) {
	end := strings.LastIndex(text, ")")
	if end < 0 || end+2 >= len(text) {
		return 0, false
	}
	return text[end+2], true
}

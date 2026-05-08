//go:build !windows

package proc

import (
	"os"
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
	return !isLinuxZombie(pid)
}

func isLinuxZombie(pid int) bool {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
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

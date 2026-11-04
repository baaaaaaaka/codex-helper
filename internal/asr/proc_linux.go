package asr

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

// setDeathSignal makes the kernel kill the child when the helper dies, so a
// resident llama-server never outlives the helper.
func setDeathSignal(attr *syscall.SysProcAttr) {
	if deathSignal {
		attr.Pdeathsig = syscall.SIGKILL
	}
}

// processID identifies a running process across PID reuse: its start time
// (clock ticks since boot) and executable. Zombies count as gone.
func processID(pid int) (string, bool) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", false
	}
	s := string(stat)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:]) // after "pid (comm)"
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
		return "", false
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return "", false
	}
	return fields[19] + " " + strings.TrimSuffix(exe, " (deleted)"), true
}

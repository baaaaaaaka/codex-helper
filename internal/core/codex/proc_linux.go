//go:build linux

package codex

import (
	"os"
	"strconv"
	"strings"
)

// startTime returns an identifier of when pid started (the start time in
// clock ticks since boot from /proc), or "" when unknown. It guards against
// killing a recycled pid.
func startTime(pid int) string {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	s := string(data)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return ""
	}
	f := strings.Fields(s[i+1:])
	if len(f) < 20 {
		return ""
	}
	return f[19] // field 22 overall: starttime
}

// bootID identifies this kernel instance: it changes on every boot and
// differs between machines.
func bootID() string {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// pidNamespace identifies the pid namespace pids are numbered in.
func pidNamespace() string {
	s, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return ""
	}
	return s
}

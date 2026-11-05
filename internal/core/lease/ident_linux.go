//go:build linux

package lease

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func pidNamespace() string {
	s, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return ""
	}
	return s
}

// startTime is field 22 of /proc/<pid>/stat (clock ticks since boot): with
// the pid it identifies a process across pid reuse.
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
	return f[19]
}

// bootTime is when this machine booted (btime in /proc/stat).
func bootTime() time.Time {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return time.Time{}
	}
	for _, line := range strings.Split(string(data), "\n") {
		if rest, ok := strings.CutPrefix(line, "btime "); ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(rest), 10, 64); err == nil && n > 0 {
				return time.Unix(n, 0)
			}
		}
	}
	return time.Time{}
}

// mountInfo is this process's /proc/self/mountinfo.
func mountInfo() (string, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	return string(data), err
}

//go:build linux

package lease

import (
	"os"
	"strconv"
	"strings"
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

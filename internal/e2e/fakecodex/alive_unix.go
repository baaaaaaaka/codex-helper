//go:build !windows

package fakecodex

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
)

func pidAlive(pid int) bool {
	// Minimal containers may not reap orphan zombies. A zombie cannot own
	// a writer or continue a turn, even though signal 0 still succeeds.
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
			if end := strings.LastIndexByte(string(data), ')'); end >= 0 {
				fields := strings.Fields(string(data[end+1:]))
				if len(fields) > 0 && (fields[0] == "Z" || fields[0] == "X") {
					return false
				}
			}
		}
	}

	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

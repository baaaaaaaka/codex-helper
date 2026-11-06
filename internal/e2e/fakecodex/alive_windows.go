//go:build windows

package fakecodex

import "os"

// pidAlive: on Windows FindProcess opens the process and fails when it is
// gone.
func pidAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	_ = p.Release()
	return true
}

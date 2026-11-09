//go:build windows

package fakecodex

import "golang.org/x/sys/windows"

// Opening a process can succeed after it exits while a handle is retained.
// An exited process is signaled; a live process times out at zero wait.
func pidAlive(pid int) bool {
	h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err != nil {
		return false
	}
	defer windows.CloseHandle(h)
	status, err := windows.WaitForSingleObject(h, 0)
	return err == nil && status == uint32(windows.WAIT_TIMEOUT)
}

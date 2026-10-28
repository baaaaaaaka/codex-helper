//go:build windows

package proc

import (
	"errors"

	"golang.org/x/sys/windows"
)

func IsAlive(pid int) bool {
	alive, _ := CheckAlive(pid)
	return alive
}

// CheckAlive distinguishes a confirmed exit from an inspection failure.
func CheckAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, nil
	}

	h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if err != nil {
		if errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
			return false, nil
		}
		return false, err
	}
	defer windows.CloseHandle(h)

	var code uint32
	if err := windows.GetExitCodeProcess(h, &code); err != nil {
		return false, err
	}

	// Windows STILL_ACTIVE (259) indicates the process has not terminated.
	const stillActive = 259
	return code == stillActive, nil
}

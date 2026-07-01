//go:build windows

package candidateupdate

import (
	"os"

	"golang.org/x/sys/windows"
)

func directParentExecutable() (string, error) {
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(os.Getppid()))
	if err != nil {
		return "", err
	}
	defer windows.CloseHandle(handle)
	buffer := make([]uint16, 32768)
	size := uint32(len(buffer))
	if err := windows.QueryFullProcessImageName(handle, 0, &buffer[0], &size); err != nil {
		return "", err
	}
	return windows.UTF16ToString(buffer[:size]), nil
}

func replaceLegacyStableEntry(candidate string, entry string, runningRuntime string, root string) error {
	// The stable cxp.exe is the live launcher parent and cannot be replaced.
	// Activating the immutable target is sufficient: after one service restart
	// the old launcher dispatches into the new runtime, whose fixed fresh-entry
	// validation no longer requires the physical launcher version to match.
	return nil
}

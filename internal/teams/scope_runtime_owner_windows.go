//go:build windows

package teams

import (
	"errors"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"golang.org/x/sys/windows"
)

func runtimeStoreOwnerExitConfirmed(owner teamstore.OwnerMetadata) bool {
	if owner.PID <= 0 || !teamstore.OwnerAppearsLocal(owner) {
		return false
	}
	handle, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(owner.PID))
	if err != nil {
		// Access denied is not proof of exit. Windows reports an absent PID as
		// ERROR_INVALID_PARAMETER.
		return errors.Is(err, windows.ERROR_INVALID_PARAMETER)
	}
	defer windows.CloseHandle(handle)
	var exitCode uint32
	if err := windows.GetExitCodeProcess(handle, &exitCode); err != nil {
		return false
	}
	const stillActive = 259
	return exitCode != stillActive
}

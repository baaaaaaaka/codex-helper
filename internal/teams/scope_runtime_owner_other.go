//go:build !linux && !windows

package teams

import (
	"errors"
	"syscall"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func runtimeStoreOwnerExitConfirmed(owner teamstore.OwnerMetadata) bool {
	if owner.PID <= 0 || !teamstore.OwnerAppearsLocal(owner) {
		return false
	}
	err := syscall.Kill(owner.PID, 0)
	// ESRCH is positive evidence that the PID is absent. EPERM and every other
	// result remain unknown so migration stays fail-closed.
	return errors.Is(err, syscall.ESRCH)
}

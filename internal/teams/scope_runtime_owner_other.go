//go:build !linux

package teams

import teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"

func runtimeStoreOwnerExitConfirmed(teamstore.OwnerMetadata) bool {
	// Other platforms do not expose a cheap process-identity primitive that is
	// uniform across service and sandbox boundaries. The managed stop path is
	// expected to clear owner metadata; otherwise migration remains deferred.
	return false
}
